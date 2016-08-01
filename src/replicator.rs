use std::collections::VecDeque;
use std::collections::HashMap;
use io::Storage;
use io::Postbox;
use io::Timer;
use super::*;

pub enum Error<M, IO>
    where M: Machine,
          IO: io::IoModule<M>
{
    NotClusterMember,
    Storage(<IO::Storage as Storage<M>>::Error),
    Consensus(consensus::Error),
}
impl<M, IO> From<consensus::Error> for Error<M, IO>
    where M: Machine,
          IO: io::IoModule<M>
{
    fn from(f: consensus::Error) -> Self {
        Error::Consensus(f)
    }
}

// TODO: エイリアスを定義しない方が良いかも
pub type Result<T, M, IO> = ::std::result::Result<T, Error<M, IO>>;

pub struct Replicator<M, IO>
    where M: Machine
{
    machine: M,
    io: IO,
    last_applied: log::EntryVersion,
    last_commited: log::EntryVersion,
    consensus: consensus::ConsensusModule,
    event_queue: VecDeque<Event>,
    action_queue: VecDeque<Action<M>>,
    syncings: VecDeque<(consensus::Timestamp, AsyncKey)>,
    commitings: VecDeque<(log::Index, AsyncKey)>,
    applicables: VecDeque<(Option<AsyncKey>, log::Entry<M::Command>)>,
    snapshotting: bool,
    io_waits: HashMap<AsyncKey, Action<M>>,
    next_key: AsyncKey,
}
impl<M, IO> Replicator<M, IO>
    where M: Machine,
          IO: io::IoModule<M>
{
    pub fn new(node_id: &NodeId, mut io: IO, config: config::Builder) -> Result<Self, M, IO> {
        let config = config.build();
        if !config.is_member(node_id) {
            return Err(Error::NotClusterMember);
        }

        let initial_version = log::EntryVersion::new(0, 0);
        let ballot = election::Ballot::new(initial_version.term);
        io.storage_mut().log_truncate(initial_version.index, 0);
        io.storage_mut().log_append(vec![log::Entry::noop(initial_version.term)], 0);

        let machine = M::default();
        let snapshot = Snapshot::new(&config, &initial_version, &machine);
        io.storage_mut().save_snapshot(snapshot, 0);
        io.storage_mut().save_ballot(ballot.clone(), 0);
        try!(io.storage_mut().flush().map_err(Error::Storage));

        let log_index_table = io.storage_ref().build_log_index_table();
        let consensus = consensus::ConsensusModule::new(node_id, &ballot, &config, log_index_table);
        Ok(Self::new_impl(machine, io, consensus, initial_version))
    }
    pub fn load(node_id: &NodeId, mut io: IO) -> Result<Option<Self>, M, IO> {
        try!(io.storage_mut().flush().map_err(Error::Storage));

        io.storage_mut().load_ballot();
        let ballot = match try!(io.storage_mut().run_once().result.map_err(Error::Storage)) {
            io::StorageData::Ballot(x) => x,
            io::StorageData::None => return Ok(None),
            _ => unreachable!(),
        };

        io.storage_mut().load_snapshot();
        let snapshot = match try!(io.storage_mut().run_once().result.map_err(Error::Storage)) {
            io::StorageData::Snapshot(x) => x,
            io::StorageData::None => return Ok(None),
            _ => unreachable!(),
        };
        io.storage_mut().log_drop_until(snapshot.last_included.index + 1, 0);
        try!(io.storage_mut().flush().map_err(Error::Storage));

        let log_index_table = io.storage_ref().build_log_index_table();
        let consensus =
            consensus::ConsensusModule::new(node_id, &ballot, &snapshot.config, log_index_table);
        let machine = snapshot.state.into();
        Ok(Some(Self::new_impl(machine, io, consensus, snapshot.last_included)))
    }
    pub fn try_run_once(&mut self) -> Result<Option<Event>, M, IO> {
        if let Some(event) = self.event_queue.pop_front() {
            return Ok(Some(event));
        }
        if let Some(key) = self.apply_if_possible() {
            return Ok(Some(Event::Commited(key)));
        }
        self.check_timer();
        self.check_postbox();
        try!(self.check_storage());
        if let Some(action) = self.action_queue.pop_front() {
            try!(self.handle_action(action));
        }
        self.check_sync();
        Ok(self.event_queue.pop_front())
    }
    pub fn state(&self) -> &M {
        &self.machine
    }
    pub fn execute(&mut self, command: M::Command) -> AsyncKey {
        let key = self.next_key();
        self.action_queue.push_back(Action::Execute(key, command));
        key
    }
    pub fn update_config(&mut self, config: config::Builder) -> AsyncKey {
        let key = self.next_key();
        self.action_queue.push_back(Action::UpdateConfig(key, config.build()));
        key
    }
    pub fn sync(&mut self) -> AsyncKey {
        let key = self.next_key();
        let timestamp = self.consensus.timestamp();
        self.action_queue.push_back(Action::Sync(timestamp));
        self.syncings.push_back((timestamp, key));
        key
    }
    pub fn take_snapshot(&mut self) -> Option<AsyncKey> {
        if self.snapshotting {
            None
        } else {
            let key = self.next_key();
            self.snapshotting = true;
            let snapshot =
                Snapshot::new(self.consensus.config(), &self.last_applied, &self.machine);
            self.io.storage_mut().save_snapshot(snapshot, key);
            self.io_waits.insert(key,
                                 Action::HandleSnapshotSaved {
                                     key: key,
                                     last_included: self.last_applied.clone(),
                                 });
            Some(key)
        }
    }

    fn apply_if_possible(&mut self) -> Option<AsyncKey> {
        self.applicables.pop_front().and_then(|(key, entry)| {
            self.last_applied.term = entry.term;
            self.last_applied.index += 1;
            match entry.data {
                log::Data::Noop => {}
                log::Data::Config(config) => {
                    self.event_queue.push_back(Event::ConfigChanged(config));
                }
                log::Data::Command(command) => {
                    self.machine.execute(command);
                }
            }
            key
        })
    }
    fn check_sync(&mut self) {
        while !self.syncings.is_empty() {
            if self.syncings.front().unwrap().0 >= self.consensus.timestamp() {
                break;
            }
            let (_, key) = self.syncings.pop_front().unwrap();
            self.event_queue.push_back(Event::Synced(key));
        }
    }
    fn check_timer(&mut self) {
        if self.io.timer_ref().is_elapsed() {
            self.action_queue.push_front(Action::HandleTimeout); // high priority
            self.io.timer_mut().clear();
        }
    }
    fn check_postbox(&mut self) {
        if let Some(message) = self.io.postbox_mut().try_recv() {
            self.action_queue.push_back(Action::HandleMessage(message));
        }
    }
    fn check_storage(&mut self) -> Result<(), M, IO> {
        panic!()
    }
    fn handle_action(&mut self, action: Action<M>) -> Result<(), M, IO> {
        match action {
            Action::Noop => {}
            Action::Wait(key) => {
                if self.io_waits.contains_key(&key) {
                    self.action_queue.push_front(Action::Wait(key));
                }
            }
            Action::HandleTimeout => {
                let actions = try!(self.consensus.handle_timeout());
                self.action_queue.extend(actions.into_iter().map(Action::Consensus));
            }
            Action::HandleMessage(message) => {
                let actions = try!(self.consensus.handle_message(message));
                self.action_queue.extend(actions.into_iter().map(Action::Consensus));
            }
            Action::HandleSnapshotSaved { key, last_included } => {
                let drop_key = self.next_key();
                try!(self.consensus.handle_snapshot_installed(&last_included));
                self.io.storage_mut().log_drop_until(last_included.index + 1, drop_key);
                self.io_waits.insert(drop_key, Action::Noop);
                self.snapshotting = false;
                self.event_queue.push_back(Event::SnapshotInstalled(key));
            }
            Action::Execute(key, command) => {
                if let Some(actions) = try!(self.consensus.propose(command)) {
                    let index = self.io.storage_ref().last_appended().index + 1;
                    self.commitings.push_back((index, key));
                    self.action_queue.extend(actions.into_iter().map(Action::Consensus));
                } else {
                    let reason = AbortReason::NotLeader(self.consensus.leader().cloned());
                    self.event_queue.push_back(Event::Aborted(key, reason));
                }
            }
            Action::UpdateConfig(key, config) => {
                if let Some(actions) = try!(self.consensus.update_config(config)) {
                    let index = self.io.storage_ref().last_appended().index + 1;
                    self.commitings.push_back((index, key));
                    self.action_queue.extend(actions.into_iter().map(Action::Consensus));
                } else {
                    let reason = AbortReason::NotLeader(self.consensus.leader().cloned());
                    self.event_queue.push_back(Event::Aborted(key, reason));
                }
            }
            Action::Sync(timestamp) => {
                if timestamp < self.consensus.timestamp() {
                    // NOTE: Already synced
                    return Ok(());
                }
                let actions = try!(self.consensus.sync());
                self.action_queue.extend(actions.into_iter().map(Action::Consensus));
            }
            Action::Consensus(action) => {
                try!(self.handle_consensus_action(action));
            }
        }
        Ok(())
    }
    fn handle_consensus_action(&mut self, action: consensus::Action<M>) -> Result<(), M, IO> {
        use consensus as C;
        match action {
            C::Action::ResetTimeout(kind) => {
                let after = self.io.timer_ref().calc_after(kind, self.consensus.config());
                self.io.timer_mut().reset(&after);
            }
            C::Action::Postpone(message) => {
                self.action_queue.push_front(Action::HandleMessage(message));
            }
            C::Action::BroadcastMsg(_) => panic!(),
            C::Action::UnicastMsg(_, _) => panic!(),
            C::Action::UnicastLog { .. } => panic!(),
            C::Action::InstallSnapshot(_) => panic!(),
            C::Action::SaveBallot(ballot) => {
                let key = self.next_key();
                self.io.storage_mut().save_ballot(ballot, key);
                self.io_waits.insert(key, Action::Noop);
                self.action_queue.push_front(Action::Wait(key));
            }
            C::Action::LogAppend(entries) => {
                let key = self.next_key();
                self.io.storage_mut().log_append(entries, key);
                self.io_waits.insert(key, Action::Noop);
                self.action_queue.push_front(Action::Wait(key));
            }
            C::Action::LogRollback(next_index) => {
                // TODO: handle commiging
                // TODO: handle conflict (rollbacks of commited entries)
                let key = self.next_key();
                self.io.storage_mut().log_truncate(next_index, key);
                self.io_waits.insert(key, Action::Noop);
                self.action_queue.push_front(Action::Wait(key));
            }
            C::Action::LogApply(last_commited) => {
                self.last_commited = last_commited;
                // TODO: handle commiging
            }
        }
        Ok(())
    }
    fn new_impl(machine: M,
                io: IO,
                consensus: consensus::ConsensusModule,
                last_commited: log::EntryVersion)
                -> Self {
        Replicator {
            machine: machine,
            io: io,
            consensus: consensus,
            last_applied: last_commited.clone(),
            last_commited: last_commited,
            event_queue: VecDeque::new(),
            action_queue: VecDeque::new(),
            syncings: VecDeque::new(),
            commitings: VecDeque::new(),
            applicables: VecDeque::new(),
            io_waits: HashMap::new(),
            next_key: 0,
            snapshotting: false,
        }
    }
    fn next_key(&mut self) -> AsyncKey {
        let key = self.next_key;
        self.next_key = self.next_key.wrapping_add(1);
        key
    }
}

pub enum Action<M>
    where M: Machine
{
    Noop,
    Wait(AsyncKey),
    HandleTimeout,
    HandleMessage(consensus::Message<M>),
    Execute(AsyncKey, M::Command),
    UpdateConfig(AsyncKey, config::Config),
    Sync(consensus::Timestamp),
    Consensus(consensus::Action<M>),
    HandleSnapshotSaved {
        key: AsyncKey,
        last_included: log::EntryVersion,
    },
}

pub enum Event {
    Commited(AsyncKey),
    Aborted(AsyncKey, AbortReason),
    Synced(AsyncKey),
    SnapshotInstalled(AsyncKey),
    ConfigChanged(config::Config),
}
pub enum AbortReason {
    NotLeader(Option<NodeId>),
}

pub struct Snapshot<M>
    where M: Machine
{
    pub last_included: log::EntryVersion,
    pub config: config::Config,
    pub state: M::Snapshot,
}
impl<M> Snapshot<M>
    where M: Machine
{
    pub fn new(config: &config::Config, last_included: &log::EntryVersion, machine: &M) -> Self {
        Snapshot {
            last_included: last_included.clone(),
            config: config.clone(),
            state: machine.take_snapshot(),
        }
    }
}
