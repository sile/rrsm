use std::collections::VecDeque;
use std::collections::HashMap;
use io::Storage;
use io::Postbox;
use io::Timer;
use super::*;

// TODO: Debug
pub enum Error<R>
    where R: Rsm
{
    NotClusterMember,
    CommitConflict,
    Storage(<R::Storage as Storage<R::Machine>>::Error),
}

pub struct Replicator<R>
    where R: Rsm
{
    machine: R::Machine,
    storage: R::Storage,
    postbox: R::Postbox,
    timer: R::Timer,
    last_applied: log::EntryVersion,
    last_commited: log::EntryVersion,
    consensus: consensus::ConsensusModule<R::Machine>,
    event_queue: VecDeque<Event>,
    action_queue: VecDeque<Action<R::Machine>>,
    syncings: VecDeque<(consensus::Timestamp, AsyncKey)>,
    commitings: VecDeque<(log::Index, AsyncKey)>,
    applicables: VecDeque<(Option<AsyncKey>, log::Entry<<R::Machine as Machine>::Command>)>,
    snapshotting: bool,
    io_waits: HashMap<AsyncKey, Reaction>,
    next_key: AsyncKey,
}
impl<R> Replicator<R>
    where R: Rsm,
          R::Machine: 'static
{
    pub fn new(node_id: &NodeId,
               mut storage: R::Storage,
               postbox: R::Postbox,
               timer: R::Timer, // TODO: default
               config: config::Builder)
               -> Result<Self, Error<R>> {
        let config = config.build();
        if !config.is_member(node_id) {
            return Err(Error::NotClusterMember);
        }

        let initial_version = log::EntryVersion::new(0, 0);
        let ballot = election::Ballot::new(initial_version.term);
        storage.log_truncate(initial_version.index, 0);
        storage.log_append(vec![log::Entry::noop(initial_version.term)], 0);

        let machine = R::Machine::default();
        let snapshot = Snapshot::new(&config, &initial_version, &machine);
        storage.save_snapshot(snapshot, 0);
        storage.save_ballot(ballot.clone(), 0);
        try!(storage.flush().map_err(Error::Storage));

        let log_index_table = storage.build_log_index_table();
        let consensus = consensus::ConsensusModule::new(node_id, &ballot, &config, log_index_table);
        Ok(Self::new_impl(machine, storage, postbox, timer, consensus, initial_version))
    }
    // TODO:
    // pub fn load(node_id: &NodeId, mut io: IO) -> Result<Option<Self>, Error<M, IO>> {
    //     try!(storage.flush().map_err(Error::Storage));

    //     storage.load_ballot(0);
    //     let ballot = match try!(storage.run_once().result.map_err(Error::Storage)) {
    //         io::StorageData::Ballot(x) => x,
    //         io::StorageData::None => return Ok(None),
    //         _ => unreachable!(),
    //     };

    //     storage.load_snapshot(0);
    //     let snapshot = match try!(storage.run_once().result.map_err(Error::Storage)) {
    //         io::StorageData::Snapshot(x) => x,
    //         io::StorageData::None => return Ok(None),
    //         _ => unreachable!(),
    //     };
    //     storage.log_drop_until(snapshot.last_included.index + 1, 0);
    //     try!(storage.flush().map_err(Error::Storage));

    //     let log_index_table = storage.build_log_index_table();
    //     let consensus =
    //         consensus::ConsensusModule::new(node_id, &ballot, &snapshot.config, log_index_table);
    //     let machine = snapshot.state.into();
    //     Ok(Some(Self::new_impl(machine, io, consensus, snapshot.last_included)))
    // }
    // pub fn io_module(&mut self) -> &mut IO {
    //     &mut self.io
    // }
    pub fn timer_mut(&mut self) -> &mut R::Timer {
        &mut self.timer
    }
    pub fn try_run_once(&mut self) -> Result<Option<Event>, Error<R>> {
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
    pub fn state(&self) -> &R::Machine {
        &self.machine
    }
    pub fn execute(&mut self, command: <R::Machine as Machine>::Command) -> AsyncKey {
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
            let snapshot =
                Snapshot::new(self.consensus.config(), &self.last_applied, &self.machine);
            self.install_snapshot(None, snapshot, Some(key));
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
                    let is_member = config.is_member(&self.consensus.node().id);
                    self.event_queue.push_back(Event::ConfigChanged(config));
                    if !is_member {
                        self.event_queue.push_back(Event::Purged);
                    }
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
        if self.timer.is_elapsed() {
            println!("{}: elapsed", self.consensus.node().id);
            self.action_queue.push_back(Action::HandleTimeout);
            self.timer.clear();
        }
    }
    fn check_postbox(&mut self) {
        if let Some(message) = self.postbox.try_recv() {
            self.action_queue.push_back(Action::HandleMessage(message));
        }
    }
    fn check_storage(&mut self) -> Result<(), Error<R>> {
        if let Some(async) = self.storage.try_run_once() {
            let key = async.key;
            let data = try!(async.result.map_err(Error::Storage));
            let reaction = self.io_waits.remove(&key).unwrap();
            match data {
                io::StorageData::Ballot(_) => unreachable!(),
                io::StorageData::Entries(entries) => {
                    match reaction {
                        Reaction::ApplyEntries => {
                            let mut index = self.last_applied.index + 1;
                            for e in entries {
                                let key =
                                    if self.commitings.front().map_or(false, |x| x.0 == index) {
                                        self.commitings.pop_front().take().map(|x| x.1)
                                    } else {
                                        None
                                    };
                                self.applicables.push_back((key, e));
                                index += 1;
                            }
                        }
                        Reaction::SendAppendEntries(node, header) => {
                            let message =
                                consensus::Message::make_append_entries_call(header,
                                                                             entries,
                                                                             self.last_commited
                                                                                 .index);
                            self.postbox.send_val(&node, message);
                        }
                        _ => unreachable!(),
                    }
                }
                io::StorageData::Snapshot(snapshot) => {
                    match reaction {
                        Reaction::SendSnapshot(node, header) => {
                            let message = consensus::Message::make_install_snapshot_call(header,
                                                                                         snapshot);
                            self.postbox.send_val(&node, message);
                        }
                        Reaction::LoadSnapshot(key) => {
                            self.last_applied = snapshot.last_included.clone();
                            self.last_commited = snapshot.last_included.clone();
                            self.machine = snapshot.state.into();
                            if let Some(key) = key {
                                self.event_queue.push_back(Event::SnapshotInstalled(key));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                io::StorageData::None => {
                    match reaction {
                        Reaction::HandleSnapshotSaved { key, node_id, config, last_included } => {
                            let actions = self.consensus
                                .handle_snapshot_installed(node_id, config, &last_included);
                            self.action_queue.extend(actions.into_iter().map(Action::Consensus));

                            let drop_key = self.next_key();
                            self.storage.log_drop_until(last_included.index + 1, drop_key);
                            self.io_waits.insert(drop_key, Reaction::Noop);
                            self.snapshotting = false;
                            if last_included.index > self.last_applied.index {
                                let snapshot_key = self.next_key();
                                self.storage.load_snapshot(snapshot_key);
                                self.io_waits.insert(snapshot_key, Reaction::LoadSnapshot(key));
                                self.action_queue.push_front(Action::Wait(snapshot_key));
                            } else {
                                if let Some(key) = key {
                                    self.event_queue.push_back(Event::SnapshotInstalled(key));
                                }
                            }
                        }
                        Reaction::Noop => {}
                        _ => unreachable!(),
                    }
                }
            }
        }
        Ok(())
    }
    fn handle_action(&mut self, action: Action<R::Machine>) -> Result<(), Error<R>> {
        match action {
            Action::Wait(key) => {
                if self.io_waits.contains_key(&key) {
                    self.action_queue.push_front(Action::Wait(key));
                }
            }
            Action::HandleTimeout => {
                let actions = self.consensus.handle_timeout();
                self.action_queue.extend(actions.into_iter().map(Action::Consensus));
            }
            Action::HandleMessage(message) => {
                let actions = self.consensus.handle_message(message);
                self.action_queue.extend(actions.into_iter().map(Action::Consensus));
            }
            Action::Execute(key, command) => {
                if let Some(actions) = self.consensus.propose(command) {
                    let index = self.storage.last_appended().index;
                    self.commitings.push_back((index, key));
                    self.action_queue.extend(actions.into_iter().map(Action::Consensus));
                } else {
                    let reason = AbortReason::NotLeader(self.consensus.leader().cloned());
                    self.event_queue.push_back(Event::Aborted(key, reason));
                }
            }
            Action::UpdateConfig(key, config) => {
                if let Some(actions) = self.consensus.update_config(config) {
                    let index = self.storage.last_appended().index + 1;
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
                let actions = self.consensus.sync();
                self.action_queue.extend(actions.into_iter().map(Action::Consensus));
            }
            Action::Consensus(action) => {
                try!(self.handle_consensus_action(action));
            }
        }
        Ok(())
    }
    fn install_snapshot(&mut self,
                        node_id: Option<NodeId>,
                        snapshot: Snapshot<R::Machine>,
                        event_key: Option<AsyncKey>)
                        -> bool {
        if self.snapshotting {
            false
        } else {
            let key = self.next_key();
            let last_included = snapshot.last_included.clone();
            let config = snapshot.config.clone();
            self.snapshotting = true;
            self.storage.save_snapshot(snapshot, key);
            self.io_waits.insert(key,
                                 Reaction::HandleSnapshotSaved {
                                     key: event_key,
                                     node_id: node_id,
                                     config: config,
                                     last_included: last_included,
                                 });
            true
        }
    }
    fn handle_consensus_action(&mut self,
                               action: consensus::Action<R::Machine>)
                               -> Result<(), Error<R>> {
        use consensus as C;
        match action {
            C::Action::ResetTimeout(kind) => {
                let after = self.timer.calc_after(kind, self.consensus.config());
                self.timer.reset(after);
            }
            C::Action::Postpone(message) => {
                self.action_queue.push_front(Action::HandleMessage(message));
            }
            C::Action::BroadcastMsg(message) => {
                for node in self.consensus.config().all_members() {
                    if *node != self.consensus.node().id {
                        self.postbox.send_ref(&node.clone(), &message);
                    }
                }
            }
            C::Action::UnicastMsg(node, message) => {
                self.postbox.send_val(&node, message);
            }
            C::Action::UnicastLog { destination, header, first_index, max_len } => {
                // TODO: snapshotかどうかの判定はconsensusに任せるかも
                let key = self.next_key();
                if first_index < self.storage.least_stored().index {
                    self.storage.load_snapshot(key);
                    self.io_waits.insert(key, Reaction::SendSnapshot(destination, header));
                } else {
                    self.storage.log_get(first_index, max_len, key);
                    self.io_waits.insert(key, Reaction::SendAppendEntries(destination, header));
                }
            }
            C::Action::InstallSnapshot(node_id, snapshot) => {
                self.install_snapshot(Some(node_id), snapshot, None);
            }
            C::Action::SaveBallot(ballot) => {
                let key = self.next_key();
                self.storage.save_ballot(ballot, key);
                self.io_waits.insert(key, Reaction::Noop);
                self.action_queue.push_front(Action::Wait(key));
            }
            C::Action::LogAppend(entries) => {
                let key = self.next_key();
                self.storage.log_append(entries, key);
                self.io_waits.insert(key, Reaction::Noop);
                self.action_queue.push_front(Action::Wait(key));
            }
            C::Action::LogRollback(next_index) => {
                if self.last_commited.index <= next_index {
                    // TODO: エラーが発生したら
                    // 以降は継続不可にする
                    return Err(Error::CommitConflict);
                }
                let key = self.next_key();
                self.storage.log_truncate(next_index, key);
                self.io_waits.insert(key, Reaction::Noop);
                self.action_queue.push_front(Action::Wait(key));
                while !self.commitings.is_empty() {
                    let &(index, key) = self.commitings.back().unwrap();
                    if index >= next_index {
                        self.event_queue.push_back(Event::Aborted(key, AbortReason::Rollbacked));
                        self.commitings.pop_back();
                    } else {
                        break;
                    }
                }
            }
            C::Action::LogApply(last_commited) => {
                let key = self.next_key();
                let max_len = (last_commited.index - self.last_applied.index) as usize;
                self.last_commited = last_commited;
                self.storage.log_get(self.last_applied.index + 1, max_len, key);
                self.io_waits.insert(key, Reaction::ApplyEntries);
                self.action_queue.push_front(Action::Wait(key));
            }
        }
        Ok(())
    }
    fn new_impl(machine: R::Machine,
                storage: R::Storage,
                postbox: R::Postbox,
                timer: R::Timer,
                mut consensus: consensus::ConsensusModule<R::Machine>,
                last_commited: log::EntryVersion)
                -> Self {
        let actions = consensus.init();
        Replicator {
            machine: machine,
            storage: storage,
            postbox: postbox,
            timer: timer,
            consensus: consensus,
            last_applied: last_commited.clone(),
            last_commited: last_commited,
            event_queue: VecDeque::new(),
            action_queue: actions.into_iter().map(Action::Consensus).collect(),
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
    Wait(AsyncKey),
    HandleTimeout,
    HandleMessage(consensus::Message<M>),
    Execute(AsyncKey, M::Command),
    UpdateConfig(AsyncKey, config::Config),
    Sync(consensus::Timestamp),
    Consensus(consensus::Action<M>),
}

pub enum Reaction {
    Noop,
    HandleSnapshotSaved {
        key: Option<AsyncKey>,
        node_id: Option<NodeId>,
        config: config::Config,
        last_included: log::EntryVersion,
    },
    ApplyEntries,
    SendSnapshot(NodeId, consensus::MessageHeader),
    SendAppendEntries(NodeId, consensus::MessageHeader),
    LoadSnapshot(Option<AsyncKey>),
}

#[derive(Debug)]
pub enum Event {
    Commited(AsyncKey),
    Aborted(AsyncKey, AbortReason),
    Synced(AsyncKey),
    SnapshotInstalled(AsyncKey),
    ConfigChanged(config::Config),
    Purged,
}

#[derive(Debug)]
pub enum AbortReason {
    NotLeader(Option<NodeId>),
    Rollbacked,
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
impl<M> Clone for Snapshot<M>
    where M: Machine,
          M::Snapshot: Clone
{
    fn clone(&self) -> Self {
        Snapshot {
            last_included: self.last_included.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
        }
    }
}
