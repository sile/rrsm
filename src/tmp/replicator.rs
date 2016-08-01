#![allow(unused_variables)]
use super::*;

pub struct MessageHeader {
    // TODO: clock or timestamp or seq
    pub from: NodeId,
    pub node_instance_id: usize,
    pub version: Version,
}

pub struct Message<T> {
    pub header: MessageHeader,
    pub data: T,
}

pub enum Error<S> {
    NotMember,
    Storage(S),
    Consensus(raft::Error),
    CommitConfliction,
}
impl<S> From<raft::Error> for Error<S> {
    fn from(e: raft::Error) -> Self {
        Error::Consensus(e)
    }
}

pub type Result<T, S> = ::std::result::Result<T, Error<S>>;

pub struct ConsensusModule;
impl ConsensusModule {
    pub fn new(id: &NodeId,
               ballot: &Ballot,
               config: &config::Config,
               table: io::LogIndexTable)
               -> Self {
        panic!()
    }
    pub fn config(&self) -> &config::Config {
        panic!()
    }
    pub fn handle_timeout<T: Machine>(&mut self)
                                      -> ::std::result::Result<Vec<Action<T>>, raft::Error> {
        panic!()
    }
    pub fn handle_snapshot_installed<T: Machine>
        (&mut self,
         new_first_index: LogIndex)
         -> ::std::result::Result<Vec<Action<T>>, raft::Error> {
        panic!()
    }
    pub fn handle_sync<T: Machine>(&mut self)
                                   -> ::std::result::Result<Vec<Action<T>>, raft::Error> {
        panic!()
    }
    pub fn handle_message<T: Machine>(&mut self,
                                      message: Message<T::Command>)
                                      -> ::std::result::Result<Vec<Action<T>>, raft::Error> {
        panic!()
    }
    pub fn handle_command<T: Machine>(&mut self,
                                      command: T::Command)
                                      -> ::std::result::Result<Vec<Action<T>>, raft::Error> {
        panic!()
    }
    pub fn handle_update_config<T: Machine>
        (&mut self,
         config: config::Config)
         -> ::std::result::Result<Vec<Action<T>>, raft::Error> {
        panic!()
    }
    pub fn members(&self) -> Vec<&NodeId> {
        panic!()
    }
}

use std::collections::VecDeque;

pub struct Replicator<M, S, R>
    where M: Machine,
          R: io::Rpc<Message<M::Command>>
{
    id: NodeId,
    machine: M,
    state_version: Version,
    commit_version: Version,
    storage: S,
    rpc: R,
    consensus: Option<ConsensusModule>,
    timer: Timer,
    event_queue: VecDeque<Event>,
    action_queue: VecDeque<Action<M>>,
    io_wait: Option<Action<M>>,
    syncs: VecDeque<u64>,
}
impl<M, S, R> Replicator<M, S, R>
    where M: Machine,
          S: io::Storage<M>,
          R: io::Rpc<Message<M::Command>>
{
    pub fn new(id: &NodeId, storage: S, rpc: R, config: config::Builder) -> Result<Self, S::Error> {
        let config = config.build();
        if !config.is_member(id) {
            return Err(Error::NotMember);
        }

        let mut this = Self::default(id, storage, rpc);
        let ballot = Ballot::new(0);
        this.storage.log_truncate(0);
        this.storage.log_append(&[io::LogEntry::noop(ballot.term)]);

        let snapshot = this.machine.take_snapshot();
        let metadata = io::SnapshotMetadata::new(&this.commit_version, &config);
        this.storage.save_snapshot(metadata, snapshot);
        this.storage.save_ballot(&ballot);
        try!(this.storage.flush().map_err(Error::Storage));

        let log_index_table = this.storage.build_log_table();
        this.consensus = Some(ConsensusModule::new(id, &ballot, &config, log_index_table));
        Ok(this)
    }
    pub fn load(id: &NodeId, storage: S, rpc: R) -> Result<Option<Self>, S::Error> {
        let mut this = Self::default(id, storage, rpc);

        try!(this.storage.flush().map_err(Error::Storage));
        this.storage.load_ballot();
        let last_ballot =
            match try!(this.storage.run_once(false).unwrap().map_err(Error::Storage)) {
                io::StorageData::NotFound => return Ok(None),
                io::StorageData::Ballot(ballot) => ballot,
                _ => unreachable!(),
            };

        this.storage.load_snapshot();
        let (metadata, snapshot) =
            match try!(this.storage.run_once(false).unwrap().map_err(Error::Storage)) {
                io::StorageData::NotFound => return Ok(None),
                io::StorageData::Snapshot { metadata, snapshot } => (metadata, snapshot),
                _ => unreachable!(),
            };
        this.storage.log_drop_until(metadata.last.log_index + 1);
        try!(this.storage.flush().map_err(Error::Storage));

        let log_index_table = this.storage.build_log_table();
        this.consensus =
            Some(ConsensusModule::new(id, &last_ballot, &metadata.config, log_index_table));
        this.machine = snapshot.into();
        Ok(Some(this))
    }
    pub fn run_once(&mut self) -> Result<Option<Event>, S::Error> {
        self.check_timer();
        self.check_mailbox();
        try!(self.storage_run_once());
        if let Some(action) = self.action_queue.pop_front() {
            try!(self.handle_action(action));
        }
        Ok(self.event_queue.pop_front())
    }
    pub fn sync(&mut self) -> u64 {
        let seq = 0; // XXX:
        self.syncs.push_back(seq);
        self.action_queue.push_back(Action::Sync(seq));
        seq
    }
    pub fn execute(&mut self, command: M::Command) -> LogIndex {
        let reserved_index = 0; // TODO:
        self.action_queue
            .push_back(Action::ExecuteCommand(command));
        reserved_index
    }
    pub fn update_config(&mut self, config: config::Builder) -> LogIndex {
        let reserved_index = 0; // TODO:
        self.action_queue.push_back(Action::UpdateConfig(config.build()));
        reserved_index
    }
    pub fn take_snapshot(&mut self) {
        // TODO: duplication check
        // NOTE: high priority
        let metadata = io::SnapshotMetadata::new(self.state_version(),
                                                 self.consensus.as_ref().unwrap().config());
        let snapshot = self.machine.take_snapshot();
        self.action_queue
            .push_front(Action::InstallSnapshot(IsFsm::SnapshotSave(metadata, snapshot)));
    }
    pub fn queue_len(&self) -> usize {
        self.event_queue.len() + self.action_queue.len()
    }
    pub fn node_id(&self) -> &NodeId {
        &self.id
    }
    pub fn state(&self) -> &M {
        &self.machine
    }
    pub fn state_version(&self) -> &Version {
        &self.state_version
    }

    fn default(id: &NodeId, storage: S, rpc: R) -> Self {
        Replicator {
            id: id.clone(),
            machine: M::default(),
            state_version: Version::new(0, 0),
            commit_version: Version::new(0, 0),
            storage: storage,
            rpc: rpc,
            consensus: None,
            timer: Timer::new(),
            event_queue: VecDeque::new(),
            action_queue: VecDeque::new(),
            io_wait: None,
            syncs: VecDeque::new(),
        }
    }
    fn check_timer(&mut self) {
        if self.timer.is_elapsed() {
            self.timer.clear();
            // NOTE: timeouts are interaption event (i.e., higher priority)
            self.action_queue.push_front(Action::Timeout);
        }
    }
    fn check_mailbox(&mut self) {
        if let Some(recv) = self.rpc.try_recv() {
            self.action_queue.push_back(Action::Recv(recv));
        }
    }
    fn storage_run_once(&mut self) -> Result<(), S::Error> {
        if self.io_wait.is_none() {
            return Ok(());
        }
        if let Some(result) = self.storage.run_once(true) {
            use super::io::StorageData;
            let data = try!(result.map_err(Error::Storage));
            match data {
                StorageData::LogEntries(entries) => {
                    let action = self.io_wait.take().unwrap();
                    match action {
                        Action::LogApply(_) => {
                            self.action_queue.push_front(Action::LogApply(entries));
                        }
                        Action::AppendEntriesCall(dest, header, leader_commit) => {
                            // TODO:
                            panic!()
                        }
                        _ => unreachable!(),
                    }
                }
                StorageData::Ballot(_ballot) => unreachable!(),
                StorageData::Snapshot { metadata, snapshot } => panic!(),
                StorageData::NotFound => panic!(),
                StorageData::Done => {
                    let action = self.io_wait.take().unwrap();
                    self.action_queue.push_front(action);
                }
            }
        }
        Ok(())
    }
    fn consensus_mut(&mut self) -> &mut ConsensusModule {
        self.consensus.as_mut().unwrap()
    }
    fn consensus(&self) -> &ConsensusModule {
        self.consensus.as_ref().unwrap()
    }
    fn handle_action(&mut self, action: Action<M>) -> Result<(), S::Error> {
        if self.io_wait.is_some() {
            return Ok(());
        }

        let actions = match action {
            Action::LogApply(entries) => {
                for e in entries {
                    let index = self.state_version.log_index + 1;
                    self.state_version = Version::new(index, e.term);
                    match e.data {
                        io::LogData::Command(c) => {
                            self.machine.execute(c);
                        }
                        _ => {}
                    }
                }
                Vec::new()
            }
            Action::AppendEntriesCall(_, _, _) => unreachable!(),
            Action::Timeout => try!(self.consensus_mut().handle_timeout()),
            Action::Recv(message) => try!(self.consensus_mut().handle_message(message)),
            Action::InstallSnapshot(IsFsm::SnapshotSave(metadata, snapshot)) => {
                let next_action = Action::InstallSnapshot(IsFsm::LogDrop(metadata.last.log_index +
                                                                         1));
                self.storage.save_snapshot(metadata, snapshot);
                self.io_wait = Some(next_action);
                Vec::new()
            }
            Action::InstallSnapshot(IsFsm::LogDrop(index)) => {
                let next_action = Action::InstallSnapshot(IsFsm::UpdateConsensus(index));
                self.storage.log_drop_until(index);
                self.io_wait = Some(next_action);
                Vec::new()
            }
            Action::InstallSnapshot(IsFsm::UpdateConsensus(index)) => {
                // TODO: LogDropと順序が逆な気がしてきた
                // あとこの関数は非同期にする必要はなさそう
                try!(self.consensus_mut().handle_snapshot_installed(index))
            }
            Action::Sync(seq) => {
                // skip if already syned
                try!(self.consensus_mut().handle_sync())
            }
            Action::UpdateConfig(config) => try!(self.consensus_mut().handle_update_config(config)),
            Action::ExecuteCommand(command) => try!(self.consensus_mut().handle_command(command)),
            Action::Raft(raft) => try!(self.handle_raft_action(raft)),
        };
        self.action_queue.extend(actions);
        Ok(())
    }
    fn handle_commited(&mut self, version: Version) -> Result<Vec<Action<M>>, S::Error> {
        if self.commit_version.log_index >= version.log_index {
            return Err(Error::CommitConfliction);
        }

        // TODO: state_version => applied_version
        let last_applied = self.state_version.log_index;
        self.storage.log_get(last_applied + 1,
                             (version.log_index - last_applied) as usize);
        self.commit_version = version;
        self.io_wait = Some(Action::LogApply(Vec::new()));
        Ok(Vec::new())
    }
    fn handle_raft_action(&mut self,
                          action: RaftAction<M::Command, M::Snapshot>)
                          -> Result<Vec<Action<M>>, S::Error> {
        Ok(match action {
            RaftAction::LogAppend(entries, next) => {
                self.storage.log_append(&entries);
                self.io_wait = Some(Action::Raft(*next));
                Vec::new()
            }
            RaftAction::LogRollback(first_index, next) => {
                self.storage.log_truncate(first_index);
                self.io_wait = Some(Action::Raft(*next));
                Vec::new()
            }
            RaftAction::LogCommited(version) => try!(self.handle_commited(version)),
            RaftAction::SaveBallot(ballot, next) => {
                self.storage.save_ballot(&ballot);
                self.io_wait = Some(Action::Raft(*next));
                Vec::new()
            }
            RaftAction::InstallSnapshot { metadata, snapshot } => {
                let action = Action::InstallSnapshot(IsFsm::SnapshotSave(metadata, snapshot));
                vec![action]
            }
            RaftAction::BroadcastMsg(message) => {
                // TODO: optimize
                let ns: Vec<_> = self.consensus().members().into_iter().cloned().collect();
                for n in ns {
                    self.rpc.send(&n, &message);
                }
                Vec::new()
            }
            RaftAction::UnicastMsg(dest, message) => {
                self.rpc.send(&dest, &message);
                Vec::new()
            }
            RaftAction::UnicastLog(dest, header, commit, start, length) => {
                // TODO: if dropped , then send snapshot
                self.storage.log_get(start, length);
                self.io_wait = Some(Action::AppendEntriesCall(dest, header, commit));
                Vec::new()
            }
            RaftAction::Postpone(message) => {
                self.action_queue.push_front(Action::Recv(message));
                Vec::new()
            }
            RaftAction::ResetTimeout(kind) => {
                // TODO:
                self.timer.clear();
                self.timer.set(::std::time::Duration::from_millis(100));
                Vec::new()
            }
            RaftAction::Panic(error) => return Err(Error::Consensus(error)),
        })
    }
}

pub enum IsFsm<M, S> {
    SnapshotSave(M, S),
    LogDrop(LogIndex),
    UpdateConsensus(LogIndex),
}

pub enum Action<M>
    where M: Machine
{
    Timeout,
    Recv(Message<M::Command>),
    InstallSnapshot(IsFsm<io::SnapshotMetadata, M::Snapshot>),
    Sync(u64),
    UpdateConfig(config::Config),
    ExecuteCommand(M::Command),
    Raft(RaftAction<M::Command, M::Snapshot>),
    LogApply(Vec<io::LogEntry<M::Command>>),
    AppendEntriesCall(NodeId, MessageHeader, LogIndex),
}

pub enum RaftAction<T, S> {
    LogAppend(Vec<io::LogEntry<T>>, Box<RaftAction<T, S>>),
    LogRollback(LogIndex, Box<RaftAction<T, S>>),
    LogCommited(Version),
    SaveBallot(Ballot, Box<RaftAction<T, S>>),
    InstallSnapshot {
        metadata: io::SnapshotMetadata,
        snapshot: S,
    },
    BroadcastMsg(Message<T>),
    UnicastMsg(NodeId, Message<T>),

    // TODO: 範囲外なら勝手にスナップショットモードに切り替わる
    UnicastLog(NodeId, MessageHeader, LogIndex, LogIndex, usize),
    Postpone(Message<T>),
    ResetTimeout(TimeoutKind),
    Panic(raft::Error),
}
pub enum TimeoutKind {
    Min,
    Max,
    Random,
}

pub enum ErrorEvent {
    NotLeader(Option<NodeId>),
}

pub enum Event {
    Commited(LogIndex),
    Aborted(LogIndex, ErrorEvent),
    Synced(u64),
    CannotSynced(u64, ErrorEvent),
    SnapshotOk,
    SnapshotFailed(ErrorEvent),
    ConfigChanged {
        stable: bool,
        config: config::Config,
    },
}

pub struct Timer;
impl Timer {
    pub fn new() -> Self {
        Timer
    }
    pub fn is_elapsed(&self) -> bool {
        panic!()
    }
    pub fn clear(&mut self) {
        panic!()
    }
    pub fn set(&mut self, duration: ::std::time::Duration) {
        panic!()
    }
}
