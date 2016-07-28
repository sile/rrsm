#![allow(dead_code,unused_variables)]
use std::time::Duration;
use std::ops::Range;
use std::result::Result as StdResult;
use std::collections::VecDeque;
use Machine;
use Config;
use LogIndex;
use NodeId;
use Term;
use CommitVersion;
use raft_protocol as raft;
use raft_protocol::LogEntry;
use raft_protocol::Message;

pub struct PersistentNodeState {
    pub node_id: NodeId,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub config: Config,
}

pub trait Storage<M>
    where M: Machine
{
    type Error;
    type Iter: Iterator<Item = StdResult<LogEntry<M::Command>, Self::Error>>;

    fn log_append(&mut self, entries: Vec<LogEntry<M::Command>>) -> StdResult<(), Self::Error>;
    fn log_add(&mut self, entry: LogEntry<M::Command>) -> StdResult<(), Self::Error> {
        self.log_append(vec![entry])
    }
    fn log_truncate(&mut self, range: Range<LogIndex>) -> StdResult<Range<LogIndex>, Self::Error>;
    fn log_iter(&mut self, range: Range<LogIndex>) -> StdResult<Self::Iter, Self::Error>;
    fn log_index_table(&self) -> raft::LogIndexTable;

    fn state_save(&mut self, state: &PersistentNodeState) -> StdResult<(), Self::Error>;
    fn state_load(&mut self) -> StdResult<Option<PersistentNodeState>, Self::Error>;

    fn snapshot_save(&mut self, snapshot: M::Snapshot) -> StdResult<(), Self::Error>;
    fn snapshot_load(&mut self) -> StdResult<Option<M::Snapshot>, Self::Error>;
}

pub type RecvTodo<F, T> = (F, raft::Sender, Message<T>);
pub trait Rpc<T> {
    type Error;
    type From;
    type Reply;
    fn multicall(&mut self,
                 members: &::ClusterMembers,
                 message: Message<T>)
                 -> StdResult<Self::Reply, Self::Error>;
    fn call(&mut self,
            destination: &NodeId,
            message: Message<T>)
            -> StdResult<Self::Reply, Self::Error>;
    fn reply(&mut self, from: &Self::From, message: Message<T>) -> StdResult<(), Self::Error>;
    fn try_recv(&mut self) -> StdResult<Option<RecvTodo<Self::From, T>>, Self::Error>;
}

pub enum Error<S, R> {
    Storage(S),
    Rpc(R),
    Consensus(raft::Error),
}
impl<S, R> From<raft::Error> for Error<S, R> {
    fn from(x: raft::Error) -> Self {
        Error::Consensus(x)
    }
}

pub type Result<T, S, R> = StdResult<T, Error<S, R>>;

macro_rules! storage_try {
    ($e:expr) => { try!($e.map_err(Error::Storage)) }
}
macro_rules! rpc_try {
    ($e:expr) => { try!($e.map_err(Error::Rpc)) }
}
impl From<::std::collections::HashSet<NodeId>> for raft::Config {
    fn from(x: ::std::collections::HashSet<NodeId>) -> Self {
        raft::Config { members: x }
    }
}
impl From<Config> for raft::Config {
    fn from(x: Config) -> Self {
        // XXX
        From::from(x.cluster.members)
    }
}
impl From<Config> for raft::ConfigState {
    fn from(x: Config) -> Self {
        let mut c = x.cluster;
        match c.temporary_members.take() {
            None => raft::ConfigState::Stable(From::from(c.members)),
            Some(::Temporary::New(m)) => {
                raft::ConfigState::Prepare {
                    prepare: From::from(m),
                    current: From::from(c.members),
                }
            }
            Some(::Temporary::Old(m)) => {
                raft::ConfigState::NewOld {
                    new: From::from(c.members),
                    old: From::from(m),
                }
            }
        }
    }
}

// TODO: ReplicatedLog
pub struct Replicator<M, S, R>
    where M: Machine
{
    machine: M,
    storage: S,
    rpc: R,
    consensus: Option<raft::ConsensusModule>,
    action_queue: VecDeque<raft::Action<M::Command>>,
    next_token: Token,
    request_queue: VecDeque<Request<M::Command>>,
    commit_version: CommitVersion,
    timer: Timer,
}
impl<M, S, R> Replicator<M, S, R>
    where M: Machine,
          S: Storage<M>,
          R: Rpc<M::Command>
{
    // create_macine
    pub fn new(node_id: NodeId,
               mut storage: S,
               rpc: R,
               config: Config)
               -> Result<Self, S::Error, R::Error> {
        storage_try!(storage.log_truncate(0..0));
        storage_try!(storage.state_save(&PersistentNodeState {
            node_id: node_id.clone(),
            current_term: 0,
            voted_for: None,
            config: config.clone(),
        }));
        storage_try!(storage.log_add(LogEntry::noop(0)));
        let (consensus, actions) = raft::ConsensusModule::new(node_id.clone(),
                                                              0,
                                                              None,
                                                              From::from(config),
                                                              storage.log_index_table());
        Ok(Replicator {
            machine: M::default(),
            storage: storage,
            rpc: rpc,
            consensus: Some(consensus),
            action_queue: actions.into_iter().collect(),
            next_token: Token::new(),
            request_queue: VecDeque::new(),
            commit_version: CommitVersion {
                term: 0,
                index: 0,
            },
            timer: Timer::new(),
        })
    }
    pub fn load(mut storage: S, rpc: R) -> Result<Self, S::Error, R::Error> {
        let pns = storage_try!(storage.state_load()).unwrap(); // XXX:
        let snapshot = storage_try!(storage.snapshot_load()).unwrap(); // XXX
        let log_index_table = storage.log_index_table();
        let commit_version = CommitVersion {
            term: log_index_table.snapshot_term(),
            index: log_index_table.snapshot_log_index(),
        };

        let (consensus, actions) = raft::ConsensusModule::new(pns.node_id.clone(),
                                                              pns.current_term,
                                                              pns.voted_for.clone(),
                                                              From::from(pns.config.clone()),
                                                              log_index_table);
        Ok(Replicator {
            machine: snapshot.into(),
            storage: storage,
            rpc: rpc,
            consensus: Some(consensus),
            action_queue: actions.into_iter().collect(),
            next_token: Token::new(),
            request_queue: VecDeque::new(),
            commit_version: commit_version,
            timer: Timer::new(),
        })
    }
    pub fn run_once(&mut self) -> Result<Option<Vec<Event>>, S::Error, R::Error> {
        if let Some(r) = self.request_queue.pop_front() {
            try!(self.handle_request(r));
        }
        try!(self.check_timer());
        try!(self.check_mailbox());
        if let Some(a) = self.action_queue.pop_front() {
            self.handle_action(a).map(Some)
        } else {
            Ok(None)
        }
    }
    pub fn updage_config(&mut self, config: Config) -> Token {
        let token = self.next_token.allocate();
        self.request_queue.push_back(Request::UpdateConfig(token, config));
        token
    }
    pub fn sync(&mut self) -> Token {
        let token = self.next_token.allocate();
        self.request_queue.push_back(Request::Sync(token));
        token
    }
    // TODO: commit
    pub fn execute(&mut self, command: M::Command, expected: Option<CommitVersion>) -> Token {
        let token = self.next_token.allocate();
        self.request_queue.push_back(Request::Execute(token, command, expected));
        token
    }
    pub fn query(&self) -> &M {
        &self.machine
    }
    pub fn commit_version(&self) -> &CommitVersion {
        &self.commit_version
    }
    pub fn leader(&self) -> Option<&NodeId> {
        // self.consensus.leader()
        panic!()
    }
    pub fn config(&self) -> &Config {
        // self.consensus.config()
        panic!()
    }

    fn handle_request(&mut self, r: Request<M::Command>) -> Result<(), S::Error, R::Error> {
        match r {
            Request::Sync(token) => {
                // TODO: support token and implements
                panic!()
            }
            Request::UpdateConfig(token, config) => {
                // TODO: save config
                // TODO: support token
                let actions =
                    try!(self.consensus.as_mut().unwrap().handle_change_config(From::from(config)));
                self.action_queue.extend(actions);
            }
            Request::Execute(token, command, expected) => {
                // TODO: support token and command
                let (_, actions) = try!(self.consensus.as_mut().unwrap().handle_command(command));
                self.action_queue.extend(actions);
            }
        }
        Ok(())
    }
    fn handle_action(&mut self,
                     a: raft::Action<M::Command>)
                     -> Result<Vec<Event>, S::Error, R::Error> {
        use raft_protocol::Action;
        let events = Vec::new();
        match a {
            Action::LogAppend(entries) => {
                storage_try!(self.storage.log_append(entries));
            }
            Action::LogTruncate(new_last_index) => {
                storage_try!(self.storage.log_truncate(0..new_last_index));
            }
            Action::LogCommit(index) => {
                let mut last_term = self.commit_version.term;

                for e in storage_try!(self.storage.log_iter(self.commit_version.index..index)) {
                    let e = storage_try!(e);
                    match e.data {
                        raft::LogData::Command(command) => {
                            last_term = e.term;
                            self.machine.execute(command);
                        }
                        _ => {}
                    }

                    // TODO: handle token and events
                }

                self.commit_version = CommitVersion {
                    term: last_term,
                    index: index,
                };
            }
            Action::SaveState(_, _) => panic!(),
            Action::InstallSnapshot(offset, data, done, term, index, config) => {
                // TODO: etc
                // let snapshot = data;
                // let machine = snapshot.into();
                // let actions = self.cs_mut()
                //     .handle_event(raft::Event::SnapshotInstalled(index, term));
                panic!()
            }
            Action::SendSnapshot(sender, dest) => {
                let _snapshot = storage_try!(self.storage.snapshot_load());
                let message = raft::Message::InstallSnapshotCall {
                    offset: 0,
                    data: Vec::new(), // TODO
                    done: true,
                    config: From::from(self.config().clone()),
                };
                rpc_try!(self.rpc.call(&dest, message));
            }
            Action::BroadcastMsg(sender, message) => {
                // TODO: handle sender
                let members = self.config().cluster.all_members();

                // TODO: handle result
                rpc_try!(self.rpc.multicall(&members, message));
            }
            Action::UnicastMsg(sender, dest, message) => {
                // TODO: same as above
                rpc_try!(self.rpc.call(&dest, message));
            }
            Action::UnicastLog(sender, leader_commit, dest, range) => {
                // TODO: same as above
                let mut entries = Vec::new();

                // TODO: check out-of-range and overlimit
                for e in storage_try!(self.storage.log_iter(range)) {
                    let e = storage_try!(e);
                    entries.push(e);
                }
                let message = raft::Message::AppendEntriesCall {
                    entries: entries,
                    leader_commit: leader_commit,
                };
                rpc_try!(self.rpc.call(&dest, message));
            }
            Action::ResetTimeout => {
                // TODO: randomize
                let after = self.config().election_timeout.start.clone();
                self.timer.set(after);
            }
            Action::Transit(role) => {
                let (consensus, actions) = self.consensus.take().unwrap().transit(role);
                self.consensus = Some(consensus);
                self.action_queue.extend(actions);
            }
            Action::Postpone(sender, message) => {
                let actions = self.cs_mut().handle_message(sender, message);
                self.action_queue.extend(actions);
            }
            Action::Panic => try!(Err(raft::Error::Panic)),
        }
        Ok(events)
    }
    fn check_timer(&mut self) -> Result<(), S::Error, R::Error> {
        if self.timer.is_elapsed() {
            let actions = self.cs_mut().handle_event(raft::Event::Timeout);
            self.action_queue.extend(actions);
            self.timer.clear();
        }
        Ok(())
    }
    fn check_mailbox(&mut self) -> Result<(), S::Error, R::Error> {
        if let Some((from, sender, message)) = rpc_try!(self.rpc.try_recv()) {
            // TODO: handle from
            self.action_queue.push_back(raft::Action::Postpone(sender, message));
        }
        Ok(())
    }
    fn cs_mut(&mut self) -> &mut raft::ConsensusModule {
        self.consensus.as_mut().unwrap()
    }
}

use std::time;
pub struct Timer {
    expiry_time: Option<time::SystemTime>,
}

impl Timer {
    pub fn new() -> Self {
        Timer { expiry_time: None }
    }
    pub fn is_elapsed(&self) -> bool {
        self.expiry_time.map_or(false, |t| t >= time::SystemTime::now())
    }
    pub fn clear(&mut self) {
        self.expiry_time = None;
    }
    pub fn set(&mut self, after: Duration) {
        self.expiry_time = Some(time::SystemTime::now() + after);
    }
}

enum Request<T> {
    Sync(Token),
    UpdateConfig(Token, Config),
    Execute(Token, T, Option<CommitVersion>),
}

#[derive(Clone,Copy)]
pub struct Token {
    value: u64,
}
impl Token {
    pub fn new() -> Self {
        Token { value: 0 }
    }
    pub fn allocate(&mut self) -> Self {
        let allocated = Token { value: self.value };
        self.value += 1;
        allocated
    }
}

pub enum Event {
    Done {
        token: Token,
        success: bool,
    },
}
