use std::ops::Range;
use std::collections::HashSet;

pub mod role;
use self::role::ConsensusRole;
pub use self::role::LogIndexTable;

pub type NodeId = String;
pub type Term = u64;
pub type LogIndex = u64;

pub enum Error {
    NotLeader(Option<NodeId>),
    Panic,
}
impl<'a> From<Option<&'a NodeId>> for Error {
    fn from(x: Option<&'a NodeId>) -> Self {
        Error::NotLeader(x.cloned())
    }
}

pub enum ConsensusModule {
    Follower(role::FollowerRole),
    Candidate(role::CandidateRole),
    Leader(role::LeaderRole),
}
impl ConsensusModule {
    pub fn new<T>(id: NodeId,
                  term: Term,
                  voted_for: Option<NodeId>,
                  config: ConfigState,
                  log_index_table: role::LogIndexTable)
                  -> (Self, Actions<T>) {
        let common = role::Common {
            id: id,
            term: term,
            voted_for: voted_for,
            config: config,
            log_index_table: log_index_table,
        };
        let mut follower = role::FollowerRole::new(common, 0);
        let actions = follower.init();
        (ConsensusModule::Follower(follower), actions)
    }
    pub fn handle_command<T>(&mut self, command: T) -> Result<(LogIndex, Actions<T>), Error> {
        Ok(match *self {
            ConsensusModule::Follower(ref mut x) => try!(x.handle_command(command)),
            ConsensusModule::Candidate(ref mut x) => try!(x.handle_command(command)),
            ConsensusModule::Leader(ref mut x) => try!(x.handle_command(command)),
        })
    }
    pub fn handle_change_config<T>(&mut self, new_config: Config) -> Result<Actions<T>, Error> {
        Ok(match *self {
            ConsensusModule::Follower(ref mut x) => try!(x.handle_change_config(new_config)),
            ConsensusModule::Candidate(ref mut x) => try!(x.handle_change_config(new_config)),
            ConsensusModule::Leader(ref mut x) => try!(x.handle_change_config(new_config)),
        })
    }
    pub fn handle_message<T>(&mut self, sender: Sender, message: Message<T>) -> Actions<T> {
        match *self {
            ConsensusModule::Follower(ref mut x) => x.handle_message(sender, message),
            ConsensusModule::Candidate(ref mut x) => x.handle_message(sender, message),
            ConsensusModule::Leader(ref mut x) => x.handle_message(sender, message),
        }
    }
    pub fn handle_event<T>(&mut self, event: Event) -> Actions<T> {
        match *self {
            ConsensusModule::Follower(ref mut x) => x.handle_event(event),
            ConsensusModule::Candidate(ref mut x) => x.handle_event(event),
            ConsensusModule::Leader(ref mut x) => x.handle_event(event),
        }
    }
    pub fn transit<T>(self, role: role::RoleKind) -> (Self, Actions<T>) {
        match self {
            ConsensusModule::Follower(_x) => {
                match role {
                    role::RoleKind::Follower => {
                        // let t = Follower::from(x);
                        // let actions = t.init();
                        // (ConsensusModule::Follower(t), actions)
                        panic!()
                    }
                    role::RoleKind::Candidate => panic!(),
                    _ => unreachable!(),
                }
            }
            ConsensusModule::Candidate(_x) => {
                match role {
                    role::RoleKind::Follower => panic!(),
                    role::RoleKind::Candidate => panic!(),
                    role::RoleKind::Leader => panic!(),
                }
            }
            ConsensusModule::Leader(_x) => {
                match role {
                    role::RoleKind::Follower => panic!(),
                    _ => unreachable!(),
                }
            }
        }
    }
}

pub enum Event {
    SnapshotInstalled(LogIndex, Term),
    Timeout,
}

pub enum Action<T> {
    LogAppend(Vec<LogEntry<T>>),
    LogTruncate(LogIndex), // i.e., Abort
    LogCommit(LogIndex),
    SaveState(Term, Option<NodeId>),
    InstallSnapshot(u64, Vec<u8>, bool, Term, LogIndex, ConfigState),
    BroadcastMsg(Sender, Message<T>),
    //    BroadcastLog(Sender, LogIndex, Range<LogIndex>),
    UnicastMsg(Sender, NodeId, Message<T>),
    UnicastLog(Sender, LogIndex, NodeId, Range<LogIndex>),
    SendSnapshot(Sender, NodeId),
    Postpone(Sender, Message<T>),
    ResetTimeout,
    Transit(role::RoleKind),
    Panic,
}
impl<T> Action<T> {
    pub fn to_leader() -> Self {
        Action::Transit(role::RoleKind::Leader)
    }
    pub fn to_follower() -> Self {
        Action::Transit(role::RoleKind::Follower)
    }
    pub fn to_candidate() -> Self {
        Action::Transit(role::RoleKind::Candidate)
    }
}

pub type Actions<T> = Vec<Action<T>>;

pub struct Sender {
    pub id: NodeId,
    pub term: Term,
    pub log_index: LogIndex,
    pub log_term: Term,
}

pub enum Message<T> {
    RequestVoteCall,
    RequestVoteReply {
        vote_granted: bool,
    },
    AppendEntriesCall {
        entries: Vec<LogEntry<T>>,
        leader_commit: LogIndex,
    },
    AppendEntriesReply,
    // AppendEntriesReply {
    //     success: bool,
    // },
    InstallSnapshotCall {
        config: ConfigState,
        data: Vec<u8>,
        offset: u64,
        done: bool,
    },
    InstallSnapshotReply,
}

pub struct LogEntry<T> {
    pub term: Term,
    pub data: LogData<T>,
}
impl<T> LogEntry<T> {
    pub fn new(term: Term, data: LogData<T>) -> Self {
        LogEntry {
            term: term,
            data: data,
        }
    }
    pub fn noop(term: Term) -> Self {
        Self::new(term, LogData::Noop)
    }
    pub fn config(term: Term, config: &ConfigState) -> Self {
        Self::new(term, LogData::Config(config.clone()))
    }
    pub fn command(term: Term, command: T) -> Self {
        Self::new(term, LogData::Command(command))
    }
}

pub enum LogData<T> {
    Config(ConfigState),

    // TODO: add Elected(Config),
    Noop,

    Command(T),
}

#[derive(Clone)]
pub struct Config {
    pub members: HashSet<NodeId>,
}
impl Config {
    pub fn is_member(&self, id: &NodeId) -> bool {
        self.members.contains(id)
    }
}

#[derive(Clone)]
pub enum ConfigState {
    Stable(Config),
    Prepare {
        prepare: Config,
        current: Config,
    },
    NewOld {
        new: Config,
        old: Config,
    },
}
impl ConfigState {
    pub fn next(&self) -> Option<Self> {
        match *self {
            ConfigState::Stable(_) => None,
            ConfigState::Prepare { ref current, ref prepare } => {
                Some(ConfigState::NewOld {
                    new: prepare.clone(),
                    old: current.clone(),
                })
            }
            ConfigState::NewOld { ref new, .. } => Some(ConfigState::Stable(new.clone())),
        }
    }
    pub fn update(&self, new: Config) -> Self {
        match *self {
            ConfigState::Stable(ref c) => {
                ConfigState::Prepare {
                    current: c.clone(),
                    prepare: new,
                }
            }
            ConfigState::Prepare { ref current, .. } => {
                ConfigState::Prepare {
                    current: current.clone(),
                    prepare: new,
                }
            }
            ConfigState::NewOld { ref old, .. } => {
                ConfigState::Prepare {
                    current: old.clone(),
                    prepare: new,
                }
            }
        }
    }
    pub fn all_members(&self) -> HashSet<&NodeId> {
        match *self {
            ConfigState::Stable(ref c) => c.members.iter().collect(),
            ConfigState::Prepare { ref prepare, ref current } => {
                prepare.members.union(&current.members).collect()
            }
            ConfigState::NewOld { ref new, ref old } => new.members.union(&old.members).collect(),
        }
    }
    pub fn is_member(&self, id: &NodeId) -> bool {
        match *self {
            ConfigState::Stable(ref c) => c.is_member(id),
            ConfigState::Prepare { ref prepare, ref current } => {
                prepare.is_member(id) || current.is_member(id)
            }
            ConfigState::NewOld { ref new, ref old } => new.is_member(id) || old.is_member(id),
        }
    }
    pub fn votable_configs(&self) -> Vec<&Config> {
        match *self {
            ConfigState::Stable(ref c) => vec![c],
            ConfigState::Prepare { ref current, .. } => vec![current],
            ConfigState::NewOld { ref new, ref old } => vec![new, old],
        }
    }
}
