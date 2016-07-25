use std::ops::Range;

pub type NodeId = String;
pub type Term = u64;
pub type LogIndex = u64;

pub trait ConsensusRole<T> {
    fn init(&mut self) -> Actions<T>;
    fn handle_command(&mut self, command: T) -> Result<Actions<T>, Option<&NodeId>>;
    fn handle_message(&mut self, sender: Sender, message: Message<T>) -> Actions<T>;
    fn handle_timeout(&mut self) -> Actions<T>;
    fn handle_snapshot_installed(&mut self, index: LogIndex) -> Actions<T>;
}

pub enum Action<T> {
    LogAppend(Vec<LogEntry<T>>),
    LogTruncate(LogIndex), // i.e., Abort
    LogCommit(LogIndex),
    InstallSnapshot(Vec<u8>, Term, LogIndex, Config),
    BroadcastMsg(Sender, Message<T>),
    BroadcastLog(Sender, LogIndex, Range<LogIndex>),
    UnicastMsg(Sender, NodeId, Message<T>),
    UnicastLog(Sender, LogIndex, NodeId, Range<LogIndex>),
    Postpone(Sender, Message<T>),
    ResetTimeout,
    Transit(Role),
}
pub type Actions<T> = Vec<Action<T>>;

pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct Sender;

pub enum Message<T> {
    Dummy(T),
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
    pub fn config(term: Term, config: &Config) -> Self {
        Self::new(term, LogData::Config(config.clone()))
    }
    pub fn command(term: Term, command: T) -> Self {
        Self::new(term, LogData::Command(command))
    }
}

pub enum LogData<T> {
    Config(Config),
    Noop,
    Command(T),
}

#[derive(Clone)]
pub struct Config {
    
}
