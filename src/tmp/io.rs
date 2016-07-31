use super::*;

// TODO: Transport(?)
pub trait Rpc<T> {
    fn send(&mut self, destination: &NodeId, message: &T);
    fn try_recv(&mut self) -> Option<T>;
}

pub struct LogEntry<T> {
    pub term: Term,
    pub data: LogData<T>,
}
impl<T> LogEntry<T> {
    pub fn noop(term: Term) -> Self {
        LogEntry {
            term: term,
            data: LogData::Noop,
        }
    }
    pub fn command(term: Term, command: T) -> Self {
        LogEntry {
            term: term,
            data: LogData::Command(command),
        }
    }
    pub fn config(term: Term, config: Config) -> Self {
        LogEntry {
            term: term,
            data: LogData::Config(config),
        }
    }
}

pub enum LogData<T> {
    Noop,
    Config(Config),
    Command(T),
}

pub struct LogIndexTable;

pub struct SnapshotMetadata {
    pub last: Version,
    pub config: Config,
}
impl SnapshotMetadata {
    pub fn new(version: &Version, config: &Config) -> Self {
        SnapshotMetadata {
            last: version.clone(),
            config: config.clone(),
        }
    }
}

pub trait Storage<M>
    where M: Machine
{
    type Error;
    fn log_append(&mut self, &[LogEntry<M::Command>], token: Token) -> LogIndex;
    fn log_truncate(&mut self, end_index: LogIndex, token: Token);
    fn log_drop_until(&mut self, first_index: LogIndex, token: Token);
    fn log_get(&mut self, offset: LogIndex, max_length: usize, token: Token);
    fn build_log_table(&self) -> LogIndexTable;

    fn save_ballot(&mut self, ballot: &Ballot, token: Token);
    fn load_ballot(&mut self, token: Token);
    fn save_snapshot(&mut self, metadata: SnapshotMetadata, snapshot: M::Snapshot, token: Token);
    fn load_snapshot(&mut self, token: Token);

    fn run_once(&mut self, async: bool) -> Option<(Token, Result<StorageData<M>, Self::Error>)>;

    fn queue_len(&self) -> usize;
    fn flush(&mut self) -> Result<(), Self::Error>;
}

pub enum StorageData<M>
    where M: Machine
{
    LogEntries(LogEntry<M::Command>),
    Ballot(Ballot),
    Snapshot {
        metadata: SnapshotMetadata,
        snapshot: M::Snapshot,
    },
    NotFound,
    Done,
}
