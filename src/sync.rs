use std;
use std::collections::VecDeque;
use std::ops::Range;
use StateMachine;
use raft_protocol;
use raft_protocol::LogEntry;
use LogIndex;
use Term;
use NodeId;

pub trait Storage<T> {
    type Error;
    fn log_append(&mut self, entries: Vec<LogEntry<T>>) -> Result<(), Self::Error>;
    fn log_truncate(&mut self, range: Range<LogIndex>) -> Result<(), Self::Error>;
    fn data_write(&mut self, key: &str, offset: u64, data: &[u8]) -> Result<(), Self::Error>;
    fn data_read(&mut self,
                 key: &str,
                 offset: u64,
                 data: &mut [u8])
                 -> Result<Option<usize>, Self::Error>;
    fn data_delete(&mut self, key: &str) -> Result<(), Self::Error>;
    fn data_put(&mut self, key: &str, data: &[u8]) -> Result<(), Self::Error> {
        try!(self.data_delete(key));
        self.data_write(key, 0, data)
    }
    fn data_get(&mut self, key: &str) -> Result<Option<Vec<u8>>, Self::Error> {
        let mut buf = vec![0; 1024];
        let mut offset = 0 as usize;
        while let Some(read_size) = try!(self.data_read(key, offset as u64, &mut buf[offset..])) {
            offset += read_size;
            if offset < buf.len() {
                buf.truncate(offset);
                return Ok(Some(buf));
            }
            buf.resize(offset * 2, 0);
        }
        Ok(None)
    }
}

pub trait Rpc<T> {
    type Error;
    type From;
    fn call(&mut self,
            destination: &NodeId,
            message: T,
            reply_channel: std::sync::mpsc::Sender<T>)
            -> Result<(), Self::Error>;
    fn reply(&mut self, from: &Self::From, message: T) -> Result<(), Self::Error>;
}

pub enum Error2 {
    Storage,
    Rpc,
    Consensus,
}

pub trait LogStorage<T> {
    type Error;
    fn append(&mut self, entries: Vec<LogEntry<T>>) -> Result<(), Self::Error>;
    fn truncate(&mut self, last_index: LogIndex) -> Result<(), Self::Error>;
    fn drop(&mut self, first_index: LogIndex) -> Result<(), Self::Error>;
}
pub trait StateStorage {}
pub trait Transport {}

pub trait IoSystem<T> {
    type LogStorage: LogStorage<T>;
    type StateStorage: StateStorage;
    type Transport: Transport;

    fn get_log_storage_mut(&mut self) -> &mut Self::LogStorage;
    fn get_state_storage_mut(&mut self) -> &mut Self::StateStorage;
    fn get_transport_mut(&mut self) -> &mut Self::Transport;
}

pub enum Error<IO, T>
    where IO: IoSystem<T>
{
    LogStorage(<IO::LogStorage as LogStorage<T>>::Error),
    Protocol(raft_protocol::Error),
}
impl<IO, T> Error<IO, T>
    where IO: IoSystem<T>
{
    pub fn from_log_storage_err(e: <IO::LogStorage as LogStorage<T>>::Error) -> Self {
        Error::LogStorage(e)
    }
}

pub struct SyncRsm<S, IO>
    where S: StateMachine
{
    state_machine: S,
    consensus: Option<raft_protocol::ConsensusModule>,
    action_queue: VecDeque<raft_protocol::Action<S::Command>>,
    io_system: IO,
}
impl<S, IO> SyncRsm<S, IO>
    where S: StateMachine,
          IO: IoSystem<S::Command>
{
    pub fn new(state_machine: S, io_system: IO) -> Self {
        let conf = unsafe { std::mem::zeroed() };
        let table = unsafe { std::mem::zeroed() };
        let (consensus, actions) =
            raft_protocol::ConsensusModule::new("todo".to_string(), 0, None, conf, table);
        SyncRsm {
            state_machine: state_machine,
            consensus: Some(consensus),
            action_queue: actions.into_iter().collect(),
            io_system: io_system,
        }
    }
    pub fn run_once(&mut self) -> Result<bool, Error<IO, S::Command>> {
        if let Some(action) = self.action_queue.pop_front() {
            use raft_protocol::Action;
            match action {
                Action::LogAppend(entries) => {
                    try!(self.io_system
                        .get_log_storage_mut()
                        .append(entries)
                        .map_err(Error::from_log_storage_err))
                }
                Action::LogTruncate(index) => {
                    try!(self.io_system
                        .get_log_storage_mut()
                        .truncate(index)
                        .map_err(Error::from_log_storage_err))
                }
                Action::LogCommit(_) => panic!(),
                Action::SaveState(_, _) => panic!(),
                Action::InstallSnapshot(_, _, _, _, _, _) => panic!(),
                Action::BroadcastMsg(_, _) => panic!(),
                Action::UnicastMsg(_, _, _) => panic!(),
                Action::UnicastLog(_, _, _, _) => panic!(),
                Action::SendSnapshot(_, _) => panic!(),
                Action::ResetTimeout => panic!(),
                Action::Transit(role) => {
                    let (consensus, actions) = self.consensus.take().unwrap().transit(role);
                    self.consensus = Some(consensus);
                    self.action_queue.extend(actions);
                }
                Action::Postpone(_, _) => panic!(),
                Action::Panic => panic!(),
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
