use std;
use std::collections::VecDeque;
use std::ops::Range;
use StateMachine;
use raft_protocol;
use raft_protocol::LogEntry;
use raft_protocol::Message;
use LogIndex;
use Term;
use NodeId;

pub trait Storage<T> {
    type Error;
    fn log_append(&mut self, entries: Vec<LogEntry<T>>) -> Result<(), Self::Error>;
    fn log_truncate(&mut self, range: Range<LogIndex>) -> Result<(), Self::Error>;
    fn log_get(&mut self, range: Range<LogIndex>) -> Result<Vec<LogEntry<T>>, Self::Error>;
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
            message: Message<T>,
            reply_channel: std::sync::mpsc::Sender<Message<T>>)
            -> Result<(), Self::Error>;
    fn reply(&mut self, from: &Self::From, message: Message<T>) -> Result<(), Self::Error>;
}

pub enum Error<S, R> {
    Storage(S),
    Rpc(R),
    Consensus(raft_protocol::Error),
}
impl<S, R> Error<S, R> {
    pub fn from_storage_error(e: S) -> Self {
        Error::Storage(e)
    }
}

pub struct SyncRsm<M, S, R>
    where M: StateMachine
{
    state_machine: M,
    storage: S,
    rpc: R,
    consensus: Option<raft_protocol::ConsensusModule>,
    action_queue: VecDeque<raft_protocol::Action<M::Command>>,
}
impl<M, S, R> SyncRsm<M, S, R>
    where M: StateMachine,
          S: Storage<M::Command>,
          R: Rpc<M::Command>
{
    pub fn new(state_machine: M, storage: S, rpc: R) -> Self {
        let conf = unsafe { std::mem::zeroed() };
        let table = unsafe { std::mem::zeroed() };
        let (consensus, actions) =
            raft_protocol::ConsensusModule::new("todo".to_string(), 0, None, conf, table);
        SyncRsm {
            state_machine: state_machine,
            storage: storage,
            rpc: rpc,
            consensus: Some(consensus),
            action_queue: actions.into_iter().collect(),
        }
    }
    pub fn run_once(&mut self) -> Result<bool, Error<S::Error, R::Error>> {
        if let Some(action) = self.action_queue.pop_front() {
            use raft_protocol::Action;
            match action {
                Action::LogAppend(entries) => {
                    try!(self.storage.log_append(entries).map_err(Error::from_storage_error))
                }
                Action::LogTruncate(_) => panic!(),
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
