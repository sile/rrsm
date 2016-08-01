use super::*;
use replicator;

pub struct MessageHeader;
pub struct Message<T>(pub T);
pub struct Error;

pub struct ConsensusModule;
impl ConsensusModule {
    pub fn new(_node_id: &NodeId,
               _ballot: &election::Ballot,
               _config: &config::Config,
               _table: log::IndexTable)
               -> Self {
        panic!()
    }
    pub fn handle_timeout<M>(&mut self) -> Result<Vec<Action<M>>, Error>
        where M: Machine
    {
        panic!()
    }
    pub fn handle_message<M>(&mut self, _message: Message<M>) -> Result<Vec<Action<M>>, Error>
        where M: Machine
    {
        panic!()
    }
    pub fn sync<M>(&mut self) -> Result<Vec<Action<M>>, Error>
        where M: Machine
    {
        panic!()
    }
    pub fn propose<M>(&mut self, _command: M::Command) -> Result<Option<Vec<Action<M>>>, Error>
        where M: Machine
    {
        panic!()
    }
    pub fn handle_snapshot_installed(&mut self,
                                     _last_included: &log::EntryVersion)
                                     -> Result<(), Error> {
        panic!()
    }
    pub fn update_config<M>(&mut self,
                            _config: config::Config)
                            -> Result<Option<Vec<Action<M>>>, Error>
        where M: Machine
    {
        panic!()
    }
    pub fn leader(&self) -> Option<&NodeId> {
        panic!()
    }
    pub fn timestamp(&self) -> Timestamp {
        panic!()
    }
    pub fn config(&self) -> &config::Config {
        panic!()
    }
}

pub enum Action<M>
    where M: Machine
{
    ResetTimeout(TimeoutKind),
    Postpone(Message<M>),
    BroadcastMsg(Message<M>),
    UnicastMsg(NodeId, Message<M>),
    UnicastLog {
        destination: NodeId,
        header: MessageHeader,
        first_index: log::Index,
        max_len: usize,
        leader_commit: log::Index,
    },
    InstallSnapshot(replicator::Snapshot<M>),
    SaveBallot(election::Ballot),
    LogAppend(Vec<log::Entry<M::Command>>),
    LogRollback(log::Index),
    LogApply(log::EntryVersion),
}

pub enum TimeoutKind {
    Min,
    Max,
    Random,
}

pub type Timestamp = u64;
