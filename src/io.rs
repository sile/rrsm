use AsyncKey;
use AsyncResult;
use NodeId;
use Machine;
use log;
use election;
use consensus;
use replicator;

// non-byzantine unreliable communication channel
pub trait Postbox<M>
    where M: Machine
{
    fn send_ref(&mut self, destination: &NodeId, message: &consensus::Message<M>);
    fn send_val(&mut self, destination: &NodeId, message: consensus::Message<M>);
    fn try_recv(&mut self) -> Option<consensus::Message<M>>;
}

pub trait Storage<M>
    where M: Machine
{
    type Error;
    fn log_append(&mut self, entries: Vec<log::Entry<M::Command>>, AsyncKey);
    fn log_truncate(&mut self, next_index: log::Index, AsyncKey);
    fn log_drop_until(&mut self, first_index: log::Index, AsyncKey);
    fn log_get(&mut self, first_index: log::Index, max_len: usize, AsyncKey);
    fn last_appended(&self) -> log::EntryVersion;
    fn least_stored(&self) -> log::EntryVersion;
    fn build_log_index_table(&self) -> log::IndexTable;

    fn save_ballot(&mut self, ballot: election::Ballot, AsyncKey);
    fn load_ballot(&mut self, AsyncKey);
    fn save_snapshot(&mut self, snapshot: replicator::Snapshot<M>, AsyncKey);
    fn load_snapshot(&mut self, AsyncKey);

    fn try_run_once(&mut self) -> Option<StorageAsyncResult<M, Self::Error>>;
    fn run_once(&mut self) -> StorageAsyncResult<M, Self::Error>;
    fn flush(&mut self) -> Result<(), Self::Error>;
}

pub type StorageAsyncResult<M, E> = AsyncResult<StorageData<M>, E>;

pub enum StorageData<M>
    where M: Machine
{
    Entries(Vec<log::Entry<M::Command>>),
    Ballot(election::Ballot),
    Snapshot(replicator::Snapshot<M>),
    None,
}
