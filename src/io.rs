use std::marker::PhantomData;
use AsyncKey;
use AsyncResult;
use Node;
use Machine;
use log;
use election;
use consensus;
use replicator;
use config;

// non-byzantine unreliable communication channel
pub trait Postbox<M>
    where M: Machine
{
    fn send_ref(&mut self, destination: &Node, message: &consensus::Message<M>);
    fn send_val(&mut self, destination: &Node, message: consensus::Message<M>);
    fn try_recv(&mut self) -> Option<consensus::Message<M>>;
}

pub trait Storage<M>
    where M: Machine
{
    type Error;
    fn log_append(&mut self, entries: Vec<log::Entry<M::Command>>, AsyncKey);
    fn log_truncate(&mut self, next_index: log::Index, AsyncKey);
    fn log_drop_until(&mut self, first_index: log::Index, AsyncKey);
    fn log_get(&mut self, first_index: log::Index, max_len: usize) -> AsyncKey;
    fn last_appended(&self) -> log::EntryVersion;
    fn build_log_index_table(&self) -> log::IndexTable;

    fn save_ballot(&mut self, ballot: election::Ballot, AsyncKey);
    fn load_ballot(&mut self) -> AsyncKey;
    fn save_snapshot(&mut self, snapshot: replicator::Snapshot<M>, AsyncKey);
    fn load_snapshot(&mut self) -> AsyncKey;

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

pub trait Timer {
    fn is_elapsed(&self) -> bool;
    fn clear(&mut self);
    fn reset(&mut self, after: &::std::time::Duration);
    fn calc_after(&self, consensus::TimeoutKind, &config::Config) -> ::std::time::Duration;
}

pub trait IoModule<M>
    where M: Machine
{
    type Postbox: Postbox<M>;
    type Storage: Storage<M>;
    type Timer: Timer;

    fn postbox_ref(&self) -> &Self::Postbox;
    fn postbox_mut(&mut self) -> &mut Self::Postbox;
    fn storage_ref(&self) -> &Self::Storage;
    fn storage_mut(&mut self) -> &mut Self::Storage;
    fn timer_ref(&self) -> &Self::Timer;
    fn timer_mut(&mut self) -> &mut Self::Timer;
}

pub struct DefaultIoModule<M, P, S, T> {
    pub postbox: P,
    pub storage: S,
    pub timer: T,
    _machine: PhantomData<M>,
}
impl<M, P, S, T> IoModule<M> for DefaultIoModule<M, P, S, T>
    where M: Machine,
          P: Postbox<M>,
          S: Storage<M>,
          T: Timer
{
    type Postbox = P;
    type Storage = S;
    type Timer = T;

    fn postbox_ref(&self) -> &Self::Postbox {
        &self.postbox
    }
    fn postbox_mut(&mut self) -> &mut Self::Postbox {
        &mut self.postbox
    }
    fn storage_ref(&self) -> &Self::Storage {
        &self.storage
    }
    fn storage_mut(&mut self) -> &mut Self::Storage {
        &mut self.storage
    }
    fn timer_ref(&self) -> &Self::Timer {
        &self.timer
    }
    fn timer_mut(&mut self) -> &mut Self::Timer {
        &mut self.timer
    }
}

pub fn module<M, P, S, T>(postbox: P, storage: S, timer: T) -> DefaultIoModule<M, P, S, T>
    where M: Machine,
          P: Postbox<M>,
          S: Storage<M>,
          T: Timer
{
    DefaultIoModule {
        postbox: postbox,
        storage: storage,
        timer: timer,
        _machine: PhantomData,
    }
}
