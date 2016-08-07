use rand;
use std::marker::PhantomData;
use AsyncKey;
use AsyncResult;
use NodeId;
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

use std::time;
pub trait Timer {
    fn is_elapsed(&self) -> bool;
    fn clear(&mut self);
    fn reset(&mut self, after: time::Duration);
    fn calc_after(&self, consensus::TimeoutKind, &config::Config) -> time::Duration;
}

pub struct DefaultTimer {
    duration: Option<time::Duration>,
    start_time: time::Instant,
}
impl DefaultTimer {
    pub fn new() -> Self {
        DefaultTimer {
            duration: None,
            start_time: time::Instant::now(),
        }
    }
}
impl Timer for DefaultTimer {
    fn is_elapsed(&self) -> bool {
        self.duration.as_ref().map_or(false, |d| *d <= self.start_time.elapsed())
    }
    fn clear(&mut self) {
        self.duration = None;
    }
    fn reset(&mut self, after: time::Duration) {
        self.start_time = time::Instant::now();
        self.duration = Some(after);
    }
    fn calc_after(&self, kind: consensus::TimeoutKind, config: &config::Config) -> time::Duration {
        match kind {
            consensus::TimeoutKind::Min => config.min_election_timeout,
            consensus::TimeoutKind::Max => config.max_election_timeout,
            consensus::TimeoutKind::Mid => {
                (config.max_election_timeout + config.min_election_timeout) / 2
            }
            consensus::TimeoutKind::Random => {
                // XXX: maybe incorrect
                let delta = config.max_election_timeout - config.min_election_timeout;
                let max_nanos = (delta.as_secs() << 32) + delta.subsec_nanos() as u64;
                let nanos = rand::random::<u64>() % max_nanos;
                time::Duration::new(nanos >> 32, nanos as u32) + config.min_election_timeout
            }
        }
    }
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

pub fn module<M, P, S>(postbox: P, storage: S) -> DefaultIoModule<M, P, S, DefaultTimer>
    where M: Machine,
          P: Postbox<M>,
          S: Storage<M>
{
    DefaultIoModule {
        postbox: postbox,
        storage: storage,
        timer: DefaultTimer::new(),
        _machine: PhantomData,
    }
}
