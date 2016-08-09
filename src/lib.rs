use std::time;

pub mod election;
pub mod log;
pub mod io;
pub mod config;
pub mod consensus;
pub mod replicator;

pub trait Machine: Default {
    type Command;
    type Snapshot: Into<Self>;
    fn execute(&mut self, command: Self::Command);
    fn take_snapshot(&self) -> Self::Snapshot;
}

pub trait Rsm {
    type Machine: Machine;
    type Storage: io::Storage<Self::Machine>;
    type Postbox: io::Postbox<Self::Machine>;
    type Timer: Timer;
}

pub trait Timer: Default {
    fn expires_between(&mut self, min_after: time::Duration, max_after: time::Duration);
    fn is_expired(&self) -> bool;
    fn clear(&mut self);
}

// TODO:
pub use replicator::Replicator;

pub type NodeId = String;
pub type NodeGeneration = u64;

pub type AsyncKey = u64;

pub struct AsyncResult<T, E> {
    pub key: AsyncKey,
    pub result: Result<T, E>,
}

#[derive(Clone,Debug)]
pub struct Node {
    pub id: NodeId,
    pub generation: NodeGeneration,
}
impl Node {
    pub fn new(id: NodeId, generation: NodeGeneration) -> Self {
        Node {
            id: id,
            generation: generation,
        }
    }
}
