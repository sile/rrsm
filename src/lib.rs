extern crate rand;

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
    type Timer: io::Timer;
    // TODO: type Clock;
}


// TODO:
pub use replicator::Replicator;

pub type NodeId = String;
pub type CheckDigit = u64;
pub type AsyncKey = u64;

pub struct AsyncResult<T, E> {
    pub key: AsyncKey,
    pub result: Result<T, E>,
}

#[derive(Clone,Debug)]
pub struct Node {
    pub id: NodeId,
    pub check_digit: CheckDigit,
}
impl Node {
    pub fn new(id: &NodeId) -> Self {
        Node {
            id: id.clone(),
            check_digit: rand::random(),
        }
    }
}
