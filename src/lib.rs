extern crate rand;

pub mod election;
pub mod log;
pub mod io;
pub mod config;
pub mod consensus;
mod state_machine;
pub mod replicator;

pub use state_machine::Machine;
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
