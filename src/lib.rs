pub mod raft_protocol;
pub mod sync;

pub use self::raft_protocol::{NodeId, Term, LogIndex, LogEntry};

pub trait StateMachine {
    type Command;
    fn execute(&mut self, command: Self::Command);
    // fn install_snapshot();
    // fn take_snapshot();
}
