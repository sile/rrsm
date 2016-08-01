pub mod replicator;
pub mod io;
pub mod config;
pub mod raft;

pub type Term = u64;
pub type LogIndex = u64;
pub type NodeId = String;

pub use self::config::Config;

pub trait Machine: Default {
    type Command;
    type Snapshot: Into<Self>;
    fn execute(&mut self, command: Self::Command);
    fn take_snapshot(&self) -> Self::Snapshot;
}

#[derive(Clone)]
pub struct Version {
    pub log_index: LogIndex,
    pub term: Term,
}
impl Version {
    pub fn new(log_index: LogIndex, term: Term) -> Self {
        Version {
            log_index: log_index,
            term: term,
        }
    }
}

pub enum NodeRole {
    Follower {
        voted_for: Option<NodeId>,
    },
    Candidate,
    Leader,
}

#[derive(Clone)]
pub struct Ballot {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}
impl Ballot {
    pub fn new(term: Term) -> Self {
        Ballot {
            term: term,
            voted_for: None,
        }
    }
    pub fn vote(mut self, id: &NodeId) -> Result<Self, Self> {
        if self.voted_for.is_none() {
            self.voted_for = Some(id.clone());
            Ok(self)
        } else {
            Err(self)
        }
    }
}
