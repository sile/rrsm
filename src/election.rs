use super::*;

pub type Term = u64;

pub enum Role {
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
    pub fn increment_term(&mut self) {
        self.term = self.term.checked_add(1).unwrap();
    }
    pub fn vote(&mut self, node: NodeId) {
        self.voted_for = Some(node);
    }
}
