use std::ops::Range;
use std::time::Duration;
use std::collections::HashSet;
use super::NodeId;

pub type ClusterMembers = HashSet<NodeId>;

pub struct Builder {
    members: ClusterMembers,
    election_timeout: Range<Duration>,
}
impl Builder {
    pub fn new(members: ClusterMembers) -> Self {
        Builder {
            members: members,
            election_timeout: Duration::from_millis(100)..Duration::from_millis(1000),
        }
    }
    pub fn election_timeout(mut self, timeout: Range<Duration>) -> Self {
        self.election_timeout = timeout;
        self
    }
    pub fn build(self) -> Config {
        Config {
            cluster_state: ClusterState::Stable { members: self.members },
            election_timeout: self.election_timeout,
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub cluster_state: ClusterState,
    pub election_timeout: Range<Duration>,
}
impl Config {
    pub fn is_member(&self, id: &NodeId) -> bool {
        match self.cluster_state {
            ClusterState::Stable { ref members } => members.contains(id),
            ClusterState::InTransition { ref new_members, ref old_members, .. } => {
                new_members.contains(id) || old_members.contains(id)
            }
        }
    }
}

#[derive(Clone)]
pub enum ClusterState {
    Stable {
        members: ClusterMembers,
    },
    InTransition {
        phase: TransitionPhase,
        new_members: ClusterMembers,
        old_members: ClusterMembers,
    },
}

#[derive(Clone)]
pub enum TransitionPhase {
    CatchUp,
    CutOff,
}
