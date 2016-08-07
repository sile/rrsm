use std::time;
use std::collections::hash_set;
use std::collections::HashSet;
use super::*;

#[derive(Clone,Debug)]
pub struct Config {
    cluster_state: ClusterState,
    pub min_election_timeout: time::Duration,
    pub max_election_timeout: time::Duration,
}
impl Config {
    pub fn is_member(&self, node_id: &NodeId) -> bool {
        match self.cluster_state {
            ClusterState::Stable { ref cluster } => cluster.is_member(node_id),
            ClusterState::Transition { ref old, ref new, .. } => {
                old.is_member(node_id) && new.is_member(node_id)
            }
        }
    }
    pub fn all_members(&self) -> NodeIter {
        match self.cluster_state {
            ClusterState::Stable { ref cluster } => NodeIter::new(cluster, None),
            ClusterState::Transition { ref old, ref new, .. } => NodeIter::new(old, Some(new)),
        }
    }
    pub fn iter_votable_cluster(&self) -> ClusterIter {
        match self.cluster_state {
            ClusterState::Stable { ref cluster } => ClusterIter::new(Some(cluster), None), 
            ClusterState::Transition { phase: Phase::CatchUp, ref old, .. } => {
                ClusterIter::new(Some(old), None)
            }
            ClusterState::Transition { phase: Phase::CutOff, ref old, ref new } => {
                ClusterIter::new(Some(old), Some(new))
            }
        }
    }
    pub fn iter_nonvotable_cluster(&self) -> ClusterIter {
        match self.cluster_state {
            ClusterState::Stable { .. } => ClusterIter::new(None, None),
            ClusterState::Transition { phase: Phase::CatchUp, ref new, .. } => {
                ClusterIter::new(Some(new), None)
            }
            ClusterState::Transition { phase: Phase::CutOff, .. } => ClusterIter::new(None, None),
        }
    }
    pub fn merge_config(&self, config: Config) -> Config {
        // TODO: Config => Builder
        let new_cluster = if let ClusterState::Stable{cluster} = config.cluster_state {
            cluster
        } else {
            panic!()
        };
        let cluster_state =
            match self.cluster_state {
                ClusterState::Stable{ref cluster} => {
                    ClusterState::Transition{
                        phase: Phase::CatchUp,
                        new: new_cluster,
                        old: cluster.clone()
                    }
                }
                ClusterState::Transition{ref old, ..} => {
                    ClusterState::Transition{
                        phase: Phase::CatchUp,
                        new: new_cluster,
                        old: old.clone()
                    }
                }
            };
        Config{
            cluster_state: cluster_state,
            min_election_timeout: config.min_election_timeout,
            max_election_timeout: config.max_election_timeout
        }
    }
    pub fn next_phase(&self) -> Option<Self> {
        self.cluster_state.next_phase().map(|c| {
            Config{cluster_state: c, ..self.clone()}
        })
    }
}

pub struct Builder {
    members: HashSet<NodeId>,
    min_election_timeout: time::Duration,
    max_election_timeout: time::Duration,
}
impl Builder {
    pub fn new(members: HashSet<NodeId>) -> Self {
        Builder {
            members: members,
            min_election_timeout: time::Duration::from_millis(100),
            max_election_timeout: time::Duration::from_millis(1000),
        }
    }
    pub fn election_timeout(mut self, min: time::Duration, max: time::Duration) -> Self {
        assert!(min <= max);
        self.min_election_timeout = min;
        self.max_election_timeout = max;
        self
    }
    pub fn build(self) -> Config {
        Config {
            cluster_state: ClusterState::Stable { cluster: Cluster { members: self.members } },
            min_election_timeout: self.min_election_timeout,
            max_election_timeout: self.max_election_timeout,
        }
    }
}

#[derive(Clone,Debug)]
pub struct Cluster {
    pub members: HashSet<NodeId>,
}
impl Cluster {
    pub fn is_member(&self, node_id: &NodeId) -> bool {
        self.members.contains(node_id)
    }
}

#[derive(Clone,Debug)]
enum ClusterState {
    Stable {
        cluster: Cluster,
    },
    Transition {
        phase: Phase,
        new: Cluster,
        old: Cluster,
    },
}
impl ClusterState {
    fn next_phase(&self) -> Option<Self> {
        match *self {
            ClusterState::Stable{..} => None,
            ClusterState::Transition{phase: Phase::CatchUp, ref new, ref old} => {
                Some(ClusterState::Transition{phase: Phase::CutOff, new: new.clone(), old: old.clone()})
            }
            ClusterState::Transition{phase: Phase::CutOff, ref new,..} => {
                Some(ClusterState::Stable{cluster: new.clone()})
            }
        }
    }
}

#[derive(Clone,Debug)]
enum Phase {
    CatchUp,
    CutOff,
}

pub struct ClusterIter<'a> {
    old: Option<&'a Cluster>,
    new: Option<&'a Cluster>,
}
impl<'a> ClusterIter<'a> {
    pub fn new(old: Option<&'a Cluster>, new: Option<&'a Cluster>) -> Self {
        ClusterIter {
            old: old,
            new: new,
        }
    }
}
impl<'a> Iterator for ClusterIter<'a> {
    type Item = &'a Cluster;
    fn next(&mut self) -> Option<Self::Item> {
        self.old.take().or_else(|| self.new.take())
    }
}

pub struct NodeIter<'a> {
    old: hash_set::Iter<'a, NodeId>,
    new: Option<hash_set::Iter<'a, NodeId>>,
}
impl<'a> NodeIter<'a> {
    pub fn new(old: &'a Cluster, new: Option<&'a Cluster>) -> Self {
        NodeIter {
            old: old.members.iter(),
            new: new.map(|c| c.members.iter()),
        }
    }
}
impl<'a> Iterator for NodeIter<'a> {
    type Item = &'a NodeId;
    fn next(&mut self) -> Option<Self::Item> {
        self.old.next().or_else(|| self.new.as_mut().and_then(|i| i.next()))
    }
}
