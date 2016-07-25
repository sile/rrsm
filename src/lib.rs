extern crate eventual;

use std::collections::HashMap;
use std::collections::HashSet;
use eventual::Future;
use eventual::Async;

pub mod raft;

pub type NodeId = String;
pub type Term = u64;
pub type Index = u64;

pub trait Storage {}

pub struct Source {
    pub id: NodeId,
    pub term: Term,
    pub log_index: Index,
    pub log_term: Term,
}

pub trait StateMachine {
    type Command: Send + Clone;
    fn execute(&mut self, command: Self::Command);
    // fn snapshot();
}

enum RoleState {
    Follower,
    Candidate {
        vote_granted_count: usize,
    },
    Leader {
        next_index: HashMap<NodeId, Index>,
        match_index: HashMap<NodeId, Index>,
    },
}

pub trait Log<C>
    where C: Send
{
    type Error: Send;
    fn save_current_term(&mut self, term: Term) -> Future<(), Self::Error>;
    fn save_voted_for(&mut self, voted_for: Option<&NodeId>) -> Future<(), Self::Error>;
    fn save_command(&mut self, term: Term, command: C) -> Future<C, Self::Error>;
    fn save_commands(&mut self, commands: Vec<(Term, C)>) -> Future<Vec<(Term, C)>, Self::Error>;
}

pub enum TimeoutKind {
    FollowerWait,
    CandidateWait,
    LeaderHeartbeat,
}

pub trait Transport<C> {
    // TODO: Return `Result`
    fn send(&mut self, destination: &NodeId, message: Message<C>);
}

// TODO
pub enum HandleError {
    NotLeader,
    Todo,
}

// TODO: Support snapshots
// TODO: Support member change
pub struct Rsm<S, L, T> {
    id: NodeId,
    // new_peers: Option<HashSet<NodeId>>,
    peers: HashSet<NodeId>,
    role_state: RoleState,
    state_machine: S,

    // TODO: => outbox (? or mailbox, postbox, pillar-box)
    transporter: T,

    // Persistent state on all nodes
    current_term: Term,
    voted_for: Option<NodeId>,
    log: L,

    // Volatile state on all nodes
    commit_index: Index,
    last_applied: Index,

    timeout_kind: Option<TimeoutKind>,
}
impl<S, L, T> Rsm<S, L, T>
    where S: StateMachine + Send,
          L: Log<S::Command> + Send,
          T: Transport<S::Command> + Send
{
    pub fn new(id: NodeId,
               peers: HashSet<NodeId>,
               state_machine: S,
               log: L,
               transporter: T)
               -> Self {
        Rsm {
            id: id,
            peers: peers,
            role_state: RoleState::Follower,
            state_machine: state_machine,
            transporter: transporter,
            current_term: 0,
            voted_for: None,
            log: log,
            commit_index: 0,
            last_applied: 0,
            timeout_kind: None,
        }
    }
    pub fn get_leader_id(&self) -> Option<&NodeId> {
        match self.role_state {
            RoleState::Leader { .. } => Some(&self.id),
            RoleState::Follower => self.voted_for.as_ref(),
            RoleState::Candidate { .. } => None,
        }
    }
    pub fn is_leader(&self) -> bool {
        self.get_leader_id().map_or(false, |id| self.id == *id)
    }
    pub fn take_timeout(&mut self) -> Option<TimeoutKind> {
        self.timeout_kind.take()
    }
    pub fn execute(mut self, command: S::Command) -> Future<Self, HandleError> {
        if !self.is_leader() {
            return Future::error(HandleError::NotLeader);
        }
        let future = self.log.save_command(self.current_term, command);
        future.and_then(|c| {
                self.broadcast_append_entries_rpc(c);
                Ok(self)
            })
            .or_else(|_| Err(HandleError::Todo))
    }
    pub fn handle_message(mut self, message: Message<S::Command>) -> Future<Self, HandleError> {
        match message {
            Message::AppendEntriesCall { source, entries, leader_commit } => {
                self.handle_append_entries(source, entries, leader_commit)
            }
            Message::RequestVoteCall { source } => self.handle_request_vote(source),
            Message::Reply { source, success } => self.handle_reply(source, success),
        }
    }
    pub fn handle_timeout(mut self) -> Future<Self, HandleError> {
        match self.role_state {
            RoleState::Follower => {
                self.role_state = RoleState::Candidate { vote_granted_count: 0 };
                self.broadcast_request_vote_rpc();
                Future::of(self)
            }
            RoleState::Candidate { .. } => {
                self.role_state = RoleState::Candidate { vote_granted_count: 0 };
                self.timeout_kind = Some(TimeoutKind::CandidateWait);
                self.broadcast_request_vote_rpc();
                Future::of(self)
            }
            RoleState::Leader { .. } => {
                self.timeout_kind = Some(TimeoutKind::LeaderHeartbeat);
                self.broadcast_heartbeat();
                Future::of(self)
            }
        }
    }
    fn handle_append_entries(mut self,
                             source: Source,
                             entries: Vec<(Term, S::Command)>,
                             leader_commit: Index)
                             -> Future<Self, HandleError> {
        if entries.is_empty() {
            let message = self.make_reply(true);
            self.transporter.send(&source.id, message);
            Future::of(self)
        } else {
            // TODO: より正確に
            let future = self.log.save_commands(entries);
            future.and_then(move |_| {
                    let message = self.make_reply(true);
                    self.transporter.send(&source.id, message);
                    Ok(self)
                })
                .or_else(|_| Err(HandleError::Todo))
        }
    }
    fn handle_request_vote(mut self, source: Source) -> Future<Self, HandleError> {
        let vote_granted = self.does_grant(&source);
        let message = self.make_reply(vote_granted);
        if vote_granted {
            self.voted_for = Some(source.id.clone());
        }
        self.transporter.send(&source.id, message);
        Future::of(self)
    }
    fn does_grant(&self, candidate: &Source) -> bool {
        // TODO: より正確に
        if candidate.term < self.current_term {
            return false;
        }
        if self.voted_for.as_ref().unwrap_or(&candidate.id) != &candidate.id {
            return false;
        }
        // TODO: Compare log indices
        return true;
    }
    fn handle_reply(mut self, source: Source, success: bool) -> Future<Self, HandleError> {
        // TODO: リクエストとの対応が取れるようにする
        match self.role_state {
            RoleState::Leader { .. } => {
                // TODO: 次に適用可能なコマンドを探して実行する
                Future::of(self)
            }
            RoleState::Candidate { vote_granted_count } => {
                if success {
                    // TODO: HashSetにして、同じIDはカウント外にする
                    self.role_state =
                        RoleState::Candidate { vote_granted_count: vote_granted_count + 1 };
                }
                Future::of(self)
            }
            RoleState::Follower => Future::of(self),
        }
    }
    fn broadcast_append_entries_rpc(&mut self, command: S::Command) {
        // TODO: 最新のみではなく、差分がある場合にはそれも送信する
        // (ただし上限は必要)
        for p in &self.peers {
            let message = self.make_append_entries_rpc(&command);
            self.transporter.send(p, message);
        }
    }
    fn broadcast_heartbeat(&mut self) {
        for p in &self.peers {
            let message = self.make_heartbeat_message();
            self.transporter.send(p, message);
        }
    }
    fn broadcast_request_vote_rpc(&mut self) {
        for p in &self.peers {
            let message = self.make_request_vote_rpc();
            self.transporter.send(p, message);
        }
    }
    fn make_reply(&self, success: bool) -> Message<S::Command> {
        let source = self.make_source();
        Message::Reply {
            source: source,
            success: success,
        }
    }
    fn make_append_entries_rpc(&self, command: &S::Command) -> Message<S::Command> {
        // TODO: adjust prevLogXXX
        let source = self.make_source();
        Message::AppendEntriesCall {
            source: source,
            entries: vec![(self.current_term, command.clone())], // TODO: optimize
            leader_commit: self.commit_index,
        }
    }
    fn make_heartbeat_message(&self) -> Message<S::Command> {
        // TODO: adjust prevLogXXX
        let source = self.make_source();
        Message::AppendEntriesCall {
            source: source,
            entries: Vec::with_capacity(0),
            leader_commit: self.commit_index,
        }
    }
    fn make_request_vote_rpc(&self) -> Message<S::Command> {
        let source = self.make_source();
        Message::RequestVoteCall { source: source }
    }
    fn make_source(&self) -> Source {
        Source {
            id: self.id.clone(),
            term: self.current_term,

            // TODO: self.log.last.{index,term}
            log_index: self.commit_index,
            log_term: self.current_term,
        }
    }
}

pub enum Message<C> {
    RequestVoteCall {
        source: Source,
    },
    AppendEntriesCall {
        source: Source,
        entries: Vec<(Term, C)>,
        leader_commit: Index,
    },
    // TODO
    Reply {
        source: Source,
        success: bool,
    },
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
