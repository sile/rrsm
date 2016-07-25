use std::collections::VecDeque;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;

pub type NodeId = String;
pub type Term = u64;
pub type LogIndex = u64;

pub trait ConsensusState<T> {
    fn init(&mut self) -> ActionQueue<T>;
    fn handle_command(&mut self, command: T) -> Result<ActionQueue<T>, Option<NodeId>>;
    fn handle_message(&mut self, sender: SenderInfo, message: Message<T>) -> ActionQueue<T>;
    fn handle_timeout(&mut self) -> ActionQueue<T>;
}

pub struct LogEntry<T> {
    pub term: Term,
    pub data: LogData<T>,
}
impl<T> LogEntry<T> {
    pub fn new(term: Term, data: LogData<T>) -> Self {
        LogEntry {
            term: term,
            data: data,
        }
    }
    pub fn noop(term: Term) -> Self {
        Self::new(term, LogData::Noop)
    }
    pub fn config(term: Term, config: &Config) -> Self {
        Self::new(term, LogData::Config(config.clone()))
    }
    pub fn command(term: Term, command: T) -> Self {
        Self::new(term, LogData::Command(command))
    }
}

pub enum LogData<T> {
    Config(Config),
    Noop,
    Command(T),
}

pub enum Action<T> {
    LogAppend(Vec<LogEntry<T>>),
    LogTruncate(LogIndex),
    InstallSnapshot(Vec<u8>, Term, LogIndex, Config),
    Broadcast(SenderInfo, Message<T>),
    BroadcastLog(SenderInfo, LogIndex, Range<LogIndex>),
    Unicast(SenderInfo, NodeId, Message<T>),
    UnicastLog(SenderInfo, LogIndex, NodeId, Range<LogIndex>),
    Postpone(SenderInfo, Message<T>),
    ResetTimeout,
    Commit(LogIndex),
    Transit(Role),
}

pub type ActionQueue<T> = VecDeque<Action<T>>;
fn queue<T>(from: Vec<Action<T>>) -> ActionQueue<T> {
    From::from(from)
}
fn single_action<T>(action: Action<T>) -> ActionQueue<T> {
    queue(vec![action])
}

pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct SenderInfo {
    pub id: NodeId,
    pub term: Term,
    pub log_index: LogIndex,
    pub log_term: Term,
}

pub enum Message<T> {
    RequestVoteCall,
    RequestVoteReply {
        vote_granted: bool,
    },
    AppendEntriesCall {
        entries: Vec<LogEntry<T>>,
        leader_commit: LogIndex,
    },
    AppendEntriesReply {
        success: bool,
    },
    InstallSnapshotCall {
        config: Config,
        data: Vec<u8>,
    },
    InstallSnapshotReply,
}

pub struct CommonState {
    pub id: NodeId,
    pub term: Term,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
    pub voted_for: Option<NodeId>,
    pub config: Config,
}
impl CommonState {
    pub fn increment_term(&mut self) {
        self.term = self.term.checked_add(1).unwrap();
    }
    pub fn make_sender_info(&self) -> SenderInfo {
        SenderInfo {
            id: self.id.clone(),
            term: self.term,
            log_index: self.last_log_index,
            log_term: self.last_log_term,
        }
    }
    fn make_reply_action<T>(&self, from: &SenderInfo, reply: Message<T>) -> Action<T> {
        Action::Unicast(self.make_sender_info(), from.id.clone(), reply)
    }
    fn is_unknown_node(&self, id: &NodeId) -> bool {
        !self.config.iter().any(|c| c.members.contains(id))
    }
    fn handle_message<T>(&mut self,
                         sender: SenderInfo,
                         message: Message<T>)
                         -> Result<ActionQueue<T>, (SenderInfo, Message<T>)> {
        if self.is_unknown_node(&sender.id) {
            Ok(queue(vec![]))
        } else if sender.term > self.term {
            self.term = sender.term;
            self.voted_for = None;
            Ok(queue(vec![Action::Transit(Role::Follower), Action::Postpone(sender, message)]))
        } else if sender.term < self.term {
            match message {
                Message::RequestVoteCall => {
                    let reply = Message::RequestVoteReply { vote_granted: false };
                    Ok(single_action(self.make_reply_action(&sender, reply)))
                }
                Message::AppendEntriesCall { .. } => {
                    let reply = Message::AppendEntriesReply { success: false };
                    Ok(single_action(self.make_reply_action(&sender, reply)))
                }
                _ => Ok(queue(vec![])),
            }
        } else {
            match message {
                Message::RequestVoteCall => {
                    let up_to_date = sender.log_index >= self.last_log_index;
                    if !up_to_date {
                        let reply = Message::RequestVoteReply { vote_granted: false };
                        Ok(single_action(self.make_reply_action(&sender, reply)))
                    } else if let Some(ref voted_for) = self.voted_for {
                        let reply =
                            Message::RequestVoteReply { vote_granted: *voted_for == sender.id };
                        Ok(single_action(self.make_reply_action(&sender, reply)))
                    } else {
                        self.voted_for = Some(sender.id.clone());
                        Ok(queue(vec![Action::Transit(Role::Follower),
                                      Action::Postpone(sender, message)]))
                    }
                }
                _ => Err((sender, message)),
            }
        }
    }
}


#[derive(Clone)]
pub struct Config {
    pub members: HashSet<NodeId>,
    pub old: Option<Box<Config>>,
}
impl Config {
    pub fn all_nodes(&self) -> HashSet<&NodeId> {
        let nodes = self.iter().flat_map(|c| c.members.iter()).collect();
        nodes
    }
    pub fn iter(&self) -> ConfigIter {
        ConfigIter { curr: Some(self) }
    }
}
pub struct ConfigIter<'a> {
    curr: Option<&'a Config>,
}
impl<'a> Iterator for ConfigIter<'a> {
    type Item = &'a Config;
    fn next(&mut self) -> Option<Self::Item> {
        use std::ops::Deref;
        let curr = self.curr.take();
        if let Some(c) = curr {
            self.curr = c.old.as_ref().map(|o| o.deref());
        }
        curr
    }
}

pub struct CandidateState {
    common: CommonState,
    vote_granted: HashMap<NodeId, bool>,
}
impl CandidateState {
    pub fn new(mut common: CommonState) -> Self {
        let vote_granted =
            common.config.all_nodes().into_iter().map(|n| (n.clone(), n == &common.id)).collect();
        common.increment_term();
        common.voted_for = Some(common.id.clone());
        CandidateState {
            common: common,
            vote_granted: vote_granted,
        }
    }
    pub fn is_elected(&self) -> bool {
        self.common.config.iter().all(|c| {
            let vote_granted =
                c.members.iter().filter(|n| self.vote_granted.contains_key(*n)).count();
            vote_granted > c.members.len() / 2
        })
    }
}
impl<T> ConsensusState<T> for CandidateState {
    fn init(&mut self) -> ActionQueue<T> {
        if self.is_elected() {
            single_action(Action::Transit(Role::Leader))
        } else {
            let message = Message::RequestVoteCall;
            queue(vec![Action::Broadcast(self.common.make_sender_info(), message),
                       Action::ResetTimeout])
        }
    }
    fn handle_command(&mut self, _command: T) -> Result<ActionQueue<T>, Option<NodeId>> {
        Err(Some(self.common.id.clone()))
    }
    fn handle_message(&mut self, sender: SenderInfo, message: Message<T>) -> ActionQueue<T> {
        self.common.handle_message(sender, message).unwrap_or_else(|(sender, message)| {
            match message {
                Message::AppendEntriesCall { .. } => {
                    self.common.voted_for = Some(sender.id.clone());
                    queue(vec![Action::Transit(Role::Follower), Action::Postpone(sender, message)])
                }
                Message::RequestVoteReply { vote_granted } if vote_granted => {
                    self.vote_granted.insert(sender.id, true);
                    if self.is_elected() {
                        single_action(Action::Transit(Role::Leader))
                    } else {
                        queue(vec![])
                    }
                }
                _ => queue(vec![]),
            }
        })
    }
    fn handle_timeout(&mut self) -> ActionQueue<T> {
        single_action(Action::Transit(Role::Candidate))
    }
}

pub struct FollowerState {
    common: CommonState,
}
impl FollowerState {
    pub fn new(common: CommonState) -> Self {
        FollowerState { common: common }
    }
    pub fn handle_append_entries<T>(&mut self,
                                    sender: SenderInfo,
                                    entries: Vec<LogEntry<T>>,
                                    leader_commit: LogIndex)
                                    -> ActionQueue<T> {
        if sender.log_index > self.common.last_log_index {
            let reply = Message::AppendEntriesReply { success: false };
            queue(vec![Action::Unicast(self.common.make_sender_info(), sender.id, reply),
                       Action::ResetTimeout])
        } else if sender.log_index == self.common.last_log_index &&
           sender.log_term == self.common.last_log_term {
            let reply = Message::AppendEntriesReply { success: true };
            if !entries.is_empty() {
                self.common.last_log_index = sender.log_index + entries.len() as u64;
                self.common.last_log_term = entries.last().unwrap().term;
                queue(vec![Action::LogAppend(entries),
                           Action::Commit(leader_commit),
                           Action::Unicast(self.common.make_sender_info(), sender.id, reply),
                           Action::ResetTimeout])
            } else {
                queue(vec![Action::Commit(leader_commit),
                           Action::Unicast(self.common.make_sender_info(), sender.id, reply),
                           Action::ResetTimeout])
            }
        } else {
            let rollback_point = if sender.log_index < self.common.last_log_index {
                // TODO: don't truncate already appended logs
                sender.log_index
            } else {
                sender.log_index - 1
            };
            let message = Message::AppendEntriesCall {
                entries: entries,
                leader_commit: leader_commit,
            };
            queue(vec![Action::LogTruncate(rollback_point), Action::Postpone(sender, message)])
        }
    }
    // NOTE: Invoked by driver after log truncated
    pub fn set_last_log_term(&mut self, term: Term) {
        self.common.last_log_term = term;
    }
}
impl<T> ConsensusState<T> for FollowerState {
    fn init(&mut self) -> ActionQueue<T> {
        single_action(Action::ResetTimeout)
    }
    fn handle_command(&mut self, _command: T) -> Result<ActionQueue<T>, Option<NodeId>> {
        Err(self.common.voted_for.clone())
    }
    fn handle_message(&mut self, sender: SenderInfo, message: Message<T>) -> ActionQueue<T> {
        self.common.handle_message(sender, message).unwrap_or_else(|(sender, message)| {
            match message {
                Message::AppendEntriesCall { entries, leader_commit } => {
                    self.handle_append_entries(sender, entries, leader_commit)
                }
                Message::InstallSnapshotCall { config, data } => {
                    // TODO: Support incremental snapshot
                    self.common.last_log_index = sender.log_index;
                    self.common.last_log_term = sender.log_term;
                    self.common.config = config.clone();
                    queue(vec![Action::InstallSnapshot(data,
                                                       sender.log_index,
                                                       sender.log_term,
                                                       config),
                               Action::Unicast(self.common.make_sender_info(),
                                               sender.id.clone(),
                                               Message::InstallSnapshotReply)])
                }
                _ => queue(vec![]),
            }
        })
    }
    fn handle_timeout(&mut self) -> ActionQueue<T> {
        single_action(Action::Transit(Role::Candidate))
    }
}

pub struct LeaderState {
    common: CommonState,
    commit_index: LogIndex,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
}
impl LeaderState {
    pub fn new(common: CommonState, commit_index: LogIndex) -> Self {
        let nodes = common.config
            .all_nodes()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let next_index = nodes.iter().map(|id| (id.clone(), common.last_log_index)).collect();
        let match_index = nodes.iter().map(|id| (id.clone(), 0)).collect();
        LeaderState {
            common: common,
            commit_index: commit_index,
            next_index: next_index,
            match_index: match_index,
        }
    }
    pub fn make_append_messages<T>(&mut self) -> Vec<Action<T>> {
        self.next_index
            .iter()
            .map(|(id, &next)| {
                let mut sender = self.common.make_sender_info();
                sender.log_index = next;
                // sender.log_term; TODO: self.common.log_term(next);
                Action::UnicastLog(sender,
                                   self.commit_index,
                                   id.clone(),
                                   next..self.common.last_log_index)
            })
            .collect()
    }
    pub fn find_last_commitable_index(&self) -> LogIndex {
        (self.commit_index..).take_while(|&i| self.is_commitable(i)).last().unwrap()
    }
    fn is_commitable(&self, i: LogIndex) -> bool {
        self.common.config.iter().all(|c| {
            let count = c.members
                .iter()
                .filter(|id| *id == &self.common.id || self.match_index[*id] >= i)
                .count();
            count > c.members.len()
        })
    }
}
impl<T> ConsensusState<T> for LeaderState {
    fn init(&mut self) -> ActionQueue<T> {
        let entry = LogEntry::noop(self.common.term);
        let message = Message::AppendEntriesCall {
            entries: vec![entry],
            leader_commit: self.commit_index,
        };

        let entry = LogEntry::noop(self.common.term);
        self.common.last_log_index += 1;
        queue(vec![Action::LogAppend(vec![entry]),
                   Action::Broadcast(self.common.make_sender_info(), message),
                   Action::ResetTimeout])
    }
    fn handle_command(&mut self, command: T) -> Result<ActionQueue<T>, Option<NodeId>> {
        self.common.last_log_index += 1;
        let entry = LogEntry::command(self.common.term, command);
        let mut actions = single_action(Action::LogAppend(vec![entry]));
        actions.extend(self.make_append_messages());
        actions.push_back(Action::ResetTimeout);
        Ok(actions)
    }
    // TODO: handle_query()
    fn handle_message(&mut self, sender: SenderInfo, message: Message<T>) -> ActionQueue<T> {
        self.common.handle_message(sender, message).unwrap_or_else(|(sender, message)| {
            match message {
                Message::AppendEntriesReply { .. } => {
                    // XXX:
                    self.next_index.insert(sender.id.clone(), sender.log_index);
                    self.match_index.insert(sender.id, sender.log_index);

                    // TODO: send delta (?)

                    let last_commit_index = self.find_last_commitable_index();
                    if last_commit_index != self.commit_index {
                        self.commit_index = last_commit_index;
                        queue(vec![Action::Commit(last_commit_index)])
                    } else {
                        queue(vec![])
                    }
                }
                _ => queue(vec![]),
            }
        })
    }
    fn handle_timeout(&mut self) -> ActionQueue<T> {
        let mut actions = self.make_append_messages();
        actions.push(Action::ResetTimeout);
        queue(actions)
    }
}

// TODO: Support change config

