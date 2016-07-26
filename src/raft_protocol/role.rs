use std::collections::HashSet;
use std::collections::HashMap;

use super::*;

pub trait ConsensusRole<T> {
    fn init(&mut self) -> Actions<T>;
    fn handle_command(&mut self, command: T) -> Result<(LogIndex, Actions<T>), Option<&NodeId>>;
    fn handle_change_config(&mut self, new_config: Config) -> Result<Actions<T>, Option<&NodeId>>;
    fn handle_message(&mut self, sender: Sender, message: Message<T>) -> Actions<T>;
    fn handle_event(&mut self, event: Event) -> Actions<T>;
}

pub enum RoleKind {
    Follower,
    Candidate,
    Leader,
}

// NOTE: 0-th entry is reserved for noop
pub struct LogIndexTable {
    last_log_index: LogIndex,
    table: Vec<(LogIndex, Term)>,
}
impl LogIndexTable {
    pub fn last_log_index(&self) -> LogIndex {
        self.last_log_index
    }
    pub fn last_log_term(&self) -> Term {
        self.table.last().map(|t| t.1).unwrap_or(0)
    }
    pub fn first_log_index(&self) -> LogIndex {
        self.table.first().map(|t| t.0).unwrap_or(0)
    }
    pub fn get_term(&self, index: LogIndex) -> Option<Term> {
        // TODO: binary search
        if index > self.last_log_index() {
            None
        } else {
            self.table.iter().skip_while(|e| e.0 <= index).map(|e| e.1).last()
        }
    }
    pub fn truncate_if_needed<T>(&mut self,
                                 sender: &Sender,
                                 entries: &[LogEntry<T>])
                                 -> Option<bool> {
        // TODO: optimize
        let base_index = sender.log_index + 1;
        let mut last_invalid = None;
        for j in 0..entries.len() {
            let i = entries.len() - j - 1;
            let index = base_index + i as u64;
            if let Some(t) = self.get_term(index) {
                if t == entries[i].term {
                    break;
                } else {
                    last_invalid = Some(index);
                }
            }
        }
        if let Some(t) = self.get_term(sender.log_index) {
            if t != sender.log_term {
                last_invalid = Some(sender.log_index);
            }
        }
        if let Some(invalid) = last_invalid {
            let table = self.table.iter().take_while(|e| e.0 < invalid).cloned().collect();
            self.table = table;
            self.last_log_index = invalid - 1;
            Some(!self.table.is_empty())
        } else {
            None
        }
    }
    pub fn handle_snapshot_installed(&mut self,
                                     first_log_index: LogIndex,
                                     first_log_term: Term)
                                     -> bool {
        if first_log_term < self.first_log_index() {
            return false;
        }
        if !self.get_term(first_log_index).map_or(true, |t| first_log_term == t) {
            return false;
        }
        let mut table = vec![(first_log_index, first_log_term)];
        table.extend(self.table.iter().skip_while(|e| e.0 <= first_log_index));
        self.table = table;
        true
    }
    fn extend<T>(&mut self, entries: &[LogEntry<T>]) {
        let next_index = self.last_log_index() + 1;
        for (offset, e) in entries.iter().enumerate() {
            if self.last_log_term() != e.term {
                let index = next_index + offset as u64;
                self.table.push((index, e.term));
            }
        }
        self.last_log_index += entries.len() as u64;
    }
}

pub struct Common {
    pub id: NodeId,
    pub term: Term,
    pub voted_for: Option<NodeId>,
    pub config: ConfigState,
    pub log_index_table: LogIndexTable,
}
impl Common {
    fn last_log_index(&self) -> LogIndex {
        self.log_index_table.last_log_index()
    }
    fn last_log_term(&self) -> Term {
        self.log_index_table.last_log_term()
    }
    pub fn make_sender(&self) -> Sender {
        Sender {
            id: self.id.clone(),
            term: self.term,
            log_index: self.last_log_index(),
            log_term: self.last_log_term(),
        }
    }
    pub fn save_persistent_state<T>(&self) -> Action<T> {
        Action::SaveState(self.term, self.voted_for.clone())
    }
    pub fn reply<T>(&self, from: &Sender, message: Message<T>) -> Action<T> {
        Action::UnicastMsg(self.make_sender(), from.id.clone(), message)
    }
    pub fn reply2<T>(&self,
                     from: &Sender,
                     index: LogIndex,
                     term: Term,
                     message: Message<T>)
                     -> Action<T> {
        let mut sender = self.make_sender();
        sender.log_index = index;
        sender.log_term = term;
        Action::UnicastMsg(sender, from.id.clone(), message)
    }

    pub fn handle_message<T>(&mut self,
                             sender: Sender,
                             message: Message<T>)
                             -> Result<Actions<T>, (Sender, Message<T>)> {
        if !self.config.is_member(&sender.id) {
            Ok(no_action())
        } else if sender.term > self.term {
            self.term = sender.term;
            self.voted_for = None;
            Ok(vec![self.save_persistent_state(),
                    Action::to_follower(),
                    Action::Postpone(sender, message)])
        } else if sender.term < self.term {
            match message {
                Message::RequestVoteCall => {
                    let reply = Message::RequestVoteReply { vote_granted: false };
                    Ok(vec![self.reply(&sender, reply)])
                }
                Message::AppendEntriesCall { .. } => {
                    let reply = Message::AppendEntriesReply;// { success: false };
                    Ok(vec![self.reply(&sender, reply)])
                }
                _ => Ok(no_action()),
            }
        } else {
            // NOTE: `sender.term == self.term`
            match message {
                Message::RequestVoteCall => {
                    let up_to_date = if sender.log_term == self.last_log_term() {
                        sender.log_index >= self.last_log_index()
                    } else {
                        sender.log_term > self.last_log_term()
                    };
                    if !up_to_date {
                        let reply = Message::RequestVoteReply { vote_granted: false };
                        Ok(vec![self.reply(&sender, reply)])
                    } else if let Some(ref voted_for) = self.voted_for {
                        let reply =
                            Message::RequestVoteReply { vote_granted: voted_for == &sender.id };
                        Ok(vec![self.reply(&sender, reply)])
                    } else {
                        self.voted_for = Some(sender.id.clone());
                        Ok(vec![self.save_persistent_state(),
                                Action::to_follower(),
                                Action::Postpone(sender, message)])
                    }
                }
                _ => Err((sender, message)),
            }
        }
    }
}

fn no_action<T>() -> Actions<T> {
    Vec::with_capacity(0)
}

pub struct CandidateRole {
    common: Common,
    vote_granted: HashSet<NodeId>,
}
impl CandidateRole {
    pub fn new(mut common: Common) -> Self {
        assert!(common.config.is_member(&common.id));
        let vote_granted = [&common.id].iter().cloned().cloned().collect();
        common.term += 1;
        common.voted_for = Some(common.id.clone());
        CandidateRole {
            common: common,
            vote_granted: vote_granted,
        }
    }
    pub fn is_elected(&self) -> bool {
        self.common.config.votable_configs().into_iter().all(|c| {
            let votes = c.members.iter().filter(|&n| self.vote_granted.contains(n)).count();
            votes > c.members.len() / 2
        })
    }
}
impl<T> ConsensusRole<T> for CandidateRole {
    fn init(&mut self) -> Actions<T> {
        let mut actions = vec![self.common.save_persistent_state()];
        if self.is_elected() {
            actions.push(Action::to_leader());
        } else {
            let call = Message::RequestVoteCall;
            actions.push(Action::BroadcastMsg(self.common.make_sender(), call));
            actions.push(Action::ResetTimeout);
        }
        actions
    }
    fn handle_command(&mut self, _command: T) -> Result<(LogIndex, Actions<T>), Option<&NodeId>> {
        Err(self.common.voted_for.as_ref())
    }
    fn handle_change_config(&mut self, _new_config: Config) -> Result<Actions<T>, Option<&NodeId>> {
        Err(self.common.voted_for.as_ref())
    }
    fn handle_message(&mut self, sender: Sender, message: Message<T>) -> Actions<T> {
        self.common.handle_message(sender, message).unwrap_or_else(|(sender, message)| {
            match message {
                Message::AppendEntriesCall { .. } => {
                    self.common.voted_for = Some(sender.id.clone());
                    vec![Action::to_follower(), Action::Postpone(sender, message)]
                }
                Message::RequestVoteReply { vote_granted } if vote_granted => {
                    self.vote_granted.insert(sender.id);
                    if self.is_elected() {
                        vec![Action::to_leader()]
                    } else {
                        no_action()
                    }
                }
                _ => no_action(),
            }
        })
    }
    fn handle_event(&mut self, event: Event) -> Actions<T> {
        match event {
            Event::Timeout => vec![Action::to_candidate()],
            Event::SnapshotInstalled(first_log_index, first_log_term) => {
                if !self.common
                    .log_index_table
                    .handle_snapshot_installed(first_log_index, first_log_term) {
                    // inconsistent snapshot
                    vec![Action::Panic]
                } else {
                    no_action()
                }
            }
        }
    }
}

pub struct FollowerRole {
    common: Common,
    leader_commit: LogIndex,
    last_applied: LogIndex,
}
impl FollowerRole {
    pub fn new(common: Common, last_applied: LogIndex) -> Self {
        FollowerRole {
            common: common,
            leader_commit: last_applied,
            last_applied: last_applied,
        }
    }
    pub fn handle_append_entries<T>(&mut self,
                                    sender: Sender,
                                    entries: Vec<LogEntry<T>>,
                                    leader_commit: LogIndex)
                                    -> Actions<T> {
        use std::cmp;
        // TODO: refactoring
        // NOTE: assert(sender.log_index => self.common.log_index_table.first_log_index())
        let mut actions = vec![];
        if self.leader_commit < leader_commit {
            self.leader_commit = leader_commit;
        }
        let mut applicable = self.last_applied;

        let latest_index = sender.log_index + entries.len() as u64;
        let latest_term = entries.last().map(|e| e.term).unwrap_or(sender.log_term);
        let reply = Message::AppendEntriesReply;
        if sender.log_index > self.common.last_log_index() {
            actions.push(self.common.reply(&sender, reply));
        } else if let Some(success) = self.common
            .log_index_table
            .truncate_if_needed(&sender, &entries) {
            if !success {
                // inconsistency detected
                actions.push(Action::Panic);
            } else if self.common.last_log_index() < self.last_applied {
                // inconsistency detected
                actions.push(Action::Panic);
            } else {
                actions.push(Action::LogTruncate(self.common.last_log_index()));
                actions.extend(self.handle_append_entries(sender, entries, leader_commit));
            }
        } else if latest_index <= self.common.last_log_index() {
            applicable = cmp::min(self.leader_commit, latest_index);
            actions.push(self.common.reply2(&sender, latest_index, latest_term, reply));
        } else {
            applicable = cmp::min(self.leader_commit, latest_index);
            let offset = (latest_index - self.common.last_log_index()) as usize;
            let entries = if offset == 0 {
                entries
            } else {
                entries.into_iter().skip(offset).collect()
            };
            self.common.log_index_table.extend(&entries);
            actions.push(Action::LogAppend(entries));
            actions.push(self.common.reply(&sender, reply));
        }
        if applicable > self.last_applied {
            actions.push(Action::LogCommit(applicable));
            self.last_applied = applicable;
        }
        actions
    }
}
impl<T> ConsensusRole<T> for FollowerRole {
    fn init(&mut self) -> Actions<T> {
        vec![Action::ResetTimeout]
    }
    fn handle_command(&mut self, _command: T) -> Result<(LogIndex, Actions<T>), Option<&NodeId>> {
        Err(self.common.voted_for.as_ref())
    }
    fn handle_change_config(&mut self, _new_config: Config) -> Result<Actions<T>, Option<&NodeId>> {
        Err(self.common.voted_for.as_ref())
    }
    fn handle_message(&mut self, sender: Sender, message: Message<T>) -> Actions<T> {
        self.common.handle_message(sender, message).unwrap_or_else(|(sender, message)| {
            if sender.log_index < self.common.log_index_table.first_log_index() {
                // NOTE: assume ...
                // TODO: AppendEntriesの場合、無視ではなく、
                // あるものとして応答した方が良いかも
                // (そうすればいつか共通部分に到達するはずなので、
                // そこで正当性の判断が行える)
                return no_action();
            }
            match message {
                Message::AppendEntriesCall { entries, leader_commit } => {
                    let mut actions = self.handle_append_entries(sender, entries, leader_commit);
                    actions.push(Action::ResetTimeout);
                    actions
                }
                Message::InstallSnapshotCall { config, offset, data, done } => {
                    let snapshot = Action::InstallSnapshot(offset,
                                                           data,
                                                           done,
                                                           sender.log_term,
                                                           sender.log_index,
                                                           config);
                    let reply = Message::InstallSnapshotReply;
                    vec![snapshot, self.common.reply(&sender, reply)]
                }
                _ => no_action(),
            }
        })
    }
    fn handle_event(&mut self, event: Event) -> Actions<T> {
        match event {
            Event::Timeout => vec![Action::to_candidate()],
            Event::SnapshotInstalled(first_log_index, first_log_term) => {
                if !self.common
                    .log_index_table
                    .handle_snapshot_installed(first_log_index, first_log_term) {
                    // inconsistent snapshot
                    vec![Action::Panic]
                } else {
                    no_action()
                }
            }
        }
    }
}

pub struct LeaderRole {
    common: Common,
    commit_index: LogIndex,
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,
    change_config_index: Option<LogIndex>,
}
impl LeaderRole {
    pub fn new(common: Common, commit_index: LogIndex) -> Self {
        let nodes = common.config.all_members().into_iter().cloned().collect::<Vec<_>>();
        let next_index = nodes.iter().map(|id| (id.clone(), common.last_log_index() + 1)).collect();
        let match_index = nodes.iter().map(|id| (id.clone(), 0)).collect();
        LeaderRole {
            common: common,
            commit_index: commit_index,
            next_index: next_index,
            match_index: match_index,
            change_config_index: None,
        }
    }
    pub fn update_config(&mut self, config: ConfigState) {
        let nodes = config.all_members().into_iter().cloned().collect::<Vec<_>>();
        let mut next_index: HashMap<_, _> =
            nodes.iter().map(|id| (id.clone(), self.common.last_log_index() + 1)).collect();
        let mut match_index: HashMap<_, _> = nodes.iter().map(|id| (id.clone(), 0)).collect();
        for (k, v) in &self.next_index {
            next_index.get_mut(k).map(|x| {
                *x = *v;
            });
        }
        for (k, v) in &self.match_index {
            match_index.get_mut(k).map(|x| {
                *x = *v;
            });
        }
        self.common.config = config;
        self.next_index = next_index;
        self.match_index = match_index;
    }
    pub fn make_append_entries<T>(&self) -> Actions<T> {
        self.common
            .config
            .all_members().into_iter()
            .map(|id| {
                let low_index = self.next_index[id];
                if low_index <= self.common.log_index_table.first_log_index() {
                    let mut sender = self.common.make_sender();
                    sender.log_index = low_index.checked_sub(1).unwrap();
                    // TODO: set snapshot term
                    // sender.log_term = self.common.log_index_table.get_term(self.log_index).unwrap();
                    Action::SendSnapshot(sender, id.clone())
                } else {
                    let mut sender = self.common.make_sender();
                    sender.log_index = low_index.checked_sub(1).unwrap();
                    sender.log_term = self.common.log_index_table.get_term(sender.log_index).unwrap();
                    let high_index = self.common.last_log_index();
                    Action::UnicastLog(sender, self.commit_index, id.clone(), low_index..high_index)
                }
            })
            .collect()
    }
    pub fn handle_append_entries_reply<T>(&mut self, sender: Sender) -> Actions<T> {
        // TODO: 巻き戻りを許容するかどうか
        // => 一旦は無視する(matchよりも前には戻れない)
        // (自動で齟齬を認識して構成変更、とかをやりたい)

        use std::cmp;

        let match_index = cmp::max(*self.match_index.get(&sender.id).unwrap(), sender.log_index);
        self.match_index.insert(sender.id.clone(), match_index);
        self.next_index.insert(sender.id.clone(), match_index + 1);

        let old_commmit = self.commit_index;
        self.update_commit_index();
        let mut actions = Vec::new();
        if old_commmit < self.commit_index {
            actions.push(Action::LogCommit(self.commit_index));
        }

        if self.change_config_index.map_or(false, |i| i <= self.commit_index) {
            // XXX: 簡易実装
            self.change_config_index = None;
            if let Some(config) = self.common.config.next() {
                // TODO: 共通化
                let entries = vec![LogEntry::config(self.common.term, &config)];
                self.update_config(config);
                self.common.log_index_table.extend(&entries);
                let index = self.common.last_log_index();
                self.change_config_index = Some(index);

                actions.push(Action::LogAppend(entries));
                actions.extend(self.make_append_entries());
                actions.push(Action::ResetTimeout);
            }
        }

        actions
    }
    fn update_commit_index(&mut self) {
        use std::cmp;

        let mut min = self.common.last_log_index();
        for c in self.common.config.votable_configs() {
            for id in &c.members {
                min = cmp::min(min, self.match_index[id]);
            }
        }
        if min > self.commit_index &&
           self.common.log_index_table.get_term(min) == Some(self.common.term) {
            self.commit_index = min;
        }
    }
}
impl<T> ConsensusRole<T> for LeaderRole {
    fn init(&mut self) -> Actions<T> {
        let entry = LogEntry::noop(self.common.term);
        let call = Message::AppendEntriesCall {
            entries: vec![entry],
            leader_commit: self.commit_index,
        };
        let message = Action::BroadcastMsg(self.common.make_sender(), call);

        let entries = vec![LogEntry::noop(self.common.term)];
        self.common.log_index_table.extend(&entries);
        vec![Action::LogAppend(entries), message, Action::ResetTimeout]
    }
    fn handle_command(&mut self, command: T) -> Result<(LogIndex, Actions<T>), Option<&NodeId>> {
        let entries = vec![LogEntry::command(self.common.term, command)];
        self.common.log_index_table.extend(&entries);
        let index = self.common.last_log_index();

        let mut actions = vec![Action::LogAppend(entries)];
        actions.extend(self.make_append_entries());
        actions.push(Action::ResetTimeout);
        Ok((index, actions))
    }
    fn handle_change_config(&mut self, new_config: Config) -> Result<Actions<T>, Option<&NodeId>> {
        let config = self.common.config.update(new_config);
        let entries = vec![LogEntry::config(self.common.term, &config)];
        self.update_config(config);
        self.common.log_index_table.extend(&entries);
        let index = self.common.last_log_index();
        self.change_config_index = Some(index);

        let mut actions = vec![Action::LogAppend(entries)];
        actions.extend(self.make_append_entries());
        actions.push(Action::ResetTimeout);
        Ok(actions)
    }
    fn handle_message(&mut self, sender: Sender, message: Message<T>) -> Actions<T> {
        self.common.handle_message(sender, message).unwrap_or_else(|(sender, message)| {
            match message {
                Message::AppendEntriesReply => self.handle_append_entries_reply(sender),
                _ => no_action(),
            }
        })
    }
    fn handle_event(&mut self, event: Event) -> Actions<T> {
        match event {
            Event::Timeout => {
                let mut actions = self.make_append_entries();
                actions.push(Action::ResetTimeout);
                actions
            }
            Event::SnapshotInstalled(first_log_index, first_log_term) => {
                if !self.common
                    .log_index_table
                    .handle_snapshot_installed(first_log_index, first_log_term) {
                    // inconsistent snapshot
                    vec![Action::Panic]
                } else {
                    no_action()
                }
            }
        }
    }
}
