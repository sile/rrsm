use std::cmp;
use std::collections::HashSet;
use std::collections::HashMap;
use std::marker::PhantomData;
use super::super::*;
use super::Action;
use super::Message;
use super::MessageHeader;
use super::MessageData;

pub struct CommonState {
    pub node: Node,
    pub ballot: election::Ballot,
    pub config: config::Config,
    pub index_table: log::IndexTable,
    pub timestamp: super::Timestamp,
    pub last_acked_timestamp: super::Timestamp,
    pub generations: HashMap<NodeId, NodeGeneration>,
    pub in_lease_priod: bool,
}
impl CommonState {
    pub fn new(node: Node,
               ballot: election::Ballot,
               config: config::Config,
               index_table: log::IndexTable)
               -> Self {
        CommonState {
            node: node,
            ballot: ballot,
            config: config,
            index_table: index_table,
            // TODO: timestamp_acksもこの層で管理する
            timestamp: 0,
            last_acked_timestamp: 0,
            generations: HashMap::new(),
            in_lease_priod: false,
        }
    }
    pub fn make_header(&mut self) -> MessageHeader {
        let timestamp = self.timestamp;
        self.timestamp += timestamp.checked_add(1).unwrap();
        MessageHeader {
            from: self.node.clone(),
            term: self.ballot.term,
            version: self.index_table.last_appended(),
            // TODO: reply時にはもともとの値を引き継ぐ
            timestamp: timestamp,
        }
    }
    pub fn make_reply_header(&mut self, from: &MessageHeader) -> MessageHeader {
        let mut header = self.make_header();
        header.timestamp = from.timestamp;
        header
    }
    pub fn handle_message<M>
        (&mut self,
         message: Message<M>,
         mut state: Box<ConsensusState<M>>)
         -> Result<(Box<ConsensusState<M>>, Vec<Action<M>>), (Message<M>, Box<ConsensusState<M>>)>
        where M: Machine + 'static
    {
        {
            let from = &message.header.from;
            if !self.config.is_member(&from.id) &&
               self.ballot.voted_for.as_ref() != Some(&from.id) {
                return Ok((state, vec![]));
            }
            if !self.generations.contains_key(&from.id) {
                self.generations.insert(from.id.clone(), from.generation);
            }
            if *self.generations.get(&from.id).unwrap() != from.generation {
                // The peer have been restarted
                state.clear_node_state(self, &from.id);
            }
        }

        let can_vote = !self.in_lease_priod || self.ballot.voted_for.is_none();
        if can_vote && message.header.term > self.ballot.term {
            self.ballot.term = message.header.term;
            self.ballot.voted_for = None;
            let (next, mut actions) = Box::new(FollowerState::new()).init(self);
            actions.extend(vec![Action::SaveBallot(self.ballot.clone()),
                                Action::Postpone(message)]);
            Ok((next, actions))
        } else if message.header.term < self.ballot.term {
            match message.data {
                MessageData::RequestVoteCall => {
                    let header = self.make_reply_header(&message.header);
                    let reply = Message::make_request_vote_reply(header, false);
                    Ok((state, vec![Action::UnicastMsg(message.header.from.id, reply)]))
                }
                MessageData::AppendEntriesCall { .. } => {
                    let header = self.make_reply_header(&message.header);
                    let reply = Message::make_append_entries_reply(header);
                    Ok((state, vec![Action::UnicastMsg(message.header.from.id, reply)]))
                }
                MessageData::InstallSnapshotCall { .. } => {
                    let header = self.make_reply_header(&message.header);
                    let reply = Message::make_install_snapshot_reply(header);
                    Ok((state, vec![Action::UnicastMsg(message.header.from.id, reply)]))
                }
                _ => Ok((state, vec![])),
            }
        } else {
            // NOTE: message.header.term == self.ballot.term
            match message.data {
                MessageData::RequestVoteCall => {
                    let up_to_date = message.header.version >= self.index_table.last_appended();
                    if !up_to_date || self.ballot.voted_for.is_some() {
                        let header = self.make_reply_header(&message.header);
                        let grant = up_to_date &&
                                    self.ballot.voted_for.as_ref().unwrap() ==
                                    &message.header.from.id;
                        let reply = Message::make_request_vote_reply(header, grant);
                        Ok((state, vec![Action::UnicastMsg(message.header.from.id, reply)]))
                    } else {
                        self.ballot.vote(message.header.from.id.clone());
                        let (next, mut actions) = Box::new(FollowerState::new()).init(self);
                        actions.push(Action::SaveBallot(self.ballot.clone()));
                        actions.push(Action::Postpone(message));
                        Ok((next, actions))
                    }
                }
                MessageData::AppendEntriesCall { .. } => {
                    if self.ballot.voted_for.as_ref() == Some(&message.header.from.id) {
                        // maybe a follower
                        Err((message, state))
                    } else {
                        // leader is elected: maybe a candidate
                        self.ballot.vote(message.header.from.id.clone());
                        let (next, mut actions) = Box::new(FollowerState::new()).init(self);
                        actions.push(Action::Postpone(message));
                        Ok((next, actions))
                    }
                }
                _ => Err((message, state)),
            }
        }
    }
}

pub trait ConsensusState<M>
    where M: Machine
{
    fn init(self: Box<Self>, common: &mut CommonState) -> (Box<ConsensusState<M>>, Vec<Action<M>>);
    fn handle_timeout(self: Box<Self>,
                      common: &mut CommonState)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>);
    fn handle_message(self: Box<Self>,
                      common: &mut CommonState,
                      message: Message<M>)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>);
    fn sync(self: Box<Self>, common: &mut CommonState) -> (Box<ConsensusState<M>>, Vec<Action<M>>);
    fn propose(self: Box<Self>,
               common: &mut CommonState,
               entry: log::Entry<M::Command>)
               -> (Box<ConsensusState<M>>, Option<Vec<Action<M>>>);
    fn is_leader(&self) -> bool;
    fn clear_node_state(&mut self, common: &mut CommonState, node_id: &NodeId);
}

pub struct FollowerState<M> {
    leader_commit: log::Index,
    follower_commit: log::Index,
    _machine: PhantomData<M>,
}
impl<M> FollowerState<M>
    where M: Machine + 'static
{
    pub fn new() -> Self {
        FollowerState {
            leader_commit: 0,
            follower_commit: 0,
            _machine: PhantomData,
        }
    }
    pub fn handle_append_entries(mut self: Box<Self>,
                                 common: &mut CommonState,
                                 header: MessageHeader,
                                 entries: Vec<log::Entry<M::Command>>,
                                 leader_commit: log::Index)
                                 -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        let mut actions = vec![Action::ResetTimeout(super::TimeoutKind::Mid)];
        common.in_lease_priod = true;

        self.leader_commit = cmp::max(self.leader_commit, leader_commit);

        let leader_prev = header.version.clone();
        let leader_first = entries.first().map_or(leader_prev.clone(), |e| {
            log::EntryVersion::new(e.term, leader_prev.index + 1)
        });
        let leader_last = entries.last().map_or(leader_first.clone(), |e| {
            log::EntryVersion::new(e.term, leader_prev.index + entries.len() as log::Index)
        });

        let last_appended = common.index_table.last_appended();
        let last_match = common.index_table.find_last_match(&leader_prev, &entries);
        let entries = if let Some(last_match) = last_match {
            if last_match == leader_last {
                // follower >= leader
                vec![]
            } else {
                let offset = last_match.index - leader_prev.index;
                println!("==== IN: len={}, offset={}, commit={}",
                         entries.len(),
                         offset,
                         leader_commit);
                common.index_table.truncate(leader_first.index + offset - 1);
                if offset == 0 {
                    entries
                } else {
                    entries.into_iter().skip(offset as usize).collect()
                }
            }
        } else {
            // follower < leader
            common.index_table.truncate(leader_prev.index);
            vec![]
        };

        if common.index_table.last_appended().index < last_appended.index {
            actions.push(Action::LogRollback(leader_last.index + 1 - entries.len() as log::Index));
        }
        if !entries.is_empty() {
            common.index_table.extend(&entries);
            actions.push(Action::LogAppend(entries));
        }

        let reply_action =
                       Action::UnicastMsg(header.from.id.clone(),
                                          Message::make_append_entries_reply(
                                              common.make_reply_header(&header)));
        actions.push(reply_action);

        let commitable = cmp::min(self.leader_commit, common.index_table.last_appended().index);
        if self.follower_commit < commitable {
            self.follower_commit = commitable;
            if let Some(version) = common.index_table.get(commitable) {
                actions.push(Action::LogApply(version));
            }
        }
        (self, actions)
    }
}
impl<M> ConsensusState<M> for FollowerState<M>
    where M: Machine + 'static
{
    fn init(self: Box<Self>, common: &mut CommonState) -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        // common.ballot.voted_for = None;
        common.in_lease_priod = true;
        (self, vec![Action::ResetTimeout(super::TimeoutKind::Mid)])
    }
    fn handle_timeout(self: Box<Self>,
                      common: &mut CommonState)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        if common.in_lease_priod {
            common.in_lease_priod = false;
            (self, vec![Action::ResetTimeout(super::TimeoutKind::Mid)])
        } else {
            Box::new(CandidateState::new()).init(common)
        }
    }
    fn handle_message(self: Box<Self>,
                      common: &mut CommonState,
                      message: Message<M>)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        match message.data {
            MessageData::AppendEntriesCall { entries, leader_commit } => {
                self.handle_append_entries(common, message.header, entries, leader_commit)
            }
            MessageData::InstallSnapshotCall { snapshot } => {
                (self, vec![Action::InstallSnapshot(message.header.from.id, snapshot)])
            }
            _ => (self, vec![]),
        }
    }
    fn sync(self: Box<Self>,
            _common: &mut CommonState)
            -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        // TODO: non-leaderの場合は、leaderが決定するまで待っても良いかも
        // リーダが決まるまで待つ、的なニュアンスにする?
        (self, vec![])
    }
    fn propose(self: Box<Self>,
               _common: &mut CommonState,
               _entry: log::Entry<M::Command>)
               -> (Box<ConsensusState<M>>, Option<Vec<Action<M>>>) {
        (self, None)
    }
    fn is_leader(&self) -> bool {
        false
    }
    fn clear_node_state(&mut self, _common: &mut CommonState, _node_id: &NodeId) {}
}

pub struct CandidateState<M> {
    vote_granted: HashSet<NodeId>,
    _machine: PhantomData<M>,
}
impl<M> CandidateState<M> {
    pub fn new() -> Self {
        CandidateState {
            vote_granted: HashSet::new(),
            _machine: PhantomData,
        }
    }
    pub fn is_elected(&self, common: &CommonState) -> bool {
        for c in common.config.iter_votable_cluster() {
            let votes = c.members.iter().filter(|&n| self.vote_granted.contains(n)).count();
            if votes <= c.members.len() / 2 {
                return false;
            }
        }
        true
    }
}
impl<M> ConsensusState<M> for CandidateState<M>
    where M: Machine + 'static
{
    fn init(mut self: Box<Self>,
            common: &mut CommonState)
            -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        common.in_lease_priod = false;
        common.ballot.increment_term();
        common.ballot.vote(common.node.id.clone());
        self.vote_granted.insert(common.node.id.clone());

        let mut actions = vec![Action::SaveBallot(common.ballot.clone())];
        if self.is_elected(common) {
            let (state, leader_actions) = Box::new(LeaderState::new()).init(common);
            actions.extend(leader_actions);
            (state, actions)
        } else {
            let call = Message::make_request_vote_call(common.make_header());
            actions.push(Action::ResetTimeout(super::TimeoutKind::Random));
            actions.push(Action::BroadcastMsg(call));
            (self, actions)
        }
    }
    fn handle_timeout(self: Box<Self>,
                      common: &mut CommonState)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        Box::new(CandidateState::new()).init(common)
    }
    fn handle_message(mut self: Box<Self>,
                      common: &mut CommonState,
                      message: Message<M>)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        match message.data {
            MessageData::RequestVoteReply { vote_granted } if vote_granted => {
                self.vote_granted.insert(message.header.from.id);
                if self.is_elected(common) {
                    Box::new(LeaderState::new()).init(common)
                } else {
                    (self, vec![])
                }
            }
            _ => (self, vec![]),
        }
    }
    fn sync(self: Box<Self>,
            _common: &mut CommonState)
            -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        // TODO: non-leaderの場合は、leaderが決定するまで待っても良いかも
        (self, vec![])
    }
    fn propose(self: Box<Self>,
               _common: &mut CommonState,
               _entry: log::Entry<M::Command>)
               -> (Box<ConsensusState<M>>, Option<Vec<Action<M>>>) {
        (self, None)
    }
    fn is_leader(&self) -> bool {
        false
    }
    fn clear_node_state(&mut self, _common: &mut CommonState, node_id: &NodeId) {
        self.vote_granted.remove(node_id);
    }
}

pub struct LeaderState<M> {
    commit_index: log::Index,
    update_config_index: Option<log::Index>,
    next_index: HashMap<NodeId, log::Index>,
    match_index: HashMap<NodeId, log::Index>,
    timestamp_acks: HashMap<NodeId, super::Timestamp>,
    _machine: PhantomData<M>,
}
impl<M> LeaderState<M>
    where M: Machine + 'static
{
    pub fn new() -> Self {
        LeaderState {
            commit_index: 0,
            update_config_index: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            timestamp_acks: HashMap::new(),
            _machine: PhantomData,
        }
    }
    fn handle_update_config(&mut self, common: &mut CommonState) {
        let mut nodes = common.config.all_members().into_iter().cloned().collect::<Vec<_>>();
        if common.config.is_member(&common.node.id) {
            nodes.push(common.node.id.clone());
        }

        let last_appended = common.index_table.last_appended().index;
        let next_index = nodes.iter()
            .map(|id| {
                let index = *self.next_index.get(id).unwrap_or(&(last_appended + 1));
                (id.clone(), index)
            })
            .collect();
        let match_index = nodes.iter()
            .map(|id| {
                let index = *self.match_index.get(id).unwrap_or(&0);
                (id.clone(), index)
            })
            .collect();
        let timestamp_acks = nodes.iter()
            .map(|id| {
                let t = *self.timestamp_acks.get(id).unwrap_or(&common.last_acked_timestamp);
                (id.clone(), t)
            })
            .collect();
        self.next_index = next_index;
        self.match_index = match_index;
        self.timestamp_acks = timestamp_acks;
    }
    fn make_append_entries(&mut self, common: &mut CommonState) -> Vec<Action<M>> {
        let header = common.make_header();
        let end_index = common.index_table.last_appended().index + 1;
        *self.next_index.get_mut(&common.node.id).unwrap() = end_index;
        *self.match_index.get_mut(&common.node.id).unwrap() = end_index - 1;
        *self.timestamp_acks.get_mut(&common.node.id).unwrap() = header.timestamp;
        common.config
            .all_members()
            .into_iter()
            .filter(|id| **id != common.node.id)
            .map(|id| {
                let start_index = self.next_index[id];
                let mut header = header.clone();
                let dummy_term = 0; // XXX: for snapshot
                header.version.index = start_index - 1;
                header.version.term = common.index_table
                    .get(start_index - 1)
                    .map_or(dummy_term, |x| x.term);
                println!("append: {}: first={}: {:?}", id, start_index, header);
                Action::UnicastLog {
                    destination: id.clone(),
                    header: header,
                    first_index: start_index,
                    max_len: (end_index - start_index) as usize,
                }
            })
            .collect()
    }
    fn handle_append_entries_reply(mut self: Box<Self>,
                                   common: &mut CommonState,
                                   header: MessageHeader)
                                   -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        // TODO: peerのversionが自分のログ内に存在するかの確認は必要
        // 存在しないなら乖離しているので、peer.index-1までnextを戻す
        let from = &header.from;
        let match_index = cmp::max(*self.match_index.get(&header.from.id).unwrap(),
                                   header.version.index);
        let timestamp = cmp::max(*self.timestamp_acks.get(&header.from.id).unwrap(),
                                 header.timestamp);
        *self.match_index.get_mut(&from.id).unwrap() = match_index;
        *self.next_index.get_mut(&from.id).unwrap() = match_index + 1;
        *self.timestamp_acks.get_mut(&from.id).unwrap() = timestamp;

        let mut actions = Vec::new();
        if self.update_commit_index(common) {
            let version = common.index_table.get(self.commit_index).unwrap();
            actions.push(Action::LogApply(version));
        }
        if timestamp == header.timestamp {
            common.last_acked_timestamp = self.last_agreed_timestamp(common);
        }

        if self.has_nonvotable_cluster(common) {
            return (self, actions);
        }
        if self.update_config_index.map_or(false, |i| i <= self.commit_index) {
            self.update_config_index = None;
            if let Some(config) = common.config.next_phase() {
                common.config = config.clone();
                let entry = log::Entry::config(common.ballot.term, config);
                let (next, propose_actions) = self.propose(common, entry);
                actions.extend(propose_actions.unwrap());
                return (next, actions);
            }
        }
        (self, actions)
    }
    fn update_commit_index(&mut self, common: &CommonState) -> bool {
        let mut min = common.index_table.last_appended().index;
        for c in common.config.iter_votable_cluster() {
            let mut matches = c.members.iter().map(|id| self.match_index[id]).collect::<Vec<_>>();
            matches.sort();
            min = cmp::min(min, matches[c.members.len() / 2]);
        }
        if min != self.commit_index &&
           common.index_table.get(min).map(|x| x.term) == Some(common.ballot.term) {
            self.commit_index = min;
            true
        } else {
            false
        }
    }
    fn has_nonvotable_cluster(&self, common: &CommonState) -> bool {
        common.config.iter_nonvotable_cluster().any(|c| {
            let up_to_dates = c.members
                .iter()
                .filter(|id| self.match_index[*id] >= self.update_config_index.unwrap())
                .count();
            up_to_dates <= c.members.len()
        })
    }
    fn last_agreed_timestamp(&self, common: &CommonState) -> super::Timestamp {
        let mut last = common.last_acked_timestamp;
        for c in common.config.iter_votable_cluster() {
            let mut acks = c.members.iter().map(|id| self.timestamp_acks[id]).collect::<Vec<_>>();
            acks.sort();
            last = cmp::min(last, acks[c.members.len() / 2]);
        }
        last
    }
}
impl<M> ConsensusState<M> for LeaderState<M>
    where M: Machine + 'static
{
    fn init(mut self: Box<Self>,
            common: &mut CommonState)
            -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        common.in_lease_priod = false;
        self.handle_update_config(common);

        let entries = vec![log::Entry::noop(common.ballot.term)];
        common.index_table.extend(&entries);

        let mut actions = vec![Action::ResetTimeout(super::TimeoutKind::Min),
                               Action::LogAppend(entries)];
        actions.extend(self.make_append_entries(common));
        (self, actions)
    }
    fn handle_timeout(mut self: Box<Self>,
                      common: &mut CommonState)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        let mut actions = self.make_append_entries(common);
        actions.push(Action::ResetTimeout(super::TimeoutKind::Min));
        (self, actions)
    }
    fn handle_message(self: Box<Self>,
                      common: &mut CommonState,
                      message: Message<M>)
                      -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        match message.data {
            MessageData::AppendEntriesReply => {
                self.handle_append_entries_reply(common, message.header)
            }
            _ => (self, vec![]),
        }
    }
    fn propose(mut self: Box<Self>,
               common: &mut CommonState,
               entry: log::Entry<M::Command>)
               -> (Box<ConsensusState<M>>, Option<Vec<Action<M>>>) {
        let is_config = if let log::Entry { data: log::Data::Config(_), .. } = entry {
            true
        } else {
            false
        };
        let entries = vec![entry];
        common.index_table.extend(&entries);

        if is_config {
            self.update_config_index = Some(common.index_table.last_appended().index);
            self.handle_update_config(common);
        }

        let mut actions = vec![Action::ResetTimeout(super::TimeoutKind::Min),
                               Action::LogAppend(entries)];
        actions.extend(self.make_append_entries(common));
        (self, Some(actions))
    }
    fn sync(mut self: Box<Self>,
            common: &mut CommonState)
            -> (Box<ConsensusState<M>>, Vec<Action<M>>) {
        let mut actions = vec![Action::ResetTimeout(super::TimeoutKind::Min)];
        actions.extend(self.make_append_entries(common));
        (self, actions)
    }
    fn clear_node_state(&mut self, common: &mut CommonState, node_id: &NodeId) {
        let last_appended = common.index_table.last_appended().index;
        *self.next_index.get_mut(node_id).unwrap() = last_appended;
        *self.match_index.get_mut(node_id).unwrap() = 0;
        *self.timestamp_acks.get_mut(node_id).unwrap() = common.last_acked_timestamp;
    }
    fn is_leader(&self) -> bool {
        true
    }
}
