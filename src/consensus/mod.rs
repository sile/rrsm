use super::*;

mod state;

#[derive(Clone,Debug)]
pub struct MessageHeader {
    pub from: Node,
    pub term: election::Term,
    pub version: log::EntryVersion,
    pub timestamp: Timestamp,
}

pub enum MessageData<M>
    where M: Machine
{
    RequestVoteCall,
    RequestVoteReply {
        vote_granted: bool,
    },
    AppendEntriesCall {
        entries: Vec<log::Entry<M::Command>>,
        leader_commit: log::Index,
    },
    AppendEntriesReply,
    InstallSnapshotCall {
        snapshot: replicator::Snapshot<M>,
    },
    InstallSnapshotReply,
}
impl<M> MessageData<M>
    where M: Machine
{
    pub fn kind(&self) -> &'static str {
        match *self {
            MessageData::RequestVoteCall => "RequestVoteCall",
            MessageData::RequestVoteReply { .. } => "RequestVoteReply",
            MessageData::AppendEntriesCall { .. } => "AppendEntriesCall",
            MessageData::AppendEntriesReply => "AppendEntriesReply",
            MessageData::InstallSnapshotCall { .. } => "InstallSnapshotCall",
            MessageData::InstallSnapshotReply => "InstallSnapshotReply",
        }
    }
}
impl<M> Clone for MessageData<M>
    where M: Machine,
          M::Command: Clone
{
    fn clone(&self) -> Self {
        match *self {
            MessageData::RequestVoteCall => MessageData::RequestVoteCall,
            MessageData::RequestVoteReply { vote_granted } => {
                MessageData::RequestVoteReply { vote_granted: vote_granted }
            }
            MessageData::AppendEntriesCall { ref entries, leader_commit } => {
                MessageData::AppendEntriesCall {
                    entries: entries.clone(),
                    leader_commit: leader_commit,
                }
            }
            MessageData::AppendEntriesReply => MessageData::AppendEntriesReply,
            MessageData::InstallSnapshotCall { .. } => panic!(),
            MessageData::InstallSnapshotReply => MessageData::InstallSnapshotReply,
        }
    }
}

pub struct Message<M>
    where M: Machine
{
    pub header: MessageHeader,
    pub data: MessageData<M>,
}
impl<M> Clone for Message<M>
    where M: Machine,
          M::Command: Clone
{
    fn clone(&self) -> Self {
        Message {
            header: self.header.clone(),
            data: self.data.clone(),
        }
    }
}
impl<M> Message<M>
    where M: Machine
{
    pub fn make_append_entries_call(header: MessageHeader,
                                    entries: Vec<log::Entry<M::Command>>,
                                    leader_commit: log::Index)
                                    -> Self {
        Message {
            header: header,
            data: MessageData::AppendEntriesCall {
                entries: entries,
                leader_commit: leader_commit,
            },
        }
    }
    pub fn make_install_snapshot_call(header: MessageHeader,
                                      snapshot: replicator::Snapshot<M>)
                                      -> Self {
        Message {
            header: header,
            data: MessageData::InstallSnapshotCall { snapshot: snapshot },
        }
    }
    pub fn make_request_vote_call(header: MessageHeader) -> Self {
        Message {
            header: header,
            data: MessageData::RequestVoteCall,
        }
    }
    pub fn make_install_snapshot_reply(header: MessageHeader) -> Self {
        Message {
            header: header,
            data: MessageData::InstallSnapshotReply,
        }
    }
    pub fn make_request_vote_reply(header: MessageHeader, vote_granted: bool) -> Self {
        Message {
            header: header,
            data: MessageData::RequestVoteReply { vote_granted: vote_granted },
        }
    }
    pub fn make_append_entries_reply(header: MessageHeader) -> Self {
        Message {
            header: header,
            data: MessageData::AppendEntriesReply,
        }
    }
}

pub struct ConsensusModule<M>
    where M: Machine
{
    common_state: state::CommonState,
    state: Option<Box<state::ConsensusState<M>>>,
}
impl<M> ConsensusModule<M>
    where M: Machine + 'static
{
    pub fn new(node_id: &NodeId,
               ballot: &election::Ballot,
               config: &config::Config,
               table: log::IndexTable)
               -> Self {
        let node = Node::new(node_id);
        let common = state::CommonState::new(node, ballot.clone(), config.clone(), table);
        ConsensusModule {
            common_state: common,
            state: Some(Box::new(state::FollowerState::new())),
        }
    }
    pub fn init(&mut self) -> Vec<Action<M>> {
        let (state, actions) = self.state.take().unwrap().init(&mut self.common_state);
        self.state = Some(state);
        actions
    }
    pub fn handle_timeout(&mut self) -> Vec<Action<M>> {
        let (state, actions) = self.state.take().unwrap().handle_timeout(&mut self.common_state);
        self.state = Some(state);
        actions
    }
    pub fn handle_message(&mut self, message: Message<M>) -> Vec<Action<M>> {
        let state = self.state.take().unwrap();
        let (state, actions) = self.common_state
            .handle_message(message, state)
            .unwrap_or_else(|(message, state)| {
                state.handle_message(&mut self.common_state, message)
            });
        self.state = Some(state);
        actions
    }
    pub fn sync(&mut self) -> Vec<Action<M>> {
        let (state, actions) = self.state.take().unwrap().sync(&mut self.common_state);
        self.state = Some(state);
        actions
    }
    pub fn propose(&mut self, command: M::Command) -> Option<Vec<Action<M>>> {
        let entry = log::Entry::command(self.common_state.ballot.term, command);
        let (state, actions) = self.state.take().unwrap().propose(&mut self.common_state, entry);
        self.state = Some(state);
        actions
    }
    pub fn update_config(&mut self, config: config::Config) -> Option<Vec<Action<M>>> {
        let config = self.common_state.config.merge_config(config);
        self.common_state.config = config.clone();
        let entry = log::Entry::config(self.common_state.ballot.term, config);
        let (state, actions) = self.state.take().unwrap().propose(&mut self.common_state, entry);
        self.state = Some(state);
        actions
    }
    pub fn handle_snapshot_installed(&mut self,
                                     node_id: Option<NodeId>,
                                     config: config::Config,
                                     last_included: &log::EntryVersion)
                                     -> Vec<Action<M>> {
        if self.common_state.index_table.last_appended().index < last_included.index {
            self.common_state.config = config;
        }
        self.common_state.index_table.drop_until(last_included.clone());

        // TODO: consistency check

        if let Some(node_id) = node_id {
            // TODO: use sender's timestamp
            let reply = Message::make_install_snapshot_reply(self.common_state.make_header());
            vec![Action::UnicastMsg(node_id, reply)]
        } else {
            vec![]
        }
    }
    pub fn leader(&self) -> Option<&NodeId> {
        if self.state.as_ref().unwrap().is_leader() {
            Some(&self.common_state.node.id)
        } else {
            self.common_state.ballot.voted_for.as_ref()
        }
    }
    pub fn timestamp(&self) -> Timestamp {
        self.common_state.last_acked_timestamp
    }
    pub fn config(&self) -> &config::Config {
        &self.common_state.config
    }
    pub fn node(&self) -> &Node {
        &self.common_state.node
    }
}

pub enum Action<M>
    where M: Machine
{
    ResetTimeout(TimeoutKind),
    Postpone(Message<M>),
    BroadcastMsg(Message<M>),
    UnicastMsg(NodeId, Message<M>),
    UnicastLog {
        destination: NodeId,
        header: MessageHeader,
        first_index: log::Index,
        max_len: usize,
    },
    InstallSnapshot(NodeId, replicator::Snapshot<M>),
    SaveBallot(election::Ballot),
    LogAppend(Vec<log::Entry<M::Command>>),
    LogRollback(log::Index),
    LogApply(log::EntryVersion),
}

#[derive(Debug)]
pub enum TimeoutKind {
    Min,
    Mid,
    Max,
    Random,
}

pub type Timestamp = u64;
