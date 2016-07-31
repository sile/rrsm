use std::collections::HashMap;

use super::*;

pub struct Message<T>(pub T);

pub enum Error<S> {
    NotMember,
    Storage(S),
    Consensus(raft::Error),
}
impl<S> From<raft::Error> for Error<S> {
    fn from(e: raft::Error) -> Self {
        Error::Consensus(e)
    }
}

pub type Result<T, S> = ::std::result::Result<T, Error<S>>;

pub struct ConsensusModule;
impl ConsensusModule {
    pub fn new(id: &NodeId,
               ballot: &Ballot,
               config: &config::Config,
               table: io::LogIndexTable)
               -> Self {
        panic!()
    }
    pub fn config(&self) -> &config::Config {
        panic!()
    }
}

use std::collections::VecDeque;

pub struct Replicator<M, S, R>
    where M: Machine,
          R: io::Rpc<Message<M::Command>>
{
    id: NodeId,
    machine: M,
    state_version: Version,
    commit_version: Version,
    storage: S,
    rpc: R,
    consensus: Option<ConsensusModule>,
    token_gen: TokenGenerator,
    timer: Timer,
    event_queue: VecDeque<Event>,
    action_queue: VecDeque<Action<M>>,
    shutdown: bool,
    token_to_action: HashMap<Token, Action<M>>,
    index_to_token: HashMap<LogIndex, Token>,
}
impl<M, S, R> Replicator<M, S, R>
    where M: Machine,
          S: io::Storage<M>,
          R: io::Rpc<Message<M::Command>>
{
    pub fn new(id: &NodeId, storage: S, rpc: R, config: config::Builder) -> Result<Self, S::Error> {
        let config = config.build();
        if !config.is_member(id) {
            return Err(Error::NotMember);
        }

        let mut this = Self::default(id, storage, rpc);
        let ballot = Ballot::new(0);
        this.storage.log_truncate(0, this.token_gen.next());
        this.storage.log_append(&[io::LogEntry::noop(ballot.term)], this.token_gen.next());

        let snapshot = this.machine.take_snapshot();
        let metadata = io::SnapshotMetadata::new(&this.commit_version, &config);
        this.storage.save_snapshot(metadata, snapshot, this.token_gen.next());
        this.storage.save_ballot(&ballot, this.token_gen.next());
        try!(this.storage.flush().map_err(Error::Storage));

        let log_index_table = this.storage.build_log_table();
        this.consensus = Some(ConsensusModule::new(id, &ballot, &config, log_index_table));
        Ok(this)
    }
    pub fn load(id: &NodeId, storage: S, rpc: R) -> Result<Option<Self>, S::Error> {
        let mut this = Self::default(id, storage, rpc);

        try!(this.storage.flush().map_err(Error::Storage));
        this.storage.load_ballot(this.token_gen.next());
        let last_ballot =
            match try!(this.storage.run_once(false).unwrap().1.map_err(Error::Storage)) {
                io::StorageData::NotFound => return Ok(None),
                io::StorageData::Ballot(ballot) => ballot,
                _ => unreachable!(),
            };

        this.storage.load_snapshot(this.token_gen.next());
        let (metadata, snapshot) =
            match try!(this.storage.run_once(false).unwrap().1.map_err(Error::Storage)) {
                io::StorageData::NotFound => return Ok(None),
                io::StorageData::Snapshot { metadata, snapshot } => (metadata, snapshot),
                _ => unreachable!(),
            };
        this.storage.log_drop_until(metadata.last.log_index + 1, this.token_gen.next());
        try!(this.storage.flush().map_err(Error::Storage));

        let log_index_table = this.storage.build_log_table();
        this.consensus =
            Some(ConsensusModule::new(id, &last_ballot, &metadata.config, log_index_table));
        this.machine = snapshot.into();
        Ok(Some(this))
    }
    pub fn run_once(&mut self) -> Result<Option<Event>, S::Error> {
        self.check_timer();
        self.check_mailbox();
        try!(self.storage_run_once());
        if let Some(action) = self.action_queue.pop_front() {
            try!(self.handle_action(action));
        }
        Ok(self.event_queue.pop_front())
    }
    pub fn sync(&mut self) -> Token {
        let token = self.token_gen.next();
        self.action_queue.push_back(Action::Sync(token.clone()));
        token
    }
    pub fn propose(&mut self, proposal: M::Command) -> Token {
        let token = self.token_gen.next();
        self.action_queue
            .push_back(Action::ExecuteCommand(token.clone(), proposal));
        token
    }
    pub fn update_config(&mut self, config: config::Builder) -> Token {
        let token = self.token_gen.next();
        self.action_queue.push_back(Action::UpdateConfig(token.clone(), config.build()));
        token
    }
    pub fn take_snapshot(&mut self) -> Token {
        // NOTE: high priority
        let token = self.token_gen.next();
        let metadata = io::SnapshotMetadata::new(self.state_version(),
                                                 self.consensus.as_ref().unwrap().config());
        let snapshot = self.machine.take_snapshot();
        self.action_queue.push_front(Action::InstallSnapshot(token.clone(),
                                                             IsFsm::SnapshotSave(metadata,
                                                                                 snapshot)));
        token
    }
    pub fn shutdown(mut self) -> Result<(), S::Error> {
        self.shutdown = true;
        while self.queue_len() > 0 {
            try!(self.run_once());
        }
        Ok(())
    }
    pub fn queue_len(&self) -> usize {
        self.event_queue.len() + self.action_queue.len()
    }
    pub fn node_id(&self) -> &NodeId {
        &self.id
    }
    pub fn state(&self) -> &M {
        &self.machine
    }
    pub fn state_version(&self) -> &Version {
        &self.state_version
    }

    fn default(id: &NodeId, storage: S, rpc: R) -> Self {
        Replicator {
            id: id.clone(),
            machine: M::default(),
            state_version: Version::new(0, 0),
            commit_version: Version::new(0, 0),
            storage: storage,
            rpc: rpc,
            consensus: None,
            token_gen: TokenGenerator::new(),
            timer: Timer::new(),
            event_queue: VecDeque::new(),
            action_queue: VecDeque::new(),
            shutdown: false,
            token_to_action: HashMap::new(),
            index_to_token: HashMap::new(),
        }
    }
    fn check_timer(&mut self) {
        if self.timer.is_elapsed() {
            self.timer.clear();
            // NOTE: timeouts are interaption event (i.e., higher priority)
            self.action_queue.push_front(Action::Timeout);
        }
    }
    fn check_mailbox(&mut self) {
        if self.shutdown {
            return;
        }
        if let Some(recv) = self.rpc.try_recv() {
            self.action_queue.push_back(Action::Recv(recv));
        }
    }
    fn storage_run_once(&mut self) -> Result<(), S::Error> {
        if let Some((token, result)) = self.storage.run_once(true) {
            let data = try!(result.map_err(Error::Storage));
            self.action_queue.push_back(Action::Storage(token, data));
        }
        Ok(())
    }
    fn consensus_mut(&mut self) -> &mut ConsensusModule {
        self.consensus.as_mut().unwrap()
    }
    fn consensus(&self) -> &ConsensusModule {
        self.consensus.as_ref().unwrap()
    }
    // TODO: handle_reaction
    fn handle_action(&mut self, action: Action<M>) -> Result<(), S::Error> {
        let actions = match action {
            Action::Timeout => try!(self.consensus_mut().handle_timeout()),
            Action::Recv(message) => try!(self.consensus_mut().handle_message(message)),
            Action::Storage(token, data) => {
                // state-handling
                panic!()
            }
            Action::InstallSnapshot(token, IsFsm::SnapshotSave(metadata, snapshot)) => {
                //
            }
            Action::InstallSnapshot(token, IsFsm::LogDrop(index)) => {
                //
            }
            Action::InstallSnapshot(token, IsFsm::UpdateConsensus(index)) => {
                //
            }
            Action::Sync(token) => {
                // 1. 現在の論理時刻を取得
                //    - 論理時刻: leaderになってからのメッセージ送信数?
                // 2. それを載せてRPCを実行
                // 3. replyを監
                // 4. consensusがreplyを処理後に、
                //    leaderだと過半数に認められた最小時刻を取得
                // 5. それが1より大きくなったらクライアントに応答を返す
            }
            Action::UpdateConfig(token, config) => {
                //
            }
            Action::ExecuteCommand(token, command) => {
                //
            }
        };
        self.action_queue.extend(actions);
        Ok(())
    }
}

pub enum IsFsm<M, S> {
    SnapshotSave(M, S),
    LogDrop(LogIndex),
    UpdateConsensus(LogIndex),
}

pub enum Action<M>
    where M: Machine
{
    Timeout,
    Recv(Message<M::Command>),
    Storage(Token, io::StorageData<M>),
    InstallSnapshot(Token, IsFsm<io::SnapshotMetadata, M::Snapshot>),
    Sync(Token),
    UpdateConfig(Token, config::Config),
    ExecuteCommand(Token, M::Command),
}

pub enum Event {
    Done(Token),
    Aborted(Token),
    NotLeader(Option<NodeId>),
    ConfigChanged(config::Config),
    RoleChanged(NodeRole),
}

pub struct TokenGenerator {
    value: u64,
}
impl TokenGenerator {
    pub fn new() -> Self {
        TokenGenerator { value: 0 }
    }
    pub fn next(&mut self) -> Token {
        let v = self.value;
        self.value += 1;
        Token(v)
    }
}

pub struct Timer;
impl Timer {
    pub fn new() -> Self {
        Timer
    }
    pub fn is_elapsed(&self) -> bool {
        panic!()
    }
    pub fn clear(&mut self) {
        panic!()
    }
}
