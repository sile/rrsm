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
}

use std::collections::VecDeque;

pub struct Replicator<M, S, R> {
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
    action_queue: VecDeque<Action>,
    request_queue: VecDeque<Request>,
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
        panic!()
    }
    pub fn sync(&mut self) -> Token {
        panic!()
    }
    pub fn propose(&mut self, proposal: M::Command, precondition: Option<Version>) -> Token {
        panic!()
    }
    pub fn update_config(&mut self, config: config::Builder) -> Token {
        panic!()
    }
    pub fn take_snapshot(&mut self) -> Token {
        panic!()
    }
    pub fn cancel(&mut self) -> bool {
        panic!()
    }
    pub fn shutdown(self) -> Result<(), S::Error> {
        // don't receive new messages
        panic!()
    }
    pub fn queue_length(&self) -> usize {
        self.event_queue.len() + self.action_queue.len() + self.request_queue.len()
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
            request_queue: VecDeque::new(),
        }
    }
}

pub struct Action;
pub struct Request;

pub enum Event {
    Done(Token),
    Aborted(Token),
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
}
