use std::collections::HashSet;
use std::ops::Range;
use std::time::Duration;

pub mod raft_protocol;
pub mod sync;

pub mod tmp;

pub use self::raft_protocol::{NodeId, Term, LogIndex, LogEntry};

pub trait Machine: Default {
    type Command;
    type Snapshot: Into<Self>;
    fn execute(&mut self, command: Self::Command);
    fn take_snapshot(&self) -> Option<Self::Snapshot>;
}

#[derive(Clone)]
pub struct CommitVersion {
    pub term: Term,
    pub index: LogIndex,
}

pub type ClusterMembers = HashSet<NodeId>;

#[derive(Clone)]
pub struct Cluster {
    pub members: ClusterMembers,
    pub temporary_members: Option<Temporary>,
}
impl Cluster {
    pub fn all_members(&self) -> HashSet<NodeId> {
        // TODO: optimize
        let mut members = self.members.clone();
        match self.temporary_members {
            Some(Temporary::New(ref m)) => members.extend(m.clone()),
            Some(Temporary::Old(ref m)) => members.extend(m.clone()),
            _ => {}
        }
        members
    }
}

#[derive(Clone)]
pub enum Temporary {
    New(ClusterMembers),
    Old(ClusterMembers),
}

#[derive(Clone)]
pub struct Config {
    pub cluster: Cluster,
    pub election_timeout: Range<Duration>,
}

pub struct CommitedCommand<T> {
    version: CommitVersion,
    command: T,
}

pub enum NodeRole {
    Follower {
        voted_for: Option<NodeId>,
    },
    Candidate,
    Leader,
}

pub struct State<M> {
    context: Context,
    machine: M,
}
impl<M> State<M>
    where M: Machine
{
    pub fn execute(&mut self, command: CommitedCommand<M::Command>) -> bool {
        if self.context.version.index + 1 != command.version.index {
            false
        } else {
            self.machine.execute(command.command);
            self.context.version = command.version;
            true
        }
    }
    pub fn take_snapshot(&self) -> Option<M::Snapshot> {
        self.machine.take_snapshot()
    }
    pub fn query(&self) -> &M {
        &self.machine
    }
    pub fn context(&self) -> &Context {
        &self.context
    }
}

pub struct Context {
    pub version: CommitVersion,
    pub config: Config,
    pub role: NodeRole,
}

pub struct Token;

pub trait ReplicatedLog<M>
    where M: Machine
{
    type Error;
    fn create_state_machine(&mut self) -> Result<State<M>, Self::Error>;
    fn run_once(&mut self) -> Result<Option<Event<M>>, Self::Error>;

    fn commit(&mut self, command: M::Command, expected: Option<CommitVersion>) -> Token;
    fn sync(&mut self) -> Token;
    fn install_snapshot(&mut self, snapshot: M::Snapshot) -> Token;

    // TODO: delete
    fn update_config(&mut self, config: Config) -> Token;
}

pub enum TodoCommand {
    SystemUpdateConfig,
    User,
}

pub enum Event<M>
    where M: Machine
{
    Commited(Token, CommitedCommand<M::Command>),
    Updated(Token, Config),
    Synced(Token),
    Installed(Token),
    Aborted(Token),
}
