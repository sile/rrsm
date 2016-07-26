extern crate eventual;

use std::collections::HashSet;
use std::collections::VecDeque;
use std::time::Duration;
use eventual::Future;

pub mod raft_protocol;
pub mod sync;

pub use self::raft_protocol::{NodeId, Term, LogIndex, LogEntry};

pub trait StateMachine {
    type Command;
    fn execute(&mut self, command: Self::Command);
    // fn install_snapshot();
    // fn take_snapshot();
}

pub trait Transport<T: Send> {
    type Error: Send;
    fn send(&mut self, dest: &NodeId, message: T) -> Future<(), Self::Error>;
    fn recv(&mut self) -> Future<(NodeId, T), Self::Error>;
}

pub trait LogStorage<T> {
    type Error: Send;

    fn append(&mut self, entries: Vec<LogEntry<T>>) -> Future<(), Self::Error>;
    // fn truncate();
    // fn drop();
    // fn get(Range);
}

pub trait StateStorage {
    type Error: Send;

    // TODO: snapshot

    fn save_state(&mut self,
                  current_term: Term,
                  voted_for: Option<NodeId>)
                  -> Future<(), Self::Error>;
    fn load_state(&mut self) -> Future<Option<(Term, Option<NodeId>)>, Self::Error>;
}

pub struct ProcotolConfig {
    pub members: HashSet<NodeId>,
    pub election_timeout: Duration,
}

// pub trait Env<T: Send> {
//     type LogStorage: LogStorage<T>;
//     type StateStorage: StateStorage;
//     type Transport: Transport<T>;
// }

#[allow(dead_code)]
pub struct AsyncRsm<S, T, LS, SS>
    where S: StateMachine
{
    state_machine: S,
    config: ProcotolConfig,
    transport: T,
    log_storage: LS,
    state_storage: SS,
    consensus: Option<raft_protocol::ConsensusModule>,
    action_queue: VecDeque<raft_protocol::Action<S::Command>>,
}
impl<S, T, LS, SS> AsyncRsm<S, T, LS, SS>
    where S: StateMachine,
          T: Transport<S::Command>,
          LS: LogStorage<S::Command>,
          SS: StateStorage,
          S::Command: Send
{
    pub fn new(state_machine: S,
               transport: T,
               log_storage: LS,
               state_storage: SS,
               config: ProcotolConfig)
               -> Self {
        let conf = unsafe { std::mem::zeroed() };
        let table = unsafe { std::mem::zeroed() };
        let (consensus, actions) =
            raft_protocol::ConsensusModule::new("todo".to_string(), 0, None, conf, table);
        AsyncRsm {
            state_machine: state_machine,
            transport: transport,
            log_storage: log_storage,
            state_storage: state_storage,
            consensus: Some(consensus),
            config: config,
            action_queue: actions.into_iter().collect(),
        }
    }
    pub fn run_once(&mut self) -> bool {
        if let Some(action) = self.action_queue.pop_front() {
            use self::raft_protocol::Action;
            match action {
                Action::LogAppend(_) => panic!(),
                Action::LogTruncate(_) => panic!(),
                Action::LogCommit(_) => panic!(),
                Action::SaveState(_, _) => panic!(),
                Action::InstallSnapshot(_, _, _, _, _, _) => panic!(),
                Action::BroadcastMsg(_, _) => panic!(),
                Action::UnicastMsg(_, _, _) => panic!(),
                Action::UnicastLog(_, _, _, _) => panic!(),
                Action::SendSnapshot(_, _) => panic!(),
                Action::ResetTimeout => panic!(),
                Action::Transit(role) => {
                    let (consensus, actions) = self.consensus.take().unwrap().transit(role);
                    self.consensus = Some(consensus);
                    self.action_queue.extend(actions);
                }
                Action::Postpone(_, _) => panic!(),
                Action::Panic => panic!(),
            }
            true
        } else {
            false
        }
    }
    // pub fn execute(&mut self, command: S::Command) -> Future<(), Error>;
    // pub fn sync(&mut self) -> Future<(), Error>;
    // pub fn query(&self) -> &S {
    //     &self.state_machine
    // }
}
