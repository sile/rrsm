extern crate rrsm;

mod timer;

use std::collections::HashMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::cell::RefCell;

struct OnMemoryPostOffice<M>
    where M: rrsm::Machine
{
    messages: Rc<RefCell<HashMap<rrsm::NodeId, VecDeque<rrsm::consensus::Message<M>>>>>,
}
impl<M> OnMemoryPostOffice<M>
    where M: rrsm::Machine
{
    pub fn new() -> Self {
        OnMemoryPostOffice { messages: Rc::new(RefCell::new(HashMap::new())) }
    }
    pub fn create_postbox(&mut self, node: &rrsm::NodeId) -> OnMemoryPostbox<M> {
        OnMemoryPostbox::new(node, self.messages.clone())
    }
}

struct OnMemoryPostbox<M>
    where M: rrsm::Machine
{
    node_id: rrsm::NodeId,
    messages: Rc<RefCell<HashMap<rrsm::NodeId, VecDeque<rrsm::consensus::Message<M>>>>>,
}
impl<M> OnMemoryPostbox<M>
    where M: rrsm::Machine
{
    pub fn new(node_id: &rrsm::NodeId,
               messages: Rc<RefCell<HashMap<rrsm::NodeId, VecDeque<rrsm::consensus::Message<M>>>>>)
               -> Self {
        OnMemoryPostbox {
            node_id: node_id.clone(),
            messages: messages,
        }
    }
}
impl<M> rrsm::io::Postbox<M> for OnMemoryPostbox<M>
    where M: rrsm::Machine,
          M::Command: Clone
{
    fn send_ref(&mut self, destination: &rrsm::NodeId, message: &rrsm::consensus::Message<M>) {
        println!("[{}] send to: {}", self.node_id, destination);
        if !self.messages.borrow().contains_key(destination) {
            self.messages.borrow_mut().insert(destination.clone(), VecDeque::new());
        }
        self.messages.borrow_mut().get_mut(destination).unwrap().push_back(message.clone());
    }
    fn send_val(&mut self, destination: &rrsm::NodeId, message: rrsm::consensus::Message<M>) {
        println!("[{}] send to: {}", self.node_id, destination);
        if !self.messages.borrow().contains_key(destination) {
            self.messages.borrow_mut().insert(destination.clone(), VecDeque::new());
        }
        self.messages.borrow_mut().get_mut(destination).unwrap().push_back(message);
    }
    fn try_recv(&mut self) -> Option<rrsm::consensus::Message<M>> {
        if !self.messages.borrow().contains_key(&self.node_id) {
            self.messages.borrow_mut().insert(self.node_id.clone(), VecDeque::new());
        }
        self.messages.borrow_mut().get_mut(&self.node_id).unwrap().pop_front().map(|m| {
            println!("[{}] recv from: {}: {}: {:?}",
                     self.node_id,
                     m.header.from.id,
                     m.data.kind(),
                     m.header);
            m
        })
    }
}

use std::marker::PhantomData;
struct OnMemoryStorage<M>
    where M: rrsm::Machine
{
    ballot: Option<rrsm::election::Ballot>,
    snapshot: Option<rrsm::replicator::Snapshot<M>>,
    log: Vec<rrsm::log::Entry<M::Command>>,
    result_queue: VecDeque<rrsm::io::StorageAsyncResult<M, ()>>,
    _machine: PhantomData<M>,
}
impl<M> OnMemoryStorage<M>
    where M: rrsm::Machine
{
    pub fn new() -> Self {
        OnMemoryStorage {
            ballot: None,
            snapshot: None,
            log: Vec::new(),
            result_queue: VecDeque::new(),
            _machine: PhantomData,
        }
    }
}
impl<M> rrsm::io::Storage<M> for OnMemoryStorage<M>
    where M: rrsm::Machine,
          M::Command: Clone,
          M::Snapshot: Clone
{
    type Error = ();
    fn log_append(&mut self, entries: Vec<rrsm::log::Entry<M::Command>>, key: rrsm::AsyncKey) {
        self.log.extend(entries);
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: Ok(rrsm::io::StorageData::None),
        });
    }
    fn log_truncate(&mut self, next_index: rrsm::log::Index, key: rrsm::AsyncKey) {
        self.log.truncate(next_index as usize);
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: Ok(rrsm::io::StorageData::None),
        });
    }
    fn log_drop_until(&mut self, _first_index: rrsm::log::Index, key: rrsm::AsyncKey) {
        // TODO: support
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: Ok(rrsm::io::StorageData::None),
        });
    }
    fn log_get(&mut self, first_index: rrsm::log::Index, max_len: usize, key: rrsm::AsyncKey) {
        let entries = self.log.iter().skip(first_index as usize).take(max_len).cloned().collect();
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: Ok(rrsm::io::StorageData::Entries(entries)),
        });
    }
    fn last_appended(&self) -> rrsm::log::EntryVersion {
        rrsm::log::EntryVersion::new(self.log.last().unwrap().term,
                                     self.log.len() as rrsm::log::Index)
    }
    fn least_stored(&self) -> rrsm::log::EntryVersion {
        rrsm::log::EntryVersion::new(0, 0)
    }
    fn build_log_index_table(&self) -> rrsm::log::IndexTable {
        let mut builder = rrsm::log::IndexTableBuilder::new(self.log.len() as rrsm::log::Index - 1);

        let mut term = None;
        for (i, e) in self.log.iter().enumerate() {
            if term != Some(e.term) {
                builder.add_new_term(rrsm::log::EntryVersion::new(e.term, i as rrsm::log::Index));
                term = Some(e.term);
            }
        }
        builder.build()
    }

    fn save_ballot(&mut self, ballot: rrsm::election::Ballot, key: rrsm::AsyncKey) {
        self.ballot = Some(ballot);
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: Ok(rrsm::io::StorageData::None),
        });
    }
    fn load_ballot(&mut self, key: rrsm::AsyncKey) {
        let result = if let Some(ref b) = self.ballot {
            Ok(rrsm::io::StorageData::Ballot(b.clone()))
        } else {
            Ok(rrsm::io::StorageData::None)
        };
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: result,
        });
    }
    fn save_snapshot(&mut self, snapshot: rrsm::replicator::Snapshot<M>, key: rrsm::AsyncKey) {
        self.snapshot = Some(snapshot);
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: Ok(rrsm::io::StorageData::None),
        });
    }
    fn load_snapshot(&mut self, key: rrsm::AsyncKey) {
        let result = if let Some(ref s) = self.snapshot {
            Ok(rrsm::io::StorageData::Snapshot(s.clone()))
        } else {
            Ok(rrsm::io::StorageData::None)
        };
        self.result_queue.push_back(rrsm::AsyncResult {
            key: key,
            result: result,
        });
    }

    fn try_run_once(&mut self) -> Option<rrsm::io::StorageAsyncResult<M, Self::Error>> {
        self.result_queue.pop_front()
    }
    fn run_once(&mut self) -> rrsm::io::StorageAsyncResult<M, Self::Error> {
        if let Some(r) = self.try_run_once() {
            r
        } else {
            panic!()
        }
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.result_queue.clear();
        Ok(())
    }
}

#[derive(Clone)]
struct Calculator {
    pub value: i64,
}
impl Default for Calculator {
    fn default() -> Self {
        Calculator { value: 0 }
    }
}
impl rrsm::Machine for Calculator {
    type Command = Op;
    type Snapshot = Self;
    fn execute(&mut self, op: Self::Command) {
        match op {
            Op::Add(n) => self.value += n,
            Op::Sub(n) => self.value -= n,
        }
    }
    fn take_snapshot(&self) -> Self::Snapshot {
        self.clone()
    }
}

struct CalculatorRsm;
impl rrsm::Rsm for CalculatorRsm {
    type Machine = Calculator;
    type Storage = OnMemoryStorage<Self::Machine>;
    type Postbox = OnMemoryPostbox<Self::Machine>;
    type Timer = timer::ManualTimer;
}

#[derive(Clone)]
enum Op {
    Add(i64),
    Sub(i64),
}

#[test]
fn it_works() {
    let mut clock = timer::ManualClock::new();
    let mut postoffice = OnMemoryPostOffice::new();
    let mut a = new_replicator("a", config(), &mut clock, &mut postoffice);
    let mut b = new_replicator("b", config(), &mut clock, &mut postoffice);
    let mut c = new_replicator("c", config(), &mut clock, &mut postoffice);
    for i in 0..10 {
        a.try_run_once().ok().unwrap().map(|e| println!("{}: a: {:?}", i, e));
    }
    clock.elapse_ms(10000);
    for i in 0..10 {
        a.try_run_once().ok().unwrap().map(|e| println!("{}: a: {:?}", i, e));
    }
    clock.elapse_ms(10000);
    for i in 0..30 {
        a.try_run_once().ok().unwrap().map(|e| println!("{}: a: {:?}", i, e));
        b.try_run_once().ok().unwrap().map(|e| println!("{}: b: {:?}", i, e));
        c.try_run_once().ok().unwrap().map(|e| println!("{}: c: {:?}", i, e));
    }
    println!("------------------ EXECUTE -----------------");
    a.execute(Op::Add(10));
    for i in 0..40 {
        a.try_run_once().ok().unwrap().map(|e| println!("{}: {}: a: {:?}", line!(), i, e));
        b.try_run_once().ok().unwrap().map(|e| println!("{}: {}: b: {:?}", line!(), i, e));
        c.try_run_once().ok().unwrap().map(|e| println!("{}: {}: c: {:?}", line!(), i, e));
    }
    assert_eq!(10, a.state().value);

    println!("------------------ SYNC -----------------");
    a.sync();
    for i in 0..40 {
        a.try_run_once().ok().unwrap().map(|e| println!("{}: {}: a: {:?}", line!(), i, e));
        b.try_run_once().ok().unwrap().map(|e| println!("{}: {}: b: {:?}", line!(), i, e));
        c.try_run_once().ok().unwrap().map(|e| println!("{}: {}: c: {:?}", line!(), i, e));
    }
    assert_eq!(10, a.state().value);
    assert_eq!(10, b.state().value);
    assert_eq!(10, c.state().value);
}
fn new_replicator(node: &str,
                  config: rrsm::config::Builder,
                  clock: &mut timer::ManualClock,
                  postoffice: &mut OnMemoryPostOffice<Calculator>)
                  -> rrsm::Replicator<CalculatorRsm> {
    let postbox = postoffice.create_postbox(&node.to_string());
    let storage = OnMemoryStorage::new();
    rrsm::Replicator::new(rrsm::Node::new(node.to_string(), 0),
                          storage,
                          postbox,
                          clock.make_timer(),
                          config)
        .ok()
        .unwrap()
}
fn config() -> rrsm::config::Builder {
    rrsm::config::Builder::new(["a", "b", "c"].iter().map(|n| n.to_string()).collect())
}
