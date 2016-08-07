use election;
use config;

pub type Index = u64;

#[derive(Clone,PartialOrd,Ord,PartialEq,Eq,Debug)]
pub struct EntryVersion {
    pub term: election::Term,
    pub index: Index,
}
impl EntryVersion {
    pub fn new(term: election::Term, index: Index) -> Self {
        EntryVersion {
            term: term,
            index: index,
        }
    }
}

pub struct Entry<T> {
    pub term: election::Term,
    pub data: Data<T>,
}
impl<T> Entry<T> {
    pub fn noop(term: election::Term) -> Self {
        Entry {
            term: term,
            data: Data::Noop,
        }
    }
    pub fn command(term: election::Term, command: T) -> Self {
        Entry {
            term: term,
            data: Data::Command(command),
        }
    }
    pub fn config(term: election::Term, config: config::Config) -> Self {
        Entry {
            term: term,
            data: Data::Config(config),
        }
    }
}
impl<T> Clone for Entry<T>
    where T: Clone
{
    fn clone(&self) -> Self {
        Entry {
            term: self.term,
            data: self.data.clone(),
        }
    }
}

pub enum Data<T> {
    Noop,
    Config(config::Config),
    Command(T),
}
impl<T> Clone for Data<T>
    where T: Clone
{
    fn clone(&self) -> Self {
        match *self {
            Data::Noop => Data::Noop,
            Data::Config(ref c) => Data::Config(c.clone()),
            Data::Command(ref c) => Data::Command(c.clone()),
        }
    }
}

// TODO: これは不要かも
// StorageがIndexTableを管理して、そのcloneを返せば良さそう
pub struct IndexTableBuilder {
    last_index: Index,
    table: Vec<EntryVersion>,
}
impl IndexTableBuilder {
    pub fn new(last_index: Index) -> Self {
        IndexTableBuilder {
            last_index: last_index,
            table: Vec::new(),
        }
    }
    pub fn add_new_term(&mut self, version: EntryVersion) {
        self.table.push(version);
    }
    pub fn build(mut self) -> IndexTable {
        // TODO: validation
        self.table.reverse();
        IndexTable {
            last_index: self.last_index,
            table: self.table,
        }
    }
}

pub struct IndexTable {
    // INVARIANT: table.len() > 0
    last_index: Index,
    table: Vec<EntryVersion>,
}
impl IndexTable {
    pub fn last_appended(&self) -> EntryVersion {
        EntryVersion::new(self.table.last().unwrap().term, self.last_index)
    }
    pub fn get(&self, index: Index) -> Option<EntryVersion> {
        println!("get: {}, {:?}: {}", self.last_index, self.table, index);
        if self.last_index < index {
            None
        } else {
            let term = match self.table.binary_search_by_key(&index, |e| e.index) {
                Ok(i) => Some(self.table[i].term),
                Err(0) => None,
                Err(i) => Some(self.table[i - 1].term),
            };
            term.map(|t| EntryVersion::new(t, index))
        }
    }
    // XXX: 名前が不適切
    pub fn drop_until(&mut self, new_first: EntryVersion) {
        match self.table.binary_search_by_key(&new_first.index, |e| e.index) {
            Ok(i) => {
                assert_eq!(self.table[i], new_first);
                self.table.drain(0..i);
            }
            Err(0) => {
                // TODO: panic?
                self.last_index = new_first.index;
                self.table = vec![new_first];
            }
            Err(i) => {
                if self.last_index < new_first.index {
                    self.last_index = new_first.index;
                }
                let preceding = self.table.drain(0..i).last().unwrap();
                assert_eq!(preceding.term, new_first.term);
                self.table.insert(0, new_first);
            }
        }
    }
    pub fn truncate(&mut self, new_last: Index) {
        match self.table.binary_search_by_key(&new_last, |e| e.index) {
            Ok(i) => {
                self.table.truncate(i + 1);
                self.last_index = new_last;
            }
            Err(0) => {
                // TODO: panic?
                self.table = vec![EntryVersion::new(0, 0)];
                self.last_index = 0;
            }
            Err(i) => {
                self.table.truncate(i);
                self.last_index = new_last;
            }
        }
    }
    pub fn extend<T>(&mut self, entries: &[Entry<T>]) {
        for e in entries {
            self.last_index += 1;
            if e.term != self.table.last().unwrap().term {
                self.table.push(EntryVersion::new(e.term, self.last_index));
            }
        }
    }
    pub fn find_last_match<T>(&self,
                              prev: &EntryVersion,
                              entries: &[Entry<T>])
                              -> Option<EntryVersion> {
        if self.last_index < prev.index {
            None
        } else if self.table.first().unwrap().index > prev.index + entries.len() as Index {
            // XXX:
            let term = entries.last().map_or(prev.term, |e| e.term);
            let index = prev.index + entries.len() as Index;
            Some(EntryVersion::new(term, index))
        } else {
            let mut index = prev.index + entries.len() as Index;
            for e in entries.iter().rev() {
                if self.get(index).map(|e| e.term) == Some(e.term) {
                    return self.get(index);
                }
                index -= 1;
            }
            if Some(prev) != self.get(prev.index).as_ref() {
                None
            } else {
                Some(prev.clone())
            }
        }
    }
}
