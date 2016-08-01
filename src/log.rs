use election;
use config;

pub type Index = u64;

#[derive(Clone)]
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
}

pub enum Data<T> {
    Noop,
    Config(config::Config),
    Command(T),
}

pub struct IndexTable;
