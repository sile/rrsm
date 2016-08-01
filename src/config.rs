use super::*;

#[derive(Clone)]
pub struct Config;
impl Config {
    pub fn is_member(&self, _node_id: &NodeId) -> bool {
        panic!()
    }
}

pub struct Builder;
impl Builder {
    pub fn build(self) -> Config {
        Config
    }
}
