use std::time;

pub trait Timer: Default {
    fn expires_between(&mut self, min_after: time::Duration, max_after: time::Duration);
    fn is_expired(&self) -> bool;
    fn clear(&mut self);
}
