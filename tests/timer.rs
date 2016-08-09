extern crate rrsm;
extern crate rand;

use std::time::Duration;
use std::rc::Rc;
use std::cell::RefCell;

pub struct ManualClock {
    time: Rc<RefCell<Duration>>,
}
impl ManualClock {
    pub fn new() -> Self {
        ManualClock { time: Rc::new(RefCell::new(Duration::from_secs(0))) }
    }
    pub fn elapse(&mut self, duration: Duration) {
        *self.time.borrow_mut() += duration;
    }
    pub fn elapse_ms(&mut self, ms: u64) {
        self.elapse(Duration::from_millis(ms));
    }
    pub fn make_timer(&mut self) -> ManualTimer {
        ManualTimer {
            time: self.time.clone(),
            expires_at: None,
        }
    }
}

pub struct ManualTimer {
    time: Rc<RefCell<Duration>>,
    expires_at: Option<Duration>,
}
impl Default for ManualTimer {
    fn default() -> Self {
        panic!()
    }
}
impl rrsm::Timer for ManualTimer {
    fn expires_between(&mut self, min_after: Duration, max_after: Duration) {
        let now = *self.time.borrow();
        if min_after >= max_after {
            self.expires_at = Some(now + min_after);
        } else {
            let delta = max_after - min_after;
            let delta_nanos = delta.as_secs() * 1000_000_000 + delta.subsec_nanos() as u64;
            let rand_nanos = rand::random::<u64>() % delta_nanos;
            let rand = Duration::new(rand_nanos / 1000_000_000, rand_nanos as u32 % 1000_000_000);
            self.expires_at = Some(now + min_after + rand);
        }
    }
    fn is_expired(&self) -> bool {
        self.expires_at.map_or(false, |d| d <= *self.time.borrow())
    }
    fn clear(&mut self) {
        self.expires_at = None;
    }
}
