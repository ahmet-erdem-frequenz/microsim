use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    time::Duration,
};

use std::time::Instant;

#[derive(Clone, Default)]
pub(crate) struct TimeoutTracker {
    default_timeout: Duration,
    data: Rc<RefCell<HashMap<u64, (Instant, Option<Duration>)>>>,
}

// Tokio is configured to use the current_thread runtime, so it is not unsafe to
// make this Send and Sync.
unsafe impl Send for TimeoutTracker {}
unsafe impl Sync for TimeoutTracker {}

impl TimeoutTracker {
    pub(crate) fn new(default_timeout: Duration) -> Self {
        Self {
            default_timeout,
            data: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub(crate) fn add(&self, id: u64, timeout: Option<Duration>) {
        let now = Instant::now();
        self.data.borrow_mut().insert(id, (now, timeout));
    }

    pub(crate) fn remove_expired(&self) -> HashSet<u64> {
        let now = Instant::now();
        let mut expired_ids = HashSet::new();

        self.data.borrow_mut().retain(|&id, (instant, duration)| {
            if *instant <= now - duration.unwrap_or(self.default_timeout) {
                expired_ids.insert(id);
                false
            } else {
                true
            }
        });

        expired_ids
    }
}
