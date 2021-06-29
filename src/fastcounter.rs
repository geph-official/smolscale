use std::sync::atomic::{AtomicUsize, Ordering};

use cache_padded::CachePadded;
use thread_local::ThreadLocal;

/// A write-mostly, read-rarely counter
#[derive(Default, Debug)]
pub struct FastCounter {
    counters: ThreadLocal<CachePadded<AtomicUsize>>,
}

impl FastCounter {
    /// Increment the counter
    #[inline]
    pub fn incr(&self) {
        self.counters
            .get_or_default()
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the counter
    #[inline]
    pub fn decr(&self) {
        self.counters.get().unwrap().fetch_sub(1, Ordering::Relaxed);
    }

    /// Get the total count
    pub fn count(&self) -> usize {
        self.counters
            .iter()
            .map(|f| f.load(Ordering::Relaxed))
            .sum()
    }
}
