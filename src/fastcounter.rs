use std::sync::atomic::{AtomicUsize, Ordering};

/// A write-mostly, read-rarely counter
#[derive(Default, Debug)]
pub struct FastCounter {
    counter: AtomicUsize,
}

impl FastCounter {
    /// Increment the counter
    #[inline]
    pub fn incr(&self) {
        self.counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the counter
    #[inline]
    pub fn decr(&self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get the total count
    pub fn count(&self) -> usize {
        self.counter.load(Ordering::Relaxed)
    }
}
