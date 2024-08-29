use crate::bit;
use async_task::Runnable;

use crossbeam_utils::sync::Unparker;
use diatomic_waker::WakeSource;
use st3::fifo::Stealer;
use std::any::Any;
use std::sync::atomic::{self, AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;

/// Manager of worker threads.
///
/// The manager currently only supports up to `usize::BITS` threads.
pub(super) struct PoolManager {
    /// Number of worker threads.
    pool_size: usize,
    /// List of the stealers associated to each worker thread.
    stealers: Box<[Stealer<Runnable>]>,
    /// List of the thread unparkers associated to each worker thread.
    worker_unparkers: Box<[Unparker]>,
    /// Bit field of all workers that are currently unparked.
    active_workers: AtomicUsize,
    /// Count of all workers currently searching for tasks.
    searching_workers: AtomicUsize,
    /// Flag requesting all workers to return immediately.
    terminate_signal: AtomicBool,
    /// Panic caught in a worker thread.
    worker_panic: Mutex<Option<Box<dyn Any + Send + 'static>>>,
}

impl PoolManager {
    /// Creates a new pool manager.
    ///
    /// # Panics
    ///
    /// This will panic if the specified pool size is zero or is more than
    /// `usize::BITS`.
    pub(super) fn new(
        pool_size: usize,
        stealers: Box<[Stealer<Runnable>]>,
        worker_unparkers: Box<[Unparker]>,
    ) -> Self {
        assert!(
            pool_size >= 1,
            "the executor pool size should be at least one"
        );
        assert!(
            pool_size <= usize::BITS as usize,
            "the executor pool size should be at most {}",
            usize::BITS
        );

        Self {
            pool_size,
            stealers,
            worker_unparkers,
            active_workers: AtomicUsize::new(0),
            searching_workers: AtomicUsize::new(0),
            terminate_signal: AtomicBool::new(false),
            worker_panic: Mutex::new(None),
        }
    }

    /// Unparks an idle worker if any is found and mark it as active, or do
    /// nothing otherwise.
    ///
    /// For performance reasons, no synchronization is established if no worker
    /// is found, meaning that workers in other threads may later transition to
    /// idle state without observing the tasks scheduled by this caller. If this
    /// is not tolerable (for instance if this method is called from a
    /// non-worker thread), use the more expensive `activate_worker`.
    pub(super) fn activate_worker_relaxed(&self) {
        let mut active_workers = self.active_workers.load(Ordering::Relaxed);
        loop {
            let first_idle_worker = active_workers.trailing_ones() as usize;
            if first_idle_worker >= self.pool_size {
                return;
            };
            active_workers = self
                .active_workers
                .fetch_or(1 << first_idle_worker, Ordering::Relaxed);
            if active_workers & (1 << first_idle_worker) == 0 {
                self.begin_worker_search();
                self.worker_unparkers[first_idle_worker].unpark();
                return;
            }
        }
    }

    /// Unparks an idle worker if any is found and mark it as active, or ensure
    /// that at least the last active worker will observe all memory operations
    /// performed before this call when calling `try_set_worker_inactive`.
    pub(super) fn activate_worker(&self) {
        let mut active_workers = self.active_workers.load(Ordering::Relaxed);
        loop {
            let first_idle_worker = active_workers.trailing_ones() as usize;
            if first_idle_worker >= self.pool_size {
                // There is apparently no free worker, so a dummy RMW with
                // Release ordering is performed with the sole purpose of
                // synchronizing with the Acquire fence in `set_inactive` so
                // that the last worker sees the tasks that were queued prior to
                // this call to `activate_worker`.
                let new_active_workers = self.active_workers.fetch_or(0, Ordering::Release);
                if new_active_workers == active_workers {
                    return;
                }
                active_workers = new_active_workers;
            } else {
                active_workers = self
                    .active_workers
                    .fetch_or(1 << first_idle_worker, Ordering::Relaxed);
                if active_workers & (1 << first_idle_worker) == 0 {
                    self.begin_worker_search();
                    self.worker_unparkers[first_idle_worker].unpark();
                    return;
                }
            }
        }
    }

    /// Marks the specified worker as inactive unless it is the last active
    /// worker.
    ///
    /// Parking the worker thread is the responsibility of the caller.
    ///
    /// If this was the last active worker, `false` is returned and it is
    /// guaranteed that all memory operations performed by threads that called
    /// `activate_worker` will be visible. The worker is in such case expected
    /// to check again the injector queue and then to explicitly call
    /// `set_all_workers_inactive` if it can confirm that the injector queue is
    /// empty.
    pub(super) fn try_set_worker_inactive(&self, worker_id: usize) -> bool {
        eprintln!("setting worker {worker_id} inactive");
        // Ordering: this Release operation synchronizes with the Acquire fence
        // in the below conditional if this is is the last active worker, and/or
        // with the Acquire state load in the `pool_state` method.
        let active_workers = self
            .active_workers
            .fetch_update(Ordering::Release, Ordering::Relaxed, |active_workers| {
                if active_workers == (1 << worker_id) {
                    // It looks like this is the last worker, but the value
                    // could be stale so it is necessary to make sure of this by
                    // enforcing the CAS rather than returning `None`.
                    Some(active_workers)
                } else {
                    Some(active_workers & !(1 << worker_id))
                }
            })
            .unwrap();

        eprintln!("current status: {:032b}", active_workers);

        assert_ne!(active_workers & (1 << worker_id), 0);

        if active_workers == (1 << worker_id) {
            // This is the last worker so we need to ensures that after this
            // call, all tasks pushed on the injector queue before
            // `set_one_active` was called unsuccessfully are visible.
            //
            // Ordering: this Acquire fence synchronizes with all Release RMWs
            // in this and in the previous calls to `set_inactive` via a release
            // sequence.
            atomic::fence(Ordering::Acquire);

            false
        } else {
            true
        }
    }

    /// Marks all pool workers as active.
    ///
    /// Unparking the worker threads is the responsibility of the caller.
    pub(super) fn set_all_workers_active(&self) {
        // Mark all workers as busy.
        self.active_workers.store(
            !0 >> (usize::BITS - self.pool_size as u32),
            Ordering::Relaxed,
        );
    }

    /// Marks all pool workers as inactive.
    ///
    /// This should only be called by the last active worker. Unparking the
    /// executor threads is the responsibility of the caller.
    pub(super) fn set_all_workers_inactive(&self) {
        // Ordering: this Release store synchronizes with the Acquire load in
        // `is_idle`.
        self.active_workers.store(0, Ordering::Release);
    }

    /// Check if the pool is idle, i.e. if no worker is currently active.
    ///
    /// If `true` is returned, it is guaranteed that all operations performed by
    /// the now-inactive workers become visible in this thread.
    pub(super) fn pool_is_idle(&self) -> bool {
        // Ordering: this Acquire operation synchronizes with all Release
        // RMWs in the `set_worker_inactive` method via a release sequence.
        self.active_workers.load(Ordering::Acquire) == 0
    }

    /// Increments the count of workers actively searching for tasks.
    pub(super) fn begin_worker_search(&self) {
        self.searching_workers.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the count of workers actively searching for tasks.
    pub(super) fn end_worker_search(&self) {
        self.searching_workers.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the count of workers actively searching for tasks.
    pub(super) fn searching_worker_count(&self) -> usize {
        self.searching_workers.load(Ordering::Relaxed)
    }

    /// Triggers the termination signal and unparks all worker threads so they
    /// can cleanly terminate.
    pub(super) fn trigger_termination(&self) {
        self.terminate_signal.store(true, Ordering::Relaxed);

        self.set_all_workers_active();
        for unparker in &*self.worker_unparkers {
            unparker.unpark();
        }
    }

    /// Returns true if the termination signal was triggered.
    pub(super) fn termination_is_triggered(&self) -> bool {
        self.terminate_signal.load(Ordering::Relaxed)
    }

    /// Registers a panic associated with the provided worker ID.
    ///
    /// If no panic is currently registered, the panic in argument is
    /// registered. If a panic was already registered by a worker and was not
    /// yet processed by the executor, then nothing is done.
    pub(super) fn register_panic(&self, panic: Box<dyn Any + Send + 'static>) {
        let mut worker_panic = self.worker_panic.lock().unwrap();
        if worker_panic.is_none() {
            *worker_panic = Some(panic);
        }
    }

    /// Takes a worker panic if any is registered.
    pub(super) fn take_panic(&self) -> Option<Box<dyn Any + Send + 'static>> {
        let mut worker_panic = self.worker_panic.lock().unwrap();
        worker_panic.take()
    }

    /// Returns an iterator yielding the stealers associated with all active
    /// workers, starting from a randomly selected active worker. The worker
    /// which ID is provided in argument (if any) is excluded from the pool of
    /// candidates.
    pub(super) fn shuffled_stealers(
        &self,
        excluded_worker_id: Option<usize>,
    ) -> ShuffledStealers<'_> {
        // All active workers except the specified one are candidate for stealing.
        let mut candidates = self.active_workers.load(Ordering::Relaxed);
        if let Some(excluded_worker_id) = excluded_worker_id {
            candidates &= !(1 << excluded_worker_id);
        }

        ShuffledStealers::new(candidates, &self.stealers)
    }
}

/// An iterator over active workers that yields their associated stealer,
/// starting from a randomly selected active worker.
pub(super) struct ShuffledStealers<'a> {
    stealers: &'a [Stealer<Runnable>],
    // A bit-rotated bit field of the remaining candidate workers to steal from.
    // If set, the LSB represents the next candidate.
    candidates: usize,
    next_candidate: usize, // index of the next candidate
}

impl<'a> ShuffledStealers<'a> {
    /// A new `ShuffledStealer` iterator initialized at a randomly selected
    /// active worker.
    fn new(candidates: usize, stealers: &'a [Stealer<Runnable>]) -> Self {
        let (candidates, next_candidate) = if candidates == 0 {
            (0, 0)
        } else {
            let next_candidate = bit::find_bit(candidates, |count| fastrand::usize(..count) + 1);

            // Right-rotate the candidates so that the bit corresponding to the
            // randomly selected worker becomes the LSB.
            let candidate_count = stealers.len();
            let lower_bits = candidates & ((1 << next_candidate) - 1);
            let candidates =
                (candidates >> next_candidate) | (lower_bits << (candidate_count - next_candidate));

            (candidates, next_candidate)
        };

        Self {
            stealers,
            candidates,
            next_candidate,
        }
    }
}

impl<'a> Iterator for ShuffledStealers<'a> {
    type Item = &'a Stealer<Runnable>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.candidates == 0 {
            return None;
        }

        // Clear the bit corresponding to the current candidate worker.
        self.candidates &= !1;

        let current_candidate = self.next_candidate;

        if self.candidates != 0 {
            // Locate the next candidate worker and make it the LSB.
            let shift = self.candidates.trailing_zeros();
            self.candidates >>= shift;

            // Update the next candidate.
            self.next_candidate += shift as usize;
            if self.next_candidate >= self.stealers.len() {
                self.next_candidate -= self.stealers.len();
            }
        }

        Some(&self.stealers[current_candidate])
    }
}
