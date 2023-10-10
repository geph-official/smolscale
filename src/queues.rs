use async_task::Runnable;

use crossbeam_utils::sync::ShardedLock;
use event_listener::{Event, EventListener};

use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use st3::fifo::{Stealer, Worker};
use std::{
    cell::RefCell,
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
};

/// The global task queue, also including handles for stealing from local queues.
///
/// Tasks can be pushed to it. Popping requires first subscribing to it, producing a [LocalQueue], which then can be popped from.
pub struct GlobalQueue {
    queue: parking_lot::Mutex<VecDeque<Runnable>>,
    stealers: ShardedLock<FxHashMap<u64, Stealer<Runnable>>>,
    id_ctr: AtomicU64,
    event: Event,
}

impl GlobalQueue {
    /// Creates a new GlobalQueue.
    pub fn new() -> Self {
        Self {
            queue: Default::default(),
            stealers: Default::default(),
            id_ctr: AtomicU64::new(0),
            event: Event::new(),
        }
    }

    /// Pushes a task to the GlobalQueue, notifying at least one [LocalQueue].  
    pub fn push(&self, task: Runnable) {
        self.queue.lock().push_back(task);
        self.event.notify(1);
    }

    /// Rebalances the executor.
    pub fn rebalance(&self) {
        self.event.notify_relaxed(usize::MAX);
    }

    /// Subscribes to tasks, returning a LocalQueue.
    pub fn subscribe(&self) -> LocalQueue<'_> {
        let worker = Worker::<Runnable>::new(1024);
        let id = self.id_ctr.fetch_add(1, Ordering::Relaxed);
        self.stealers.write().unwrap().insert(id, worker.stealer());

        LocalQueue {
            id,
            global: self,
            local: worker,
            next_task: RefCell::new(None),
        }
    }

    /// Wait for activity
    pub fn wait(&self) -> EventListener {
        self.event.listen()
    }
}

/// A thread-local queue, bound to some [GlobalQueue].
pub struct LocalQueue<'a> {
    id: u64,
    global: &'a GlobalQueue,
    local: Worker<Runnable>,

    next_task: RefCell<Option<Runnable>>,
}

impl<'a> Drop for LocalQueue<'a> {
    fn drop(&mut self) {
        // push all the local tasks to the global queue
        while let Some(task) = self.local.pop() {
            self.global.push(task);
        }
        // deregister the local queue from the global list
        self.global.stealers.write().unwrap().remove(&self.id);
    }
}

impl<'a> LocalQueue<'a> {
    /// Pops a task from the local queue, other local queues, or the global queue.
    pub fn pop(&self) -> Option<Runnable> {
        self.next_task
            .borrow_mut()
            .take()
            .or_else(|| self.local.pop())
            .or_else(|| self.steal_and_pop())
    }

    /// Pushes an item to the local queue, falling back to the global queue if the local queue is full.
    pub fn push(&self, runnable: Runnable) {
        if let Some(runnable) = self.next_task.borrow_mut().replace(runnable) {
            if let Err(runnable) = self.local.push(runnable) {
                log::trace!("{} pushed globally", self.id);
                self.global.push(runnable);
            } else {
                log::trace!("{} pushed locally", self.id);
            }
        }
    }

    /// Steals a whole batch and pops one.
    fn steal_and_pop(&self) -> Option<Runnable> {
        {
            let stealers = self.global.stealers.read().unwrap();
            let mut ids: SmallVec<[u64; 64]> = stealers.keys().copied().collect();
            fastrand::shuffle(&mut ids);
            for id in ids {
                if let Ok((val, count)) =
                    stealers[&id].steal_and_pop(&self.local, |n| (n / 2 + 1).min(64))
                {
                    log::trace!("{} stole {} from {id}", count + 1, self.id);
                    return Some(val);
                }
            }
        }

        // try stealing from the global
        if let Some(mut global) = self.global.queue.try_lock() {
            let to_steal = (global.len() / 2 + 1).min(64).min(global.len());
            for _ in 0..to_steal {
                let stolen = global.pop_front().unwrap();
                if let Err(back) = self.local.push(stolen) {
                    return Some(back);
                }
            }
            return self.local.pop();
        }
        None
    }
}
