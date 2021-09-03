use std::{
    cell::{RefCell, UnsafeCell},
    sync::{Arc, Mutex, RwLock},
};

use async_task::Runnable;
use concurrent_queue::ConcurrentQueue;
use crossbeam_deque::{Injector, Steal, Stealer};
use event_listener::{Event, EventListener};
use futures_intrusive::sync::ManualResetEvent;
use futures_lite::{Future, FutureExt};
use slab::Slab;

/// A self-contained executor context.
pub struct Executor {
    global_queue: Arc<Injector<Runnable>>,
    global_notifier: Arc<Event>,
    stealers: Arc<RwLock<Slab<Stealer<Runnable>>>>,
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor {
    /// Creates a new executor.
    pub fn new() -> Self {
        Self {
            global_queue: Arc::new(Injector::new()),
            global_notifier: Arc::new(Event::new()),
            stealers: Default::default(),
        }
    }

    /// Spawns a new task onto this executor.
    pub fn spawn<F>(&self, future: F) -> async_task::Task<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let global_queue = self.global_queue.clone();
        let global_evt = self.global_notifier.clone();
        let (runnable, task) = async_task::spawn(future, move |runnable| {
            // attempt to spawn onto the worker that last ran
            let local_success: Result<(), Runnable> = TLS.with(|tls| {
                if let Some(tls) = tls.borrow_mut().as_mut() {
                    if !Arc::ptr_eq(&tls.global_queue, &global_queue) {
                        // shoot, does not belong to this executor
                        log::trace!("oh no doesn't belong");
                        Err(runnable)
                    } else {
                        // this is great
                        unsafe { tls.schedule_local(runnable) }?;
                        Ok(())
                    }
                } else {
                    log::trace!("no TLS");
                    Err(runnable)
                }
            });
            if let Err(runnable) = local_success {
                // fall back to global queue
                // eprintln!("scheduled globally");
                // let bt = Backtrace::new();
                // println!("{:?}", bt);
                global_queue.push(runnable);
                global_evt.notify_additional(1);
            }
        });
        runnable.schedule();
        task
    }

    /// Obtains a new worker.
    pub fn worker(&self) -> Worker {
        let local_queue = crossbeam_deque::Worker::new_fifo();
        let stealer = local_queue.stealer();
        let notifier = Arc::new(ManualResetEvent::new(false));
        let worker_id = self.stealers.write().unwrap().insert(stealer);
        Worker {
            worker_id,
            local_queue,
            local_notifier: notifier,
            global_notifier: self.global_notifier.clone(),
            global_notifier_handle: self.global_notifier.listen(),
            global_queue: self.global_queue.clone(),
            stealers: self.stealers.clone(),
        }
    }

    /// Rebalance the executor. Can/should be called from a monitor thread.
    pub fn rebalance(&self) {
        // all we need to do is to notify something.
        // if everybody's busy, we don't have to do anything anyway because they'll work-steal once they're done
        // otherwise, there may be an idle thread that doesn't know that it can steal from others, so we notify it
        self.global_notifier.notify_additional(1);
    }
}

thread_local! {
    static TLS: RefCell<Option<TlsState>> = Default::default();
}

struct TlsState {
    inner_sender: Vec<Runnable>,
    local_notifier: Arc<ManualResetEvent>,
    global_queue: Arc<Injector<Runnable>>, // for identification purposes
    counter: UnsafeCell<usize>,
}

impl TlsState {
    #[inline]
    unsafe fn schedule_local(&mut self, task: Runnable) -> Result<(), Runnable> {
        *self.counter.get() += 1;
        // occasionally, we intentionally fail to push tasks to the global queue. this improves fairness.
        if *self.counter.get() % 256 == 0 {
            return Err(task);
        }
        self.inner_sender.push(task);
        self.local_notifier.set();
        Ok(())
    }
}

pub struct Worker {
    worker_id: usize,

    local_queue: crossbeam_deque::Worker<Runnable>,
    local_notifier: Arc<ManualResetEvent>,
    global_notifier: Arc<Event>,
    global_notifier_handle: EventListener,
    global_queue: Arc<Injector<Runnable>>,
    stealers: Arc<RwLock<Slab<Stealer<Runnable>>>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stealers.write().unwrap().remove(self.worker_id);
        TLS.with(|v| v.borrow_mut().take());
    }
}

impl Worker {
    /// Runs this worker.
    #[inline]
    pub async fn run(&mut self) {
        self.set_tls();
        loop {
            let global = std::mem::replace(
                &mut self.global_notifier_handle,
                self.global_notifier.listen(),
            );
            self.set_tls();
            while let Some(task) = self.run_once() {
                task.run();
                if fastrand::u8(0..=u8::MAX) == 0 {
                    futures_lite::future::yield_now().await;
                }
            }
            let local = self.local_notifier.wait();
            local.or(global).await;
            self.local_notifier.reset();
        }
    }

    #[inline]
    fn run_once(&mut self) -> Option<Runnable> {
        TLS.with(|tls| {
            if let Some(tls) = tls.borrow_mut().as_mut() {
                for task in tls.inner_sender.drain(0..) {
                    self.local_queue.push(task);
                }
            }
        });
        // self.local_queue.pop()
        if let Some(task) = self.local_queue.pop() {
            return Some(task);
        }
        self.steal_global();
        if let Some(task) = self.local_queue.pop() {
            return Some(task);
        }
        // we do work stealing here
        let stealers = self.stealers.read().unwrap();
        let mut stealers: Vec<&Stealer<_>> = stealers.iter().map(|(_, s)| s).collect();
        fastrand::shuffle(&mut stealers);
        for stealer in stealers {
            if let Steal::Success(some) = stealer.steal_batch_and_pop(&self.local_queue) {
                return Some(some);
            }
        }
        None
    }

    #[inline]
    fn steal_global(&mut self) {
        while let Steal::Retry = self.global_queue.steal_batch(&self.local_queue) {}
    }

    #[inline]
    fn set_tls(&mut self) {
        TLS.with(|f| {
            let mut f = f.borrow_mut();
            if f.is_none() {
                *f = Some(TlsState {
                    inner_sender: Vec::new(),
                    local_notifier: self.local_notifier.clone(),
                    global_queue: self.global_queue.clone(),
                    counter: Default::default(),
                });
            }
        })
    }
}
