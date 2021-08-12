use std::{
    cell::{RefCell, UnsafeCell},
    sync::Arc,
};

use async_task::Runnable;
use concurrent_queue::ConcurrentQueue;
use event_listener::Event;
use futures_intrusive::sync::ManualResetEvent;
use futures_lite::{Future, FutureExt};
use slab::Slab;
use spin::Mutex;

use crate::sp2c::{sp2c, Sp2cReceiver, Sp2cSender, Sp2cStealer};

/// A self-contained executor context.
pub struct Executor {
    global_queue: Arc<ConcurrentQueue<Runnable>>,
    global_notifier: Arc<Event>,
    stealers: Arc<Mutex<Slab<Sp2cStealer<Runnable>>>>,
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
            global_queue: Arc::new(ConcurrentQueue::unbounded()),
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
                if let Some(tls) = tls.borrow().as_ref() {
                    if !Arc::ptr_eq(&tls.global_queue, &global_queue) {
                        // shoot, does not belong to this executor
                        log::trace!("oh no doesn't belong");
                        Err(runnable)
                    } else {
                        // this is great
                        unsafe { tls.schedule_local(runnable) }?;
                        log::trace!("scheduled locally");
                        Ok(())
                    }
                } else {
                    Err(runnable)
                }
            });
            if let Err(runnable) = local_success {
                // fall back to global queue
                log::trace!("scheduled globally");
                // let bt = Backtrace::new();
                // println!("{:?}", bt);
                global_queue.push(runnable).unwrap();
                global_evt.notify_additional(1);
            }
        });
        runnable.schedule();
        task
    }

    /// Obtains a new worker.
    pub fn worker(&self) -> Worker {
        let (send, recv, stealer) = sp2c();
        let sender: UnsafeLocalSender = Arc::new(UnsafeCell::new(send));
        let notifier = Arc::new(ManualResetEvent::new(false));
        let worker_id = self.stealers.lock().insert(stealer);
        Worker {
            worker_id,
            sender,
            local_notifier: notifier,
            global_notifier: self.global_notifier.clone(),
            receiver: recv,
            global_queue: self.global_queue.clone(),
            stealers: self.stealers.clone(),
        }
    }

    /// Rebalance the executor. Can/should be called from a monitor thread.
    pub fn rebalance(&self) {
        let mut stealers = self.stealers.lock();
        if stealers.is_empty() {
            return;
        }
        let mut stolen = Vec::with_capacity(16);
        let random_start = fastrand::usize(0..stealers.len());
        for (_, stealer) in stealers.iter_mut().skip(random_start) {
            stealer.steal_batch(&mut stolen);
            if !stolen.is_empty() {
                break;
            }
        }
        if stolen.is_empty() {
            for (_, stealer) in stealers.iter_mut().take(random_start) {
                stealer.steal_batch(&mut stolen);
                if !stolen.is_empty() {
                    break;
                }
            }
        }
        for stolen in stolen {
            self.global_queue.push(stolen).unwrap();
        }
        self.global_notifier.notify_additional(1);
        // if fastrand::f32() < 0.01 {
        //     log::debug!("{} in global queue", self.global_queue.len());
        //     for (idx, stealer) in stealers.iter() {
        //         log::debug!("{} in local queue {}", stealer.stealable(), idx)
        //     }
        // }
    }
}

thread_local! {
    static TLS: RefCell<Option<TlsState>> = Default::default();
}

type UnsafeLocalSender = Arc<UnsafeCell<Sp2cSender<Runnable>>>;

struct TlsState {
    inner_sender: UnsafeLocalSender,
    local_notifier: Arc<ManualResetEvent>,
    global_queue: Arc<ConcurrentQueue<Runnable>>, // for identification purposes
    counter: UnsafeCell<usize>,
}

impl TlsState {
    #[inline]
    unsafe fn schedule_local(&self, task: Runnable) -> Result<(), Runnable> {
        let inner = &mut *self.inner_sender.get();
        *self.counter.get() += 1;
        // occasionally, we intentionally fail to push tasks to the global queue. this improves fairness.
        if *self.counter.get() % 256 == 0 {
            return Err(task);
        }
        inner.send(task)?;
        self.local_notifier.set();
        Ok(())
    }
}

pub struct Worker {
    worker_id: usize,

    sender: UnsafeLocalSender,
    local_notifier: Arc<ManualResetEvent>,
    global_notifier: Arc<Event>,
    receiver: Sp2cReceiver<Runnable>,
    global_queue: Arc<ConcurrentQueue<Runnable>>,
    stealers: Arc<Mutex<Slab<Sp2cStealer<Runnable>>>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stealers.lock().remove(self.worker_id);
        TLS.with(|v| v.borrow_mut().take());
    }
}

impl Worker {
    /// Runs this worker.
    pub async fn run(&mut self) {
        self.set_tls();
        loop {
            let global = self.global_notifier.listen();
            self.set_tls();
            while let Some(task) = self.run_once() {
                task.run();
                if fastrand::u8(0..u8::MAX) == 0 {
                    futures_lite::future::yield_now().await;
                }
            }
            let local = self.local_notifier.wait();
            local.or(global).await;
            self.local_notifier.reset();
        }
    }

    fn run_once(&mut self) -> Option<Runnable> {
        if let Some(task) = self.receiver.pop() {
            return Some(task);
        }
        // SAFETY: cannot alias with TLS method
        let sender = unsafe { &mut *self.sender.get() };
        let steal_limit = sender.slots().min(self.global_queue.len() / 2);
        let mut steal_count = 0;
        while let Some(stolen) = self.global_queue.pop().ok() {
            steal_count += 1;
            sender.send(stolen).unwrap();
            if steal_count >= steal_limit {
                break;
            }
        }
        self.receiver.pop()
    }

    fn set_tls(&mut self) {
        TLS.with(|f| {
            let mut f = f.borrow_mut();
            if f.is_none() {
                *f = Some(TlsState {
                    inner_sender: self.sender.clone(),
                    local_notifier: self.local_notifier.clone(),
                    global_queue: self.global_queue.clone(),
                    counter: Default::default(),
                });
            }
        })
    }
}
