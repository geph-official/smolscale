use std::{
    cell::{RefCell, UnsafeCell},
    sync::{Arc, Mutex},
};

use async_task::Runnable;
use concurrent_queue::ConcurrentQueue;
use event_listener::Event;
use futures_lite::{Future, FutureExt};
use slab::Slab;

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
        runnable.run();
        task
    }

    /// Obtains a new worker.
    pub fn worker(&self) -> Worker {
        let (send, recv, stealer) = sp2c();
        let sender: UnsafeLocalSender = Arc::new(UnsafeCell::new(send));
        let notifier = Arc::new(Event::new());
        let worker_id = self.stealers.lock().unwrap().insert(stealer);
        Worker {
            worker_id,
            sender,
            notifier,
            global_notifier: self.global_notifier.clone(),
            receiver: recv,
            global_queue: self.global_queue.clone(),
            stealers: self.stealers.clone(),
        }
    }

    /// Rebalance the executor. Can/should be called from a monitor thread.
    pub fn rebalance(&self) {
        let mut stealers = self.stealers.lock().unwrap();
        let biggest_catch = stealers.iter_mut().max_by_key(|(_, st)| st.stealable());
        if let Some((idx, stealer)) = biggest_catch {
            let mut stolen = Vec::with_capacity(256);
            stealer.steal_batch(&mut stolen);
            log::debug!("stealing {} tasks from idx {}", idx, stolen.len());
            for stolen in stolen {
                self.global_queue.push(stolen).unwrap();
            }
            self.global_notifier.notify_additional(1);
        }
    }
}

thread_local! {
    static TLS: RefCell<Option<TlsState>> = Default::default();
}

type UnsafeLocalSender = Arc<UnsafeCell<Sp2cSender<Runnable>>>;

struct TlsState {
    inner_sender: UnsafeLocalSender,
    notifier: Arc<Event>,
    global_queue: Arc<ConcurrentQueue<Runnable>>, // for identification purposes
}

impl TlsState {
    unsafe fn schedule_local(&self, task: Runnable) -> Result<(), Runnable> {
        let inner = &mut *self.inner_sender.get();
        inner.send(task)?;
        self.notifier.notify_relaxed(1);
        Ok(())
    }
}

pub struct Worker {
    worker_id: usize,

    sender: UnsafeLocalSender,
    notifier: Arc<Event>,
    global_notifier: Arc<Event>,
    receiver: Sp2cReceiver<Runnable>,
    global_queue: Arc<ConcurrentQueue<Runnable>>,
    stealers: Arc<Mutex<Slab<Sp2cStealer<Runnable>>>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.stealers.lock().unwrap().remove(self.worker_id);
        TLS.with(|v| v.borrow_mut().take());
    }
}

impl Worker {
    /// Runs this worker.
    pub async fn run(&mut self) {
        self.set_tls();
        loop {
            let global = self.global_notifier.listen();
            let local = self.notifier.listen();
            self.set_tls();
            self.run_once();
            local.or(global).await;
            log::trace!("notified");
        }
    }

    fn run_once(&mut self) {
        while let Some(task) = self.receiver.pop().or_else(|| self.global_queue.pop().ok()) {
            task.run();
        }
    }

    fn set_tls(&mut self) {
        TLS.with(|f| {
            let mut f = f.borrow_mut();
            if f.is_none() {
                *f = Some(TlsState {
                    inner_sender: self.sender.clone(),
                    notifier: self.notifier.clone(),
                    global_queue: self.global_queue.clone(),
                });
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    #[test]
    fn simple() {
        let exec = Executor::new();
        let _tasks = (0..10)
            .map(|i| {
                exec.spawn(async move {
                    loop {
                        eprintln!("hello world {}", i);
                        async_io::Timer::after(Duration::from_secs(1)).await;
                    }
                })
            })
            .collect::<Vec<_>>();
        async_io::block_on(exec.worker().run());
    }
}
