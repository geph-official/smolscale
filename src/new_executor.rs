use std::{cell::Cell, rc::Rc};

use futures_intrusive::sync::LocalManualResetEvent;
use futures_lite::{Future, FutureExt};

use crate::queues::{GlobalQueue, LocalQueue};

static GLOBAL_QUEUE: once_cell::sync::Lazy<GlobalQueue> =
    once_cell::sync::Lazy::new(GlobalQueue::new);

thread_local! {
    static LOCAL_QUEUE: once_cell::unsync::Lazy<LocalQueue<'static>> = once_cell::unsync::Lazy::new(|| GLOBAL_QUEUE.subscribe());

    static LOCAL_EVT: Rc<LocalManualResetEvent> = Rc::new(LocalManualResetEvent::new(false));

    static RUNNING: Cell<bool> = Cell::new(false);
}

/// Runs a queue
pub async fn run_local_queue() {
    RUNNING.with(|r| r.set(true));
    scopeguard::defer!(RUNNING.with(|r| r.set(false)));
    loop {
        // subscribe
        let local_evt = async {
            let local = LOCAL_EVT.with(|le| le.clone());
            local.wait().await;
            log::debug!("local fired!");
            local.reset();
        };
        let evt = local_evt.or(GLOBAL_QUEUE.wait());
        LOCAL_QUEUE.with(|q| q.run_all());
        // wait now, so that when we get woken up, we *know* that something happened to the global queue.
        evt.await;
    }
}

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runnable, task) = async_task::spawn(future, |runnable| {
        // if the current thread is not processing tasks, we go to the global queue directly.
        if !RUNNING.with(|r| r.get()) {
            GLOBAL_QUEUE.push(runnable);
        } else {
            LOCAL_QUEUE.with(|lq| lq.push(runnable));
            LOCAL_EVT.with(|le| le.set());
        }
    });
    runnable.schedule();
    task
}

/// Globally rebalance.
pub fn global_rebalance() {
    GLOBAL_QUEUE.rebalance();
}
