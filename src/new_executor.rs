use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
};

use async_task::Runnable;
use futures_intrusive::sync::LocalManualResetEvent;
use futures_lite::FutureExt;

use crate::queues::{GlobalQueue, LocalQueue};

static GLOBAL_QUEUE: once_cell::sync::Lazy<GlobalQueue> =
    once_cell::sync::Lazy::new(GlobalQueue::new);

thread_local! {
    static LOCAL_QUEUE: once_cell::unsync::Lazy<LocalQueue<'static>> = once_cell::unsync::Lazy::new(|| GLOBAL_QUEUE.subscribe());

    static LOCAL_EVT: Rc<LocalManualResetEvent> = Rc::new(LocalManualResetEvent::new(false));

    static LOCAL_QUEUE_ACTIVE: Cell<bool> = Cell::new(false);

    static LOCAL_QUEUE_HOLDING: RefCell<Vec<Runnable>> = RefCell::new(vec![]);
}

/// Runs a queue
pub async fn run_local_queue() {
    LOCAL_QUEUE_ACTIVE.with(|r| r.set(true));
    scopeguard::defer!(LOCAL_QUEUE_ACTIVE.with(|r| r.set(false)));
    loop {
        // subscribe
        let local_evt = async {
            let local = LOCAL_EVT.with(|le| le.clone());
            local.wait().await;
            local.reset();
        };
        let evt = local_evt.or(GLOBAL_QUEUE.wait());
        {
            while let Some(r) = LOCAL_QUEUE.with(|q| q.pop()) {
                r.run();
                if fastrand::usize(0..256) == 0 {
                    futures_lite::future::yield_now().await;
                }
            }
        }
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
        if !LOCAL_QUEUE_ACTIVE.with(|r| r.get()) || fastrand::usize(0..512) == 0 {
            log::trace!("pushed to global queue");
            GLOBAL_QUEUE.push(runnable);
        } else {
            log::trace!("pushed to local queue");
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
