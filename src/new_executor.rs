use std::cell::{Cell, RefCell};

use async_task::Runnable;
use futures_lite::Future;

use crate::queues::{GlobalQueue, LocalQueue};

static GLOBAL_QUEUE: once_cell::sync::Lazy<GlobalQueue> =
    once_cell::sync::Lazy::new(GlobalQueue::new);

thread_local! {
    static LOCAL_QUEUE: LocalQueue<'static> = GLOBAL_QUEUE.subscribe();


    static LOCAL_QUEUE_ACTIVE: Cell<bool> = const { Cell::new(false) };

    static LOCAL_QUEUE_HOLDING: RefCell<Vec<Runnable>> = const { RefCell::new(vec![]) };
}

/// Runs a queue
pub async fn run_local_queue() {
    LOCAL_QUEUE_ACTIVE.with(|r| r.set(true));
    scopeguard::defer!(LOCAL_QUEUE_ACTIVE.with(|r| r.set(false)));
    let mut ctr = 0u32;
    loop {
        while let Some(r) = LOCAL_QUEUE.with(|q| q.pop()) {
            r.run();
            GLOBAL_QUEUE.notify();
            ctr = ctr.wrapping_add(1);
            if ctr % 64 == 0 {
                futures_lite::future::yield_now().await;
            }
        }

        // we only wait here because we want *idle* workers to be notified, not just anyone
        let evt = GLOBAL_QUEUE.wait();
        // if we missed anything we should run them
        while let Some(r) = LOCAL_QUEUE.with(|q| q.pop()) {
            r.run();
            ctr = ctr.wrapping_add(1);
            if ctr % 64 == 0 {
                futures_lite::future::yield_now().await;
            }
        }
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
        if fastrand::u8(..) == 0 {
            log::trace!("pushed to global queue");
            GLOBAL_QUEUE.push(runnable);
        } else {
            log::trace!("pushed to local queue");
            LOCAL_QUEUE.with(|lq| lq.push(runnable));
        }
        GLOBAL_QUEUE.notify();
    });
    runnable.schedule();
    task
}

/// Globally rebalance.
pub fn global_rebalance() {
    GLOBAL_QUEUE.notify();
}
