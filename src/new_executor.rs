use std::cell::{Cell, RefCell};

use async_task::Runnable;
use futures_lite::Future;

use crate::queues::{GlobalQueue, LocalQueue};

static GLOBAL_QUEUE: once_cell::sync::Lazy<GlobalQueue> =
    once_cell::sync::Lazy::new(GlobalQueue::new);

static GLOBAL_QUEUE_EVT: once_cell::sync::Lazy<async_event::Event> =
    once_cell::sync::Lazy::new(async_event::Event::new);

thread_local! {
    static LOCAL_QUEUE: LocalQueue<'static> = GLOBAL_QUEUE.subscribe();


    static LOCAL_QUEUE_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static LOCAL_QUEUE_RUNNING: Cell<bool> = const { Cell::new(false) };

    static LOCAL_QUEUE_HOLDING: RefCell<Vec<Runnable>> = const { RefCell::new(vec![]) };
}

/// Runs a queue
pub async fn run_local_queue() {
    LOCAL_QUEUE_ACTIVE.with(|r| r.set(true));
    scopeguard::defer!(LOCAL_QUEUE_ACTIVE.with(|r| r.set(false)));
    loop {
        for _ in 0..200 {
            let runnable = GLOBAL_QUEUE_EVT
                .wait_until(|| LOCAL_QUEUE.with(|q| q.pop()))
                .await;
            LOCAL_QUEUE_RUNNING.with(|r| r.set(true));
            runnable.run();
            LOCAL_QUEUE_RUNNING.with(|r| r.set(false));
        }
        futures_lite::future::yield_now().await;
    }
}

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (runnable, task) = async_task::spawn(future, |runnable| {
        if fastrand::u8(..) == 0 || !LOCAL_QUEUE_ACTIVE.get() {
            log::trace!("pushed to global queue");
            GLOBAL_QUEUE.push(runnable);
        } else {
            log::trace!("pushed to local queue");
            LOCAL_QUEUE.with(|lq| lq.push(runnable));
        }
        if !LOCAL_QUEUE_RUNNING.get() {
            GLOBAL_QUEUE_EVT.notify(1);
        }
    });
    runnable.schedule();
    task
}

/// Globally rebalance.
pub fn global_rebalance() {
    GLOBAL_QUEUE_EVT.notify(1);
}
