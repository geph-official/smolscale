use std::{
    cell::{Cell, RefCell},
    sync::{atomic::AtomicUsize, Arc},
    task::Poll,
};

use async_task::Runnable;
use crossbeam_queue::SegQueue;
use diatomic_waker::WakeSink;
use futures_lite::Future;
use futures_util::future::poll_fn;
use st3::fifo::Worker;

use crate::{
    pool_manager::PoolManager,
    queues::{GlobalQueue, LocalQueue},
};

/// A local worker with access to global executor resources.
pub(crate) struct WorkThread {
    local_queue: Worker<Runnable>,
    fast_slot: Cell<Option<Runnable>>,
    executor_context: Arc<ExecutorContext>,
}

impl WorkThread {
    async fn run(&self, id: usize, mut parker: WakeSink) {
        let pool_manager = &self.executor_context.pool_manager;
        let injector = &self.executor_context.injector;
        let local_queue = &self.local_queue;
        let fast_slot = &self.fast_slot;

        let mut wake_registered = false;

        loop {
            if pool_manager.try_set_worker_inactive(id) {
                poll_fn(|cx| {
                    if wake_registered {
                        parker.unregister();
                        Poll::Ready(())
                    } else {
                        parker.register(cx.waker());
                        Poll::Pending
                    }
                })
                .await;
            }
        }
    }
}

struct ExecutorContext {
    injector: SegQueue<Runnable>,
    pool_manager: PoolManager,
}

/// Runs a queue
pub async fn run_local_queue() {
    todo!()
}

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    todo!()
    // let (runnable, task) = async_task::spawn(future, |runnable| {
    //     if LOCAL_QUEUE_ACTIVE.with(|r| r.get()) {
    //         let went_to_global = LOCAL_QUEUE.with(|lq| lq.push(runnable));
    //         if went_to_global {
    //             GLOBAL_QUEUE_EVT.notify(1);
    //         }
    //     } else {
    //         GLOBAL_QUEUE.push(runnable);
    //         GLOBAL_QUEUE_EVT.notify(1);
    //     }
    // });
    // runnable.schedule();
    // task
}
