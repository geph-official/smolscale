use std::{
    cell::Cell,
    sync::Arc,
    thread::available_parallelism,
    time::{Duration, Instant},
};

use async_compat::CompatExt;
use async_task::Runnable;
use crossbeam_queue::SegQueue;

use crossbeam_utils::sync::Parker;
use futures_lite::Future;

use once_cell::sync::Lazy;
use scoped_tls::scoped_thread_local;
use st3::fifo::Worker;

use crate::{pool_manager::PoolManager, SINGLE_THREAD};

/// A local worker with access to global executor resources.
pub(crate) struct WorkThread {
    local_queue: Worker<Runnable>,
    fast_slot: Cell<Option<Runnable>>,
    executor_context: Arc<ExecutorContext>,
}

impl WorkThread {
    fn run(&self, id: usize, parker: Parker) {
        let pool_manager = &self.executor_context.pool_manager;
        let injector = &self.executor_context.injector;
        let local_queue = &self.local_queue;
        let fast_slot = &self.fast_slot;

        loop {
            // pool_manager.set_worker_inactive(id);
            // parker.park();
            if pool_manager.try_set_worker_inactive(id) {
                parker.park();
            } else if injector.is_empty() {
                // This worker could not be deactivated because it was the last
                // active worker. In such case, the call to
                // `try_set_worker_inactive` establishes a synchronization with
                // all threads that pushed tasks to the injector queue but could
                // not activate a new worker, which is why some tasks may now be
                // visible in the injector queue.
                pool_manager.set_all_workers_inactive();

                parker.park();
                // No need to call `begin_worker_search()`: this was done by the
                // thread that unparked the worker.
            } else {
                eprintln!("*********** WEIRD SITUATION HERE ************");
                pool_manager.begin_worker_search();
            }

            let mut search_start = Instant::now();
            loop {
                if let Some(injected) = injector.pop() {
                    // Spin a little here
                    while local_queue.spare_capacity() == 0 {}

                    local_queue
                        .push(injected)
                        .expect("local queue should not be full here");
                } else {
                    let mut stealers = pool_manager.shuffled_stealers(Some(id));
                    // Set how long to spin when searching for a task.
                    const MAX_SEARCH_DURATION: Duration = Duration::from_nanos(1000);
                    if stealers.all(|stealer| {
                        stealer
                            .steal_and_pop(local_queue, |n| n - n / 2)
                            .map(|(task, _)| {
                                let prev_task = fast_slot.replace(Some(task));
                                assert!(prev_task.is_none());
                            })
                            .is_err()
                    }) {
                        // Give up if unsuccessful for too long.
                        if search_start.elapsed() > MAX_SEARCH_DURATION {
                            pool_manager.end_worker_search();
                            break;
                        }

                        // Re-try.
                        continue;
                    }
                }

                // Signal the end of the search so that another worker can be
                // activated when a new task is scheduled.
                pool_manager.end_worker_search();

                // Pop tasks from the fast slot or the local queue.
                while let Some(task) = fast_slot.take().or_else(|| local_queue.pop()) {
                    task.run();
                }

                // Resume the search for tasks.
                pool_manager.begin_worker_search();
                search_start = Instant::now();
            }
        }
    }
}

struct ExecutorContext {
    injector: SegQueue<Runnable>,
    pool_manager: PoolManager,
}

static EXEC_CONTEXT: Lazy<Arc<ExecutorContext>> = Lazy::new(|| {
    let injector = SegQueue::new();
    let pool_size = if SINGLE_THREAD.load(std::sync::atomic::Ordering::Relaxed) {
        1
    } else {
        available_parallelism()
            .unwrap()
            .get()
            .min(usize::BITS as usize)
    };
    let workers = (0..pool_size)
        .map(|_| Worker::new(65536))
        .collect::<Vec<_>>();
    let stealers = workers.iter().map(|w| w.stealer()).collect::<Vec<_>>();
    let parkers = (0..pool_size).map(|_| Parker::new()).collect::<Vec<_>>();
    let unparkers = parkers
        .iter()
        .map(|p| p.unparker().clone())
        .collect::<Vec<_>>();
    let pool_manager = PoolManager::new(
        pool_size,
        stealers.into_boxed_slice(),
        unparkers.into_boxed_slice(),
    );

    let context = ExecutorContext {
        injector,
        pool_manager,
    };
    let context = Arc::new(context);

    context.pool_manager.set_all_workers_active();

    for (id, (worker, parker)) in workers.into_iter().zip(parkers.into_iter()).enumerate() {
        let thread = WorkThread {
            local_queue: worker,
            fast_slot: Default::default(),
            executor_context: context.clone(),
        };
        std::thread::Builder::new()
            .name("sscale-exec".into())
            .spawn(move || LOCAL_WORKER.set(&thread, || thread.run(id, parker)))
            .unwrap();
    }

    context
});

scoped_thread_local!(static LOCAL_WORKER: WorkThread);

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let future = future.compat();
    let (runnable, task) = async_task::spawn(future, |runnable| {
        if LOCAL_WORKER.is_set() {
            LOCAL_WORKER.with(|worker| {
                let prev_task = match worker.fast_slot.replace(Some(runnable)) {
                    Some(prev_task) => prev_task,
                    None => return,
                };

                // push the task to the local or global queue
                if let Err(prev_task) = worker.local_queue.push(prev_task) {
                    // the local queue is full, go to the global
                    worker.executor_context.injector.push(prev_task)
                }

                // wake up another worker if needed
                if worker
                    .executor_context
                    .pool_manager
                    .searching_worker_count()
                    == 0
                {
                    worker
                        .executor_context
                        .pool_manager
                        .activate_worker_relaxed()
                }
            })
        } else {
            EXEC_CONTEXT.injector.push(runnable);
            EXEC_CONTEXT.pool_manager.activate_worker();
        }
    });
    runnable.schedule();
    task
}
