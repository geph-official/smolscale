use std::thread::available_parallelism;

use futures_lite::Future;
use once_cell::sync::Lazy;

use crate::{thread::ExecutorThread, SINGLE_THREAD};

static THREADS: Lazy<Vec<ExecutorThread>> = Lazy::new(|| {
    let thread_count = if SINGLE_THREAD.load(std::sync::atomic::Ordering::Relaxed) {
        1
    } else {
        available_parallelism().unwrap().get()
    };
    let mut threads = Vec::new();
    for _ in 0..thread_count {
        threads.push(ExecutorThread::new());
    }
    threads
});

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let thread_id = fastrand::usize(..THREADS.len());
    let (runnable, task) = async_task::spawn(future, move |runnable| {
        THREADS[thread_id].schedule(runnable);
    });
    runnable.schedule();
    task
}
