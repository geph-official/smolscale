use async_channel::{Receiver, Sender};
use async_executor::Task;
use futures_lite::{FutureExt, StreamExt};
use futures_util::stream::FuturesUnordered;

/// Experimental: a "reaper" for `async_task` tasks that kills all that's inside when dropped, yet does not leak handles to tasks that have already died.
pub struct TaskReaper<T> {
    send_task: Sender<Task<T>>,
    _reaper: Task<()>,
}

impl<T: Send + 'static> TaskReaper<T> {
    /// Create a new reaper.
    pub fn new() -> Self {
        let (send_task, recv_task) = async_channel::unbounded();
        let _reaper = crate::spawn(reaper_loop(recv_task));
        Self { send_task, _reaper }
    }

    /// Attach a task to this reaper.
    pub fn attach(&self, task: Task<T>) {
        let _ = self.send_task.try_send(task);
    }
}

async fn reaper_loop<T>(recv_task: Receiver<Task<T>>) {
    let mut inner = FuturesUnordered::new();
    loop {
        let next = async { recv_task.recv().await }
            .or(async {
                inner.next().await;
                std::future::pending().await
            })
            .await;
        if let Ok(next) = next {
            inner.push(next)
        } else {
            return;
        }
    }
}
