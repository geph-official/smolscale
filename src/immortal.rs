use std::{convert::Infallible, future::Future, time::Duration};

use async_task::Task;

/// Immortal represents a task that can never stop, unless it is explicity cancelled or dropped from the outside. We can think of this as a Task<!>, except with some nice convenience methods.
pub struct Immortal(Task<Infallible>);

impl Immortal {
    /// Directly spawns an immortal future.
    pub fn spawn<F: Future<Output = Infallible> + Send + 'static>(f: F) -> Self {
        Self(crate::spawn(f))
    }

    /// Spawns an immortal that runs a piece of code repeatedly, restarting when it returns using a particular restart strategy.
    pub fn respawn<T: Send, F: Future<Output = T> + Send>(
        strategy: RespawnStrategy,
        mut inner: impl FnMut() -> F + Send + 'static,
    ) -> Self {
        let task = crate::spawn(async move {
            loop {
                inner().await;
                match strategy {
                    RespawnStrategy::Immediate => futures_lite::future::yield_now().await,
                    RespawnStrategy::FixedDelay(delay) => {
                        async_io::Timer::after(delay).await;
                    }
                    RespawnStrategy::JitterDelay(low, high) => {
                        let low = low.min(high);
                        let delay = Duration::from_millis(fastrand::u64(
                            (low.as_millis() as u64)..=(high.as_millis() as u64),
                        ));
                        async_io::Timer::after(delay).await;
                    }
                }
            }
        });
        Self(task)
    }

    /// Takes ownership of the immortal and cancels it, waiting only when it has fully stopped running.
    ///
    /// Unless you need to wait until the immortal stops before doing something else, it's easier to just drop this immortal rather than using this method.
    pub async fn cancel(self) {
        self.0.cancel().await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RespawnStrategy {
    Immediate,
    FixedDelay(Duration),
    JitterDelay(Duration, Duration),
}
