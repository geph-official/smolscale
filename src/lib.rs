//! A global, auto-scaling, preemptive scheduler using work-balancing.
//!
//! ## What? Another executor?
//!
//! `smolscale` is a **work-balancing** executor based on [async-task], designed to be a drop-in replacement to `smol` and `async-global-executor`. It is designed based on the thesis that work-stealing, the usual approach in async executors like `async-executor` and `tokio`, is not the right algorithm for scheduling huge amounts of tiny, interdependent work units, which are what message-passing futures end up being. Instead, `smolscale` uses *work-balancing*, an approach also found in Erlang, where a global "balancer" thread periodically balances work between workers, but workers do not attempt to steal tasks from each other. This avoids the extremely frequent stealing attempts that work-stealing schedulers generate when applied to async tasks.
//!
//! `smolscale`'s approach especially excels in two circumstances:
//! - **When the CPU cores are not fully loaded**: Traditional work stealing optimizes for the case where most workers have work to do, which is only the case in fully-loaded scenarios. When workers often wake up and go back to sleep, however, a lot of CPU time is wasted stealing work. `smolscale` will instead drastically reduce CPU usage in these circumstances --- a `async-executor` app that takes 80% of CPU time may now take only 20%. Although this does not improve fully-loaded throughput, it significantly reduces power consumption and does increase throughput in circumstances where multiple thread pools compete for CPU time.
//! - **When a lot of message-passing is happening**: Message-passing workloads often involve tasks quickly waking up and going back to sleep. In a work-stealing scheduler, this again floods the scheduler with stealing requests. `smolscale` can significantly improve throughput, especially compared to executors like `async-executor` that do not special-case message passing.
//!
//! Furthermore, smolscale has a **preemptive** thread pool that ensures that tasks cannot block other tasks no matter what. This means that you can do things like run expensive computations or even do blocking I/O within a task without worrying about causing deadlocks. Even with "traditional" tasks that do not block, this approach can reduce worst-case latency. Preemption is heavily inspired by Stjepan Glavina's [previous work on async-std](https://async.rs/blog/stop-worrying-about-blocking-the-new-async-std-runtime/).
//!
//! `smolscale` also experimentally includes `Nursery`, a helper for [structured concurrency](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/) on the `smolscale` global executor.
//!
//! ## Show me the benchmarks!
//!
//! Right now, `smolscale` uses a very naive implementation (for example, stealable local queues are implemented as SPSC queues with a spinlock on the consumer side, and worker parking is done naively through `event-listener`), and its performance is expected to drastically increase. However, at most tasks it is already much faster than `async-global-executor` (the de-facto standard "non-Tokio-world" executor, which powers `async-std`), sometimes an order of magnitude faster. Here are some unscientific benchmark results; percentages are compared to `async-global-executor`:
//! ```
//! spawn_one               time:   [105.08 ns 105.21 ns 105.36 ns]                      
//!                         change: [-98.570% -98.549% -98.530%] (p = 0.00 < 0.05)
//!                         Performance has improved.
//!
//! spawn_many              time:   [3.0585 ms 3.0598 ms 3.0613 ms]                        
//!                         change: [-87.576% -87.291% -86.948%] (p = 0.00 < 0.05)
//!                         Performance has improved.
//!
//! yield_now               time:   [4.1676 ms 4.1917 ms 4.2166 ms]                       
//!                         change: [-50.455% -49.994% -49.412%] (p = 0.00 < 0.05)
//!                         Performance has improved.
//
//! ping_pong               time:   [8.5389 ms 8.6990 ms 8.8525 ms]                      
//!                         change: [+12.264% +14.548% +16.917%] (p = 0.00 < 0.05)
//!                         Performance has regressed.
//!
//! Benchmarking spawn_executors_recursively:
//! spawn_executors_recursively                                                                            
//!                         time:   [180.26 ms 180.40 ms 180.56 ms]
//!                         change: [+497.14% +500.08% +502.97%] (p = 0.00 < 0.05)
//!                         Performance has regressed.
//!
//! context_switch_quiet    time:   [100.67 us 102.05 us 103.07 us]                                 
//!                         change: [-42.789% -41.170% -39.490%] (p = 0.00 < 0.05)
//!                         Performance has improved.
//!
//! context_switch_busy     time:   [8.7637 ms 8.9012 ms 9.0561 ms]                                
//!                         change: [+3.3147% +5.5719% +7.6684%] (p = 0.00 < 0.05)
//!                         Performance has regressed.
//!```

use fastcounter::FastCounter;
use futures_lite::prelude::*;
use once_cell::sync::{Lazy, OnceCell};
use std::{
    pin::Pin,
    sync::atomic::AtomicUsize,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
    time::{Duration, Instant},
};
mod executor;
mod fastcounter;
mod nursery;
pub use executor::*;
pub use nursery::*;

//const CHANGE_THRESH: u32 = 10;
const MONITOR_MS: u64 = 5;

const MAX_THREADS: usize = 1500;

// thread_local! {
//     static LEXEC: Rc<async_executor::LocalExecutor<'static>> = Rc::new(async_executor::LocalExecutor::new())
// }
static EXEC: Lazy<Executor> = Lazy::new(Executor::new);

static POLL_COUNT: Lazy<FastCounter> = Lazy::new(Default::default);

static THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

static MONITOR: OnceCell<std::thread::JoinHandle<()>> = OnceCell::new();

static SINGLE_THREAD: AtomicBool = AtomicBool::new(false);

/// Irrevocably puts smolscale into single-threaded mode.
pub fn permanently_single_threaded() {
    SINGLE_THREAD.store(true, Ordering::Relaxed);
}

fn start_monitor() {
    MONITOR.get_or_init(|| {
        std::thread::Builder::new()
            .name("sscale-mon".into())
            .spawn(monitor_loop)
            .unwrap()
    });
}

fn monitor_loop() {
    fn start_thread(exitable: bool, process_io: bool) {
        THREAD_COUNT.fetch_add(1, Ordering::Relaxed);
        std::thread::Builder::new()
            .name(
                if exitable {
                    "sscale-wkr-e"
                } else {
                    "sscale-wkr-c"
                }
                .into(),
            )
            .stack_size(10 * 1024 * 1024)
            .spawn(move || {
                // let local_exec = LEXEC.with(|v| Rc::clone(v));
                let future = async {
                    scopeguard::defer!({
                        THREAD_COUNT.fetch_sub(1, Ordering::Relaxed);
                    });
                    // let run_local = local_exec.run(futures_lite::future::pending::<()>());
                    if exitable {
                        EXEC.worker()
                            .run()
                            .or(async {
                                async_io::Timer::after(Duration::from_secs(3)).await;
                            })
                            .await;
                    } else {
                        EXEC.worker().run().await;
                    };
                };
                if process_io {
                    async_io::block_on(future)
                } else {
                    futures_lite::future::block_on(future)
                }
            })
            .unwrap();
    }
    if SINGLE_THREAD.load(Ordering::Relaxed) {
        start_thread(false, true);
        return;
    } else {
        for _ in 0..num_cpus::get() {
            start_thread(false, true);
        }
    }

    // "Token bucket"
    let mut token_bucket = 100;
    for count in 0u64.. {
        if count % 100 == 0 && token_bucket < 100 {
            token_bucket += 1
        }
        EXEC.rebalance();
        if SINGLE_THREAD.load(Ordering::Relaxed) {
            return;
        }
        let before_sleep = POLL_COUNT.count();
        std::thread::sleep(Duration::from_millis(MONITOR_MS));
        let after_sleep = POLL_COUNT.count();
        let running_threads = THREAD_COUNT.load(Ordering::Relaxed);
        if after_sleep == before_sleep && running_threads <= MAX_THREADS && token_bucket > 0 {
            start_thread(true, false);
            token_bucket -= 1;
        }
    }
    unreachable!()
}

/// Spawns a future onto the global executor and immediately blocks on it.
pub fn block_on<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> T {
    futures_lite::future::block_on(spawn(future))
}

/// Spawns a task onto the lazily-initialized global executor.
///
/// The task can block or run CPU-intensive code if needed --- it will not block other tasks.
pub fn spawn<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> async_executor::Task<T> {
    start_monitor();
    EXEC.spawn(WrappedFuture::new(future))
    // async_global_executor::spawn(future)
}

// /// Spawns a task onto the lazily-initialized thread-local executor.
// ///
// /// The task should **NOT** block or run CPU-intensive code
// pub fn spawn<T: 'static>(
//     future: impl Future<Output = T> + 'static,
// ) -> async_executor::Task<T> {
//     start_monitor();
//     LEXEC.with(|v| v.spawn(future))
//     // async_global_executor::spawn(future)
// }

struct WrappedFuture<T, F: Future<Output = T>> {
    fut: F,
}

static ACTIVE_TASKS: Lazy<FastCounter> = Lazy::new(Default::default);

/// Returns the current number of active tasks.
pub fn active_task_count() -> usize {
    ACTIVE_TASKS.count()
}

// /// Returns the current number of running tasks.
// pub fn running_task_count() -> usize {
//     FUTURES_BEING_POLLED.count()
// }

impl<T, F: Future<Output = T>> Drop for WrappedFuture<T, F> {
    fn drop(&mut self) {
        ACTIVE_TASKS.decr();
    }
}

impl<T, F: Future<Output = T>> Future for WrappedFuture<T, F> {
    type Output = T;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        POLL_COUNT.incr();
        let start = Instant::now();

        let fut = unsafe { self.map_unchecked_mut(|v| &mut v.fut) };
        let res = fut.poll(cx);
        log::trace!("poll took {:?}", start.elapsed());
        res
    }
}

impl<T, F: Future<Output = T> + 'static> WrappedFuture<T, F> {
    pub fn new(fut: F) -> Self {
        ACTIVE_TASKS.incr();
        WrappedFuture { fut }
    }
}
