//! A global, auto-scaling scheduler for [async-task] using work-balancing.
//!
//! ## What? Another executor?
//!
//! `smolscale` is a **work-balancing** executor based on [async-task], designed to be a drop-in replacement to `smol` and `async-global-executor`. It is designed based on the idea that work-stealing, the usual approach in async executors like `async-executor` and `tokio`, is not the right algorithm for scheduling huge amounts of tiny, interdependent work units, which are what message-passing futures end up being. Instead, `smolscale` uses *work-balancing*, an approach also found in Erlang, where a global "balancer" thread periodically instructs workers with no work to do to steal work from each other, but workers are not signalled to steal tasks from each other on every task scheduling. This avoids the extremely frequent stealing attempts that work-stealing schedulers generate when applied to async tasks.
//!
//! `smolscale`'s approach especially excels in two circumstances:
//! - **When the CPU cores are not fully loaded**: Traditional work stealing optimizes for the case where most workers have work to do, which is only the case in fully-loaded scenarios. When workers often wake up and go back to sleep, however, a lot of CPU time is wasted stealing work. `smolscale` will instead drastically reduce CPU usage in these circumstances --- a `async-executor` app that takes 80% of CPU time may now take only 20%. Although this does not improve fully-loaded throughput, it significantly reduces power consumption and does increase throughput in circumstances where multiple thread pools compete for CPU time.
//! - **When a lot of message-passing is happening**: Message-passing workloads often involve tasks quickly waking up and going back to sleep. In a work-stealing scheduler, this again floods the scheduler with stealing requests. `smolscale` can significantly improve throughput, especially compared to executors like `async-executor` that do not special-case message passing.

use async_compat::CompatExt;
use backtrace::Backtrace;
use dashmap::DashMap;
use fastcounter::FastCounter;
use futures_lite::prelude::*;
use once_cell::sync::{Lazy, OnceCell};
use std::io::Write;
use std::{
    io::stderr,
    pin::Pin,
    sync::atomic::AtomicUsize,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tabwriter::TabWriter;

mod fastcounter;
pub mod immortal;
mod new_executor;
mod queues;
pub mod reaper;

//const CHANGE_THRESH: u32 = 10;
const MONITOR_MS: u64 = 50;

static THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

static MONITOR: OnceCell<std::thread::JoinHandle<()>> = OnceCell::new();

static SINGLE_THREAD: AtomicBool = AtomicBool::new(false);

static SMOLSCALE_USE_AGEX: Lazy<bool> = Lazy::new(|| std::env::var("SMOLSCALE_USE_AGEX").is_ok());

/// Irrevocably puts smolscale into single-threaded mode.
pub fn permanently_single_threaded() {
    SINGLE_THREAD.store(true, Ordering::Relaxed);
}

/// Returns the number of running threads.
pub fn running_threads() -> usize {
    THREAD_COUNT.load(Ordering::Relaxed)
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
    if *SMOLSCALE_USE_AGEX {
        return;
    }
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
            .spawn(move || {
                let future = async {
                    scopeguard::defer!({
                        THREAD_COUNT.fetch_sub(1, Ordering::Relaxed);
                    });
                    // let run_local = local_exec.run(futures_lite::future::pending::<()>());
                    if exitable {
                        new_executor::run_local_queue()
                            .or(async {
                                async_io::Timer::after(Duration::from_secs(3)).await;
                            })
                            .await;
                    } else {
                        new_executor::run_local_queue().await;
                    };
                }
                .compat();
                if process_io {
                    async_io::block_on(future)
                } else {
                    futures_lite::future::block_on(future)
                }
            })
            .expect("cannot spawn thread");
    }
    if SINGLE_THREAD.load(Ordering::Relaxed) || std::env::var("SMOLSCALE_SINGLE").is_ok() {
        start_thread(false, true)
    } else {
        for _ in 0..num_cpus::get() {
            start_thread(false, true);
        }
    }

    // This loop here eventually "unstucks" the executor in the rare case that it gets stuck due to intentional use of "looser" synchronization than necessary
    loop {
        new_executor::global_rebalance();
        std::thread::sleep(Duration::from_millis(MONITOR_MS));
    }
}

/// Spawns a future onto the global executor and immediately blocks on it.`
pub fn block_on<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> T {
    async_io::block_on(future.compat())
}

/// Spawns a task onto the lazily-initialized global executor.
pub fn spawn<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> async_executor::Task<T> {
    start_monitor();

    if *SMOLSCALE_USE_AGEX {
        async_global_executor::spawn(future)
    } else {
        new_executor::spawn(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nested_block_on() {
        assert_eq!(1u64, block_on(async { block_on(async { 1u64 }) }))
    }
}
