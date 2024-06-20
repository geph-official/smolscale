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

static SMOLSCALE_PROFILE: Lazy<bool> = Lazy::new(|| std::env::var("SMOLSCALE_PROFILE").is_ok());

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

struct WrappedFuture<T, F: Future<Output = T>> {
    task_id: u64,
    spawn_btrace: Option<Arc<Backtrace>>,
    fut: F,
}

static ACTIVE_TASKS: Lazy<FastCounter> = Lazy::new(Default::default);

/// Returns the current number of active tasks.
pub fn active_task_count() -> usize {
    ACTIVE_TASKS.count()
}

impl<T, F: Future<Output = T>> Drop for WrappedFuture<T, F> {
    fn drop(&mut self) {
        ACTIVE_TASKS.decr();
        if *SMOLSCALE_PROFILE {
            PROFILE_MAP.remove(&self.task_id);
        }
    }
}

impl<T, F: Future<Output = T>> Future for WrappedFuture<T, F> {
    type Output = T;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let task_id = self.task_id;
        let btrace = self.spawn_btrace.clone();
        let fut = unsafe { self.map_unchecked_mut(|v| &mut v.fut) };
        if *SMOLSCALE_PROFILE && fastrand::u64(0..100) == 0 {
            let start = Instant::now();
            let result = fut.poll(cx);
            let elapsed = start.elapsed();
            let mut entry = PROFILE_MAP
                .entry(task_id)
                .or_insert_with(|| (btrace.unwrap(), Duration::from_secs(0)));
            entry.1 += elapsed * 100;
            result
        } else {
            fut.poll(cx)
        }
    }
}

impl<T, F: Future<Output = T> + 'static> WrappedFuture<T, F> {
    pub fn new(fut: F) -> Self {
        ACTIVE_TASKS.incr();
        static TASK_ID: AtomicU64 = AtomicU64::new(0);
        let task_id = TASK_ID.fetch_add(1, Ordering::Relaxed);
        WrappedFuture {
            task_id,
            spawn_btrace: if *SMOLSCALE_PROFILE {
                let bt = Arc::new(Backtrace::new());
                PROFILE_MAP.insert(task_id, (bt.clone(), Duration::from_secs(0)));
                Some(bt)
            } else {
                None
            },
            fut,
        }
    }
}

/// Profiling map.
static PROFILE_MAP: Lazy<DashMap<u64, (Arc<Backtrace>, Duration)>> = Lazy::new(|| {
    std::thread::Builder::new()
        .name("sscale-prof".into())
        .spawn(|| loop {
            let mut vv = PROFILE_MAP
                .iter()
                .map(|k| (*k.key(), k.value().clone()))
                .collect::<Vec<_>>();
            vv.sort_unstable_by_key(|s| s.1 .1);
            vv.reverse();
            eprintln!("----- SMOLSCALE PROFILE -----");
            let mut tw = TabWriter::new(stderr());
            writeln!(&mut tw, "INDEX\tTOTAL\tTASK ID\tCPU TIME\tBACKTRACE").unwrap();
            for (count, (task_id, (bt, duration))) in vv.into_iter().enumerate() {
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}\t{:?}\t{}",
                    count,
                    ACTIVE_TASKS.count(),
                    task_id,
                    duration,
                    {
                        let s = format!("{:?}", bt);
                        format!(
                            "{:?}",
                            s.lines()
                                .filter(|l| !l.contains("smolscale")
                                    && !l.contains("spawn")
                                    && !l.contains("runtime"))
                                .take(2)
                                .map(|s| s.trim())
                                .collect::<Vec<_>>()
                        )
                    }
                )
                .unwrap();
            }
            tw.flush().unwrap();
            std::thread::sleep(Duration::from_secs(10));
        })
        .unwrap();
    Default::default()
});

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nested_block_on() {
        assert_eq!(1u64, block_on(async { block_on(async { 1u64 }) }))
    }
}
