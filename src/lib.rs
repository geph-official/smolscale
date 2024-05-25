#![doc = include_str!("../README.md")]

use async_compat::CompatExt;
use backtrace::Backtrace;
use cfg_if::cfg_if;
use dashmap::DashMap;
use fastcounter::FastCounter;
use futures_lite::prelude::*;
use once_cell::sync::Lazy;
use std::io::Write;
use std::{
    io::stderr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
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
const MONITOR_MS: u64 = 10;

cfg_if! {
    if #[cfg(feature = "preempt")] {
        const MAX_THREADS: usize = 1500;
        static POLL_COUNT: Lazy<FastCounter> = Lazy::new(Default::default);
        static FUTURES_BEING_POLLED: Lazy<FastCounter> = Lazy::new(Default::default);
    }
}

static THREAD_COUNT: AtomicUsize = AtomicUsize::new(0);

static MONITOR: OnceLock<std::thread::JoinHandle<()>> = OnceLock::new();

static SINGLE_THREAD: Lazy<AtomicBool> =
    Lazy::new(|| AtomicBool::new(std::env::var("SMOLSCALE_SINGLE").is_ok()));

static SMOLSCALE_USE_AGEX: Lazy<bool> = Lazy::new(|| std::env::var("SMOLSCALE_USE_AGEX").is_ok());

static SMOLSCALE_PROFILE: Lazy<bool> = Lazy::new(|| std::env::var("SMOLSCALE_PROFILE").is_ok());

/// Irrevocably puts smolscale into single-threaded mode.
#[inline]
pub fn permanently_single_threaded() {
    SINGLE_THREAD.store(true, Ordering::Relaxed);
}

/// Returns the number of running threads.
#[inline]
pub fn running_threads() -> usize {
    THREAD_COUNT.load(Ordering::Relaxed)
}

fn start_monitor() {
    MONITOR.get_or_init(|| {
        std::thread::Builder::new()
            .name("sscale-mon".into())
            .spawn(monitor_loop)
            .expect("Couldn't spawn MONITOR thread")
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
                // let local_exec = LEXEC.with(|v| Rc::clone(v));
                let future = async {
                    scopeguard::defer!({
                        THREAD_COUNT.fetch_sub(1, Ordering::Relaxed);
                    });
                    // let run_local = local_exec.run(std::future::pending());
                    if exitable {
                        new_executor::run_local_queue()
                            .or(async {
                                async_io::Timer::after(Duration::from_secs(3)).await;
                            })
                            .await;
                    } else {
                        new_executor::run_local_queue().await;
                    }
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
        new_executor::global_rebalance();
        if SINGLE_THREAD.load(Ordering::Relaxed) {
            return;
        }
        #[cfg(not(feature = "preempt"))]
        {
            std::thread::sleep(Duration::from_millis(MONITOR_MS));
        }
        #[cfg(feature = "preempt")]
        {
            let before_sleep = POLL_COUNT.count();
            std::thread::sleep(Duration::from_millis(MONITOR_MS));
            let after_sleep = POLL_COUNT.count();
            let running_tasks = FUTURES_BEING_POLLED.count();
            let running_threads = running_threads();
            if after_sleep == before_sleep
                && running_threads <= MAX_THREADS
                && token_bucket > 0
                && running_tasks >= running_threads
            {
                start_thread(true, false);
                token_bucket -= 1;
            }
        }
    }
    unreachable!()
}

/// Spawns a future onto the global executor and immediately blocks on it.`
pub fn block_on<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> T {
    async_io::block_on(WrappedFuture::new(future).compat())
}

/// Spawns a task onto the lazily-initialized global executor.
pub fn spawn<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> async_executor::Task<T> {
    start_monitor();
    log::trace!("monitor started");
    if *SMOLSCALE_USE_AGEX {
        async_global_executor::spawn(future)
    } else {
        new_executor::spawn(WrappedFuture::new(future))
    }
}

struct WrappedFuture<T, F: Future<Output = T>> {
    task_id: u64,
    spawn_btrace: Option<Arc<Backtrace>>,
    fut: F,
}

static ACTIVE_TASKS: Lazy<FastCounter> = Lazy::new(Default::default);

/// Returns the current number of active tasks.
#[inline]
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
        #[cfg(feature = "preempt")]
        {
            POLL_COUNT.incr();
            FUTURES_BEING_POLLED.incr();
            scopeguard::defer!(FUTURES_BEING_POLLED.decr());
        }
        let task_id = self.task_id;
        let btrace = self.spawn_btrace.as_ref().map(Arc::clone);
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
                    active_task_count(),
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
