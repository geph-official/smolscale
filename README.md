A global, auto-scaling, preemptive scheduler using work-balancing.

## What? Another executor?

`smolscale` is a **work-balancing** executor based on [async-task], designed to be a drop-in replacement to `smol` and `async-global-executor`. It is designed based on the thesis that work-stealing, the usual approach in async executors like `async-executor` and `tokio`, is not the right algorithm for scheduling huge amounts of tiny, interdependent work units, which are what message-passing futures end up being. Instead, `smolscale` uses _work-balancing_, an approach also found in Erlang, where a global "balancer" thread periodically balances work between workers, but workers do not attempt to steal tasks from each other. This avoids the extremely frequent stealing attempts that work-stealing schedulers generate when applied to async tasks.

`smolscale`'s approach especially excels in two circumstances:

- **When the CPU cores are not fully loaded**: Traditional work stealing optimizes for the case where most workers have work to do, which is only the case in fully-loaded scenarios. When workers often wake up and go back to sleep, however, a lot of CPU time is wasted stealing work. `smolscale` will instead drastically reduce CPU usage in these circumstances --- a `async-executor` app that takes 80% of CPU time may now take only 20%. Although this does not improve fully-loaded throughput, it significantly reduces power consumption and does increase throughput in circumstances where multiple thread pools compete for CPU time.
- **When a lot of message-passing is happening**: Message-passing workloads often involve tasks quickly waking up and going back to sleep. In a work-stealing scheduler, this again floods the scheduler with stealing requests. `smolscale` can significantly improve throughput, especially compared to executors like `async-executor` that do not special-case message passing.

Furthermore, smolscale has a **preemptive** thread pool that ensures that tasks cannot block other tasks no matter what. This means that you can do things like run expensive computations or even do blocking I/O within a task without worrying about causing deadlocks. Even with "traditional" tasks that do not block, this approach can reduce worst-case latency. Preemption is heavily inspired by Stjepan Glavina's [previous work on async-std](https://async.rs/blog/stop-worrying-about-blocking-the-new-async-std-runtime/).

`smolscale` also experimentally includes `Nursery`, a helper for [structured concurrency](https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/) on the `smolscale` global executor.

## Show me the benchmarks!

Right now, `smolscale` uses a very naive implementation (for example, stealable local queues are implemented as SPSC queues with a spinlock on the consumer side, and worker parking is done naively through `event-listener`), and its performance is expected to drastically increase. However, at most tasks it is already much faster than `async-global-executor` (the de-facto standard "non-Tokio-world" executor, which powers `async-std`), sometimes an order of magnitude faster. Here are some unscientific benchmark results; percentages are compared to `async-global-executor`:

```
spawn_one               time:   [105.08 ns 105.21 ns 105.36 ns]
                        change: [-98.570% -98.549% -98.530%] (p = 0.00 < 0.05)
                        Performance has improved.

spawn_many              time:   [3.0585 ms 3.0598 ms 3.0613 ms]
                        change: [-87.576% -87.291% -86.948%] (p = 0.00 < 0.05)
                        Performance has improved.

yield_now               time:   [4.1676 ms 4.1917 ms 4.2166 ms]
                        change: [-50.455% -49.994% -49.412%] (p = 0.00 < 0.05)
                        Performance has improved.
//
ping_pong               time:   [8.5389 ms 8.6990 ms 8.8525 ms]
                        change: [+12.264% +14.548% +16.917%] (p = 0.00 < 0.05)
                        Performance has regressed.

Benchmarking spawn_executors_recursively:
spawn_executors_recursively
                        time:   [180.26 ms 180.40 ms 180.56 ms]
                        change: [+497.14% +500.08% +502.97%] (p = 0.00 < 0.05)
                        Performance has regressed.

context_switch_quiet    time:   [100.67 us 102.05 us 103.07 us]
                        change: [-42.789% -41.170% -39.490%] (p = 0.00 < 0.05)
                        Performance has improved.

context_switch_busy     time:   [8.7637 ms 8.9012 ms 9.0561 ms]
                        change: [+3.3147% +5.5719% +7.6684%] (p = 0.00 < 0.05)
                        Performance has regressed.
```
