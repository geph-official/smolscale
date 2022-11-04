# smolscale

A global, auto-scaling scheduler for [async-task] using work-balancing.

### What? Another executor?

`smolscale` is a **work-balancing** executor based on [async-task], designed to be a drop-in replacement to `smol` and `async-global-executor`. It is designed based on the idea that work-stealing, the usual approach in async executors like `async-executor` and `tokio`, is not the right algorithm for scheduling huge amounts of tiny, interdependent work units, which are what message-passing futures end up being. Instead, `smolscale` uses *work-balancing*, an approach also found in Erlang, where a global "balancer" thread periodically balances work between workers, but workers do not attempt to steal tasks from each other. This avoids the extremely frequent stealing attempts that work-stealing schedulers generate when applied to async tasks.

`smolscale`'s approach especially excels in two circumstances:
- **When the CPU cores are not fully loaded**: Traditional work stealing optimizes for the case where most workers have work to do, which is only the case in fully-loaded scenarios. When workers often wake up and go back to sleep, however, a lot of CPU time is wasted stealing work. `smolscale` will instead drastically reduce CPU usage in these circumstances --- a `async-executor` app that takes 80% of CPU time may now take only 20%. Although this does not improve fully-loaded throughput, it significantly reduces power consumption and does increase throughput in circumstances where multiple thread pools compete for CPU time.
- **When a lot of message-passing is happening**: Message-passing workloads often involve tasks quickly waking up and going back to sleep. In a work-stealing scheduler, this again floods the scheduler with stealing requests. `smolscale` can significantly improve throughput, especially compared to executors like `async-executor` that do not special-case message passing.

License: ISC
