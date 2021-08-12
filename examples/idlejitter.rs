use std::time::{Duration, Instant};

fn main() {
    // smolscale::permanently_single_threaded();
    smolscale::block_on(async move {
        for _ in 0..1000 {
            smolscale::spawn(async move {
                loop {
                    futures_lite::future::yield_now().await;
                }
            })
            .detach();
        }

        let mut accum = 1.0;
        for iter in 0u64.. {
            let start = Instant::now();
            async_io::Timer::after(Duration::from_millis(1)).await;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            accum = accum * 0.99 + elapsed_ms * 0.01;
            if iter % 1000 == 0 {
                eprintln!("drift {} µs", (accum - 1.0) * 1000.0)
            }
        }
    })
}
