use async_compat::CompatExt;
use async_task::Runnable;
use tachyonix::{Receiver, Sender, TrySendError};

pub struct ExecutorThread {
    send: Sender<Runnable>,
}

impl ExecutorThread {
    pub fn new() -> Self {
        let (send, recv) = tachyonix::channel(65536);
        std::thread::Builder::new()
            .name("sscale-exec".into())
            .stack_size(1_000_000)
            .spawn(move || async_io::block_on(execute_loop(recv).compat()))
            .unwrap();
        Self { send }
    }

    pub fn schedule(&self, runnable: Runnable) {
        if let Err(TrySendError::Full(runnable)) = self.send.try_send(runnable) {
            log::warn!("local queue full, waiting...");
            let _ = pollster::block_on(self.send.send(runnable));
        }
    }
}

async fn execute_loop(mut recv: Receiver<Runnable>) {
    while let Ok(runnable) = recv.recv().await {
        runnable.run();
    }
}
