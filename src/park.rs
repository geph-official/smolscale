use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use diatomic_waker::WakeSink;

pub struct Parker {
    flag: Arc<AtomicBool>,
    recv: WakeSink,
}

impl Parker {
    pub fn new() -> Parker {
        Parker {
            flag: Arc::new(AtomicBool::new(false)),
            recv: WakeSink::new(),
        }
    }

    pub async fn park(&mut self) {
        self.recv
            .wait_until(|| {
                if self.flag.swap(false, Ordering::Relaxed) {
                    Some(())
                } else {
                    None
                }
            })
            .await
    }
}
