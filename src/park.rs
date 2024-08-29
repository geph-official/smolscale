use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use diatomic_waker::{WakeSink, WakeSource};

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

    pub fn unparker(&self) -> Unparker {
        Unparker {
            flag: self.flag.clone(),
            send: self.recv.source(),
        }
    }
}

pub struct Unparker {
    flag: Arc<AtomicBool>,
    send: WakeSource,
}

impl Unparker {
    pub fn unpark(&self) {
        self.flag.store(true, Ordering::Relaxed);
        std::sync::atomic::fence(Ordering::SeqCst);
        self.send.notify();
    }
}
