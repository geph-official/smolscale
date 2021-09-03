use std::sync::Arc;

use rtrb::PushError;

/// Create a new sp2c
pub fn sp2c<T>() -> (Sp2cSender<T>, Sp2cReceiver<T>, Sp2cStealer<T>) {
    let ring = rtrb::RingBuffer::new(512);
    let (send, recv) = ring.split();
    let recv = Arc::new(std::sync::Mutex::new(recv));
    let sender = Sp2cSender { send };
    let receiver = Sp2cReceiver { recv: recv.clone() };
    let stealer = Sp2cStealer { recv };
    (sender, receiver, stealer)
}

/// Sending side of a sp2c
pub struct Sp2cSender<T> {
    send: rtrb::Producer<T>,
}

impl<T> Sp2cSender<T> {
    /// Sends to the other side. If the queue is full, the item is returned back.
    #[inline]
    pub fn send(&mut self, item: T) -> Result<(), T> {
        self.send.push(item).map_err(|v| match v {
            PushError::Full(v) => v,
        })
    }

    /// Length
    #[inline]
    pub fn slots(&mut self) -> usize {
        self.send.slots()
    }
}

/// "Main" receiving end for the sp2c
pub struct Sp2cReceiver<T> {
    recv: Arc<std::sync::Mutex<rtrb::Consumer<T>>>,
}

impl<T> Sp2cReceiver<T> {
    /// Pops an item from the queue. If the queue is empty, returns None.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.recv.lock().unwrap().pop().ok()
    }
}

/// "Side" receiving end for the sp2c
pub struct Sp2cStealer<T> {
    recv: Arc<std::sync::Mutex<rtrb::Consumer<T>>>,
}

impl<T> Sp2cStealer<T> {
    /// Steals a bunch of items from the queue (at most roughly half the queue), and puts them into the given buffer.
    pub fn steal_batch(&mut self, buf: &mut Vec<T>) -> Option<usize> {
        let mut recv = self.recv.try_lock().ok()?; // give up if we can't acquire the lock
        let to_pop = recv.slots() / 2;
        // TODO some kind of voodoo to not incur constant sync overhead
        for _ in 0..to_pop {
            buf.push(recv.pop().unwrap())
        }
        Some(to_pop)
    }
}
