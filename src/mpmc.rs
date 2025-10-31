use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError,
};
use crate::wait::{block_recv, block_send, ring_capacity, CachePadded};

struct Slot<T> {
    seq: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

struct Inner<T> {
    buf: Box<[Slot<T>]>,
    mask: usize,
    enqueue: CachePadded<AtomicUsize>,
    dequeue: CachePadded<AtomicUsize>,
    senders: AtomicUsize,
    receivers: AtomicUsize,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    fn capacity(&self) -> usize {
        self.mask + 1
    }

    fn push(&self, value: T) -> Result<(), T> {
        let mut pos = self.enqueue.load(Ordering::Relaxed);
        loop {
            let slot = &self.buf[pos & self.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos) as isize;
            if diff == 0 {
                match self.enqueue.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        unsafe {
                            (*slot.value.get()).write(value);
                        }
                        slot.seq.store(pos.wrapping_add(1), Ordering::Release);
                        return Ok(());
                    }
                    Err(current) => pos = current,
                }
            } else if diff < 0 {
                return Err(value);
            } else {
                pos = self.enqueue.load(Ordering::Relaxed);
            }
        }
    }

    fn pop(&self) -> Option<T> {
        let mut pos = self.dequeue.load(Ordering::Relaxed);
        loop {
            let slot = &self.buf[pos & self.mask];
            let seq = slot.seq.load(Ordering::Acquire);
            let diff = seq.wrapping_sub(pos.wrapping_add(1)) as isize;
            if diff == 0 {
                match self.dequeue.compare_exchange_weak(
                    pos,
                    pos.wrapping_add(1),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let value = unsafe { (*slot.value.get()).assume_init_read() };
                        slot.seq
                            .store(pos.wrapping_add(self.capacity()), Ordering::Release);
                        return Some(value);
                    }
                    Err(current) => pos = current,
                }
            } else if diff < 0 {
                return None;
            } else {
                pos = self.dequeue.load(Ordering::Relaxed);
            }
        }
    }

    fn len(&self) -> usize {
        loop {
            let deq = self.dequeue.load(Ordering::SeqCst);
            let enq = self.enqueue.load(Ordering::SeqCst);
            if self.dequeue.load(Ordering::SeqCst) == deq {
                return enq.wrapping_sub(deq).min(self.capacity());
            }
        }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut pos = *self.dequeue.0.get_mut();
        let mask = self.mask;
        loop {
            let slot = &mut self.buf[pos & mask];
            if *slot.seq.get_mut() != pos.wrapping_add(1) {
                break;
            }
            unsafe {
                slot.value.get_mut().assume_init_drop();
            }
            pos = pos.wrapping_add(1);
        }
    }
}

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let cap = ring_capacity(capacity, 2);
    let buf: Vec<Slot<T>> = (0..cap)
        .map(|i| Slot {
            seq: AtomicUsize::new(i),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect();
    let inner = Arc::new(Inner {
        buf: buf.into_boxed_slice(),
        mask: cap - 1,
        enqueue: CachePadded(AtomicUsize::new(0)),
        dequeue: CachePadded(AtomicUsize::new(0)),
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
    });
    (
        Sender {
            inner: Arc::clone(&inner),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.inner.receivers.load(Ordering::Acquire) == 0 {
            return Err(TrySendError::Disconnected(value));
        }
        self.inner.push(value).map_err(TrySendError::Full)
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        block_send(value, None, |v| self.try_send(v)).map_err(|e| SendError(e.into_inner()))
    }

    pub fn send_timeout(&self, value: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        block_send(value, Some(timeout), |v| self.try_send(v))
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    pub fn is_disconnected(&self) -> bool {
        self.inner.receivers.load(Ordering::Acquire) == 0
    }

    pub fn receiver_count(&self) -> usize {
        self.inner.receivers.load(Ordering::Acquire)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Sender<T> {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Sender {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.senders.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("mpmc::Sender")
            .field("capacity", &self.capacity())
            .finish()
    }
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(v) = self.inner.pop() {
            return Ok(v);
        }
        if self.inner.senders.load(Ordering::Acquire) != 0 {
            return Err(TryRecvError::Empty);
        }
        match self.inner.pop() {
            Some(v) => Ok(v),
            None => Err(TryRecvError::Disconnected),
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        block_recv(None, || self.try_recv()).map_err(|_| RecvError)
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        block_recv(Some(timeout), || self.try_recv())
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_disconnected(&self) -> bool {
        self.inner.senders.load(Ordering::Acquire) == 0
    }

    pub fn sender_count(&self) -> usize {
        self.inner.senders.load(Ordering::Acquire)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Receiver<T> {
        self.inner.receivers.fetch_add(1, Ordering::Relaxed);
        Receiver {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receivers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("mpmc::Receiver")
            .field("capacity", &self.capacity())
            .finish()
    }
}

receiver_iters!();
