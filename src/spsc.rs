use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError,
};
use crate::wait::{block_recv, block_send, ring_capacity, CachePadded};

struct Inner<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    tx_alive: AtomicBool,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    fn capacity(&self) -> usize {
        self.mask + 1
    }

    fn len(&self) -> usize {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            if self.head.load(Ordering::Relaxed) == head {
                return tail.wrapping_sub(head).min(self.capacity());
            }
        }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let mut i = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        while i != tail {
            unsafe {
                self.buf[i & self.mask].get_mut().assume_init_drop();
            }
            i = i.wrapping_add(1);
        }
    }
}

pub struct Sender<T> {
    inner: Arc<Inner<T>>,
    cached_head: Cell<usize>,
}

pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
    cached_tail: Cell<usize>,
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let cap = ring_capacity(capacity, 1);
    let buf: Vec<UnsafeCell<MaybeUninit<T>>> = (0..cap)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();
    let inner = Arc::new(Inner {
        buf: buf.into_boxed_slice(),
        mask: cap - 1,
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
        tx_alive: AtomicBool::new(true),
        rx_alive: AtomicBool::new(true),
    });
    (
        Sender {
            inner: Arc::clone(&inner),
            cached_head: Cell::new(0),
        },
        Receiver {
            inner,
            cached_tail: Cell::new(0),
        },
    )
}

impl<T> Sender<T> {
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let inner = &*self.inner;
        if !inner.rx_alive.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(value));
        }
        let tail = inner.tail.load(Ordering::Relaxed);
        if tail.wrapping_sub(self.cached_head.get()) == inner.capacity() {
            self.cached_head.set(inner.head.load(Ordering::Acquire));
            if tail.wrapping_sub(self.cached_head.get()) == inner.capacity() {
                return Err(TrySendError::Full(value));
            }
        }
        unsafe {
            (*inner.buf[tail & inner.mask].get()).write(value);
        }
        inner.tail.store(tail.wrapping_add(1), Ordering::Release);
        Ok(())
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
        !self.inner.rx_alive.load(Ordering::Acquire)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.tx_alive.store(false, Ordering::Release);
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("spsc::Sender")
            .field("capacity", &self.capacity())
            .finish()
    }
}

impl<T> Receiver<T> {
    fn pop(&self) -> Option<T> {
        let inner = &*self.inner;
        let head = inner.head.load(Ordering::Relaxed);
        if head == self.cached_tail.get() {
            self.cached_tail.set(inner.tail.load(Ordering::Acquire));
            if head == self.cached_tail.get() {
                return None;
            }
        }
        let value = unsafe { (*inner.buf[head & inner.mask].get()).assume_init_read() };
        inner.head.store(head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(v) = self.pop() {
            return Ok(v);
        }
        if self.inner.tx_alive.load(Ordering::Acquire) {
            return Err(TryRecvError::Empty);
        }
        match self.pop() {
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
        !self.inner.tx_alive.load(Ordering::Acquire)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.rx_alive.store(false, Ordering::Release);
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("spsc::Receiver")
            .field("capacity", &self.capacity())
            .finish()
    }
}

receiver_iters!();
