use std::hint;
use std::ops::Deref;
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{RecvTimeoutError, SendTimeoutError, TryRecvError, TrySendError};

#[repr(align(128))]
pub struct CachePadded<T>(pub T);

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

pub struct Backoff {
    step: u32,
}

impl Backoff {
    pub fn new() -> Backoff {
        Backoff { step: 0 }
    }

    pub fn snooze(&mut self) {
        if self.step <= 6 {
            for _ in 0..(1u32 << self.step) {
                hint::spin_loop();
            }
        } else if self.step <= 10 {
            thread::yield_now();
        } else {
            thread::sleep(Duration::from_micros(50));
        }
        if self.step <= 10 {
            self.step += 1;
        }
    }
}

fn expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}

pub fn block_send<T>(
    mut value: T,
    timeout: Option<Duration>,
    mut try_send: impl FnMut(T) -> Result<(), TrySendError<T>>,
) -> Result<(), SendTimeoutError<T>> {
    let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
    let mut backoff = Backoff::new();
    loop {
        match try_send(value) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(v)) => return Err(SendTimeoutError::Disconnected(v)),
            Err(TrySendError::Full(v)) => {
                value = v;
                if expired(deadline) {
                    return Err(SendTimeoutError::Timeout(value));
                }
                backoff.snooze();
            }
        }
    }
}

pub fn block_recv<T>(
    timeout: Option<Duration>,
    mut try_recv: impl FnMut() -> Result<T, TryRecvError>,
) -> Result<T, RecvTimeoutError> {
    let deadline = timeout.and_then(|t| Instant::now().checked_add(t));
    let mut backoff = Backoff::new();
    loop {
        match try_recv() {
            Ok(v) => return Ok(v),
            Err(TryRecvError::Disconnected) => return Err(RecvTimeoutError::Disconnected),
            Err(TryRecvError::Empty) => {
                if expired(deadline) {
                    return Err(RecvTimeoutError::Timeout);
                }
                backoff.snooze();
            }
        }
    }
}

pub fn ring_capacity(requested: usize, minimum: usize) -> usize {
    assert!(requested > 0, "channel capacity must be non-zero");
    requested
        .max(minimum)
        .checked_next_power_of_two()
        .filter(|c| *c <= usize::MAX / 4)
        .expect("channel capacity is too large")
}

