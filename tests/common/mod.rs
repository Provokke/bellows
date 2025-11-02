#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Tracked {
    pub id: usize,
    drops: Arc<AtomicUsize>,
}

impl Tracked {
    pub fn new(id: usize, drops: &Arc<AtomicUsize>) -> Tracked {
        Tracked {
            id,
            drops: Arc::clone(drops),
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

pub fn count(c: &Arc<AtomicUsize>) -> usize {
    c.load(Ordering::SeqCst)
}
