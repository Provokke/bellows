mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bellows::mpmc::{channel, Receiver, Sender};
use bellows::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError,
};
use common::{count, counter, Tracked};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn endpoints_are_send_and_sync() {
    assert_send_sync::<Sender<String>>();
    assert_send_sync::<Receiver<String>>();
}

#[test]
fn capacity_rounds_up_with_a_floor_of_two() {
    assert_eq!(channel::<u8>(1).0.capacity(), 2);
    assert_eq!(channel::<u8>(3).0.capacity(), 4);
    assert_eq!(channel::<u8>(100).1.capacity(), 128);
}

#[test]
#[should_panic(expected = "non-zero")]
fn zero_capacity_panics() {
    let _ = channel::<u8>(0);
}

#[test]
fn values_come_out_in_order_single_threaded() {
    let (tx, rx) = channel(8);
    for i in 0..8 {
        tx.try_send(i).unwrap();
    }
    for i in 0..8 {
        assert_eq!(rx.try_recv(), Ok(i));
    }
}

#[test]
fn full_channel_hands_the_value_back() {
    let (tx, rx) = channel(2);
    tx.try_send("a").unwrap();
    tx.try_send("b").unwrap();
    match tx.try_send("c") {
        Err(TrySendError::Full(v)) => assert_eq!(v, "c"),
        other => panic!("unexpected {:?}", other),
    }
    assert_eq!(rx.try_recv(), Ok("a"));
    tx.try_send("c").unwrap();
}

#[test]
fn empty_channel_reports_empty() {
    let (_tx, rx) = channel::<u32>(4);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn len_tracks_contents() {
    let (tx, rx) = channel(4);
    assert!(tx.is_empty());
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    assert_eq!(rx.len(), 2);
    tx.try_send(3).unwrap();
    tx.try_send(4).unwrap();
    assert!(tx.is_full());
    rx.try_recv().unwrap();
    assert_eq!(tx.len(), 3);
}

#[test]
fn indices_wrap_cleanly_on_a_tiny_ring() {
    let (tx, rx) = channel(2);
    for i in 0..100_000u32 {
        tx.try_send(i).unwrap();
        assert_eq!(rx.try_recv(), Ok(i));
    }
}

#[test]
fn clones_are_counted() {
    let (tx, rx) = channel::<u8>(4);
    assert_eq!(rx.sender_count(), 1);
    assert_eq!(tx.receiver_count(), 1);
    let tx2 = tx.clone();
    let rx2 = rx.clone();
    assert_eq!(rx.sender_count(), 2);
    assert_eq!(tx.receiver_count(), 2);
    drop(tx2);
    drop(rx2);
    assert_eq!(rx.sender_count(), 1);
    assert_eq!(tx.receiver_count(), 1);
}

#[test]
fn clones_share_one_queue() {
    let (tx, rx) = channel(4);
    let tx2 = tx.clone();
    let rx2 = rx.clone();
    tx.try_send(1).unwrap();
    tx2.try_send(2).unwrap();
    assert_eq!(rx2.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Ok(2));
}

#[test]
fn disconnect_needs_every_sender_gone() {
    let (tx, rx) = channel(4);
    let tx2 = tx.clone();
    tx.try_send(1).unwrap();
    drop(tx);
    assert!(!rx.is_disconnected());
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    drop(tx2);
    assert!(rx.is_disconnected());
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn receivers_drain_after_all_senders_drop() {
    let (tx, rx) = channel(8);
    let rx2 = rx.clone();
    for i in 0..4 {
        tx.try_send(i).unwrap();
    }
    drop(tx);
    assert_eq!(rx.recv(), Ok(0));
    assert_eq!(rx2.recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx2.recv(), Ok(3));
    assert_eq!(rx.recv(), Err(RecvError));
    assert_eq!(rx2.recv(), Err(RecvError));
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(5)),
        Err(RecvTimeoutError::Disconnected)
    );
}

#[test]
fn senders_fail_once_every_receiver_is_gone() {
    let (tx, rx) = channel(4);
    let rx2 = rx.clone();
    drop(rx);
    assert!(tx.try_send(1).is_ok());
    drop(rx2);
    assert!(tx.is_disconnected());
    assert_eq!(tx.send(2), Err(SendError(2)));
    assert!(tx.try_send(3).unwrap_err().is_disconnected());
}

#[test]
fn leftovers_are_dropped_exactly_once() {
    let drops = counter();
    let (tx, rx) = channel(8);
    for i in 0..6 {
        tx.try_send(Tracked::new(i, &drops)).unwrap();
    }
    drop(rx.try_recv().unwrap());
    assert_eq!(count(&drops), 1);
    drop(tx);
    assert_eq!(count(&drops), 1);
    drop(rx);
    assert_eq!(count(&drops), 6);
}

#[test]
fn leftovers_dropped_when_wrapped() {
    let drops = counter();
    let (tx, rx) = channel(4);
    for i in 0..10 {
        tx.try_send(Tracked::new(i, &drops)).unwrap();
        if i < 7 {
            drop(rx.try_recv().unwrap());
        }
    }
    assert_eq!(count(&drops), 7);
    drop(tx);
    drop(rx);
    assert_eq!(count(&drops), 10);
}

#[test]
fn zero_sized_values_work() {
    let (tx, rx) = channel::<()>(4);
    for _ in 0..4 {
        tx.try_send(()).unwrap();
    }
    assert!(tx.try_send(()).is_err());
    for _ in 0..4 {
        assert_eq!(rx.try_recv(), Ok(()));
    }
}

#[test]
fn recv_timeout_expires() {
    let (_tx, rx) = channel::<u8>(2);
    let start = Instant::now();
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(30)),
        Err(RecvTimeoutError::Timeout)
    );
    assert!(start.elapsed() >= Duration::from_millis(30));
}

#[test]
fn send_timeout_returns_the_value() {
    let (tx, _rx) = channel(2);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    match tx.send_timeout(3, Duration::from_millis(30)) {
        Err(SendTimeoutError::Timeout(v)) => assert_eq!(v, 3),
        other => panic!("unexpected {:?}", other),
    }
}

#[test]
fn blocked_sender_wakes_when_space_appears() {
    let (tx, rx) = channel(2);
    tx.send(1).unwrap();
    tx.send(2).unwrap();
    let handle = thread::spawn(move || tx.send(3).unwrap());
    thread::sleep(Duration::from_millis(30));
    assert_eq!(rx.recv(), Ok(1));
    handle.join().unwrap();
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.recv(), Ok(3));
}

#[test]
fn blocked_receivers_wake_on_disconnect() {
    let (tx, rx) = channel::<u8>(2);
    let handles: Vec<_> = (0..3)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || rx.recv())
        })
        .collect();
    drop(rx);
    thread::sleep(Duration::from_millis(30));
    drop(tx);
    for h in handles {
        assert_eq!(h.join().unwrap(), Err(RecvError));
    }
}

#[test]
fn blocked_senders_wake_on_disconnect() {
    let (tx, rx) = channel(2);
    tx.send(0).unwrap();
    tx.send(0).unwrap();
    let handles: Vec<_> = (0..3)
        .map(|i| {
            let tx = tx.clone();
            thread::spawn(move || tx.send(i + 1))
        })
        .collect();
    drop(tx);
    thread::sleep(Duration::from_millis(30));
    drop(rx);
    for h in handles {
        assert!(h.join().unwrap().is_err());
    }
}

#[test]
fn iterators() {
    let (tx, rx) = channel(8);
    for i in 0..4 {
        tx.try_send(i).unwrap();
    }
    assert_eq!(rx.try_iter().collect::<Vec<_>>(), [0, 1, 2, 3]);
    tx.try_send(9).unwrap();
    drop(tx);
    assert_eq!(rx.into_iter().collect::<Vec<_>>(), [9]);
}

struct Tag {
    producer: usize,
    seq: usize,
}

fn run_mpmc(capacity: usize, producers: usize, consumers: usize, per_producer: usize) {
    let (tx, rx) = channel::<Tag>(capacity);
    let start = Arc::new(Barrier::new(producers + consumers));
    let mut senders = Vec::new();
    for p in 0..producers {
        let tx = tx.clone();
        let start = Arc::clone(&start);
        senders.push(thread::spawn(move || {
            start.wait();
            for seq in 0..per_producer {
                tx.send(Tag { producer: p, seq }).unwrap();
            }
        }));
    }
    drop(tx);
    let mut receivers = Vec::new();
    for _ in 0..consumers {
        let rx = rx.clone();
        let start = Arc::clone(&start);
        receivers.push(thread::spawn(move || {
            start.wait();
            let mut last = vec![None::<usize>; producers];
            let mut seen = Vec::new();
            for tag in rx.iter() {
                if let Some(prev) = last[tag.producer] {
                    assert!(tag.seq > prev, "producer {} went backwards", tag.producer);
                }
                last[tag.producer] = Some(tag.seq);
                seen.push((tag.producer, tag.seq));
            }
            seen
        }));
    }
    drop(rx);
    for s in senders {
        s.join().unwrap();
    }
    let mut hits = vec![vec![0u8; per_producer]; producers];
    let mut total = 0;
    for r in receivers {
        for (p, seq) in r.join().unwrap() {
            hits[p][seq] += 1;
            total += 1;
        }
    }
    assert_eq!(total, producers * per_producer);
    assert!(hits.iter().all(|h| h.iter().all(|&c| c == 1)));
}

#[test]
fn stress_four_producers_four_consumers() {
    run_mpmc(256, 4, 4, 100_000);
}

#[test]
fn stress_many_producers_one_consumer() {
    run_mpmc(64, 8, 1, 50_000);
}

#[test]
fn stress_one_producer_many_consumers() {
    run_mpmc(64, 1, 8, 200_000);
}

#[test]
fn stress_minimum_capacity_under_contention() {
    run_mpmc(2, 4, 4, 30_000);
}

#[test]
fn stress_more_threads_than_cores() {
    run_mpmc(32, 16, 16, 10_000);
}

#[test]
fn stress_sum_of_a_million() {
    let (tx, rx) = channel::<u64>(128);
    let total = 1_000_000u64;
    let producers = 4u64;
    let handles: Vec<_> = (0..producers)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                let mut i = p;
                while i < total {
                    tx.send(i).unwrap();
                    i += producers;
                }
            })
        })
        .collect();
    drop(tx);
    let sums: Vec<_> = (0..3)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || rx.iter().sum::<u64>())
        })
        .collect();
    drop(rx);
    for h in handles {
        h.join().unwrap();
    }
    let sum: u64 = sums.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(sum, total * (total - 1) / 2);
}

#[test]
fn stress_try_calls_only() {
    let (tx, rx) = channel::<usize>(8);
    let received = Arc::new(AtomicUsize::new(0));
    let producers: Vec<_> = (0..3)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..50_000 {
                    let mut v = i;
                    while let Err(e) = tx.try_send(v) {
                        v = e.into_inner();
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    drop(tx);
    let consumers: Vec<_> = (0..3)
        .map(|_| {
            let rx = rx.clone();
            let received = Arc::clone(&received);
            thread::spawn(move || loop {
                match rx.try_recv() {
                    Ok(_) => {
                        received.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TryRecvError::Empty) => std::hint::spin_loop(),
                    Err(TryRecvError::Disconnected) => break,
                }
            })
        })
        .collect();
    drop(rx);
    for h in producers.into_iter().chain(consumers) {
        h.join().unwrap();
    }
    assert_eq!(received.load(Ordering::SeqCst), 150_000);
}

#[test]
fn stress_heap_payloads_survive_intact() {
    let (tx, rx) = channel::<(usize, String)>(16);
    let handles: Vec<_> = (0..4)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..20_000 {
                    tx.send((p, format!("p{}-{}", p, i))).unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    let consumers: Vec<_> = (0..2)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || {
                let mut n = 0;
                for (p, text) in rx.iter() {
                    assert!(text.starts_with(&format!("p{}-", p)));
                    n += 1;
                }
                n
            })
        })
        .collect();
    drop(rx);
    for h in handles {
        h.join().unwrap();
    }
    let n: usize = consumers.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(n, 80_000);
}

#[test]
fn stress_drops_balance_under_churn() {
    let drops = counter();
    let created = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = channel::<Tracked>(8);
    let producers: Vec<_> = (0..3)
        .map(|_| {
            let tx = tx.clone();
            let drops = Arc::clone(&drops);
            let created = Arc::clone(&created);
            thread::spawn(move || {
                for i in 0..30_000 {
                    created.fetch_add(1, Ordering::SeqCst);
                    if tx.send(Tracked::new(i, &drops)).is_err() {
                        break;
                    }
                }
            })
        })
        .collect();
    drop(tx);
    let consumers: Vec<_> = (0..2)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || rx.iter().take(20_000).count())
        })
        .collect();
    drop(rx);
    let taken: usize = consumers.into_iter().map(|h| h.join().unwrap()).sum();
    for h in producers {
        h.join().unwrap();
    }
    assert!(taken >= 20_000);
    assert_eq!(count(&drops), created.load(Ordering::SeqCst));
}

#[test]
fn stress_ping_pong_between_clones() {
    let (to_b, from_a) = channel::<u64>(2);
    let (to_a, from_b) = channel::<u64>(2);
    let echo = {
        let from_a = from_a.clone();
        thread::spawn(move || {
            for v in from_a {
                to_a.send(v * 2).unwrap();
            }
        })
    };
    drop(from_a);
    for i in 0..30_000u64 {
        to_b.send(i).unwrap();
        assert_eq!(from_b.recv(), Ok(i * 2));
    }
    drop(to_b);
    echo.join().unwrap();
}
