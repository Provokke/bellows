mod common;

use std::thread;
use std::time::{Duration, Instant};

use bellows::spsc::{channel, Receiver, Sender};
use bellows::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError,
};
use common::{count, counter, Tracked};

fn assert_send<T: Send>() {}

#[test]
fn endpoints_are_send() {
    assert_send::<Sender<String>>();
    assert_send::<Receiver<String>>();
}

#[test]
fn capacity_rounds_up_to_power_of_two() {
    assert_eq!(channel::<u8>(1).0.capacity(), 1);
    assert_eq!(channel::<u8>(3).0.capacity(), 4);
    assert_eq!(channel::<u8>(64).1.capacity(), 64);
    assert_eq!(channel::<u8>(1000).0.capacity(), 1024);
}

#[test]
#[should_panic(expected = "non-zero")]
fn zero_capacity_panics() {
    let _ = channel::<u8>(0);
}

#[test]
fn values_come_out_in_order() {
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
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    let err = tx.try_send(3).unwrap_err();
    assert!(err.is_full());
    assert_eq!(err.into_inner(), 3);
    assert_eq!(rx.try_recv(), Ok(1));
    tx.try_send(3).unwrap();
    assert_eq!(rx.try_recv(), Ok(2));
    assert_eq!(rx.try_recv(), Ok(3));
}

#[test]
fn empty_channel_reports_empty() {
    let (_tx, rx) = channel::<u32>(4);
    assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
}

#[test]
fn len_tracks_contents() {
    let (tx, rx) = channel(4);
    assert!(tx.is_empty() && rx.is_empty());
    tx.try_send('a').unwrap();
    tx.try_send('b').unwrap();
    assert_eq!(tx.len(), 2);
    assert_eq!(rx.len(), 2);
    tx.try_send('c').unwrap();
    tx.try_send('d').unwrap();
    assert!(tx.is_full());
    rx.try_recv().unwrap();
    assert_eq!(rx.len(), 3);
    assert!(!tx.is_full());
}

#[test]
fn indices_wrap_cleanly_on_a_tiny_ring() {
    let (tx, rx) = channel(4);
    for i in 0..50_000u32 {
        tx.try_send(i).unwrap();
        assert_eq!(rx.try_recv(), Ok(i));
    }
}

#[test]
fn wrap_with_partial_fill() {
    let (tx, rx) = channel(4);
    let mut next_out = 0u32;
    let mut next_in = 0u32;
    for round in 0..10_000 {
        let burst = 1 + (round % 4);
        for _ in 0..burst {
            if tx.try_send(next_in).is_ok() {
                next_in += 1;
            }
        }
        for _ in 0..(1 + (round % 3)) {
            if let Ok(v) = rx.try_recv() {
                assert_eq!(v, next_out);
                next_out += 1;
            }
        }
    }
    while let Ok(v) = rx.try_recv() {
        assert_eq!(v, next_out);
        next_out += 1;
    }
    assert_eq!(next_out, next_in);
}

#[test]
fn receiver_drains_after_sender_drops() {
    let (tx, rx) = channel(4);
    tx.try_send(1).unwrap();
    tx.try_send(2).unwrap();
    drop(tx);
    assert!(rx.is_disconnected());
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.recv(), Ok(2));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    assert_eq!(rx.recv(), Err(RecvError));
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(5)),
        Err(RecvTimeoutError::Disconnected)
    );
}

#[test]
fn sender_gets_value_back_after_receiver_drops() {
    let (tx, rx) = channel(4);
    drop(rx);
    assert!(tx.is_disconnected());
    match tx.try_send(String::from("lost")) {
        Err(TrySendError::Disconnected(s)) => assert_eq!(s, "lost"),
        other => panic!("unexpected {:?}", other),
    }
    assert_eq!(
        tx.send(String::from("7")),
        Err(SendError(String::from("7")))
    );
}

#[test]
fn leftovers_are_dropped_exactly_once() {
    let drops = counter();
    let (tx, rx) = channel(8);
    for i in 0..5 {
        tx.try_send(Tracked::new(i, &drops)).unwrap();
    }
    drop(rx.try_recv().unwrap());
    drop(rx.try_recv().unwrap());
    assert_eq!(count(&drops), 2);
    drop(tx);
    assert_eq!(count(&drops), 2);
    drop(rx);
    assert_eq!(count(&drops), 5);
}

#[test]
fn leftovers_dropped_when_wrapped() {
    let drops = counter();
    let (tx, rx) = channel(4);
    for i in 0..3 {
        tx.try_send(Tracked::new(i, &drops)).unwrap();
        drop(rx.try_recv().unwrap());
    }
    for i in 3..6 {
        tx.try_send(Tracked::new(i, &drops)).unwrap();
    }
    assert_eq!(count(&drops), 3);
    drop(tx);
    drop(rx);
    assert_eq!(count(&drops), 6);
}

#[test]
fn zero_sized_values_work() {
    let (tx, rx) = channel::<()>(8);
    for _ in 0..8 {
        tx.try_send(()).unwrap();
    }
    assert!(tx.try_send(()).is_err());
    for _ in 0..8 {
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
    let (tx, _rx) = channel(1);
    tx.send(1).unwrap();
    let start = Instant::now();
    match tx.send_timeout(2, Duration::from_millis(30)) {
        Err(SendTimeoutError::Timeout(v)) => assert_eq!(v, 2),
        other => panic!("unexpected {:?}", other),
    }
    assert!(start.elapsed() >= Duration::from_millis(30));
}

#[test]
fn recv_timeout_succeeds_when_value_arrives() {
    let (tx, rx) = channel(2);
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        tx.send(99).unwrap();
    });
    assert_eq!(rx.recv_timeout(Duration::from_secs(10)), Ok(99));
    handle.join().unwrap();
}

#[test]
fn blocked_sender_wakes_when_space_appears() {
    let (tx, rx) = channel(1);
    tx.send(1).unwrap();
    let handle = thread::spawn(move || tx.send(2).unwrap());
    thread::sleep(Duration::from_millis(30));
    assert_eq!(rx.recv(), Ok(1));
    handle.join().unwrap();
    assert_eq!(rx.recv(), Ok(2));
}

#[test]
fn blocked_receiver_wakes_on_send() {
    let (tx, rx) = channel(1);
    let handle = thread::spawn(move || rx.recv().unwrap());
    thread::sleep(Duration::from_millis(30));
    tx.send("hello").unwrap();
    assert_eq!(handle.join().unwrap(), "hello");
}

#[test]
fn blocked_receiver_wakes_on_disconnect() {
    let (tx, rx) = channel::<u8>(1);
    let handle = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(30));
    drop(tx);
    assert_eq!(handle.join().unwrap(), Err(RecvError));
}

#[test]
fn blocked_sender_wakes_on_disconnect() {
    let (tx, rx) = channel(1);
    tx.send(1).unwrap();
    let handle = thread::spawn(move || tx.send(2));
    thread::sleep(Duration::from_millis(30));
    drop(rx);
    assert_eq!(handle.join().unwrap(), Err(SendError(2)));
}

#[test]
fn iter_runs_until_the_sender_is_gone() {
    let (tx, rx) = channel(4);
    let handle = thread::spawn(move || {
        for i in 0..100 {
            tx.send(i).unwrap();
        }
    });
    let got: Vec<i32> = rx.iter().collect();
    handle.join().unwrap();
    assert_eq!(got, (0..100).collect::<Vec<_>>());
}

#[test]
fn try_iter_stops_at_empty() {
    let (tx, rx) = channel(8);
    for i in 0..5 {
        tx.try_send(i).unwrap();
    }
    assert_eq!(rx.try_iter().collect::<Vec<_>>(), [0, 1, 2, 3, 4]);
    assert_eq!(rx.try_iter().count(), 0);
}

#[test]
fn borrowed_and_owned_into_iter() {
    let (tx, rx) = channel(8);
    for i in 0..3 {
        tx.try_send(i).unwrap();
    }
    let mut seen = Vec::new();
    for v in rx.try_iter() {
        seen.push(v);
    }
    tx.try_send(10).unwrap();
    drop(tx);
    seen.extend(rx);
    assert_eq!(seen, [0, 1, 2, 10]);
}

fn run_ordered(capacity: usize, total: u64) {
    let (tx, rx) = channel(capacity);
    let producer = thread::spawn(move || {
        for i in 0..total {
            tx.send(i).unwrap();
        }
    });
    let mut expected = 0u64;
    let mut sum = 0u64;
    for v in rx {
        assert_eq!(v, expected);
        expected += 1;
        sum += v;
    }
    producer.join().unwrap();
    assert_eq!(expected, total);
    assert_eq!(sum, total * (total - 1) / 2);
}

#[test]
fn stress_one_million_through_capacity_one() {
    run_ordered(1, 1_000_000);
}

#[test]
fn stress_one_million_through_capacity_64() {
    run_ordered(64, 1_000_000);
}

#[test]
fn stress_one_million_through_capacity_4096() {
    run_ordered(4096, 1_000_000);
}

#[test]
fn stress_spinning_on_try_calls() {
    let (tx, rx) = channel(16);
    let total = 300_000u32;
    let producer = thread::spawn(move || {
        let mut i = 0;
        while i < total {
            if tx.try_send(i).is_ok() {
                i += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    });
    let mut next = 0;
    while next < total {
        match rx.try_recv() {
            Ok(v) => {
                assert_eq!(v, next);
                next += 1;
            }
            Err(TryRecvError::Empty) => std::hint::spin_loop(),
            Err(TryRecvError::Disconnected) => panic!("producer vanished early"),
        }
    }
    producer.join().unwrap();
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

#[test]
fn stress_heap_payloads_survive_intact() {
    let (tx, rx) = channel::<(u32, String)>(32);
    let total = 100_000u32;
    let producer = thread::spawn(move || {
        for i in 0..total {
            tx.send((i, format!("item-{}-{}", i, i.wrapping_mul(2654435761))))
                .unwrap();
        }
    });
    let mut next = 0;
    for (i, text) in rx {
        assert_eq!(i, next);
        assert_eq!(text, format!("item-{}-{}", i, i.wrapping_mul(2654435761)));
        next += 1;
    }
    producer.join().unwrap();
    assert_eq!(next, total);
}

#[test]
fn stress_receiver_quits_midway_and_nothing_leaks() {
    let drops = counter();
    let created = 50_000usize;
    let (tx, rx) = channel(16);
    let producer_drops = drops.clone();
    let producer = thread::spawn(move || {
        let mut sent = 0;
        for i in 0..created {
            match tx.send(Tracked::new(i, &producer_drops)) {
                Ok(()) => sent += 1,
                Err(SendError(_)) => break,
            }
        }
        sent
    });
    for (taken, item) in rx.iter().take(20_000).enumerate() {
        assert_eq!(item.id, taken);
    }
    drop(rx);
    let sent = producer.join().unwrap();
    assert!(sent >= 20_000);
    assert_eq!(count(&drops), sent + usize::from(sent < created));
}

#[test]
fn stress_ping_pong() {
    let (to_b, from_a) = channel::<u64>(1);
    let (to_a, from_b) = channel::<u64>(1);
    let echo = thread::spawn(move || {
        for v in from_a {
            to_a.send(v + 1).unwrap();
        }
    });
    for i in 0..50_000u64 {
        to_b.send(i).unwrap();
        assert_eq!(from_b.recv(), Ok(i + 1));
    }
    drop(to_b);
    echo.join().unwrap();
}

