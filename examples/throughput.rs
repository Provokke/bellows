use std::thread;
use std::time::Instant;

const MESSAGES: u64 = 5_000_000;

fn spsc(capacity: usize) -> f64 {
    let (tx, rx) = bellows::spsc::channel::<u64>(capacity);
    let start = Instant::now();
    let producer = thread::spawn(move || {
        for i in 0..MESSAGES {
            tx.send(i).unwrap();
        }
    });
    let mut sum = 0u64;
    for v in rx {
        sum += v;
    }
    producer.join().unwrap();
    assert_eq!(sum, MESSAGES * (MESSAGES - 1) / 2);
    MESSAGES as f64 / start.elapsed().as_secs_f64() / 1e6
}

fn mpmc(capacity: usize, producers: u64, consumers: usize) -> f64 {
    let (tx, rx) = bellows::mpmc::channel::<u64>(capacity);
    let start = Instant::now();
    let per = MESSAGES / producers;
    let senders: Vec<_> = (0..producers)
        .map(|_| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..per {
                    tx.send(i).unwrap();
                }
            })
        })
        .collect();
    drop(tx);
    let receivers: Vec<_> = (0..consumers)
        .map(|_| {
            let rx = rx.clone();
            thread::spawn(move || rx.iter().count() as u64)
        })
        .collect();
    drop(rx);
    for s in senders {
        s.join().unwrap();
    }
    let total: u64 = receivers.into_iter().map(|r| r.join().unwrap()).sum();
    assert_eq!(total, per * producers);
    total as f64 / start.elapsed().as_secs_f64() / 1e6
}

fn main() {
    for cap in [64, 1024, 65536] {
        println!("spsc  cap {:>6}: {:>7.2} M msg/s", cap, spsc(cap));
    }
    for (p, c) in [(1, 1), (2, 2), (4, 4)] {
        println!(
            "mpmc  cap   1024 {}p/{}c: {:>7.2} M msg/s",
            p,
            c,
            mpmc(1024, p, c)
        );
    }
}
