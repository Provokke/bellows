<div align="center">

# bellows

Bounded ring-buffer channels for Rust: a lock-free single-producer single-consumer queue and a Vyukov-style multi-producer multi-consumer queue. Standard library only, no dependencies, and a lot of stress testing.

![License](https://img.shields.io/badge/LICENSE-MIT-blue?style=for-the-badge)  ![Language](https://img.shields.io/badge/RUST-dea584?style=for-the-badge&logo=rust&logoColor=white)  ![Tests](https://img.shields.io/badge/TESTS-68-brightgreen?style=for-the-badge)

Refer to [LICENSE](LICENSE) for the legal bits.

</div>

## What's in it

- **`bellows::spsc`**, a lock-free single-producer single-consumer channel. One `Sender`, one `Receiver`, neither cloneable, so the two sides can skip every compare-and-swap. Each side caches the other's index and only reloads it when the ring looks full or empty.
- **`bellows::mpmc`**, a lock-free multi-producer multi-consumer channel. `Sender` and `Receiver` are both `Clone`, and the queue is the bounded sequence-number design from Dmitry Vyukov.
- The same API on both: `try_send`, `send`, `send_timeout`, `try_recv`, `recv`, `recv_timeout`, plus `len`, `capacity`, `is_empty` and `is_disconnected`. The receivers have `iter()`, `try_iter()` and `IntoIterator`.
- Disconnection that behaves: once every sender is gone, receivers drain whatever is left and then get an error. Once every receiver is gone, senders get their value handed back.
- Values that are never lost or doubled: anything still in the buffer when the channel is dropped gets dropped exactly once, which the tests check with drop counters.
- Capacity is rounded up to a power of two (so index wrapping is a mask). The MPMC queue has a floor of two slots, because its sequence-number scheme can't tell "full" from "free" with one.
- Blocking calls back off in three stages: spin, `yield_now`, then short sleeps. There's no parking, so a blocked thread costs a little latency in exchange for keeping the fast path free of any lock or syscall.

```rust
use bellows::mpmc;
use std::thread;

let (tx, rx) = mpmc::channel::<u32>(64);
let workers: Vec<_> = (0..4)
    .map(|_| {
        let rx = rx.clone();
        thread::spawn(move || rx.iter().sum::<u32>())
    })
    .collect();
drop(rx);
for i in 1..=100 {
    tx.send(i).unwrap();
}
drop(tx);
let total: u32 = workers.into_iter().map(|w| w.join().unwrap()).sum();
assert_eq!(total, 5050);
```

## 🏗️ Architecture

```
 spsc                                       mpmc
 ┌──────────────────────────────┐           ┌───────────────────────────────────┐
 │ buf: [MaybeUninit<T>; 2^n]   │           │ buf: [Slot { seq, value }; 2^n]   │
 │ head  (consumer, padded)     │           │ dequeue (CAS, padded)             │
 │ tail  (producer, padded)     │           │ enqueue (CAS, padded)             │
 │ tx_alive / rx_alive          │           │ senders / receivers (counts)      │
 └──────────────────────────────┘           └───────────────────────────────────┘
        both sit behind Arc<Inner<T>>             both sit behind Arc<Inner<T>>

 error.rs  SendError, TrySendError, SendTimeoutError, RecvError, TryRecvError, RecvTimeoutError
 wait.rs   CachePadded, Backoff, block_send / block_recv, ring_capacity
```

- **spsc** uses ever-increasing head and tail counters that wrap at `usize`, masked into the buffer. The producer owns `tail`, the consumer owns `head`, and a release store on one side pairs with an acquire load on the other. The `Sender` and `Receiver` hold `Cell`s for the cached counters, which is why they are `Send` but not `Sync`.
- **mpmc** gives every slot a sequence number. A producer claims position `p` with a CAS on `enqueue` when the slot's sequence equals `p`, writes the value, then publishes by setting the sequence to `p + 1`. A consumer claims with a CAS on `dequeue` when the sequence is `p + 1`, reads the value, and releases the slot by setting the sequence to `p + capacity`.
- **wait** holds the shared pieces. `CachePadded` keeps the hot counters on separate cache lines, `Backoff` is the spin/yield/sleep ladder, and `block_send` and `block_recv` are the retry loops (with optional deadlines) that both channels use.
- **error** has the error types. Their `Debug` impls don't need `T: Debug`, so you can use them with any payload.
- Both `Inner` types implement `Drop` to drop whatever is still in flight.

## 🚀 Quick start

```
cargo build
cargo test
cargo test --release
cargo run --release --example throughput
```

A quick taste:

```rust
use bellows::spsc;
use bellows::TrySendError;

let (tx, rx) = spsc::channel::<&str>(2);
tx.try_send("a").unwrap();
tx.try_send("b").unwrap();
match tx.try_send("c") {
    Err(TrySendError::Full(v)) => println!("full, got {:?} back", v),
    _ => unreachable!(),
}
println!("{:?} {:?}", rx.try_recv(), rx.try_recv());
```

```
full, got "c" back
Ok("a") Ok("b")
```

## ⏱️ Benchmarks

`examples/throughput.rs` pushes 5 million `u64`s through each channel and prints millions of messages per second. One run in a Docker container on a shared 28-thread box, release build, so treat the numbers as rough:

```
spsc  cap     64:   20.29 M msg/s
spsc  cap   1024:   29.45 M msg/s
spsc  cap  65536:  321.74 M msg/s
mpmc  cap   1024 1p/1c:   49.17 M msg/s
mpmc  cap   1024 2p/2c:   15.35 M msg/s
mpmc  cap   1024 4p/4c:   10.30 M msg/s
```

The 65536 figure is flattered by the producer running far ahead of the consumer in a ring that big. The MPMC numbers drop as threads are added, which is what you'd expect from every thread fighting over two counters.

## 🧪 Testing

68 tests, all passing (they also pass in release mode and when the container is limited to two CPUs):

- `tests/spsc.rs` (32): ordering, capacity rounding, full and empty behaviour, index wrap-around, partial fills, disconnection in both directions, drop accounting (including wrapped buffers), zero-sized types, timeouts, blocked threads waking up, iterators, and stress runs of a million messages through capacities 1, 64 and 4096, a try-only spin test, heap payloads, a receiver quitting midway and a ping-pong pair.
- `tests/mpmc.rs` (31): the same basics plus clone counting, disconnection needing every handle gone, blocked senders and receivers waking on disconnect, and stress runs for 4 producers and 4 consumers, 8 to 1, 1 to 8, capacity 2 under contention and 16 threads each side. The stress tests check that each producer's messages arrive in order per consumer, that every message arrives exactly once, and that drops balance under churn.
- `tests/errors.rs` (5): Debug without a `Debug` payload, Display text, trait objects and the helper methods.
