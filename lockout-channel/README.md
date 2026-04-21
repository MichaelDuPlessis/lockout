# lockout-channel

Lock-free channels built on top of `lockout-hazard`.

## Overview

`lockout-channel` currently provides an unbounded **multi-producer, multi-consumer (MPMC)** channel in `mpmc`.

Internally, it uses:

- a lock-free Michael-Scott queue for message storage,
- a lock-free waiter stack for parking/unparking blocked receivers,
- hazard pointers (`lockout-hazard`) for safe memory reclamation.

## Quick start

```rust
use lockout_channel::mpmc::channel;

let (tx, rx) = channel();
tx.send("hello").unwrap();
assert_eq!(rx.recv().unwrap(), "hello");
```

## API at a glance

### Producer

- `Sender::send(T) -> Result<(), SendError<T>>`
  - Fails only when all receivers are dropped.

### Consumer

- `Reciever::recv() -> Result<T, RecvError>`
  - Blocking receive.
- `Reciever::try_recv() -> Result<T, TryRecvError>`
  - Non-blocking receive.
- `Reciever::recv_timeout(Duration) -> Result<T, RecvTimeoutError>`
  - Blocking receive with timeout.
- `Reciever::iter() -> Iter<'_, T>`
  - Blocking iterator until disconnected and drained.
- `Reciever::try_iter() -> TryIter<'_, T>`
  - Non-blocking iterator over currently available items.

## Examples

Timeout receive:

```rust
use lockout_channel::mpmc::channel;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

let (_tx, rx) = channel::<i32>();
assert!(matches!(
    rx.recv_timeout(Duration::from_millis(1)),
    Err(RecvTimeoutError::Timeout)
));
```

Non-blocking drain:

```rust
use lockout_channel::mpmc::channel;

let (tx, rx) = channel();
tx.send(1).unwrap();
tx.send(2).unwrap();

let mut values: Vec<_> = rx.try_iter().collect();
values.sort_unstable();
assert_eq!(values, vec![1, 2]);
```
