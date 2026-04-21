# lockout-hazard

Lock-free hazard pointers for safe memory reclamation in concurrent Rust data structures.

## Why hazard pointers?

When one thread unlinks a node from a lock-free structure, other threads may still hold transient references to that node. Freeing immediately can cause use-after-free. Hazard pointers solve this by:

1. Letting readers publish protected pointers in hazard slots.
2. Deferring reclamation of retired nodes until no hazard slot references them.

## Usage

```rust
use lockout_hazard::{AtomicPtr, Domain};
use std::sync::atomic::Ordering;

static DOMAIN: Domain = Domain::new();

let shared = AtomicPtr::from_box(Box::new(42));

let guard = DOMAIN.protect(&shared).unwrap();
assert_eq!(*guard, 42);

// Replace and retire old pointer.
shared
    .swap(Box::into_raw(Box::new(100)), Ordering::SeqCst)
    .retire(&DOMAIN);

guard.clear();
DOMAIN.collect();

// Final cleanup of current pointer.
shared
    .swap(std::ptr::null_mut(), Ordering::SeqCst)
    .retire(&DOMAIN);
DOMAIN.collect();
```

## Core types

- `Domain` — owns hazard slots and retired-node reclamation.
- `AtomicPtr<T>` — managed atomic pointer wrapper that returns `Replaced<T>` from mutation ops.
- `Guard<'_, T>` — protected reference preventing reclamation while held.
- `Replaced<T>` — displaced pointer token that must be retired (or intentionally forgotten).

## Reclamation model

- Retired pointers are pushed to a lock-free retired stack.
- `collect()` snapshots active hazards and frees only unprotected retired pointers.
- Automatic collection is triggered periodically (default threshold: 8 retires).

## Safety requirements

For `Domain::retire_ptr::<T>(ptr)` / `Replaced::retire`:

1. Pointer must no longer be reachable from shared atomics.
2. Pointer must originate from `Box`.
3. Pointer must not be retired more than once.
