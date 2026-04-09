# hazardous

A lock-free hazard pointer library for safe memory reclamation in Rust.

## What are hazard pointers?

Hazard pointers were introduced by Maged Michael and solve the problem of safely reclaiming memory in lock-free data structures. When multiple threads share pointers to heap-allocated objects, a thread removing an object can't immediately free it — another thread might still be reading it. Hazard pointers let readers announce which pointers they're accessing, so writers defer reclamation until it's safe.

## Usage

```rust
use std::sync::atomic::{AtomicPtr, Ordering};
use hazardous::Domain;

static DOMAIN: Domain = Domain::new();

// Shared pointer to some data
let shared = AtomicPtr::new(Box::into_raw(Box::new(42)));

// Reader: protect the pointer before reading
let guard = DOMAIN.protect(&shared).unwrap();
assert_eq!(*guard, 42);

// Writer: swap in a new value, retire the old one
let old = shared.swap(Box::into_raw(Box::new(100)), Ordering::SeqCst);
guard.clear();
DOMAIN.retire_ptr::<i32>(old);

// Reclaim retired pointers not currently protected
DOMAIN.collect();
```

## Design

- **Lock-free**: All operations (protect, retire, collect) are lock-free using atomic CAS operations
- **Treiber stack**: Retired pointers are stored in a lock-free intrusive stack within the domain — no thread-local state, no leaks on thread exit
- **Automatic collection**: `collect()` is triggered automatically every 8 retires, or can be called manually
- **`const` constructable**: `Domain::new()` is `const`, so domains can be declared as `static`

## API

- `Domain::new()` — create a new hazard pointer domain
- `Domain::protect(&self, ptr: &AtomicPtr<T>)` — protect a shared atomic pointer with a load-verify loop
- `Domain::protect_ptr(&self, ptr: *mut T)` — protect an arbitrary raw pointer
- `Domain::retire(guard)` — retire the pointer held by a guard
- `Domain::retire_ptr::<T>(ptr)` — retire a raw pointer directly
- `Domain::collect()` — scan hazards and reclaim unprotected retired pointers
- `Guard::clear()` — explicitly release a hazard slot
- `Guard` implements `Deref<Target = T>` for ergonomic access

## Safety contract

The caller must ensure that a pointer passed to `retire` or `retire_ptr` is:
1. No longer reachable from any shared atomic (i.e., it has been swapped out)
2. Was originally allocated with `Box`
