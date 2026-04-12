#![cfg(loom)]

use lockout_hazard::{AtomicPtr, Domain};
use loom::sync::Arc;
use loom::thread;
use std::sync::atomic::Ordering;

/// One reader protects while one writer swaps — the core hazard pointer scenario.
/// Verifies the reader never sees freed memory (no use-after-free).
#[test]
fn protect_while_swap() {
    loom::model(|| {
        let domain = Arc::new(Domain::new());
        let shared = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(42u64))));

        let d = domain.clone();
        let s = shared.clone();
        let writer = thread::spawn(move || {
            let new = Box::into_raw(Box::new(100u64));
            s.swap(new, Ordering::SeqCst).retire(&d);
        });

        // Reader: protect and read — must see either 42 or 100, never garbage.
        if let Some(guard) = domain.protect(&shared) {
            let val = *guard;
            assert!(val == 42 || val == 100);
        }

        writer.join().unwrap();

        shared
            .swap(std::ptr::null_mut(), Ordering::SeqCst)
            .retire(&domain);
        domain.collect();
    });
}

/// Collect must not free a pointer while a guard is held.
/// Reader holds a guard, writer retires and collects — the value must survive.
#[test]
fn protect_during_collect() {
    loom::model(|| {
        let domain = Arc::new(Domain::new());
        let shared = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(42u64))));

        let guard = domain.protect(&shared).unwrap();
        assert_eq!(*guard, 42);

        let d = domain.clone();
        let s = shared.clone();
        let writer = thread::spawn(move || {
            let new = Box::into_raw(Box::new(100u64));
            s.swap(new, Ordering::SeqCst).retire(&d);
            d.collect();
        });

        // Value must still be readable — collect must not have freed it.
        assert_eq!(*guard, 42);
        drop(guard);

        writer.join().unwrap();

        shared
            .swap(std::ptr::null_mut(), Ordering::SeqCst)
            .retire(&domain);
        domain.collect();
    });
}

/// Two writers racing to swap the same pointer.
/// Tests concurrent Treiber stack pushes in retire/push_retired.
#[test]
fn concurrent_swaps() {
    loom::model(|| {
        let domain = Arc::new(Domain::new());
        let shared = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(1u64))));

        let d = domain.clone();
        let s = shared.clone();
        let w1 = thread::spawn(move || {
            let new = Box::into_raw(Box::new(2u64));
            s.swap(new, Ordering::SeqCst).retire(&d);
        });

        let d = domain.clone();
        let s = shared.clone();
        let w2 = thread::spawn(move || {
            let new = Box::into_raw(Box::new(3u64));
            s.swap(new, Ordering::SeqCst).retire(&d);
        });

        w1.join().unwrap();
        w2.join().unwrap();

        // Final value must be one of the swapped-in values.
        let guard = domain.protect(&shared).unwrap();
        assert!(*guard == 2 || *guard == 3);
        drop(guard);

        shared
            .swap(std::ptr::null_mut(), Ordering::SeqCst)
            .retire(&domain);
        domain.collect();
    });
}

/// Writer swaps to null — reader's protect must return None.
#[test]
fn protect_sees_null() {
    loom::model(|| {
        let domain = Arc::new(Domain::new());
        let shared = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(42u64))));

        let d = domain.clone();
        let s = shared.clone();
        let writer = thread::spawn(move || {
            s.swap(std::ptr::null_mut(), Ordering::SeqCst).retire(&d);
        });

        // May see Some(42) or None, both are valid.
        if let Some(guard) = domain.protect(&shared) {
            assert_eq!(*guard, 42);
        }

        writer.join().unwrap();
        domain.collect();
    });
}

/// Guard is dropped (clearing hazard slot) while collect scans the hazard list.
/// Tests ordering between guard's store(null) and collect's snapshot.
#[test]
fn guard_drop_during_collect() {
    loom::model(|| {
        let domain = Arc::new(Domain::new());
        let shared = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(42u64))));

        // Retire the original so there's something for collect to process.
        let old = shared.swap(Box::into_raw(Box::new(100u64)), Ordering::SeqCst);
        old.retire(&domain);

        // Protect the new value.
        let guard = domain.protect(&shared).unwrap();
        assert_eq!(*guard, 100);

        let d = domain.clone();
        let collector = thread::spawn(move || {
            d.collect();
        });

        // Drop guard concurrently with collect — clears the hazard slot.
        drop(guard);

        collector.join().unwrap();

        shared
            .swap(std::ptr::null_mut(), Ordering::SeqCst)
            .retire(&domain);
        domain.collect();
    });
}
