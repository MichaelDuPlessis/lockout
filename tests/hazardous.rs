use hazardous::Domain;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

static DOMAIN: Domain = Domain::new();

#[test]
fn protect_returns_none_for_null() {
    let ptr: AtomicPtr<i32> = AtomicPtr::new(std::ptr::null_mut());
    assert!(DOMAIN.protect(&ptr).is_none());
}

#[test]
fn protect_ptr_returns_none_for_null() {
    assert!(DOMAIN.protect_ptr::<i32>(std::ptr::null_mut()).is_none());
}

#[test]
fn protect_and_deref() {
    let val = Box::into_raw(Box::new(42));
    let ptr = AtomicPtr::new(val);

    let guard = DOMAIN.protect(&ptr).unwrap();
    assert_eq!(*guard, 42);
    guard.clear();

    // Clean up
    DOMAIN.retire_ptr::<i32>(val);
    DOMAIN.collect();
}

#[test]
fn protect_ptr_and_deref() {
    let val = Box::into_raw(Box::new(99));
    let guard = DOMAIN.protect_ptr(val).unwrap();
    assert_eq!(*guard, 99);
    guard.clear();

    DOMAIN.retire_ptr::<i32>(val);
    DOMAIN.collect();
}

#[test]
fn guard_clear_releases_slot() {
    let val = Box::into_raw(Box::new(10));
    let ptr = AtomicPtr::new(val);

    let guard = DOMAIN.protect(&ptr).unwrap();
    guard.clear();

    // After clear, retiring and collecting should reclaim
    ptr.store(std::ptr::null_mut(), Ordering::Relaxed);
    DOMAIN.retire_ptr::<i32>(val);
    DOMAIN.collect();
}

#[test]
fn guard_drop_releases_slot() {
    let val = Box::into_raw(Box::new(10));
    let ptr = AtomicPtr::new(val);

    {
        let _guard = DOMAIN.protect(&ptr).unwrap();
    }
    // Guard dropped, slot released

    ptr.store(std::ptr::null_mut(), Ordering::Relaxed);
    DOMAIN.retire_ptr::<i32>(val);
    DOMAIN.collect();
}

#[test]
fn retire_with_guard() {
    let val = Box::into_raw(Box::new(77));
    let ptr = AtomicPtr::new(val);

    let guard = DOMAIN.protect(&ptr).unwrap();
    assert_eq!(*guard, 77);

    ptr.store(std::ptr::null_mut(), Ordering::Relaxed);
    DOMAIN.retire(guard);
    DOMAIN.collect();
}

#[test]
fn collect_does_not_reclaim_protected_pointer() {
    let val = Box::into_raw(Box::new(123));
    let ptr = AtomicPtr::new(val);

    let guard = DOMAIN.protect(&ptr).unwrap();

    // Retire the pointer while it's still protected
    let new_val = Box::into_raw(Box::new(456));
    ptr.store(new_val, Ordering::Relaxed);
    DOMAIN.retire_ptr::<i32>(val);
    DOMAIN.collect();

    // Should still be readable through the guard
    assert_eq!(*guard, 123);
    guard.clear();
    DOMAIN.collect();

    // Clean up new_val
    ptr.store(std::ptr::null_mut(), Ordering::Relaxed);
    DOMAIN.retire_ptr::<i32>(new_val);
    DOMAIN.collect();
}

#[test]
fn concurrent_protect_and_retire() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let domain = Arc::new(Domain::new());

    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let shared = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(Tracked(
        drop_count.clone(),
    )))));

    let mut handles = Vec::new();

    // Spawn readers
    for _ in 0..4 {
        let domain = domain.clone();
        let shared = shared.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                if let Some(guard) = domain.protect(&shared) {
                    let _ = &guard.get().0;
                    drop(guard);
                }
            }
        }));
    }

    // Spawn writers
    for _ in 0..2 {
        let domain = domain.clone();
        let shared = shared.clone();
        let drop_count = drop_count.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                let new = Box::into_raw(Box::new(Tracked(drop_count.clone())));
                let old = shared.swap(new, Ordering::AcqRel);
                if !old.is_null() {
                    domain.retire_ptr::<Tracked>(old);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Final cleanup
    let last = shared.swap(std::ptr::null_mut(), Ordering::Relaxed);
    if !last.is_null() {
        domain.retire_ptr::<Tracked>(last);
    }
    domain.collect();

    // 1 initial + 100 swaps = 101 total allocations, all should be dropped
    assert_eq!(drop_count.load(Ordering::Relaxed), 101);
}

#[test]
fn multiple_guards_same_domain() {
    let a = Box::into_raw(Box::new(1));
    let b = Box::into_raw(Box::new(2));
    let ptr_a = AtomicPtr::new(a);
    let ptr_b = AtomicPtr::new(b);

    let guard_a = DOMAIN.protect(&ptr_a).unwrap();
    let guard_b = DOMAIN.protect(&ptr_b).unwrap();

    assert_eq!(*guard_a, 1);
    assert_eq!(*guard_b, 2);

    guard_a.clear();
    guard_b.clear();

    DOMAIN.retire_ptr::<i32>(a);
    DOMAIN.retire_ptr::<i32>(b);
    DOMAIN.collect();
}

#[test]
fn domain_drop_frees_nodes() {
    let domain = Domain::new();
    let mut ptrs = Vec::new();

    // Create enough guards to force node allocation
    for i in 0..16 {
        let p = Box::into_raw(Box::new(i));
        ptrs.push((p, AtomicPtr::new(p)));
    }

    let guards: Vec<_> = ptrs
        .iter()
        .map(|(_, ap)| domain.protect(ap).unwrap())
        .collect();

    for g in guards {
        g.clear();
    }

    for (p, _) in &ptrs {
        domain.retire_ptr::<i32>(*p);
    }
    domain.collect();

    // Domain drop should free all hazard list nodes without leaking
    drop(domain);
}
