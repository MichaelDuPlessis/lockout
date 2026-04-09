use hazardous::{AtomicPtr, Domain};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

static DOMAIN: Domain = Domain::new();

#[test]
fn protect_returns_none_for_null() {
    let ptr = AtomicPtr::<i32>::new(std::ptr::null_mut());
    assert!(DOMAIN.protect(&ptr).is_none());
}

#[test]
fn protect_ptr_returns_none_for_null() {
    assert!(unsafe { DOMAIN.protect_ptr::<i32>(std::ptr::null_mut()) }.is_none());
}

#[test]
fn protect_and_deref() {
    let domain = Domain::new();
    let ptr = AtomicPtr::from_box(Box::new(42));

    let guard = domain.protect(&ptr).unwrap();
    assert_eq!(*guard, 42);
    guard.clear();

    ptr.swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    domain.collect();
}

#[test]
fn protect_ptr_and_deref() {
    let domain = Domain::new();
    let val = Box::into_raw(Box::new(99));
    let guard = unsafe { domain.protect_ptr(val) }.unwrap();
    assert_eq!(*guard, 99);
    guard.clear();

    unsafe { domain.retire_ptr::<i32>(val) };
    domain.collect();
}

#[test]
fn guard_clear_releases_slot() {
    let domain = Domain::new();
    let ptr = AtomicPtr::from_box(Box::new(10));

    let guard = domain.protect(&ptr).unwrap();
    guard.clear();

    ptr.swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    domain.collect();
}

#[test]
fn guard_drop_releases_slot() {
    let domain = Domain::new();
    let ptr = AtomicPtr::from_box(Box::new(10));

    {
        let _guard = domain.protect(&ptr).unwrap();
    }

    ptr.swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    domain.collect();
}

#[test]
fn retire_with_guard() {
    let domain = Domain::new();
    let ptr = AtomicPtr::from_box(Box::new(77));

    let guard = domain.protect(&ptr).unwrap();
    assert_eq!(*guard, 77);

    ptr.swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    guard.clear();
    domain.collect();
}

#[test]
fn collect_does_not_reclaim_protected_pointer() {
    let domain = Domain::new();
    let ptr = AtomicPtr::from_box(Box::new(123));

    let guard = domain.protect(&ptr).unwrap();

    // Swap in a new value, retire the old one while it's still protected
    let new_val = Box::into_raw(Box::new(456));
    ptr.swap(new_val, Ordering::SeqCst).unwrap().retire(&domain);
    domain.collect();

    // Should still be readable through the guard
    assert_eq!(*guard, 123);
    guard.clear();
    domain.collect();

    // Clean up new_val
    ptr.swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    domain.collect();
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
                if let Some(old) = shared.swap(new, Ordering::AcqRel) {
                    old.retire(&domain);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Final cleanup
    if let Some(last) = shared.swap(std::ptr::null_mut(), Ordering::Relaxed) {
        last.retire(&domain);
    }
    domain.collect();

    // 1 initial + 100 swaps = 101 total allocations, all should be dropped
    assert_eq!(drop_count.load(Ordering::Relaxed), 101);
}

#[test]
fn multiple_guards_same_domain() {
    let domain = Domain::new();
    let ptr_a = AtomicPtr::from_box(Box::new(1));
    let ptr_b = AtomicPtr::from_box(Box::new(2));

    let guard_a = domain.protect(&ptr_a).unwrap();
    let guard_b = domain.protect(&ptr_b).unwrap();

    assert_eq!(*guard_a, 1);
    assert_eq!(*guard_b, 2);

    guard_a.clear();
    guard_b.clear();

    ptr_a
        .swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    ptr_b
        .swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    domain.collect();
}

#[test]
fn domain_drop_frees_nodes() {
    let domain = Domain::new();
    let ptrs: Vec<_> = (0..16).map(|i| AtomicPtr::from_box(Box::new(i))).collect();

    let guards: Vec<_> = ptrs.iter().map(|p| domain.protect(p).unwrap()).collect();

    for g in guards {
        g.clear();
    }

    for p in &ptrs {
        p.swap(std::ptr::null_mut(), Ordering::SeqCst)
            .unwrap()
            .retire(&domain);
    }
    domain.collect();

    // Domain drop should free all hazard list nodes without leaking
    drop(domain);
}

#[test]
fn replaced_from_swap() {
    let domain = Domain::new();
    let ptr = AtomicPtr::from_box(Box::new(42));

    // swap returns Replaced
    let replaced = ptr.swap(Box::into_raw(Box::new(99)), Ordering::SeqCst).unwrap();
    replaced.retire(&domain);

    // swap with null returns the last value
    let replaced = ptr.swap(std::ptr::null_mut(), Ordering::SeqCst).unwrap();
    replaced.retire(&domain);

    // swap on null returns None
    assert!(ptr.swap(std::ptr::null_mut(), Ordering::SeqCst).is_none());

    domain.collect();
}

#[test]
fn replaced_from_compare_exchange() {
    let domain = Domain::new();
    let val = Box::into_raw(Box::new(42));
    let ptr = AtomicPtr::new(val);

    let new_val = Box::into_raw(Box::new(99));

    // Successful CAS returns Ok(Some(Replaced))
    let result = ptr.compare_exchange(val, new_val, Ordering::SeqCst, Ordering::SeqCst);
    result.unwrap().unwrap().retire(&domain);

    // Failed CAS returns Err with current value
    let wrong = std::ptr::null_mut();
    let result = ptr.compare_exchange(wrong, wrong, Ordering::SeqCst, Ordering::SeqCst);
    assert!(result.is_err());

    // Clean up
    ptr.swap(std::ptr::null_mut(), Ordering::SeqCst)
        .unwrap()
        .retire(&domain);
    domain.collect();
}
