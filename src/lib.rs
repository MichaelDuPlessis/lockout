//! A lock-free hazard pointer implementation for safe memory reclamation.
//!
//! Hazard pointers allow threads to safely read shared pointers while other threads
//! may concurrently remove and deallocate the pointed-to objects. A thread "protects"
//! a pointer by publishing it in a hazard slot; reclamation of retired objects is
//! deferred until no thread holds a matching hazard.
//!
//! # Example
//!
//! ```
//! use std::sync::atomic::{AtomicPtr, Ordering};
//! use hazardous::Domain;
//!
//! static DOMAIN: Domain = Domain::new();
//!
//! let shared = AtomicPtr::new(Box::into_raw(Box::new(42)));
//!
//! // Protect the pointer so it won't be reclaimed while we read it.
//! let guard = DOMAIN.protect(&shared).unwrap();
//! assert_eq!(*guard, 42);
//!
//! // Swap in a new value and retire the old one.
//! let old = shared.swap(Box::into_raw(Box::new(100)), Ordering::SeqCst);
//! guard.clear();
//! DOMAIN.retire_ptr::<i32>(old);
//!
//! // Clean up the remaining allocation.
//! let last = shared.swap(std::ptr::null_mut(), Ordering::SeqCst);
//! DOMAIN.retire_ptr::<i32>(last);
//! DOMAIN.collect();
//! ```

use std::{
    cell::{Cell, RefCell},
    ops::Deref,
    sync::atomic::{AtomicPtr, Ordering},
};

/// Number of retires before an automatic collection is triggered (per-thread).
const COLLECTION_THRESHOLD: u8 = 8;

/// Wrapper around the per-thread retired list that flushes on thread exit.
struct RetiredList(Vec<Retired>);

impl Drop for RetiredList {
    fn drop(&mut self) {
        for r in self.0.drain(..) {
            unsafe { (r.deleter)(r.ptr) };
        }
    }
}

thread_local! {
    static RETIRED_OBJECTS: RefCell<RetiredList> =
        const { RefCell::new(RetiredList(Vec::new())) };

    static COLLECTION_COUNT: Cell<u8> = const { Cell::new(0) };
}

/// A node in the domain's lock-free hazard linked list.
#[derive(Debug, Default)]
struct Node {
    hazard: AtomicPtr<()>,
    next: AtomicPtr<Node>,
}

impl Node {
    const fn new(ptr: *mut ()) -> Self {
        Self {
            hazard: AtomicPtr::new(ptr),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

/// A retired pointer paired with its type-erased deleter.
#[derive(Debug)]
struct Retired {
    ptr: *mut (),
    deleter: unsafe fn(*mut ()),
}

impl Retired {
    fn new(ptr: *mut (), deleter: unsafe fn(*mut ())) -> Self {
        Self { ptr, deleter }
    }
}

/// A protected reference to a hazard-pointer-guarded value.
///
/// While a `Guard` exists, the underlying pointer is published in a hazard slot,
/// preventing any concurrent [`Domain::collect`] from reclaiming it.
///
/// Implements [`Deref`] for ergonomic access to the protected value.
/// Dropping the guard (or calling [`clear`](Guard::clear)) releases the hazard slot.
#[derive(Debug)]
pub struct Guard<'a, T> {
    // TODO: Look into whether an atomic read is worth storing less bytes
    slot: &'a AtomicPtr<()>,
    ptr: *mut T,
}

// Safety: The guard only provides &T access and the hazard slot is atomic.
// Sending/sharing a guard across threads is safe as long as T itself is Send + Sync.
unsafe impl<T: Send + Sync> Send for Guard<'_, T> {}
unsafe impl<T: Send + Sync> Sync for Guard<'_, T> {}

impl<'a, T> Guard<'a, T> {
    fn new(slot: &'a AtomicPtr<()>, ptr: *mut T) -> Self {
        Self { slot, ptr }
    }

    /// Returns a reference to the protected value.
    pub fn get(&self) -> &T {
        unsafe { &*self.ptr }
    }

    fn set_null(&self) {
        self.slot.store(std::ptr::null_mut(), Ordering::SeqCst);
    }

    /// Releases the hazard slot without running the destructor twice.
    ///
    /// Equivalent to dropping the guard, but can be called explicitly when
    /// you want to release protection at a specific point.
    pub fn clear(self) {
        self.set_null();
        std::mem::forget(self);
    }
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.get()
    }
}

impl<T> Drop for Guard<'_, T> {
    fn drop(&mut self) {
        self.set_null();
    }
}

/// A hazard pointer domain that manages hazard slots and deferred reclamation.
///
/// All pointers protected and retired through the same domain share a single
/// hazard list. Typically one global domain is sufficient:
///
/// ```
/// use hazardous::Domain;
/// static DOMAIN: Domain = Domain::new();
/// ```
#[derive(Debug, Default)]
pub struct Domain {
    hazard_list: Node,
}

// Safety: The hazard list is a lock-free linked list using atomics.
// All mutations go through atomic operations, making concurrent access safe.
unsafe impl Send for Domain {}
unsafe impl Sync for Domain {}

impl Drop for Domain {
    fn drop(&mut self) {
        let mut next = self.hazard_list.next.load(Ordering::Relaxed);
        while !next.is_null() {
            let node = unsafe { Box::from_raw(next) };
            next = node.next.load(Ordering::Relaxed);
        }
    }
}

impl Domain {
    /// Creates a new hazard pointer domain.
    ///
    /// This is a `const fn`, so it can be used in `static` declarations.
    pub const fn new() -> Self {
        Self {
            hazard_list: Node::new(std::ptr::null_mut()),
        }
    }

    /// Protects the pointer stored in `ptr` by publishing it in a hazard slot.
    ///
    /// Uses a load-reserve-verify loop to ensure the returned guard protects
    /// the value that was in `ptr` at the time of the call. Returns `None` if
    /// the pointer is null.
    pub fn protect<T>(&self, ptr: &AtomicPtr<T>) -> Option<Guard<'_, T>> {
        loop {
            let ptr_before = ptr.load(Ordering::SeqCst);
            if ptr_before.is_null() {
                return None;
            }

            let guard = self.reserve(ptr_before);

            let ptr_after = ptr.load(Ordering::SeqCst);
            if ptr_after == ptr_before {
                return Some(guard);
            }
        }
    }

    /// Protects an arbitrary raw pointer by publishing it in a hazard slot.
    ///
    /// Unlike [`protect`](Domain::protect), this does not verify the pointer
    /// against an `AtomicPtr` source. The caller must ensure the pointer is
    /// valid. Returns `None` if the pointer is null.
    pub fn protect_ptr<T>(&self, ptr: *mut T) -> Option<Guard<'_, T>> {
        if ptr.is_null() {
            return None;
        }
        Some(self.reserve(ptr))
    }

    /// Reserves a hazard slot for `ptr` by walking the lock-free linked list.
    ///
    /// Tries to claim an existing free slot via CAS. If all slots are occupied,
    /// allocates a new node and appends it to the list.
    fn reserve<T>(&self, ptr: *mut T) -> Guard<'_, T> {
        let mut current = &self.hazard_list;

        // TODO: Change ordering. SeqCst just for now
        loop {
            if current
                .hazard
                .compare_exchange_weak(
                    std::ptr::null_mut(),
                    ptr as *mut (),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return Guard::new(&current.hazard, ptr);
            }

            let next = current.next.load(Ordering::SeqCst);
            if !next.is_null() {
                current = unsafe { next.as_ref().unwrap_unchecked() };
                continue;
            }

            let new_node = Box::into_raw(Box::new(Node::new(ptr as *mut ())));
            match current.next.compare_exchange_weak(
                std::ptr::null_mut(),
                new_node,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Guard::new(
                        unsafe { &new_node.as_ref().unwrap_unchecked().hazard },
                        ptr,
                    );
                }
                Err(_) => {
                    drop(unsafe { Box::from_raw(new_node) });
                    current = unsafe {
                        current
                            .next
                            .load(Ordering::SeqCst)
                            .as_ref()
                            .unwrap_unchecked()
                    };
                }
            }
        }
    }

    /// Retires the pointer held by `guard`, scheduling it for deferred reclamation.
    ///
    /// The guard is consumed, releasing its hazard slot. The caller must ensure
    /// the pointer is no longer reachable through any shared atomic before calling this.
    pub fn retire<T>(&self, guard: Guard<'_, T>) {
        self.retire_ptr::<T>(guard.ptr);
    }

    /// Retires a raw pointer, scheduling it for deferred reclamation.
    ///
    /// The pointer will be deallocated (via `Box::from_raw`) once no hazard slot
    /// references it. The caller must ensure the pointer was originally allocated
    /// with `Box` and is no longer reachable through any shared atomic.
    pub fn retire_ptr<T>(&self, ptr: *mut T) {
        unsafe fn deleter<T>(p: *mut ()) {
            let tp = p as *mut T;
            drop(unsafe { Box::from_raw(tp) });
        }

        RETIRED_OBJECTS.with(|objects| {
            objects
                .borrow_mut()
                .0
                .push(Retired::new(ptr as *mut (), deleter::<T>));
        });

        let counter = COLLECTION_COUNT.get() + 1;
        if counter.is_multiple_of(COLLECTION_THRESHOLD) {
            self.collect();
        }
        COLLECTION_COUNT.set(counter);
    }

    /// Scans all hazard slots and reclaims any retired pointers that are not
    /// currently protected.
    ///
    /// This is called automatically every [`COLLECTION_THRESHOLD`] retires, but
    /// can also be called manually to force reclamation. Resets the per-thread
    /// retire counter.
    pub fn collect(&self) {
        COLLECTION_COUNT.set(0);
        let mut current = &self.hazard_list;
        let mut hazard_ptrs = Vec::new();

        loop {
            let ptr = current.hazard.load(Ordering::SeqCst);
            if !ptr.is_null() {
                hazard_ptrs.push(ptr);
            }
            let next = current.next.load(Ordering::SeqCst);
            if next.is_null() {
                break;
            }
            current = unsafe { &*next };
        }

        RETIRED_OBJECTS.with(|objects| {
            let mut list = objects.borrow_mut();
            let vec = &mut list.0;
            let mut i = 0;
            while i < vec.len() {
                if hazard_ptrs.contains(&vec[i].ptr) {
                    i += 1;
                } else {
                    let r = vec.swap_remove(i);
                    unsafe { (r.deleter)(r.ptr) };
                }
            }
        });
    }
}
