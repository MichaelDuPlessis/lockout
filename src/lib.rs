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
//! // Safety: `old` was allocated with Box and is no longer reachable from `shared`.
//! unsafe { DOMAIN.retire_ptr::<i32>(old) };
//!
//! // Clean up the remaining allocation.
//! let last = shared.swap(std::ptr::null_mut(), Ordering::SeqCst);
//! unsafe { DOMAIN.retire_ptr::<i32>(last) };
//! DOMAIN.collect();
//! ```

use std::{
    ops::Deref,
    sync::atomic::{AtomicPtr, AtomicU8, Ordering},
};

/// Number of retires before an automatic collection is triggered (per-thread).
const COLLECTION_THRESHOLD: u8 = 8;

/// A node in the domain's lock-free hazard linked list.
#[derive(Debug, Default)]
struct HazardNode {
    hazard: AtomicPtr<()>,
    next: AtomicPtr<HazardNode>,
}

impl HazardNode {
    const fn new(ptr: *mut ()) -> Self {
        Self {
            hazard: AtomicPtr::new(ptr),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

/// A retired pointer with its type-erased deleter, forming a lock-free intrusive stack.
struct RetiredNode {
    ptr: *mut (),
    deleter: unsafe fn(*mut ()),
    next: *mut RetiredNode,
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
/// hazard list. Retired pointers are stored in a lock-free Treiber stack and
/// reclaimed when no hazard slot references them.
///
/// Typically one global domain is sufficient:
///
/// ```
/// use hazardous::Domain;
/// static DOMAIN: Domain = Domain::new();
/// ```
#[derive(Debug)]
pub struct Domain {
    hazard_list: HazardNode,
    retired_head: AtomicPtr<RetiredNode>,
    retire_count: AtomicU8,
}

// Safety: All fields use atomic operations for concurrent access.
unsafe impl Send for Domain {}
unsafe impl Sync for Domain {}

impl Default for Domain {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        // Free all retired nodes unconditionally — no guards can exist
        // since they borrow the domain.
        let mut retired = *self.retired_head.get_mut();
        while !retired.is_null() {
            let node = unsafe { Box::from_raw(retired) };
            unsafe { (node.deleter)(node.ptr) };
            retired = node.next;
        }

        // Free all hazard list nodes.
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
            hazard_list: HazardNode::new(std::ptr::null_mut()),
            retired_head: AtomicPtr::new(std::ptr::null_mut()),
            retire_count: AtomicU8::new(0),
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
    ///
    /// # Safety
    ///
    /// The pointer must point to a valid, live allocation.
    pub unsafe fn protect_ptr<T>(&self, ptr: *mut T) -> Option<Guard<'_, T>> {
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

        loop {
            if current
                .hazard
                .compare_exchange_weak(
                    std::ptr::null_mut(),
                    ptr as *mut (),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return Guard::new(&current.hazard, ptr);
            }

            let next = current.next.load(Ordering::Acquire);
            if !next.is_null() {
                current = unsafe { next.as_ref().unwrap_unchecked() };
                continue;
            }

            let new_node = Box::into_raw(Box::new(HazardNode::new(ptr as *mut ())));
            match current.next.compare_exchange(
                std::ptr::null_mut(),
                new_node,
                Ordering::Release,
                Ordering::Acquire,
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
                            .load(Ordering::Acquire)
                            .as_ref()
                            .unwrap_unchecked()
                    };
                }
            }
        }
    }

    /// Pushes a retired node onto the lock-free Treiber stack.
    fn push_retired(&self, node: *mut RetiredNode) {
        loop {
            let head = self.retired_head.load(Ordering::Relaxed);
            unsafe { (*node).next = head };
            if self
                .retired_head
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Retires the pointer held by `guard`, scheduling it for deferred reclamation.
    ///
    /// The guard is consumed, releasing its hazard slot. The caller must ensure
    /// the pointer is no longer reachable through any shared atomic before calling this.
    pub fn retire<T>(&self, guard: Guard<'_, T>) {
        // Safety: the guard proves the pointer was obtained through protect,
        // and the caller is responsible for ensuring it's no longer reachable.
        unsafe { self.retire_ptr::<T>(guard.ptr) };
    }

    /// Retires a raw pointer, scheduling it for deferred reclamation.
    ///
    /// The pointer will be deallocated (via `Box::from_raw`) once no hazard slot
    /// references it. The caller must ensure the pointer was originally allocated
    /// with `Box` and is no longer reachable through any shared atomic.
    ///
    /// # Safety
    ///
    /// - The pointer must have been allocated with `Box`.
    /// - The pointer must no longer be reachable from any shared atomic.
    /// - The pointer must not be retired more than once.
    pub unsafe fn retire_ptr<T>(&self, ptr: *mut T) {
        unsafe fn deleter<T>(p: *mut ()) {
            drop(unsafe { Box::from_raw(p as *mut T) });
        }

        let node = Box::into_raw(Box::new(RetiredNode {
            ptr: ptr as *mut (),
            deleter: deleter::<T>,
            next: std::ptr::null_mut(),
        }));
        self.push_retired(node);

        let count = self.retire_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count.is_multiple_of(COLLECTION_THRESHOLD) {
            self.collect();
        }
    }

    /// Scans all hazard slots and reclaims any retired pointers that are not
    /// currently protected.
    ///
    /// This is called automatically every [`COLLECTION_THRESHOLD`] retires, but
    /// can also be called manually to force reclamation. Resets the retire counter.
    pub fn collect(&self) {
        self.retire_count.store(0, Ordering::Relaxed);

        // Snapshot all active hazard pointers.
        let mut hazard_ptrs = Vec::new();
        let mut current = &self.hazard_list;
        loop {
            let ptr = current.hazard.load(Ordering::SeqCst);
            if !ptr.is_null() {
                hazard_ptrs.push(ptr);
            }
            let next = current.next.load(Ordering::Acquire);
            if next.is_null() {
                break;
            }
            current = unsafe { &*next };
        }

        // Atomically claim the entire retired stack.
        let mut retired = self
            .retired_head
            .swap(std::ptr::null_mut(), Ordering::Acquire);

        // Walk the claimed list: free unprotected entries, push back protected ones.
        while !retired.is_null() {
            let node = unsafe { Box::from_raw(retired) };
            retired = node.next;

            if hazard_ptrs.contains(&node.ptr) {
                let raw = Box::into_raw(node);
                self.push_retired(raw);
            } else {
                unsafe { (node.deleter)(node.ptr) };
            }
        }
    }
}
