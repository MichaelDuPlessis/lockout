//! A lock-free MPMC queue based on the Michael-Scott algorithm.
//!
//! This is an implementation of the classic lock-free linked-list queue described in:
//! - Michael, M. M., & Scott, M. L. (1996). *Simple, Fast, and Practical Non-Blocking
//!   and Blocking Concurrent Queue Algorithms*. <https://doi.org/10.1145/248052.248106>
//!
//! The Rust implementation follows the approach described in:
//! - <https://karevongeijer.com/blog/lock-free-queue-in-rust/>
//!
//! Safe memory reclamation is provided by [`lockout_hazard`] hazard pointers.

use lockout_hazard::{AtomicPtr, Domain};
use std::{mem::MaybeUninit, sync::atomic::Ordering};

/// A node in the lock-free linked list.
///
/// Uses `MaybeUninit` for data because the sentinel node has no initialized value,
/// and dequeued nodes transfer ownership via `assume_init_read` without double-dropping.
pub(crate) struct Node<T> {
    data: MaybeUninit<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    /// Creates a node with initialized data.
    fn new(data: T) -> Self {
        Self {
            data: MaybeUninit::new(data),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Creates a sentinel node with uninitialized data.
    fn uninit() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Heap-allocates this node and returns a raw pointer.
    fn into_raw(self) -> *mut Node<T> {
        Box::into_raw(Box::new(self))
    }
}

/// A lock-free multi-producer, multi-consumer queue.
///
/// Uses a linked list of nodes with a sentinel (dummy) head node.
/// Both `head` and `tail` are always non-null, and the queue is empty
/// when `head == tail`.
pub(crate) struct Queue<T> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    domain: Domain,
}

impl<T> Queue<T> {
    /// Creates a new empty queue.
    pub(crate) fn new() -> Self {
        let empty = Node::uninit().into_raw();
        Self {
            head: AtomicPtr::new(empty),
            tail: AtomicPtr::new(empty),
            domain: Domain::new(),
        }
    }
}

impl<T: Send + Sync> Queue<T> {
    /// Enqueues a value at the tail of the queue.
    ///
    /// This operation is lock-free: if a concurrent enqueue is in progress,
    /// this thread will help advance the tail before retrying.
    pub(crate) fn enqueue(&self, data: T) {
        let new_node = Node::new(data).into_raw();

        loop {
            let tail = unsafe { self.domain.protect(&self.tail).unwrap_unchecked() };
            let next = tail.next.load(Ordering::Acquire);

            if !next.is_null() {
                // Tail is behind — help advance it.
                if let Ok(replaced) = self.tail.compare_exchange(
                    tail.as_raw(),
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    replaced.forget();
                }
            } else {
                // Tail is current — try to link the new node.
                if tail
                    .next
                    .compare_exchange(
                        std::ptr::null_mut(),
                        new_node,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    // Try to advance tail to the new node.
                    if let Ok(replaced) = self.tail.compare_exchange(
                        tail.as_raw(),
                        new_node,
                        Ordering::Release,
                        Ordering::Relaxed,
                    ) {
                        replaced.forget();
                    }
                    return;
                }
            }
        }
    }

    /// Dequeues a value from the head of the queue.
    ///
    /// Returns `None` if the queue is empty. This operation is lock-free:
    /// if a concurrent enqueue is partially complete, this thread will help
    /// advance the tail before retrying.
    pub(crate) fn dequeue(&self) -> Option<T> {
        loop {
            let head = self
                .domain
                .protect(&self.head)
                .expect("The queue should never be empty.");
            let next = head.next.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);

            if head.as_raw() != tail {
                // Queue is not empty — try to advance head.
                let guarded_next = unsafe { self.domain.protect(&head.next).unwrap_unchecked() };

                if let Ok(unlinked_head) = self.head.compare_exchange(
                    head.as_raw(),
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    unlinked_head.retire(&self.domain);

                    return Some(unsafe { guarded_next.data.assume_init_read() });
                }
            } else if !next.is_null() {
                // Help a partial enqueue by advancing tail.
                if let Ok(replaced) =
                    self.tail
                        .compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed)
                {
                    replaced.forget();
                }
            } else {
                // Queue is empty.
                return None;
            }
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        let head = unsafe { Box::from_raw(self.head.load(Ordering::Relaxed)) };
        let mut next = head.next;

        while !next.load(Ordering::Relaxed).is_null() {
            let mut node = unsafe { Box::from_raw(next.load(Ordering::Relaxed)) };
            unsafe { node.data.assume_init_drop() };
            next = node.next;
        }
    }
}

// Safety: All shared access uses atomic operations and hazard pointers.
unsafe impl<T: Send + Sync> Send for Queue<T> {}
unsafe impl<T: Send + Sync> Sync for Queue<T> {}
