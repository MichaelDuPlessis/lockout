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
                if let Ok(old_null) = tail
                    .next
                    .compare_exchange(
                        std::ptr::null_mut(),
                        new_node,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                {
                    old_null.forget();
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
            let tail = self.tail.load(Ordering::Relaxed);

            if head.as_raw() != tail {
                // Queue is not empty — try to advance head.
                let guarded_next = unsafe { self.domain.protect(&head.next).unwrap_unchecked() };

                if let Ok(unlinked_head) = self.head.compare_exchange(
                    head.as_raw(),
                    guarded_next.as_raw(),
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    unlinked_head.retire(&self.domain);

                    return Some(unsafe { guarded_next.data.assume_init_read() });
                }
            } else {
                let next = head.next.load(Ordering::Acquire);

                if !next.is_null() {
                    // Help a partial enqueue by advancing tail.
                    if let Ok(replaced) =
                        self.tail.compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn dequeue_empty() {
        let q = Queue::<i32>::new();
        assert!(q.dequeue().is_none());
    }

    #[test]
    fn enqueue_dequeue_single() {
        let q = Queue::new();
        q.enqueue(42);
        assert_eq!(q.dequeue(), Some(42));
        assert!(q.dequeue().is_none());
    }

    #[test]
    fn fifo_order() {
        let q = Queue::new();
        for i in 0..10 {
            q.enqueue(i);
        }
        for i in 0..10 {
            assert_eq!(q.dequeue(), Some(i));
        }
        assert!(q.dequeue().is_none());
    }

    #[test]
    fn drop_with_remaining_items() {
        let drop_count = Arc::new(AtomicUsize::new(0));

        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        {
            let q = Queue::new();
            for _ in 0..5 {
                q.enqueue(Tracked(drop_count.clone()));
            }
        }

        assert_eq!(drop_count.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn concurrent_enqueue_dequeue() {
        let q = Arc::new(Queue::new());
        let count = 1000;
        let producers = 4;
        let consumers = 4;

        let mut handles = Vec::new();

        for _ in 0..producers {
            let q = q.clone();
            handles.push(thread::spawn(move || {
                for i in 0..count {
                    q.enqueue(i);
                }
            }));
        }

        let total_dequeued = Arc::new(AtomicUsize::new(0));

        for _ in 0..consumers {
            let q = q.clone();
            let total_dequeued = total_dequeued.clone();
            handles.push(thread::spawn(move || {
                loop {
                    if q.dequeue().is_some() {
                        total_dequeued.fetch_add(1, Ordering::Relaxed);
                    } else if total_dequeued.load(Ordering::Relaxed) >= producers * count {
                        break;
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(total_dequeued.load(Ordering::Relaxed), producers * count);
    }

    #[test]
    fn concurrent_no_duplicates() {
        let q = Arc::new(Queue::new());
        let count = 500;
        let producers = 4;

        let mut handles = Vec::new();

        for p in 0..producers {
            let q = q.clone();
            handles.push(thread::spawn(move || {
                for i in 0..count {
                    q.enqueue(p * count + i);
                }
            }));
        }

        for h in handles.drain(..) {
            h.join().unwrap();
        }

        let mut results = Vec::new();
        while let Some(v) = q.dequeue() {
            results.push(v);
        }

        results.sort();
        let expected: Vec<usize> = (0..producers * count).collect();
        assert_eq!(results, expected);
    }
}
