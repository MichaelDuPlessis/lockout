//! A lock-free Treiber stack with hazard-pointer reclamation.
//!
//! This is the classic CAS-based LIFO stack algorithm described by:
//! - Treiber, R. K. (1986). *Systems Programming: Coping with Parallelism*.
//!
//! Memory reclamation is handled by [`lockout_hazard`] so popped nodes can be
//! safely retired without use-after-free.

use lockout_hazard::{AtomicPtr, Domain};
use std::{mem::MaybeUninit, sync::atomic::Ordering};

/// A stack node.
///
/// `MaybeUninit` lets a successful pop move out `T` without double-drop when
/// the node is later reclaimed via hazard pointers.
#[derive(Debug)]
struct Node<T> {
    data: MaybeUninit<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    fn new(data: T) -> Self {
        Self {
            data: MaybeUninit::new(data),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    fn into_raw(self) -> *mut Node<T> {
        Box::into_raw(Box::new(self))
    }
}

/// A lock-free LIFO stack.
#[derive(Debug)]
pub(crate) struct Stack<T> {
    head: AtomicPtr<Node<T>>,
    domain: Domain,
}

impl<T> Stack<T> {
    pub(crate) fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
            domain: Domain::new(),
        }
    }

    /// Pushes a value onto the stack.
    pub(crate) fn push(&self, value: T) {
        let new_head = Node::new(value).into_raw();

        loop {
            let head = self.head.load(Ordering::Acquire);
            unsafe {
                (*new_head).next = AtomicPtr::new(head);
            }

            if let Ok(old_head) =
                self.head
                    .compare_exchange(head, new_head, Ordering::Release, Ordering::Relaxed)
            {
                old_head.forget();
                return;
            }
        }
    }

    /// Pops a value from the stack, or `None` if empty.
    pub(crate) fn pop(&self) -> Option<T> {
        loop {
            let head = self.domain.protect(&self.head)?;
            let next = head.next.load(Ordering::Acquire);

            if let Ok(unlinked_head) =
                self.head
                    .compare_exchange(head.as_raw(), next, Ordering::AcqRel, Ordering::Relaxed)
            {
                let value = unsafe { head.data.assume_init_read() };
                unlinked_head.retire(&self.domain);
                return Some(value);
            }
        }
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        let mut current = self.head.load(Ordering::Relaxed);

        while !current.is_null() {
            let mut node = unsafe { Box::from_raw(current) };
            unsafe { node.data.assume_init_drop() };
            current = node.next.load(Ordering::Relaxed);
        }
    }
}

// Safety: All shared access uses atomic operations and hazard pointers.
unsafe impl<T: Send + Sync> Send for Stack<T> {}
unsafe impl<T: Send + Sync> Sync for Stack<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn pop_empty() {
        let s = Stack::<i32>::new();
        assert!(s.pop().is_none());
    }

    #[test]
    fn push_pop_single() {
        let s = Stack::new();
        s.push(42);
        assert_eq!(s.pop(), Some(42));
        assert!(s.pop().is_none());
    }

    #[test]
    fn lifo_order() {
        let s = Stack::new();
        for i in 0..10 {
            s.push(i);
        }
        for i in (0..10).rev() {
            assert_eq!(s.pop(), Some(i));
        }
        assert!(s.pop().is_none());
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
            let s = Stack::new();
            for _ in 0..5 {
                s.push(Tracked(drop_count.clone()));
            }
        }

        assert_eq!(drop_count.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn concurrent_push_pop_count() {
        let s = Arc::new(Stack::new());
        let producers = 4;
        let consumers = 4;
        let count = 1000;
        let target = producers * count;

        let mut handles = Vec::new();
        for p in 0..producers {
            let s = s.clone();
            handles.push(thread::spawn(move || {
                for i in 0..count {
                    s.push(p * count + i);
                }
            }));
        }

        let popped = Arc::new(AtomicUsize::new(0));
        for _ in 0..consumers {
            let s = s.clone();
            let popped = popped.clone();
            handles.push(thread::spawn(move || {
                loop {
                    if popped.load(Ordering::Relaxed) >= target {
                        break;
                    }
                    if s.pop().is_some() {
                        popped.fetch_add(1, Ordering::Relaxed);
                    } else {
                        thread::yield_now();
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(popped.load(Ordering::Relaxed), target);
        assert!(s.pop().is_none());
    }

    #[test]
    fn concurrent_no_duplicates() {
        let s = Arc::new(Stack::new());
        let producers = 4;
        let count = 500;

        let mut handles = Vec::new();
        for p in 0..producers {
            let s = s.clone();
            handles.push(thread::spawn(move || {
                for i in 0..count {
                    s.push(p * count + i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let mut values = Vec::new();
        while let Some(v) = s.pop() {
            values.push(v);
        }
        values.sort_unstable();

        let expected: Vec<usize> = (0..producers * count).collect();
        assert_eq!(values, expected);
    }
}
