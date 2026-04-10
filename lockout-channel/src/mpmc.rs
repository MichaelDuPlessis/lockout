use lockout_hazard::{AtomicPtr, Domain};
use std::{mem::MaybeUninit, sync::atomic::Ordering};

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

    fn uninit() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    fn into_raw(self) -> *mut Node<T> {
        Box::into_raw(Box::new(self))
    }
}

struct Queue<T> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
    domain: Domain,
}

impl<T> Queue<T> {
    fn new() -> Self {
        let empty = Node::uninit().into_raw();
        Self {
            head: AtomicPtr::new(empty),
            tail: AtomicPtr::new(empty),
            domain: Domain::new(),
        }
    }
}

impl<T> Queue<T> {
    pub fn enqueue(&self, data: T) {
        let new_node = Node::new(data).into_raw();

        loop {
            let tail = unsafe { self.domain.protect(&self.tail).unwrap_unchecked() };
            let next = tail.next.load(Ordering::Acquire);

            if !next.is_null() {
                if let Ok(replaced) = self.tail.compare_exchange(
                    tail.as_raw(),
                    next,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    replaced.forget();
                }
            } else {
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

    pub fn dequeue(&self) -> Option<T> {
        loop {
            let head = self
                .domain
                .protect(&self.head)
                .expect("The queue should never be empty.");
            let next = head.next.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);

            if head.as_raw() != tail {
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
                if let Ok(replaced) =
                    self.tail
                        .compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed)
                {
                    replaced.forget();
                }
            } else {
                return None;
            }
        }
    }
}
