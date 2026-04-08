use std::{
    cell::{Cell, RefCell},
    ops::Deref,
    sync::atomic::{AtomicPtr, Ordering},
};

const COLLECTION_THRESHOLD: usize = 8;

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

    static COLLECTION_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[derive(Debug, Default)]
struct Node {
    hazard: AtomicPtr<()>,
    next: AtomicPtr<Node>,
}

impl Node {
    fn new(ptr: *mut ()) -> Self {
        Self {
            hazard: AtomicPtr::new(ptr),
            ..Default::default()
        }
    }
}

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

#[derive(Debug)]
pub struct Guard<'a, T> {
    // TODO: Look into whether an atomic read is worth storing less bytes
    slot: &'a AtomicPtr<()>,
    ptr: *mut T,
}

unsafe impl<T: Send + Sync> Send for Guard<'_, T> {}
unsafe impl<T: Send + Sync> Sync for Guard<'_, T> {}

impl<'a, T> Guard<'a, T> {
    fn new(slot: &'a AtomicPtr<()>, ptr: *mut T) -> Self {
        Self { slot, ptr }
    }

    pub fn get(&self) -> &T {
        unsafe { &*self.ptr }
    }

    fn set_null(&self) {
        self.slot.store(std::ptr::null_mut(), Ordering::SeqCst);
    }

    pub fn clear(self) {
        self.set_null();
        std::mem::forget(self); // prevent Drop from running again
    }
}

impl<T> Deref for Guard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.get()
    }
}

impl<'a, T> Drop for Guard<'a, T> {
    fn drop(&mut self) {
        self.set_null();
    }
}

#[derive(Debug, Default)]
pub struct Domain {
    hazard_list: Node,
}

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
    pub fn new() -> Self {
        Self {
            hazard_list: Node::default(),
        }
    }

    pub fn protect<T>(&self, ptr: &AtomicPtr<T>) -> Option<Guard<'_, T>> {
        loop {
            // Check if the ptr is fine
            let ptr_before = ptr.load(Ordering::SeqCst);
            if ptr_before.is_null() {
                return None;
            }

            // Get a guard
            let guard = self.reserve(ptr_before);

            // Make sure its the same
            let ptr_after = ptr.load(Ordering::SeqCst);
            if ptr_after == ptr_before {
                return Some(guard);
            }
        }
    }

    fn reserve<T>(&self, ptr: *mut T) -> Guard<'_, T> {
        // loop through hazard list to find free ptr
        let mut current = &self.hazard_list;

        // TODO: Change ordering. SeqCst just for now
        loop {
            // If we find a node that is not protecting anything we try to make it protect ptr
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

            // If the next is not null we move current forward
            let next = current.next.load(Ordering::SeqCst);
            if !next.is_null() {
                current = unsafe { next.as_ref().unwrap_unchecked() };
                continue;
            }

            // If none of the previous occured it means we have gone through the entire list and it is time to allocate a new node
            let new_node = Box::into_raw(Box::new(Node::new(ptr as *mut ())));
            match current.next.compare_exchange_weak(
                std::ptr::null_mut(),
                new_node,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                // if good return the node just allocated
                Ok(_) => {
                    return Guard::new(
                        unsafe { &new_node.as_ref().unwrap_unchecked().hazard },
                        ptr,
                    );
                }
                // on failure deallocate and move current forward
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

    pub fn retire<T>(&self, guard: Guard<'_, T>) {
        unsafe fn deleter<T>(p: *mut ()) {
            let tp = p as *mut T;
            drop(unsafe { Box::from_raw(tp) });
        }

        // Push onto this thread's retired list
        RETIRED_OBJECTS.with(|objects| {
            objects
                .borrow_mut()
                .0
                .push(Retired::new(guard.ptr as *mut (), deleter::<T>));
        });

        // Check if its time to try and collect retired nodes
        let (counter, _) = COLLECTION_COUNT.get().overflowing_add(1);
        if counter.is_multiple_of(COLLECTION_THRESHOLD) {
            self.collect();
        }
        COLLECTION_COUNT.set(counter);
    }

    fn collect(&self) {
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
