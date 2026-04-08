use std::{
    cell::{Cell, RefCell},
    sync::atomic::{AtomicPtr, Ordering},
};

const COLLECTION_THRESHOLD: usize = 8;

thread_local! {
    static RETIRED_OBJECTS: RefCell<Vec<Retired>> =
        RefCell::new(Vec::new());

    static COLLECTION_COUNT: Cell<usize> = Cell::new(0);
}

#[derive(Debug, Default)]
struct Node {
    hazard: AtomicPtr<()>,
    next: AtomicPtr<Node>,
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
    slot: &'a AtomicPtr<()>,
    ptr: *mut T,
}

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

impl<'a, T> Drop for Guard<'a, T> {
    fn drop(&mut self) {
        self.set_null();
    }
}

#[derive(Debug)]
pub struct Domain {
    hazard_list: Node,
}

impl Domain {
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
            let new_node = Box::into_raw(Box::new(Node::default()));
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
        let current = &self.hazard_list;
        let mut hazard_ptrs = Vec::new();

        while current.next.load(Ordering::SeqCst) != std::ptr::null_mut() {
            let ptr = current.hazard.load(Ordering::SeqCst);
            if ptr != std::ptr::null_mut() {
                hazard_ptrs.push(ptr);
            }
        }

        RETIRED_OBJECTS.with(|objects| {
            let mut vec = objects.borrow_mut();
            let mut i = 0;
            while i < vec.len() {
                if hazard_ptrs.contains(unsafe { &vec.get_unchecked(i).ptr }) {
                    i += 1;
                } else {
                    let r = vec.swap_remove(i);
                    unsafe { (r.deleter)(r.ptr) };
                }
            }
        });
    }
}
