use crate::ms_queue::Queue;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
    mpsc::SendError,
};

#[derive(Debug)]
struct Inner<T> {
    sender_count: AtomicUsize,
    reciever_count: AtomicUsize,
    queue: Queue<T>,
}

impl<T> Inner<T> {
    fn reciever_count(&self) -> usize {
        self.reciever_count.load(Ordering::Relaxed)
    }

    fn sender_count(&self) -> usize {
        self.reciever_count.load(Ordering::Relaxed)
    }

    fn has_recievers(&self) -> bool {
        self.reciever_count() > 0
    }

    fn increment_sender(&self) {
        self.sender_count.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_reciever(&self) {
        self.sender_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    pub fn send(&self, msg: T) -> Result<(), SendError<T>> {
        if !self.inner.has_recievers() {
            Err(SendError(msg))
        } else {
            Ok(self.inner.queue.enqueue(msg))
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.increment_sender();

        Self::new(Arc::clone(&self.inner))
    }
}

#[derive(Debug)]
pub struct Reciever<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Reciever<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }
}

pub fn channel<T>() -> (Sender<T>, Reciever<T>) {
    let inner = Arc::new(Inner {
        sender_count: AtomicUsize::new(0),
        reciever_count: AtomicUsize::new(0),
        queue: Queue::new(),
    });

    let sender = Sender::new(Arc::clone(&inner));
    let receiver = Reciever::new(inner);

    (sender, receiver)
}
