use crate::ms_queue::Queue;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{RecvError, SendError},
    },
    thread,
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
        self.sender_count.load(Ordering::Relaxed)
    }

    fn has_recievers(&self) -> bool {
        self.reciever_count() > 0
    }

    fn has_senders(&self) -> bool {
        self.sender_count() > 0
    }

    fn increment_reciever(&self) {
        self.reciever_count.fetch_add(1, Ordering::Relaxed);
    }

    fn increment_sender(&self) {
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
            self.inner.queue.enqueue(msg);
            Ok(())
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

    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            if !self.inner.has_senders() {
                return Err(RecvError);
            }

            if let Some(msg) = self.inner.queue.dequeue() {
                return Ok(msg);
            }

            thread::park();
        }
    }
}

pub fn channel<T>() -> (Sender<T>, Reciever<T>) {
    let inner = Arc::new(Inner {
        sender_count: AtomicUsize::new(1),
        reciever_count: AtomicUsize::new(1),
        queue: Queue::new(),
    });

    let sender = Sender::new(Arc::clone(&inner));
    let receiver = Reciever::new(inner);

    (sender, receiver)
}
