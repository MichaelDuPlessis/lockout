use crate::{ms_queue::Queue, treiber_stack::Stack};
use std::{
    hint::unreachable_unchecked,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
        mpsc::{RecvError, SendError},
    },
    thread::{self, Thread},
};

#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
enum WaiterState {
    Waiting,
    Notified,
    Cancelled,
}

impl WaiterState {
    fn is_waiting(&self) -> bool {
        *self == Self::Waiting
    }
}

impl From<WaiterState> for u8 {
    fn from(value: WaiterState) -> Self {
        value as u8
    }
}

impl From<WaiterState> for AtomicU8 {
    fn from(value: WaiterState) -> Self {
        Self::new(value.into())
    }
}

impl From<u8> for WaiterState {
    fn from(value: u8) -> Self {
        match value {
            0 => WaiterState::Waiting,
            1 => WaiterState::Notified,
            2 => WaiterState::Cancelled,
            _ => panic!("This should never be reachable WaiterState can only be 0, 1, 2."),
        }
    }
}

#[derive(Debug)]
struct Waiter {
    state: AtomicU8,
    thread: Thread,
}

impl Waiter {
    fn new(state: WaiterState) -> Self {
        Self {
            state: state.into(),
            thread: thread::current(),
        }
    }
}

#[derive(Debug)]
struct Inner<T> {
    sender_count: AtomicUsize,
    reciever_count: AtomicUsize,
    messages: Queue<T>,
    waiters: Stack<Arc<Waiter>>,
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
        if self.inner.has_recievers() {
            self.inner.messages.enqueue(msg);

            while let Some(waiter) = self.inner.waiters.pop() {
                if waiter
                    .state
                    .compare_exchange(
                        WaiterState::Waiting.into(),
                        WaiterState::Notified.into(),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    waiter.thread.unpark();
                    break;
                }
            }

            Ok(())
        } else {
            Err(SendError(msg))
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
            // try to dequeue a message otherwise put in waiting list
            if let Some(msg) = self.inner.messages.dequeue() {
                return Ok(msg);
            }

            if !self.inner.has_senders() {
                return Err(RecvError);
            }

            // TODO: Investigate if this should be an Arc
            let waiter = Arc::new(Waiter::new(WaiterState::Waiting));
            self.inner.waiters.push(Arc::clone(&waiter));

            loop {
                // just double check there tehre is nothing
                if let Some(msg) = self.inner.messages.dequeue() {
                    waiter
                        .state
                        .store(WaiterState::Cancelled as u8, Ordering::Relaxed);
                    return Ok(msg);
                }

                if !self.inner.has_senders() {
                    return Err(RecvError);
                }

                // if still waiting park
                let state = WaiterState::from(waiter.state.load(Ordering::Relaxed));
                match state {
                    WaiterState::Waiting => thread::park(),
                    WaiterState::Notified => break,
                    _ => unsafe { unreachable_unchecked() },
                }
            }
        }
    }
}

impl<T> Clone for Reciever<T> {
    fn clone(&self) -> Self {
        self.inner.increment_reciever();

        Self::new(Arc::clone(&self.inner))
    }
}

pub fn channel<T>() -> (Sender<T>, Reciever<T>) {
    let inner = Arc::new(Inner {
        sender_count: AtomicUsize::new(1),
        reciever_count: AtomicUsize::new(1),
        messages: Queue::new(),
        waiters: Stack::new(),
    });

    let sender = Sender::new(Arc::clone(&inner));
    let receiver = Reciever::new(inner);

    (sender, receiver)
}
