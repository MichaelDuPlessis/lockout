use crate::{ms_queue::Queue, treiber_stack::Stack};
use std::{
    hint::unreachable_unchecked,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
        mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError},
    },
    thread::{self, Thread},
    time::{Duration, Instant},
};

#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
enum WaiterState {
    Waiting,
    Notified,
    Cancelled,
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

    fn decrement_reciever(&self) {
        self.reciever_count.fetch_sub(1, Ordering::Relaxed);
    }

    fn decrement_sender(&self) {
        self.sender_count.fetch_sub(1, Ordering::Relaxed);
    }

    fn send(&self, msg: T) -> Result<(), SendError<T>> {
        if self.has_recievers() {
            self.messages.enqueue(msg);

            while let Some(waiter) = self.waiters.pop() {
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

    fn recv(&self, deadline: Option<Instant>) -> Result<T, RecvTimeoutError> {
        loop {
            // try to dequeue a message otherwise put in waiting list
            if let Some(msg) = self.messages.dequeue() {
                return Ok(msg);
            }

            if !self.has_senders() {
                return Err(RecvTimeoutError::Disconnected);
            }

            // TODO: Investigate if this should be an Arc
            let waiter = Arc::new(Waiter::new(WaiterState::Waiting));
            self.waiters.push(Arc::clone(&waiter));

            loop {
                // just double check there tehre is nothing
                if let Some(msg) = self.messages.dequeue() {
                    waiter
                        .state
                        .store(WaiterState::Cancelled as u8, Ordering::Relaxed);
                    return Ok(msg);
                }

                if !self.has_senders() {
                    return Err(RecvTimeoutError::Disconnected);
                }

                // if still waiting park
                let state = WaiterState::from(waiter.state.load(Ordering::Relaxed));
                match state {
                    WaiterState::Waiting => {
                        if let Some(deadline) = deadline {
                            let now = Instant::now();
                            if now >= deadline {
                                waiter
                                    .state
                                    .store(WaiterState::Cancelled as u8, Ordering::Relaxed);
                                return Err(RecvTimeoutError::Timeout);
                            }

                            thread::park_timeout(deadline.saturating_duration_since(now));
                        } else {
                            thread::park();
                        }
                    }
                    WaiterState::Notified => break,
                    _ => unsafe { unreachable_unchecked() },
                }
            }
        }
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(msg) = self.messages.dequeue() {
            Ok(msg)
        } else if !self.has_senders() {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
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
        self.inner.send(msg)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.inner.increment_sender();

        Self::new(Arc::clone(&self.inner))
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.inner.decrement_sender();

        // if no more senders we need wake all recievers that are waiting
        if !self.inner.has_senders() {
            while let Some(waiter) = self.inner.waiters.pop() {
                if WaiterState::from(waiter.state.load(Ordering::Relaxed)) == WaiterState::Waiting {
                    waiter.thread.unpark();
                }
            }
        }
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
        match self.inner.recv(None) {
            Ok(msg) => Ok(msg),
            Err(RecvTimeoutError::Disconnected) => Err(RecvError),
            Err(RecvTimeoutError::Timeout) => unsafe { unreachable_unchecked() },
        }
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.inner.recv(Some(Instant::now() + timeout))
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter { reciever: self }
    }

    pub fn try_iter(&self) -> TryIter<'_, T> {
        TryIter { reciever: self }
    }
}

#[derive(Debug)]
pub struct Iter<'a, T> {
    reciever: &'a Reciever<T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.reciever.recv().ok()
    }
}

#[derive(Debug)]
pub struct TryIter<'a, T> {
    reciever: &'a Reciever<T>,
}

impl<'a, T> Iterator for TryIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.reciever.try_recv().ok()
    }
}

impl<T> Clone for Reciever<T> {
    fn clone(&self) -> Self {
        self.inner.increment_reciever();

        Self::new(Arc::clone(&self.inner))
    }
}

impl<T> Drop for Reciever<T> {
    fn drop(&mut self) {
        self.inner.decrement_reciever();
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
