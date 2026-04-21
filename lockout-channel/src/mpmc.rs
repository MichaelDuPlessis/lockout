//! Lock-free multi-producer, multi-consumer channel.
//!
//! This module provides an unbounded MPMC channel built on:
//! - a lock-free Michael-Scott queue for message storage, and
//! - a lock-free waiter stack for parking/unparking blocked receivers.
//!
//! # Semantics
//!
//! - `send` succeeds while at least one [`Reciever`] is alive.
//! - `recv` blocks until a message is available, or returns disconnect when all
//!   [`Sender`] handles are dropped and the queue is empty.
//! - `try_recv` is non-blocking and returns immediately.
//! - `recv_timeout` blocks for at most the supplied timeout.
//!
//! # Examples
//!
//! Basic send/receive:
//! ```
//! use lockout_channel::mpmc::channel;
//!
//! let (tx, rx) = channel();
//! tx.send(42).unwrap();
//! assert_eq!(rx.recv().unwrap(), 42);
//! ```
//!
//! Multi-producer:
//! ```
//! use lockout_channel::mpmc::channel;
//!
//! let (tx, rx) = channel();
//! let tx2 = tx.clone();
//!
//! tx.send(1).unwrap();
//! tx2.send(2).unwrap();
//!
//! let a = rx.recv().unwrap();
//! let b = rx.recv().unwrap();
//! assert!(a == 1 || a == 2);
//! assert!(b == 1 || b == 2);
//! assert_ne!(a, b);
//! ```
//!
//! Non-blocking drain:
//! ```
//! use lockout_channel::mpmc::channel;
//!
//! let (tx, rx) = channel();
//! tx.send(1).unwrap();
//! tx.send(2).unwrap();
//!
//! let mut v: Vec<_> = rx.try_iter().collect();
//! v.sort();
//! assert_eq!(v, vec![1, 2]);
//! ```

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
/// Sending side of the MPMC channel.
///
/// Cloning creates another producer handle that can send concurrently.
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    /// Sends a value into the channel.
    ///
    /// Returns [`SendError`] with the original value when all receivers have
    /// been dropped.
    ///
    /// # Examples
    /// ```
    /// use lockout_channel::mpmc::channel;
    ///
    /// let (tx, rx) = channel();
    /// tx.send("hello").unwrap();
    /// assert_eq!(rx.recv().unwrap(), "hello");
    /// ```
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
/// Receiving side of the MPMC channel.
///
/// Cloning creates another consumer handle that can receive concurrently.
pub struct Reciever<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Reciever<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    /// Receives the next value, blocking until one is available or disconnected.
    ///
    /// Returns [`RecvError`] only when all senders are dropped and the channel
    /// queue is empty.
    ///
    /// # Examples
    /// ```
    /// use lockout_channel::mpmc::channel;
    ///
    /// let (tx, rx) = channel();
    /// tx.send(7).unwrap();
    /// assert_eq!(rx.recv().unwrap(), 7);
    /// ```
    pub fn recv(&self) -> Result<T, RecvError> {
        match self.inner.recv(None) {
            Ok(msg) => Ok(msg),
            Err(RecvTimeoutError::Disconnected) => Err(RecvError),
            Err(RecvTimeoutError::Timeout) => unsafe { unreachable_unchecked() },
        }
    }

    /// Attempts to receive without blocking.
    ///
    /// Returns:
    /// - [`TryRecvError::Empty`] if the channel is currently empty but still connected.
    /// - [`TryRecvError::Disconnected`] if no senders remain and no message is available.
    ///
    /// # Examples
    /// ```
    /// use lockout_channel::mpmc::channel;
    /// use std::sync::mpsc::TryRecvError;
    ///
    /// let (_tx, rx) = channel::<i32>();
    /// assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    /// ```
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }

    /// Receives the next value, waiting up to `timeout`.
    ///
    /// Returns [`RecvTimeoutError::Timeout`] when the deadline is reached before
    /// a value arrives.
    ///
    /// # Examples
    /// ```
    /// use lockout_channel::mpmc::channel;
    /// use std::sync::mpsc::RecvTimeoutError;
    /// use std::time::Duration;
    ///
    /// let (_tx, rx) = channel::<i32>();
    /// assert!(matches!(
    ///     rx.recv_timeout(Duration::from_millis(1)),
    ///     Err(RecvTimeoutError::Timeout)
    /// ));
    /// ```
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.inner.recv(Some(Instant::now() + timeout))
    }

    /// Returns a blocking iterator over values received from this channel.
    ///
    /// The iterator yields messages until the channel becomes disconnected and
    /// drained.
    pub fn iter(&self) -> Iter<'_, T> {
        Iter { reciever: self }
    }

    /// Returns a non-blocking iterator over values currently available.
    ///
    /// Iteration stops immediately once the channel is observed empty.
    pub fn try_iter(&self) -> TryIter<'_, T> {
        TryIter { reciever: self }
    }
}

#[derive(Debug)]
/// Blocking iterator produced by [`Reciever::iter`].
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
/// Non-blocking iterator produced by [`Reciever::try_iter`].
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

/// Creates a new multi-producer, multi-consumer channel pair.
///
/// # Examples
/// ```
/// use lockout_channel::mpmc::channel;
///
/// let (tx, rx) = channel();
/// tx.send(1).unwrap();
/// assert_eq!(rx.recv().unwrap(), 1);
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn send_recv_single() {
        let (tx, rx) = channel();
        tx.send(42).unwrap();
        assert_eq!(rx.recv().unwrap(), 42);
    }

    #[test]
    fn send_fails_without_receivers() {
        let (tx, rx) = channel::<i32>();
        drop(rx);
        assert!(tx.send(1).is_err());
    }

    #[test]
    fn recv_reports_disconnected_when_empty() {
        let (_tx, rx) = channel::<i32>();
        drop(_tx);
        assert!(matches!(rx.recv(), Err(RecvError)));
    }

    #[test]
    fn try_recv_empty_and_disconnected() {
        let (tx, rx) = channel::<i32>();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
        drop(tx);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[test]
    fn recv_timeout_times_out() {
        let (_tx, rx) = channel::<i32>();
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(10)),
            Err(RecvTimeoutError::Timeout)
        ));
    }

    #[test]
    fn recv_timeout_receives_before_deadline() {
        let (tx, rx) = channel();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(5));
            tx.send(99).unwrap();
        });

        let got = rx.recv_timeout(Duration::from_millis(100)).unwrap();
        handle.join().unwrap();
        assert_eq!(got, 99);
    }

    #[test]
    fn iter_drains_until_disconnect() {
        let (tx, rx) = channel();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        drop(tx);

        let mut got: Vec<_> = rx.iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn try_iter_drains_currently_available() {
        let (tx, rx) = channel();
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        let mut got: Vec<_> = rx.try_iter().collect();
        got.sort_unstable();
        assert_eq!(got, vec![1, 2]);

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn concurrent_send_recv_count_matches() {
        let (tx, rx) = channel::<usize>();
        let rx = Arc::new(rx);
        let producers = 4;
        let per_producer = 500;
        let consumers = 4;
        let total = producers * per_producer;

        let mut producer_handles = Vec::new();
        for p in 0..producers {
            let tx = tx.clone();
            producer_handles.push(thread::spawn(move || {
                for i in 0..per_producer {
                    tx.send(p * per_producer + i).unwrap();
                }
            }));
        }
        drop(tx);

        let received = Arc::new(AtomicUsize::new(0));
        let mut consumer_handles = Vec::new();
        for _ in 0..consumers {
            let rx = rx.clone();
            let received = received.clone();
            consumer_handles.push(thread::spawn(move || {
                while let Ok(_) = rx.recv() {
                    received.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for h in producer_handles {
            h.join().unwrap();
        }
        for h in consumer_handles {
            h.join().unwrap();
        }

        assert_eq!(received.load(Ordering::Relaxed), total);
    }
}
