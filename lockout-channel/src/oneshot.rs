//! A oneshot channel for sending a single value between threads.
//!
//! The channel is created with [`channel`], which returns a [`Sender`] and [`Receiver`] pair.
//! Both halves are consumed on use, enforcing the single-use guarantee at the type level.
//!
//! # Examples
//!
//! ```
//! use lockout_channel::oneshot;
//! use std::thread;
//!
//! let (tx, rx) = oneshot::channel();
//! thread::spawn(move || tx.send(42).unwrap());
//! assert_eq!(rx.recv().unwrap(), 42);
//! ```

use std::{
    cell::UnsafeCell,
    hint::unreachable_unchecked,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicU8, Ordering},
        mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError},
    },
    thread::{self, Thread},
    time::{Duration, Instant},
};

/// Initial state — no flags set.
const EMPTY: u8 = 0b0;
/// Data has been written to the channel.
const SENT: u8 = 0b00000001;
/// The sender has been dropped or consumed.
const SENDER_CLOSED: u8 = 0b00000010;
/// The receiver has been dropped or consumed.
const RECEIVER_CLOSED: u8 = 0b00000100;
/// The receiver is parked and waiting for a value.
const WAITING: u8 = 0b00001000;
/// The receiver has read the data.
const RECEIVED: u8 = 0b00010000;

fn dealloc<T>(ptr: *mut T) {
    drop(unsafe { Box::from_raw(ptr) })
}

fn has(state: u8, flag: u8) -> bool {
    state & flag == flag
}

fn has_any(state: u8, flags: u8) -> bool {
    state & flags != 0
}

/// Shared state between [`Sender`] and [`Receiver`].
#[derive(Debug)]
struct Inner<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
    /// Stores the receiver's thread handle so the sender can unpark it.
    receiver_thread: UnsafeCell<*mut Thread>,
}

// Safety: Data is transferred (not shared) between threads. All access is
// synchronized via atomic state transitions with appropriate orderings.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

/// The sending half of a oneshot channel.
///
/// Created by [`channel`]. Consumed on [`send`](Sender::send) or dropped
/// to signal disconnection to the receiver.
#[derive(Debug)]
pub struct Sender<T> {
    inner: *mut Inner<T>,
}

impl<T> Sender<T> {
    fn new(inner: *mut Inner<T>) -> Self {
        Self { inner }
    }

    fn inner(&self) -> &Inner<T> {
        unsafe { &*self.inner }
    }

    /// Sends `msg` through the channel, consuming the sender.
    ///
    /// Returns `Err(SendError(msg))` if the receiver has been dropped.
    ///
    /// # Examples
    /// ```
    /// use lockout_channel::oneshot;
    ///
    /// let (tx, rx) = oneshot::channel();
    /// tx.send(42).unwrap();
    /// assert_eq!(rx.recv().unwrap(), 42);
    /// ```
    pub fn send(self, msg: T) -> Result<(), SendError<T>> {
        unsafe { (&mut *self.inner().data.get()).write(msg) };
        let state = self
            .inner()
            .state
            .fetch_or(SENT | SENDER_CLOSED, Ordering::AcqRel);

        if has(state, RECEIVER_CLOSED) {
            let msg = unsafe { (&*self.inner().data.get()).assume_init_read() };

            dealloc(self.inner);
            std::mem::forget(self);

            return Err(SendError(msg));
        }

        if has(state, WAITING) {
            let thread_ptr = unsafe { *(*self.inner).receiver_thread.get() };
            unsafe { (*thread_ptr).unpark() };
        }

        std::mem::forget(self);
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = self
            .inner()
            .state
            .fetch_or(SENDER_CLOSED, Ordering::Acquire);

        if has(state, RECEIVER_CLOSED) {
            if has(state, SENT) && !has(state, RECEIVED) {
                unsafe { (&mut *self.inner().data.get()).assume_init_drop() };
            }

            if has(state, WAITING) {
                dealloc(unsafe { *self.inner().receiver_thread.get() });
            }

            dealloc(self.inner);
        } else {
            if has(state, WAITING) {
                let thread_ptr = unsafe { *self.inner().receiver_thread.get() };
                unsafe { (*thread_ptr).unpark() };
            }
        }
    }
}

/// The receiving half of a oneshot channel.
///
/// Created by [`channel`]. All receive methods consume the receiver,
/// enforcing the single-use guarantee.
#[derive(Debug)]
pub struct Receiver<T> {
    inner: *mut Inner<T>,
}

impl<T> Receiver<T> {
    fn new(inner: *mut Inner<T>) -> Self {
        Self { inner }
    }

    fn inner(&self) -> &Inner<T> {
        unsafe { &*self.inner }
    }

    /// Blocks until a value is received or the sender is dropped.
    ///
    /// # Examples
    /// ```
    /// use lockout_channel::oneshot;
    /// use std::thread;
    ///
    /// let (tx, rx) = oneshot::channel();
    /// thread::spawn(move || tx.send(7).unwrap());
    /// assert_eq!(rx.recv().unwrap(), 7);
    /// ```
    pub fn recv(self) -> Result<T, RecvError> {
        self.wait(None).map_err(|_| RecvError)
    }

    /// Returns the value if it has already been sent, without blocking.
    ///
    /// # Examples
    /// ```
    /// use lockout_channel::oneshot;
    /// use std::sync::mpsc::TryRecvError;
    ///
    /// let (tx, rx) = oneshot::channel::<i32>();
    /// assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    /// ```
    pub fn try_recv(self) -> Result<T, TryRecvError> {
        let state = self.inner().state.load(Ordering::Acquire);

        if !has_any(state, SENT | SENDER_CLOSED) {
            Err(TryRecvError::Empty)
        } else if has(state, SENT) {
            self.inner().state.fetch_or(RECEIVED, Ordering::Acquire);
            Ok(unsafe { (&*self.inner().data.get()).assume_init_read() })
        } else if has(state, SENDER_CLOSED) {
            Err(TryRecvError::Disconnected)
        } else {
            unsafe { unreachable_unchecked() }
        }
    }

    /// Blocks for at most `timeout` waiting for a value.
    pub fn recv_timeout(self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.recv_deadline(Instant::now() + timeout)
    }

    /// Blocks until a value is received, the sender is dropped, or `deadline` is reached.
    pub fn recv_deadline(self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        self.wait(Some(deadline))
    }

    /// Parks the current thread until a value is sent, the sender is dropped,
    /// or the optional deadline expires.
    fn wait(self, deadline: Option<Instant>) -> Result<T, RecvTimeoutError> {
        let thread = Box::into_raw(Box::new(thread::current()));
        unsafe { *self.inner().receiver_thread.get() = thread };
        let mut state = self.inner().state.fetch_or(WAITING, Ordering::AcqRel);

        loop {
            if !has_any(state, SENT | SENDER_CLOSED) {
                match deadline {
                    Some(dl) => {
                        let now = Instant::now();
                        if now >= dl {
                            return Err(RecvTimeoutError::Timeout);
                        }
                        thread::park_timeout(dl - now);
                    }
                    None => thread::park(),
                }
            } else if has(state, SENT) {
                break;
            } else if has(state, SENDER_CLOSED) {
                return Err(RecvTimeoutError::Disconnected);
            }

            state = self.inner().state.load(Ordering::Acquire);
        }

        self.inner().state.fetch_or(RECEIVED, Ordering::Acquire);
        Ok(unsafe { (&*self.inner().data.get()).assume_init_read() })
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = self
            .inner()
            .state
            .fetch_or(RECEIVER_CLOSED, Ordering::Acquire);

        if has(state, SENDER_CLOSED) {
            if !has(state, RECEIVED) && has(state, SENT) {
                unsafe { (&mut *self.inner().data.get()).assume_init_drop() };
            }

            if has(state, WAITING) {
                dealloc(unsafe { *self.inner().receiver_thread.get() });
            }

            dealloc(self.inner);
        }
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Send for Receiver<T> {}

/// Creates a new oneshot channel, returning the sender and receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Box::into_raw(Box::new(Inner {
        data: UnsafeCell::new(MaybeUninit::uninit()),
        state: AtomicU8::new(EMPTY),
        receiver_thread: UnsafeCell::new(std::ptr::null_mut()),
    }));

    let sender = Sender::new(inner);
    let receiver = Receiver::new(inner);

    (sender, receiver)
}
