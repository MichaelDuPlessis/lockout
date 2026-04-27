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
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicU8, Ordering},
        mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError},
    },
    thread::{self, Thread},
    time::{Duration, Instant},
};

/// Internal channel state, tracked atomically as a `u8`.
#[derive(Debug)]
#[repr(u8)]
enum State {
    /// No value has been sent yet.
    Empty,
    /// A value has been written and is ready to read.
    Sent,
    /// The channel is closed (sender or receiver was dropped).
    Closed,
}

impl State {
    const fn to_u8(self) -> u8 {
        self as u8
    }
}

impl From<State> for u8 {
    fn from(value: State) -> Self {
        value as u8
    }
}

impl From<State> for AtomicU8 {
    fn from(value: State) -> Self {
        Self::new(value.into())
    }
}

impl From<u8> for State {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Empty,
            1 => Self::Sent,
            2 => Self::Closed,
            _ => panic!("This should never be reachable State can only be 0, 1, 2."),
        }
    }
}

/// Shared state between [`Sender`] and [`Receiver`].
#[derive(Debug)]
struct Inner<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
    /// Stores the receiver's thread handle so the sender can unpark it.
    receiver_thread: AtomicPtr<Thread>,
}

// Safety: Data is transferred (not shared) between threads. All access is
// synchronized via atomic state transitions with appropriate orderings.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    /// Writes `msg` into the channel and transitions state to `Sent`.
    ///
    /// The data is written before the CAS. `SeqCst` ordering on the state
    /// CAS and `receiver_thread` load prevents store-buffer reordering that
    /// could cause the sender to miss the receiver's thread handle.
    /// On failure the receiver is already gone, so reading the data back is safe.
    fn send(&self, msg: T) -> Result<(), SendError<T>> {
        unsafe { (&mut *self.data.get()).write(msg) };
        if self
            .state
            .compare_exchange(
                State::Empty.to_u8(),
                State::Sent.to_u8(),
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return Err(SendError(unsafe {
                (&mut *self.data.get()).assume_init_read()
            }));
        }

        let thread_ptr = self.receiver_thread.load(Ordering::SeqCst);
        if !thread_ptr.is_null() {
            unsafe { (*thread_ptr).unpark() };
        }

        Ok(())
    }

    /// Blocks until a value is available or the sender is dropped.
    ///
    /// Registers the current thread for unparking before checking state.
    /// Both operations use `SeqCst` to prevent store-buffer reordering
    /// where the sender could miss the thread handle while the receiver
    /// misses the state update.
    fn recv(&self) -> Result<T, RecvError> {
        let thread = Box::into_raw(Box::new(thread::current()));
        self.receiver_thread.store(thread, Ordering::SeqCst);

        loop {
            let state = State::from(self.state.load(Ordering::SeqCst));
            match state {
                State::Empty => thread::park(),
                State::Sent => break,
                State::Closed => {
                    let _ = unsafe { Box::from_raw(self.receiver_thread.load(Ordering::Relaxed)) };
                    return Err(RecvError);
                }
            }
        }

        let _ = unsafe { Box::from_raw(self.receiver_thread.load(Ordering::Relaxed)) };

        Ok(unsafe { (&*self.data.get()).assume_init_read() })
    }

    /// Returns the value immediately if available, without blocking.
    fn try_recv(&self) -> Result<T, TryRecvError> {
        match State::from(self.state.load(Ordering::Acquire)) {
            State::Empty => Err(TryRecvError::Empty),
            State::Sent => Ok(unsafe { (&*self.data.get()).assume_init_read() }),
            State::Closed => Err(TryRecvError::Disconnected),
        }
    }

    /// Blocks until a value is available, the sender is dropped, or the deadline passes.
    fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        let thread = Box::into_raw(Box::new(thread::current()));
        self.receiver_thread.store(thread, Ordering::SeqCst);

        loop {
            let state = State::from(self.state.load(Ordering::SeqCst));
            match state {
                State::Empty => {
                    let now = Instant::now();
                    if now >= deadline {
                        let _ = unsafe { Box::from_raw(self.receiver_thread.load(Ordering::Relaxed)) };
                        return Err(RecvTimeoutError::Timeout);
                    }
                    thread::park_timeout(deadline - now);
                }
                State::Sent => break,
                State::Closed => {
                    let _ = unsafe { Box::from_raw(self.receiver_thread.load(Ordering::Relaxed)) };
                    return Err(RecvTimeoutError::Disconnected);
                }
            }
        }

        let _ = unsafe { Box::from_raw(self.receiver_thread.load(Ordering::Relaxed)) };

        Ok(unsafe { (&*self.data.get()).assume_init_read() })
    }
}

/// The sending half of a oneshot channel.
///
/// Created by [`channel`]. Consumed on [`send`](Sender::send) or dropped
/// to signal disconnection to the receiver.
#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    /// Sends `msg` through the channel, consuming the sender.
    ///
    /// Returns `Err(SendError(msg))` if the receiver has been dropped.
    pub fn send(self, msg: T) -> Result<(), SendError<T>> {
        self.inner.send(msg)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // Transition to Closed only if no value was sent.
        if self
            .inner
            .state
            .compare_exchange(
                State::Empty.to_u8(),
                State::Closed.to_u8(),
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            let thread_ptr = self.inner.receiver_thread.load(Ordering::SeqCst);
            if !thread_ptr.is_null() {
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
    inner: Arc<Inner<T>>,
}

impl<T> Receiver<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    /// Blocks until a value is received or the sender is dropped.
    pub fn recv(self) -> Result<T, RecvError> {
        self.inner.recv()
    }

    /// Returns the value if it has already been sent, without blocking.
    pub fn try_recv(self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }

    /// Blocks for at most `timeout` waiting for a value.
    pub fn recv_timeout(self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.recv_deadline(Instant::now() + timeout)
    }

    /// Blocks until a value is received, the sender is dropped, or `deadline` is reached.
    pub fn recv_deadline(self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        self.inner.recv_deadline(deadline)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // If a value was sent but never received, drop it.
        if self
            .inner
            .state
            .swap(State::Closed.to_u8(), Ordering::Acquire)
            == State::Sent.to_u8()
        {
            unsafe { (&mut *self.inner.data.get()).assume_init_drop() };
        }
    }
}

/// Creates a new oneshot channel, returning the sender and receiver halves.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        data: UnsafeCell::new(MaybeUninit::uninit()),
        state: AtomicU8::new(State::Empty.to_u8()),
        receiver_thread: AtomicPtr::new(std::ptr::null_mut()),
    });

    let sender = Sender::new(Arc::clone(&inner));
    let receiver = Receiver::new(inner);

    (sender, receiver)
}
