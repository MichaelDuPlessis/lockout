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

#[derive(Debug)]
#[repr(u8)]
enum State {
    Empty,
    Sent,
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

#[derive(Debug)]
struct Inner<T> {
    data: UnsafeCell<MaybeUninit<T>>,
    state: AtomicU8,
    receiver_thread: AtomicPtr<Thread>,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    fn send(&self, msg: T) -> Result<(), SendError<T>> {
        unsafe { (&mut *self.data.get()).write(msg) };
        if self
            .state
            .compare_exchange(
                State::Empty.to_u8(),
                State::Sent.to_u8(),
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return Err(SendError(unsafe {
                (&mut *self.data.get()).assume_init_read()
            }));
        }

        let thread_ptr = self.receiver_thread.load(Ordering::Acquire);
        if !thread_ptr.is_null() {
            unsafe { (*thread_ptr).unpark() };
        }

        Ok(())
    }

    fn recv(&self) -> Result<T, RecvError> {
        let thread = Box::into_raw(Box::new(thread::current()));
        self.receiver_thread.store(thread, Ordering::Release);

        loop {
            let state = State::from(self.state.load(Ordering::Acquire));
            match state {
                State::Empty => thread::park(),
                State::Sent => break,
                State::Closed => return Err(RecvError),
            }
        }

        let _ = unsafe { Box::from_raw(self.receiver_thread.load(Ordering::Relaxed)) };

        Ok(unsafe { (&*self.data.get()).assume_init_read() })
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        match State::from(self.state.load(Ordering::Acquire)) {
            State::Empty => Err(TryRecvError::Empty),
            State::Sent => Ok(unsafe { (&*self.data.get()).assume_init_read() }),
            State::Closed => Err(TryRecvError::Disconnected),
        }
    }

    fn recv_deadline(&self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        let thread = Box::into_raw(Box::new(thread::current()));
        self.receiver_thread.store(thread, Ordering::Release);

        loop {
            let state = State::from(self.state.load(Ordering::Acquire));
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

#[derive(Debug)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Sender<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    pub fn send(self, msg: T) -> Result<(), SendError<T>> {
        self.inner.send(msg)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self
            .inner
            .state
            .compare_exchange(
                State::Empty.to_u8(),
                State::Closed.to_u8(),
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            let thread_ptr = self.inner.receiver_thread.load(Ordering::Acquire);
            if !thread_ptr.is_null() {
                unsafe { (*thread_ptr).unpark() };
            }
        }
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Receiver<T> {
    fn new(inner: Arc<Inner<T>>) -> Self {
        Self { inner }
    }

    pub fn recv(self) -> Result<T, RecvError> {
        self.inner.recv()
    }

    pub fn try_recv(self) -> Result<T, TryRecvError> {
        self.inner.try_recv()
    }

    pub fn recv_timeout(self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.recv_deadline(Instant::now() + timeout)
    }

    pub fn recv_deadline(self, deadline: Instant) -> Result<T, RecvTimeoutError> {
        self.inner.recv_deadline(deadline)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
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
