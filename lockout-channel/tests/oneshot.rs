use lockout_channel::oneshot::channel;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn send_recv_single() {
    let (tx, rx) = channel();
    tx.send(42).unwrap();
    assert_eq!(rx.recv().unwrap(), 42);
}

#[test]
fn send_recv_across_threads() {
    let (tx, rx) = channel();
    thread::spawn(move || tx.send(7).unwrap());
    assert_eq!(rx.recv().unwrap(), 7);
}

#[test]
fn try_recv_empty() {
    let (_tx, rx) = channel::<i32>();
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn try_recv_ready() {
    let (tx, rx) = channel();
    tx.send(99).unwrap();
    assert_eq!(rx.try_recv().unwrap(), 99);
}

#[test]
fn try_recv_disconnected() {
    let (tx, rx) = channel::<i32>();
    drop(tx);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
}

#[test]
fn recv_timeout_success() {
    let (tx, rx) = channel();
    thread::spawn(move || tx.send(5).unwrap());
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 5);
}

#[test]
fn recv_timeout_expires() {
    let (_tx, rx) = channel::<i32>();
    assert!(matches!(
        rx.recv_timeout(Duration::from_millis(10)),
        Err(RecvTimeoutError::Timeout),
    ));
}

#[test]
fn recv_timeout_disconnected() {
    let (tx, rx) = channel::<i32>();
    drop(tx);
    assert!(matches!(
        rx.recv_timeout(Duration::from_secs(1)),
        Err(RecvTimeoutError::Disconnected),
    ));
}

#[test]
fn recv_deadline_success() {
    let (tx, rx) = channel();
    thread::spawn(move || tx.send(3).unwrap());
    let deadline = Instant::now() + Duration::from_secs(1);
    assert_eq!(rx.recv_deadline(deadline).unwrap(), 3);
}

#[test]
fn recv_deadline_expires() {
    let (_tx, rx) = channel::<i32>();
    let deadline = Instant::now() + Duration::from_millis(10);
    assert!(matches!(
        rx.recv_deadline(deadline),
        Err(RecvTimeoutError::Timeout),
    ));
}

#[test]
fn recv_sender_dropped() {
    let (tx, rx) = channel::<i32>();
    thread::spawn(move || drop(tx));
    assert!(matches!(rx.recv(), Err(RecvError)));
}

#[test]
fn send_receiver_dropped() {
    let (tx, rx) = channel();
    drop(rx);
    assert!(tx.send(1).is_err());
}

#[test]
fn drop_with_unsent_value() {
    let drop_count = Arc::new(AtomicUsize::new(0));

    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (tx, rx) = channel();
    tx.send(Tracked(drop_count.clone())).unwrap();
    drop(rx);

    assert_eq!(drop_count.load(Ordering::Relaxed), 1);
}

#[test]
fn drop_no_leak_on_send_failure() {
    let drop_count = Arc::new(AtomicUsize::new(0));

    struct Tracked(Arc<AtomicUsize>);
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let (tx, rx) = channel();
    drop(rx);
    let _ = tx.send(Tracked(drop_count.clone()));

    assert_eq!(drop_count.load(Ordering::Relaxed), 1);
}
