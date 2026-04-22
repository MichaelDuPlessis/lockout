use lockout_channel::mpmc::channel;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, TryRecvError};
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
    let (tx, rx) = channel::<i32>();
    drop(tx);
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

    let got = rx.recv_timeout(Duration::from_millis(1000)).unwrap();
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
            while rx.recv().is_ok() {
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
