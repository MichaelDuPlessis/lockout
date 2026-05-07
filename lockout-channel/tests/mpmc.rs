use lockout_channel::mpmc::channel;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, TryRecvError};
use std::thread;
use std::time::Duration;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Repeat a closure `n` times to increase the chance of hitting a race window.
/// Under Miri the count is reduced automatically via the cfg flag.
#[cfg(not(miri))]
const REPEAT: usize = 200;
#[cfg(miri)]
const REPEAT: usize = 10;

fn repeat(f: impl Fn()) {
    for _ in 0..REPEAT {
        f();
    }
}

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

// ── waiter / park-unpark races ────────────────────────────────────────────────

/// A receiver blocks (parks) while a sender sends concurrently.
/// The waiter-notification path must not lose the message.
#[test]
fn blocking_recv_woken_by_send() {
    repeat(|| {
        let (tx, rx) = channel();
        let h = thread::spawn(move || rx.recv().unwrap());
        tx.send(1).unwrap();
        assert_eq!(h.join().unwrap(), 1);
    });
}

/// Multiple receivers block simultaneously; each must receive exactly one value.
#[test]
fn multiple_blocked_receivers_all_woken() {
    repeat(|| {
        let (tx, rx) = channel::<usize>();
        let rx = Arc::new(rx);
        let n = 4;

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let rx = rx.clone();
                thread::spawn(move || rx.recv().unwrap())
            })
            .collect();

        for i in 0..n {
            tx.send(i).unwrap();
        }

        let mut got: Vec<usize> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        got.sort_unstable();
        assert_eq!(got, (0..n).collect::<Vec<_>>());
    });
}

/// All blocked receivers must observe `RecvError` when the last sender drops.
#[test]
fn blocked_receivers_disconnected_on_sender_drop() {
    repeat(|| {
        let (tx, rx) = channel::<i32>();
        let rx = Arc::new(rx);
        let n = 4;

        let handles: Vec<_> = (0..n)
            .map(|_| {
                let rx = rx.clone();
                thread::spawn(move || rx.recv())
            })
            .collect();

        drop(tx);

        for h in handles {
            assert!(matches!(h.join().unwrap(), Err(RecvError)));
        }
    });
}

/// A receiver that times out must not prevent subsequent sends/receives from
/// working correctly (cancelled waiter left in the stack).
#[test]
fn timed_out_waiter_does_not_block_future_messages() {
    repeat(|| {
        let (tx, rx) = channel::<i32>();

        // Let the receiver time out, leaving a Cancelled waiter in the stack.
        let _ = rx.recv_timeout(Duration::from_nanos(1));

        // The channel must still be usable.
        tx.send(42).unwrap();
        assert_eq!(rx.recv().unwrap(), 42);
    });
}

// ── sender / receiver clone-drop races ───────────────────────────────────────

/// Clone a sender on one thread while the original is being dropped on another.
/// No message must be lost and no panic must occur.
#[test]
fn sender_clone_drop_race() {
    repeat(|| {
        let (tx, rx) = channel::<i32>();
        let tx2 = tx.clone();

        let h = thread::spawn(move || {
            tx2.send(1).unwrap();
        });

        drop(tx); // races with the clone above
        h.join().unwrap();
        assert_eq!(rx.recv().unwrap(), 1);
    });
}

/// Clone a receiver on one thread while the original is being dropped on another.
#[test]
fn receiver_clone_drop_race() {
    repeat(|| {
        let (tx, rx) = channel::<i32>();
        let rx2 = rx.clone();

        let h = thread::spawn(move || {
            drop(rx2);
        });

        tx.send(1).unwrap();
        h.join().unwrap();
        // rx is still alive — must receive the message.
        assert_eq!(rx.recv().unwrap(), 1);
    });
}

// ── send-after-last-receiver-drops ───────────────────────────────────────────

/// Dropping the last receiver while a send is in flight must return SendError,
/// never panic or deadlock.
#[test]
fn send_races_with_last_receiver_drop() {
    repeat(|| {
        let (tx, rx) = channel::<i32>();

        let h = thread::spawn(move || drop(rx));

        // May succeed or fail depending on timing — must not panic.
        let _ = tx.send(1);
        h.join().unwrap();
    });
}

// ── no messages lost under high contention ───────────────────────────────────

/// Many senders and receivers, verify every sent message is received exactly once.
#[test]
fn no_messages_lost_high_contention() {
    let (tx, rx) = channel::<usize>();
    let rx = Arc::new(rx);

    #[cfg(not(miri))]
    let (producers, per_producer, consumers) = (8, 1_000, 8);
    #[cfg(miri)]
    let (producers, per_producer, consumers) = (2, 10, 2);

    let total = producers * per_producer;
    let received = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for p in 0..producers {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..per_producer {
                tx.send(p * per_producer + i).unwrap();
            }
        }));
    }
    drop(tx);

    for _ in 0..consumers {
        let rx = rx.clone();
        let received = received.clone();
        handles.push(thread::spawn(move || {
            while rx.recv().is_ok() {
                received.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(received.load(Ordering::Relaxed), total);
}

// ── send races with recv_timeout ─────────────────────────────────────────────

/// A message sent concurrently with recv_timeout must either be received by
/// that call or remain in the queue for the next recv.
#[test]
fn send_races_with_recv_timeout() {
    repeat(|| {
        let (tx, rx) = channel::<i32>();

        let h = thread::spawn(move || {
            let _ = tx.send(1);
        });

        let result = rx.recv_timeout(Duration::from_millis(5));
        h.join().unwrap();

        match result {
            Ok(v) => assert_eq!(v, 1),
            Err(RecvTimeoutError::Timeout) => {
                match rx.try_recv() {
                    Ok(v) => assert_eq!(v, 1),
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => {}
                }
            }
            Err(RecvTimeoutError::Disconnected) => {}
        }
    });
}
