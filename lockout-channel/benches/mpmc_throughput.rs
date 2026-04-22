use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::thread;

fn lockout_mpmc_single_sender_receiver(c: &mut Criterion) {
    c.bench_function("lockout_mpmc_1s_1r_1000", |b| {
        b.iter(|| {
            let (tx, rx) = lockout_channel::mpmc::channel::<usize>();
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    tx.send(black_box(i)).unwrap();
                }
            });

            let mut count = 0;
            while let Ok(_) = rx.recv() {
                count += 1;
                if count >= 1000 {
                    break;
                }
            }
            handle.join().unwrap();
            count
        });
    });
}

fn lockout_mpmc_multi_producer(c: &mut Criterion) {
    let mut group = c.benchmark_group("lockout_mpmc_multi_producer");
    
    for num_producers in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_producers),
            num_producers,
            |b, &num_producers| {
                b.iter(|| {
                    let (tx, rx) = lockout_channel::mpmc::channel::<usize>();
                    let rx = std::sync::Arc::new(rx);
                    
                    let mut handles = Vec::new();
                    for p in 0..num_producers {
                        let tx = tx.clone();
                        handles.push(thread::spawn(move || {
                            for i in 0..100 {
                                tx.send(black_box(p * 100 + i)).unwrap();
                            }
                        }));
                    }
                    drop(tx);

                    let mut count = 0;
                    while let Ok(_) = rx.recv() {
                        count += 1;
                        if count >= num_producers * 100 {
                            break;
                        }
                    }
                    
                    for h in handles {
                        h.join().unwrap();
                    }
                    count
                });
            },
        );
    }
    group.finish();
}

fn crossbeam_single_sender_receiver(c: &mut Criterion) {
    c.bench_function("crossbeam_1s_1r_1000", |b| {
        b.iter(|| {
            let (tx, rx) = crossbeam_channel::unbounded::<usize>();
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    tx.send(black_box(i)).unwrap();
                }
            });

            let mut count = 0;
            while let Ok(_) = rx.recv() {
                count += 1;
                if count >= 1000 {
                    break;
                }
            }
            handle.join().unwrap();
            count
        });
    });
}

fn crossbeam_multi_producer(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_multi_producer");
    
    for num_producers in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_producers),
            num_producers,
            |b, &num_producers| {
                b.iter(|| {
                    let (tx, rx) = crossbeam_channel::unbounded::<usize>();
                    let rx = std::sync::Arc::new(rx);
                    
                    let mut handles = Vec::new();
                    for p in 0..num_producers {
                        let tx = tx.clone();
                        handles.push(thread::spawn(move || {
                            for i in 0..100 {
                                tx.send(black_box(p * 100 + i)).unwrap();
                            }
                        }));
                    }
                    drop(tx);

                    let mut count = 0;
                    while let Ok(_) = rx.recv() {
                        count += 1;
                        if count >= num_producers * 100 {
                            break;
                        }
                    }
                    
                    for h in handles {
                        h.join().unwrap();
                    }
                    count
                });
            },
        );
    }
    group.finish();
}

fn std_mpsc_single_sender_receiver(c: &mut Criterion) {
    c.bench_function("std_mpsc_1s_1r_1000", |b| {
        b.iter(|| {
            let (tx, rx) = std::sync::mpsc::channel::<usize>();
            let handle = thread::spawn(move || {
                for i in 0..1000 {
                    tx.send(black_box(i)).unwrap();
                }
            });

            let mut count = 0;
            while let Ok(_) = rx.recv() {
                count += 1;
                if count >= 1000 {
                    break;
                }
            }
            handle.join().unwrap();
            count
        });
    });
}

criterion_group!(
    benches,
    lockout_mpmc_single_sender_receiver,
    lockout_mpmc_multi_producer,
    crossbeam_single_sender_receiver,
    crossbeam_multi_producer,
    std_mpsc_single_sender_receiver
);

criterion_main!(benches);
