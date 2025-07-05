use criterion::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;

mod common;
use common::*;

fn _crossbeam_bounded_sync(bound: usize, tx_count: usize, rx_count: usize, msg_count: usize) {
    let (tx, rx) = crossbeam_channel::bounded::<usize>(bound);
    let send_counter = Arc::new(AtomicUsize::new(0));
    let recv_counter = Arc::new(AtomicUsize::new(0));
    let mut th_s = Vec::new();
    for _ in 0..tx_count {
        let _send_counter = send_counter.clone();
        let _tx = tx.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _send_counter.fetch_add(1, Ordering::SeqCst);
            if i < msg_count {
                _tx.send(i).expect("send");
            } else {
                break;
            }
        }));
    }
    drop(tx);
    for _ in 0..(rx_count - 1) {
        let _rx = rx.clone();
        let _recv_counter = recv_counter.clone();
        th_s.push(thread::spawn(move || loop {
            match _rx.recv() {
                Ok(_) => {
                    let _ = _recv_counter.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    break;
                }
            }
        }));
    }
    loop {
        match rx.recv() {
            Ok(_) => {
                let _ = recv_counter.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_s {
        let _ = th.join();
    }
    assert!(send_counter.load(Ordering::Acquire) >= msg_count);
    assert!(recv_counter.load(Ordering::Acquire) >= msg_count);
}

fn bench_crossbeam_bounded_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_bounded_sync");
    group.significance_level(0.1).sample_size(50);
    for input in [1, 2, 4, 8, 16] {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", input), &param, |b, i| {
            b.iter(|| _crossbeam_bounded_sync(100, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", param.to_string()), &param, |b, i| {
            b.iter(|| _crossbeam_bounded_sync(100, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_crossbeam_bounded_sync,);
criterion_main!(benches);
