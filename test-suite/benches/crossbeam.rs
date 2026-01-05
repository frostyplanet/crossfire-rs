use criterion::*;
use std::thread;
use std::time::Duration;

#[allow(unused_imports)]
mod common;
use common::*;

fn _crossbeam_bounded_sync(bound: usize, tx_count: usize, rx_count: usize, msg_count: usize) {
    let (tx, rx) = crossbeam_channel::bounded::<usize>(bound);
    let mut th_tx = Vec::new();
    let mut th_rx = Vec::new();
    let mut send_counter: usize = 0;
    let _send_counter = msg_count / tx_count;
    for _ in 0..tx_count {
        send_counter += _send_counter;
        let _tx = tx.clone();
        th_tx.push(thread::spawn(move || {
            for i in 0.._send_counter {
                _tx.send(i).expect("send");
            }
        }));
    }
    drop(tx);
    let mut recv_counter = 0;
    for _ in 0..(rx_count - 1) {
        let _rx = rx.clone();
        th_rx.push(thread::spawn(move || -> usize {
            let mut i = 0;
            loop {
                match _rx.recv() {
                    Ok(_) => {
                        i += 1;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            i
        }));
    }
    loop {
        match rx.recv() {
            Ok(_) => {
                recv_counter += 1;
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_tx {
        let _ = th.join();
    }
    for th in th_rx {
        if let Ok(count) = th.join() {
            recv_counter += count;
        }
    }
    assert_eq!(send_counter, recv_counter);
}

fn _crossbeam_unbounded_sync(tx_count: usize, rx_count: usize, msg_count: usize) {
    let (tx, rx) = crossbeam_channel::unbounded::<usize>();
    let mut th_tx = Vec::new();
    let mut th_rx = Vec::new();
    let mut send_counter: usize = 0;
    let _send_counter = msg_count / tx_count;
    for _ in 0..tx_count {
        send_counter += _send_counter;
        let _tx = tx.clone();
        th_tx.push(thread::spawn(move || {
            for i in 0.._send_counter {
                _tx.send(i).expect("send");
            }
        }));
    }
    drop(tx);
    let mut recv_counter = 0;
    for _ in 0..(rx_count - 1) {
        let _rx = rx.clone();
        th_rx.push(thread::spawn(move || -> usize {
            let mut i = 0;
            loop {
                match _rx.recv() {
                    Ok(_) => {
                        i += 1;
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
            i
        }));
    }
    loop {
        match rx.recv() {
            Ok(_) => {
                recv_counter += 1;
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_tx {
        let _ = th.join();
    }
    for th in th_rx {
        if let Ok(count) = th.join() {
            recv_counter += count;
        }
    }
    assert_eq!(send_counter, recv_counter);
}

fn bench_crossbeam_bounded_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_bounded");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc size 1", input), &param, |b, i| {
            b.iter(|| _crossbeam_bounded_sync(1, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc size 100", input), &param, |b, i| {
            b.iter(|| _crossbeam_bounded_sync(100, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in n_n() {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(
            BenchmarkId::new("mpmc size 100", param.to_string()),
            &param,
            |b, i| b.iter(|| _crossbeam_bounded_sync(100, i.tx_count, i.rx_count, ONE_MILLION)),
        );
    }
    group.finish();
}

fn bench_crossbeam_unbounded_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_unbounded");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", input), &param, |b, i| {
            b.iter(|| _crossbeam_unbounded_sync(i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in n_n() {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", param.to_string()), &param, |b, i| {
            b.iter(|| _crossbeam_unbounded_sync(i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_crossbeam_bounded_sync, bench_crossbeam_unbounded_sync);
criterion_main!(benches);
