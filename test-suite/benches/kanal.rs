use criterion::*;
use std::thread;
use std::time::Duration;

#[allow(unused_imports)]
mod common;
use common::*;

fn _kanal_bounded_blocking(bound: usize, tx_count: usize, rx_count: usize, msg_count: usize) {
    let (tx, rx) = kanal::bounded::<usize>(bound);
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

fn _kanal_unbounded_blocking(tx_count: usize, rx_count: usize, msg_count: usize) {
    let (tx, rx) = kanal::unbounded::<usize>();
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

async fn _kanal_bounded_async(bound: usize, tx_count: usize, rx_count: usize, msg_count: usize) {
    let (tx, rx) = kanal::bounded_async(bound);
    let mut th_tx = Vec::new();
    let mut th_rx = Vec::new();
    let mut send_counter: usize = 0;
    let _send_counter = msg_count / tx_count;
    for _tx_i in 0..tx_count {
        send_counter += _send_counter;
        let _tx = tx.clone();
        th_tx.push(async_spawn!(async move {
            for i in 0.._send_counter {
                if let Err(e) = _tx.send(i).await {
                    panic!("send error: {:?}", e);
                }
            }
        }));
    }
    drop(tx);
    let mut recv_counter = 0;
    for _ in 0..(rx_count - 1) {
        let _rx = rx.clone();
        th_rx.push(async_spawn!(async move {
            let mut i = 0;
            loop {
                match _rx.recv().await {
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
        match rx.recv().await {
            Ok(_) => {
                recv_counter += 1;
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_tx {
        let _ = async_join_result!(th);
    }
    for th in th_rx {
        recv_counter += async_join_result!(th);
    }
    assert_eq!(send_counter, recv_counter);
}

async fn _kanal_unbounded_async(tx_count: usize, rx_count: usize, msg_count: usize) {
    let (tx, rx) = kanal::unbounded_async();
    let mut th_tx = Vec::new();
    let mut th_rx = Vec::new();
    let mut send_counter: usize = 0;
    let _send_counter = msg_count / tx_count;
    for _tx_i in 0..tx_count {
        send_counter += _send_counter;
        let _tx = tx.clone();
        th_tx.push(async_spawn!(async move {
            for i in 0.._send_counter {
                if let Err(e) = _tx.send(i).await {
                    panic!("send error: {:?}", e);
                }
            }
        }));
    }
    drop(tx);
    let mut recv_counter = 0;
    for _ in 0..(rx_count - 1) {
        let _rx = rx.clone();
        th_rx.push(async_spawn!(async move {
            let mut i = 0;
            loop {
                match _rx.recv().await {
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
        match rx.recv().await {
            Ok(_) => {
                recv_counter += 1;
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_tx {
        let _ = async_join_result!(th);
    }
    for th in th_rx {
        recv_counter += async_join_result!(th);
    }
    assert_eq!(send_counter, recv_counter);
}

fn bench_kanal_bounded_blocking(c: &mut Criterion) {
    let mut group = c.benchmark_group("kanal_bounded_blocking");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpsc size 1", &param), &param, |b, i| {
            b.iter(|| _kanal_bounded_blocking(1, i.tx_count, i.rx_count, TEN_THOUSAND))
        });
    }
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc size 100", &param), &param, |b, i| {
            b.iter(|| _kanal_bounded_blocking(100, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in n_n() {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc size 100", &param), &param, |b, i| {
            b.iter(|| _kanal_bounded_blocking(100, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
}

fn bench_kanal_unbounded_blocking(c: &mut Criterion) {
    let mut group = c.benchmark_group("kanal_unbounded_blocking");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", &param), &param, |b, i| {
            b.iter(|| _kanal_unbounded_blocking(i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in n_n() {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.iter(|| _kanal_unbounded_blocking(i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
}

fn bench_kanal_bounded_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("kanal_bounded_async");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpsc size 1", &param), &param, |b, i| {
            b.to_async(BenchExecutor())
                .iter(|| _kanal_bounded_async(1, i.tx_count, i.rx_count, TEN_THOUSAND))
        });
    }

    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc size 100", &param), &param, |b, i| {
            b.to_async(BenchExecutor())
                .iter(|| _kanal_bounded_async(100, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in n_n() {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc size 100", &param), &param, |b, i| {
            b.to_async(BenchExecutor())
                .iter(|| _kanal_bounded_async(100, i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
}

fn bench_kanal_unbounded_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("kanal_unbounded_async");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in n_1() {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", &param), &param, |b, i| {
            b.to_async(BenchExecutor())
                .iter(|| _kanal_unbounded_async(i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
    for input in n_n() {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.to_async(BenchExecutor())
                .iter(|| _kanal_unbounded_async(i.tx_count, i.rx_count, ONE_MILLION))
        });
    }
}

criterion_group!(
    benches,
    bench_kanal_bounded_async,
    bench_kanal_unbounded_async,
    bench_kanal_bounded_blocking,
    bench_kanal_unbounded_blocking
);
criterion_main!(benches);
