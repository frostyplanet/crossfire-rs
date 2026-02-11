use criterion::*;
use crossbeam_utils::sync::WaitGroup;
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

fn _crossbeam_select_mpsc(num_channels: usize, bound: usize, total_msgs: usize, is_bias: bool) {
    let msg_count_per_channel = total_msgs / num_channels;
    let mut rxs = Vec::new();
    let mut th_tx = Vec::new();
    for _ in 0..num_channels {
        let (tx, rx) = crossbeam_channel::bounded::<usize>(bound);
        rxs.push(rx);
        th_tx.push(thread::spawn(move || {
            for i in 0..msg_count_per_channel {
                tx.send(i).expect("send");
            }
        }));
    }

    // Receive all messages using select - reuse Select instance
    let mut recv_counter = 0;

    let mut select = if is_bias {
        crossbeam_channel::Select::new_biased()
    } else {
        crossbeam_channel::Select::new()
    };
    let mut handles = Vec::with_capacity(num_channels);
    for rx in &rxs {
        let op = select.recv(rx);
        handles.push(op);
    }
    while recv_counter < total_msgs {
        // Perform the selection
        let oper = select.select();
        let i = oper.index();
        match oper.recv(&rxs[i]) {
            Ok(_) => recv_counter += 1,
            Err(_) => {
                // https://docs.rs/crossbeam-channel/latest/crossbeam_channel/struct.Select.html#method.remove
                // If new operations are added after removing some, the indices of removed operations will not be reused
                select.remove(i);
            }
        }
    }
    assert_eq!(total_msgs, recv_counter);
    // Wait for all senders to finish before receiving
    for th in th_tx {
        let _ = th.join();
    }
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

fn bench_crossbeam_select_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_select");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));

    let param = (4, 100, ONE_MILLION); // 3 channels, bound=100, 1M/3 messages per channel
    group.throughput(Throughput::Elements(ONE_MILLION as u64));
    group.bench_with_input(
        BenchmarkId::new("select_mpsc_4_channels_bias", "4"),
        &param,
        |b, &(num_channels, bound, msg_count_per_channel)| {
            b.iter(|| _crossbeam_select_mpsc(num_channels, bound, msg_count_per_channel, true))
        },
    );
    group.bench_with_input(
        BenchmarkId::new("select_mpsc_4_channels_fair", "4"),
        &param,
        |b, &(num_channels, bound, msg_count_per_channel)| {
            b.iter(|| _crossbeam_select_mpsc(num_channels, bound, msg_count_per_channel, false))
        },
    );

    group.finish();
}

fn bench_crossbeam_wait_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossbeam_wait_group");
    let count = TEN_THOUSAND;
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function("add_guard", |b| {
        let wg = WaitGroup::new();
        b.iter(|| {
            let mut guards: Vec<crossbeam_utils::sync::WaitGroup> = Vec::with_capacity(count);
            for _i in 0..count {
                guards.push(wg.clone());
            }
            // guards are dropped here
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_crossbeam_bounded_sync,
    bench_crossbeam_unbounded_sync,
    bench_crossbeam_select_mpsc,
    bench_crossbeam_wait_group
);
criterion_main!(benches);
