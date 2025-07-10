use criterion::*;
use crossfire::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

mod common;
use common::*;

fn _crossfire_btx_clone(tx: MTx<usize>, count: usize) -> Vec<MTx<usize>> {
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
        v.push(tx.clone());
    }
    v
}

fn _crossfire_brx_clone(rx: MRx<usize>, count: usize) -> Vec<MRx<usize>> {
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
        v.push(rx.clone());
    }
    v
}

fn _crossfire_atx_clone(tx: MAsyncTx<usize>, count: usize) -> Vec<MAsyncTx<usize>> {
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
        v.push(tx.clone());
    }
    v
}

fn _crossfire_arx_clone(rx: MAsyncRx<usize>, count: usize) -> Vec<MAsyncRx<usize>> {
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
        v.push(rx.clone());
    }
    v
}

fn _crossfire_blocking<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let send_counter = Arc::new(AtomicUsize::new(0));
    let recv_counter = Arc::new(AtomicUsize::new(0));
    let mut th_s = Vec::new();
    for _tx in txs {
        let _send_counter = send_counter.clone();
        th_s.push(thread::spawn(move || loop {
            let i = _send_counter.fetch_add(1, Ordering::SeqCst);
            if i < msg_count {
                _tx.send(i).expect("send");
            } else {
                break;
            }
        }));
    }
    let rx_count = rxs.len();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
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
    let rx = rxs.pop().unwrap();
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

async fn _crossfire_blocking_async<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut sender_th_s = Vec::new();
    for tx in txs {
        let _counter = counter.clone();
        sender_th_s.push(thread::spawn(move || loop {
            let i = _counter.fetch_add(1, Ordering::SeqCst);
            if i < msg_count {
                if let Err(e) = tx.send(i) {
                    panic!("send error: {:?}", e);
                }
            } else {
                break;
            }
        }));
    }
    let recv_counter = Arc::new(AtomicUsize::new(0));
    let rx_count = rxs.len();
    let mut recv_th_s = Vec::new();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
        let _recv_counter = recv_counter.clone();
        recv_th_s.push(tokio::spawn(async move {
            loop {
                match _rx.recv().await {
                    Ok(_) => {
                        let _ = _recv_counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }));
    }
    let rx = rxs.pop().unwrap();
    loop {
        match rx.recv().await {
            Ok(_) => {
                let _ = recv_counter.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in recv_th_s {
        let _ = th.await;
    }
    for th in sender_th_s {
        let _ = th.join();
    }
    assert!(counter.load(Ordering::Acquire) >= msg_count);
    assert!(recv_counter.load(Ordering::Acquire) >= msg_count);
}

async fn _crossfire_bounded_async<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let counter = Arc::new(AtomicUsize::new(0));
    let mut th_s = Vec::new();
    for tx in txs {
        let _counter = counter.clone();
        th_s.push(tokio::spawn(async move {
            loop {
                let i = _counter.fetch_add(1, Ordering::SeqCst);
                if i < msg_count {
                    if let Err(e) = tx.send(i).await {
                        panic!("send error: {:?}", e);
                    }
                } else {
                    break;
                }
            }
        }));
    }
    let recv_counter = Arc::new(AtomicUsize::new(0));
    let rx_count = rxs.len();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
        let _recv_counter = recv_counter.clone();
        th_s.push(tokio::spawn(async move {
            loop {
                match _rx.recv().await {
                    Ok(_) => {
                        let _ = _recv_counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }));
    }
    let rx = rxs.pop().unwrap();
    loop {
        match rx.recv().await {
            Ok(_) => {
                let _ = recv_counter.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                break;
            }
        }
    }
    for th in th_s {
        let _ = th.await;
    }
    assert!(counter.load(Ordering::Acquire) >= msg_count);
    assert!(recv_counter.load(Ordering::Acquire) >= msg_count);
}

fn bench_crossfire_bounded_blocking_1_1(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_blocking_1_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    for (size, msg_count) in [(1, TEN_THOUSAND), (100, ONE_MILLION)] {
        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_function(format!("spsc 1x1 size {}", size).to_string(), |b| {
            b.iter(move || {
                let (tx, rx) = crossfire::spsc::bounded_blocking(size);
                _crossfire_blocking(vec![tx], vec![rx], msg_count);
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }

        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_function(format!("mpsc 1x1 size {}", size).to_string(), |b| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpsc::bounded_blocking(size);
                _crossfire_blocking(vec![tx], vec![rx], msg_count);
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }

        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_function(format!("mpmc 1x1 size {}", size).to_string(), |b| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpmc::bounded_blocking(size);
                _crossfire_blocking(vec![tx], vec![rx], msg_count);
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }
    }
    group.finish();
}

fn bench_crossfire_bounded_1_blocking_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_n_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    for tx_count in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", tx_count), &tx_count, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpsc::bounded_blocking(1);
                _crossfire_blocking(_crossfire_btx_clone(tx, *i), vec![rx], TEN_THOUSAND);
            })
        });
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", tx_count), &tx_count, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpmc::bounded_blocking(1);
                _crossfire_blocking(_crossfire_btx_clone(tx, *i), vec![rx], TEN_THOUSAND);
            })
        });
    }
    group.finish();
}

fn bench_crossfire_bounded_1_blocking_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_n_n");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpmc::bounded_blocking(1);
                _crossfire_blocking(
                    _crossfire_btx_clone(tx, i.tx_count),
                    _crossfire_brx_clone(rx, i.rx_count),
                    TEN_THOUSAND,
                );
            })
        });
    }
    group.finish();
}

fn bench_crossfire_bounded_100_blocking_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_n_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    for tx_count in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", tx_count), &tx_count, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpsc::bounded_blocking(100);
                _crossfire_blocking(_crossfire_btx_clone(tx, *i), vec![rx], ONE_MILLION);
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }
    }

    for tx_count in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", tx_count), &tx_count, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpmc::bounded_blocking(100);
                _crossfire_blocking(_crossfire_btx_clone(tx, *i), vec![rx], ONE_MILLION);
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }
    }

    group.finish();
}

fn bench_crossfire_bounded_100_blocking_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_n_n");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpmc::bounded_blocking(100);
                _crossfire_blocking(
                    _crossfire_btx_clone(tx, i.tx_count),
                    _crossfire_brx_clone(rx, i.rx_count),
                    ONE_MILLION,
                );
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }

    group.finish();
}

fn bench_crossfire_bounded_async_1_1(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_async_1_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    for (size, msg_count) in [(1, TEN_THOUSAND), (100, ONE_MILLION)] {
        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_function(format!("spsc 1x1 size {}", size).to_string(), |b| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::spsc::bounded_async(size);
                _crossfire_bounded_async(vec![tx], vec![rx], msg_count).await;
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }

        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_function(format!("mpsc 1x1 size {}", size).to_string(), |b| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpsc::bounded_async(size);
                _crossfire_bounded_async(vec![tx], vec![rx], msg_count).await;
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }

        group.throughput(Throughput::Elements(msg_count as u64));
        group.bench_function(format!("mpmc 1x1 size {}", size).to_string(), |b| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpmc::bounded_async(size);
                _crossfire_bounded_async(vec![tx], vec![rx], msg_count).await;
            })
        });
        #[cfg(feature = "profile")]
        {
            println!("stats {}", crossfire::ChannelStats::to_string());
            crossfire::ChannelStats::clear();
        }
    }
    group.finish();
}

fn bench_crossfire_bounded_1_async_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_1_async_n_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    for tx_count in [2, 4, 8, 16] {
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", tx_count), &tx_count, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpsc::bounded_async(1);
                _crossfire_bounded_async(_crossfire_atx_clone(tx, *i), vec![rx], TEN_THOUSAND)
                    .await;
            })
        });
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", tx_count), &tx_count, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpmc::bounded_async(1);
                _crossfire_bounded_async(_crossfire_atx_clone(tx, *i), vec![rx], TEN_THOUSAND)
                    .await;
            })
        });
    }
    group.finish();
}

fn bench_crossfire_bounded_1_async_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_1_async_n_n");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpmc::bounded_async(1);
                _crossfire_bounded_async(
                    _crossfire_atx_clone(tx, i.tx_count),
                    _crossfire_arx_clone(rx, i.rx_count),
                    TEN_THOUSAND,
                )
                .await;
            })
        });
    }
    group.finish();
}

fn bench_crossfire_bounded_100_async_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_100_async_n_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    for tx_count in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", tx_count), &tx_count, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpsc::bounded_async(100);
                _crossfire_bounded_async(_crossfire_atx_clone(tx, *i), vec![rx], ONE_MILLION).await;
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    for tx_count in [1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", tx_count), &tx_count, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpmc::bounded_async(100);
                _crossfire_bounded_async(_crossfire_atx_clone(tx, *i), vec![rx], ONE_MILLION).await;
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.finish();
}

fn bench_crossfire_bounded_100_async_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_bounded_100_async_n_n");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpmc::bounded_async(100);
                _crossfire_bounded_async(
                    _crossfire_atx_clone(tx, i.tx_count),
                    _crossfire_arx_clone(rx, i.rx_count),
                    ONE_MILLION,
                )
                .await;
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }

    group.finish();
}

fn bench_crossfire_unbounded_blocking_1_1(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_1_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));
    group.bench_function("spsc 1x1", |b| {
        b.iter(move || {
            let (tx, rx) = crossfire::spsc::unbounded_blocking();
            _crossfire_blocking(vec![tx], vec![rx], ONE_MILLION);
        })
    });
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }

    group.bench_function("mpsc 1x1", |b| {
        b.iter(move || {
            let (tx, rx) = crossfire::mpsc::unbounded_blocking();
            _crossfire_blocking(vec![tx], vec![rx], ONE_MILLION);
        })
    });
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }

    group.bench_function("mpmc 1x1", |b| {
        b.iter(move || {
            let (tx, rx) = crossfire::mpmc::unbounded_blocking();
            _crossfire_blocking(vec![tx], vec![rx], ONE_MILLION);
        })
    });
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }

    group.finish();
}

fn bench_crossfire_unbounded_blocking_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_n_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));
    for input in [1, 2, 4, 8, 16] {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", &param), &param, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpsc::unbounded_blocking();
                _crossfire_blocking(_crossfire_btx_clone(tx, i.tx_count), vec![rx], ONE_MILLION);
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    for input in [1, 2, 4, 8, 16] {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpmc::unbounded_blocking();
                _crossfire_blocking(_crossfire_btx_clone(tx, i.tx_count), vec![rx], ONE_MILLION);
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.finish();
}

fn bench_crossfire_unbounded_blocking_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_n_n");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_function(format!("{}", param).to_string(), |b| {
            b.iter(move || {
                let (tx, rx) = crossfire::mpmc::unbounded_blocking();
                _crossfire_blocking(
                    _crossfire_btx_clone(tx, input.0),
                    _crossfire_brx_clone(rx, input.1),
                    ONE_MILLION,
                );
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.finish();
}

fn bench_crossfire_unbounded_async_1_1(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_unbounded_async_1_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));
    group.bench_function("spsc 1x1", |b| {
        b.to_async(get_runtime()).iter(async || {
            let (tx, rx) = crossfire::spsc::unbounded_async();
            _crossfire_blocking_async(vec![tx], vec![rx], ONE_MILLION).await;
        })
    });
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.bench_function("mpsc 1x1", |b| {
        b.to_async(get_runtime()).iter(async || {
            let (tx, rx) = crossfire::mpsc::unbounded_async();
            _crossfire_blocking_async(vec![tx], vec![rx], ONE_MILLION).await;
        })
    });
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.bench_function("mpmc 1x1", |b| {
        b.to_async(get_runtime()).iter(async || {
            let (tx, rx) = crossfire::mpmc::unbounded_async();
            _crossfire_blocking_async(vec![tx], vec![rx], ONE_MILLION).await;
        })
    });
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.finish();
}

fn bench_crossfire_unbounded_async_mpsc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_unbounded_async_n_1");
    group.significance_level(0.1).sample_size(100);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));
    for input in [1, 2, 4, 8, 16] {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpsc", &param), &param, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpsc::unbounded_async();
                _crossfire_blocking_async(
                    _crossfire_btx_clone(tx, i.tx_count),
                    vec![rx],
                    ONE_MILLION,
                )
                .await;
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    for input in [1, 2, 4, 8, 16] {
        let param = Concurrency { tx_count: input, rx_count: 1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_with_input(BenchmarkId::new("mpmc", &param), &param, |b, i| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpmc::unbounded_async();
                _crossfire_blocking_async(
                    _crossfire_btx_clone(tx, i.tx_count),
                    vec![rx],
                    ONE_MILLION,
                )
                .await;
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.finish();
}

fn bench_crossfire_unbounded_async_mpmc(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_unbounded_async_n_n");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    for input in [(2, 2), (4, 4), (8, 8), (16, 16)] {
        let param = Concurrency { tx_count: input.0, rx_count: input.1 };
        group.throughput(Throughput::Elements(ONE_MILLION as u64));
        group.bench_function(format!("{}", param).to_string(), |b| {
            b.to_async(get_runtime()).iter(async || {
                let (tx, rx) = crossfire::mpmc::unbounded_async();
                _crossfire_blocking_async(
                    _crossfire_btx_clone(tx, input.0),
                    _crossfire_arx_clone(rx, input.1),
                    ONE_MILLION,
                )
                .await;
            })
        });
    }
    #[cfg(feature = "profile")]
    {
        println!("stats {}", crossfire::ChannelStats::to_string());
        crossfire::ChannelStats::clear();
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_crossfire_bounded_async_1_1,
    bench_crossfire_bounded_100_async_mpsc,
    bench_crossfire_bounded_100_async_mpmc,
    bench_crossfire_unbounded_async_1_1,
    bench_crossfire_unbounded_async_mpsc,
    bench_crossfire_unbounded_async_mpmc,
    bench_crossfire_bounded_blocking_1_1,
    bench_crossfire_bounded_1_blocking_mpsc,
    bench_crossfire_bounded_1_blocking_mpmc,
    bench_crossfire_bounded_1_async_mpsc,
    bench_crossfire_bounded_1_async_mpmc,
    bench_crossfire_bounded_100_blocking_mpsc,
    bench_crossfire_bounded_100_blocking_mpmc,
    bench_crossfire_unbounded_blocking_1_1,
    bench_crossfire_unbounded_blocking_mpsc,
    bench_crossfire_unbounded_blocking_mpmc,
);
criterion_main!(benches);
