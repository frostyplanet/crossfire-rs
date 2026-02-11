use criterion::*;
use crossfire::waitgroup::{WaitGroup, WaitGroupGuard};
use crossfire::*;
use std::thread;
use std::time::Duration;

#[allow(unused_imports)]
mod common;
use common::*;

// Initialize logger for benchmarks
fn init_logger() {
    #[cfg(feature = "trace_log")]
    {
        use captains_log::*;
        use std::sync::Once;
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let format = recipe::LOG_FORMAT_THREADED_DEBUG;
            let ring = ringfile::LogRingFile::new(
                "/tmp/crossfire_ring.log",
                500 * 1024 * 1024,
                Level::Debug,
                format,
            );
            let mut config = Builder::default()
                .signal(signal_consts::SIGINT)
                .signal(signal_consts::SIGTERM)
                .add_sink(ring)
                .add_sink(LogConsole::new(
                    ConsoleTarget::Stdout,
                    Level::Info,
                    recipe::LOG_FORMAT_DEBUG,
                ));
            config.dynamic = true;
            config.panic = true;
            config.build().expect("log_setup");
        });
    }
}

macro_rules! bench_bounded_blocking {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr) => {
        bench_bounded_blocking!($group, $name, $tx, $rx, $new, $size, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}_{}", $name, $size).to_string(), &param),
            &param,
            |b, i| {
                b.iter(move || {
                    let (tx, rx) = $new($size);
                    _crossfire_blocking(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    );
                })
            },
        );
    };
}

macro_rules! bench_unbounded_blocking {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr) => {
        bench_unbounded_blocking!($group, $name, $tx, $rx, $new, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}", $name).to_string(), &param),
            &param,
            |b, i| {
                b.iter(move || {
                    let (tx, rx) = $new();
                    _crossfire_blocking(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    );
                })
            },
        );
    };
}

macro_rules! bench_bounded_async {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr) => {
        bench_bounded_async!($group, $name, $tx, $rx, $new, $size, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $size: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}_{}", $name, $size).to_string(), &param),
            &param,
            |b, i| {
                b.to_async(BenchExecutor()).iter(async || {
                    let (tx, rx) = $new($size);
                    _crossfire_bounded_async(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    )
                    .await;
                })
            },
        );
    };
}

macro_rules! bench_unbounded_async {
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr) => {
        bench_unbounded_async!($group, $name, $tx, $rx, $new, $count, 20, 100);
    };
    ($group: expr, $name: expr, $tx: expr, $rx: expr, $new: expr, $count: expr, $time: expr, $sample: expr) => {
        $group.throughput(Throughput::Elements($count as u64));
        $group.significance_level(0.1).sample_size($sample);
        $group.measurement_time(Duration::from_secs($time));
        let param = Concurrency { tx_count: $tx, rx_count: $rx };
        $group.bench_with_input(
            BenchmarkId::new(format!("{}", $name).to_string(), &param),
            &param,
            |b, i| {
                b.to_async(BenchExecutor()).iter(async || {
                    let (tx, rx) = $new();
                    _crossfire_blocking_async(
                        tx.clone_to_vec(i.tx_count),
                        rx.clone_to_vec(i.rx_count),
                        $count,
                    )
                    .await;
                })
            },
        );
    };
}

fn _crossfire_blocking<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let mut th_tx = Vec::new();
    let mut th_rx = Vec::new();
    let mut send_counter: usize = 0;
    let _send_counter = msg_count / txs.len();
    for (i, _tx) in txs.into_iter().enumerate() {
        send_counter += _send_counter;
        let th_builder = thread::Builder::new().name(format!("sender{}", i));
        th_tx.push(
            th_builder
                .spawn(move || {
                    for i in 0.._send_counter {
                        _tx.send(i).expect("send");
                    }
                    crossfire::trace_log!("sender exit {:?}", _tx);
                })
                .expect("spawn"),
        );
    }
    let rx_count = rxs.len();
    for i in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
        let th_builder = thread::Builder::new().name(format!("receiver{}", i));
        th_rx.push(
            th_builder
                .spawn(move || -> usize {
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
                })
                .expect("spawn"),
        );
    }
    let rx = rxs.pop().unwrap();
    let mut recv_counter = 0;
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
        let _ = th.join().unwrap();
    }
    for th in th_rx {
        recv_counter += th.join().unwrap();
    }
    assert_eq!(send_counter, recv_counter);
    crossfire::trace_log!("---");
}

async fn _crossfire_blocking_async<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let mut send_counter: usize = 0;
    let _send_counter = msg_count / txs.len();
    let mut th_tx = Vec::new();
    for tx in txs {
        send_counter += _send_counter;
        th_tx.push(thread::spawn(move || {
            for i in 0.._send_counter {
                if let Err(e) = tx.send(i) {
                    panic!("send error: {:?}", e);
                }
            }
        }));
    }
    let mut recv_counter = 0;
    let rx_count = rxs.len();
    let mut th_rx = Vec::new();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
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
    let rx = rxs.pop().unwrap();
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
    assert_eq!(rxs.len(), 0);
    for th in th_tx {
        let _ = th.join().unwrap();
    }
    for th in th_rx {
        recv_counter += async_join_result!(th);
    }
    assert_eq!(send_counter, recv_counter);
}

async fn _crossfire_bounded_async<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    txs: Vec<T>, mut rxs: Vec<R>, msg_count: usize,
) {
    let mut send_counter: usize = 0;
    let _send_counter = msg_count / txs.len();
    let mut th_tx = Vec::new();
    let mut th_rx = Vec::new();
    for tx in txs {
        send_counter += _send_counter;
        th_tx.push(async_spawn!(async move {
            for i in 0.._send_counter {
                if let Err(e) = tx.send(i).await {
                    panic!("send error: {:?}", e);
                }
            }
        }));
    }
    let mut recv_counter = 0;
    let rx_count = rxs.len();
    for _ in 0..(rx_count - 1) {
        let _rx = rxs.pop().unwrap();
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
    let rx = rxs.pop().unwrap();
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
        let _ = th.await;
    }
    for th in th_rx {
        recv_counter += async_join_result!(th);
    }
    assert_eq!(send_counter, recv_counter);
}

fn crossfire_bounded_1_blocking_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_1_1");
    bench_bounded_blocking!(group, "spsc", 1, 1, spsc::bounded_blocking, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_blocking!(group, "mpsc", 1, 1, mpsc::bounded_blocking, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_blocking!(group, "mpmc", 1, 1, mpmc::bounded_blocking, 1, TEN_THOUSAND, 10, 100);
    group.finish();
}

fn crossfire_bounded_1_blocking_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_n_1");
    for input in n_1() {
        bench_bounded_blocking!(
            group,
            "mpsc",
            input,
            1,
            mpsc::bounded_blocking,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    for input in n_1() {
        bench_bounded_blocking!(
            group,
            "mpmc",
            input,
            1,
            mpmc::bounded_blocking,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_1_blocking_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_1_blocking_n_n");
    for input in n_n() {
        bench_bounded_blocking!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_blocking,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_100_blocking_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_1_1");
    bench_bounded_blocking!(group, "spsc", 1, 1, spsc::bounded_blocking, 100, ONE_MILLION);
    bench_bounded_blocking!(group, "mpsc", 1, 1, mpsc::bounded_blocking, 100, ONE_MILLION);
    bench_bounded_blocking!(group, "mpmc", 1, 1, mpmc::bounded_blocking, 100, ONE_MILLION);
    group.finish();
}

fn crossfire_bounded_100_blocking_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_n_1");
    for input in n_1() {
        bench_bounded_blocking!(group, "mpsc", input, 1, mpsc::bounded_blocking, 100, ONE_MILLION);
    }
    for input in n_1() {
        bench_bounded_blocking!(group, "mpmc", input, 1, mpmc::bounded_blocking, 100, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_bounded_100_blocking_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_100_blocking_n_n");
    for input in n_n() {
        bench_bounded_blocking!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_blocking,
            100,
            ONE_MILLION
        );
    }
    group.finish();
}

fn crossfire_bounded_1_async_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_1_async_1_1");
    bench_bounded_async!(group, "spsc", 1, 1, spsc::bounded_async, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_async!(group, "mpsc", 1, 1, mpsc::bounded_async, 1, TEN_THOUSAND, 10, 100);
    bench_bounded_async!(group, "mpmc", 1, 1, mpmc::bounded_async, 1, TEN_THOUSAND, 10, 100);
    group.finish();
}

fn crossfire_bounded_1_async_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_1_async_n_1");
    for input in n_1() {
        bench_bounded_async!(
            group,
            "mpsc",
            input,
            1,
            mpsc::bounded_async,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    for input in n_1() {
        bench_bounded_async!(
            group,
            "mpmc",
            input,
            1,
            mpmc::bounded_async,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_1_async_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_1_async_n_n");
    for input in n_n() {
        bench_bounded_async!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_async,
            1,
            TEN_THOUSAND,
            10,
            100
        );
    }
    group.finish();
}

fn crossfire_bounded_100_async_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_100_async_1_1");
    bench_bounded_async!(group, "spsc", 1, 1, spsc::bounded_async, 100, ONE_MILLION);
    bench_bounded_async!(group, "mpsc", 1, 1, mpsc::bounded_async, 100, ONE_MILLION);
    bench_bounded_async!(group, "mpmc", 1, 1, mpmc::bounded_async, 100, ONE_MILLION);
    group.finish();
}

fn crossfire_bounded_100_async_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_100_async_n_1");
    for input in n_1() {
        bench_bounded_async!(group, "mpsc", input, 1, mpsc::bounded_async, 100, ONE_MILLION);
    }

    for input in n_1() {
        bench_bounded_async!(group, "mpmc", input, 1, mpmc::bounded_async, 100, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_bounded_100_async_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_bounded_100_async_n_n");
    for input in n_n() {
        bench_bounded_async!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::bounded_async,
            100,
            ONE_MILLION
        );
    }
    group.finish();
}

fn crossfire_unbounded_blocking_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_1_1");
    bench_unbounded_blocking!(group, "spsc", 1, 1, spsc::unbounded_blocking, ONE_MILLION);
    bench_unbounded_blocking!(group, "mpsc", 1, 1, mpsc::unbounded_blocking, ONE_MILLION);
    bench_unbounded_blocking!(group, "mpmc", 1, 1, mpmc::unbounded_blocking, ONE_MILLION);
    group.finish();
}

fn crossfire_unbounded_blocking_n_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_n_1");
    for input in n_1() {
        bench_unbounded_blocking!(group, "mpsc", input, 1, mpsc::unbounded_blocking, ONE_MILLION);
    }
    for input in n_1() {
        bench_unbounded_blocking!(group, "mpmc", input, 1, mpmc::unbounded_blocking, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_unbounded_blocking_n_n(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_unbounded_blocking_n_n");
    for input in n_n() {
        bench_unbounded_blocking!(
            group,
            "mpmc",
            input.0,
            input.1,
            mpmc::unbounded_blocking,
            ONE_MILLION
        );
    }
    group.finish();
}

fn crossfire_unbounded_async_1_1(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_unbounded_async_1_1");
    bench_unbounded_async!(group, "spsc", 1, 1, spsc::unbounded_async, ONE_MILLION);
    bench_unbounded_async!(group, "mpsc", 1, 1, mpsc::unbounded_async, ONE_MILLION);
    bench_unbounded_async!(group, "mpmc", 1, 1, mpmc::unbounded_async, ONE_MILLION);
    group.finish();
}

fn crossfire_unbounded_async_mpsc(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_unbounded_async_n_1");
    for input in n_1() {
        bench_unbounded_async!(group, "mpsc", input, 1, mpsc::unbounded_async, ONE_MILLION);
    }
    for input in n_1() {
        bench_unbounded_async!(group, "mpmc", input, 1, mpmc::unbounded_async, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_unbounded_async_mpmc(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_unbounded_async_n_n");
    for input in n_n() {
        bench_unbounded_async!(group, "mpmc", input.0, input.1, mpmc::unbounded_async, ONE_MILLION);
    }
    group.finish();
}

fn crossfire_oneshot_blocking(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_oneshot_blocking");
    let count = TEN_THOUSAND;
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function("spawn", |b| {
        b.iter(|| {
            let mut txs = Vec::with_capacity(count);
            let mut rxs = Vec::with_capacity(count);
            for _i in 0..count {
                let (tx, rx) = crossfire::oneshot::oneshot();
                txs.push(tx);
                rxs.push(rx);
            }
            thread::spawn(move || {
                for tx in txs {
                    let _ = tx.send(0);
                }
            });
            for rx in rxs {
                let _ = rx.recv();
            }
        })
    });
    group.finish();
}

fn crossfire_oneshot_async(c: &mut Criterion) {
    detect_backoff_cfg();
    init_logger();
    let mut group = c.benchmark_group("crossfire_oneshot_async");
    let count = TEN_THOUSAND;
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function("spawn", |b| {
        b.to_async(BenchExecutor()).iter(|| async move {
            let mut txs = Vec::with_capacity(count);
            let mut rxs = Vec::with_capacity(count);
            for _i in 0..count {
                let (tx, rx) = crossfire::oneshot::oneshot();
                txs.push(tx);
                rxs.push(rx);
            }
            let th = async_spawn!(async move {
                for tx in txs {
                    tx.send(0);
                }
            });
            for rx in rxs {
                let _ = rx.await;
            }
            let _ = async_join_result!(th);
        })
    });
    group.finish();
}

fn bench_crossfire_wait_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("crossfire_wait_group");
    let count = TEN_THOUSAND; // Or some appropriate number for throughput
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function("add_guard", |b| {
        let wg = WaitGroup::new(0);
        b.iter(|| {
            let mut guards: Vec<WaitGroupGuard> = Vec::with_capacity(count);
            for _i in 0..count {
                guards.push(wg.add_guard());
            }
            // guards are dropped here
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    crossfire_bounded_1_blocking_1_1,
    crossfire_bounded_1_blocking_n_1,
    crossfire_bounded_1_blocking_n_n,
    crossfire_bounded_100_blocking_1_1,
    crossfire_bounded_100_blocking_n_1,
    crossfire_bounded_100_blocking_n_n,
    crossfire_unbounded_blocking_1_1,
    crossfire_unbounded_blocking_n_1,
    crossfire_unbounded_blocking_n_n,
    crossfire_bounded_1_async_1_1,
    crossfire_bounded_1_async_n_1,
    crossfire_bounded_1_async_n_n,
    crossfire_bounded_100_async_1_1,
    crossfire_bounded_100_async_n_1,
    crossfire_bounded_100_async_n_n,
    crossfire_unbounded_async_1_1,
    crossfire_unbounded_async_mpsc,
    crossfire_unbounded_async_mpmc,
    crossfire_oneshot_blocking,
    crossfire_oneshot_async,
    bench_crossfire_wait_group,
);
criterion_main!(benches);
