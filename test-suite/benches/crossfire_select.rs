use criterion::*;
use crossfire::{
    mpsc::Array,
    select::{Multiplex, Mux, Select, SelectMode},
    *,
};
use std::thread;
use std::time::Duration;

#[allow(unused_imports, dead_code)]
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
                .tracing_global()
                .add_sink(ring)
                .add_sink(LogConsole::new(
                    ConsoleTarget::Stdout,
                    Level::Info,
                    recipe::LOG_FORMAT_DEBUG,
                ));
            config.dynamic = true;
            config.build().expect("log_setup");
        });
    }
}

const NUM_CHANNELS: usize = 4;
const BOUND: usize = 100;

fn spawn_senders<T>(txs: Vec<T>, total_msgs: usize) -> Vec<thread::JoinHandle<()>>
where
    T: BlockingTxTrait<usize> + Send + Clone + 'static,
{
    let msgs_per_channel = total_msgs / txs.len();
    txs.into_iter()
        .map(|tx| {
            thread::spawn(move || {
                for i in 0..msgs_per_channel {
                    tx.send(i).expect("send");
                }
            })
        })
        .collect()
}

fn run_select(mode: SelectMode, total_msgs: usize) {
    let mut receivers = Vec::with_capacity(NUM_CHANNELS);
    let mut senders = Vec::with_capacity(NUM_CHANNELS);
    for _ in 0..NUM_CHANNELS {
        let (tx, rx) = mpsc::bounded_blocking::<usize>(BOUND);
        receivers.push(rx);
        senders.push(tx);
    }
    let mut select = Select::new_with(mode);
    for rx in &receivers {
        select.add(rx);
    }
    let handles = spawn_senders(senders, total_msgs);
    let mut recv_counter = 0;
    while recv_counter < total_msgs {
        match select.select() {
            Ok(res) => {
                for rx in &receivers {
                    if res == *rx {
                        match rx.read_select(res) {
                            Ok(_) => {
                                recv_counter += 1;
                            }
                            Err(RecvError) => {
                                select.remove(rx);
                            }
                        }
                        break;
                    }
                }
            }
            Err(RecvError) => break,
        }
    }
    assert_eq!(total_msgs, recv_counter);
    for h in handles {
        h.join().unwrap();
    }
}

fn run_multiplex(mode: SelectMode, total_msgs: usize) {
    let mut mp = Multiplex::<Array<usize>>::new_with(mode);
    let mut senders: Vec<MTx<Mux<Array<usize>>>> = Vec::with_capacity(NUM_CHANNELS);
    for _ in 0..NUM_CHANNELS {
        let tx = mp.bounded_tx(BOUND);
        senders.push(tx);
    }
    let handles = spawn_senders(senders, total_msgs);
    let mut recv_counter = 0;
    while recv_counter < total_msgs {
        match mp.recv() {
            Ok(_) => {
                recv_counter += 1;
            }
            Err(RecvError) => break,
        }
    }
    assert_eq!(total_msgs, recv_counter);
    for h in handles {
        h.join().unwrap();
    }
}

fn bench_select(c: &mut Criterion) {
    init_logger();
    let mut group = c.benchmark_group("select");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));

    group.bench_function("select_rr", |b| b.iter(|| run_select(SelectMode::RR, ONE_MILLION)));
    group.bench_function("select_rand", |b| b.iter(|| run_select(SelectMode::Rand, ONE_MILLION)));
    group.bench_function("select_bias", |b| b.iter(|| run_select(SelectMode::Bias, ONE_MILLION)));

    group.finish();
}

fn bench_multiplex(c: &mut Criterion) {
    init_logger();
    let mut group = c.benchmark_group("multiplex");
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(20));
    group.throughput(Throughput::Elements(ONE_MILLION as u64));

    group.bench_function("multiplex_rr", |b| b.iter(|| run_multiplex(SelectMode::RR, ONE_MILLION)));
    group.bench_function("multiplex_rand", |b| {
        b.iter(|| run_multiplex(SelectMode::Rand, ONE_MILLION))
    });
    group.bench_function("multiplex_bias", |b| {
        b.iter(|| run_multiplex(SelectMode::Bias, ONE_MILLION))
    });

    group.finish();
}

criterion_group!(benches, bench_select, bench_multiplex);
criterion_main!(benches);
