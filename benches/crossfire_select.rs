use criterion::*;
use crossfire::blocking_select::{Select, SelectMode};
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

// Helper function to benchmark select with multiple receive channels (MPMC)
fn bench_recv_select_mpmc_select(
    num_channels: usize, capacity: usize, msg_count: usize, biased: bool, mode: SelectMode,
) {
    trace_log!("starting bench_select_mpmc_select");
    let msgs_per_channel = msg_count / num_channels;

    // Create channels
    let mut channels = Vec::new();
    for _ in 0..num_channels {
        channels.push(mpmc::bounded_blocking::<usize>(capacity));
    }

    // Spawn sender threads
    let mut sender_threads = Vec::new();
    for (idx, (tx, _)) in channels.iter().enumerate() {
        let tx = tx.clone();
        sender_threads.push(thread::spawn(move || {
            for i in 0..msgs_per_channel {
                let _ = tx.send(idx * 1000000 + i);
            }
        }));
    }

    // Use Select to receive from all channels
    let receivers: Vec<_> = channels.drain(..).map(|(_, r)| r).collect();

    // Create the select structure once and reuse mode-specific selects
    let mut select = Select::new(biased);
    for rx in receivers.iter() {
        select.recv(rx);
    }

    let mut recv_count = 0;
    match mode {
        SelectMode::FirstReady => {
            let mut first_ready = select.first_ready();
            while recv_count < msg_count {
                let results = first_ready.select();
                recv_count += results.success_count();
                if results.is_empty() {
                    break;
                }

                if !first_ready.has_ready() {
                    break;
                }
            }
        }
        SelectMode::AnyReady => {
            let mut any_ready = select.any_ready();
            while recv_count < msg_count {
                let results = any_ready.select();
                recv_count += results.success_count();
                if results.is_empty() {
                    break;
                }

                if !any_ready.has_ready() {
                    break;
                }
            }
        }
        SelectMode::AllComplete => {
            let mut all_complete = select.all_complete();
            while recv_count < msg_count {
                let results = all_complete.select();
                recv_count += results.success_count();
                if results.is_empty() {
                    break;
                }

                if !all_complete.has_ready() {
                    break;
                }
            }
        }
    }

    assert_eq!(msg_count, recv_count);

    // Wait for senders
    for th in sender_threads {
        let _ = th.join();
    }

    trace_log!("finished bench_select_mpmc_select");
}

// Helper function for select_next benchmarks
fn bench_recv_select_mpmc_next(
    num_channels: usize, capacity: usize, msg_count: usize, mode: SelectMode,
) {
    let msgs_per_channel = msg_count / num_channels;

    // Create channels
    let mut channels = Vec::new();
    for _ in 0..num_channels {
        channels.push(mpmc::bounded_blocking::<usize>(capacity));
    }

    // Spawn sender threads
    let mut sender_threads = Vec::new();
    for (tx, _) in channels.iter() {
        let tx = tx.clone();
        sender_threads.push(thread::spawn(move || {
            for i in 0..msgs_per_channel {
                let _ = tx.send(i);
            }
        }));
    }

    // Collect receivers
    let receivers: Vec<_> = channels.iter().map(|(_, rx)| rx).collect();

    // Create the select structure once and reuse mode-specific selects
    let mut select = Select::new(false);
    for rx in receivers.iter() {
        select.recv(rx);
    }

    let mut recv_count = 0;
    match mode {
        SelectMode::FirstReady => {
            let mut first_ready = select.first_ready();
            while recv_count < msg_count {
                if first_ready.select_next().all_ok() {
                    recv_count += 1;
                }
            }
        }
        SelectMode::AnyReady => {
            let mut any_ready = select.any_ready();
            while recv_count < msg_count {
                let results = any_ready.select_next();
                recv_count += results.success_count();
            }
        }
        SelectMode::AllComplete => {
            let mut all_complete = select.all_complete();
            while recv_count < msg_count {
                let results = all_complete.select_next();
                recv_count += results.success_count();
            }
        }
    }

    // Wait for senders
    for th in sender_threads {
        let _ = th.join();
    }
}

// Benchmark: Select with varying channel counts (bounded, capacity 1)
fn select_bounded_1_recv_channels(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("select_bounded_1_recv_channels");
    group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for num_channels in [2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_first_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(n, 1, TEN_THOUSAND, true, SelectMode::FirstReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_first_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(n, 1, TEN_THOUSAND, false, SelectMode::FirstReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_any_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(n, 1, TEN_THOUSAND, true, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_any_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(n, 1, TEN_THOUSAND, false, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_all_complete", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(n, 1, TEN_THOUSAND, true, SelectMode::AllComplete)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_all_complete", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(
                        n,
                        1,
                        TEN_THOUSAND,
                        false,
                        SelectMode::AllComplete,
                    )
                })
            },
        );
    }

    group.finish();
}

// Benchmark: Select with varying capacities (4 channels)
fn select_bounded_recv_select_capacities(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("select_bounded_recv_select_capacities");
    group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for capacity in [1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_first_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        true,
                        SelectMode::FirstReady,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_first_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        false,
                        SelectMode::FirstReady,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_any_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(4, cap, TEN_THOUSAND, true, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_any_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(4, cap, TEN_THOUSAND, false, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_all_complete", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        true,
                        SelectMode::AllComplete,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_all_complete", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_recv_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        false,
                        SelectMode::AllComplete,
                    )
                })
            },
        );
    }

    group.finish();
}

// Benchmark: Select with select_next (varying capacities, 4 channels)
fn select_bounded_recv_next_capacities(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("select_bounded_recv_next_capacities");
    group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for capacity in [1, 10, 100, 1000] {
        let msg_count = if capacity >= 100 { ONE_MILLION } else { TEN_THOUSAND };

        group.bench_with_input(
            BenchmarkId::new("mpmc_first_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| bench_recv_select_mpmc_next(4, cap, msg_count, SelectMode::FirstReady))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_any_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| bench_recv_select_mpmc_next(4, cap, msg_count, SelectMode::AnyReady))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_all_complete", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| bench_recv_select_mpmc_next(4, cap, msg_count, SelectMode::AllComplete))
            },
        );
    }

    group.finish();
}

// Helper function to benchmark select with multiple send channels (MPMC)
fn bench_send_select_mpmc_select(
    num_channels: usize, capacity: usize, msg_count: usize, biased: bool, mode: SelectMode,
) {
    trace_log!("starting bench_send_select_mpmc_select");
    let msgs_per_channel = msg_count / num_channels;

    // Create channels
    let mut channels = Vec::new();
    for _ in 0..num_channels {
        channels.push(mpmc::bounded_blocking::<usize>(capacity));
    }

    // Spawn receiver threads
    let mut receiver_threads = Vec::new();
    for (_, rx) in channels.iter() {
        let rx = rx.clone();
        receiver_threads.push(thread::spawn(move || {
            let mut cnt = 0;
            while let Ok(_idx) = rx.recv() {
                cnt += 1;
                trace_log!("received {}", _idx);
            }

            assert_eq!(msgs_per_channel, cnt);
        }));
    }

    // Use Select to send to all channels
    let senders: Vec<_> = channels.drain(..).map(|(tx, _)| tx).collect();

    let mut send_count = 0;
    match mode {
        SelectMode::FirstReady => {
            while send_count < msg_count {
                // Create the select structure for each iteration since items are consumed
                let mut select = Select::new(biased);
                for (idx, tx) in senders.iter().enumerate() {
                    select.send(tx, idx * 1000000 + send_count);
                    trace_log!("sent idx={}, {}", idx, idx * 1000000 + send_count);
                }

                let mut first_ready = select.first_ready();
                let mut batch_sent = 0;
                while batch_sent < num_channels {
                    let results = first_ready.select();
                    batch_sent += results.success_count();
                    if results.is_empty() {
                        break;
                    }
                    if !first_ready.has_ready() {
                        break;
                    }
                }
                send_count += batch_sent;
            }
            trace_log!("finished {:?}", mode);
        }
        SelectMode::AnyReady => {
            while send_count < msg_count {
                let mut select = Select::new(biased);
                for (idx, tx) in senders.iter().enumerate() {
                    select.send(tx, idx * 1000000 + send_count);
                    trace_log!("sent idx={}, {}", idx, idx * 1000000 + send_count);
                }

                let mut any_ready = select.any_ready();
                let mut batch_sent = 0;
                while batch_sent < num_channels {
                    let results = any_ready.select();
                    batch_sent += results.success_count();
                    if results.is_empty() {
                        break;
                    }
                    if !any_ready.has_ready() {
                        break;
                    }
                }
                send_count += batch_sent;
            }
            trace_log!("finished {:?}", mode);
        }
        SelectMode::AllComplete => {
            while send_count < msg_count {
                let mut select = Select::new(biased);
                for (idx, tx) in senders.iter().enumerate() {
                    select.send(tx, idx * 1000000 + send_count);
                    trace_log!("sent idx={}, {}", idx, idx * 1000000 + send_count);
                }

                let mut all_complete = select.all_complete();
                let results = all_complete.select();
                send_count += results.success_count();
                if results.is_empty() {
                    break;
                }
            }
            trace_log!("finished {:?}", mode);
        }
    }

    drop(senders);

    assert_eq!(msg_count, send_count);

    // Wait for receivers
    for th in receiver_threads {
        let _ = th.join();
    }

    trace_log!("finished bench_send_select_mpmc_select");
}

// Helper function for send select_next benchmarks
fn bench_send_select_mpmc_next(
    num_channels: usize, capacity: usize, msg_count: usize, mode: SelectMode,
) {
    let msgs_per_channel = msg_count / num_channels;

    // Create channels
    let mut channels = Vec::new();
    for _ in 0..num_channels {
        channels.push(mpmc::bounded_blocking::<usize>(capacity));
    }

    // Spawn receiver threads
    let mut receiver_threads = Vec::new();
    for (_, rx) in channels.iter() {
        let rx = rx.clone();
        receiver_threads.push(thread::spawn(move || {
            for _ in 0..msgs_per_channel {
                let _ = rx.recv();
            }
        }));
    }

    // Collect senders
    let senders: Vec<_> = channels.iter().map(|(tx, _)| tx).collect();

    let mut send_count = 0;
    match mode {
        SelectMode::FirstReady => {
            while send_count < msg_count {
                let mut select = Select::new(false);
                for tx in senders.iter() {
                    select.send(tx, send_count);
                }

                let mut first_ready = select.first_ready();
                if first_ready.select_next().all_ok() {
                    send_count += 1;
                }
            }
        }
        SelectMode::AnyReady => {
            while send_count < msg_count {
                let mut select = Select::new(false);
                for tx in senders.iter() {
                    select.send(tx, send_count);
                }

                let mut any_ready = select.any_ready();
                let results = any_ready.select_next();
                send_count += results.success_count();
            }
        }
        SelectMode::AllComplete => {
            while send_count < msg_count {
                let mut select = Select::new(false);
                for tx in senders.iter() {
                    select.send(tx, send_count);
                }

                let mut all_complete = select.all_complete();
                let results = all_complete.select_next();
                send_count += results.success_count();
            }
        }
    }

    // Wait for receivers
    for th in receiver_threads {
        let _ = th.join();
    }
}

// Benchmark: Select send with varying channel counts (bounded, capacity 1)
fn select_bounded_1_send_channels(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("select_bounded_1_send_channels");
    group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for num_channels in [2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_first_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_send_select_mpmc_select(n, 1, TEN_THOUSAND, true, SelectMode::FirstReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_first_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_send_select_mpmc_select(n, 1, TEN_THOUSAND, false, SelectMode::FirstReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_any_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_send_select_mpmc_select(n, 1, TEN_THOUSAND, true, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_any_ready", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_send_select_mpmc_select(n, 1, TEN_THOUSAND, false, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_all_complete", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_send_select_mpmc_select(n, 1, TEN_THOUSAND, true, SelectMode::AllComplete)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_all_complete", num_channels),
            &num_channels,
            |b, &n| {
                b.iter(|| {
                    bench_send_select_mpmc_select(
                        n,
                        1,
                        TEN_THOUSAND,
                        false,
                        SelectMode::AllComplete,
                    )
                })
            },
        );
    }

    group.finish();
}

// Benchmark: Select send with varying capacities (4 channels)
fn select_bounded_send_select_capacities(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("select_bounded_send_select_capacities");
    group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for capacity in [1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_first_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_send_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        true,
                        SelectMode::FirstReady,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_first_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_send_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        false,
                        SelectMode::FirstReady,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_any_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_send_select_mpmc_select(4, cap, TEN_THOUSAND, true, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_any_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_send_select_mpmc_select(4, cap, TEN_THOUSAND, false, SelectMode::AnyReady)
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_biased_all_complete", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_send_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        true,
                        SelectMode::AllComplete,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_unbiased_all_complete", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| {
                    bench_send_select_mpmc_select(
                        4,
                        cap,
                        TEN_THOUSAND,
                        false,
                        SelectMode::AllComplete,
                    )
                })
            },
        );
    }

    group.finish();
}

// Benchmark: Select send with select_next (varying capacities, 4 channels)
fn select_bounded_send_next_capacities(c: &mut Criterion) {
    detect_backoff_cfg();
    let mut group = c.benchmark_group("select_bounded_send_next_capacities");
    group.throughput(Throughput::Elements(TEN_THOUSAND as u64));
    group.significance_level(0.1).sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for capacity in [1, 10, 100, 1000] {
        let msg_count = if capacity >= 100 { ONE_MILLION } else { TEN_THOUSAND };

        group.bench_with_input(
            BenchmarkId::new("mpmc_first_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| bench_send_select_mpmc_next(4, cap, msg_count, SelectMode::FirstReady))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_any_ready", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| bench_send_select_mpmc_next(4, cap, msg_count, SelectMode::AnyReady))
            },
        );

        group.bench_with_input(
            BenchmarkId::new("mpmc_all_complete", capacity),
            &capacity,
            |b, &cap| {
                b.iter(|| bench_send_select_mpmc_next(4, cap, msg_count, SelectMode::AllComplete))
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = {
        init_logger();
        Criterion::default()
    };
    targets = select_bounded_1_recv_channels,
    select_bounded_recv_select_capacities,
    select_bounded_recv_next_capacities,
    select_bounded_1_send_channels,
    select_bounded_send_select_capacities,
    select_bounded_send_next_capacities,
}

criterion_main!(benches);
