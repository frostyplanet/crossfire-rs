use criterion::*;
use std::thread;
mod common;
use common::*;

fn bench_async_oneshot_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_oneshot_async");
    let count = TEN_THOUSAND;
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function("spawn", |b| {
        b.to_async(BenchExecutor()).iter(|| async move {
            let mut txs = Vec::with_capacity(count);
            let mut rxs = Vec::with_capacity(count);
            for _i in 0..count {
                let (tx, rx) = async_oneshot::oneshot();
                txs.push(tx);
                rxs.push(rx);
            }
            async_spawn!(async move {
                for mut tx in txs {
                    let _ = tx.send(0);
                }
            });
            for rx in rxs {
                let _ = rx.await;
            }
        })
    });
    group.finish();
}

fn bench_oneshot_async(c: &mut Criterion) {
    let mut group = c.benchmark_group("oneshot_async");
    let count = TEN_THOUSAND;
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function("spawn", |b| {
        b.to_async(BenchExecutor()).iter(|| async move {
            let mut txs = Vec::with_capacity(count);
            let mut rxs = Vec::with_capacity(count);
            for _i in 0..count {
                let (tx, rx) = oneshot::channel();
                txs.push(tx);
                rxs.push(rx);
            }
            async_spawn!(async move {
                for tx in txs {
                    let _ = tx.send(0);
                }
            });
            for rx in rxs {
                let _ = rx.await;
            }
        })
    });
    group.finish();
}

fn bench_oneshot_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("oneshot_thread");
    let count = TEN_THOUSAND;
    group.throughput(Throughput::Elements(count as u64));
    group.bench_function("thread", |b| {
        b.iter(|| {
            let mut txs = Vec::with_capacity(count);
            let mut rxs = Vec::with_capacity(count);
            for _i in 0..count {
                let (tx, rx) = oneshot::channel();
                txs.push(tx);
                rxs.push(rx);
            }
            let t = thread::spawn(move || {
                for tx in txs {
                    let _ = tx.send(0);
                }
            });
            for rx in rxs {
                let _ = rx.recv();
            }
            t.join().unwrap();
        })
    });
    group.finish();
}

criterion_group!(
    extra_benches,
    bench_async_oneshot_async,
    bench_oneshot_async,
    bench_oneshot_thread
);
criterion_main!(extra_benches);
