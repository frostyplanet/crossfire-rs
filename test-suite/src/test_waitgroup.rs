use crate::*;
use crossfire::waitgroup::{WaitGroup, WaitGroupInline};
use crossfire::*;
use fastrand;
use rstest::*;
use std::sync::Arc;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
    // Seed fastrand for more deterministic testing.
    fastrand::seed(
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
    );
}

#[logfn]
#[rstest]
fn test_basic_wg_try_wait(setup_log: ()) {
    let mut wg = WaitGroup::new((), 0);
    assert_eq!(wg.get_left(), 0);
    wg.wait(); // should return immediately
    assert_eq!(wg.try_wait(), Ok(()));
    // change threshold
    wg.set_threshold(1);
    assert_eq!(wg.try_wait(), Ok(()));
    let guard1 = wg.add_guard();
    assert_eq!(wg.try_wait(), Ok(()));
    let guard2 = wg.add_guard();
    assert_eq!(wg.try_wait(), Err(()));
    drop(guard2);
    assert_eq!(wg.try_wait(), Ok(()));
    // change threshold
    wg.set_threshold(0);
    assert_eq!(wg.try_wait(), Err(()));
    drop(guard1);
    assert_eq!(wg.try_wait(), Ok(()));
    assert_eq!(wg.try_wait(), Ok(()));
}

#[logfn]
#[rstest]
fn test_waitgroup_with_state(setup_log: ()) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let wg = WaitGroup::new(AtomicBool::new(true), 0);
    for i in 0..10 {
        let guard = wg.add_guard();
        std::thread::spawn(move || {
            if i == 5 {
                guard.store(false, Ordering::SeqCst);
            }
            drop(guard);
        });
    }
    wg.wait();
    assert_eq!(wg.load(Ordering::SeqCst), false);
}

#[logfn]
#[rstest]
fn test_basic_wg_timeout_blocking(setup_log: ()) {
    // Test timeout case
    let wg = WaitGroup::new((), 0);
    let _guard = wg.add_guard();
    assert_eq!(wg.wait_timeout(Duration::from_millis(100)), Err(()));
    let _wg = WaitGroup::new((), 0);
    let _guard_parent = _wg.add_guard();
    // Test drop while guard not finish
    let th = std::thread::spawn(move || {
        _wg.wait();
        std::thread::sleep(Duration::from_secs(1));
        drop(_guard);
    });
    assert!(wg.wait_timeout(Duration::from_millis(10)).is_err());
    drop(_guard_parent);
    if wg.get_left() > 0 {
        println!("drop early");
        drop(wg);
    }
    th.join().expect("join");
}

#[logfn]
#[rstest]
fn test_basic_no_wait_async(setup_log: ()) {
    runtime_block_on!(async move {
        let wg = WaitGroup::new((), 0);
        assert_eq!(wg.get_left(), 0);
        wg.wait_async().await; // should return immediately
        assert_eq!(wg.try_wait(), Ok(()));
    });
}

#[logfn]
#[rstest]
fn test_basic_wg_one_guard_async(setup_log: ()) {
    runtime_block_on!(async move {
        let wg = WaitGroup::new((), 0);
        let guard = wg.add_guard();
        assert_eq!(wg.get_left(), 1);
        assert_eq!(wg.try_wait(), Err(()));

        let _ = async_spawn!(async move {
            sleep(Duration::from_millis(100)).await;
            drop(guard);
        });

        wg.wait_async().await;
        assert_eq!(wg.get_left_seqcst(), 0);
    });
}

#[logfn]
#[rstest]
fn test_basic_wg_multi_guards_async(setup_log: ()) {
    const NUM_GUARDS: usize = 10;
    runtime_block_on!(async move {
        let mut wg = WaitGroup::new((), 3);
        let mut guards = Vec::new();
        for _ in 0..NUM_GUARDS {
            guards.push(wg.add_guard());
        }
        assert_eq!(wg.get_left(), NUM_GUARDS);
        // test clone of the WaitGroupGuard
        let guards1 = guards.clone();
        assert_eq!(wg.get_left(), NUM_GUARDS * 2);
        let guards2 = guards;
        let _ = async_spawn!(async move {
            sleep(Duration::from_millis(10)).await;
            drop(guards1);
        });
        let _ = async_spawn!(async move {
            sleep(Duration::from_millis(10)).await;
            drop(guards2);
        });
        wg.wait_async().await;
        assert!(wg.get_left() <= 3);
        // change threshold
        wg.set_threshold(0);
        wg.wait_async().await;
        assert_eq!(wg.get_left(), 0);
    });
}

#[logfn]
#[rstest]
fn test_basic_wg_timeout_async(setup_log: ()) {
    runtime_block_on!(async move {
        let wg = WaitGroup::new((), 0);
        let guard = wg.add_guard();
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            drop(guard);
        });
        assert_eq!(wg.wait_async_with_timer(sleep(Duration::from_secs(1))).await, Ok(()));
        async_join_result!(th);

        #[cfg(feature = "tokio")]
        {
            let wg_child = WaitGroup::new((), 0);
            let guard_parent = wg_child.add_guard();
            let guard = wg.add_guard();
            let th = async_spawn!(async move {
                wg_child.wait_async().await;
                sleep(Duration::from_secs(1)).await;
                drop(guard);
                log::info!("drop guard");
            });
            assert!(tokio::time::timeout(Duration::from_millis(10), wg.wait_async())
                .await
                .is_err());
            drop(wg);
            log::info!("drop wg");
            drop(guard_parent);
            async_join_result!(th);
        }
    });
}

#[logfn]
#[rstest]
#[cfg_attr(miri, ignore)]
fn test_pressure_wg_blocking_spawn_sleep(setup_log: ()) {
    let wg = WaitGroup::new((), 0);
    let mut loop_cnt = 0;
    for _ in 0..50 {
        let num_guards = fastrand::u32(1..=10); // Generate between 1 and 10 guards
        loop_cnt += 1;
        info!("loop_cnt={} threads={}", loop_cnt, num_guards);

        let mut guards = Vec::new();
        for _ in 0..num_guards {
            guards.push(wg.add_guard());
        }
        let mut handles = Vec::new();
        for (i, guard) in guards.into_iter().enumerate() {
            handles.push(spawn_named_thread(&format!("worker-{}", i), move || {
                let millis = fastrand::u64(0..=10); // Sleep for 0 to 10 milliseconds
                std::thread::sleep(Duration::from_millis(millis));
                drop(guard);
            }));
        }
        wg.wait();
        assert_eq!(wg.get_left_seqcst(), 0);

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

#[logfn]
#[rstest]
#[case(0, 5)]
#[case(2, 8)]
#[case(3, 20)]
#[case(10, 50)]
fn test_pressure_wg_async_channel(
    setup_log: (), #[case] threshold: usize, #[case] num_tasks: usize,
) {
    #[cfg(miri)]
    {
        if num_tasks > 10 {
            println!("skip");
            return;
        }
    }
    runtime_block_on!(async move {
        let (tx, rx) = mpmc::unbounded_async();
        let mut wg = WaitGroup::new((), threshold);
        let mut total_received = 0;

        // Spawn consumer tasks
        let mut th_s = Vec::new();
        for _ in 0..num_tasks {
            let _rx = rx.clone();
            let th = async_spawn!(async move {
                let mut count = 0;
                while let Ok(guard) = _rx.recv().await {
                    count += 1;
                    drop(guard);
                }
                count
            });
            th_s.push(th);
        }
        drop(rx);

        for i in 0..ROUND {
            wg.wait_async().await;
            assert!(wg.get_left() <= threshold);
            log::trace!("send {i}");
            // Publish next batch.
            for _ in 0..num_tasks {
                let guard = wg.add_guard();
                tx.send(guard).expect("send");
            }
        }
        drop(tx);
        log::info!("change threshold");
        wg.set_threshold(0);
        wg.wait_async().await;
        assert_eq!(wg.get_left(), 0);
        for th in th_s {
            total_received += async_join_result!(th);
        }
        assert_eq!(num_tasks * ROUND, total_received);
    });
}

#[logfn]
#[rstest]
#[case(0, 5)]
#[case(2, 4)]
#[case(3, 20)]
#[case(10, 50)]
fn test_pressure_wg_async_channel_sleep(
    setup_log: (), #[case] threshold: usize, #[case] num_tasks: usize,
) {
    let rounds: usize = {
        #[cfg(miri)]
        {
            if num_tasks > 5 {
                println!("skip");
                return;
            }
            10
        }
        #[cfg(not(miri))]
        100
    };
    runtime_block_on!(async move {
        let (tx, rx) = mpmc::unbounded_async();
        let mut wg = WaitGroup::new((), threshold);
        let mut total_received = 0;

        // Spawn consumer tasks
        let mut th_s = Vec::new();
        for _ in 0..num_tasks {
            let _rx = rx.clone();
            let th = async_spawn!(async move {
                let mut count = 0;
                while let Ok(guard) = _rx.recv().await {
                    count += 1;
                    // Simulate work
                    sleep(Duration::from_millis(fastrand::u64(1..=5))).await;
                    drop(guard);
                }
                count
            });
            th_s.push(th);
        }
        drop(rx);

        for i in 0..rounds {
            wg.wait_async().await;
            assert!(wg.get_left() <= threshold);
            log::trace!("send {i}");
            // Publish next batch.
            for _ in 0..num_tasks {
                let guard = wg.add_guard();
                tx.send(guard).expect("send");
            }
        }
        drop(tx);
        log::info!("change threshold");
        wg.set_threshold(0);
        wg.wait_async().await;
        assert_eq!(wg.get_left(), 0);
        for th in th_s {
            total_received += async_join_result!(th);
        }
        assert_eq!(num_tasks * rounds, total_received);
    });
}

#[logfn]
#[rstest]
#[case(0, 5)]
#[case(2, 8)]
#[case(3, 20)]
#[case(4, 10)]
fn test_pressure_wg_blocking_channel(
    setup_log: (), #[case] threshold: usize, #[case] num_threads: usize,
) {
    #[cfg(miri)]
    {
        if num_threads > 10 {
            println!("skip");
            return;
        }
    }
    runtime_block_on!(async move {
        let (tx, rx) = mpmc::unbounded_blocking();
        let mut wg = WaitGroup::new((), threshold);
        let mut total_received = 0;

        // Spawn consumer tasks
        let mut th_s = Vec::new();
        for _ in 0..num_threads {
            let _rx = rx.clone();
            let th = std::thread::spawn(move || {
                let mut count = 0;
                while let Ok(guard) = _rx.recv() {
                    count += 1;
                    drop(guard);
                }
                count
            });
            th_s.push(th);
        }
        drop(rx);

        for i in 0..ROUND {
            wg.wait();
            assert!(wg.get_left() <= threshold);
            log::trace!("send {i}");
            // Publish next batch.
            for _ in 0..num_threads {
                let guard = wg.add_guard();
                tx.send(guard).expect("send");
            }
        }
        drop(tx);
        log::info!("change threshold");
        wg.set_threshold(0);
        wg.wait();
        assert_eq!(wg.get_left(), 0);
        for th in th_s {
            total_received += th.join().unwrap();
        }
        assert_eq!(num_threads * ROUND, total_received);
    });
}

#[logfn]
#[rstest]
fn test_waitgroup_inline(setup_log: ()) {
    let wg = Arc::new(WaitGroupInline::<0>::new());
    assert_eq!(wg.get_left_seqcst(), 0);
    wg.add_many(1);
    assert!(wg.try_wait().is_err());
    let _wg = wg.clone();
    let th = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        unsafe { _wg.done_many(1) };
    });
    unsafe { wg.wait() };
    th.join().expect("join");
    assert_eq!(wg.get_left_seqcst(), 0);

    runtime_block_on!(async move {
        let _wg = wg.clone();
        wg.add();
        async_spawn!(async move {
            sleep(Duration::from_secs(1)).await;
            unsafe { _wg.done() };
        });
        unsafe { wg.wait_async().await };
        assert_eq!(wg.get_left_seqcst(), 0);
    });
}

#[test]
#[should_panic]
fn test_waitgroup_inline_underflow() {
    recipe::console_logger(ConsoleTarget::Stdout, Level::Trace).test().build().expect("log");
    let wg = WaitGroupInline::<0>::new();
    unsafe { wg.done() };
}
