use crate::*;
use captains_log::logfn;
use crossfire::*;
use fastrand;
use rstest::*;
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_basic(setup_log: ()) {
    let (tx, mut rx) = oneshot::oneshot();
    assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    assert_eq!(rx.is_empty(), true);
    tx.send(42);
    assert_eq!(rx.is_empty(), false);
    assert_eq!(rx.recv(), Ok(42));

    let (tx, mut rx) = oneshot::oneshot();
    assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
    tx.send(41);
    assert_eq!(rx.try_recv(), Ok(41));
    assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
    assert_eq!(rx.recv().unwrap_err(), RecvError);
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_tx(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    drop(tx);
    assert_eq!(rx.recv(), Err(RecvError));

    let (tx, rx) = oneshot::oneshot::<i32>();
    let th = thread::spawn(move || {
        // Should be wake up on sender drop
        assert_eq!(rx.recv(), Err(RecvError));
    });
    thread::sleep(Duration::from_millis(fastrand::u64(1..=500)));
    drop(tx);
    th.join().expect("join");
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_rx(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    drop(rx);
    // send consumes tx, returns ()
    tx.send(42);
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_leak(setup_log: ()) {
    // Check if OneShot drops the value if not received
    reset_drop_counter();
    {
        let (tx, _rx) = oneshot::oneshot::<SmallMsg>();
        tx.send(SmallMsg::new(1));
    } // tx dropped (closed), rx dropped (OneShot dropped). msg should be dropped.
    assert_eq!(get_drop_counter(), 1);
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_after_recv(setup_log: ()) {
    // Check if OneShot drops the value after recv (it shouldn't, Rx has it)
    reset_drop_counter();
    {
        let (tx, rx) = oneshot::oneshot::<SmallMsg>();
        tx.send(SmallMsg::new(1));
        let msg = rx.recv().unwrap();
        assert_eq!(get_drop_counter(), 0);
        drop(msg);
        assert_eq!(get_drop_counter(), 1);
    }
    // OneShot dropped. Should NOT drop again.
    assert_eq!(get_drop_counter(), 1);
}

#[logfn]
#[rstest]
fn test_oneshot_async_basic(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, mut rx) = oneshot::oneshot();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        assert_eq!(rx.is_empty(), true);
        tx.send(42);
        assert_eq!(rx.is_empty(), false);
        assert_eq!(rx.await, Ok(42));
        let (tx, mut rx) = oneshot::oneshot();
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);
        tx.send(41);
        assert_eq!(rx.try_recv(), Ok(41));
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        assert_eq!(rx.await.unwrap_err(), RecvError);
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_drop_tx(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::oneshot::<i32>();
        drop(tx);
        assert_eq!(rx.await, Err(RecvError));
        log::debug!("next test");
        let (tx, rx) = oneshot::oneshot::<i32>();
        let th = async_spawn!(async move {
            // Should be wake up on sender drop
            assert_eq!(rx.await, Err(RecvError));
        });
        sleep(Duration::from_millis(fastrand::u64(1..=500))).await;
        drop(tx);
        let _ = async_join_result!(th);
    });
}

#[logfn]
#[rstest]
fn test_oneshot_async_pressure(setup_log: ()) {
    let count = {
        #[cfg(miri)]
        {
            10usize
        }
        #[cfg(not(miri))]
        {
            100usize
        }
    };
    runtime_block_on!(async move {
        let mut tasks = Vec::new();
        for i in 0..count {
            tasks.push(async_spawn!(async move {
                let (tx, rx) = oneshot::oneshot();
                tx.send(i);
                assert_eq!(rx.await, Ok(i));
            }));
        }
        for t in tasks {
            let _ = async_join_result!(t);
        }
    });
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_batch(setup_log: ()) {
    let mut txs = Vec::with_capacity(ROUND);
    let mut rxs = Vec::with_capacity(ROUND);
    for _i in 0..ROUND {
        let (tx, rx) = oneshot::oneshot();
        txs.push(tx);
        rxs.push(rx);
    }
    let th = thread::spawn(move || {
        for (i, tx) in txs.into_iter().enumerate() {
            tx.send(i);
        }
    });
    for (i, rx) in rxs.into_iter().enumerate() {
        assert_eq!(rx.recv(), Ok(i));
    }
    th.join().unwrap();
}

#[logfn]
#[rstest]
fn test_oneshot_async_batch(setup_log: ()) {
    runtime_block_on!(async move {
        let mut txs = Vec::with_capacity(ROUND);
        let mut rxs = Vec::with_capacity(ROUND);
        for _i in 0..ROUND {
            let (tx, rx) = oneshot::oneshot();
            txs.push(tx);
            rxs.push(rx);
        }
        let th = async_spawn!(async move {
            for (i, tx) in txs.into_iter().enumerate() {
                tx.send(i);
            }
        });
        for (i, rx) in rxs.into_iter().enumerate() {
            assert_eq!(rx.await, Ok(i));
        }
        async_join_result!(th);
    });
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_concurrent(setup_log: ()) {
    let count = {
        #[cfg(miri)]
        {
            10usize
        }
        #[cfg(not(miri))]
        {
            50usize
        }
    };
    let mut th_s = Vec::new();
    for i in 0..count {
        let (tx, rx) = oneshot::oneshot();
        th_s.push(thread::spawn(move || {
            tx.send(i);
        }));
        th_s.push(thread::spawn(move || {
            assert_eq!(rx.recv(), Ok(i));
        }));
    }
    for th in th_s {
        th.join().unwrap();
    }
}

#[logfn]
#[rstest]
fn test_oneshot_async_concurrent(setup_log: ()) {
    let count = {
        #[cfg(miri)]
        {
            10usize
        }
        #[cfg(not(miri))]
        {
            100usize
        }
    };
    runtime_block_on!(async move {
        let mut tasks = Vec::new();
        for i in 0..count {
            let (tx, rx) = oneshot::oneshot();
            tasks.push(async_spawn!(async move {
                tx.send(i);
            }));
            tasks.push(async_spawn!(async move {
                assert_eq!(rx.await, Ok(i));
            }));
        }
        for t in tasks {
            let _ = async_join_result!(t);
        }
    });
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_with_sleep(setup_log: ()) {
    #[cfg(miri)]
    {
        // sleep in miri will be too slow
        println!("skip on miri");
        return;
    }
    #[cfg(not(miri))]
    {
        let count = 50usize;
        let mut th_s = Vec::new();
        for i in 0..(count as u64) {
            th_s.push(thread::spawn(move || {
                let (tx, rx) = oneshot::oneshot();
                // Spawn a thread that sends after a short delay
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(i % 10)); // Vary the delay
                    tx.send(i);
                });
                // Wait for the value
                assert_eq!(rx.recv(), Ok(i));
            }));
        }
        for th in th_s {
            th.join().unwrap();
        }
    }
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_with_sleep(setup_log: ()) {
    #[cfg(miri)]
    {
        // sleep in miri will be too slow
        println!("skip on miri");
    }
    #[cfg(not(miri))]
    {
        let count = 50usize;
        runtime_block_on!(async move {
            let mut tasks = Vec::new();
            for i in 0..count {
                tasks.push(async_spawn!(async move {
                    let (tx, rx) = oneshot::oneshot();
                    let th = async_spawn!(async move {
                        sleep(Duration::from_millis((i % 10) as u64)).await;
                        tx.send(i);
                    });

                    // Wait for the value
                    assert_eq!(rx.await, Ok(i));
                    let _ = async_join_result!(th);
                }));
            }
            for t in tasks {
                let _ = async_join_result!(t);
            }
        });
    }
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_batch_with_interval(setup_log: ()) {
    #[cfg(miri)]
    {
        // sleep in miri will be too slow
        println!("skip on miri");
        return;
    }
    #[cfg(not(miri))]
    {
        let batch_size = 30;
        runtime_block_on!(async move {
            let mut tasks = Vec::new();

            // Create a batch of oneshots
            for i in 0..batch_size {
                tasks.push(async_spawn!(async move {
                    let (tx, rx) = oneshot::oneshot();
                    let th = async_spawn!(async move {
                        // Sleep for different durations based on index
                        sleep(Duration::from_millis((i * 2) as u64)).await;
                        tx.send(i);
                    });

                    // Wait for the value
                    assert_eq!(rx.await, Ok(i));
                    let _ = async_join_result!(th);
                }));
            }
            for t in tasks {
                let _ = async_join_result!(t);
            }
        });
    }
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_blocking_timeout_fail(setup_log: ()) {
    let (_tx, rx) = oneshot::oneshot::<i32>();
    let start = std::time::Instant::now();
    let res = rx.recv_timeout(Duration::from_millis(100));
    assert_eq!(res, Err(RecvTimeoutError::Timeout));
    assert!(start.elapsed() >= Duration::from_millis(100));
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_blocking_timeout_success(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    let th = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        tx.send(42);
    });
    let _res = rx.recv_timeout(Duration::from_millis(200));
    let _ = th.join();
    #[cfg(not(miri))]
    assert_eq!(_res, Ok(42));
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_blocking_timeout_disconnected(setup_log: ()) {
    let (tx, rx) = oneshot::oneshot::<i32>();
    let th = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        drop(tx);
    });
    let _res = rx.recv_timeout(Duration::from_millis(200));
    let _ = th.join();
    assert!(_res.is_err());
    // might be timeout or disconnected
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_timeout_fail(setup_log: ()) {
    runtime_block_on!(async move {
        let (_tx, rx) = oneshot::oneshot::<i32>();
        let start = std::time::Instant::now();
        let sleep_fut = sleep(Duration::from_millis(100));
        futures_util::pin_mut!(sleep_fut);
        let res = rx.recv_async_with_timer(sleep_fut).await;
        assert_eq!(res, Err(RecvTimeoutError::Timeout));
        assert!(start.elapsed() >= Duration::from_millis(100));
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_timeout_disconnected(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::oneshot::<i32>();
        let th = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            drop(tx);
        });
        let _res = rx.recv_async_with_timer(Box::pin(sleep(Duration::from_secs(1)))).await;
        let _ = th.join();
        #[cfg(not(miri))]
        assert_eq!(_res, Err(RecvTimeoutError::Disconnected));
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_oneshot_async_timeout_success(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::oneshot::<i32>();
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            tx.send(42);
        });
        let _res = rx.recv_async_with_timer(Box::pin(sleep(Duration::from_secs(2)))).await;
        #[cfg(not(miri))]
        assert_eq!(_res, Ok(42));
        async_join_result!(th);
    });
}
