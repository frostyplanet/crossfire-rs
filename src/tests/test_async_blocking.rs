use super::common::*;
use crate::*;
use captains_log::{logfn, *};
use rstest::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    let _ = recipe::env_logger("LOG_FILE", "LOG_LEVEL").build().expect("log setup");
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_async_rx_blocking::<usize>(100))]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(100))]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(100))]
fn test_basic_1_tx_async_1_rx_blocking<T: AsyncTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    let batch_1: usize = 100;
    let batch_2: usize = 200;
    let th = thread::spawn(move || {
        for _ in 0..(batch_1 + batch_2) {
            match rx.recv() {
                Ok(i) => {
                    debug!("recv {}", i);
                }
                Err(e) => {
                    panic!("error {}", e);
                }
            }
        }
        let res = rx.recv();
        assert!(res.is_err());
    });

    runtime_block_on!(async move {
        for i in 0..batch_1 {
            let tx_res = tx.send(i).await;
            assert!(tx_res.is_ok());
        }
        for i in batch_1..(batch_1 + batch_2) {
            assert!(tx.send(10 + i).await.is_ok());
            sleep(Duration::from_millis(2)).await;
        }
    });
    let _ = th.join();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_async_rx_blocking::<usize>(100))]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(100))]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(100))]
fn test_timeout_1_tx_async_1_rx_blocking<T: AsyncTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    let batch_1: usize = 100;
    let batch_2: usize = 200;
    let th = thread::spawn(move || {
        for _ in 0..(batch_1 + batch_2) {
            match rx.recv() {
                Ok(i) => {
                    debug!("recv {}", i);
                }
                Err(e) => {
                    panic!("error {}", e);
                }
            }
        }

        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_ok());

        let res = rx.recv();
        assert!(res.is_err());
    });

    runtime_block_on!(async move {
        for i in 0..batch_1 {
            let tx_res = tx.send(i).await;
            assert!(tx_res.is_ok());
        }
        for i in batch_1..(batch_1 + batch_2) {
            assert!(tx.send(10 + i).await.is_ok());
            sleep(Duration::from_millis(2)).await;
        }

        sleep(Duration::from_millis(200)).await;
        assert!(tx.send(123).await.is_ok());
    });
    let _ = th.join();
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(10), 8)]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(10), 100)]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(10), 1000)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 8)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 100)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 1000)]
fn test_basic_multi_tx_async_1_rx_blocking<R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;

    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    let (noti_tx, noti_rx) = mpmc::unbounded_blocking::<usize>();
    let batch_1: usize = 100;
    let batch_2: usize = 200;
    let th = thread::spawn(move || {
        for _ in 0..((batch_1 + batch_2) * tx_count) {
            match rx.recv() {
                Ok(i) => {
                    debug!("recv {}", i);
                }
                Err(e) => {
                    panic!("error {}", e);
                }
            }
        }
        let res = rx.recv();
        assert!(res.is_err());
        // Wait for spawn exit
        for _ in 0..tx_count {
            if let Err(_) = noti_rx.recv() {
                break;
            }
        }
    });
    runtime_block_on!(async move {
        let mut th_s = Vec::new();
        for tx_i in 0..tx_count {
            let _tx = tx.clone();
            let _noti_tx = noti_tx.clone();
            th_s.push(async_spawn!(async move {
                for i in 0..batch_1 {
                    let tx_res = _tx.send(i).await;
                    assert!(tx_res.is_ok());
                }
                for i in batch_1..(batch_1 + batch_2) {
                    assert!(_tx.send(10 + i).await.is_ok());
                    sleep(Duration::from_millis(2)).await;
                }
                _noti_tx.send(tx_i).expect("noti send ok");
            }));
        }
        drop(tx);

        for th in th_s {
            let _ = th.await;
        }
    });
    let _ = th.join();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_async_rx_blocking::<usize>(1))]
#[case(spsc::bounded_tx_async_rx_blocking::<usize>(10))]
#[case(spsc::bounded_tx_async_rx_blocking::<usize>(100))]
#[case(spsc::bounded_tx_async_rx_blocking::<usize>(1000))]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(1))]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(10))]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(100))]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(1000))]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(1))]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10))]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(100))]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(1000))]
fn test_pressure_1_tx_async_1_rx_blocking<T: AsyncTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round: usize = 1000000;
    let _round = round;
    let _counter = counter.clone();
    let th = thread::spawn(move || {
        'A: loop {
            match rx.recv() {
                Ok(i) => {
                    _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                    debug!("recv {}", i);
                }
                Err(_) => break 'A,
            }
        }
        debug!("rx exit");
    });
    runtime_block_on!(async move {
        for i in 0..round {
            match tx.send(i).await {
                Err(e) => panic!("{}", e),
                _ => {}
            }
        }
        debug!("tx exit");
    });
    let _ = th.join();
    assert_eq!(counter.as_ref().load(Ordering::Acquire), round);
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(10), 8)]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(10), 10)]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(10), 100)]
#[case(mpsc::bounded_tx_async_rx_blocking::<usize>(100), 200)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 8)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 100)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 10)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(100), 200)]
fn test_pressure_multi_tx_async_1_rx_blocking<R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round: usize = 100000;
    let _round = round;
    let _counter = counter.clone();
    let th = thread::spawn(move || {
        'A: loop {
            match rx.recv() {
                Ok(_i) => {
                    _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                    debug!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        debug!("rx exit");
    });
    runtime_block_on!(async move {
        let (noti_tx, noti_rx) = mpmc::unbounded_async::<usize>();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            let mut _noti_tx = noti_tx.clone();
            async_spawn!(async move {
                for i in 0..round {
                    match _tx.send(i).await {
                        Err(e) => panic!("{}", e),
                        _ => {}
                    }
                }
                let _ = _noti_tx.send(_tx_i);
                debug!("tx {} exit", _tx_i);
            });
        }
        drop(tx);
        drop(noti_tx);
        for _ in 0..(tx_count) {
            if let Err(_) = noti_rx.recv().await {
                break;
            }
        }
    });
    let _ = th.join();
    assert_eq!(counter.as_ref().load(Ordering::Acquire), round * (tx_count));
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 8, 8)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 100, 100)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(10), 10, 1000)]
#[case(mpmc::bounded_tx_async_rx_blocking::<usize>(100), 500, 500)]
fn test_pressure_multi_tx_async_multi_rx_blocking(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, MRx<usize>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round: usize = 100000;
    let mut rx_th_s = Vec::new();
    for _rx_i in 0..rx_count {
        let _rx = rx.clone();
        let _round = round;
        let _counter = counter.clone();
        rx_th_s.push(thread::spawn(move || {
            'A: loop {
                match _rx.recv() {
                    Ok(i) => {
                        _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                        debug!("recv {} {}", _rx_i, i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx {} exit", _rx_i);
        }));
    }
    drop(rx);
    runtime_block_on!(async move {
        let (noti_tx, noti_rx) = mpmc::unbounded_async::<usize>();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            let mut _noti_tx = noti_tx.clone();
            async_spawn!(async move {
                for i in 0..round {
                    match _tx.send(i).await {
                        Err(e) => panic!("{}", e),
                        _ => {}
                    }
                }
                let _ = _noti_tx.send(_tx_i);
                debug!("tx {} exit", _tx_i);
            });
        }
        drop(tx);
        drop(noti_tx);
        for _ in 0..(tx_count) {
            if let Err(_) = noti_rx.recv().await {
                break;
            }
        }
    });
    for th in rx_th_s {
        let _ = th.join();
    }
    assert_eq!(counter.as_ref().load(Ordering::Acquire), round * (tx_count));
}
