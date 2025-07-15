use super::common::*;
use crate::*;
use captains_log::{logfn, *};
use rstest::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::*;

#[fixture]
fn setup_log() {
    let _ = recipe::env_logger("LOG_FILE", "LOG_LEVEL").build().expect("log setup");
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_blocking_rx_async::<usize>(10))]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(10))]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10))]
fn test_basic_1_tx_blocking_1_rx_async<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    for i in 0usize..10 {
        let tx_res = tx.send(i);
        assert!(tx_res.is_ok());
    }
    let tx_res = tx.try_send(11);
    assert!(tx_res.is_err());
    assert!(tx_res.unwrap_err().is_full());

    let th = thread::spawn(move || {
        assert!(tx.send(10).is_ok());
        std::thread::sleep(Duration::from_secs(1));
        assert!(tx.send(11).is_ok());
    });
    runtime_block_on!(async move {
        for i in 0usize..12 {
            match rx.recv().await {
                Ok(j) => {
                    debug!("recv {}", i);
                    assert_eq!(i, j);
                }
                Err(e) => {
                    panic!("error {}", e);
                }
            }
        }
        let res = rx.recv().await;
        assert!(res.is_err());
        debug!("rx close");
    });
    let _ = th.join();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_blocking_rx_async::<usize>(10))]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(10))]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10))]
fn test_timeout_1_tx_blocking_1_rx_async<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    for i in 0usize..10 {
        let tx_res = tx.send(i);
        assert!(tx_res.is_ok());
    }
    let tx_res = tx.send_timeout(11, Duration::from_millis(100));
    assert!(tx_res.is_err());
    assert!(tx_res.unwrap_err().is_timeout());

    let th = thread::spawn(move || {
        assert!(tx.send_timeout(10, Duration::from_millis(100)).is_err());
        assert!(tx.send_timeout(10, Duration::from_millis(200)).is_ok());
    });

    runtime_block_on!(async move {
        sleep(Duration::from_millis(200)).await;

        for i in 0usize..11 {
            match rx.recv().await {
                Ok(j) => {
                    debug!("recv {}", i);
                    assert_eq!(i, j);
                }
                Err(e) => {
                    panic!("error {}", e);
                }
            }
        }
        let res = rx.recv().await;
        assert!(res.is_err());
        debug!("rx close");
    });
    let _ = th.join();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_blocking_rx_async::<usize>(1))]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(1))]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1))]
#[case(spsc::bounded_tx_blocking_rx_async::<usize>(100))]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(100))]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100))]
#[case(spsc::unbounded_async::<usize>())]
#[case(mpsc::unbounded_async::<usize>())]
#[case(mpmc::unbounded_async::<usize>())]
fn test_pressure_1_tx_blocking_1_rx_async<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let round: usize = 100000;
    let th = thread::spawn(move || {
        for i in 0..round {
            tx.send(i).expect("send ok");
        }
    });
    runtime_block_on!(async move {
        for i in 0..round {
            match rx.recv().await {
                Ok(msg) => {
                    debug!("recv {}", msg);
                    assert_eq!(msg, i);
                }
                Err(_e) => {
                    panic!("channel closed");
                }
            }
        }
        assert!(rx.recv().await.is_err());
    });
    let _ = th.join();
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(1), 10)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(1), 100)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(1), 1000)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(100), 10)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(100), 100)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(100), 1000)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 10)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 100)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 1000)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 10)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 100)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 1000)]
#[case(mpsc::unbounded_async::<usize>(), 10)]
#[case(mpsc::unbounded_async::<usize>(), 100)]
#[case(mpsc::unbounded_async::<usize>(), 1000)]
#[case(mpmc::unbounded_async::<usize>(), 10)]
#[case(mpmc::unbounded_async::<usize>(), 100)]
#[case(mpmc::unbounded_async::<usize>(), 1000)]
fn test_pressure_tx_multi_blocking_1_rx_async<R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (MTx<usize>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;
    let counter = Arc::new(AtomicUsize::new(0));
    let round = 1000000;
    let mut tx_th_s = Vec::new();
    let send_msg = Arc::new(AtomicUsize::new(0));
    for _tx_i in 0..tx_count {
        let _tx = tx.clone();
        let _round = round;
        let _send_msg = send_msg.clone();
        tx_th_s.push(thread::spawn(move || {
            loop {
                let i = _send_msg.fetch_add(1, Ordering::SeqCst);
                if i >= round {
                    break;
                }
                match _tx.send(i) {
                    Err(e) => panic!("{}", e),
                    _ => {
                        debug!("tx {} {}", _tx_i, i);
                    }
                }
            }
            debug!("tx {} exit", _tx_i);
        }));
    }
    drop(tx);
    let _counter = counter.clone();
    runtime_block_on!(async move {
        'A: loop {
            match rx.recv().await {
                Ok(_i) => {
                    _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                    debug!("rx {}r", _i);
                }
                Err(_) => break 'A,
            }
        }
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round);
    });
    for th in tx_th_s {
        let _ = th.join();
    }
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 10, 10)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 100, 20)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 1000, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10), 10, 10)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10), 100, 20)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10), 1000, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 10, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 100, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 300, 500)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 30, 1000)]
#[case(mpmc::unbounded_async::<usize>(), 10, 10)]
#[case(mpmc::unbounded_async::<usize>(), 100, 20)]
#[case(mpmc::unbounded_async::<usize>(), 1000, 200)]
#[case(mpmc::unbounded_async::<usize>(), 10, 200)]
#[case(mpmc::unbounded_async::<usize>(), 100, 200)]
#[case(mpmc::unbounded_async::<usize>(), 300, 500)]
#[case(mpmc::unbounded_async::<usize>(), 30, 1000)]
fn test_pressure_tx_multi_blocking_multi_rx_async(
    setup_log: (), #[case] channel: (MTx<usize>, MAsyncRx<usize>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round = 1000000;
    let mut tx_th_s = Vec::new();
    let send_msg = Arc::new(AtomicUsize::new(0));
    for _tx_i in 0..tx_count {
        let _tx = tx.clone();
        let _round = round;
        let _send_msg = send_msg.clone();
        tx_th_s.push(thread::spawn(move || {
            loop {
                let i = _send_msg.fetch_add(1, Ordering::SeqCst);
                if i >= round {
                    break;
                }
                match _tx.send(i) {
                    Err(e) => panic!("{}", e),
                    _ => {
                        debug!("tx {} {}", _tx_i, i);
                    }
                }
            }
            debug!("tx {} exit", _tx_i);
        }));
    }
    drop(tx);
    runtime_block_on!(async move {
        let (noti_tx, noti_rx) = mpmc::unbounded_async();
        for _rx_i in 0..rx_count {
            let _rx = rx.clone();
            let mut _noti_tx = noti_tx.clone();
            let _counter = counter.clone();
            async_spawn!(async move {
                'A: loop {
                    match _rx.recv().await {
                        Ok(_i) => {
                            _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                            debug!("rx {} {}", _rx_i, _i);
                        }
                        Err(_) => break 'A,
                    }
                }
                debug!("rx {} exiting", _rx_i);
                let _ = _noti_tx.send(_rx_i);
                debug!("rx {} exit", _rx_i);
            });
        }
        drop(rx);
        drop(noti_tx);
        for _ in 0..(rx_count) {
            if let Err(_) = noti_rx.recv().await {
                break;
            }
        }
        assert_eq!(counter.as_ref().load(Ordering::Acquire), round);
    });
    for th in tx_th_s {
        let _ = th.join();
    }
}
