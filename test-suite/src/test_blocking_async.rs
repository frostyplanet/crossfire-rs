use crate::*;
use captains_log::{logfn, *};
use crossfire::*;
use rstest::*;
use std::thread;
use std::time::*;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_blocking_rx_async(1))]
#[case(mpsc::bounded_tx_blocking_rx_async(1))]
#[case(mpmc::bounded_tx_blocking_rx_async(1))]
fn test_basic_bounded_empty_full_drop_rx<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    tx.try_send(1).expect("Ok");
    assert!(tx.is_full());
    assert!(rx.is_full());
    assert!(!tx.is_empty());
    assert_eq!(tx.is_disconnected(), false);
    assert_eq!(rx.is_disconnected(), false);
    drop(rx);
    assert_eq!(tx.is_disconnected(), true);
    assert_eq!(tx.as_ref().get_rx_count(), 0);
    assert_eq!(tx.as_ref().get_tx_count(), 1);
}

#[logfn]
#[rstest]
#[case(spsc::bounded_tx_blocking_rx_async(1))]
#[case(mpsc::bounded_tx_blocking_rx_async(1))]
#[case(mpmc::bounded_tx_blocking_rx_async(1))]
fn test_basic_bounded_empty_full_drop_tx<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    tx.try_send(1).expect("Ok");
    assert!(tx.is_full());
    assert!(rx.is_full());
    assert!(!tx.is_empty());
    assert_eq!(tx.is_disconnected(), false);
    assert_eq!(rx.is_disconnected(), false);
    drop(tx);
    assert_eq!(rx.is_disconnected(), true);
    assert_eq!(rx.as_ref().get_tx_count(), 0);
    assert_eq!(rx.as_ref().get_rx_count(), 1);
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async())]
#[case(mpsc::unbounded_async())]
#[case(mpmc::unbounded_async())]
fn test_basic_unbounded_empty_drop_tx<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    tx.try_send(1).expect("Ok");
    assert!(!tx.is_empty());
    assert_eq!(tx.is_disconnected(), false);
    assert_eq!(rx.is_disconnected(), false);
    drop(tx);
    assert_eq!(rx.is_disconnected(), true);
    assert_eq!(rx.as_ref().get_tx_count(), 0);
    assert_eq!(rx.as_ref().get_rx_count(), 1);
}

#[logfn]
#[rstest]
fn test_basic_compile_bounded_empty_full() {
    let (tx, rx) = mpmc::bounded_tx_blocking_rx_async::<usize>(1);
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    tx.try_send(1).expect("ok");
    assert!(tx.is_full());
    assert!(!tx.is_empty());
    assert!(rx.is_full());
    assert_eq!(tx.get_tx_count(), 1);
    assert_eq!(rx.get_tx_count(), 1);
    assert_eq!(tx.is_disconnected(), false);
    assert_eq!(rx.is_disconnected(), false);
    drop(rx);
    assert_eq!(tx.is_disconnected(), true);
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
                    trace!("recv {}", i);
                    assert_eq!(i, j);
                }
                Err(e) => {
                    panic!("error {}", e);
                }
            }
        }
        let res = rx.recv().await;
        assert!(res.is_err());
        trace!("rx close");
    });
    let _ = th.join().unwrap();
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
    let round: usize;
    #[cfg(miri)]
    {
        round = ROUND;
    }
    #[cfg(not(miri))]
    {
        round = ROUND * 100;
    }
    let th = thread::spawn(move || {
        for i in 0..round {
            tx.send(i).expect("send ok");
        }
    });
    runtime_block_on!(async move {
        for i in 0..round {
            match rx.recv().await {
                Ok(msg) => {
                    trace!("recv {}", msg);
                    assert_eq!(msg, i);
                }
                Err(_e) => {
                    panic!("channel closed");
                }
            }
        }
        assert!(rx.recv().await.is_err());
    });
    let _ = th.join().unwrap();
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(1), 5)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(1), 100)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(1), 200)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(100), 10)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(100), 100)]
#[case(mpsc::bounded_tx_blocking_rx_async::<usize>(100), 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 5)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 100)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 300)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 5)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 100)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 200)]
#[case(mpsc::unbounded_async::<usize>(), 5)]
#[case(mpsc::unbounded_async::<usize>(), 100)]
#[case(mpsc::unbounded_async::<usize>(), 300)]
#[case(mpmc::unbounded_async::<usize>(), 6)]
#[case(mpmc::unbounded_async::<usize>(), 100)]
#[case(mpmc::unbounded_async::<usize>(), 300)]
fn test_pressure_tx_multi_blocking_1_rx_async<R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (MTx<usize>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;
    #[cfg(miri)]
    {
        if tx_count > 5 {
            println!("skip");
            return;
        }
    }

    let mut tx_th_s = Vec::new();
    for _tx_i in 0..tx_count {
        let _tx = tx.clone();
        tx_th_s.push(thread::spawn(move || {
            debug!("tx {} spawn", _tx_i);
            for i in 0..ROUND {
                match _tx.send(i) {
                    Err(e) => panic!("{}", e),
                    _ => {
                        trace!("tx {} {}", _tx_i, i);
                    }
                }
            }
            debug!("tx {} exit", _tx_i);
        }));
    }
    drop(tx);
    let rx_count = runtime_block_on!(async move {
        let mut count = 0;
        'A: loop {
            match rx.recv().await {
                Ok(_i) => {
                    count += 1;
                    trace!("rx {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        count
    });
    for th in tx_th_s {
        let _ = th.join().unwrap();
    }
    assert_eq!(rx_count, ROUND * tx_count);
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 5, 5)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 100, 20)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(1), 300, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10), 10, 10)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10), 100, 20)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(10), 300, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 10, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 100, 200)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 300, 300)]
#[case(mpmc::bounded_tx_blocking_rx_async::<usize>(100), 30, 500)]
#[case(mpmc::unbounded_async::<usize>(), 5, 5)]
#[case(mpmc::unbounded_async::<usize>(), 100, 20)]
#[case(mpmc::unbounded_async::<usize>(), 300, 200)]
#[case(mpmc::unbounded_async::<usize>(), 10, 200)]
#[case(mpmc::unbounded_async::<usize>(), 100, 200)]
#[case(mpmc::unbounded_async::<usize>(), 300, 300)]
#[case(mpmc::unbounded_async::<usize>(), 30, 500)]
fn test_pressure_tx_multi_blocking_multi_rx_async(
    setup_log: (), #[case] channel: (MTx<usize>, MAsyncRx<usize>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    let (tx, rx) = channel;
    #[cfg(miri)]
    {
        if tx_count > 5 || rx_count > 5 {
            println!("skip");
            return;
        }
    }

    let mut tx_th_s = Vec::new();
    for _tx_i in 0..tx_count {
        let _tx = tx.clone();
        tx_th_s.push(thread::spawn(move || {
            for i in 0..ROUND {
                match _tx.send(i) {
                    Err(e) => panic!("{}", e),
                    _ => {
                        trace!("tx {} {}", _tx_i, i);
                    }
                }
            }
            debug!("tx {} exit", _tx_i);
        }));
    }
    drop(tx);
    let total_count = runtime_block_on!(async move {
        let mut th_co = Vec::new();
        for _rx_i in 0..rx_count {
            let _rx = rx.clone();
            th_co.push(async_spawn!(async move {
                debug!("rx{:?} {} spawn", tokio_task_id!(), _rx_i);
                let mut count = 0;
                'A: loop {
                    match _rx.recv().await {
                        Ok(_i) => {
                            count += 1;
                            trace!("rx {} {}", _rx_i, _i);
                        }
                        Err(_) => break 'A,
                    }
                }
                debug!("rx{:?} {} exit", tokio_task_id!(), _rx_i);
                count
            }));
        }
        drop(rx);
        let mut total = 0;
        for th in th_co {
            total += async_join_result!(th);
        }
        total
    });
    for th in tx_th_s {
        let _ = th.join().unwrap();
    }
    assert_eq!(total_count, tx_count * ROUND);
}
