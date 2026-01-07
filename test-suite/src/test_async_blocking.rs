use crate::*;
use captains_log::{logfn, *};
use crossfire::flavor::Flavor;
use crossfire::tokio_task_id;
use crossfire::*;
use rstest::*;
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async_blocking(1))]
#[case(mpsc::bounded_async_blocking(1))]
#[case(mpmc::bounded_async_blocking(1))]
fn test_basic_bounded_empty_full_drop_rx<T: AsyncTxTrait<usize>, R: BlockingRxTrait<usize>>(
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
    assert_eq!(tx.get_rx_count(), 0);
    assert_eq!(tx.get_tx_count(), 1);
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async_blocking(1))]
#[case(mpsc::bounded_async_blocking(1))]
#[case(mpmc::bounded_async_blocking(1))]
fn test_basic_bounded_empty_full_drop_tx<T: AsyncTxTrait<usize>, R: BlockingRxTrait<usize>>(
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
    assert_eq!(rx.get_tx_count(), 0);
    assert_eq!(rx.get_rx_count(), 1);
}

#[logfn]
#[rstest]
fn test_basic_compile_bounded_empty_full() {
    let (tx, rx) = mpmc::bounded_async_blocking(1);
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
#[case(spsc::bounded_async_blocking(100))]
#[case(mpsc::bounded_async_blocking(100))]
#[case(mpmc::bounded_async_blocking(100))]
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
                    trace!("recv {}", i);
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
    let _ = th.join().unwrap();
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_async_blocking(10), 5)]
#[case(mpsc::bounded_async_blocking(10), 8)]
#[case(mpsc::bounded_async_blocking(10), 100)]
#[case(mpsc::bounded_async_blocking(10), 1000)]
#[case(mpmc::bounded_async_blocking(10), 5)]
#[case(mpmc::bounded_async_blocking(10), 8)]
#[case(mpmc::bounded_async_blocking(10), 100)]
#[case(mpmc::bounded_async_blocking(10), 1000)]
fn test_basic_multi_tx_async_1_rx_blocking<
    F: Flavor<Item = usize> + 'static,
    R: BlockingRxTrait<usize>,
>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;
    let batch_1: usize;
    let batch_2: usize;

    #[cfg(miri)]
    {
        if tx_count > 5 {
            println!("skip");
            return;
        }
        batch_1 = 10;
        batch_2 = 20;
    }
    #[cfg(not(miri))]
    {
        batch_1 = 100;
        batch_2 = 200;
    }
    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    let th = thread::spawn(move || {
        for _ in 0..((batch_1 + batch_2) * tx_count) {
            match rx.recv() {
                Ok(i) => {
                    trace!("recv {}", i);
                }
                Err(e) => {
                    panic!("error {}", e);
                }
            }
        }
        let res = rx.recv();
        assert!(res.is_err());
        // Wait for spawn exit
    });
    runtime_block_on!(async move {
        let mut th_s = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            th_s.push(async_spawn!(async move {
                for i in 0..batch_1 {
                    let tx_res = _tx.send(i).await;
                    assert!(tx_res.is_ok());
                }
                for i in batch_1..(batch_1 + batch_2) {
                    assert!(_tx.send(10 + i).await.is_ok());
                    sleep(Duration::from_millis(2)).await;
                }
            }));
        }
        drop(tx);

        for th in th_s {
            let _ = async_join_result!(th);
        }
    });
    let _ = th.join().unwrap();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async_blocking(1))]
#[case(spsc::bounded_async_blocking(10))]
#[case(spsc::bounded_async_blocking(100))]
#[case(spsc::bounded_async_blocking(1000))]
#[case(mpsc::bounded_async_blocking(1))]
#[case(mpsc::bounded_async_blocking(10))]
#[case(mpsc::bounded_async_blocking(100))]
#[case(mpsc::bounded_async_blocking(1000))]
#[case(mpmc::bounded_async_blocking(1))]
#[case(mpmc::bounded_async_blocking(10))]
#[case(mpmc::bounded_async_blocking(100))]
#[case(mpmc::bounded_async_blocking(1000))]
fn test_pressure_1_tx_async_1_rx_blocking<T: AsyncTxTrait<usize>, R: BlockingRxTrait<usize>>(
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
        let mut count = 0;
        'A: loop {
            match rx.recv() {
                Ok(i) => {
                    count += 1;
                    trace!("recv {}", i);
                }
                Err(_) => break 'A,
            }
        }
        debug!("rx exit");
        count
    });
    runtime_block_on!(async move {
        for i in 0..round {
            match tx.send(i).await {
                Err(e) => panic!("{}", e),
                _ => {}
            }
        }
        debug!("tx{:?} exit", tokio_task_id!());
    });
    let rx_count = th.join().unwrap();
    assert_eq!(rx_count, round);
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_async_blocking(10), 5)]
#[case(mpsc::bounded_async_blocking(10), 10)]
#[case(mpsc::bounded_async_blocking(10), 100)]
#[case(mpsc::bounded_async_blocking(100), 200)]
#[case(mpmc::bounded_async_blocking(10), 5)]
#[case(mpmc::bounded_async_blocking(10), 100)]
#[case(mpmc::bounded_async_blocking(10), 10)]
#[case(mpmc::bounded_async_blocking(100), 200)]
fn test_pressure_multi_tx_async_1_rx_blocking<
    F: Flavor<Item = usize> + 'static,
    R: BlockingRxTrait<usize>,
>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;
    #[cfg(miri)]
    {
        if tx_count > 5 {
            println!("skip");
            return;
        }
    }

    let round: usize = ROUND;
    let th = thread::spawn(move || {
        let mut count = 0;
        'A: loop {
            match rx.recv() {
                Ok(_i) => {
                    count += 1;
                    trace!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        debug!("rx exit");
        count
    });
    runtime_block_on!(async move {
        let mut th_co = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            th_co.push(async_spawn!(async move {
                debug!("tx{:?} {} spawn", tokio_task_id!(), _tx_i);
                for i in 0..round {
                    match _tx.send(i).await {
                        Err(e) => panic!("{}", e),
                        _ => {}
                    }
                }
                debug!("tx{:?} {} exit", tokio_task_id!(), _tx_i);
            }));
        }
        drop(tx);
        for th in th_co {
            let _ = async_join_result!(th);
        }
    });
    let rx_count = th.join().unwrap();
    assert_eq!(rx_count, round * tx_count);
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async_blocking(10), 5, 5)]
#[case(mpmc::bounded_async_blocking(10), 100, 100)]
#[case(mpmc::bounded_async_blocking(10), 10, 300)]
#[case(mpmc::bounded_async_blocking(100), 300, 300)]
fn test_pressure_multi_tx_async_multi_rx_blocking<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, MRx<F>), #[case] tx_count: usize,
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

    let round: usize = ROUND;
    let mut rx_th_s = Vec::new();
    for _rx_i in 0..rx_count {
        let _rx = rx.clone();
        rx_th_s.push(thread::spawn(move || {
            debug!("rx {} spawn", _rx_i);
            let mut count = 0;
            'A: loop {
                match _rx.recv() {
                    Ok(i) => {
                        count += 1;
                        trace!("recv {} {}", _rx_i, i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx {} exit", _rx_i);
            count
        }));
    }
    drop(rx);
    runtime_block_on!(async move {
        let mut th_co = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            th_co.push(async_spawn!(async move {
                debug!("tx{:?} {} spawn", tokio_task_id!(), _tx_i);
                for i in 0..round {
                    match _tx.send(i).await {
                        Err(e) => panic!("{}", e),
                        _ => {}
                    }
                }
                debug!("tx{:?} {} exit", tokio_task_id!(), _tx_i);
            }));
        }
        drop(tx);
        for th in th_co {
            let _ = async_join_result!(th);
        }
    });
    let mut total_count = 0;
    for th in rx_th_s {
        total_count += th.join().unwrap();
    }
    assert_eq!(total_count, round * tx_count);
}
