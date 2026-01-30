use crate::*;
use captains_log::{logfn, *};
use crossfire::flavor::Flavor;
use crossfire::tokio_task_id;
use crossfire::{sink::*, stream::*, *};
use futures_util::{
    pin_mut, select,
    stream::{Stream, StreamExt},
    FutureExt,
};
use rstest::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::*;
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(1))]
#[case(mpsc::bounded_async(1))]
#[case(mpmc::bounded_async(1))]
fn test_basic_bounded_empty_full_drop_rx<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    assert_eq!(tx.capacity(), Some(1));
    assert_eq!(rx.capacity(), Some(1));
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
#[case(spsc::bounded_async(1))]
#[case(mpsc::bounded_async(1))]
#[case(mpmc::bounded_async(1))]
fn test_basic_bounded_empty_full_drop_tx<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert!(tx.is_empty());
    assert!(rx.is_empty());
    assert_eq!(tx.capacity(), Some(1));
    assert_eq!(rx.capacity(), Some(1));
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
    let (tx, rx) = mpmc::bounded_async(1);
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
fn test_sync() {
    runtime_block_on!(async move {
        let (tx, rx) = spsc::bounded_async::<usize>(100);
        //  Example1: should fail to compile with Arc
        //    let tx = Arc::new(tx);
        let _task = async_spawn!(async move {
            let _ = tx.send(2).await;
        });
        drop(rx);

        let (tx, rx) = mpsc::bounded_async::<usize>(100);
        //  example2: should fail to compile with Arc
        //    let rx = Arc::new(rx);
        let _task = async_spawn!(async move {
            let _ = rx.recv().await;
        });
        drop(tx);

        let (tx, rx) = mpsc::bounded_blocking::<usize>(100);
        ////  example3: should fail to compile with Arc
        //    let rx = Arc::new(rx);
        let _task = std::thread::spawn(move || {
            let _ = rx.recv();
        });
        drop(tx);

        let (tx, rx) = spsc::bounded_blocking::<usize>(100);
        ////  example4: should fail to compile after Arc
        //   let tx = Arc::new(tx);
        std::thread::spawn(move || {
            let _ = tx.send(1);
        });
        drop(rx);

        let (tx, rx) = mpmc::bounded_blocking::<usize>(100);
        // MRx can put in Arc
        let rx = Arc::new(rx);
        std::thread::spawn(move || {
            let _ = rx.try_recv();
        });
        // MTx can put in Arc
        let tx = Arc::new(tx);
        std::thread::spawn(move || {
            let _ = tx.try_send(1);
        });

        let (tx, rx) = spsc::bounded_async::<usize>(100);
        let th = async_spawn!(async move {
            let mut i = 0;
            loop {
                sleep(Duration::from_secs(1)).await;
                i += 1;
                if let Err(_) = tx.send(i).await {
                    println!("rx dropped");
                    return;
                }
            }
        });
        'LOOP: for _ in 0..10 {
            select! {
                _ = sleep(Duration::from_millis(500)).fuse() =>{
                    println!("tick");
                },
                r = rx.recv().fuse() => {
                    match r {
                        Ok(item)=>{
                            println!("recv {}", item);
                        }
                        Err(e)=>{
                            println!("tx dropped {:?}", e);
                            break 'LOOP;
                        }
                    }
                }
            }
        }
        drop(rx);
        let _ = async_join_result!(th);
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(100))]
#[case(mpsc::bounded_async(100))]
#[case(mpmc::bounded_async(100))]
fn test_basic_bounded_rx_drop<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    runtime_block_on!(async move {
        let tx = {
            let (tx, _rx) = channel;
            tx.send(1).await.expect("ok");
            tx.send(2).await.expect("ok");
            tx.send(3).await.expect("ok");
            tx
        };
        {
            info!("try to send after rx dropped");
            assert_eq!(tx.send(4).await.unwrap_err(), SendError(4));
            drop(tx);
            info!("dropped tx");
        }
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async())]
#[case(mpsc::unbounded_async())]
#[case(mpmc::unbounded_async())]
fn test_basic_unbounded_rx_drop<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    runtime_block_on!(async move {
        let tx = {
            let (tx, _rx) = channel;
            tx.send(1).expect("ok");
            tx.send(2).expect("ok");
            tx.send(3).expect("ok");
            tx
        };
        {
            info!("try to send after rx dropped");
            assert_eq!(tx.send(4).unwrap_err(), SendError(4));
            drop(tx);
            info!("dropped tx");
        }
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(10))]
#[case(mpsc::bounded_async(10))]
#[case(mpmc::bounded_async(10))]
fn test_basic_bounded_1_thread<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        let rx_res = rx.try_recv();
        assert!(rx_res.is_err());
        assert!(rx_res.unwrap_err().is_empty());
        for i in 0usize..10 {
            let tx_res = tx.try_send(i);
            assert!(tx_res.is_ok());
        }
        let tx_res = tx.try_send(11);
        assert!(tx_res.is_err());
        assert!(tx_res.unwrap_err().is_full());

        let th = async_spawn!(async move {
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
            debug!("rx close");
        });
        assert!(tx.send(10).await.is_ok());
        sleep(Duration::from_secs(1)).await;
        assert!(tx.send(11).await.is_ok());
        drop(tx);
        let _ = async_join_result!(th);
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async())]
#[case(mpsc::unbounded_async())]
#[case(mpmc::unbounded_async())]
fn test_basic_unbounded_1_thread<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    assert_eq!(tx.capacity(), None);
    assert_eq!(rx.capacity(), None);
    runtime_block_on!(async move {
        let rx_res = rx.try_recv();
        assert!(rx_res.is_err());
        assert!(rx_res.unwrap_err().is_empty());
        for i in 0usize..10 {
            let tx_res = tx.try_send(i);
            assert!(tx_res.is_ok());
        }

        let th = async_spawn!(async move {
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
            debug!("rx close");
        });
        assert!(tx.send(10).is_ok());
        sleep(Duration::from_secs(1)).await;
        assert!(tx.send(11).is_ok());
        drop(tx);
        let _ = async_join_result!(th);
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async())]
#[case(mpsc::unbounded_async())]
#[case(mpmc::unbounded_async())]
fn test_basic_unbounded_idle_select<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (_tx, rx) = channel;
    let round = {
        #[cfg(miri)]
        {
            10
        }
        #[cfg(not(miri))]
        {
            200
        }
    };

    runtime_block_on!(async move {
        let c = rx.recv().fuse();
        pin_mut!(c);
        for _ in 0..round {
            {
                let f = sleep(Duration::from_millis(1)).fuse();
                pin_mut!(f);
                select! {
                    _ = f => {
                        let (_tx_wakers, _rx_wakers) = rx.get_wakers_count();
                        trace!("waker tx {} rx {}", _tx_wakers, _rx_wakers);
                    },
                    _ = c => {
                        unreachable!()
                    },
                }
            }
        }
        let (tx_wakers, rx_wakers) = rx.get_wakers_count();
        assert_eq!(tx_wakers, 0);
        info!("waker rx {}", rx_wakers);
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(10))]
#[case(mpsc::bounded_async(10))]
#[case(mpmc::bounded_async(10))]
fn test_basic_bounded_recv_after_sender_close<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let total_msg_count = 5;
    for i in 0..total_msg_count {
        let _ = tx.try_send(i).expect("send ok");
    }
    drop(tx);

    runtime_block_on!(async move {
        // NOTE: 5 < 10
        let mut recv_msg_count = 0;
        loop {
            match rx.recv().await {
                Ok(_) => {
                    recv_msg_count += 1;
                }
                Err(_) => {
                    break;
                }
            }
        }
        assert_eq!(recv_msg_count, total_msg_count);
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async())]
#[case(mpsc::unbounded_async())]
#[case(mpmc::unbounded_async())]
fn test_basic_unbounded_recv_after_sender_close<
    T: BlockingTxTrait<usize>,
    R: AsyncRxTrait<usize>,
>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    let total_msg_count = 500;
    for i in 0..total_msg_count {
        let _ = tx.send(i).expect("send ok");
    }
    drop(tx);
    runtime_block_on!(async move {
        let mut recv_msg_count = 0;
        loop {
            match rx.recv().await {
                Ok(_) => {
                    recv_msg_count += 1;
                }
                Err(_) => {
                    break;
                }
            }
        }
        assert_eq!(recv_msg_count, total_msg_count);
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(100))]
#[case(mpsc::bounded_async(100))]
#[case(mpmc::bounded_async(100))]
fn test_basic_timeout_recv_async_waker<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        for _ in 0..1000 {
            assert!(rx.recv_with_timer(sleep(Duration::from_millis(1))).await.is_err());
        }
        let (tx_wakers, rx_wakers) = rx.get_wakers_count();
        println!("wakers: {}, {}", tx_wakers, rx_wakers);
        assert!(tx_wakers <= 1);
        assert!(rx_wakers <= 1);
        sleep(Duration::from_secs(1)).await;
        let _ = tx.send(1).await;
        assert_eq!(rx.recv().await.unwrap(), 1);
        let (tx_wakers, rx_wakers) = rx.get_wakers_count();
        println!("wakers: {}, {}", tx_wakers, rx_wakers);
        assert!(tx_wakers <= 1);
        assert!(rx_wakers <= 1);
    });
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_async())]
#[case(mpsc::unbounded_async())]
#[case(mpmc::unbounded_async())]
fn test_basic_unbounded_recv_timeout_async<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    let (tx, rx) = _channel;
    runtime_block_on!(async move {
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            let _ = tx.send(1);
        });
        let _r = rx.recv_with_timer(sleep(Duration::from_millis(1))).await;
        #[cfg(not(miri))]
        {
            assert_eq!(_r.unwrap_err(), RecvTimeoutError::Timeout);
        }
        let _ = async_join_result!(th);
        let (tx_wakers, rx_wakers) = rx.get_wakers_count();
        println!("wakers: {}, {}", tx_wakers, rx_wakers);
        assert_eq!(tx_wakers, 0);
        assert_eq!(rx_wakers, 0);
        let _r = rx.recv_with_timer(sleep(Duration::from_millis(200))).await;
        #[cfg(not(miri))]
        {
            assert_eq!(_r.unwrap(), 1);
        }
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(10))]
#[case(mpsc::bounded_async(10))]
#[case(mpmc::bounded_async(10))]
fn test_basic_send_timeout_async<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    let (tx, rx) = _channel;
    for i in 0..10 {
        assert!(tx.try_send(i).is_ok());
    }

    runtime_block_on!(async move {
        assert_eq!(
            tx.send_with_timer(11, sleep(Duration::from_millis(1))).await.unwrap_err(),
            SendTimeoutError::Timeout(11)
        );
        let th = async_spawn!(async move {
            loop {
                sleep(Duration::from_millis(2)).await;
                if let Err(_) = rx.recv().await {
                    println!("tx dropped");
                    break;
                }
            }
        });
        let mut try_times = 0;
        loop {
            try_times += 1;
            match tx.send_with_timer(11, sleep(Duration::from_millis(1))).await {
                Ok(_) => {
                    println!("send ok after {} tries", try_times);
                    break;
                }
                Err(SendTimeoutError::Timeout(msg)) => {
                    println!("timeout");
                    assert_eq!(msg, 11);
                }
                Err(SendTimeoutError::Disconnected(_)) => {
                    unreachable!();
                }
            }
        }
        let (tx_wakers, rx_wakers) = tx.get_wakers_count();
        println!("wakers: {}, {}", tx_wakers, rx_wakers);
        assert!(tx_wakers <= 1, "{:?}", tx_wakers);
        assert!(rx_wakers <= 1, "{:?}", rx_wakers);
        drop(tx);
        let _ = async_join_result!(th);
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async(1))]
fn test_pressure_bounded_timeout_async<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] _channel: (MAsyncTx<F>, MAsyncRx<F>),
) {
    use std::collections::HashMap;
    let (tx, rx) = _channel;
    let tx_count: usize = 3;
    let rx_count: usize = 2;

    runtime_block_on!(async move {
        assert_eq!(
            rx.recv_with_timer(sleep(Duration::from_millis(1))).await.unwrap_err(),
            RecvTimeoutError::Timeout
        );
        let (tx_wakers, rx_wakers) = rx.get_wakers_count();
        println!("wakers: {}, {}", tx_wakers, rx_wakers);
        assert_eq!(tx_wakers, 0);
        assert_eq!(rx_wakers, 0);

        let recv_map = Arc::new(Mutex::new(HashMap::new()));

        let mut th_tx = Vec::new();
        let mut th_rx = Vec::new();

        for thread_id in 0..tx_count {
            let _recv_map = recv_map.clone();
            let _tx = tx.clone();
            th_tx.push(async_spawn!(async move {
                let mut local_send_timeout_count = 0;
                let mut i = 0;
                // randomize start up
                sleep(Duration::from_millis((thread_id & 3) as u64)).await;
                loop {
                    if i >= ROUND {
                        return local_send_timeout_count;
                    }
                    {
                        let mut guard = _recv_map.lock().unwrap();
                        guard.insert(i, ());
                    }
                    if i & 2 == 0 {
                        sleep(Duration::from_millis(3)).await;
                    } else {
                        sleep(Duration::from_millis(1)).await;
                    }
                    loop {
                        match _tx.send_with_timer(i, sleep(Duration::from_millis(1))).await {
                            Ok(_) => {
                                i += 1;
                                break;
                            }
                            Err(SendTimeoutError::Timeout(_i)) => {
                                local_send_timeout_count += 1;
                                assert_eq!(_i, i);
                            }
                            Err(SendTimeoutError::Disconnected(_)) => {
                                unreachable!();
                            }
                        }
                    }
                }
            }));
        }

        for _thread_id in 0..rx_count {
            let _rx = rx.clone();
            let _recv_map = recv_map.clone();
            th_rx.push(async_spawn!(async move {
                let mut step: usize = 0;
                let mut local_recv_count: usize = 0;
                let mut local_recv_timeout_count: usize = 0;
                loop {
                    step += 1;
                    let timeout = if step & 2 == 0 { 1 } else { 2 };
                    if step & 2 > 0 {
                        sleep(Duration::from_millis(1)).await;
                    }
                    match _rx.recv_with_timer(sleep(Duration::from_millis(timeout))).await {
                        Ok(item) => {
                            local_recv_count += 1;
                            {
                                let mut guard = _recv_map.lock().unwrap();
                                guard.remove(&item);
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            local_recv_timeout_count += 1;
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            return (local_recv_count, local_recv_timeout_count);
                        }
                    }
                }
            }));
        }
        drop(tx);
        drop(rx);

        let mut total_send_timeout_count = 0;
        for th in th_tx {
            total_send_timeout_count += async_join_result!(th);
        }
        let mut total_recv_count = 0;
        let mut total_recv_timeout_count = 0;
        for th in th_rx {
            let (recv_count, recv_timeout_count) = async_join_result!(th);
            total_recv_count += recv_count;
            total_recv_timeout_count += recv_timeout_count;
        }
        {
            let guard = recv_map.lock().unwrap();
            assert!(guard.is_empty());
        }
        assert_eq!(ROUND * tx_count, total_recv_count);
        println!("send timeout count: {}", total_send_timeout_count);
        println!("recv timeout count: {}", total_recv_timeout_count);
    });
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(1))]
#[case(spsc::bounded_async(10))]
#[case(spsc::bounded_async(100))]
#[case(mpmc::bounded_async(1))]
#[case(mpmc::bounded_async(10))]
#[case(mpmc::bounded_async(100))]
#[case(mpmc::bounded_async(300))]
fn test_pressure_bounded_async_1_1<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let (tx, rx) = channel;

    runtime_block_on!(async move {
        let mut counter: usize = 0;
        let th = async_spawn!(async move {
            for i in 0..ROUND {
                if let Err(e) = tx.send(i).await {
                    panic!("{:?}", e);
                }
            }
            debug!("tx{:?} exit", tokio_task_id!());
        });
        'A: loop {
            match rx.recv().await {
                Ok(_i) => {
                    assert_eq!(_i, counter);
                    trace!("recv {}", _i);
                    counter += 1;
                }
                Err(_) => break 'A,
            }
        }
        drop(rx);
        let _ = async_join_result!(th);
        assert_eq!(counter, ROUND);
    });
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_async(1), 5)]
#[case(mpsc::bounded_async(1), 100)]
#[case(mpsc::bounded_async(1), 300)]
#[case(mpsc::bounded_async(10), 5)]
#[case(mpsc::bounded_async(10), 100)]
#[case(mpsc::bounded_async(10), 300)]
#[case(mpsc::bounded_async(100), 10)]
#[case(mpsc::bounded_async(100), 100)]
#[case(mpsc::bounded_async(100), 300)]
#[case(mpmc::bounded_async(1), 5)]
#[case(mpmc::bounded_async(1), 100)]
#[case(mpmc::bounded_async(1), 300)]
#[case(mpmc::bounded_async(10), 5)]
#[case(mpmc::bounded_async(10), 100)]
#[case(mpmc::bounded_async(10), 300)]
#[case(mpmc::bounded_async(100), 5)]
#[case(mpmc::bounded_async(100), 100)]
#[case(mpmc::bounded_async(100), 300)]
fn test_pressure_bounded_async_multi_1<
    F: Flavor<Item = usize> + 'static,
    R: AsyncRxTrait<usize>,
>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, R), #[case] tx_count: usize,
) {
    let (tx, rx) = channel;
    #[cfg(miri)]
    {
        if tx_count > 10 {
            println!("skip");
            return;
        }
    }
    runtime_block_on!(async move {
        let mut counter = 0;
        let mut th_s = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            th_s.push(async_spawn!(async move {
                debug!("tx{:?} {} spawn", tokio_task_id!(), _tx_i);
                for i in 0..ROUND {
                    match _tx.send(i).await {
                        Err(e) => panic!("{:?}", e),
                        _ => {}
                    }
                }
                debug!("tx{:?} {} exit", tokio_task_id!(), _tx_i);
            }));
        }
        drop(tx);
        'A: loop {
            match rx.recv().await {
                Ok(_i) => {
                    counter += 1;
                    trace!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        drop(rx);
        for th in th_s {
            let _ = async_join_result!(th);
        }
        assert_eq!(counter, ROUND * tx_count);
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async(1), 5, 5)]
#[case(mpmc::bounded_async(1), 100, 10)]
#[case(mpmc::bounded_async(1), 10, 100)]
#[case(mpmc::bounded_async(1), 300, 300)]
#[case(mpmc::bounded_async(10), 5, 5)]
#[case(mpmc::bounded_async(10), 100, 10)]
#[case(mpmc::bounded_async(10), 10, 100)]
#[case(mpmc::bounded_async(10), 300, 300)]
#[case(mpmc::bounded_async(100), 5, 5)]
#[case(mpmc::bounded_async(100), 100, 10)]
#[case(mpmc::bounded_async(100), 10, 100)]
#[case(mpmc::bounded_async(100), 300, 300)]
fn test_pressure_bounded_async_multi<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, MAsyncRx<F>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    #[cfg(miri)]
    {
        if rx_count > 5 || tx_count > 5 {
            println!("skip");
            return;
        }
    }
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        let mut th_tx = Vec::new();
        let mut th_rx = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            th_tx.push(async_spawn!(async move {
                debug!("tx{:?} {} spawn", tokio_task_id!(), _tx_i);
                for i in 0..ROUND {
                    match _tx.send(i).await {
                        Err(e) => panic!("{:?}", e),
                        _ => {}
                    }
                }
                debug!("tx{:?} {} exit", tokio_task_id!(), _tx_i);
            }));
        }
        for _rx_i in 0..rx_count {
            let _rx = rx.clone();
            th_rx.push(async_spawn!(async move {
                debug!("rx{:?} {} spawn", tokio_task_id!(), _rx_i);
                let mut count = 0;
                'A: loop {
                    match _rx.recv().await {
                        Ok(_i) => {
                            count += 1;
                            trace!("recv {} {}", _rx_i, _i);
                        }
                        Err(_) => break 'A,
                    }
                }
                debug!("rx{:?} {} exit", tokio_task_id!(), _rx_i);
                count
            }));
        }
        drop(tx);
        drop(rx);
        for th in th_tx {
            let _ = async_join_result!(th);
        }
        let mut recv_count = 0;
        for th in th_rx {
            recv_count += async_join_result!(th);
        }
        assert_eq!(recv_count, ROUND * tx_count);
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async(1))]
#[case(mpmc::bounded_async(10))]
#[case(mpmc::bounded_async(100))]
fn test_pressure_bounded_mixed_async_blocking_conversion<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, MAsyncRx<F>),
) {
    let (tx, rx) = channel;
    runtime_block_on!(async move {
        let mut recv_counter = 0;
        let mut th_tx = Vec::new();
        let mut th_rx = Vec::new();
        let mut co_tx = Vec::new();
        let mut co_rx = Vec::new();
        let _tx: MTx<F> = tx.clone().into();
        th_tx.push(thread::spawn(move || {
            for i in 0..ROUND {
                match _tx.send(i) {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            debug!("tx blocking exit");
        }));
        co_tx.push(async_spawn!(async move {
            for i in 0..ROUND {
                match tx.send(i).await {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            debug!("tx{:?} async exit", tokio_task_id!());
        }));
        let _rx: MRx<F> = rx.clone().into();
        th_rx.push(thread::spawn(move || {
            let mut count: usize = 0;
            'A: loop {
                match _rx.recv() {
                    Ok(_i) => {
                        count += 1;
                        trace!("recv blocking {}", _i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx blocking exit");
            count
        }));

        co_rx.push(async_spawn!(async move {
            let mut count: usize = 0;
            'A: loop {
                match rx.recv().await {
                    Ok(_i) => {
                        count += 1;
                        trace!("recv async {}", _i);
                    }
                    Err(_) => break 'A,
                }
            }
            debug!("rx{:?} async exit", tokio_task_id!());
            count
        }));
        for th in co_tx {
            let _ = async_join_result!(th);
        }
        for th in co_rx {
            recv_counter += async_join_result!(th);
        }
        for th in th_tx {
            let _ = th.join().unwrap();
        }
        for th in th_rx {
            recv_counter += th.join().unwrap();
        }
        assert_eq!(recv_counter, ROUND * 2);
    });
}

#[test]
fn test_conversion() {
    let (mtx, mrx) = mpmc::bounded_async::<usize>(1);
    let _tx: AsyncTx<_> = mtx.into();
    let _rx: AsyncRx<_> = mrx.into();
    let (_mtx, rx) = mpsc::bounded_async::<usize>(1);
    let _stream: AsyncStream<_> = rx.into(); // AsyncRx -> AsyncStream
    let (_mtx, mrx) = mpmc::bounded_async::<usize>(1);
    let _stream: AsyncStream<_> = mrx.into(); // AsyncRx -> AsyncStream
}

#[allow(dead_code)]
struct SpuriousTx<F: Flavor> {
    sink: AsyncSink<F>,
    normal: bool,
    step: usize,
}

impl<F: Flavor<Item = usize> + Unpin> Future for SpuriousTx<F> {
    type Output = Result<usize, usize>;

    fn poll(self: Pin<&mut Self>, ctx: &mut std::task::Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        if !_self.normal && _self.step > 0 {
            return Poll::Ready(Err(_self.step));
        }
        match _self.sink.poll_send(ctx, _self.step) {
            Ok(_) => {
                let res = _self.step;
                _self.step += 1;
                return Poll::Ready(Ok(res));
            }
            Err(TrySendError::Disconnected(_)) => {
                return Poll::Ready(Err(_self.step));
            }
            Err(TrySendError::Full(_)) => {
                _self.step += 1;
                return Poll::Pending;
            }
        }
    }
}

#[allow(dead_code)]
struct SpuriousRx<F: Flavor> {
    stream: AsyncStream<F>,
    normal: bool,
    step: usize,
}

impl<F: Flavor<Item = usize> + Unpin> Future for SpuriousRx<F> {
    type Output = Result<usize, usize>;

    fn poll(self: Pin<&mut Self>, ctx: &mut std::task::Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        if !_self.normal && _self.step > 0 {
            return Poll::Ready(Err(_self.step));
        }
        match _self.stream.poll_item(ctx) {
            Poll::Ready(Some(item)) => {
                _self.step += 1;
                return Poll::Ready(Ok(item));
            }
            Poll::Ready(None) => {
                return Poll::Ready(Err(_self.step));
            }
            Poll::Pending => {
                _self.step += 1;
                return Poll::Pending;
            }
        }
    }
}

#[logfn]
#[rstest]
fn test_spurious_sink(setup_log: ()) {
    #[cfg(feature = "tokio")]
    {
        let (tx, rx) = mpmc::bounded_async(1);

        async fn spawn_tx<F: Flavor<Item = usize> + Unpin + 'static>(
            tx: MAsyncTx<F>, normal: bool,
        ) {
            let sink = tx.into_sink();
            let _tx = SpuriousTx { sink, normal, step: 0 };
            if normal {
                assert_eq!(_tx.await.expect("send ok"), 1);
            } else {
                match tokio::time::timeout(Duration::from_secs(5), _tx).await {
                    Ok(Err(step)) => {
                        assert_eq!(step, 1);
                    }
                    Ok(Ok(step)) => {
                        panic!("unexpected ok in step={}", step);
                    }
                    Err(_) => {
                        panic!("tokio timeout");
                    }
                }
            }
        }
        runtime_block_on!(async move {
            tx.send(0).await.expect("send");
            let _tx = tx.clone();
            let mut th_s = Vec::new();
            println!("spawn spurious");
            // Make sure its the first
            th_s.push(tokio::spawn(async move { spawn_tx(_tx, false).await }));
            sleep(Duration::from_secs(1)).await;
            let _tx = tx.clone();
            println!("spawn normal");
            th_s.push(tokio::spawn(async move { spawn_tx(_tx, true).await }));
            sleep(Duration::from_secs(1)).await;
            println!("recv 1 to wake the 2 senders in Init state");
            assert_eq!(rx.recv().await.expect("recv"), 0);
            for th in th_s {
                let _ = async_join_result!(th);
            }
        });
    }
}

#[logfn]
#[rstest]
fn test_spurious_stream(setup_log: ()) {
    #[cfg(feature = "tokio")]
    {
        let (tx, rx) = mpmc::bounded_async(1);

        async fn spawn_rx<F: Flavor<Item = usize> + Unpin + 'static>(
            rx: MAsyncRx<F>, normal: bool,
        ) {
            let stream = rx.into_stream();
            let _rx = SpuriousRx { stream, normal, step: 0 };
            if normal {
                assert_eq!(_rx.await.expect("recv ok"), 1);
            } else {
                if let Ok(Err(step)) = tokio::time::timeout(Duration::from_secs(10), _rx).await {
                    assert_eq!(step, 1);
                } else {
                    unreachable!();
                }
            }
        }
        runtime_block_on!(async move {
            let _rx = rx.clone();
            let mut th_s = Vec::new();
            println!("spawn spurious");
            // Make sure its the first
            th_s.push(tokio::spawn(async move { spawn_rx(_rx, false).await }));
            sleep(Duration::from_millis(500)).await;
            let _rx = rx.clone();
            println!("spawn normal");
            th_s.push(tokio::spawn(async move { spawn_rx(_rx, true).await }));
            sleep(Duration::from_secs(1)).await;
            println!("send");
            tx.send(1).await.expect("send");
            sleep(Duration::from_secs(2)).await;
            for th in th_s {
                let _ = async_join_result!(th);
            }
        });
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_async(1))]
#[case(spsc::bounded_async(2))]
#[case(mpsc::bounded_async(1))]
#[case(mpsc::bounded_async(2))]
#[case(mpmc::bounded_async(1))]
#[case(mpmc::bounded_async(2))]
fn test_basic_into_stream_1_1<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    runtime_block_on!(async move {
        let total_message = 100;
        let (tx, rx) = channel;
        let th = async_spawn!(async move {
            println!("sender thread send {} message start", total_message);
            for i in 0usize..total_message {
                let _ = tx.send(i).await;
                // println!("send {}", i);
            }
            println!("sender thread send {} message end", total_message);
        });
        let mut s: Pin<Box<dyn Stream<Item = usize>>> = rx.to_stream();

        for _i in 0..total_message {
            assert_eq!(s.next().await, Some(_i));
        }
        assert_eq!(s.next().await, None);
        //assert!(s.is_terminated());
        async_join_result!(th);
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async(1), 2)]
#[case(mpmc::bounded_async(2), 4)]
#[case(mpmc::bounded_async(2), 10)]
#[case(mpmc::bounded_async(10), 3)]
#[case(mpmc::bounded_async(10), 30)]
#[case(mpmc::bounded_async(100), 2)]
#[case(mpmc::bounded_async(100), 4)]
#[case(mpmc::bounded_async(100), 50)]
fn test_pressure_stream_multi<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, MAsyncRx<F>), #[case] rx_count: usize,
) {
    #[cfg(miri)]
    {
        if rx_count > 5 {
            println!("skip");
            return;
        }
    }
    runtime_block_on!(async move {
        let (tx, rx) = channel;
        let mut th_s = Vec::new();
        let mut recv_counter = 0;
        for rx_i in 0..rx_count {
            let _rx = rx.clone();
            th_s.push(async_spawn!(async move {
                let mut counter = 0;
                let mut stream = _rx.into_stream();
                while let Some(_item) = stream.next().await {
                    counter += 1;
                }
                debug!("rx{:?} {} exit", tokio_task_id!(), rx_i);
                counter
            }));
        }
        drop(rx);
        for i in 0..ROUND {
            tx.send(i).await.expect("send");
        }
        drop(tx);
        for th in th_s {
            recv_counter += async_join_result!(th);
        }
        assert_eq!(recv_counter, ROUND);
    });
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_async(1), 2)]
#[case(mpmc::bounded_async(2), 4)]
#[case(mpmc::bounded_async(2), 10)]
#[case(mpmc::bounded_async(10), 3)]
#[case(mpmc::bounded_async(10), 30)]
#[case(mpmc::bounded_async(100), 2)]
#[case(mpmc::bounded_async(100), 4)]
#[case(mpmc::bounded_async(100), 50)]
fn test_pressure_stream_multi_idle<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] channel: (MAsyncTx<F>, MAsyncRx<F>), #[case] rx_count: usize,
) {
    #[cfg(miri)]
    {
        if rx_count > 5 {
            println!("skip");
            return;
        }
    }
    runtime_block_on!(async move {
        let total_message = ROUND / rx_count;
        let (tx, rx) = channel;
        let mut th_s = Vec::new();
        for rx_i in 0..rx_count {
            let _rx = rx.clone();
            th_s.push(async_spawn!(async move {
                debug!("rx{:?} {} spawn", tokio_task_id!(), rx_i);
                let mut count = 0;
                let mut stream = _rx.into_stream();
                while let Some(_item) = stream.next().await {
                    count += 1;
                }
                debug!("rx{:?} {} exit", tokio_task_id!(), rx_i);
                count
            }));
        }
        drop(rx);
        for i in 0..total_message {
            tx.send(i).await.expect("send");
            sleep(Duration::from_millis(3)).await;
        }
        drop(tx);

        let mut recv_counter = 0;
        for th in th_s {
            recv_counter += async_join_result!(th);
        }
        assert_eq!(recv_counter, total_message);
    });
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_async(1))]
#[case(spsc::bounded_async(10))]
#[case(mpsc::bounded_async(1))]
#[case(mpsc::bounded_async(10))]
#[case(mpmc::bounded_async(1))]
#[case(mpmc::bounded_async(10))]
fn test_async_drop_small_msg<T: AsyncTxTrait<SmallMsg>, R: AsyncRxTrait<SmallMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<SmallMsg>());
    _test_async_drop_msg(channel);
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_async(1))]
#[case(spsc::bounded_async(10))]
#[case(mpsc::bounded_async(1))]
#[case(mpsc::bounded_async(10))]
#[case(mpmc::bounded_async(1))]
#[case(mpmc::bounded_async(10))]
fn test_async_drop_large_msg<T: AsyncTxTrait<LargeMsg>, R: AsyncRxTrait<LargeMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<LargeMsg>());
    _test_async_drop_msg(channel);
}

fn _test_async_drop_msg<M: TestDropMsg, T: AsyncTxTrait<M>, R: AsyncRxTrait<M>>(channel: (T, R)) {
    let (tx, rx) = channel;
    reset_drop_counter();
    runtime_block_on!(async move {
        let cap = tx.capacity().unwrap();
        let mut ids = cap;
        for i in 0..ids {
            let msg = M::new(i);
            assert!(tx.try_send(msg).is_ok());
        }
        assert_eq!(get_drop_counter(), 0);
        let msg = M::new(ids);
        if let Err(TrySendError::Full(_msg)) = tx.try_send(msg) {
            assert_eq!(_msg.get_value(), ids);
            assert_eq!(get_drop_counter(), 0);
            drop(_msg);
            assert_eq!(get_drop_counter(), 1);
        } else {
            unreachable!();
        }
        let th = async_spawn!(async move {
            let _msg = rx.recv().await.expect("recv");
            assert_eq!(_msg.get_value(), 0);
            drop(_msg);
            rx
        });
        let msg = M::new(ids);
        tx.send(msg).await.expect("send");
        ids += 1;
        let rx = async_join_result!(th);
        drop(rx);
        assert_eq!(get_drop_counter(), 2);
        let msg = M::new(ids);
        if let Err(TrySendError::Disconnected(_msg)) = tx.try_send(msg) {
            assert_eq!(_msg.get_value(), ids);
        } else {
            unreachable!();
        }
        ids += 1;
        let msg = M::new(ids);
        if let Err(SendError(_msg)) = tx.send(msg).await {
            assert_eq!(_msg.get_value(), ids);
        } else {
            unreachable!();
        }
        assert_eq!(get_drop_counter(), 4);
        ids += 1;
        drop(tx);
        // every thing dropped inside the channel
        assert_eq!(get_drop_counter(), ids + 1); // ids begins at 0
        assert_eq!(get_drop_counter(), 4 + cap);
    });
}
