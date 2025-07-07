use super::common::*;
use crate::*;
use futures::{select, FutureExt};
use log::*;
use rstest::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::time::*;

#[tokio::test]
async fn test_sync() {
    let (tx, rx) = spsc::bounded_async::<usize>(100);
    //  Example1: should fail to compile with Arc
    //    let tx = Arc::new(tx);
    tokio::spawn(async move {
        let _ = tx.send(2).await;
    });
    drop(rx);

    let (tx, rx) = mpsc::bounded_async::<usize>(100);
    //  example2: should fail to compile with Arc
    //    let rx = Arc::new(rx);
    tokio::spawn(async move {
        let _ = rx.recv().await;
    });
    drop(tx);

    let (tx, rx) = mpsc::bounded_blocking::<usize>(100);
    ////  example3: should fail to compile with Arc
    //    let rx = Arc::new(rx);
    std::thread::spawn(move || {
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
    let th = tokio::spawn(async move {
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
    let mut inv = tokio::time::interval(Duration::from_millis(500));
    'LOOP: for _ in 0..10 {
        select! {
            _ = inv.tick().fuse() =>{
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
    let _ = th.await;
}

#[rstest]
#[case(spsc::bounded_async::<usize>(100))]
#[case(mpsc::bounded_async::<usize>(100))]
#[case(mpmc::bounded_async::<usize>(100))]
#[tokio::test]
async fn test_basic_bounded_rx_drop<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
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
}

#[rstest]
#[case(spsc::unbounded_async::<usize>())]
#[case(mpsc::unbounded_async::<usize>())]
#[case(mpmc::unbounded_async::<usize>())]
#[tokio::test]
async fn test_basic_unbounded_rx_drop<T: BlockingTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
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
}

#[rstest]
#[case(spsc::bounded_async::<i32>(10))]
#[case(mpsc::bounded_async::<i32>(10))]
#[case(mpmc::bounded_async::<i32>(10))]
#[tokio::test]
async fn test_basic_bounded_1_thread<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
    let (tx, rx) = channel;
    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    for i in 0i32..10 {
        let tx_res = tx.try_send(i);
        assert!(tx_res.is_ok());
    }
    let tx_res = tx.try_send(11);
    assert!(tx_res.is_err());
    assert!(tx_res.unwrap_err().is_full());

    let (noti_tx, noti_rx) = tokio::sync::oneshot::channel::<bool>();
    tokio::spawn(async move {
        for i in 0i32..12 {
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
        let _ = noti_tx.send(true);
    });
    assert!(tx.send(10).await.is_ok());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(tx.send(11).await.is_ok());
    drop(tx);
    let _ = noti_rx.await;
}

#[rstest]
#[case(spsc::unbounded_async::<i32>())]
#[case(mpsc::unbounded_async::<i32>())]
#[case(mpmc::unbounded_async::<i32>())]
#[tokio::test]
async fn test_basic_unbounded_1_thread<T: BlockingTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log;
    let (tx, rx) = channel;
    let rx_res = rx.try_recv();
    assert!(rx_res.is_err());
    assert!(rx_res.unwrap_err().is_empty());
    for i in 0i32..10 {
        let tx_res = tx.try_send(i);
        assert!(tx_res.is_ok());
    }

    let (noti_tx, noti_rx) = tokio::sync::oneshot::channel::<bool>();
    tokio::spawn(async move {
        for i in 0i32..12 {
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
        let _ = noti_tx.send(true);
    });
    assert!(tx.send(10).is_ok());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(tx.send(11).is_ok());
    drop(tx);
    let _ = noti_rx.await;
}

#[rstest]
#[case(spsc::unbounded_async::<i32>())]
#[case(mpsc::unbounded_async::<i32>())]
#[case(mpmc::unbounded_async::<i32>())]
#[tokio::test]
async fn test_basic_unbounded_idle_select<T: BlockingTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
    let (_tx, rx) = channel;

    use futures::{pin_mut, select, FutureExt};

    async fn loop_fn() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let mut c = rx.recv().fuse();
    for _ in 0..1000 {
        {
            let f = loop_fn().fuse();
            pin_mut!(f);
            select! {
                _ = f => {
                    let (_tx_wakers, _rx_wakers) = rx.get_waker_size();
                    debug!("waker tx {} rx {}", _tx_wakers, _rx_wakers);
                },
                _ = c => {
                    unreachable!()
                },
            }
        }
    }
    let (tx_wakers, rx_wakers) = rx.get_waker_size();
    assert_eq!(tx_wakers, 0);
    info!("waker rx {}", rx_wakers);
}

#[rstest]
#[case(spsc::bounded_async::<i32>(10))]
#[case(mpsc::bounded_async::<i32>(10))]
#[case(mpmc::bounded_async::<i32>(10))]
#[tokio::test]
async fn test_basic_bounded_recv_after_sender_close<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
    let (tx, rx) = channel;
    // NOTE: 5 < 10
    let total_msg_count = 5;
    for i in 0..total_msg_count {
        let _ = tx.try_send(i).expect("send ok");
    }
    drop(tx);
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
}

#[rstest]
#[case(spsc::unbounded_async::<i32>())]
#[case(mpsc::unbounded_async::<i32>())]
#[case(mpmc::unbounded_async::<i32>())]
#[tokio::test]
async fn test_basic_unbounded_recv_after_sender_close<
    T: BlockingTxTrait<i32>,
    R: AsyncRxTrait<i32>,
>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
    let (tx, rx) = channel;
    let total_msg_count = 500;
    for i in 0..total_msg_count {
        let _ = tx.send(i).expect("send ok");
    }
    drop(tx);
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
}

#[rstest]
#[case(spsc::bounded_async::<i32>(100))]
#[case(mpsc::bounded_async::<i32>(100))]
#[case(mpmc::bounded_async::<i32>(100))]
#[tokio::test]
async fn test_timeout_recv_async<T: AsyncTxTrait<i32>, R: AsyncRxTrait<i32>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
    let (tx, rx) = channel;
    for _ in 0..1000 {
        assert!(timeout(Duration::from_millis(1), rx.recv()).await.is_err());
    }
    let (tx_wakers, rx_wakers) = rx.get_waker_size();
    println!("wakers: {}, {}", tx_wakers, rx_wakers);
    assert!(tx_wakers <= 1);
    assert!(rx_wakers <= 1);
    sleep(Duration::from_secs(1)).await;
    let _ = tx.send(1).await;
    assert_eq!(rx.recv().await.unwrap(), 1);
    let (tx_wakers, rx_wakers) = rx.get_waker_size();
    println!("wakers: {}, {}", tx_wakers, rx_wakers);
    assert!(tx_wakers <= 1);
    assert!(rx_wakers <= 1);
}

#[rstest]
#[case(spsc::bounded_async::<usize>(1))]
#[case(spsc::bounded_async::<usize>(10))]
#[case(spsc::bounded_async::<usize>(100))]
#[case(spsc::bounded_async::<usize>(300))]
#[case(mpsc::bounded_async::<usize>(1))]
#[case(mpsc::bounded_async::<usize>(10))]
#[case(mpsc::bounded_async::<usize>(100))]
#[case(mpsc::bounded_async::<usize>(300))]
#[case(mpmc::bounded_async::<usize>(1))]
#[case(mpmc::bounded_async::<usize>(10))]
#[case(mpmc::bounded_async::<usize>(100))]
#[case(mpmc::bounded_async::<usize>(300))]
#[tokio::test]
async fn test_pressure_bounded_async_1_1<T: AsyncTxTrait<usize>, R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (T, R),
) {
    let _ = setup_log; // Disable unused var warning
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round: usize = 10000;
    let _round = round;
    tokio::spawn(async move {
        for i in 0.._round {
            if let Err(e) = tx.send(i).await {
                panic!("{:?}", e);
            }
        }
        info!("tx exit");
    });
    'A: loop {
        match rx.recv().await {
            Ok(_i) => {
                counter.as_ref().fetch_add(1, Ordering::SeqCst);
                //debug!("recv {} {}\r", _rx_i, _i);
            }
            Err(_) => break 'A,
        }
    }
    drop(rx);
    assert_eq!(counter.as_ref().load(Ordering::Acquire), round);
}

#[rstest]
#[case(mpsc::bounded_async::<usize>(1), 10)]
#[case(mpsc::bounded_async::<usize>(1), 100)]
#[case(mpsc::bounded_async::<usize>(1), 300)]
#[case(mpsc::bounded_async::<usize>(10), 10)]
#[case(mpsc::bounded_async::<usize>(10), 100)]
#[case(mpsc::bounded_async::<usize>(10), 300)]
#[case(mpsc::bounded_async::<usize>(100), 10)]
#[case(mpsc::bounded_async::<usize>(100), 100)]
#[case(mpsc::bounded_async::<usize>(100), 300)]
#[case(mpmc::bounded_async::<usize>(1), 10)]
#[case(mpmc::bounded_async::<usize>(1), 100)]
#[case(mpmc::bounded_async::<usize>(1), 300)]
#[case(mpmc::bounded_async::<usize>(10), 10)]
#[case(mpmc::bounded_async::<usize>(10), 100)]
#[case(mpmc::bounded_async::<usize>(10), 300)]
#[case(mpmc::bounded_async::<usize>(100), 10)]
#[case(mpmc::bounded_async::<usize>(100), 100)]
#[case(mpmc::bounded_async::<usize>(100), 300)]
#[tokio::test]
async fn test_pressure_bounded_async_multi_1<R: AsyncRxTrait<usize>>(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, R), #[case] tx_count: usize,
) {
    let (noti_tx, mut noti_rx) = tokio::sync::mpsc::channel::<usize>(tx_count);
    let _ = setup_log; // Disable unused var warning
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round: usize = 10000;
    for _tx_i in 0..tx_count {
        let _tx = tx.clone();
        let mut _noti_tx = noti_tx.clone();
        let _round = round;
        tokio::spawn(async move {
            for i in 0.._round {
                match _tx.send(i).await {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            let _ = _noti_tx.send(_tx_i).await;
            info!("tx {} exit", _tx_i);
        });
    }
    drop(tx);
    'A: loop {
        match rx.recv().await {
            Ok(_i) => {
                counter.as_ref().fetch_add(1, Ordering::SeqCst);
                //debug!("recv {} {}\r", _rx_i, _i);
            }
            Err(_) => break 'A,
        }
    }
    drop(rx);
    drop(noti_tx);
    for _ in 0..tx_count {
        match noti_rx.recv().await {
            Some(_) => {}
            None => break,
        }
    }
    assert_eq!(counter.as_ref().load(Ordering::Acquire), round * tx_count);
}

#[rstest]
#[case(mpmc::bounded_async::<usize>(1), 100, 10)]
#[case(mpmc::bounded_async::<usize>(1), 10, 100)]
#[case(mpmc::bounded_async::<usize>(1), 300, 300)]
#[case(mpmc::bounded_async::<usize>(10), 10, 10)]
#[case(mpmc::bounded_async::<usize>(10), 100, 10)]
#[case(mpmc::bounded_async::<usize>(10), 10, 100)]
#[case(mpmc::bounded_async::<usize>(10), 300, 300)]
#[case(mpmc::bounded_async::<usize>(100), 10, 10)]
#[case(mpmc::bounded_async::<usize>(100), 100, 10)]
#[case(mpmc::bounded_async::<usize>(100), 10, 100)]
#[case(mpmc::bounded_async::<usize>(100), 300, 300)]
#[tokio::test]
async fn test_pressure_bounded_async_multi(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, MAsyncRx<usize>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    let (noti_tx, mut noti_rx) = tokio::sync::mpsc::channel::<usize>(tx_count + rx_count);
    let _ = setup_log; // Disable unused var warning
    let (tx, rx) = channel;

    let counter = Arc::new(AtomicUsize::new(0));
    let round: usize = 10000;
    for _tx_i in 0..tx_count {
        let _tx = tx.clone();
        let mut _noti_tx = noti_tx.clone();
        let _round = round;
        tokio::spawn(async move {
            for i in 0.._round {
                match _tx.send(i).await {
                    Err(e) => panic!("{:?}", e),
                    _ => {}
                }
            }
            let _ = _noti_tx.send(_tx_i).await;
            info!("tx {} exit", _tx_i);
        });
    }
    for _rx_i in 0..rx_count {
        let _rx = rx.clone();
        let mut _noti_tx = noti_tx.clone();
        let _counter = counter.clone();
        tokio::spawn(async move {
            'A: loop {
                match _rx.recv().await {
                    Ok(_i) => {
                        _counter.as_ref().fetch_add(1, Ordering::SeqCst);
                        //debug!("recv {} {}\r", _rx_i, _i);
                    }
                    Err(_) => break 'A,
                }
            }
            let _ = _noti_tx.send(_rx_i).await;
            //debug!("rx {} exit", _rx_i);
        });
    }
    drop(tx);
    drop(rx);
    drop(noti_tx);
    for _ in 0..(rx_count + tx_count) {
        match noti_rx.recv().await {
            Some(_) => {}
            None => break,
        }
    }
    assert_eq!(counter.as_ref().load(Ordering::Acquire), round * tx_count);
}
