use crate::*;
use captains_log::{logfn, *};
use crossfire::flavor::Flavor;
use crossfire::*;
use rstest::*;
use std::sync::Arc;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(1))]
#[case(mpsc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(1))]
fn test_basic_bounded_empty_full_drop_rx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    // Just don't want to run duplicately in the workflow
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
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
        assert_eq!(tx.try_send(2).unwrap_err(), TrySendError::Disconnected(2));
        assert_eq!(tx.send(2).unwrap_err(), SendError(2));
        let start = Instant::now();
        assert_eq!(
            tx.send_timeout(3, Duration::from_secs(1)).unwrap_err(),
            SendTimeoutError::Disconnected(3)
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(1))]
#[case(mpsc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(1))]
fn test_basic_bounded_empty_full_drop_tx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
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
        assert_eq!(rx.try_recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        assert_eq!(rx.recv().unwrap_err(), RecvError);
        let start = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap_err(),
            RecvTimeoutError::Disconnected
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_blocking())]
#[case(mpsc::unbounded_blocking())]
#[case(mpmc::unbounded_blocking())]
fn test_basic_unbounded_empty_drop_rx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        assert!(tx.is_empty());
        assert!(rx.is_empty());
        assert_eq!(tx.capacity(), None);
        assert_eq!(rx.capacity(), None);
        tx.try_send(1).expect("Ok");
        assert!(!tx.is_empty());
        assert_eq!(tx.is_disconnected(), false);
        assert_eq!(rx.is_disconnected(), false);
        drop(rx);
        assert_eq!(tx.is_disconnected(), true);
        assert_eq!(tx.get_rx_count(), 0);
        assert_eq!(tx.get_tx_count(), 1);
        assert_eq!(tx.try_send(2).unwrap_err(), TrySendError::Disconnected(2));
        assert_eq!(tx.send(2).unwrap_err(), SendError(2));
        let start = Instant::now();
        assert_eq!(
            tx.send_timeout(3, Duration::from_secs(1)).unwrap_err(),
            SendTimeoutError::Disconnected(3)
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_blocking())]
#[case(mpsc::unbounded_blocking())]
#[case(mpmc::unbounded_blocking())]
fn test_basic_unbounded_empty_drop_tx<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        assert!(tx.is_empty());
        assert!(rx.is_empty());
        tx.try_send(1).expect("Ok");
        assert!(!tx.is_empty());
        assert_eq!(tx.is_disconnected(), false);
        assert_eq!(rx.is_disconnected(), false);
        drop(tx);
        assert_eq!(rx.is_disconnected(), true);
        assert_eq!(rx.get_tx_count(), 0);
        assert_eq!(rx.get_rx_count(), 1);
        assert_eq!(rx.recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Disconnected);
        assert_eq!(rx.recv().unwrap_err(), RecvError);
        let start = Instant::now();
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)).unwrap_err(),
            RecvTimeoutError::Disconnected
        );
        assert!(Instant::now() - start < Duration::from_secs(1));
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(10))]
#[case(mpsc::bounded_blocking(10))]
#[case(mpmc::bounded_blocking(10))]
fn test_basic_bounded_1_thread<T: BlockingTxTrait<i32>, R: BlockingRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
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

        let th = thread::spawn(move || {
            for i in 0i32..12 {
                match rx.recv() {
                    Ok(j) => {
                        trace!("recv {}", i);
                        assert_eq!(i, j);
                    }
                    Err(e) => {
                        panic!("error {}", e);
                    }
                }
            }
            let res = rx.recv();
            assert!(res.is_err());
            trace!("rx close");
        });
        assert!(tx.send(10).is_ok());
        sleep(Duration::from_secs(1));
        assert!(tx.send(11).is_ok());
        drop(tx);
        let _ = th.join().unwrap();
    }
}

#[logfn]
#[rstest]
#[case(spsc::unbounded_blocking())]
#[case(mpsc::unbounded_blocking())]
#[case(mpmc::unbounded_blocking())]
fn test_basic_unbounded_1_thread<T: BlockingTxTrait<i32>, R: BlockingRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        let rx_res = rx.try_recv();
        assert!(rx_res.is_err());
        assert!(rx_res.unwrap_err().is_empty());
        for i in 0i32..10 {
            let tx_res = tx.try_send(i);
            assert!(tx_res.is_ok());
        }

        let th = thread::spawn(move || {
            for i in 0i32..12 {
                match rx.recv() {
                    Ok(j) => {
                        trace!("recv {}", i);
                        assert_eq!(i, j);
                    }
                    Err(e) => {
                        panic!("error {}", e);
                    }
                }
            }
            let res = rx.recv();
            assert!(res.is_err());
            trace!("rx close");
        });
        assert!(tx.send(10).is_ok());
        sleep(Duration::from_secs(1));
        assert!(tx.send(11).is_ok());
        drop(tx);
        let _ = th.join().unwrap();
    }
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(10))]
#[case(mpsc::bounded_blocking(10))]
#[case(mpmc::bounded_blocking(10))]
#[case(spsc::unbounded_blocking())]
#[case(mpsc::unbounded_blocking())]
#[case(mpmc::unbounded_blocking())]
fn test_basic_recv_after_sender_close<T: BlockingTxTrait<i32>, R: BlockingRxTrait<i32>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        let total_msg_count = 5;
        for i in 0..total_msg_count {
            let _ = tx.try_send(i).expect("send ok");
        }
        drop(tx);

        // NOTE: 5 < 10
        let mut recv_msg_count = 0;
        loop {
            match rx.recv() {
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
}

#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(1))]
#[case(spsc::bounded_blocking(10))]
#[case(spsc::bounded_blocking(100))]
#[case(spsc::bounded_blocking(300))]
#[case(mpsc::bounded_blocking(1))]
#[case(mpsc::bounded_blocking(10))]
#[case(mpsc::bounded_blocking(100))]
#[case(mpsc::bounded_blocking(300))]
#[case(mpmc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(10))]
#[case(mpmc::bounded_blocking(100))]
#[case(mpmc::bounded_blocking(300))]
fn test_pressure_bounded_blocking_1_1<T: BlockingTxTrait<usize>, R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] _channel: (T, R),
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
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
                if let Err(e) = tx.send(i) {
                    panic!("{:?}", e);
                }
            }
            trace!("tx exit");
        });
        let mut count = 0;
        'A: loop {
            match rx.recv() {
                Ok(_i) => {
                    assert_eq!(_i, count);
                    count += 1;
                    trace!("recv {}", _i);
                }
                Err(_) => break 'A,
            }
        }
        drop(rx);
        let _ = th.join().unwrap();
        assert_eq!(count, round);
    }
}

#[logfn]
#[rstest]
#[case(mpsc::bounded_blocking(1), 3)]
#[case(mpsc::bounded_blocking(1), 5)]
#[case(mpsc::bounded_blocking(1), 10)]
#[case(mpsc::bounded_blocking(1), 16)]
#[case(mpsc::bounded_blocking(10), 4)]
#[case(mpsc::bounded_blocking(10), 7)]
#[case(mpsc::bounded_blocking(10), 12)]
#[case(mpsc::bounded_blocking(100), 3)]
#[case(mpsc::bounded_blocking(100), 9)]
#[case(mpsc::bounded_blocking(100), 13)]
#[case(mpmc::bounded_blocking(1), 2)]
#[case(mpmc::bounded_blocking(1), 5)]
#[case(mpmc::bounded_blocking(1), 15)]
#[case(mpmc::bounded_blocking(10), 3)]
#[case(mpmc::bounded_blocking(10), 7)]
#[case(mpmc::bounded_blocking(10), 16)]
#[case(mpmc::bounded_blocking(100), 2)]
#[case(mpmc::bounded_blocking(100), 8)]
#[case(mpmc::bounded_blocking(100), 16)]
fn test_pressure_bounded_blocking_multi_1<
    F: Flavor<Item = usize> + 'static,
    R: BlockingRxTrait<usize>,
>(
    setup_log: (), #[case] _channel: (MTx<F>, R), #[case] tx_count: usize,
) {
    #[cfg(not(feature = "async_std"))]
    {
        let (tx, rx) = _channel;
        #[cfg(miri)]
        {
            if tx_count > 5 {
                println!("skip");
                return;
            }
        }

        let round: usize = ROUND * 10;
        let mut th_s = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            th_s.push(thread::spawn(move || {
                for i in 0..round {
                    match _tx.send(i) {
                        Err(e) => panic!("{:?}", e),
                        _ => {}
                    }
                }
                trace!("tx {} exit", _tx_i);
            }));
        }
        drop(tx);
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
        drop(rx);
        for th in th_s {
            let _ = th.join().unwrap();
        }
        assert_eq!(count, round * tx_count);
    }
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_blocking(1), 2, 2)]
#[case(mpmc::bounded_blocking(1), 16, 2)]
#[case(mpmc::bounded_blocking(1), 2, 16)]
#[case(mpmc::bounded_blocking(10), 2, 2)]
#[case(mpmc::bounded_blocking(10), 13, 2)]
#[case(mpmc::bounded_blocking(10), 3, 10)]
#[case(mpmc::bounded_blocking(100), 3, 3)]
#[case(mpmc::bounded_blocking(100), 8, 3)]
#[case(mpmc::bounded_blocking(100), 3, 8)]
#[case(mpmc::bounded_blocking(100), 5, 5)]
fn test_pressure_bounded_blocking_multi<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] _channel: (MTx<F>, MRx<F>), #[case] tx_count: usize,
    #[case] rx_count: usize,
) {
    #[cfg(not(feature = "async_std"))]
    {
        let round: usize;
        #[cfg(miri)]
        {
            if tx_count > 5 || rx_count > 5 {
                println!("skip");
                return;
            }
            round = ROUND;
        }
        #[cfg(not(miri))]
        {
            round = ROUND * 10;
        }
        let (tx, rx) = _channel;
        let mut th_tx = Vec::new();
        let mut th_rx = Vec::new();
        for _tx_i in 0..tx_count {
            let _tx = tx.clone();
            th_tx.push(thread::spawn(move || {
                for i in 0..round {
                    match _tx.send(i) {
                        Err(e) => panic!("{:?}", e),
                        _ => {}
                    }
                }
                trace!("tx {} exit", _tx_i);
            }));
        }
        for _rx_i in 0..rx_count {
            let _rx = rx.clone();
            th_rx.push(thread::spawn(move || {
                let mut count = 0;
                'A: loop {
                    match _rx.recv() {
                        Ok(_i) => {
                            count += 1;
                            trace!("recv {} {}", _rx_i, _i);
                        }
                        Err(_) => break 'A,
                    }
                }
                trace!("rx {} exit", _rx_i);
                count
            }));
        }
        drop(tx);
        drop(rx);
        let mut total_count = 0;
        for th in th_tx {
            let _ = th.join().unwrap();
        }
        for th in th_rx {
            total_count += th.join().unwrap();
        }
        assert_eq!(total_count, round * tx_count);
    }
}

#[logfn]
#[rstest]
#[case(mpmc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(10))]
fn test_pressure_bounded_timeout_blocking<F: Flavor<Item = usize> + 'static>(
    setup_log: (), #[case] _channel: (MTx<F>, MRx<F>),
) {
    #[cfg(not(feature = "async_std"))]
    {
        use std::collections::HashMap;
        use std::sync::Mutex;
        let (tx, rx) = _channel;

        assert_eq!(
            rx.recv_timeout(Duration::from_millis(1)).unwrap_err(),
            RecvTimeoutError::Timeout
        );
        let (tx_wakers, rx_wakers) = rx.get_wakers_count();
        println!("wakers: {}, {}", tx_wakers, rx_wakers);
        assert_eq!(tx_wakers, 0);
        assert_eq!(rx_wakers, 0);

        let recv_map = Arc::new(Mutex::new(HashMap::new()));

        let mut th_tx = Vec::new();
        let mut th_rx = Vec::new();
        let tx_count: usize = 3;
        for thread_id in 0..tx_count {
            let _recv_map = recv_map.clone();
            let _tx = tx.clone();
            th_tx.push(thread::spawn(move || {
                // randomize start up
                sleep(Duration::from_millis((thread_id & 3) as u64));
                let mut local_timeout_counter = 0;
                for i in 0..ROUND {
                    {
                        let mut guard = _recv_map.lock().unwrap();
                        guard.insert(i, ());
                    }
                    if i & 2 == 0 {
                        sleep(Duration::from_millis(3));
                    } else {
                        sleep(Duration::from_millis(1));
                    }
                    loop {
                        match _tx.send_timeout(i, Duration::from_millis(1)) {
                            Ok(_) => break,
                            Err(SendTimeoutError::Timeout(_i)) => {
                                local_timeout_counter += 1;
                                assert_eq!(_i, i);
                            }
                            Err(SendTimeoutError::Disconnected(_)) => {
                                unreachable!();
                            }
                        }
                    }
                }
                local_timeout_counter
            }));
        }
        for _thread_id in 0..2 {
            let _rx = rx.clone();
            let _recv_map = recv_map.clone();
            th_rx.push(thread::spawn(move || {
                let mut step: usize = 0;
                let mut local_recv_counter = 0;
                let mut local_timeout_counter = 0;
                loop {
                    step += 1;
                    let timeout = if step & 2 == 0 { 1 } else { 2 };
                    if step & 2 > 0 {
                        sleep(Duration::from_millis(1));
                    }
                    match _rx.recv_timeout(Duration::from_millis(timeout)) {
                        Ok(item) => {
                            local_recv_counter += 1;
                            {
                                let mut guard = _recv_map.lock().unwrap();
                                guard.remove(&item);
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            local_timeout_counter += 1;
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            return (local_recv_counter, local_timeout_counter);
                        }
                    }
                }
            }));
        }
        drop(tx);
        drop(rx);
        let mut total_recv_count = 0;
        let mut total_send_timeout = 0;
        let mut total_recv_timeout = 0;
        for th in th_tx {
            total_send_timeout += th.join().unwrap();
        }
        for th in th_rx {
            // rx threads return recv_count
            let (local_recv_counter, local_timeout_counter) = th.join().unwrap();
            total_recv_count += local_recv_counter;
            total_recv_timeout += local_timeout_counter;
        }
        {
            let guard = recv_map.lock().unwrap();
            assert!(guard.is_empty());
        }
        assert_eq!(ROUND * tx_count, total_recv_count);
        println!("send timeout count: {}", total_send_timeout);
        println!("recv timeout count: {}", total_recv_timeout);
    }
}

#[test]
fn test_conversion() {
    let (mtx, mrx) = mpmc::bounded_blocking::<usize>(1);
    let _tx: Tx<_> = mtx.into();
    let _rx: Rx<_> = mrx.into();
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(1))]
#[case(spsc::bounded_blocking(10))]
#[case(mpsc::bounded_blocking(1))]
#[case(mpsc::bounded_blocking(10))]
#[case(mpmc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(10))]
fn test_drop_small_msg<T: BlockingTxTrait<SmallMsg>, R: BlockingRxTrait<SmallMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<SmallMsg>());
    _test_drop_msg(channel);
}

// This test make sure we have correctly use of maybeuninit
#[logfn]
#[rstest]
#[case(spsc::bounded_blocking(1))]
#[case(spsc::bounded_blocking(10))]
#[case(mpsc::bounded_blocking(1))]
#[case(mpsc::bounded_blocking(10))]
#[case(mpmc::bounded_blocking(1))]
#[case(mpmc::bounded_blocking(10))]
fn test_drop_large_msg<T: BlockingTxTrait<LargeMsg>, R: BlockingRxTrait<LargeMsg>>(
    setup_log: (), #[case] channel: (T, R),
) {
    println!("needs_drop {}", std::mem::needs_drop::<LargeMsg>());
    _test_drop_msg(channel);
}

fn _test_drop_msg<M: TestDropMsg, T: BlockingTxTrait<M>, R: BlockingRxTrait<M>>(channel: (T, R)) {
    let (tx, rx) = channel;
    reset_drop_counter();
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
    let th = thread::spawn(move || {
        let _msg = rx.recv().expect("recv");
        assert_eq!(_msg.get_value(), 0);
        drop(_msg);
        rx
    });
    let msg = M::new(ids);
    tx.send(msg).expect("send");
    ids += 1;
    let rx = th.join().unwrap();
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
    if let Err(SendError(_msg)) = tx.send(msg) {
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
}
