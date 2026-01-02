use super::common::*;
use crate::oneshot;
use crate::*;
use captains_log::logfn;
use rstest::*;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_basic(setup_log: ()) {
    let (tx, rx) = oneshot::new_blocking();
    tx.send(42);
    assert_eq!(rx.recv(), Ok(42));
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_tx(setup_log: ()) {
    let (tx, rx) = oneshot::new_blocking::<i32>();
    drop(tx);
    assert_eq!(rx.recv(), Err(RecvError));
}

#[logfn]
#[rstest]
fn test_oneshot_blocking_drop_rx(setup_log: ()) {
    let (tx, rx) = oneshot::new_blocking::<i32>();
    drop(rx);
    // send consumes tx, returns ()
    tx.send(42);
}

#[logfn]
#[rstest]
fn test_oneshot_async_basic(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::new_async();
        tx.send(42);
        assert_eq!(rx.await, Ok(42));
    });
}

#[logfn]
#[rstest]
fn test_oneshot_async_drop_tx(setup_log: ()) {
    runtime_block_on!(async move {
        let (tx, rx) = oneshot::new_async::<i32>();
        drop(tx);
        assert_eq!(rx.await, Err(RecvError));
    });
}

#[logfn]
#[rstest]
fn test_oneshot_pressure(setup_log: ()) {
    let round = 1000;
    runtime_block_on!(async move {
        let mut tasks = Vec::new();
        for i in 0..round {
            tasks.push(async_spawn!(async move {
                let (tx, rx) = oneshot::new_async();
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
fn test_oneshot_leak(setup_log: ()) {
    // Check if OneShot drops the value if not received
    reset_drop_counter();
    {
        let (tx, _rx) = oneshot::new_blocking::<SmallMsg>();
        tx.send(SmallMsg::new(1));
    } // tx dropped (closed), rx dropped (OneShot dropped). msg should be dropped.
    assert_eq!(get_drop_counter(), 1);
}

#[logfn]
#[rstest]
fn test_oneshot_drop_after_recv(setup_log: ()) {
    // Check if OneShot drops the value after recv (it shouldn't, Rx has it)
    reset_drop_counter();
    {
        let (tx, rx) = oneshot::new_blocking::<SmallMsg>();
        tx.send(SmallMsg::new(1));
        let msg = rx.recv().unwrap();
        assert_eq!(get_drop_counter(), 0);
        drop(msg);
        assert_eq!(get_drop_counter(), 1);
    }
    // OneShot dropped. Should NOT drop again.
    assert_eq!(get_drop_counter(), 1);
}
