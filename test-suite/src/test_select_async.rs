use crate::*;
use captains_log::logfn;
use crossfire::{mpmc, mpsc};
use futures_util::{select, FutureExt};
use rstest::*;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_mpmc_null_async_close(setup_log: ()) {
    let flavor = mpmc::Null::new();
    let (tx, rx) = flavor.new_async();

    runtime_block_on!(async move {
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            drop(tx);
        });

        let res = rx.recv().await;
        assert!(res.is_err());
        async_join_result!(th);
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_mpsc_null_async_close(setup_log: ()) {
    let flavor = mpsc::Null::new();
    let (tx, rx) = flavor.new_async();

    runtime_block_on!(async move {
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            drop(tx);
        });

        let res = rx.recv().await;
        assert!(res.is_err());
        async_join_result!(th);
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_mpmc_null_select(setup_log: ()) {
    let flavor = mpmc::Null::new();
    let (tx, rx) = flavor.new_async();

    runtime_block_on!(async move {
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            drop(tx);
        });

        let closed = select! {
            res = rx.recv().fuse() => {
                if res.is_err() {
                    true
                } else {
                    panic!("Should not receive message from null");
                }
            }
        };
        assert!(closed);
        async_join_result!(th);
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_mpsc_null_select(setup_log: ()) {
    let flavor = mpsc::Null::new();
    let (tx, rx) = flavor.new_async();

    runtime_block_on!(async move {
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            drop(tx);
        });

        let closed = select! {
            res = rx.recv().fuse() => {
                if res.is_err() {
                    true
                } else {
                    panic!("Should not receive message from null");
                }
            }
        };
        assert!(closed);
        async_join_result!(th);
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_null_select_timeout(setup_log: ()) {
    let flavor = mpmc::Null::new();
    let (tx, rx) = flavor.new_async();

    runtime_block_on!(async move {
        // Don't drop tx yet
        let timed_out = select! {
            res = rx.recv().fuse() => {
                if res.is_err() {
                    panic!("Should not be closed yet");
                }
                false
            }
            _ = sleep(Duration::from_millis(50)).fuse() => {
                true
            }
        };
        assert!(timed_out);
        drop(tx);
    });
}

#[logfn]
#[rstest]
fn test_null_mixed_with_active_channel(setup_log: ()) {
    let flavor = mpmc::Null::new();
    let (tx_null, rx_null) = flavor.new_async();
    let (tx_data, rx_data) = mpmc::bounded_async::<i32>(10);

    runtime_block_on!(async move {
        tx_data.send(42).await.unwrap();

        // Data ready, null not triggered
        select! {
            _ = rx_null.recv().fuse() => {
                panic!("Null triggered unexpectedly");
            }
            res = rx_data.recv().fuse() => {
                assert_eq!(res.unwrap(), 42);
            }
        }
        drop(tx_null);
    });
}

#[cfg(feature = "time")]
#[logfn]
#[rstest]
fn test_null_mixed_trigger(setup_log: ()) {
    let flavor = mpmc::Null::new();
    let (tx_null, rx_null) = flavor.new_async();
    let (_tx_data, rx_data) = mpmc::bounded_async::<i32>(10);

    runtime_block_on!(async move {
        let th = async_spawn!(async move {
            sleep(Duration::from_millis(50)).await;
            drop(tx_null);
        });

        // Data not ready (empty), null triggered via drop
        let null_triggered = select! {
            res = rx_null.recv().fuse() => {
                assert!(res.is_err());
                true
            }
            _ = rx_data.recv().fuse() => {
                panic!("Data triggered unexpectedly");
            }
        };
        assert!(null_triggered);
        async_join_result!(th);
    });
}
