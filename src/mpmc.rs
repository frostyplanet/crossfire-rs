//! Multiple producers, multiple consumers.

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::share::*;

macro_rules! init_share {
    ($flavor: expr) => {{
        let send_wakers = $flavor.new_reg_sender::<true>();
        let recv_wakers = $flavor.new_reg_recv::<true>();
        ChannelShared::new($flavor.to_flavor(), send_wakers, recv_wakers)
    }};
}

macro_rules! init_array {
    ($bound: expr) => {{
        Array::<T>::new($bound)
    }};
}

/// Creates an unbounded channel for use in a blocking context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub fn unbounded_blocking<T: Unpin>() -> (MTx<T>, MRx<T>) {
    let share = init_share!(List::<T>::new());
    let tx = MTx::new(share.clone());
    let rx = MRx::new(share);
    (tx, rx)
}

/// Creates an unbounded channel for use in an async context.
///
/// Although the sender type is `MTx`, it will never block.
pub fn unbounded_async<T: Unpin>() -> (MTx<T>, MAsyncRx<T>) {
    let share = init_share!(List::<T>::new());
    let tx = MTx::new(share.clone());
    let rx = MAsyncRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel for use in a blocking context.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_blocking<T: Unpin>(size: usize) -> (MTx<T>, MRx<T>) {
    let share = init_share!(init_array!(size));
    let tx = MTx::new(share.clone());
    let rx = MRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel for use in an async context.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_async<T: Unpin>(size: usize) -> (MAsyncTx<T>, MAsyncRx<T>) {
    let share = init_share!(init_array!(size));
    let tx = MAsyncTx::new(share.clone());
    let rx = MAsyncRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where the sender is async and the receiver is blocking.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_async_rx_blocking<T: Unpin>(size: usize) -> (MAsyncTx<T>, MRx<T>) {
    let share = init_share!(init_array!(size));
    let tx = MAsyncTx::new(share.clone());
    let rx = MRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where the sender is blocking and the receiver is async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_blocking_rx_async<T: Unpin>(size: usize) -> (MTx<T>, MAsyncRx<T>) {
    let share = init_share!(init_array!(size));
    let tx = MTx::new(share.clone());
    let rx = MAsyncRx::new(share);
    (tx, rx)
}
