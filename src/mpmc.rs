//! Multiple producers, multiple consumers.

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::share::*;

/// Creates an unbounded channel for use in a blocking context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub fn unbounded_blocking<T: Unpin>() -> (MTx<T>, MRx<T>) {
    let send_wakers = RegistrySender::Dummy;
    let recv_wakers = RegistryRecv::new_multi();
    let shared = ChannelShared::new(List::new(), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = MRx::new(shared);
    (tx, rx)
}

/// Creates an unbounded channel for use in an async context.
///
/// Although the sender type is `MTx`, it will never block.
pub fn unbounded_async<T: Unpin>() -> (MTx<T>, MAsyncRx<T>) {
    let send_wakers = RegistrySender::Dummy;
    let recv_wakers = RegistryRecv::new_multi();
    let shared = ChannelShared::new(List::new(), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = MAsyncRx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel for use in a blocking context.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_blocking<T: Unpin>(mut size: usize) -> (MTx<T>, MRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_multi();
    let recv_wakers = RegistryRecv::new_multi();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = MRx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel for use in an async context.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_async<T: Unpin>(mut size: usize) -> (MAsyncTx<T>, MAsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_multi();
    let recv_wakers = RegistryRecv::new_multi();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);
    let tx = MAsyncTx::new(shared.clone());
    let rx = MAsyncRx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel where the sender is async and the receiver is blocking.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_async_rx_blocking<T: Unpin>(mut size: usize) -> (MAsyncTx<T>, MRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_multi();
    let recv_wakers = RegistryRecv::new_multi();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);

    let tx = MAsyncTx::new(shared.clone());
    let rx = MRx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel where the sender is blocking and the receiver is async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_blocking_rx_async<T: Unpin>(mut size: usize) -> (MTx<T>, MAsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_multi();
    let recv_wakers = RegistryRecv::new_multi();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);

    let tx = MTx::new(shared.clone());
    let rx = MAsyncRx::new(shared);
    (tx, rx)
}
