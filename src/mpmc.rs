//! Multiple producers, multiple consumers.

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::channel::*;

/// Initiate an unbounded channel for blocking context.
///
/// Sender will never block, so we use the same TxBlocking for threads
pub fn unbounded_blocking<T: Unpin>() -> (MTx<T>, MRx<T>) {
    let send_wakers = RegistrySender::Dummy(RegistryDummy::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Multi(RegistryMulti::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_list(), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = MRx::new(shared);
    (tx, rx)
}

/// Initiate an unbounded channel for async context.
///
/// Although sender type is MTx, will never block.
pub fn unbounded_async<T: Unpin>() -> (MTx<T>, MAsyncRx<T>) {
    let send_wakers = RegistrySender::Dummy(RegistryDummy::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Multi(RegistryMulti::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_list(), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = MAsyncRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel for blocking context
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_blocking<T: Unpin>(mut size: usize) -> (MTx<T>, MRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Multi(RegistryMulti::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Multi(RegistryMulti::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = MRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel for async context.
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_async<T: Unpin>(mut size: usize) -> (MAsyncTx<T>, MAsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Multi(RegistryMulti::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Multi(RegistryMulti::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = MAsyncTx::new(shared.clone());
    let rx = MAsyncRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender is async, receiver is blocking.
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_tx_async_rx_blocking<T: Unpin>(mut size: usize) -> (MAsyncTx<T>, MRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Multi(RegistryMulti::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Multi(RegistryMulti::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);

    let tx = MAsyncTx::new(shared.clone());
    let rx = MRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender is blocking, receiver is async
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_tx_blocking_rx_async<T: Unpin>(mut size: usize) -> (MTx<T>, MAsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Multi(RegistryMulti::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Multi(RegistryMulti::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);

    let tx = MTx::new(shared.clone());
    let rx = MAsyncRx::new(shared);
    (tx, rx)
}
