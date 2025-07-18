//! Single producer, single consumer.
//!
//! The optimization assumes single producer and consumer condition.
//!
//! **NOTE**: For SP / SC version [AsyncTx], [AsyncRx], [Tx], [Rx], is not `Clone`, and without `Sync`,
//! Although can be moved to other thread, but not allowed to use send/recv while in Arc.
//!
//! The following code is OK :
//!
//! ``` rust
//! use crossfire::*;
//! async fn foo() {
//!     let (tx, rx) = spsc::bounded_async::<usize>(100);
//!     tokio::spawn(async move {
//!          let _ = tx.send(2).await;
//!     });
//!     drop(rx);
//! }
//! ```
//!
//! Because AsyncTx does not have Sync marker, using `Arc<AsyncTx>` will lose Send marker.
//!
//! For your safety, the following code **should not compile**:
//!
//! ``` compile_fail
//! use crossfire::*;
//! use std::sync::Arc;
//! async fn foo() {
//!     let (tx, rx) = spsc::bounded_async::<usize>(100);
//!     let tx = Arc::new(tx);
//!     tokio::spawn(async move {
//!          let _ = tx.send(2).await;
//!     });
//!     drop(rx);
//! }
//! ```

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::channel::*;

/// Initiate an unbounded channel for blocking context.
///
/// Sender will never block, so we use the same TxBlocking for threads
pub fn unbounded_blocking<T: Unpin>() -> (Tx<T>, Rx<T>) {
    let send_wakers = RegistrySender::Dummy(RegistryDummy::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Single(RegistrySingle::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_list(), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Initiate an unbounded channel for async context.
///
/// Sender will never block, so we use the same TxBlocking for threads
pub fn unbounded_async<T: Unpin>() -> (Tx<T>, AsyncRx<T>) {
    let send_wakers = RegistrySender::Dummy(RegistryDummy::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Single(RegistrySingle::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_list(), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel for blocking context
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_blocking<T: Unpin>(mut size: usize) -> (Tx<T>, Rx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Single(RegistrySingle::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Single(RegistrySingle::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender and receiver are async
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_async<T: Unpin>(mut size: usize) -> (AsyncTx<T>, AsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Single(RegistrySingle::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Single(RegistrySingle::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = AsyncTx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender is async, receiver is blocking
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_tx_async_rx_blocking<T: Unpin>(mut size: usize) -> (AsyncTx<T>, Rx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Single(RegistrySingle::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Single(RegistrySingle::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = AsyncTx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender is blocking, receiver is sync
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_tx_blocking_rx_async<T>(mut size: usize) -> (Tx<T>, AsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::Single(RegistrySingle::<SendWaker<T>>::new());
    let recv_wakers = RegistryRecv::Single(RegistrySingle::<RecvWaker>::new());
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}
