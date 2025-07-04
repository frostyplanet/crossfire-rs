//! Multiple producers, single consumer.
//!
//! The optimization assumes single consumer condition.
//!
//! **NOTE**: For SP / SC version [AsyncRx], [Rx], is not `Clone`, and without `Sync`,
//! Although can be moved to other thread, but not allowed to use send/recv while in Arc.
//!
//! The following code is OK :
//!
//! ``` rust
//! use crossfire::*;
//! async fn foo() {
//!     let (tx, rx) = mpsc::bounded_async::<usize>(100);
//!     tokio::spawn(async move {
//!         let _ = rx.recv().await;
//!     });
//!     drop(tx);
//! }
//! ```
//!
//! Because AsyncRx does not have Sync marker, using `Arc<AsyncRx>` will lose Send marker.
//!
//! For your safety, the following code **should not compile**:
//!
//! ``` compile_fail
//! use crossfire::*;
//! use std::sync::Arc;
//! async fn foo() {
//!     let (tx, rx) = mpsc::bounded_async::<usize>(100);
//!     let rx = Arc::new(rx);
//!     tokio::spawn(async move {
//!         let _ = rx.recv().await;
//!     });
//!     drop(tx);
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
pub fn unbounded_blocking<T: Unpin>() -> (MTx<T>, Rx<T>) {
    let send_wakers = RegistryDummy::new();
    let recv_wakers = RegistrySingle::new();
    let shared = ChannelShared::new(Channel::new_list(), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Initiate an unbounded channel for async context.
///
/// Although sender type is MTx, will never block.
pub fn unbounded_async<T: Unpin>() -> (MTx<T>, AsyncRx<T>) {
    let send_wakers = RegistryDummy::new();
    let recv_wakers = RegistrySingle::new();
    let shared = ChannelShared::new(Channel::new_list(), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel for blocking context
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_blocking<T: Unpin>(mut size: usize) -> (MTx<T>, Rx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistryMulti::new();
    let recv_wakers = RegistrySingle::new();
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender and receiver is async.
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_async<T: Unpin>(mut size: usize) -> (MAsyncTx<T>, AsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistryMulti::new();
    let recv_wakers = RegistrySingle::new();
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = MAsyncTx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender is async, receiver is blocking.
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_tx_async_rx_blocking<T: Unpin>(mut size: usize) -> (MAsyncTx<T>, Rx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistryMulti::new();
    let recv_wakers = RegistrySingle::new();
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = MAsyncTx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Initiate a bounded channel that sender is blocking, receiver is async.
///
/// Special case: 0 size is not supported yet, threat it as 1 size for now.
pub fn bounded_tx_blocking_rx_async<T>(mut size: usize) -> (MTx<T>, AsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistryMulti::new();
    let recv_wakers = RegistrySingle::new();
    let shared = ChannelShared::new(Channel::new_array(size), send_wakers, recv_wakers);
    let tx = MTx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}
