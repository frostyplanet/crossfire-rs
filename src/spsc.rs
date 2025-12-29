//! Single producer, single consumer.
//!
//! The optimization assumes a single producer and consumer, so waker registration is completely lockless.
//!
//! **NOTE**: For the SP/SC version, [AsyncTx], [AsyncRx], [Tx], and [Rx] are not `Clone` and do not implement `Sync`.
//! Although they can be moved to other threads, they are not allowed to be used with `send`/`recv` while in an `Arc`.
//!
//! The following code is OK:
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
//! Because the `AsyncTx` does not have the `Sync` marker, using `Arc<AsyncTx>` will lose the `Send` marker.
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
use crate::share::*;

/// Creates an unbounded channel for use in a blocking context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub fn unbounded_blocking<T: Unpin>() -> (Tx<T>, Rx<T>) {
    let send_wakers = RegistrySender::Dummy;
    let recv_wakers = RegistryRecv::new_single();
    let shared = ChannelShared::new(List::new(), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Creates an unbounded channel for use in an async context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub fn unbounded_async<T: Unpin>() -> (Tx<T>, AsyncRx<T>) {
    let send_wakers = RegistrySender::Dummy;
    let recv_wakers = RegistryRecv::new_single();
    let shared = ChannelShared::new(List::new(), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel for use in a blocking context.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_blocking<T: Unpin>(mut size: usize) -> (Tx<T>, Rx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_single();
    let recv_wakers = RegistryRecv::new_single();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel where both the sender and receiver are async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_async<T: Unpin>(mut size: usize) -> (AsyncTx<T>, AsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_single();
    let recv_wakers = RegistryRecv::new_single();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);
    let tx = AsyncTx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel where the sender is async and the receiver is blocking.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_async_rx_blocking<T: Unpin>(mut size: usize) -> (AsyncTx<T>, Rx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_single();
    let recv_wakers = RegistryRecv::new_single();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);
    let tx = AsyncTx::new(shared.clone());
    let rx = Rx::new(shared);
    (tx, rx)
}

/// Creates a bounded channel where the sender is blocking and the receiver is async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_blocking_rx_async<T>(mut size: usize) -> (Tx<T>, AsyncRx<T>) {
    if size == 0 {
        size = 1;
    }
    let send_wakers = RegistrySender::new_single();
    let recv_wakers = RegistryRecv::new_single();
    let shared = ChannelShared::new(Array::new(size), send_wakers, recv_wakers);
    let tx = Tx::new(shared.clone());
    let rx = AsyncRx::new(shared);
    (tx, rx)
}
