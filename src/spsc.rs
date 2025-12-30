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

macro_rules! init_share {
    ($flavor: expr) => {{
        let send_wakers = $flavor.new_reg_sender::<false>();
        let recv_wakers = $flavor.new_reg_recv::<false>();
        ChannelShared::new($flavor.to_flavor(), send_wakers, recv_wakers)
    }};
}

macro_rules! init_array {
    ($bound: expr) => {{
        if $bound <= 1 {
            init_share!(OneSize::<T>::new())
        } else {
            init_share!(Array::<T, false, false>::new($bound))
        }
    }};
}

/// Creates an unbounded channel for use in a blocking context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub fn unbounded_blocking<T: Unpin>() -> (Tx<T>, Rx<T>) {
    let share = init_share!(List::<T>::new());
    let tx = Tx::new(share.clone());
    let rx = Rx::new(share);
    (tx, rx)
}

/// Creates an unbounded channel for use in an async context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub fn unbounded_async<T: Unpin>() -> (Tx<T>, AsyncRx<T>) {
    let share = init_share!(List::<T>::new());
    let tx = Tx::new(share.clone());
    let rx = AsyncRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel for use in a blocking context.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_blocking<T: Unpin>(size: usize) -> (Tx<T>, Rx<T>) {
    let share = init_array!(size);
    let tx = Tx::new(share.clone());
    let rx = Rx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where both the sender and receiver are async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_async<T: Unpin>(size: usize) -> (AsyncTx<T>, AsyncRx<T>) {
    let share = init_array!(size);
    let tx = AsyncTx::new(share.clone());
    let rx = AsyncRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where the sender is async and the receiver is blocking.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_async_rx_blocking<T: Unpin>(size: usize) -> (AsyncTx<T>, Rx<T>) {
    let share = init_array!(size);
    let tx = AsyncTx::new(share.clone());
    let rx = Rx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where the sender is blocking and the receiver is async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_blocking_rx_async<T>(size: usize) -> (Tx<T>, AsyncRx<T>) {
    let share = init_array!(size);
    let tx = Tx::new(share.clone());
    let rx = AsyncRx::new(share);
    (tx, rx)
}
