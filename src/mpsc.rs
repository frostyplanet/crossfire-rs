//! Multiple producers, single consumer.
//!
//! The optimization assumes a single consumer. The waker registration of the receiver is lossless compared to `mpmc`.
//!
//! **NOTE**: For the SC (single consumer) version, [AsyncRx] and [Rx] are not `Clone` and do not implement `Sync`.
//! Although they can be moved to other threads, they are not allowed to be used with `send`/`recv` while in an `Arc`.
//!
//! The following code is OK:
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
//! Because the `AsyncRx` does not have the `Sync` marker, using `Arc<AsyncRx>` will lose the `Send` marker.
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
use crate::shared::*;

macro_rules! init_share {
    ($flavor: expr) => {{
        let send_wakers = $flavor.new_reg_sender::<true>();
        let recv_wakers = $flavor.new_reg_recv::<false>();
        ChannelShared::new($flavor, send_wakers, recv_wakers)
    }};
}

macro_rules! init_array {
    ($bound: expr) => {{
        if $bound <= 1 {
            init_share!(OneSize::<T>::new())
        } else {
            init_share!(Array::<T, true, true>::new($bound))
        }
    }};
}

/// Creates an unbounded channel for use in a blocking context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub fn unbounded_blocking<T: Unpin + Send + 'static>() -> (MTx<T>, Rx<T>) {
    let share = init_share!(List::<T>::new());
    let tx = MTx::new(share.clone());
    let rx = Rx::new(share);
    (tx, rx)
}

/// Creates an unbounded channel for use in an async context.
///
/// Although the sender type is `MTx`, it will never block.
pub fn unbounded_async<T: Unpin + Send + 'static>() -> (MTx<T>, AsyncRx<T>) {
    let share = init_share!(List::<T>::new());
    let tx = MTx::new(share.clone());
    let rx = AsyncRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel for use in a blocking context.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_blocking<T: Unpin + Send + 'static>(size: usize) -> (MTx<T>, Rx<T>) {
    let share = init_array!(size);
    let tx = MTx::new(share.clone());
    let rx = Rx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where both the sender and receiver are async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_async<T: Unpin + Send + 'static>(size: usize) -> (MAsyncTx<T>, AsyncRx<T>) {
    let share = init_array!(size);
    let tx = MAsyncTx::new(share.clone());
    let rx = AsyncRx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where the sender is async and the receiver is blocking.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_async_rx_blocking<T: Unpin + Send + 'static>(
    size: usize,
) -> (MAsyncTx<T>, Rx<T>) {
    let share = init_array!(size);
    let tx = MAsyncTx::new(share.clone());
    let rx = Rx::new(share);
    (tx, rx)
}

/// Creates a bounded channel where the sender is blocking and the receiver is async.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
pub fn bounded_tx_blocking_rx_async<T: Unpin + Send + 'static>(
    size: usize,
) -> (MTx<T>, AsyncRx<T>) {
    let share = init_array!(size);
    let tx = MTx::new(share.clone());
    let rx = AsyncRx::new(share);
    (tx, rx)
}
