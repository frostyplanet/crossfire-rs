#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

//! # Crossfire
//!
//! High-performance lockless spsc/mpsc/mpmc channels.
//!
//! It supports async contexts and bridges the gap between async and blocking contexts.
//!
//! The low level is based on crossbeam-queue.
//! For the concept, please refer to the [wiki](https://github.com/frostyplanet/crossfire-rs/wiki).
//!
//! ## Version history
//!
//! * v1.0: Released in 2022.12 and used in production.
//!
//! * v2.0: Released in 2025.6. Refactored the codebase and API
//! by removing generic types from the ChannelShared type, which made it easier to code with.
//!
//! * v2.1: Released in 2025.9. Removed the dependency on crossbeam-channel
//! and implemented with [a modified version of crossbeam-queue](https://github.com/frostyplanet/crossfire-rs/wiki/crossbeam-related),
//! which brings performance improvements for both async and blocking contexts.
//!
//! ## Test status
//!
//! **WARNING**: Because v2.1 has push the speed to a level no one has gone before, it can put a pure pressure to the async runtime.
//! Some hidden bug (especially atomic ops on weaker ordering platform) might occur.
//!
//! Refer to the [README](https://github.com/frostyplanet/crossfire-rs?tab=readme-ov-file#test-status) page for known issue on specified platform and runtime.
//!
//! ## Performance
//!
//! Being a lockless channel, crossfire outperforms other async-capable channels.
//! And thanks to a lighter notification mechanism, some cases in blocking context are even
//! better than the original crossbeam-channel,
//!
//! benchmark data is posted on [wiki](https://github.com/frostyplanet/crossfire-rs/wiki/benchmark-v2.1.0-vs-v2.0.26-2025%E2%80%9009%E2%80%9021).
//!
//! Also, being a lockless channel, the algorithm relies on spinning and yielding. Spinning is good on
//! multi-core systems, but not friendly to single-core systems (like virtual machines).
//! So we provide a function [detect_backoff_cfg()] to detect the running platform.
//! Calling it within the initialization section of your code, will get a 2x performance boost on
//! VPS.
//!
//! The benchmark is written in the criterion framework. You can run the benchmark by:
//!
//! ``` shell
//! cargo bench --bench crossfire
//! ```
//!
//! ## APIs
//!
//! ### Modules and functions
//!
//! There are 3 modules: [spsc], [mpsc], [mpmc]. Each has different underlying implementation
//! optimized to it concurrent model.
//! The SP or SC interface is only for non-concurrent operation. It's more memory-efficient in waker registration,
//! and has atomic ops cost reduced in the lockless algorithm.
//!
//! - Each module has [build()](crate::mpmc::build()) function, which can apply to all the channel flavors.
//! - Altough the [One](crate::mpmc::One) flavor can be used standalone, it's also wrapped inside enum `Array`,
//! to provide unified type for bounded channel.
//! - **NOTE** : Although the name [Array](crate::mpmc::Array), [List](crate::mpmc::List) are the same between spsc/mpsc/mpmc module,
//! they are different type alias local to its parent module. We suggest distinguish by
//! namespace when import for use.
//! - **NOTE** :  For a bounded channel, a 0 size case is not supported yet. (rewrite as 1 size).
//!
//!
//! ### Types
//!
//! <table align="center" cellpadding="20">
//! <tr> <th rowspan="2"> Context</th><th colspan="2" align="center">Sender (Producer)</th> <th colspan="2" align="center">Receiver (Consumer)</th> </tr>
//! <tr> <td>Single</td> <td>Multiple</td><td>Single</td><td>Multiple</td></tr>
//! <tr><td rowspan="2"><b>Blocking</b></td><td colspan="2" align="center"><a href="trait.BlockingTxTrait.html">BlockingTxTrait</a></td>
//! <td colspan="2" align="center"><a href="trait.BlockingRxTrait.html">BlockingRxTrait</a></td></tr>
//! <tr>
//! <td align="center"><a href="struct.Tx.html">Tx</a></td>
//! <td align="center"><a href="struct.MTx.html">MTx</a></td>
//! <td align="center"><a href="struct.Rx.html">Rx</a></td>
//! <td align="center"><a href="struct.MRx.html">MRx</a></td> </tr>
//!
//! <tr><td rowspan="2"><b>Async</b></td>
//! <td colspan="2" align="center"><a href="trait.AsyncTxTrait.html">AsyncTxTrait</a></td>
//! <td colspan="2" align="center"><a href="trait.AsyncRxTrait.html">AsyncRxTrait</a></td></tr>
//! <tr><td><a href="struct.AsyncTx.html">AsyncTx</a></td>
//! <td><a href="struct.MAsyncTx.html">MAsyncTx</a></td><td><a href="struct.AsyncRx.html">AsyncRx</a></td>
//! <td><a href="struct.MAsyncRx.html">MAsyncRx</a></td></tr>
//!
//! </table>
//!
//! For the SP / SC version, [AsyncTx], [AsyncRx], [Tx], and [Rx] are not `Clone` and without `Sync`.
//! Although can be moved to other threads, but not allowed to use send/recv while in an Arc. (Refer to the compile_fail
//! examples in the type document).
//!
//! The benefit of using the SP / SC API is completely lockless waker registration, in exchange for a performance boost.
//!
//! The sender/receiver can use the `From` trait to **convert between blocking and async context
//! counterparts**.
//!
//! ### Error types
//!
//! Error types are the same as crossbeam-channel:
//!
//! [TrySendError], [SendError], [SendTimeoutError], [TryRecvError], [RecvError], [RecvTimeoutError]
//!
//! ### Async compatibility
//!
//! Tested on tokio-1.x and async-std-1.x, crossfire is runtime-agnostic.
//!
//! The following scenarios are considered:
//!
//! * The [AsyncTx::send()] and [AsyncRx::recv()] operations are **cancellation-safe** in an async context.
//! You can safely use the select! macro and timeout() function in tokio/futures in combination with recv().
//!  On cancellation, [SendFuture] and [RecvFuture] will trigger drop(), which will clean up the state of the waker,
//! making sure there is no memory-leak and deadlock.
//! But you cannot know the true result from SendFuture, since it's dropped
//! upon cancellation. Thus, we suggest using [AsyncTx::send_timeout()] instead.
//!
//! * When the "tokio" or "async_std" feature is enabled, we also provide two additional functions:
//!
//! - [send_timeout()](crate::AsyncTx::send_timeout()), which will return the message that failed to be sent in
//! [SendTimeoutError]. We guarantee the result is atomic. Alternatively, you can use
//! [send_with_timer()](crate::AsyncTx::send_with_timer()).
//!
//! - [recv_timeout()](crate::AsyncRx::recv_timeout()), we guarantee the result is atomic.
//! Alternatively, you can use [recv_with_timer()](crate::AsyncRx::recv_with_timer())
//!
//! * Between blocking context and async context, and between different async runtime instances.
//!
//! * The async waker footprint.
//!
//! When using a multi-producer and multi-consumer scenario, there's a small memory overhead to pass along a `Weak`
//! reference of wakers.
//! Because we aim to be lockless, when the sending/receiving futures are canceled (like tokio::time::timeout()),
//! it might trigger an immediate cleanup if the try-lock is successful, otherwise will rely on lazy cleanup.
//! (This won't be an issue because weak wakers will be consumed by actual message send and recv).
//! On an idle-select scenario, like a notification for close, the waker will be reused as much as possible
//! if poll() returns pending.//!
//!
//! ## Usage
//!
//! Cargo.toml:
//! ```toml
//! [dependencies]
//! crossfire = "3.0"
//! ```
//!
//! ### Feature flags
//!
//! * `compat`: Enable the [compat] model, which has the same API namespace struct as V2.x
//!
//! * `tokio`: Enable [send_timeout](crate::AsyncTx::send_timeout()), [recv_timeout](crate::AsyncRx::recv_timeout()) with tokio sleep function. (conflict
//! with `async_std` feature)
//!
//! * `async_std`: Enable send_timeout, recv_timeout with async-std sleep function. (conflict
//! with `tokio` feature)
//!
//! * `trace_log`: Development mode, to enable internal log while testing or benchmark, to debug deadlock issues.
//!
//! ### Example with tokio::select!
//!
//! ```rust
//!
//! extern crate crossfire;
//! use crossfire::*;
//! #[macro_use]
//! extern crate tokio;
//! use tokio::time::{sleep, interval, Duration};
//!
//! #[tokio::main]
//! async fn main() {
//!     let (tx, rx) = mpsc::bounded_async::<i32>(100);
//!     for _ in 0..10 {
//!         let _tx = tx.clone();
//!         tokio::spawn(async move {
//!             for i in 0i32..10 {
//!                 let _ = _tx.send(i).await;
//!                 sleep(Duration::from_millis(100)).await;
//!                 println!("sent {}", i);
//!             }
//!         });
//!     }
//!     drop(tx);
//!     let mut inv = tokio::time::interval(Duration::from_millis(500));
//!     loop {
//!         tokio::select! {
//!             _ = inv.tick() =>{
//!                 println!("tick");
//!             }
//!             r = rx.recv() => {
//!                 if let Ok(_i) = r {
//!                     println!("recv {}", _i);
//!                 } else {
//!                     println!("rx closed");
//!                     break;
//!                 }
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ### For Future customization
//!
//! The future object created by [AsyncTx::send()], [AsyncTx::send_timeout()], [AsyncRx::recv()],
//! [AsyncRx::recv_timeout()] is `Sized`. You don't need to put them in `Box`.
//!
//! If you like to use poll function directly for complex behavior, you can call
//! [AsyncSink::poll_send()](crate::sink::AsyncSink::poll_send()) or [AsyncStream::poll_item()](crate::stream::AsyncStream::poll_item()) with Context.

#[allow(private_bounds)]
pub mod flavor;
mod shared;
pub use shared::ChannelShared;

mod backoff;
pub use backoff::detect_backoff_cfg;

mod collections;
mod waker;
#[allow(private_bounds)]
mod waker_registry;

pub mod mpmc;
pub mod mpsc;
pub mod oneshot;
pub mod spsc;

mod blocking_tx;
pub use blocking_tx::*;
#[allow(private_bounds)]
mod blocking_rx;
pub use blocking_rx::*;
mod async_tx;
pub use async_tx::*;
#[allow(private_bounds)]
mod async_rx;
pub use async_rx::*;

#[cfg(feature = "compat")]
pub mod compat;
pub mod sink;
pub mod stream;

mod crossbeam;
pub use crossbeam::err::*;
#[allow(private_bounds)]
pub mod select;

/// logging macro for development
#[macro_export(local_inner_macros)]
macro_rules! trace_log {
    ($($arg:tt)+)=>{
        #[cfg(feature="trace_log")]
        {
            log::debug!($($arg)+);
        }
    };
}

/// logging macro for development under tokio
#[macro_export(local_inner_macros)]
macro_rules! tokio_task_id {
    () => {{
        #[cfg(all(feature = "trace_log", feature = "tokio"))]
        {
            tokio::task::try_id()
        }
        #[cfg(not(all(feature = "trace_log", feature = "tokio")))]
        {
            ""
        }
    }};
}

use flavor::Flavor;
use std::sync::Arc;

/// type limiter for channel builder
pub trait SenderType {
    type Flavor: Flavor;
    fn new(shared: Arc<ChannelShared<Self::Flavor>>) -> Self;
}

/// type limiter for channel builder
pub trait ReceiverType: AsRef<ChannelShared<Self::Flavor>> {
    type Flavor: Flavor;

    fn new(shared: Arc<ChannelShared<Self::Flavor>>) -> Self;
}

pub trait NotClonable {}
