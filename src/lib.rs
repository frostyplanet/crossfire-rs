#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

//! # Crossfire
//!
//! High-performance spsc/mpsc/mpmc channels.
//!
//! It supports async context, and bridge the gap between async and blocking context.
//!
//! Implemented with lockless in mind, low level is based on crossbeam-channel.
//! For the concept, please refer to [wiki](https://github.com/frostyplanet/crossfire-rs/wiki).
//!
//! ## Stability and versions
//!
//! Crossfire v1.0 has been released and used in production since 2022.12. Heavily tested on X86_64 and ARM.
//!
//! V2.0 has refactored the codebase and API at 2025.6. By removing generic types of ChannelShared object in sender and receiver,
//! it's easier to remember and code.
//!
//! V2.0.x branch will remain in maintenance mode. Future optimization might be in v2.x_beta
//! version until long run tests prove to be stable.
//!
//! ## Performance
//!
//! We focus on optimization of async logic, outperforming other async capability channel
//! implementations
//! (flume, tokio::mpsc, etc).
//!
//! Due to context switching between sleep and wake, there is a certain
//! overhead over crossbeam-channel.
//!
//! Benchmark is written in criterion framework. You can run benchmark by:
//!
//! ``` shell
//! cargo bench --bench crossfire
//! ```
//!
//! Some benchmark data is posted on [wiki](https://github.com/frostyplanet/crossfire-rs/wiki/benchmark-v2.0.14-2025%E2%80%9008%E2%80%9003).
//!
//! <img src="https://github.com/frostyplanet/crossfire-rs/wiki/images/benchmark-2.0.14-2025-08-03/mpsc_size_100_async.png" alt="mpsc bounded size 100 async context">
//!
//! ## APIs
//!
//! ### modules and functions
//!
//! There are 3 modules: [spsc], [mpsc], [mpmc], providing functions to allocate different types of channels.
//!
//! The SP or SC interface, only for non-concurrent operation, it's more memory efficient than MP or MC implementations, and sometimes slightly faster.
//!
//! The return types in these 3 modules are different:
//!
//! * [mpmc::bounded_blocking()]: tx blocking, rx blocking
//!
//! * [mpmc::bounded_async()]:  tx async, rx async
//!
//! * [mpmc::bounded_tx_async_rx_blocking()]: tx async, rx blocking
//!
//! * [mpmc::bounded_tx_blocking_rx_async()]: tx blocking, rx async
//!
//! * [mpmc::unbounded_blocking()]: tx non-blocking, rx blocking
//!
//! * [mpmc::unbounded_async()]: tx non-blocking, rx async
//!
//! > **NOTE** :  For bounded channel, 0 size case is not supported yet. (Temporary rewrite as 1 size).
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
//! > **NOTE**: For SP / SC version [AsyncTx], [AsyncRx], [Tx], [Rx], is not `Clone`, and without `Sync`,
//! Although can be moved to other thread, but not allowed to use send/recv while in Arc. (Refer to the compile_fail
//! examples in type document).
//!
//! ### Error types
//!
//! Error types are re-exported from crossbeam-channel:
//!
//! [TrySendError], [SendError], [SendTimeoutError], [TryRecvError], [RecvError], [RecvTimeoutError]
//!
//! ### Feature flags
//!
//! - `tokio`: Enable send_timeout, recv_timeout API for async context, based on `tokio`.
//!
//! - `async_std`: Enable send_timeout, recv_timeout API for async context, base on `async-std`.
//!
//! ### Async compatibility
//!
//! Tested on tokio-1.x and async-std-1.x, by default we do not depend on any async runtime.
//!
//! In async context, tokio-select! or future-select! can be used. Cancellation is supported. You can combine
//! recv() future with tokio::time::timeout.
//!
//! When feature "tokio" or "async_std" enable, we also provide two additional functions:
//!
//! - [send_timeout](crate::AsyncTx::send_timeout()), which will return the message failed to sent in
//! [SendTimeoutError].
//!
//! - [recv_timeout](crate::AsyncRx::recv_timeout())
//!
//! Unlike the possibility in Kanal race condition on future cancellation, in crossfire,
//! message will not be operated outside the context of poll() function, message only moved to, or from the queue in async context,
//! we can **guarantee** that no message will be lost due to the cancellation of recv(), and no spuriously send failure that message
//! actually copied to the receiver.
//!
//! While using MAsyncTx or MAsyncRx, there's memory overhead to pass along small size wakers
//! for pending async producer or consumer. Because we aim to be lockless,
//! when the sending/receiving futures are cancelled (like tokio::time::timeout()),
//! might trigger immediate cleanup if non-conflict conditions are met.
//! Otherwise will rely on lazy cleanup. (waker will be consumed by actual message send and recv).
//!
//! ## Usage
//!
//! Cargo.toml:
//! ```toml
//! [dependencies]
//! crossfire = "2.0"
//! ```
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

extern crate crossbeam;
extern crate futures;
#[macro_use]
extern crate enum_dispatch;

pub use crossbeam::channel::{RecvError, RecvTimeoutError, TryRecvError};
pub use crossbeam::channel::{SendError, SendTimeoutError, TrySendError};

mod channel;
pub use channel::ChannelShared;
mod collections;
mod locked_waker;
mod waker_registry;

pub mod mpmc;
pub mod mpsc;
pub mod spsc;

mod blocking_tx;
pub use blocking_tx::*;
mod blocking_rx;
pub use blocking_rx::*;
mod async_tx;
pub use async_tx::*;
mod async_rx;
pub use async_rx::*;
pub mod sink;
pub mod stream;

#[cfg(test)]
mod tests;
