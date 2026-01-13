//! A null flavor type that use to notify thread/future to close
//!
//! It's common practice we use `()` channel in async code, not intended for any message, just
//! subscribe for close event. (For example, cancelling socket operations, stopping worker loops...)
//! This is a module designed for that, with minimized polling cost.
//!
//! You can initialize a null channel with [crate::mpsc::Null::new_async()] or
//! [crate::mpmc::Null::new_async()], which return a [CloseHandle], (which can only be `clone` or `drop`,
//! but unable to send any message), and a normal receiver type (which recv method is always
//! blocked until the all copy of `CloseHandle` is dropped).
//!
//! > NOTE: using mpsc version has less cost then mpmc version.
//!
//! # Examples
//!
//! Use null channel to stop a background loop.
//!
//! ```rust
//! use crossfire::{null::CloseHandle, *};
//! use std::time::Duration;
//!
//! # #[tokio::main]
//! # async fn main() {
//! // Create a null channel
//! let (stop_tx, stop_rx): (CloseHandle<mpmc::Null>, MAsyncRx<mpmc::Null>)  = mpmc::Null::new().new_async();
//! let (data_tx, data_rx): (MAsyncTx<mpmc::Array<i32>>, MAsyncRx<mpmc::Array<i32>>) = mpmc::bounded_async::<i32>(10);
//!
//! // Spawn a background task
//! let task = tokio::spawn(async move {
//!     loop {
//!         tokio::select! {
//!             // If the null channel is closed (stop_tx dropped), this branch will be selected
//!             res = stop_rx.recv() => {
//!                 if res.is_err() {
//!                     println!("Stopping task");
//!                     break;
//!                 }
//!             }
//!             res = data_rx.recv() => {
//!                 match res {
//!                     Ok(data) => println!("Received data: {}", data),
//!                     Err(_) => break,
//!                 }
//!             }
//!         }
//!     }
//! });
//!
//! data_tx.send(1).await.unwrap();
//! tokio::time::sleep(Duration::from_millis(10)).await;
//!
//! // Drop the stop handle to signal the task to stop
//! drop(stop_tx);
//!
//! task.await.unwrap();
//! # }
//! ```

use crate::flavor::Flavor;
use crate::flavor::{FlavorImpl, FlavorNew, FlavorSelect, Queue, Token};
use crate::shared::ChannelShared;
use crate::SenderType;
use core::mem::MaybeUninit;
use std::sync::Arc;

/// an flavor type can never receive any message
pub struct Null();

impl Queue for Null {
    type Item = ();

    #[inline(always)]
    fn pop(&self) -> Option<()> {
        None
    }

    #[inline(always)]
    fn push(&self, _item: ()) -> Result<(), ()> {
        unreachable!();
    }

    #[inline(always)]
    fn len(&self) -> usize {
        0
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        None
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        true
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        true
    }
}

impl FlavorImpl for Null {
    #[inline(always)]
    fn try_send(&self, _item: &MaybeUninit<()>) -> bool {
        // work as an /dev/null, although normally init with CloseHandle which don't have send() method
        true
    }

    #[inline(always)]
    fn try_send_oneshot(&self, _item: *const ()) -> Option<bool> {
        Some(true)
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<Self::Item> {
        // always empty
        None
    }

    #[inline(always)]
    fn try_recv_final(&self) -> Option<Self::Item> {
        None
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        0
    }
}

impl FlavorNew for Null {
    #[inline]
    fn new() -> Self {
        Self()
    }
}

impl FlavorSelect for Null {
    #[inline(always)]
    fn try_select(&self, _final_check: bool) -> Option<Token> {
        None
    }

    #[inline(always)]
    fn read_with_token(&self, _token: Token) -> () {
        unreachable!();
    }
}

/// The CloseHandle is a special type for flavor [Null], only impl `Clone` and `Drop`
pub struct CloseHandle<F: Flavor>(Arc<ChannelShared<F>>);

impl<F: Flavor> Clone for CloseHandle<F> {
    #[inline(always)]
    fn clone(&self) -> Self {
        self.0.add_tx();
        Self(self.0.clone())
    }
}

impl<F: Flavor> Drop for CloseHandle<F> {
    #[inline(always)]
    fn drop(&mut self) {
        self.0.close_tx();
    }
}

impl<F: Flavor> SenderType for CloseHandle<F>
where
    F: Flavor<Item = ()>,
{
    type Flavor = F;

    #[inline(always)]
    fn new(shared: Arc<ChannelShared<Self::Flavor>>) -> Self {
        CloseHandle(shared)
    }
}
