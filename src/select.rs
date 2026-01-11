//! # Select
//!
//! This module provides a `Select` struct that allows selecting from multiple receivers.
//! It supports both `mpmc`, `mpsc`, and `spsc` channels.
//!
//! ## Example
//!
//! ```rust
//! use crossfire::{mpmc, mpsc, RecvError};
//! use crossfire::select::Select;
//!
//! let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
//! let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);
//!
//! // Send some messages
//! tx1.send(100).unwrap();
//! tx2.send(200).unwrap();
//!
//! // Drop senders to simulate disconnection after messages are sent
//! drop(tx1);
//! drop(tx2);
//!
//! let mut select = Select::new();
//! select.add(&rx1);
//! select.add(&rx2);
//!
//! // Loop until all channels are disconnected and removed from select
//! loop {
//!     // When `select()` returns `Err(RecvError)`, it means all channels
//!     // previously added to `select` have been disconnected or removed.
//!     // In such a case, there's nothing left to select from, so we break.
//!     let res = match select.select() {
//!         Ok(res) => res,
//!         Err(RecvError) => {
//!             println!("All channels disconnected or removed from select. Breaking loop.");
//!             break;
//!         },
//!     };
//!
//!     // Handle the result from the ready receiver
//!     if res == rx1 {
//!         match rx1.read_select(res) {
//!             Ok(val) => println!("Received from rx1: {}", val),
//!             Err(RecvError) => { // Now RecvError
//!                 println!("rx1 disconnected, removing from select.");
//!                 select.remove(&rx1); // Remove disconnected receiver
//!             },
//!         }
//!     } else if res == rx2 {
//!         match rx2.read_select(res) {
//!             Ok(val) => println!("Received from rx2: {}", val),
//!             Err(RecvError) => { // Now RecvError
//!                 println!("rx2 disconnected, removing from select.");
//!                 select.remove(&rx2); // Remove disconnected receiver
//!             },
//!         }
//!     }
//! }
//! ```
// Internal Implementation Details:
//
// Since mixing send and receive operations is rare, and the waker types for senders and receivers
// are different, we only implement `select` for receive operations.
//
// In `shared.rs`, `SelectHandle` is implemented for `ChannelShare<F::Recv=RegistryMultiRecv>`
// and `ChannelShare<F::Recv=RegistrySingle>`.
//
// ## SelectWaker
//
// `SelectWaker` is wrapped in an `Arc<SelectWaker>`.
//
// ### RegistryMultiRecv
// - Requires `reg_waker()` to be called only once, so the `registered` flag is saved as `true`.
// - Provides `cancel_waker()`.
// - `RegistryMultiInner` maintains a `Vec<(channel_id, Arc<SelectWaker>)>`.
//   It does not remove the waker after waking it up.
// - When waking up `SelectWaker`, it saves its own `channel_id` into the `SelectWaker`'s hint.
// - The `is_empty` flag in `RegistryMulti` can be extended from `bool` to `u8` to represent three states:
//   `empty`, `has select`, and `without select`.
//
// ### RegistrySingle
// - Needs to re-register in every select loop, so `RecvHandle` saves `registered` as `false`.
// - `cancel_waker` is an empty implementation.
// - During registration, it clones the `ArcWaker` (generated at the start of the select flow inside `Arc<SelectWaker>`)
//   into `RegistrySingle`. A new method can be added to abstract this process.
//
// ## Select Flow
//
// ### Select::select loop
// 1. `try_select` from all handlers (no need to check for closed channels yet).
// 2. Initialize an `ArcWaker` in `SelectWaker`.
// 3. Register all handlers (handlers with `registered=true` may be skipped).
// 4. Check `try_select` again to handle race conditions and check if any channel is closed.
// 5. Park on `SelectWaker`.
//
// ### Select::drop
// - Unregister using `cancel_waker()` for all handles.
//
// ## Hint and Indexing
// - When no channel is removed, `channel_id` equals the `hint`, which is used to fast-path the check
//   after waking up from park.
// - The `hint` does not need to be strictly accurate because in an MPMC environment, different selects
//   on multiple channels might contend.
// - If a `RecvHandle` is removed, the `channel_id` of subsequent handlers needs to be updated to
//   correspond to their index in the vector.
//
// ## Safety and Validation
// - `channel`: `*const u8` is used to validate the `SelectResult`.
// - `SelectResult` is returned to the user and contains a pointer to the slot.
// - If the user incorrectly uses a `SelectResult` from one channel on a different receiver,
//   this pointer address is checked, causing a panic to ensure safety.

use crate::collections::WeakCell;
use crate::flavor::Token;
use crate::shared::{check_timeout, ChannelShared};
use crate::waker::*;
use crate::ReceiverType;
use crate::{RecvError, RecvTimeoutError};
use std::cell::UnsafeCell;
use std::mem::transmute;
use std::ops::Add;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
};
use std::task::Context;
use std::time::{Duration, Instant};

#[allow(private_bounds)]
pub(crate) trait SelectHandle: Send {
    /// If final_check is true, should check channel closing, should use SeqCst ordering
    fn try_select(&self, final_check: bool) -> Option<Token>;

    /// For RegistryMulti return true means the waker will be persistent, otherwise return false
    fn reg_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool;

    fn cancel_waker(&self, waker: &Arc<SelectWaker>);
}

/// The select interface only support select from receivers
pub struct Select<'a> {
    handlers: Vec<RecvHandle<'a>>,
    waker: Arc<SelectWaker>,
}

impl<'a> Select<'a> {
    #[inline]
    pub fn new() -> Self {
        Self {
            handlers: Vec::with_capacity(32),
            waker: Arc::new(SelectWaker {
                o_waker: UnsafeCell::new(None),
                cell: WeakCell::new(),
                hint: AtomicUsize::new(0),
            }),
        }
    }

    #[inline]
    pub fn add<R: ReceiverType>(&mut self, recv: &'a R)
    where
        ChannelShared<R::Flavor>: SelectHandle,
    {
        let shared: &ChannelShared<R::Flavor> = recv.as_ref();
        self.handlers.push(RecvHandle {
            registered: false,
            shared: shared as &dyn SelectHandle,
            channel: recv as *const R as *const u8,
        });
    }

    pub fn remove<R: ReceiverType>(&mut self, recv: &R) {
        let channel = recv as *const R as *const u8;
        if let Some(index) = self.handlers.iter().position(|h| h.channel == channel) {
            self.handlers[index].shared.cancel_waker(&self.waker);
            self.handlers.remove(index);

            for handler in &mut self.handlers {
                handler.registered = false;
                handler.shared.cancel_waker(&self.waker);
            }
        }
    }

    /// - Return Ok(SelectResult) when one of the channel has result or close.
    /// - For closed channel, you have to remove the receiver from select, otherwise the select
    /// will already return immediately.
    /// - If there's no handler left in it, will return RecvError
    pub fn select(&mut self) -> Result<SelectResult, RecvError> {
        match self._select_blocking(None) {
            Ok(res) => Ok(res),
            Err(true) => Err(RecvError),
            _ => unreachable!(),
        }
    }

    /// - Return Ok(SelectResult) when one of the channel has result or close.
    /// - For closed channel, you have to remove the receiver from select, otherwise the select
    /// will already return immediately.
    /// - For Timeout returns RecvTimeoutError::Timeout;
    /// - If there's no handler left in it, will return RecvTimeoutError::Disconnected.
    pub fn select_timeout(&mut self, timeout: Duration) -> Result<SelectResult, RecvTimeoutError> {
        let deadline = Instant::now().add(timeout);
        match self._select_blocking(Some(deadline)) {
            Ok(res) => Ok(res),
            Err(true) => Err(RecvTimeoutError::Disconnected),
            Err(false) => Err(RecvTimeoutError::Timeout),
        }
    }

    #[inline(always)]
    fn _select_blocking(&mut self, deadline: Option<Instant>) -> Result<SelectResult, bool> {
        for handler in self.handlers.iter() {
            if let Ok(res) = handler.try_select(false) {
                return Ok(res);
            }
        }
        if self.handlers.is_empty() {
            return Err(true);
        }
        loop {
            // init SelectWaker
            self.waker.init_blocking();
            for (i, handler) in self.handlers.iter_mut().enumerate() {
                handler.reg_waker(i, &self.waker);
            }
            for handler in self.handlers.iter() {
                if let Ok(res) = handler.try_select(true) {
                    return Ok(res);
                }
            }
            match check_timeout(deadline) {
                Ok(None) => {
                    std::thread::park();
                }
                Ok(Some(dur)) => {
                    std::thread::park_timeout(dur);
                }
                Err(_) => {
                    return Err(false);
                }
            }
            let mut idx = self.waker.hint.load(Ordering::Acquire);
            for _ in 0..self.handlers.len() {
                // Ensure idx is within bounds for the current iteration.
                if idx >= self.handlers.len() {
                    idx = 0;
                }
                if let Ok(res) = self.handlers[idx].try_select(true) {
                    return Ok(res);
                }
                idx += 1;
            }
        }
    }
}

impl<'a> Drop for Select<'a> {
    #[inline(always)]
    fn drop(&mut self) {
        for handler in &self.handlers {
            handler.shared.cancel_waker(&self.waker);
        }
    }
}

struct RecvHandle<'a> {
    shared: &'a dyn SelectHandle,
    // If multi is true, the registeration is persistent until cancel
    registered: bool,
    // for validate against unsafe usage
    channel: *const u8,
}

impl<'a> RecvHandle<'a> {
    #[inline(always)]
    fn try_select(&self, final_check: bool) -> Result<SelectResult, ()> {
        if let Some(token) = self.shared.try_select(final_check) {
            return Ok(SelectResult { channel: self.channel, token });
        }
        Err(())
    }

    #[inline(always)]
    fn reg_waker(&mut self, index: usize, global_waker: &Arc<SelectWaker>) {
        if self.registered {
            return;
        }
        if self.shared.reg_waker(index, global_waker) {
            self.registered = true;
        }
    }
}

pub(crate) struct SelectWakerMulti(Arc<SelectWaker>, usize);

impl SelectWakerMulti {
    #[inline(always)]
    pub(crate) fn wake(&self) {
        if let Some(waker) = self.0.cell.pop() {
            self.0.hint.store(self.1, Ordering::Relaxed);
            waker.wake();
        }
    }

    #[inline(always)]
    pub(crate) fn eq(&self, waker: &Arc<SelectWaker>) -> bool {
        Arc::ptr_eq(&self.0, waker)
    }
}

pub(crate) struct SelectWaker {
    cell: WeakCell<WakerInner<()>>,
    // does not need to be corrent, just a hint for the try_select
    hint: AtomicUsize,
    o_waker: UnsafeCell<Option<ArcWaker<()>>>,
}
unsafe impl Send for SelectWaker {}
unsafe impl Sync for SelectWaker {}

impl SelectWaker {
    #[inline(always)]
    fn init_blocking(&self) {
        let waker = ArcWaker::new_blocking(());
        let weak = waker.weak();
        self.get_waker().replace(waker);
        self.cell.replace(weak);
        self.hint.store(0, Ordering::Release)
    }

    #[inline(always)]
    fn init_async(&self, ctx: &mut Context) {
        let waker = ArcWaker::new_async(ctx, ());
        let weak = waker.weak();
        self.get_waker().replace(waker);
        self.cell.replace(weak);
        self.hint.store(0, Ordering::Release)
    }

    #[inline(always)]
    fn get_waker(&self) -> &mut Option<ArcWaker<()>> {
        unsafe { transmute(self.o_waker.get()) }
    }

    #[inline(always)]
    pub(crate) fn clone_weak(&self) -> Weak<WakerInner<()>> {
        self.get_waker().as_ref().unwrap().weak()
    }

    #[inline(always)]
    pub(crate) fn to_multi_waker(self: Arc<SelectWaker>, channel_id: usize) -> SelectWakerMulti {
        SelectWakerMulti(self, channel_id)
    }
}

pub struct SelectResult {
    // for validation
    pub(crate) channel: *const u8,
    pub(crate) token: Token,
}

impl SelectResult {
    pub fn is_from<R: ReceiverType>(&self, rx: &R) -> bool {
        self.channel == rx as *const R as *const u8
    }
}

impl<R: ReceiverType> PartialEq<R> for SelectResult {
    fn eq(&self, other: &R) -> bool {
        self.is_from(other)
    }
}
