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
// In `shared.rs`, `SelectHandle` is implemented for `ChannelShare<F>`
//
// ## SelectWaker
//
// `SelectWaker` is wrapped in an `Arc<SelectWaker>`, holding the actual waker
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
// ### Select::drop
// - Unregister using `cancel_waker()` for all handles.
//
// ## Safety and Validation
// - `SelectResult` is returned to the user and contains a pointer of receiver to the slot.
// - If the user incorrectly uses a `SelectResult` from one channel on a different receiver,
//   this pointer address is checked, causing a panic to ensure safety.

use crate::collections::WeakCell;
use crate::flavor::Token;
use crate::shared::{check_timeout, ChannelShared};
use crate::waker::*;
use crate::ReceiverType;
use crate::{RecvError, RecvTimeoutError, TryRecvError};
use std::cell::UnsafeCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem::transmute;
use std::ops::Add;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
};
use std::task::Context;
use std::thread;
use std::time::{Duration, Instant};

/// The select interface only support select from receivers.
///
/// - The user add receivers for subscription.
/// - call [Select::select] or [Select::select_timeout] and get [SelectResult]
/// - Handle [SelectResult] with corrasponding channel receiver.
/// - The `Select` object and be reused in a loop.
/// - On drop it will automatically cancel all registeration.
pub struct Select<'a> {
    handlers: Vec<RecvHandle<'a>>,
    waker: Arc<SelectWaker>,
    mode: SelectMode,
    next_index: usize,
    rng: u64,
}

#[derive(PartialEq)]
#[repr(u8)]
enum SelectMode {
    RR,
    Rand,
    Bias,
}

impl<'a> Select<'a> {
    /// Initialize Select with fair, round-robin stratergy
    pub fn new() -> Self {
        Self::_new(SelectMode::RR)
    }

    /// Initialize Select with fair stratergy (check start from random channel)
    #[inline]
    pub fn new_random() -> Self {
        Self::_new(SelectMode::Rand)
    }

    /// Initialize Select with bias stratergy (check according to the order of `add()`)
    #[inline]
    pub fn new_bias() -> Self {
        Self::_new(SelectMode::Bias)
    }

    #[inline]
    fn _new(mode: SelectMode) -> Self {
        let rng = if let SelectMode::Rand = mode {
            let mut hasher = DefaultHasher::new();
            Instant::now().hash(&mut hasher);
            thread::current().id().hash(&mut hasher);
            hasher.finish()
        } else {
            0
        };

        Self {
            mode,
            handlers: Vec::with_capacity(32),
            waker: Arc::new(SelectWaker {
                o_waker: UnsafeCell::new(None),
                cell: WeakCell::new(),
                hint: AtomicUsize::new(0),
            }),
            next_index: 0,
            rng,
        }
    }

    /// Add a channel receiver for watch
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

    /// Remove a channel receiver from watch
    pub fn remove<R: ReceiverType>(&mut self, recv: &R) {
        let channel = recv as *const R as *const u8;
        if let Some(index) = self.handlers.iter().position(|h| h.channel == channel) {
            self.handlers[index].shared.cancel_waker(&self.waker);
            self.handlers.remove(index);
            if !self.handlers.is_empty() {
                if self.next_index >= self.handlers.len() {
                    self.next_index = 0;
                }
                for handler in &mut self.handlers {
                    handler.registered = false;
                    handler.shared.cancel_waker(&self.waker);
                }
            }
        }
    }

    /// Attempts to select a message from any of the registered receivers without blocking.
    ///
    /// Returns:
    /// - `Ok(SelectResult)` if a message is immediately available from any channel.
    /// - `Err(TryRecvError::Empty)` if no messages are ready, but at least one channel is still connected.
    /// - `Err(TryRecvError::Disconnected)` if all registered channels are disconnected or removed from select.
    pub fn try_select(&mut self) -> Result<SelectResult, TryRecvError> {
        if self.handlers.is_empty() {
            return Err(TryRecvError::Disconnected);
        }
        if let Some(res) = self._try_select_begin(true) {
            return Ok(res);
        }
        Err(TryRecvError::Empty)
    }

    #[inline(always)]
    fn _try_select_begin(&mut self, final_check: bool) -> Option<SelectResult> {
        let len = self.handlers.len();
        debug_assert!(len > 0);
        let start_index = match self.mode {
            SelectMode::Bias => 0,
            SelectMode::RR => {
                if self.next_index >= self.handlers.len() {
                    0
                } else {
                    self.next_index
                }
            }
            SelectMode::Rand => {
                let mut x = self.rng;
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                self.rng = x;
                (x as usize) % len
            }
        };
        let mut idx = start_index;
        for _ in 0..len {
            if let Ok(res) = self.handlers[idx].try_select(final_check) {
                if SelectMode::RR == self.mode {
                    self.next_index = idx + 1;
                }
                return Some(res); // Message available
            }
            idx += 1;
            if idx >= len {
                idx = 0;
            }
        }
        None
    }

    /// Blocking current thread and wait for message from multiple receivers or close event
    ///
    /// See [crate::select] document for usage
    ///
    /// # Return conditions:
    ///
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

    /// Blocking current thread and wait with a timeout, for message from multiple receivers or close event
    ///
    /// See [crate::select] document for usage
    ///
    /// # Return conditions:
    ///
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
        // Initial non-blocking check, respecting SelectMode
        if self.handlers.is_empty() {
            return Err(true); // All handlers are disconnected or removed
        }
        if let Some(res) = self._try_select_begin(false) {
            return Ok(res);
        }
        let is_rr = self.mode == SelectMode::RR;
        // If try_select returned None, we check if all handlers are gone.
        let len = self.handlers.len();
        loop {
            // init SelectWaker
            self.waker.init_blocking();
            // Register all handlers (handlers with `registered=true` may be skipped).
            for (i, handler) in self.handlers.iter_mut().enumerate() {
                handler.reg_waker(i, &self.waker);
            }
            // After registration, do another check, this time with final_check=true
            for (i, handler) in self.handlers.iter().enumerate() {
                // final_check=true also check if any channel is closed.
                if let Ok(res) = handler.try_select(true) {
                    if is_rr {
                        self.next_index = i + 1;
                    }
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
            // wake up, first check the one with hint
            let mut idx = self.waker.hint.load(Ordering::Acquire);
            for _ in 0..len {
                // Ensure idx is within bounds for the current iteration.
                if idx >= len {
                    idx = 0;
                }
                // final_check=true also check if any channel is closed.
                if let Ok(res) = self.handlers[idx].try_select(true) {
                    if is_rr {
                        self.next_index = idx + 1;
                    }
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

#[allow(private_bounds)]
pub(crate) trait SelectHandle: Send {
    /// If final_check is true, should check channel closing, should use SeqCst ordering
    fn try_select(&self, final_check: bool) -> Option<Token>;

    /// For RegistryMulti return true means the waker will be persistent, otherwise return false
    fn reg_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool;

    fn cancel_waker(&self, waker: &Arc<SelectWaker>);
}
