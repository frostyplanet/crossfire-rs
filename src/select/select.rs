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

use super::SelectMode;
use crate::backoff::*;
use crate::flavor::Token;
use crate::shared::{check_timeout, ChannelShared};
use crate::trace_log;
use crate::waker::WakerState;
use crate::waker_registry::SelectWaker;
use crate::ReceiverType;
use crate::{RecvError, RecvTimeoutError, TryRecvError};
use smallvec::SmallVec;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Add;
use std::sync::{atomic::Ordering, Arc};
use std::thread;
use std::time::{Duration, Instant};

/// The select interface only support select from receivers.
///
/// - The user add receivers for subscription.
/// - call [Select::select] or [Select::select_timeout] and get [SelectResult]
/// - Use [read_select](crate::Rx::read_select) to handle [SelectResult]. (**Safety**: If `SelectResult`
///  dropped without processed, will result in message leak/hang.)
/// - Although the `Select` object has a lifecycle and should live inside a function scope, it can be reused in a loop.
/// - On drop it will automatically cancel all registration.
///
/// ## Example
///
/// ```rust
/// use crossfire::{mpmc, mpsc, RecvError};
/// use crossfire::select::Select;
///
/// let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
/// let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);
///
/// // Send some messages
/// tx1.send(100).unwrap();
/// tx2.send(200).unwrap();
///
/// // Drop senders to simulate disconnection after messages are sent
/// drop(tx1);
/// drop(tx2);
///
/// let mut select = Select::new();
/// select.add(&rx1);
/// select.add(&rx2);
///
/// // Loop until all channels are disconnected and removed from select
/// loop {
///     // When `select()` returns `Err(RecvError)`, it means all channels
///     // previously added to `select` have been disconnected or removed.
///     // In such a case, there's nothing left to select from, so we break.
///     let res = match select.select() {
///         Ok(res) => res,
///         Err(RecvError) => {
///             println!("All channels disconnected or removed from select. Breaking loop.");
///             break;
///         },
///     };
///
///     // Handle the result from the ready receiver
///     if res == rx1 {
///         match rx1.read_select(res) {
///             Ok(val) => println!("Received from rx1: {}", val),
///             Err(RecvError) => { // Now RecvError
///                 println!("rx1 disconnected, removing from select.");
///                 select.remove(&rx1); // Remove disconnected receiver
///             },
///         }
///     } else if res == rx2 {
///         match rx2.read_select(res) {
///             Ok(val) => println!("Received from rx2: {}", val),
///             Err(RecvError) => { // Now RecvError
///                 println!("rx2 disconnected, removing from select.");
///                 select.remove(&rx2); // Remove disconnected receiver
///             },
///         }
///     }
/// }
/// ```
pub struct Select<'a> {
    handlers: SmallVec<[RecvHandle<'a>; 32]>,
    waker: Arc<SelectWaker>,
    mode: SelectMode,
    next_index: usize,
    rng: u64,
}

impl<'a> Select<'a> {
    /// Initialize Select with fair, round-robin strategy
    pub fn new() -> Self {
        Self::new_with(SelectMode::RR)
    }

    /// Initialize Select with fair strategy (check start from random channel)
    #[inline]
    pub fn new_random() -> Self {
        Self::new_with(SelectMode::Rand)
    }

    /// Initialize Select with bias strategy (check according to the order of `add()`)
    #[inline]
    pub fn new_bias() -> Self {
        Self::new_with(SelectMode::Bias)
    }

    #[inline]
    pub fn new_with(mode: SelectMode) -> Self {
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
            handlers: SmallVec::new(),
            waker: Arc::new(SelectWaker::new()),
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
        let idx = self._try_select_begin();
        if let Some(res) = self._try_select(idx, true) {
            return Ok(res);
        }
        Err(TryRecvError::Empty)
    }

    #[inline(always)]
    fn _try_select(&mut self, mut idx: usize, final_check: bool) -> Option<SelectResult> {
        let len = self.handlers.len();
        debug_assert!(len > 0);
        for _ in 0..len {
            // Ensure idx is within bounds for the current iteration.
            if idx >= len {
                idx = 0;
            }
            // final_check=true also check if any channel is closed.
            if let Ok(res) = self.handlers[idx].try_select(final_check) {
                trace_log!("select ok idx={}", idx);
                if self.mode == SelectMode::RR {
                    self.next_index = idx + 1;
                }
                return Some(res);
            } else {
                if final_check {
                    trace_log!("select: final_check {}", idx);
                }
            }
            idx += 1;
        }
        None
    }

    #[inline(always)]
    fn _try_select_begin(&mut self) -> usize {
        return match self.mode {
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
                (x as usize) % self.handlers.len()
            }
        };
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
        let mut idx = self._try_select_begin();
        if let Some(res) = self._try_select(idx, false) {
            return Ok(res);
        }
        let mut backoff = Backoff::from(BackoffConfig::detect());
        backoff.snooze();
        // If try_select returned None, we check if all handlers are gone.
        loop {
            loop {
                if let Some(res) = self._try_select(idx, false) {
                    return Ok(res);
                }
                if backoff.snooze() {
                    break;
                }
            }
            // init SelectWaker
            self.waker.init_blocking();
            // Register all handlers (handlers with `registered=true` may be skipped).
            for (i, handler) in self.handlers.iter_mut().enumerate() {
                handler.reg_waker(i, &self.waker);
            }
            // After registration, do another check, this time with final_check=true
            if let Some(res) = self._try_select(idx, true) {
                return Ok(res);
            }
            trace_log!("select: park");
            let mut state = WakerState::Init as u8;
            while state < WakerState::Woken as u8 {
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
                state = self.waker.get_waker_state(Ordering::SeqCst);
                trace_log!("select: unpark state={}", state);
            }
            // NOTE: there may be spurious wakeup, but since the SelectWaker is registered in
            // wake up, first check the one with hint
            idx = self.waker.get_hint();
            trace_log!("select: hint idx {}", idx);
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

impl<'a> std::fmt::Debug for Select<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Select")
    }
}

struct RecvHandle<'a> {
    shared: &'a dyn SelectHandle,
    // If multi is true, the registration is persistent until cancel
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
            trace_log!("select: reg waker");
            self.registered = true;
        }
    }
}

/// The result from [Select::select], use for calling `read_select()` on the receiver type, may contains event to receive or disconnected event
///
/// **Safety**: If `SelectResult` dropped without processed, will result in message leak/hang.
///
/// See the example of select interface.
pub struct SelectResult {
    // for validation
    pub(crate) channel: *const u8,
    pub(crate) token: Token,
}

impl fmt::Debug for SelectResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SelectResult(from {:p})", self.channel)
    }
}

impl SelectResult {
    /// Check if the result is for specified receiver
    #[inline]
    pub fn is_from<R: ReceiverType>(&self, rx: &R) -> bool {
        self.channel == rx as *const R as *const u8
    }
}

impl<R: ReceiverType> PartialEq<R> for SelectResult {
    /// Short cut for [SelectResult::is_from()]
    #[inline]
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
