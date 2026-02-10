use crate::backoff::*;
use crate::flavor::FlavorMP;
use crate::{shared::*, trace_log, AsyncTx, MAsyncTx, NotCloneable, SenderType};
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A single producer (sender) that works in a blocking context.
///
/// Additional methods in [ChannelShared] can be accessed through `Deref`.
///
/// **NOTE**: `Tx` is not `Clone` or `Sync`.
/// If you need concurrent access, use [MTx] instead.
///
/// `Tx` has a `Send` marker and can be moved to other threads.
/// The following code is OK:
///
/// ``` rust
/// use crossfire::*;
/// let (tx, rx) = spsc::bounded_blocking::<usize>(100);
/// std::thread::spawn(move || {
///     let _ = tx.send(1);
/// });
/// drop(rx);
/// ```
///
/// Because `Tx` does not have a `Sync` marker, using `Arc<Tx>` will lose the `Send` marker.
///
/// For your safety, the following code **should not compile**:
///
/// ``` compile_fail
/// use crossfire::*;
/// use std::sync::Arc;
/// let (tx, rx) = spsc::bounded_blocking::<usize>(100);
/// let tx = Arc::new(tx);
/// std::thread::spawn(move || {
///     let _ = tx.send(1);
/// });
/// drop(rx);
/// ```
pub struct Tx<F: Flavor> {
    pub(crate) shared: Arc<ChannelShared<F>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    waker_cache: WakerCache<*const F::Item>,
}

unsafe impl<F: Flavor> Send for Tx<F> {}

impl<F: Flavor> fmt::Debug for Tx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tx{:p}", self)
    }
}

impl<F: Flavor> fmt::Display for Tx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Tx{:p}", self)
    }
}

impl<F: Flavor> Drop for Tx<F> {
    #[inline(always)]
    fn drop(&mut self) {
        self.shared.close_tx();
    }
}

impl<F: Flavor> From<AsyncTx<F>> for Tx<F> {
    fn from(value: AsyncTx<F>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

impl<F: Flavor> Tx<F> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self { shared, waker_cache: WakerCache::new(), _phan: Default::default() }
    }

    /// Return true if the other side has closed
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_rx_closed()
    }

    #[inline]
    pub fn into_async(self) -> AsyncTx<F> {
        self.into()
    }
}

impl<F: Flavor> Tx<F>
where
    F::Item: Send + 'static,
{
    #[inline(always)]
    pub(crate) fn _send_bounded(
        &self, item: &MaybeUninit<F::Item>, deadline: Option<Instant>,
    ) -> Result<(), SendTimeoutError<F::Item>> {
        let shared = &self.shared;
        let large = shared.large;
        let backoff_cfg = BackoffConfig::detect().spin(2).limit(shared.backoff_limit);
        let mut backoff = Backoff::from(backoff_cfg);
        let congest = shared.sender_direct_copy();
        // disable because of issue #54
        let direct_copy = false;
        //        let direct_copy = deadline.is_none() && shared.sender_direct_copy();
        if large {
            backoff.set_step(2);
        }
        loop {
            let r = if large { backoff.yield_now() } else { backoff.spin() };
            if direct_copy && large {
                match shared.inner.try_send_oneshot(item.as_ptr()) {
                    Some(false) => break,
                    None => {
                        if r {
                            break;
                        }
                        continue;
                    }
                    _ => {
                        shared.on_send();
                        trace_log!("tx: send");
                        std::thread::yield_now();
                        return Ok(());
                    }
                }
            } else {
                if !shared.inner.try_send(item) {
                    if r {
                        break;
                    }
                    continue;
                }
                shared.on_send();
                trace_log!("tx: send");
                return Ok(());
            }
        }
        let direct_copy_ptr: *const F::Item = std::ptr::null();
        //            if direct_copy { item.as_ptr() } else { std::ptr::null() };

        let mut state: u8;
        let mut o_waker: Option<<F::Send as Registry>::Waker> = None;
        macro_rules! return_ok {
            () => {
                trace_log!("tx: send {:?}", o_waker);
                if shared.is_full() {
                    // It's for 8x1, 16x1.
                    std::thread::yield_now();
                    self.senders.cache_waker(o_waker, &self.waker_cache);
                }
                return Ok(())
            };
        }
        loop {
            self.senders.reg_waker_blocking(&mut o_waker, &self.waker_cache, direct_copy_ptr);
            // For nx1 (more likely congest), need to reset backoff
            // to allow more yield to receivers.
            // For nxn (the backoff is already complete), wait a little bit.
            state = shared.sender_double_check::<false>(item, &mut o_waker);
            trace_log!("tx: sender_double_check {:?} state={}", o_waker, state);
            while state < WakerState::Woken as u8 {
                if congest {
                    state = shared.sender_snooze(&o_waker, &mut backoff);
                }
                if state <= WakerState::Waiting as u8 {
                    match check_timeout(deadline) {
                        Ok(None) => {
                            std::thread::park();
                        }
                        Ok(Some(dur)) => {
                            std::thread::park_timeout(dur);
                        }
                        Err(_) => {
                            if shared.abandon_send_waker(o_waker.as_ref().unwrap()) {
                                return Err(SendTimeoutError::Timeout(unsafe {
                                    item.assume_init_read()
                                }));
                            } else {
                                // NOTE: Unlikely since we disable direct copy with deadline
                                // state is WakerState::Done
                                return Ok(());
                            }
                        }
                    }
                    state = self.senders.get_waker_state(&o_waker, Ordering::SeqCst);
                    trace_log!("tx: after park state={}", state);
                }
            }
            if state == WakerState::Woken as u8 {
                backoff.reset();
                loop {
                    if shared.inner.try_send(item) {
                        shared.on_send();
                        return_ok!();
                    }
                    if backoff.is_completed() {
                        break;
                    }
                    backoff.snooze();
                }
            } else if state == WakerState::Done as u8 {
                return_ok!();
            } else {
                debug_assert_eq!(state, WakerState::Closed as u8);
                return Err(SendTimeoutError::Disconnected(unsafe { item.assume_init_read() }));
            }
        }
    }

    /// Sends a message. This method will block until the message is sent or the channel is closed.
    ///
    /// Returns `Ok(())` on success.
    ///
    /// Returns `Err(SendError)` if the receiver has been dropped.
    ///
    #[inline]
    pub fn send(&self, item: F::Item) -> Result<(), SendError<F::Item>> {
        let shared = &self.shared;
        if shared.is_rx_closed() {
            return Err(SendError(item));
        }
        let _item = MaybeUninit::new(item);
        if shared.inner.try_send(&_item) {
            shared.on_send();
            return Ok(());
        }
        match self._send_bounded(&_item, None) {
            Ok(_) => Ok(()),
            Err(SendTimeoutError::Disconnected(e)) => Err(SendError(e)),
            Err(SendTimeoutError::Timeout(_)) => unreachable!(),
        }
    }

    /// Attempts to send a message without blocking.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([TrySendError::Full]) if the channel is full.
    ///
    /// Returns Err([TrySendError::Disconnected]) if the receiver has been dropped.
    #[inline]
    pub fn try_send(&self, item: F::Item) -> Result<(), TrySendError<F::Item>> {
        let shared = &self.shared;
        if shared.is_rx_closed() {
            return Err(TrySendError::Disconnected(item));
        }
        let _item = MaybeUninit::new(item);
        if shared.inner.try_send(&_item) {
            shared.on_send();
            Ok(())
        } else {
            Err(TrySendError::Full(unsafe { _item.assume_init_read() }))
        }
    }

    /// Sends a message with a timeout.
    /// Will block when channel is full.
    ///
    /// The behavior is atomic: the message is either sent successfully or returned on error.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) if the operation timed out.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) if the receiver has been dropped.
    #[inline]
    pub fn send_timeout(
        &self, item: F::Item, timeout: Duration,
    ) -> Result<(), SendTimeoutError<F::Item>> {
        let shared = &self.shared;
        if shared.is_rx_closed() {
            return Err(SendTimeoutError::Disconnected(item));
        }
        match Instant::now().checked_add(timeout) {
            None => self.try_send(item).map_err(|e| match e {
                TrySendError::Disconnected(t) => SendTimeoutError::Disconnected(t),
                TrySendError::Full(t) => SendTimeoutError::Timeout(t),
            }),
            Some(deadline) => {
                let _item = MaybeUninit::new(item);
                if shared.inner.try_send(&_item) {
                    shared.on_send();
                    return Ok(());
                }
                match self._send_bounded(&_item, Some(deadline)) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
        }
    }
}

/// A multi-producer (sender) that works in a blocking context.
///
/// Inherits from [`Tx<F>`] and implements `Clone`.
/// Additional methods can be accessed through `Deref<Target=[ChannelShared]>`.
///
/// You can use `into()` to convert it to `Tx<F>`.
pub struct MTx<F: Flavor>(pub(crate) Tx<F>);

impl<F: Flavor> fmt::Debug for MTx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MTx{:p}", self)
    }
}

impl<F: Flavor> fmt::Display for MTx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MTx{:p}", self)
    }
}

impl<F: Flavor> From<MTx<F>> for Tx<F> {
    fn from(tx: MTx<F>) -> Self {
        tx.0
    }
}

impl<F: Flavor> From<MAsyncTx<F>> for MTx<F> {
    fn from(value: MAsyncTx<F>) -> Self {
        value.add_tx();
        Self(Tx::new(value.shared.clone()))
    }
}

unsafe impl<F: Flavor> Sync for MTx<F> {}

impl<F: Flavor + FlavorMP> MTx<F> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self(Tx::new(shared))
    }

    #[inline]
    pub fn into_async(self) -> MAsyncTx<F> {
        self.into()
    }
}

impl<F: Flavor> Clone for MTx<F> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_tx();
        Self(Tx::new(inner.shared.clone()))
    }
}

impl<F: Flavor> Deref for MTx<F> {
    type Target = Tx<F>;

    /// Inherits all the functions of [Tx].
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// For writing generic code with MTx & Tx
pub trait BlockingTxTrait<T: Send + 'static>: Send + 'static + fmt::Debug + fmt::Display {
    /// Sends a message. This method will block until the message is sent or the channel is closed.
    ///
    /// Returns `Ok(())` on success.
    ///
    /// Returns Err([SendError]) if the receiver has been dropped.
    fn send(&self, _item: T) -> Result<(), SendError<T>>;

    /// Attempts to send a message without blocking.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns `Err([TrySendError::Full])` if the channel is full.
    ///
    /// Returns Err([TrySendError::Disconnected]) if the receiver has been dropped.
    fn try_send(&self, _item: T) -> Result<(), TrySendError<T>>;

    /// Sends a message with a timeout.
    /// Will block when channel is empty.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) if the message could not be sent because the channel is full and the operation timed out.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) if the receiver has been dropped.
    fn send_timeout(&self, item: T, timeout: Duration) -> Result<(), SendTimeoutError<T>>;

    /// The number of messages in the channel at the moment
    fn len(&self) -> usize;

    /// The capacity of the channel, return None for unbounded channel.
    fn capacity(&self) -> Option<usize>;

    /// Whether channel is empty at the moment
    fn is_empty(&self) -> bool;

    /// Whether the channel is full at the moment
    fn is_full(&self) -> bool;

    /// Return true if the other side has closed
    fn is_disconnected(&self) -> bool;

    /// Return the number of senders
    fn get_tx_count(&self) -> usize;

    /// Return the number of receivers
    fn get_rx_count(&self) -> usize;

    fn clone_to_vec(self, count: usize) -> Vec<Self>
    where
        Self: Sized;

    fn get_wakers_count(&self) -> (usize, usize);
}

impl<F: Flavor> BlockingTxTrait<F::Item> for Tx<F>
where
    F::Item: Send + 'static,
{
    #[inline(always)]
    fn clone_to_vec(self, _count: usize) -> Vec<Self> {
        assert_eq!(_count, 1);
        vec![self]
    }

    #[inline(always)]
    fn send(&self, item: F::Item) -> Result<(), SendError<F::Item>> {
        Tx::send(self, item)
    }

    #[inline(always)]
    fn try_send(&self, item: F::Item) -> Result<(), TrySendError<F::Item>> {
        Tx::try_send(self, item)
    }

    #[inline(always)]
    fn send_timeout(
        &self, item: F::Item, timeout: Duration,
    ) -> Result<(), SendTimeoutError<F::Item>> {
        Tx::send_timeout(self, item, timeout)
    }

    /// The number of messages in the channel at the moment
    #[inline(always)]
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// The capacity of the channel, return None for unbounded channel.
    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        self.as_ref().capacity()
    }

    /// Whether channel is empty at the moment
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    /// Whether the channel is full at the moment
    #[inline(always)]
    fn is_full(&self) -> bool {
        self.as_ref().is_full()
    }

    /// Return true if the other side has closed
    #[inline(always)]
    fn is_disconnected(&self) -> bool {
        self.as_ref().get_rx_count() == 0
    }

    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.as_ref().get_tx_count()
    }

    #[inline(always)]
    fn get_rx_count(&self) -> usize {
        self.as_ref().get_rx_count()
    }

    fn get_wakers_count(&self) -> (usize, usize) {
        self.as_ref().get_wakers_count()
    }
}

impl<F: Flavor + FlavorMP> BlockingTxTrait<F::Item> for MTx<F>
where
    F::Item: Send + 'static,
{
    #[inline(always)]
    fn clone_to_vec(self, count: usize) -> Vec<Self> {
        let mut v = Vec::with_capacity(count);
        for _ in 0..count - 1 {
            v.push(self.clone());
        }
        v.push(self);
        v
    }

    #[inline(always)]
    fn send(&self, item: F::Item) -> Result<(), SendError<F::Item>> {
        self.0.send(item)
    }

    #[inline(always)]
    fn try_send(&self, item: F::Item) -> Result<(), TrySendError<F::Item>> {
        self.0.try_send(item)
    }

    #[inline(always)]
    fn send_timeout(
        &self, item: F::Item, timeout: Duration,
    ) -> Result<(), SendTimeoutError<F::Item>> {
        self.0.send_timeout(item, timeout)
    }

    /// The number of messages in the channel at the moment
    #[inline(always)]
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// The capacity of the channel, return None for unbounded channel.
    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        self.as_ref().capacity()
    }

    /// Whether channel is empty at the moment
    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    /// Whether the channel is full at the moment
    #[inline(always)]
    fn is_full(&self) -> bool {
        self.as_ref().is_full()
    }

    /// Return true if the other side has closed
    #[inline(always)]
    fn is_disconnected(&self) -> bool {
        self.as_ref().get_rx_count() == 0
    }

    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.as_ref().get_tx_count()
    }

    #[inline(always)]
    fn get_rx_count(&self) -> usize {
        self.as_ref().get_rx_count()
    }

    fn get_wakers_count(&self) -> (usize, usize) {
        self.as_ref().get_wakers_count()
    }
}

impl<F: Flavor> Deref for Tx<F> {
    type Target = ChannelShared<F>;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for Tx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for MTx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.0.shared
    }
}

impl<T: Send + 'static, F: Flavor<Item = T>> SenderType for Tx<F> {
    type Flavor = F;
    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self::new(shared)
    }
}

impl<F: Flavor> NotCloneable for Tx<F> {}

impl<T: Send + 'static, F: Flavor<Item = T> + FlavorMP> SenderType for MTx<F> {
    type Flavor = F;
    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        MTx::new(shared)
    }
}
