use crate::backoff::*;
use crate::sink::AsyncSink;
use crate::{channel::*, MTx, Tx};
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::{needs_drop, MaybeUninit};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicI8, Ordering},
    Arc,
};
use std::task::{Context, Poll};

/// Single producer (sender) that works in async context.
///
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// `AsyncTx` can be converted into `Tx` via `From` trait,
/// that means you can have two types of senders both within async context and
/// blocking context to the same channel.
///
/// **NOTE: AsyncTx is not Clone, nor Sync.**
/// If you need concurrent access, use [MAsyncTx](crate::MAsyncTx) instead.
///
/// AsyncTx has Send marker, can be moved to other coroutine.
/// The following code is OK :
///
/// ``` rust
/// use crossfire::*;
/// async fn foo() {
///     let (tx, rx) = spsc::bounded_async::<usize>(100);
///     tokio::spawn(async move {
///          let _ = tx.send(2).await;
///     });
///     drop(rx);
/// }
/// ```
///
/// Because AsyncTx does not have Sync marker, using `Arc<AsyncTx>` will lose Send marker.
///
/// For your safety, the following code **should not compile**:
///
/// ``` compile_fail
/// use crossfire::*;
/// use std::sync::Arc;
/// async fn foo() {
///     let (tx, rx) = spsc::bounded_async::<usize>(100);
///     let tx = Arc::new(tx);
///     tokio::spawn(async move {
///          let _ = tx.send(2).await;
///     });
///     drop(rx);
/// }
/// ```
pub struct AsyncTx<T> {
    pub(crate) shared: Arc<ChannelShared<T>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    backoff: AtomicI8,
}

impl<T> fmt::Debug for AsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncTx")
    }
}

impl<T> fmt::Display for AsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncTx")
    }
}

unsafe impl<T: Send> Send for AsyncTx<T> {}

impl<T> Drop for AsyncTx<T> {
    fn drop(&mut self) {
        self.shared.close_tx();
    }
}

impl<T> From<Tx<T>> for AsyncTx<T> {
    fn from(value: Tx<T>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

impl<T> AsyncTx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self { shared, _phan: Default::default(), backoff: AtomicI8::new(-1) }
    }

    #[inline(always)]
    pub(crate) fn _detect_runtime(&self) -> u16 {
        let mut backoff = self.backoff.load(Ordering::Relaxed);
        if backoff < 0 {
            backoff = self.shared.detect_async_backoff_tx();
            self.backoff.store(backoff, Ordering::Release);
        }
        return backoff as u16;
    }

    #[inline]
    pub fn into_sink(self) -> AsyncSink<T> {
        AsyncSink::new(self)
    }

    #[inline]
    pub fn into_blocking(self) -> Tx<T> {
        self.into()
    }
}

impl<T: Unpin + Send + 'static> AsyncTx<T> {
    /// Send message. Will await when channel is full.
    ///
    /// Returns `Ok(())` on successful.
    ///
    /// Returns Err([SendError]) when all Rx is dropped.
    #[inline(always)]
    pub fn send<'a>(&'a self, item: T) -> SendFuture<'a, T> {
        return SendFuture { tx: &self, item: MaybeUninit::new(item), waker: None };
    }

    /// Try to send message, non-blocking
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([TrySendError::Full]) on channel full for bounded channel.
    ///
    /// Returns Err([TrySendError::Disconnected]) when all Rx dropped.
    #[inline]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        if self.shared.is_disconnected() {
            return Err(TrySendError::Disconnected(item));
        }
        let _item = MaybeUninit::new(item);
        if self.shared.send(&_item) {
            self.shared.on_send();
            return Ok(());
        } else {
            return unsafe { Err(TrySendError::Full(_item.assume_init())) };
        }
    }

    /// Waits for a message to be sent into the channel, but only for a limited time.
    /// Will await when channel is full.
    ///
    /// The behavior is atomic, either message sent successfully or returned on error.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) when the operation timed out.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) when all Rx dropped.
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline]
    pub fn send_timeout<'a>(
        &'a self, item: T, duration: std::time::Duration,
    ) -> SendTimeoutFuture<'a, T> {
        let sleep = {
            #[cfg(feature = "tokio")]
            {
                Box::pin(tokio::time::sleep(duration))
            }
            #[cfg(not(feature = "tokio"))]
            {
                Box::pin(async_std::task::sleep(duration))
            }
        };
        return SendTimeoutFuture { tx: &self, item: MaybeUninit::new(item), waker: None, sleep };
    }

    /// Internal function might change in the future. For public version, use AsyncSink::poll_send() instead
    ///
    /// Returns `Poll::Ready(Ok(()))` on message sent.
    ///
    /// Returns `Poll::Pending` for Poll::Pending case.
    ///
    /// Returns `Poll::Ready(Err(())` when all Rx dropped.
    #[inline(always)]
    pub(crate) fn poll_send<'a>(
        &self, ctx: &'a mut Context, item: &mut MaybeUninit<T>,
        o_waker: &'a mut Option<SendWaker<T>>,
    ) -> Poll<Result<(), ()>> {
        let shared = &self.shared;
        if shared.is_disconnected() {
            return Poll::Ready(Err(()));
        }
        let mut _waker;
        let mut state;
        let mut backoff: Option<Backoff> = None;
        macro_rules! process_state {
            ($state: expr) => {{
                let _ = o_waker.take();
                if $state == WakerState::DONE as u8 {
                    // receiver done it's job
                    Poll::Ready(Ok(()))
                } else {
                    debug_assert_eq!($state, WakerState::CLOSED as u8);
                    Poll::Ready(Err(()))
                }
            }};
        }
        // When the result is not TrySendError::Full,
        // make sure always take the o_waker out and abandon,
        // to skip the timeout cleaning logic in Drop.
        loop {
            if let Some(waker) = o_waker.as_ref() {
                state = waker.get_state();
                if state < WakerState::WAKED as u8 {
                    // Spurious waked by runtime, or
                    // Normally only selection or multiplex future will get here.
                    // No need to reg again, since waker is not consumed.
                    (state, *o_waker) = shared.sender_try_again_async(o_waker.take().unwrap(), ctx);
                    if state == WakerState::WAITING as u8 {
                        return Poll::Pending;
                    }
                }
                if state > WakerState::WAKED as u8 {
                    return process_state!(state);
                } else {
                    debug_assert_eq!(state, WakerState::WAKED as u8);
                    if shared.send(item) {
                        shared.on_send();
                        let _ = o_waker.take();
                        return Poll::Ready(Ok(()));
                    }
                }
                // register again
            } else {
                if shared.send(item) {
                    shared.on_send();
                    return Poll::Ready(Ok(()));
                }
            }
            let backoff_limit = self._detect_runtime();
            if backoff_limit > 0 {
                let config = BackoffConfig { spin_limit: 6, limit: backoff_limit };
                let mut _backoff = Backoff::new(config);
                loop {
                    _backoff.spin();
                    if shared.send(item) {
                        shared.on_send();
                        let _ = o_waker.take();
                        return Poll::Ready(Ok(()));
                    }
                    if _backoff.is_completed() {
                        break;
                    }
                }
                backoff.replace(_backoff);
            }
            let waker = if let Some(w) = o_waker.take() {
                w.set_ptr(std::ptr::null_mut());
                w.check_waker_nolock(ctx);
                w
            } else {
                SendWaker::new_async(ctx, std::ptr::null_mut())
            };
            (state, _waker) = shared.sender_reg_and_try(item, waker);
            *o_waker = _waker;
            if state < WakerState::WAKED as u8 {
                if let Some(_backoff) = backoff.as_mut() {
                    state = shared.sender_snooze(o_waker.as_ref().unwrap(), _backoff);
                }
            }
            if state < WakerState::WAKED as u8 {
                return Poll::Pending;
            } else if state > WakerState::WAKED as u8 {
                return process_state!(state);
            }
            debug_assert_eq!(state, WakerState::WAKED as u8);
            continue;
        }
    }
}

/// A fixed-sized future object constructed by [AsyncTx::make_send_future()]
pub struct SendFuture<'a, T: Unpin> {
    tx: &'a AsyncTx<T>,
    item: MaybeUninit<T>,
    waker: Option<SendWaker<T>>,
}

unsafe impl<T: Unpin + Send> Send for SendFuture<'_, T> {}

impl<T: Unpin> Drop for SendFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // Cancelling the future, poll is not ready
            if self.tx.shared.abandon_send_waker(waker) {
                if needs_drop::<T>() {
                    if size_of::<T>() > size_of::<*mut T>() {
                        unsafe { self.item.assume_init_drop() };
                    }
                }
            }
        }
    }
}

impl<T: Unpin + Send + 'static> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.tx.poll_send(ctx, &mut _self.item, &mut _self.waker) {
            Poll::Ready(Ok(())) => {
                debug_assert!(_self.waker.is_none());
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Err(())) => {
                let _ = _self.waker.take();
                return Poll::Ready(Err(SendError(unsafe { _self.item.assume_init_read() })));
            }
            Poll::Pending => return Poll::Pending,
        }
    }
}

/// A fixed-sized future object constructed by [AsyncTx::send_timeout()]
pub struct SendTimeoutFuture<'a, T: Unpin> {
    tx: &'a AsyncTx<T>,
    sleep: Pin<Box<dyn Future<Output = ()>>>,
    item: MaybeUninit<T>,
    waker: Option<SendWaker<T>>,
}

unsafe impl<T: Unpin + Send> Send for SendTimeoutFuture<'_, T> {}

impl<T: Unpin> Drop for SendTimeoutFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // Cancelling the future, poll is not ready
            if self.tx.shared.abandon_send_waker(waker) {
                if needs_drop::<T>() {
                    if size_of::<T>() > size_of::<*mut T>() {
                        unsafe { self.item.assume_init_drop() };
                    }
                }
            }
        }
    }
}

impl<T: Unpin + Send + 'static> Future for SendTimeoutFuture<'_, T> {
    type Output = Result<(), SendTimeoutError<T>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        let r = _self.tx.poll_send(ctx, &mut _self.item, &mut _self.waker);
        match r {
            Poll::Ready(Ok(())) => {
                debug_assert!(_self.waker.is_none());
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Err(())) => {
                let _ = _self.waker.take();
                return Poll::Ready(Err(SendTimeoutError::Disconnected(unsafe {
                    _self.item.assume_init_read()
                })));
            }
            Poll::Pending => {
                if let Poll::Ready(()) = _self.sleep.as_mut().poll(ctx) {
                    if let Some(waker) = _self.waker.take() {
                        if _self.tx.shared.abandon_send_waker(waker) {
                            return Poll::Ready(Err(SendTimeoutError::Timeout(unsafe {
                                _self.item.assume_init_read()
                            })));
                        } else {
                            // Message already sent in background (on_recv).
                            return Poll::Ready(Ok(()));
                        }
                    } else {
                        unreachable!();
                    }
                }
                return Poll::Pending;
            }
        }
    }
}

/// For writing generic code with MAsyncTx & AsyncTx
pub trait AsyncTxTrait<T: Unpin + Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared<T>>
{
    /// Try to send message, non-blocking
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([TrySendError::Full]) on channel full for bounded channel.
    ///
    /// Returns Err([TrySendError::Disconnected]) when all Rx dropped.
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>>;

    /// The number of messages in the channel at the moment
    #[inline(always)]
    fn len(&self) -> usize {
        self.as_ref().len()
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
        self.as_ref().is_disconnected()
    }

    /// Send message. Will await when channel is full.
    ///
    /// Returns `Ok(())` on successful.
    ///
    /// Returns Err([SendError]) when all Rx is dropped.
    fn send<'a>(&'a self, item: T) -> SendFuture<'a, T>;

    /// Waits for a message to be sent into the channel, but only for a limited time.
    /// Will await when channel is full.
    ///
    /// The behavior is atomic, either message sent successfully or returned on error.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) when the operation timed out.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) when all Rx dropped.
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    fn send_timeout<'a>(
        &'a self, item: T, duration: std::time::Duration,
    ) -> SendTimeoutFuture<'a, T>;
}

impl<T: Unpin + Send + 'static> AsyncTxTrait<T> for AsyncTx<T> {
    #[inline(always)]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        AsyncTx::try_send(self, item)
    }

    #[inline(always)]
    fn send<'a>(&'a self, item: T) -> SendFuture<'a, T> {
        AsyncTx::send(self, item)
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn send_timeout<'a>(
        &'a self, item: T, duration: std::time::Duration,
    ) -> SendTimeoutFuture<'a, T> {
        AsyncTx::send_timeout(self, item, duration)
    }
}

/// Multi-producer (sender) that works in async context.
///
/// Inherits [`AsyncTx<T>`] and implements [Clone].
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// You can use `into()` to convert it to `AsyncTx<T>`.
///
/// `MAsyncTx` can be converted into `MTx` via `From` trait,
/// that means you can have two types of senders both within async and
/// blocking context to the same channel.

pub struct MAsyncTx<T>(pub(crate) AsyncTx<T>);

impl<T> fmt::Debug for MAsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncTx")
    }
}

impl<T> fmt::Display for MAsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncTx")
    }
}

unsafe impl<T: Send> Sync for MAsyncTx<T> {}

impl<T: Unpin> Clone for MAsyncTx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_tx();
        Self(AsyncTx::new(inner.shared.clone()))
    }
}

impl<T> From<MAsyncTx<T>> for AsyncTx<T> {
    fn from(tx: MAsyncTx<T>) -> Self {
        tx.0
    }
}

impl<T> MAsyncTx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self(AsyncTx::new(shared))
    }

    #[inline]
    pub fn into_sink(self) -> AsyncSink<T> {
        AsyncSink::new(self.0)
    }

    #[inline]
    pub fn into_blocking(self) -> MTx<T> {
        self.into()
    }
}

impl<T> Deref for MAsyncTx<T> {
    type Target = AsyncTx<T>;

    /// inherit all the functions of [AsyncTx]
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<MTx<T>> for MAsyncTx<T> {
    fn from(value: MTx<T>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

impl<T: Unpin + Send + 'static> AsyncTxTrait<T> for MAsyncTx<T> {
    #[inline(always)]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.0.try_send(item)
    }

    #[inline(always)]
    fn send<'a>(&'a self, item: T) -> SendFuture<'a, T> {
        self.0.send(item)
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn send_timeout<'a>(
        &'a self, item: T, duration: std::time::Duration,
    ) -> SendTimeoutFuture<'a, T> {
        self.0.send_timeout(item, duration)
    }
}

impl<T> Deref for AsyncTx<T> {
    type Target = ChannelShared<T>;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for AsyncTx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for MAsyncTx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.0.shared
    }
}
