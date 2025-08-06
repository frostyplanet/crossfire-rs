use crate::sink::AsyncSink;
use crate::{channel::*, MTx, Tx};
use crossbeam::channel::Sender;
use crossbeam::utils::Backoff;
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

const BACKOFF_LIMIT: usize = 6;

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
    bound_size: usize,
    pub(crate) sender: Sender<T>,
    pub(crate) shared: Arc<ChannelShared>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
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

impl<T> AsyncTx<T> {
    #[inline]
    pub(crate) fn new(sender: Sender<T>, shared: Arc<ChannelShared>) -> Self {
        let bound_size = sender.capacity().unwrap_or(0);
        Self { bound_size, sender, shared, _phan: Default::default() }
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
        match self.sender.try_send(item) {
            Err(e) => return Err(e),
            Ok(_) => {
                self.shared.on_send();
                return Ok(());
            }
        }
    }

    /// The number of messages in the channel at the moment
    #[inline]
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    /// Whether channel is empty at the moment
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Whether the channel is full at the moment
    #[inline]
    pub fn is_full(&self) -> bool {
        self.sender.is_full()
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
        return SendFuture { tx: &self, item: Some(item), waker: None };
    }

    /// Use send() instead
    #[inline(always)]
    #[deprecated]
    pub fn make_send_future<'a>(&'a self, item: T) -> SendFuture<'a, T> {
        return SendFuture { tx: &self, item: Some(item), waker: None };
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
        return SendTimeoutFuture { tx: &self, item: Some(item), waker: None, sleep };
    }

    /// Internal function might change in the future. For public version, use AsyncSink::poll_send() instead
    ///
    /// Returns `Ok(())` on message sent.
    ///
    /// Returns Err([TrySendError::Full]) for Poll::Pending case.
    ///
    /// Returns Err([TrySendError::Disconnected]) when all Rx dropped.
    #[inline(always)]
    pub(crate) fn poll_send<'a>(
        &'a self, ctx: &'a mut Context, mut item: T, o_waker: &'a mut Option<LockedWaker>,
    ) -> Result<(), TrySendError<T>> {
        // When the result is not TrySendError::Full,
        // make sure always take the o_waker out and abandon,
        // to skip the timeout cleaning logic in Drop.
        //
        // crossbeam-channel will check disconnected for us (if not raced)
        let backoff = Backoff::new();
        let limit = if self.bound_size == 1 { BACKOFF_LIMIT } else { 1 };
        for _ in 0..limit {
            let r = self.try_send(item);
            if let Some(old_waker) = o_waker.take() {
                // https://github.com/frostyplanet/crossfire-rs/issues/14
                old_waker.cancel();
            }
            if let Err(TrySendError::Full(t)) = r {
                item = t;
            } else {
                return r;
            }
            backoff.snooze();
        }
        let waker = self.shared.reg_send_async(ctx);
        // NOTE: The other side put something whie reg_send and did not see the waker,
        // should check the channel again, otherwise might incur a dead lock.
        let r = self.try_send(item);
        if let Err(TrySendError::Full(t)) = r {
            if self.shared.is_disconnected() {
                // Check channel close before sleep, otherwise might block forever
                // Confirmed by test_pressure_1_tx_blocking_1_rx_async()
                return Err(TrySendError::Disconnected(t));
            }
            waker.commit();
            o_waker.replace(waker);
            return Err(TrySendError::Full(t));
        }
        return r;
    }

    /// Send a message while **blocking the current thread**. Be careful!
    ///
    /// Returns `Ok(())`on successful.
    ///
    /// Returns Err([SendError]) when all Rx is dropped.
    ///
    /// **NOTE: Do not use it in async context otherwise will block the runtime.**
    #[inline]
    pub fn send_blocking(&self, item: T) -> Result<(), SendError<T>> {
        match self.sender.send(item) {
            Ok(()) => {
                self.shared.on_send();
                return Ok(());
            }
            Err(e) => return Err(e),
        }
    }
}

/// A fixed-sized future object constructed by [AsyncTx::make_send_future()]
pub struct SendFuture<'a, T: Unpin> {
    tx: &'a AsyncTx<T>,
    item: Option<T>,
    waker: Option<LockedWaker>,
}

unsafe impl<T: Unpin + Send> Send for SendFuture<'_, T> {}

impl<T: Unpin> Drop for SendFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // Cancelling the future, poll is not ready
            let state = waker.abandon();
            if state == WakerState::WAKED as u8 {
                // We are waked, but give up sending, should notify another sender for safety
                self.tx.shared.on_recv();
            } else {
                debug_assert_eq!(state, WakerState::WAITING as u8);
                self.tx.shared.clear_send_wakers(waker.get_seq());
            }
        }
    }
}

impl<T: Unpin + Send + 'static> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        let item = _self.item.take().unwrap();
        let tx = _self.tx;
        let r = tx.poll_send(ctx, item, &mut _self.waker);
        match r {
            Ok(()) => {
                return Poll::Ready(Ok(()));
            }
            Err(TrySendError::Disconnected(t)) => {
                return Poll::Ready(Err(SendError(t)));
            }
            Err(TrySendError::Full(t)) => {
                _self.item.replace(t);
                return Poll::Pending;
            }
        }
    }
}

/// A fixed-sized future object constructed by [AsyncTx::send_timeout()]
pub struct SendTimeoutFuture<'a, T: Unpin> {
    tx: &'a AsyncTx<T>,
    item: Option<T>,
    waker: Option<LockedWaker>,
    sleep: Pin<Box<dyn Future<Output = ()>>>,
}

unsafe impl<T: Unpin + Send> Send for SendTimeoutFuture<'_, T> {}

impl<T: Unpin> Drop for SendTimeoutFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // Cancelling the future, poll is not ready
            let state = waker.abandon();
            if state == WakerState::WAKED as u8 {
                // We are waked, but give up sending, should notify another sender for safety
                self.tx.shared.on_recv();
            } else {
                debug_assert_eq!(state, WakerState::WAITING as u8);
                self.tx.shared.clear_send_wakers(waker.get_seq());
            }
        }
    }
}

impl<T: Unpin + Send + 'static> Future for SendTimeoutFuture<'_, T> {
    type Output = Result<(), SendTimeoutError<T>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        let item = _self.item.take().unwrap();
        let tx = _self.tx;
        let r = tx.poll_send(ctx, item, &mut _self.waker);
        match r {
            Ok(()) => {
                return Poll::Ready(Ok(()));
            }
            Err(TrySendError::Disconnected(t)) => {
                return Poll::Ready(Err(SendTimeoutError::Disconnected(t)));
            }
            Err(TrySendError::Full(t)) => {
                if let Poll::Ready(()) = _self.sleep.as_mut().poll(ctx) {
                    return Poll::Ready(Err(SendTimeoutError::Timeout(t)));
                }
                _self.item.replace(t);
                return Poll::Pending;
            }
        }
    }
}

/// For writing generic code with MAsyncTx & AsyncTx
pub trait AsyncTxTrait<T: Unpin + Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared>
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
    fn len(&self) -> usize;

    /// Whether channel is empty at the moment
    fn is_empty(&self) -> bool;

    /// Whether the channel is full at the moment
    fn is_full(&self) -> bool;

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
    fn len(&self) -> usize {
        AsyncTx::len(self)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        AsyncTx::is_empty(self)
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        AsyncTx::is_full(self)
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
        Self(AsyncTx::new(inner.sender.clone(), inner.shared.clone()))
    }
}

impl<T> From<MAsyncTx<T>> for AsyncTx<T> {
    fn from(tx: MAsyncTx<T>) -> Self {
        tx.0
    }
}

impl<T> MAsyncTx<T> {
    #[inline]
    pub(crate) fn new(send: Sender<T>, shared: Arc<ChannelShared>) -> Self {
        Self(AsyncTx::new(send, shared))
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

impl<T: Unpin + Send + 'static> AsyncTxTrait<T> for MAsyncTx<T> {
    #[inline(always)]
    fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        self.0.try_send(item)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        self.0.is_full()
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
    type Target = ChannelShared;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared> for AsyncTx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared> for MAsyncTx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared {
        &self.0.shared
    }
}
