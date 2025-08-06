use crate::stream::AsyncStream;
use crate::{channel::*, MRx, Rx};
use crossbeam::channel::Receiver;
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

/// Single consumer (receiver) that works in async context.
///
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// `AsyncRx` can be converted into `Rx` via `From` trait,
/// that means you can have two types of receivers both within async and
/// blocking context for the same channel.

///
/// **NOTE: AsyncRx is not Clone, nor Sync.**
/// If you need concurrent access, use [MAsyncRx](crate::MAsyncRx) instead.
///
/// AsyncRx has Send marker, can be moved to other coroutine.
/// The following code is OK :
///
/// ``` rust
/// use crossfire::*;
/// async fn foo() {
///     let (tx, rx) = mpsc::bounded_async::<usize>(100);
///     tokio::spawn(async move {
///         let _ = rx.recv().await;
///     });
///     drop(tx);
/// }
/// ```
///
/// Because AsyncRx does not have Sync marker, using `Arc<AsyncRx>` will lose Send marker.
///
/// For your safety, the following code **should not compile**:
///
/// ``` compile_fail
/// use crossfire::*;
/// use std::sync::Arc;
/// async fn foo() {
///     let (tx, rx) = mpsc::bounded_async::<usize>(100);
///     let rx = Arc::new(rx);
///     tokio::spawn(async move {
///         let _ = rx.recv().await;
///     });
///     drop(tx);
/// }
/// ```
pub struct AsyncRx<T> {
    bound_size: usize,
    pub(crate) recv: Receiver<T>,
    pub(crate) shared: Arc<ChannelShared>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
}

unsafe impl<T: Send> Send for AsyncRx<T> {}

impl<T> fmt::Debug for AsyncRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncRx")
    }
}

impl<T> fmt::Display for AsyncRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncRx")
    }
}

impl<T> Drop for AsyncRx<T> {
    fn drop(&mut self) {
        self.shared.close_rx();
    }
}

impl<T> AsyncRx<T> {
    #[inline]
    pub(crate) fn new(recv: Receiver<T>, shared: Arc<ChannelShared>) -> Self {
        let bound_size = recv.capacity().unwrap_or(0);
        Self { bound_size, recv, shared, _phan: Default::default() }
    }

    /// Receive message, will await when channel is empty.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// returns Err([RecvError]) when all Tx dropped.
    #[inline(always)]
    pub fn recv<'a>(&'a self) -> ReceiveFuture<'a, T> {
        return ReceiveFuture { rx: &self, waker: None };
    }

    /// Waits for a message to be received from the channel, but only for a limited time.
    /// Will await when channel is empty.
    ///
    /// The behavior is atomic, either successfully polls a message,
    /// or operation cancelled due to timeout.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// returns Err([RecvTimeoutError::Disconnected]) when all Tx dropped and channel is empty.
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline]
    pub fn recv_timeout<'a>(
        &'a self, duration: std::time::Duration,
    ) -> ReceiveTimeoutFuture<'a, T> {
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
        return ReceiveTimeoutFuture { rx: &self, waker: None, sleep };
    }

    /// Try to receive message, non-blocking.
    ///
    /// Returns `Ok(T)` on successful.
    ///
    /// Returns Err([TryRecvError::Empty]) when channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.recv.try_recv() {
            Err(e) => return Err(e),
            Ok(i) => {
                self.shared.on_recv();
                return Ok(i);
            }
        }
    }

    /// Use recv() instead.
    #[inline(always)]
    #[deprecated]
    pub fn make_recv_future<'a>(&'a self) -> ReceiveFuture<'a, T> {
        return ReceiveFuture { rx: &self, waker: None };
    }

    /// The number of messages in the channel at the moment
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.recv.len()
    }

    /// Whether channel is empty at the moment
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.recv.is_empty()
    }

    /// Whether the channel is full at the moment
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.recv.is_full()
    }

    /// Internal function might change in the future. For public version, use AsyncStream::poll_item() instead
    ///
    /// Returns `Ok(T)` on successful.
    ///
    /// Return Err([TryRecvError::Empty]) for Poll::Pending case.
    ///
    /// Return Err([TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    #[inline(always)]
    pub(crate) fn poll_item(
        &self, ctx: &mut Context, o_waker: &mut Option<LockedWaker>,
    ) -> Result<T, TryRecvError> {
        // When the result is not TryRecvError::Empty,
        // make sure always take the o_waker out and abandon,
        // to skip the timeout cleaning logic in Drop.
        let backoff = Backoff::new();
        let limit = if self.bound_size == 1 { BACKOFF_LIMIT } else { 1 };
        for _ in 0..limit {
            let r = self.try_recv();
            if let Some(old_waker) = o_waker.take() {
                // https://github.com/frostyplanet/crossfire-rs/issues/14
                old_waker.cancel();
            }
            if let Err(TryRecvError::Empty) = &r {
            } else {
                return r;
            }
            backoff.snooze();
        }
        let waker = self.shared.reg_recv_async(ctx);
        // NOTE: The other side put something whie reg_send and did not see the waker,
        // should check the channel again, otherwise might incur a dead lock.
        let r = self.try_recv();
        if let Err(TryRecvError::Empty) = &r {
            waker.commit();
            // Check channel close before sleep, otherwise might block forever
            // Confirmed by test_pressure_1_tx_blocking_1_rx_async()
            if self.shared.is_disconnected() {
                // Ensure all message is received.
                if let Ok(msg) = self.try_recv() {
                    return Ok(msg);
                }
                return Err(TryRecvError::Disconnected);
            }
            o_waker.replace(waker);
        }
        return r;
    }

    #[inline]
    pub fn into_stream(self) -> AsyncStream<T>
    where
        T: Send + Unpin + 'static,
    {
        AsyncStream::new(self)
    }

    #[inline]
    pub fn into_blocking(self) -> Rx<T> {
        self.into()
    }

    /// Receive a message while **blocking the current thread**. Be careful!
    ///
    /// Returns `Ok(T)` on successful.
    ///
    /// Returns Err([RecvError]) when all Tx dropped.
    ///
    /// **NOTE: Do not use it in async context otherwise will block the runtime.**
    #[inline(always)]
    pub fn recv_blocking(&self) -> Result<T, RecvError> {
        match self.recv.recv() {
            Err(e) => return Err(e),
            Ok(i) => {
                self.shared.on_recv();
                return Ok(i);
            }
        }
    }
}

/// A fixed-sized future object constructed by [AsyncRx::recv()]
pub struct ReceiveFuture<'a, T> {
    rx: &'a AsyncRx<T>,
    waker: Option<LockedWaker>,
}

unsafe impl<T: Send> Send for ReceiveFuture<'_, T> {}

impl<T> Drop for ReceiveFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // Cancelling the future, poll is not ready
            let state = waker.abandon();
            if state == WakerState::WAKED as u8 {
                // We are waked, but giving up to recv, should notify another receiver for safety
                self.rx.shared.on_send();
            } else {
                debug_assert_eq!(state, WakerState::WAITING as u8);
                self.rx.shared.clear_recv_wakers(waker.get_seq());
            }
        }
    }
}

impl<T> Future for ReceiveFuture<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.rx.poll_item(ctx, &mut _self.waker) {
            Err(e) => {
                if !e.is_empty() {
                    return Poll::Ready(Err(RecvError {}));
                } else {
                    return Poll::Pending;
                }
            }
            Ok(item) => {
                return Poll::Ready(Ok(item));
            }
        }
    }
}

/// A fixed-sized future object constructed by [AsyncRx::recv_timeout()]
pub struct ReceiveTimeoutFuture<'a, T> {
    rx: &'a AsyncRx<T>,
    waker: Option<LockedWaker>,
    sleep: Pin<Box<dyn Future<Output = ()>>>,
}

unsafe impl<T: Unpin + Send> Send for ReceiveTimeoutFuture<'_, T> {}

impl<T> Drop for ReceiveTimeoutFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // Cancelling the future, poll is not ready
            let state = waker.abandon();
            if state == WakerState::WAKED as u8 {
                // We are waked, but giving up to recv, should notify another receiver for safety
                self.rx.shared.on_send();
            } else {
                debug_assert_eq!(state, WakerState::WAITING as u8);
                self.rx.shared.clear_recv_wakers(waker.get_seq());
            }
        }
    }
}

impl<T> Future for ReceiveTimeoutFuture<'_, T> {
    type Output = Result<T, RecvTimeoutError>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.rx.poll_item(ctx, &mut _self.waker) {
            Err(TryRecvError::Empty) => {
                if let Poll::Ready(()) = _self.sleep.as_mut().poll(ctx) {
                    return Poll::Ready(Err(RecvTimeoutError::Timeout));
                }
                return Poll::Pending;
            }
            Err(TryRecvError::Disconnected) => {
                return Poll::Ready(Err(RecvTimeoutError::Disconnected));
            }
            Ok(item) => {
                return Poll::Ready(Ok(item));
            }
        }
    }
}

/// For writing generic code with MAsyncRx & AsyncRx
pub trait AsyncRxTrait<T: Unpin + Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared>
{
    /// Receive message, will await when channel is empty.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// returns Err([RecvError]) when all Tx dropped.
    fn recv<'a>(&'a self) -> ReceiveFuture<'a, T>;

    /// Waits for a message to be received from the channel, but only for a limited time.
    /// Will await when channel is empty.
    ///
    /// The behavior is atomic, either successfully polls a message,
    /// or operation cancelled due to timeout.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// returns Err([RecvTimeoutError::Disconnected]) when all Tx dropped and channel is empty.
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    fn recv_timeout<'a>(&'a self, timeout: std::time::Duration) -> ReceiveTimeoutFuture<'a, T>;

    /// Try to receive message, non-blocking.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([TryRecvError::Empty]) when channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    fn try_recv(&self) -> Result<T, TryRecvError>;

    /// The number of messages in the channel at the moment
    fn len(&self) -> usize;

    /// Whether channel is empty at the moment
    fn is_empty(&self) -> bool;

    /// Whether the channel is full at the moment
    fn is_full(&self) -> bool;

    #[inline(always)]
    fn is_disconnected(&self) -> bool {
        self.as_ref().is_disconnected()
    }
}

impl<T: Unpin + Send + 'static> AsyncRxTrait<T> for AsyncRx<T> {
    #[inline(always)]
    fn recv<'a>(&'a self) -> ReceiveFuture<'a, T> {
        AsyncRx::recv(self)
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn recv_timeout<'a>(&'a self, duration: std::time::Duration) -> ReceiveTimeoutFuture<'a, T> {
        AsyncRx::recv_timeout(self, duration)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        AsyncRx::try_recv(self)
    }

    #[inline(always)]
    fn len(&self) -> usize {
        AsyncRx::len(self)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        AsyncRx::is_empty(self)
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        AsyncRx::is_full(self)
    }
}

/// Multi-consumer (receiver) that works in async context.
///
/// Inherits [`AsyncRx<T>`] and implements [Clone].
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// You can use `into()` to convert it to `AsyncRx<T>`.
///
/// `MAsyncRx` can be converted into `MRx` via `From` trait,
/// that means you can have two types of receivers both within async and
/// blocking context for the same channel.

pub struct MAsyncRx<T>(pub(crate) AsyncRx<T>);

impl<T> fmt::Debug for MAsyncRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncRx")
    }
}

impl<T> fmt::Display for MAsyncRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncRx")
    }
}

unsafe impl<T: Send> Sync for MAsyncRx<T> {}

impl<T> Clone for MAsyncRx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_rx();
        Self(AsyncRx::new(inner.recv.clone(), inner.shared.clone()))
    }
}

impl<T> From<MAsyncRx<T>> for AsyncRx<T> {
    fn from(rx: MAsyncRx<T>) -> Self {
        rx.0
    }
}

impl<T> MAsyncRx<T> {
    #[inline]
    pub(crate) fn new(recv: Receiver<T>, shared: Arc<ChannelShared>) -> Self {
        Self(AsyncRx::new(recv, shared))
    }

    #[inline]
    pub fn into_stream(self) -> AsyncStream<T>
    where
        T: Send + Unpin + 'static,
    {
        AsyncStream::new(self.0)
    }

    #[inline]
    pub fn into_blocking(self) -> MRx<T> {
        self.into()
    }
}

impl<T> Deref for MAsyncRx<T> {
    type Target = AsyncRx<T>;

    /// inherit all the functions of [AsyncRx]
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Unpin + Send + 'static> AsyncRxTrait<T> for MAsyncRx<T> {
    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.0.try_recv()
    }

    #[inline(always)]
    fn recv<'a>(&'a self) -> ReceiveFuture<'a, T> {
        self.0.recv()
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn recv_timeout<'a>(&'a self, duration: std::time::Duration) -> ReceiveTimeoutFuture<'a, T> {
        self.0.recv_timeout(duration)
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
}

impl<T> Deref for AsyncRx<T> {
    type Target = ChannelShared;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared> for AsyncRx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared> for MAsyncRx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared {
        &self.0.shared
    }
}
