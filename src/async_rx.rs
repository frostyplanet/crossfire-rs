use crate::backoff::*;
use crate::stream::AsyncStream;
use crate::{channel::*, MRx, Rx};
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicI8, Ordering},
    Arc,
};
use std::task::{Context, Poll};

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
    pub(crate) shared: Arc<ChannelShared<T>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    backoff: AtomicI8,
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

impl<T> From<Rx<T>> for AsyncRx<T> {
    fn from(value: Rx<T>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

impl<T> AsyncRx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self { shared, _phan: Default::default(), backoff: AtomicI8::new(-1) }
    }

    #[inline(always)]
    pub(crate) fn _detect_runtime(&self) -> u16 {
        let mut backoff = self.backoff.load(Ordering::Relaxed);
        if backoff < 0 {
            backoff = self.shared.detect_async_backoff_rx();
            self.backoff.store(backoff, Ordering::Release);
        }
        return backoff as u16;
    }

    /// Receive message, will await when channel is empty.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// returns Err([RecvError]) when all Tx dropped.
    #[inline(always)]
    pub fn recv<'a>(&'a self) -> RecvFuture<'a, T> {
        return RecvFuture { rx: self, waker: None };
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
    pub fn recv_timeout<'a>(&'a self, duration: std::time::Duration) -> RecvTimeoutFuture<'a, T> {
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
        return RecvTimeoutFuture { rx: self, waker: None, sleep };
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
        if let Some(item) = self.shared.try_recv() {
            self.shared.on_recv();
            return Ok(item);
        } else {
            if self.shared.is_disconnected() {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }
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
        &self, ctx: &mut Context, o_waker: &mut Option<RecvWaker>,
    ) -> Result<T, TryRecvError> {
        let shared = &self.shared;
        // When the result is not TryRecvError::Empty,
        // make sure always take the o_waker out and abandon,
        // to skip the timeout cleaning logic in Drop.
        macro_rules! try_recv {
            () => {
                if let Some(item) = shared.try_recv() {
                    shared.on_recv();
                    return Ok(item);
                }
            };
            ($waker: expr) => {
                if let Some(item) = shared.try_recv() {
                    shared.on_recv();
                    shared.recv_waker_cancel($waker);
                    let _ = o_waker.take();
                    return Ok(item);
                }
            };
        }

        if let Some(waker) = o_waker.as_ref() {
            try_recv!(waker);
            let state = waker.get_state();
            if state == WakerState::WAITING as u8 {
                if waker.will_wake(ctx) {
                    return Err(TryRecvError::Empty);
                } else {
                    if let Some(waker) = o_waker.take() {
                        waker.cancel();
                    }
                }
            } else if state == WakerState::WAKED as u8 {
                waker.check_waker_nolock(ctx);
            }
        } else {
            // First call
            try_recv!();
            let backoff_limit = self._detect_runtime();
            if backoff_limit > 0 {
                let config = BackoffConfig { spin_limit: 6, limit: backoff_limit };
                let mut backoff = Backoff::new(config);
                loop {
                    backoff.snooze();
                    try_recv!();
                    if backoff.is_completed() {
                        break;
                    }
                }
            }
        }
        let _waker;
        if let Some(waker) = o_waker.as_ref() {
            _waker = waker;
        } else {
            let waker = RecvWaker::new_async(ctx);
            o_waker.replace(waker);
            _waker = o_waker.as_ref().unwrap();
        }
        if shared.reg_recv(_waker).is_ok() {
            // NOTE: The other side put something whie reg_send and did not see the waker,
            // should check the channel again, otherwise might incur a dead lock.
            if !shared.is_empty() {
                try_recv!(_waker);
            }
            _waker.commit_waiting();
        }
        if shared.is_disconnected() {
            try_recv!();
            return Err(TryRecvError::Disconnected);
        }
        return Err(TryRecvError::Empty);
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
}

/// A fixed-sized future object constructed by [AsyncRx::recv()]
pub struct RecvFuture<'a, T> {
    rx: &'a AsyncRx<T>,
    waker: Option<RecvWaker>,
}

unsafe impl<T: Send> Send for RecvFuture<'_, T> {}

impl<T> Drop for RecvFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // cancelled
            self.rx.shared.abandon_recv_waker(waker);
        }
    }
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.rx.poll_item(ctx, &mut _self.waker) {
            Err(e) => {
                if !e.is_empty() {
                    let _ = _self.waker.take();
                    return Poll::Ready(Err(RecvError {}));
                } else {
                    return Poll::Pending;
                }
            }
            Ok(item) => {
                debug_assert!(_self.waker.is_none());
                return Poll::Ready(Ok(item));
            }
        }
    }
}

/// A fixed-sized future object constructed by [AsyncRx::recv_timeout()]
pub struct RecvTimeoutFuture<'a, T> {
    rx: &'a AsyncRx<T>,
    waker: Option<RecvWaker>,
    sleep: Pin<Box<dyn Future<Output = ()>>>,
}

unsafe impl<T: Unpin + Send> Send for RecvTimeoutFuture<'_, T> {}

impl<T> Drop for RecvTimeoutFuture<'_, T> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.take() {
            // cancelled
            self.rx.shared.abandon_recv_waker(waker);
        }
    }
}

impl<T> Future for RecvTimeoutFuture<'_, T> {
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
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared<T>>
{
    /// Receive message, will await when channel is empty.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// returns Err([RecvError]) when all Tx dropped.
    fn recv<'a>(&'a self) -> RecvFuture<'a, T>;

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
    fn recv_timeout<'a>(&'a self, timeout: std::time::Duration) -> RecvTimeoutFuture<'a, T>;

    /// Try to receive message, non-blocking.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([TryRecvError::Empty]) when channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    fn try_recv(&self) -> Result<T, TryRecvError>;

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
}

impl<T: Unpin + Send + 'static> AsyncRxTrait<T> for AsyncRx<T> {
    #[inline(always)]
    fn recv<'a>(&'a self) -> RecvFuture<'a, T> {
        AsyncRx::recv(self)
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn recv_timeout<'a>(&'a self, duration: std::time::Duration) -> RecvTimeoutFuture<'a, T> {
        AsyncRx::recv_timeout(self, duration)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        AsyncRx::<T>::try_recv(self)
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
        Self(AsyncRx::new(inner.shared.clone()))
    }
}

impl<T> From<MAsyncRx<T>> for AsyncRx<T> {
    fn from(rx: MAsyncRx<T>) -> Self {
        rx.0
    }
}

impl<T> MAsyncRx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self(AsyncRx::new(shared))
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

impl<T> From<MRx<T>> for MAsyncRx<T> {
    fn from(value: MRx<T>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

impl<T: Unpin + Send + 'static> AsyncRxTrait<T> for MAsyncRx<T> {
    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.0.try_recv()
    }

    #[inline(always)]
    fn recv<'a>(&'a self) -> RecvFuture<'a, T> {
        self.0.recv()
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn recv_timeout<'a>(&'a self, duration: std::time::Duration) -> RecvTimeoutFuture<'a, T> {
        self.0.recv_timeout(duration)
    }
}

impl<T> Deref for AsyncRx<T> {
    type Target = ChannelShared<T>;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for AsyncRx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for MAsyncRx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.0.shared
    }
}
