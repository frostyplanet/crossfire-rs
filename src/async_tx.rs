use crate::sink::AsyncSink;
#[cfg(feature = "trace_log")]
use crate::tokio_task_id;
use crate::{shared::*, trace_log, MTx, Tx};
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::{needs_drop, MaybeUninit};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A single producer (sender) that works in an async context.
///
/// Additional methods in [ChannelShared] can be accessed through `Deref`.
///
/// `AsyncTx` can be converted into `Tx` via the `From` trait.
/// This means you can have two types of senders, both within async and blocking contexts, for the same channel.
///
/// **NOTE**: `AsyncTx` is not `Clone` or `Sync`.
/// If you need concurrent access, use [MAsyncTx] instead.
///
/// `AsyncTx` has a `Send` marker and can be moved to other coroutines.
/// The following code is OK:
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
/// Because `AsyncTx` does not have a `Sync` marker, using `Arc<AsyncTx>` will lose the `Send` marker.
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
pub struct AsyncTx<T: Send + 'static> {
    pub(crate) shared: Arc<ChannelShared<T>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    flavor: *const (),
    _try_send: TrySendFunc<T>,
    _poll_send: PollSendFunc<T>,
}

impl<T: Send + 'static> fmt::Debug for AsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncTx")
    }
}

impl<T: Send + 'static> fmt::Display for AsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncTx")
    }
}

unsafe impl<T: Send> Send for AsyncTx<T> {}

impl<T: Send + 'static> Drop for AsyncTx<T> {
    fn drop(&mut self) {
        self.shared.close_tx();
    }
}

impl<T: Send + 'static> From<Tx<T>> for AsyncTx<T> {
    fn from(value: Tx<T>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

impl<T: Send + 'static> AsyncTx<T> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self {
            flavor: shared.get_flavor_ptr(),
            _try_send: shared._try_send,
            _poll_send: shared._poll_send,
            shared,
            _phan: Default::default(),
        }
    }

    #[inline]
    pub fn into_sink(self) -> AsyncSink<T> {
        AsyncSink::new(self)
    }

    #[inline]
    pub fn into_blocking(self) -> Tx<T> {
        self.into()
    }

    /// Return true if the other side has closed
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_rx_closed()
    }

    /// Internal function might change in the future. For public version, use AsyncSink::poll_send() instead.
    ///
    /// Returns `Poll::Ready(Ok(()))` on message sent.
    ///
    /// Returns `Poll::Pending` for Poll::Pending case.
    ///
    /// Returns `Poll::Ready(Err(())` when all Rx dropped.
    #[inline(always)]
    pub(crate) fn _poll_send<'a, F: FlavorImpl<T>>(
        shared: &ChannelShared<T>, flavor: &F, ctx: &'a mut Context, item: &MaybeUninit<T>,
        o_waker: &'a mut Option<SendWaker<T>>, sink: bool,
    ) -> Poll<Result<(), ()>> {
        if shared.is_rx_closed() {
            trace_log!("tx{:?}: closed {:?}", tokio_task_id!(), o_waker);
            return Poll::Ready(Err(()));
        }
        // When the result is not TrySendError::Full,
        // make sure always take the o_waker out and abandon,
        // to skip the timeout cleaning logic in Drop.
        loop {
            if flavor.try_send(item) {
                shared.on_send();
                if let Some(_waker) = o_waker.take() {
                    trace_log!("tx{:?}: send {:?}", tokio_task_id!(), _waker);
                } else {
                    trace_log!("tx{:?}: send", tokio_task_id!());
                }
                return Poll::Ready(Ok(()));
            }
            if o_waker.is_none() {
                if let Some(mut backoff) = shared.get_async_backoff() {
                    loop {
                        backoff.spin();
                        if flavor.try_send(item) {
                            shared.on_send();
                            trace_log!("tx{:?}: send", tokio_task_id!());
                            return Poll::Ready(Ok(()));
                        }
                        if backoff.is_completed() {
                            break;
                        }
                    }
                }
            }
            match shared.senders.reg_waker_async(ctx, o_waker) {
                Some(Poll::Pending) => return Poll::Pending,
                Some(Poll::Ready(())) => return Poll::Ready(Err(())),
                _ => {}
            }
            let state = shared.sender_double_check::<F>(flavor, item, o_waker, sink);
            trace_log!("tx{:?}: sender_double_check {:?} {}", tokio_task_id!(), o_waker, state);
            if state < WakerState::Woken as u8 {
                return Poll::Pending;
            } else if state > WakerState::Woken as u8 {
                if state == WakerState::Done as u8 {
                    trace_log!("tx{:?}: send {:?} done", o_waker, tokio_task_id!());
                    let _ = o_waker.take();
                    return Poll::Ready(Ok(()));
                } else {
                    debug_assert_eq!(state, WakerState::Closed as u8);
                    trace_log!("tx{:?}: closed {:?}", o_waker, tokio_task_id!());
                    let _ = o_waker.take();
                    return Poll::Ready(Err(()));
                }
            }
            debug_assert_eq!(state, WakerState::Woken as u8);
            continue;
        }
    }
}

impl<T: Unpin + Send + 'static> AsyncTx<T> {
    /// Sends a message. This method will await until the message is sent or the channel is closed.
    ///
    /// This function is cancellation-safe, so it's safe to use with `timeout()` and the `select!` macro.
    /// When a [SendFuture] is dropped, no message will be sent. However, the original message
    /// cannot be returned due to API limitations. For timeout scenarios, we recommend using
    /// [AsyncTx::send_timeout()], which returns the message in a [SendTimeoutError].
    ///
    /// Returns `Ok(())` on success.
    ///
    /// Returns Err([SendError]) if the receiver has been dropped.
    #[inline(always)]
    pub fn send<'a>(&'a self, item: T) -> SendFuture<'a, T> {
        return SendFuture { tx: &self, item: MaybeUninit::new(item), waker: None };
    }

    /// Attempts to send a message without blocking.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([TrySendError::Full]) if the channel is full.
    ///
    /// Returns Err([TrySendError::Disconnected]) if the receiver has been dropped.
    #[inline]
    pub fn try_send(&self, item: T) -> Result<(), TrySendError<T>> {
        if self.shared.is_rx_closed() {
            return Err(TrySendError::Disconnected(item));
        }
        let _item = MaybeUninit::new(item);
        if unsafe { (self._try_send)(self.flavor, &_item) } {
            self.shared.on_send();
            return Ok(());
        } else {
            return unsafe { Err(TrySendError::Full(_item.assume_init())) };
        }
    }

    /// Sends a message with a timeout.
    /// Will await when channel is full.
    ///
    /// The behavior is atomic: the message is either sent successfully or returned with error.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) if the operation timed out. The error contains the message that failed to be sent.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) if the receiver has been dropped. The error contains the message that failed to be sent.
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline]
    pub fn send_timeout<'a>(
        &'a self, item: T, duration: std::time::Duration,
    ) -> SendTimeoutFuture<'a, T, ()> {
        let sleep = {
            #[cfg(feature = "tokio")]
            {
                tokio::time::sleep(duration)
            }
            #[cfg(feature = "async_std")]
            {
                async_std::task::sleep(duration)
            }
        };
        self.send_with_timer(item, sleep)
    }

    /// Sends a message with a custom timer function (from other async runtime).
    ///
    /// The behavior is atomic: the message is either sent successfully or returned with error.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) if the operation timed out. The error contains the message that failed to be sent.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) if the receiver has been dropped. The error contains the message that failed to be sent.
    ///
    /// # Argument:
    ///
    /// * `fut`: The sleep function. It's possible to wrap this function with cancelable handle,
    /// you can control when to stop polling. the return value of `fut` is ignore.
    /// We add generic `R` just in order to support smol::Timer.
    ///
    /// # Example:
    ///
    /// ```ignore
    /// extern crate smol;
    /// use std::time::Duration;
    /// use crossfire::*;
    /// async fn foo() {
    ///     let (tx, rx) = mpmc::bounded_async::<usize>(10);
    ///     match tx.send_with_timer(1, smol::Timer::after(Duration::from_secs(1))).await {
    ///         Ok(_)=>{
    ///             println!("message sent");
    ///         }
    ///         Err(SendTimeoutError::Timeout(_item))=>{
    ///             println!("send timeout");
    ///         }
    ///         Err(SendTimeoutError::Disconnected(_item))=>{
    ///             println!("receiver-side closed");
    ///         }
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn send_with_timer<'a, F, R>(&'a self, item: T, fut: F) -> SendTimeoutFuture<'a, T, R>
    where
        F: Future<Output = R> + 'static,
    {
        SendTimeoutFuture {
            tx: &self,
            item: MaybeUninit::new(item),
            waker: None,
            sleep: Box::pin(fut),
        }
    }

    /// Internal function might change in the future. For public version, use AsyncSink::poll_send() instead.
    ///
    /// Returns `Poll::Ready(Ok(()))` on message sent.
    ///
    /// Returns `Poll::Pending` for Poll::Pending case.
    ///
    /// Returns `Poll::Ready(Err(())` when all Rx dropped.
    #[inline(always)]
    pub(crate) fn poll_send<'a>(
        &self, ctx: &'a mut Context, item: &MaybeUninit<T>, o_waker: &'a mut Option<SendWaker<T>>,
        sink: bool,
    ) -> Poll<Result<(), ()>> {
        unsafe { (self._poll_send)(&self.shared, self.flavor, ctx, item, o_waker, sink) }
    }
}

/// A fixed-sized future object constructed by [AsyncTx::make_send_future()]
#[must_use]
pub struct SendFuture<'a, T: Unpin + Send + 'static> {
    tx: &'a AsyncTx<T>,
    item: MaybeUninit<T>,
    waker: Option<SendWaker<T>>,
}

unsafe impl<T: Unpin + Send> Send for SendFuture<'_, T> {}

impl<T: Unpin + Send + 'static> Drop for SendFuture<'_, T> {
    fn drop(&mut self) {
        // Cancelling the future, poll is not ready
        if self.tx.shared.abandon_send_waker(&mut self.waker) {
            if needs_drop::<T>() {
                unsafe { self.item.assume_init_drop() };
            }
        }
    }
}

impl<T: Unpin + Send + 'static> Future for SendFuture<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.tx.poll_send(ctx, &_self.item, &mut _self.waker, false) {
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
#[must_use]
pub struct SendTimeoutFuture<'a, T: Unpin + Send + 'static, R> {
    tx: &'a AsyncTx<T>,
    sleep: Pin<Box<dyn Future<Output = R>>>,
    item: MaybeUninit<T>,
    waker: Option<SendWaker<T>>,
}

unsafe impl<T: Unpin + Send, R> Send for SendTimeoutFuture<'_, T, R> {}

impl<T: Unpin + Send + 'static, R> Drop for SendTimeoutFuture<'_, T, R> {
    fn drop(&mut self) {
        // Cancelling the future, poll is not ready
        if self.tx.shared.abandon_send_waker(&mut self.waker) {
            if needs_drop::<T>() {
                unsafe { self.item.assume_init_drop() };
            }
        }
    }
}

impl<T: Unpin + Send + 'static, R> Future for SendTimeoutFuture<'_, T, R> {
    type Output = Result<(), SendTimeoutError<T>>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.tx.poll_send(ctx, &_self.item, &mut _self.waker, false) {
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
                if let Poll::Ready(_) = _self.sleep.as_mut().poll(ctx) {
                    if _self.tx.shared.abandon_send_waker(&mut _self.waker) {
                        return Poll::Ready(Err(SendTimeoutError::Timeout(unsafe {
                            _self.item.assume_init_read()
                        })));
                    } else {
                        // Message already sent in background (on_recv).
                        return Poll::Ready(Ok(()));
                    }
                }
                return Poll::Pending;
            }
        }
    }
}

/// For writing generic code with MAsyncTx & AsyncTx
pub trait AsyncTxTrait<T: Unpin + Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared<T>> + Sized + Into<AsyncSink<T>>
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
        self.as_ref().is_rx_closed()
    }

    fn clone_to_vec(self, count: usize) -> Vec<Self>;

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
    ) -> SendTimeoutFuture<'a, T, ()>;

    /// Sends a message with a custom timer function.
    /// Will await when channel is full.
    ///
    /// The behavior is atomic: the message is either sent successfully or returned with error.
    ///
    /// Returns `Ok(())` when successful.
    ///
    /// Returns Err([SendTimeoutError::Timeout]) if the operation timed out. The error contains the message that failed to be sent.
    ///
    /// Returns Err([SendTimeoutError::Disconnected]) if the receiver has been dropped. The error contains the message that failed to be sent.
    ///
    /// # Argument:
    ///
    /// * `fut`: The sleep function. It's possible to wrap this function with cancelable handle,
    /// you can control when to stop polling. the return value of `fut` is ignore.
    /// We add generic `R` just in order to support smol::Timer

    fn send_with_timer<'a, F, R>(&'a self, item: T, fut: F) -> SendTimeoutFuture<'a, T, R>
    where
        F: Future<Output = R> + 'static;
}

impl<T: Unpin + Send + 'static> AsyncTxTrait<T> for AsyncTx<T> {
    #[inline(always)]
    fn clone_to_vec(self, count: usize) -> Vec<Self> {
        assert_eq!(count, 1);
        vec![self]
    }

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
    ) -> SendTimeoutFuture<'a, T, ()> {
        AsyncTx::send_timeout(self, item, duration)
    }

    #[inline(always)]
    fn send_with_timer<'a, F, R>(&'a self, item: T, fut: F) -> SendTimeoutFuture<'a, T, R>
    where
        F: Future<Output = R> + 'static,
    {
        AsyncTx::send_with_timer(self, item, fut)
    }
}

/// A multi-producer (sender) that works in an async context.
///
/// Inherits from [`AsyncTx<T>`] and implements `Clone`.
/// Additional methods in [ChannelShared] can be accessed through `Deref`.
///
/// You can use `into()` to convert it to `AsyncTx<T>`.
///
/// `MAsyncTx` can be converted into `MTx` via the `From` trait,
/// which means you can have two types of senders, both within async and
/// blocking contexts, for the same channel.

pub struct MAsyncTx<T: Send + 'static>(pub(crate) AsyncTx<T>);

impl<T: Send + 'static> fmt::Debug for MAsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncTx")
    }
}

impl<T: Send + 'static> fmt::Display for MAsyncTx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncTx")
    }
}

unsafe impl<T: Send> Sync for MAsyncTx<T> {}

impl<T: Unpin + Send + 'static> Clone for MAsyncTx<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_tx();
        Self(AsyncTx::new(inner.shared.clone()))
    }
}

impl<T: Send + 'static> From<MAsyncTx<T>> for AsyncTx<T> {
    fn from(tx: MAsyncTx<T>) -> Self {
        tx.0
    }
}

impl<T: Send + 'static> MAsyncTx<T> {
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

impl<T: Send + 'static> Deref for MAsyncTx<T> {
    type Target = AsyncTx<T>;

    /// inherit all the functions of [AsyncTx]
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Send + 'static> From<MTx<T>> for MAsyncTx<T> {
    fn from(value: MTx<T>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

impl<T: Unpin + Send + 'static> AsyncTxTrait<T> for MAsyncTx<T> {
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
    ) -> SendTimeoutFuture<'a, T, ()> {
        self.0.send_timeout(item, duration)
    }

    #[inline(always)]
    fn send_with_timer<'a, F, R>(&'a self, item: T, fut: F) -> SendTimeoutFuture<'a, T, R>
    where
        F: Future<Output = R> + 'static,
    {
        self.0.send_with_timer(item, fut)
    }
}

impl<T: Send + 'static> Deref for AsyncTx<T> {
    type Target = ChannelShared<T>;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T: Send + 'static> AsRef<ChannelShared<T>> for AsyncTx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T: Send + 'static> AsRef<ChannelShared<T>> for MAsyncTx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.0.shared
    }
}
