use crate::flavor::FlavorMP;
use crate::sink::AsyncSink;
#[cfg(feature = "trace_log")]
use crate::tokio_task_id;
use crate::{shared::*, trace_log, MTx, NotCloneable, SenderType, Tx};
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
pub struct AsyncTx<F: Flavor> {
    pub(crate) shared: Arc<ChannelShared<F>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
}

impl<F: Flavor> fmt::Debug for AsyncTx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncTx{:p}", self)
    }
}

impl<F: Flavor> fmt::Display for AsyncTx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncTx{:p}", self)
    }
}

unsafe impl<F: Flavor> Send for AsyncTx<F> {}

impl<F: Flavor> Drop for AsyncTx<F> {
    #[inline(always)]
    fn drop(&mut self) {
        self.shared.close_tx();
    }
}

impl<F: Flavor> From<Tx<F>> for AsyncTx<F> {
    fn from(value: Tx<F>) -> Self {
        value.add_tx();
        Self::new(value.shared.clone())
    }
}

impl<F: Flavor> AsyncTx<F> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self { shared, _phan: Default::default() }
    }

    #[inline]
    pub fn into_sink(self) -> AsyncSink<F> {
        AsyncSink::new(self)
    }

    #[inline]
    pub fn into_blocking(self) -> Tx<F> {
        self.into()
    }

    /// Return true if the other side has closed
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_rx_closed()
    }
}

impl<F: Flavor> AsyncTx<F>
where
    F::Item: Send + 'static + Unpin,
{
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
    pub fn send<'a>(&'a self, item: F::Item) -> SendFuture<'a, F> {
        SendFuture { tx: self, item: MaybeUninit::new(item), waker: None }
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
        if self.shared.is_rx_closed() {
            return Err(TrySendError::Disconnected(item));
        }
        let _item = MaybeUninit::new(item);
        if self.shared.inner.try_send(&_item) {
            self.shared.on_send();
            Ok(())
        } else {
            unsafe { Err(TrySendError::Full(_item.assume_init())) }
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
        &'a self, item: F::Item, duration: std::time::Duration,
    ) -> SendTimeoutFuture<'a, F, ()> {
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
    pub fn send_with_timer<'a, FR, R>(
        &'a self, item: F::Item, fut: FR,
    ) -> SendTimeoutFuture<'a, F, R>
    where
        FR: Future<Output = R> + 'static,
    {
        SendTimeoutFuture {
            tx: self,
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
    pub(crate) fn poll_send<'a, const SINK: bool>(
        &self, ctx: &'a mut Context, item: &MaybeUninit<F::Item>,
        o_waker: &'a mut Option<<F::Send as Registry>::Waker>,
    ) -> Poll<Result<(), ()>> {
        let shared = &self.shared;
        if shared.is_rx_closed() {
            trace_log!("tx{:?}: closed {:?}", tokio_task_id!(), o_waker);
            return Poll::Ready(Err(()));
        }
        // When the result is not TrySendError::Full,
        // make sure always take the o_waker out and abandon,
        // to skip the timeout cleaning logic in Drop.
        loop {
            if shared.inner.try_send(item) {
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
                        if shared.inner.try_send(item) {
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
            let state = shared.sender_double_check::<SINK>(item, o_waker);
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

/// A fixed-sized future object constructed by [AsyncTx::send()]
#[must_use]
pub struct SendFuture<'a, F: Flavor> {
    tx: &'a AsyncTx<F>,
    item: MaybeUninit<F::Item>,
    waker: Option<<F::Send as Registry>::Waker>,
}

unsafe impl<F: Flavor> Send for SendFuture<'_, F> {}

impl<F: Flavor> Drop for SendFuture<'_, F> {
    #[inline]
    fn drop(&mut self) {
        // Cancelling the future, poll is not ready
        if let Some(waker) = self.waker.as_ref() {
            if self.tx.shared.abandon_send_waker(waker) && needs_drop::<F::Item>() {
                unsafe { self.item.assume_init_drop() };
            }
        }
    }
}

impl<F: Flavor> Future for SendFuture<'_, F>
where
    F::Item: Send + 'static + Unpin,
{
    type Output = Result<(), SendError<F::Item>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.tx.poll_send::<false>(ctx, &_self.item, &mut _self.waker) {
            Poll::Ready(Ok(())) => {
                debug_assert!(_self.waker.is_none());
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(())) => {
                let _ = _self.waker.take();
                Poll::Ready(Err(SendError(unsafe { _self.item.assume_init_read() })))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A fixed-sized future object constructed by [AsyncTx::send_timeout()]
#[must_use]
pub struct SendTimeoutFuture<'a, F: Flavor, R> {
    tx: &'a AsyncTx<F>,
    sleep: Pin<Box<dyn Future<Output = R>>>,
    item: MaybeUninit<F::Item>,
    waker: Option<<F::Send as Registry>::Waker>,
}

unsafe impl<F: Flavor, R> Send for SendTimeoutFuture<'_, F, R> {}

impl<F: Flavor, R> Drop for SendTimeoutFuture<'_, F, R> {
    #[inline]
    fn drop(&mut self) {
        if let Some(waker) = self.waker.as_ref() {
            // Cancelling the future, poll is not ready
            if self.tx.shared.abandon_send_waker(waker) && needs_drop::<F::Item>() {
                unsafe { self.item.assume_init_drop() };
            }
        }
    }
}

impl<F: Flavor, R> Future for SendTimeoutFuture<'_, F, R>
where
    F::Item: Send + 'static + Unpin,
{
    type Output = Result<(), SendTimeoutError<F::Item>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.tx.poll_send::<false>(ctx, &_self.item, &mut _self.waker) {
            Poll::Ready(Ok(())) => {
                debug_assert!(_self.waker.is_none());
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(())) => {
                let _ = _self.waker.take();
                Poll::Ready(Err(SendTimeoutError::Disconnected(unsafe {
                    _self.item.assume_init_read()
                })))
            }
            Poll::Pending => {
                if _self.sleep.as_mut().poll(ctx).is_ready() {
                    if _self.tx.shared.abandon_send_waker(&_self.waker.take().unwrap()) {
                        return Poll::Ready(Err(SendTimeoutError::Timeout(unsafe {
                            _self.item.assume_init_read()
                        })));
                    } else {
                        // Message already sent in background (on_recv).
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Pending
            }
        }
    }
}

/// For writing generic code with MAsyncTx & AsyncTx
pub trait AsyncTxTrait<T: Send + 'static + Unpin>:
    Send + 'static + fmt::Debug + fmt::Display
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

    /// Send message. Will await when channel is full.
    ///
    /// Returns `Ok(())` on successful.
    ///
    /// Returns Err([SendError]) when all Rx is dropped.
    fn send(&self, item: T) -> impl Future<Output = Result<(), SendError<T>>> + Send
    where
        T: Send + 'static + Unpin;

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
    ) -> impl Future<Output = Result<(), SendTimeoutError<T>>> + Send
    where
        T: Send + 'static + Unpin;

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
    ///   you can control when to stop polling. the return value of `fut` is ignore.
    ///   We add generic `R` just in order to support smol::Timer
    fn send_with_timer<FR, R>(
        &self, item: T, fut: FR,
    ) -> impl Future<Output = Result<(), SendTimeoutError<T>>> + Send
    where
        FR: Future<Output = R> + 'static,
        T: Send + 'static + Unpin;
}

impl<F: Flavor> AsyncTxTrait<F::Item> for AsyncTx<F>
where
    F::Item: Send + 'static + Unpin,
{
    #[inline(always)]
    fn clone_to_vec(self, count: usize) -> Vec<Self> {
        assert_eq!(count, 1);
        vec![self]
    }

    #[inline(always)]
    fn try_send(&self, item: F::Item) -> Result<(), TrySendError<F::Item>> {
        AsyncTx::try_send(self, item)
    }

    #[inline(always)]
    fn send(&self, item: F::Item) -> impl Future<Output = Result<(), SendError<F::Item>>> + Send
    where
        F::Item: Send + 'static + Unpin,
    {
        AsyncTx::send(self, item)
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn send_timeout<'a>(
        &'a self, item: F::Item, duration: std::time::Duration,
    ) -> impl Future<Output = Result<(), SendTimeoutError<F::Item>>> + Send
    where
        F::Item: Send + 'static + Unpin,
    {
        AsyncTx::send_timeout(self, item, duration)
    }

    #[inline(always)]
    fn send_with_timer<FR, R>(
        &self, item: F::Item, fut: FR,
    ) -> impl Future<Output = Result<(), SendTimeoutError<F::Item>>> + Send
    where
        FR: Future<Output = R> + 'static,
        F::Item: Send + 'static + Unpin,
    {
        AsyncTx::send_with_timer(self, item, fut)
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

pub struct MAsyncTx<F: Flavor>(pub(crate) AsyncTx<F>);

impl<F: Flavor> fmt::Debug for MAsyncTx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncTx{:p}", self)
    }
}

impl<F: Flavor> fmt::Display for MAsyncTx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncTx{:p}", self)
    }
}

unsafe impl<F: Flavor> Sync for MAsyncTx<F> {}

impl<F: Flavor> Clone for MAsyncTx<F> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_tx();
        Self(AsyncTx::new(inner.shared.clone()))
    }
}

impl<F: Flavor> From<MAsyncTx<F>> for AsyncTx<F> {
    fn from(tx: MAsyncTx<F>) -> Self {
        tx.0
    }
}

impl<F: Flavor + FlavorMP> MAsyncTx<F> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self(AsyncTx::new(shared))
    }
}

impl<F: Flavor> MAsyncTx<F> {
    #[inline]
    pub fn into_sink(self) -> AsyncSink<F> {
        AsyncSink::new(self.0)
    }

    #[inline]
    pub fn into_blocking(self) -> MTx<F> {
        self.into()
    }
}

impl<F: Flavor> Deref for MAsyncTx<F> {
    type Target = AsyncTx<F>;

    /// inherit all the functions of [AsyncTx]
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: Flavor> From<MTx<F>> for MAsyncTx<F> {
    fn from(value: MTx<F>) -> Self {
        value.add_tx();
        Self(AsyncTx::new(value.shared.clone()))
    }
}

impl<F: Flavor + FlavorMP> AsyncTxTrait<F::Item> for MAsyncTx<F>
where
    F::Item: Send + 'static + Unpin,
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
    fn try_send(&self, item: F::Item) -> Result<(), TrySendError<F::Item>> {
        self.0.try_send(item)
    }

    #[inline(always)]
    fn send(&self, item: F::Item) -> impl Future<Output = Result<(), SendError<F::Item>>> + Send
    where
        F::Item: Send + 'static + Unpin,
    {
        self.0.send(item)
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn send_timeout<'a>(
        &'a self, item: F::Item, duration: std::time::Duration,
    ) -> impl Future<Output = Result<(), SendTimeoutError<F::Item>>> + Send
    where
        F::Item: Send + 'static + Unpin,
    {
        self.0.send_timeout(item, duration)
    }

    #[inline(always)]
    fn send_with_timer<FR, R>(
        &self, item: F::Item, fut: FR,
    ) -> impl Future<Output = Result<(), SendTimeoutError<F::Item>>> + Send
    where
        FR: Future<Output = R> + 'static,
        F::Item: Send + 'static + Unpin,
    {
        self.0.send_with_timer::<FR, R>(item, fut)
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

impl<F: Flavor> Deref for AsyncTx<F> {
    type Target = ChannelShared<F>;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for AsyncTx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for MAsyncTx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.0.shared
    }
}

impl<T: Send + 'static, F: Flavor<Item = T>> SenderType for AsyncTx<F> {
    type Flavor = F;
    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        AsyncTx::new(shared)
    }
}

impl<F: Flavor> NotCloneable for AsyncTx<F> {}

impl<T: Send + 'static, F: Flavor<Item = T> + FlavorMP> SenderType for MAsyncTx<F> {
    type Flavor = F;
    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        MAsyncTx::new(shared)
    }
}
