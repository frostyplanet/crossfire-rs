use crate::flavor::{FlavorMC, FlavorSelect};
use crate::select::SelectResult;
use crate::stream::AsyncStream;
#[cfg(feature = "trace_log")]
use crate::tokio_task_id;
use crate::{shared::*, trace_log, MRx, NotCloneable, ReceiverType, Rx};
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A single consumer (receiver) that works in an async context.
///
/// Additional methods in [ChannelShared] can be accessed through `Deref`.
///
/// `AsyncRx` can be converted into `Rx` via the `From` trait,
/// which means you can have two types of receivers, both within async and
/// blocking contexts, for the same channel.
///
/// **NOTE**: `AsyncRx` is not `Clone` or `Sync`.
/// If you need concurrent access, use [MAsyncRx] instead.
///
/// `AsyncRx` has a `Send` marker and can be moved to other coroutines.
/// The following code is OK:
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
/// Because `AsyncRx` does not have a `Sync` marker, using `Arc<AsyncRx>` will lose the `Send` marker.
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
pub struct AsyncRx<F: Flavor> {
    pub(crate) shared: Arc<ChannelShared<F>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
}

unsafe impl<F: Flavor> Send for AsyncRx<F> {}

impl<F: Flavor> fmt::Debug for AsyncRx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncRx{:p}", self)
    }
}

impl<F: Flavor> fmt::Display for AsyncRx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncRx{:p}", self)
    }
}

impl<F: Flavor> Drop for AsyncRx<F> {
    #[inline(always)]
    fn drop(&mut self) {
        self.shared.close_rx();
    }
}

impl<F: Flavor> From<Rx<F>> for AsyncRx<F> {
    fn from(value: Rx<F>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

impl<F: Flavor> AsyncRx<F> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self { shared, _phan: Default::default() }
    }

    /// Return true if the other side has closed
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_tx_closed()
    }

    #[inline]
    pub fn into_stream(self) -> AsyncStream<F> {
        AsyncStream::new(self)
    }

    #[inline]
    pub fn into_blocking(self) -> Rx<F> {
        self.into()
    }
}

impl<F: Flavor> AsyncRx<F>
where
    F::Item: Send + 'static,
{
    /// Receives a message from the channel. This method will await until a message is received or the channel is closed.
    ///
    /// This function is cancellation-safe, so it's safe to use with `timeout()` and the `select!` macro.
    /// When a [RecvFuture] is dropped, no message will be received from the channel.
    ///
    /// For timeout scenarios, there's an alternative: [AsyncRx::recv_timeout()].
    ///
    /// Returns `Ok(T)` on success.
    ///
    /// Returns Err([RecvError]) if the sender has been dropped.
    #[inline(always)]
    pub fn recv<'a>(&'a self) -> RecvFuture<'a, F> {
        RecvFuture { rx: self, waker: None }
    }

    /// Receives a message from the channel with a timeout.
    /// Will await when channel is empty.
    ///
    /// The behavior is atomic: the message is either received successfully or the operation is canceled due to a timeout.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// Returns Err([RecvTimeoutError::Disconnected]) if the sender has been dropped and the channel is empty.
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline]
    pub fn recv_timeout<'a>(
        &'a self, duration: std::time::Duration,
    ) -> RecvTimeoutFuture<'a, F, ()> {
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
        self.recv_with_timer(sleep)
    }

    /// Receives a message from the channel with a custom timer function (from other async runtime).
    ///
    /// The behavior is atomic: the message is either received successfully or the operation is canceled due to a timeout.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// Returns Err([RecvTimeoutError::Disconnected]) if the sender has been dropped and the channel is empty.
    ///
    /// # Argument:
    ///
    /// * `fut`: The sleep function. It's possible to wrap this function with cancelable handle,
    ///   you can control when to stop polling. the return value of `fut` is ignore.
    ///   We add generic `R` just in order to support smol::Timer
    ///
    /// # Example:
    ///
    /// ```ignore
    /// extern crate smol;
    /// use std::time::Duration;
    /// use crossfire::*;
    /// async fn foo() {
    ///     let (tx, rx) = mpmc::bounded_async::<usize>(10);
    ///     match rx.recv_with_timer(smol::Timer::after(Duration::from_secs(1))).await {
    ///         Ok(_item)=>{
    ///             println!("message recv");
    ///         }
    ///         Err(RecvTimeoutError::Timeout)=>{
    ///             println!("timeout");
    ///         }
    ///         Err(RecvTimeoutError::Disconnected)=>{
    ///             println!("sender-side closed");
    ///         }
    ///     }
    /// }
    /// ```
    #[inline]
    pub fn recv_with_timer<'a, FR, R>(&'a self, fut: FR) -> RecvTimeoutFuture<'a, F, R>
    where
        FR: Future<Output = R> + 'static,
    {
        RecvTimeoutFuture { rx: self, waker: None, sleep: Box::pin(fut) }
    }

    /// Attempts to receive a message from the channel without blocking.
    ///
    /// Returns `Ok(T)` on successful.
    ///
    /// Returns Err([TryRecvError::Empty]) if the channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) if the sender has been dropped and the channel is empty.
    #[inline(always)]
    pub fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        self.shared.try_recv()
    }

    /// This method use with [select](crate::select::Select), guarantee non-blocking
    ///
    /// # Panics
    ///
    /// Panics if SelectResult from other receiver is passed.
    #[inline(always)]
    pub fn read_select(&self, result: SelectResult) -> Result<F::Item, RecvError>
    where
        F: FlavorSelect,
    {
        assert_eq!(
            self as *const Self as *const u8, result.channel,
            "invalid use select with another channel"
        );
        self.as_ref().read_with_token(result.token)
    }

    /// Internal function might change in the future. For public version, use AsyncStream::poll_item() instead
    ///
    /// Returns `Ok(T)` on successful.
    ///
    /// Return Err([TryRecvError::Empty]) for Poll::Pending case.
    ///
    /// Return Err([TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    #[inline(always)]
    pub(crate) fn poll_item<const STREAM: bool>(
        &self, ctx: &mut Context, o_waker: &mut Option<<F::Recv as Registry>::Waker>,
    ) -> Result<F::Item, TryRecvError> {
        let shared = &self.shared;
        // When the result is not TryRecvError::Empty,
        // make sure always take the o_waker out and abandon,
        // to skip the timeout cleaning logic in Drop.
        macro_rules! on_recv_no_waker {
            () => {{
                trace_log!("rx{:?}: recv", tokio_task_id!());
            }};
        }
        macro_rules! on_recv_waker {
            ($state: expr) => {{
                trace_log!("rx{:?}: recv {:?} {:?}", tokio_task_id!(), o_waker, $state);
                shared.recvs.cancel_waker(o_waker);
            }};
        }
        macro_rules! try_recv {
            ($recv_func: ident => $waker_handle: block) => {
                if let Some(item) = shared.inner.$recv_func() {
                    shared.on_recv();
                    $waker_handle
                    return Ok(item);
                }
            };
        }
        loop {
            if o_waker.is_none() {
                try_recv!(try_recv=>{ on_recv_no_waker!()});
                // First call
                if let Some(mut backoff) = shared.get_async_backoff() {
                    loop {
                        let complete = backoff.spin();
                        try_recv!(try_recv=>{ on_recv_no_waker!()});
                        if complete {
                            break;
                        }
                    }
                }
            } else {
                try_recv!(try_recv => {on_recv_waker!(WakerState::Woken)});
            }
            if shared.recvs.reg_waker_async(ctx, o_waker).is_some() {
                break;
            }
            // NOTE: The other side put something while reg_send and did not see the waker,
            // should check the channel again, otherwise might incur a dead lock.
            // NOTE: special API before we park
            // because Miri is not happy about ArrayQueue pop ordering, which is not SeqCst
            try_recv!(try_recv_final =>{ on_recv_waker!(WakerState::Init)});
            if !STREAM {
                let state = shared.recvs.commit_waiting(o_waker);
                trace_log!("rx{:?}: commit_waiting {:?} {}", tokio_task_id!(), o_waker, state);
                if state == WakerState::Woken as u8 {
                    continue;
                }
            }
            break;
        }
        if shared.is_tx_closed() {
            try_recv!(try_recv =>{ on_recv_waker!(WakerState::Closed)});
            trace_log!("rx{:?}: disconnected {:?}", tokio_task_id!(), o_waker);
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

/// A fixed-sized future object constructed by [AsyncRx::recv()]
#[must_use]
pub struct RecvFuture<'a, F: Flavor> {
    rx: &'a AsyncRx<F>,
    waker: Option<<F::Recv as Registry>::Waker>,
}

unsafe impl<F: Flavor> Send for RecvFuture<'_, F> {}

impl<F: Flavor> Drop for RecvFuture<'_, F> {
    #[inline]
    fn drop(&mut self) {
        if let Some(waker) = self.waker.as_ref() {
            self.rx.shared.abandon_recv_waker(waker);
        }
    }
}

impl<F: Flavor> Future for RecvFuture<'_, F>
where
    F::Item: Send + 'static,
{
    type Output = Result<F::Item, RecvError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.rx.poll_item::<false>(ctx, &mut _self.waker) {
            Err(e) => {
                if !e.is_empty() {
                    let _ = _self.waker.take();
                    Poll::Ready(Err(RecvError {}))
                } else {
                    Poll::Pending
                }
            }
            Ok(item) => {
                debug_assert!(_self.waker.is_none());
                Poll::Ready(Ok(item))
            }
        }
    }
}

/// A fixed-sized future object constructed by [AsyncRx::recv_timeout()]
#[must_use]
pub struct RecvTimeoutFuture<'a, F: Flavor, R> {
    rx: &'a AsyncRx<F>,
    waker: Option<<F::Recv as Registry>::Waker>,
    sleep: Pin<Box<dyn Future<Output = R>>>,
}

unsafe impl<F: Flavor, R> Send for RecvTimeoutFuture<'_, F, R> {}

impl<F: Flavor, R> Drop for RecvTimeoutFuture<'_, F, R> {
    #[inline]
    fn drop(&mut self) {
        if let Some(waker) = self.waker.as_ref() {
            self.rx.shared.abandon_recv_waker(waker);
        }
    }
}

impl<F: Flavor, R> Future for RecvTimeoutFuture<'_, F, R>
where
    F::Item: Send + 'static,
{
    type Output = Result<F::Item, RecvTimeoutError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        match _self.rx.poll_item::<false>(ctx, &mut _self.waker) {
            Err(TryRecvError::Empty) => {
                if _self.sleep.as_mut().poll(ctx).is_ready() {
                    return Poll::Ready(Err(RecvTimeoutError::Timeout));
                }
                Poll::Pending
            }
            Err(TryRecvError::Disconnected) => Poll::Ready(Err(RecvTimeoutError::Disconnected)),
            Ok(item) => Poll::Ready(Ok(item)),
        }
    }
}

/// For writing generic code with MAsyncRx & AsyncRx
pub trait AsyncRxTrait<T: Send + 'static>: Send + 'static + fmt::Debug + fmt::Display {
    /// Receive message, will await when channel is empty.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// returns Err([RecvError]) when all Tx dropped.
    fn recv(&self) -> impl Future<Output = Result<T, RecvError>> + Send
    where
        T: Send + 'static;

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
    fn recv_timeout<'a>(
        &'a self, timeout: std::time::Duration,
    ) -> impl Future<Output = Result<T, RecvTimeoutError>> + Send
    where
        T: Send + 'static;

    /// Receives a message from the channel with a custom timer function (from other async runtime).
    ///
    /// The behavior is atomic: the message is either received successfully or the operation is canceled due to a timeout.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// Returns Err([RecvTimeoutError::Disconnected]) if the sender has been dropped and the channel is empty.
    ///
    /// # Argument:
    ///
    /// * `fut`: The sleep function. It's possible to wrap this function with cancelable handle,
    ///   you can control when to stop polling. the return value of `fut` is ignore.
    ///   We add generic `R` just in order to support smol::Timer.
    fn recv_with_timer<FR, R>(
        &self, fut: FR,
    ) -> impl Future<Output = Result<T, RecvTimeoutError>> + Send
    where
        FR: Future<Output = R> + 'static,
        T: Send + 'static;

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

    fn to_stream(self) -> Pin<Box<dyn futures_core::stream::Stream<Item = T>>>;

    fn get_wakers_count(&self) -> (usize, usize);
}

impl<F: Flavor> AsyncRxTrait<F::Item> for AsyncRx<F>
where
    F::Item: Send + 'static,
{
    #[inline(always)]
    fn clone_to_vec(self, _count: usize) -> Vec<Self> {
        assert_eq!(_count, 1);
        vec![self]
    }

    #[inline(always)]
    fn recv(&self) -> impl Future<Output = Result<F::Item, RecvError>> + Send
    where
        F::Item: Send + 'static,
    {
        AsyncRx::recv(self)
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn recv_timeout(
        &self, duration: std::time::Duration,
    ) -> impl Future<Output = Result<F::Item, RecvTimeoutError>> + Send
    where
        F::Item: Send + 'static,
    {
        AsyncRx::recv_timeout(self, duration)
    }

    #[inline(always)]
    fn recv_with_timer<FR, R>(
        &self, fut: FR,
    ) -> impl Future<Output = Result<F::Item, RecvTimeoutError>> + Send
    where
        FR: Future<Output = R> + 'static,
        F::Item: Send + 'static,
    {
        AsyncRx::recv_with_timer(self, fut)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        AsyncRx::<F>::try_recv(self)
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
        self.as_ref().get_tx_count() == 0
    }

    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.as_ref().get_tx_count()
    }

    #[inline(always)]
    fn get_rx_count(&self) -> usize {
        self.as_ref().get_rx_count()
    }

    #[inline(always)]
    fn to_stream(self) -> Pin<Box<dyn futures_core::stream::Stream<Item = F::Item>>> {
        Box::pin(self.into_stream())
    }

    fn get_wakers_count(&self) -> (usize, usize) {
        self.as_ref().get_wakers_count()
    }
}

/// A multi-consumer (receiver) that works in an async context.
///
/// Inherits from [`AsyncRx<F>`] and implements `Clone`.
/// Additional methods in [ChannelShared] can be accessed through `Deref`.
///
/// You can use `into()` to convert it to `AsyncRx<F>`.
///
/// `MAsyncRx` can be converted into `MRx` via the `From` trait,
/// which means you can have two types of receivers, both within async and
/// blocking contexts, for the same channel.
pub struct MAsyncRx<F: Flavor>(pub(crate) AsyncRx<F>);

impl<F: Flavor> fmt::Debug for MAsyncRx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncRx{:p}", self)
    }
}

impl<F: Flavor> fmt::Display for MAsyncRx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MAsyncRx{:p}", self)
    }
}

unsafe impl<F: Flavor> Sync for MAsyncRx<F> {}

impl<F: Flavor> Clone for MAsyncRx<F> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_rx();
        Self(AsyncRx::new(inner.shared.clone()))
    }
}

impl<F: Flavor> From<MAsyncRx<F>> for AsyncRx<F> {
    fn from(rx: MAsyncRx<F>) -> Self {
        rx.0
    }
}

impl<F: Flavor + FlavorMC> MAsyncRx<F> {
    #[inline]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self(AsyncRx::new(shared))
    }
}

impl<F: Flavor> MAsyncRx<F> {
    #[inline]
    pub fn into_stream(self) -> AsyncStream<F> {
        AsyncStream::new(self.0)
    }

    #[inline]
    pub fn into_blocking(self) -> MRx<F> {
        self.into()
    }
}

impl<F: Flavor> Deref for MAsyncRx<F> {
    type Target = AsyncRx<F>;

    /// inherit all the functions of [AsyncRx]
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: Flavor> From<MRx<F>> for MAsyncRx<F> {
    fn from(value: MRx<F>) -> Self {
        value.add_rx();
        Self(AsyncRx::new(value.shared.clone()))
    }
}

impl<F: Flavor + FlavorMC> AsyncRxTrait<F::Item> for MAsyncRx<F>
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
    fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        self.0.try_recv()
    }

    #[inline(always)]
    fn recv(&self) -> impl Future<Output = Result<F::Item, RecvError>> + Send
    where
        F::Item: Send + 'static,
    {
        self.0.recv()
    }

    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline(always)]
    fn recv_timeout<'a>(
        &'a self, duration: std::time::Duration,
    ) -> impl Future<Output = Result<F::Item, RecvTimeoutError>> + Send
    where
        F::Item: Send + 'static,
    {
        self.0.recv_timeout(duration)
    }

    #[inline(always)]
    fn recv_with_timer<FR, R>(
        &self, fut: FR,
    ) -> impl Future<Output = Result<F::Item, RecvTimeoutError>> + Send
    where
        FR: Future<Output = R> + 'static,
        F::Item: Send + 'static,
    {
        self.0.recv_with_timer(fut)
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
        self.as_ref().get_tx_count() == 0
    }

    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.as_ref().get_tx_count()
    }

    #[inline(always)]
    fn get_rx_count(&self) -> usize {
        self.as_ref().get_rx_count()
    }

    #[inline(always)]
    fn to_stream(self) -> Pin<Box<dyn futures_core::stream::Stream<Item = F::Item>>> {
        Box::pin(self.into_stream())
    }

    fn get_wakers_count(&self) -> (usize, usize) {
        self.as_ref().get_wakers_count()
    }
}

impl<F: Flavor> Deref for AsyncRx<F> {
    type Target = ChannelShared<F>;
    #[inline(always)]
    fn deref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for AsyncRx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for MAsyncRx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.0.shared
    }
}

impl<T: Send + 'static, F: Flavor<Item = T>> ReceiverType for AsyncRx<F> {
    type Flavor = F;
    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        AsyncRx::new(shared)
    }
}

impl<F: Flavor> NotCloneable for AsyncRx<F> {}

impl<T: Send + 'static, F: Flavor<Item = T> + FlavorMC> ReceiverType for MAsyncRx<F> {
    type Flavor = F;
    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        MAsyncRx::new(shared)
    }
}
