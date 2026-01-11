use crate::backoff::*;
use crate::flavor::FlavorSelect;
use crate::select::SelectResult;
use crate::{shared::*, trace_log, AsyncRx, MAsyncRx, NotClonable, ReceiverType};
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};

/// A single consumer (receiver) that works in a blocking context.
///
/// Additional methods in [ChannelShared] can be accessed through `Deref`.
///
/// **NOTE**: `Rx` is not `Clone` or `Sync`.
/// If you need concurrent access, use [MRx] instead.
///
/// `Rx` has a `Send` marker and can be moved to other threads.
/// The following code is OK:
///
/// ``` rust
/// use crossfire::*;
/// let (tx, rx) = mpsc::bounded_blocking::<usize>(100);
/// std::thread::spawn(move || {
///     let _ = rx.recv();
/// });
/// drop(tx);
/// ```
///
/// Because `Rx` does not have a `Sync` marker, using `Arc<Rx>` will lose the `Send` marker.
///
/// For your safety, the following code **should not compile**:
///
/// ``` compile_fail
/// use crossfire::*;
/// use std::sync::Arc;
/// let (tx, rx) = mpsc::bounded_blocking(100);
/// let rx = Arc::new(rx);
/// std::thread::spawn(move || {
///     let _ = rx.recv();
/// });
/// drop(tx);
/// ```
pub struct Rx<F: Flavor> {
    pub(crate) shared: Arc<ChannelShared<F>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    waker_cache: WakerCache<()>,
}

unsafe impl<F: Flavor> Send for Rx<F> {}

impl<F: Flavor> fmt::Debug for Rx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Rx")
    }
}

impl<F: Flavor> fmt::Display for Rx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Rx")
    }
}

impl<F: Flavor> Drop for Rx<F> {
    fn drop(&mut self) {
        self.shared.close_rx();
    }
}

impl<F: Flavor> From<AsyncRx<F>> for Rx<F> {
    fn from(value: AsyncRx<F>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

impl<F: Flavor> Rx<F> {
    #[inline(always)]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self { shared, waker_cache: WakerCache::new(), _phan: Default::default() }
    }

    #[inline(always)]
    pub(crate) fn _recv_blocking(
        &self, deadline: Option<Instant>,
    ) -> Result<F::Item, RecvTimeoutError> {
        let shared = &self.shared;
        let mut o_waker: Option<<F::Recv as Registry>::Waker> = None;
        macro_rules! on_recv_no_waker {
            () => {{
                trace_log!("rx: recv");
            }};
        }
        macro_rules! on_recv_waker {
            () => {{
                trace_log!("rx: recv {:?}", o_waker);
                self.recvs.cache_waker(o_waker, &self.waker_cache);
            }};
        }
        macro_rules! try_recv {
            ($handle_waker: block) => {
                if let Some(item) = shared.inner.try_recv() {
                    shared.on_recv();
                    $handle_waker
                    return Ok(item);
                }
            };
        }
        try_recv!({ on_recv_no_waker!() });
        let mut cfg = BackoffConfig::default().limit(shared.backoff_limit);
        if shared.large {
            cfg = cfg.spin(2);
        }
        let mut backoff = Backoff::new(cfg);
        loop {
            let r = backoff.snooze();
            try_recv!({ on_recv_no_waker!() });
            if r {
                break;
            }
        }
        let mut state;
        'MAIN: loop {
            shared.recvs.reg_waker_blocking(&mut o_waker, &self.waker_cache);
            // NOTE: special API before we park
            // because Miri is not happy about ArrayQueue pop ordering, which is not SeqCst
            if let Some(item) = shared.inner.try_recv_final() {
                shared.on_recv();
                trace_log!("rx: recv cancel {:?} Init", o_waker);
                self.recvs.cancel_waker(&mut o_waker);
                return Ok(item);
            }
            state = shared.recvs.commit_waiting(&o_waker);
            trace_log!("rx: {:?} commit_waiting state={}", o_waker, state);
            if shared.is_tx_closed() {
                break 'MAIN;
            }
            while state < WakerState::Woken as u8 {
                match check_timeout(deadline) {
                    Ok(None) => {
                        std::thread::park();
                    }
                    Ok(Some(dur)) => {
                        std::thread::park_timeout(dur);
                    }
                    Err(_) => {
                        shared.abandon_recv_waker(o_waker.as_ref().unwrap());
                        return Err(RecvTimeoutError::Timeout);
                    }
                }
                state = self.recvs.get_waker_state(&o_waker, Ordering::SeqCst);
                trace_log!("rx: after park state={}", state);
            }
            if state == WakerState::Closed as u8 {
                break 'MAIN;
            }
            backoff.reset();
            loop {
                try_recv!({ on_recv_waker!() });
                if backoff.snooze() {
                    break;
                }
            }
        }
        try_recv!({ on_recv_waker!() });
        // make sure all msgs received, since we have soonze
        return Err(RecvTimeoutError::Disconnected);
    }

    /// Receives a message from the channel. This method will block until a message is received or the channel is closed.
    ///
    /// Returns `Ok(T)` on success.
    ///
    /// Returns Err([RecvError]) if the sender has been dropped.
    #[inline]
    pub fn recv<'a>(&'a self) -> Result<F::Item, RecvError> {
        self._recv_blocking(None).map_err(|err| match err {
            RecvTimeoutError::Disconnected => RecvError,
            RecvTimeoutError::Timeout => unreachable!(),
        })
    }

    /// Attempts to receive a message from the channel without blocking.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([TryRecvError::Empty]) if the channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) if the sender has been dropped and the channel is empty.
    #[inline]
    pub fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        self.shared.try_recv()
    }

    /// Receives a message from the channel with a timeout.
    /// Will block when channel is empty.
    ///
    /// The behavior is atomic: the message is either received successfully or the operation is canceled due to a timeout.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// Returns Err([RecvTimeoutError::Disconnected]) if the sender has been dropped and the channel is empty.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<F::Item, RecvTimeoutError> {
        match Instant::now().checked_add(timeout) {
            Some(deadline) => self._recv_blocking(Some(deadline)),
            None => self.try_recv().map_err(|e| match e {
                TryRecvError::Disconnected => RecvTimeoutError::Disconnected,
                TryRecvError::Empty => RecvTimeoutError::Timeout,
            }),
        }
    }

    /// Return true if the other side has closed
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_tx_closed()
    }

    /// This method use with select, guarantee non-blocking
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
}

/// A multi-consumer (receiver) that works in a blocking context.
///
/// Inherits from [`Rx<F>`] and implements `Clone`.
/// Additional methods can be accessed through `Deref<Target=[ChannelShared]>`.
///
/// You can use `into()` to convert it to `Rx<F>`.
pub struct MRx<F: Flavor>(pub(crate) Rx<F>);

impl<F: Flavor> fmt::Debug for MRx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MRx")
    }
}

impl<F: Flavor> fmt::Display for MRx<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MRx")
    }
}

unsafe impl<F: Flavor> Sync for MRx<F> {}

impl<F: Flavor> MRx<F> {
    #[inline(always)]
    pub(crate) fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self(Rx::new(shared))
    }
}

impl<F: Flavor> Clone for MRx<F> {
    #[inline(always)]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_rx();
        Self(Rx::new(inner.shared.clone()))
    }
}

impl<F: Flavor> Deref for MRx<F> {
    type Target = Rx<F>;

    /// Inherits all the functions of [Rx].
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<F: Flavor> From<MRx<F>> for Rx<F> {
    fn from(rx: MRx<F>) -> Self {
        rx.0
    }
}

impl<F: Flavor> From<MAsyncRx<F>> for MRx<F> {
    fn from(value: MAsyncRx<F>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

/// For writing generic code with MRx & Rx
pub trait BlockingRxTrait<T: Send + 'static>: Send + 'static + fmt::Debug + fmt::Display {
    /// Receives a message from the channel. This method will block until a message is received or the channel is closed.
    ///
    /// Returns `Ok(T)` on success.
    ///
    /// Returns Err([RecvError]) if the sender has been dropped.
    fn recv<'a>(&'a self) -> Result<T, RecvError>;

    /// Attempts to receive a message from the channel without blocking.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([TryRecvError::Empty]) if the channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) if the sender has been dropped and the channel is empty.
    fn try_recv(&self) -> Result<T, TryRecvError>;

    /// Receives a message from the channel with a timeout.
    /// Will block when channel is empty.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// Returns Err([RecvTimeoutError::Disconnected]) if the sender has been dropped and the channel is empty.
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError>;

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

impl<F: Flavor> BlockingRxTrait<F::Item> for Rx<F> {
    #[inline(always)]
    fn clone_to_vec(self, _count: usize) -> Vec<Self> {
        assert_eq!(_count, 1);
        vec![self]
    }

    #[inline(always)]
    fn recv<'a>(&'a self) -> Result<F::Item, RecvError> {
        Rx::recv(self)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        Rx::try_recv(self)
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<F::Item, RecvTimeoutError> {
        Rx::recv_timeout(self, timeout)
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
        self.as_ref().is_tx_closed()
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

impl<F: Flavor> BlockingRxTrait<F::Item> for MRx<F> {
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
    fn recv<'a>(&'a self) -> Result<F::Item, RecvError> {
        self.0.recv()
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        self.0.try_recv()
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<F::Item, RecvTimeoutError> {
        self.0.recv_timeout(timeout)
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
        self.as_ref().is_tx_closed()
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

impl<F: Flavor> Deref for Rx<F> {
    type Target = ChannelShared<F>;

    #[inline(always)]
    fn deref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for Rx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.shared
    }
}

impl<F: Flavor> AsRef<ChannelShared<F>> for MRx<F> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<F> {
        &self.0.shared
    }
}

impl<T: Send + Unpin + 'static, F: Flavor<Item = T>> ReceiverType for Rx<F> {
    type Flavor = F;
    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self::new(shared)
    }
}

impl<F: Flavor> NotClonable for Rx<F> {}

impl<T: Send + Unpin + 'static, F: Flavor<Item = T>> ReceiverType for MRx<F> {
    type Flavor = F;

    #[inline(always)]
    fn new(shared: Arc<ChannelShared<F>>) -> Self {
        Self::new(shared)
    }
}
