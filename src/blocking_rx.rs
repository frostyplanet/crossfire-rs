use crate::backoff::*;
use crate::{shared::*, trace_log, AsyncRx, MAsyncRx};
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
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
/// let (tx, rx) = mpsc::bounded_blocking::<usize>(100);
/// let rx = Arc::new(rx);
/// std::thread::spawn(move || {
///     let _ = rx.recv();
/// });
/// drop(tx);
/// ```
pub struct Rx<T: Send + 'static> {
    pub(crate) shared: Arc<ChannelShared<T>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    waker_cache: WakerCache<()>,
    flavor: *const (),
    _try_recv: TryRecvFunc<T>,
    _recv_blocking: RecvBlocking<T>,
}

unsafe impl<T: Send> Send for Rx<T> {}

impl<T: Send + 'static> fmt::Debug for Rx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Rx")
    }
}

impl<T: Send + 'static> fmt::Display for Rx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Rx")
    }
}

impl<T: Send + 'static> Drop for Rx<T> {
    fn drop(&mut self) {
        self.shared.close_rx();
    }
}

impl<T: Send + 'static> From<AsyncRx<T>> for Rx<T> {
    fn from(value: AsyncRx<T>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

impl<T: Send + 'static> Rx<T> {
    #[inline(always)]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self {
            waker_cache: WakerCache::new(),
            _phan: Default::default(),
            flavor: shared.get_flavor_ptr(),
            _try_recv: shared._try_recv,
            _recv_blocking: shared._recv_blocking,
            shared,
        }
    }

    #[inline(always)]
    pub(crate) fn recv_blocking<F: FlavorImpl<T>>(
        shared: &ChannelShared<T>, flavor: &F, deadline: Option<Instant>,
        waker_cache: &WakerCache<()>,
    ) -> Result<T, bool> {
        macro_rules! on_recv_no_waker {
            () => {{
                trace_log!("rx: recv");
            }};
        }
        macro_rules! on_recv_waker {
            ($waker: expr) => {{
                trace_log!("rx: recv {:?}", $waker);
                waker_cache.push($waker);
            }};
        }
        macro_rules! try_recv {
            ($handle_waker: block) => {
                if let Some(item) = flavor.try_recv() {
                    shared.on_recv_shim::<F>(flavor);
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
        let waker = waker_cache.new_blocking(());
        let mut state;
        'MAIN: loop {
            if waker.get_state() == WakerState::Woken as u8 {
                waker.reset_init();
            }
            shared.reg_recv(&waker);
            // NOTE: special API before we park
            // because Miri is not happy about ArrayQueue pop ordering, which is not SeqCst
            if let Some(item) = flavor.try_recv_final() {
                shared.on_recv_shim::<F>(flavor);
                trace_log!("rx: recv cancel {:?} Init", waker);
                shared.recvs.cancel_waker(&waker);
                return Ok(item);
            }
            state = shared.recvs.commit_waiting(&waker);
            trace_log!("rx: {:?} commit_waiting state={}", waker, state);
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
                        let _ = shared.abandon_recv_waker(waker);
                        return Err(false);
                    }
                }
                state = waker.get_state();
            }
            if state == WakerState::Closed as u8 {
                break 'MAIN;
            }
            backoff.reset();
            loop {
                try_recv!({ on_recv_waker!(waker) });
                if backoff.snooze() {
                    break;
                }
            }
        }
        try_recv!({ on_recv_waker!(waker) });
        // make sure all msgs received, since we have soonze
        return Err(true);
    }

    /// Receives a message from the channel. This method will block until a message is received or the channel is closed.
    ///
    /// Returns `Ok(T)` on success.
    ///
    /// Returns Err([RecvError]) if the sender has been dropped.
    #[inline]
    pub fn recv<'a>(&'a self) -> Result<T, RecvError> {
        match unsafe { (self._recv_blocking)(&self.shared, self.flavor, None, &self.waker_cache) } {
            Ok(item) => Ok(item),
            Err(true) => Err(RecvError),
            Err(false) => unreachable!(),
        }
    }

    /// Attempts to receive a message from the channel without blocking.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([TryRecvError::Empty]) if the channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) if the sender has been dropped and the channel is empty.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(item) = unsafe { (self._try_recv)(&self.shared, self.flavor) } {
            // in try_recv_shim already call on_recv
            return Ok(item);
        } else {
            if self.shared.is_tx_closed() {
                return Err(TryRecvError::Disconnected);
            }
            return Err(TryRecvError::Empty);
        }
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
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match Instant::now().checked_add(timeout) {
            None => self.try_recv().map_err(|e| match e {
                TryRecvError::Disconnected => RecvTimeoutError::Disconnected,
                TryRecvError::Empty => RecvTimeoutError::Timeout,
            }),
            Some(deadline) => {
                match unsafe {
                    (self._recv_blocking)(
                        &self.shared,
                        self.flavor,
                        Some(deadline),
                        &self.waker_cache,
                    )
                } {
                    Ok(item) => Ok(item),
                    Err(true) => Err(RecvTimeoutError::Disconnected),
                    Err(false) => Err(RecvTimeoutError::Timeout),
                }
            }
        }
    }

    /// Return true if the other side has closed
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.shared.is_tx_closed()
    }
}

/// A multi-consumer (receiver) that works in a blocking context.
///
/// Inherits from [`Rx<T>`] and implements `Clone`.
/// Additional methods can be accessed through `Deref<Target=[ChannelShared]>`.
///
/// You can use `into()` to convert it to `Rx<T>`.
pub struct MRx<T: Send + 'static>(pub(crate) Rx<T>);

impl<T: Send + 'static> fmt::Debug for MRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MRx")
    }
}

impl<T: Send + 'static> fmt::Display for MRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MRx")
    }
}

unsafe impl<T: Send> Sync for MRx<T> {}

impl<T: Send + 'static> MRx<T> {
    #[inline(always)]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self(Rx::new(shared))
    }
}

impl<T: Send + 'static> Clone for MRx<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_rx();
        Self(Rx::new(inner.shared.clone()))
    }
}

impl<T: Send + 'static> Deref for MRx<T> {
    type Target = Rx<T>;

    /// Inherits all the functions of [Rx].
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Send + 'static> From<MRx<T>> for Rx<T> {
    fn from(rx: MRx<T>) -> Self {
        rx.0
    }
}

impl<T: Send + 'static> From<MAsyncRx<T>> for MRx<T> {
    fn from(value: MAsyncRx<T>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

/// For writing generic code with MRx & Rx
pub trait BlockingRxTrait<T: Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared<T>> + Sized
{
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

    fn clone_to_vec(self, count: usize) -> Vec<Self>;
}

impl<T: Send + 'static> BlockingRxTrait<T> for Rx<T> {
    #[inline(always)]
    fn clone_to_vec(self, _count: usize) -> Vec<Self> {
        assert_eq!(_count, 1);
        vec![self]
    }

    #[inline(always)]
    fn recv<'a>(&'a self) -> Result<T, RecvError> {
        Rx::recv(self)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        Rx::try_recv(self)
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        Rx::recv_timeout(self, timeout)
    }
}

impl<T: Send + 'static> BlockingRxTrait<T> for MRx<T> {
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
    fn recv<'a>(&'a self) -> Result<T, RecvError> {
        self.0.recv()
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.0.try_recv()
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.0.recv_timeout(timeout)
    }
}

impl<T: Send + 'static> Deref for Rx<T> {
    type Target = ChannelShared<T>;

    #[inline(always)]
    fn deref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T: Send + 'static> AsRef<ChannelShared<T>> for Rx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T: Send + 'static> AsRef<ChannelShared<T>> for MRx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.0.shared
    }
}
