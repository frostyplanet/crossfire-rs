use crate::backoff::*;
use crate::{channel::*, AsyncRx, MAsyncRx};
use std::cell::Cell;
use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Single consumer (receiver) that works in blocking context.
///
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// **NOTE: Rx is not Clone, nor Sync.**
/// If you need concurrent access, use [MRx](crate::MRx) instead.
///
/// Rx has Send marker, can be moved to other thread.
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
/// Because Rx does not have Sync marker, using `Arc<Rx>` will lose Send marker.
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
pub struct Rx<T> {
    pub(crate) shared: Arc<ChannelShared<T>>,
    // Remove the Sync marker to prevent being put in Arc
    _phan: PhantomData<Cell<()>>,
    waker_cache: WakerCache<RecvWaker>,
}

unsafe impl<T: Send> Send for Rx<T> {}

impl<T> fmt::Debug for Rx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Rx")
    }
}

impl<T> fmt::Display for Rx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Rx")
    }
}

impl<T> Drop for Rx<T> {
    fn drop(&mut self) {
        self.shared.close_rx();
    }
}

impl<T> From<AsyncRx<T>> for Rx<T> {
    fn from(value: AsyncRx<T>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

impl<T> Rx<T> {
    #[inline(always)]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self { shared, waker_cache: WakerCache::new(), _phan: Default::default() }
    }

    #[inline(always)]
    pub(crate) fn _recv_blocking(&self, deadline: Option<Instant>) -> Result<T, RecvTimeoutError> {
        let shared = &self.shared;
        if shared.is_zero() {
            todo!();
        } else {
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
                        shared.recv_waker_done(&$waker);
                        self.waker_cache.push($waker);
                        return Ok(item);
                    }
                };
            }
            try_recv!();
            let mut backoff = Backoff::new(BackoffConfig::default());
            loop {
                backoff.snooze();
                try_recv!();
                if backoff.is_completed() {
                    break;
                }
            }
            let waker = self.waker_cache.new_blocking();
            debug_assert!(waker.is_waked());
            let mut state;
            'MAIN: loop {
                match shared.reg_recv(&waker) {
                    Ok(_) => {
                        if shared.is_empty() {
                            if shared.is_disconnected() {
                                break 'MAIN;
                            }
                            state = waker.commit_waiting();
                        } else {
                            state = waker.cancel();
                        }
                    }
                    Err(s) => {
                        state = s;
                    }
                }
                while state == WakerState::WAITING as u8 {
                    match check_timeout(deadline) {
                        Ok(None) => {
                            std::thread::park();
                        }
                        Ok(Some(dur)) => {
                            std::thread::park_timeout(dur);
                        }
                        Err(_) => {
                            let _ = shared.abandon_recv_waker(waker);
                            return Err(RecvTimeoutError::Timeout);
                        }
                    }
                    state = waker.get_state();
                }
                if state == WakerState::CLOSED as u8 {
                    break 'MAIN;
                }
                backoff.reset();
                // Waited due to park_timeout or spurious waked
                loop {
                    try_recv!(waker);
                    if backoff.is_completed() {
                        break;
                    }
                    backoff.snooze();
                }
            }
            try_recv!(waker);
            // make sure all msgs received, since we have soonze
            return Err(RecvTimeoutError::Disconnected);
        }
    }

    /// Receive message, will block when channel is empty.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([RecvError]) when all Tx dropped.
    #[inline]
    pub fn recv<'a>(&'a self) -> Result<T, RecvError> {
        self._recv_blocking(None).map_err(|err| match err {
            RecvTimeoutError::Disconnected => RecvError,
            RecvTimeoutError::Timeout => unreachable!(),
        })
    }

    /// Try to receive message, non-blocking.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([TryRecvError::Empty]) when channel is empty.
    ///
    /// returns Err([TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if self.shared.is_zero() {
            todo!();
        } else {
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
    }

    /// Waits for a message to be received from the channel, but only for a limited time.
    /// Will block when channel is empty.
    ///
    /// The behavior is atomic, either successfully polls a message,
    /// or operation cancelled due to timeout.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// returns Err([RecvTimeoutError::Disconnected]) when all Tx dropped and channel is empty.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match Instant::now().checked_add(timeout) {
            Some(deadline) => self._recv_blocking(Some(deadline)),
            None => self.try_recv().map_err(|e| match e {
                TryRecvError::Disconnected => RecvTimeoutError::Disconnected,
                TryRecvError::Empty => RecvTimeoutError::Timeout,
            }),
        }
    }
}

/// Multi-consumer (receiver) that works in blocking context.
///
/// Inherits [`Rx<T>`] and implements [Clone].
/// Additional methods can be accessed through Deref<Target=[ChannelShared]>.
///
/// You can use `into()` to convert it to `Rx<T>`.
pub struct MRx<T>(pub(crate) Rx<T>);

impl<T> fmt::Debug for MRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MRx")
    }
}

impl<T> fmt::Display for MRx<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MRx")
    }
}

unsafe impl<T: Send> Sync for MRx<T> {}

impl<T> MRx<T> {
    #[inline(always)]
    pub(crate) fn new(shared: Arc<ChannelShared<T>>) -> Self {
        Self(Rx::new(shared))
    }
}

impl<T> Clone for MRx<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        let inner = &self.0;
        inner.shared.add_rx();
        Self(Rx::new(inner.shared.clone()))
    }
}

impl<T> Deref for MRx<T> {
    type Target = Rx<T>;

    /// inherit all the functions of [Rx]
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> From<MRx<T>> for Rx<T> {
    fn from(rx: MRx<T>) -> Self {
        rx.0
    }
}

impl<T> From<MAsyncRx<T>> for MRx<T> {
    fn from(value: MAsyncRx<T>) -> Self {
        value.add_rx();
        Self::new(value.shared.clone())
    }
}

/// For writing generic code with MRx & Rx
pub trait BlockingRxTrait<T: Send + 'static>:
    Send + 'static + fmt::Debug + fmt::Display + AsRef<ChannelShared<T>>
{
    /// Receive message, will block when channel is empty.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvError]) when all Tx dropped.
    fn recv<'a>(&'a self) -> Result<T, RecvError>;

    /// Try to receive message, non-blocking.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([TryRecvError::Empty]) when channel is empty.
    ///
    /// Returns Err([TryRecvError::Disconnected]) when all Tx dropped and channel is empty.
    fn try_recv(&self) -> Result<T, TryRecvError>;

    /// Waits for a message to be received from the channel, but only for a limited time.
    /// Will block when channel is empty.
    ///
    /// Returns Ok(T) when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// returns Err([RecvTimeoutError::Disconnected]) when all Tx dropped and channel is empty.
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError>;

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

impl<T: Send + 'static> BlockingRxTrait<T> for Rx<T> {
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

impl<T> Deref for Rx<T> {
    type Target = ChannelShared<T>;

    #[inline(always)]
    fn deref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for Rx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.shared
    }
}

impl<T> AsRef<ChannelShared<T>> for MRx<T> {
    #[inline(always)]
    fn as_ref(&self) -> &ChannelShared<T> {
        &self.0.shared
    }
}
