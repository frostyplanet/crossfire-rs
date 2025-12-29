//! OneShot channel support both thread and async
//!
//! NOTE: In order to reduce initialization and teardown cost, this module use specialized sender [TxOneshot] and
//! receiver [RxOneshot] types.
//!
//! # Examples
//!
//! ## Thread Context
//!
//! ```
//! use crossfire::oneshot::oneshot;
//!
//! let (tx, rx) = oneshot();
//!
//! std::thread::spawn(move || {
//!     tx.send("Hello from sender!");
//! });
//!
//! let received = rx.recv().unwrap();
//! assert_eq!(received, "Hello from sender!");
//! ```
//!
//! ## Async Context
//!
//! ```
//! use crossfire::oneshot::oneshot;
//!
//! async fn example() {
//!     let (tx, rx) = oneshot();
//!
//!     tokio::spawn(async move {
//!         tx.send("Hello from async sender!");
//!     });
//!
//!     let received = rx.await.unwrap();
//!     assert_eq!(received, "Hello from async sender!");
//! }
//! ```

use crate::shared::*;
use crate::trace_log;
use core::cell::UnsafeCell;
use core::mem::transmute;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::thread;
use std::time::Instant;

const EXIST_FLAG: u8 = 0x1;
const WAKER_SET_FLAG: u8 = 0x2;
const CLOSE_FLAG: u8 = 0x4;

struct OneShot<T> {
    state: AtomicU8,
    value: UnsafeCell<Option<T>>,
}

unsafe impl<T: Send> Send for OneShot<T> {}
unsafe impl<T: Send> Sync for OneShot<T> {}

impl<T> OneShot<T> {
    #[inline]
    pub fn new() -> Self {
        Self { value: UnsafeCell::new(None), state: AtomicU8::new(0) }
    }

    #[inline(always)]
    fn value_mut(&self) -> &mut Option<T> {
        unsafe { transmute(self.value.get()) }
    }

    #[inline(always)]
    fn set_state(&self, flag: u8) -> u8 {
        self.state.fetch_or(flag, Ordering::AcqRel)
    }

    #[inline(always)]
    fn unset_state(&self, flag: u8) -> u8 {
        self.state.fetch_and(!flag, Ordering::AcqRel)
    }

    #[inline(always)]
    fn _try_recv(&self, order: Ordering) -> Result<T, u8> {
        let state = self.state.load(order);
        if state & EXIST_FLAG > 0 {
            if let Some(item) = self._consume_value() {
                Ok(item)
            } else {
                Err(state | CLOSE_FLAG)
            }
        } else {
            Err(state)
        }
    }

    #[inline(always)]
    fn _consume_value(&self) -> Option<T> {
        self.value_mut().take()
    }

    #[inline(always)]
    fn send_value(&self, item: T) -> u8 {
        self.value_mut().replace(item);
        self.set_state(EXIST_FLAG)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        state & EXIST_FLAG == 0
    }
}

/// In order to save some extra cost for async,
/// use special shared struct instead of ChannelShared,
/// and we opt out stander AsyncRx/Rx.
struct Shared<T> {
    inner: OneShot<T>,
    waker: UnsafeCell<Option<ThinWaker>>,
}

impl<T> Shared<T> {
    #[inline]
    fn get_waker(&self) -> &mut Option<ThinWaker> {
        unsafe { transmute(self.waker.get()) }
    }
}

unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

/// Sender for oneshot channel
pub struct TxOneshot<T>(Option<Arc<Shared<T>>>);

impl<T> TxOneshot<T> {
    /// Consume itself and send the item
    #[inline]
    pub fn send(mut self, item: T) {
        if let Some(shared) = self.0.take() {
            let state = shared.inner.send_value(item);
            if state & WAKER_SET_FLAG > 0 {
                if let Some(waker) = shared.get_waker().as_ref() {
                    trace_log!("tx: wake");
                    waker.wake_by_ref();
                } else {
                    trace_log!("tx: wake flag is set but no waker");
                }
            } else {
                trace_log!("tx: set value");
            }
        }
    }
}

impl<T> Drop for TxOneshot<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(shared) = self.0.take() {
            let state = shared.inner.set_state(CLOSE_FLAG);
            if state & WAKER_SET_FLAG > 0 {
                if let Some(waker) = shared.get_waker().as_ref() {
                    trace_log!("drop noti");
                    waker.wake_by_ref();
                } else {
                    trace_log!("drop missing waker but flag set");
                }
            } else {
                trace_log!("drop no waker");
            }
        }
    }
}

/// Receiver for oneshot channel
#[must_use]
pub struct RxOneshot<T>(Arc<Shared<T>>);

impl<T> RxOneshot<T> {
    /// NOTE: this will blocking current thread
    #[inline]
    pub fn recv(self) -> Result<T, RecvError> {
        if let Ok(item) = self._recv_blocking(None) {
            return Ok(item);
        }
        Err(RecvError)
    }

    /// NOTE: this will blocking current thread with a timeout
    #[inline]
    pub fn recv_timeout(self) -> Result<T, RecvTimeoutError> {
        match self._recv_blocking(None) {
            Ok(item) => Ok(item),
            Err(true) => Err(RecvTimeoutError::Timeout),
            Err(false) => Err(RecvTimeoutError::Disconnected),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.0.inner.is_empty()
    }

    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match self.0.inner._try_recv(Ordering::Acquire) {
            Ok(item) => return Ok(item),
            Err(state) => {
                if state & CLOSE_FLAG > 0 {
                    return Err(TryRecvError::Disconnected);
                } else {
                    return Err(TryRecvError::Empty);
                }
            }
        }
    }

    #[inline]
    pub async fn recv_async(self) -> Result<T, RecvError> {
        self.await
    }

    #[inline(always)]
    pub(crate) fn _recv_blocking(&self, deadline: Option<Instant>) -> Result<T, bool> {
        let shared = &self.0;
        loop {
            match shared.inner._try_recv(Ordering::SeqCst) {
                Ok(item) => {
                    trace_log!("poll value");
                    return Ok(item);
                }
                Err(mut state) => {
                    if state & CLOSE_FLAG > 0 {
                        trace_log!("poll closed");
                        return Err(false);
                    }
                    if state & WAKER_SET_FLAG == 0 {
                        shared.get_waker().replace(ThinWaker::Blocking(thread::current()));
                        state = shared.inner.set_state(WAKER_SET_FLAG);
                        if state & EXIST_FLAG > 0 {
                            if let Some(item) = shared.inner._consume_value() {
                                trace_log!("poll value");
                                return Ok(item);
                            }
                            trace_log!("poll value closed");
                            // Might have try_recv consume the value
                            return Err(false);
                        }
                    }
                    if state & CLOSE_FLAG > 0 {
                        trace_log!("poll closed");
                        return Err(false);
                    }
                    match check_timeout(deadline) {
                        Ok(None) => {
                            std::thread::park();
                        }
                        Ok(Some(dur)) => {
                            std::thread::park_timeout(dur);
                        }
                        Err(_) => {
                            trace_log!("poll timeout");
                            return Err(true);
                        }
                    }
                }
            }
        }
    }
}

impl<T> Future for RxOneshot<T> {
    type Output = Result<T, RecvError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        let shared = &_self.0;
        match shared.inner._try_recv(Ordering::SeqCst) {
            Ok(item) => {
                trace_log!("poll value");
                return Poll::Ready(Ok(item));
            }
            Err(mut state) => {
                if state & WAKER_SET_FLAG > 0 {
                    let waker = shared.get_waker().as_ref().unwrap();
                    if waker.will_wake(ctx) {
                        trace_log!("spurious waked state {}", state);
                        if state & CLOSE_FLAG > 0 {
                            trace_log!("poll closed");
                            return Poll::Ready(Err(RecvError));
                        }
                        return Poll::Pending;
                    } else {
                        state = shared.inner.unset_state(WAKER_SET_FLAG);
                        if state & EXIST_FLAG > 0 {
                            if let Some(item) = shared.inner._consume_value() {
                                trace_log!("poll value");
                                return Poll::Ready(Ok(item));
                            } else {
                                trace_log!("poll value closed");
                                // Might have try_recv consume the value
                                return Poll::Ready(Err(RecvError));
                            }
                        }
                    }
                }
                if state & CLOSE_FLAG == 0 {
                    shared.get_waker().replace(ThinWaker::Async(ctx.waker().clone()));
                    state = shared.inner.set_state(WAKER_SET_FLAG);
                    if state & EXIST_FLAG > 0 {
                        if let Some(item) = shared.inner._consume_value() {
                            trace_log!("poll value");
                            return Poll::Ready(Ok(item));
                        } else {
                            trace_log!("poll value closed");
                            // Might have try_recv consume the value
                            return Poll::Ready(Err(RecvError));
                        }
                    }
                }
                if state & CLOSE_FLAG > 0 {
                    trace_log!("poll closed");
                    return Poll::Ready(Err(RecvError));
                }
                trace_log!("poll pending: state={}", state);
                return Poll::Pending;
            }
        }
    }
}

#[inline]
pub fn oneshot<T>() -> (TxOneshot<T>, RxOneshot<T>) {
    let shared = Arc::new(Shared { inner: OneShot::new(), waker: UnsafeCell::new(None) });
    let tx = TxOneshot(Some(shared.clone()));
    let rx = RxOneshot(shared);
    (tx, rx)
}
