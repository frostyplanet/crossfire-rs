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

use crate::backoff::Backoff;
use crate::shared::*;
#[allow(unused_imports)]
use crate::{tokio_task_id, trace_log};
use core::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{
    fence, AtomicU8,
    Ordering::{self, AcqRel, Acquire, SeqCst},
};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

/// Send/TxOneshot::drop will set this flag once, never changed.
const LOCK_FLAG: u8 = 0x1;
/// set by RxOneshot
const WAKER_SET_FLAG: u8 = 0x2;
/// set by any of TxOneshot/RxOneshot if it exit
const CLOSE_FLAG: u8 = 0x4;
const EXIST_FLAG: u8 = 0x8;

struct OneShotInner<T> {
    state: AtomicU8,
    value: UnsafeCell<Option<T>>,
    o_waker: UnsafeCell<Option<ThinWaker>>,
}

unsafe impl<T: Send> Send for OneShotInner<T> {}
unsafe impl<T: Send> Sync for OneShotInner<T> {}

impl<T> OneShotInner<T> {
    #[inline]
    fn new() -> Box<Self> {
        Box::new(Self {
            value: UnsafeCell::new(None),
            state: AtomicU8::new(0),
            o_waker: UnsafeCell::new(None),
        })
    }

    #[inline]
    fn get_waker(&self) -> &mut Option<ThinWaker> {
        unsafe { &mut *self.o_waker.get() }
    }

    #[inline(always)]
    fn value_mut(&self) -> &mut Option<T> {
        unsafe { &mut *self.value.get() }
    }

    #[inline(always)]
    fn set_state(&self, flag: u8) -> u8 {
        self.state.fetch_or(flag, Ordering::AcqRel)
    }

    #[inline(always)]
    fn _try_recv(&self, order: Ordering) -> Result<u8, u8> {
        let state = self.state.load(order);
        if state & LOCK_FLAG > 0 {
            Ok(state)
        } else {
            Err(state)
        }
    }

    // NOTE: in order to avoid miri borrow checker, use raw ptr here
    #[inline(always)]
    fn _consume_value(p: NonNull<Self>, mut state: u8) -> Option<T> {
        debug_assert!(
            state & LOCK_FLAG > 0,
            "oneshot:({:?}) consume value unexpected {state}",
            tokio_task_id!()
        );
        let this = unsafe { p.as_ref() };
        let item = if state & EXIST_FLAG > 0 { this.value_mut().take() } else { None };
        loop {
            if state & CLOSE_FLAG > 0 {
                trace_log!(
                    "oneshot:({:?}) recv value={} & destroy",
                    tokio_task_id!(),
                    item.is_some()
                );
                fence(Acquire);
                let _ = unsafe { Box::from_raw(p.as_ptr()) };
                // they close first
                return item;
            }
            if let Err(s) = this.state.compare_exchange(state, CLOSE_FLAG | state, AcqRel, Acquire)
            {
                trace_log!(
                    "oneshot:({:?}) recv value={} {state} close retry",
                    tokio_task_id!(),
                    item.is_some()
                );
                state = s;
            } else {
                trace_log!(
                    "oneshot:({:?}) recv value={} {state}",
                    tokio_task_id!(),
                    item.is_some()
                );
                // we close first
                return item;
            }
        }
    }

    /// return true to destroy
    #[inline(always)]
    fn _notify_rx(p: NonNull<Self>, exist: bool) -> bool {
        let this = unsafe { p.as_ref() };
        let mut old_state = 0;
        let exist_flag: u8 = if exist { EXIST_FLAG } else { 0 };
        loop {
            let new_state = if old_state == 0 {
                LOCK_FLAG | CLOSE_FLAG | exist_flag
            } else if old_state == WAKER_SET_FLAG {
                LOCK_FLAG | WAKER_SET_FLAG | exist_flag
            } else if old_state & CLOSE_FLAG > 0 {
                // WAKER_SET_FLAG | CLOSE_FLAG, or just CLOSE_FLAG
                trace_log!("oneshot:({:?}) rx closed", tokio_task_id!());
                return true;
            } else {
                panic!("unexpected state {}", old_state);
            };
            match this.state.compare_exchange_weak(old_state, new_state, AcqRel, Acquire) {
                Ok(_) => {
                    if old_state == 0 {
                        trace_log!("oneshot:({:?}) send value", tokio_task_id!());
                        return false;
                    } else {
                        if let Some(waker) = this.get_waker().as_ref() {
                            // the sender should never move the waker, because rx::poll will
                            // validate it.
                            trace_log!("oneshot:({:?}) wake rx", tokio_task_id!());
                            waker.wake_by_ref();
                        } else {
                            unreachable!();
                        }
                        if let Err(state) = this.state.compare_exchange(
                            new_state,
                            CLOSE_FLAG | LOCK_FLAG | exist_flag,
                            AcqRel,
                            Acquire,
                        ) {
                            // Safety: although we have no use for fail value other than debug log,
                            // but consider use failure ordering Acquire instead of Relaxed for miri,
                            // as a fence (stop the following from_raw to re-ordering).
                            debug_assert!(state & CLOSE_FLAG > 0, "unexpected state {state}");
                            trace_log!("oneshot:({:?}) rx closed {state}", tokio_task_id!());
                            return true;
                        } else {
                            // we close first, let rx do the cleanup
                            return false;
                        }
                    }
                }
                Err(s) => {
                    old_state = s;
                }
            }
        }
    }

    #[inline(always)]
    fn set_waker(&self, waker: ThinWaker) -> Result<(), u8> {
        // thread context only need set waker once.
        // NOTE we should guarantee waker not set twice
        // (the recv_timeout API should not allow recv twice),
        // it will complicate things (like async poll).
        self.get_waker().replace(waker);
        if let Err(state) = self.state.compare_exchange(0, WAKER_SET_FLAG, AcqRel, Acquire) {
            return Err(state);
        }
        Ok(())
    }

    #[inline(always)]
    fn cancel_waker(&self, abandon: bool) -> Result<(), u8> {
        let new_state = if abandon { CLOSE_FLAG } else { 0 };
        if let Err(state) = self.state.compare_exchange(WAKER_SET_FLAG, new_state, AcqRel, Acquire)
        {
            // expect LOCK_FLAG | CLOSE_FLAG, or LOCK_FLAG | WAKER_SET_FLAG
            return Err(state);
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        let state = self.state.load(Ordering::SeqCst);
        state & EXIST_FLAG == 0
    }
}

/// Sender for oneshot channel
pub struct TxOneshot<T>(NonNull<OneShotInner<T>>);

unsafe impl<T> Send for TxOneshot<T> {}

impl<T> TxOneshot<T> {
    /// Sending the item is one-time non-blocking behavior
    #[inline]
    pub fn send(self, item: T) {
        unsafe { self.0.as_ref() }.value_mut().replace(item);
        if OneShotInner::_notify_rx(self.0, true) {
            // drop inner
            let _ = unsafe { Box::from_raw(self.0.as_ptr()) };
        }
        std::mem::forget(self);
    }
}

impl<T> Drop for TxOneshot<T> {
    #[inline]
    fn drop(&mut self) {
        if OneShotInner::_notify_rx(self.0, false) {
            // drop inner
            let _ = unsafe { Box::from_raw(self.0.as_ptr()) };
        }
    }
}

/// Receiver for oneshot channel
#[must_use]
pub struct RxOneshot<T>(Option<NonNull<OneShotInner<T>>>);

unsafe impl<T> Send for RxOneshot<T> {}

impl<T> Drop for RxOneshot<T> {
    #[inline]
    fn drop(&mut self) {
        if let Some(p) = self.0.as_ref() {
            let inner = unsafe { p.as_ref() };
            let old_state = inner.set_state(CLOSE_FLAG);
            if old_state & CLOSE_FLAG > 0 {
                trace_log!("oneshot:({:?}) rx drop destroy, state={}", tokio_task_id!(), old_state);
                debug_assert_eq!(
                    old_state & (!EXIST_FLAG),
                    CLOSE_FLAG | LOCK_FLAG,
                    "unexpected state {old_state}"
                ); // tx drop
                   // drop inner
                let _ = unsafe { Box::from_raw(p.as_ptr()) };
            } else {
                // let tx do the cleanup
                trace_log!("oneshot:({:?}) rx drop, state={}", tokio_task_id!(), old_state);
                debug_assert!(
                    old_state == 0 // we drop first, tx not trigger
                        || old_state == WAKER_SET_FLAG // rx.await cancel, or rx.recv_timeout() timeout
                        || old_state | EXIST_FLAG== (EXIST_FLAG | LOCK_FLAG | WAKER_SET_FLAG), // tx waking while rx.await cancel, or rx.recv_timeout() timeout
                    "oneshot:({:?}) rx drop, unexpected state={}",
                    tokio_task_id!(),
                    old_state
                );
            }
        }
    }
}

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
    pub fn recv_timeout(self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        match self._recv_blocking(Some(deadline)) {
            Ok(item) => Ok(item),
            Err(true) => Err(RecvTimeoutError::Timeout),
            Err(false) => Err(RecvTimeoutError::Disconnected),
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        if let Some(p) = self.0.as_ref() {
            let inner = unsafe { p.as_ref() };
            inner.is_empty()
        } else {
            true
        }
    }

    #[inline]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(p) = self.0.as_ref() {
            let p = *p;
            if let Ok(state) = unsafe { p.as_ref() }._try_recv(Acquire) {
                self.0 = None;
                if let Some(item) = OneShotInner::_consume_value(p, state) {
                    return Ok(item);
                } else {
                    return Err(TryRecvError::Disconnected);
                }
            } else {
                Err(TryRecvError::Empty)
            }
        } else {
            Err(TryRecvError::Disconnected)
        }
    }

    #[inline]
    pub async fn recv_async(self) -> Result<T, RecvError> {
        self.await
    }

    #[inline]
    fn poll(&mut self, ctx: &mut Context<'_>) -> Poll<Result<T, ()>> {
        let p: NonNull<OneShotInner<T>> = if let Some(p) = self.0.as_ref() {
            *p
        } else {
            // might poll after try_recv() finish
            return Poll::Ready(Err(()));
        };
        let inner = unsafe { p.as_ref() };
        macro_rules! process {
            ($state: expr) => {
                self.0 = None;
                if let Some(item) = OneShotInner::_consume_value(p, $state) {
                    return Poll::Ready(Ok(item));
                } else {
                    return Poll::Ready(Err(()));
                }
            };
        }
        macro_rules! check_exist {
            ($order: expr) => {{
                match inner._try_recv($order) {
                    Ok(state) => {
                        process!(state);
                    }
                    Err(s) => s,
                }
            }};
        }
        let state = check_exist!(SeqCst);
        if state & WAKER_SET_FLAG > 0 {
            let waker = inner.get_waker().as_ref().unwrap();
            if waker.will_wake(ctx) {
                trace_log!("oneshot:({:?}) spurious waked state {}", tokio_task_id!(), state,);
                return Poll::Pending;
            }
            if let Err(state) = inner.cancel_waker(false) {
                process!(state);
            }
        }
        if let Err(state) = inner.set_waker(ThinWaker::Async(ctx.waker().clone())) {
            process!(state);
        }
        Poll::Pending
    }

    /// On Disconnected return Err(false),
    /// Err(true) when timeout.
    #[inline(always)]
    pub(crate) fn _recv_blocking(self, deadline: Option<Instant>) -> Result<T, bool> {
        let p: NonNull<OneShotInner<T>> = if let Some(p) = self.0.as_ref() {
            *p
        } else {
            // might recv() after try_recv() ok/disconnect
            return Err(false);
        };
        let inner = unsafe { p.as_ref() };
        macro_rules! process {
            ($state: expr) => {
                let _ = inner;
                std::mem::forget(self);
                if let Some(item) = OneShotInner::_consume_value(p, $state) {
                    return Ok(item);
                } else {
                    return Err(false);
                }
            };
        }
        macro_rules! try_recv {
            ($order: expr) => {
                if let Ok(state) = inner._try_recv($order) {
                    trace_log!("try_recv got {state}");
                    process!(state);
                }
            };
        }
        try_recv!(Acquire);
        let mut backoff = Backoff::new();
        while !backoff.snooze() {
            try_recv!(Acquire);
        }
        if let Err(state) = inner.set_waker(ThinWaker::Blocking(thread::current())) {
            process!(state);
        }
        trace_log!("oneshot: waker set");
        loop {
            try_recv!(SeqCst);
            match check_timeout(deadline) {
                Ok(None) => {
                    std::thread::park();
                }
                Ok(Some(dur)) => {
                    std::thread::park_timeout(dur);
                }
                Err(_) => {
                    trace_log!("oneshot: to cancel_waker on timeout");
                    if let Err(state) = inner.cancel_waker(true) {
                        process!(state);
                    } else {
                        let _ = inner;
                        // we close first
                        std::mem::forget(self);
                        return Err(true);
                    }
                }
            }
        }
    }

    /// Wrap RxOneshot with timeout, consume self when it's done.
    /// The Future returns `Result<T, RecvTimeoutError>`
    #[cfg(any(feature = "tokio", feature = "async_std"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "tokio", feature = "async_std"))))]
    #[inline]
    pub async fn recv_async_timeout(
        self, timeout: std::time::Duration,
    ) -> Result<T, RecvTimeoutError> {
        #[cfg(feature = "tokio")]
        {
            let sleep = tokio::time::sleep(timeout);
            self.recv_async_with_timer(sleep).await
        }
        #[cfg(feature = "async_std")]
        {
            let sleep = async_std::task::sleep(timeout);
            self.recv_async_with_timer(sleep).await
        }
    }

    /// Wrap RxOneshot with custom sleep function, consume self when it's done.
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
    /// * `sleep`: The sleep function. the return value of `sleep` is ignore. We add generic `R` just in order to support smol::Timer
    /// # Example
    ///
    /// Example with smol
    ///
    /// ```rust
    /// extern crate smol;
    /// use std::time::Duration;
    /// use crossfire::*;
    /// async fn foo() {
    ///     let (tx, rx) = oneshot::oneshot::<usize>();
    ///     match rx.recv_async_with_timer(smol::Timer::after(Duration::from_secs(1))).await {
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
    ///
    /// Example with tokio:
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use crossfire::*;
    /// async fn foo() {
    ///     let (tx, rx) = oneshot::oneshot::<usize>();
    ///     let sleep = tokio::time::sleep(Duration::from_secs(1));
    ///     let _r = rx.recv_async_with_timer(sleep).await;
    /// }
    /// ```
    #[inline]
    pub fn recv_async_with_timer<F, R>(self, sleep: F) -> OneshotTimeoutFuture<T, F, R>
    where
        F: Future<Output = R>,
    {
        OneshotTimeoutFuture { rx: self, sleep }
    }
}

impl<T> Future for RxOneshot<T> {
    type Output = Result<T, RecvError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.poll(ctx) {
            Poll::Ready(Ok(item)) => Poll::Ready(Ok(item)),
            Poll::Ready(Err(())) => Poll::Ready(Err(RecvError)),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct OneshotTimeoutFuture<T, F, R>
where
    F: Future<Output = R>,
{
    rx: RxOneshot<T>,
    sleep: F,
}

impl<T, F, R> Future for OneshotTimeoutFuture<T, F, R>
where
    F: Future<Output = R>,
{
    type Output = Result<T, RecvTimeoutError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        // NOTE: we can use unchecked to bypass pin because we are not movig "sleep",
        // neither it's exposed outside
        let this = unsafe { self.get_unchecked_mut() };
        match this.rx.poll(ctx) {
            Poll::Ready(Ok(item)) => return Poll::Ready(Ok(item)),
            Poll::Ready(Err(())) => return Poll::Ready(Err(RecvTimeoutError::Disconnected)),
            _ => {}
        }
        let sleep = unsafe { Pin::new_unchecked(&mut this.sleep) };
        if sleep.poll(ctx).is_ready() {
            Poll::Ready(Err(RecvTimeoutError::Timeout))
        } else {
            Poll::Pending
        }
    }
}

#[inline]
pub fn oneshot<T>() -> (TxOneshot<T>, RxOneshot<T>) {
    let p = unsafe { NonNull::new_unchecked(Box::into_raw(OneShotInner::new())) };
    let tx = TxOneshot(p);
    let rx = RxOneshot(Some(p));
    (tx, rx)
}
