use crate::backoff::*;
use crate::collections::ArcCell;
use std::cell::UnsafeCell;
use std::fmt;
use std::mem::transmute;
use std::ops::Deref;
use std::ptr;
use std::sync::{
    atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering},
    Arc, Weak,
};
use std::task::*;
use std::thread;

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum WakerState {
    INIT = 0, // A temporary state, https://github.com/frostyplanet/crossfire-rs/issues/22
    WAITING = 1,
    WAKED = 2,
    DONE = 3,
    CLOSED = 4, // Channel closed, or timeout cancellation
}

pub trait WakerTrait: Deref<Target = Self::Inner> {
    type Inner;

    fn from_arc(inner: Arc<Self::Inner>) -> Self;

    fn to_arc(self) -> Arc<Self::Inner>;

    fn reset(inner: &Arc<Self::Inner>);

    fn update_blocking_thread(inner: &Arc<Self::Inner>);

    fn new_async(ctx: &Context) -> Self;

    fn new_blocking() -> Self;

    fn get_seq(&self) -> usize;

    fn set_seq(&self, seq: usize);

    fn get_state(&self) -> u8;

    fn weak(&self) -> Weak<Self::Inner>;

    /// return true to stop; return false to continue the search.
    fn try_to_clear(&self, seq: usize) -> bool;
}

pub struct SendWaker<T>(Arc<WakerInner<AtomicPtr<T>>>);

impl<T> fmt::Debug for SendWaker<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "waker({} state={})", self.get_seq(), self.get_state())
    }
}

impl<T> SendWaker<T> {
    #[inline(always)]
    pub fn cancel(&self) -> u8 {
        match self.try_change_state(WakerState::WAITING, WakerState::WAKED) {
            Ok(_) => return WakerState::WAKED as u8,
            Err(s) => return s,
        }
    }

    #[inline(always)]
    pub fn set_ptr(&self, p: *mut T) {
        self.payload.store(p, Ordering::Release);
    }

    #[inline(always)]
    pub fn load_ptr(&self) -> *mut T {
        self.payload.load(Ordering::Acquire)
    }
}

impl<T> Deref for SendWaker<T> {
    type Target = WakerInner<AtomicPtr<T>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<T> WakerTrait for SendWaker<T> {
    type Inner = WakerInner<AtomicPtr<T>>;

    #[inline(always)]
    fn from_arc(inner: Arc<Self::Inner>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    fn to_arc(self) -> Arc<Self::Inner> {
        self.0
    }

    #[inline(always)]
    fn new_async(ctx: &Context) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Async(ctx.waker().clone())),
            payload: AtomicPtr::new(ptr::null_mut()),
        }))
    }

    #[inline(always)]
    fn new_blocking() -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Blocking(thread::current())),
            payload: AtomicPtr::new(ptr::null_mut()),
        }))
    }

    #[inline(always)]
    fn reset(inner: &Arc<Self::Inner>) {
        inner.state.store(WakerState::WAKED as u8, Ordering::Release);
        inner.payload.store(ptr::null_mut(), Ordering::Release);
    }

    #[inline(always)]
    fn update_blocking_thread(inner: &Arc<Self::Inner>) {
        inner.update_thread_handle();
    }

    #[inline(always)]
    fn get_seq(&self) -> usize {
        self.0.seq.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn set_seq(&self, seq: usize) {
        self.0.seq.store(seq, Ordering::Release);
    }

    #[inline(always)]
    fn get_state(&self) -> u8 {
        self.0.get_state()
    }

    #[inline(always)]
    fn weak(&self) -> Weak<Self::Inner> {
        Arc::downgrade(&self.0)
    }

    /// return true to stop; return false to continue the search.
    #[inline(always)]
    fn try_to_clear(&self, seq: usize) -> bool {
        self.0.try_to_clear(seq)
    }
}

pub struct RecvWaker(Arc<WakerInner<()>>);

impl fmt::Debug for RecvWaker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "waker({} state={})", self.get_seq(), self.get_state())
    }
}

impl Deref for RecvWaker {
    type Target = WakerInner<()>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl RecvWaker {
    #[inline(always)]
    pub fn cancel(&self) -> u8 {
        match self.try_change_state(WakerState::INIT, WakerState::WAKED) {
            Ok(_) => return WakerState::WAKED as u8,
            Err(s) => return s,
        }
    }
}

impl WakerTrait for RecvWaker {
    type Inner = WakerInner<()>;

    #[inline(always)]
    fn from_arc(inner: Arc<Self::Inner>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    fn to_arc(self) -> Arc<Self::Inner> {
        self.0
    }

    #[inline(always)]
    fn new_async(ctx: &Context) -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Async(ctx.waker().clone())),
            payload: (),
        }))
    }

    #[inline(always)]
    fn new_blocking() -> Self {
        Self(Arc::new(WakerInner {
            seq: AtomicUsize::new(0),
            locked: AtomicBool::new(false),
            state: AtomicU8::new(WakerState::WAKED as u8),
            waker: UnsafeCell::new(WakerType::Blocking(thread::current())),
            payload: (),
        }))
    }

    #[inline(always)]
    fn reset(inner: &Arc<Self::Inner>) {
        inner.state.store(WakerState::WAKED as u8, Ordering::Release);
    }

    #[inline(always)]
    fn update_blocking_thread(inner: &Arc<Self::Inner>) {
        inner.update_thread_handle();
    }

    #[inline(always)]
    fn get_seq(&self) -> usize {
        self.0.seq.load(Ordering::Acquire)
    }

    #[inline(always)]
    fn set_seq(&self, seq: usize) {
        self.0.seq.store(seq, Ordering::Release);
    }

    #[inline(always)]
    fn get_state(&self) -> u8 {
        self.0.get_state()
    }

    #[inline(always)]
    fn weak(&self) -> Weak<Self::Inner> {
        Arc::downgrade(&self.0)
    }

    /// return true to stop; return false to continue the search.
    #[inline(always)]
    fn try_to_clear(&self, seq: usize) -> bool {
        self.0.try_to_clear(seq)
    }
}

enum WakerType {
    Async(Waker),
    Blocking(thread::Thread),
}

pub struct WakerInner<P> {
    state: AtomicU8,
    locked: AtomicBool,
    seq: AtomicUsize,
    waker: UnsafeCell<WakerType>,
    pub payload: P,
}

pub struct WakerInnerGuard<'a, P>(&'a WakerInner<P>);

impl<'a, P> Drop for WakerInnerGuard<'a, P> {
    fn drop(&mut self) {
        self.0.unlock();
    }
}

unsafe impl<P> Send for WakerInner<P> {}
unsafe impl<P> Sync for WakerInner<P> {}

impl<P> WakerInner<P> {
    #[inline(always)]
    fn get_waker(&self) -> &WakerType {
        unsafe { transmute(self.waker.get()) }
    }

    #[inline(always)]
    fn get_waker_mut(&self) -> &mut WakerType {
        unsafe { transmute(self.waker.get()) }
    }

    #[inline(always)]
    fn update_thread_handle(&self) {
        let _waker = self.get_waker_mut();
        *_waker = WakerType::Blocking(thread::current());
    }

    /// return true to stop; return false to continue the search.
    #[inline(always)]
    fn try_to_clear(&self, seq: usize) -> bool {
        let _seq = self.seq.load(Ordering::Acquire);
        if _seq == seq {
            // It's my waker, stopped
            return true;
        }
        let _ = self.wake_simple();
        return _seq > seq;
    }

    #[inline(always)]
    pub fn try_change_state(&self, cur: WakerState, new_state: WakerState) -> Result<(), u8> {
        if let Err(s) = self.state.compare_exchange(
            cur as u8,
            new_state as u8,
            Ordering::SeqCst,
            Ordering::Acquire,
        ) {
            return Err(s);
        }
        return Ok(());
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn get_state_relaxed(&self) -> u8 {
        self.state.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set_state(&self, state: WakerState) -> u8 {
        let _state = state as u8;
        #[cfg(test)]
        {
            if _state != WakerState::CLOSED as u8 {
                let __state = self.get_state();
                assert!(__state <= WakerState::WAKED as u8, "unexpected state: {}", __state);
            }
        }
        self.state.store(_state, Ordering::Release);
        return _state;
    }

    /// Return current status,
    /// CLOSED: might be channel closed, or future successfully cancelled, the future should drop message; try to clear its waker.
    /// DONE: the message actually sent, nothing to DO
    /// WAKED: the future should drop message, and waked another counterpart.
    #[inline(always)]
    pub fn abandon(&self) -> u8 {
        // should have lock because it will content with close() and on_recv()
        let mut backoff = Backoff::new(BackoffConfig::default());
        loop {
            if let Some(_guard) = self.try_lock_weak() {
                // Acquire lock first, might be try_send_with_lock suc from on_recv().
                match self.state.compare_exchange(
                    WakerState::WAITING as u8,
                    WakerState::CLOSED as u8,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return WakerState::CLOSED as u8,
                    Err(s) => {
                        return s;
                    }
                }
            }
            backoff.snooze();
        }
    }

    #[inline(always)]
    pub fn commit_waiting(&self) -> u8 {
        if let Err(s) = self.try_change_state(WakerState::INIT, WakerState::WAITING) {
            return s;
        } else {
            return WakerState::WAITING as u8;
        }
    }

    #[inline(always)]
    pub fn is_waked(&self) -> bool {
        self.state.load(Ordering::Acquire) >= WakerState::WAKED as u8
    }

    #[inline(always)]
    pub fn close_wake(&self) {
        // should have lock because it will content with abandon()
        loop {
            if let Some(_guard) = self.try_lock_weak() {
                if self.change_state_smaller_eq(WakerState::WAITING, WakerState::CLOSED).is_ok() {
                    self._wake_nolock();
                }
                return;
            } else {
                std::hint::spin_loop();
            }
        }
    }

    #[inline(always)]
    pub fn active_close(&self) -> u8 {
        match self.change_state_smaller_eq(WakerState::WAKED, WakerState::CLOSED) {
            Ok(_) => {
                return WakerState::CLOSED as u8;
            }
            Err(s) => {
                return s;
            }
        }
    }

    // Return Ok(pre_state), otherwise return Err(current_state)
    #[inline(always)]
    pub fn change_state_smaller_eq(
        &self, condition: WakerState, target: WakerState,
    ) -> Result<u8, u8> {
        // Save one load()
        let mut state = condition as u8;
        loop {
            match self.state.compare_exchange_weak(
                state,
                target as u8,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(state);
                }
                Err(s) => {
                    if s > condition as u8 {
                        return Err(s);
                    }
                    state = s;
                }
            }
        }
    }

    #[inline(always)]
    fn get_state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    /// Assume no lock
    #[inline(always)]
    pub fn wake_simple(&self) -> Result<u8, ()> {
        if let WakerType::Blocking(t) = self.get_waker() {
            if let Ok(state) = self.change_state_smaller_eq(WakerState::WAITING, WakerState::WAKED)
            {
                t.unpark();
                return Ok(state);
            }
            return Err(());
        } else {
            loop {
                if let Some(_guard) = self.try_lock_weak() {
                    if let Ok(state) =
                        self.change_state_smaller_eq(WakerState::WAITING, WakerState::WAKED)
                    {
                        self._wake_nolock();
                        return Ok(state);
                    }
                    return Err(());
                }
                std::hint::spin_loop();
            }
        }
    }

    #[inline(always)]
    pub fn try_lock_weak<'a>(&'a self) -> Option<WakerInnerGuard<'a, P>> {
        if self
            .locked
            .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            return Some(WakerInnerGuard(self));
        }
        None
    }

    #[inline(always)]
    pub fn try_lock<'a>(&'a self) -> Option<WakerInnerGuard<'a, P>> {
        if self.locked.compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed).is_ok() {
            return Some(WakerInnerGuard(self));
        }
        None
    }

    #[inline(always)]
    fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }

    /// no lock version
    #[inline(always)]
    pub fn _check_waker_nolock(&self, ctx: &mut Context) {
        // ref: https://github.com/frostyplanet/crossfire-rs/issues/14
        // https://docs.rs/tokio/latest/tokio/runtime/index.html#:~:text=Normally%2C%20tasks%20are%20scheduled%20only,is%20called%20a%20spurious%20wakeup
        // There might be situation like spurious wakeup, poll() again under no fire() ever
        // happened, waker still exists but cannot be used to wake the current future.
        // Since there's no lock inside fire(), to avoid race, can not update the content but to put a new one.
        let o_waker = self.get_waker_mut();
        if let WakerType::Async(_waker) = o_waker {
            if !_waker.will_wake(ctx.waker()) {
                *o_waker = WakerType::Async(ctx.waker().clone());
            }
        } else {
            unreachable!();
        }
    }

    #[inline(always)]
    pub fn check_waker(&self, ctx: &mut Context) -> u8 {
        // ref: https://github.com/frostyplanet/crossfire-rs/issues/14
        // https://docs.rs/tokio/latest/tokio/runtime/index.html#:~:text=Normally%2C%20tasks%20are%20scheduled%20only,is%20called%20a%20spurious%20wakeup
        // There might be situation like spurious wakeup, poll() again under no fire() ever
        // happened, waker still exists but cannot be used to wake the current future.
        // Since there's no lock inside fire(), to avoid race, can not update the content but to put a new one.
        loop {
            if let Some(_guard) = self.try_lock_weak() {
                let state = self.get_state();
                if state >= WakerState::DONE as u8 {
                    return state;
                }
                self._check_waker_nolock(ctx);
                return state;
            } else {
                std::hint::spin_loop();
            }
        }
    }

    // Assume have lock
    #[inline(always)]
    pub fn _wake_nolock(&self) {
        match self.get_waker() {
            WakerType::Async(w) => w.wake_by_ref(),
            WakerType::Blocking(th) => th.unpark(),
        }
    }
}

pub struct WakerCache<T: WakerTrait>(ArcCell<T::Inner>);

impl<T: WakerTrait> WakerCache<T> {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self(ArcCell::new())
    }

    #[inline(always)]
    pub(crate) fn new_blocking(&self) -> T {
        if let Some(inner) = self.0.pop() {
            T::update_blocking_thread(&inner);
            return T::from_arc(inner);
        }
        return T::new_blocking();
    }

    #[inline(always)]
    pub(crate) fn push(&self, waker: T) {
        if waker.get_state() < WakerState::WAKED as u8 {
            return;
        }
        let a = waker.to_arc();
        if Arc::weak_count(&a) == 0 && Arc::strong_count(&a) == 1 {
            T::reset(&a);
            self.0.try_put(a);
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        !self.0.exists()
    }
}
