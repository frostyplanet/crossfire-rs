pub use super::waker_registry::*;
use crate::backoff::*;
use crate::crossbeam::array_queue::ArrayQueue;
pub use crate::crossbeam::err::*;
pub use crate::locked_waker::*;
use crossbeam_queue::SegQueue;
use lazy_static::lazy_static;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::ptr;
use std::sync::Arc;
use std::task::Context;
use std::time::{Duration, Instant};

pub(crate) enum Channel<T> {
    List(SegQueue<T>),
    Array(ArrayQueue<T>),
}

impl<T> Channel<T> {
    #[inline(always)]
    pub fn new_list() -> Self {
        Self::List(SegQueue::new())
    }

    #[inline(always)]
    pub fn new_array(bound: usize) -> Self {
        assert!(bound <= u32::MAX as usize);
        assert!(bound > 0);
        Self::Array(ArrayQueue::new(bound))
    }

    #[inline(always)]
    fn get_bound(&self) -> Option<usize> {
        match self {
            Self::List(_) => None,
            Self::Array(s) => Some(s.capacity()),
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::List(s) => s.len(),
            Self::Array(s) => s.len(),
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        match self {
            Self::List(s) => s.is_empty(),
            Self::Array(s) => s.is_empty(),
        }
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        match self {
            Self::Array(s) => s.is_full(),
            Self::List(_) => false,
        }
    }
}

pub struct ChannelShared<T> {
    closed: AtomicBool,
    tx_count: AtomicUsize,
    rx_count: AtomicUsize,
    inner: Channel<T>,
    pub(crate) senders: RegistrySender<T>,
    pub(crate) recvs: RegistryRecv,
    bound_size: Option<u32>,
}

impl<T> ChannelShared<T> {
    pub(crate) fn new(
        inner: Channel<T>, senders: RegistrySender<T>, recvs: RegistryRecv,
    ) -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
            tx_count: AtomicUsize::new(1),
            rx_count: AtomicUsize::new(1),
            senders,
            recvs,
            bound_size: if let Some(bound) = inner.get_bound() { Some(bound as u32) } else { None },
            inner,
        })
    }

    #[inline(always)]
    pub(crate) fn try_recv(&self) -> Option<T> {
        match &self.inner {
            Channel::List(inner) => {
                return inner.pop();
            }
            Channel::Array(inner) => {
                return inner.pop();
            }
        }
    }

    #[inline(always)]
    pub(crate) fn is_zero(&self) -> bool {
        self.bound_size == Some(0)
    }

    /// The number of messages in the channel at the moment.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether channel is empty at the moment
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Whether the channel is full at the moment
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Return true if all the senders or receivers are dropped
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Get the count of alive senders
    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.tx_count.load(Ordering::Acquire) as usize
    }

    /// Get the count of alive receivers
    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.rx_count.load(Ordering::Acquire) as usize
    }

    /// Just for debugging purpose, to monitor queue size
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }

    #[inline(always)]
    pub(crate) fn add_tx(&self) {
        let _ = self.tx_count.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        let _ = self.rx_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Call when tx drop
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        if self.tx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::Release);
            self._close_all();
        }
    }

    /// Call when rx drop
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        if self.rx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::Release);
            self._close_all();
        }
    }

    #[inline(always)]
    fn _close_all(&self) {
        while let Some(waker) = self.recvs.pop() {
            waker.close_wake();
        }
        while let Some(waker) = self.senders.pop() {
            waker.close_wake();
        }
    }

    /// Register waker for current rx
    #[inline(always)]
    pub(crate) fn reg_recv(&self, o_waker: &RecvWaker) -> Result<(), u8> {
        self.recvs.reg_waker(o_waker)
    }

    #[inline(always)]
    pub(crate) fn send(&self, item: &MaybeUninit<T>) -> bool {
        match &self.inner {
            Channel::Array(inner) => {
                return unsafe { inner.push_with_ptr(item.as_ptr()) };
            }
            Channel::List(inner) => {
                inner.push(unsafe { item.assume_init_read() });
                return true;
            }
        }
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn try_send_oneshot(&self, item: &MaybeUninit<T>) -> Option<bool> {
        match &self.inner {
            Channel::Array(inner) => {
                return unsafe { inner.try_push_oneshot(item.as_ptr()) };
            }
            Channel::List(_inner) => {
                unreachable!();
            }
        }
    }

    /// When waker exists and reused for async_tx, might need to check_waker every poll(),
    /// in case of spurious waked up by runtime.
    #[inline]
    pub(crate) fn sender_try_again_async(
        &self, item: &mut MaybeUninit<T>, waker: &SendWaker<T>, ctx: &mut Context,
        backoff_conf: BackoffConfig,
    ) -> u8 {
        let mut backoff = Backoff::new(backoff_conf);
        if self.is_disconnected() {
            return waker.active_close();
        }
        // Assume WAITING, must check_waker
        loop {
            if let Some(guard) = waker.try_lock_weak() {
                let state = waker.get_state();
                if state >= WakerState::WAKED as u8 {
                    // Return if WAKED, waker should re-register anyway
                    return state;
                }
                if self.send(item) {
                    waker.set_state(WakerState::DONE);
                    drop(guard);
                    if state <= WakerState::WAITING as u8 {
                        self.senders.cancel_waker();
                    }
                    self.on_send();
                    return WakerState::DONE as u8;
                } else {
                    assert!(waker.load_ptr() != ptr::null_mut());
                }
                waker._check_waker_nolock(ctx);
                return state; // might be WAITING or WAKED
            }
            backoff.snooze();
        }
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    #[inline]
    pub(crate) fn sender_reg_and_try(
        &self, item: &mut MaybeUninit<T>, waker: SendWaker<T>, backoff: &mut Backoff,
    ) -> (u8, Option<SendWaker<T>>) {
        self.senders.reg_waker(&waker);
        let mut state: u8;
        // Not allow Spurious wake and enter this function again;
        debug_assert_eq!(waker.load_ptr(), ptr::null_mut());
        if let Some(res) = self.try_send_oneshot(item) {
            if res {
                waker.set_state(WakerState::DONE);
                self.senders.cancel_waker();
                self.on_send();
                return (WakerState::DONE as u8, Some(waker));
            } else {
                if self.is_disconnected() {
                    return (waker.active_close(), None);
                }
                waker.set_ptr(item.as_mut_ptr());
                state = waker.get_state();
            }
        } else {
            // other's changing, omit close check and return
            return (waker.cancel(), None);
        }
        while state < WakerState::WAKED as u8 {
            if backoff.is_completed() {
                break;
            }
            backoff.yield_now();
            state = waker.get_state();
        }
        return (state, Some(waker));
    }

    /// Return is_waked
    #[inline]
    pub(crate) fn on_recv_try_send(&self, waker: &SendWaker<T>) -> bool {
        if let Some(_guard) = waker.try_lock() {
            let state = waker.get_state();
            if state >= WakerState::WAKED as u8 {
                // It's not possible to be WAKED
                return false;
            }
            // the receiver no need to check disconnect,
            // its impossible if there's live waker
            // Check the state again, during locked, no one allowed to change the status
            let p = waker.load_ptr();
            if p == ptr::null_mut() {
                // Let the sender to re-register
                // Might contend with sender_try_again_blocking
                if waker.change_state_smaller_eq(WakerState::WAITING, WakerState::WAKED).is_ok() {
                    waker._wake_nolock();
                }
                drop(_guard);
                return self.is_full();
            }
            if let Channel::Array(inner) = &self.inner {
                if unsafe { inner.push_with_ptr(p) } {
                    waker.set_state(WakerState::DONE);
                    waker._wake_nolock();
                    drop(_guard);
                    self.on_send();
                    return true;
                }
            } else {
                unreachable!();
            }
            // still full
            // Let the sender to re-register
            waker.set_state(WakerState::WAKED);
            waker._wake_nolock();
            return true; // Do not try another
        } else {
            if let Ok(_) = waker.wake_simple() {
                return self.is_full();
            } else {
                return false;
            }
        }
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        while let Some(waker) = self.recvs.pop() {
            if let Ok(state) = waker.wake_simple() {
                if state != WakerState::INIT as u8 {
                    return;
                }
            }
        }
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        while let Some(waker) = self.senders.pop() {
            if self.on_recv_try_send(&waker) {
                return;
            }
        }
    }

    #[inline(always)]
    pub(crate) fn recv_waker_done(&self, waker: &RecvWaker) {
        let state = waker.get_state();
        if state <= WakerState::WAITING as u8 {
            // including WakerState::INIT
            waker.set_state(WakerState::DONE);
            self.recvs.cancel_waker();
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    /// return false to indicate already DONE.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(&self, waker: SendWaker<T>) -> bool {
        let state = waker.abandon();
        if state == WakerState::CLOSED as u8 {
            self.senders.clear_wakers(waker.get_seq());
            return true;
        } else if state == WakerState::DONE as u8 {
            return false;
        } else {
            debug_assert_eq!(state, WakerState::WAKED as u8);
            // We are waked, but give up sending, should notify another sender for safety
            self.on_recv();
            return true;
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(&self, waker: RecvWaker) -> bool {
        let state = waker.abandon();
        if state == WakerState::CLOSED as u8 {
            self.recvs.clear_wakers(waker.get_seq());
            return true;
        } else if state == WakerState::DONE as u8 {
            return false;
        } else {
            debug_assert_eq!(state, WakerState::WAKED as u8);
            // We are waked, but give up receiving, should notify another receiver for safety
            self.on_send();
            return true;
        }
    }

    /// On timeout, clear dead wakers on receiver queue
    #[inline(always)]
    pub(crate) fn clear_recv_wakers(&self, seq: usize) {
        self.recvs.clear_wakers(seq);
    }

    #[inline]
    pub fn detect_async_backoff_tx(&self) -> i8 {
        // Async parameter is determine by runtime,
        // like tokio you might have multiple runtime. So the result should stored in
        // sender and receivers, not in the ChannelShared
        #[cfg(feature = "tokio")]
        {
            use tokio::runtime::Handle;
            if Handle::current().metrics().num_workers() <= 1 {
                return 0;
            }
        }
        if self.bound_size > Some(0) && self.bound_size <= Some(2) {
            return 6;
        } else {
            return 1;
        }
    }

    #[inline]
    pub fn detect_async_backoff_rx(&self) -> i8 {
        // Async parameter is determine by runtime,
        // like tokio you might have multiple runtime. So the result should stored in
        // sender and receivers, not in the ChannelShared
        #[cfg(feature = "tokio")]
        {
            use tokio::runtime::Handle;
            if Handle::current().metrics().num_workers() <= 1 {
                return 0;
            }
        }
        if self.bound_size > Some(0) && self.bound_size <= Some(2) {
            return 5;
        } else {
            return 1;
        }
    }
}

/// On timed out, returns Err(())
#[inline(always)]
pub fn check_timeout(deadline: Option<Instant>) -> Result<Option<Duration>, ()> {
    if let Some(end) = deadline {
        let now = Instant::now();
        if now < end {
            return Ok(Some(end - now));
        } else {
            return Err(());
        }
    }
    Ok(None)
}

#[allow(dead_code)]
#[derive(Default)]
pub struct ChannelStats {
    tx_try: AtomicUsize,
    tx_poll: AtomicUsize,
    tx_done: AtomicUsize,
    rx_try: AtomicUsize,
    rx_poll: AtomicUsize,
    rx_done: AtomicUsize,
}

lazy_static! {
    static ref STATS: ChannelStats = Default::default();
}

#[cfg(feature = "profile")]
impl ChannelStats {
    pub fn to_string() -> String {
        let mut tx_try = STATS.tx_try.load(Ordering::Acquire) as f64;
        let mut tx_poll = STATS.tx_poll.load(Ordering::Acquire) as f64;
        let tx_done = STATS.tx_done.load(Ordering::Acquire);
        let mut rx_try = STATS.rx_try.load(Ordering::Acquire) as f64;
        let mut rx_poll = STATS.rx_poll.load(Ordering::Acquire) as f64;
        let rx_done = STATS.rx_done.load(Ordering::Acquire);
        if tx_done > 0 {
            tx_try /= tx_poll;
            tx_poll /= tx_done as f64;
        }
        if rx_done > 0 {
            rx_try /= rx_poll;
            rx_poll /= rx_done as f64;
        }
        format!(
            "tx:[avg(try={}, poll={}) op={}], rx[avg(try={}, poll={}), op={}]",
            tx_try, tx_poll, tx_done, rx_try, rx_poll, rx_done,
        )
        .to_string()
    }

    pub fn clear() {
        STATS.tx_try.store(0, Ordering::Release);
        STATS.rx_try.store(0, Ordering::Release);
        STATS.tx_poll.store(0, Ordering::Release);
        STATS.rx_poll.store(0, Ordering::Release);
        STATS.tx_done.store(0, Ordering::Release);
        STATS.rx_done.store(0, Ordering::Release);
    }

    #[inline(always)]
    pub(crate) fn tx_poll(retry: usize) {
        STATS.tx_try.fetch_add(retry, Ordering::SeqCst);
        STATS.tx_poll.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn rx_poll(retry: usize) {
        STATS.rx_try.fetch_add(retry, Ordering::SeqCst);
        STATS.rx_poll.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn tx_done() {
        STATS.tx_done.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn rx_done() {
        STATS.rx_done.fetch_add(1, Ordering::SeqCst);
    }
}

#[macro_export(local_inner_macros)]
macro_rules! rx_stats {
    ($try: expr, $done: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::rx_poll($try);
            if $done {
                ChannelStats::rx_done();
            }
        }
    };
    ($try: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::rx_poll($try);
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! tx_stats {
    ($try: expr, $done: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::tx_poll($try);
            if $done {
                ChannelStats::tx_done();
            }
        }
    };
    ($try: expr) => {
        #[cfg(feature = "profile")]
        {
            ChannelStats::tx_poll($try);
        }
    };
}
