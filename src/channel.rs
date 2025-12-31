use crate::backoff::*;
use crate::crossbeam::array_queue::ArrayQueue;
pub use crate::crossbeam::err::*;
pub use crate::locked_waker::*;
use crate::trace_log;
pub use crate::waker_registry::*;
use crossbeam_queue::SegQueue;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering};
use std::sync::Arc;
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
    fn len(&self) -> usize {
        match self {
            Self::List(s) => s.len(),
            Self::Array(s) => s.len(),
        }
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        match self {
            Self::Array(s) => Some(s.capacity()),
            Self::List(_) => None,
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
    pub(crate) congest: AtomicIsize,
    pub(crate) inner: Channel<T>,
    pub(crate) senders: RegistrySender<T>,
    pub(crate) recvs: RegistryRecv,
    pub(crate) bound_size: Option<u32>,
    pub(crate) backoff_limit: u16,
    pub(crate) large: bool,
    pub(crate) may_direct_copy: bool,
}

impl<T> ChannelShared<T> {
    pub(crate) fn new(
        inner: Channel<T>, senders: RegistrySender<T>, recvs: RegistryRecv,
    ) -> Arc<Self> {
        let bound_size;
        let backoff_limit;
        let mut large = false;
        let may_direct_copy;
        backoff_limit = crate::backoff::DEFAULT_LIMIT;
        if let Some(bound) = inner.capacity() {
            bound_size = Some(bound as u32);
            if bound >= 10 {
                large = true;
                may_direct_copy = true;
            } else {
                may_direct_copy = false;
            }
        } else {
            bound_size = None;
            may_direct_copy = false;
        }
        Arc::new(Self {
            closed: AtomicBool::new(false),
            tx_count: AtomicUsize::new(1),
            rx_count: AtomicUsize::new(1),
            congest: AtomicIsize::new(0),
            senders,
            recvs,
            bound_size,
            backoff_limit,
            large,
            may_direct_copy,
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

    /// The number of messages in the channel.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// The capacity of the channel. Returns `None` for unbounded channels.
    #[inline(always)]
    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }

    /// Returns `true` if the channel is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns `true` if the channel is full.
    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    /// Returns `true` if all senders or receivers have been dropped.
    #[inline(always)]
    pub fn is_disconnected(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Returns the number of senders for the channel.
    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.tx_count.load(Ordering::Acquire) as usize
    }

    /// Returns the number of receivers for the channel.
    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.rx_count.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    pub(crate) fn sender_direct_copy(&self) -> bool {
        self.may_direct_copy && self.senders.use_direct_copy(self)
    }

    /// Returns the number of wakers for senders and receivers. For debugging purposes.
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }

    #[inline(always)]
    pub(crate) fn add_tx(&self) {
        let _ = self.tx_count.fetch_add(1, Ordering::Acquire);
        let _ = self.congest.fetch_add(1, Ordering::Acquire);
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        let _ = self.rx_count.fetch_add(1, Ordering::Acquire);
        let _ = self.congest.fetch_sub(1, Ordering::Acquire);
    }

    /// This method is called when a sender is dropped.
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        let _ = self.congest.fetch_sub(1, Ordering::Relaxed);
        let old = self.tx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from tx");
            self.closed.store(true, Ordering::SeqCst); // serve as fence
            self._close_all();
        } else {
            trace_log!("drop tx {}", old - 1);
        }
    }

    /// This method is called when a receiver is dropped.
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        let _ = self.congest.fetch_add(1, Ordering::Relaxed);
        let old = self.rx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from rx");
            self.closed.store(true, Ordering::SeqCst); // serve as fence
            self._close_all();
        } else {
            trace_log!("drop rx {}", old - 1);
        }
    }

    #[inline(always)]
    fn _close_all(&self) {
        self.senders.close();
        self.recvs.close();
    }

    /// Register waker for current rx
    #[inline(always)]
    pub(crate) fn reg_recv(&self, o_waker: &RecvWaker) {
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

    #[inline]
    pub(crate) fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        match &self.inner {
            Channel::Array(inner) => {
                return unsafe { inner.try_push_oneshot(item) };
            }
            Channel::List(_inner) => {
                unreachable!();
            }
        }
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    ///
    /// NOTE: when return state=Done, the waker is not set to Done
    #[inline]
    pub(crate) fn sender_reg_and_try(
        &self, item: &MaybeUninit<T>, waker: SendWaker<T>, sink: bool,
    ) -> (u8, Option<SendWaker<T>>) {
        self.senders.reg_waker(&waker);
        // Not allow Spurious wake and enter this function again;
        if let Some(res) = self.try_send_oneshot(item.as_ptr()) {
            if res {
                self.on_send();
                return self.senders.cancel_reuse_waker(waker, WakerState::Done);
            } else {
                if sink {
                    if self.is_disconnected() {
                        return (WakerState::Closed as u8, None);
                    } else {
                        // outside logic only recognize Waiting
                        return (WakerState::Waiting as u8, Some(waker));
                    }
                } else {
                    let state = waker.commit_waiting();
                    // let on_recv do it's job,
                    // is_disconnected == true means no receivers
                    if self.is_disconnected() {
                        return (WakerState::Closed as u8, None);
                    } else {
                        return (state, Some(waker));
                    }
                }
            }
        } else {
            // Unlikely to be disconnected,
            return self.senders.cancel_reuse_waker(waker, WakerState::Woken);
        }
    }

    /// Wait a little more for the waker state change,
    /// NOTE: it's important to yield when you have more sender than receiver
    #[inline(always)]
    pub(crate) fn sender_snooze(&self, waker: &SendWaker<T>, backoff: &mut Backoff) -> u8 {
        backoff.reset();
        loop {
            let state = waker.get_state_relaxed();
            if state >= WakerState::Woken as u8 {
                return state;
            }
            if backoff.snooze() {
                return state;
            }
        }
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        self.recvs.fire();
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        if WakeResult::Sent == self.senders.fire(self) {
            self.on_send();
        }
    }

    #[inline(always)]
    pub(crate) fn on_recv_try_send(&self, waker: &WakerInner<*const T>) -> WakeResult {
        waker.wake_or_copy(|p: *const T| -> u8 {
            if let Channel::Array(inner) = &self.inner {
                if unsafe { inner.push_with_ptr(p) } {
                    WakerState::Done as u8
                } else {
                    WakerState::Woken as u8
                }
            } else {
                unreachable!();
            }
        })
    }

    /// Call on cancellation, return true to indicate drop temporary message
    /// return false to indicate already Done.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(&self, waker: SendWaker<T>) -> bool {
        match waker.abandon() {
            Ok(()) => {
                trace_log!("tx: abandon cancel {:?}", waker);
                self.senders.clear_wakers(&waker);
                return true;
            }
            Err(state) => {
                trace_log!("tx: abandon err  {:?} {}", waker, state);
                if state == WakerState::Woken as u8 {
                    // We are awake, but give up sending, should notify another sender for safety
                    self.on_recv();
                    return true;
                } else if state == WakerState::Closed as u8 {
                    return true;
                } else if state == WakerState::Init as u8 {
                    // For dropping AsyncSink, clear only one
                    self.senders.cancel_waker(&waker);
                    return true;
                } else {
                    debug_assert_eq!(state, WakerState::Done as u8);
                    // Unused code for direct_copy
                    return false;
                }
            }
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(&self, waker: RecvWaker) -> bool {
        match waker.abandon() {
            Ok(()) => {
                trace_log!("rx: abandon cancel {:?}", waker);
                self.recvs.clear_wakers(&waker);
                return true;
            }
            Err(state) => {
                trace_log!("rx: abandon err {:?} {}", waker, state);
                if state == WakerState::Woken as u8 {
                    // We are awake, but give up receiving, should notify another receiver for safety
                    self.on_send();
                    return true;
                } else if state == WakerState::Closed as u8 {
                    // Closed
                    return true;
                } else if state == WakerState::Init as u8 {
                    // For AsyncStream::poll_item, clear only one
                    self.recvs.cancel_waker(&waker);
                    return true;
                } else {
                    debug_assert_eq!(state, WakerState::Done as u8);
                    // Unused code for direct_copy
                    return false;
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) fn get_async_backoff(&self) -> Option<Backoff> {
        if self.large {
            return None;
        }
        let cfg = BackoffConfig::default();
        if cfg.spin_limit == 0 {
            // 1 core don't backoff
            return None;
        }
        Some(Backoff::new(cfg))
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
