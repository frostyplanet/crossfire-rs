use crate::backoff::*;
pub(crate) use crate::crossbeam::err::*;
pub(crate) use crate::flavor::{ChannelSharedSelect, Flavor, FlavorSelect, Token};
use crate::trace_log;
pub(crate) use crate::waker::*;
pub(crate) use crate::waker_registry::*;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{compiler_fence, fence, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct ChannelShared<F: Flavor> {
    pub(crate) inner: F,
    tx_count: AtomicUsize,
    rx_count: AtomicUsize,
    // initially contains two (1 for tx and 1 for rx)
    weak_count: AtomicUsize,
    pub(crate) senders: F::Send,
    pub(crate) recvs: F::Recv,
    pub(crate) backoff_limit: u16,
    pub(crate) large: bool,
}

impl<F: Flavor> ChannelShared<F> {
    pub(crate) fn new(inner: F, senders: F::Send, recvs: F::Recv) -> NonNull<Self> {
        let mut large = false;
        if let Some(bound) = inner.capacity() {
            if bound >= 10 {
                large = true;
            }
        }
        unsafe {
            NonNull::new_unchecked(Box::into_raw(Box::new(Self {
                tx_count: AtomicUsize::new(1),
                rx_count: AtomicUsize::new(1),
                weak_count: AtomicUsize::new(2),
                senders,
                recvs,
                backoff_limit: inner.backoff_limit(),
                large,
                inner,
            })))
        }
    }

    #[inline(always)]
    pub(crate) fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        if !self.is_tx_closed() {
            if let Some(item) = self.inner.try_recv() {
                self.on_recv();
                return Ok(item);
            }
        } else {
            if let Some(item) = self.inner.try_recv_final() {
                self.on_recv();
                return Ok(item);
            }
            return Err(TryRecvError::Disconnected);
        }
        Err(TryRecvError::Empty)
    }

    #[inline(always)]
    pub(crate) fn read_with_token(&self, token: Token) -> Result<F::Item, RecvError>
    where
        F: FlavorSelect,
    {
        if token.pos.is_null() {
            Err(RecvError)
        } else {
            let item = self.inner.read_with_token(token);
            self.on_recv();
            Ok(item)
        }
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

    /// Returns the number of senders for the channel.
    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.tx_count.load(Ordering::Acquire)
    }

    /// Returns the number of receivers for the channel.
    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.rx_count.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(crate) fn might_congest(&self) -> bool {
        F::IS_BOUNDED && self.senders.use_direct_copy()
    }

    /// Returns the number of wakers for senders and receivers. For debugging purposes.
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }

    #[inline(always)]
    pub(crate) fn is_tx_closed(&self) -> bool {
        self.tx_count.load(Ordering::SeqCst) == 0
    }

    #[inline(always)]
    pub(crate) fn is_rx_closed(&self) -> bool {
        self.rx_count.load(Ordering::SeqCst) == 0
    }

    #[inline(always)]
    pub(crate) fn add_tx(&self) {
        // The drop will close_tx, which has release fence
        let _ = self.tx_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_tx_weak(&self) {
        let _ = self.weak_count.fetch_add(1, Ordering::Relaxed);
    }

    /// for Upgrade of WeakTx
    #[inline(always)]
    pub(crate) fn try_add_tx(&self) -> bool {
        let mut count = self.tx_count.load(Ordering::Relaxed);
        loop {
            if count == 0 {
                return false;
            }
            match self.tx_count.compare_exchange(
                count,
                count + 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return true;
                }
                Err(_count) => {
                    count = _count;
                    std::hint::spin_loop();
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        // The drop will close_rx, which has release fence
        let _ = self.rx_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn dec_ref(this: *mut Self) {
        unsafe {
            if 1 == (*this).weak_count.fetch_sub(1, Ordering::Release) {
                fence(Ordering::Acquire);
                // drop the ChannelShared
                let _ = Box::from_raw(this);
            }
        }
    }

    /// This method is called when a sender is dropped.
    #[inline(always)]
    pub(crate) fn close_tx(this: *mut Self) {
        unsafe {
            let old = (*this).tx_count.fetch_sub(1, Ordering::Release);
            if old <= 1 {
                trace_log!("closing from tx");
                // Safety NOTE it has SeqCst fence for is_tx_closed/is_rx_closed
                fence(Ordering::SeqCst);
                (*this).recvs.close();
                Self::dec_ref(this);
            } else {
                trace_log!("drop tx {}", old - 1);
            }
        }
    }

    /// This method is called when a receiver is dropped.
    #[inline(always)]
    pub(crate) fn close_rx(this: *mut Self) {
        unsafe {
            let old = (*this).rx_count.fetch_sub(1, Ordering::Release);
            if old <= 1 {
                trace_log!("closing from rx");
                // Safety NOTE it has SeqCst fence for is_tx_closed/is_rx_closed
                fence(Ordering::SeqCst);
                (*this).senders.close();
                Self::dec_ref(this);
            } else {
                trace_log!("drop rx {}", old - 1);
            }
        }
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    ///
    /// NOTE: when return state=Done, the waker is not set to Done
    #[inline]
    pub(crate) fn sender_double_check<const SINK: bool>(
        &self, item: &MaybeUninit<F::Item>, o_waker: &mut Option<<F::Send as Registry>::Waker>,
    ) -> u8 {
        // Not allow Spurious wake and enter this function again;
        if let Some(res) = self.inner.try_send_oneshot(item.as_ptr()) {
            if res {
                self.on_send();
                self.senders.cancel_reuse_waker(o_waker, WakerState::Done)
            } else {
                let state = if SINK {
                    WakerState::Init as u8
                } else {
                    self.senders.commit_waiting(o_waker)
                };
                if self.is_rx_closed() {
                    return WakerState::Closed as u8;
                }
                state
            }
        } else {
            // Unlikely to be disconnected,
            self.senders.cancel_reuse_waker(o_waker, WakerState::Woken)
        }
    }

    /// Wait a little more for the waker state change,
    /// NOTE: it's important to yield when you have more sender than receiver
    #[inline(always)]
    pub(crate) fn sender_snooze(
        &self, o_waker: &Option<<F::Send as Registry>::Waker>, backoff: &mut Backoff,
    ) -> u8 {
        backoff.reset();
        loop {
            let state = self.senders.get_waker_state(o_waker, Ordering::Relaxed);
            compiler_fence(Ordering::AcqRel);
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
        self.senders.fire();
        //        if WakeResult::Sent == self.senders.fire(&self.inner) {
        //            self.on_send();
        //        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    /// return false to indicate already Done.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(&self, waker: &<F::Send as Registry>::Waker) -> bool {
        match self.senders.abandon_waker(waker) {
            Ok(_) => true,
            Err(state) => {
                trace_log!("tx: abandon err  {:?} {}", waker, state);
                if state == WakerState::Woken as u8 {
                    // We are awake, but give up sending, should notify another sender for safety
                    self.on_recv();
                } else if state == WakerState::Closed as u8 {
                } else {
                    debug_assert_eq!(state, WakerState::Done as u8);
                    // Unused code for direct_copy
                    return false;
                }
                true
            }
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(&self, waker: &<F::Recv as Registry>::Waker) {
        if let Err(state) = self.recvs.abandon_waker(waker) {
            trace_log!("rx: abandon err {:?} {}", waker, state);
            if state == WakerState::Woken as u8 {
                // We are awake, but give up receiving, should notify another receiver for safety
                self.on_send();
            } else if state == WakerState::Closed as u8 {
                // Closed
            } else {
                debug_assert_eq!(state, WakerState::Done as u8);
                // Unused code for direct_copy
            }
        }
    }

    #[inline(always)]
    pub(crate) fn get_async_backoff(&self) -> Option<Backoff> {
        if self.large {
            return None;
        }
        let cfg = BackoffConfig::detect();
        if cfg.spin_limit == 0 {
            // 1 core don't backoff
            return None;
        }
        // It's effective to yield for size=1
        Some(Backoff::from(cfg.limit(self.backoff_limit)))
    }
}

impl<F: Flavor + FlavorSelect> ChannelSharedSelect for ChannelShared<F> {
    #[inline(always)]
    fn try_select(&self, final_check: bool) -> Option<Token> {
        // NOTE: we cannot use generic const for final_check because ChannelSharedSelect trait
        // require Object Safety
        let closed = if !final_check { false } else { self.is_tx_closed() };
        if let Some(token) = self.inner.try_select(final_check) {
            return Some(token);
        }
        if closed {
            return Some(Token::default());
        }
        None
    }

    #[inline(always)]
    fn reg_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool {
        self.recvs.reg_select_waker(channel_id, waker)
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: &Arc<SelectWaker>) {
        self.recvs.cancel_select_waker(waker)
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
