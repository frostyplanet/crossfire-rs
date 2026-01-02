use crate::backoff::*;
pub(crate) use crate::crossbeam::err::*;
pub(crate) use crate::flavor::Flavor;
use crate::trace_log;
pub(crate) use crate::waker::*;
pub(crate) use crate::waker_registry::*;
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, fence, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct ChannelShared<F: Flavor> {
    pub(crate) inner: F,
    tx_count: AtomicUsize,
    rx_count: AtomicUsize,
    pub(crate) senders: RegistrySender<F::Item>,
    pub(crate) recvs: RegistryRecv,
    pub(crate) backoff_limit: u16,
    pub(crate) large: bool,
    pub(crate) may_direct_copy: bool,
}

impl<F: Flavor> ChannelShared<F> {
    pub(crate) fn new(
        inner: F, senders: RegistrySender<F::Item>, recvs: RegistryRecv,
    ) -> Arc<Self> {
        let mut large = false;
        if let Some(bound) = inner.capacity() {
            if bound >= 10 {
                large = true;
            }
        }
        Arc::new(Self {
            tx_count: AtomicUsize::new(1),
            rx_count: AtomicUsize::new(1),
            senders,
            recvs,
            backoff_limit: inner.backoff_limit(),
            large,
            may_direct_copy: inner.may_direct_copy(),
            inner,
        })
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
        self.tx_count.load(Ordering::SeqCst) as usize
    }

    /// Returns the number of receivers for the channel.
    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.rx_count.load(Ordering::SeqCst) as usize
    }

    #[inline(always)]
    pub(crate) fn sender_direct_copy(&self) -> bool {
        self.may_direct_copy && self.senders.use_direct_copy()
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

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        // The drop will close_rx, which has release fence
        let _ = self.rx_count.fetch_add(1, Ordering::Relaxed);
    }

    /// This method is called when a sender is dropped.
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        let old = self.tx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from tx");
            fence(Ordering::SeqCst);
            self.recvs.close();
        } else {
            trace_log!("drop tx {}", old - 1);
        }
    }

    /// This method is called when a receiver is dropped.
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        let old = self.rx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from rx");
            fence(Ordering::SeqCst);
            // There's SeqCst fence inside RegistrySender::close
            self.senders.close();
        } else {
            trace_log!("drop rx {}", old - 1);
        }
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    ///
    /// NOTE: when return state=Done, the waker is not set to Done
    #[inline]
    pub(crate) fn sender_double_check<const SINK: bool>(
        &self, item: &MaybeUninit<F::Item>, o_waker: &mut Option<SendWaker<F::Item>>,
    ) -> u8 {
        // Not allow Spurious wake and enter this function again;
        if let Some(res) = self.inner.try_send_oneshot(item.as_ptr()) {
            if res {
                self.on_send();
                return self.senders.cancel_reuse_waker(o_waker, WakerState::Done);
            } else {
                let state = if SINK {
                    self.senders.commit_waiting(&o_waker)
                } else {
                    WakerState::Waiting as u8
                };
                if self.is_rx_closed() {
                    return WakerState::Closed as u8;
                }
                return state;
            }
        } else {
            // Unlikely to be disconnected,
            return self.senders.cancel_reuse_waker(o_waker, WakerState::Woken);
        }
    }

    /// Wait a little more for the waker state change,
    /// NOTE: it's important to yield when you have more sender than receiver
    #[inline(always)]
    pub(crate) fn sender_snooze(
        &self, o_waker: &Option<SendWaker<F::Item>>, backoff: &mut Backoff,
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
        if WakeResult::Sent == self.senders.fire(self) {
            self.on_send();
        }
    }

    #[inline(always)]
    pub(crate) fn on_recv_try_send(&self, waker: &WakerInner<*const F::Item>) -> WakeResult {
        waker.wake_or_copy(|p: *const F::Item| -> u8 {
            if let Some(true) = self.inner.try_send_oneshot(p) {
                WakerState::Done as u8
            } else {
                WakerState::Woken as u8
            }
        })
    }

    /// Call on cancellation, return true to indicate drop temporary message
    /// return false to indicate already Done.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(&self, o_waker: &mut Option<SendWaker<F::Item>>) -> bool {
        match o_waker.take() {
            Some(SendWaker::Multi(waker)) => {
                // which change Waiting/Init to Closed
                match waker.abandon() {
                    Ok(()) => {
                        trace_log!("tx: abandon cancel {:?}", waker);
                        self.senders.clear_wakers(&waker);
                    }
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
                    }
                }
                return true;
            }
            Some(SendWaker::Single) => true,
            None => false,
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(&self, o_waker: &mut Option<RecvWaker>) {
        if let Some(RecvWaker::Multi(waker)) = o_waker.take() {
            // which change Waiting/Init to Closed
            match waker.abandon() {
                Ok(()) => {
                    trace_log!("rx: abandon cancel {:?}", waker);
                    self.recvs.clear_wakers(&waker);
                    return;
                }
                Err(state) => {
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
