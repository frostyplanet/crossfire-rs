pub use super::waker_registry::*;
pub use crate::locked_waker::*;
pub use crossbeam::channel::{RecvError, RecvTimeoutError, TryRecvError};
pub use crossbeam::channel::{SendError, SendTimeoutError, TrySendError};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Context;

pub struct ChannelShared {
    tx_count: AtomicU64,
    rx_count: AtomicU64,
    recvs: Registry,
    senders: Registry,
    closed: AtomicBool,
}

impl ChannelShared {
    pub(crate) fn new(senders: Registry, recvs: Registry) -> Arc<Self> {
        Arc::new(Self {
            tx_count: AtomicU64::new(1),
            rx_count: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            senders,
            recvs,
        })
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
            self.recvs.close();
        }
    }

    /// Call when rx drop
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        if self.rx_count.fetch_sub(1, Ordering::SeqCst) <= 1 {
            self.closed.store(true, Ordering::Release);
            self.senders.close();
        }
    }

    /// Register waker for current rx
    #[inline(always)]
    pub(crate) fn reg_recv_async(&self, ctx: &mut Context) -> LockedWaker {
        self.recvs.reg_async(ctx)
    }

    /// Register waker for current tx
    #[inline(always)]
    pub(crate) fn reg_send_async(&self, ctx: &mut Context) -> LockedWaker {
        self.senders.reg_async(ctx)
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        self.recvs.fire()
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        self.senders.fire()
    }

    /// On timeout, clear dead wakers on sender queue
    #[inline(always)]
    pub(crate) fn clear_send_wakers(&self, seq: u64) {
        self.senders.clear_wakers(seq);
    }

    /// On timeout, clear dead wakers on receiver queue
    #[inline(always)]
    pub(crate) fn clear_recv_wakers(&self, seq: u64) {
        self.recvs.clear_wakers(seq);
    }

    /// Just for debugging purpose, to monitor queue size
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }
}
