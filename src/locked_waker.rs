use crate::collections::WeakCell;
use std::fmt;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Weak,
};
use std::task::*;
use std::thread;

/// Waker object used by [crate::AsyncTx::poll_send()] and [crate::AsyncRx::poll_item()]
pub struct LockedWaker(Arc<LockedWakerInner>);

impl LockedWaker {
    #[inline(always)]
    pub(crate) fn new_async(ctx: &Context) -> Self {
        Self(Arc::new(LockedWakerInner {
            seq: AtomicU64::new(0),
            waked: AtomicBool::new(false),
            waker: WakerType::Async(ctx.waker().clone()),
        }))
    }

    #[inline(always)]
    pub(crate) fn new_blocking() -> Self {
        Self(Arc::new(LockedWakerInner {
            seq: AtomicU64::new(0),
            waked: AtomicBool::new(true),
            waker: WakerType::Blocking(thread::current()),
        }))
    }

    // return is_already waked
    #[inline(always)]
    pub(crate) fn abandon(&self) -> bool {
        self.0.waked.swap(true, Ordering::SeqCst)
    }

    #[inline(always)]
    pub(crate) fn cancel(&self) {
        self.0.waked.store(true, Ordering::Release)
    }

    #[inline(always)]
    pub(crate) fn get_seq(&self) -> u64 {
        self.0.seq.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(crate) fn set_seq(&self, seq: u64) {
        self.0.seq.store(seq, Ordering::Release);
    }

    #[inline(always)]
    pub(crate) fn is_waked(&self) -> bool {
        self.0.waked.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub(crate) fn weak(&self) -> LockedWakerRef {
        self.0.waked.store(false, Ordering::Release);
        LockedWakerRef(Arc::downgrade(&self.0))
    }
}

impl fmt::Debug for LockedWaker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _self = self.0.as_ref();
        write!(
            f,
            "LockedWaker(seq={}, waked={})",
            _self.seq.load(Ordering::Acquire),
            _self.waked.load(Ordering::Acquire)
        )
    }
}

enum WakerType {
    Async(Waker),
    Blocking(thread::Thread),
}

struct LockedWakerInner {
    waked: AtomicBool,
    seq: AtomicU64,
    waker: WakerType,
}

unsafe impl Send for LockedWakerInner {}
unsafe impl Sync for LockedWakerInner {}

pub struct LockedWakerRef(Weak<LockedWakerInner>);

impl fmt::Debug for LockedWakerRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LockedWakerRef")
    }
}

impl LockedWakerInner {
    /// return true on suc wake up, false when already woken up.
    #[inline(always)]
    pub fn wake(&self) -> bool {
        if self.waked.swap(true, Ordering::SeqCst) {
            return false;
        }
        match &self.waker {
            WakerType::Async(waker) => waker.wake_by_ref(),
            WakerType::Blocking(th) => th.unpark(),
        }
        true
    }
}

impl LockedWakerRef {
    #[inline(always)]
    pub(crate) fn wake(&self) -> bool {
        if let Some(w) = self.0.upgrade() {
            return w.wake();
        } else {
            return false;
        }
    }

    /// return true to stop; return false to continue the search.
    pub(crate) fn try_to_clear(&self, seq: u64) -> bool {
        if let Some(waker) = self.0.upgrade() {
            let _seq = waker.seq.load(Ordering::Acquire);
            if _seq == seq {
                // It's my waker, stopped
                return true;
            }
            waker.wake();
            return _seq > seq;
        }
        return false;
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn check_eq(&self, other: LockedWakerRef) -> bool {
        if self.0.ptr_eq(&other.0) {
            return true;
        }
        other.wake();
        false
    }
}

pub struct WakerCell(WeakCell<LockedWakerInner>);

impl WakerCell {
    #[inline(always)]
    pub fn new() -> Self {
        Self(WeakCell::new())
    }

    #[inline(always)]
    pub fn wake(&self) -> bool {
        if let Some(waker) = self.0.pop() {
            return waker.wake();
        }
        false
    }

    #[inline(always)]
    pub fn clear(&self) {
        self.0.clear();
    }

    #[inline(always)]
    pub fn put(&self, waker: LockedWakerRef) {
        self.0.put(waker.0);
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn exists(&self) -> bool {
        self.0.exists()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_waker() {
        println!("waker size {}", std::mem::size_of::<LockedWakerRef>());
        println!("arc size {}", std::mem::size_of::<Arc<WakerCell>>());
        println!("arc size {}", std::mem::size_of::<Weak<WakerCell>>());
    }
}
