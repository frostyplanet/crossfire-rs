use crate::collections::WeakCell;
use std::fmt;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Weak,
};
use std::task::*;

/// Waker object used by [crate::AsyncTx::poll_send()] and [crate::AsyncRx::poll_item()]
pub struct LockedWaker(Arc<LockedWakerInner>);

impl fmt::Debug for LockedWaker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _self = self.0.as_ref();
        write!(f, "LockedWaker(seq={}, state={})", _self.seq, _self.state.load(Ordering::Acquire))
    }
}

#[repr(u8)]
pub(crate) enum WakerState {
    INIT = 0,
    WAITING = 1,
    WAKED = 2,
}

struct LockedWakerInner {
    waker: std::task::Waker,
    state: AtomicU8,
    seq: u64,
}

pub struct LockedWakerRef(Weak<LockedWakerInner>);

impl fmt::Debug for LockedWakerRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LockedWakerRef")
    }
}

impl LockedWaker {
    #[inline(always)]
    pub(crate) fn new(ctx: &Context, seq: u64) -> Self {
        let s = Arc::new(LockedWakerInner {
            seq,
            waker: ctx.waker().clone(),
            state: AtomicU8::new(WakerState::INIT as u8),
        });
        Self(s)
    }

    #[inline(always)]
    pub(crate) fn get_seq(&self) -> u64 {
        self.0.seq
    }

    // return is_already waked
    #[inline(always)]
    pub(crate) fn cancel(&self) {
        self.0.state.store(WakerState::WAKED as u8, Ordering::Release)
    }

    pub(crate) fn commit(&self) {
        self.0.state.store(WakerState::WAITING as u8, Ordering::Release);
    }

    // return is_already waked
    #[inline(always)]
    pub(crate) fn abandon(&self) -> u8 {
        return self.0.state.swap(WakerState::WAKED as u8, Ordering::SeqCst);
    }

    #[inline(always)]
    pub(crate) fn weak(&self) -> LockedWakerRef {
        LockedWakerRef(Arc::downgrade(&self.0))
    }

    #[inline(always)]
    pub(crate) fn is_waked(&self) -> bool {
        self.0.state.load(Ordering::Acquire) == WakerState::WAKED as u8
    }

    /// return true on suc wake up, false when already woken up.
    #[inline(always)]
    pub(crate) fn wake(&self) -> bool {
        let _self = self.0.as_ref();
        let old_state = _self.state.swap(WakerState::WAKED as u8, Ordering::SeqCst);
        // No matter the flag, call wake anyway
        if old_state != WakerState::WAKED as u8 {
            _self.waker.wake_by_ref();
        }
        // NOTE: INIT is in between state
        return old_state == WakerState::WAITING as u8;
    }
}

impl LockedWakerRef {
    #[inline(always)]
    pub(crate) fn wake(&self) -> bool {
        if let Some(_self) = self.0.upgrade() {
            return LockedWaker(_self).wake();
        } else {
            return false;
        }
    }

    /// return true to stop; return false to continue the search.
    pub(crate) fn try_to_clear(&self, seq: u64) -> bool {
        if let Some(w) = self.0.upgrade() {
            let waker = LockedWaker(w);
            let _seq = waker.get_seq();
            if _seq == seq {
                // It's my waker, stopped
                return true;
            }
            if !waker.is_waked() {
                waker.wake();
                // other future is before me, but not canceled, i should stop.
                // we do not known push back may have concurrent problem
                return true;
            }
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
    pub fn pop(&self) -> Option<LockedWaker> {
        if let Some(waker) = self.0.pop() {
            return Some(LockedWaker(waker));
        }
        None
    }

    #[inline(always)]
    pub fn clear(&self) {
        self.0.clear();
    }

    #[inline(always)]
    pub fn put(&self, waker: LockedWakerRef) {
        self.0.put(waker.0);
    }

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
