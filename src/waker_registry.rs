use crate::collections::LockedQueue;
use crate::locked_waker::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::Context;

#[enum_dispatch(RegistryTrait)]
pub enum Registry {
    Single(RegistrySingle),
    Multi(RegistryMulti),
    Dummy(RegistryDummy),
}

#[enum_dispatch]
pub trait RegistryTrait {
    /// For async context
    fn reg_async(&self, _ctx: &mut Context) -> LockedWaker;

    fn clear_wakers(&self, _seq: u64);

    fn cancel_waker(&self, _waker: LockedWaker);

    fn fire(&self);

    fn close(&self);

    /// return waker queue size
    fn len(&self) -> usize;
}

/// RegistryDummy is for unbounded channel tx, which is never blocked
pub struct RegistryDummy();

impl RegistryDummy {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Dummy(RegistryDummy())
    }
}

impl RegistryTrait for RegistryDummy {
    #[inline(always)]
    fn reg_async(&self, _ctx: &mut Context) -> LockedWaker {
        unreachable!();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64) {}

    #[inline(always)]
    fn cancel_waker(&self, _waker: LockedWaker) {}

    #[inline(always)]
    fn fire(&self) {}

    #[inline(always)]
    fn close(&self) {}

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }
}

pub struct RegistrySingle {
    cell: WakerCell,
}

impl RegistrySingle {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Single(Self { cell: WakerCell::new() })
    }
}

impl RegistryTrait for RegistrySingle {
    /// return is_skip
    #[inline(always)]
    fn reg_async(&self, ctx: &mut Context) -> LockedWaker {
        let waker = LockedWaker::new(ctx, 0);
        self.cell.put(waker.weak());
        waker
    }

    #[inline(always)]
    fn cancel_waker(&self, _waker: LockedWaker) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn fire(&self) {
        self.cell.wake();
    }

    #[inline(always)]
    fn close(&self) {
        self.fire();
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        if self.cell.exists() {
            1
        } else {
            0
        }
    }
}

pub struct RegistryMulti {
    queue: LockedQueue<LockedWakerRef>,
    seq: AtomicU64,
    checking: AtomicBool,
}

impl RegistryMulti {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Multi(Self {
            queue: LockedQueue::new(32),
            seq: AtomicU64::new(0),
            checking: AtomicBool::new(false),
        })
    }
}

impl RegistryTrait for RegistryMulti {
    #[inline(always)]
    fn reg_async(&self, ctx: &mut Context) -> LockedWaker {
        let waker = LockedWaker::new(ctx, self.seq.fetch_add(1, Ordering::SeqCst));
        self.queue.push(waker.weak());
        waker
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: LockedWaker) {
        // Just abandon and leave it to fire() to clean it
        waker.cancel();
    }

    /// Call when ReceiveFuture is cancelled.
    /// to clear the LockedWakerRef which has been sent to the other side.
    #[inline(always)]
    fn clear_wakers(&self, seq: u64) {
        if self.checking.swap(true, Ordering::SeqCst) {
            // Other thread is cleaning
            return;
        }
        while let Some(waker_ref) = self.queue.pop() {
            if waker_ref.try_to_clear(seq) {
                // we do not known push back may have concurrent problem
                break;
            }
        }
        self.checking.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn fire(&self) {
        while let Some(waker) = self.queue.pop() {
            if waker.wake() {
                return;
            }
        }
    }

    #[inline(always)]
    fn close(&self) {
        while let Some(waker) = self.queue.pop() {
            waker.wake();
        }
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        self.queue.len()
    }
}
