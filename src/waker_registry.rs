use crate::locked_waker::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
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
    fn get_size(&self) -> usize;
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
    fn get_size(&self) -> usize {
        0
    }
}

pub struct RegistrySingle {
    queue: ArrayQueue<LockedWakerRef>,
}

impl RegistrySingle {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Single(Self { queue: ArrayQueue::new(1) })
    }
}

impl RegistryTrait for RegistrySingle {
    /// return is_skip
    #[inline(always)]
    fn reg_async(&self, ctx: &mut Context) -> LockedWaker {
        let waker = LockedWaker::new(ctx, 0);
        match self.queue.push(waker.weak()) {
            Ok(_) => {}
            Err(_weak) => {
                if let Some(old_waker) = self.queue.pop() {
                    _weak.check_eq(old_waker);
                    self.queue.push(_weak).expect("Do not misuse mpsc as mpmc");
                } else {
                    self.queue.push(_weak).expect("Do not misuse mpsc as mpmc");
                }
            }
        }
        waker
    }

    #[inline(always)]
    fn cancel_waker(&self, waker: LockedWaker) {
        // Got to be it, because only one single thread.
        if let Some(waker_ref) = self.queue.pop() {
            // protect miss-use of multi thread
            waker.weak().check_eq(waker_ref);
        }
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: u64) {
        // Got to be it, because only one single thread.
        if let Some(_waker) = self.queue.pop() {
            _waker.wake();
        }
    }

    #[inline(always)]
    fn fire(&self) {
        if let Some(waker) = self.queue.pop() {
            waker.wake();
        }
    }

    #[inline(always)]
    fn close(&self) {
        self.fire();
    }

    /// return waker queue size
    #[inline(always)]
    fn get_size(&self) -> usize {
        self.queue.len()
    }
}

pub struct RegistryMulti {
    queue: SegQueue<LockedWakerRef>,
    seq: AtomicU64,
    checking: AtomicBool,
}

impl RegistryMulti {
    #[inline(always)]
    pub fn new() -> Registry {
        Registry::Multi(Self {
            queue: SegQueue::new(),
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
    fn get_size(&self) -> usize {
        self.queue.len()
    }
}
