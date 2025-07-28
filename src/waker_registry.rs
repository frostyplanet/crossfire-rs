use crate::backoff::BackoffConfig;
use crate::collections::WeakCell;
use crate::locked_waker::*;
use crate::spinlock::Spinlock;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;

pub enum RegistrySender<T> {
    Single(RegistrySingle<SendWaker<T>>),
    Multi(RegistryMulti<SendWaker<T>>),
    Dummy(RegistryDummy<SendWaker<T>>),
}

impl<T> RegistrySender<T> {
    #[inline(always)]
    pub fn not_congest(&self) -> bool {
        match self {
            RegistrySender::Single(_) => true,
            RegistrySender::Multi(inner) => inner.is_empty(),
            RegistrySender::Dummy(_) => true,
        }
    }

    #[inline(always)]
    pub fn reg_waker(&self, waker: &SendWaker<T>) {
        debug_assert_eq!(waker.get_state(), WakerState::WAKED as u8);
        waker.set_state(WakerState::WAITING);
        // Clear the ptr in waker if it want to re-register
        waker.set_ptr(ptr::null_mut());
        match self {
            RegistrySender::Single(inner) => inner.reg_waker(waker),
            RegistrySender::Multi(inner) => inner.reg_waker(waker),
            RegistrySender::Dummy(_) => {}
        }
    }

    #[inline(always)]
    pub fn clear_wakers(&self, seq: usize) {
        match self {
            RegistrySender::Single(inner) => inner.clear_wakers(seq),
            RegistrySender::Multi(inner) => inner.clear_wakers(seq),
            RegistrySender::Dummy(_) => {}
        }
    }

    #[inline(always)]
    pub fn cancel_waker(&self) {
        match self {
            RegistrySender::Single(inner) => inner.cancel_waker(),
            _ => {}
        }
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<SendWaker<T>> {
        match self {
            RegistrySender::Single(inner) => inner.pop(),
            RegistrySender::Multi(inner) => inner.pop(),
            RegistrySender::Dummy(_) => None,
        }
    }

    /// return waker queue size
    pub fn len(&self) -> usize {
        match self {
            RegistrySender::Single(inner) => inner.len(),
            RegistrySender::Multi(inner) => inner.len(),
            RegistrySender::Dummy(_) => 0,
        }
    }
}

pub enum RegistryRecv {
    Single(RegistrySingle<RecvWaker>),
    Multi(RegistryMulti<RecvWaker>),
}

impl RegistryRecv {
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        match self {
            RegistryRecv::Single(inner) => inner.is_empty(),
            RegistryRecv::Multi(inner) => inner.is_empty(),
        }
    }

    #[inline(always)]
    pub fn reg_waker(&self, waker: &RecvWaker) -> Result<(), u8> {
        let state = waker.get_state();
        if state == WakerState::WAKED as u8 {
            waker.set_state(WakerState::INIT);
        } else {
            // Might be WAITING
            return Err(state);
        }
        match self {
            RegistryRecv::Single(inner) => inner.reg_waker(waker),
            RegistryRecv::Multi(inner) => inner.reg_waker(waker),
        }
        Ok(())
    }

    #[inline(always)]
    pub fn clear_wakers(&self, seq: usize) {
        match self {
            RegistryRecv::Single(inner) => inner.clear_wakers(seq),
            RegistryRecv::Multi(inner) => inner.clear_wakers(seq),
        }
    }

    #[inline(always)]
    pub fn cancel_waker(&self) {
        match self {
            RegistryRecv::Single(inner) => inner.cancel_waker(),
            _ => {}
        }
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<RecvWaker> {
        match self {
            RegistryRecv::Single(inner) => inner.pop(),
            RegistryRecv::Multi(inner) => inner.pop(),
        }
    }

    /// return waker queue size
    pub fn len(&self) -> usize {
        match self {
            RegistryRecv::Single(inner) => inner.len(),
            RegistryRecv::Multi(inner) => inner.len(),
        }
    }
}

/// RegistryDummy is for unbounded channel tx, which is never blocked
pub struct RegistryDummy<W: WakerTrait>(PhantomData<W>);

impl<W: WakerTrait> RegistryDummy<W> {
    #[inline(always)]
    pub fn new() -> Self {
        Self(Default::default())
    }
}

pub struct RegistrySingle<W: WakerTrait> {
    cell: WeakCell<W::Inner>,
}

impl<W: WakerTrait> RegistrySingle<W> {
    #[inline(always)]
    pub fn new() -> Self {
        Self { cell: WeakCell::new() }
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn is_empty(&self) -> bool {
        !self.cell.exists()
    }

    /// return is_skip
    #[inline(always)]
    fn reg_waker(&self, waker: &W) {
        self.cell.put(waker.weak());
    }

    #[inline(always)]
    fn cancel_waker(&self) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn clear_wakers(&self, _seq: usize) {
        // Got to be it, because only one single thread.
        self.cell.clear();
    }

    #[inline(always)]
    fn pop(&self) -> Option<W> {
        if let Some(w) = self.cell.pop() {
            Some(W::from_arc(w))
        } else {
            None
        }
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

struct RegistryMultiInner<W: WakerTrait> {
    queue: VecDeque<Weak<W::Inner>>,
    seq: usize,
}

pub struct RegistryMulti<W: WakerTrait> {
    checking: AtomicBool,
    is_empty: AtomicBool,
    inner: Spinlock<RegistryMultiInner<W>>,
    backoff: BackoffConfig,
}

impl<W: WakerTrait> RegistryMulti<W> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: Spinlock::new(RegistryMultiInner { queue: VecDeque::with_capacity(32), seq: 0 }),
            checking: AtomicBool::new(false),
            is_empty: AtomicBool::new(true),
            backoff: BackoffConfig::default(),
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.is_empty.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn reg_waker(&self, waker: &W) {
        let weak = waker.weak();
        let mut guard = self.inner.lock(self.backoff);
        let seq = guard.seq.wrapping_add(1);
        guard.seq = seq;
        waker.set_seq(seq);
        if guard.queue.is_empty() {
            self.is_empty.store(false, Ordering::Release);
        }
        guard.queue.push_back(weak);
    }

    /// Call when ReceiveFuture is cancelled.
    /// to clear the LockedWakerRef which has been sent to the other side.
    #[inline(always)]
    fn clear_wakers(&self, seq: usize) {
        if self.checking.swap(true, Ordering::SeqCst) {
            // Other thread is cleaning
            return;
        }
        while let Some(w) = self.pop() {
            if w.try_to_clear(seq) {
                // we do not known push back may have concurrent problem
                break;
            }
        }
        self.checking.store(false, Ordering::Release);
    }

    #[inline(always)]
    fn pop(&self) -> Option<W> {
        if self.is_empty.load(Ordering::Acquire) {
            return None;
        }
        let mut waker: Option<W> = None;
        let mut guard = self.inner.lock(self.backoff);
        while let Some(weak) = guard.queue.pop_front() {
            if let Some(_waker) = weak.upgrade() {
                waker = Some(W::from_arc(_waker));
                break;
            }
        }
        if guard.queue.is_empty() {
            self.is_empty.store(true, Ordering::Release);
        }
        return waker;
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        let guard = self.inner.lock(self.backoff);
        guard.queue.len()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::locked_waker::RecvWaker;
    #[test]
    fn test_registry_multi() {
        let reg = RegistryRecv::Multi(RegistryMulti::new());

        // test push
        let waker1 = RecvWaker::new_blocking();
        assert_eq!(reg.is_empty(), true);
        assert_eq!(waker1.get_state(), WakerState::WAKED as u8);
        reg.reg_waker(&waker1).expect("reg");
        assert_eq!(waker1.get_state(), WakerState::INIT as u8);
        assert!(waker1.get_seq() > 0);
        assert_eq!(reg.is_empty(), false);
        assert_eq!(reg.len(), 1);
        assert_eq!(waker1.is_waked(), false);

        let waker2 = RecvWaker::new_blocking();
        reg.reg_waker(&waker2).expect("reg");
        assert_eq!(reg.len(), 2);
        assert_eq!(waker2.get_seq(), waker1.get_seq() + 1);
        assert_eq!(waker2.is_waked(), false);

        if let Some(w) = reg.pop() {
            assert!(w.wake_simple().is_ok());
        }
        assert_eq!(waker1.is_waked(), true);
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.is_empty(), false);
        if let Some(w) = reg.pop() {
            assert!(w.wake_simple().is_ok());
        }
        assert_eq!(waker2.is_waked(), true);
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.is_empty(), true);

        // test seq

        let waker3 = RecvWaker::new_blocking();
        reg.reg_waker(&waker3).expect("reg");
        let waker4 = RecvWaker::new_blocking();
        reg.reg_waker(&waker4).expect("reg");
        waker4.set_state(WakerState::WAITING);
        for _ in 0..10 {
            let _waker = RecvWaker::new_blocking();
            reg.reg_waker(&_waker).expect("reg");
        }
        assert_eq!(reg.len(), 12);
        assert_eq!(waker4.abandon(), WakerState::CLOSED as u8);
        reg.clear_wakers(waker4.get_seq());
        assert_eq!(reg.len(), 10);
        assert!(waker3.is_waked());
        assert!(waker4.is_waked());

        // test close
        assert_eq!(reg.is_empty(), false);
        while let Some(waker) = reg.pop() {
            waker.close_wake();
        }
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.is_empty(), true);
    }
}
