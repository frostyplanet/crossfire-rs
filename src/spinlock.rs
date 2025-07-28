use crate::backoff::*;
use std::cell::UnsafeCell;
use std::mem::transmute;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Spinlock<T> {
    inner: UnsafeCell<T>,
    lock: AtomicBool,
}

pub struct SpinlockGuard<'a, T> {
    inner: &'a mut T,
    lock: &'a AtomicBool,
}

impl<'a, T> Deref for SpinlockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T> DerefMut for SpinlockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'a, T> AsRef<T> for SpinlockGuard<'a, T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<'a, T> Drop for SpinlockGuard<'a, T> {
    fn drop(&mut self) {
        // NOTE: use SeqCst to prevent atomic ordering (store) happens inside the guard scope.
        // (issue #24)
        self.lock.store(false, Ordering::SeqCst);
    }
}

impl<T> Spinlock<T> {
    #[inline(always)]
    pub fn new(inner: T) -> Self {
        Self { inner: UnsafeCell::new(inner), lock: AtomicBool::new(false) }
    }

    #[inline(always)]
    pub fn lock<'a>(&'a self, cfg: BackoffConfig) -> SpinlockGuard<'a, T> {
        if self
            .lock
            .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            let mut backoff = Backoff::new(cfg);
            while self
                .lock
                .compare_exchange_weak(false, true, Ordering::SeqCst, Ordering::Relaxed)
                .is_err()
            {
                backoff.snooze();
            }
        }
        let inner: &mut T = unsafe { transmute(self.inner.get()) };
        SpinlockGuard { inner, lock: &self.lock }
    }
}
