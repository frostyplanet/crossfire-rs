use super::{Flavor, FlavorImpl, FlavorPrivate};
use crate::crossbeam::array_queue::ArrayQueue;
use crate::waker_registry::*;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicIsize, Ordering};

pub struct Array<T, const MP: bool, const MC: bool> {
    inner: ArrayQueue<T, MP, MC>,
    congest: AtomicIsize,
}

impl<T, const MP: bool, const MC: bool> Array<T, MP, MC> {
    pub fn new(mut bound: usize) -> Self {
        assert!(bound <= u32::MAX as usize);
        if bound == 0 {
            bound = 1;
        }
        Self { inner: ArrayQueue::<T, MP, MC>::new(bound), congest: AtomicIsize::new(0) }
    }
}

impl<T, const MP: bool, const MC: bool> FlavorImpl<T> for Array<T, MP, MC> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        Some(self.inner.capacity())
    }

    #[inline(always)]
    fn is_large(&self) -> bool {
        self.inner.capacity() > 10
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        return unsafe { self.inner.push_with_ptr(item.as_ptr()) };
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        return unsafe { self.inner.try_push_oneshot(item) };
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self.inner.pop()
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        if self.inner.capacity() > 10 {
            crate::backoff::DEFAULT_LIMIT
        } else {
            #[cfg(target_arch = "x86_64")]
            {
                crate::backoff::DEFAULT_LIMIT
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                crate::backoff::MAX_LIMIT
            }
        }
    }

    #[inline]
    fn may_direct_copy(&self) -> bool {
        if self.inner.capacity() > 10 {
            if MP {
                return self.congest.load(Ordering::Relaxed) > 0;
            } else {
                false
            }
        } else {
            false
        }
    }

    #[inline(always)]
    fn add_tx(&self) {
        self.congest.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    fn add_rx(&self) {
        self.congest.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline(always)]
    fn close_tx(&self) {
        self.congest.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline(always)]
    fn close_rx(&self) {
        self.congest.fetch_add(1, Ordering::Relaxed);
    }
}

impl<T> FlavorPrivate<T> for Array<T, true, true> {
    #[inline]
    fn to_flavor(self) -> Flavor<T> {
        Flavor::ArrayMPMC(self)
    }

    #[inline]
    fn new_reg_sender<const _MP: bool>(&self) -> RegistrySender<T> {
        debug_assert_eq!(_MP, true);
        RegistrySender::<T>::new_multi()
    }

    #[inline]
    fn new_reg_recv<const _MC: bool>(&self) -> RegistryRecv {
        if _MC {
            RegistryRecv::new_multi()
        } else {
            RegistryRecv::new_single()
        }
    }
}

impl<T> FlavorPrivate<T> for Array<T, false, false> {
    #[inline]
    fn to_flavor(self) -> Flavor<T> {
        Flavor::ArraySPSC(self)
    }

    #[inline]
    fn new_reg_sender<const _MP: bool>(&self) -> RegistrySender<T> {
        debug_assert_eq!(_MP, false);
        RegistrySender::<T>::new_single()
    }

    #[inline]
    fn new_reg_recv<const _MC: bool>(&self) -> RegistryRecv {
        debug_assert_eq!(_MC, false);
        RegistryRecv::new_single()
    }
}
