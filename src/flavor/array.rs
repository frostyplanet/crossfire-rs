use super::{Flavor, FlavorImpl, FlavorPrivate, TryRecvError, TrySendErr};
use crate::crossbeam::array_queue::ArrayQueue;
use crate::waker_registry::*;
use std::mem::MaybeUninit;

pub struct Array<T, const MP: bool, const MC: bool>(ArrayQueue<T, MP, MC>);

impl<T, const MP: bool, const MC: bool> Array<T, MP, MC> {
    pub fn new(mut bound: usize) -> Self {
        assert!(bound <= u32::MAX as usize);
        if bound == 0 {
            bound = 1;
        }
        Array(ArrayQueue::<T, MP, MC>::new(bound))
    }
}

impl<T, const MP: bool, const MC: bool> FlavorImpl<T> for Array<T, MP, MC> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        Some(self.0.capacity())
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        self.0.is_full()
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> Result<(), TrySendErr> {
        return unsafe { self.0.try_send(item.as_ptr()) };
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<Result<(), TrySendErr>> {
        return unsafe { self.0.try_send_oneshot(item) };
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        self.0.pop()
    }

    #[inline]
    fn close(&self) -> bool {
        self.0.disconnect()
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        if self.0.capacity() > 10 {
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
        if self.0.capacity() > 10 {
            true
        } else {
            false
        }
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
