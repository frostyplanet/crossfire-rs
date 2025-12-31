use super::{Flavor, FlavorImpl, FlavorPrivate};
use crate::crossbeam::array_queue::ArrayQueue;
use crate::waker_registry::*;
use std::mem::MaybeUninit;

pub struct Array<T>(ArrayQueue<T>);

impl<T> Array<T> {
    pub fn new(mut bound: usize) -> Self {
        assert!(bound <= u32::MAX as usize);
        if bound == 0 {
            bound = 1;
        }
        Array(ArrayQueue::<T>::new(bound))
    }
}

impl<T> FlavorImpl<T> for Array<T> {
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
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        return unsafe { self.0.push_with_ptr(item.as_ptr()) };
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        return unsafe { self.0.try_push_oneshot(item) };
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self.0.pop()
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
}

impl<T> FlavorPrivate<T> for Array<T> {
    #[inline]
    fn to_flavor(self) -> Flavor<T> {
        Flavor::Array(self)
    }

    #[inline]
    fn new_reg_sender<const MP: bool>(&self) -> RegistrySender<T> {
        if MP {
            RegistrySender::<T>::new_multi()
        } else {
            RegistrySender::<T>::new_single()
        }
    }

    #[inline]
    fn new_reg_recv<const MC: bool>(&self) -> RegistryRecv {
        if MC {
            RegistryRecv::new_multi()
        } else {
            RegistryRecv::new_single()
        }
    }
}
