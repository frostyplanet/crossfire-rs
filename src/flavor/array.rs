use super::{Flavor, FlavorImpl};
use crate::crossbeam::array_queue::ArrayQueue;
use std::mem::MaybeUninit;

pub struct Array<T>(ArrayQueue<T>);

impl<T> Array<T> {
    pub fn new(bound: usize) -> Flavor<T> {
        assert!(bound <= u32::MAX as usize);
        assert!(bound > 0);
        Flavor::Array(Self(ArrayQueue::new(bound)))
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

    #[inline]
    fn may_direct_copy(&self) -> bool {
        if self.0.capacity() > 10 {
            true
        } else {
            false
        }
    }
}
