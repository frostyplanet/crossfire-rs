use super::{Flavor, FlavorBounded, FlavorMC, FlavorMP};
use crate::crossbeam::array_queue::ArrayQueue;
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

impl<T: Send + 'static + Unpin, const MP: bool, const MC: bool> Flavor for Array<T, MP, MC> {
    type Item = T;

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
    fn try_send(&self, item: &MaybeUninit<Self::Item>) -> bool {
        return unsafe { self.0.push_with_ptr(item.as_ptr()) };
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const Self::Item) -> Option<bool> {
        return unsafe { self.0.try_push_oneshot(item) };
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<Self::Item> {
        self.0.pop(false)
    }

    #[inline]
    fn try_recv_final(&self) -> Option<Self::Item> {
        self.0.pop(true)
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
            if MP {
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

impl<T: Send + Unpin + 'static, const MP: bool, const MC: bool> FlavorBounded for Array<T, MP, MC> {
    #[inline]
    fn new_with_bound(mut size: usize) -> Self {
        if size < 1 {
            size = 1;
        }
        Self::new(size)
    }
}

impl<T> FlavorMP for Array<T, true, false> {}
impl<T> FlavorMP for Array<T, true, true> {}
impl<T> FlavorMC for Array<T, true, true> {}
