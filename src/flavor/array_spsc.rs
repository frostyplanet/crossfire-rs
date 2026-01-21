use super::{FlavorBounded, FlavorImpl, FlavorSelect, Queue, Token};
use crate::crossbeam::array_queue_spsc::ArrayQueueSpsc;
use std::mem::MaybeUninit;

/// Ultra light-weight bounded SPSC
///
/// which derives from ArrayQueue, but without stamp.
/// With only two atomics for cache affinity, the fastpath only require two ops to one atomic.
///
pub struct ArraySpsc<T>(ArrayQueueSpsc<T>);

impl<T> ArraySpsc<T> {
    pub fn new(mut bound: usize) -> Self {
        assert!(bound <= u32::MAX as usize);
        if bound == 0 {
            bound = 1;
        }
        Self(ArrayQueueSpsc::<T>::new(bound))
    }
}

impl<T: Send + 'static + Unpin> Queue for ArraySpsc<T> {
    type Item = T;

    #[inline(always)]
    fn pop(&self) -> Option<T> {
        self.0.pop(true)
    }

    #[inline(always)]
    fn push(&self, item: T) -> Result<(), T> {
        let _item = MaybeUninit::new(item);
        if unsafe { self.0.push_with_ptr_final(_item.as_ptr()) } {
            Ok(())
        } else {
            Err(unsafe { _item.assume_init_read() })
        }
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
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        Some(self.0.capacity())
    }
}

impl<T: Send + 'static + Unpin> FlavorImpl for ArraySpsc<T> {
    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        return unsafe { self.0.push_with_ptr(item.as_ptr()) };
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        return Some(unsafe { self.0.push_with_ptr_final(item) });
    }

    #[inline]
    fn try_recv_cached(&self) -> Option<T> {
        self.0.pop_cached()
    }

    #[inline]
    fn try_recv(&self) -> Option<T> {
        self.0.pop(false)
    }

    #[inline]
    fn try_recv_final(&self) -> Option<T> {
        self.0.pop(true)
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        crate::backoff::MAX_LIMIT
    }

    #[inline]
    fn may_direct_copy(&self) -> bool {
        // NOTE:
        // The spsc is not safe for direct copy,
        // because it has no cas, consumer cannot touch the producers pointer
        false
    }
}

impl<T: Send + 'static + Unpin> FlavorSelect for ArraySpsc<T> {
    #[inline]
    fn try_select(&self, final_check: bool) -> Option<Token> {
        self.0.start_read(final_check)
    }

    #[inline(always)]
    fn read_with_token(&self, token: Token) -> T {
        self.0.read(token)
    }
}

impl<T: Send + 'static + Unpin> FlavorBounded for ArraySpsc<T> {
    #[inline(always)]
    fn new_with_bound(size: usize) -> Self {
        Self::new(size)
    }
}
