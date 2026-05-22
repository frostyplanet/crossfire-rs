use super::{FlavorBounded, FlavorImpl, FlavorSelect, Queue, Token};
use crate::crossbeam::array_queue::ArrayQueue;
use std::mem::MaybeUninit;

/// Which Equals to crossbeam_queue::ArrayQueue
pub type Array<T> = _Array<T, true, true>;

pub struct _Array<T, const MP: bool, const MC: bool>(ArrayQueue<T, MP, MC>);

impl<T, const MP: bool, const MC: bool> _Array<T, MP, MC> {
    pub fn new(mut bound: usize) -> Self {
        assert!(bound <= u32::MAX as usize);
        if bound == 0 {
            bound = 1;
        }
        Self(ArrayQueue::<T, MP, MC>::new(bound))
    }
}

impl<T, const MP: bool, const MC: bool> Queue for _Array<T, MP, MC> {
    type Item = T;

    #[inline(always)]
    fn pop(&self) -> Option<T> {
        self.0.pop(true)
    }

    #[inline(always)]
    fn push(&self, item: T) -> Result<(), T> {
        let _item = MaybeUninit::new(item);
        if unsafe { self.0.push_with_ptr(_item.as_ptr()) } {
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

impl<T, const MP: bool, const MC: bool> FlavorImpl for _Array<T, MP, MC> {
    const IS_BOUNDED: bool = true;

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        unsafe { self.0.push_with_ptr(item.as_ptr()) }
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        unsafe { self.0.try_push_oneshot(item) }
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

impl<T, const MP: bool, const MC: bool> FlavorSelect for _Array<T, MP, MC> {
    #[inline]
    fn try_select(&self, final_check: bool) -> Option<Token> {
        self.0.start_read(final_check)
    }

    #[inline(always)]
    fn read_with_token(&self, token: Token) -> T {
        self.0.read(token)
    }
}

impl<T, const MP: bool, const MC: bool> FlavorBounded for _Array<T, MP, MC> {
    #[inline(always)]
    fn new_with_bound(size: usize) -> Self {
        Self::new(size)
    }
}
