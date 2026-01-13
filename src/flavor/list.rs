use super::{FlavorImpl, FlavorNew, FlavorSelect, Queue, Token};
use crate::crossbeam::seg_queue::SegQueue;
use std::mem::MaybeUninit;

/// Which equals to crossbeam_queue::SeqQueue
pub struct List<T>(SegQueue<T>);

impl<T> List<T> {
    #[inline(always)]
    pub fn new() -> Self {
        Self(SegQueue::<T>::new())
    }
}

impl<T: Send + Unpin + 'static> Queue for List<T> {
    type Item = T;

    #[inline(always)]
    fn pop(&self) -> Option<T> {
        self.0.pop()
    }

    #[inline(always)]
    fn push(&self, item: T) -> Result<(), T> {
        self.0.push(item);
        Ok(())
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        None
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        false
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T: Send + Unpin + 'static> FlavorImpl for List<T> {
    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        self.0.push(unsafe { item.assume_init_read() });
        true
    }

    #[inline]
    fn try_recv(&self) -> Option<T> {
        self.0.pop()
    }

    #[inline]
    fn try_recv_final(&self) -> Option<T> {
        if !self.0.is_empty() {
            // TODO review atomic ordering in SegQueue
            self.0.pop()
        } else {
            None
        }
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        crate::backoff::DEFAULT_LIMIT
    }

    #[inline]
    fn may_direct_copy(&self) -> bool {
        false
    }
}

impl<T> FlavorNew for List<T> {
    #[inline]
    fn new() -> Self {
        List::new()
    }
}

impl<T: Send + Unpin + 'static> FlavorSelect for List<T> {
    #[inline]
    fn try_select(&self, final_check: bool) -> Option<Token> {
        if final_check {
            if self.0.is_empty() {
                return None;
            }
        }
        self.0.start_read()
    }

    #[inline(always)]
    fn read_with_token(&self, token: Token) -> T {
        self.0.read(token)
    }
}
