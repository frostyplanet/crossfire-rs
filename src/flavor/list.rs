use super::{Flavor, FlavorImpl};
use crossbeam_queue::SegQueue;
use std::mem::MaybeUninit;

pub struct List<T>(SegQueue<T>);

impl<T> List<T> {
    #[inline(always)]
    pub fn new() -> Flavor<T> {
        Flavor::List(Self(SegQueue::new()))
    }
}

impl<T> FlavorImpl<T> for List<T> {
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

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        self.0.push(unsafe { item.assume_init_read() });
        true
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self.0.pop()
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
