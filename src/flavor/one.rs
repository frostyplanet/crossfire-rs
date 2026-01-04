use super::{Flavor, FlavorImpl, FlavorPrivate};
use crate::backoff::*;
use crate::waker_registry::*;
use core::cell::UnsafeCell;
use core::mem::{needs_drop, MaybeUninit};
use core::ptr;
use core::sync::atomic::{
    compiler_fence, AtomicU16, AtomicU32,
    Ordering::{self, Acquire, Relaxed, Release, SeqCst},
};
use crossbeam_utils::CachePadded;

/// A simplify ArrayQueue specialized for size=1
pub struct OneSize<T> {
    pos: CachePadded<AtomicU32>,

    /// The value in this slot.
    slots: [Slot<T>; 2],
}

unsafe impl<T: Send> Sync for OneSize<T> {}
unsafe impl<T: Send> Send for OneSize<T> {}

impl<T> OneSize<T> {
    #[inline]
    pub fn new() -> Self {
        Self { pos: CachePadded::new(AtomicU32::new(0)), slots: [Slot::init(0), Slot::init(1)] }
    }

    #[inline(always)]
    fn unpack(pos: u32) -> (u16, u16) {
        let head = (pos >> 16) as u16;
        let tail = pos as u16;
        (head, tail)
    }

    #[inline(always)]
    fn pack(head: u16, tail: u16) -> u32 {
        ((head as u32) << 16) | (tail as u32)
    }

    /// return Ok(true) on ok, Ok(false) on full, Err(()) to spin
    #[inline(always)]
    unsafe fn _try_push(
        &self, order: Ordering, value: *const T, failure: Ordering,
    ) -> Result<(), ()> {
        let mut pos = self.pos.load(order);
        compiler_fence(Acquire);
        loop {
            let (head, tail) = Self::unpack(pos);
            if head == tail {
                let new_pos = Self::pack(head, tail.wrapping_add(1));
                match self.pos.compare_exchange_weak(pos, new_pos, SeqCst, failure) {
                    Ok(_) => {
                        let index = tail & 0x1;
                        self.slots[index as usize].write(tail, value);
                        return Ok(());
                    }
                    Err(_pos) => {
                        pos = _pos;
                    }
                }
            } else {
                return Err(());
            }
        }
    }

    #[inline(always)]
    fn _pop(&self, order: Ordering) -> Option<T> {
        let mut pos = self.pos.load(order);
        compiler_fence(Acquire);
        loop {
            let (head, tail) = Self::unpack(pos);
            if head == tail {
                return None;
            }
            let next_head = head.wrapping_add(1);
            let new_pos = Self::pack(next_head, tail);
            match self.pos.compare_exchange_weak(pos, new_pos, SeqCst, Acquire) {
                Err(_pos) => {
                    pos = _pos;
                }
                Ok(_) => {
                    let index = head & 0x1;
                    return Some(self.slots[index as usize].read(next_head));
                }
            }
        }
    }
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    stamp: AtomicU16,
}

impl<T> Slot<T> {
    #[inline]
    fn init(i: u16) -> Self {
        Self { value: UnsafeCell::new(MaybeUninit::uninit()), stamp: AtomicU16::new(i) }
    }

    #[inline(always)]
    fn write(&self, tail: u16, value: *const T) {
        let mut stamp = self.stamp.load(Acquire);
        if stamp != tail {
            let mut backoff = Backoff::new(BackoffConfig::default());
            loop {
                backoff.spin();
                stamp = self.stamp.load(Acquire);
                if stamp == tail {
                    break;
                }
            }
        }
        unsafe { (*self.value.get()).write(ptr::read(value)) };
        self.stamp.store(tail.wrapping_add(1), Release);
    }

    #[inline(always)]
    fn read(&self, head: u16) -> T {
        let mut stamp = self.stamp.load(Acquire);
        if stamp != head {
            let mut backoff = Backoff::new(BackoffConfig::default());
            loop {
                backoff.spin();
                stamp = self.stamp.load(Acquire);
                if stamp == head {
                    break;
                }
            }
        }
        let msg = unsafe { self.value.get().read().assume_init() };
        // there might be slow reader, update the stamp to allow writer reuse the slot
        self.stamp.store(head.wrapping_add(1), Release);
        msg
    }

    #[inline(always)]
    fn drop(&self) {
        unsafe { self.value.get().read().assume_init_drop() };
    }
}

impl<T> Drop for OneSize<T> {
    fn drop(&mut self) {
        if needs_drop::<T>() {
            let pos = *self.pos.get_mut();
            let (head, tail) = Self::unpack(pos);
            if head != tail {
                let index = head & 0x1;
                self.slots[index as usize].drop();
            }
        }
    }
}

impl<T> FlavorImpl<T> for OneSize<T> {
    #[inline(always)]
    fn len(&self) -> usize {
        if self.is_full() {
            1
        } else {
            0
        }
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        Some(1)
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        !self.is_empty()
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        let pos = self.pos.load(SeqCst);
        let (head, tail) = Self::unpack(pos);
        head == tail
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        // Will always double-check with is_full or try_send_oneshot()
        unsafe { self._try_push(Relaxed, item.as_ptr(), Relaxed).is_ok() }
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        Some(unsafe { self._try_push(SeqCst, item, SeqCst).is_ok() })
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self._pop(SeqCst)
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        crate::backoff::DEFAULT_LIMIT
    }
}

impl<T> FlavorPrivate<T> for OneSize<T> {
    #[inline]
    fn to_flavor(self) -> Flavor<T> {
        Flavor::One(self)
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
