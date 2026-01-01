use super::{Flavor, FlavorImpl, FlavorPrivate, TryRecvError, TrySendErr};
use crate::backoff::*;
use crate::waker_registry::*;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use crossbeam_utils::CachePadded;
use std::ptr;
use std::sync::atomic::{
    AtomicU32, AtomicU8,
    Ordering::{self, Acquire, Relaxed, Release, SeqCst},
};

const CLOSED_BIT: u32 = 0x1;

/// A simplify ArrayQueue specialized for size=1
pub struct OneSize<T> {
    /// bit 0 , IS_CLOSED marker
    /// bit 1-9, tail,
    /// bit 16 ~ 16+7, head
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
    fn unpack(pos: u32) -> (u8, u8) {
        let head = (pos >> 16) as u8;
        let tail = (pos >> 1) as u8;
        (head, tail)
    }

    #[inline(always)]
    fn pack(head: u8, tail: u8) -> u32 {
        ((head as u32) << 16) | ((tail as u32) << 1)
    }

    /// return Ok(true) on ok, Ok(false) on full, Err(()) to spin
    #[inline(always)]
    unsafe fn try_push(
        &self, mut pos: u32, value: *const T, failure: Ordering,
    ) -> Result<(), TrySendErr> {
        loop {
            if pos & CLOSED_BIT > 0 {
                return Err(TrySendErr::Disconnected);
            }
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
                return Err(TrySendErr::Full);
            }
        }
    }
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    stamp: AtomicU8,
}

impl<T> Slot<T> {
    #[inline]
    fn init(i: u8) -> Self {
        Self { value: UnsafeCell::new(MaybeUninit::uninit()), stamp: AtomicU8::new(i) }
    }

    #[inline(always)]
    fn write(&self, tail: u8, value: *const T) {
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
    fn read(&self, head: u8) -> T {
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
        let pos = self.pos.load(SeqCst);
        let (head, tail) = Self::unpack(pos);
        head < tail
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        let pos = self.pos.load(SeqCst);
        let (head, tail) = Self::unpack(pos);
        head == tail
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> Result<(), TrySendErr> {
        // Will always double-check with is_full or try_send_oneshot()
        let pos = self.pos.load(Relaxed);
        unsafe { self.try_push(pos, item.as_ptr(), Relaxed) }
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<Result<(), TrySendErr>> {
        let pos = self.pos.load(SeqCst);
        Some(unsafe { self.try_push(pos, item, SeqCst) })
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut pos = self.pos.load(Acquire);
        loop {
            let (head, tail) = Self::unpack(pos);
            if head == tail {
                if pos & CLOSED_BIT > 0 {
                    return Err(TryRecvError::Disconnected);
                }
                return Err(TryRecvError::Empty);
            }
            debug_assert!(head < tail, "head {} tail {}", head, tail);
            let next_head = head.wrapping_add(1);
            let mut new_pos = Self::pack(next_head, tail);
            if pos & CLOSED_BIT > 0 {
                new_pos |= CLOSED_BIT;
            }
            match self.pos.compare_exchange_weak(pos, new_pos, SeqCst, Acquire) {
                Err(_pos) => {
                    pos = _pos;
                }
                Ok(_) => {
                    let index = head & 0x1;
                    return Ok(self.slots[index as usize].read(next_head));
                }
            }
        }
    }

    #[inline]
    fn close(&self) -> bool {
        let old = self.pos.fetch_or(CLOSED_BIT, Ordering::SeqCst);
        old & CLOSED_BIT == 0
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

impl<T> Drop for OneSize<T> {
    fn drop(&mut self) {
        let _ = self.try_recv();
    }
}
