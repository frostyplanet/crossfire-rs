use super::{FlavorImpl, FlavorNew, FlavorSelect, Queue, Token};
use crate::backoff::*;
use core::cell::UnsafeCell;
use core::mem::{needs_drop, MaybeUninit};
use core::ptr;
use core::sync::atomic::{
    AtomicU16, AtomicU32,
    Ordering::{self, Acquire, Release, SeqCst},
};
use crossbeam_utils::CachePadded;

/// A simplify ArrayQueue specialized for size=1
pub struct OneMpsc<T> {
    pos: CachePadded<AtomicU32>,

    /// The value in this slot.
    slots: [Slot<T>; 2],
}

unsafe impl<T> Sync for OneMpsc<T> {}
unsafe impl<T> Send for OneMpsc<T> {}

impl<T> Queue for OneMpsc<T> {
    type Item = T;

    #[inline(always)]
    fn pop(&self) -> Option<T> {
        self._pop(Ordering::SeqCst)
    }

    #[inline(always)]
    fn push(&self, item: T) -> Result<(), T> {
        let _item = MaybeUninit::new(item);
        if unsafe { self._try_push(SeqCst, _item.as_ptr(), Acquire).is_ok() } {
            Ok(())
        } else {
            Err(unsafe { _item.assume_init_read() })
        }
    }

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
}

impl<T> OneMpsc<T> {
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
    fn _start_read(&self, order: Ordering) -> Option<(u16, u16)> {
        let pos = self.pos.load(order);
        let (head, tail) = Self::unpack(pos);
        if head == tail {
            return None;
        }
        let index = head & 0x1;
        Some((index, tail))
    }

    #[inline(always)]
    fn _read(&self, slot: &Slot<T>, next_head: u16) -> T {
        let new_pos = Self::pack(next_head, next_head);
        // Because we have two slot, the sender will write to next index,
        // it's safe to update the pos before we read, so that sender may begin to write
        self.pos.store(new_pos, SeqCst);

        slot.read(next_head)
    }

    #[inline(always)]
    fn _pop(&self, order: Ordering) -> Option<T> {
        if let Some((index, new_head)) = self._start_read(order) {
            Some(self._read(&self.slots[index as usize], new_head))
        } else {
            None
        }
    }
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    stamp: CachePadded<AtomicU16>,
}

impl<T> Slot<T> {
    #[inline]
    fn init(i: u16) -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            stamp: CachePadded::new(AtomicU16::new(i)),
        }
    }

    #[inline(always)]
    fn write(&self, tail: u16, value: *const T) {
        unsafe { (*self.value.get()).write(ptr::read(value)) };
        self.stamp.store(tail.wrapping_add(1), Release);
    }

    #[inline(always)]
    fn read(&self, head: u16) -> T {
        let mut stamp = self.stamp.load(Acquire);
        if stamp != head {
            let mut backoff = Backoff::new();
            loop {
                backoff.snooze();
                stamp = self.stamp.load(Acquire);
                if stamp == head {
                    break;
                }
            }
        }

        unsafe { self.value.get().read().assume_init() }
    }

    #[inline(always)]
    fn drop(&self) {
        unsafe { self.value.get().read().assume_init_drop() };
    }
}

impl<T> Drop for OneMpsc<T> {
    #[inline(always)]
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

impl<T> FlavorImpl for OneMpsc<T> {
    const IS_BOUNDED: bool = true;

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        // Will always double-check with is_full or try_send_oneshot()
        unsafe { self._try_push(Acquire, item.as_ptr(), Acquire).is_ok() }
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        Some(unsafe { self._try_push(SeqCst, item, Acquire).is_ok() })
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self._pop(Acquire)
    }

    #[inline(always)]
    fn try_recv_final(&self) -> Option<T> {
        self._pop(SeqCst)
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        // Due to bound is too small,
        // yield with MAX_LIMIT to prevent collapse in high contention
        crate::backoff::MAX_LIMIT
    }
}

impl<T> FlavorNew for OneMpsc<T> {
    #[inline]
    fn new() -> Self {
        OneMpsc::new()
    }
}

impl<T> FlavorSelect for OneMpsc<T> {
    #[inline]
    fn try_select(&self, final_check: bool) -> Option<Token> {
        if let Some((index, head)) =
            self._start_read(if final_check { Ordering::SeqCst } else { Ordering::Acquire })
        {
            Some(Token::new(
                &self.slots[index as usize] as *const Slot<T> as *const u8,
                head as usize,
            ))
        } else {
            None
        }
    }

    #[inline(always)]
    fn read_with_token(&self, token: Token) -> T {
        let slot: &Slot<T> = unsafe { &*token.pos.cast::<Slot<T>>() };
        self._read(slot, token.stamp as u16)
    }
}
