use super::FlavorImpl;
use core::cell::UnsafeCell;
use core::mem::{needs_drop, MaybeUninit};
use crossbeam_utils::CachePadded;
use std::ptr;
use std::sync::atomic::{
    compiler_fence, AtomicU64,
    Ordering::{self, Acquire, Relaxed, SeqCst},
};

/// This is a spmc without stamp, use lockless technique simular to OFLIT.
///
/// The sender side allow to push and drop it's own previous value, if receivers had not consumed it.
pub struct OneSpmc<T> {
    pos: CachePadded<AtomicU64>,

    /// The value in this slot.
    slots: [Slot<T>; 2],
}

unsafe impl<T: Send> Sync for OneSpmc<T> {}
unsafe impl<T: Send> Send for OneSpmc<T> {}

impl<T> OneSpmc<T> {
    #[inline]
    pub fn new() -> Self {
        Self { pos: CachePadded::new(AtomicU64::new(0)), slots: [Slot::init(), Slot::init()] }
    }

    #[inline(always)]
    fn unpack(pos: u64) -> (u32, u32) {
        let head = (pos >> 32) as u32;
        let tail = pos as u32;
        (head, tail)
    }

    #[inline(always)]
    fn pack(head: u32, tail: u32) -> u64 {
        ((head as u64) << 32) | (tail as u64)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let pos = self.pos.load(SeqCst);
        let (head, tail) = Self::unpack(pos);
        head == tail
    }

    #[inline]
    pub fn replace(&self, value: T) {
        let item = MaybeUninit::new(value);
        self._replace(item.as_ptr());
    }

    #[inline]
    fn try_push(&self, value: *const T, order: Ordering) -> bool {
        let pos = self.pos.load(order);
        let (head, tail) = Self::unpack(pos);
        if head == tail {
            let new_tail = tail.wrapping_add(1);
            let index = new_tail & 0x1;
            self.slots[index as usize].write(value);
            let new_pos = Self::pack(head, new_tail);
            self.pos.store(new_pos, Ordering::SeqCst);
            return true;
        } else {
            return false;
        }
    }

    /// return Ok(true) on ok, Ok(false) on full, Err(()) to spin
    #[inline(always)]
    fn _replace(&self, value: *const T) {
        // No one will advance tail except me
        let mut pos = self.pos.load(Relaxed);
        compiler_fence(Ordering::SeqCst);
        let (mut head, tail) = Self::unpack(pos);
        let new_tail = tail.wrapping_add(1);
        let index = new_tail & 0x1;
        self.slots[index as usize].write(value);
        loop {
            if head == tail {
                let new_pos = Self::pack(head, new_tail);
                self.pos.store(new_pos, Ordering::SeqCst);
                return;
            } else {
                debug_assert_eq!(head.wrapping_add(1), tail);
                let new_pos = Self::pack(head.wrapping_add(1), new_tail);
                match self.pos.compare_exchange_weak(pos, new_pos, SeqCst, Acquire) {
                    Ok(_) => {
                        let index = tail & 0x1;
                        self.slots[index as usize].drop();
                        return;
                    }
                    Err(_pos) => {
                        if pos != _pos {
                            pos = _pos;
                            let _tail;
                            (head, _tail) = Self::unpack(_pos);
                            debug_assert_eq!(_tail, tail);
                        }
                        continue;
                    }
                }
            }
        }
    }

    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        self._pop(Ordering::SeqCst)
    }

    #[inline(always)]
    fn _pop(&self, order: Ordering) -> Option<T> {
        let mut pos = self.pos.load(order);
        compiler_fence(Ordering::SeqCst);
        let mut value_copy: MaybeUninit<T> = MaybeUninit::uninit();
        loop {
            let (head, tail) = Self::unpack(pos);
            if head == tail {
                return None;
            }
            let index = tail & 0x1;
            self.slots[index as usize].read_into(value_copy.as_mut_ptr());
            debug_assert_eq!(head.wrapping_add(1), tail);
            let new_pos = Self::pack(tail, tail);
            match self.pos.compare_exchange_weak(pos, new_pos, SeqCst, order) {
                Err(_pos) => {
                    pos = _pos;
                }
                Ok(_) => {
                    return Some(unsafe { value_copy.assume_init_read() });
                }
            }
        }
    }
}

impl<T> Drop for OneSpmc<T> {
    fn drop(&mut self) {
        if needs_drop::<T>() {
            let pos = *self.pos.get_mut();
            let (head, tail) = Self::unpack(pos);
            if head != tail {
                let index = tail & 0x1;
                self.slots[index as usize].drop();
            }
        }
    }
}

struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    #[inline]
    fn init() -> Self {
        Self { value: UnsafeCell::new(MaybeUninit::uninit()) }
    }

    #[inline(always)]
    fn write(&self, value: *const T) {
        unsafe { (*self.value.get()).write(ptr::read(value)) };
    }

    #[inline(always)]
    fn read_into(&self, dest: *mut T) {
        unsafe {
            let src_ptr = (*self.value.get()).as_ptr();
            ptr::copy_nonoverlapping(src_ptr, dest, 1);
        }
    }

    #[inline(always)]
    fn drop(&self) {
        unsafe { self.value.get().read().assume_init_drop() };
    }
}

impl<T: Send + Unpin + 'static> FlavorImpl for OneSpmc<T> {
    type Item = T;

    #[inline(always)]
    fn len(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            1
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        Some(1)
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        let pos = self.pos.load(SeqCst);
        let (head, tail) = Self::unpack(pos);
        head != tail
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        self.try_push(item.as_ptr(), Acquire)
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        Some(self.try_push(item, SeqCst))
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self._pop(Ordering::Acquire)
    }

    #[inline]
    fn try_recv_final(&self) -> Option<T> {
        self._pop(Ordering::SeqCst)
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        #[cfg(target_arch = "x86_64")]
        {
            crate::backoff::DEFAULT_LIMIT
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            crate::backoff::MAX_LIMIT
        }
    }

    #[inline]
    fn may_direct_copy(&self) -> bool {
        false
    }
}
