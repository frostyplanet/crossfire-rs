use super::{FlavorImpl, FlavorNew, FlavorSelect, Queue, Token};
use core::cell::UnsafeCell;
use core::mem::{needs_drop, MaybeUninit};
use crossbeam_utils::CachePadded;
use std::ptr;
use std::sync::atomic::{
    compiler_fence, AtomicU64,
    Ordering::{self, Acquire, Relaxed, SeqCst},
};

/// This is a spsc version of `One` without stamp.
///
/// The sender side allow to push and drop it's own previous value, if receivers had not consumed it.
pub type OneSpsc<T> = OneSp<T, false>;

/// This is a spmc version of `One` without stamp, allow replace() on the sender side.
///
/// The sender side allow to push and drop it's own previous value, if receivers had not consumed it.
///
/// NOTE: use lockless technique inspired by the OFLIT paper, miri will probably report data racing issue,
/// but it's intentional.
/// This module cannot not separate pop into start_read/read interface,
/// so it cannot implement Flavor interface.
pub type OneSpmc<T> = OneSp<T, true>;

pub struct OneSp<T, const MC: bool> {
    pos: CachePadded<AtomicU64>,

    /// The value in this slot.
    slots: [Slot<T>; 2],
}

unsafe impl<T: Send, const MC: bool> Sync for OneSp<T, MC> {}
unsafe impl<T: Send, const MC: bool> Send for OneSp<T, MC> {}

impl<T, const MC: bool> OneSp<T, MC> {
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

    #[inline(always)]
    pub fn len(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            1
        }
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
}

impl<T, const MC: bool> Drop for OneSp<T, MC> {
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

impl<T: Send + 'static> OneSpsc<T> {
    #[inline(always)]
    fn _pop(&self, order: Ordering) -> Option<T> {
        if let Some(tail) = self.start_read(order) {
            let index = (tail & 0x1) as usize;
            let item = self.slots[index as usize].read();
            let new_pos = Self::pack(tail, tail);
            self.pos.store(new_pos, SeqCst);
            Some(item)
        } else {
            None
        }
    }

    #[inline(always)]
    fn start_read(&self, order: Ordering) -> Option<u32> {
        let pos = self.pos.load(order);
        compiler_fence(Ordering::SeqCst);
        loop {
            let (head, tail) = Self::unpack(pos);
            if head == tail {
                return None;
            }
            debug_assert_eq!(head.wrapping_add(1), tail);
            return Some(tail);
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
    fn read(&self) -> T {
        unsafe { self.value.get().read().assume_init() }
    }

    #[inline(always)]
    fn drop(&self) {
        unsafe { self.value.get().read().assume_init_drop() };
    }
}

impl<T> OneSpmc<T> {
    #[inline]
    pub fn replace(&self, value: T) {
        let item = MaybeUninit::new(value);
        self._replace(item.as_ptr());
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
                    // Other might read the value, or send might use replace to cancel the value,
                    // should be cas suc to confirm
                    pos = _pos;
                }
                Ok(_) => {
                    return Some(unsafe { value_copy.assume_init_read() });
                }
            }
        }
    }
}

impl<T: Send + Unpin + 'static> Queue for OneSpmc<T> {
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
        !Self::is_empty(self)
    }

    #[inline(always)]
    fn pop(&self) -> Option<T> {
        self._pop(Ordering::SeqCst)
    }

    #[inline]
    fn push(&self, value: T) -> Result<(), T> {
        let item = MaybeUninit::new(value);
        if self.try_push(item.as_ptr(), Ordering::SeqCst) {
            Ok(())
        } else {
            Err(unsafe { item.assume_init_read() })
        }
    }
}

impl<T: Send + Unpin + 'static> Queue for OneSpsc<T> {
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
        !Self::is_empty(self)
    }

    #[inline(always)]
    fn pop(&self) -> Option<T> {
        self._pop(Ordering::SeqCst)
    }

    #[inline]
    fn push(&self, value: T) -> Result<(), T> {
        let item = MaybeUninit::new(value);
        if self.try_push(item.as_ptr(), Ordering::SeqCst) {
            Ok(())
        } else {
            Err(unsafe { item.assume_init_read() })
        }
    }
}

impl<T: Send + Unpin + 'static> FlavorImpl for OneSpsc<T> {
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
        self.pop()
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
        true
    }
}

impl<T> FlavorNew for OneSpsc<T> {
    #[inline]
    fn new() -> Self {
        OneSpsc::new()
    }
}

impl<T: Send + 'static + Unpin> FlavorSelect for OneSpsc<T> {
    #[inline]
    fn try_select(&self, final_check: bool) -> Option<Token> {
        if let Some(tail) =
            self.start_read(if final_check { Ordering::SeqCst } else { Ordering::Acquire })
        {
            let index = (tail & 0x1) as usize;
            let new_pos = Self::pack(tail, tail);

            Some(Token::new(
                &self.slots[index as usize] as *const Slot<T> as *const u8,
                new_pos as usize,
            ))
        } else {
            None
        }
    }

    #[inline(always)]
    fn read_with_token(&self, token: Token) -> T {
        let slot: &Slot<T> = unsafe { &*token.pos.cast::<Slot<T>>() };
        let item = slot.read();
        // NOTE: This is only valid for SPSC
        self.pos.store(token.stamp as u64, SeqCst);
        item
    }
}
