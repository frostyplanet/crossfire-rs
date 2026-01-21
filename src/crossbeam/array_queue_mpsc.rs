//! Modify by frostyplanet@gmail.com for the crossfire crate:
//!
//!   - Optimise for MPSC scenario;
//!   - Pack head/tail for cache efficiency;
//!   - Add token interface according to crossbeam-channel
//!   - Modified push() to push_with_ptr();
//!   - Add try_push_oneshot();
//!
//! Fork from crossbeam-queue crate commit 5a154def002304814d50f3c7658bd30eb46b2fad
//!
//! The MIT License (MIT)
//!
//! Copyright (c) 2025, 2026 frostyplanet@gmail.com
//!
//! Copyright (c) 2019 The Crossbeam Project Developers
//!
//! Permission is hereby granted, free of charge, to any
//! person obtaining a copy of this software and associated
//! documentation files (the "Software"), to deal in the
//! Software without restriction, including without
//! limitation the rights to use, copy, modify, merge,
//! publish, distribute, sublicense, and/or sell copies of
//! the Software, and to permit persons to whom the Software
//! is furnished to do so, subject to the following
//! conditions:
//!
//! The above copyright notice and this permission notice
//! shall be included in all copies or substantial portions
//! of the Software.
//!
//! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
//! ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
//! TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
//! PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
//! SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
//! CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
//! OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
//! IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
//! DEALINGS IN THE SOFTWARE.
//!
//! The implementation is based on Dmitry Vyukov's bounded MPMC queue.
//!
//! Source:
//!   - <http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>

use core::cell::UnsafeCell;

use crate::flavor::Token;
use core::mem::{self, MaybeUninit};
use core::panic::{RefUnwindSafe, UnwindSafe};
use core::ptr;
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crossbeam_utils::{Backoff, CachePadded};

/// A slot in a queue.
struct Slot<T> {
    /// The current stamp.
    stamp: AtomicUsize,

    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded multi-producer single-consumer queue.
pub struct ArrayQueueMpsc<T> {
    /// The sender state.
    ///
    /// High bits: head_cached
    /// Low bits: tail
    sender: CachePadded<AtomicU64>,

    /// The receiver state.
    ///
    /// High bits: tail_cached
    /// Low bits: head
    recv: CachePadded<AtomicU64>,

    /// The buffer holding slots.
    buffer: Box<[Slot<T>]>,

    /// A stamp with the value of `{ lap: 1, index: 0 }`.
    one_lap: u32,
}

unsafe impl<T: Send> Sync for ArrayQueueMpsc<T> {}
unsafe impl<T: Send> Send for ArrayQueueMpsc<T> {}

impl<T> UnwindSafe for ArrayQueueMpsc<T> {}
impl<T> RefUnwindSafe for ArrayQueueMpsc<T> {}

impl<T> ArrayQueueMpsc<T> {
    /// Creates a new bounded queue with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if the capacity is zero.
    pub fn new(cap: usize) -> Self {
        assert!(cap > 0, "capacity must be non-zero");
        assert!(cap < (1 << 31), "capacity too large for u32 logic");

        // Head is initialized to `{ lap: 0, index: 0 }`.
        // Tail is initialized to `{ lap: 0, index: 0 }`.
        let head = 0;
        let tail = 0;

        // Allocate a buffer of `cap` slots initialized
        // with stamps.
        let buffer: Box<[Slot<T>]> = (0..cap)
            .map(|i| {
                // Set the stamp to `i`.
                Slot { stamp: AtomicUsize::new(i), value: UnsafeCell::new(MaybeUninit::uninit()) }
            })
            .collect();

        // One lap is the smallest power of two greater than `cap`.
        let one_lap = (cap + 1).next_power_of_two() as u32;

        Self {
            buffer,
            one_lap,
            recv: CachePadded::new(AtomicU64::new(((tail as u64) << 32) | (head as u64))),
            sender: CachePadded::new(AtomicU64::new(((head as u64) << 32) | (tail as u64))),
        }
    }

    #[inline(always)]
    fn _try_push(
        &self, sender_val: u64, tail: u32, head_cached: u32, value: *const T,
    ) -> Result<bool, u64> {
        let index = (tail & (self.one_lap - 1)) as usize;
        let new_tail = if index + 1 < self.buffer.len() {
            tail + 1
        } else {
            let lap = tail & !(self.one_lap - 1);
            lap.wrapping_add(self.one_lap)
        };
        let new_sender_val = ((head_cached as u64) << 32) | (new_tail as u64);
        match self.sender.compare_exchange_weak(
            sender_val,
            new_sender_val,
            Ordering::SeqCst,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                debug_assert!(index < self.buffer.len());
                unsafe {
                    let slot = self.buffer.get_unchecked(index);

                    let item: &mut MaybeUninit<T> = mem::transmute(slot.value.get());
                    item.write(ptr::read(value));
                    slot.stamp.store((tail as usize).wrapping_add(1), Ordering::Release);
                }
                return Ok(true);
            }
            Err(current) => return Err(current),
        }
    }

    #[inline(always)]
    pub unsafe fn push_with_ptr(&self, value: *const T) -> bool {
        let backoff = Backoff::new();
        let mut sender_val = self.sender.load(Ordering::Relaxed);
        loop {
            let tail = sender_val as u32;
            let mut head_cached = (sender_val >> 32) as u32;

            if head_cached.wrapping_add(self.one_lap) == tail {
                backoff.spin();
                let head = self.recv.load(Ordering::SeqCst) as u32;
                if head == head_cached {
                    return false;
                }
                head_cached = head;
            }
            match self._try_push(sender_val, tail, head_cached, value) {
                Ok(res) => return res,
                Err(current) => {
                    sender_val = current;
                    backoff.snooze();
                }
            }
        }
    }

    /// This function is optimise for channel suspected to be full,
    /// It's an equal replacement to is_full(), if not try only oneshot,
    /// return Ok(true) when push ok, Ok(false) when channel is full.
    /// None when uncertain (normally needs a loop)
    #[inline(always)]
    pub unsafe fn try_push_oneshot(&self, value: *const T) -> Option<bool> {
        let sender_val = self.sender.load(Ordering::SeqCst);
        let tail = sender_val as u32;
        let mut head_cached = (sender_val >> 32) as u32;
        if head_cached.wrapping_add(self.one_lap) == tail {
            let head = self.recv.load(Ordering::SeqCst) as u32;
            if head == head_cached {
                return Some(false);
            }
            head_cached = head;
        }
        match self._try_push(sender_val, tail, head_cached, value) {
            Ok(res) => Some(res),
            Err(_) => None,
        }
    }

    #[inline]
    pub fn start_read(&self, final_check: bool) -> Option<Token> {
        if let Some((head, tail_cached)) = self._start_read::<true>(final_check) {
            let (slot, packed_recv) = self._read(head, tail_cached);
            Some(Token::new(slot as *const Slot<T> as *const u8, packed_recv as usize))
        } else {
            None
        }
    }

    #[inline]
    pub fn pop(&self, final_check: bool) -> Option<T> {
        if let Some((head, tail_cached)) = self._start_read::<true>(final_check) {
            let (slot, packed_recv) = self._read(head, tail_cached);
            let msg = unsafe { slot.value.get().read().assume_init() };
            // Update recv (which contains head) to free the slot.
            self.recv.store(packed_recv, Ordering::SeqCst);
            Some(msg)
        } else {
            None
        }
    }

    #[inline]
    pub fn pop_cached(&self) -> Option<T> {
        if let Some((head, tail_cached)) = self._start_read::<false>(false) {
            let (slot, packed_recv) = self._read(head, tail_cached);
            let msg = unsafe { slot.value.get().read().assume_init() };
            // Update recv (which contains head) to free the slot.
            self.recv.store(packed_recv, Ordering::SeqCst);
            Some(msg)
        } else {
            None
        }
    }

    /// return head, tail_cached
    #[inline]
    fn _start_read<const SPIN: bool>(&self, _final_check: bool) -> Option<(u32, u32)> {
        let recv_val = self.recv.load(Ordering::Relaxed);
        let head = recv_val as u32;
        let mut tail_cached = (recv_val >> 32) as u32;

        if tail_cached == head {
            if SPIN {
                std::hint::spin_loop();
                let tail = if _final_check {
                    self.sender.load(Ordering::SeqCst) as u32
                } else {
                    self.sender.load(Ordering::Acquire) as u32
                };
                if head == tail {
                    return None;
                }
                tail_cached = tail;
            } else {
                return None;
            }
        }
        Some((head, tail_cached))
    }

    #[inline]
    fn _read(&self, head: u32, tail_cached: u32) -> (&Slot<T>, u64) {
        // Deconstruct the head.
        let index = (head & (self.one_lap - 1)) as usize;
        debug_assert!(index < self.buffer.len());
        let slot = unsafe { self.buffer.get_unchecked(index) };
        // Wait for stamp update
        let backoff = Backoff::new();
        let target_stamp = (head as usize).wrapping_add(1);
        loop {
            let stamp = slot.stamp.load(Ordering::Acquire);
            if stamp == target_stamp {
                break;
            }
            backoff.snooze();
        }
        // Update head
        let new_head = if index + 1 < self.buffer.len() {
            head + 1
        } else {
            let lap = head & !(self.one_lap - 1);
            lap.wrapping_add(self.one_lap)
        };
        return (slot, ((tail_cached as u64) << 32) | (new_head as u64));
    }

    #[inline(always)]
    pub fn read(&self, token: Token) -> T {
        let slot: &Slot<T> = unsafe { &*token.pos.cast::<Slot<T>>() };
        let msg = unsafe { slot.value.get().read().assume_init() };
        // Do not update stamp
        self.recv.store(token.stamp as u64, Ordering::SeqCst);
        msg
    }

    /// Returns the capacity of the queue.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the queue is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head = self.recv.load(Ordering::SeqCst) as u32;
        let tail = self.sender.load(Ordering::SeqCst) as u32;
        tail == head
    }

    /// Returns `true` if the queue is full.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let tail = self.sender.load(Ordering::SeqCst) as u32;
        let head = self.recv.load(Ordering::SeqCst) as u32;
        head.wrapping_add(self.one_lap) == tail
    }

    /// Returns the number of elements in the queue.
    #[inline]
    pub fn len(&self) -> usize {
        loop {
            let tail = self.sender.load(Ordering::SeqCst) as u32;
            let head = self.recv.load(Ordering::SeqCst) as u32;

            if self.sender.load(Ordering::SeqCst) as u32 == tail {
                let hix = head & (self.one_lap - 1);
                let tix = tail & (self.one_lap - 1);

                return if hix < tix {
                    (tix - hix) as usize
                } else if hix > tix {
                    self.capacity() - (hix - tix) as usize
                } else if tail == head {
                    0
                } else {
                    self.capacity()
                };
            }
        }
    }
}

impl<T> Drop for ArrayQueueMpsc<T> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            let recv_val = *self.recv.get_mut();
            let sender_val = *self.sender.get_mut();

            let head = recv_val as u32;
            let tail = sender_val as u32;

            let hix = head & (self.one_lap - 1);
            let tix = tail & (self.one_lap - 1);

            let len = if hix < tix {
                tix - hix
            } else if hix > tix {
                self.capacity() as u32 - hix + tix
            } else if tail == head {
                0
            } else {
                self.capacity() as u32
            };

            for i in 0..(len as usize) {
                let index = if (hix as usize) + i < self.capacity() {
                    (hix as usize) + i
                } else {
                    (hix as usize) + i - self.capacity()
                };

                unsafe {
                    debug_assert!(index < self.buffer.len());
                    let slot = self.buffer.get_unchecked_mut(index);
                    (*slot.value.get()).assume_init_drop();
                }
            }
        }
    }
}
