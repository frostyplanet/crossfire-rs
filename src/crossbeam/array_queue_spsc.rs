//! Modify by frostyplanet@gmail.com for the crossfire crate:
//!
//!   - Modify for SPSC, remove the `stamp` field
//!   - Optimization: pack head/tail and their cached counterparts into single AtomicU64 (u32 each).
//!   - Add token interface according to crossbeam-channel
//!   - Modified push() to push_with_ptr();
//!   - Add try_push_oneshot() which combinds the logic of push and check_full in one step;
//!   - Remove unused functions.
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
use core::sync::atomic::{AtomicU64, Ordering};
use crossbeam_utils::CachePadded;

/// A slot in a queue.
struct Slot<T> {
    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded multi-producer multi-consumer queue.
///
/// This queue allocates a fixed-capacity buffer on construction, which is used to store pushed
/// elements. The queue cannot hold more elements than the buffer allows. Attempting to push an
/// element into a full queue will fail. Alternatively, [`force_push`] makes it possible for
/// this queue to be used as a ring-buffer. Having a buffer allocated upfront makes this queue
/// a bit faster than [`SegQueue`].
///
/// [`SegQueue`]: super::SegQueue
pub struct ArrayQueueSpsc<T> {
    /// The head of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
    /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
    ///
    /// Elements are popped from the head of the queue.
    ///
    /// High bits: head_cached
    /// Low bits: tail
    sender: CachePadded<AtomicU64>,

    /// The tail of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
    /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
    ///
    /// Elements are pushed into the tail of the queue.
    ///
    /// High bits: tail_cached
    /// Low bits: head
    recv: CachePadded<AtomicU64>,

    /// The buffer holding slots.
    buffer: Box<[Slot<T>]>,

    /// A stamp with the value of `{ lap: 1, index: 0 }`.
    one_lap: u32,
}

unsafe impl<T> Sync for ArrayQueueSpsc<T> {}
unsafe impl<T> Send for ArrayQueueSpsc<T> {}

impl<T> UnwindSafe for ArrayQueueSpsc<T> {}
impl<T> RefUnwindSafe for ArrayQueueSpsc<T> {}

impl<T> ArrayQueueSpsc<T> {
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
        let buffer: Box<[Slot<T>]> =
            (0..cap).map(|_i| Slot { value: UnsafeCell::new(MaybeUninit::uninit()) }).collect();

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
    fn _try_push(&self, order: Ordering, value: *const T) -> bool {
        let sender_val = self.sender.load(Ordering::Relaxed);
        let tail = sender_val as u32;
        let mut head_cached = (sender_val >> 32) as u32;

        if head_cached.wrapping_add(self.one_lap) == tail {
            let head = self.recv.load(order) as u32;
            if head == head_cached {
                return false;
            }
            head_cached = head;
        }

        let cap = self.capacity();
        // Deconstruct the tail.
        let index = (tail & (self.one_lap - 1)) as usize;
        // Inspect the corresponding slot.
        debug_assert!(index < self.buffer.len());
        let slot = unsafe { self.buffer.get_unchecked(index) };
        let new_tail = if index + 1 < cap {
            // Same lap, incremented index.
            // Set to `{ lap: lap, index: index + 1 }`.
            tail + 1
        } else {
            let lap = tail & !(self.one_lap - 1);
            // One lap forward, index wraps around to zero.
            // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
            lap.wrapping_add(self.one_lap)
        };
        // Write the value into the slot.
        unsafe {
            let item: &mut MaybeUninit<T> = mem::transmute(slot.value.get());
            item.write(ptr::read(value));
        }
        self.sender.store(((head_cached as u64) << 32) | (new_tail as u64), Ordering::SeqCst);
        return true;
    }

    #[inline(always)]
    pub unsafe fn push_with_ptr(&self, value: *const T) -> bool {
        self._try_push(Ordering::Acquire, value)
    }

    #[inline(always)]
    pub unsafe fn push_with_ptr_final(&self, value: *const T) -> bool {
        self._try_push(Ordering::SeqCst, value)
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
            self.recv.store(packed_recv, Ordering::SeqCst);
            Some(msg)
        } else {
            None
        }
    }

    /// return (head, tail_cached)
    #[inline]
    fn _start_read<const SPIN: bool>(&self, _final_check: bool) -> Option<(u32, u32)> {
        let recv_val = self.recv.load(Ordering::Relaxed);
        let head = recv_val as u32;
        let mut tail_cached = (recv_val >> 32) as u32;

        if tail_cached == head {
            if SPIN {
                // because we don't have stamp, and no spining loop,
                // this line is critical for performance
                std::hint::spin_loop();
                let tail = {
                    if _final_check {
                        // because we need to check is_empty before park,
                        // use SeqCst to make Miri happy
                        self.sender.load(Ordering::SeqCst) as u32
                    } else {
                        self.sender.load(Ordering::Acquire) as u32
                    }
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
        // Inspect the corresponding slot.
        debug_assert!(index < self.buffer.len());
        let slot = unsafe { self.buffer.get_unchecked(index) };
        // If the stamp is ahead of the head by 1, we may attempt to pop.
        let new_head = if index + 1 < self.capacity() {
            // Same lap, incremented index.
            // Set to `{ lap: lap, index: index + 1 }`.
            head + 1
        } else {
            let lap = head & !(self.one_lap - 1);
            // One lap forward, index wraps around to zero.
            // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
            lap.wrapping_add(self.one_lap)
        };
        return (slot, ((tail_cached as u64) << 32) | (new_head as u64));
    }

    #[inline(always)]
    pub fn read(&self, token: Token) -> T {
        let slot: &Slot<T> = unsafe { &*token.pos.cast::<Slot<T>>() };
        let msg = unsafe { slot.value.get().read().assume_init() };
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

        // Is the tail lagging one lap behind head?
        // Is the tail equal to the head?
        //
        // Note: If the head changes just before we load the tail, that means there was a moment
        // when the channel was not empty, so it is safe to just return `false`.
        tail == head
    }

    /// Returns `true` if the queue is full.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let tail = self.sender.load(Ordering::SeqCst) as u32;
        let head = self.recv.load(Ordering::SeqCst) as u32;

        // Is the head lagging one lap behind tail?
        //
        // Note: If the tail changes just before we load the head, that means there was a moment
        // when the queue was not full, so it is safe to just return `false`.
        head.wrapping_add(self.one_lap) == tail
    }

    /// Returns the number of elements in the queue.
    #[inline]
    pub fn len(&self) -> usize {
        loop {
            // Load the tail, then load the head.
            let tail = self.sender.load(Ordering::SeqCst) as u32;
            let head = self.recv.load(Ordering::SeqCst) as u32;

            // If the tail didn't change, we've got consistent values to work with.
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

impl<T> Drop for ArrayQueueSpsc<T> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            // Get the index of the head.
            let head = (*self.recv.get_mut()) as u32;
            let tail = (*self.sender.get_mut()) as u32;

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

            // Loop over all slots that hold a message and drop them.
            for i in 0..(len as usize) {
                // Compute the index of the next slot holding a message.
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
