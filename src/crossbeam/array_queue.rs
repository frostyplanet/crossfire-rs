//! Modify by frostyplanet@gmail.com for the crossfire crate:
//!
//!   - Optimise for single consumer scenario;
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

use core::mem::{self, MaybeUninit};
use core::panic::{RefUnwindSafe, UnwindSafe};
use core::ptr;
use core::sync::atomic::{self, AtomicUsize, Ordering};

use crossbeam_utils::{Backoff, CachePadded};

/// A slot in a queue.
struct Slot<T> {
    /// The current stamp.
    ///
    /// If the stamp equals the tail, this node will be next written to. If it equals head + 1,
    /// this node will be next read from.
    stamp: AtomicUsize,

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
/// [`force_push`]: ArrayQueue::force_push
/// [`SegQueue`]: super::SegQueue
///
/// # Examples
///
/// ```
/// use crossbeam_queue::ArrayQueue;
///
/// let q = ArrayQueue::new(2);
///
/// assert_eq!(q.push('a'), Ok(()));
/// assert_eq!(q.push('b'), Ok(()));
/// assert_eq!(q.push('c'), Err('c'));
/// assert_eq!(q.pop(), Some('a'));
/// ```
pub struct ArrayQueue<T, const MP: bool, const MC: bool> {
    /// The head of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
    /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
    ///
    /// Elements are popped from the head of the queue.
    head: CachePadded<AtomicUsize>,

    /// The tail of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but packed into a
    /// single `usize`. The lower bits represent the index, while the upper bits represent the lap.
    ///
    /// Elements are pushed into the tail of the queue.
    tail: CachePadded<AtomicUsize>,

    /// The buffer holding slots.
    buffer: Box<[Slot<T>]>,

    /// A stamp with the value of `{ lap: 1, index: 0 }`.
    one_lap: usize,
}

unsafe impl<T: Send, const MP: bool, const MC: bool> Sync for ArrayQueue<T, MP, MC> {}
unsafe impl<T: Send, const MP: bool, const MC: bool> Send for ArrayQueue<T, MP, MC> {}

impl<T, const MP: bool, const MC: bool> UnwindSafe for ArrayQueue<T, MP, MC> {}
impl<T, const MP: bool, const MC: bool> RefUnwindSafe for ArrayQueue<T, MP, MC> {}

impl<T, const MP: bool, const MC: bool> ArrayQueue<T, MP, MC> {
    /// Creates a new bounded queue with the given capacity.
    ///
    /// # Panics
    ///
    /// Panics if the capacity is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_queue::ArrayQueue;
    ///
    /// let q = ArrayQueue::<i32>::new(100);
    /// ```
    pub fn new(cap: usize) -> Self {
        assert!(cap > 0, "capacity must be non-zero");

        // Head is initialized to `{ lap: 0, index: 0 }`.
        // Tail is initialized to `{ lap: 0, index: 0 }`.
        let head = 0;
        let tail = 0;

        // Allocate a buffer of `cap` slots initialized
        // with stamps.
        let buffer: Box<[Slot<T>]> = (0..cap)
            .map(|i| {
                // Set the stamp to `{ lap: 0, index: i }`.
                Slot { stamp: AtomicUsize::new(i), value: UnsafeCell::new(MaybeUninit::uninit()) }
            })
            .collect();

        // One lap is the smallest power of two greater than `cap`.
        let one_lap = (cap + 1).next_power_of_two();

        Self {
            buffer,
            one_lap,
            head: CachePadded::new(AtomicUsize::new(head)),
            tail: CachePadded::new(AtomicUsize::new(tail)),
        }
    }

    /// This function is optimise for channel suspected to be full,
    /// It's an equal replacement to is_full(), if not try only oneshot,
    /// return Ok(true) when push ok, Ok(false) when channel is full.
    /// None when uncertain (normally needs a loop)
    #[allow(dead_code)]
    #[inline(always)]
    pub unsafe fn try_push_oneshot(&self, value: *const T) -> Option<bool> {
        // Use two SeqCst to compare tail & head, it's an equal replacement to is_full()
        let tail = self.tail.load(Ordering::SeqCst);
        macro_rules! check_full {
            ($tail: expr) => {
                let head = self.head.load(Ordering::SeqCst);
                // If the head lags one lap behind the tail as well...
                if head.wrapping_add(self.one_lap) == $tail {
                    // ...then the queue is full.
                    return Some(false);
                }
            };
        }
        check_full!(tail);
        match self._try_push(tail, value) {
            Ok(_) => return Some(true),
            Err((_stamp, _new_tail)) => {
                // after the first check_full with both loads are SeqCst, this is unlikely full, but also a hot path
                return None;
            }
        }
    }

    /// return stamp, new_tail
    #[inline]
    fn _try_push(&self, tail: usize, value: *const T) -> Result<bool, (usize, Option<usize>)> {
        let cap = self.capacity();
        // Deconstruct the tail.
        let index = tail & (self.one_lap - 1);
        let lap = tail & !(self.one_lap - 1);

        // Inspect the corresponding slot.
        debug_assert!(index < self.buffer.len());
        let slot = unsafe { self.buffer.get_unchecked(index) };
        let stamp = slot.stamp.load(Ordering::Acquire);

        // If the tail and the stamp match, we may attempt to push.
        if tail == stamp {
            let new_tail = if index + 1 < cap {
                // Same lap, incremented index.
                // Set to `{ lap: lap, index: index + 1 }`.
                tail + 1
            } else {
                // One lap forward, index wraps around to zero.
                // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
                lap.wrapping_add(self.one_lap)
            };
            if MP {
                // Try moving the tail.
                if let Err(t) = self.tail.compare_exchange_weak(
                    tail,
                    new_tail,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    return Err((stamp, Some(t)));
                }
            } else {
                self.tail.store(new_tail, Ordering::SeqCst);
            }
            // Write the value into the slot and update the stamp.
            unsafe {
                let item: &mut MaybeUninit<T> = mem::transmute(slot.value.get());
                item.write(ptr::read(value));
            }
            slot.stamp.store(tail + 1, Ordering::Release);
            return Ok(true);
        } else {
            return Err((stamp, None));
        }
    }

    #[inline(always)]
    pub unsafe fn push_with_ptr(&self, value: *const T) -> bool {
        let backoff = Backoff::new();
        let mut tail =
            if MP { self.tail.load(Ordering::Relaxed) } else { self.tail.load(Ordering::Acquire) };
        macro_rules! check_full {
            ($tail: expr) => {
                let head = if MP || MC {
                    // NOTE: The fence is preventing livestock
                    atomic::fence(Ordering::SeqCst);
                    self.head.load(Ordering::Relaxed)
                } else {
                    self.head.load(Ordering::SeqCst)
                };
                // If the head lags one lap behind the tail as well...
                if head.wrapping_add(self.one_lap) == $tail {
                    // ...then the queue is full.
                    return false;
                }
            };
        }
        loop {
            match self._try_push(tail, value) {
                Ok(res) => return res,
                Err((stamp, new_tail)) => {
                    if let Some(_tail) = new_tail {
                        tail = _tail;
                        backoff.spin();
                        continue;
                    }
                    if stamp.wrapping_add(self.one_lap) == tail + 1 {
                        check_full!(tail);
                    }
                    backoff.snooze();
                    if MP {
                        tail = self.tail.load(Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Attempts to pop an element from the queue.
    ///
    /// If the queue is empty, `None` is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_queue::ArrayQueue;
    ///
    /// let q = ArrayQueue::new(1);
    /// assert_eq!(q.push(10), Ok(()));
    ///
    /// assert_eq!(q.pop(), Some(10));
    /// assert!(q.pop().is_none());
    /// ```
    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        let backoff = Backoff::new();
        let mut head =
            if MC { self.head.load(Ordering::Relaxed) } else { self.head.load(Ordering::Acquire) };

        loop {
            // Deconstruct the head.
            let index = head & (self.one_lap - 1);
            let lap = head & !(self.one_lap - 1);

            // Inspect the corresponding slot.
            debug_assert!(index < self.buffer.len());
            let slot = unsafe { self.buffer.get_unchecked(index) };
            let stamp = slot.stamp.load(Ordering::Acquire);

            // If the stamp is ahead of the head by 1, we may attempt to pop.
            if head + 1 == stamp {
                let new = if index + 1 < self.capacity() {
                    // Same lap, incremented index.
                    // Set to `{ lap: lap, index: index + 1 }`.
                    head + 1
                } else {
                    // One lap forward, index wraps around to zero.
                    // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
                    lap.wrapping_add(self.one_lap)
                };
                if MC {
                    // Try moving the head.
                    if let Err(h) = self.head.compare_exchange_weak(
                        head,
                        new,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        head = h;
                        backoff.spin();
                        continue;
                    }
                } else {
                    self.head.store(new, Ordering::SeqCst);
                }
                // Read the value from the slot and update the stamp.
                let msg = unsafe { slot.value.get().read().assume_init() };
                slot.stamp.store(head.wrapping_add(self.one_lap), Ordering::Release);
                return Some(msg);
            } else {
                if stamp == head {
                    let tail = if MP || MC {
                        // NOTE: The fence is preventing livestock
                        atomic::fence(Ordering::SeqCst);
                        self.tail.load(Ordering::Relaxed)
                    } else {
                        self.tail.load(Ordering::SeqCst)
                    };
                    // If the tail equals the head, that means the channel is empty.
                    if tail == head {
                        return None;
                    }
                    backoff.spin();
                } else {
                    // Snooze because we need to wait for the stamp to get updated.
                    backoff.snooze();
                }
                if MC {
                    head = self.head.load(Ordering::Relaxed);
                }
            }
        }
    }

    /// Returns the capacity of the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_queue::ArrayQueue;
    ///
    /// let q = ArrayQueue::<i32>::new(100);
    ///
    /// assert_eq!(q.capacity(), 100);
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` if the queue is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_queue::ArrayQueue;
    ///
    /// let q = ArrayQueue::new(100);
    ///
    /// assert!(q.is_empty());
    /// q.push(1).unwrap();
    /// assert!(!q.is_empty());
    /// ```
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::SeqCst);
        let tail = self.tail.load(Ordering::SeqCst);

        // Is the tail lagging one lap behind head?
        // Is the tail equal to the head?
        //
        // Note: If the head changes just before we load the tail, that means there was a moment
        // when the channel was not empty, so it is safe to just return `false`.
        tail == head
    }

    /// Returns `true` if the queue is full.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_queue::ArrayQueue;
    ///
    /// let q = ArrayQueue::new(1);
    ///
    /// assert!(!q.is_full());
    /// q.push(1).unwrap();
    /// assert!(q.is_full());
    /// ```
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        let tail = self.tail.load(Ordering::SeqCst);
        let head = self.head.load(Ordering::SeqCst);

        // Is the head lagging one lap behind tail?
        //
        // Note: If the tail changes just before we load the head, that means there was a moment
        // when the queue was not full, so it is safe to just return `false`.
        head.wrapping_add(self.one_lap) == tail
    }

    /// Returns the number of elements in the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_queue::ArrayQueue;
    ///
    /// let q = ArrayQueue::new(100);
    /// assert_eq!(q.len(), 0);
    ///
    /// q.push(10).unwrap();
    /// assert_eq!(q.len(), 1);
    ///
    /// q.push(20).unwrap();
    /// assert_eq!(q.len(), 2);
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        loop {
            // Load the tail, then load the head.
            let tail = self.tail.load(Ordering::SeqCst);
            let head = self.head.load(Ordering::SeqCst);

            // If the tail didn't change, we've got consistent values to work with.
            if self.tail.load(Ordering::SeqCst) == tail {
                let hix = head & (self.one_lap - 1);
                let tix = tail & (self.one_lap - 1);

                return if hix < tix {
                    tix - hix
                } else if hix > tix {
                    self.capacity() - hix + tix
                } else if tail == head {
                    0
                } else {
                    self.capacity()
                };
            }
        }
    }
}

impl<T, const MP: bool, const MC: bool> Drop for ArrayQueue<T, MP, MC> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            // Get the index of the head.
            let head = *self.head.get_mut();
            let tail = *self.tail.get_mut();

            let hix = head & (self.one_lap - 1);
            let tix = tail & (self.one_lap - 1);

            let len = if hix < tix {
                tix - hix
            } else if hix > tix {
                self.capacity() - hix + tix
            } else if tail == head {
                0
            } else {
                self.capacity()
            };

            // Loop over all slots that hold a message and drop them.
            for i in 0..len {
                // Compute the index of the next slot holding a message.
                let index =
                    if hix + i < self.capacity() { hix + i } else { hix + i - self.capacity() };

                unsafe {
                    debug_assert!(index < self.buffer.len());
                    let slot = self.buffer.get_unchecked_mut(index);
                    (*slot.value.get()).assume_init_drop();
                }
            }
        }
    }
}
