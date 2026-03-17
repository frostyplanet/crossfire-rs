//! This module provides two waitgroup implementation, works in blocking & async context.
//! The implementation is low-cost ref-counting (counter and waker state is packed inside one atomic), the max value
//! is (1 << (usize::BITS - 2) - 2)
//!
//! - [WaitGroupInline]: Which embedded inline with its parent structure (with no dereference cost)
//!   - (Arc or other container should be used to share among threads).
//!   - Threshold is const
//!   - Requires manual ref count manager (add_many(), done_many()).
//!   - only one waiter thread is allowed.
//!
//! - [WaitGroup]: which is a safe RAII guard API.
//!   - Its a box container, optional state inside may be shared between the threads of WaitGroup and its guards.
//!   - Only one waiter is allowed.
//!   - Use [WaitGroup::add_guard()] to get [WaitGroupGuard].
//!   - `WaitGroupGuard` will increase ref, and drop will decrease ref and protentially wake the main thread.
//!   - Can change threshold at any time.
//!   - **NOTE**:
//!     threshold is carried inside generated [WaitGroupGuard] to minimize the cost of atomic ops.
//!     When changing threshold to larger value, wait() might not wake up as soon as new threshold reached.
//!
//! # Safety
//!
//! [WaitGroup] does not have `Sync` marker, because it's not safe to concurrently wait, due to only one slot reserved for waker.
//! If you know what you are doing when put it inside other struct, use unsafe impl on its parent
//! struct.
//!
//! ```
//! use crossfire::waitgroup::WaitGroup;
//! use std::sync::Arc;
//! pub struct Parent {
//!     wg: WaitGroup<()>,
//! }
//! // allow parent to have Sync marker for Arc
//! unsafe impl Sync for Parent {}
//!
//! let _parent = Arc::new(Parent{
//!     wg: WaitGroup::new((), 0),
//! });
//! ```
//!
//! # Examples
//!
//! **Blocking Example: Concurrency Limiter**
//!
//! This example simulates a task scheduler that uses a `WaitGroup` to limit
//! the number of concurrently running tasks to a specific watermark.
//! It also uses the generic `T` to carry a shared state (e.g. `AtomicBool`)
//!
//! ```
//! use crossfire::waitgroup::WaitGroup;
//! use std::thread;
//! use std::time::Duration;
//! use std::sync::atomic::{AtomicBool, Ordering};
//!
//! const MAX_CONCURRENT_TASKS: usize = 4;
//! const TOTAL_TASKS: usize = 10;
//!
//! // Initialize WaitGroup with a threshold of N-1.
//! // `wait()` will block when the number of running tasks is >= N.
//! // The `AtomicBool` is used to track if any task failed.
//! let mut wg = WaitGroup::<AtomicBool>::new(AtomicBool::new(true), MAX_CONCURRENT_TASKS - 1);
//!
//! // Use a simple for loop to spawn a total of 10 tasks.
//! for i in 0..TOTAL_TASKS {
//!     // `wait()` blocks until `wg.get_left() < MAX_CONCURRENT_TASKS`.
//!     // This effectively waits for a slot to become available.
//!     wg.wait();
//!     // A slot is available, spawn a new task.
//!     let guard = wg.add_guard();
//!     thread::spawn(move || {
//!         thread::sleep(Duration::from_millis(100));
//!         // do some work
//!         if i == 5 {
//!             // Notify failure
//!             guard.store(false, Ordering::SeqCst);
//!         }
//!         drop(guard);
//!     });
//! }
//! // After spawning all tasks, wait for the remaining running tasks to finish.
//! // Set threshold to 0 to wait until all guards are dropped.
//! wg.set_threshold(0);
//! wg.wait();
//!
//! assert_eq!(wg.get_left_seqcst(), 0);
//! assert_eq!(wg.load(Ordering::SeqCst), false);
//! ```
//!
//! **Async Example**
//!
//! This example demonstrate task and sub-task, dynamic increase ref count by cloning WaitGroupGuard.
//!
//! ```
//! use crossfire::waitgroup::WaitGroup;
//! use std::time::Duration;
//!
//! #[tokio::test]
//! async fn wait_group_async_example() {
//!     let wg = WaitGroup::new((), 0);
//!     for _j in 0..4 {
//!         // Create a guard for the manager task.
//!         let parent_guard = wg.add_guard();
//!         tokio::spawn(async move {
//!             // This manager task will spawn 2 workers.
//!             for i in 0..2 {
//!                 let child_guard = parent_guard.clone();
//!                 tokio::spawn(async move {
//!                     // Do some work...
//!                     tokio::time::sleep(Duration::from_millis(50 * (i + 1))).await;
//!                     // worker_guard is dropped here.
//!                 });
//!             }
//!             // The manager's work is to spawn workers,
//!             // so it drops its own guard after the loop.
//!             drop(manager_guard);
//!         });
//!     }
//!     // Wait until the manager guard and all its clones are dropped.
//!     wg.wait_async().await;
//!     assert_eq!(wg.get_left_seqcst(), 0);
//! }
//! ```

use crate::backoff::Backoff;
use crate::shared::{check_timeout, ThinWaker};
#[allow(unused_imports)]
use crate::{tokio_task_id, trace_log};
use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::transmute;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{
    AtomicUsize,
    Ordering::{self, Acquire, Relaxed, SeqCst},
};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

/// An unsafe version WaitGroup which does not allocate, must embedded in a parent Arc structure.
///
/// # Limitation
///
/// - THRESHOLD is const, default to zero
/// - Only one thread / coroutine to wait, all wait_XXX() function is unsafe.
/// - done() is unsafe.
/// - Also provide add_many() done_many().
pub struct WaitGroupInline<const THRESHOLD: usize = 0> {
    inner: WaitGroupInner<()>,
}

impl<const THRESHOLD: usize> WaitGroupInline<THRESHOLD> {
    pub fn new() -> Self {
        Self { inner: WaitGroupInner::new((), 0) }
    }

    /// load total reference count of `WaitGroupGuard` with SeqCst
    #[inline(always)]
    pub fn get_left_seqcst(&self) -> usize {
        // minus my own ref
        self.inner.count(SeqCst) - 1
    }

    /// Return total reference count of `WaitGroupGuard` with Acquire
    #[inline(always)]
    pub fn get_left(&self) -> usize {
        // minus my own ref
        self.inner.count(Acquire) - 1
    }

    /// Add one count to the WaitGroup
    #[inline(always)]
    pub fn add(&self) {
        self.inner.add(1);
    }

    /// Add multiple count to the WaitGroup
    #[inline(always)]
    pub fn add_many(&self, count: usize) {
        debug_assert!(count < COUNT_MASK - 2);
        self.inner.add(count);
    }

    /// Decrease one count, if it reduced to zero, will waking the waiter thread.
    ///
    /// Return true when zero has been reached
    ///
    /// # Safety
    ///
    /// You have to be careful for underflow, which will panic
    pub unsafe fn done(&self) -> bool {
        self.inner.done::<false>(1, THRESHOLD)
    }

    /// Decrease multiple count, if it reduced to zero, will waking the waiter thread.
    ///
    /// Return true when zero has been reached
    ///
    /// # Safety
    ///
    /// You have to be careful for underflow, which will panic
    pub unsafe fn done_many(&self, count: usize) -> bool {
        debug_assert!(count < COUNT_MASK - 2);
        self.inner.done::<false>(count, THRESHOLD)
    }

    /// If the ref count reaches zero, return `Ok(())`, otherwise `Err(())`
    #[inline]
    pub fn try_wait(&self) -> Result<(), ()> {
        // one ref owned by mysql
        if self.inner.count(SeqCst) <= THRESHOLD {
            Ok(())
        } else {
            Err(())
        }
    }

    /// Block current coroutine until count drop below threshold.
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub unsafe fn wait_async<'a>(&'a self) -> WaitGroupFuture<'a, ()> {
        WaitGroupFuture { inner: &self.inner, threshold: THRESHOLD, waker: None }
    }

    /// Block current coroutine until count drop below threshold, or until timeout happens
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    #[inline]
    pub unsafe fn wait_async_timeout<'a>(
        &'a self, timeout: Duration,
    ) -> WaitGroupTimeoutFuture<'a, (), tokio::time::Sleep, ()> {
        let sleep = tokio::time::sleep(timeout);
        self.wait_async_with_timer(sleep)
    }

    /// Block current coroutine until count drop below threshold, or until timeout happens
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[cfg(feature = "async_std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async_std")))]
    #[inline]
    pub unsafe fn wait_async_timeout<'a>(
        &'a self, timeout: Duration,
    ) -> WaitGroupTimeoutFuture<'a, (), impl Future<Output = ()>, ()> {
        let sleep = async_std::task::sleep(timeout);
        self.wait_async_with_timer(sleep)
    }

    /// Block current coroutine until count drop below threshold, with a custom sleep / or cancel function
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub unsafe fn wait_async_with_timer<'a, FR, R>(
        &'a self, fut: FR,
    ) -> WaitGroupTimeoutFuture<'a, (), FR, R>
    where
        FR: Future<Output = R>,
    {
        WaitGroupTimeoutFuture { inner: &self.inner, threshold: THRESHOLD, sleep: fut, waker: None }
    }

    /// Blocking current thread and Wait until count drop below threshold.
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub unsafe fn wait(&self) {
        let _ = self.inner.wait_blocking(None, THRESHOLD);
    }

    /// Blocking current thread and Wait until count drop below threshold, or until timeout
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub unsafe fn wait_timeout(&self, timeout: Duration) -> Result<(), ()> {
        self.inner.wait_blocking(Some(Instant::now() + timeout), THRESHOLD)
    }
}

/// A WaitGroup implementation allows custom threshold (>=0), works in blocking & async context.
///
/// Features:
/// - Only one waiter, concurrent ref count.
/// - Carry optional state inside, shared between the main thread and WaitGroupGuard, just like Arc.
/// - Change threshold at any time.
///   - **NOTE**:
///     threshold is carried inside generated [WaitGroupGuard] to minimize the cost of atomic ops.
///     When changing threshold to larger value, wait() might not wake up as soon as new threshold reached.
/// - Low-cost create and drop, because reference count and waker state is packed inside one atomic.
/// - WaitGroupGuard dropping is wait-free, which decrease ref count with SeqCst CAS.
/// - Max reference count to (1 << (usize::BITS - 2) - 2)
///
/// You don't need to put WaitGroup into Arc, use [WaitGroup::add_guard()] to get `WaitGroupGuard`.
/// It's ok to clone [WaitGroupGuard], which will increase internal ref count.
///
/// # Safety
///
/// It's not safe to concurrently wait, so it does not have `Sync` marker.
/// If you know what you are doing when put it inside other struct, use unsafe impl.
///
/// See module level [doc](crate::waitgroup) for example.
pub struct WaitGroup<T> {
    threshold: usize,
    inner: NonNull<WaitGroupInner<T>>,
    // Remove the Sync marker to prevent concurrent waiting
}

unsafe impl<T: Send> Send for WaitGroup<T> {}

impl<T> WaitGroup<T> {
    #[inline(always)]
    pub fn new(inner: T, threshold: usize) -> Self {
        let inner = Box::new(WaitGroupInner::new(inner, 1));
        Self {
            // one ref owned by myself
            threshold: threshold + 1,
            inner: unsafe { NonNull::new_unchecked(Box::into_raw(inner)) },
        }
    }

    /// Threshold can be changed on the fly, which only affect the next `wait()`.
    ///
    /// # Safety
    ///
    /// Previous threshold is carried inside generated `WaitGroupGuard`.
    /// When changing threshold to larger value, wait() might not wake up as soon as new threshold reached.
    #[inline]
    pub fn set_threshold(&mut self, threshold: usize) {
        // one ref owned by myself
        self.threshold = threshold + 1;
    }

    #[inline(always)]
    fn get_inner(&self) -> &WaitGroupInner<T> {
        unsafe { self.inner.as_ref() }
    }

    /// load total reference count of `WaitGroupGuard` with SeqCst
    #[inline(always)]
    pub fn get_left_seqcst(&self) -> usize {
        // minus my own ref
        self.get_inner().count(SeqCst) - 1
    }

    /// Return total reference count of `WaitGroupGuard` with Acquire
    #[inline(always)]
    pub fn get_left(&self) -> usize {
        // minus my own ref
        self.get_inner().count(Acquire) - 1
    }

    /// Add one ref count to the WaitGroup, return a guard to decrease the count on drop.
    #[inline(always)]
    pub fn add_guard(&self) -> WaitGroupGuard<T> {
        self.get_inner().add(1);
        WaitGroupGuard { inner: self.inner, threshold: self.threshold }
    }

    /// If the ref count is below threshold, return `Ok(())`, otherwise `Err(())`
    #[inline]
    pub fn try_wait(&self) -> Result<(), ()> {
        // one ref owned by mysql
        if self.get_inner().count(SeqCst) <= self.threshold {
            Ok(())
        } else {
            Err(())
        }
    }

    /// Block current coroutine until count drop below threshold.
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub fn wait_async<'a>(&'a self) -> WaitGroupFuture<'a, T>
    where
        T: Send + Unpin,
    {
        let inner = self.get_inner();
        WaitGroupFuture { inner, threshold: self.threshold, waker: None }
    }

    /// Block current coroutine until count drop below threshold, or until timeout happens
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    #[inline]
    pub fn wait_async_timeout<'a>(
        &'a self, timeout: Duration,
    ) -> WaitGroupTimeoutFuture<'a, T, tokio::time::Sleep, ()>
    where
        T: Send + Unpin,
    {
        let sleep = tokio::time::sleep(timeout);
        self.wait_async_with_timer(sleep)
    }

    /// Block current coroutine until count drop below threshold, or until timeout happens
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[cfg(feature = "async_std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async_std")))]
    #[inline]
    pub fn wait_async_timeout<'a>(
        &'a self, timeout: Duration,
    ) -> WaitGroupTimeoutFuture<'a, T, impl Future<Output = ()>, ()>
    where
        T: Send + Unpin,
    {
        let sleep = async_std::task::sleep(timeout);
        self.wait_async_with_timer(sleep)
    }

    /// Block current coroutine until count drop below threshold, with a custom sleep / or cancel function
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub fn wait_async_with_timer<'a, FR, R>(
        &'a self, fut: FR,
    ) -> WaitGroupTimeoutFuture<'a, T, FR, R>
    where
        FR: Future<Output = R>,
        T: Send + Unpin,
    {
        let inner = self.get_inner();
        WaitGroupTimeoutFuture { inner, threshold: self.threshold, sleep: fut, waker: None }
    }

    /// Blocking current thread and Wait until count drop below threshold.
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub fn wait(&self) {
        let _ = self.get_inner().wait_blocking(None, self.threshold);
    }

    /// Blocking current thread and Wait until count drop below threshold, or until timeout
    ///
    /// # Safety
    ///
    /// Only one thread is allow to wait
    #[inline]
    pub fn wait_timeout(&self, timeout: Duration) -> Result<(), ()> {
        self.get_inner().wait_blocking(Some(Instant::now() + timeout), self.threshold)
    }
}

impl<T> Drop for WaitGroup<T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            WaitGroupInner::destroy(self.inner);
        }
    }
}

impl<T> Deref for WaitGroup<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &unsafe { self.inner.as_ref() }.inner
    }
}

/// An RAII implementation got represent ref count in WaitGroup.
///
/// When cloning WaitGroupGuard, which will increase the ref count in WaitGroup.
///
/// WaitGroupGuard dropping is wait-free, which decrease ref count with SeqCst CAS.
/// will wake up the waiter once ref count decrease below threshold.
///
/// **NOTE**: Threshold is carried inside as non-atomic, not syned with the main thread for
/// efficiency. But it's sufficient for most scenario.
///
pub struct WaitGroupGuard<T> {
    inner: NonNull<WaitGroupInner<T>>,
    threshold: usize,
}

unsafe impl<T: Send> Send for WaitGroupGuard<T> {}
unsafe impl<T: Sync> Sync for WaitGroupGuard<T> {}

impl<T> Drop for WaitGroupGuard<T> {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            WaitGroupInner::done_ptr(self.inner, 1, self.threshold);
        }
    }
}

impl<T> Clone for WaitGroupGuard<T> {
    #[inline]
    fn clone(&self) -> Self {
        let inner = unsafe { self.inner.as_ref() };
        inner.add(1);
        Self { inner: self.inner, threshold: self.threshold }
    }
}

impl<T> Deref for WaitGroupGuard<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &unsafe { self.inner.as_ref() }.inner
    }
}

struct WaitGroupInner<T> {
    /// Refer to the doc of State
    state: AtomicUsize,
    o_waker: UnsafeCell<Option<ThinWaker>>,
    inner: T,
}

unsafe impl<T: Sync> Sync for WaitGroupInner<T> {}

impl<T> WaitGroupInner<T> {
    #[inline(always)]
    fn new(inner: T, init_count: usize) -> Self {
        Self { state: AtomicUsize::new(init_count), o_waker: UnsafeCell::new(None), inner }
    }

    #[inline]
    fn count(&self, order: Ordering) -> usize {
        self.state.load(order) & COUNT_MASK
    }

    #[inline(always)]
    fn get_waker(&self) -> &mut Option<ThinWaker> {
        unsafe { transmute(self.o_waker.get()) }
    }

    #[inline]
    fn add(&self, count: usize) {
        let old_state = self.state.fetch_add(count, Relaxed);
        if State::new(old_state).count() >= COUNT_MASK - 2 {
            panic!("WaitGroup count overflowed");
        }
    }

    #[inline]
    unsafe fn destroy(p: NonNull<Self>) -> bool {
        let this = unsafe { p.as_ref() };
        let mut state = this.state.load(SeqCst);
        loop {
            let s = State::new(state);
            if s.is_locked() || s.count() > 1 {
                if let Err(_state) =
                    this.state.compare_exchange_weak(state, state - 1, SeqCst, Acquire)
                {
                    state = _state;
                    continue;
                }
                trace_log!("wg:({:?}) drop delay state={}", tokio_task_id!(), state - 1);
                return false;
            }
            {
                trace_log!("wg:({:?}) drop", tokio_task_id!());
                let _ = unsafe { Box::from_raw(p.as_ptr()) };
                return true;
            }
        }
    }

    #[inline(always)]
    unsafe fn done_ptr(p: NonNull<Self>, count: usize, threshold: usize) -> bool {
        let this = unsafe { p.as_ref() };
        if this.done::<true>(count, threshold) {
            let _ = unsafe { Box::from_raw(p.as_ptr()) };
            return true;
        } else {
            false
        }
    }

    /// return true to allow drop
    #[inline]
    fn done<const OWNER_SHIP: bool>(&self, count: usize, threshold: usize) -> bool {
        trace_log!("wg:({:?}) enter done {count} {threshold}", tokio_task_id!());
        let mut state = self.state.load(Relaxed);
        loop {
            let mut s = State::new(state);
            if OWNER_SHIP {
                if s.is_last(count) {
                    // in case non SeqCst read old value, double check with SeqCst
                    let _state = self.state.load(SeqCst);
                    if _state == state {
                        trace_log!("wg:({:?}) done drop {count} {threshold}", tokio_task_id!());
                        return true;
                    }
                    state = _state;
                    continue;
                }
            }
            // NOTE: When flag == WAKER_FLAG_LOCK, means one other thread is reading the waker,
            // we just try to decrease the count, but we should not drop it even ref reach 0
            let try_lock = s.try_done(count, threshold);
            if try_lock {
                debug_assert!(s.is_locked());
            }
            match self.state.compare_exchange_weak(state, s.to_usize(), SeqCst, Acquire) {
                Ok(_) => {
                    if try_lock {
                        let o_waker = self.get_waker().take();
                        // Probably the last chance to check state, should use SeqCst to unlock.
                        // ref count may reach 0, means I'm the last one.
                        let old = self.state.fetch_and(!WAKER_FLAG_MASK, SeqCst);
                        if OWNER_SHIP && old & COUNT_MASK == 0 {
                            trace_log!(
                                "wg:({:?}) done locked drop cur {count} = 0",
                                tokio_task_id!(),
                            );
                            // Safety: we had the lock, won't be others change the waker,
                            // we are the last one, don't need to actually wake, just destroy.
                            return true;
                        }
                        if let Some(waker) = o_waker {
                            trace_log!(
                                "wg:({:?}) done waked {count} -> {} <= {threshold}",
                                tokio_task_id!(),
                                s.count()
                            );
                            waker.wake();
                        }
                    } else {
                        trace_log!("wg:({:?}) done {count} -> {}", tokio_task_id!(), s.count());
                    }
                    return false;
                }
                Err(cur) => {
                    state = cur;
                }
            }
        }
    }

    /// may_skip = true, for blocking context does not need to overwrite waker
    #[inline]
    fn try_set_waker(&self, waker: ThinWaker, threshold: usize, may_skip: bool) -> Result<(), ()> {
        let mut state = self.state.load(SeqCst);
        loop {
            let s = State::new(state);
            if s.count() <= threshold {
                // Safety: because of this, use SeqCst to prevent reading old value
                return Err(());
            } else if s.is_locked() {
                // done() is waking
                std::hint::spin_loop();
                state = self.state.load(Acquire);
                trace_log!("wg:({:?}) set_waker try again", tokio_task_id!());
                continue;
            }
            let old_state = if s.has_waker() {
                if may_skip {
                    trace_log!("wg:({:?}) set_waker skip", tokio_task_id!());
                    return Ok(());
                }
                // waker exist, first try lock, then replace
                if let Err(s) =
                    self.state.compare_exchange_weak(state, s.try_lock(), SeqCst, Acquire)
                {
                    state = s;
                    continue;
                }
                self.get_waker().replace(waker);
                trace_log!("wg:({:?}) set_waker replaced", tokio_task_id!());
                // clear WAKER_FLAG_LOCK and set WAKER_FLAG_SET
                self.state.fetch_xor(WAKER_FLAG_MASK, SeqCst)
            } else {
                self.get_waker().replace(waker);
                trace_log!("wg:({:?}) set_waker ok", tokio_task_id!());
                self.state.fetch_or(WAKER_FLAG_SET, SeqCst)
            };
            if State::new(old_state).count() <= threshold {
                return Err(());
            }
            return Ok(());
        }
    }

    #[inline]
    fn wait_blocking(&self, deadline: Option<Instant>, threshold: usize) -> Result<(), ()> {
        macro_rules! check {
            ($order: expr) => {
                let cur = self.count($order);
                if cur <= threshold {
                    trace_log!("wg:({:?}) check {cur} <= {threshold}", tokio_task_id!());
                    return Ok(());
                }
                trace_log!("wg:({:?}) check {cur} > {threshold}", tokio_task_id!());
            };
        }
        check!(Acquire);
        let mut backoff = Backoff::new();
        let mut set_waker = false;
        loop {
            let r = backoff.snooze();
            check!(Acquire);
            if r {
                let waker = ThinWaker::Blocking(thread::current());
                if self.try_set_waker(waker, threshold, set_waker).is_err() {
                    return Ok(());
                } else {
                    set_waker = true;
                }
                match check_timeout(deadline) {
                    Ok(None) => thread::park(),
                    Ok(Some(dur)) => thread::park_timeout(dur),
                    Err(_) => {
                        return Err(());
                    }
                }
                backoff.reset();
            }
        }
    }

    #[inline]
    fn poll_async(
        &self, ctx: &mut Context, o_waker: &mut Option<Waker>, threshold: usize,
    ) -> Poll<()> {
        macro_rules! check {
            ($order: expr) => {{
                let s = State::new(self.state.load($order));
                let cur = s.count();
                if cur <= threshold {
                    trace_log!("wg:({:?}) READY check {cur} <= {threshold}", tokio_task_id!());
                    return Poll::Ready(());
                }
                trace_log!("wg:({:?}) check {cur} > {threshold}", tokio_task_id!());
                s.has_waker()
            }};
        }
        let has_waker = check!(Acquire);
        let new_waker = ctx.waker();
        if has_waker {
            #[allow(clippy::needless_else)]
            if let Some(old_waker) = o_waker {
                if old_waker.will_wake(new_waker) {
                    trace_log!("wg:({:?}) will_wake=true", tokio_task_id!());
                    check!(SeqCst);
                    trace_log!("wg:({:?}) PENDING", tokio_task_id!());
                    return Poll::Pending;
                } else {
                    trace_log!("wg:({:?}) waker will_wake=false", tokio_task_id!())
                }
            }
        }
        if self.try_set_waker(ThinWaker::Async(new_waker.clone()), threshold, false).is_err() {
            trace_log!("wg:({:?}) READY during set_waker", tokio_task_id!());
            Poll::Ready(())
        } else {
            o_waker.replace(new_waker.clone());
            trace_log!("wg:({:?}) PENDING", tokio_task_id!());
            Poll::Pending
        }
    }
}

#[must_use]
pub struct WaitGroupFuture<'a, T> {
    inner: &'a WaitGroupInner<T>,
    threshold: usize,
    waker: Option<Waker>,
}

impl<'a, T> Future for WaitGroupFuture<'a, T>
where
    T: Send + Unpin,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.inner.poll_async(ctx, &mut this.waker, this.threshold)
    }
}

/// Wait until the ref count is below threshold, return `Ok(())`.
/// If timeout happens returns `Err(())`
#[must_use]
pub struct WaitGroupTimeoutFuture<'a, T, FR, R>
where
    FR: Future<Output = R>,
    T: Send + Unpin,
{
    inner: &'a WaitGroupInner<T>,
    sleep: FR,
    threshold: usize,
    waker: Option<Waker>,
}

impl<'a, T, FR, R> Future for WaitGroupTimeoutFuture<'a, T, FR, R>
where
    FR: Future<Output = R>,
    T: Send + Unpin,
{
    type Output = Result<(), ()>;

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        if this.inner.poll_async(ctx, &mut this.waker, this.threshold).is_ready() {
            return Poll::Ready(Ok(()));
        }
        let sleep = unsafe { Pin::new_unchecked(&mut this.sleep) };
        if sleep.poll(ctx).is_ready() {
            Poll::Ready(Err(()))
        } else {
            Poll::Pending
        }
    }
}

const WAKER_FLAG_SET: usize = 1 << (usize::BITS - 1);
const WAKER_FLAG_LOCK: usize = 1 << (usize::BITS - 2);
const WAKER_FLAG_MASK: usize = WAKER_FLAG_SET | WAKER_FLAG_LOCK;
const COUNT_MASK: usize = !WAKER_FLAG_MASK;

/// The 2 highest bit is WAKER_FLAG_SET | WAKER_FLAG_LOCK, they are exclusive, so there're 3
/// states:
/// - 0: waker is not set
/// - WAKER_FLAG_SET: there's a waker, some one might be waiting, it's possible to give up waiting
///   when threshold is reached
/// - WAKER_FLAG_LOCK: there's one thread is reading the waker, when he is done, should reset the
///   state to 0.
///
/// ref count:
/// - the lower bits is for ref count. When initial to be 1.
/// - The WaitGroup can be drop early, leaving the WaitGroupGuard holders to drop the count.
/// - when the last holder drop the count to 0, is responsible to free the memory, with the following exception:
/// - NOTE that When WAKER_FLAG_LOCK is set, not allow to free the memory even count reach
///   0, the last one release the lock is responsible to free the memory
struct State(usize);

impl State {
    #[inline(always)]
    fn new(state: usize) -> Self {
        Self(state)
    }

    #[inline(always)]
    fn count(&self) -> usize {
        self.0 & COUNT_MASK
    }

    #[inline(always)]
    fn waker_flag(&self) -> usize {
        self.0 & WAKER_FLAG_MASK
    }

    #[inline(always)]
    fn is_locked(&self) -> bool {
        self.0 & WAKER_FLAG_LOCK > 0
    }

    #[inline(always)]
    fn has_waker(&self) -> bool {
        self.0 & WAKER_FLAG_SET > 0
    }

    #[inline(always)]
    fn try_lock(&self) -> usize {
        self.count() | WAKER_FLAG_LOCK
    }

    /// When no one lock and I'm the last one, can drop directly, return true
    #[inline]
    fn is_last(&self, delta: usize) -> bool {
        let waker_flag = self.waker_flag();
        waker_flag != WAKER_FLAG_LOCK && self.count() == delta
    }

    /// # Return value:
    /// - should_lock==true: when reach threshold, should dec count and try_lock.
    /// - should_lock==false: just decrease count.
    #[inline(always)]
    fn try_done(&mut self, delta: usize, threshold: usize) -> bool {
        let waker_flag = self.waker_flag();
        let new_count = self.count() - delta;
        let try_lock = new_count <= threshold && waker_flag == WAKER_FLAG_SET;
        if try_lock {
            self.0 = WAKER_FLAG_LOCK | new_count;
            true
        } else {
            self.0 = waker_flag | new_count;
            false
        }
    }

    #[inline(always)]
    #[allow(clippy::wrong_self_convention)]
    fn to_usize(&self) -> usize {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use captains_log::{recipe, ConsoleTarget, Level};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_waitgroup_inner_count() {
        let wg = WaitGroup::new((), 0);
        assert_eq!(wg.get_left_seqcst(), 0);
        let guard1 = wg.add_guard();
        assert_eq!(wg.get_left_seqcst(), 1);
        let guard2 = wg.add_guard();
        assert_eq!(wg.get_left_seqcst(), 2);
        drop(guard1);
        assert_eq!(wg.get_left_seqcst(), 1);
        drop(guard2);
        assert_eq!(wg.get_left_seqcst(), 0);
    }

    #[test]
    fn test_waitgroup_state() {
        assert_eq!(State::new(2).count(), 2);
        assert!(State::new(2 | WAKER_FLAG_SET).has_waker());
        assert!(!State::new(2 | WAKER_FLAG_SET).is_locked());
        assert!(!State::new(2 | WAKER_FLAG_LOCK).has_waker());
        assert!(State::new(2 | WAKER_FLAG_LOCK).is_locked());
        let mut s = State::new(2);
        // no waker
        assert_eq!(s.try_done(1, 1), false);
        assert!(!s.is_locked());
        assert_eq!(s.count(), 1);
        // threshold is ignore, just drop
        assert!(s.is_last(1));
        // state don't need to change
        assert_eq!(s.count(), 1);

        // WAKER_FLAG_SET ( 3-1 <=2 )-> WAKER_FLAG_LOCK
        let mut s = State::new(3 | WAKER_FLAG_SET);
        assert!(!s.is_last(1));
        assert_eq!(s.try_done(1, 2), true);
        assert!(s.is_locked());
        assert!(!s.has_waker());
        assert_eq!(s.count(), 2);

        // WAKER_FLAG_LOCK -> dec
        assert_eq!(s.try_done(1, 0), false);
        assert!(s.is_locked());
        assert_eq!(s.count(), 1);

        // WAKER_FLAG_LOCK -> no waker
        let _s = s.0 & (!WAKER_FLAG_MASK);
        assert_eq!(_s, 1);

        // WAKER_FLAG_LOCK exist, don't drop, just dec
        assert_eq!(s.try_done(1, 0), false);
        assert_eq!(s.count(), 0);
    }

    #[test]
    fn test_waitgroup_ptr() {
        recipe::console_logger(ConsoleTarget::Stdout, Level::Trace).test().build().expect("log");
        let inner = Box::new(WaitGroupInner::new((), 1));
        assert_eq!(inner.count(SeqCst), 1);
        assert_eq!(State::new(inner.state.load(Ordering::SeqCst)).waker_flag(), 0);

        println!("test try_set_waker met threshold reach");
        assert_eq!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, false), Err(()));

        inner.add(1);
        assert_eq!(inner.count(SeqCst), 2);
        println!("test try_set_waker ok");
        assert!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, false).is_ok());
        let s = State::new(inner.state.load(Ordering::SeqCst));
        assert_eq!(s.waker_flag(), WAKER_FLAG_SET, "s {}, {}", s.is_locked(), s.has_waker());

        println!("test try_set_waker again skip");
        assert!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, true).is_ok());
        let s = State::new(inner.state.load(Ordering::SeqCst));
        assert_eq!(s.waker_flag(), WAKER_FLAG_SET);

        println!("test try_set_waker again force");
        assert!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, false).is_ok());
        let s = State::new(inner.state.load(Ordering::SeqCst));
        assert_eq!(s.waker_flag(), WAKER_FLAG_SET);
        assert_eq!(inner.count(SeqCst), 2);

        let p = unsafe { NonNull::new_unchecked(Box::into_raw(inner)) };
        println!("test done triggering wakeup");
        unsafe {
            assert!(!WaitGroupInner::done_ptr(p, 1, 1));
            {
                let inner = p.as_ref();
                assert_eq!(inner.count(SeqCst), 1);
                let s = State::new(inner.state.load(Ordering::SeqCst));
                assert_eq!(s.waker_flag(), 0);
            }
            println!("test done triggering drop");
            assert!(WaitGroupInner::done_ptr(p, 1, 0));
        }
    }

    #[test]
    fn test_waitgroup_inner() {
        recipe::console_logger(ConsoleTarget::Stdout, Level::Trace).test().build().expect("log");
        let inner = WaitGroupInner::new((), 1);
        assert_eq!(inner.count(SeqCst), 1);
        assert_eq!(State::new(inner.state.load(Ordering::SeqCst)).waker_flag(), 0);

        println!("test try_set_waker met threshold reach");
        assert_eq!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, false), Err(()));

        inner.add(1);
        assert_eq!(inner.count(SeqCst), 2);
        println!("test try_set_waker ok");
        assert!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, false).is_ok());
        let s = State::new(inner.state.load(Ordering::SeqCst));
        assert_eq!(s.waker_flag(), WAKER_FLAG_SET, "s {}, {}", s.is_locked(), s.has_waker());

        println!("test try_set_waker again skip");
        assert!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, true).is_ok());
        let s = State::new(inner.state.load(Ordering::SeqCst));
        assert_eq!(s.waker_flag(), WAKER_FLAG_SET);

        println!("test try_set_waker again force");
        assert!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, false).is_ok());
        let s = State::new(inner.state.load(Ordering::SeqCst));
        assert_eq!(s.waker_flag(), WAKER_FLAG_SET);
        assert_eq!(inner.count(SeqCst), 2);

        println!("test done triggering wakeup");
        assert!(!inner.done::<false>(1, 1));
        {
            assert_eq!(inner.count(SeqCst), 1);
            let s = State::new(inner.state.load(Ordering::SeqCst));
            assert_eq!(s.waker_flag(), 0);
        }
        println!("test done triggering drop");
        assert!(inner.done::<false>(1, 0));
    }

    #[test]
    fn test_waitgroup_underflow() {
        recipe::console_logger(ConsoleTarget::Stdout, Level::Trace).test().build().expect("log");
        let wg = Arc::new(WaitGroupInline::<0>::new());
        wg.add_many(1);
        let _wg = wg.clone();
        let th = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            unsafe { _wg.done_many(1) };
        });
        unsafe { wg.wait() };
        th.join().expect("join");
    }
}
