//! A WaitGroup implementation allows custom threshold (>=0), works in blocking & async context.
//!
//! Features:
//! - Only one waiter, concurrent ref count.
//! - Change threshold at any time.
//!   - **NOTE**:
//!     threshold is carried inside generated [WaitGroupGuard] to minimize the cost of atomic ops.
//!     When changing threshold to larger value, wait() might not wake up as soon as new threshold reached.
//! - Low-cost create and drop, because reference count and waker state is packed inside one atomic.
//! - WaitGroupGuard dropping is wait-free
//! - Max reference count to (1 << (usize::BITS - 2) - 2)
//!
//! You don't need to put WaitGroup into Arc, use [WaitGroup::add_guard()] to get [WaitGroupGuard].
//! It's ok to clone `WaitGroupGuard`, which will increase internal ref count.
//!
//! # Safety
//!
//! Due to
//!
//! It's not safe to concurrently wait, so it does not have `Sync` marker.
//! If you know what you are doing when put it inside other struct, use unsafe impl.
//!
//! ```
//! use crossfire::waitgroup::WaitGroup;
//! use std::sync::Arc;
//! pub struct Parent {
//!     wg: WaitGroup,
//! }
//! // allow parent to have Sync marker
//! unsafe impl Sync for Parent {}
//!
//! let _parent = Arc::new(Parent{
//!     wg: WaitGroup::new(0),
//! });
//! ```
//!
//! # Examples
//!
//! **Blocking Example: Concurrency Limiter**
//!
//! This example simulates a task scheduler that uses a `WaitGroup` to limit
//! the number of concurrently running tasks to a specific watermark.
//!
//! ```
//! use crossfire::waitgroup::WaitGroup;
//! use std::thread;
//! use std::time::Duration;
//!
//! const MAX_CONCURRENT_TASKS: usize = 4;
//! const TOTAL_TASKS: usize = 10;
//!
//! // Initialize WaitGroup with a threshold of N-1.
//! // `wait()` will block when the number of running tasks is >= N.
//! let mut wg = WaitGroup::new(MAX_CONCURRENT_TASKS - 1);
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
//!         drop(guard);
//!     });
//! }
//! // After spawning all tasks, wait for the remaining running tasks to finish.
//! // Set threshold to 0 to wait until all guards are dropped.
//! wg.set_threshold(0);
//! wg.wait();
//!
//! assert_eq!(wg.get_left_seqcst(), 0);
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
//!     let wg = WaitGroup::new(0);
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
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{
    AtomicUsize,
    Ordering::{self, Acquire, Relaxed, SeqCst},
};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

/// A WaitGroup implementation allows custom threshold (>=0), works in blocking & async context.
///
/// Features:
/// - Only one waiter, concurrent ref count.
/// - Change threshold at any time.
///   - **NOTE**:
///     threshold is carried inside generated [WaitGroupGuard] to minimize the cost of atomic ops.
///     When changing threshold to larger value, wait() might not wake up as soon as new threshold reached.
/// - Low-cost create and drop, because reference count and waker state is packed inside one atomic.
/// - WaitGroupGuard dropping is wait-free
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
pub struct WaitGroup {
    threshold: usize,
    inner: NonNull<WaitGroupInner>,
    // Remove the Sync marker to prevent concurrent waiting
}

unsafe impl Send for WaitGroup {}

impl WaitGroup {
    #[inline(always)]
    pub fn new(threshold: usize) -> Self {
        let inner = WaitGroupInner::new();
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
    fn get_inner(&self) -> &WaitGroupInner {
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
    pub fn add_guard(&self) -> WaitGroupGuard {
        self.get_inner().add();
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

    /// Wait until count drop below threshold.
    #[inline]
    pub fn wait_async<'a>(&'a self) -> WaitGroupFuture<'a> {
        let inner = self.get_inner();
        WaitGroupFuture { inner, threshold: self.threshold, waker: None }
    }

    #[cfg(feature = "tokio")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
    #[inline]
    pub fn wait_async_timeout<'a>(
        &'a self, timeout: Duration,
    ) -> WaitGroupTimeoutFuture<'a, tokio::time::Sleep, ()> {
        let sleep = tokio::time::sleep(timeout);
        self.wait_async_with_timer(sleep)
    }
    #[cfg(feature = "async_std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "async_std")))]
    #[inline]
    pub fn wait_async_timeout<'a>(
        &'a self, timeout: Duration,
    ) -> WaitGroupTimeoutFuture<'a, impl Future<Output = ()>, ()> {
        let sleep = async_std::task::sleep(timeout);
        self.wait_async_with_timer(sleep)
    }

    #[inline]
    pub fn wait_async_with_timer<'a, FR, R>(&'a self, fut: FR) -> WaitGroupTimeoutFuture<'a, FR, R>
    where
        FR: Future<Output = R>,
    {
        let inner = self.get_inner();
        WaitGroupTimeoutFuture { inner, threshold: self.threshold, sleep: fut, waker: None }
    }

    /// Blocking current thread and Wait until count drop below threshold.
    #[inline]
    pub fn wait(&self) {
        let _ = self._wait_blocking(None);
    }

    #[inline]
    pub fn wait_timeout(&self, timeout: Duration) -> Result<(), ()> {
        self._wait_blocking(Some(Instant::now() + timeout))
    }

    #[inline]
    fn _wait_blocking(&self, deadline: Option<Instant>) -> Result<(), ()> {
        let inner = self.get_inner();
        let threshold = self.threshold;
        macro_rules! check {
            ($order: expr) => {
                let cur = inner.count($order);
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
                if inner.try_set_waker(waker, threshold, set_waker).is_err() {
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
}

impl Drop for WaitGroup {
    #[inline]
    fn drop(&mut self) {
        WaitGroupInner::destroy(self.inner);
    }
}

/// An RAII implementation got represent ref count in WaitGroup.
///
/// **NOTE**: When cloning WaitGroupGuard, which will increase the count in WaitGroup
///
/// **NOTE**: Threshold is carried in inside as non-atomic,
/// will wake up the waiter once ref count decrease below threshold.
pub struct WaitGroupGuard {
    inner: NonNull<WaitGroupInner>,
    threshold: usize,
}

unsafe impl Send for WaitGroupGuard {}

impl Drop for WaitGroupGuard {
    #[inline(always)]
    fn drop(&mut self) {
        WaitGroupInner::done(self.inner, 1, self.threshold);
    }
}

impl Clone for WaitGroupGuard {
    #[inline]
    fn clone(&self) -> Self {
        let inner = unsafe { self.inner.as_ref() };
        inner.add();
        Self { inner: self.inner, threshold: self.threshold }
    }
}

struct WaitGroupInner {
    /// Refer to the doc of State
    state: AtomicUsize,
    o_waker: UnsafeCell<Option<ThinWaker>>,
}

unsafe impl Send for WaitGroupInner {}
unsafe impl Sync for WaitGroupInner {}

impl WaitGroupInner {
    #[inline(always)]
    fn new() -> Box<Self> {
        Box::new(Self { state: AtomicUsize::new(1), o_waker: UnsafeCell::new(None) })
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
    fn add(&self) {
        let old_state = self.state.fetch_add(1, Relaxed);
        if State::new(old_state).count() >= COUNT_MASK - 2 {
            panic!("WaitGroup count overflowed");
        }
    }

    #[inline]
    fn destroy(p: NonNull<Self>) -> bool {
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

    #[inline]
    fn done(p: NonNull<Self>, count: usize, threshold: usize) -> bool {
        trace_log!("wg:({:?}) enter done {count} {threshold}", tokio_task_id!());
        let this = unsafe { p.as_ref() };
        let mut state = this.state.load(Relaxed);
        loop {
            let mut s = State::new(state);
            // NOTE: When flag == WAKER_FLAG_LOCK, means one other thread is reading the waker,
            // we just try to decrease the count, but we should not drop it even ref reach 0
            let try_lock = match s.try_done(count, threshold) {
                Some(false) => {
                    // in case non SeqCst read old value, double check with SeqCst
                    let _state = this.state.load(SeqCst);
                    if _state == state {
                        trace_log!("wg:({:?}) done drop {count} {threshold}", tokio_task_id!());
                        let _ = unsafe { Box::from_raw(p.as_ptr()) };
                        return true;
                    }
                    state = _state;
                    continue;
                }
                Some(true) => {
                    debug_assert!(s.is_locked());
                    true
                }
                None => false,
            };
            match this.state.compare_exchange_weak(state, s.to_usize(), SeqCst, Acquire) {
                Ok(_) => {
                    if try_lock {
                        let o_waker = this.get_waker().take();
                        // Probably the last chance to check state, should use SeqCst to unlock.
                        // ref count may reach 0, means I'm the last one.
                        let old = this.state.fetch_and(!WAKER_FLAG_MASK, SeqCst);
                        if old & COUNT_MASK == 0 {
                            trace_log!(
                                "wg:({:?}) done locked drop cur {count} = 0",
                                tokio_task_id!(),
                            );
                            // Safety: we had the lock, won't be others change the waker
                            let _ = unsafe { Box::from_raw(p.as_ptr()) };
                            return true;
                        } else if let Some(waker) = o_waker {
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
pub struct WaitGroupFuture<'a> {
    inner: &'a WaitGroupInner,
    threshold: usize,
    waker: Option<Waker>,
}

impl<'a> Future for WaitGroupFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        this.inner.poll_async(ctx, &mut this.waker, this.threshold)
    }
}

/// Wait until the ref count is below threshold, return `Ok(())`.
/// If timeout happens returns `Err(())`
#[must_use]
pub struct WaitGroupTimeoutFuture<'a, FR, R>
where
    FR: Future<Output = R>,
{
    inner: &'a WaitGroupInner,
    sleep: FR,
    threshold: usize,
    waker: Option<Waker>,
}

impl<'a, FR, R> Future for WaitGroupTimeoutFuture<'a, FR, R>
where
    FR: Future<Output = R>,
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

    /// # Return value:
    /// - return Some(false) when can drop directly, nothing changed.
    /// - return Some(true) when reach threshold, should dec count and try_lock.
    /// - None for just decrease count.
    #[inline(always)]
    fn try_done(&mut self, delta: usize, threshold: usize) -> Option<bool> {
        let old_count = self.count();
        let waker_flag = self.waker_flag();
        if waker_flag != WAKER_FLAG_LOCK && old_count == delta {
            // no one lock and I'm the last one, can drop
            return Some(false);
        }
        let new_count = old_count - delta;
        let try_lock = new_count <= threshold && waker_flag == WAKER_FLAG_SET;
        if try_lock {
            self.0 = WAKER_FLAG_LOCK | new_count;
            Some(true)
        } else {
            self.0 = waker_flag | new_count;
            None
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

    #[test]
    fn test_waitgroup_inner_count() {
        let wg = WaitGroup::new(0);
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
        assert_eq!(s.try_done(1, 1), None);
        assert!(!s.is_locked());
        assert_eq!(s.count(), 1);
        // threshold is ignore, just drop
        assert_eq!(s.try_done(1, 1), Some(false));
        // state don't need to change
        assert_eq!(s.count(), 1);

        // WAKER_FLAG_SET ( 3-1 <=2 )-> WAKER_FLAG_LOCK
        let mut s = State::new(3 | WAKER_FLAG_SET);
        assert_eq!(s.try_done(1, 2), Some(true));
        assert!(s.is_locked());
        assert!(!s.has_waker());
        assert_eq!(s.count(), 2);

        // WAKER_FLAG_LOCK -> dec
        assert_eq!(s.try_done(1, 0), None);
        assert!(s.is_locked());
        assert_eq!(s.count(), 1);

        // WAKER_FLAG_LOCK -> no waker
        let _s = s.0 & (!WAKER_FLAG_MASK);
        assert_eq!(_s, 1);

        // WAKER_FLAG_LOCK exist, don't drop, just dec
        assert_eq!(s.try_done(1, 0), None);
        assert_eq!(s.count(), 0);
    }

    #[test]
    fn test_waitgroup_inner() {
        let inner = WaitGroupInner::new();
        assert_eq!(inner.count(SeqCst), 1);
        assert_eq!(State::new(inner.state.load(Ordering::SeqCst)).waker_flag(), 0);

        println!("test try_set_waker met threshold reach");
        assert_eq!(inner.try_set_waker(ThinWaker::Blocking(thread::current()), 1, false), Err(()));

        inner.add();
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
        assert!(!WaitGroupInner::done(p, 1, 1));
        {
            let inner = unsafe { p.as_ref() };
            assert_eq!(inner.count(SeqCst), 1);
            let s = State::new(inner.state.load(Ordering::SeqCst));
            assert_eq!(s.waker_flag(), 0);
        }
        println!("test done triggering drop");
        assert!(WaitGroupInner::done(p, 1, 0));
    }
}
