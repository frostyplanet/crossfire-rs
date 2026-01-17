#[allow(unused_imports)]
use crate::collections::WeakCell;
#[allow(unused_imports)]
use crate::flavor::{Flavor, FlavorImpl, OneSpmc};
#[cfg(feature = "trace_log")]
use crate::tokio_task_id;
use crate::trace_log;
use crate::waker::*;
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc, Weak,
};
use std::task::{Context, Poll};

// pub(crate) on type alias does not matter, mpmc::List alias works because RegistryMulti is pub
pub(crate) type RegistryMultiSend<T> = RegistryMulti<*const T>;
pub(crate) type RegistryMultiRecv = RegistryMulti<()>;

pub(crate) trait Registry: Send + 'static {
    type Waker: Send + Unpin + 'static + Debug;

    fn get_waker_state(&self, o_waker: &Option<Self::Waker>, order: Ordering) -> u8;

    #[inline(always)]
    fn clear_wakers(&self, _waker: &Self::Waker) {}

    fn close(&self);

    #[inline(always)]
    fn len(&self) -> usize {
        0
    }

    #[inline(always)]
    fn commit_waiting(&self, _o_waker: &Option<Self::Waker>) -> u8 {
        WakerState::Init as u8
    }

    #[inline(always)]
    fn cancel_waker(&self, o_waker: &mut Option<Self::Waker>) {
        let _ = o_waker.take();
    }

    #[inline(always)]
    fn abandon_waker(&self, _waker: &Self::Waker) -> Result<(), u8> {
        Ok(())
    }
}

pub(crate) trait RegistrySend<T: Send + 'static>: Registry {
    fn new() -> Self;

    #[inline(always)]
    fn use_direct_copy(&self) -> bool {
        false
    }

    #[inline(always)]
    fn reg_waker_blocking(
        &self, _o_waker: &mut Option<<Self as Registry>::Waker>, _cache: &WakerCache<*const T>,
        _payload: *const T,
    ) {
        unreachable!();
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, _ctx: &mut Context, _o_waker: &mut Option<<Self as Registry>::Waker>,
    ) -> Option<Poll<()>> {
        unreachable!();
    }

    /// remove outdated waker, make sure it does not accumulate.
    ///
    /// It's ok to set state with Relaxed here, two scenario:
    /// * set Done while the state is Init, does not matter other thread see it or not.
    /// * other thread might have wake it in the process, but we are dropping it anyway, and then
    /// reg_waker with a new one.
    #[inline(always)]
    fn cancel_reuse_waker(
        &self, o_waker: &mut Option<<Self as Registry>::Waker>, state: WakerState,
    ) -> u8 {
        let _ = o_waker.take();
        state as u8
    }

    #[inline(always)]
    fn fire<F>(&self, _flavor: &F) -> WakeResult
    where
        F: FlavorImpl<Item = T>,
    {
        WakeResult::Next
    }

    #[inline(always)]
    fn cache_waker(
        &self, _o_waker: Option<<Self as Registry>::Waker>, _cache: &WakerCache<*const T>,
    ) {
    }
}

pub(crate) trait RegistryRecv: Registry {
    fn new() -> Self;

    #[inline(always)]
    fn fire(&self) {}

    #[inline(always)]
    fn reg_waker_blocking(
        &self, _o_waker: &mut Option<<Self as Registry>::Waker>, _cache: &WakerCache<()>,
    ) {
        unreachable!();
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, _ctx: &mut Context, _o_waker: &mut Option<<Self as Registry>::Waker>,
    ) -> Option<Poll<()>> {
        unreachable!();
    }

    #[inline(always)]
    fn cache_waker(&self, _o_waker: Option<<Self as Registry>::Waker>, _cache: &WakerCache<()>) {}

    fn reg_select_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool;

    #[inline(always)]
    fn cancel_select_waker(&self, _waker: &Arc<SelectWaker>) {}
}

#[derive(Debug)]
pub struct RegistryDummy();

impl Registry for RegistryDummy {
    type Waker = ();

    #[inline(always)]
    fn get_waker_state(&self, _o_waker: &Option<Self::Waker>, _order: Ordering) -> u8 {
        unreachable!();
    }

    #[inline(always)]
    fn close(&self) {}
}

impl<T: Send + 'static> RegistrySend<T> for RegistryDummy {
    #[inline(always)]
    fn new() -> Self {
        Self()
    }
}

type SingleWaker = ArcWaker<()>;
//type SingleWaker = ThinWaker;

pub struct RegistrySingle {
    cell: WeakCell<WakerInner<()>>,
    // OneSpmc has comparable speed as WeakCell and does not allocate on waker registration,
    // but since miri will report datarace issue, commented out for now.
    //cell: OneSpmc<ThinWaker>,
    _tag: &'static str,
}

impl RegistrySingle {
    #[inline(always)]
    fn _fire(&self) {
        if let Some(waker) = self.cell.pop() {
            waker.wake();
            trace_log!("{} wake", self._tag);
        }
    }

    #[inline(always)]
    fn _reg_waker_async(&self, ctx: &mut Context, o_waker: &mut Option<SingleWaker>) {
        // XXX don't know what the waker was, always generate new
        let waker = ArcWaker::<()>::new_async(ctx, ());
        //let waker = ThinWaker::Async(ctx.waker().clone());
        trace_log!("{}{:?}: reg {:?}", self._tag, tokio_task_id!(), waker);
        self.cell.replace(waker.weak());
        o_waker.replace(waker);
        //self.cell.replace(waker);
        // should store into o_waker, AsyncTx need to drop item when SendFuture drop
    }

    #[inline(always)]
    fn _reg_waker_blocking(&self, o_waker: &mut Option<SingleWaker>) {
        let waker = ArcWaker::<()>::new_blocking(());
        //        let waker = ThinWaker::Blocking(thread::current());
        trace_log!("{}{:?}: reg {:?}", self._tag, tokio_task_id!(), waker);
        self.cell.replace(waker.weak());
        o_waker.replace(waker);
        //self.cell.replace(waker);
    }
}

impl Registry for RegistrySingle {
    type Waker = SingleWaker;

    #[inline(always)]
    fn get_waker_state(&self, _o_waker: &Option<SingleWaker>, _order: Ordering) -> u8 {
        if self.cell.is_empty() {
            WakerState::Woken as u8
        } else {
            WakerState::Init as u8
        }
    }

    #[inline(always)]
    fn close(&self) {
        self._fire();
    }
}

impl<T: Send + 'static> RegistrySend<T> for RegistrySingle {
    #[inline(always)]
    fn new() -> Self {
        //Self { cell: _OneSpmc::new(), _tag: "tx" }
        Self { cell: WeakCell::new(), _tag: "tx" }
    }

    #[inline(always)]
    fn fire<F>(&self, _flavor: &F) -> WakeResult
    where
        F: FlavorImpl<Item = T>,
    {
        self._fire();
        return WakeResult::Next;
    }

    #[inline(always)]
    fn reg_waker_blocking(
        &self, o_waker: &mut Option<SingleWaker>, _cache: &WakerCache<*const T>, _payload: *const T,
    ) {
        self._reg_waker_blocking(o_waker);
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<SingleWaker>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker);
        None
    }
}

impl RegistryRecv for RegistrySingle {
    #[inline(always)]
    fn new() -> Self {
        //Self { cell: OneSpmc::new(), _tag: "rx" }
        Self { cell: WeakCell::new(), _tag: "rx" }
    }

    #[inline(always)]
    fn fire(&self) {
        self._fire();
    }

    #[inline(always)]
    fn reg_waker_blocking(&self, o_waker: &mut Option<SingleWaker>, _cache: &WakerCache<()>) {
        self._reg_waker_blocking(o_waker)
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<SingleWaker>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker);
        None
    }

    #[inline(always)]
    fn reg_select_waker(&self, _channel_id: usize, waker: &Arc<SelectWaker>) -> bool {
        trace_log!("{}: reg for select", self._tag);
        self.cell.replace(waker.clone_weak());
        false
    }
}

struct RegistryMultiInner<P> {
    queue: VecDeque<Weak<WakerInner<P>>>,
    selectors: Vec<SelectWakerWrapper>,
    seq: u32,
}

impl<P> RegistryMultiInner<P> {
    #[inline(always)]
    fn new() -> Self {
        Self { queue: VecDeque::with_capacity(32), selectors: Vec::with_capacity(32), seq: 0 }
    }

    // it's better to use non-atomic than fetch_XXX
    #[inline(always)]
    fn check_select(&self) -> u8 {
        if self.selectors.is_empty() {
            0
        } else {
            MULTI_HAS_SELECT
        }
    }

    // it's better to use non-atomic than fetch_XXX
    #[inline(always)]
    fn check_waker(&self) -> u8 {
        if self.queue.is_empty() {
            0
        } else {
            MULTI_HAS_WAKER
        }
    }
}

const MULTI_EMPTY: u8 = 0;
const MULTI_HAS_SELECT: u8 = 1;
const MULTI_HAS_WAKER: u8 = 2;

pub struct RegistryMulti<P> {
    state: AtomicU8,
    inner: Mutex<RegistryMultiInner<P>>,
    _tag: &'static str,
}

impl<P: Copy> RegistryMulti<P> {
    #[inline(always)]
    fn reg_waker(&self, waker: &ArcWaker<P>) {
        let weak = waker.weak();
        {
            let mut guard = self.inner.lock();
            let seq = guard.seq.wrapping_add(1);
            guard.seq = seq;
            waker.set_seq(seq);
            if guard.queue.is_empty() {
                self.state.store(guard.check_select() | MULTI_HAS_WAKER, Ordering::SeqCst);
            }
            guard.queue.push_back(weak);
        }
    }

    #[inline(always)]
    fn _reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<ArcWaker<P>>, payload: P,
    ) -> Option<Poll<()>> {
        if let Some(waker) = o_waker.as_ref() {
            match waker.try_change_state(WakerState::Woken, WakerState::Init) {
                Ok(_) => {
                    if waker.will_wake(ctx) {
                        self.reg_waker(&waker);
                        return None;
                    }
                }
                Err(state) => {
                    if state < WakerState::Woken as u8 {
                        if waker.will_wake(ctx) {
                            trace_log!(
                                "{} {:?}: will_wake {:?}",
                                self._tag,
                                tokio_task_id!(),
                                waker
                            );
                            // Normally only selection or multiplex future will get here.
                            // No need to reg again, since waker is not consumed.
                            return Some(Poll::Pending);
                        } else {
                            // Spurious woken by runtime, waker can not be re-used (issue 38)
                            // If we se Woken here, only possible otherside has woken it
                            if waker.get_state_relaxed() < WakerState::Woken as u8 {
                                self._clear_wakers(waker, true);
                            }
                            trace_log!(
                                "{} {:?}: drop waker {:?}",
                                self._tag,
                                tokio_task_id!(),
                                waker
                            );
                        }
                    } else if state == WakerState::Closed as u8 {
                        return Some(Poll::Ready(()));
                    } else {
                        panic!("state: impossible for async {:?}", state);
                    }
                }
            }
        }
        let waker = ArcWaker::<P>::new_async(ctx, payload);
        self.reg_waker(&waker);
        o_waker.replace(waker);
        return None;
    }

    #[inline(always)]
    fn _reg_waker_blocking(
        &self, o_waker: &mut Option<ArcWaker<P>>, _cache: &WakerCache<P>, payload: P,
    ) {
        if let Some(waker) = o_waker.as_ref() {
            waker.reset_init();
            self.reg_waker(&waker);
            trace_log!("{}{:?}: re-reg {:?}", self._tag, tokio_task_id!(), waker);
        } else {
            debug_assert!(o_waker.is_none());
            //let waker = cache.new_blocking(payload);
            let waker = ArcWaker::<P>::new_blocking(payload);
            self.reg_waker(&waker);
            trace_log!("{}{:?}: reg {:?}", self._tag, tokio_task_id!(), waker);
            o_waker.replace(waker);
        }
    }

    /// If trigger all selector while not empty.
    /// return Some((waker, again))
    /// if there's more waker after pop_first, again=true
    #[inline(always)]
    fn pop_first(&self) -> Option<(ArcWaker<P>, Option<u32>)> {
        // This is a snapshot, it's safe to ignore the new situation after acquire lock
        let flag = self.state.load(Ordering::SeqCst);
        if flag == MULTI_EMPTY {
            return None;
        }
        {
            let mut guard = self.inner.lock();
            if flag & MULTI_HAS_SELECT > 0 {
                for select in &guard.selectors {
                    select.wake();
                }
            }
            if flag & MULTI_HAS_WAKER > 0 {
                let mut has_pop = false;
                loop {
                    if let Some(weak) = guard.queue.pop_front() {
                        has_pop = true;
                        if let Some(inner) = weak.upgrade() {
                            if guard.queue.is_empty() {
                                self.state.store(guard.check_select(), Ordering::SeqCst);
                                return Some((ArcWaker::from_arc(inner), None));
                            } else {
                                return Some((ArcWaker::from_arc(inner), Some(guard.seq)));
                            }
                        }
                    } else {
                        if has_pop {
                            // might upgrade encounter weak previous loop
                            self.state.store(guard.check_select(), Ordering::SeqCst);
                        }
                        return None;
                    }
                }
            }
            // nothing changed, don't need to touch the state
            return None;
        }
    }

    /// ignore the selectors (since triggered in pop_first())
    /// return the flags
    #[inline(always)]
    fn pop_again(&self) -> Option<ArcWaker<P>> {
        // This is a snapshot, it's safe to ignore the new situation after acquire lock
        let flag = self.state.load(Ordering::Acquire);
        if flag == MULTI_EMPTY {
            return None;
        }
        {
            let mut guard = self.inner.lock();
            let mut has_pop = false;
            loop {
                if let Some(weak) = guard.queue.pop_front() {
                    has_pop = true;
                    if let Some(inner) = weak.upgrade() {
                        if guard.queue.is_empty() {
                            self.state.store(guard.check_select(), Ordering::SeqCst);
                        }
                        return Some(ArcWaker::from_arc(inner));
                    }
                } else {
                    if has_pop {
                        // might upgrade encounter weak previous loop
                        self.state.store(guard.check_select(), Ordering::SeqCst);
                    }
                    return None;
                }
            }
        }
    }

    /// Call when waker is cancelled
    #[inline(always)]
    fn _clear_wakers(&self, old_waker: &ArcWaker<P>, oneshot: bool) {
        // Don't need accurate, it's optional
        if self.state.load(Ordering::Acquire) & MULTI_HAS_WAKER == 0 {
            return;
        }
        let old_seq = old_waker.get_seq();
        // the macro yield true to stop, false to continue
        macro_rules! process {
            ($guard: expr, $weak: expr) => {{
                if let Some(waker) = $weak.upgrade() {
                    let _seq = waker.get_seq();
                    if _seq == old_seq {
                        trace_log!("{}: clear {:?} hit", self._tag, waker);
                        // XXX, it's possible to reuse the waker, leave it for future review
                        true
                    } else if _seq > old_seq {
                        $guard.queue.push_front($weak);
                        true
                    } else {
                        // There might be later waker cancel due to success sending before commit_waiting.
                        // While earlier waker is still waiting.
                        let state = waker.get_state();
                        if state < WakerState::Woken as u8 {
                            $guard.queue.push_front($weak);
                            true
                        } else {
                            if oneshot {
                                trace_log!("{}: cancel {:?} one {}", self._tag, waker, old_seq);
                                true
                            } else {
                                trace_log!("{}: cancel {:?}<{}", self._tag, waker, old_seq);
                                false
                            }
                        }
                    }
                } else {
                    false
                }
            }};
        }
        let mut guard = self.inner.lock();
        if let Some(weak) = guard.queue.pop_front() {
            if process!(guard, weak) {
                if guard.queue.is_empty() {
                    self.state.store(guard.check_select(), Ordering::SeqCst);
                }
                return;
            }
            loop {
                if let Some(_weak) = guard.queue.pop_front() {
                    if process!(guard, _weak) {
                        if guard.queue.is_empty() {
                            self.state.store(guard.check_select(), Ordering::SeqCst);
                        }
                        return;
                    }
                } else {
                    // might upgrade encounter weak previous loop
                    self.state.store(guard.check_select(), Ordering::SeqCst);
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn _cache_waker(_o_waker: Option<ArcWaker<P>>, _cache: &WakerCache<P>) {
        // XXX: skip cache for now, until we find out miri report of race
        //if let Some(waker) = o_waker {
        //    if waker.get_state() >= WakerState::Woken as u8 {
        //        cache.push(waker);
        //    }
        //}
    }
}

impl<P: 'static + Copy> Registry for RegistryMulti<P> {
    type Waker = ArcWaker<P>;

    #[inline(always)]
    fn get_waker_state(&self, o_waker: &Option<ArcWaker<P>>, order: Ordering) -> u8 {
        if let Some(waker) = o_waker {
            waker._get_state(order)
        } else {
            unreachable!();
        }
    }

    /// Cancel outdated wakers until me, make sure it does not accumulate
    #[inline(always)]
    fn clear_wakers(&self, waker: &ArcWaker<P>) {
        self._clear_wakers(&waker, false);
    }

    #[inline(always)]
    fn close(&self) {
        let mut guard = self.inner.lock();
        for selector in &guard.selectors {
            selector.wake();
        }
        while let Some(weak) = guard.queue.pop_front() {
            if let Some(waker) = weak.upgrade() {
                let _r = waker.close_wake();
                trace_log!("close {} wake {:?} {}", self._tag, waker, _r);
            }
        }
        self.state.store(0, Ordering::SeqCst);
    }

    /// return waker queue size
    #[inline]
    fn len(&self) -> usize {
        let guard = self.inner.lock();
        guard.queue.len()
    }

    #[inline(always)]
    fn commit_waiting(&self, o_waker: &Option<ArcWaker<P>>) -> u8 {
        if let Some(waker) = &o_waker {
            return waker.commit_waiting();
        } else {
            unreachable!();
        }
    }

    /// return false when waker is none
    #[inline(always)]
    fn abandon_waker(&self, waker: &ArcWaker<P>) -> Result<(), u8> {
        // which change Waiting/Init to Closed
        match waker.abandon() {
            Ok(()) => {
                trace_log!("{}: abandon cancel {:?}", self._tag, waker);
                self.clear_wakers(&waker);
                Ok(())
            }
            Err(state) => {
                return Err(state);
            }
        }
    }

    /// cancel one outdated waker, make sure it does not accumulate
    #[inline(always)]
    fn cancel_waker(&self, o_waker: &mut Option<ArcWaker<P>>) {
        if let Some(waker) = o_waker.take() {
            // If we se Woken here, only possible otherside has woken it
            if waker.get_state_relaxed() >= WakerState::Woken as u8 {
                return;
            }
            self._clear_wakers(&waker, true);
        }
    }
}

impl<T: Send + Unpin + 'static> RegistrySend<T> for RegistryMultiSend<T> {
    #[inline(always)]
    fn new() -> Self {
        Self { inner: Mutex::new(RegistryMultiInner::new()), state: AtomicU8::new(0), _tag: "tx" }
    }

    #[inline(always)]
    fn use_direct_copy(&self) -> bool {
        self.state.load(Ordering::Relaxed) != MULTI_EMPTY
    }

    #[inline(always)]
    fn reg_waker_blocking(
        &self, o_waker: &mut Option<ArcWaker<*const T>>, cache: &WakerCache<*const T>,
        payload: *const T,
    ) {
        self._reg_waker_blocking(o_waker, cache, payload)
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<ArcWaker<*const T>>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker, std::ptr::null_mut())
    }

    /// remove outdated waker, make sure it does not accumulate.
    ///
    /// It's ok to set state with Relaxed here, two scenario:
    /// * set Done while the state is Init, does not matter other thread see it or not.
    /// * other thread might have wake it in the process, but we are dropping it anyway, and then
    /// reg_waker with a new one.
    #[inline(always)]
    fn cancel_reuse_waker(
        &self, o_waker: &mut Option<ArcWaker<*const T>>, state: WakerState,
    ) -> u8 {
        if let Some(waker) = o_waker.as_ref() {
            let cur_state = waker.get_state();
            // If we se Woken here, only possible otherside has woken it
            if cur_state >= WakerState::Woken as u8 {
                trace_log!("{}: cancel_reuse {:?} {}", self._tag, waker, cur_state);
                if cur_state < state as u8 {
                    return state as u8;
                } else {
                    return cur_state;
                }
            } else {
                self._clear_wakers(&waker, true);
                let _ = o_waker.take();
                return state as u8;
            }
        } else {
            unreachable!();
        }
    }

    #[inline(always)]
    fn fire<F>(&self, flavor: &F) -> WakeResult
    where
        F: FlavorImpl<Item = T>,
    {
        if let Some((waker, _last_seq)) = self.pop_first() {
            let r = waker.wake_or_copy::<F>(flavor);
            trace_log!("wake {} {:?} {:?}", self._tag, waker, r);
            if r.is_done() {
                return r;
            }
            drop(waker);
            if let Some(mut last_seq) = _last_seq {
                last_seq = last_seq.wrapping_sub(1);
                while let Some(_waker) = self.pop_again() {
                    let r = _waker.wake_or_copy::<F>(flavor);
                    trace_log!("wake {} {:?} {:?}", self._tag, _waker, r);
                    if r.is_done() {
                        return r;
                    }
                    // The latest seq in RegistryMulti is always last_waker.get_seq() +1
                    // Because some waker (issued by sink / stream) might be INIT all the time,
                    // prevent to dead loop situation when they are wake up and re-register again.
                    if _waker.get_seq() >= last_seq {
                        trace_log!("wake {} stop at {}", self._tag, last_seq);
                        return WakeResult::Next;
                    }
                }
            }
        }
        WakeResult::Next
    }

    #[inline(always)]
    fn cache_waker(&self, o_waker: Option<ArcWaker<*const T>>, cache: &WakerCache<*const T>) {
        Self::_cache_waker(o_waker, cache);
    }
}

impl RegistryRecv for RegistryMultiRecv {
    #[inline(always)]
    fn new() -> Self {
        Self { inner: Mutex::new(RegistryMultiInner::new()), state: AtomicU8::new(0), _tag: "rx" }
    }

    #[inline(always)]
    fn reg_waker_blocking(&self, o_waker: &mut Option<ArcWaker<()>>, cache: &WakerCache<()>) {
        self._reg_waker_blocking(o_waker, cache, ())
    }

    #[inline(always)]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<ArcWaker<()>>,
    ) -> Option<Poll<()>> {
        self._reg_waker_async(ctx, o_waker, ())
    }

    #[inline(always)]
    fn fire(&self) {
        if let Some((waker, _last_seq)) = self.pop_first() {
            let r = waker.wake();
            trace_log!("wake {} {:?} {:?}", self._tag, waker, r);
            if r.is_done() {
                return;
            }
            drop(waker);
            if let Some(mut last_seq) = _last_seq {
                last_seq = last_seq.wrapping_sub(1);
                while let Some(_waker) = self.pop_again() {
                    let r = _waker.wake();
                    trace_log!("wake {} {:?} {:?}", self._tag, _waker, r);
                    if r.is_done() {
                        return;
                    }
                    // The latest seq in RegistryMulti is always last_waker.get_seq() +1
                    // Because some waker (issued by sink / stream) might be INIT all the time,
                    // prevent to dead loop situation when they are wake up and re-register again.
                    if _waker.get_seq() >= last_seq {
                        trace_log!("wake {} stop at {}", self._tag, last_seq);
                        return;
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn cache_waker(&self, o_waker: Option<ArcWaker<()>>, cache: &WakerCache<()>) {
        Self::_cache_waker(o_waker, cache);
    }

    #[inline(always)]
    fn reg_select_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool {
        trace_log!("{}: reg for select", self._tag);
        let mut guard = self.inner.lock();
        if guard.selectors.is_empty() {
            self.state.store(guard.check_waker() | MULTI_HAS_SELECT, Ordering::SeqCst);
        }
        guard.selectors.push(SelectWaker::to_wrapper(waker.clone(), channel_id));
        true
    }

    #[inline(always)]
    fn cancel_select_waker(&self, waker: &Arc<SelectWaker>) {
        let mut guard = self.inner.lock();
        if let Some((i, _)) = guard.selectors.iter().enumerate().find(|&(_, entry)| entry.eq(waker))
        {
            guard.selectors.remove(i);
        }
        if guard.selectors.is_empty() {
            self.state.store(guard.check_waker(), Ordering::SeqCst);
        }
    }
}

// Due to it's type alias in crate::select::Mux, should be pub
pub struct SelectWakerWrapper(Arc<SelectWaker>, usize);

impl SelectWakerWrapper {
    #[inline(always)]
    pub(crate) fn wake(&self) {
        if let Some(waker) = self.0.cell.pop() {
            trace_log!("rx: wake select");
            self.0.hint.store(self.1, Ordering::Release);
            waker.wake();
        }
    }

    #[inline(always)]
    pub(crate) fn eq(&self, waker: &Arc<SelectWaker>) -> bool {
        Arc::ptr_eq(&self.0, waker)
    }
}

// For multiplex
impl Registry for SelectWakerWrapper {
    type Waker = ArcWaker<()>;

    #[inline(always)]
    fn get_waker_state(&self, _o_waker: &Option<ArcWaker<()>>, _order: Ordering) -> u8 {
        unreachable!();
    }

    #[inline(always)]
    fn close(&self) {
        // decrease the opened_channels count to hint Multiplex
        self.0.close();
        self.wake();
    }
}

// For multiplex
impl RegistryRecv for SelectWakerWrapper {
    fn new() -> Self {
        unreachable!();
    }

    #[inline(always)]
    fn fire(&self) {
        self.wake();
    }

    fn reg_select_waker(&self, _channel_id: usize, _waker: &Arc<SelectWaker>) -> bool {
        unreachable!();
    }
}

pub(crate) struct SelectWaker {
    cell: WeakCell<WakerInner<()>>,
    // does not need to be correct, just a hint for the try_select
    hint: AtomicUsize,
    o_waker: UnsafeCell<Option<ArcWaker<()>>>,
    // For multiplex, not for select
    opened_channels: AtomicUsize,
}

unsafe impl Send for SelectWaker {}
unsafe impl Sync for SelectWaker {}

impl SelectWaker {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            cell: WeakCell::new(),
            hint: AtomicUsize::new(0),
            o_waker: UnsafeCell::new(None),
            opened_channels: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn init_blocking(&self) {
        let weak = if let Some(waker) = self.get_waker().as_ref() {
            waker.reset_init();
            waker.weak()
        } else {
            let waker = ArcWaker::new_blocking(());
            let weak = waker.weak();
            self.get_waker().replace(waker);
            weak
        };
        self.cell.replace(weak);
        self.hint.store(0, Ordering::Release)
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn init_async(&self, ctx: &mut Context) {
        let waker = ArcWaker::new_async(ctx, ());
        let weak = waker.weak();
        self.get_waker().replace(waker);
        self.cell.replace(weak);
        self.hint.store(0, Ordering::Release)
    }

    #[inline(always)]
    fn get_waker(&self) -> &mut Option<ArcWaker<()>> {
        unsafe { std::mem::transmute(self.o_waker.get()) }
    }

    #[inline(always)]
    fn clone_weak(&self) -> Weak<WakerInner<()>> {
        self.get_waker().as_ref().unwrap().weak()
    }

    #[inline(always)]
    pub fn add_opened(&self) {
        self.opened_channels.fetch_add(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn get_opened_count(&self) -> usize {
        self.opened_channels.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn to_wrapper(self: Arc<SelectWaker>, idx: usize) -> SelectWakerWrapper {
        SelectWakerWrapper(self, idx)
    }

    #[inline(always)]
    pub fn get_hint(&self) -> usize {
        self.hint.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn close(&self) {
        self.opened_channels.fetch_sub(1, Ordering::SeqCst);
    }

    #[inline(always)]
    pub fn get_waker_state(&self, order: Ordering) -> u8 {
        self.get_waker().as_ref().unwrap()._get_state(order)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::waker::ArcWaker;

    #[test]
    fn print_waker_registry_size() {
        use std::mem::size_of;
        println!("RegistryMultiSend<usize> size {}", size_of::<RegistryMultiSend<usize>>());
        println!("RegistryMultiRecv size {}", size_of::<RegistryMultiRecv>());
        println!("RegistrySingle size {}", size_of::<RegistrySingle>());
        println!("RegistryMulti<()> size {}", size_of::<RegistryMultiRecv>());
    }

    #[test]
    fn test_registry_multi_pop() {
        let reg = RegistryMultiRecv::new();

        // test push
        let waker1 = ArcWaker::new_blocking(());
        assert_eq!(reg.len(), 0);
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        assert_eq!(waker1.get_seq(), 1);
        assert_eq!(reg.len(), 1);

        let waker2 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker2);
        waker2.commit_waiting();
        assert_eq!(waker2.get_seq(), 2);
        assert_eq!(reg.len(), 2);
        assert_eq!(waker2.get_seq(), waker1.get_seq() + 1);
        assert_eq!(waker2.get_state(), WakerState::Waiting as u8);

        if let Some((w, seq)) = reg.pop_first() {
            assert!(w.wake() == WakeResult::Next);
            assert!(seq.is_some());
        }
        assert_eq!(waker1.get_state(), WakerState::Woken as u8);
        assert_eq!(reg.len(), 1);
        if let Some(w) = reg.pop_again() {
            assert!(w.wake() == WakeResult::Woken);
        }
        assert_eq!(waker2.get_state(), WakerState::Woken as u8);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_multi_clear_waiting() {
        let reg = RegistryMultiRecv::new();
        // test seq
        let waker3 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker3);
        waker3.commit_waiting();
        assert_eq!(waker3.get_state(), WakerState::Waiting as u8);
        let waker4 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker4); // Init
        assert_eq!(waker4.get_state(), WakerState::Init as u8);
        let num_workers = reg.len();
        // Because waker3 not woken up, waker4 is not clear
        reg.clear_wakers(&waker4);
        assert_eq!(reg.len(), num_workers);
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        let num_workers = reg.len();
        assert_eq!(reg.len(), num_workers);
    }

    #[test]
    fn test_registry_multi_clear_oneshot() {
        let reg = RegistryMultiRecv::new();
        // test seq
        let waker1 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        let waker2 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker2); // Init
        waker2.commit_waiting();
        assert_eq!(waker2.get_state(), WakerState::Waiting as u8);
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        let num_workers = reg.len();
        println!("clear waker2 oneshot seq {}", waker2.get_seq());
        reg.cancel_waker(&mut Some(waker2));
        assert_eq!(reg.len(), num_workers); // Only nothing happen.
        reg.cancel_waker(&mut Some(waker1));
        assert_eq!(reg.len(), num_workers - 1); // Only waker1 is removed.
    }

    #[test]
    fn test_registry_multi_clear() {
        let reg = RegistryMultiRecv::new();
        // test seq
        let waker1 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        let waker2 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker2); // Init
        drop(waker2); // waker4 is dropped, weak is left
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        let waker3 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker3);
        let _num_workers = reg.len(); // Keep for debugging context, though not used in assertion
        println!("clear waker3 seq={}", waker3.get_seq());
        reg.clear_wakers(&waker3); // nothing happen, because waker3 is there
        assert_eq!(reg.len(), 13);
        reg.clear_wakers(&waker1);
        assert_eq!(reg.len(), 12);
        reg.clear_wakers(&waker3);
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_multi_close() {
        let reg = RegistryMultiRecv::new();
        println!("test close");
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        assert!(reg.len() > 0);
        reg.close();
        assert_eq!(reg.len(), 0);
    }
}
