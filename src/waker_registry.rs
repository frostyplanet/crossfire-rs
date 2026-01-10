#[allow(unused_imports)]
use crate::collections::WeakCell;
#[allow(unused_imports)]
use crate::flavor::{Flavor, FlavorImpl, OneSpmc};
use crate::select::{SelectWaker, SelectWakerMulti};
use crate::shared::ChannelShared;
#[cfg(feature = "trace_log")]
use crate::tokio_task_id;
use crate::trace_log;
use crate::waker::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Weak,
};
use std::task::{Context, Poll};

pub(crate) type RegistryMultiSend<T> = RegistryMulti<*const T>;
pub(crate) type RegistryMultiRecv = RegistryMulti<()>;

pub(crate) trait Registry: Send + 'static {
    type Waker: Send + Unpin + 'static + Debug;

    fn get_waker_state(&self, o_waker: &Option<Self::Waker>, order: Ordering) -> u8;

    #[inline(always)]
    fn clear_wakers(&self, _waker: &Self::Waker) {}

    fn close(&self);

    fn len(&self) -> usize;

    #[inline(always)]
    fn commit_waiting(&self, _o_waker: &Option<Self::Waker>) -> u8 {
        WakerState::Init as u8
    }

    #[inline(always)]
    fn cancel_waker(&self, o_waker: &mut Option<Self::Waker>) {
        let _ = o_waker.take();
    }

    /// return false when waker is none
    fn abandon_waker(&self, waker: &Self::Waker) -> Result<bool, u8>;
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
    fn fire<F: Flavor>(&self, _shared: &ChannelShared<F>) -> WakeResult
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

    #[inline(always)]
    fn len(&self) -> usize {
        0
    }

    #[inline(always)]
    fn abandon_waker(&self, _waker: &Self::Waker) -> Result<bool, u8> {
        Ok(false)
    }
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

    #[inline(always)]
    fn len(&self) -> usize {
        0
    }

    #[inline(always)]
    fn abandon_waker(&self, _waker: &SingleWaker) -> Result<bool, u8> {
        Ok(true)
    }
}

impl<T: Send + 'static> RegistrySend<T> for RegistrySingle {
    #[inline(always)]
    fn new() -> Self {
        //Self { cell: _OneSpmc::new(), _tag: "tx" }
        Self { cell: WeakCell::new(), _tag: "tx" }
    }

    #[inline(always)]
    fn fire<F: Flavor>(&self, _shared: &ChannelShared<F>) -> WakeResult
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
        self.cell.replace(waker.clone_weak());
        false
    }
}

struct RegistryMultiInner<P> {
    queue: VecDeque<Weak<WakerInner<P>>>,
    selectors: Vec<SelectWakerMulti>,
    seq: u32,
}

impl<P> RegistryMultiInner<P> {
    #[inline(always)]
    fn new() -> Self {
        Self { queue: VecDeque::with_capacity(32), selectors: Vec::with_capacity(32), seq: 0 }
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
    fn is_empty(&self) -> bool {
        self.state.load(Ordering::Acquire) == MULTI_EMPTY
    }

    #[inline(always)]
    fn reg_waker(&self, waker: &ArcWaker<P>) {
        let weak = waker.weak();
        {
            let mut guard = self.inner.lock();
            let seq = guard.seq.wrapping_add(1);
            guard.seq = seq;
            waker.set_seq(seq);
            if guard.queue.is_empty() {
                self.state.fetch_or(MULTI_HAS_WAKER, Ordering::SeqCst);
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
        &self, o_waker: &mut Option<ArcWaker<P>>, cache: &WakerCache<P>, payload: P,
    ) {
        if let Some(waker) = o_waker.as_ref() {
            waker.reset_init();
            self.reg_waker(&waker);
            trace_log!("{}{:?}: re-reg {:?}", self._tag, tokio_task_id!(), waker);
        } else {
            debug_assert!(o_waker.is_none());
            let waker = cache.new_blocking(payload);
            self.reg_waker(&waker);
            trace_log!("{}{:?}: reg {:?}", self._tag, tokio_task_id!(), waker);
            o_waker.replace(waker);
        }
    }

    #[inline(always)]
    fn pop(&self) -> Option<(ArcWaker<P>, u32)> {
        if self.state.load(Ordering::SeqCst) == MULTI_EMPTY {
            return None;
        }
        let mut res = None;
        {
            let mut guard = self.inner.lock();
            for select in &guard.selectors {
                select.wake();
            }
            loop {
                if let Some(weak) = guard.queue.pop_front() {
                    if let Some(inner) = weak.upgrade() {
                        res = Some((ArcWaker::from_arc(inner), guard.seq));
                        if guard.queue.is_empty() {
                            self.state.fetch_xor(MULTI_HAS_WAKER, Ordering::SeqCst);
                        }
                        break;
                    }
                } else {
                    self.state.fetch_xor(MULTI_HAS_WAKER, Ordering::SeqCst);
                    break;
                }
            }
        }
        return res;
    }

    #[inline(always)]
    fn _fire<F>(&self, handle: F) -> WakeResult
    where
        F: Fn(&ArcWaker<P>) -> WakeResult,
    {
        if let Some((waker, mut last_seq)) = self.pop() {
            let r = handle(&waker);
            trace_log!("wake {} {:?} {:?}", self._tag, waker, r);
            if r.is_done() {
                return r;
            }
            last_seq = last_seq.wrapping_sub(1);
            while let Some((_waker, _)) = self.pop() {
                let r = handle(&_waker);
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
        WakeResult::Next
    }

    /// Call when waker is cancelled
    #[inline(always)]
    fn _clear_wakers(&self, old_waker: &ArcWaker<P>, oneshot: bool) {
        // Don't need acurate, it's optional
        if self.state.load(Ordering::Acquire) & MULTI_HAS_WAKER == 0 {
            trace_log!("{}: skip", self._tag);
            return;
        }
        trace_log!("{}: enter clear_wakers", self._tag);
        let old_seq = old_waker.get_seq();
        macro_rules! process {
            ($guard: expr, $weak: expr) => {{
                if let Some(waker) = $weak.upgrade() {
                    let _seq = waker.get_seq();
                    if _seq == old_seq {
                        trace_log!("{}: clear {:?} hit", self._tag, waker);
                        true
                    } else {
                        // There might be later waker cancel due to success sending before commit_waiting.
                        // While earlier waker is still waiting.
                        let state = waker.get_state();
                        if state == WakerState::Init as u8 {
                            let _ = waker.wake();
                            if oneshot {
                                trace_log!("{}: cancel {:?} one {}", self._tag, waker, old_seq);
                                true
                            } else if _seq > old_seq {
                                trace_log!("{}: cancel {:?}>{} ", self._tag, waker, old_seq);
                                true
                            } else {
                                trace_log!("{}: cancel {:?}<{}", self._tag, waker, old_seq);
                                false
                            }
                        } else if state == WakerState::Waiting as u8 {
                            $guard.queue.push_front($weak);
                            return;
                        } else {
                            false
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
                    self.state.fetch_xor(MULTI_HAS_WAKER, Ordering::SeqCst);
                }
                return;
            }
            loop {
                if let Some(_weak) = guard.queue.pop_front() {
                    if process!(guard, _weak) {
                        if guard.queue.is_empty() {
                            self.state.fetch_xor(MULTI_HAS_WAKER, Ordering::SeqCst);
                        }
                        return;
                    }
                } else {
                    self.state.fetch_xor(MULTI_HAS_WAKER, Ordering::SeqCst);
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn _cache_waker(o_waker: Option<ArcWaker<P>>, cache: &WakerCache<P>) {
        if let Some(waker) = o_waker {
            if waker.get_state() >= WakerState::Woken as u8 {
                cache.push(waker);
            }
        }
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
    fn abandon_waker(&self, waker: &ArcWaker<P>) -> Result<bool, u8> {
        // which change Waiting/Init to Closed
        match waker.abandon() {
            Ok(()) => {
                trace_log!("tx: abandon cancel {:?}", waker);
                self.clear_wakers(&waker);
                Ok(true)
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
            self._clear_wakers(&waker, true)
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
        !self.is_empty()
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
                trace_log!("tx: cancel_reuse {:?} {}", waker, cur_state);
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
    fn fire<F: Flavor>(&self, shared: &ChannelShared<F>) -> WakeResult
    where
        F: FlavorImpl<Item = T>,
    {
        return self._fire(|waker| shared.on_recv_try_send(waker));
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
        self._fire(|waker| waker.wake());
    }

    #[inline(always)]
    fn cache_waker(&self, o_waker: Option<ArcWaker<()>>, cache: &WakerCache<()>) {
        Self::_cache_waker(o_waker, cache);
    }

    #[inline(always)]
    fn reg_select_waker(&self, channel_id: usize, waker: &Arc<SelectWaker>) -> bool {
        let mut guard = self.inner.lock();
        guard.selectors.push(SelectWaker::to_multi_waker(waker.clone(), channel_id));
        self.state.fetch_or(MULTI_HAS_SELECT, Ordering::SeqCst);
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
            self.state.fetch_xor(MULTI_HAS_SELECT, Ordering::SeqCst);
        }
    }
}

/*
#[cfg(test)]
mod tests {

    use super::*;
    use crate::locked_waker::Waker;
    use crate::waker::ArcWaker;

    #[test]
    fn print_waker_registry_size() {
        use std::mem::size_of;
        println!("RegistrySend size {}", size_of::<RegistrySend<usize>>());
        println!("RegistryRecv size {}", size_of::<RegistryRecv>());
        println!("RegistrySingle size {}", size_of::<RegistrySingle<()>>());
        println!("RegistryMulti size {}", size_of::<RegistryMulti<()>>());
    }

    #[test]
    fn test_registry_multi_pop() {
        let reg = RegistryMulti::new();

        // test push
        let waker1 = ArcWaker::new_blocking(());
        assert_eq!(reg.is_empty(), true);
        reg.reg_waker(&waker1);
        assert_eq!(waker1.get_state(), WakerState::Init as u8);
        assert_eq!(waker1.get_seq(), 1);
        assert_eq!(reg.is_empty(), false);
        assert_eq!(reg.len(), 1);

        let waker2 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker2);
        waker2.commit_waiting();
        assert_eq!(waker2.get_seq(), 2);
        assert_eq!(reg.len(), 2);
        assert_eq!(waker2.get_seq(), waker1.get_seq() + 1);
        assert_eq!(waker2.get_state(), WakerState::Waiting as u8);

        if let Some((w, _)) = reg.pop() {
            assert!(w.wake() == WakeResult::Next);
        }
        assert_eq!(waker1.get_state(), WakerState::Woken as u8);
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.is_empty(), false);
        if let Some((w, _)) = reg.pop() {
            assert!(w.wake() == WakeResult::Woken);
        }
        assert_eq!(waker2.get_state(), WakerState::Woken as u8);
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.is_empty(), true);
    }

    #[test]
    fn test_registry_multi_clear_waiting() {
        let reg = RegistryMulti::new();
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
        reg.clear_wakers(&waker4, false, "rx");
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
        let reg = RegistryMulti::new();
        // test seq
        let waker3 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker3);
        assert_eq!(waker3.get_state(), WakerState::Init as u8);
        let waker4 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker4); // Init
        waker4.commit_waiting();
        assert_eq!(waker4.get_state(), WakerState::Waiting as u8);
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        let num_workers = reg.len();
        println!("clear waker4 oneshot seq {}", waker4.get_seq());
        reg.clear_wakers(&waker4, true, "rx"); // oneshot only clear waker3
        assert_eq!(reg.len(), num_workers - 1);
        assert!(waker3.get_state() >= WakerState::Woken as u8);
        assert_eq!(waker4.get_state(), WakerState::Waiting as u8);
    }

    #[test]
    fn test_registry_multi_clear() {
        let reg = RegistryMulti::new();
        // test seq
        let waker3 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker3);
        assert_eq!(waker3.get_state(), WakerState::Init as u8);
        let waker4 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker4); // Init
        drop(waker4); // waker4 is dropped, weak is left
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        let waker5 = ArcWaker::new_blocking(());
        reg.reg_waker(&waker5);
        println!("clear waker5 seq={}", waker5.get_seq());
        reg.clear_wakers(&waker5, false, "rx"); // clear waker4, waker5
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_registry_multi_close() {
        let reg = RegistryMulti::new();
        println!("test close");
        for _ in 0..10 {
            let _waker = ArcWaker::new_blocking(());
            reg.reg_waker(&_waker);
        }
        assert_eq!(reg.is_empty(), false);
        reg.close("rx");
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.is_empty(), true);
    }
}
*/
