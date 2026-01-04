use crate::flavor::{one_spmc::OneSizeSpmc, FlavorImpl};
#[cfg(feature = "trace_log")]
use crate::tokio_task_id;
use crate::trace_log;
use crate::waker::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Weak;
use std::task::{Context, Poll};
use std::thread;

pub enum RegistrySender<T> {
    Single(RegistrySingle),
    Multi(RegistryMulti<*const T>),
    Dummy,
}

impl<T: Send + 'static> RegistrySender<T> {
    #[inline(always)]
    pub fn new_single() -> Self {
        Self::Single(RegistrySingle::new())
    }

    #[inline(always)]
    pub fn new_multi() -> Self {
        Self::Multi(RegistryMulti::<*const T>::new())
    }

    #[inline(always)]
    pub fn use_direct_copy(&self) -> bool {
        if let Self::Multi(inner) = self {
            !inner.is_empty()
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn get_waker_state(&self, o_waker: &Option<SendWaker<T>>, order: Ordering) -> u8 {
        if let Some(WakerHanle::Multi(waker)) = o_waker {
            waker._get_state(order)
        } else {
            if let Self::Single(inner) = self {
                if inner.is_empty() {
                    WakerState::Woken as u8
                } else {
                    WakerState::Init as u8
                }
            } else {
                unreachable!();
            }
        }
    }

    #[inline(always)]
    pub fn cache_waker(&self, o_waker: Option<SendWaker<T>>, cache: &WakerCache<*const T>) {
        if let Some(WakerHanle::Multi(waker)) = o_waker {
            if waker.get_state() >= WakerState::Woken as u8 {
                cache.push(waker);
            }
        }
    }

    pub fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<SendWaker<T>>,
    ) -> Option<Poll<()>> {
        match self {
            Self::Multi(inner) => {
                return inner.reg_waker_async(ctx, o_waker, std::ptr::null_mut(), "tx");
            }
            Self::Single(inner) => {
                let waker = ThinWaker::Async(ctx.waker().clone());
                inner.reg_waker(waker);
                o_waker.replace(WakerHanle::Single);
                // should store into o_waker, AsyncTx need to drop item when SendFuture drop
                return None;
            }
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub fn reg_waker_blocking(
        &self, o_waker: &mut Option<SendWaker<T>>, cache: &WakerCache<*const T>, payload: *const T,
    ) {
        match self {
            Self::Multi(inner) => {
                return inner.reg_waker_blocking(o_waker, cache, payload, "tx");
            }
            Self::Single(inner) => {
                let waker = ThinWaker::Blocking(thread::current());
                trace_log!("tx{:?}: reg {:?}", tokio_task_id!(), waker);
                inner.reg_waker(waker);
            }
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    fn _clear_wakers(&self, waker: &ArcWaker<*const T>, oneshot: bool) {
        if let Self::Multi(inner) = self {
            inner.clear_wakers(waker, oneshot, "tx");
        } else {
            unreachable!();
        }
    }

    /// Cancel outdated wakers until me, make sure it does not accumulate
    #[inline(always)]
    pub fn clear_wakers(&self, waker: &ArcWaker<*const T>) {
        self._clear_wakers(waker, false);
    }

    ///// remove outdated waker, make sure it does not accumulate.
    /////
    ///// It's ok to set state with Relaxed here, two scenario:
    ///// * set Done while the state is Init, does not matter other thread see it or not.
    ///// * other thread might have wake it in the process, but we are dropping it anyway, and then
    ///// reg_waker with a new one.
    //#[inline(always)]
    //pub fn cancel_waker(&self, o_waker: &mut Option<SendWaker<T>>) {
    //    if let Some(WakerHanle::Multi(waker)) = o_waker.take() {
    //        // If we se Woken here, only possible otherside has woken it
    //        if waker.get_state_relaxed() >= WakerState::Woken as u8 {
    //            return;
    //        }
    //        let aker) = o_waker {
    //            self._clear_wakers(waker, true);
    //        }
    //    }

    /// remove outdated waker, make sure it does not accumulate.
    ///
    /// It's ok to set state with Relaxed here, two scenario:
    /// * set Done while the state is Init, does not matter other thread see it or not.
    /// * other thread might have wake it in the process, but we are dropping it anyway, and then
    /// reg_waker with a new one.
    #[inline(always)]
    pub fn cancel_reuse_waker(&self, o_waker: &mut Option<SendWaker<T>>, state: WakerState) -> u8 {
        if let Self::Multi(inner) = self {
            if let Some(WakerHanle::Multi(waker)) = o_waker.as_ref() {
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
                    inner.clear_wakers(&waker, true, "tx");
                    let _ = o_waker.take();
                    return state as u8;
                }
            } else {
                unreachable!();
            }
        }
        return state as u8;
    }

    #[inline(always)]
    pub fn fire_direct_copy<F: FlavorImpl<T>>(&self, flavor: &F) -> WakeResult {
        match self {
            Self::Multi(inner) => {
                return inner.fire(
                    |waker| {
                        waker.wake_or_copy(|p: *const T| -> u8 {
                            if let Some(true) = flavor.try_send_oneshot(p) {
                                WakerState::Done as u8
                            } else {
                                WakerState::Woken as u8
                            }
                        })
                    }
                    , "tx");
            }
            Self::Single(inner) => {
                inner.fire("tx");
            },
            _ => {},
        }
        return WakeResult::Next;
    }


    #[inline(always)]
    pub fn fire(&self) {
        match self {
            Self::Multi(inner) => {
                inner.fire(|waker| waker.wake(), "rx");
            }
            Self::Single(inner) => {
                inner.fire("tx");
            }
            _ => {},
        }
    }

    #[inline(always)]
    pub fn close(&self) {
        match self {
            Self::Single(inner) => inner.fire("tx"),
            Self::Multi(inner) => inner.close("tx"),
            _ => {}
        }
    }

    /// return waker queue size
    #[inline]
    pub fn len(&self) -> usize {
        if let Self::Multi(inner) = self {
            inner.len()
        } else {
            0
        }
    }

    #[inline(always)]
    pub fn commit_waiting(&self, o_waker: &Option<SendWaker<T>>) -> u8 {
        if let Some(WakerHanle::Multi(waker)) = &o_waker {
            return waker.commit_waiting();
        }
        return WakerState::Init as u8;
    }
}

pub enum RegistryRecv {
    Single(RegistrySingle),
    Multi(RegistryMulti<()>),
}

impl RegistryRecv {
    #[inline(always)]
    pub fn new_single() -> Self {
        Self::Single(RegistrySingle::new())
    }

    #[inline(always)]
    pub fn new_multi() -> Self {
        Self::Multi(RegistryMulti::<()>::new())
    }

    #[inline(always)]
    pub fn get_waker_state(&self, o_waker: &Option<RecvWaker>) -> u8 {
        if let Some(WakerHanle::Multi(waker)) = o_waker.as_ref() {
            waker.get_state()
        } else {
            if let Self::Single(inner) = self {
                if inner.is_empty() {
                    WakerState::Woken as u8
                } else {
                    WakerState::Init as u8
                }
            } else {
                unreachable!();
            }
        }
    }

    pub fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<RecvWaker>,
    ) -> Option<Poll<()>> {
        match self {
            Self::Multi(inner) => {
                return inner.reg_waker_async(ctx, o_waker, (), "rx");
            }
            Self::Single(inner) => {
                let waker = ThinWaker::Async(ctx.waker().clone());
                inner.reg_waker(waker);
                // XXX: should store into o_waker ?
                return None;
            }
        }
    }

    #[inline(always)]
    pub fn reg_waker_blocking(&self, o_waker: &mut Option<RecvWaker>, cache: &WakerCache<()>) {
        match self {
            Self::Multi(inner) => {
                return inner.reg_waker_blocking(o_waker, cache, (), "rx");
            }
            Self::Single(inner) => {
                let waker = ThinWaker::Blocking(thread::current());
                trace_log!("rx{:?}: reg {:?}", tokio_task_id!(), waker);
                inner.reg_waker(waker);
            }
        }
    }

    #[inline(always)]
    pub fn cache_waker(&self, o_waker: Option<RecvWaker>, cache: &WakerCache<()>) {
        if let Some(WakerHanle::Multi(waker)) = o_waker {
            if waker.get_state() >= WakerState::Woken as u8 {
                cache.push(waker);
            }
        }
    }

    #[inline(always)]
    pub fn fire(&self) {
        match self {
            Self::Multi(inner) => {
                inner.fire(|waker| waker.wake(), "rx");
            }
            Self::Single(inner) => {
                inner.fire("rx");
            }
        }
    }

    #[inline(always)]
    fn _clear_wakers(&self, waker: &ArcWaker<()>, oneshot: bool) {
        match self {
            Self::Multi(inner) => {
                inner.clear_wakers(waker, oneshot, "rx");
            }
            _ => {}
        }
    }

    /// cancel outdated wakers until me, make sure it does not accumulate
    #[inline(always)]
    pub fn clear_wakers(&self, waker: &ArcWaker<()>) {
        self._clear_wakers(waker, false)
    }

    /// cancel one outdated waker, make sure it does not accumulate
    #[inline(always)]
    pub fn cancel_waker(&self, o_waker: &mut Option<RecvWaker>) {
        if let Some(WakerHanle::Multi(waker)) = o_waker.take() {
            // If we se Woken here, only possible otherside has woken it
            if waker.get_state_relaxed() >= WakerState::Woken as u8 {
                return;
            }
            self._clear_wakers(&waker, true)
        }
    }

    #[inline(always)]
    pub fn close(&self) {
        match self {
            Self::Single(inner) => inner.fire("rx"),
            Self::Multi(inner) => inner.close("rx"),
        }
    }

    /// return waker queue size
    pub fn len(&self) -> usize {
        match self {
            Self::Single(_inner) => 0,
            Self::Multi(inner) => inner.len(),
        }
    }

    #[inline(always)]
    pub fn commit_waiting(&self, o_waker: &Option<RecvWaker>) -> u8 {
        if let Some(RecvWaker::Multi(waker)) = &o_waker {
            return waker.commit_waiting();
        }
        return WakerState::Init as u8;
    }
}

pub struct RegistrySingle {
    cell: OneSizeSpmc<ThinWaker>,
}

impl RegistrySingle {
    #[inline(always)]
    pub fn new() -> Self {
        Self { cell: OneSizeSpmc::new() }
    }

    /// return is_skip
    #[inline(always)]
    fn reg_waker(&self, waker: ThinWaker) {
        self.cell.replace(waker);
    }

    #[inline(always)]
    fn fire(&self, _tag: &str) {
        if let Some(waker) = self.cell.try_recv_final() {
            waker.wake();
            trace_log!("{} wake", _tag);
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.cell.is_empty()
    }
}

struct RegistryMultiInner<P> {
    queue: VecDeque<Weak<WakerInner<P>>>,
    seq: u32,
}

pub struct RegistryMulti<P> {
    is_empty: AtomicBool,
    inner: Mutex<RegistryMultiInner<P>>,
}

impl<P: Copy> RegistryMulti<P> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(RegistryMultiInner { queue: VecDeque::with_capacity(32), seq: 0 }),
            is_empty: AtomicBool::new(true),
        }
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.is_empty.load(Ordering::Acquire)
    }

    #[inline]
    pub fn reg_waker_blocking(
        &self, o_waker: &mut Option<WakerHanle<P>>, cache: &WakerCache<P>, payload: P, _tag: &str,
    ) {
        if let Some(WakerHanle::Multi(waker)) = o_waker.as_ref() {
            waker.reset_init();
            self.reg_waker(&waker);
            trace_log!("{}{:?}: reg {:?}", _tag, tokio_task_id!(), waker);
        } else {
            debug_assert!(o_waker.is_none());
            let waker = cache.new_blocking(payload);
            self.reg_waker(&waker);
            trace_log!("{}{:?}: reg {:?}", _tag, tokio_task_id!(), waker);
            o_waker.replace(WakerHanle::Multi(waker));
        }
    }

    #[inline]
    fn reg_waker_async(
        &self, ctx: &mut Context, o_waker: &mut Option<WakerHanle<P>>, null_value: P, _tag: &str,
    ) -> Option<Poll<()>> {
        if let Some(WakerHanle::Multi(waker)) = o_waker.as_ref() {
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
                            trace_log!("{} {:?}: will_wake {:?}", _tag, tokio_task_id!(), waker);
                            // Normally only selection or multiplex future will get here.
                            // No need to reg again, since waker is not consumed.
                            return Some(Poll::Pending);
                        } else {
                            // Spurious woken by runtime, waker can not be re-used (issue 38)
                            // If we se Woken here, only possible otherside has woken it
                            if waker.get_state_relaxed() < WakerState::Woken as u8 {
                                self.clear_wakers(waker, true, _tag);
                            }
                            trace_log!("{} {:?}: drop waker {:?}", _tag, tokio_task_id!(), waker);
                        }
                    } else if state == WakerState::Closed as u8 {
                        return Some(Poll::Ready(()));
                    }
                }
            }
        }
        let waker = ArcWaker::<P>::new_async(ctx, null_value);
        self.reg_waker(&waker);
        o_waker.replace(WakerHanle::Multi(waker));
        return None;
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
                self.is_empty.store(false, Ordering::SeqCst);
            }
            guard.queue.push_back(weak);
        }
    }

    #[inline(always)]
    fn pop(&self) -> Option<(ArcWaker<P>, u32)> {
        if self.is_empty.load(Ordering::SeqCst) {
            return None;
        }
        let mut res = None;
        {
            let mut guard = self.inner.lock();
            loop {
                if let Some(weak) = guard.queue.pop_front() {
                    if let Some(inner) = weak.upgrade() {
                        res = Some((ArcWaker::from_arc(inner), guard.seq));
                        if guard.queue.is_empty() {
                            self.is_empty.store(true, Ordering::SeqCst);
                        }
                        break;
                    }
                } else {
                    self.is_empty.store(true, Ordering::SeqCst);
                    break;
                }
            }
        }
        return res;
    }

    #[inline(always)]
    fn fire<F>(&self, handle: F, _tag: &str) -> WakeResult
    where
        F: Fn(&ArcWaker<P>) -> WakeResult,
    {
        if let Some((waker, mut last_seq)) = self.pop() {
            let r = handle(&waker);
            trace_log!("wake {} {:?} {:?}", _tag, waker, r);
            if r.is_done() {
                return r;
            }
            last_seq = last_seq.wrapping_sub(1);
            while let Some((_waker, _)) = self.pop() {
                let r = handle(&_waker);
                trace_log!("wake {} {:?} {:?}", _tag, _waker, r);
                if r.is_done() {
                    return r;
                }
                // The latest seq in RegistryMulti is always last_waker.get_seq() +1
                // Because some waker (issued by sink / stream) might be INIT all the time,
                // prevent to dead loop situation when they are wake up and re-register again.
                if _waker.get_seq() >= last_seq {
                    trace_log!("wake {} stop at {}", _tag, last_seq);
                    return WakeResult::Next;
                }
            }
        }
        WakeResult::Next
    }

    /// Call when waker is cancelled
    #[inline(always)]
    fn clear_wakers(&self, old_waker: &ArcWaker<P>, oneshot: bool, _tag: &str) {
        // Don't need acurate, it's optional
        if self.is_empty.load(Ordering::Acquire) {
            trace_log!("{}: skip", _tag);
            return;
        }
        trace_log!("{}: enter clear_wakers", _tag);
        let old_seq = old_waker.get_seq();
        macro_rules! process {
            ($guard: expr, $weak: expr) => {{
                if let Some(waker) = $weak.upgrade() {
                    let _seq = waker.get_seq();
                    if _seq == old_seq {
                        trace_log!("{}: clear {:?} hit", _tag, waker);
                        true
                    } else {
                        // There might be later waker cancel due to success sending before commit_waiting.
                        // While earlier waker is still waiting.
                        let state = waker.get_state();
                        if state == WakerState::Init as u8 {
                            let _ = waker.wake();
                            if oneshot {
                                trace_log!("{}: cancel {:?} one {}", _tag, waker, old_seq);
                                true
                            } else if _seq > old_seq {
                                trace_log!("{}: cancel {:?}>{} ", _tag, waker, old_seq);
                                true
                            } else {
                                trace_log!("{}: cancel {:?}<{}", _tag, waker, old_seq);
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
                    self.is_empty.store(true, Ordering::SeqCst);
                }
                return;
            }
            loop {
                if let Some(_weak) = guard.queue.pop_front() {
                    if process!(guard, _weak) {
                        if guard.queue.is_empty() {
                            self.is_empty.store(true, Ordering::SeqCst);
                        }
                        return;
                    }
                } else {
                    self.is_empty.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn close(&self, _tag: &str) {
        let mut guard = self.inner.lock();
        while let Some(weak) = guard.queue.pop_front() {
            if let Some(waker) = weak.upgrade() {
                let _r = waker.close_wake();
                trace_log!("close {} wake {:?} {}", _tag, waker, _r);
            }
        }
        self.is_empty.store(true, Ordering::SeqCst);
    }

    /// return waker queue size
    #[inline(always)]
    fn len(&self) -> usize {
        let guard = self.inner.lock();
        guard.queue.len()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::waker::ArcWaker;

    #[test]
    fn print_waker_registry_size() {
        use std::mem::size_of;
        println!("RegistrySender size {}", size_of::<RegistrySender<usize>>());
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
