//use crate::collections::ArcCell;
use embed_collections::{
    dlist::{DLinkedList, DListItem, DListNode},
    Pointer,
};
use std::cell::UnsafeCell;
use std::fmt;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicU32, AtomicU8, Ordering},
    Arc, Weak,
};
use std::task::*;
use std::thread;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum WakerState {
    Init = 0, // A temporary state, https://github.com/frostyplanet/crossfire-rs/issues/22
    Waiting = 1,
    //Copy = 2, // Omit due to skipping direct copy on async or with deadline
    Woken = 3,
    Closed = 4, // Channel closed, or timeout cancellation
    Done = 5,
}

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum WakeResult {
    Woken = 0x1, // Woken, stop iteration
    Sent = 0x3,  // Woken with message direct copied
    Next = 0x2,  // Woken, but have to continued for more iteration
    Skip = 0x4,  // Waker Cancelled or Done
}

impl WakeResult {
    #[inline(always)]
    pub fn is_done(&self) -> bool {
        (*self as u8) & (WakeResult::Woken as u8) > 0
    }
}

pub struct ArcWaker(Arc<WakerItem>);

impl fmt::Debug for ArcWaker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Debug for WakerItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "waker({})", self.get_seq())
    }
}

impl Deref for ArcWaker {
    type Target = WakerItem;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl ArcWaker {
    #[inline(always)]
    pub fn new_async(ctx: &Context) -> Self {
        Self(Arc::new(WakerItem {
            seq: AtomicU32::new(0),
            state: AtomicU8::new(WakerState::Init as u8),
            waker: UnsafeCell::new(ThinWaker::Async(ctx.waker().clone())),
        }))
    }

    #[inline(always)]
    pub fn new_blocking() -> Self {
        Self(Arc::new(WakerItem {
            seq: AtomicU32::new(0),
            state: AtomicU8::new(WakerState::Init as u8),
            waker: UnsafeCell::new(ThinWaker::Blocking(thread::current())),
        }))
    }
}

impl ArcWaker {
    #[inline(always)]
    pub fn from_arc(inner: Arc<WakerItem>) -> Self {
        Self(inner)
    }

    #[allow(clippy::wrong_self_convention)]
    #[inline(always)]
    pub fn to_arc(self) -> Arc<WakerItem> {
        self.0
    }

    #[inline(always)]
    pub fn weak(&self) -> Weak<WakerItem> {
        Arc::downgrade(&self.0)
    }
}

#[derive(Debug)]
pub(crate) enum ThinWaker {
    Async(Waker),
    Blocking(thread::Thread),
}

impl ThinWaker {
    #[inline(always)]
    pub fn new_async(ctx: &Context) -> Self {
        Self::Async(ctx.waker().clone())
    }

    #[inline(always)]
    pub fn new_blocking() -> Self {
        Self::Blocking(thread::current())
    }

    #[inline(always)]
    pub fn wake_by_ref(&self) {
        match self {
            Self::Async(w) => w.wake_by_ref(),
            Self::Blocking(th) => th.unpark(),
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn wake(self) {
        match self {
            Self::Async(w) => w.wake(),
            Self::Blocking(th) => th.unpark(),
        }
    }

    #[inline(always)]
    pub fn will_wake(&self, ctx: &mut Context) -> bool {
        // ref: https://github.com/frostyplanet/crossfire-rs/issues/14
        // https://docs.rs/tokio/latest/tokio/runtime/index.html#:~:text=Normally%2C%20tasks%20are%20scheduled%20only,is%20called%20a%20spurious%20wakeup
        // There might be situation like spurious wakeup, poll() again under no waking up ever
        // happened, waker still exists in registry but cannot be used to wake the current future.
        if let Self::Async(_waker) = self {
            _waker.will_wake(ctx.waker())
        } else {
            unreachable!();
        }
    }
}

pub struct WakerList(DLinkedList<NonNull<WakerSeg>, ()>);

impl WakerList {
    fn push(&mut self, waker: ThinWaker) -> WakerSegRef {
        if let Some(seg_p) = self.0.get_front() {
            let seg = unsafe { seg_p.as_mut() };
            if !seg.is_full() {
                return seg.push(WakerItem::new(seq, waker));
            }
        }
        let mut seg = WakerSeg::new();
        let seg_ref = seg.push(WakerItem::new(seq, waker));
        self.0.push_back(NonNull::from(seg_ref.as_ref()));
        let _ = Box::leak(seg);
        seg_ref
    }

    fn wake(&mut self) -> WakeResult {}

    fn cancel(&mut self, waker_ref: WakerSegRef) -> Result<(), u8> {}
}

pub struct WakerSegRef {
    seg: NonNull<WakerSeg>,
    idx: usize,
}

impl Deref for WakerSegRef {
    type Target = WakerItem;
    fn deref(&self) -> &Self::Target {
        let seg = unsafe { self.seg.as_ref() };
        unsafe { seg.items[self.idx].assume_init_ref() }
    }
}

pub struct WakerSeg {
    start: u16,
    end: u16,
    ref_count: AtomicU64,
    node: UnsafeCell<DListNode<NonNull<Self>, ()>>,
    items: [MaybeUninit<WakerItem>; 4],
}

unsafe impl DListItem<()> for WakerSeg {
    fn get_node(&self) -> &mut DListNode<Self, ()> {
        unsafe { &mut *self.node.get() }
    }
}

impl WakerSeg {
    #[inline]
    pub fn new() -> Box<Self> {
        Box::new(WakerSeg {
            start: 0,
            end: 0,
            ref_count: AtomicU64::new(1),
            node: UnsafeCell::new(DListNode::default()),
        })
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.end == 4
    }

    #[inline(always)]
    pub fn push(&mut self, item: WakerItem) -> WakerSegRef {
        let idx = self.end;
        debug_assert!(idx != 4);
        self.end = idx + 1;
        self.ref_count.fetch_add(1, Ordering::SeqCst);
        unsafe { self.items[idx].write(item) };
        todo!();
    }

    #[inline(always)]
    pub fn wake(&mut self) -> Result<WakeResult, ()> {
        let idx = self.start;
        if idx == 4 {
            return Err(());
        }
        if idx == self.end {
            return Ok(WakeResult::Skip);
        }
        let item = unsafe { self.items[idx].assume_init_ref() };
        self.start = idx + 1;
        Ok(item.wake());
    }

    pub fn cancel(&mut self, waker_ref: &WakerSegRef) -> Result<(), u8> {
        let seg = unsafe { waker_ref.seg.as_mut() };
        if let Err(state) = waker_ref.abandon() {
            return Err(state);
        }
        if seg.end == waker_ref.idx + 1 {
            seg.end -= 1;
            return Ok(());
        } else if seg.start == waker_ref.idx {
            // advance
            seg.start += 1;
            if seg.start == 4 {
                // remove node
            }
        }
        return Ok(());
    }
}

pub struct WakerItem {
    state: AtomicU32,
    seq: u32,
    waker: UnsafeCell<ThinWaker>,
}

unsafe impl Send for WakerItem {}
unsafe impl Sync for WakerItem {}

impl WakerItem {
    #[inline(always)]
    pub fn new(seq: u32, waker: ThinWaker) -> Self {
        Self { seq, state: AtomicU32::new(WakerState::Init as u32), waker }
    }

    #[inline(always)]
    fn get_waker(&self) -> &ThinWaker {
        unsafe { &*self.waker.get() }
    }

    #[inline(always)]
    fn get_waker_mut(&self) -> &mut ThinWaker {
        unsafe { &mut *self.waker.get() }
    }

    #[inline(always)]
    pub fn reset(&self) {
        // From the object pool to reset value,
        // we should use SeqCst fence to clear the cache of other cores
        self.reset_init();
    }

    #[inline(always)]
    pub fn get_seq(&self) -> u32 {
        self.seq.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set_seq(&self, seq: u32) {
        self.seq.store(seq, Ordering::Relaxed);
    }

    #[inline(always)]
    fn update_thread_handle(&self) {
        let _waker = self.get_waker_mut();
        *_waker = ThinWaker::Blocking(thread::current());
    }

    #[inline(always)]
    pub fn commit_waiting(&self) -> u8 {
        if let Err(s) = self.try_change_state(WakerState::Init, WakerState::Waiting) {
            s
        } else {
            WakerState::Waiting as u8
        }
    }

    #[inline(always)]
    pub fn try_change_state(&self, cur: WakerState, new_state: WakerState) -> Result<(), u8> {
        self.state.compare_exchange(
            cur as u8,
            new_state as u8,
            Ordering::SeqCst,
            Ordering::Acquire,
        )?;
        Ok(())
    }

    #[inline(always)]
    pub fn reset_init(&self) {
        // this is before we put into registry (which will extablish happen-before relationship),
        // it safe to use Relaxed
        self.state.store(WakerState::Init as u8, Ordering::Relaxed);
    }

    /// Return current status,
    /// Closed: might be channel closed, or future successfully cancelled, the future should drop message; try to clear its waker.
    /// Done: the message actually sent, nothing to DO
    /// Woken: the future should drop message, and wake another counterpart.
    #[inline(always)]
    pub fn abandon(&self) -> Result<(), u8> {
        // it will content with close(), on_recv(), on_send()
        match self.change_state_smaller_eq(WakerState::Waiting, WakerState::Closed) {
            Ok(_) => Ok(()),
            Err(state) => Err(state),
        }
        // NOTE: there's no Copy state, so we do not loop
    }

    #[inline(always)]
    pub fn close_wake(&self) -> bool {
        // should have lock because it will content with abandon()
        if self.change_state_smaller_eq(WakerState::Waiting, WakerState::Closed).is_ok() {
            self.get_waker().wake_by_ref();
            return true;
        }
        false
    }

    // Return Ok(pre_state), otherwise return Err(current_state)
    #[inline(always)]
    pub fn change_state_smaller_eq(
        &self, condition: WakerState, target: WakerState,
    ) -> Result<u8, u8> {
        debug_assert!((condition as u8) < (target as u8));
        // Save one load()
        let mut state = condition as u8;
        loop {
            match self.state.compare_exchange_weak(
                state,
                target as u8,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(state);
                }
                Err(s) => {
                    if s > condition as u8 {
                        return Err(s);
                    }
                    state = s;
                }
            }
        }
    }

    #[inline(always)]
    pub fn _get_state(&self, order: Ordering) -> u8 {
        self.state.load(order)
    }

    #[inline(always)]
    pub fn get_state(&self) -> u8 {
        self.state.load(Ordering::SeqCst)
    }

    #[inline(always)]
    pub fn get_state_relaxed(&self) -> u8 {
        self.state.load(Ordering::Relaxed)
    }

    /// Assume no lock
    #[inline(always)]
    pub fn wake(&self) -> WakeResult {
        // This is after we get waker from waker_registry, which already happen before relationship.
        // both >= WakerState::Waiting is certain
        let mut state = self.get_state_relaxed();
        loop {
            if state >= WakerState::Woken as u8 {
                return WakeResult::Skip;
            } else if state == WakerState::Waiting as u8 {
                self.state.store(WakerState::Woken as u8, Ordering::SeqCst);
                self.get_waker().wake_by_ref();
                return WakeResult::Woken;
            } else {
                match self.state.compare_exchange_weak(
                    WakerState::Init as u8,
                    WakerState::Woken as u8,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.get_waker().wake_by_ref();
                        return WakeResult::Next;
                    }
                    Err(s) => {
                        state = s;
                    }
                }
            }
        }
    }

    #[inline(always)]
    pub fn will_wake(&self, ctx: &mut Context) -> bool {
        self.get_waker().will_wake(ctx)
    }
}

/*
impl<T> WakerItem<*const T> {
    #[inline(always)]
    fn get_payload(&self) -> *const T {
        *self.get_payload_mut()
    }

    #[inline(always)]
    pub fn wake_or_copy<F: FlavorImpl<Item = T>>(&self, flavor: &F) -> WakeResult {
        // This is after we get waker from waker_registry, which already happen before relationship.
        // both >= WakerState::Waiting is certain
        let mut state = self.get_state_relaxed();
        loop {
            if state >= WakerState::Woken as u8 {
                return WakeResult::Skip;
            } else if state == WakerState::Waiting as u8 {
                let p = self.get_payload();
                if p.is_null() {
                    self.state.store(WakerState::Woken as u8, Ordering::SeqCst);
                    self.get_waker().wake_by_ref();
                    return WakeResult::Woken;
                }
                state = if let Some(true) = flavor.try_send_oneshot(p) {
                    WakerState::Done as u8
                } else {
                    WakerState::Woken as u8
                };
                self.state.store(state, Ordering::SeqCst);
                self.get_waker().wake_by_ref();
                if state == WakerState::Done as u8 {
                    return WakeResult::Sent;
                } else {
                    return WakeResult::Woken;
                }
            } else {
                match self.state.compare_exchange_weak(
                    WakerState::Init as u8,
                    WakerState::Woken as u8,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.get_waker().wake_by_ref();
                        return WakeResult::Next;
                    }
                    Err(s) => {
                        state = s;
                    }
                }
            }
        }
    }
}

pub struct WakerCache<P: Copy>(ArcCell<WakerItem<P>>);

impl<P: Copy> WakerCache<P> {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self(ArcCell::new())
    }

    #[inline(always)]
    pub fn new_blocking(&self, payload: P) -> ArcWaker<P> {
        if let Some(inner) = self.0.pop() {
            inner.update_thread_handle();
            inner.reset(payload);
            return ArcWaker::<P>::from_arc(inner);
        }
        ArcWaker::new_blocking(payload)
    }

    #[inline(always)]
    pub(crate) fn push(&self, waker: ArcWaker<P>) {
        debug_assert!(waker.get_state() >= WakerState::Woken as u8);
        let a = waker.to_arc();
        if Arc::weak_count(&a) == 0 && Arc::strong_count(&a) == 1 {
            self.0.try_put(a);
        }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        !self.0.exists()
    }
}
*/

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_waker_size() {
        use std::mem::size_of;
        println!("wakertype {}", size_of::<ThinWaker>());
        println!("waker inner {}", size_of::<WakerItem>());
    }
}
