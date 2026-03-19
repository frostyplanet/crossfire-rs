//use crate::collections::ArcCell;
use std::cell::UnsafeCell;
use std::fmt;
use std::ops::Deref;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicU8, AtomicU32, Ordering},
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

pub struct WakerItem {
    // we only use u8 WakerState, just for padding
    state: AtomicU32,
    seq: u32,
    waker: ThinWaker,
}

impl fmt::Debug for WakerItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "waker({})", self.get_seq())
    }
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
        &self.waker
    }

    #[inline(always)]
    pub fn get_seq(&self) -> u32 {
        self.seq
    }

    #[inline(always)]
    pub fn set_seq(&mut self, seq: u32) {
        self.seq = seq;
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
        match self.state.compare_exchange(
            cur as u32,
            new_state as u32,
            Ordering::SeqCst,
            Ordering::Acquire,
        ) {
            Err(_state) => Err(_state as u8),
            Ok(_) => Ok(()),
        }
    }

    //#[inline(always)]
    //pub fn reset_init(&self) {
    //    // this is before we put into registry (which will extablish happen-before relationship),
    //    // it safe to use Relaxed
    //    self.state.store(WakerState::Init as u8, Ordering::Relaxed);
    //}

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
        debug_assert!((condition as u32) < (target as u32));
        // Save one load()
        let mut state = condition as u32;
        loop {
            match self.state.compare_exchange_weak(
                state,
                target as u32,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(state as u8);
                }
                Err(s) => {
                    if s > condition as u32 {
                        return Err(s as u8);
                    }
                    state = s;
                }
            }
        }
    }

    #[inline(always)]
    pub fn _get_state(&self, order: Ordering) -> u8 {
        self.state.load(order) as u8
    }

    #[inline(always)]
    pub fn get_state(&self) -> u8 {
        self.state.load(Ordering::SeqCst) as u8
    }

    #[inline(always)]
    pub fn get_state_relaxed(&self) -> u8 {
        self.state.load(Ordering::Relaxed) as u8
    }

    /// Assume no lock
    #[inline(always)]
    pub fn wake(&self) -> WakeResult {
        // This is after we get waker from waker_registry, which already happen before relationship.
        // both >= WakerState::Waiting is certain
        let mut state = self.get_state_relaxed() as u32;
        loop {
            if state >= WakerState::Woken as u32 {
                return WakeResult::Skip;
            } else if state == WakerState::Waiting as u32 {
                self.state.store(WakerState::Woken as u32, Ordering::SeqCst);
                self.get_waker().wake_by_ref();
                return WakeResult::Woken;
            } else {
                match self.state.compare_exchange_weak(
                    WakerState::Init as u32,
                    WakerState::Woken as u32,
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_waker_size() {
        use std::mem::size_of;
        println!("wakertype {}", size_of::<ThinWaker>());
        println!("waker inner {}", size_of::<WakerItem>());
        println!("wakerSeg {}", size_of::<WakerSeg>());
    }
}
