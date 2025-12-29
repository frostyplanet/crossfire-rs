use super::{Flavor, FlavorImpl, FlavorPrivate};
use crate::backoff::*;
use crate::waker_registry::*;
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use crossbeam_utils::CachePadded;
use std::ptr;
use std::sync::atomic::{AtomicU8, Ordering};

/// A simplify ArrayQueue specialized for size=1
pub struct OneSize<T> {
    setting: CachePadded<Setting>,
    state: AtomicU8,
    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

struct Setting {
    mp: bool,
    mc: bool,
}

#[repr(u8)]
enum SlotState {
    Empty = 0,
    Writing = 1,
    Exist = 2,
    Reading = 3,
}

unsafe impl<T: Send> Sync for OneSize<T> {}
unsafe impl<T: Send> Send for OneSize<T> {}

impl<T> OneSize<T> {
    #[inline]
    pub fn new(mp: bool, mc: bool) -> Self {
        Self {
            setting: CachePadded::new(Setting { mp, mc }),
            state: AtomicU8::new(SlotState::Empty as u8),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    /// return Ok(true) on ok, Ok(false) on full, Err(()) to spin
    #[inline(always)]
    unsafe fn try_push(&self, value: *const T) -> Result<bool, ()> {
        if self.setting.mp {
            loop {
                match self.state.compare_exchange_weak(
                    SlotState::Empty as u8,
                    SlotState::Writing as u8,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Err(state) => {
                        if state == SlotState::Empty as u8 {
                            continue;
                        } else if state == SlotState::Exist as u8 {
                            // need double check, but usually try_send have double check
                            return Ok(false);
                        }
                        return Err(());
                    }
                    Ok(_) => break,
                }
            }
        } else {
            let state = self.state.load(Ordering::SeqCst);
            if state == SlotState::Exist as u8 {
                return Ok(false);
            } else if state != SlotState::Empty as u8 {
                return Err(());
            }
        }
        unsafe {
            (*self.value.get()).write(ptr::read(value));
        }
        // spsc (does not use CAS),
        // and mpmc also need to ensure total ordering against is_full()/is_empty(),
        self.state.store(SlotState::Exist as u8, Ordering::SeqCst);
        return Ok(true);
    }

    #[inline(always)]
    pub unsafe fn push_with_ptr(&self, value: *const T) -> bool {
        if let Ok(r) = unsafe { self.try_push(value) } {
            return r;
        }
        let mut backoff = Backoff::new(BackoffConfig::default());
        loop {
            backoff.snooze();
            if let Ok(r) = unsafe { self.try_push(value) } {
                return r;
            }
        }
    }

    #[inline(always)]
    fn try_pop(&self) -> Result<Option<T>, ()> {
        let mc = self.setting.mc;
        if mc {
            loop {
                match self.state.compare_exchange_weak(
                    SlotState::Exist as u8,
                    SlotState::Reading as u8,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                ) {
                    Err(state) => {
                        if state == SlotState::Exist as u8 {
                            continue;
                        } else if state == SlotState::Empty as u8 {
                            #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
                            {
                                return Ok(None);
                            }
                            #[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
                            {
                                // XXX additional fence to ensure it's really empty
                                if self.state.load(Ordering::SeqCst) == SlotState::Empty as u8 {
                                    return Ok(None);
                                }
                            }
                        }
                        return Err(());
                    }
                    Ok(_) => break,
                }
            }
        } else {
            let state = self.state.load(Ordering::SeqCst);
            if state == SlotState::Empty as u8 {
                return Ok(None);
            } else if state != SlotState::Exist as u8 {
                return Err(());
            }
        }
        let msg = unsafe { self.value.get().read().assume_init() };
        // spsc (does not use CAS),
        // and mpmc also need to ensure total ordering against is_full()/is_empty(),
        self.state.store(SlotState::Empty as u8, Ordering::SeqCst);
        return Ok(Some(msg));
    }

    /// return None on empty
    #[inline(always)]
    pub fn pop(&self) -> Option<T> {
        if let Ok(r) = self.try_pop() {
            return r;
        }
        let mut backoff = Backoff::new(BackoffConfig::default());
        loop {
            backoff.snooze();
            if let Ok(r) = self.try_pop() {
                return r;
            }
        }
    }
}

impl<T> FlavorImpl<T> for OneSize<T> {
    #[inline(always)]
    fn len(&self) -> usize {
        if self.state.load(Ordering::SeqCst) != SlotState::Empty as u8 {
            1
        } else {
            0
        }
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        Some(1)
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        self.state.load(Ordering::SeqCst) == SlotState::Exist as u8
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.state.load(Ordering::SeqCst) == SlotState::Empty as u8
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        unsafe { self.push_with_ptr(item.as_ptr()) }
    }

    #[inline(always)]
    fn try_send_oneshot(&self, item: *const T) -> Option<bool> {
        if let Ok(r) = unsafe { self.try_push(item) } {
            if r == false {
                if self.setting.mp {
                    // XXX additional fence to ensure it's really full, on cas failure branch use
                    // Acquire (not SeqCst)
                    if self.state.load(Ordering::SeqCst) == SlotState::Exist as u8 {
                        return Some(false);
                    } else {
                        return None;
                    }
                }
            }
            Some(r)
        } else {
            None
        }
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self.pop()
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        #[cfg(target_arch = "x86_64")]
        {
            crate::backoff::DEFAULT_LIMIT
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            crate::backoff::MAX_LIMIT
        }
    }
}

impl<T> FlavorPrivate<T> for OneSize<T> {
    fn to_flavor(self) -> Flavor<T> {
        Flavor::One(self)
    }

    #[inline]
    fn new_reg_sender<const _MP: bool>(&self) -> RegistrySender<T> {
        if _MP {
            RegistrySender::<T>::new_multi()
        } else {
            RegistrySender::<T>::new_single()
        }
    }

    #[inline]
    fn new_reg_recv<const _MC: bool>(&self) -> RegistryRecv {
        if _MC {
            RegistryRecv::new_multi()
        } else {
            RegistryRecv::new_single()
        }
    }
}
