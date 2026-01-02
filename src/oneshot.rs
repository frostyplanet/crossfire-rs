/// OneShot channel implement
use crate::flavor::{Flavor, FlavorImpl, FlavorPrivate};
use crate::share::*;
use crate::{AsyncRx, Rx};
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::task::{Context, Poll};

pub struct OneShot<T> {
    exist: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Send> Send for OneShot<T> {}
unsafe impl<T: Send> Sync for OneShot<T> {}

impl<T> OneShot<T> {
    #[inline]
    pub fn new() -> Self {
        Self { value: UnsafeCell::new(MaybeUninit::uninit()), exist: AtomicBool::new(false) }
    }

    #[inline(always)]
    fn _try_recv(&self, order: Ordering) -> Option<T> {
        if self.exist.load(order) {
            let msg = unsafe { self.value.get().read().assume_init() };
            self.exist.store(false, Ordering::Release);
            Some(msg)
        } else {
            None
        }
    }
}

impl<T> Drop for OneShot<T> {
    #[inline]
    fn drop(&mut self) {
        // We can use get_mut according to ArrayQueue
        if *self.exist.get_mut() {
            unsafe {
                let p = (*self.value.get()).as_mut_ptr();
                std::ptr::drop_in_place(p);
            }
        }
    }
}

impl<T> FlavorImpl<T> for OneShot<T> {
    #[inline(always)]
    fn len(&self) -> usize {
        if self.is_full() {
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
        self.exist.load(Ordering::SeqCst)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.exist.load(Ordering::SeqCst) == false
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<T>) -> bool {
        unsafe { (*self.value.get()).write(ptr::read(item.as_ptr())) };
        self.exist.store(true, Ordering::Release);
        true
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<T> {
        self._try_recv(Ordering::Acquire)
    }

    #[inline(always)]
    fn try_recv_final(&self) -> Option<T> {
        self._try_recv(Ordering::SeqCst)
    }

    #[inline]
    fn backoff_limit(&self) -> u16 {
        3
    }

    #[inline]
    fn may_direct_copy(&self) -> bool {
        false
    }
}

impl<T> FlavorPrivate<T> for OneShot<T> {
    #[inline]
    fn to_flavor(self) -> Flavor<T> {
        Flavor::OneShot(self)
    }

    #[inline]
    fn new_reg_sender<const MP: bool>(&self) -> RegistrySender<T> {
        debug_assert_eq!(MP, false);
        RegistrySender::<T>::Dummy
    }

    #[inline]
    fn new_reg_recv<const MC: bool>(&self) -> RegistryRecv {
        debug_assert_eq!(MC, false);
        RegistryRecv::new_single()
    }
}

/// speciallized sender for oneshot channel
pub struct TxOneshot<T>(Arc<ChannelShared<T>>);

impl<T> TxOneshot<T> {
    /// Consume itself and send the item
    #[inline]
    pub fn send(self, item: T) {
        let _item = MaybeUninit::new(item);
        self.0.inner.try_send(&_item);
        // let close_tx in Drop do the wakeup
    }
}

impl<T> Drop for TxOneshot<T> {
    #[inline]
    fn drop(&mut self) {
        self.0.close_tx();
    }
}

/// speciallized sender for oneshot channel
#[must_use]
pub struct RxOneshot<T> {
    shared: Arc<ChannelShared<T>>,
    waker: Option<RecvWaker>,
}

impl<T> Future for RxOneshot<T> {
    type Output = Result<T, RecvError>;

    #[inline]
    fn poll(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
        let mut _self = self.get_mut();
        // Because the TxOneshot is nonblocking, we do not need to trigger
        // close_rx on Rx drop. which is a hot path for oneshot, so use shared directly
        match AsyncRx::poll_item(&_self.shared, ctx, &mut _self.waker, true) {
            Err(e) => {
                if !e.is_empty() {
                    return Poll::Ready(Err(RecvError {}));
                } else {
                    return Poll::Pending;
                }
            }
            Ok(item) => {
                return Poll::Ready(Ok(item));
            }
        }
    }
}

#[inline]
fn init<T>() -> Arc<ChannelShared<T>> {
    let oneshot = OneShot::new();
    let send_wakers = oneshot.new_reg_sender::<false>();
    let recv_wakers = oneshot.new_reg_recv::<false>();
    ChannelShared::new(oneshot.to_flavor(), send_wakers, recv_wakers)
}

#[inline]
pub fn new_blocking<T>() -> (TxOneshot<T>, Rx<T>) {
    let shared = init();
    (TxOneshot(shared.clone()), Rx::new(shared))
}

#[inline]
pub fn new_async<T>() -> (TxOneshot<T>, RxOneshot<T>) {
    let shared = init();
    (TxOneshot(shared.clone()), RxOneshot { shared, waker: None })
}
