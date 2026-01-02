use crate::backoff::*;
pub(crate) use crate::crossbeam::err::*;
pub(crate) use crate::flavor::*;
use crate::trace_log;
pub(crate) use crate::waker::*;
pub(crate) use crate::waker_registry::*;
use crate::{AsyncRx, AsyncTx, Rx, Tx};
use std::mem::MaybeUninit;
use std::sync::atomic::{compiler_fence, fence, AtomicUsize, AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub struct ChannelShared<T: Send + 'static> {
    pub(crate) senders: RegistrySender<T>,
    pub(crate) recvs: RegistryRecv,
    tx_count: AtomicUsize,
    rx_count: AtomicUsize,
    pub(crate) flavor: Flavor<T>,
    pub(crate) backoff_limit: u16,
    pub(crate) large: bool,
    cap: Option<usize>,
    pub(crate) may_direct_copy: bool,
    pub(crate) _try_send: TrySendFunc<T>,
    pub(crate) _try_recv: TryRecvFunc<T>,
    pub(crate) _poll_item: PollItemFunc<T>,
    pub(crate) _poll_send: PollSendFunc<T>,
    pub(crate) _send_blocking: SendBlocking<T>,
    pub(crate) _recv_blocking: RecvBlocking<T>,
    _is_empty: IsEmptyFunc,
    flavor_ptr: AtomicPtr<()>,
}

impl<T: Send + 'static> ChannelShared<T> {
    pub(crate) fn new<F: FlavorImpl<T> + FlavorPrivate<T>>(
        flavor: F, senders: RegistrySender<T>, recvs: RegistryRecv,
    ) -> Arc<Self> {
        let mut large = false;
        if let Some(bound) = flavor.capacity() {
            if bound >= 10 {
                large = true;
            }
        }
        // NOTE: we choose to store the frequently used function ptr, beause:
        // - The flvaor type is fixed per channel.
        // - There're a number of flavor types, might be incresing.
        // - trait object has VTable lookup cost
        // - we want to avoid the dereferencing cost to put flavor in another box than inline with
        // ChannelShared, so we choose enum
        // - In blocking context, although compiler will likely inline the operation and strip out
        // branch which is not possible, but if use put Tx/Rx into vec, it may confuse and fallback
        // to match.
        // - In async context, because Future is always concertain about context, the compiler
        // cannot not eliminate unused enum variant branch, leaving the match code for
        // emum-dispatch. And the worse of all, as the enum grow beyond 2~3 types, it will not
        // inline the function calls, making async code slow.
        let s = Arc::new(Self {
            tx_count: AtomicUsize::new(1),
            rx_count: AtomicUsize::new(1),
            senders,
            recvs,
            _try_send: try_send_shim::<F, T>,
            _try_recv: try_recv_shim::<F, T>,
            _poll_item: poll_item_shim::<F, T>,
            _poll_send: poll_send_shim::<F, T>,
            _recv_blocking: recv_blocking_shim::<F, T>,
            _send_blocking: send_blocking_shim::<F, T>,
            _is_empty: is_empty_shim::<F, T>,
            backoff_limit: flavor.backoff_limit(),
            large,
            cap: flavor.capacity(),
            may_direct_copy: flavor.may_direct_copy(),
            flavor: flavor.to_flavor(),
            flavor_ptr: AtomicPtr::new(std::ptr::null_mut()),
        });
        // once arc is construct, the enum variant memory location is fixed, we can store it
        let flavor_ptr = s.flavor.get_ptr();
        s.flavor_ptr.store(flavor_ptr as *mut (), Ordering::Release);
        s
    }

    #[inline(always)]
    pub(crate) fn get_flavor_ptr(&self) -> *const () {
        self.flavor_ptr.load(Ordering::Relaxed)
    }

    /// The number of messages in the channel.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.flavor.len()
    }

    /// The capacity of the channel. Returns `None` for unbounded channels.
    #[inline(always)]
    pub fn capacity(&self) -> Option<usize> {
        self.cap
    }

    /// Returns `true` if the channel is empty.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        unsafe { (self._is_empty)(self.get_flavor_ptr()) }
    }

    /// Returns `true` if the channel is full.
    pub fn is_full(&self) -> bool {
        // not frequently used
        self.flavor.is_full()
    }

    /// Returns the number of senders for the channel.
    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.tx_count.load(Ordering::Acquire) as usize
    }

    /// Returns the number of receivers for the channel.
    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.rx_count.load(Ordering::Acquire) as usize
    }

    #[inline(always)]
    pub(crate) fn sender_direct_copy(&self) -> bool {
        self.may_direct_copy && self.senders.use_direct_copy()
    }

    /// Returns the number of wakers for senders and receivers. For debugging purposes.
    pub fn get_wakers_count(&self) -> (usize, usize) {
        (self.senders.len(), self.recvs.len())
    }

    #[inline(always)]
    pub(crate) fn is_tx_closed(&self) -> bool {
        self.tx_count.load(Ordering::SeqCst) == 0
    }

    #[inline(always)]
    pub(crate) fn is_rx_closed(&self) -> bool {
        self.rx_count.load(Ordering::SeqCst) == 0
    }

    #[inline(always)]
    pub(crate) fn add_tx(&self) {
        // The drop will close_tx, which has release fence
        let _ = self.tx_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub(crate) fn add_rx(&self) {
        // The drop will close_rx, which has release fence
        let _ = self.rx_count.fetch_add(1, Ordering::Relaxed);
    }

    /// This method is called when a sender is dropped.
    #[inline(always)]
    pub(crate) fn close_tx(&self) {
        let old = self.tx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from tx");
            fence(Ordering::SeqCst);
            self.recvs.close();
        } else {
            trace_log!("drop tx {}", old - 1);
        }
    }

    /// This method is called when a receiver is dropped.
    #[inline(always)]
    pub(crate) fn close_rx(&self) {
        let old = self.rx_count.fetch_sub(1, Ordering::Release);
        if old <= 1 {
            trace_log!("closing from rx");
            fence(Ordering::SeqCst);
            // There's SeqCst fence inside RegistrySender::close
            self.senders.close();
        } else {
            trace_log!("drop rx {}", old - 1);
        }
    }

    /// if need_wake == true, called from on_recv(), when return None indicates try to wake up next.
    /// when need_wake == false, will always return Some(state).
    ///
    /// NOTE: when return state=Done, the waker is not set to Done
    #[inline]
    pub(crate) fn sender_double_check<F: FlavorImpl<T>>(
        &self, flavor: &F, item: &MaybeUninit<T>, o_waker: &mut Option<SendWaker<T>>, sink: bool,
    ) -> u8 {
        // Not allow Spurious wake and enter this function again;
        if let Some(res) = flavor.try_send_oneshot(item.as_ptr()) {
            if res {
                self.on_send();
                return self.senders.cancel_reuse_waker(o_waker, WakerState::Done);
            } else {
                let state = if sink {
                    self.senders.commit_waiting(&o_waker)
                } else {
                    WakerState::Waiting as u8
                };
                if self.is_rx_closed() {
                    return WakerState::Closed as u8;
                }
                return state;
            }
        } else {
            // Unlikely to be disconnected,
            return self.senders.cancel_reuse_waker(o_waker, WakerState::Woken);
        }
    }

    /// Wait a little more for the waker state change,
    /// NOTE: it's important to yield when you have more sender than receiver
    #[inline(always)]
    pub(crate) fn sender_snooze(
        &self, o_waker: &Option<SendWaker<T>>, backoff: &mut Backoff,
    ) -> u8 {
        backoff.reset();
        loop {
            let state = self.senders.get_waker_state(o_waker, Ordering::Relaxed);
            compiler_fence(Ordering::AcqRel);
            if state >= WakerState::Woken as u8 {
                return state;
            }
            if backoff.snooze() {
                return state;
            }
        }
    }

    /// Wake up one rx
    #[inline(always)]
    pub(crate) fn on_send(&self) {
        self.recvs.fire();
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv(&self) {
        self.senders.fire();
    }

    /// Wake up one tx
    #[inline(always)]
    pub(crate) fn on_recv_direct_copy<F: FlavorImpl<T>>(&self, flavor: &F) {
        if WakeResult::Sent == self.senders.fire_direct_copy::<F>(flavor) {
            self.on_send();
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    /// return false to indicate already Done.
    #[inline(always)]
    pub(crate) fn abandon_send_waker(&self, o_waker: &mut Option<SendWaker<T>>) -> bool {
        match o_waker.take() {
            Some(SendWaker::Multi(waker)) => {
                // which change Waiting/Init to Closed
                match waker.abandon() {
                    Ok(()) => {
                        trace_log!("tx: abandon cancel {:?}", waker);
                        self.senders.clear_wakers(&waker);
                    }
                    Err(state) => {
                        trace_log!("tx: abandon err  {:?} {}", waker, state);
                        if state == WakerState::Woken as u8 {
                            // We are awake, but give up sending, should notify another sender for safety
                            self.on_recv();
                        } else if state == WakerState::Closed as u8 {
                        } else {
                            debug_assert_eq!(state, WakerState::Done as u8);
                            // Unused code for direct_copy
                            return false;
                        }
                    }
                }
                return true;
            }
            Some(SendWaker::Single) => true,
            None => false,
        }
    }

    /// Call on cancellation, return true to indicate drop temporary message
    #[inline(always)]
    pub(crate) fn abandon_recv_waker(&self, o_waker: &mut Option<RecvWaker>) {
        if let Some(RecvWaker::Multi(waker)) = o_waker.take() {
            // which change Waiting/Init to Closed
            match waker.abandon() {
                Ok(()) => {
                    trace_log!("rx: abandon cancel {:?}", waker);
                    self.recvs.clear_wakers(&waker);
                    return;
                }
                Err(state) => {
                    trace_log!("rx: abandon err {:?} {}", waker, state);
                    if state == WakerState::Woken as u8 {
                        // We are awake, but give up receiving, should notify another receiver for safety
                        self.on_send();
                    } else if state == WakerState::Closed as u8 {
                        // Closed
                    } else {
                        debug_assert_eq!(state, WakerState::Done as u8);
                        // Unused code for direct_copy
                    }
                }
            }
        }
    }

    #[inline(always)]
    pub(crate) fn get_async_backoff(&self) -> Option<Backoff> {
        if self.large {
            return None;
        }
        let cfg = BackoffConfig::default();
        if cfg.spin_limit == 0 {
            // 1 core don't backoff
            return None;
        }
        Some(Backoff::new(cfg))
    }
}

/// On timed out, returns Err(())
#[inline(always)]
pub fn check_timeout(deadline: Option<Instant>) -> Result<Option<Duration>, ()> {
    if let Some(end) = deadline {
        let now = Instant::now();
        if now < end {
            return Ok(Some(end - now));
        } else {
            return Err(());
        }
    }
    Ok(None)
}

pub(crate) type TrySendFunc<T> = unsafe fn(*const (), &MaybeUninit<T>) -> bool;
pub(crate) type TryRecvFunc<T> = unsafe fn(*const ()) -> Option<T>;

pub(crate) type PollSendFunc<T> = unsafe fn(
    &ChannelShared<T>,
    *const (),
    &mut Context,
    &MaybeUninit<T>,
    &mut Option<SendWaker<T>>,
    bool,
) -> Poll<Result<(), ()>>;

pub(crate) type PollItemFunc<T> = unsafe fn(
    &ChannelShared<T>,
    *const (),
    &mut Context,
    &mut Option<RecvWaker>,
    bool,
) -> Result<T, TryRecvError>;

pub(crate) type SendBlocking<T> = unsafe fn(
    &ChannelShared<T>,
    *const (),
    &MaybeUninit<T>,
    Option<Instant>,
    &WakerCache<*const T>,
) -> Result<(), bool>;
pub(crate) type RecvBlocking<T> =
    unsafe fn(&ChannelShared<T>, *const (), Option<Instant>, &WakerCache<()>) -> Result<T, bool>;

pub(crate) type IsEmptyFunc = unsafe fn(ptr: *const ()) -> bool;

#[inline]
unsafe fn try_send_shim<F: FlavorImpl<T>, T: Send + 'static>(
    ptr: *const (), item: &MaybeUninit<T>,
) -> bool {
    let flavor = &*(ptr as *const F);
    flavor.try_send(item)
}

#[inline]
unsafe fn try_recv_shim<F: FlavorImpl<T>, T: Send + 'static>(
    ptr: *const (),
) -> Option<T> {
    let flavor = &*(ptr as *const F);
    flavor.try_recv()
}

#[inline]
unsafe fn send_blocking_shim<F: FlavorImpl<T>, T: Send + 'static>(
    shared: &ChannelShared<T>, ptr: *const (), item: &MaybeUninit<T>, deadline: Option<Instant>,
    waker_cache: &WakerCache<*const T>,
) -> Result<(), bool> {
    let flavor = &*(ptr as *const F);
    Tx::<T>::send_blocking::<F>(shared, flavor, item, deadline, waker_cache)
}

#[inline]
unsafe fn recv_blocking_shim<F: FlavorImpl<T>, T: Send + 'static>(
    shared: &ChannelShared<T>, ptr: *const (), deadline: Option<Instant>,
    waker_cache: &WakerCache<()>,
) -> Result<T, bool> {
    let flavor = &*(ptr as *const F);
    Rx::<T>::recv_blocking::<F>(shared, flavor, deadline, waker_cache)
}

#[inline]
unsafe fn poll_send_shim<F: FlavorImpl<T>, T: Send + 'static>(
    shared: &ChannelShared<T>, ptr: *const (), ctx: &mut Context, item: &MaybeUninit<T>,
    o_waker: &mut Option<SendWaker<T>>, sink: bool,
) -> Poll<Result<(), ()>> {
    let flavor = &*(ptr as *const F);
    AsyncTx::<T>::_poll_send::<F>(shared, flavor, ctx, item, o_waker, sink)
}

#[inline]
unsafe fn poll_item_shim<F: FlavorImpl<T>, T: Send + 'static>(
    shared: &ChannelShared<T>, ptr: *const (), ctx: &mut Context, o_waker: &mut Option<RecvWaker>,
    stream: bool,
) -> Result<T, TryRecvError> {
    let flavor = &*(ptr as *const F);
    AsyncRx::<T>::_poll_item::<F>(shared, flavor, ctx, o_waker, stream)
}

#[inline]
unsafe fn is_empty_shim<F: FlavorImpl<T>, T: Send + 'static>(ptr: *const ()) -> bool {
    let flavor = &*(ptr as *const F);
    F::is_empty(flavor)
}
