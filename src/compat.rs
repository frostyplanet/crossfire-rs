//! compatible layer for V2.0 API
//!
//! The API is exactly the same with V2.0, V2.1 :
//!
//! - The sender/receiver types does not distinguish between bounded or unbounded channels
//! - The low level queue implement is for MPMC regardless of MPSC/SPSC model (which is exactly the
//! same with V2.1)
//! - For migration, you only need to change original code from  `use crossfire::*` to `use
//! crossfire::compat::*`

use crate::flavor::Flavor;
use crate::shared::*;
pub use crate::{AsyncRxTrait, AsyncTxTrait, BlockingRxTrait, BlockingTxTrait};
use std::mem::MaybeUninit;

pub enum CompatFlavor<T> {
    Array(crate::flavor::Array<T, true, true>),
    List(crate::flavor::List<T>),
}

macro_rules! wrap_method {
    ($self: expr, $method:ident $($arg:expr)*)=>{
        match $self {
            Self::Array(inner) => inner.$method($($arg)*),
            Self::List(inner) => inner.$method($($arg)*),
        }
    };
}

impl<T: Send + Unpin + 'static> Flavor for CompatFlavor<T> {
    type Item = T;

    #[inline(always)]
    fn len(&self) -> usize {
        wrap_method!(self, len)
    }

    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        wrap_method!(self, capacity)
    }

    #[inline(always)]
    fn is_full(&self) -> bool {
        wrap_method!(self, is_full)
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        wrap_method!(self, is_empty)
    }

    #[inline(always)]
    fn try_send(&self, item: &MaybeUninit<Self::Item>) -> bool {
        wrap_method!(self, try_send item)
    }

    #[inline]
    fn try_send_oneshot(&self, _item: *const Self::Item) -> Option<bool> {
        match self {
            Self::Array(inner) => inner.try_send_oneshot(_item),
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    fn try_recv(&self) -> Option<Self::Item> {
        wrap_method!(self, try_recv)
    }

    #[inline(always)]
    fn try_recv_final(&self) -> Option<Self::Item> {
        wrap_method!(self, try_recv_final)
    }

    #[inline(always)]
    fn backoff_limit(&self) -> u16 {
        wrap_method!(self, backoff_limit)
    }

    #[inline(always)]
    fn may_direct_copy(&self) -> bool {
        wrap_method!(self, may_direct_copy)
    }
}

#[inline(always)]
fn new_list<T: Send + Unpin + 'static>() -> CompatFlavor<T> {
    CompatFlavor::<T>::List(crate::flavor::List::new())
}

#[inline(always)]
fn new_array<T: Send + Unpin + 'static>(mut size: usize) -> CompatFlavor<T> {
    if size <= 1 {
        size = 1;
    }
    CompatFlavor::<T>::Array(crate::flavor::Array::<T, true, true>::new(size))
}

pub type Tx<T> = crate::Tx<CompatFlavor<T>>;

pub type MTx<T> = crate::MTx<CompatFlavor<T>>;

pub type Rx<T> = crate::Rx<CompatFlavor<T>>;

pub type MRx<T> = crate::MRx<CompatFlavor<T>>;

pub type AsyncTx<T> = crate::AsyncTx<CompatFlavor<T>>;

pub type MAsyncTx<T> = crate::MAsyncTx<CompatFlavor<T>>;

pub type AsyncRx<T> = crate::AsyncRx<CompatFlavor<T>>;

pub type MAsyncRx<T> = crate::MAsyncRx<CompatFlavor<T>>;

pub use crate::{
    RecvError, RecvTimeoutError, SendError, SendTimeoutError, TryRecvError, TrySendError,
};

pub mod sink {
    use super::*;

    pub type AsyncSink<T> = crate::sink::AsyncSink<CompatFlavor<T>>;
}

pub mod stream {
    use super::*;

    pub type AsyncStream<T> = crate::stream::AsyncStream<CompatFlavor<T>>;
}

pub mod spsc {

    use super::*;

    macro_rules! init_share {
        ($flavor: expr) => {{
            let send_wakers = if $flavor.capacity().is_none() {
                RegistrySender::Dummy
            } else {
                RegistrySender::new_single()
            };
            let recv_wakers = RegistryRecv::new_single();
            ChannelShared::new($flavor, send_wakers, recv_wakers)
        }};
    }

    /// Creates an unbounded channel for use in a blocking context.
    ///
    /// The sender will never block, so we use the same `Tx` for all threads.
    pub fn unbounded_blocking<T: Unpin + Send + 'static>() -> (Tx<T>, Rx<T>) {
        let shared = init_share!(new_list::<T>());
        let tx = Tx::new(shared.clone());
        let rx = Rx::new(shared);
        (tx, rx)
    }

    /// Creates an unbounded channel for use in an async context.
    ///
    /// The sender will never block, so we use the same `Tx` for all threads.
    pub fn unbounded_async<T: Unpin + Send + 'static>() -> (Tx<T>, AsyncRx<T>) {
        let shared = init_share!(new_list::<T>());
        let tx = Tx::new(shared.clone());
        let rx = AsyncRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel for use in a blocking context.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_blocking<T: Unpin + Send + 'static>(size: usize) -> (Tx<T>, Rx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = Tx::new(shared.clone());
        let rx = Rx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where both the sender and receiver are async.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_async<T: Unpin + Send + 'static>(size: usize) -> (AsyncTx<T>, AsyncRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = AsyncTx::new(shared.clone());
        let rx = AsyncRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where the sender is async and the receiver is blocking.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_tx_async_rx_blocking<T: Unpin + Send + 'static>(
        size: usize,
    ) -> (AsyncTx<T>, Rx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = AsyncTx::new(shared.clone());
        let rx = Rx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where the sender is blocking and the receiver is async.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_tx_blocking_rx_async<T: Unpin + Send + 'static>(
        size: usize,
    ) -> (Tx<T>, AsyncRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = Tx::new(shared.clone());
        let rx = AsyncRx::new(shared);
        (tx, rx)
    }
}

pub mod mpsc {

    use super::*;

    macro_rules! init_share {
        ($flavor: expr) => {{
            let send_wakers = if $flavor.capacity().is_none() {
                RegistrySender::Dummy
            } else {
                RegistrySender::new_multi()
            };
            let recv_wakers = RegistryRecv::new_single();
            ChannelShared::new($flavor, send_wakers, recv_wakers)
        }};
    }

    /// Creates an unbounded channel for use in a blocking context.
    ///
    /// The sender will never block, so we use the same `Tx` for all threads.
    pub fn unbounded_blocking<T: Send + 'static + Unpin>() -> (MTx<T>, Rx<T>) {
        let shared = init_share!(new_list::<T>());
        let tx = MTx::new(shared.clone());
        let rx = Rx::new(shared);
        (tx, rx)
    }

    /// Creates an unbounded channel for use in an async context.
    ///
    /// Although the sender type is `MTx`, it will never block.
    pub fn unbounded_async<T: Send + 'static + Unpin>() -> (MTx<T>, AsyncRx<T>) {
        let shared = init_share!(new_list::<T>());
        let tx = MTx::new(shared.clone());
        let rx = AsyncRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel for use in a blocking context.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_blocking<T: Send + 'static + Unpin>(size: usize) -> (MTx<T>, Rx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MTx::new(shared.clone());
        let rx = Rx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where both the sender and receiver are async.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_async<T: Send + 'static + Unpin>(size: usize) -> (MAsyncTx<T>, AsyncRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MAsyncTx::new(shared.clone());
        let rx = AsyncRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where the sender is async and the receiver is blocking.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_tx_async_rx_blocking<T: Send + 'static + Unpin>(
        size: usize,
    ) -> (MAsyncTx<T>, Rx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MAsyncTx::new(shared.clone());
        let rx = Rx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where the sender is blocking and the receiver is async.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_tx_blocking_rx_async<T: Send + 'static + Unpin>(
        size: usize,
    ) -> (MTx<T>, AsyncRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MTx::new(shared.clone());
        let rx = AsyncRx::new(shared);
        (tx, rx)
    }
}

pub mod mpmc {
    //! v2 API Compatible Multiple producers, multiple consumers.

    use super::*;

    macro_rules! init_share {
        ($flavor: expr) => {{
            let send_wakers = if $flavor.capacity().is_none() {
                RegistrySender::Dummy
            } else {
                RegistrySender::new_multi()
            };
            let recv_wakers = RegistryRecv::new_multi();
            ChannelShared::new($flavor, send_wakers, recv_wakers)
        }};
    }

    /// Creates an unbounded channel for use in a blocking context.
    ///
    /// The sender will never block, so we use the same `Tx` for all threads.
    pub fn unbounded_blocking<T: Send + 'static + Unpin>() -> (MTx<T>, MRx<T>) {
        let shared = init_share!(new_list::<T>());
        let tx = MTx::new(shared.clone());
        let rx = MRx::new(shared);
        (tx, rx)
    }

    /// Creates an unbounded channel for use in an async context.
    ///
    /// Although the sender type is `MTx`, it will never block.
    pub fn unbounded_async<T: Send + 'static + Unpin>() -> (MTx<T>, MAsyncRx<T>) {
        let shared = init_share!(new_list::<T>());
        let tx = MTx::new(shared.clone());
        let rx = MAsyncRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel for use in a blocking context.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_blocking<T: Send + 'static + Unpin>(size: usize) -> (MTx<T>, MRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MTx::new(shared.clone());
        let rx = MRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel for use in an async context.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_async<T: Send + 'static + Unpin>(size: usize) -> (MAsyncTx<T>, MAsyncRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MAsyncTx::new(shared.clone());
        let rx = MAsyncRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where the sender is async and the receiver is blocking.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_tx_async_rx_blocking<T: Send + 'static + Unpin>(
        size: usize,
    ) -> (MAsyncTx<T>, MRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MAsyncTx::new(shared.clone());
        let rx = MRx::new(shared);
        (tx, rx)
    }

    /// Creates a bounded channel where the sender is blocking and the receiver is async.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    pub fn bounded_tx_blocking_rx_async<T: Send + 'static + Unpin>(
        size: usize,
    ) -> (MTx<T>, MAsyncRx<T>) {
        let shared = init_share!(new_array::<T>(size));
        let tx = MTx::new(shared.clone());
        let rx = MAsyncRx::new(shared);
        (tx, rx)
    }
}
