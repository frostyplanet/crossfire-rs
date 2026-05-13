//! compatible layer for V2.0 API
//!
//! # Migration from v2.* to v3
//!
//! If you want to migrate to v3 API, you may add the flavor type in [MTx], [MRx], [Tx], [Rx] type,
//! and change the channel initialization function accordingly.
//!
//! If you have a large project that use v2 API, and want to migrate gradually,
//! only need to change original import from  `use crossfire::*` to `use crossfire::compat::*`.
//! This module provides the [CompatFlavor] which erase the difference between `List` and `Array`,
//! but registry only use RegistryMulti for spsc and mpsc for compatibility.
//!
//! # Compatible consideration
//!
//! - In the legacy API, the sender/receiver types had erased the signature between bounded or unbounded channels
//! - The low level queue implement is for MPMC regardless of MPSC/SPSC model (which is exactly the
//!   same with V2.1)
//! - The module structure in `crossfire::compat::*`, is exactly the same as v2.x `crossfire::*`.
//!
//! # Incompatible notes
//!
//! - keeping Into<AsyncStream<T, F>> for `AsyncRxTrait<T>` is not possible, due to `AsyncRxTrait<T>`
//!   is erased out Flavor parameter, so we remove this method.
//!
//! # The reason of complete API refactor
//!
//! I know we all hate the contagious nature of generic code, and reluctant to use trait object,
//! it's common practice to use static dispatch like `enum-dispatch`. Originally crossfire only
//! have 2 channel variance ([CompatFlavor]), when adding more channel flavor for specific scenario,
//! other than common list and array, and specialized implement for spsc, mpsc, etc,
//! I notice that when the flavor enum grow from 2 types to 4+ types,
//! although the positive result can be observed on Arm, there was a regression in x86 async benchmark,
//! which offset the optimization effort.
//! It's impossible to erased the type while keeping the performance goal having so much types.
//!
//! From the aspect of compiler:
//! - In blocking context, the compiler can eliminate the unused branch according to the context,
//!   and keeping the function calls inline, unless you put multiple variant of enum together into a
//!   collection.
//! - In async context, the compiler is ignorance, since most of the async code is indirect calls.
//!   We can see in generated asm from cargo-show-asm, even you initialize the channel with ArrayQueue, there's still
//!   SeqQueue match branch inside the `RecvFuture::poll()`. What's worse when we have 4 types
//!   variant in the flavor enum, the compiler think the internal queue ops function no longer worth
//!   to inline (because overall flatten code will be too big), and the match branch might fallen
//!   back to a big match table instead of simple comparison. This is the reason of performance regression.
//!
//! From the aspect of CPU:
//! - I had tried a manual Vtable by putting method ptr inside AsyncTx/AsyncRx, which is ok on X86,
//!   but Arm will have -50% penalty. It looks like Arm is poor on loading / caching function ptr.
//! - Generic Arm CPU has overall poor performance (1/3 ~ 1/2) compared to mainstream x86_64, and
//!   bad at atomic CAS, a big match branch might be not so obvious than the positive effect from
//!   changing some CAS to direct load/store in the lockless algorithm.
//!
//! From the aspect of API usage:
//! - There're already nice native select mechanisms on async ecology, we don't have to worry about the
//!   difference of receiver types, for flexibility.
//! - For blocking context, it might be more common scenario to select from the same type of channels for efficiency.
//! - The crossbeam implementation of select is decouple from channel types and message type, which
//!   means the API is possible for crossfire too.

use crate::flavor::{
    flavor_dispatch, flavor_select_dispatch, queue_dispatch, Flavor, FlavorImpl, FlavorMC,
    FlavorMP, Queue,
};
use crate::shared::*;
pub use crate::{AsyncRxTrait, AsyncTxTrait, BlockingRxTrait, BlockingTxTrait};
use std::mem::MaybeUninit;

/// Compatible flavor that wraps the Array and list type
#[allow(clippy::large_enum_variant)]
pub enum CompatFlavor<T> {
    Array(crate::flavor::Array<T>),
    List(crate::flavor::List<T>),
}

macro_rules! wrap_compat {
    ($self: expr, $method:ident $($arg:expr)*)=>{
        match $self {
            Self::Array(inner) => inner.$method($($arg)*),
            Self::List(inner) => inner.$method($($arg)*),
        }
    };
}
impl<T> Queue for CompatFlavor<T> {
    type Item = T;
    queue_dispatch!(wrap_compat);
}

impl<T> FlavorImpl for CompatFlavor<T> {
    flavor_dispatch!(wrap_compat);
}

impl<T> FlavorSelect for CompatFlavor<T> {
    flavor_select_dispatch!(wrap_compat);
}

impl<T> FlavorMP for CompatFlavor<T> {}
impl<T> FlavorMC for CompatFlavor<T> {}

// There's not much performance difference between old RegistrySingle and RegistryMulti,
// we just use RegistryMulti here since this is just for compatible reason.
impl<T: Send + 'static> Flavor for CompatFlavor<T> {
    type Send = RegistryMultiSend<T>;
    type Recv = RegistryMultiRecv;
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
    CompatFlavor::<T>::Array(crate::flavor::Array::<T>::new(size))
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
            ChannelShared::new($flavor, RegistryMultiSend::new(), RegistryMultiRecv::new())
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
            ChannelShared::new($flavor, RegistryMultiSend::new(), RegistryMultiRecv::new())
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
            ChannelShared::new($flavor, RegistryMultiSend::new(), RegistryMultiRecv::new())
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
