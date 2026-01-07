//! Multiple producers, multiple consumers.
//!
//! The optimization assumes multiple consumers. The waker registration of the receiver is less efficient compared to `mpsc`.
//!
//! **NOTE**: For the MC (multiple consumer) version, [AsyncRx] and [Rx] are `Clone` and implement `Sync`.
//! They can be safely used with `send`/`recv` while in an `Arc`.

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::flavor::{flavor_enum_dispatch, Flavor, FlavorMC, FlavorMP};
use crate::shared::*;
use crate::{ReceiverType, SenderType};
use std::marker::PhantomData;
use std::mem::MaybeUninit;

/// Flavor Type alias for unbounded MPMC channel
pub type List<T> = crate::flavor::List<T>;

/// Flavor Type alias for bounded MPMC channel wrapped with specified One impl
pub enum Array<T> {
    Array(crate::flavor::Array<T, true, true>),
    One(crate::flavor::One<T>),
}

impl<T: Send + Unpin + 'static> Array<T> {
    #[inline]
    pub fn new(size: usize) -> Self {
        if size <= 1 {
            Self::One(crate::flavor::One::new())
        } else {
            Self::Array(crate::flavor::Array::<T, true, true>::new(size))
        }
    }
}

impl<T> FlavorMP for Array<T> {}
impl<T> FlavorMC for Array<T> {}

macro_rules! wrap_array {
    ($self: expr, $method:ident $($arg:expr)*)=>{
        match $self {
            Self::Array(inner) => inner.$method($($arg)*),
            Self::One(inner) => inner.$method($($arg)*),
        }
    };
}

flavor_enum_dispatch!(Array, wrap_array);

/// The generic builder for all mpmc channel type.
///
/// Initialize sender and receiver types from a flavor type,
/// you can let the compiler to infer the type according to return type signature.
/// (the falvor might have diffrent new() method, but the rest is the same.
#[inline(always)]
pub fn build<F, S, R>(flavor: F) -> (S, R)
where
    F: Flavor + FlavorMP + FlavorMC,
    S: SenderType<F> + Clone,
    R: ReceiverType<F> + Clone,
{
    let send_wakers = if flavor.capacity().is_none() {
        RegistrySender::Dummy
    } else {
        RegistrySender::new_multi()
    };
    let recv_wakers = RegistryRecv::new_multi();
    let shared = ChannelShared::new(flavor, send_wakers, recv_wakers);
    (S::new(shared.clone()), R::new(shared))
}

/// Creates an unbounded channel for use in a blocking context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
pub struct Unbounded<T>(PhantomData<fn(&T)>)
where
    T: Send + 'static + Unpin;

impl<T> Unbounded<T>
where
    T: Send + 'static + Unpin,
{
    #[inline]
    pub fn new<R>() -> (MTx<List<T>>, R)
    where
        T: Send + 'static + Unpin,
        R: ReceiverType<List<T>> + Clone,
    {
        build::<List<T>, MTx<List<T>>, R>(List::<T>::new())
    }

    #[inline]
    pub fn new_blocking() -> (MTx<List<T>>, MRx<List<T>>) {
        Self::new()
    }

    #[inline]
    pub fn new_async() -> (MTx<List<T>>, MAsyncRx<List<T>>) {
        Self::new()
    }
}

pub struct Bounded<T>(PhantomData<fn(&T)>)
where
    T: Send + 'static + Unpin;

impl<T> Bounded<T>
where
    T: Send + 'static + Unpin,
{
    /// Creates a bounded channel with specified type of sender and receiver
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new<S, R>(size: usize) -> (S, R)
    where
        S: SenderType<Array<T>> + Clone,
        R: ReceiverType<Array<T>> + Clone,
    {
        build::<Array<T>, S, R>(Array::<T>::new(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_blocking(size: usize) -> (MTx<Array<T>>, MRx<Array<T>>) {
        Self::new(size)
    }

    /// Creates a bounded channel with a pair of async sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_async(size: usize) -> (MAsyncTx<Array<T>>, MAsyncRx<Array<T>>) {
        Self::new(size)
    }

    /// Creates a bounded channel with a pair of blocking sender and async receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn blocking_async(size: usize) -> (MTx<Array<T>>, MAsyncRx<Array<T>>) {
        Self::new(size)
    }

    /// Creates a bounded channel with a pair of async sender and blocking receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn async_blocking(size: usize) -> (MAsyncTx<Array<T>>, MRx<Array<T>>) {
        Self::new(size)
    }
}
