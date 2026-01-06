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
use crate::flavor::{Flavor, FlavorBounded, FlavorMC, FlavorMP};
use crate::shared::*;
use crate::{ReceiverType, SenderType};
use std::marker::PhantomData;

/// Flavor Type alias for unbounded MPMC channel
pub type List<T> = crate::flavor::List<T>;

/// Flavor Type alias for bounded MPMC channel
pub type Array<T> = crate::flavor::Array<T, true, true>;

/// Flavor Type alias for one-size MPMC channel
pub type One<T> = crate::flavor::One<T>;

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
        build::<List<T>, MTx<List<T>>, MRx<List<T>>>(List::<T>::new())
    }

    #[inline]
    pub fn new_async() -> (MTx<List<T>>, MAsyncRx<List<T>>) {
        build::<List<T>, MTx<List<T>>, MAsyncRx<List<T>>>(List::<T>::new())
    }
}

pub struct Bounded<T, F = Array<T>>(PhantomData<fn(&T, &F)>)
where
    T: Send + 'static + Unpin,
    F: Flavor<Item = T> + FlavorBounded + FlavorMP + FlavorMC;

impl<T, F> Bounded<T, F>
where
    T: Send + 'static + Unpin,
    F: Flavor<Item = T> + FlavorBounded + FlavorMP + FlavorMC,
{
    /// Creates a bounded channel with specified type of sender and receiver
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new<S, R>(size: usize) -> (S, R)
    where
        S: SenderType<F> + Clone,
        R: ReceiverType<F> + Clone,
    {
        build::<F, S, R>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_blocking(size: usize) -> (MTx<F>, MRx<F>) {
        build::<F, MTx<F>, MRx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of async sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_async(size: usize) -> (MAsyncTx<F>, MAsyncRx<F>) {
        build::<F, MAsyncTx<F>, MAsyncRx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and async receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn blocking_async(size: usize) -> (MTx<F>, MAsyncRx<F>) {
        build::<F, MTx<F>, MAsyncRx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of async sender and blocking receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn async_blocking(size: usize) -> (MAsyncTx<F>, MRx<F>) {
        build::<F, MAsyncTx<F>, MRx<F>>(F::new_with_bound(size))
    }
}
