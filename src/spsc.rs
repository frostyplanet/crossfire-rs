//! Single producer, single consumer.
//!
//! The optimization assumes a single producer and consumer, so waker registration is completely lockless.
//!
//! **NOTE**: For the SP/SC version, [AsyncTx], [AsyncRx], [Tx], and [Rx] are not `Clone` and do not implement `Sync`.
//! Although they can be moved to other threads, they are not allowed to be used with `send`/`recv` while in an `Arc`.
//!
//! The following code is OK:
//!
//! ``` rust
//! use crossfire::*;
//! async fn foo() {
//!     let (tx, rx) = spsc::Bounded::<usize>::new_async(100);
//!     tokio::spawn(async move {
//!          let _ = tx.send(2).await;
//!     });
//!     drop(rx);
//! }
//! ```
//!
//! Because the `AsyncTx` does not have the `Sync` marker, using `Arc<AsyncTx>` will lose the `Send` marker.
//!
//! For your safety, the following code **should not compile**:
//!
//! ``` compile_fail
//! use crossfire::*;
//! use std::sync::Arc;
//! async fn foo() {
//!     let (tx, rx) = spsc::Bounded::<usize>::new_async(100);
//!     let tx = Arc::new(tx);
//!     tokio::spawn(async move {
//!          let _ = tx.send(2).await;
//!     });
//!     drop(rx);
//! }
//! ```

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::flavor::{Flavor, FlavorBounded};
use crate::shared::*;
use crate::{NotClonable, ReceiverType, SenderType};
use std::marker::PhantomData;

/// Flavor Type alias for unbounded SPSC channel
pub type List<T> = crate::flavor::List<T>;

/// Flavor Type alias for bounded SPSC channel
pub type Array<T> = crate::flavor::Array<T, false, false>;

/// Flavor Type alias for one-size SPSC channel
pub type One<T> = crate::flavor::One<T>;

/// The generic builder for all spsc channel type.
///
/// Initialize sender and receiver types from a flavor type,
/// you can let the compiler to infer the type according to return type signature.
/// (the falvor might have diffrent new() method, but the rest is the same.
#[inline(always)]
pub fn build<F, S, R>(flavor: F) -> (S, R)
where
    F: Flavor,
    S: SenderType<F>,
    R: ReceiverType<F> + NotClonable,
{
    let send_wakers = if flavor.capacity().is_none() {
        RegistrySender::Dummy
    } else {
        RegistrySender::new_single()
    };
    let recv_wakers = RegistryRecv::new_single();
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
    pub fn new<R>() -> (Tx<List<T>>, R)
    where
        T: Send + 'static + Unpin,
        R: ReceiverType<List<T>> + NotClonable,
    {
        build::<List<T>, Tx<List<T>>, R>(List::<T>::new())
    }

    #[inline]
    pub fn new_blocking() -> (Tx<List<T>>, Rx<List<T>>) {
        build::<List<T>, Tx<List<T>>, Rx<List<T>>>(List::<T>::new())
    }

    #[inline]
    pub fn new_async() -> (Tx<List<T>>, AsyncRx<List<T>>) {
        build::<List<T>, Tx<List<T>>, AsyncRx<List<T>>>(List::<T>::new())
    }
}

pub struct Bounded<T, F = Array<T>>(PhantomData<fn(&T, &F)>)
where
    T: Send + 'static + Unpin,
    F: Flavor<Item = T> + FlavorBounded;

impl<T, F> Bounded<T, F>
where
    T: Send + 'static + Unpin,
    F: Flavor<Item = T> + FlavorBounded,
{
    /// Creates a bounded channel with specified type of sender and receiver
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new<S, R>(size: usize) -> (S, R)
    where
        S: SenderType<F>,
        R: ReceiverType<F> + NotClonable,
    {
        build::<F, S, R>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_blocking(size: usize) -> (Tx<F>, Rx<F>) {
        build::<F, Tx<F>, Rx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of async sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_async(size: usize) -> (AsyncTx<F>, AsyncRx<F>) {
        build::<F, AsyncTx<F>, AsyncRx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and async receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn blocking_async(size: usize) -> (Tx<F>, AsyncRx<F>) {
        build::<F, Tx<F>, AsyncRx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of async sender and blocking receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn async_blocking(size: usize) -> (AsyncTx<F>, Rx<F>) {
        build::<F, AsyncTx<F>, Rx<F>>(F::new_with_bound(size))
    }
}
