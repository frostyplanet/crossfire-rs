//! Multiple producers, single consumer.
//!
//! The optimization assumes a single consumer. The waker registration of the receiver is lossless compared to `mpmc`.
//!
//! **NOTE**: For the SC (single consumer) version, [AsyncRx] and [Rx] are not `Clone` and do not implement `Sync`.
//! Although they can be moved to other threads, they are not allowed to be used with `send`/`recv` while in an `Arc`.
//!
//! The following code is OK:
//!
//! ``` rust
//! use crossfire::*;
//! async fn foo() {
//!     let (tx, rx) = mpsc::Bounded::<usize>::new_async(100);
//!     tokio::spawn(async move {
//!         let _ = rx.recv().await;
//!     });
//!     drop(tx);
//! }
//! ```
//!
//! Because the `AsyncRx` does not have the `Sync` marker, using `Arc<AsyncRx>` will lose the `Send` marker.
//!
//! For your safety, the following code **should not compile**:
//!
//! ``` compile_fail
//! use crossfire::*;
//! use std::sync::Arc;
//! async fn foo() {
//!     let (tx, rx) = mpsc::Bounded::<usize>::new_async(100);
//!     let rx = Arc::new(rx);
//!     tokio::spawn(async move {
//!         let _ = rx.recv().await;
//!     });
//!     drop(tx);
//! }
//! ```

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::flavor::{Flavor, FlavorBounded, FlavorMP};
use crate::shared::*;
use crate::{NotClonable, ReceiverType, SenderType};
use std::marker::PhantomData;

/// Flavor Type alias for unbounded MPSC channel
pub type List<T> = crate::flavor::List<T>;

/// Flavor Type alias for bounded MPSC channel
pub type Array<T> = crate::flavor::Array<T, true, true>;

/// Flavor Type alias for one-size MPSC channel
pub type One<T> = crate::flavor::One<T>;

/// The generic builder for all mpsc channel type.
///
/// Initialize sender and receiver types from a flavor type,
/// you can let the compiler to infer the type according to return type signature.
/// (the flavor might have different new() method, but the rest is the same.
///
/// # Examples
///
/// Create an unbounded channel with blocking receiver:
/// ```rust
/// use crossfire::mpsc;
/// let (tx, rx) = mpsc::Unbounded::<i32>::new_blocking();
/// ```
///
/// Create an unbounded channel with async receiver:
/// ```rust
/// use crossfire::mpsc;
/// let (tx, rx) = mpsc::Unbounded::<i32>::new_async();
/// ```
///
/// Create a bounded channel with custom sender and receiver types:
/// ```rust
/// use crossfire::mpsc;
/// let (tx, rx) = mpsc::Bounded::<i32>::new::<crossfire::MTx<_>, crossfire::Rx<_>>(10);
/// ```
#[inline(always)]
pub fn build<F, S, R>(flavor: F) -> (S, R)
where
    F: Flavor + FlavorMP,
    S: SenderType<F> + Clone,
    R: ReceiverType<F> + NotClonable,
{
    let send_wakers = if flavor.capacity().is_none() {
        RegistrySender::Dummy
    } else {
        RegistrySender::new_multi()
    };
    let recv_wakers = RegistryRecv::new_single();
    let shared = ChannelShared::new(flavor, send_wakers, recv_wakers);
    (S::new(shared.clone()), R::new(shared))
}

/// Creates an unbounded channel for use in a blocking context.
///
/// The sender will never block, so we use the same `Tx` for all threads.
///
/// # Examples
///
/// ```rust
/// use crossfire::mpsc;
/// let (tx, rx) = mpsc::Unbounded::<i32>::new_blocking();
/// tx.send(42).unwrap();
/// assert_eq!(rx.recv(), Ok(42));
/// ```
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
        R: ReceiverType<List<T>> + NotClonable,
    {
        build::<List<T>, MTx<List<T>>, R>(List::<T>::new())
    }

    #[inline]
    pub fn new_blocking() -> (MTx<List<T>>, Rx<List<T>>) {
        build::<List<T>, MTx<List<T>>, Rx<List<T>>>(List::<T>::new())
    }

    #[inline]
    pub fn new_async() -> (MTx<List<T>>, AsyncRx<List<T>>) {
        build::<List<T>, MTx<List<T>>, AsyncRx<List<T>>>(List::<T>::new())
    }
}

pub struct Bounded<T, F = Array<T>>(PhantomData<fn(&T, &F)>)
where
    T: Send + 'static + Unpin,
    F: Flavor<Item = T> + FlavorBounded + FlavorMP;

impl<T, F> Bounded<T, F>
where
    T: Send + 'static + Unpin,
    F: Flavor<Item = T> + FlavorBounded + FlavorMP,
{
    /// Creates a bounded channel with specified type of sender and receiver
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use crossfire::mpsc;
    /// let (tx, rx) = mpsc::Bounded::<i32>::new::<crossfire::MTx<_>, crossfire::Rx<_>>(10);
    /// tx.send(42).unwrap();
    /// assert_eq!(rx.recv(), Ok(42));
    /// ```
    #[inline]
    pub fn new<S, R>(size: usize) -> (S, R)
    where
        S: SenderType<F> + Clone,
        R: ReceiverType<F> + NotClonable,
    {
        build::<F, S, R>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_blocking(size: usize) -> (MTx<F>, Rx<F>) {
        build::<F, MTx<F>, Rx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of async sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_async(size: usize) -> (MAsyncTx<F>, AsyncRx<F>) {
        build::<F, MAsyncTx<F>, AsyncRx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and async receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn blocking_async(size: usize) -> (MTx<F>, AsyncRx<F>) {
        build::<F, MTx<F>, AsyncRx<F>>(F::new_with_bound(size))
    }

    /// Creates a bounded channel with a pair of async sender and blocking receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn async_blocking(size: usize) -> (MAsyncTx<F>, Rx<F>) {
        build::<F, MAsyncTx<F>, Rx<F>>(F::new_with_bound(size))
    }
}
