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
use crate::flavor::{flavor_dispatch, Flavor, FlavorImpl, FlavorMP, FlavorWrap};
use crate::shared::*;
use crate::{NotClonable, ReceiverType, SenderType};
use std::marker::PhantomData;
use std::mem::MaybeUninit;

/// Flavor Type alias for unbounded MPSC channel
pub type List<T> = FlavorWrap<crate::flavor::List<T>, RegistryDummy, RegistrySingle>;

/// Flavor Type alias for bounded MPSC channel wrapped with specified One impl
pub enum Array<T> {
    Array(crate::flavor::Array<T, true, false>),
    One(crate::flavor::One<T>),
}

impl<T: Send + Unpin + 'static> Array<T> {
    #[inline]
    pub fn new(size: usize) -> Self {
        if size <= 1 {
            Self::One(crate::flavor::One::new())
        } else {
            Self::Array(crate::flavor::Array::<T, true, false>::new(size))
        }
    }
}

impl<T> FlavorMP for Array<T> {}

macro_rules! wrap_array {
    ($self: expr, $method:ident $($arg:expr)*)=>{
        match $self {
            Self::Array(inner) => inner.$method($($arg)*),
            Self::One(inner) => inner.$method($($arg)*),
        }
    };
}

impl<T: Send + Unpin + 'static> FlavorImpl for Array<T> {
    type Item = T;
    flavor_dispatch!(wrap_array);
}

impl<T: Send + Unpin + 'static> Flavor for Array<T> {
    type Send = RegistryMultiSend<T>;
    type Recv = RegistrySingle;
}

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
    let shared = ChannelShared::new(flavor);
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
        build::<List<T>, MTx<List<T>>, R>(List::<T>::from(crate::flavor::List::<T>::new()))
    }

    #[inline]
    pub fn new_blocking() -> (MTx<List<T>>, Rx<List<T>>) {
        Self::new()
    }

    #[inline]
    pub fn new_async() -> (MTx<List<T>>, AsyncRx<List<T>>) {
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
        S: SenderType<Array<T>> + Clone,
        R: ReceiverType<Array<T>> + NotClonable,
    {
        build::<Array<T>, S, R>(Array::<T>::new(size))
    }

    /// Creates a bounded channel with a pair of blocking sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_blocking(size: usize) -> (MTx<Array<T>>, Rx<Array<T>>) {
        Self::new(size)
    }

    /// Creates a bounded channel with a pair of async sender and receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn new_async(size: usize) -> (MAsyncTx<Array<T>>, AsyncRx<Array<T>>) {
        Self::new(size)
    }

    /// Creates a bounded channel with a pair of blocking sender and async receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn blocking_async(size: usize) -> (MTx<Array<T>>, AsyncRx<Array<T>>) {
        Self::new(size)
    }

    /// Creates a bounded channel with a pair of async sender and blocking receiver.
    ///
    /// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
    #[inline]
    pub fn async_blocking(size: usize) -> (MAsyncTx<Array<T>>, Rx<Array<T>>) {
        Self::new(size)
    }
}
