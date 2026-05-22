//! Multiple producers, single consumer.
//!
//! The optimization assumes a single consumer. The waker registration of the receiver is lossless compared to `mpmc`.
//!
//! **NOTE**:
//! - [AsyncRx] and [Rx] are not `Clone` and do not implement `Sync`.
//!   Although they can be moved to other threads, they are not allowed to be used with `send`/`recv` while in an `Arc`.
//! - [MAsyncTx] and [MTx] are `Clone` and `Sync`
//!
//! The following code is OK:
//!
//! ``` rust
//! use crossfire::*;
//! async fn foo() {
//!     let (tx, rx) = mpsc::bounded_async::<usize>(100);
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
//!     let (tx, rx) = mpsc::bounded_async::<usize>(100);
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
use crate::flavor::{
    flavor_dispatch, flavor_select_dispatch, queue_dispatch, Flavor, FlavorBounded, FlavorImpl,
    FlavorMP, FlavorNew, FlavorWrap, Queue,
};
use crate::null::CloseHandle;
use crate::shared::*;
use crate::{NotCloneable, ReceiverType, SenderType};
use std::mem::MaybeUninit;

/// Flavor Type alias for unbounded MPSC channel
pub type List<T> = FlavorWrap<crate::flavor::List<T>, RegistryDummy, RegistrySingle>;

/// Flavor type for one-sized MPSC channel
pub type One<T> = FlavorWrap<crate::flavor::One<T>, RegistryMulti, RegistrySingle>;

/// Flavor Type alias for bounded MPSC channel wrapped with specified One impl
#[allow(clippy::large_enum_variant)]
pub enum Array<T> {
    Array(crate::flavor::ArrayMpsc<T>),
    One(crate::flavor::OneMpsc<T>),
}

impl<T> Array<T> {
    #[inline]
    pub fn new(size: usize) -> Self {
        if size <= 1 {
            Self::One(crate::flavor::OneMpsc::new())
        } else {
            Self::Array(crate::flavor::ArrayMpsc::<T>::new(size))
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

impl<T> Queue for Array<T> {
    type Item = T;
    queue_dispatch!(wrap_array);
}

impl<T> FlavorImpl for Array<T> {
    const IS_BOUNDED: bool = true;
    flavor_dispatch!(wrap_array);
}

impl<T> FlavorSelect for Array<T> {
    flavor_select_dispatch!(wrap_array);
}

impl<T> FlavorBounded for Array<T> {
    #[inline(always)]
    fn new_with_bound(size: usize) -> Self {
        Self::new(size)
    }
}

impl<T: 'static> Flavor for Array<T> {
    type Send = RegistryMulti;
    type Recv = RegistrySingle;
}

/// The generic builder for all mpsc channel types with a new method (except Array).
///
/// Initialize sender and receiver types from a flavor type,
/// you can let the compiler to infer the type according to return type signature.
/// (the falvor might have different new() method, but the rest is the same.
/// # Examples
///
/// ```rust
/// use crossfire::*;
/// let (tx, rx): (MTx<_>, Rx<_>) = mpsc::new::<mpsc::List<i32>, _, _>();
/// let (tx, rx): (MAsyncTx<mpsc::One<usize>>, Rx<mpsc::One<usize>>) = mpsc::new();
/// ```
#[inline(always)]
pub fn new<F, S, R>() -> (S, R)
where
    F: Flavor + FlavorNew + FlavorMP,
    S: SenderType<Flavor = F> + Clone,
    R: ReceiverType<Flavor = F> + NotCloneable,
{
    build::<F, S, R>(F::new())
}

/// The generic builder for all mpsc channel types
///
/// Initialize sender and receiver types from a flavor type,
/// you can let the compiler to infer the type according to return type signature.
/// (the flavor might have different new() method, but the rest is the same.
///
/// # Examples
///
/// ```rust
/// use crossfire::{*, mpsc::*};
/// let (tx, rx): (MTx<_>, Rx<_>) = build::<List<i32>, _, _>(List::new());
/// let (tx, rx): (MAsyncTx<One<usize>>, Rx<One<usize>>)  = build(One::new());
/// ```
#[inline(always)]
pub fn build<F, S, R>(flavor: F) -> (S, R)
where
    F: Flavor + FlavorMP,
    S: SenderType<Flavor = F> + Clone,
    R: ReceiverType<Flavor = F> + NotCloneable,
{
    let shared = ChannelShared::new(flavor, F::Send::new(), F::Recv::new());
    (S::new(shared), R::new(shared))
}

#[inline]
fn unbounded_new<T, R>() -> (MTx<List<T>>, R)
where
    T: 'static,
    R: ReceiverType<Flavor = List<T>> + NotCloneable,
{
    build::<List<T>, MTx<List<T>>, R>(List::<T>::from_inner(crate::flavor::List::<T>::new()))
}

#[inline]
pub fn unbounded_blocking<T>() -> (MTx<List<T>>, Rx<List<T>>)
where
    T: 'static,
{
    unbounded_new()
}

#[inline]
pub fn unbounded_async<T>() -> (MTx<List<T>>, AsyncRx<List<T>>)
where
    T: 'static,
{
    unbounded_new()
}

fn bounded_new<T, S, R>(size: usize) -> (S, R)
where
    T: 'static,
    S: SenderType<Flavor = Array<T>> + Clone,
    R: ReceiverType<Flavor = Array<T>> + NotCloneable,
{
    build::<Array<T>, S, R>(Array::<T>::new(size))
}

/// Creates a bounded channel with a pair of blocking sender and receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_blocking<T>(size: usize) -> (MTx<Array<T>>, Rx<Array<T>>)
where
    T: 'static,
{
    bounded_new(size)
}

/// Creates a bounded channel with a pair of async sender and receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_async<T>(size: usize) -> (MAsyncTx<Array<T>>, AsyncRx<Array<T>>)
where
    T: 'static,
{
    bounded_new(size)
}

/// Creates a bounded channel with a pair of blocking sender and async receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_blocking_async<T>(size: usize) -> (MTx<Array<T>>, AsyncRx<Array<T>>)
where
    T: 'static,
{
    bounded_new(size)
}

/// Creates a bounded channel with a pair of async sender and blocking receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_async_blocking<T>(size: usize) -> (MAsyncTx<Array<T>>, Rx<Array<T>>)
where
    T: 'static,
{
    bounded_new(size)
}

/// Flavor type for close notification, refer to [crate::null] for usage
pub type Null = FlavorWrap<crate::null::Null, RegistryDummy, RegistrySingle>;

impl Null {
    #[inline(always)]
    pub fn new_blocking(&self) -> (CloseHandle<Null>, Rx<Null>) {
        new()
    }

    #[inline(always)]
    pub fn new_async(self) -> (CloseHandle<Null>, AsyncRx<Null>) {
        new()
    }
}
