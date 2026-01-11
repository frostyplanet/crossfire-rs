//! Multiple producers, multiple consumers.
//!
//! The optimization assumes multiple consumers. The waker registration of the receiver is less efficient compared to `mpsc`.
//!
//! **NOTE**: For the MC (multiple consumer) version, [AsyncRx] and [Rx] are `Clone` and implement `Sync`.
//! They can be safely used with `send`/`recv` while in an `Arc`.
//!
//! # Examples
//!
//! ```
//! use crossfire::*;
//! use std::thread;
//!
//! struct Worker {
//!     tx: MAsyncTx<mpmc::Array<usize>>,
//! }
//!
//! impl Worker {
//!     pub fn new() -> Self {
//!         // use type hint
//!         let (tx, rx): (MAsyncTx<_>, MRx<_>) = mpmc::build(mpmc::Array::<usize>::new(100));
//!         // equals to
//!         // let (tx, rx): (MAsyncTx<_>, MRx<_>) = mpmc::bounded_blocking::<usize>(100);
//!         for _ in 0..4 {
//!             let _rx = rx.clone();
//!             thread::spawn(move || {
//!                 match _rx.recv() {
//!                     Ok(item)=>{
//!                         println!("recv job {}", item);
//!                     }
//!                     Err(_)=>return,
//!                 }
//!             });
//!         }
//!         Self{
//!             tx,
//!         }
//!     }
//!     pub async fn submit(&self, msg: usize) {
//!         self.tx.send(msg).await.expect("send");
//!     }
//! }
//! ```

use crate::async_rx::*;
use crate::async_tx::*;
use crate::blocking_rx::*;
use crate::blocking_tx::*;
use crate::flavor::{
    flavor_dispatch, flavor_select_dispatch, Flavor, FlavorBounded, FlavorImpl, FlavorMC, FlavorMP,
    FlavorNew, FlavorWrap,
};
use crate::shared::*;
use crate::{ReceiverType, SenderType};
use std::mem::MaybeUninit;

/// Flavor Type for unbounded MPMC channel
pub type List<T> = FlavorWrap<crate::flavor::List<T>, RegistryDummy, RegistryMultiRecv>;

/// Flavor Type for one-sized MPMC channel
pub type One<T> = FlavorWrap<crate::flavor::One<T>, RegistryMultiSend<T>, RegistryMultiRecv>;

/// Flavor Type for bounded MPMC channel
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

impl<T: Send + Unpin + 'static> FlavorImpl for Array<T> {
    type Item = T;
    flavor_dispatch!(wrap_array);
}

impl<T: Send + Unpin + 'static> FlavorSelect for Array<T> {
    flavor_select_dispatch!(wrap_array);
}

impl<T: Send + Unpin + 'static> FlavorBounded for Array<T> {
    #[inline(always)]
    fn new_with_bound(size: usize) -> Self {
        Self::new(size)
    }
}

impl<T: Send + Unpin + 'static> Flavor for Array<T> {
    type Send = RegistryMultiSend<T>;
    type Recv = RegistryMultiRecv;
}

/// The generic builder for all mpmc channel types with a new method (except Array).
///
/// Initialize sender and receiver types from a flavor type,
/// you can let the compiler to infer the type according to return type signature.
/// (the falvor might have diffrent new() method, but the rest is the same.
/// # Examples
///
/// ```rust
/// use crossfire::*;
/// let (tx, rx): (MTx<_>, MRx<_>) = mpmc::new::<mpmc::List<i32>, _, _>();
/// let (tx, rx): (MAsyncTx<mpmc::One<usize>>, MRx<mpmc::One<usize>>) = mpmc::new();
/// ```
#[inline(always)]
pub fn new<F, S, R>() -> (S, R)
where
    F: Flavor + FlavorNew + FlavorMP + FlavorMC,
    S: SenderType<Flavor = F> + Clone,
    R: ReceiverType<Flavor = F> + Clone,
{
    build::<F, S, R>(F::new())
}

/// The generic builder for all mpmc channel types.
///
/// Initialize sender and receiver types from a flavor type,
/// you can let the compiler to infer the type according to return type signature.
/// (the falvor might have diffrent new() method, but the rest is the same.
///
/// # Examples
///
/// ```rust
/// use crossfire::{*, mpmc::*};
/// let (tx, rx): (MTx<_>, MRx<_>) = build::<List<i32>, _, _>(List::new());
/// let (tx, rx): (MAsyncTx<One<usize>>, MRx<One<usize>>)  = build(One::new());
/// ```
#[inline(always)]
pub fn build<F, S, R>(flavor: F) -> (S, R)
where
    F: Flavor + FlavorMP + FlavorMC,
    S: SenderType<Flavor = F> + Clone,
    R: ReceiverType<Flavor = F> + Clone,
{
    let shared = ChannelShared::new(flavor, F::Send::new(), F::Recv::new());
    (S::new(shared.clone()), R::new(shared))
}

#[inline]
fn unbounded_new<T, R>() -> (MTx<List<T>>, R)
where
    T: Send + 'static + Unpin,
    R: ReceiverType<Flavor = List<T>> + Clone,
{
    build::<List<T>, MTx<List<T>>, R>(List::<T>::from_inner(crate::flavor::List::<T>::new()))
}

#[inline]
pub fn unbounded_blocking<T>() -> (MTx<List<T>>, MRx<List<T>>)
where
    T: Send + 'static + Unpin,
{
    unbounded_new()
}

#[inline]
pub fn unbounded_async<T>() -> (MTx<List<T>>, MAsyncRx<List<T>>)
where
    T: Send + 'static + Unpin,
{
    unbounded_new()
}

fn bounded_new<T, S, R>(size: usize) -> (S, R)
where
    T: Send + 'static + Unpin,
    S: SenderType<Flavor = Array<T>> + Clone,
    R: ReceiverType<Flavor = Array<T>> + Clone,
{
    build::<Array<T>, S, R>(Array::<T>::new(size))
}

/// MPMC Bounded channel builder
///
/// # Examples
///
/// ```rust
/// use crossfire::{mpmc, *};
/// let (tx, rx) = mpmc::bounded_blocking::<i32>(10);
/// tx.send(42).unwrap();
/// assert_eq!(rx.recv(), Ok(42));
/// ```
/// Creates a bounded channel with a pair of blocking sender and receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_blocking<T>(size: usize) -> (MTx<Array<T>>, MRx<Array<T>>)
where
    T: Send + 'static + Unpin,
{
    bounded_new(size)
}

/// Creates a bounded channel with a pair of async sender and receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_async<T>(size: usize) -> (MAsyncTx<Array<T>>, MAsyncRx<Array<T>>)
where
    T: Send + 'static + Unpin,
{
    bounded_new(size)
}

/// Creates a bounded channel with a pair of blocking sender and async receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_blocking_async<T>(size: usize) -> (MTx<Array<T>>, MAsyncRx<Array<T>>)
where
    T: Send + 'static + Unpin,
{
    bounded_new(size)
}

/// Creates a bounded channel with a pair of async sender and blocking receiver.
///
/// As a special case, a channel size of 0 is not supported and will be treated as a channel of size 1.
#[inline]
pub fn bounded_async_blocking<T>(size: usize) -> (MAsyncTx<Array<T>>, MRx<Array<T>>)
where
    T: Send + 'static + Unpin,
{
    bounded_new(size)
}
