use crate::waker_registry::*;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;

pub mod array;
pub use array::Array;
mod array_mpsc;
pub use array_mpsc::ArrayMpsc;
mod array_spsc;
pub use array_spsc::ArraySpsc;
mod list;
pub use list::*;
mod one;
pub use one::*;
mod one_mpsc;
pub use one_mpsc::OneMpsc;
mod one_spmc;
pub use one_spmc::OneSpsc;

/// Essential struct for select and read interface
pub(crate) struct Token {
    pub(crate) pos: *const u8,
    pub(crate) stamp: usize,
}

impl Token {
    #[inline]
    pub(crate) fn new(pos: *const u8, stamp: usize) -> Self {
        Self { pos, stamp }
    }
}

impl Default for Token {
    #[inline]
    fn default() -> Self {
        Self { pos: std::ptr::null_mut(), stamp: 0 }
    }
}

// The queue trait should be public because AsyncStream, AsyncRx ... all use it's associate type `Item`
/// Trait for lockless queue, it's safe to use if you don't want the channel mechanisms
pub trait Queue: Send + 'static {
    type Item: Send + 'static + Unpin;

    fn pop(&self) -> Option<Self::Item>;

    fn push(&self, item: Self::Item) -> Result<(), Self::Item>;

    fn len(&self) -> usize;

    fn capacity(&self) -> Option<usize>;

    fn is_full(&self) -> bool;

    fn is_empty(&self) -> bool;
}

/// Internal flavor interface
pub(crate) trait FlavorImpl: Send + 'static + Queue {
    fn try_send(&self, item: &MaybeUninit<Self::Item>) -> bool;

    #[inline]
    fn try_send_oneshot(&self, _item: *const Self::Item) -> Option<bool> {
        unimplemented!()
    }

    fn try_recv(&self) -> Option<Self::Item>;

    fn try_recv_final(&self) -> Option<Self::Item>;

    fn backoff_limit(&self) -> u16;

    #[inline(always)]
    fn may_direct_copy(&self) -> bool {
        false
    }
}

pub(crate) trait FlavorSelect: Queue {
    /// Note: this is internal function, it does not check if the token has other result
    fn try_select(&self, final_check: bool) -> Option<Token>;

    /// Note: this is internal function, it does not check if the token is valid
    fn read_with_token(&self, token: Token) -> Self::Item;
}

// because enum_dispatch does not support associate type
macro_rules! queue_dispatch {
    ($wrap_method: ident)=>{
        #[inline(always)]
        fn pop(&self) -> Option<Self::Item> {
            $wrap_method!(self, pop)
        }

        #[inline(always)]
        fn push(&self, item: Self::Item) -> Result<(), Self::Item> {
            $wrap_method!(self, push item)
        }

        #[inline(always)]
        fn len(&self) -> usize {
            $wrap_method!(self, len)
        }

        #[inline(always)]
        fn capacity(&self) -> Option<usize> {
            $wrap_method!(self, capacity)
        }

        #[inline(always)]
        fn is_full(&self) -> bool {
            $wrap_method!(self, is_full)
        }

        #[inline(always)]
        fn is_empty(&self) -> bool {
            $wrap_method!(self, is_empty)
        }
    };
}
pub(super) use queue_dispatch;

// because enum_dispatch does not support associate type
macro_rules! flavor_dispatch {
    ($wrap_method: ident)=>{
        #[inline(always)]
        fn try_send(&self, item: &MaybeUninit<Self::Item>) -> bool {
            $wrap_method!(self, try_send item)
        }

        #[inline]
        fn try_send_oneshot(&self, _item: *const Self::Item) -> Option<bool> {
            $wrap_method!(self, try_send_oneshot _item)
        }

        #[inline(always)]
        fn try_recv(&self) -> Option<Self::Item> {
            $wrap_method!(self, try_recv)
        }

        #[inline(always)]
        fn try_recv_final(&self) -> Option<Self::Item> {
            $wrap_method!(self, try_recv_final)
        }

        #[inline(always)]
        fn backoff_limit(&self) -> u16 {
            $wrap_method!(self, backoff_limit)
        }

        #[inline(always)]
        fn may_direct_copy(&self) -> bool {
            $wrap_method!(self, may_direct_copy)
        }
    };
}
pub(super) use flavor_dispatch;

// because enum_dispatch does not support associate type
macro_rules! flavor_select_dispatch {
    ($wrap_method: ident) => {
        #[inline(always)]
        fn try_select(&self, final_check: bool) -> Option<Token> {
            $wrap_method!(self, try_select final_check)
        }

        #[inline(always)]
        fn read_with_token(&self, token: Token) -> Self::Item {
            $wrap_method!(self, read_with_token token)
        }
    };
}
#[allow(unused_imports)]
pub(super) use flavor_select_dispatch;

pub trait Flavor: Send + 'static + FlavorImpl {
    type Send: RegistrySend<Self::Item>;
    type Recv: RegistryRecv;
}

pub trait FlavorMP {}
pub trait FlavorMC {}

pub trait FlavorNew {
    fn new() -> Self;
}

pub trait FlavorBounded {
    fn new_with_bound(size: usize) -> Self;
}

/// A type wrapper for channel flavor
pub struct FlavorWrap<F: FlavorImpl, S, R> {
    inner: F,
    _phan: PhantomData<fn(&S, &R)>,
}

impl<F, S, R> FlavorWrap<F, S, R>
where
    F: FlavorImpl,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    #[inline(always)]
    pub fn new() -> Self
    where
        F: FlavorNew,
    {
        Self::from_inner(<F as FlavorNew>::new())
    }

    #[inline(always)]
    pub(crate) fn from_inner(f: F) -> Self {
        Self { inner: f, _phan: Default::default() }
    }
}

impl<F, S, R> FlavorNew for FlavorWrap<F, S, R>
where
    F: FlavorImpl + FlavorNew,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    #[inline(always)]
    fn new() -> Self {
        Self::from_inner(<F as FlavorNew>::new())
    }
}

impl<F, S, R> FlavorBounded for FlavorWrap<F, S, R>
where
    F: FlavorImpl + FlavorBounded,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    #[inline(always)]
    fn new_with_bound(size: usize) -> Self {
        Self::from_inner(<F as FlavorBounded>::new_with_bound(size))
    }
}

impl<F, S, R> Flavor for FlavorWrap<F, S, R>
where
    F: FlavorImpl,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    type Send = S;
    type Recv = R;
}

impl<F, S, R> Deref for FlavorWrap<F, S, R>
where
    F: FlavorImpl,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    type Target = F;

    #[inline(always)]
    fn deref(&self) -> &F {
        &self.inner
    }
}

impl<F, R> FlavorMP for FlavorWrap<F, RegistryDummy, R>
where
    F: FlavorImpl,
    R: RegistryRecv,
{
}
impl<T, F, R> FlavorMP for FlavorWrap<F, RegistryMultiSend<T>, R>
where
    T: Send + Unpin + 'static,
    F: FlavorImpl,
    R: RegistryRecv,
{
}

impl<F: FlavorImpl, S> FlavorMC for FlavorWrap<F, S, RegistryMultiRecv> {}

macro_rules! wrap_new_type {
    ($self: expr, $method:ident $($arg:expr)*)=>{
        $self.inner.$method($($arg)*)
    };
}

impl<F, S, R> Queue for FlavorWrap<F, S, R>
where
    F: FlavorImpl,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    type Item = F::Item;
    queue_dispatch!(wrap_new_type);
}

impl<F, S, R> FlavorImpl for FlavorWrap<F, S, R>
where
    F: FlavorImpl,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    flavor_dispatch!(wrap_new_type);
}

impl<F, S, R> FlavorSelect for FlavorWrap<F, S, R>
where
    F: FlavorImpl + FlavorSelect,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    flavor_select_dispatch!(wrap_new_type);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn print_flavor_size() {
        //        println!("Flavor size {}", size_of::<Flavor<usize>>());
        println!("one size {}", size_of::<One<usize>>());
        println!("array size {}", size_of::<Array<usize>>());
        println!("list size {}", size_of::<List<usize>>());
    }
}
