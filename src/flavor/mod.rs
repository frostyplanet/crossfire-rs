use crate::waker_registry::*;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ops::Deref;

mod array;
pub(crate) use array::*;
mod list;
pub(crate) use list::*;
mod one;
pub(crate) use one::*;
mod one_spmc;
pub(crate) use one_spmc::*;

pub trait FlavorImpl: Send + 'static {
    type Item: Send + 'static + Unpin;

    fn len(&self) -> usize;

    fn capacity(&self) -> Option<usize>;

    fn is_full(&self) -> bool;

    fn is_empty(&self) -> bool;

    fn try_send(&self, item: &MaybeUninit<Self::Item>) -> bool;

    #[inline]
    fn try_send_oneshot(&self, _item: *const Self::Item) -> Option<bool> {
        unimplemented!()
    }

    fn try_recv(&self) -> Option<Self::Item>;

    #[inline(always)]
    fn try_recv_final(&self) -> Option<Self::Item> {
        if !self.is_empty() {
            self.try_recv()
        } else {
            None
        }
    }

    fn backoff_limit(&self) -> u16;

    #[inline(always)]
    fn may_direct_copy(&self) -> bool {
        false
    }
}

// because enum_dispatch does not support associate type
macro_rules! flavor_dispatch {
    ($wrap_method: ident)=>{
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

pub trait Flavor: Send + 'static + FlavorImpl {
    type Send: RegistrySend<Self::Item>;
    type Recv: RegistryRecv;
}

pub trait FlavorMP {}
pub trait FlavorMC {}

pub struct FlavorWrap<F: FlavorImpl, S, R> {
    inner: F,
    _phan: PhantomData<fn(&S, &R)>,
}

impl<F, S, R> From<F> for FlavorWrap<F, S, R>
where
    F: FlavorImpl,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    #[inline(always)]
    fn from(f: F) -> Self {
        Self { inner: f, _phan: Default::default() }
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

impl<F: FlavorImpl, R> FlavorMP for FlavorWrap<F, RegistryDummy, R> where R: RegistryRecv {}
impl<F: FlavorImpl, R> FlavorMP for FlavorWrap<F, RegistryMultiRecv, R> where R: RegistryRecv {}

impl<F: FlavorImpl, S> FlavorMC for FlavorWrap<F, S, RegistryMultiRecv> {}

macro_rules! wrap_new_type {
    ($self: expr, $method:ident $($arg:expr)*)=>{
        $self.inner.$method($($arg)*)
    };
}

impl<F, S, R> FlavorImpl for FlavorWrap<F, S, R>
where
    F: FlavorImpl,
    S: RegistrySend<F::Item>,
    R: RegistryRecv,
{
    type Item = F::Item;
    flavor_dispatch!(wrap_new_type);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn print_flavor_size() {
        //        println!("Flavor size {}", size_of::<Flavor<usize>>());
        println!("one size {}", size_of::<One<usize>>());
        println!("array size {}", size_of::<Array<usize, true, true>>());
        println!("list size {}", size_of::<List<usize>>());
    }
}
