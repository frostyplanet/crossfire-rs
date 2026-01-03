use std::mem::MaybeUninit;

mod array;
pub(crate) use array::*;
mod list;
pub(crate) use list::*;
mod one;
pub(crate) use one::*;
mod one_spmc;
pub(crate) use one_spmc::*;

pub trait Flavor: Send + 'static {
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

pub trait FlavorMP {}
pub trait FlavorMC {}

// because enum_dispatch does not support associate type
macro_rules! flavor_enum_dispatch {
    ($cls: ident, $wrap_method: ident)=>{
        impl<T: Send + Unpin + 'static> Flavor for $cls<T> {
            type Item = T;

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
        }
    };
}
pub(super) use flavor_enum_dispatch;

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
