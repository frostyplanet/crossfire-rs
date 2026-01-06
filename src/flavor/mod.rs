use std::mem::MaybeUninit;

mod array;
pub(crate) use array::*;
mod list;
pub(crate) use list::*;
mod one;
pub(crate) use one::*;

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

pub trait FlavorBounded: Flavor {
    fn new_with_bound(size: usize) -> Self;
}

pub trait FlavorMP {}
pub trait FlavorMC {}

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
