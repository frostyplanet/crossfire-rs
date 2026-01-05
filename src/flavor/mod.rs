use crate::waker_registry::*;
use std::mem::MaybeUninit;

mod array;
pub(crate) use array::*;
mod list;
pub(crate) use list::*;
mod one;
pub(crate) use one::*;

#[enum_dispatch::enum_dispatch]
pub(crate) trait FlavorImpl<T: Send + 'static>: Send + Sync + 'static {
    fn len(&self) -> usize;

    fn capacity(&self) -> Option<usize>;

    fn is_full(&self) -> bool;

    fn is_empty(&self) -> bool;

    fn try_send(&self, item: &MaybeUninit<T>) -> bool;

    #[inline]
    fn try_send_oneshot(&self, _item: *const T) -> Option<bool> {
        unimplemented!()
    }

    fn try_recv(&self) -> Option<T>;

    #[inline(always)]
    fn try_recv_final(&self) -> Option<T> {
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

    #[inline(always)]
    fn get_ptr(&self) -> *const () {
        self as *const Self as *const ()
    }
}

pub(crate) trait FlavorPrivate<T: Send + 'static> {
    fn to_flavor(self) -> Flavor<T>;

    fn new_reg_sender<const MP: bool>(&self) -> RegistrySender<T>;

    fn new_reg_recv<const MC: bool>(&self) -> RegistryRecv;
}

#[enum_dispatch::enum_dispatch(FlavorImpl<T>)]
pub enum Flavor<T: Send + 'static> {
    ArrayMPMC(Array<T, true, true>),
    ArraySPSC(Array<T, false, false>),
    List(List<T>),
    One(OneSize<T>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn print_flavor_size() {
        println!("Flavor size {}", size_of::<Flavor<usize>>());
        println!("one size {}", size_of::<OneSize<usize>>());
        println!("array size {}", size_of::<Array<usize, true, true>>());
        println!("list size {}", size_of::<List<usize>>());
    }
}
