use std::mem::MaybeUninit;

mod array;
pub use array::*;
mod list;
use crate::waker_registry::*;
pub use list::*;

#[enum_dispatch::enum_dispatch]
pub(crate) trait FlavorImpl<T> {
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

    fn backoff_limit(&self) -> u16;

    #[inline(always)]
    fn may_direct_copy(&self) -> bool {
        false
    }
}

pub(crate) trait FlavorPrivate<T> {
    fn to_flavor(self) -> crate::flavor::Flavor<T>;

    fn new_reg_sender<const MP: bool>(&self) -> RegistrySender<T>;

    fn new_reg_recv<const MC: bool>(&self) -> RegistryRecv;
}

#[enum_dispatch::enum_dispatch(FlavorImpl<T>)]
pub enum Flavor<T> {
    List(List<T>),
    Array(Array<T>),
}
