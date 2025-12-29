use enum_dispatch::enum_dispatch;
use std::mem::MaybeUninit;

mod array;
pub use array::*;
mod list;
pub use list::*;

#[enum_dispatch]
pub trait FlavorImpl<T> {
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
}

#[enum_dispatch(FlavorImpl<T>)]
pub enum Flavor<T> {
    List(List<T>),
    Array(Array<T>),
}
