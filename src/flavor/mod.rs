use crate::waker_registry::*;
use std::mem::MaybeUninit;

mod array;
pub(crate) use array::*;
mod list;
pub(crate) use list::*;
mod one;
pub(crate) use crate::crossbeam::err::*;
pub(crate) use one::*;

#[enum_dispatch::enum_dispatch]
pub(crate) trait FlavorImpl<T> {
    fn len(&self) -> usize;

    fn capacity(&self) -> Option<usize>;

    /// NOTE: this does not detect disconnected
    fn is_full(&self) -> bool;

    /// NOTE: this does not detect disconnected
    fn is_empty(&self) -> bool;

    /// Return Ok(true) for sent, Ok(false) for disconnected, Err(()) for try again
    /// NOTE: this is not a sync point, TrySendErr::Full means try again
    fn try_send(&self, item: &MaybeUninit<T>) -> Result<(), TrySendErr>;

    /// None for try again
    #[inline]
    fn try_send_oneshot(&self, _item: *const T) -> Option<Result<(), TrySendErr>> {
        unimplemented!()
    }

    fn try_recv(&self) -> Result<T, TryRecvError>;

    /// try recv before park
    #[inline(always)]
    fn try_recv_final(&self) -> Result<T, TryRecvError> {
        self.try_recv()
    }

    fn close(&self) -> bool;

    fn backoff_limit(&self) -> u16;

    fn may_direct_copy(&self) -> bool;
}

pub(crate) trait FlavorPrivate<T> {
    fn to_flavor(self) -> Flavor<T>;

    fn new_reg_sender<const MP: bool>(&self) -> RegistrySender<T>;

    fn new_reg_recv<const MC: bool>(&self) -> RegistryRecv;
}

#[enum_dispatch::enum_dispatch(FlavorImpl<T>)]
pub enum Flavor<T> {
    List(List<T>),
    One(OneSize<T>),
    ArraySPSC(Array<T, false, false>),
    ArrayMPMC(Array<T, true, true>),
}

/// This name differ from crossbeam error, it does not have T in it
#[derive(PartialEq, Eq, Debug)]
#[repr(u8)]
pub(crate) enum TrySendErr {
    Full = 0,
    Disconnected = 1,
}

impl TrySendErr {
    #[inline]
    pub fn is_full(&self) -> bool {
        *self == Self::Full
    }

    #[inline]
    pub fn to_try_send<T>(self, item: MaybeUninit<T>) -> TrySendError<T> {
        match self {
            Self::Full => {
                return TrySendError::Full(unsafe { item.assume_init_read() });
            }
            Self::Disconnected => {
                return TrySendError::Disconnected(unsafe { item.assume_init_read() });
            }
        }
    }

    #[inline]
    pub fn to_send<T>(self, item: MaybeUninit<T>) -> SendError<T> {
        return SendError(unsafe { item.assume_init_read() });
    }

    #[inline]
    pub fn to_timeout<T>(self, item: MaybeUninit<T>) -> SendTimeoutError<T> {
        return SendTimeoutError::Disconnected(unsafe { item.assume_init_read() });
    }
}
