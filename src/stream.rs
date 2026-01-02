use crate::shared::*;
use crate::{AsyncRx, MAsyncRx};
use futures_core::stream;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::task::*;

/// Constructed by [AsyncRx::into_stream()](crate::AsyncRx::into_stream())
///
/// Implements `futures_core::stream::Stream`.
pub struct AsyncStream<F: Flavor> {
    rx: AsyncRx<F>,
    waker: Option<RecvWaker>,
    ended: bool,
}

impl<F: Flavor> fmt::Debug for AsyncStream<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncStream")
    }
}

impl<F: Flavor> fmt::Display for AsyncStream<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncStream")
    }
}

impl<F: Flavor> AsyncStream<F> {
    #[inline(always)]
    pub fn new(rx: AsyncRx<F>) -> Self {
        Self { rx, waker: None, ended: false }
    }

    /// `poll_item()` will try to receive a message.
    /// If the channel is empty, it will register a notification for the next poll.
    ///
    /// # Behavior
    ///
    /// The polling behavior is different from [RecvFuture](crate::RecvFuture).
    /// Because the waker is not exposed to the user, you cannot perform delicate operations on
    /// the waker (compared to the `Drop` handler in `RecvFuture`).
    /// To make sure no deadlock happens on cancellation, the `WakerState` will be `Init`
    /// after being registered (and will not be converted to `Waiting`).
    /// The senders will wake up all `Init` state wakers until they find a normal
    /// pending receiver in the `Waiting` state.
    ///
    /// # Return Value:
    ///
    /// Returns `Ok(T)` on success.
    ///
    /// Returns Err([TryRecvError::Empty]) for a `Poll::Pending` case.
    /// The next time the channel is not empty, your future will be woken again.
    /// You should then continue calling `poll_item()` to receive the message.
    /// If you want to cancel, just don't call `poll_item()` again. Others will still have a chance
    /// to receive messages.
    ///
    /// Returns Err([TryRecvError::Disconnected]) if all `Tx` have been dropped and the channel is empty.
    #[inline]
    pub fn poll_item(&mut self, ctx: &mut Context) -> Poll<Option<F::Item>> {
        match self.rx.poll_item::<true>(ctx, &mut self.waker) {
            Ok(item) => Poll::Ready(Some(item)),
            Err(e) => {
                if e.is_empty() {
                    return Poll::Pending;
                }
                self.ended = true;
                return Poll::Ready(None);
            }
        }
    }
}

impl<F: Flavor> Deref for AsyncStream<F> {
    type Target = AsyncRx<F>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl<F: Flavor> stream::Stream for AsyncStream<F> {
    type Item = F::Item;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut _self = self.get_mut();
        if _self.ended {
            return Poll::Ready(None);
        }
        match _self.rx.poll_item::<false>(ctx, &mut _self.waker) {
            Ok(item) => Poll::Ready(Some(item)),
            Err(e) => {
                if e.is_empty() {
                    return Poll::Pending;
                }
                _self.ended = true;
                return Poll::Ready(None);
            }
        }
    }
}

impl<F: Flavor> stream::FusedStream for AsyncStream<F> {
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

impl<F: Flavor> Drop for AsyncStream<F> {
    fn drop(&mut self) {
        self.rx.shared.abandon_recv_waker(&mut self.waker);
    }
}

impl<F: Flavor> From<AsyncRx<F>> for AsyncStream<F> {
    #[inline]
    fn from(rx: AsyncRx<F>) -> Self {
        rx.into_stream()
    }
}

impl<F: Flavor> From<MAsyncRx<F>> for AsyncStream<F> {
    #[inline]
    fn from(rx: MAsyncRx<F>) -> Self {
        rx.into_stream()
    }
}
