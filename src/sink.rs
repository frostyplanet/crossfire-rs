use crate::shared::*;
use crate::TrySendError;
use crate::{AsyncTx, MAsyncTx};
use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::task::*;

/// An async sink that allows you to write custom futures with `poll_send(ctx)`.
pub struct AsyncSink<F: Flavor> {
    tx: AsyncTx<F>,
    waker: Option<<F::Send as Registry>::Waker>,
}

impl<F: Flavor> fmt::Debug for AsyncSink<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncSink")
    }
}

impl<F: Flavor> fmt::Display for AsyncSink<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncSink")
    }
}

impl<F: Flavor> AsyncSink<F> {
    #[inline]
    pub fn new(tx: AsyncTx<F>) -> Self {
        Self { tx, waker: None }
    }
}

impl<F: Flavor> Deref for AsyncSink<F> {
    type Target = AsyncTx<F>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<F: Flavor> From<AsyncTx<F>> for AsyncSink<F> {
    #[inline]
    fn from(tx: AsyncTx<F>) -> Self {
        tx.into_sink()
    }
}

impl<F: Flavor> From<MAsyncTx<F>> for AsyncSink<F> {
    #[inline]
    fn from(tx: MAsyncTx<F>) -> Self {
        tx.into_sink()
    }
}

impl<F: Flavor> AsyncSink<F>
where
    F::Item: Send + 'static + Unpin,
{
    /// `poll_send()` will try to send a message.
    /// If the channel is full, it will register a notification for the next poll.
    ///
    /// # Behavior
    ///
    /// The polling behavior is different from [SendFuture](crate::SendFuture).
    /// Because the waker is not exposed to the user, you cannot perform delicate operations on
    /// the waker (compared to the `Drop` handler in `SendFuture`).
    /// To make sure no deadlock happens on cancellation, the `WakerState` will be `Init`
    /// after being registered (and will not be converted to `Waiting`).
    /// The receivers will wake up all `Init` state wakers until they find a normal
    /// pending sender in the `Waiting` state.
    ///
    /// # Return value:
    ///
    /// Returns `Ok(())` on message sent.
    ///
    /// Returns `Err([crate::TrySendError::Full])` for a `Poll::Pending` case.
    /// The next time the channel is not full, your future will be woken again.
    /// You should then continue calling `poll_send()` to send the message.
    /// If you want to cancel, just don't call `poll_send()` again. There are no side effects,
    /// and other senders will have a chance to send their messages.
    ///
    /// Returns `Err([crate::TrySendError::Disconnected])` when all `Rx` are dropped.
    #[inline]
    pub fn poll_send(
        &mut self, ctx: &mut Context, item: F::Item,
    ) -> Result<(), TrySendError<F::Item>> {
        let _item = MaybeUninit::new(item);
        let shared = &self.tx.shared;
        if shared.inner.try_send(&_item) {
            shared.on_send();
            return Ok(());
        }
        match self.tx.poll_send::<true>(ctx, &_item, &mut self.waker) {
            Poll::Ready(Ok(())) => Ok(()),
            Poll::Ready(Err(())) => Err(TrySendError::Disconnected(unsafe { _item.assume_init() })),
            Poll::Pending => Err(TrySendError::Full(unsafe { _item.assume_init() })),
        }
    }
}

impl<F: Flavor> Drop for AsyncSink<F> {
    fn drop(&mut self) {
        if let Some(waker) = self.waker.as_ref() {
            self.tx.shared.abandon_send_waker(waker);
        }
    }
}
