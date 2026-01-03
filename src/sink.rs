use crate::share::*;
use crate::TrySendError;
use crate::{AsyncTx, MAsyncTx};
use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Deref;
use std::task::*;

/// An async sink that allows you to write custom futures with `poll_send(ctx)`.
pub struct AsyncSink<T> {
    tx: AsyncTx<T>,
    waker: Option<SendWaker<T>>,
}

impl<T> fmt::Debug for AsyncSink<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncSink")
    }
}

impl<T> fmt::Display for AsyncSink<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AsyncSink")
    }
}

impl<T> AsyncSink<T> {
    #[inline]
    pub fn new(tx: AsyncTx<T>) -> Self {
        Self { tx, waker: None }
    }
}

impl<T> Deref for AsyncSink<T> {
    type Target = AsyncTx<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<T: Unpin + Send + 'static> From<AsyncTx<T>> for AsyncSink<T> {
    #[inline]
    fn from(tx: AsyncTx<T>) -> Self {
        tx.into_sink()
    }
}

impl<T: Unpin + Send + 'static> From<MAsyncTx<T>> for AsyncSink<T> {
    #[inline]
    fn from(tx: MAsyncTx<T>) -> Self {
        tx.into_sink()
    }
}

impl<T: Send + Unpin + 'static> AsyncSink<T> {
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
    pub fn poll_send(&mut self, ctx: &mut Context, item: T) -> Result<(), TrySendError<T>> {
        let _item = MaybeUninit::new(item);
        let shared = &self.tx.shared;
        if shared.inner.try_send(&_item) {
            shared.on_send();
            return Ok(());
        }
        match self.tx.poll_send(ctx, &_item, &mut self.waker, true) {
            Poll::Ready(Ok(())) => Ok(()),
            Poll::Ready(Err(())) => Err(TrySendError::Disconnected(unsafe { _item.assume_init() })),
            Poll::Pending => Err(TrySendError::Full(unsafe { _item.assume_init() })),
        }
    }
}

impl<T> Drop for AsyncSink<T> {
    fn drop(&mut self) {
        self.tx.shared.abandon_send_waker(&mut self.waker);
    }
}
