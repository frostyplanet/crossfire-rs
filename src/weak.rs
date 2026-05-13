use crate::flavor::FlavorMP;
use crate::{shared::*, SenderType};
use std::mem::MaybeUninit;
use std::sync::Arc;

/// A weak reference of SenderType
///
/// Can be obtain from [MTx::downgrade](crate::MTx::downgrade) or [MAsyncTx::downgrade](crate::MAsyncTx::downgrade).
/// When the number of valid sender is non-zero, can try [upgrade](WeakTx::upgrade) to a [MTx](crate::MTx) or [MAsyncTx](crate::MAsyncTx).
pub struct WeakTx<F: Flavor + FlavorMP>(pub(crate) Arc<ChannelShared<F>>);

impl<F: Flavor + FlavorMP> WeakTx<F> {
    /// Upgrade to MTx or MAsyncTx (Only allow for mpsc or mpmc)
    ///
    /// # Safety
    ///
    /// Warning: You should **be careful using this on a bounded channel receiver-side**
    /// (Because when the channel is full and `send()` blocks while no one is receiving,
    /// will result in **dead-lock**)
    ///
    /// # Example
    ///
    /// ```
    /// use crossfire::*;
    /// let (tx, rx) = mpsc::bounded_blocking::<usize>(100);
    /// let weak_tx = tx.downgrade();
    /// let tx_clone = weak_tx.upgrade::<MTx<_>>().unwrap();
    /// drop(tx);
    /// drop(tx_clone);
    /// assert!(weak_tx.upgrade::<MTx<_>>().is_none());
    /// assert_eq!(weak_tx.get_tx_count(), 0);
    /// drop(rx);
    /// ```
    #[inline]
    pub fn upgrade<S: SenderType<Flavor = F>>(&self) -> Option<S> {
        if self.0.try_add_tx() {
            Some(S::new(self.0.clone()))
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.0.get_tx_count()
    }

    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.0.get_rx_count()
    }
}

impl<F: Flavor<Send = RegistryDummy> + FlavorMP> WeakTx<F> {
    /// Sends a message without checking channel closed first (Only available for **unbounded** channel)
    ///
    /// It's for scenario that receiver-side never exit early,
    /// but they holds a WeakTx to re-submit message.
    /// NOTE that it will not faster than `send()`,
    /// just useful save the operation of `upgrade()` if you don't care about the error.
    ///
    /// Always success, never blocked, no return type.
    ///
    /// # Safety
    ///
    /// It may result in message send successfully without anyone to receive them.
    /// You have to rely on the Drop trait of the message to cleanup.
    #[inline]
    pub unsafe fn send_unchecked(&self, item: F::Item) {
        let shared = &self.0;
        let _item = MaybeUninit::new(item);
        shared.inner.try_send(&_item);
        shared.on_send();
    }
}
