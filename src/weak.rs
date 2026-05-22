use crate::flavor::{FlavorMP, FlavorUnbounded};
use crate::{shared::*, SenderType};
use core::mem::MaybeUninit;
use core::ptr::NonNull;

/// A weak reference of SenderType
///
/// Can be obtain from [MTx::downgrade](crate::MTx::downgrade) or [MAsyncTx::downgrade](crate::MAsyncTx::downgrade).
/// When the number of valid sender is non-zero, can try [upgrade](WeakTx::upgrade) to a [MTx](crate::MTx) or [MAsyncTx](crate::MAsyncTx).
pub struct WeakTx<F: Flavor + FlavorMP>(pub(crate) NonNull<ChannelShared<F>>);

unsafe impl<F: Flavor + FlavorMP> Send for WeakTx<F> {}
unsafe impl<F: Flavor + FlavorMP> Sync for WeakTx<F> {}

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
        if self.shared().try_add_tx() {
            Some(S::new(self.0))
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn get_tx_count(&self) -> usize {
        self.shared().get_tx_count()
    }

    #[inline(always)]
    pub fn get_rx_count(&self) -> usize {
        self.shared().get_rx_count()
    }

    #[inline(always)]
    fn shared(&self) -> &ChannelShared<F> {
        unsafe { self.0.as_ref() }
    }
}

impl<F: Flavor + FlavorMP> Drop for WeakTx<F> {
    #[inline]
    fn drop(&mut self) {
        ChannelShared::<F>::dec_ref(self.0.as_ptr());
    }
}

impl<F> WeakTx<F>
where
    F: Flavor + FlavorMP + FlavorUnbounded,
{
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
        let shared = self.shared();
        let _item = MaybeUninit::new(item);
        shared.inner.try_send(&_item);
        shared.on_send();
    }
}
