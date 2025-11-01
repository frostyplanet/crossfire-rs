use std::mem::MaybeUninit;

use crate::{
    backoff::BackoffConfig,
    channel::{RecvWaker, SendWaker, WakerState},
    trace_log, Rx, Tx,
};

use super::errors::TrySelectError;
use super::results::SelectResp;

/// State of a select handle during the select operation.
#[derive(Debug, Clone, Copy)]
pub enum HandleState {
    /// Handle is newly created and hasn't been polled yet.
    New,
    /// Handle has registered a waker and is actively waiting.
    Active,
    /// Handle's operation has completed successfully.
    Completed,
    /// Handle's channel is disconnected.
    Disconnected,
}

impl HandleState {
    /// Returns true if this handle should be polled.
    ///
    /// Only `New` and `Active` handles should be polled.
    pub fn should_poll(&self) -> bool {
        match self {
            HandleState::Active | HandleState::New => true,
            HandleState::Completed | HandleState::Disconnected => false,
        }
    }
}

/// A handle for either a receive or send operation in a select.
pub enum SelectHandle<'a, T> {
    /// A receive handle.
    Rx(RxHandle<'a, T>),
    /// A send handle.
    Tx(TxHandle<'a, T>),
}

/// Handle for a receive operation in a select.
pub struct RxHandle<'a, T> {
    pub(crate) rx: &'a Rx<T>,
    pub(crate) waker: Option<RecvWaker>,
    pub(crate) state: HandleState,
    pub(crate) idx: usize,
}

impl<'a, T> RxHandle<'a, T> {
    /// Creates a new receive handle.
    pub(crate) fn new(rx: &'a Rx<T>, idx: usize) -> Self {
        RxHandle { rx, waker: None, state: HandleState::New, idx }
    }

    /// Returns the index of this handle.
    pub(crate) fn idx(&self) -> usize {
        self.idx
    }
}

/// Handle for a send operation in a select.
pub struct TxHandle<'a, T> {
    pub(crate) tx: &'a Tx<T>,
    pub(crate) item: MaybeUninit<T>,
    pub(crate) direct: bool,
    pub(crate) waker: Option<SendWaker<T>>,
    pub(crate) state: HandleState,
    pub(crate) idx: usize,
}

impl<'a, T> TxHandle<'a, T> {
    /// Creates a new send handle.
    pub(crate) fn new(tx: &'a Tx<T>, item: T, idx: usize) -> Self {
        let _item = MaybeUninit::new(item);
        let direct = tx.shared.sender_direct_copy();
        TxHandle { tx, item: _item, direct, waker: None, state: HandleState::New, idx }
    }

    /// Returns the index of this handle.
    pub(crate) fn idx(&self) -> usize {
        self.idx
    }
}

impl<'a, T> SelectHandle<'a, T> {
    /// Returns the index of this handle.
    #[inline(always)]
    pub(crate) fn idx(&self) -> usize {
        match self {
            SelectHandle::Rx(rx_handle) => rx_handle.idx(),
            SelectHandle::Tx(tx_handle) => tx_handle.idx(),
        }
    }

    /// Returns true if the underlying channel is disconnected.
    #[inline(always)]
    pub(crate) fn is_disconnected(&self) -> bool {
        #[allow(unused_variables)]
        let idx = self.idx();
        let disconnected = match self {
            SelectHandle::Rx(rx_handle) => rx_handle.rx.shared.is_disconnected(),
            SelectHandle::Tx(tx_handle) => tx_handle.tx.shared.is_disconnected(),
        };
        trace_log!("is_disconnected: idx={}, disconnected={}", idx, disconnected);
        disconnected
    }

    /// Returns the state of the waker associated with this handle.
    #[inline(always)]
    pub(crate) fn waker_state(&self) -> u8 {
        match self {
            SelectHandle::Rx(rx_handle) => {
                rx_handle.waker.as_ref().map_or(WakerState::Init as u8, |w| w.get_state())
            }
            SelectHandle::Tx(tx_handle) => {
                tx_handle.waker.as_ref().map_or(WakerState::Init as u8, |w| w.get_state())
            }
        }
    }

    /// Clears the waker, either returning it to the cache or abandoning it.
    #[inline(always)]
    pub(crate) fn clear_waker(&mut self) {
        match self {
            SelectHandle::Rx(rx_handle) => {
                trace_log!(
                    "clear_waker: rx, idx={}, has_waker={}",
                    rx_handle.idx,
                    rx_handle.waker.is_some()
                );
                match rx_handle.state {
                    HandleState::Completed | HandleState::New => {
                        if let Some(waker) = rx_handle.waker.take() {
                            rx_handle.rx.waker_cache.push(waker);
                        }
                    }
                    HandleState::Active | HandleState::Disconnected => {
                        if let Some(waker) = rx_handle.waker.take() {
                            rx_handle.rx.abandon_recv_waker(waker);
                        }
                    }
                }
            }
            SelectHandle::Tx(tx_handle) => {
                trace_log!(
                    "clear_waker: tx, idx={}, has_waker={}",
                    tx_handle.idx,
                    tx_handle.waker.is_some()
                );
                match tx_handle.state {
                    HandleState::Completed | HandleState::New => {
                        if let Some(waker) = tx_handle.waker.take() {
                            if waker.get_state() >= WakerState::Waked as u8 {
                                tx_handle.tx.waker_cache.push(waker);
                            } else {
                                tx_handle.tx.abandon_send_waker(waker);
                            }
                        }
                    }
                    HandleState::Active | HandleState::Disconnected => {
                        if let Some(waker) = tx_handle.waker.take() {
                            tx_handle.tx.abandon_send_waker(waker);
                        }
                    }
                }
            }
        }
    }

    /// Converts this handle to a disconnected error, consuming any item in a send handle.
    #[inline(always)]
    pub(crate) fn as_disconnected(&mut self) -> TrySelectError<T> {
        let idx = self.idx();
        trace_log!("as_disconnected: idx={}", idx);
        match self {
            SelectHandle::Rx(rx_handle) => {
                rx_handle.state = HandleState::Disconnected;
                trace_log!("as_disconnected: rx, idx={}, marked as RecvDisconnected", idx);

                TrySelectError::RecvDisconnected { idx }
            }
            SelectHandle::Tx(tx_handle) => {
                let item: T = unsafe { std::ptr::read(tx_handle.item.as_ptr()) };
                tx_handle.item = MaybeUninit::zeroed();
                tx_handle.state = HandleState::Disconnected;
                trace_log!("as_disconnected: tx, idx={}, marked as SendDisconnected", idx);

                TrySelectError::SendDisconnected { idx, item }
            }
        }
    }

    /// Returns the current state of this handle.
    #[inline(always)]
    pub(crate) fn state(&self) -> HandleState {
        match self {
            SelectHandle::Rx(handle) => handle.state,
            SelectHandle::Tx(handle) => handle.state,
        }
    }

    /// Resets the handle state to prepare for the next select operation.
    #[inline(always)]
    pub(crate) fn reset(&mut self) {
        match self {
            SelectHandle::Rx(handle) => {
                trace_log!("reset: rx, idx={}, state={:?}", handle.idx, handle.state);
                if !matches!(handle.state, HandleState::Disconnected) {
                    handle.state = HandleState::New;
                }
            }
            SelectHandle::Tx(handle) => {
                trace_log!("reset: tx, idx={}, state={:?}", handle.idx, handle.state);
                if matches!(handle.state, HandleState::Active | HandleState::New) {
                    handle.state = HandleState::New;
                }
            }
        }
    }
}

impl<'a, T> SelectHandle<'a, T>
where
    T: Send + 'static,
{
    /// Attempts a fast-path operation without blocking.
    #[inline(always)]
    pub(crate) fn select_fast(&mut self) -> Option<SelectResp<T>> {
        let idx = self.idx();
        trace_log!("select_fast: idx={}", idx);
        match self {
            SelectHandle::Rx(rx_handle) => {
                trace_log!("select_fast: rx, idx={}, has_waker={}", idx, rx_handle.waker.is_some());
                let backoff = BackoffConfig::default()
                    .limit(rx_handle.rx.shared.backoff_limit)
                    .large(rx_handle.rx.shared.large)
                    .build();

                match rx_handle.waker.take() {
                    Some(waker) => {
                        trace_log!("select_fast: rx trying _recv_fast_waker, idx={}", idx);
                        match Rx::_recv_fast_waker(
                            &rx_handle.rx.shared,
                            &rx_handle.rx.waker_cache,
                            waker,
                            backoff,
                        ) {
                            Ok(item) => {
                                trace_log!(
                                    "select_fast: rx _recv_fast_waker succeeded, idx={}",
                                    idx
                                );
                                rx_handle.state = HandleState::Completed;
                                return Some(SelectResp::Recv { idx, item });
                            }
                            Err(w) => {
                                trace_log!("select_fast: rx _recv_fast_waker failed, idx={}", idx);
                                rx_handle.waker = Some(w);
                            }
                        }

                        None
                    }
                    None => {
                        trace_log!("select_fast: rx trying _recv_fast, idx={}", idx);
                        let result = Rx::_recv_fast(&rx_handle.rx.shared, backoff).map(|item| {
                            trace_log!("select_fast: rx _recv_fast succeeded, idx={}", idx);
                            rx_handle.state = HandleState::Completed;
                            SelectResp::Recv { idx, item }
                        });
                        if result.is_none() {
                            trace_log!("select_fast: rx _recv_fast returned None, idx={}", idx);
                        }
                        result
                    }
                }
            }
            SelectHandle::Tx(tx_handle) => {
                trace_log!("select_fast: tx, idx={}, has_waker={}", idx, tx_handle.waker.is_some());
                let backoff = BackoffConfig::default()
                    .limit(tx_handle.tx.shared.backoff_limit)
                    .large(tx_handle.tx.shared.large)
                    .build();

                match tx_handle.waker.take() {
                    Some(waker) => {
                        trace_log!("select_fast: tx trying _send_fast_waker, idx={}", idx);
                        match Tx::_send_fast_waker(
                            &tx_handle.tx.shared,
                            &tx_handle.tx.waker_cache,
                            waker,
                            &tx_handle.item,
                            backoff,
                        ) {
                            Ok(_) => {
                                trace_log!(
                                    "select_fast: tx _send_fast_waker succeeded, idx={}",
                                    idx
                                );
                                tx_handle.state = HandleState::Completed;
                                tx_handle.item = MaybeUninit::zeroed();
                                return Some(SelectResp::Send { idx });
                            }
                            Err(w) => {
                                trace_log!("select_fast: tx _send_fast_waker failed, idx={}", idx);
                                tx_handle.waker = Some(w);
                            }
                        }

                        None
                    }
                    None => {
                        trace_log!("select_fast: tx trying _send_fast, idx={}", idx);
                        let result = Tx::_send_fast(
                            &tx_handle.tx.shared,
                            &tx_handle.item,
                            tx_handle.direct && tx_handle.tx.shared.large,
                            backoff,
                        )
                        .map(|_| {
                            trace_log!("select_fast: tx _send_fast succeeded, idx={}", idx);
                            tx_handle.state = HandleState::Completed;
                            tx_handle.item = MaybeUninit::zeroed();

                            SelectResp::Send { idx }
                        });
                        if result.is_none() {
                            trace_log!("select_fast: tx _send_fast returned None, idx={}", idx);
                        }
                        result
                    }
                }
            }
        }
    }

    /// Registers a waker for this handle in preparation for parking the thread.
    #[inline(always)]
    pub(crate) fn select_prepark(&mut self, state: &mut u8) -> Option<SelectResp<T>> {
        let idx = self.idx();
        trace_log!("select_prepark: idx={}", idx);
        match self {
            SelectHandle::Rx(rx_handle) => {
                trace_log!(
                    "select_prepark: rx, idx={}, has_waker={}",
                    idx,
                    rx_handle.waker.is_some()
                );

                if rx_handle.waker.is_none() {
                    trace_log!("select_prepark: rx creating new waker, idx={}", idx);

                    rx_handle.waker = Some(rx_handle.rx.waker_cache.new_blocking(()));
                }

                let waker = rx_handle.waker.take().expect("waker should be initialized");
                match Rx::_recv_prepark(&rx_handle.rx.shared, waker, state) {
                    Ok(w) => {
                        trace_log!(
                            "select_prepark: rx registered waker, idx={}, state={}",
                            idx,
                            *state
                        );

                        rx_handle.state = HandleState::Active;
                        rx_handle.waker = Some(w);
                        None
                    }
                    Err(item) => {
                        trace_log!("select_prepark: rx immediately received, idx={}", idx);

                        rx_handle.state = HandleState::Completed;
                        Some(SelectResp::Recv { idx, item })
                    }
                }
            }
            SelectHandle::Tx(tx_handle) => {
                trace_log!(
                    "select_prepark: tx, idx={}, has_waker={}",
                    idx,
                    tx_handle.waker.is_some()
                );

                if tx_handle.waker.is_none() {
                    trace_log!(
                        "select_prepark: tx creating new waker, idx={}, direct={}",
                        idx,
                        tx_handle.direct
                    );
                    let direct_copy_ptr: *const T =
                        if tx_handle.direct { tx_handle.item.as_ptr() } else { std::ptr::null() };

                    tx_handle.waker = Some(tx_handle.tx.waker_cache.new_blocking(direct_copy_ptr));
                }

                let w = tx_handle.waker.take().expect("waker should be initialized");
                let (new_state, o_waker) =
                    Tx::_send_prepark(&tx_handle.tx.shared, w, &tx_handle.item);

                *state = new_state;
                tx_handle.waker = o_waker;

                trace_log!(
                    "select_prepark: tx registered waker, idx={}, state={}, waker_retained={}",
                    idx,
                    *state,
                    tx_handle.waker.is_some()
                );

                // we could figure out the SelectResp here but its easier to understand if its done later in the waker state check
                if *state == WakerState::Done as u8 {
                    trace_log!(
                        "select_prepark: tx completed immediately, idx={}, state={}",
                        idx,
                        *state
                    );
                    tx_handle.waker.take().map(|w| {
                        tx_handle.tx.waker_cache.push(w);
                    });
                    tx_handle.state = HandleState::Completed;

                    return Some(SelectResp::Send { idx });
                }

                // if tx_handle.waker.is_none() || *state >= WakerState::Waked as u8 {
                //     trace_log!("select_prepark: tx completed immediately, idx={}", idx);

                //     tx_handle.waker.take().map(|w| {
                //         tx_handle.tx.waker_cache.push(w);
                //     });
                //     tx_handle.state = HandleState::Completed;

                //     Some(SelectResp::Send { idx })
                // } else {
                //     None
                // }

                None
            }
        }
    }
}

impl<'a, T> Drop for SelectHandle<'a, T> {
    fn drop(&mut self) {
        self.clear_waker();
    }
}
