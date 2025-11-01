use std::time::{Duration, Instant};

use crate::{
    channel::{check_timeout, WakerState},
    trace_log, Rx, Tx,
};

use super::errors::{SelectTimeoutError, TrySelectError};
use super::handles::{RxHandle, SelectHandle, TxHandle};
use super::results::{SelectResp, SelectResults};

/// Determines how the select operation waits for channel operations to complete.
#[derive(Debug, Clone, Copy)]
pub enum SelectMode {
    /// Returns immediately when the first operation completes (respects biased ordering).
    ///
    /// When multiple operations are ready, only the first one (according to registration order
    /// if biased, or random order if unbiased) will be returned.
    FirstReady,

    /// Returns when any operation(s) complete during a single poll attempt.
    ///
    /// Multiple operations may complete simultaneously and all will be returned in a single call.
    /// There is no guarantee about the number of results (≥1), ordering, or which specific
    /// operations completed, only that at least one succeeded during the poll.
    AnyReady,

    /// Waits until all registered operations complete.
    ///
    /// This mode blocks until every registered send/recv operation has either completed
    /// successfully or disconnected.
    AllComplete,
}

/// Builder for creating select operations on multiple channels.
///
/// Use this to register send and receive operations on channels, then call one of
/// the mode methods ([`first_ready`](Select::first_ready), [`any_ready`](Select::any_ready),
/// or [`all_complete`](Select::all_complete)) to get a typed select struct.
///
/// # Example
///
/// ```no_run
/// use crossfire::mpsc;
/// use crossfire::blocking_select::Select;
///
/// let (tx1, rx1) = mpsc::bounded_blocking(10);
/// let (tx2, rx2) = mpsc::bounded_blocking(10);
///
/// // Build a select operation
/// let mut select = Select::new(false);
/// select.recv(&rx1);
/// select.recv(&rx2);
///
/// // Choose a mode and execute
/// let result = select.first_ready().select();
/// ```
pub struct Select<'a, T> {
    handles: Vec<SelectHandle<'a, T>>,
    biased: bool,
    counter: usize,
}

/// Select that returns the first ready operation.
///
/// This struct is created by calling [`Select::first_ready()`]. When multiple operations
/// are ready, only the first one (according to registration order if biased, or random
/// order if unbiased) will be returned.
///
/// The `select()` method returns a single `Result<SelectResp<T>, TrySelectError<T>>` instead
/// of a `Vec`.
pub struct FirstReadySelect<'a, T> {
    handles: Vec<SelectHandle<'a, T>>,
    biased: bool,
}

/// Select that returns any ready operations from a single poll.
///
/// This struct is created by calling [`Select::any_ready()`]. Multiple operations may
/// complete simultaneously and all will be returned in a single call. There is no
/// guarantee about the number of results (≥1), ordering, or which specific operations
/// completed, only that at least one succeeded during the poll.
pub struct AnyReadySelect<'a, T> {
    handles: Vec<SelectHandle<'a, T>>,
    biased: bool,
}

/// Select that waits until all registered operations complete.
///
/// This struct is created by calling [`Select::all_complete()`]. This mode blocks
/// until every registered send/recv operation has either completed successfully or
/// disconnected.
pub struct AllCompleteSelect<'a, T> {
    handles: Vec<SelectHandle<'a, T>>,
    biased: bool,
}

impl<'a, T> Select<'a, T>
where
    T: Send + 'static,
{
    /// Creates a new Select builder.
    ///
    /// # Arguments
    ///
    /// * `biased` - If true, operations are polled in registration order. If false, they are shuffled randomly.
    pub fn new(biased: bool) -> Self {
        Self { handles: Vec::new(), biased, counter: 0 }
    }

    /// Registers a receive operation on the given channel.
    ///
    /// Returns the index of this operation, which can be used to identify results.
    pub fn recv<'b: 'a>(&mut self, rx: &'b Rx<T>) -> usize {
        let idx = self.counter;
        self.counter += 1;
        trace_log!("recv: adding rx handle, idx={}", idx);
        self.handles.push(SelectHandle::Rx(RxHandle::new(rx, idx)));
        idx
    }

    /// Registers a send operation on the given channel with the provided item.
    ///
    /// Returns the index of this operation, which can be used to identify results.
    pub fn send<'b: 'a>(&mut self, tx: &'b Tx<T>, item: T) -> usize {
        let idx = self.counter;
        self.counter += 1;
        trace_log!("send: adding tx handle, idx={}", idx);

        self.handles.push(SelectHandle::Tx(TxHandle::new(tx, item, idx)));
        idx
    }

    /// Removes a handle from the select by its vector index (not the operation idx).
    ///
    /// Note: This removes from the internal vector, which may shift indices of subsequent handles.
    pub fn remove(&mut self, index: usize) {
        trace_log!("remove: removing handle, idx={}", index);
        self.handles.remove(index);
    }

    /// Returns the number of registered operations.
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// Returns true if there are any channels that can be polled.
    ///
    /// A channel can be polled if its state is `New` or `Active`.
    /// Returns false if all channels are either `Completed` or `Disconnected`.
    pub fn has_ready(&self) -> bool {
        self.handles.iter().any(|h| h.state().should_poll())
    }

    /// Converts this builder into a [`FirstReadySelect`] that returns the first ready operation.
    ///
    /// When multiple operations are ready, only the first one will be returned.
    pub fn first_ready(self) -> FirstReadySelect<'a, T> {
        FirstReadySelect { handles: self.handles, biased: self.biased }
    }

    /// Converts this builder into an [`AnyReadySelect`] that returns any ready operations.
    ///
    /// Multiple operations may complete simultaneously and all will be returned.
    pub fn any_ready(self) -> AnyReadySelect<'a, T> {
        AnyReadySelect { handles: self.handles, biased: self.biased }
    }

    /// Converts this builder into an [`AllCompleteSelect`] that waits for all operations.
    ///
    /// Blocks until every registered operation has either completed or disconnected.
    pub fn all_complete(self) -> AllCompleteSelect<'a, T> {
        AllCompleteSelect { handles: self.handles, biased: self.biased }
    }

    // Deprecated methods for backwards compatibility
    /// Deprecated: Use the builder pattern methods instead.
    ///
    /// Use `first_ready().select()`, `any_ready().select()`, or `all_complete().select()`.
    #[deprecated(note = "Use first_ready().select() instead")]
    pub fn select(&mut self, mode: SelectMode) -> Vec<Result<SelectResp<T>, TrySelectError<T>>> {
        trace_log!("select: starting, mode={:?}, handles={}", mode, self.handles.len());
        self._select(None, mode, true).unwrap_or_else(|_| panic!("no timeout"))
    }

    /// Deprecated: Use the builder pattern methods instead.
    ///
    /// Use `first_ready().select_timeout()`, `any_ready().select_timeout()`, or `all_complete().select_timeout()`.
    #[deprecated(note = "Use first_ready().select_timeout() or similar instead")]
    pub fn select_timeout(
        &mut self, timeout: Duration, mode: SelectMode,
    ) -> Result<Vec<Result<SelectResp<T>, TrySelectError<T>>>, SelectTimeoutError> {
        trace_log!(
            "select_timeout: starting, mode={:?}, timeout={:?}, handles={}",
            mode,
            timeout,
            self.handles.len()
        );
        self._select(Some(timeout), mode, true)
    }

    /// Deprecated: Use the builder pattern methods instead.
    ///
    /// Use `first_ready().select_next()`, `any_ready().select_next()`, or `all_complete().select_next()`.
    #[deprecated(note = "Use first_ready().select_next() or similar instead")]
    pub fn select_next(
        &mut self, mode: SelectMode,
    ) -> Vec<Result<SelectResp<T>, TrySelectError<T>>> {
        trace_log!("select_next: starting, mode={:?}, handles={}", mode, self.handles.len());
        self._select(None, mode, false).unwrap_or_else(|_| panic!("no timeout"))
    }

    /// Deprecated: Use the builder pattern methods instead.
    ///
    /// Use `first_ready().select_next_timeout()`, `any_ready().select_next_timeout()`, or `all_complete().select_next_timeout()`.
    #[deprecated(note = "Use first_ready().select_next_timeout() or similar instead")]
    pub fn select_next_timeout(
        &mut self, timeout: Duration, mode: SelectMode,
    ) -> Result<Vec<Result<SelectResp<T>, TrySelectError<T>>>, SelectTimeoutError> {
        trace_log!(
            "select_next_timeout: starting, mode={:?}, timeout={:?}, handles={}",
            mode,
            timeout,
            self.handles.len()
        );
        self._select(Some(timeout), mode, false)
    }

    pub(crate) fn _select(
        &mut self, timeout: Option<Duration>, mode: SelectMode, reset_handles: bool,
    ) -> Result<Vec<Result<SelectResp<T>, TrySelectError<T>>>, SelectTimeoutError> {
        Self::_select_impl(&mut self.handles, timeout, mode, reset_handles, self.biased)
    }

    fn _select_impl(
        handles: &mut Vec<SelectHandle<'a, T>>, timeout: Option<Duration>, mode: SelectMode,
        reset_handles: bool, biased: bool,
    ) -> Result<Vec<Result<SelectResp<T>, TrySelectError<T>>>, SelectTimeoutError> {
        trace_log!(
            "_select: mode={:?}, timeout={:?}, reset={}, biased={}, handles={}",
            mode,
            timeout,
            reset_handles,
            biased,
            handles.len()
        );

        if handles.is_empty() {
            panic!("no actions added to select");
        }

        let deadline = timeout.map(|t| Instant::now().checked_add(t).expect("timeout too large"));

        if !biased {
            trace_log!("_select_impl: shuffling handles");
            fastrand::Rng::new().shuffle(handles);
        }

        trace_log!("_select_impl: calling _select_poll");
        let res = Self::_select_poll(handles, deadline, mode);

        if reset_handles {
            trace_log!("_select_impl: resetting all handles");
            for handle in handles.iter_mut() {
                handle.clear_waker();
                handle.reset();
            }
        } else {
            trace_log!("_select_impl: selectively resetting handles");
            if let Ok(resp) = &res {
                for r in resp.iter() {
                    match r {
                        Ok(sr) => {
                            trace_log!("_select_impl: resetting handle, idx={}", sr.idx());
                            for handle in handles.iter_mut() {
                                if handle.idx() == sr.idx() {
                                    handle.reset();
                                }
                            }
                        }
                        Err(_) => {}
                    }
                }
            }
        }

        trace_log!("_select_impl: returning result");
        res
    }

    fn _select_poll(
        handles: &mut Vec<SelectHandle<'a, T>>, deadline: Option<Instant>, mode: SelectMode,
    ) -> Result<Vec<Result<SelectResp<T>, TrySelectError<T>>>, SelectTimeoutError> {
        macro_rules! result_check {
            ($mode:expr, $results:expr, $result:expr) => {
                match $mode {
                    SelectMode::FirstReady => return Ok(vec![$result]),
                    SelectMode::AllComplete => {
                        $results.push($result);
                    }
                    SelectMode::AnyReady => {
                        $results.push($result);
                    }
                }
            };
            ($mode:expr, $waked_indexes:expr, $idx:expr, $handle:expr) => {
                match $mode {
                    SelectMode::FirstReady => {
                        if let Some(value) = $handle.select_fast() {
                            trace_log!(
                                "_select_poll: FirstReady fast return, idx={}",
                                $handle.idx()
                            );
                            return Ok(vec![Ok(value)]);
                        }
                    }
                    SelectMode::AllComplete => {
                        $waked_indexes.push($idx);
                    }
                    SelectMode::AnyReady => {
                        $waked_indexes.push($idx);
                    }
                }
            };
        }

        macro_rules! return_check {
            ($mode:expr, $handles:expr, $results:expr) => {
                if !$results.is_empty() {
                    match $mode {
                        SelectMode::AnyReady | SelectMode::FirstReady => {
                            trace_log!(
                                "_select_poll: AnyReady/FirstReady return, count={}",
                                $results.len()
                            );
                            return Ok($results);
                        }
                        SelectMode::AllComplete => {
                            if $handles.iter().find(|h| h.state().should_poll()).is_none() {
                                trace_log!(
                                    "_select_poll: AllComplete return, count={}",
                                    $results.len()
                                );
                                return Ok($results);
                            }
                        }
                    }
                }
            };
        }

        trace_log!("_select_poll: starting initial fast poll phase");
        let mut results = Vec::new();
        let mut all_done = true;
        for handle in handles.iter_mut().filter(|h| h.state().should_poll()) {
            all_done = false;
            if let Some(resp) = handle.select_fast() {
                trace_log!("_select_poll: fast result, idx={}", resp.idx());
                result_check!(mode, results, Ok(resp));
            }
        }

        if all_done {
            return Ok(vec![]);
        }

        return_check!(mode, handles, results);

        trace_log!("_select_poll: entering main select loop");
        loop {
            // Register all wakers and check their states
            let mut waked_indexes = Vec::new();

            trace_log!("_select_poll: registering wakers phase");
            for (i, handle) in
                handles.iter_mut().enumerate().filter(|(_, h)| h.state().should_poll())
            {
                #[allow(unused_variables)]
                let idx = handle.idx();
                let mut state = WakerState::Init as u8;
                trace_log!("_select_poll: prepark, idx={}", idx);
                if let Some(value) = handle.select_prepark(&mut state) {
                    trace_log!("_select_poll: prepark returned value, idx={}", idx);
                    result_check!(mode, results, Ok(value));
                }

                trace_log!("_select_poll: prepark state, idx={}, state={:?}", idx, state);

                match state.into() {
                    WakerState::Init | WakerState::Waiting => {
                        trace_log!("_select_poll: waiting, idx={}", idx);
                        continue;
                    }
                    WakerState::Closed => {
                        trace_log!("_select_poll: closed after prepark, idx={}", idx);
                        // Check the state after registration
                        // result_check!(mode, results, Err(handle.as_disconnected()));
                        result_check!(mode, waked_indexes, i, handle);
                    }
                    WakerState::Waked => {
                        trace_log!("_select_poll: waked after prepark, idx={}", idx);
                        result_check!(mode, waked_indexes, i, handle);
                    }
                    WakerState::Done => {
                        trace_log!("_select_poll: done after prepark, idx={}", idx);
                        // Done is only set by senders. no other polling is needed for it
                        // result_check!(mode, results, Ok(SelectResp::Send { idx }));
                    }
                };
            }

            if !waked_indexes.is_empty() {
                trace_log!("_select_poll: processing waked handles, count={}", waked_indexes.len());
                for i in waked_indexes.drain(..) {
                    let handle = &mut handles[i];

                    trace_log!("_select_poll: polling waked handle, idx={}", handle.idx());
                    if let Some(res) = handle.select_fast() {
                        trace_log!(
                            "_select_poll: waked handle returned result, idx={}",
                            handle.idx()
                        );

                        result_check!(mode, results, Ok(res));
                    } else {
                        trace_log!(
                            "_select_poll: waked handle returned None, idx={}",
                            handle.idx()
                        );
                    }
                }
            }

            return_check!(mode, handles, results);

            trace_log!("_select_poll: checking for disconnected handles");
            for handle in handles.iter_mut().filter(|h| h.state().should_poll()) {
                trace_log!("_select_poll: checking disconnect, idx={}", handle.idx());
                let state = handle.waker_state();
                if state >= WakerState::Closed as u8 && handle.is_disconnected() {
                    trace_log!("_select_poll: handle is disconnected, idx={}", handle.idx());
                    result_check!(mode, results, Err(handle.as_disconnected()));
                }
            }

            return_check!(mode, handles, results);

            trace_log!("_select_poll: checking timeout before park");
            match check_timeout(deadline) {
                Ok(None) => {
                    trace_log!("_select_poll: parking thread indefinitely");
                    std::thread::park();
                    trace_log!("_select_poll: thread unparked");
                }
                Ok(Some(dur)) => {
                    trace_log!("_select_poll: parking thread for {:?}", dur);
                    std::thread::park_timeout(dur);
                    trace_log!("_select_poll: thread unparked after timeout");
                }
                Err(_) => {
                    trace_log!("_select_poll: timeout reached");
                    return Err(SelectTimeoutError::Timeout);
                }
            }

            trace_log!("_select_poll: post wakeup timeout check");

            if let Err(_) = check_timeout(deadline) {
                trace_log!("_select_poll: timeout reached after wakeup");
                return Err(SelectTimeoutError::Timeout);
            }

            // fast check
            trace_log!("_select_poll: checking waker states after wakeup");
            for (i, handle) in
                handles.iter_mut().enumerate().filter(|(_, h)| h.state().should_poll())
            {
                #[allow(unused_variables)]
                let idx = handle.idx();
                let waker_state: WakerState = handle.waker_state().into();
                trace_log!("_select_poll: post wakeup check, idx={}, state={:?}", idx, waker_state);
                match waker_state.into() {
                    WakerState::Init | WakerState::Waiting => {
                        trace_log!("_select_poll: still waiting after wakeup, idx={}", idx);
                        continue;
                    }
                    WakerState::Closed => {
                        trace_log!("_select_poll: closed after wakeup, idx={}", idx);
                        // Check the state after registration
                        // result_check!(mode, results, Err(handle.as_disconnected()));
                        result_check!(mode, waked_indexes, i, handle);
                    }
                    WakerState::Waked => {
                        trace_log!("_select_poll: waked after wakeup, idx={}", idx);
                        result_check!(mode, waked_indexes, i, handle);
                    }
                    WakerState::Done => {
                        // this branch should not be reachable
                        trace_log!("_select_poll: done after wakeup, idx={}", idx);
                        // Done is only set by senders. no other polling is needed for it
                        // result_check!(mode, results, Ok(SelectResp::Send { idx }));
                    }
                };
            }

            if !waked_indexes.is_empty() {
                trace_log!(
                    "_select_poll: processing waked handles after wakeup, count={}",
                    waked_indexes.len()
                );
                for i in waked_indexes.drain(..) {
                    let handle = &mut handles[i];
                    trace_log!("_select_poll: polling post-wakeup handle, idx={}", handle.idx());
                    if let Some(res) = handle.select_fast() {
                        trace_log!("_select_poll: fast result, idx={}", handle.idx());
                        result_check!(mode, results, Ok(res));
                    } else {
                        trace_log!(
                            "_select_poll: post-wakeup handle returned None, idx={}",
                            handle.idx()
                        );
                    }
                }
            }

            return_check!(mode, handles, results);
        }
    }
}

impl<'a, T> FirstReadySelect<'a, T>
where
    T: Send + 'static,
{
    /// Block until the first operation completes.
    ///
    /// Returns a single result for the first operation that becomes ready.
    /// Resets all handles after completion.
    pub fn select(&mut self) -> SelectResults<T> {
        trace_log!("first_ready_select: starting, handles={}", self.handles.len());

        let result = Select::_select_impl(
            &mut self.handles,
            None,
            SelectMode::FirstReady,
            true,
            self.biased,
        )
        .unwrap_or_else(|_| panic!("no timeout"));

        SelectResults::new(result)
    }

    /// Block until the first operation completes or the timeout expires.
    ///
    /// Returns `Ok(result)` if an operation completes, or `Err(SelectTimeoutError::Timeout)`
    /// if the timeout expires. Resets all handles after completion.
    pub fn select_timeout(
        &mut self, timeout: Duration,
    ) -> Result<SelectResults<T>, SelectTimeoutError> {
        trace_log!(
            "first_ready_select_timeout: starting, timeout={:?}, handles={}",
            timeout,
            self.handles.len()
        );

        let result = Select::_select_impl(
            &mut self.handles,
            Some(timeout),
            SelectMode::FirstReady,
            true,
            self.biased,
        )?;

        Ok(SelectResults::new(result))
    }

    /// Block until the first operation completes without resetting completed handles.
    ///
    /// Similar to [`select()`](FirstReadySelect::select) but does not reset handles,
    /// allowing reuse of the select operation.
    pub fn select_next(&mut self) -> SelectResults<T> {
        trace_log!("first_ready_select_next: starting, handles={}", self.handles.len());

        let result = Select::_select_impl(
            &mut self.handles,
            None,
            SelectMode::FirstReady,
            false,
            self.biased,
        )
        .unwrap_or_else(|_| panic!("no timeout"));

        SelectResults::new(result)
    }

    /// Block until the first operation completes or timeout without resetting handles.
    ///
    /// Similar to [`select_timeout()`](FirstReadySelect::select_timeout) but does not
    /// reset handles, allowing reuse of the select operation.
    pub fn select_next_timeout(
        &mut self, timeout: Duration,
    ) -> Result<SelectResults<T>, SelectTimeoutError> {
        trace_log!(
            "first_ready_select_next_timeout: starting, timeout={:?}, handles={}",
            timeout,
            self.handles.len()
        );

        let result = Select::_select_impl(
            &mut self.handles,
            Some(timeout),
            SelectMode::FirstReady,
            false,
            self.biased,
        )?;

        Ok(SelectResults::new(result))
    }

    /// Returns true if there are any channels that can be polled.
    ///
    /// A channel can be polled if its state is `New` or `Active`.
    /// Returns false if all channels are either `Completed` or `Disconnected`.
    pub fn has_ready(&self) -> bool {
        self.handles.iter().any(|h| h.state().should_poll())
    }
}

impl<'a, T> AnyReadySelect<'a, T>
where
    T: Send + 'static,
{
    /// Block until any operations complete during a single poll.
    ///
    /// Returns all operations that became ready during the poll attempt.
    /// Resets all handles after completion.
    pub fn select(&mut self) -> SelectResults<T> {
        trace_log!("any_ready_select: starting, handles={}", self.handles.len());
        let results =
            Select::_select_impl(&mut self.handles, None, SelectMode::AnyReady, true, self.biased)
                .unwrap_or_else(|_| panic!("no timeout"));

        SelectResults::new(results)
    }

    /// Block until any operations complete or the timeout expires.
    ///
    /// Returns all operations that became ready, or `Err(SelectTimeoutError::Timeout)`
    /// if the timeout expires. Resets all handles after completion.
    pub fn select_timeout(
        &mut self, timeout: Duration,
    ) -> Result<SelectResults<T>, SelectTimeoutError> {
        trace_log!(
            "any_ready_select_timeout: starting, timeout={:?}, handles={}",
            timeout,
            self.handles.len()
        );

        let results = Select::_select_impl(
            &mut self.handles,
            Some(timeout),
            SelectMode::AnyReady,
            true,
            self.biased,
        )?;

        Ok(SelectResults::new(results))
    }

    /// Block until any operations complete without resetting completed handles.
    ///
    /// Similar to [`select()`](AnyReadySelect::select) but does not reset handles,
    /// allowing reuse of the select operation.
    pub fn select_next(&mut self) -> SelectResults<T> {
        trace_log!("any_ready_select_next: starting, handles={}", self.handles.len());
        let results =
            Select::_select_impl(&mut self.handles, None, SelectMode::AnyReady, false, self.biased)
                .unwrap_or_else(|_| panic!("no timeout"));

        SelectResults::new(results)
    }

    /// Block until any operations complete or timeout without resetting handles.
    ///
    /// Similar to [`select_timeout()`](AnyReadySelect::select_timeout) but does not
    /// reset handles, allowing reuse of the select operation.
    pub fn select_next_timeout(
        &mut self, timeout: Duration,
    ) -> Result<SelectResults<T>, SelectTimeoutError> {
        trace_log!(
            "any_ready_select_next_timeout: starting, timeout={:?}, handles={}",
            timeout,
            self.handles.len()
        );

        let results = Select::_select_impl(
            &mut self.handles,
            Some(timeout),
            SelectMode::AnyReady,
            false,
            self.biased,
        )?;

        Ok(SelectResults::new(results))
    }

    /// Returns true if there are any channels that can be polled.
    ///
    /// A channel can be polled if its state is `New` or `Active`.
    /// Returns false if all channels are either `Completed` or `Disconnected`.
    pub fn has_ready(&self) -> bool {
        self.handles.iter().any(|h| h.state().should_poll())
    }
}

impl<'a, T> AllCompleteSelect<'a, T>
where
    T: Send + 'static,
{
    /// Block until all registered operations complete.
    ///
    /// Returns all operations once every handle has either completed or disconnected.
    /// Resets all handles after completion.
    pub fn select(&mut self) -> SelectResults<T> {
        trace_log!("all_complete_select: starting, handles={}", self.handles.len());

        let results = Select::_select_impl(
            &mut self.handles,
            None,
            SelectMode::AllComplete,
            true,
            self.biased,
        )
        .unwrap_or_else(|_| panic!("no timeout"));

        SelectResults::new(results)
    }

    /// Block until all operations complete or the timeout expires.
    ///
    /// Returns all completed operations, or `Err(SelectTimeoutError::Timeout)` if the
    /// timeout expires before all operations complete. Resets all handles after completion.
    pub fn select_timeout(
        &mut self, timeout: Duration,
    ) -> Result<SelectResults<T>, SelectTimeoutError> {
        trace_log!(
            "all_complete_select_timeout: starting, timeout={:?}, handles={}",
            timeout,
            self.handles.len()
        );

        let results = Select::_select_impl(
            &mut self.handles,
            Some(timeout),
            SelectMode::AllComplete,
            true,
            self.biased,
        )?;

        Ok(SelectResults::new(results))
    }

    /// Block until all operations complete without resetting completed handles.
    ///
    /// Similar to [`select()`](AllCompleteSelect::select) but does not reset handles,
    /// allowing reuse of the select operation.
    pub fn select_next(&mut self) -> SelectResults<T> {
        trace_log!("all_complete_select_next: starting, handles={}", self.handles.len());
        let results = Select::_select_impl(
            &mut self.handles,
            None,
            SelectMode::AllComplete,
            false,
            self.biased,
        )
        .unwrap_or_else(|_| panic!("no timeout"));

        SelectResults::new(results)
    }

    /// Block until all operations complete or timeout without resetting handles.
    ///
    /// Similar to [`select_timeout()`](AllCompleteSelect::select_timeout) but does not
    /// reset handles, allowing reuse of the select operation.
    pub fn select_next_timeout(
        &mut self, timeout: Duration,
    ) -> Result<SelectResults<T>, SelectTimeoutError> {
        trace_log!(
            "all_complete_select_next_timeout: starting, timeout={:?}, handles={}",
            timeout,
            self.handles.len()
        );

        let results = Select::_select_impl(
            &mut self.handles,
            Some(timeout),
            SelectMode::AllComplete,
            false,
            self.biased,
        )?;

        Ok(SelectResults::new(results))
    }

    /// Returns true if there are any channels that can be polled.
    ///
    /// A channel can be polled if its state is `New` or `Active`.
    /// Returns false if all channels are either `Completed` or `Disconnected`.
    pub fn has_ready(&self) -> bool {
        self.handles.iter().any(|h| h.state().should_poll())
    }
}
