use crate::backoff::*;
use crate::flavor::{ChannelSharedMultiplex, Queue};
use crate::shared::{check_timeout, ChannelShared};
use crate::waker::WakerState;
use crate::waker_registry::*;
use crate::{trace_log, BlockingRxTrait, ReceiverType, RecvError, RecvTimeoutError, TryRecvError};
use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_WEIGHT: u32 = 128;

/// `MultiplexDyn` owns multi channel receivers of the same item type.
///
/// Unlike select, it focus on round-robin mode, allow to specified weight on each channel.
/// It maintains a count of message received for each channel.
/// That means if the last message recv on the `idx` channel, it will keep trying the same channel
/// until the number equals to weight has been received. If the channel is empty, it will try the
/// next one without touching the count. This strategy improves the hit rate of cpu cache and ensures no starvation.
///
/// NOTE: The default weight is 128. (When the weight of all channel set to 1, the performance is
/// the worst because of cpu cache thrashing)
///
/// ## Capability and limitation:
/// - New channel may be added and removed on the fly, internally use dynamic dispatch.
/// - It can select from bounded and unbounded channels at the same time
/// - **Limits and Safety**
///   - In order to preserve internal order, if one of the sender closed, nothing will happen
///     (because close is a rare case). User will not be notify until all its channels closed.
///   - It has internal mutability because it need to impl [BlockingRxTrait](crate::BlockingRxTrait),
///     the adding channel process remains `&mut self`.
///   - Although `MultiplexDyn` can support mpmc, it does not have `Sync`, just like [Rx](crate::Rx).
///     It's UB to receive from the same instance of MultiplexDyn concurrently.
///   - For mpmc usage, it's legal to construct multiple instance of `MultiplexDyn` from [MRx](crate::MRx).
///     But because MultiplexDyn is not aware of the Flavor type inside, it does not implement
///     `Clone`.
///   - For mpsc/spsc sencario, if you guarantee no concurrent access,
///      you can manutally add the `Sync` back in parent struct.
///
///
/// # Examples
///
/// Basic usage with multiple senders:
///
/// ```
/// use crossfire::{mpmc, MTx, select::MultiplexDyn};
/// use std::thread::spawn;
///
/// // Create a multiplexer with bounded + unbounded channels
/// let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
/// let (tx2, rx2) = mpmc::unbounded_blocking::<i32>();
///
/// let mut ths = Vec::new();
/// // Send values from different threads
/// ths.push(spawn(move || {
///     for i in 0..10 {
///         tx1.send(i).expect("send");
///     }
/// }));
/// ths.push(spawn(move || {
///     for i in 10..20 {
///         tx2.send(i).expect("send");
///     }
/// }));
/// for _ in 0..2 {
///     let mut mux = MultiplexDyn::<i32>::new();
///     mux.add(rx1.clone());
///     mux.add(rx2.clone());
///     ths.push(spawn(move || {
///         while let Ok(msg) = mux.recv() {
///             println!("recv {msg}");
///         }
///     }));
/// }
/// for th in ths {
///     th.join().expect("join");
/// }
/// ```
pub struct MultiplexDyn<T> {
    _handlers: UnsafeCell<Vec<MultiplexHandle<T>>>,
    waker: Arc<SelectWaker>,
    last_idx: Cell<u32>,
    count: Cell<u32>,
}

unsafe impl<T> Send for MultiplexDyn<T> {}

struct MultiplexHandle<T> {
    _shared: NonNull<dyn ChannelSharedMultiplex<T>>,
    weight: u32,
    registered: Cell<bool>,
    drop_fn: fn(*mut ()),
}

impl<T> MultiplexHandle<T> {
    #[inline(always)]
    fn shared(&self) -> &dyn ChannelSharedMultiplex<T> {
        unsafe { self._shared.as_ref() }
    }

    #[inline(always)]
    fn reg_waker(&self, idx: usize, global_waker: &Arc<SelectWaker>) {
        if !self.registered.get() & self.shared().reg_waker(idx, global_waker) {
            trace_log!("MultiplexDyn: reg waker for {idx}");
            self.registered.set(true);
        }
    }

    #[inline]
    fn close(&self, global_waker: &Arc<SelectWaker>) {
        self.shared().cancel_waker(global_waker);
        (self.drop_fn)(self._shared.as_ptr() as *mut ());
    }
}

macro_rules! remove_handle {
    ($self: expr, $handlers: expr, $idx: expr, $handle: expr) => {
        $handle.close(&$self.waker);
        $handlers.remove($idx as usize);
        if $self.last_idx.get() as usize > $handlers.len() {
            $self.last_idx.set(0);
        }
    };
}

impl<T> MultiplexDyn<T> {
    /// Initialize Select with fair, round-robin strategy
    pub fn new() -> Self {
        Self {
            waker: Arc::new(SelectWaker::new()),
            _handlers: UnsafeCell::new(Vec::with_capacity(4)),
            count: Cell::new(0),
            last_idx: Cell::new(0),
        }
    }

    /// Add a channel receiver, with default weight (128)
    pub fn add<R: ReceiverType>(&mut self, rx: R)
    where
        R: ReceiverType,
        R::Flavor: Queue<Item = T>,
    {
        self.add_with_weight(rx, DEFAULT_WEIGHT)
    }

    /// Add a channel receiver, with custom weight instead of default
    #[inline]
    pub fn add_with_weight<R>(&mut self, rx: R, weight: u32)
    where
        R: ReceiverType,
        R::Flavor: Queue<Item = T>,
    {
        self.waker.add_opened();
        let shared_pt = rx.shared_ptr();
        let _shared = unsafe {
            NonNull::new_unchecked(shared_pt.as_ptr() as *mut dyn ChannelSharedMultiplex<T>)
        };
        let handlers = self.handlers_mut();
        handlers.push(MultiplexHandle {
            _shared,
            weight: weight - 1,
            registered: Cell::new(false),
            drop_fn: ChannelShared::<R::Flavor>::close_rx_erased,
        });
        let l = handlers.len();
        if l < u32::MAX as usize {
            self.last_idx.set(l as u32 - 1);
            std::mem::forget(rx);
        } else {
            panic!("too many handlers");
        }
    }

    pub fn remove<R: ReceiverType>(&mut self, rx: &R)
    where
        R: ReceiverType,
        ChannelShared<R::Flavor>: ChannelSharedMultiplex<T>,
    {
        let shared_pt = rx.shared_ptr().as_ptr() as *const ();
        if let Some(idx) =
            self.handlers().iter().position(|h| h._shared.as_ptr() as *const () == shared_pt)
        {
            let handlers = self.handlers_mut();
            let handle = &handlers[idx];
            remove_handle!(self, handlers, idx, handle);
            trace_log!("{self}: remove handle {idx}");
        }
    }

    /// Attempts to receive a message from any of the multiplexed channels without blocking.
    ///
    /// Returns `Ok(item)` if a message is available on any of the channels.
    /// Returns `Err(TryRecvError::Empty)` if no messages are available.
    /// Returns `Err(TryRecvError::Disconnected)` if all senders have been dropped.
    ///
    /// # Example
    ///
    /// ```
    /// use crossfire::{mpsc, select::{MultiplexDyn, Mux}, MTx, TryRecvError};
    ///
    /// let mut mux = MultiplexDyn::<i32>::new();
    /// let (tx1, rx1) = mpsc::bounded_blocking::<i32>(10);
    /// let (_tx2, rx2) = mpsc::unbounded_blocking::<i32>();
    /// mux.add(rx1);
    /// mux.add(rx2);
    /// // No message available yet
    /// assert_eq!(mux.try_recv(), Err(TryRecvError::Empty));
    /// tx1.send(42).unwrap();
    /// // Now a message is available
    /// assert_eq!(mux.try_recv(), Ok(42));
    /// ```
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        // we have to check connectivity, because user might be only using try_recv without recv().
        // and we should ensuer SeqCst is used here
        let mut idx = self.last_idx.get();
        let handlers = self.handlers_mut();
        let mut len = handlers.len() as u32;
        if idx >= len {
            idx = 0;
        }
        let mut loop_limit = len;
        while loop_limit > 0 {
            let handle = unsafe { handlers.get_unchecked(idx as usize) };
            match handle.shared().try_recv_final() {
                Ok(msg) => {
                    trace_log!("{self}: try_recv from {idx}");
                    // on_recv already included in try_recv method
                    self.count.set(handle.weight);
                    self.last_idx.set(idx);
                    return Ok(msg);
                }
                Err(true) => {
                    trace_log!("{self}: try_recv close handle {idx}");
                    remove_handle!(self, handlers, idx, handle);
                    // on removal, the idx become next slot
                    len -= 1;
                    if idx >= len {
                        idx = 0;
                    }
                }
                Err(false) => {
                    if idx + 1 < len {
                        idx = idx + 1;
                    } else {
                        idx = 0;
                    }
                }
            }
            loop_limit -= 1;
        }
        if len > 0 {
            Err(TryRecvError::Empty)
        } else {
            Err(TryRecvError::Disconnected)
        }
    }

    /// Receives a message from any of the multiplexed channels, blocking if necessary.
    ///
    /// This method will block the current thread until a message is available on any of the channels,
    /// or until all senders are dropped.
    #[inline]
    pub fn recv(&self) -> Result<T, RecvError> {
        match self._recv_blocking(None) {
            Ok(item) => Ok(item),
            Err(_) => Err(RecvError),
        }
    }

    /// Receives a message from any of the multiplexed channels with a timeout.
    /// Will block when channel is empty.
    ///
    /// The behavior is atomic: the message is either received successfully or the operation is canceled due to a timeout.
    ///
    /// Returns `Ok(T)` when successful.
    ///
    /// Returns Err([RecvTimeoutError::Timeout]) when a message could not be received because the channel is empty and the operation timed out.
    ///
    /// Returns Err([RecvTimeoutError::Disconnected]) if the sender has been dropped and the channel is empty.
    #[inline]
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        match Instant::now().checked_add(timeout) {
            Some(deadline) => match self._recv_blocking(Some(deadline)) {
                Ok(item) => Ok(item),
                Err(true) => Err(RecvTimeoutError::Disconnected),
                Err(false) => Err(RecvTimeoutError::Timeout),
            },
            None => self.try_recv().map_err(|e| match e {
                TryRecvError::Disconnected => RecvTimeoutError::Disconnected,
                TryRecvError::Empty => RecvTimeoutError::Timeout,
            }),
        }
    }

    #[inline(always)]
    fn handlers_mut(&self) -> &mut Vec<MultiplexHandle<T>> {
        unsafe { &mut *self._handlers.get() }
    }

    #[inline(always)]
    fn handlers(&self) -> &Vec<MultiplexHandle<T>> {
        unsafe { &*self._handlers.get() }
    }

    /// start select from last_idx, if no message return `Err((last_idx, handlers_len))`.
    ///
    /// # Safety
    /// Be aware that _try_recv_cached does not guarantee all message will be receive,
    /// should retry again.
    /// The return value does not reveal if all channels have been close.
    /// We will need to guarantee with _check_all_final
    #[inline(always)]
    fn _try_select_cached(&self) -> Result<T, (u32, u32)> {
        let last_idx = self.last_idx.get();
        let handlers = self.handlers();
        let len = handlers.len() as u32;
        if last_idx < len && len > 0 {
            let handle = unsafe { handlers.get_unchecked(last_idx as usize) };
            let count = self.count.get();
            let loop_count = if count > 0 {
                if let Some(msg) = handle.shared().try_recv_cached() {
                    trace_log!("{self}: recv from {last_idx}");
                    // on_recv already included in try_recv method
                    self.count.set(count - 1);
                    return Ok(msg);
                }
                count - 1
            } else {
                count
            };
            if let Some(item) = self._try_select_all::<false>(last_idx, loop_count) {
                return Ok(item);
            }
            Err((last_idx, len))
        } else {
            // it's possible we remove handler before and forget to update last_idx,
            // but it's find we will always double check, keep the hot-path clean
            Err((0, len))
        }
    }

    /// # Safety
    /// The return value does not reveal if all channels have been close.
    /// We will need to guarantee with _check_all_final
    #[inline(always)]
    fn _try_select_all<const FINAL: bool>(&self, mut idx: u32, loop_count: u32) -> Option<T> {
        let handlers = self.handlers();
        let len = handlers.len() as u32;
        for _ in 0..loop_count {
            idx = if idx + 1 < len { idx + 1 } else { 0 };
            let handle = unsafe { handlers.get_unchecked(idx as usize) };
            let msg = {
                if FINAL {
                    if let Ok(msg) = handle.shared().try_recv_final() {
                        msg
                    } else {
                        return None;
                    }
                } else {
                    handle.shared().try_recv()?
                }
            };
            trace_log!("{self}: recv from {idx}");
            // on_recv already included in try_recv method
            self.count.set(handle.weight);
            self.last_idx.set(idx);
            return Some(msg);
        }
        None
    }

    /// check and remove closed channels if there's any.
    ///
    /// If there's handlers closing, will update the len.
    /// Caller should check the len on return.
    #[inline(always)]
    fn _check_all_final(&self, len: &mut u32) -> Option<T> {
        let mut idx: u32 = 0;
        let handlers = self.handlers_mut();
        while idx < handlers.len() as u32 {
            let handle = unsafe { handlers.get_unchecked(idx as usize) };
            match handle.shared().try_recv_final() {
                Ok(item) => {
                    trace_log!("{self}: recv from {idx}");
                    // on_recv already included in try_recv method
                    self.count.set(handle.weight);
                    self.last_idx.set(idx);
                    return Some(item);
                }
                Err(true) => {
                    trace_log!("{self}: close handle {idx}");
                    // on removal, the idx become next slot
                    remove_handle!(self, handlers, idx, handle);
                    *len = handlers.len() as u32;
                }
                Err(false) => {
                    idx += 1;
                }
            }
        }
        None
    }

    /// Internal method to perform blocking receive with optional timeout
    ///
    /// # Parameters
    ///
    /// * `deadline` - Optional deadline for the operation; if None, blocks indefinitely
    ///
    /// # Returns
    ///
    /// Returns `Ok(item)` on successful receive, `Err(true)` if disconnected, `Err(false)` if timed out
    #[inline]
    fn _recv_blocking(&self, deadline: Option<Instant>) -> Result<T, bool> {
        let (mut start_idx, mut len): (u32, u32);
        match self._try_select_cached() {
            Ok(item) => return Ok(item),
            Err((_last_idx, _len)) => {
                if _len > 0 {
                    start_idx = _last_idx;
                    len = _len;
                } else {
                    return Err(true);
                }
            }
        }
        trace_log!("{self}: recv begin start={start_idx} len={len}");
        let mut backoff = Backoff::from(BackoffConfig::detect());
        backoff.snooze();
        loop {
            loop {
                if let Some(item) = self._try_select_all::<false>(start_idx, len) {
                    return Ok(item);
                }
                if backoff.snooze() {
                    break;
                }
            }
            // TODO For thread, actually the waker can be reuse and not change
            self.waker.init_blocking();
            for (i, handler) in self.handlers().iter().enumerate() {
                handler.reg_waker(i, &self.waker);
            }
            // NOTE: MultiplexDyn is not like Multiplex, we don't have registry replaced here.
            // we have to check their seperate connectivity states just like select::select
            if let Some(item) = self._check_all_final(&mut len) {
                return Ok(item);
            }
            trace_log!("{self}: final check len={len}");
            if len > 0 {
                let mut state = WakerState::Init as u8;
                while state < WakerState::Woken as u8 {
                    match check_timeout(deadline) {
                        Ok(None) => {
                            thread::park();
                        }
                        Ok(Some(dur)) => {
                            thread::park_timeout(dur);
                        }
                        Err(_) => {
                            // As sc don't need to abandon
                            return Err(false);
                        }
                    }
                    state = self.waker.get_waker_state(Ordering::SeqCst);
                    trace_log!("{self}: unpark state={}", state);
                }
                backoff.reset();
                start_idx = self.waker.get_hint() as u32;
            } else {
                return Err(true); // all closed
            }
        }
    }
}

impl<T> Drop for MultiplexDyn<T> {
    #[inline]
    fn drop(&mut self) {
        for handle in self.handlers() {
            handle.close(&self.waker);
        }
    }
}

impl<T> fmt::Debug for MultiplexDyn<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MultiplexDyn<{}>", std::any::type_name::<T>())
    }
}

impl<T> fmt::Display for MultiplexDyn<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl<T> BlockingRxTrait<T> for MultiplexDyn<T> {
    #[inline(always)]
    fn recv(&self) -> Result<T, RecvError> {
        MultiplexDyn::recv(self)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        MultiplexDyn::try_recv(self)
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        MultiplexDyn::recv_timeout(self, timeout)
    }

    /// The number of messages in the channel at the moment
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }

    /// always return None
    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        None
    }

    /// Returns true when all the channel's empty
    #[inline(always)]
    fn is_empty(&self) -> bool {
        for handle in self.handlers() {
            if !handle.shared().is_empty() {
                return false;
            }
        }
        true
    }

    /// Not practical to impl
    #[inline(always)]
    fn is_full(&self) -> bool {
        false
    }

    /// Return true if all sender has been close
    #[inline(always)]
    fn is_disconnected(&self) -> bool {
        self.get_tx_count() == 0
    }

    /// NOTE: it does not count all the clones to the senders, only update after recv()
    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.handlers().len()
    }

    /// This is single consumer
    #[inline(always)]
    fn get_rx_count(&self) -> usize {
        1
    }

    fn get_wakers_count(&self) -> (usize, usize) {
        (0, 0)
    }
}

impl<T> BlockingRxTrait<T> for &MultiplexDyn<T> {
    #[inline(always)]
    fn recv(&self) -> Result<T, RecvError> {
        MultiplexDyn::recv(self)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<T, TryRecvError> {
        MultiplexDyn::try_recv(self)
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        MultiplexDyn::recv_timeout(self, timeout)
    }

    /// The number of messages in the channel at the moment
    #[inline(always)]
    fn len(&self) -> usize {
        0
    }

    /// always return None
    #[inline(always)]
    fn capacity(&self) -> Option<usize> {
        None
    }

    /// Returns true when all the channel's empty
    #[inline(always)]
    fn is_empty(&self) -> bool {
        for handle in self.handlers() {
            if !handle.shared().is_empty() {
                return false;
            }
        }
        true
    }

    /// Not practical to impl
    #[inline(always)]
    fn is_full(&self) -> bool {
        false
    }

    /// Return true if all sender has been close
    #[inline(always)]
    fn is_disconnected(&self) -> bool {
        self.get_tx_count() == 0
    }

    /// NOTE: it does not count all the clones to the senders, only update after recv()
    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.handlers().len()
    }

    /// This is single consumer
    #[inline(always)]
    fn get_rx_count(&self) -> usize {
        1
    }

    fn get_wakers_count(&self) -> (usize, usize) {
        (0, 0)
    }
}
