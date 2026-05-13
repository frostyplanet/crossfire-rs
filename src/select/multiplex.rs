use crate::backoff::*;
use crate::flavor::{Flavor, FlavorBounded, FlavorImpl, FlavorNew, FlavorWrap};
use crate::shared::{check_timeout, ChannelShared};
use crate::waker::WakerState;
use crate::waker_registry::{RegistrySend, SelectWaker, SelectWakerWrapper};
use crate::BlockingRxTrait;
use crate::SenderType;
use crate::{RecvError, RecvTimeoutError, TryRecvError};
use std::cell::Cell;
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub const DEFAULT_WEIGHT: u32 = 128;

/// Type alias for multiplexed channel flavor
pub type Mux<F> = FlavorWrap<F, <F as Flavor>::Send, SelectWakerWrapper>;

/// A multiplexer that owns multi channel receivers of the same Flavor type.
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
/// - New channel may be added on the fly
/// - This abstraction is only designed for stable channels for most efficient select.
/// - If channel close by sender, the receiver will be automatically close inside the Multiplex,
///   user will not be notify until all its channels closed.
/// - Due to it binds on Flavor interface, it cannot be use between different type.
///   If you want to multiplex between list and array, can use the
///   [CompatFlavor](crate::compat::CompatFlavor)
/// - **NOTE** : It has internal mutability because it need to impl [BlockingRxTrait](crate::BlockingRxTrait),
///   the adding channel process remains `&mut self`. Because `Multiplex` is a single consumer just
///   like [Rx](crate::Rx), it does not have `Sync`. If you can guarantee no concurrent access you
///   can manutally add the `Sync` back in parent struct.
///
///
/// # Examples
///
/// Basic usage with multiple senders:
///
/// ```
/// use crossfire::{mpsc::Array, MTx, select::{Multiplex, Mux}};
/// use std::thread;
///
/// // Create a multiplexer with Array flavor
/// let mut mp = Multiplex::<Array<i32>>::new();
///
/// // Create multiple senders through the multiplexer
/// let tx1: MTx<Mux<Array<i32>>> = mp.bounded_tx(10);
/// let tx2: MTx<Mux<Array<i32>>> = mp.bounded_tx(10);
///
/// // Send values from different threads
/// let h1 = thread::spawn(move || {
///     tx1.send(1).unwrap();
/// });
/// let h2 = thread::spawn(move || {
///     tx2.send(2).unwrap();
/// });
///
/// // Receive values through the multiplexer (order may vary)
/// let val1 = mp.recv().unwrap();
/// let val2 = mp.recv().unwrap();
///
/// h1.join().unwrap();
/// h2.join().unwrap();
/// ```
pub struct Multiplex<F: Flavor> {
    waker: Arc<SelectWaker>,
    handlers: Vec<MultiplexHandle<F>>,
    last_idx: Cell<usize>,
    count: Cell<u32>,
}

unsafe impl<F: Flavor> Send for Multiplex<F> {}

struct MultiplexHandle<F: Flavor> {
    shared: Arc<ChannelShared<Mux<F>>>,
    weight: u32,
}

impl<F: Flavor> Multiplex<F> {
    /// Initialize Select with fair, round-robin strategy
    pub fn new() -> Self {
        Self {
            waker: Arc::new(SelectWaker::new()),
            handlers: Vec::with_capacity(10),
            count: Cell::new(0),
            last_idx: Cell::new(0),
        }
    }

    #[inline]
    fn _add_item(&mut self, flavor: F, weight: u32) -> Arc<ChannelShared<Mux<F>>> {
        self.waker.add_opened();
        let recvs = self.waker.clone().to_wrapper(self.handlers.len());
        let shared = ChannelShared::new(Mux::<F>::from_inner(flavor), F::Send::new(), recvs);
        self.handlers.push(MultiplexHandle { shared: shared.clone(), weight: weight - 1 });
        self.last_idx.set(self.handlers.len() - 1);
        shared
    }

    /// Add a new channels with a new() method to multiplex, return its sender.
    ///
    /// # Type Parameters
    ///
    /// * `S`: The sender type that implements SenderType with the appropriate Flavor,
    ///   may be async or blocking sender, MP or SP that match the `Flavor` type.
    ///
    /// # Note
    ///
    /// This method is only available for flavors that implement `FlavorNew` trait,
    /// such as `List` / `One` flavor. For flavors like Array that don't implement `FlavorNew`,
    /// use `bounded_tx` instead.
    ///
    /// # Example
    ///
    /// with mpsc::List (which sender type is [MTx](crate::MTx) and allow to clone)
    ///
    /// ```
    /// use crossfire::{mpsc::List, MTx, select::{Multiplex, Mux}};
    /// use tokio;
    ///
    /// let mut mp = Multiplex::<List<i32>>::new();
    /// let tx1: MTx<Mux<List<i32>>> = mp.new_tx();
    /// let tx2: MTx<Mux<List<i32>>> = mp.new_tx();
    /// tx1.send(42).expect("send");
    /// tx2.send(42).expect("send");
    /// let value = mp.recv().unwrap();
    /// assert_eq!(value, 42);
    /// let value = mp.recv().unwrap();
    /// assert_eq!(value, 42);
    /// ```
    ///
    /// with spsc::One (which sender type is [Tx](crate::Tx) and not cloneable)
    /// ```
    /// use crossfire::{spsc::One, Tx, select::{Multiplex, Mux}};
    /// use tokio;
    ///
    /// let mut mp = Multiplex::<One<i32>>::new();
    /// // Creates an size-1 channel
    /// let tx1: Tx<Mux<One<i32>>> = mp.new_tx();
    /// // Creates another size-1 channel
    /// let tx2: Tx<Mux<One<i32>>> = mp.new_tx();
    /// std::thread::spawn(move ||{
    ///     tx2.send(42).expect("send");
    /// });
    /// let value = mp.recv().unwrap();
    /// assert_eq!(value, 42);
    /// ```
    pub fn new_tx<S>(&mut self) -> S
    where
        F: FlavorNew,
        S: SenderType<Flavor = Mux<F>>,
    {
        let shared = self._add_item(F::new(), DEFAULT_WEIGHT);
        S::new(shared)
    }

    /// Add a channel of flavor (impl FlavorNew), with custom weight instead of default
    /// (the default weight is 128)
    pub fn new_tx_with_weight<S>(&mut self, weight: u32) -> S
    where
        F: FlavorNew,
        S: SenderType<Flavor = Mux<F>>,
    {
        let shared = self._add_item(F::new(), weight);
        S::new(shared)
    }

    /// Creates a new bounded sender for the multiplexer
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum capacity of the channel
    ///
    /// # Type Parameters
    ///
    /// * `S` - The sender type that implements SenderType with the appropriate Flavor
    ///
    /// # Example
    ///
    /// ```
    /// use crossfire::{mpsc::Array, *, select::{Multiplex, Mux}};
    ///
    /// let mut mp = Multiplex::<Array<i32>>::new();
    /// // Creates a bounded channel with capacity 10
    /// let tx1: MTx<Mux<Array<i32>>> = mp.bounded_tx(10);
    /// // Creates another bounded channel with capacity 20
    /// let tx2: MTx<Mux<Array<i32>>> = mp.bounded_tx(20);
    /// tx1.send(42).expect("send");
    /// std::thread::spawn(move || {
    ///     tx2.send(42).expect("send");
    /// });
    /// let value = mp.recv().unwrap();
    /// assert_eq!(value, 42);
    /// let value = mp.recv().unwrap();
    /// assert_eq!(value, 42);
    /// ```
    pub fn bounded_tx<S>(&mut self, size: usize) -> S
    where
        F: FlavorBounded,
        S: SenderType<Flavor = Mux<F>>,
    {
        let shared = self._add_item(F::new_with_bound(size), DEFAULT_WEIGHT);
        S::new(shared)
    }

    /// Add a bounded channel to the multiplex, with custom weight (the default is 128)
    pub fn bounded_tx_with_weight<S>(&mut self, size: usize, weight: u32) -> S
    where
        F: FlavorBounded,
        S: SenderType<Flavor = Mux<F>>,
    {
        let shared = self._add_item(F::new_with_bound(size), weight);
        S::new(shared)
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
    /// use crossfire::{mpsc::Array, select::{Multiplex, Mux}, MTx, TryRecvError};
    ///
    /// let mut mp = Multiplex::<Array<i32>>::new();
    /// let tx1: MTx<Mux<_>> = mp.bounded_tx(10);
    /// let _tx2: MTx<Mux<_>> = mp.bounded_tx(10);
    /// // No message available yet
    /// assert_eq!(mp.try_recv(), Err(TryRecvError::Empty));
    /// tx1.send(42).unwrap();
    /// // Now a message is available
    /// assert_eq!(mp.try_recv(), Ok(42));
    /// ```
    #[inline]
    pub fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        let last_idx = self.last_idx.get();
        if let Some(item) = self._try_select_all::<true>(last_idx, self.handlers.len()) {
            return Ok(item);
        }
        if self.waker.get_opened_count() == 0 {
            return Err(TryRecvError::Disconnected);
        }
        Err(TryRecvError::Empty)
    }

    /// Receives a message from any of the multiplexed channels, blocking if necessary.
    ///
    /// This method will block the current thread until a message is available on any of the channels,
    /// or until all senders are dropped.
    #[inline]
    pub fn recv(&self) -> Result<F::Item, RecvError> {
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
    pub fn recv_timeout(&self, timeout: Duration) -> Result<F::Item, RecvTimeoutError> {
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

    /// NOTE: be aware that _try_recv_cached does not guarantee all message will be receive,
    /// should retry again
    #[inline(always)]
    fn _try_select_cached<const FINAL: bool>(&self) -> Result<F::Item, usize> {
        let last_idx = self.last_idx.get();
        let handle = unsafe { self.handlers.get_unchecked(last_idx) };
        let count = self.count.get();
        let loop_count = if count > 0 {
            if let Some(msg) = handle.shared.inner.try_recv_cached() {
                handle.shared.on_recv();
                self.count.set(count - 1);
                return Ok(msg);
            }
            self.handlers.len() - 1
        } else {
            self.handlers.len()
        };
        if let Some(item) = self._try_select_all::<FINAL>(last_idx, loop_count) {
            return Ok(item);
        }
        Err(last_idx)
    }

    #[inline(always)]
    fn _try_select_all<const FINAL: bool>(
        &self, mut idx: usize, loop_count: usize,
    ) -> Option<F::Item> {
        let len = self.handlers.len();
        for _ in 0..loop_count {
            idx = if idx + 1 >= len { 0 } else { idx + 1 };
            let handle = unsafe { self.handlers.get_unchecked(idx) };
            if let Some(msg) = if FINAL {
                handle.shared.inner.try_recv_final()
            } else {
                handle.shared.inner.try_recv()
            } {
                handle.shared.on_recv();
                self.count.set(handle.weight);
                self.last_idx.set(idx);
                return Some(msg);
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
    fn _recv_blocking(&self, deadline: Option<Instant>) -> Result<F::Item, bool> {
        let mut start_idx;
        match self._try_select_cached::<false>() {
            Ok(item) => return Ok(item),
            Err(idx) => {
                start_idx = idx;
            }
        }
        let mut backoff = Backoff::from(BackoffConfig::detect());
        backoff.snooze();
        let len = self.handlers.len();
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
            let closing = self.waker.get_opened_count() == 0;
            if let Some(item) = self._try_select_all::<true>(start_idx, len) {
                return Ok(item);
            }
            if closing {
                // NOTE: double check the channels after checking close count, otherwise we will be
                // missing some last messages
                return Err(true);
            }
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
            }
            backoff.reset();
            start_idx = self.waker.get_hint();
        }
    }
}

impl<F: Flavor> Drop for Multiplex<F> {
    #[inline]
    fn drop(&mut self) {
        for handle in &self.handlers {
            handle.shared.close_rx();
        }
    }
}

impl<F: Flavor> fmt::Debug for Multiplex<F> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Multiplex<{}>", std::any::type_name::<F>())
    }
}

impl<F: Flavor> fmt::Display for Multiplex<F> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl<F: Flavor> BlockingRxTrait<F::Item> for Multiplex<F> {
    #[inline(always)]
    fn recv(&self) -> Result<F::Item, RecvError> {
        Multiplex::recv(self)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        Multiplex::try_recv(self)
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<F::Item, RecvTimeoutError> {
        Multiplex::recv_timeout(self, timeout)
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
        for handle in &self.handlers {
            if !handle.shared.is_empty() {
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

    /// NOTE: it does not count all the clones to the senders
    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.waker.get_opened_count()
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

impl<F: Flavor> BlockingRxTrait<F::Item> for &Multiplex<F> {
    #[inline(always)]
    fn recv(&self) -> Result<F::Item, RecvError> {
        Multiplex::recv(self)
    }

    #[inline(always)]
    fn try_recv(&self) -> Result<F::Item, TryRecvError> {
        Multiplex::try_recv(self)
    }

    #[inline(always)]
    fn recv_timeout(&self, timeout: Duration) -> Result<F::Item, RecvTimeoutError> {
        Multiplex::recv_timeout(self, timeout)
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
        for handle in &self.handlers {
            if !handle.shared.is_empty() {
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

    /// NOTE: it does not count all the clones to the senders
    #[inline(always)]
    fn get_tx_count(&self) -> usize {
        self.waker.get_opened_count()
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
