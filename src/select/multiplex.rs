use super::SelectMode;
use crate::backoff::*;
use crate::flavor::{Flavor, FlavorBounded, FlavorImpl, FlavorNew, FlavorWrap};
use crate::shared::{check_timeout, ChannelShared};
use crate::waker::WakerState;
use crate::waker_registry::{RegistrySend, SelectWaker, SelectWakerWrapper};
use crate::SenderType;
use crate::{RecvError, RecvTimeoutError, TryRecvError};
/// A multiplex blocking receiver of channels of the same type, only supports mpsc
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Type alias for multiplexed channel flavor
pub type Mux<F> = FlavorWrap<F, <F as Flavor>::Send, SelectWakerWrapper>;

/// A multiplexer that owns multi channel receivers of the same Flavor type.
///
/// ## selection modes
/// - Round-robin (RR): Fair distribution by cycling through channels
/// - Random (Rand): Random selection from available channels
/// - Bias: Priority based on the order channels were added
///
/// ## Capability and limitation:
/// - New channel may be added on the fly
/// - This abstraction is only designed for stable channels for most efficient select.
/// - If channel close by sender, the receiver will be automatically close inside the Multiplex,
/// user will not be notify until all its channels closed.
/// - Due to it binds on Flavor interface, it cannot be use between different type.
/// If you want to multiplex between list and array, can use the
/// [CompatFlavor](crate::compat::CompatFlavor)
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
    mode: SelectMode,
    handlers: Vec<Option<Arc<ChannelShared<Mux<F>>>>>,
    waker: Arc<SelectWaker>,
    next_index: usize,
    opened_count: usize,
    rng: usize,
}

unsafe impl<F: Flavor> Send for Multiplex<F> {}

impl<F: Flavor> Multiplex<F> {
    /// Initialize Select with fair, round-robin strategy
    pub fn new() -> Self {
        Self::new_with(SelectMode::RR)
    }

    /// Initialize Select with fair strategy (check start from random channel)
    ///
    /// # Example
    ///
    /// ```
    /// use crossfire::{mpsc::Array, select::{Multiplex, SelectMode}};
    ///
    /// let mut mp = Multiplex::<Array<i32>>::new_random();
    /// // The selection will start from a random channel each time
    /// ```
    #[inline]
    pub fn new_random() -> Self {
        Self::new_with(SelectMode::Rand)
    }

    /// Initialize Select with bias strategy (check according to the order of `add()`)
    ///
    /// # Example
    ///
    /// ```
    /// use crossfire::{mpsc::Array, select::{Multiplex, SelectMode}};
    ///
    /// let mut mp = Multiplex::<Array<i32>>::new_bias();
    /// // The selection will prioritize channels in the order they were added
    /// ```
    #[inline]
    pub fn new_bias() -> Self {
        Self::new_with(SelectMode::Bias)
    }

    /// Initialize Select with a custom selection mode
    ///
    /// # Arguments
    ///
    /// * `mode` - The selection mode to use (Round-robin, Random, or Bias)
    ///
    /// # Example
    ///
    /// ```
    /// use crossfire::{mpsc::Array, select::{Multiplex, SelectMode}};
    ///
    /// let mut mp = Multiplex::<Array<i32>>::new_with(SelectMode::RR);
    /// ```
    #[inline(always)]
    pub fn new_with(mode: SelectMode) -> Self {
        Self {
            handlers: Vec::with_capacity(10),
            waker: Arc::new(SelectWaker::new()),
            next_index: 0,
            rng: 0,
            opened_count: 0,
            mode,
        }
    }

    /// Add a new channels with a new() method to multiplex, return its sender.
    ///
    /// # Type Parameters
    ///
    /// * `S` - The sender type that implements SenderType with the appropriate Flavor,
    /// may be async or blocking sender, MP or SP that match the `Flavor` type.
    ///
    /// # Note
    ///
    /// This method is only available for flavors that implement `FlavorNew` trait,
    /// such as `List` / `One` flavor. For flavors like Array that don't implement `FlavorNew`,
    /// use `bounded_tx` instead.
    ///
    /// # Example
    ///
    /// with mpsc::List
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
    /// with spsc::One
    /// ```
    /// use crossfire::{spsc::One, Tx, select::{Multiplex, Mux}};
    /// use tokio;
    ///
    /// let mut mp = Multiplex::<One<i32>>::new();
    /// let tx1: Tx<Mux<One<i32>>> = mp.new_tx(); // Creates an unbounded sender for List flavor
    /// let tx2: Tx<Mux<One<i32>>> = mp.new_tx(); // Creates an unbounded sender for List flavor
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
        self.opened_count += 1;
        self.waker.add_opened();
        let recvs = self.waker.clone().to_wrapper(self.handlers.len());
        let shared = ChannelShared::new(Mux::<F>::from_inner(F::new()), F::Send::new(), recvs);
        self.handlers.push(Some(shared.clone()));
        return S::new(shared);
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
    /// let tx1: MTx<Mux<Array<i32>>> = mp.bounded_tx(10); // Creates a bounded sender with capacity 10
    /// let tx2: MTx<Mux<Array<i32>>> = mp.bounded_tx(10); // Creates a bounded sender with capacity 10
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
        self.opened_count += 1;
        self.waker.add_opened();
        let recvs = self.waker.clone().to_wrapper(self.handlers.len());
        let shared =
            ChannelShared::new(Mux::from_inner(F::new_with_bound(size)), F::Send::new(), recvs);
        self.handlers.push(Some(shared.clone()));
        return S::new(shared);
    }

    #[inline(always)]
    fn _try_select_begin(&mut self) -> usize {
        let len = self.handlers.len();
        debug_assert!(len > 0);
        match self.mode {
            SelectMode::Bias => 0,
            SelectMode::RR => {
                if self.next_index >= self.handlers.len() {
                    0
                } else {
                    self.next_index
                }
            }
            SelectMode::Rand => {
                let mut x = self.rng;
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                self.rng = x;
                (x as usize) % len
            }
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
    pub fn try_recv(&mut self) -> Result<F::Item, TryRecvError> {
        if self.opened_count == 0 {
            return Err(TryRecvError::Disconnected);
        }
        let idx = self._try_select_begin();
        if let Some(item) = self._try_select::<true>(idx) {
            return Ok(item);
        }
        Err(TryRecvError::Empty)
    }

    /// Receives a message from any of the multiplexed channels, blocking if necessary.
    ///
    /// This method will block the current thread until a message is available on any of the channels,
    /// or until all senders are dropped.
    #[inline]
    pub fn recv(&mut self) -> Result<F::Item, RecvError> {
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
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<F::Item, RecvTimeoutError> {
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

    /// Internal method to attempt selecting a message from the channels starting at a given index
    ///
    /// # Parameters
    ///
    /// * `FINAL` - A const generic parameter that determines whether to use final receive operation
    /// * `idx` - The starting index to check for available messages
    #[inline(always)]
    fn _try_select<const FINAL: bool>(&mut self, mut idx: usize) -> Option<F::Item> {
        let should_check_close = if FINAL {
            let opened_count = self.waker.get_opened_count();
            // if the flag in SelectWaker equals to self.opened_count, can skip loading the tx_count atomic
            opened_count != self.opened_count
        } else {
            false
        };
        let len = self.handlers.len();
        for _ in 0..len {
            if let Some(shared) = self.handlers[idx].as_ref() {
                let r = if FINAL { shared.inner.try_recv_final() } else { shared.inner.try_recv() };
                if let Some(item) = r {
                    shared.on_recv();
                    if SelectMode::RR == self.mode {
                        self.next_index = idx + 1;
                    }
                    return Some(item); // Message available
                }
                if should_check_close {
                    // check close only after all message is received from the channel
                    if shared.get_tx_count() == 0 {
                        self.handlers[idx] = None;
                        self.opened_count -= 1;
                    }
                }
            }
            idx += 1;
            if idx >= len {
                idx = 0;
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
    fn _recv_blocking(&mut self, deadline: Option<Instant>) -> Result<F::Item, bool> {
        if self.opened_count == 0 {
            return Err(true);
        }
        let mut start_idx = self._try_select_begin();
        if let Some(item) = self._try_select::<false>(start_idx) {
            return Ok(item);
        }
        let cfg = BackoffConfig::default();
        let mut backoff = Backoff::new(cfg);
        backoff.snooze();
        loop {
            loop {
                if let Some(item) = self._try_select::<false>(start_idx) {
                    return Ok(item);
                }
                if backoff.snooze() {
                    break;
                }
            }
            self.waker.init_blocking();
            if let Some(item) = self._try_select::<true>(start_idx) {
                return Ok(item);
            }
            // FINAL=true will check close and decrease opened_count
            if self.opened_count == 0 {
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
    fn drop(&mut self) {
        for _handler in &self.handlers {
            if let Some(handler) = _handler.as_ref() {
                handler.close_rx();
            }
        }
    }
}

impl<F: Flavor> std::fmt::Debug for Multiplex<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Multiplex<{}>", std::any::type_name::<F>())
    }
}
