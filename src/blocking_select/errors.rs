/// Error type for try-select operations that can fail in multiple ways.
///
/// Each variant includes the index of the operation that failed, allowing callers
/// to identify which channel operation encountered the error.
#[derive(Debug, Clone)]
pub enum TrySelectError<T> {
    /// The receive operation found an empty channel.
    RecvEmpty { idx: usize },
    /// The receive operation found a disconnected channel.
    RecvDisconnected { idx: usize },
    /// The send operation found a full channel and returned the item.
    SendEmpty { idx: usize, item: T },
    /// The send operation found a disconnected channel and returned the item.
    SendDisconnected { idx: usize, item: T },
}

impl<T> TrySelectError<T> {
    /// Returns the index of the operation that encountered this error.
    pub fn idx(&self) -> usize {
        match self {
            TrySelectError::RecvEmpty { idx } => *idx,
            TrySelectError::RecvDisconnected { idx } => *idx,
            TrySelectError::SendEmpty { idx, .. } => *idx,
            TrySelectError::SendDisconnected { idx, .. } => *idx,
        }
    }
}

/// Error type for select operations with timeouts.
#[derive(Debug)]
pub enum SelectTimeoutError {
    /// The operation timed out before any channels became ready.
    Timeout,
}
