//! Blocking select operations for choosing between multiple channel operations.
//!
//! This module provides functionality for waiting on multiple send and receive operations
//! simultaneously, similar to Go's `select` statement or Rust's `tokio::select!` macro
//! but for blocking/synchronous code.
//!
//! # Overview
//!
//! The blocking select API allows you to:
//! - Wait for the first channel operation to complete ([`FirstReadySelect`])
//! - Get all operations that complete in a single poll ([`AnyReadySelect`])
//! - Wait for all registered operations to complete ([`AllCompleteSelect`])
//!
//! # Examples
//!
//! ## Basic usage with first ready
//!
//! ```no_run
//! use crossfire::mpsc;
//! use crossfire::blocking_select::Select;
//!
//! let (tx1, rx1) = mpsc::bounded_blocking(10);
//! let (tx2, rx2) = mpsc::bounded_blocking(10);
//!
//! // Build a select operation
//! let mut select = Select::new(false);
//! select.recv(&rx1);
//! select.recv(&rx2);
//!
//! // Wait for the first operation to complete
//! let results = select.first_ready().select();
//! ```
//!
//! ## Checking multiple operations
//!
//! ```no_run
//! use crossfire::mpsc;
//! use crossfire::blocking_select::Select;
//!
//! let (tx1, rx1) = mpsc::bounded_blocking(10);
//! let (tx2, rx2) = mpsc::bounded_blocking(10);
//!
//! let mut select = Select::new(false);
//! select.recv(&rx1);
//! select.recv(&rx2);
//!
//! // Get all operations that became ready
//! let results = select.any_ready().select();
//! for result in results.successes() {
//!     println!("Received at idx {}", result.idx());
//! }
//! ```

mod errors;
mod handles;
mod results;
mod select;

// Re-export public types
pub use errors::{SelectTimeoutError, TrySelectError};
pub use results::{SelectResp, SelectResults};
pub use select::{AllCompleteSelect, AnyReadySelect, FirstReadySelect, Select, SelectMode};
