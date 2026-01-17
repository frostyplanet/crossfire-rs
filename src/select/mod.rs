//! # Selection between channels
//!
//! This module provides:
//! - [Select]: Allows selecting from multiple borrowed receiver references,
//!   which is a type-erased interface similar to the select in crossbeam-channel, supporting both `mpmc`, `mpsc`, and `spsc` channels.
//! - [Multiplex]: Owns and reads from multiple channels as a non-concurrent consumer, mainly for `spsc`, `mpsc`.
//!
//! Performance:  dedicated channel > multiplex > select

pub(crate) mod select;
pub use select::{Select, SelectResult};
#[allow(private_interfaces)]
mod multiplex;
pub use multiplex::{Multiplex, Mux};

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum SelectMode {
    RR,
    Rand,
    Bias,
}
