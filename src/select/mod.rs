//! # Selection between channels
//!
//! This module provides:
//! - [Select]: Allows selecting from multiple borrowed receiver references,
//!   which is a type-erased interface similar to the select in crossbeam-channel, supporting both `mpmc`, `mpsc`, and `spsc` channels.
//! - [Multiplex]: Owns and reads from multiple channels as a non-concurrent consumer, for `spsc`, `mpsc`.
//! - [MultiplexDyn]: Multiplex that owns multiple,
//!   mixing different type of receivers (spsc/mpsc/mpmc, bounded/unbounded),
//!   for the same type of message.
//!
//! Performance:  dedicated channel > `Multiplex` > `MultiplexDyn` > `Select`

#[allow(clippy::module_inception)]
pub(crate) mod select;
pub use select::{Select, SelectResult};
#[allow(private_interfaces)]
mod multiplex;
pub use multiplex::{Multiplex, Mux};
mod multiplex_dyn;
pub use multiplex_dyn::MultiplexDyn;

#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum SelectMode {
    RR,
    Rand,
    Bias,
}
