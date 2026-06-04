#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Bounded stream adapter foundation.

use rakka_core::Subsystem;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-stream";

/// Default bounded buffer capacity for examples and tests.
pub const DEFAULT_BUFFER_CAPACITY: usize = 1024;

/// Subsystem associated with streams.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Stream
}
