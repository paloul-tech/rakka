#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Durable workflow reliability foundation.

use rakka_core::Subsystem;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-workflow";

/// Subsystem associated with durable workflow reliability.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Workflow
}

/// Default telemetry label for durable inbox processing.
pub const DURABLE_INBOX: &str = "durable-inbox";

/// Default telemetry label for durable outbox processing.
pub const DURABLE_OUTBOX: &str = "durable-outbox";
