#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Durable state API foundation.

use rakka_core::Subsystem;
use serde::{Deserialize, Serialize};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-persistence";

/// Subsystem associated with durable state APIs.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Persistence
}

/// Stable durable identity for an actor or entity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistenceId(String);

impl PersistenceId {
    /// Creates a new persistence id.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the persistence id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Marker trait for future durable state stores.
pub trait DurableStateStore: Send + Sync {
    /// Stable backend name used in telemetry.
    fn backend_name(&self) -> &'static str;
}
