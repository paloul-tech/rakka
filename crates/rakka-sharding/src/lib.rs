#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Cluster sharding and entity routing foundation.

use rakka_core::Subsystem;
use serde::{Deserialize, Serialize};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-sharding";

/// Subsystem associated with sharding.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Sharding
}

/// Named actor type for sharded entities.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityType(String);

impl EntityType {
    /// Creates a new entity type name.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the entity type as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
