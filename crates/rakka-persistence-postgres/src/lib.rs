#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! PostgreSQL durable state plugin foundation.

use rakka_core::Subsystem;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-persistence-postgres";

/// Subsystem associated with the PostgreSQL durable state plugin.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::PersistencePostgres
}

/// Backend name for PostgreSQL durable state telemetry.
pub const BACKEND_NAME: &str = "postgres";
