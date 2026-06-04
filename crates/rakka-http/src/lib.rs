#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! HTTP integration foundation.

use rakka_core::Subsystem;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-http";

/// Default readiness path for HTTP examples.
pub const DEFAULT_READINESS_PATH: &str = "/ready";

/// Default liveness path for HTTP examples.
pub const DEFAULT_LIVENESS_PATH: &str = "/live";

/// Subsystem associated with HTTP integration.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Http
}
