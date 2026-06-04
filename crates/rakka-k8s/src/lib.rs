#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Kubernetes integration foundation.

use rakka_core::Subsystem;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-k8s";

/// Default Kubernetes readiness endpoint.
pub const DEFAULT_READINESS_PATH: &str = "/ready";

/// Default Kubernetes liveness endpoint.
pub const DEFAULT_LIVENESS_PATH: &str = "/live";

/// Subsystem associated with Kubernetes integration.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::K8s
}
