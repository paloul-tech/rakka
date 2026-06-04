#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! gRPC integration foundation.

use rakka_core::Subsystem;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-grpc";

/// Result type used by gRPC integration helpers.
pub type GrpcResult<T> = Result<T, tonic::Status>;

/// Subsystem associated with gRPC integration.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Grpc
}
