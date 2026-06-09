#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! gRPC integration foundation.

use rakka_core::Subsystem;

mod config;
mod error;
mod unary;

pub use config::{
    GrpcUnaryConfig, DEFAULT_GRPC_REQUEST_TIMEOUT, V1_GRPC_PROTOBUF_COMPATIBILITY,
    V1_GRPC_RUNTIME_PRIMITIVE,
};
pub use error::{
    decode_status, service_status, validation_status, GrpcError, RAKKA_GRPC_ERROR_CODE_METADATA,
};
pub use unary::{
    effective_request_timeout, request_timeout_from_metadata, unary_actor_ask, unary_actor_tell,
    unary_entity_ask, unary_entity_tell, unary_service, GrpcUnaryService,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-grpc";

/// Result type used by gRPC integration helpers.
pub type GrpcResult<T> = Result<T, tonic::Status>;

/// Subsystem associated with gRPC integration.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Grpc
}
