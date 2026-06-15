#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! gRPC integration foundation.

use rakka_core::Subsystem;

mod config;
mod error;
mod metrics;
mod shutdown;
mod streaming;
mod unary;

pub use config::{
    GrpcStreamConfig, GrpcUnaryConfig, DEFAULT_GRPC_REQUEST_TIMEOUT,
    DEFAULT_GRPC_STREAM_BUFFER_CAPACITY, DEFAULT_GRPC_STREAM_DRAIN_TIMEOUT,
    V1_GRPC_GENERATED_API_VERSION, V1_GRPC_PROTOBUF_COMPATIBILITY, V1_GRPC_RUNTIME_PRIMITIVE,
};
pub use error::{
    decode_status, service_status, stream_status, validation_status, GrpcError,
    RAKKA_GRPC_ERROR_CODE_METADATA,
};
pub use metrics::record_grpc_request_metrics;
pub use shutdown::{
    register_grpc_shutdown_task, GrpcServerShutdownResult, GrpcShutdownHandle, GrpcShutdownSignal,
    GrpcShutdownSnapshot,
};
pub use streaming::{
    bidi_stream_pair, bidi_stream_pair_from_request, bidi_stream_pair_from_stream,
    bidi_streaming_service, bidi_streaming_service_from_stream, client_stream_from_request,
    client_stream_from_stream, client_streaming_service, client_streaming_service_from_stream,
    server_streaming_response, server_streaming_service, GrpcBidiStreaming, GrpcClientStreaming,
    GrpcResponseStream, GrpcServerStreamingService, GrpcStreamPump,
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
