//! gRPC unary adapter configuration and compatibility constants.

use std::time::Duration;

/// Tonic is the gRPC runtime selected for Rakka v1.
pub const V1_GRPC_RUNTIME_PRIMITIVE: &str = "tonic";

/// Protobuf compatibility rule for Rakka v1 rolling updates.
pub const V1_GRPC_PROTOBUF_COMPATIBILITY: &str =
    "Rakka v1 gRPC APIs require N/N+1 Protobuf compatibility during rolling updates: \
     add fields compatibly, reserve removed field numbers and enum values, and keep defaults \
     backward compatible.";

/// Default timeout for unary gRPC request handling.
pub const DEFAULT_GRPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-method unary gRPC adapter configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcUnaryConfig {
    request_timeout: Duration,
}

impl GrpcUnaryConfig {
    /// Creates unary adapter config with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            request_timeout: DEFAULT_GRPC_REQUEST_TIMEOUT,
        }
    }

    /// Sets the maximum request timeout enforced by the adapter.
    #[must_use]
    pub const fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Maximum request timeout enforced by the adapter.
    #[must_use]
    pub const fn request_timeout_value(&self) -> Duration {
        self.request_timeout
    }
}

impl Default for GrpcUnaryConfig {
    fn default() -> Self {
        Self::new()
    }
}
