//! gRPC adapter configuration and compatibility constants.

use std::time::Duration;

use rakka_stream::DEFAULT_BUFFER_CAPACITY;

/// Tonic is the gRPC runtime selected for Rakka v1.
pub const V1_GRPC_RUNTIME_PRIMITIVE: &str = "tonic";

/// Protobuf compatibility rule for Rakka v1 rolling updates.
pub const V1_GRPC_PROTOBUF_COMPATIBILITY: &str =
    "Rakka v1 gRPC APIs require N/N+1 Protobuf compatibility during rolling updates: \
     add fields compatibly, reserve removed field numbers and enum values, and keep defaults \
     backward compatible.";

/// Default timeout for unary gRPC request handling.
pub const DEFAULT_GRPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Default bounded buffer capacity for gRPC stream adapters.
pub const DEFAULT_GRPC_STREAM_BUFFER_CAPACITY: usize = DEFAULT_BUFFER_CAPACITY;

/// Default timeout used while waiting for streaming handlers to drain.
pub const DEFAULT_GRPC_STREAM_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Per-method streaming gRPC adapter configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcStreamConfig {
    request_timeout: Duration,
    drain_timeout: Duration,
    buffer_capacity: usize,
}

impl GrpcStreamConfig {
    /// Creates streaming adapter config with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            request_timeout: DEFAULT_GRPC_REQUEST_TIMEOUT,
            drain_timeout: DEFAULT_GRPC_STREAM_DRAIN_TIMEOUT,
            buffer_capacity: DEFAULT_GRPC_STREAM_BUFFER_CAPACITY,
        }
    }

    /// Sets the maximum request timeout enforced by the adapter.
    #[must_use]
    pub const fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Sets the graceful drain timeout used by streaming service helpers.
    #[must_use]
    pub const fn drain_timeout(mut self, drain_timeout: Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }

    /// Sets the bounded buffer capacity between tonic streams and Rakka streams.
    #[must_use]
    pub const fn buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.buffer_capacity = buffer_capacity;
        self
    }

    /// Maximum request timeout enforced by the adapter.
    #[must_use]
    pub const fn request_timeout_value(&self) -> Duration {
        self.request_timeout
    }

    /// Graceful drain timeout used by streaming service helpers.
    #[must_use]
    pub const fn drain_timeout_value(&self) -> Duration {
        self.drain_timeout
    }

    /// Bounded buffer capacity between tonic streams and Rakka streams.
    #[must_use]
    pub const fn buffer_capacity_value(&self) -> usize {
        self.buffer_capacity
    }
}

impl Default for GrpcStreamConfig {
    fn default() -> Self {
        Self::new()
    }
}
