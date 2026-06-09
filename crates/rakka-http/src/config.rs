//! HTTP server and route configuration.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// HTTP server primitive selected for Rakka v1.
pub const V1_HTTP_SERVER_PRIMITIVE: &str = "axum";

/// Default maximum unary request payload size.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Default timeout for unary request handling.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Default graceful shutdown timeout used by server configuration.
pub const DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Default bind port used by HTTP examples.
pub const DEFAULT_HTTP_PORT: u16 = 8080;

/// Per-route unary HTTP adapter configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpRouteConfig {
    request_timeout: Duration,
    max_payload_bytes: usize,
}

impl HttpRouteConfig {
    /// Creates a route config with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        }
    }

    /// Sets the request timeout.
    #[must_use]
    pub const fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Sets the maximum request payload size.
    #[must_use]
    pub const fn max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }

    /// Request timeout for service handlers and actor/entity ask adapters.
    #[must_use]
    pub const fn request_timeout_value(&self) -> Duration {
        self.request_timeout
    }

    /// Maximum unary request payload size.
    #[must_use]
    pub const fn max_payload_bytes_value(&self) -> usize {
        self.max_payload_bytes
    }
}

impl Default for HttpRouteConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// HTTP server configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpServerConfig {
    bind_addr: SocketAddr,
    route: HttpRouteConfig,
    graceful_shutdown_timeout: Duration,
}

impl HttpServerConfig {
    /// Creates a server config bound to the provided address.
    #[must_use]
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            route: HttpRouteConfig::default(),
            graceful_shutdown_timeout: DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT,
        }
    }

    /// Sets per-route defaults used by server builders.
    #[must_use]
    pub const fn route(mut self, route: HttpRouteConfig) -> Self {
        self.route = route;
        self
    }

    /// Sets the graceful shutdown timeout recorded by this server config.
    #[must_use]
    pub const fn graceful_shutdown_timeout(mut self, graceful_shutdown_timeout: Duration) -> Self {
        self.graceful_shutdown_timeout = graceful_shutdown_timeout;
        self
    }

    /// Address the server should bind.
    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Per-route defaults used by server builders.
    #[must_use]
    pub const fn route_config(&self) -> HttpRouteConfig {
        self.route
    }

    /// Graceful shutdown timeout configured for operators.
    #[must_use]
    pub const fn graceful_shutdown_timeout_value(&self) -> Duration {
        self.graceful_shutdown_timeout
    }
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            DEFAULT_HTTP_PORT,
        ))
    }
}
