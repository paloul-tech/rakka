#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! HTTP integration foundation.

use rakka_core::Subsystem;

mod config;
mod error;
mod routes;
mod server;

pub use axum::Router;
pub use config::{
    HttpRouteConfig, HttpServerConfig, DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT, DEFAULT_HTTP_PORT,
    DEFAULT_MAX_PAYLOAD_BYTES, DEFAULT_REQUEST_TIMEOUT, V1_HTTP_SERVER_PRIMITIVE,
};
pub use error::{HttpError, HttpErrorBody, HttpResult};
pub use routes::{
    binary_service_route, into_response, json_actor_ask_route, json_actor_tell_route,
    json_entity_ask_route, json_entity_tell_route, json_service_route, HttpAccepted, HttpRouter,
};
pub use server::serve_with_graceful_shutdown;

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
