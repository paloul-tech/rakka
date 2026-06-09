//! Axum server helper for Rakka HTTP routers.

use std::future::Future;

use axum::Router;

use crate::{HttpError, HttpResult, HttpServerConfig};

/// Starts an Axum server with a caller-provided graceful shutdown signal.
pub async fn serve_with_graceful_shutdown(
    router: Router,
    config: HttpServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> HttpResult<()> {
    let address = config.bind_addr();
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .map_err(|error| HttpError::Bind {
            address,
            message: error.to_string(),
        })?;

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(|error| HttpError::Serve {
            message: error.to_string(),
        })
}
