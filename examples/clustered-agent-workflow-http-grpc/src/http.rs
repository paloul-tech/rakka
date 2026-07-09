//! HTTP ingress adapter (public ingress only).
//!
//! Thin translation between Axum and the protocol-neutral [`crate::ingress`]
//! core. All cluster routing happens in the core; this module only maps requests
//! and statuses.

use axum::extract::{Path as AxumPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use rakka::http::{HttpError, Router};

use crate::ingress::{self, AppState, IngressError, ViewOutcome};
use crate::model::{SubmitWorkflowRequest, WorkflowRunView};

/// Builds the HTTP ingress router.
pub fn router(state: AppState) -> Router {
    routes().with_state(state)
}

// Kept state-free so tests can construct the route table without booting the
// cluster; axum validates path syntax inside `route()`.
fn routes() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/cluster", get(cluster_view))
        .route("/workflows", post(submit_workflow))
        .route("/workflows/{run_id}", get(get_workflow))
}

async fn health() -> &'static str {
    "ok\n"
}

async fn cluster_view(State(app): State<AppState>) -> Response {
    Json(ingress::cluster(&app)).into_response()
}

async fn submit_workflow(
    State(app): State<AppState>,
    Json(request): Json<SubmitWorkflowRequest>,
) -> Result<Response, HttpError> {
    let view = ingress::submit(&app, request).await.map_err(http_error)?;
    Ok(view_response(view))
}

async fn get_workflow(
    State(app): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
) -> Result<Response, HttpError> {
    let view = ingress::get_run(&app, run_id).await.map_err(http_error)?;
    Ok(view_response(view))
}

fn view_response(view: WorkflowRunView) -> Response {
    match ingress::classify(view) {
        ViewOutcome::Completed(view) => Json(view).into_response(),
        ViewOutcome::NotFound(view) => (StatusCode::NOT_FOUND, Json(view)).into_response(),
        ViewOutcome::Failed(message) => HttpError::service(message).into_response(),
    }
}

fn http_error(error: IngressError) -> HttpError {
    match error {
        IngressError::BadRequest(message) => HttpError::JsonDecode { message },
        IngressError::Unavailable(message) => HttpError::EntityNoRoute { message },
        IngressError::Internal(message) => HttpError::service(message),
    }
}

#[cfg(test)]
mod tests {
    use super::routes;

    /// Route paths must satisfy the axum syntax rules (`{param}` captures);
    /// axum only enforces them at router construction, so build the table in
    /// CI instead of discovering a panic at example startup.
    #[test]
    fn ingress_routes_construct_under_current_axum() {
        let _ = routes();
    }
}
