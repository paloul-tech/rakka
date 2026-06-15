//! HTTP route helpers for Kubernetes probes and pre-stop drain.

use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};

use crate::{KubernetesDrainController, KubernetesDrainOutcome};

/// Creates a GET route that runs Kubernetes pre-stop drain.
///
/// The route returns the drain report as JSON. A complete drain maps to `200`,
/// a partial drain maps to `500`, and a timed-out drain maps to `504`.
pub fn kubernetes_drain_route(
    path: &'static str,
    controller: KubernetesDrainController,
    timeout: Duration,
) -> Router {
    Router::new().route(
        path,
        get(move || {
            let controller = controller.clone();
            async move {
                let report = controller.drain(timeout).await;
                (drain_status(report.outcome()), Json(report))
            }
        }),
    )
}

fn drain_status(outcome: KubernetesDrainOutcome) -> StatusCode {
    match outcome {
        KubernetesDrainOutcome::Complete => StatusCode::OK,
        KubernetesDrainOutcome::Partial => StatusCode::INTERNAL_SERVER_ERROR,
        KubernetesDrainOutcome::TimedOut => StatusCode::GATEWAY_TIMEOUT,
    }
}
