//! HTTP observability routes for metrics exporters and operational snapshots.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::header::{HeaderValue, CONTENT_TYPE};
use axum::http::{Response, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use rakka_core::{
    export_open_telemetry_metrics, export_prometheus_text, CoordinatedShutdown,
    CoordinatedShutdownSnapshot, MetricAttribute, MetricsSnapshot, OpenTelemetryMetricsExport,
};
use serde::Serialize;
use serde_json::{json, Value};

/// Default operational snapshot name for coordinated shutdown state.
pub const DEFAULT_COORDINATED_SHUTDOWN_SNAPSHOT_NAME: &str = "coordinated_shutdown";

/// Shared snapshot provider stored by the operational snapshot registry.
type SnapshotProvider = Arc<dyn Fn() -> Value + Send + Sync>;

/// Registry of named operational snapshot providers.
#[derive(Clone, Default)]
pub struct OperationalSnapshotRegistry {
    snapshots: Arc<Mutex<BTreeMap<String, SnapshotProvider>>>,
}

impl OperationalSnapshotRegistry {
    /// Creates an empty operational snapshot registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers or replaces a named serializable snapshot provider.
    pub fn register_snapshot<T, F>(&self, name: impl Into<String>, provider: F)
    where
        T: Serialize,
        F: Fn() -> T + Send + Sync + 'static,
    {
        let provider: SnapshotProvider = Arc::new(move || {
            serde_json::to_value(provider()).unwrap_or_else(|error| {
                json!({
                    "error": "snapshot-serialization-failed",
                    "message": error.to_string(),
                })
            })
        });
        self.snapshots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(name.into(), provider);
    }

    /// Returns all registered snapshots by stable name.
    #[must_use]
    pub fn snapshot(&self) -> OperationalSnapshots {
        let snapshots = self
            .snapshots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(name, provider)| (name.clone(), provider()))
            .collect();
        OperationalSnapshots::new(snapshots)
    }
}

/// Registers coordinated shutdown state under the default snapshot name.
pub fn register_coordinated_shutdown_snapshot(
    registry: &OperationalSnapshotRegistry,
    shutdown: CoordinatedShutdown,
) {
    register_named_coordinated_shutdown_snapshot(
        registry,
        DEFAULT_COORDINATED_SHUTDOWN_SNAPSHOT_NAME,
        shutdown,
    );
}

/// Registers coordinated shutdown state under a custom snapshot name.
pub fn register_named_coordinated_shutdown_snapshot(
    registry: &OperationalSnapshotRegistry,
    name: impl Into<String>,
    shutdown: CoordinatedShutdown,
) {
    registry.register_snapshot::<CoordinatedShutdownSnapshot, _>(name, move || shutdown.snapshot());
}

/// Serializable collection of named operational snapshots.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OperationalSnapshots {
    snapshots: BTreeMap<String, Value>,
}

impl OperationalSnapshots {
    /// Creates an operational snapshot collection.
    #[must_use]
    pub const fn new(snapshots: BTreeMap<String, Value>) -> Self {
        Self { snapshots }
    }

    /// Named snapshots in deterministic order.
    #[must_use]
    pub const fn snapshots(&self) -> &BTreeMap<String, Value> {
        &self.snapshots
    }
}

/// Creates a GET route that serves Prometheus text exposition from a metrics snapshot provider.
pub fn prometheus_metrics_route<F>(path: &'static str, snapshot: F) -> Router
where
    F: Fn() -> MetricsSnapshot + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        get(move || {
            let snapshot = snapshot.clone();
            async move {
                let body = export_prometheus_text(&snapshot());
                text_response(
                    body,
                    HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
                )
            }
        }),
    )
}

/// Creates a GET route that serves an OpenTelemetry-oriented JSON metrics bridge.
pub fn open_telemetry_metrics_json_route<F>(
    path: &'static str,
    resource_attributes: Vec<MetricAttribute>,
    snapshot: F,
) -> Router
where
    F: Fn() -> MetricsSnapshot + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        get(move || {
            let snapshot = snapshot.clone();
            let resource_attributes = resource_attributes.clone();
            async move {
                let metrics = export_open_telemetry_metrics(&snapshot(), &[]);
                Json(OpenTelemetryMetricsExport::new(
                    resource_attributes,
                    metrics.metrics().to_vec(),
                ))
            }
        }),
    )
}

/// Creates a GET route that serves one serializable operational snapshot as JSON.
pub fn json_snapshot_route<T, F>(path: &'static str, snapshot: F) -> Router
where
    T: Serialize + Send + 'static,
    F: Fn() -> T + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        get(move || {
            let snapshot = snapshot.clone();
            async move { Json(snapshot()) }
        }),
    )
}

/// Creates a GET route that serves all snapshots from an operational snapshot registry.
pub fn operational_snapshots_route(
    path: &'static str,
    registry: OperationalSnapshotRegistry,
) -> Router {
    Router::new().route(
        path,
        get(move || {
            let registry = registry.clone();
            async move { Json(registry.snapshot()) }
        }),
    )
}

fn text_response(body: String, content_type: HeaderValue) -> Response<Body> {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(CONTENT_TYPE, content_type);
    response
}
