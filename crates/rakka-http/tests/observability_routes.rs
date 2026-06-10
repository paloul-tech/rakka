//! HTTP observability route tests.

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use rakka_core::{
    InMemoryMetricsRecorder, MetricAttribute, MetricsRecorder, OpenTelemetryMetricsExport,
    METRIC_HTTP_REQUEST_LATENCY_MS, METRIC_STREAM_PRESSURE,
};
use rakka_http::{
    json_snapshot_route, open_telemetry_metrics_json_route, operational_snapshots_route,
    prometheus_metrics_route, OperationalSnapshotRegistry,
};
use serde::Serialize;
use serde_json::Value;
use tower::ServiceExt;

#[derive(Debug, Clone, Serialize)]
struct DemoSnapshot {
    state: &'static str,
    depth: usize,
}

#[tokio::test]
async fn observability_routes_export_metrics_and_snapshots() {
    let recorder = InMemoryMetricsRecorder::new();
    recorder.record_histogram(
        METRIC_HTTP_REQUEST_LATENCY_MS,
        12.0,
        &[("method", "GET"), ("route", "/ready")],
    );
    recorder.record_gauge(METRIC_STREAM_PRESSURE, 0.25, &[("stream", "ingress")]);

    let registry = OperationalSnapshotRegistry::new();
    registry.register_snapshot("actor_system", || DemoSnapshot {
        state: "running",
        depth: 2,
    });
    registry.register_snapshot("grpc", || serde_json::json!({ "requests": 1 }));

    let router = prometheus_metrics_route("/metrics", {
        let recorder = recorder.clone();
        move || recorder.snapshot()
    })
    .merge(open_telemetry_metrics_json_route(
        "/otel/metrics",
        vec![MetricAttribute::new("service.name", "rakka-demo")],
        {
            let recorder = recorder.clone();
            move || recorder.snapshot()
        },
    ))
    .merge(operational_snapshots_route("/snapshots", registry))
    .merge(json_snapshot_route::<DemoSnapshot, _>(
        "/snapshot/stream",
        || DemoSnapshot {
            state: "open",
            depth: 1,
        },
    ));

    let metrics = get(router.clone(), "/metrics").await;
    assert_eq!(metrics.status, StatusCode::OK);
    assert_eq!(
        metrics.content_type.as_deref(),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let metrics_body = String::from_utf8(metrics.body.to_vec()).expect("metrics should be UTF-8");
    assert!(metrics_body.contains("rakka_http_request_latency_ms_count"));
    assert!(metrics_body.contains("rakka_stream_pressure{stream=\"ingress\"} 0.25"));

    let otel = get(router.clone(), "/otel/metrics").await;
    assert_eq!(otel.status, StatusCode::OK);
    let otel: OpenTelemetryMetricsExport =
        serde_json::from_slice(&otel.body).expect("OpenTelemetry bridge JSON should decode");
    assert_eq!(
        otel.resource_attributes(),
        &[MetricAttribute::new("service.name", "rakka-demo")]
    );
    assert!(otel
        .metrics()
        .iter()
        .any(|metric| metric.name() == METRIC_HTTP_REQUEST_LATENCY_MS));

    let snapshots = get(router.clone(), "/snapshots").await;
    assert_eq!(snapshots.status, StatusCode::OK);
    let snapshots: Value =
        serde_json::from_slice(&snapshots.body).expect("snapshot registry JSON should decode");
    assert_eq!(snapshots["snapshots"]["actor_system"]["state"], "running");
    assert_eq!(snapshots["snapshots"]["grpc"]["requests"], 1);

    let stream_snapshot = get(router, "/snapshot/stream").await;
    assert_eq!(stream_snapshot.status, StatusCode::OK);
    let stream_snapshot: Value =
        serde_json::from_slice(&stream_snapshot.body).expect("snapshot JSON should decode");
    assert_eq!(stream_snapshot["state"], "open");
}

struct CapturedResponse {
    status: StatusCode,
    content_type: Option<String>,
    body: Bytes,
}

async fn get(router: axum::Router, path: &str) -> CapturedResponse {
    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .expect("request should build"),
        )
        .await
        .expect("router should respond");
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should collect");
    CapturedResponse {
        status,
        content_type,
        body,
    }
}
