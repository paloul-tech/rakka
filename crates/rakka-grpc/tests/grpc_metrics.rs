//! gRPC metrics helper tests.

use rakka_core::{InMemoryMetricsRecorder, METRIC_GRPC_REQUEST_LATENCY_MS};
use rakka_grpc::{record_grpc_request_metrics, service_status};

#[tokio::test]
async fn grpc_metrics_record_latency_and_error_labels() {
    let recorder = InMemoryMetricsRecorder::new();

    let result = record_grpc_request_metrics(
        &recorder,
        "rakka.example.CartService",
        "GetCart",
        "unary",
        async { Err::<(), _>(service_status("boom")) },
    )
    .await;

    assert!(result.is_err());
    let metrics = recorder.snapshot();
    let observation = metrics
        .last_observation(
            METRIC_GRPC_REQUEST_LATENCY_MS,
            rakka_core::MetricKind::Histogram,
        )
        .expect("gRPC latency should be recorded");
    assert_eq!(
        observation.attribute("service"),
        Some("rakka.example.CartService")
    );
    assert_eq!(observation.attribute("method"), Some("GetCart"));
    assert_eq!(observation.attribute("rpc_kind"), Some("unary"));
    assert_eq!(observation.attribute("outcome"), Some("error"));
    assert_eq!(observation.attribute("error"), Some("service-error"));
    assert_eq!(observation.attribute("status"), Some("Internal"));
}
