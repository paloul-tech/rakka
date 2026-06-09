//! HTTP metrics helper tests.

use rakka_core::{InMemoryMetricsRecorder, METRIC_HTTP_REQUEST_LATENCY_MS};
use rakka_http::{record_http_request_metrics, HttpError};

#[tokio::test]
async fn http_metrics_record_latency_and_error_labels() {
    let recorder = InMemoryMetricsRecorder::new();

    let result = record_http_request_metrics(&recorder, "POST", "/boom", async {
        Err::<(), _>(HttpError::service("boom"))
    })
    .await;

    assert!(matches!(result, Err(HttpError::Service { .. })));
    let metrics = recorder.snapshot();
    let observation = metrics
        .last_observation(
            METRIC_HTTP_REQUEST_LATENCY_MS,
            rakka_core::MetricKind::Histogram,
        )
        .expect("HTTP latency should be recorded");
    assert_eq!(observation.attribute("method"), Some("POST"));
    assert_eq!(observation.attribute("route"), Some("/boom"));
    assert_eq!(observation.attribute("outcome"), Some("error"));
    assert_eq!(observation.attribute("error"), Some("service-error"));
    assert_eq!(observation.attribute("status"), Some("500"));
}
