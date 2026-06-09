//! HTTP request metrics and tracing helpers.

use std::future::Future;
use std::time::Instant;

use rakka_core::{MetricsRecorder, METRIC_HTTP_REQUEST_LATENCY_MS};
use tracing::Instrument;

use crate::{HttpError, HttpResult};

/// Records latency and outcome labels for one HTTP adapter request.
pub async fn record_http_request_metrics<T>(
    recorder: &dyn MetricsRecorder,
    method: &str,
    route: &str,
    future: impl Future<Output = HttpResult<T>>,
) -> HttpResult<T> {
    let span = tracing::info_span!(
        target: "rakka.http",
        "http.request",
        method = method,
        route = route
    );
    let start = Instant::now();
    let result = future.instrument(span).await;
    record_result(recorder, method, route, start, &result);
    result
}

fn record_result<T>(
    recorder: &dyn MetricsRecorder,
    method: &str,
    route: &str,
    start: Instant,
    result: &HttpResult<T>,
) {
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = result
        .as_ref()
        .map(|_value| "200".to_string())
        .unwrap_or_else(|error| error.status_code().as_u16().to_string());
    let error = result.as_ref().err().map(HttpError::code).unwrap_or("none");
    let outcome = if result.is_ok() { "ok" } else { "error" };

    recorder.record_histogram(
        METRIC_HTTP_REQUEST_LATENCY_MS,
        elapsed_ms,
        &[
            ("method", method),
            ("route", route),
            ("status", status.as_str()),
            ("outcome", outcome),
            ("error", error),
        ],
    );
}
