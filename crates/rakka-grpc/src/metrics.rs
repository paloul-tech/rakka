//! gRPC request metrics and tracing helpers.

use std::future::Future;
use std::time::Instant;

use rakka_core::{MetricsRecorder, METRIC_GRPC_REQUEST_LATENCY_MS};
use tonic::Status;
use tracing::Instrument;

use crate::{GrpcResult, RAKKA_GRPC_ERROR_CODE_METADATA};

/// Records latency and outcome labels for one gRPC adapter request.
pub async fn record_grpc_request_metrics<T>(
    recorder: &dyn MetricsRecorder,
    service: &str,
    method: &str,
    rpc_kind: &str,
    future: impl Future<Output = GrpcResult<T>>,
) -> GrpcResult<T> {
    let span = tracing::info_span!(
        target: "rakka.grpc",
        "grpc.request",
        service = service,
        method = method,
        rpc_kind = rpc_kind
    );
    let start = Instant::now();
    let result = future.instrument(span).await;
    record_result(recorder, service, method, rpc_kind, start, &result);
    result
}

fn record_result<T>(
    recorder: &dyn MetricsRecorder,
    service: &str,
    method: &str,
    rpc_kind: &str,
    start: Instant,
    result: &GrpcResult<T>,
) {
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let status = result
        .as_ref()
        .map(|_value| "ok".to_string())
        .unwrap_or_else(|status| format!("{:?}", status.code()));
    let error = result.as_ref().err().and_then(error_code).unwrap_or("none");
    let outcome = if result.is_ok() { "ok" } else { "error" };

    recorder.record_histogram(
        METRIC_GRPC_REQUEST_LATENCY_MS,
        elapsed_ms,
        &[
            ("service", service),
            ("method", method),
            ("rpc_kind", rpc_kind),
            ("status", status.as_str()),
            ("outcome", outcome),
            ("error", error),
        ],
    );
}

fn error_code(status: &Status) -> Option<&str> {
    status
        .metadata()
        .get(RAKKA_GRPC_ERROR_CODE_METADATA)
        .and_then(|value| value.to_str().ok())
}
