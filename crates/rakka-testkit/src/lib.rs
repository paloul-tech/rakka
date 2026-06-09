#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Testkit utilities for local actor, integration adapter, and operational tests.

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorRef, ActorSystem, Message, RakkaError,
    RakkaResult, Subsystem,
};
use rakka_core::{MetricKind, MetricObservation, MetricsSnapshot};
use rakka_grpc::{GrpcResponseStream, GrpcResult};
use rakka_k8s::{
    KubernetesDrainOutcome, KubernetesDrainReport, KubernetesDrainStepStatus,
    KubernetesProbeSnapshot,
};
use rakka_stream::{StreamLifecycle, StreamSource};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;
use tonic::{Code, Request as GrpcRequest, Response as GrpcResponse, Status};
use tower::ServiceExt;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-testkit";

/// Subsystem associated with testkit helpers.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Testkit
}

/// Runs a future on Tokio for testkit callers.
pub async fn run_async<T>(future: impl std::future::Future<Output = T>) -> T {
    future.await
}

/// Captured HTTP response returned by in-process router helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpTestResponse {
    status: StatusCode,
    body: Bytes,
}

impl HttpTestResponse {
    /// Creates a captured HTTP response.
    #[must_use]
    pub fn new(status: StatusCode, body: Bytes) -> Self {
        Self { status, body }
    }

    /// HTTP status code.
    #[must_use]
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    /// Raw response body bytes.
    #[must_use]
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Decodes the body as JSON.
    pub fn json<T>(&self) -> T
    where
        T: DeserializeOwned,
    {
        serde_json::from_slice(&self.body).expect("HTTP test response body should decode as JSON")
    }
}

/// Sends a JSON POST request to an in-process Axum router.
pub async fn http_post_json<T>(router: axum::Router, path: &str, payload: &T) -> HttpTestResponse
where
    T: Serialize,
{
    let body = serde_json::to_vec(payload).expect("HTTP test request should encode as JSON");
    http_post_bytes(router, path, body, "application/json").await
}

/// Sends a byte POST request to an in-process Axum router.
pub async fn http_post_bytes(
    router: axum::Router,
    path: &str,
    body: impl Into<Bytes>,
    content_type: &'static str,
) -> HttpTestResponse {
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header(CONTENT_TYPE, content_type)
                .body(Body::from(body.into()))
                .expect("HTTP test request should build"),
        )
        .await
        .expect("HTTP router should respond");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("HTTP response body should collect");
    HttpTestResponse::new(status, body)
}

/// Sends a GET request to an in-process Axum router.
pub async fn http_get(router: axum::Router, path: &str) -> HttpTestResponse {
    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .body(Body::empty())
                .expect("HTTP test request should build"),
        )
        .await
        .expect("HTTP router should respond");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("HTTP response body should collect");
    HttpTestResponse::new(status, body)
}

/// Asserts that an HTTP response has the expected status.
pub fn assert_http_status(response: &HttpTestResponse, expected: StatusCode) {
    assert_eq!(
        response.status(),
        expected,
        "unexpected HTTP status with body {:?}",
        String::from_utf8_lossy(response.body())
    );
}

/// Creates a tonic unary request.
#[must_use]
pub fn grpc_request<T>(message: T) -> GrpcRequest<T> {
    GrpcRequest::new(message)
}

/// Awaits a gRPC unary call and returns the decoded response payload.
pub async fn expect_grpc_unary_ok<T>(call: impl Future<Output = GrpcResult<GrpcResponse<T>>>) -> T {
    call.await
        .expect("gRPC unary call should succeed")
        .into_inner()
}

/// Awaits a gRPC unary call and returns the expected status.
pub async fn expect_grpc_unary_status<T>(
    call: impl Future<Output = GrpcResult<GrpcResponse<T>>>,
    expected: Code,
) -> Status {
    match call.await {
        Ok(_response) => panic!("gRPC unary call should fail"),
        Err(status) => {
            assert_eq!(status.code(), expected);
            status
        }
    }
}

/// Collects all successful items from a gRPC response stream.
pub async fn collect_grpc_stream<T>(mut stream: GrpcResponseStream<T>) -> GrpcResult<Vec<T>> {
    let mut values = Vec::new();
    while let Some(next) = stream.next().await {
        values.push(next?);
    }
    Ok(values)
}

/// Collects all items from a gRPC response stream and expects success.
pub async fn expect_grpc_stream_items<T>(stream: GrpcResponseStream<T>) -> Vec<T> {
    collect_grpc_stream(stream)
        .await
        .expect("gRPC stream should complete successfully")
}

/// Collects a Rakka bounded stream source until normal completion.
pub async fn collect_stream_source<T>(source: &StreamSource<T>) -> RakkaResult<Vec<T>> {
    let mut items = Vec::new();
    while let Some(item) = source
        .next()
        .await
        .map_err(|error| error.into_rakka_error())?
    {
        items.push(item);
    }
    Ok(items)
}

/// Collects a bounded stream and asserts its exact items.
pub async fn expect_stream_source_items<T>(source: &StreamSource<T>, expected: Vec<T>)
where
    T: Debug + PartialEq,
{
    let items = collect_stream_source(source)
        .await
        .expect("Rakka stream should complete successfully");
    assert_eq!(items, expected);
}

/// Waits until a bounded stream source reaches the expected buffered depth.
pub async fn wait_for_stream_depth<T>(
    source: &StreamSource<T>,
    expected: usize,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if source.status().depth() == expected {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            assert_eq!(source.status().depth(), expected);
            return;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Asserts that a bounded stream has the expected lifecycle.
pub fn assert_stream_lifecycle<T>(source: &StreamSource<T>, expected: StreamLifecycle) {
    assert_eq!(source.status().lifecycle(), expected);
}

/// Asserts that a Kubernetes probe passed.
pub fn assert_probe_passed(probe: &KubernetesProbeSnapshot) {
    assert!(
        probe.passed(),
        "expected Kubernetes probe to pass, got reasons {:?}",
        probe.reasons()
    );
}

/// Asserts that a Kubernetes probe failed with one stable reason code.
pub fn assert_probe_failed_with_reason(probe: &KubernetesProbeSnapshot, reason: &str) {
    assert!(!probe.passed(), "expected Kubernetes probe to fail");
    assert!(
        probe.reasons().iter().any(|observed| observed == reason),
        "expected Kubernetes probe reason {reason:?}, got {:?}",
        probe.reasons()
    );
}

/// Asserts that a drain report completed all registered steps.
pub fn assert_drain_complete(report: &KubernetesDrainReport) {
    assert_eq!(report.outcome(), KubernetesDrainOutcome::Complete);
    assert!(
        report
            .steps()
            .iter()
            .all(|step| step.status() == KubernetesDrainStepStatus::Completed),
        "expected all drain steps to complete, got {:?}",
        report.steps()
    );
}

/// Asserts that a drain report has the expected outcome.
pub fn assert_drain_outcome(report: &KubernetesDrainReport, expected: KubernetesDrainOutcome) {
    assert_eq!(report.outcome(), expected);
}

/// Returns the last metric observation with the expected name and kind.
#[must_use]
pub fn expect_metric_observation(
    snapshot: &MetricsSnapshot,
    name: &str,
    kind: MetricKind,
) -> MetricObservation {
    snapshot
        .last_observation(name, kind)
        .cloned()
        .unwrap_or_else(|| panic!("expected metric observation {name:?} with kind {kind:?}"))
}

/// Asserts that a metric observation has the expected attribute value.
pub fn assert_metric_attribute(observation: &MetricObservation, key: &str, expected: &str) {
    assert_eq!(observation.attribute(key), Some(expected));
}

/// Asserts the accumulated counter total for a metric name.
pub fn assert_counter_total(snapshot: &MetricsSnapshot, name: &str, expected: f64) {
    assert_eq!(snapshot.counter_total(name), expected);
}

/// Probe actor that records every message it receives.
pub struct TestProbe<M>
where
    M: Message,
{
    actor_ref: ActorRef<M>,
    receiver: mpsc::Receiver<M>,
}

impl<M> TestProbe<M>
where
    M: Message,
{
    /// Spawns a probe actor in the provided system.
    pub fn spawn(system: &ActorSystem, name: impl AsRef<str>) -> RakkaResult<Self> {
        let (sender, receiver) = mpsc::channel(1024);
        let actor_ref = system.spawn_actor(name, ProbeActor { sender })?;
        Ok(Self {
            actor_ref,
            receiver,
        })
    }

    /// Returns the probe actor reference.
    #[must_use]
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    /// Waits for the next probe message.
    pub async fn expect_message(&mut self, timeout: Duration) -> RakkaResult<M> {
        match tokio::time::timeout(timeout, self.receiver.recv()).await {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(RakkaError::new(
                Subsystem::Testkit,
                "probe-closed",
                "test probe channel closed",
            )),
            Err(_elapsed) => Err(RakkaError::new(
                Subsystem::Testkit,
                "probe-timeout",
                "timed out waiting for test probe message",
            )),
        }
    }
}

struct ProbeActor<M>
where
    M: Message,
{
    sender: mpsc::Sender<M>,
}

impl<M> Actor for ProbeActor<M>
where
    M: Message,
{
    type Msg = M;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        let sender = self.sender.clone();
        actor_future(async move {
            sender.send(msg).await.map_err(|_closed| {
                RakkaError::new(
                    Subsystem::Testkit,
                    "probe-receiver-closed",
                    "test probe receiver closed",
                )
            })?;
            Ok(ActorAction::Continue)
        })
    }
}
