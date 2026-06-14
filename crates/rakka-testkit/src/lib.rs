#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Testkit utilities for local actor, integration adapter, and operational tests.

pub mod compatibility;

use std::fmt::Debug;
use std::future::Future;
use std::time::Duration;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use rakka_cluster::{ClusterEvent, ClusterSubscription, ClusterSubscriptionError, NodeId};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorRef, ActorSystem, ActorTerminated,
    AskError, GroupRouter, GroupRouterSnapshot, Listing, Message, PoolRouter, RakkaError,
    RakkaResult, ReceptionistSubscription, ReplyTo, Subsystem,
};
use rakka_core::{MetricKind, MetricObservation, MetricsSnapshot};
use rakka_grpc::{GrpcResponseStream, GrpcResult};
use rakka_k8s::{
    KubernetesDrainOutcome, KubernetesDrainReport, KubernetesDrainStepStatus,
    KubernetesProbeSnapshot,
};
use rakka_persistence::{
    DurableEffect, DurableState, DurableStateBehavior, DurableStateChange, EventSourcedBehavior,
    InMemoryDurableStateStore, InMemoryEventJournal, InMemorySnapshotStore, PersistenceEvent,
    Revision, SequenceNr, StashDirective, TaggedEvent,
};
use rakka_remote::{RemoteReceptionistListing, RemoteServiceProxyRegistrySnapshot};
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

/// Reusable in-memory persistence stores for event-sourced and durable-state tests.
#[derive(Debug, Clone)]
pub struct PersistenceTestKit<E, S, D>
where
    E: PersistenceEvent,
    S: DurableState,
    D: DurableState,
{
    journal: InMemoryEventJournal<E>,
    snapshots: InMemorySnapshotStore<S>,
    durable_state: InMemoryDurableStateStore<D>,
}

impl<E, S, D> PersistenceTestKit<E, S, D>
where
    E: PersistenceEvent,
    S: DurableState,
    D: DurableState,
{
    /// Creates an empty persistence testkit.
    #[must_use]
    pub fn new() -> Self {
        Self {
            journal: InMemoryEventJournal::new(),
            snapshots: InMemorySnapshotStore::new(),
            durable_state: InMemoryDurableStateStore::new(),
        }
    }

    /// Creates a persistence testkit from explicit stores.
    #[must_use]
    pub fn from_parts(
        journal: InMemoryEventJournal<E>,
        snapshots: InMemorySnapshotStore<S>,
        durable_state: InMemoryDurableStateStore<D>,
    ) -> Self {
        Self {
            journal,
            snapshots,
            durable_state,
        }
    }

    /// Returns the in-memory event journal.
    #[must_use]
    pub fn journal(&self) -> InMemoryEventJournal<E> {
        self.journal.clone()
    }

    /// Returns the in-memory snapshot store.
    #[must_use]
    pub fn snapshots(&self) -> InMemorySnapshotStore<S> {
        self.snapshots.clone()
    }

    /// Returns the in-memory durable state store.
    #[must_use]
    pub fn durable_state(&self) -> InMemoryDurableStateStore<D> {
        self.durable_state.clone()
    }
}

impl<E, S, D> Default for PersistenceTestKit<E, S, D>
where
    E: PersistenceEvent,
    S: DurableState,
    D: DurableState,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Persistence testkit for event-sourced behavior tests without durable state.
pub type EventSourcedPersistenceTestKit<E, S> = PersistenceTestKit<E, S, ()>;

/// Result of running one command through an event-sourced behavior testkit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventSourcedBehaviorTestOutcome<E, S>
where
    E: PersistenceEvent,
    S: DurableState,
{
    /// Events selected by the command.
    pub events: Vec<TaggedEvent<E>>,
    /// State after applying selected events.
    pub state: S,
    /// Highest sequence number after the command.
    pub sequence_nr: SequenceNr,
    /// Whether the command requested a snapshot.
    pub snapshot: bool,
    /// Whether the command requested actor stop.
    pub stop: bool,
    /// Stash directive selected by the command.
    pub stash: StashDirective,
    /// Whether the command was unhandled.
    pub unhandled: bool,
}

/// Reusable testkit for [`EventSourcedBehavior`].
pub struct EventSourcedBehaviorTestKit<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    behavior: EventSourcedBehavior<C, E, S>,
    state: S,
    sequence_nr: SequenceNr,
    events: Vec<TaggedEvent<E>>,
    snapshots: Vec<S>,
}

impl<C, E, S> EventSourcedBehaviorTestKit<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    /// Creates a behavior testkit.
    #[must_use]
    pub fn new(behavior: EventSourcedBehavior<C, E, S>) -> Self {
        let state = behavior.initial_state();
        Self {
            behavior,
            state,
            sequence_nr: SequenceNr::INITIAL,
            events: Vec::new(),
            snapshots: Vec::new(),
        }
    }

    /// Returns the current testkit state.
    #[must_use]
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Returns the highest applied sequence number.
    #[must_use]
    pub const fn sequence_nr(&self) -> SequenceNr {
        self.sequence_nr
    }

    /// Returns all persisted test events.
    #[must_use]
    pub fn events(&self) -> &[TaggedEvent<E>] {
        &self.events
    }

    /// Returns all snapshots captured by explicit snapshot effects.
    #[must_use]
    pub fn snapshots(&self) -> &[S] {
        &self.snapshots
    }

    /// Runs one command through the behavior.
    pub fn run_command(
        &mut self,
        command: C,
    ) -> rakka_persistence::DurableResult<EventSourcedBehaviorTestOutcome<E, S>> {
        let effect = self.behavior.evaluate_command(&self.state, command)?;
        let (events, snapshot, stop, stash, unhandled, side_effects) = effect.into_test_parts();

        for tagged in &events {
            self.sequence_nr = self.sequence_nr.next();
            self.state = self.behavior.evaluate_event(&self.state, &tagged.event);
        }
        if snapshot {
            self.snapshots.push(self.state.clone());
        }
        for side_effect in side_effects {
            side_effect();
        }
        self.events.extend(events.clone());

        Ok(EventSourcedBehaviorTestOutcome {
            events,
            state: self.state.clone(),
            sequence_nr: self.sequence_nr,
            snapshot,
            stop,
            stash,
            unhandled,
        })
    }
}

/// Result of running one command through a durable-state behavior testkit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableStateBehaviorTestOutcome<S>
where
    S: DurableState,
{
    /// State after applying the durable effect.
    pub state: S,
    /// Revision after applying the durable effect.
    pub revision: Revision,
    /// State change selected by the command.
    pub state_change: DurableStateChange<S>,
    /// Whether the command requested actor stop.
    pub stop: bool,
}

/// Reusable testkit for [`DurableStateBehavior`].
pub struct DurableStateBehaviorTestKit<C, S>
where
    C: Message,
    S: DurableState,
{
    behavior: DurableStateBehavior<C, S>,
    state: S,
    revision: Revision,
}

impl<C, S> DurableStateBehaviorTestKit<C, S>
where
    C: Message,
    S: DurableState,
{
    /// Creates a durable-state behavior testkit.
    #[must_use]
    pub fn new(behavior: DurableStateBehavior<C, S>) -> Self {
        let state = behavior.initial_state();
        Self {
            behavior,
            state,
            revision: Revision::INITIAL,
        }
    }

    /// Returns the current testkit state.
    #[must_use]
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Returns the current testkit revision.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Runs one command through the behavior.
    pub fn run_command(
        &mut self,
        command: C,
    ) -> rakka_persistence::DurableResult<DurableStateBehaviorTestOutcome<S>> {
        let effect: DurableEffect<S> = self.behavior.evaluate_command(&self.state, command)?;
        let (state_change, stop, side_effects) = effect.into_test_parts();

        match &state_change {
            DurableStateChange::None | DurableStateChange::Unhandled => {}
            DurableStateChange::Persist(state) => {
                self.state = state.clone();
                self.revision = self.revision.next();
            }
            DurableStateChange::Delete => {
                self.state = self.behavior.initial_state();
                self.revision = Revision::INITIAL;
            }
        }
        for side_effect in side_effects {
            side_effect();
        }

        Ok(DurableStateBehaviorTestOutcome {
            state: self.state.clone(),
            revision: self.revision,
            state_change,
            stop,
        })
    }
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

/// Asserts that a receptionist listing has the expected routee count.
pub fn assert_receptionist_listing_count<M>(listing: &Listing<M>, expected: usize)
where
    M: Message,
{
    assert_eq!(
        listing.len(),
        expected,
        "unexpected receptionist listing size for service {:?}",
        listing.key()
    );
}

/// Asserts that a receptionist listing contains a specific actor incarnation.
pub fn assert_receptionist_listing_contains<M>(listing: &Listing<M>, actor_ref: &ActorRef<M>)
where
    M: Message,
{
    assert!(
        listing.contains(actor_ref),
        "expected receptionist listing for service {:?} to contain {}#{}",
        listing.key(),
        actor_ref.path(),
        actor_ref.uid()
    );
}

/// Waits for a receptionist subscription update with the expected routee count.
pub async fn expect_receptionist_listing_count<M>(
    subscription: &mut ReceptionistSubscription<M>,
    expected: usize,
    timeout: Duration,
) -> RakkaResult<Listing<M>>
where
    M: Message,
{
    let listing = tokio::time::timeout(timeout, subscription.recv())
        .await
        .map_err(|_elapsed| {
            RakkaError::new(
                Subsystem::Testkit,
                "receptionist-listing-timeout",
                format!("timed out waiting for receptionist listing with {expected} routees"),
            )
        })?
        .map_err(RakkaError::from)?;
    assert_receptionist_listing_count(&listing, expected);
    Ok(listing)
}

/// Asserts that a remote receptionist wire listing has the expected routee count.
pub fn assert_remote_receptionist_listing_count(
    listing: &RemoteReceptionistListing,
    expected: usize,
) {
    assert_eq!(
        listing.len(),
        expected,
        "unexpected remote receptionist listing size for service {:?}",
        listing.service_id()
    );
}

/// Asserts that a remote receptionist wire listing targets the expected service.
pub fn assert_remote_receptionist_listing_service(
    listing: &RemoteReceptionistListing,
    service_id: &str,
    service_message_type: &str,
) {
    assert_eq!(
        listing.service_id(),
        service_id,
        "unexpected remote receptionist service id"
    );
    assert_eq!(
        listing.service_message_type(),
        service_message_type,
        "unexpected remote receptionist service message type"
    );
}

/// Asserts the number of materialized remote service proxy routees.
pub fn assert_remote_service_proxy_count(
    snapshot: &RemoteServiceProxyRegistrySnapshot,
    expected: usize,
) {
    assert_eq!(
        snapshot.proxy_count(),
        expected,
        "unexpected remote service proxy count for {:?}",
        snapshot
    );
}

/// Asserts the number of tracked remote service listings.
pub fn assert_remote_service_listing_count(
    snapshot: &RemoteServiceProxyRegistrySnapshot,
    expected: usize,
) {
    assert_eq!(
        snapshot.listing_count(),
        expected,
        "unexpected remote service listing count for {:?}",
        snapshot
    );
}

/// Waits for a remote proxy-registry snapshot with the expected counts.
pub async fn expect_remote_proxy_registry_snapshot(
    mut snapshot: impl FnMut() -> RemoteServiceProxyRegistrySnapshot,
    expected_proxies: usize,
    expected_listings: usize,
    timeout: Duration,
) -> RakkaResult<RemoteServiceProxyRegistrySnapshot> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = snapshot();
        if current.proxy_count() == expected_proxies && current.listing_count() == expected_listings
        {
            return Ok(current);
        }

        if tokio::time::Instant::now() >= deadline {
            assert_remote_service_proxy_count(&current, expected_proxies);
            assert_remote_service_listing_count(&current, expected_listings);
            return Ok(current);
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Asserts that a local pool router has the expected live routee count.
pub fn assert_pool_routee_count<M>(router: &PoolRouter<M>, expected: usize)
where
    M: Message,
{
    assert_eq!(
        router.routee_count(),
        expected,
        "unexpected pool router routee count for {:?}",
        router
    );
}

/// Asserts that a receptionist-backed group router has the expected live routee count.
pub fn assert_group_routee_count<M>(router: &GroupRouter<M>, expected: usize)
where
    M: Message,
{
    assert_eq!(
        router.routee_count(),
        expected,
        "unexpected group router routee count for {:?}",
        router
    );
}

/// Asserts that an observable group-router snapshot has the expected routee count.
pub fn assert_group_router_snapshot_routee_count(snapshot: &GroupRouterSnapshot, expected: usize) {
    assert_eq!(
        snapshot.routee_count(),
        expected,
        "unexpected group router snapshot routee count for {:?}",
        snapshot
    );
}

/// Waits for the next cluster subscription event.
pub async fn expect_cluster_event(
    subscription: &mut ClusterSubscription,
    timeout: Duration,
) -> RakkaResult<ClusterEvent> {
    tokio::time::timeout(timeout, subscription.recv())
        .await
        .map_err(|_elapsed| {
            RakkaError::new(
                Subsystem::Testkit,
                "cluster-event-timeout",
                "timed out waiting for cluster event",
            )
        })?
        .map_err(cluster_subscription_error)
}

/// Waits until a cluster subscription emits an event matching `predicate`.
pub async fn expect_cluster_event_matching(
    subscription: &mut ClusterSubscription,
    timeout: Duration,
    description: impl Into<String>,
    predicate: impl Fn(&ClusterEvent) -> bool,
) -> RakkaResult<ClusterEvent> {
    let description = description.into();
    tokio::time::timeout(timeout, async {
        loop {
            let event = subscription
                .recv()
                .await
                .map_err(cluster_subscription_error)?;
            if predicate(&event) {
                return Ok(event);
            }
        }
    })
    .await
    .map_err(|_elapsed| {
        RakkaError::new(
            Subsystem::Testkit,
            "cluster-event-timeout",
            format!("timed out waiting for cluster event: {description}"),
        )
    })?
}

/// Waits for a `MemberUp` event for the expected node id.
pub async fn expect_cluster_member_up(
    subscription: &mut ClusterSubscription,
    expected_node: &NodeId,
    timeout: Duration,
) -> RakkaResult<ClusterEvent> {
    expect_cluster_event_matching(
        subscription,
        timeout,
        format!("member up for {expected_node}"),
        |event| {
            matches!(
                event,
                ClusterEvent::MemberUp { member } if member.node().id() == expected_node
            )
        },
    )
    .await
}

/// Asserts that a cluster event belongs to the expected member node.
pub fn assert_cluster_event_node(event: &ClusterEvent, expected_node: &NodeId) {
    assert_eq!(
        event.node_id(),
        Some(expected_node),
        "unexpected cluster event node for {:?}",
        event
    );
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

fn cluster_subscription_error(error: ClusterSubscriptionError) -> RakkaError {
    RakkaError::new(
        Subsystem::Testkit,
        "cluster-subscription-error",
        error.to_string(),
    )
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

    /// Waits for the next probe message and asserts exact equality.
    pub async fn expect_message_eq(&mut self, expected: M, timeout: Duration) -> RakkaResult<()>
    where
        M: Debug + PartialEq,
    {
        let actual = self.expect_message(timeout).await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    /// Asserts that no probe message arrives before the timeout elapses.
    pub async fn expect_no_message(&mut self, timeout: Duration) -> RakkaResult<()> {
        match tokio::time::timeout(timeout, self.receiver.recv()).await {
            Ok(Some(_message)) => Err(RakkaError::new(
                Subsystem::Testkit,
                "unexpected-probe-message",
                "test probe received an unexpected message",
            )),
            Ok(None) => Err(RakkaError::new(
                Subsystem::Testkit,
                "probe-closed",
                "test probe channel closed",
            )),
            Err(_elapsed) => Ok(()),
        }
    }
}

/// Waits for an actor to terminate.
pub async fn expect_terminated<M>(
    actor: &ActorRef<M>,
    timeout: Duration,
) -> RakkaResult<ActorTerminated>
where
    M: Message,
{
    tokio::time::timeout(timeout, actor.when_terminated())
        .await
        .map_err(|_elapsed| {
            RakkaError::new(
                Subsystem::Testkit,
                "expect-terminated-timeout",
                "timed out waiting for actor termination",
            )
        })
}

/// Event emitted by [`ActorContextProbe`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorContextProbeEvent {
    /// A keyed timer fired.
    TimerFired(String),
    /// A configured receive timeout fired.
    ReceiveTimeout,
    /// A watch was registered.
    WatchRegistered,
    /// A watch was cancelled.
    WatchCancelled,
    /// A watched actor terminated.
    WatchObserved(ActorTerminated),
    /// A context ask completed.
    AskCompleted(Result<String, AskError>),
    /// A pipe-to-self future completed.
    PipeCompleted(String),
}

/// Command protocol accepted by [`ActorContextProbe`].
#[derive(Debug, Clone)]
pub enum ActorContextProbeCommand {
    /// Starts a keyed timer.
    StartTimer {
        /// Timer key.
        key: String,
        /// Timer delay.
        delay: Duration,
    },
    /// Internal timer-fired message.
    TimerElapsed {
        /// Timer key.
        key: String,
    },
    /// Enables one receive timeout.
    EnableReceiveTimeout {
        /// Timeout delay.
        delay: Duration,
    },
    /// Internal receive-timeout message.
    ReceiveTimeout,
    /// Watches a stopper actor.
    WatchStopper {
        /// Actor to watch.
        target: ActorRef<StopProbeCommand>,
    },
    /// Watches and immediately unwatches a stopper actor.
    WatchAndUnwatchStopper {
        /// Actor to watch and unwatch.
        target: ActorRef<StopProbeCommand>,
    },
    /// Internal watch notification.
    WatchObserved(ActorTerminated),
    /// Asks an echo probe.
    AskEcho {
        /// Actor to ask.
        target: ActorRef<EchoProbeCommand>,
        /// Request value.
        value: String,
        /// Ask timeout.
        timeout: Duration,
    },
    /// Internal ask completion message.
    AskCompleted(Result<String, AskError>),
    /// Starts a pipe-to-self operation.
    PipeValue {
        /// Value to pipe.
        value: String,
    },
    /// Internal pipe completion message.
    PipeCompleted(String),
}

impl From<ActorTerminated> for ActorContextProbeCommand {
    fn from(terminated: ActorTerminated) -> Self {
        Self::WatchObserved(terminated)
    }
}

/// Probe actor for Phase 2 actor-context APIs.
pub struct ActorContextProbe {
    events: ActorRef<ActorContextProbeEvent>,
}

impl ActorContextProbe {
    /// Creates an actor-context probe.
    #[must_use]
    pub fn new(events: ActorRef<ActorContextProbeEvent>) -> Self {
        Self { events }
    }
}

impl Actor for ActorContextProbe {
    type Msg = ActorContextProbeCommand;

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        let events = self.events.clone();

        match msg {
            ActorContextProbeCommand::StartTimer { key, delay } => {
                ctx.start_timer_once(
                    key.clone(),
                    delay,
                    ActorContextProbeCommand::TimerElapsed { key },
                );
                actor_future(async { Ok(ActorAction::Continue) })
            }
            ActorContextProbeCommand::TimerElapsed { key } => actor_future(async move {
                let _ = events.tell(ActorContextProbeEvent::TimerFired(key));
                Ok(ActorAction::Continue)
            }),
            ActorContextProbeCommand::EnableReceiveTimeout { delay } => {
                ctx.set_receive_timeout(delay, ActorContextProbeCommand::ReceiveTimeout);
                actor_future(async { Ok(ActorAction::Continue) })
            }
            ActorContextProbeCommand::ReceiveTimeout => {
                ctx.cancel_receive_timeout();
                actor_future(async move {
                    let _ = events.tell(ActorContextProbeEvent::ReceiveTimeout);
                    Ok(ActorAction::Continue)
                })
            }
            ActorContextProbeCommand::WatchStopper { target } => {
                ctx.watch(&target);
                actor_future(async move {
                    let _ = events.tell(ActorContextProbeEvent::WatchRegistered);
                    Ok(ActorAction::Continue)
                })
            }
            ActorContextProbeCommand::WatchAndUnwatchStopper { target } => {
                let handle = ctx.watch(&target);
                ctx.unwatch(&handle);
                actor_future(async move {
                    let _ = events.tell(ActorContextProbeEvent::WatchCancelled);
                    Ok(ActorAction::Continue)
                })
            }
            ActorContextProbeCommand::WatchObserved(terminated) => actor_future(async move {
                let _ = events.tell(ActorContextProbeEvent::WatchObserved(terminated));
                Ok(ActorAction::Continue)
            }),
            ActorContextProbeCommand::AskEcho {
                target,
                value,
                timeout,
            } => {
                ctx.ask(
                    &target,
                    |reply_to| EchoProbeCommand::Ask { value, reply_to },
                    timeout,
                    ActorContextProbeCommand::AskCompleted,
                );
                actor_future(async { Ok(ActorAction::Continue) })
            }
            ActorContextProbeCommand::AskCompleted(result) => actor_future(async move {
                let _ = events.tell(ActorContextProbeEvent::AskCompleted(result));
                Ok(ActorAction::Continue)
            }),
            ActorContextProbeCommand::PipeValue { value } => {
                ctx.pipe_to_self(async move { Ok::<String, ()>(value) }, |result| {
                    ActorContextProbeCommand::PipeCompleted(
                        result.unwrap_or_else(|()| "pipe-error".to_owned()),
                    )
                });
                actor_future(async { Ok(ActorAction::Continue) })
            }
            ActorContextProbeCommand::PipeCompleted(value) => actor_future(async move {
                let _ = events.tell(ActorContextProbeEvent::PipeCompleted(value));
                Ok(ActorAction::Continue)
            }),
        }
    }
}

/// Spawns an actor-context probe.
pub fn spawn_actor_context_probe(
    system: &ActorSystem,
    name: impl AsRef<str>,
    events: ActorRef<ActorContextProbeEvent>,
) -> RakkaResult<ActorRef<ActorContextProbeCommand>> {
    system.spawn_actor(name, ActorContextProbe::new(events))
}

/// Command protocol for a stopper probe actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopProbeCommand {
    /// Stop the actor.
    Stop,
}

struct StopProbeActor;

impl Actor for StopProbeActor {
    type Msg = StopProbeCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Stop) })
    }
}

/// Spawns a stopper probe actor.
pub fn spawn_stop_probe(
    system: &ActorSystem,
    name: impl AsRef<str>,
) -> RakkaResult<ActorRef<StopProbeCommand>> {
    system.spawn_actor(name, StopProbeActor)
}

/// Command protocol for an echo probe actor.
pub enum EchoProbeCommand {
    /// Ask for a string echo.
    Ask {
        /// Echo value.
        value: String,
        /// Reply channel.
        reply_to: ReplyTo<String>,
    },
}

struct EchoProbeActor;

impl Actor for EchoProbeActor {
    type Msg = EchoProbeCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async move {
            match msg {
                EchoProbeCommand::Ask { value, reply_to } => {
                    let _ = reply_to.reply(value);
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

/// Spawns an echo probe actor.
pub fn spawn_echo_probe(
    system: &ActorSystem,
    name: impl AsRef<str>,
) -> RakkaResult<ActorRef<EchoProbeCommand>> {
    system.spawn_actor(name, EchoProbeActor)
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
