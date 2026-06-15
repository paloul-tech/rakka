#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Testkit utilities for local actor, integration adapter, and operational tests.

pub mod compatibility;

use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use rakka_cluster::{ClusterEvent, ClusterSubscription, ClusterSubscriptionError, NodeId};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorRef, ActorSystem, ActorTerminated,
    AskError, CoordinatedShutdown, CoordinatedShutdownReason, CoordinatedShutdownReport,
    CoordinatedShutdownResult, CoordinatedShutdownSettings, GroupRouter, GroupRouterSnapshot,
    Listing, Message, PoolRouter, RakkaError, RakkaResult, ReceptionistSubscription, ReplyTo,
    ShutdownOutcome, ShutdownPhase, ShutdownTask, ShutdownTaskOptions, ShutdownTaskReport,
    ShutdownTaskStatus, Subsystem, METRIC_SHUTDOWN_TIMEOUTS,
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
use rakka_stream::{
    bounded_channel, ActorSinkMessage, Sink, Source, StreamError, StreamLifecycle, StreamSink,
    StreamSource, StreamStatus,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{mpsc, Semaphore};
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

/// Default timeout used by coordinated shutdown testkit probes.
pub const DEFAULT_SHUTDOWN_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Event recorded by [`CoordinatedShutdownTestKit`] task probes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownTaskProbeEvent {
    /// A registered task started.
    Started {
        /// Phase that owns the task.
        phase: ShutdownPhase,
        /// Stable task name.
        task_name: String,
    },
    /// A registered task finished before the runner moved on.
    Finished {
        /// Phase that owns the task.
        phase: ShutdownPhase,
        /// Stable task name.
        task_name: String,
        /// Status recorded by the probe task.
        status: ShutdownTaskStatus,
    },
}

impl ShutdownTaskProbeEvent {
    /// Phase associated with the event.
    #[must_use]
    pub const fn phase(&self) -> &ShutdownPhase {
        match self {
            Self::Started { phase, .. } | Self::Finished { phase, .. } => phase,
        }
    }

    /// Task name associated with the event.
    #[must_use]
    pub fn task_name(&self) -> &str {
        match self {
            Self::Started { task_name, .. } | Self::Finished { task_name, .. } => task_name,
        }
    }

    /// Status associated with a finished event.
    #[must_use]
    pub const fn status(&self) -> Option<ShutdownTaskStatus> {
        match self {
            Self::Started { .. } => None,
            Self::Finished { status, .. } => Some(*status),
        }
    }
}

/// Descriptor returned for a registered shutdown probe task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownTaskProbe {
    descriptor: ShutdownTask,
}

impl ShutdownTaskProbe {
    /// Creates a task probe descriptor.
    #[must_use]
    pub fn new(descriptor: ShutdownTask) -> Self {
        Self { descriptor }
    }

    /// Registered task descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &ShutdownTask {
        &self.descriptor
    }

    /// Phase this probe task belongs to.
    #[must_use]
    pub const fn phase(&self) -> &ShutdownPhase {
        self.descriptor.phase()
    }

    /// Stable task name.
    #[must_use]
    pub fn task_name(&self) -> &str {
        self.descriptor.name()
    }
}

/// Handle for a controlled shutdown task that waits until released.
#[derive(Debug, Clone)]
pub struct ControlledShutdownTask {
    descriptor: ShutdownTask,
    release: Arc<Semaphore>,
    started: Arc<Semaphore>,
    finished: Arc<Semaphore>,
    timeout: Duration,
}

impl ControlledShutdownTask {
    /// Creates a controlled task handle.
    #[must_use]
    pub fn new(
        descriptor: ShutdownTask,
        release: Arc<Semaphore>,
        started: Arc<Semaphore>,
        finished: Arc<Semaphore>,
        timeout: Duration,
    ) -> Self {
        Self {
            descriptor,
            release,
            started,
            finished,
            timeout,
        }
    }

    /// Registered task descriptor.
    #[must_use]
    pub const fn descriptor(&self) -> &ShutdownTask {
        &self.descriptor
    }

    /// Phase this controlled task belongs to.
    #[must_use]
    pub const fn phase(&self) -> &ShutdownPhase {
        self.descriptor.phase()
    }

    /// Stable task name.
    #[must_use]
    pub fn task_name(&self) -> &str {
        self.descriptor.name()
    }

    /// Releases the controlled task so shutdown can continue.
    pub fn release(&self) {
        self.release.add_permits(1);
    }

    /// Waits until the controlled task has started.
    pub async fn wait_started(&self) -> RakkaResult<()> {
        self.wait_started_within(self.timeout).await
    }

    /// Waits until the controlled task has started within a custom timeout.
    pub async fn wait_started_within(&self, timeout: Duration) -> RakkaResult<()> {
        acquire_probe_permit(
            self.started.clone(),
            timeout,
            "shutdown-probe-start-timeout",
            "timed out waiting for controlled shutdown task to start",
        )
        .await
    }

    /// Waits until the controlled task has completed after being released.
    pub async fn wait_finished(&self) -> RakkaResult<()> {
        self.wait_finished_within(self.timeout).await
    }

    /// Waits until the controlled task has completed within a custom timeout.
    pub async fn wait_finished_within(&self, timeout: Duration) -> RakkaResult<()> {
        acquire_probe_permit(
            self.finished.clone(),
            timeout,
            "shutdown-probe-finish-timeout",
            "timed out waiting for controlled shutdown task to finish",
        )
        .await
    }
}

/// Reusable testkit for real coordinated shutdown registries.
///
/// ```no_run
/// use rakka_core::{
///     CoordinatedShutdownReason, ShutdownOutcome, ShutdownPhase, ShutdownTaskStatus,
/// };
/// use rakka_testkit::{
///     assert_shutdown_outcome, assert_shutdown_task_status, CoordinatedShutdownTestKit,
/// };
///
/// # async fn example() -> rakka_core::RakkaResult<()> {
/// let kit = CoordinatedShutdownTestKit::new();
/// let phase = ShutdownPhase::stop_ingress();
/// let controlled = kit.register_controlled_task(phase.clone(), "drain-ingress")?;
///
/// let shutdown = kit.shutdown();
/// let run = tokio::spawn(async move {
///     shutdown
///         .run(CoordinatedShutdownReason::user_request())
///         .await
/// });
///
/// controlled.wait_started().await?;
/// controlled.release();
/// let report = run.await.expect("shutdown task should join").unwrap();
///
/// assert_shutdown_outcome(&report, ShutdownOutcome::Complete);
/// assert_shutdown_task_status(
///     &report,
///     &phase,
///     "drain-ingress",
///     ShutdownTaskStatus::Completed,
/// );
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct CoordinatedShutdownTestKit {
    shutdown: CoordinatedShutdown,
    events: Arc<Mutex<Vec<ShutdownTaskProbeEvent>>>,
    timeout: Duration,
}

impl CoordinatedShutdownTestKit {
    /// Creates a testkit around a fresh core-only coordinated shutdown registry.
    #[must_use]
    pub fn new() -> Self {
        Self::from_shutdown(CoordinatedShutdown::new())
    }

    /// Creates a testkit around a fresh core-only registry with custom settings.
    #[must_use]
    pub fn with_settings(settings: CoordinatedShutdownSettings) -> Self {
        Self::from_shutdown(CoordinatedShutdown::with_settings(settings))
    }

    /// Creates a testkit around an existing coordinated shutdown registry.
    #[must_use]
    pub fn from_shutdown(shutdown: CoordinatedShutdown) -> Self {
        Self {
            shutdown,
            events: Arc::new(Mutex::new(Vec::new())),
            timeout: DEFAULT_SHUTDOWN_PROBE_TIMEOUT,
        }
    }

    /// Creates a testkit around an actor system's owned coordinated shutdown registry.
    #[must_use]
    pub fn for_system(system: &ActorSystem) -> Self {
        Self::from_shutdown(system.coordinated_shutdown())
    }

    /// Returns this testkit with a different default probe timeout.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Returns the wrapped coordinated shutdown registry.
    #[must_use]
    pub fn shutdown(&self) -> CoordinatedShutdown {
        self.shutdown.clone()
    }

    /// Returns recorded task probe events in insertion order.
    #[must_use]
    pub fn events(&self) -> Vec<ShutdownTaskProbeEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Clears recorded task probe events.
    pub fn clear_events(&self) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    /// Registers an immediately completing task that records start and finish events.
    pub fn register_task(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
    ) -> RakkaResult<ShutdownTaskProbe> {
        self.register_task_with_options(phase, name, ShutdownTaskOptions::default())
    }

    /// Registers an immediately completing task with explicit options.
    pub fn register_task_with_options(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
        options: ShutdownTaskOptions,
    ) -> RakkaResult<ShutdownTaskProbe> {
        let events = self.events.clone();
        let descriptor =
            self.shutdown
                .add_task_with_options(phase, name, options, move |context| {
                    let events = events.clone();
                    async move {
                        record_shutdown_probe_event(
                            &events,
                            ShutdownTaskProbeEvent::Started {
                                phase: context.phase().clone(),
                                task_name: context.task_name().to_owned(),
                            },
                        );
                        record_shutdown_probe_event(
                            &events,
                            ShutdownTaskProbeEvent::Finished {
                                phase: context.phase().clone(),
                                task_name: context.task_name().to_owned(),
                                status: ShutdownTaskStatus::Completed,
                            },
                        );
                        Ok(())
                    }
                })?;
        Ok(ShutdownTaskProbe::new(descriptor))
    }

    /// Registers a task that records start, waits for manual release, then completes.
    pub fn register_controlled_task(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
    ) -> RakkaResult<ControlledShutdownTask> {
        self.register_controlled_task_with_options(phase, name, ShutdownTaskOptions::default())
    }

    /// Registers a controlled task with explicit options.
    pub fn register_controlled_task_with_options(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
        options: ShutdownTaskOptions,
    ) -> RakkaResult<ControlledShutdownTask> {
        let events = self.events.clone();
        let release = Arc::new(Semaphore::new(0));
        let started = Arc::new(Semaphore::new(0));
        let finished = Arc::new(Semaphore::new(0));
        let release_task = release.clone();
        let started_task = started.clone();
        let finished_task = finished.clone();
        let descriptor =
            self.shutdown
                .add_task_with_options(phase, name, options, move |context| {
                    let events = events.clone();
                    let release = release_task.clone();
                    let started = started_task.clone();
                    let finished = finished_task.clone();
                    async move {
                        record_shutdown_probe_event(
                            &events,
                            ShutdownTaskProbeEvent::Started {
                                phase: context.phase().clone(),
                                task_name: context.task_name().to_owned(),
                            },
                        );
                        started.add_permits(1);
                        let _permit = release.acquire().await.map_err(|_closed| {
                            shutdown_testkit_error(
                                "shutdown-probe-release-closed",
                                "controlled shutdown task release semaphore closed",
                            )
                        })?;
                        record_shutdown_probe_event(
                            &events,
                            ShutdownTaskProbeEvent::Finished {
                                phase: context.phase().clone(),
                                task_name: context.task_name().to_owned(),
                                status: ShutdownTaskStatus::Completed,
                            },
                        );
                        finished.add_permits(1);
                        Ok(())
                    }
                })?;
        Ok(ControlledShutdownTask::new(
            descriptor,
            release,
            started,
            finished,
            self.timeout,
        ))
    }

    /// Registers a task that records start and then fails with the provided error code.
    pub fn register_failing_task(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> RakkaResult<ShutdownTaskProbe> {
        self.register_failing_task_with_options(
            phase,
            name,
            ShutdownTaskOptions::default(),
            code,
            message,
        )
    }

    /// Registers a failing task with explicit options.
    pub fn register_failing_task_with_options(
        &self,
        phase: ShutdownPhase,
        name: impl Into<String>,
        options: ShutdownTaskOptions,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> RakkaResult<ShutdownTaskProbe> {
        let events = self.events.clone();
        let code = code.into();
        let message = message.into();
        let descriptor =
            self.shutdown
                .add_task_with_options(phase, name, options, move |context| {
                    let events = events.clone();
                    let code = code.clone();
                    let message = message.clone();
                    async move {
                        record_shutdown_probe_event(
                            &events,
                            ShutdownTaskProbeEvent::Started {
                                phase: context.phase().clone(),
                                task_name: context.task_name().to_owned(),
                            },
                        );
                        record_shutdown_probe_event(
                            &events,
                            ShutdownTaskProbeEvent::Finished {
                                phase: context.phase().clone(),
                                task_name: context.task_name().to_owned(),
                                status: ShutdownTaskStatus::Failed,
                            },
                        );
                        Err(shutdown_testkit_error(code, message))
                    }
                })?;
        Ok(ShutdownTaskProbe::new(descriptor))
    }

    /// Runs the wrapped coordinated shutdown registry.
    pub async fn run(
        &self,
        reason: CoordinatedShutdownReason,
    ) -> CoordinatedShutdownResult<CoordinatedShutdownReport> {
        self.shutdown.run(reason).await
    }

    /// Runs shutdown twice and asserts both calls return the same outcome and report.
    pub async fn assert_idempotent(
        &self,
        reason: CoordinatedShutdownReason,
    ) -> CoordinatedShutdownResult<CoordinatedShutdownReport> {
        let first = self.shutdown.run(reason.clone()).await;
        let second = self.shutdown.run(reason).await;
        assert_eq!(first, second, "coordinated shutdown should be idempotent");
        first
    }
}

impl Default for CoordinatedShutdownTestKit {
    fn default() -> Self {
        Self::new()
    }
}

/// Asserts a coordinated shutdown report outcome.
pub fn assert_shutdown_outcome(report: &CoordinatedShutdownReport, expected: ShutdownOutcome) {
    assert_eq!(report.outcome(), expected);
}

/// Asserts that expected phases appear in the report in the provided order.
pub fn assert_shutdown_phase_order(report: &CoordinatedShutdownReport, expected: &[ShutdownPhase]) {
    let actual = report
        .phases()
        .iter()
        .map(|phase| phase.phase().name())
        .collect::<Vec<_>>();
    let mut search_from = 0;
    for expected_phase in expected {
        let Some(relative_index) = actual[search_from..]
            .iter()
            .position(|phase| *phase == expected_phase.name())
        else {
            panic!(
                "expected shutdown phase '{}' after index {search_from}; actual phases: {actual:?}",
                expected_phase.name()
            );
        };
        search_from += relative_index + 1;
    }
}

/// Asserts the exact start-event order for shutdown probe tasks.
pub fn assert_shutdown_task_start_order(
    events: &[ShutdownTaskProbeEvent],
    expected: &[(&ShutdownPhase, &str)],
) {
    let actual = events
        .iter()
        .filter_map(|event| match event {
            ShutdownTaskProbeEvent::Started { phase, task_name } => {
                Some((phase.name().to_owned(), task_name.clone()))
            }
            ShutdownTaskProbeEvent::Finished { .. } => None,
        })
        .collect::<Vec<_>>();
    let expected = expected
        .iter()
        .map(|(phase, task_name)| (phase.name().to_owned(), (*task_name).to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(actual, expected, "unexpected shutdown task start order");
}

/// Returns a task report from a coordinated shutdown report.
#[must_use]
pub fn expect_shutdown_task_report<'a>(
    report: &'a CoordinatedShutdownReport,
    phase: &ShutdownPhase,
    task_name: &str,
) -> &'a ShutdownTaskReport {
    report
        .phases()
        .iter()
        .find(|phase_report| phase_report.phase() == phase)
        .and_then(|phase_report| {
            phase_report
                .tasks()
                .iter()
                .find(|task| task.task_name() == task_name)
        })
        .unwrap_or_else(|| {
            panic!(
                "expected shutdown task '{}' in phase '{}'",
                task_name,
                phase.name()
            )
        })
}

/// Asserts a task status in a coordinated shutdown report.
pub fn assert_shutdown_task_status(
    report: &CoordinatedShutdownReport,
    phase: &ShutdownPhase,
    task_name: &str,
    expected: ShutdownTaskStatus,
) {
    let task = expect_shutdown_task_report(report, phase, task_name);
    assert_eq!(task.status(), expected);
}

/// Asserts a shutdown timeout counter with the expected bounded labels exists.
pub fn assert_shutdown_timeout_metric(
    snapshot: &MetricsSnapshot,
    phase: &ShutdownPhase,
    task_name: &str,
    scope: &str,
) {
    let matched = snapshot
        .observations_named(METRIC_SHUTDOWN_TIMEOUTS)
        .into_iter()
        .any(|observation| {
            observation.kind() == MetricKind::Counter
                && observation.attribute("phase") == Some(phase.name())
                && observation.attribute("task") == Some(task_name)
                && observation.attribute("scope") == Some(scope)
                && observation.attribute("status") == Some(ShutdownOutcome::TimedOut.as_str())
        });
    assert!(
        matched,
        "expected shutdown timeout metric for phase '{}', task '{task_name}', scope '{scope}'",
        phase.name()
    );
}

fn record_shutdown_probe_event(
    events: &Arc<Mutex<Vec<ShutdownTaskProbeEvent>>>,
    event: ShutdownTaskProbeEvent,
) {
    events
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(event);
}

async fn acquire_probe_permit(
    semaphore: Arc<Semaphore>,
    timeout: Duration,
    timeout_code: &'static str,
    timeout_message: &'static str,
) -> RakkaResult<()> {
    match tokio::time::timeout(timeout, semaphore.acquire_owned()).await {
        Ok(Ok(permit)) => {
            drop(permit);
            Ok(())
        }
        Ok(Err(_closed)) => Err(shutdown_testkit_error(
            "shutdown-probe-closed",
            "controlled shutdown task probe semaphore closed",
        )),
        Err(_elapsed) => Err(shutdown_testkit_error(timeout_code, timeout_message)),
    }
}

fn shutdown_testkit_error(code: impl Into<String>, message: impl Into<String>) -> RakkaError {
    RakkaError::new(Subsystem::Testkit, code, message)
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

/// Default timeout used by stream testkit probes.
pub const DEFAULT_STREAM_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Default bounded capacity used by stream testkit probes.
pub const DEFAULT_STREAM_PROBE_CAPACITY: usize = 16;

/// Factory for Akka-shaped stream testkit probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamTestKit {
    capacity: usize,
    timeout: Duration,
}

impl StreamTestKit {
    /// Creates a stream testkit with the default bounded capacity and timeout.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            capacity: DEFAULT_STREAM_PROBE_CAPACITY,
            timeout: DEFAULT_STREAM_PROBE_TIMEOUT,
        }
    }

    /// Returns this testkit with a different bounded probe capacity.
    #[must_use]
    pub const fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Returns this testkit with a different assertion timeout.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Creates a default source probe and facade source.
    pub fn source_probe<T>() -> RakkaResult<(Source<T>, TestSourceProbe<T>)>
    where
        T: Send + 'static,
    {
        Self::new().source_probe_pair()
    }

    /// Creates a source probe and facade source with explicit bounded capacity.
    pub fn source_probe_with_capacity<T>(
        capacity: usize,
    ) -> RakkaResult<(Source<T>, TestSourceProbe<T>)>
    where
        T: Send + 'static,
    {
        Self::new().with_capacity(capacity).source_probe_pair()
    }

    /// Creates a source probe and facade source from this testkit configuration.
    pub fn source_probe_pair<T>(&self) -> RakkaResult<(Source<T>, TestSourceProbe<T>)>
    where
        T: Send + 'static,
    {
        let (sink, source) =
            bounded_channel(self.capacity).map_err(|error| error.into_rakka_error())?;
        Ok((
            Source::from_stream_source(source),
            TestSourceProbe {
                sink,
                timeout: self.timeout,
            },
        ))
    }

    /// Creates a default sink probe and facade sink.
    pub fn sink_probe<T>() -> RakkaResult<(Sink<T, usize>, TestSinkProbe<T>)>
    where
        T: Send + 'static,
    {
        Self::new().sink_probe_pair()
    }

    /// Creates a sink probe and facade sink with explicit bounded capacity.
    pub fn sink_probe_with_capacity<T>(
        capacity: usize,
    ) -> RakkaResult<(Sink<T, usize>, TestSinkProbe<T>)>
    where
        T: Send + 'static,
    {
        Self::new().with_capacity(capacity).sink_probe_pair()
    }

    /// Creates a sink probe and facade sink from this testkit configuration.
    pub fn sink_probe_pair<T>(&self) -> RakkaResult<(Sink<T, usize>, TestSinkProbe<T>)>
    where
        T: Send + 'static,
    {
        let (sink, source) =
            bounded_channel(self.capacity).map_err(|error| error.into_rakka_error())?;
        Ok((
            Sink::from_stream_sink_with_lifecycle(sink),
            TestSinkProbe {
                source,
                requested: 0,
                timeout: self.timeout,
            },
        ))
    }

    /// Creates a demand-controlled actor sink probe using the default settings.
    pub fn demand_probe<T, Ack>(
        system: &ActorSystem,
        name: impl AsRef<str>,
        ack: Ack,
    ) -> RakkaResult<TestDemandProbePair<T, Ack>>
    where
        T: Send + 'static,
        Ack: Clone + Send + 'static,
    {
        Self::new().demand_probe_pair(system, name, ack)
    }

    /// Creates a demand-controlled actor sink probe from this testkit configuration.
    pub fn demand_probe_pair<T, Ack>(
        &self,
        system: &ActorSystem,
        name: impl AsRef<str>,
        ack: Ack,
    ) -> RakkaResult<TestDemandProbePair<T, Ack>>
    where
        T: Send + 'static,
        Ack: Clone + Send + 'static,
    {
        let (events, receiver) = mpsc::channel(self.capacity);
        let permits = Arc::new(Semaphore::new(0));
        let actor_ref = system.spawn_actor(
            name,
            DemandProbeActor {
                events,
                permits: Arc::clone(&permits),
                ack,
                _item: PhantomData,
            },
        )?;
        Ok((
            actor_ref,
            TestDemandProbe {
                receiver,
                permits,
                timeout: self.timeout,
            },
        ))
    }
}

impl Default for StreamTestKit {
    fn default() -> Self {
        Self::new()
    }
}

/// Probe handle that manually drives a facade source.
#[derive(Debug, Clone)]
pub struct TestSourceProbe<T> {
    sink: StreamSink<T>,
    timeout: Duration,
}

impl<T> TestSourceProbe<T>
where
    T: Send + 'static,
{
    /// Returns the low-level producer handle backing this probe.
    #[must_use]
    pub fn stream_sink(&self) -> StreamSink<T> {
        self.sink.clone()
    }

    /// Returns the current bounded source-side status.
    #[must_use]
    pub fn status(&self) -> StreamStatus {
        self.sink.status()
    }

    /// Sends one source element using the probe's default timeout.
    pub async fn send_next(&self, item: T) -> RakkaResult<()> {
        self.send_next_within(item, self.timeout).await
    }

    /// Sends one source element using an explicit timeout.
    pub async fn send_next_within(&self, item: T, timeout: Duration) -> RakkaResult<()> {
        tokio::time::timeout(timeout, self.sink.send(item))
            .await
            .map_err(|_elapsed| {
                stream_probe_error(
                    "stream-source-send-timeout",
                    "timed out sending stream probe item",
                )
            })?
            .map_err(|error| {
                let (error, _item) = error.into_parts();
                error.into_rakka_error()
            })
    }

    /// Completes the source normally after buffered items drain.
    pub fn send_complete(&self) -> RakkaResult<()> {
        self.sink.drain().map_err(|error| error.into_rakka_error())
    }

    /// Cancels the source with a test failure reason.
    pub fn send_error(&self, reason: impl Into<String>) -> usize {
        self.cancel_with(reason)
    }

    /// Cancels the source with a custom reason.
    pub fn cancel_with(&self, reason: impl Into<String>) -> usize {
        self.sink.cancel(reason)
    }

    /// Waits for downstream cancellation using the probe's default timeout.
    pub async fn expect_cancelled(&self) -> RakkaResult<StreamStatus> {
        self.expect_cancelled_within(self.timeout).await
    }

    /// Waits for downstream cancellation using an explicit timeout.
    pub async fn expect_cancelled_within(&self, timeout: Duration) -> RakkaResult<StreamStatus> {
        wait_for_stream_lifecycle(
            || self.sink.status(),
            StreamLifecycle::Cancelled,
            timeout,
            "source probe",
        )
        .await
    }
}

/// Probe handle that asserts items and lifecycle observed by a facade sink.
#[derive(Debug, Clone)]
pub struct TestSinkProbe<T> {
    source: StreamSource<T>,
    requested: usize,
    timeout: Duration,
}

impl<T> TestSinkProbe<T>
where
    T: Send + 'static,
{
    /// Returns the low-level consumer handle backing this probe.
    #[must_use]
    pub fn stream_source(&self) -> StreamSource<T> {
        self.source.clone()
    }

    /// Returns the current bounded sink-side status.
    #[must_use]
    pub fn status(&self) -> StreamStatus {
        self.source.status()
    }

    /// Records demand for `n` items.
    pub fn request(&mut self, n: usize) -> RakkaResult<()> {
        if n == 0 {
            return Err(stream_probe_error(
                "stream-probe-zero-demand",
                "stream sink probe demand must be greater than zero",
            ));
        }
        self.requested = self.requested.saturating_add(n);
        Ok(())
    }

    /// Expects the next item using the probe's default timeout.
    pub async fn expect_next(&mut self) -> RakkaResult<T> {
        self.expect_next_within(self.timeout).await
    }

    /// Expects the next item using an explicit timeout.
    pub async fn expect_next_within(&mut self, timeout: Duration) -> RakkaResult<T> {
        if self.requested == 0 {
            return Err(stream_probe_error(
                "stream-probe-no-demand",
                "stream sink probe must request demand before expecting an item",
            ));
        }

        match tokio::time::timeout(timeout, self.source.next()).await {
            Ok(Ok(Some(item))) => {
                self.requested = self.requested.saturating_sub(1);
                Ok(item)
            }
            Ok(Ok(None)) => Err(stream_probe_error(
                "stream-probe-unexpected-complete",
                "stream sink probe completed before the expected item arrived",
            )),
            Ok(Err(error)) => Err(stream_probe_error(
                "stream-probe-unexpected-error",
                format!("stream sink probe failed before the expected item arrived: {error}"),
            )),
            Err(_elapsed) => Err(stream_probe_error(
                "stream-probe-next-timeout",
                "timed out waiting for stream sink probe item",
            )),
        }
    }

    /// Expects `count` items using the probe's default timeout for each item.
    pub async fn expect_next_n(&mut self, count: usize) -> RakkaResult<Vec<T>> {
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(self.expect_next().await?);
        }
        Ok(items)
    }

    /// Asserts that no item or terminal signal arrives before the timeout.
    pub async fn expect_no_message(&mut self, timeout: Duration) -> RakkaResult<()> {
        match tokio::time::timeout(timeout, self.source.next()).await {
            Ok(Ok(Some(_item))) => Err(stream_probe_error(
                "stream-probe-unexpected-item",
                "stream sink probe received an unexpected item",
            )),
            Ok(Ok(None)) => Err(stream_probe_error(
                "stream-probe-unexpected-complete",
                "stream sink probe completed unexpectedly",
            )),
            Ok(Err(error)) => Err(stream_probe_error(
                "stream-probe-unexpected-error",
                format!("stream sink probe failed unexpectedly: {error}"),
            )),
            Err(_elapsed) => Ok(()),
        }
    }

    /// Expects normal sink completion using the probe's default timeout.
    pub async fn expect_complete(&mut self) -> RakkaResult<()> {
        self.expect_complete_within(self.timeout).await
    }

    /// Expects normal sink completion using an explicit timeout.
    pub async fn expect_complete_within(&mut self, timeout: Duration) -> RakkaResult<()> {
        match tokio::time::timeout(timeout, self.source.next()).await {
            Ok(Ok(None)) => Ok(()),
            Ok(Ok(Some(_item))) => Err(stream_probe_error(
                "stream-probe-unexpected-item",
                "stream sink probe received an item while expecting completion",
            )),
            Ok(Err(error)) => Err(stream_probe_error(
                "stream-probe-unexpected-error",
                format!("stream sink probe failed while expecting completion: {error}"),
            )),
            Err(_elapsed) => Err(stream_probe_error(
                "stream-probe-complete-timeout",
                "timed out waiting for stream sink probe completion",
            )),
        }
    }

    /// Expects a stream error using the probe's default timeout.
    pub async fn expect_error(&mut self) -> RakkaResult<StreamError> {
        self.expect_error_within(self.timeout).await
    }

    /// Expects a stream error using an explicit timeout.
    pub async fn expect_error_within(&mut self, timeout: Duration) -> RakkaResult<StreamError> {
        match tokio::time::timeout(timeout, self.source.next()).await {
            Ok(Err(error)) => Ok(error),
            Ok(Ok(Some(_item))) => Err(stream_probe_error(
                "stream-probe-unexpected-item",
                "stream sink probe received an item while expecting an error",
            )),
            Ok(Ok(None)) => Err(stream_probe_error(
                "stream-probe-unexpected-complete",
                "stream sink probe completed while expecting an error",
            )),
            Err(_elapsed) => Err(stream_probe_error(
                "stream-probe-error-timeout",
                "timed out waiting for stream sink probe error",
            )),
        }
    }

    /// Cancels the probe sink and returns the number of buffered items dropped.
    pub fn cancel(&self, reason: impl Into<String>) -> usize {
        self.source.cancel(reason)
    }
}

/// Event observed by a demand-controlled actor sink probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestDemandProbeEvent<T> {
    /// The actor sink initialized.
    Init,
    /// One stream element reached the actor sink boundary.
    Element(T),
    /// The actor sink completed normally.
    Complete,
    /// The actor sink observed an upstream failure.
    Failure(StreamError),
    /// The actor sink was cancelled before normal completion.
    Cancelled(String),
}

/// Probe actor handle for ack-driven actor sink demand tests.
pub struct TestDemandProbe<T> {
    receiver: mpsc::Receiver<TestDemandProbeEvent<T>>,
    permits: Arc<Semaphore>,
    timeout: Duration,
}

/// Actor reference and probe handle returned by demand probe factories.
pub type TestDemandProbePair<T, Ack> = (ActorRef<ActorSinkMessage<T, Ack>>, TestDemandProbe<T>);

impl<T> TestDemandProbe<T>
where
    T: Send + 'static,
{
    /// Grants demand for `n` pending or future elements.
    pub fn request(&self, n: usize) -> RakkaResult<()> {
        if n == 0 {
            return Err(stream_probe_error(
                "stream-probe-zero-demand",
                "stream demand probe demand must be greater than zero",
            ));
        }
        self.permits.add_permits(n);
        Ok(())
    }

    /// Expects actor sink initialization using the probe's default timeout.
    pub async fn expect_init(&mut self) -> RakkaResult<()> {
        match self.expect_event().await? {
            TestDemandProbeEvent::Init => Ok(()),
            event => Err(unexpected_demand_event("init", event)),
        }
    }

    /// Expects the next demanded item using the probe's default timeout.
    pub async fn expect_next(&mut self) -> RakkaResult<T> {
        match self.expect_event().await? {
            TestDemandProbeEvent::Element(item) => Ok(item),
            event => Err(unexpected_demand_event("element", event)),
        }
    }

    /// Expects normal actor sink completion using the probe's default timeout.
    pub async fn expect_complete(&mut self) -> RakkaResult<()> {
        match self.expect_event().await? {
            TestDemandProbeEvent::Complete => Ok(()),
            event => Err(unexpected_demand_event("completion", event)),
        }
    }

    /// Expects actor sink failure using the probe's default timeout.
    pub async fn expect_error(&mut self) -> RakkaResult<StreamError> {
        match self.expect_event().await? {
            TestDemandProbeEvent::Failure(error) => Ok(error),
            event => Err(unexpected_demand_event("failure", event)),
        }
    }

    /// Expects actor sink cancellation using the probe's default timeout.
    pub async fn expect_cancelled(&mut self) -> RakkaResult<String> {
        match self.expect_event().await? {
            TestDemandProbeEvent::Cancelled(reason) => Ok(reason),
            event => Err(unexpected_demand_event("cancellation", event)),
        }
    }

    /// Asserts that no actor sink event arrives before the timeout.
    pub async fn expect_no_message(&mut self, timeout: Duration) -> RakkaResult<()> {
        match tokio::time::timeout(timeout, self.receiver.recv()).await {
            Ok(Some(event)) => Err(unexpected_demand_event("no event", event)),
            Ok(None) => Err(stream_probe_error(
                "stream-probe-closed",
                "stream demand probe channel closed",
            )),
            Err(_elapsed) => Ok(()),
        }
    }

    async fn expect_event(&mut self) -> RakkaResult<TestDemandProbeEvent<T>> {
        match tokio::time::timeout(self.timeout, self.receiver.recv()).await {
            Ok(Some(event)) => Ok(event),
            Ok(None) => Err(stream_probe_error(
                "stream-probe-closed",
                "stream demand probe channel closed",
            )),
            Err(_elapsed) => Err(stream_probe_error(
                "stream-probe-event-timeout",
                "timed out waiting for stream demand probe event",
            )),
        }
    }
}

struct DemandProbeActor<T, Ack> {
    events: mpsc::Sender<TestDemandProbeEvent<T>>,
    permits: Arc<Semaphore>,
    ack: Ack,
    _item: PhantomData<fn() -> T>,
}

impl<T, Ack> Actor for DemandProbeActor<T, Ack>
where
    T: Send + 'static,
    Ack: Clone + Send + 'static,
{
    type Msg = ActorSinkMessage<T, Ack>;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        let events = self.events.clone();
        let permits = Arc::clone(&self.permits);
        let ack = self.ack.clone();
        actor_future(async move {
            match msg {
                ActorSinkMessage::Init { reply_to } => {
                    let _sent = events.send(TestDemandProbeEvent::Init).await;
                    let _ignored = reply_to.reply(ack);
                }
                ActorSinkMessage::Element { item, reply_to } => {
                    if events
                        .send(TestDemandProbeEvent::Element(item))
                        .await
                        .is_ok()
                    {
                        if let Ok(permit) = permits.acquire().await {
                            permit.forget();
                            let _ignored = reply_to.reply(ack);
                        }
                    } else {
                        let _ignored = reply_to.reply(ack);
                    }
                }
                ActorSinkMessage::Complete => {
                    let _sent = events.send(TestDemandProbeEvent::Complete).await;
                }
                ActorSinkMessage::Failure { error } => {
                    let _sent = events.send(TestDemandProbeEvent::Failure(error)).await;
                }
                ActorSinkMessage::Cancelled { reason } => {
                    let _sent = events.send(TestDemandProbeEvent::Cancelled(reason)).await;
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

fn stream_probe_error(code: &'static str, message: impl Into<String>) -> RakkaError {
    RakkaError::new(Subsystem::Testkit, code, message)
}

fn unexpected_demand_event<T>(
    expected: &'static str,
    event: TestDemandProbeEvent<T>,
) -> RakkaError {
    stream_probe_error(
        "stream-probe-unexpected-event",
        format!(
            "stream demand probe expected {expected}, received {}",
            demand_event_name(&event)
        ),
    )
}

fn demand_event_name<T>(event: &TestDemandProbeEvent<T>) -> &'static str {
    match event {
        TestDemandProbeEvent::Init => "init",
        TestDemandProbeEvent::Element(_) => "element",
        TestDemandProbeEvent::Complete => "completion",
        TestDemandProbeEvent::Failure(_) => "failure",
        TestDemandProbeEvent::Cancelled(_) => "cancellation",
    }
}

async fn wait_for_stream_lifecycle(
    mut status: impl FnMut() -> StreamStatus,
    expected: StreamLifecycle,
    timeout: Duration,
    label: &'static str,
) -> RakkaResult<StreamStatus> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let current = status();
        if current.lifecycle() == expected {
            return Ok(current);
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(stream_probe_error(
                "stream-probe-lifecycle-timeout",
                format!(
                    "timed out waiting for {label} lifecycle {expected:?}; current lifecycle is {:?}",
                    current.lifecycle()
                ),
            ));
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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
