#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Core Rakka primitives and shared conventions.
//!
//! Phase 1 provides the first local actor runtime. Distributed routing,
//! persistence, and external-process ownership are implemented in later phases.

pub mod actor;
pub mod coordinated_shutdown;
pub mod dead_letter;
pub mod error;
pub mod metrics;
pub mod operational;
pub mod path;
pub mod receptionist;
pub mod routers;
pub mod supervision;
pub mod system;
pub mod telemetry;

pub use actor::{
    actor_fn, actor_future, setup, Actor, ActorAction, ActorContext, ActorFailure, ActorFn,
    ActorFuture, ActorRef, ActorResult, ActorRuntimeSnapshot, ActorTerminated, ActorTraceContext,
    AskError, Behavior, BehaviorActor, Message, ReplyTo, SerializedActorRef, SetupActor, StopError,
    TellError, TerminationReason, TimerHandle, WatchHandle, DEFAULT_MAILBOX_CAPACITY,
};
pub use coordinated_shutdown::{
    CoordinatedShutdown, CoordinatedShutdownReason, CoordinatedShutdownReport,
    CoordinatedShutdownSettings, ShutdownFailurePolicy, ShutdownOutcome, ShutdownPhase,
    ShutdownPhaseReport, ShutdownTask, ShutdownTaskAttribute, ShutdownTaskContext,
    ShutdownTaskFuture, ShutdownTaskOptions, ShutdownTaskReport, ShutdownTaskResult,
    ShutdownTaskStatus,
};
pub use dead_letter::{DeadLetter, DeadLetterReason};
pub use error::{RakkaError, RakkaResult, Subsystem};
pub use metrics::{
    export_open_telemetry_metrics, export_prometheus_text, export_prometheus_text_with_config,
    prometheus_label_name, prometheus_metric_name, InMemoryMetricsRecorder, MetricAttribute,
    MetricAttributes, MetricKind, MetricObservation, MetricsRecorder, MetricsSnapshot,
    NoopMetricsRecorder, OpenTelemetryDataPoint, OpenTelemetryInstrumentKind, OpenTelemetryMetric,
    OpenTelemetryMetricsExport, OpenTelemetryTemporality, PrometheusTextConfig, METRIC_ACTOR_COUNT,
    METRIC_ACTOR_MAILBOX_DEPTH, METRIC_CLUSTER_MEMBERS, METRIC_GRPC_REQUEST_LATENCY_MS,
    METRIC_HTTP_REQUEST_LATENCY_MS, METRIC_K8S_COMPATIBILITY, METRIC_K8S_READINESS,
    METRIC_PERSISTENCE_LATENCY_MS, METRIC_PROCESS_EXITS, METRIC_REMOTE_FAILURES,
    METRIC_SHARD_OWNERSHIP_COUNT, METRIC_STREAM_CANCELLATIONS, METRIC_STREAM_PRESSURE,
};
pub use operational::{
    DeploymentProfile, OperationalTimeoutDefaults, SecurityDefaults, DEFAULT_ACTOR_ASK_TIMEOUT,
    DEFAULT_KUBERNETES_PRESTOP_TIMEOUT, DEFAULT_KUBERNETES_TERMINATION_GRACE_PERIOD_SECONDS,
    DEFAULT_PROCESS_SHUTDOWN_TIMEOUT, DEFAULT_PROCESS_STARTUP_TIMEOUT,
    DEFAULT_REMOTE_CONNECT_TIMEOUT, DEFAULT_REMOTE_IDLE_TIMEOUT,
    DEFAULT_REMOTE_OUTBOUND_QUEUE_CAPACITY, DEFAULT_STREAM_DRAIN_TIMEOUT,
};
pub use path::{validate_actor_path_segment, ActorPath, ActorUid};
pub use receptionist::{
    Listing, Receptionist, ReceptionistError, ReceptionistRegistration, ReceptionistResult,
    ReceptionistSubscription, ServiceKey,
};
pub use routers::{
    GroupNoRouteeBehavior, GroupRouter, GroupRouterBuilder, GroupRouterSnapshot,
    GroupRouterTellError, GroupRoutingStrategy, PoolRouter, PoolRouterBuilder, PoolRouterTellError,
    PoolRoutingStrategy, Routers,
};
pub use supervision::{
    ActorOptions, ActorProps, DispatcherHint, SpawnOptions, SupervisionStrategy,
};
pub use system::{
    ActorRefResolver, ActorSystem, ActorSystemBuilder, ActorSystemRuntimeSettings,
    ActorSystemSerializationRegistry, ActorSystemShutdownConfig, ActorSystemSnapshot,
    DEFAULT_SYSTEM_TERMINATION_TIMEOUT,
};

/// Framework name used in diagnostics and metric prefixes.
pub const FRAMEWORK_NAME: &str = "rakka";

/// Async runtime selected for Rakka v1.
pub const V1_RUNTIME: &str = "tokio";

/// Tokio task handle type used by runtime-facing crates.
pub type TokioJoinHandle<T> = tokio::task::JoinHandle<T>;

/// Returns the async runtime selected for Rakka v1.
#[must_use]
pub const fn runtime_name() -> &'static str {
    V1_RUNTIME
}
