#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Core Rakka primitives and shared conventions.
//!
//! Phase 1 provides the first local actor runtime. Distributed routing,
//! persistence, and external-process ownership are implemented in later phases.

pub mod actor;
pub mod dead_letter;
pub mod error;
pub mod metrics;
pub mod path;
pub mod supervision;
pub mod system;
pub mod telemetry;

pub use actor::{
    actor_future, Actor, ActorAction, ActorContext, ActorFailure, ActorFuture, ActorRef,
    ActorResult, ActorRuntimeSnapshot, ActorTerminated, AskError, Message, ReplyTo, StopError,
    TellError, TerminationReason, TimerHandle, DEFAULT_MAILBOX_CAPACITY,
};
pub use dead_letter::{DeadLetter, DeadLetterReason};
pub use error::{RakkaError, RakkaResult, Subsystem};
pub use metrics::{
    InMemoryMetricsRecorder, MetricAttribute, MetricAttributes, MetricKind, MetricObservation,
    MetricsRecorder, MetricsSnapshot, NoopMetricsRecorder, METRIC_ACTOR_COUNT,
    METRIC_ACTOR_MAILBOX_DEPTH, METRIC_CLUSTER_MEMBERS, METRIC_GRPC_REQUEST_LATENCY_MS,
    METRIC_HTTP_REQUEST_LATENCY_MS, METRIC_PERSISTENCE_LATENCY_MS, METRIC_PROCESS_EXITS,
    METRIC_REMOTE_FAILURES, METRIC_SHARD_OWNERSHIP_COUNT, METRIC_STREAM_CANCELLATIONS,
    METRIC_STREAM_PRESSURE,
};
pub use path::ActorPath;
pub use supervision::{ActorOptions, SupervisionStrategy};
pub use system::{ActorSystem, ActorSystemSnapshot};

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
