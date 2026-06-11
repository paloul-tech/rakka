#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Durable state APIs, in-memory store, and local durable actor adapter.

pub mod actor;
pub mod effect;
pub mod error;
pub mod event_sourced;
pub mod memory;
pub mod store;

use rakka_core::{MetricsRecorder, Subsystem, METRIC_PERSISTENCE_LATENCY_MS};

pub use actor::{
    durable_actor_future, spawn_durable_actor, spawn_durable_actor_factory,
    spawn_durable_actor_factory_with_options, spawn_durable_actor_with_options, DurableActor,
    DurableActorContext, DurableActorFuture,
};
pub use effect::{DurableEffect, DurableStateChange};
pub use error::{DurableError, DurableResult};
pub use event_sourced::{
    event_sourced_actor_future, spawn_event_sourced_actor, spawn_event_sourced_actor_factory,
    spawn_event_sourced_actor_factory_with_options, spawn_event_sourced_actor_with_options,
    EventSourcedActor, EventSourcedActorContext, EventSourcedActorFuture, EventSourcedEffect,
};
pub use memory::{InMemoryDurableStateStore, InMemoryEventJournal, InMemorySnapshotStore};
pub use store::{
    DurableState, DurableStateStore, EventJournal, EventMetadata, EventRecord, PersistenceEvent,
    PersistenceId, RecoveryOptions, Revision, SequenceNr, SnapshotMetadata, SnapshotRecord,
    SnapshotSelection, SnapshotStore, StateCodec, StateRecord, StoreFuture, TaggedEvent,
    PERSISTENCE_ID_SEPARATOR,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-persistence";

/// Subsystem associated with durable state APIs.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Persistence
}

/// Records a durable-state operation latency sample in milliseconds.
pub fn record_persistence_latency_ms(
    recorder: &dyn MetricsRecorder,
    backend: &str,
    operation: &str,
    outcome: &str,
    latency_ms: f64,
) {
    recorder.record_histogram(
        METRIC_PERSISTENCE_LATENCY_MS,
        latency_ms,
        &[
            ("backend", backend),
            ("operation", operation),
            ("outcome", outcome),
        ],
    );
}
