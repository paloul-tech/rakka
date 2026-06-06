#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Durable state APIs, in-memory store, and local durable actor adapter.

pub mod actor;
pub mod effect;
pub mod error;
pub mod memory;
pub mod store;

use rakka_core::Subsystem;

pub use actor::{
    durable_actor_future, spawn_durable_actor, spawn_durable_actor_factory,
    spawn_durable_actor_factory_with_options, spawn_durable_actor_with_options, DurableActor,
    DurableActorContext, DurableActorFuture,
};
pub use effect::{DurableEffect, DurableStateChange};
pub use error::{DurableError, DurableResult};
pub use memory::InMemoryDurableStateStore;
pub use store::{
    DurableState, DurableStateStore, PersistenceId, Revision, StateCodec, StateRecord, StoreFuture,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-persistence";

/// Subsystem associated with durable state APIs.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Persistence
}
