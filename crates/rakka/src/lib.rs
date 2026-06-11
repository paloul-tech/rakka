#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Top-level Rakka facade crate.
//!
//! This crate owns the application-facing import surface for Rakka. Component
//! crates such as `rakka-core`, `rakka-sharding`, and `rakka-persistence` remain
//! available for advanced users, tests, and implementation-specific wiring.
//!
//! Phase 0 of the Akka parity plan keeps the prelude intentionally curated. It
//! exposes the common primitives needed by application code today while later
//! phases add higher-level actor, cluster, sharding, persistence, and stream
//! facades.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use rakka::prelude::*;
//!
//! enum CounterCommand {
//!     Increment { reply_to: ReplyTo<u64> },
//! }
//!
//! struct Counter;
//!
//! impl Actor for Counter {
//!     type Msg = CounterCommand;
//!
//!     fn handle<'a>(
//!         &'a mut self,
//!         _ctx: &'a mut ActorContext<Self::Msg>,
//!         msg: Self::Msg,
//!     ) -> ActorFuture<'a> {
//!         actor_future(async move {
//!             match msg {
//!                 CounterCommand::Increment { reply_to } => {
//!                     let _ = reply_to.reply(1);
//!                 }
//!             }
//!             Ok(ActorAction::Continue)
//!         })
//!     }
//! }
//!
//! async fn run() -> RakkaResult<()> {
//!     let system = ActorSystem::new("example");
//!     let counter = system.spawn_actor("counter", Counter)?;
//!     let value = counter
//!         .ask(
//!             |reply_to| CounterCommand::Increment { reply_to },
//!             Duration::from_secs(1),
//!         )
//!         .await
//!         .map_err(|error| RakkaError::core("ask-failed", error.to_string()))?;
//!     assert_eq!(value, 1);
//!     system.shutdown();
//!     Ok(())
//! }
//! ```

/// Common application imports for Rakka.
pub mod prelude {
    pub use rakka_core::{
        actor_fn, actor_future, setup, Actor, ActorAction, ActorContext, ActorFailure, ActorFn,
        ActorFuture, ActorPath, ActorProps, ActorRef, ActorRefResolver, ActorResult, ActorSystem,
        ActorSystemBuilder, ActorSystemRuntimeSettings, ActorSystemSerializationRegistry,
        ActorSystemShutdownConfig, ActorTerminated, ActorTraceContext, ActorUid, AskError,
        Behavior, BehaviorActor, DispatcherHint, InMemoryMetricsRecorder, Message, MetricsRecorder,
        NoopMetricsRecorder, RakkaError, RakkaResult, ReplyTo, SerializedActorRef, SetupActor,
        SpawnOptions, StopError, SupervisionStrategy, TellError, TerminationReason, TimerHandle,
        WatchHandle,
    };

    #[cfg(feature = "persistence")]
    pub use rakka_persistence::{
        durable_actor_future, spawn_durable_actor, spawn_durable_actor_factory, DurableActor,
        DurableActorContext, DurableActorFuture, DurableEffect, DurableState, DurableStateStore,
        InMemoryDurableStateStore, PersistenceId, Revision,
    };

    #[cfg(feature = "sharding")]
    pub use rakka_sharding::{EntityId, EntityRef, EntityType, ShardingConfig};

    #[cfg(feature = "stream")]
    pub use rakka_stream::{BoundedStream, StreamError, StreamResult, StreamSink, StreamSource};
}

/// Actor runtime primitives.
pub mod actor {
    pub use rakka_core::*;
}

#[cfg(feature = "cluster")]
/// Cluster membership and discovery primitives.
pub mod cluster {
    pub use rakka_cluster::*;
}

#[cfg(feature = "grpc")]
/// gRPC integration adapters.
pub mod grpc {
    pub use rakka_grpc::*;
}

#[cfg(feature = "http")]
/// HTTP integration adapters.
pub mod http {
    pub use rakka_http::*;
}

#[cfg(feature = "k8s")]
/// Kubernetes operation helpers.
pub mod k8s {
    pub use rakka_k8s::*;
}

#[cfg(feature = "persistence")]
/// Durable state and typed persistence primitives.
pub mod persistence {
    pub use rakka_persistence::*;
}

#[cfg(feature = "process")]
/// Supervised child-process actors and process-backed entities.
pub mod process {
    pub use rakka_process::*;
}

#[cfg(feature = "remote")]
/// Remote envelope, serialization, and transport primitives.
pub mod remote {
    pub use rakka_remote::*;
}

#[cfg(feature = "sharding")]
/// Cluster sharding and entity routing primitives.
pub mod sharding {
    pub use rakka_sharding::*;
}

#[cfg(feature = "stream")]
/// Bounded stream primitives and adapters.
pub mod stream {
    pub use rakka_stream::*;
}

#[cfg(feature = "testkit")]
/// Testkit helpers for Rakka applications and integration surfaces.
pub mod testkit {
    pub use rakka_testkit::*;
}

#[cfg(feature = "workflow")]
/// Durable inbox/outbox workflow primitives.
pub mod workflow {
    pub use rakka_workflow::*;
}

pub use rakka_core::{runtime_name, FRAMEWORK_NAME, V1_RUNTIME};
