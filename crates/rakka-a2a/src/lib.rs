#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! A2A protocol adapter for durable Rakka agent workflow runs.
//!
//! `rakka-a2a` turns Rakka's durable agent-workflow substrate into an
//! [A2A](https://a2a-protocol.org) agent surface: public `message:send`,
//! task reads, listing, cancellation, streaming, and push notification
//! configuration are mapped onto durable Rakka run state, the durable
//! workflow inbox/outbox, and (optionally) cluster-sharded run ownership.
//!
//! # Reliability boundary
//!
//! The adapter preserves Rakka's reliability contract:
//!
//! - Rakka actors, remoting, and sharding remain **at-most-once** delivery
//!   surfaces.
//! - Accepted A2A work is durable only after the workflow inbox write
//!   succeeds; public commands are acknowledged only after durable inbox
//!   acceptance.
//! - Push notifications and other outbound effects are **at-least-once**
//!   unless the target participates in idempotency.
//! - Task projections and task events are query/observability surfaces;
//!   durable run state plus durable inbox/outbox state remain authoritative.
//! - Rakka remoting stays trusted private cluster traffic and is never used
//!   as public A2A transport.
//! - The crate never persists resolved credentials or secret material in
//!   plans, state, outbox effects, task events, logs, metrics, snapshots, or
//!   indexes. Push credentials are rejected by default or replaced by an
//!   application-supplied logical binding reference.
//!
//! # Features
//!
//! The crate ships with `default = []`; every adapter surface is opt-in:
//!
//! - `server`: A2A SDK `RequestHandler` implementation, `RakkaA2AServiceBuilder`,
//!   agent-card producer, and axum route composition helpers.
//! - `sharding`: sharded run owner entity, remote-safe owner protocol, codec
//!   registration, and cluster routing.
//! - `postgres`: PostgreSQL task projection store, push config storage,
//!   scheduler watermarks, and crate-owned idempotent migrations.
//! - `http`: route composition with Rakka HTTP observability helpers.
//! - `k8s`: Kubernetes drain/readiness integration helpers.
//! - `otel`: trace-context propagation and OpenTelemetry-compatible
//!   attribute helpers.
//! - `testkit`: in-memory stores, fixtures, and compatibility probes.
//! - `agents`: typed Rakka Agent surface over `rakka-agent` entities —
//!   `AgentTaskId` task identity, durable deduplicated ingress, the
//!   specification 14.3 state projection, and the versioned agent-management
//!   extension (implies `server`).
//!
//! # A2A SDK version policy
//!
//! This crate integrates the community `a2a-lf` / `a2a-server-lf` SDK crates
//! (imported as `a2a` and `a2a-server`). SDK versions are pinned at the
//! minor level (`a2a-lf 0.3.x`, `a2a-server-lf 0.4.x`); an SDK minor-version
//! bump is treated as a semver-visible change of this crate because A2A
//! request/response types appear in its public API. `a2a-server` is consumed
//! with `default-features = false`, so no TLS provider is forced on
//! applications; enable one through your own `a2a-server-lf` dependency if
//! you serve TLS in-process. The crate follows the workspace MSRV
//! (Rust 1.85, pinned by `rust-toolchain.toml`); SDK upgrades that raise the
//! MSRV are deferred until the workspace MSRV moves.

pub mod auth;
pub mod catalog;
pub mod dispatch;
pub mod error;
pub mod mapping;
pub mod observability;
pub mod projection;
pub mod protocol;
pub mod push;
pub mod routing;
pub mod stores;
pub mod task;

#[cfg(feature = "server")]
pub mod agent_card;

#[cfg(feature = "sharding")]
pub mod codec;
#[cfg(feature = "sharding")]
pub mod host;
#[cfg(feature = "sharding")]
pub mod router;

#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "postgres")]
mod postgres_watcher;

#[cfg(feature = "server")]
pub mod handler;
#[cfg(feature = "server")]
pub mod routes;
#[cfg(feature = "server")]
pub mod stream;

#[cfg(feature = "agents")]
pub mod agents;

#[cfg(any(test, feature = "testkit"))]
pub mod testing;

// Consumed by the request handler (`server`) and the sharded owner host
// (`sharding`); compiled only when a consumer is enabled so single-feature
// builds stay warning-free.
#[cfg(any(feature = "server", feature = "sharding"))]
mod runsync;
mod support;

// Curated root re-exports for the primary public API.
pub use crate::error::RakkaA2AHandlerError;

#[cfg(feature = "server")]
pub use crate::handler::{
    RakkaA2ABuildError, RakkaA2ARequestHandler, RakkaA2AService, RakkaA2AServiceBuilder,
    RakkaA2ASettings,
};
