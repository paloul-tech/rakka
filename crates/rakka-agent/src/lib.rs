#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Durable agent domain, loop runtime, and provider-neutral model adapter.
//!
//! This crate is the M1 home for the Rakka agent surface: the goal, typed-task,
//! run, evaluation, handoff, delegation, team, moderation, and workflow-tool
//! domain, the typed client, the durable loop runtime, the model adapter trait,
//! the continuous wake controller, the escrow budget ledger, autonomy
//! admission, guardrails, gates, tool binding and dispatch grants, execution
//! policy references, bounded operational queries, memory traits, structured
//! telemetry, and deterministic test support.
//!
//! Only the module map exists today. Each module documents the specification
//! section it implements and the implementation slice that fills it, so the
//! crate shape is reviewable before any behavior lands.
//!
//! # Boundaries
//!
//! `rakka-agent-workflow` remains the durable execution substrate: inbox,
//! outbox, dispatcher, effect bridge, timers, and triggers. This crate adds the
//! agent domain on top of it and does not weaken the reliability boundaries
//! below it. Core actor, remote, and sharded delivery are at-most-once; every
//! stronger agent guarantee is built from durable state, durable inbox
//! acceptance, durable outbox effects, stable operation identifiers, and
//! recovery.
//!
//! Two rules cut across every module and hold from the first commit:
//!
//! - Resolved credentials and secret material are never persisted in durable
//!   state, effects, memory, runtime events, telemetry, or snapshots.
//!   Credentials are resolved at dispatch time and never outlive the attempt.
//! - Every persisted record carries a schema version, and an unsupported
//!   version fails closed rather than being interpreted optimistically.
//!
//! Sibling crates own the rest of the agent surface: `rakka-agent-postgres` the
//! PostgreSQL memory and retrieval adapters, `rakka-agent-knowledge-graph` the
//! communal graph, and the `rakka-a2a` `agents` feature the external protocol
//! boundary. This crate does not depend on any of them.
//!
//! # Features
//!
//! - `rig` (default): the Rig-backed implementation of [`model`]'s adapter
//!   trait, owning the pinned Rig version. The crate builds and passes its
//!   tests with `--no-default-features`, the deterministic [`testkit`] adapter
//!   never requires this feature, and Rig types never appear in the non-`rig`
//!   public API or in persisted state.
//! - `otel`: the pinned OpenTelemetry GenAI semantic-convention mapping over
//!   the existing agent-workflow OTLP bridge. It does not own application
//!   exporter credentials and does not install a global SDK.
//!
//! Both features are propagated by the `rakka` facade as `rakka-agent?/rig` and
//! `rakka-agent?/otel`.

pub mod admission;
pub mod agent;
pub mod budget;
pub mod checkpoints;
pub mod choreography;
pub mod client;
pub mod coordination;
pub mod definition;
pub mod delegation;
pub mod effect;
pub mod evaluation;
pub mod goal;
pub mod guardrails;
pub mod identity;
pub mod loop_runtime;
pub mod memory;
pub mod model;
pub mod observability;
#[cfg(feature = "otel")]
pub mod otel;
pub mod query;
#[cfg(feature = "rig")]
pub mod rig;
pub mod run;
pub mod task;
pub mod testkit;
pub mod tools;
pub mod wake;
pub mod workflow_tool;
