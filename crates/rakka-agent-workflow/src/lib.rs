#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Agentic workflow orchestration facade.
//!
//! This crate is intentionally thin in the Phase 0.1 boundary slice. It gives
//! agent workflow work a home without changing the lower-level reliability
//! semantics already implemented by `rakka-workflow`.
//!
//! The crate owns future first-class agent concepts such as runs, steps,
//! effects, human checkpoints, telemetry context, audit events, model/tool
//! adapters, and Kubernetes-scale orchestration helpers.
//!
//! `rakka-workflow` remains the durable inbox/outbox substrate. Core actor,
//! remote, and sharded delivery remain at-most-once; stronger agent workflow
//! behavior must continue to be built from durable state, durable inbox
//! acceptance, durable outbox effects, idempotency keys, and recovery.

/// Crate name used in diagnostics, docs, and feature-boundary notes.
pub const CRATE_NAME: &str = "rakka-agent-workflow";

/// Lower-level durable workflow substrate re-exports.
///
/// These items remain owned by `rakka-workflow`. They are exposed here so the
/// agent facade can compose the durable inbox/outbox substrate without moving
/// or redefining its reliability boundary.
pub mod substrate {
    pub use rakka_workflow::{
        DeduplicationKey, DurableInbox, InboxAcceptance, InboxCommand, InboxEntry, InboxStatus,
        ManualWorkflowClock, OutboxAcceptance, OutboxCommand, OutboxDispatchFuture,
        OutboxDispatchResult, OutboxDispatcher, OutboxEntry, OutboxFailureTransition,
        OutboxMessageId, OutboxStatus, OutboxTarget, RetryAttempt, RetryJitter, RetryPolicy,
        SystemWorkflowClock, WorkflowClock, WorkflowError, WorkflowId, WorkflowMessageId,
        WorkflowResult, WorkflowState, WorkflowStatus, WorkflowTelemetryEvent, WorkflowTimestamp,
    };
}

/// Common imports for early agent workflow consumers.
///
/// Later phases should add first-class agent domain types here as they become
/// stable enough for application code.
pub mod prelude {
    pub use crate::CRATE_NAME;
}
