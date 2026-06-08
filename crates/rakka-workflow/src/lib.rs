#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Durable workflow reliability foundation.

use rakka_core::Subsystem;

mod clock;
mod error;
mod inbox;
mod model;

pub use clock::{ManualWorkflowClock, SystemWorkflowClock, WorkflowClock};
pub use error::{WorkflowError, WorkflowResult};
pub use inbox::{
    DurableInbox, InboxAcceptance, InboxCommand, OutboxAcceptance, OutboxCommand,
    OutboxDispatchFuture, OutboxDispatchResult, OutboxDispatcher,
};
pub use model::{
    DeduplicationKey, InboxEntry, InboxStatus, OutboxEntry, OutboxFailureTransition,
    OutboxMessageId, OutboxStatus, OutboxTarget, RetryAttempt, RetryJitter, RetryPolicy,
    WorkflowId, WorkflowMessageId, WorkflowState, WorkflowStatus, WorkflowTelemetryEvent,
    WorkflowTimestamp,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-workflow";

/// Subsystem associated with durable workflow reliability.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Workflow
}

/// Default telemetry label for durable inbox processing.
pub const DURABLE_INBOX: &str = "durable-inbox";

/// Default telemetry label for durable outbox processing.
pub const DURABLE_OUTBOX: &str = "durable-outbox";
