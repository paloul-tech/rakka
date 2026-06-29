//! Durable inbox facade for agent workflow commands.
//!
//! This module maps first-class [`AgentCommand`] values to
//! the lower-level `rakka-workflow` durable inbox. The acceptance boundary stays
//! in the substrate: a command is reported as accepted only after the durable
//! inbox has persisted it.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::{DurableStateStore, Revision};
use rakka_workflow::{
    DurableInbox, InboxAcceptance, InboxCommand, InboxEntry, SystemWorkflowClock, WorkflowClock,
    WorkflowError, WorkflowId, WorkflowState,
};

use crate::{validate_command, AgentCommand, AgentFacadeError, AgentRunId};

/// Counter for agent durable inbox command acceptance attempts.
pub const METRIC_AGENT_INBOX_COMMANDS: &str = "rakka.agent_workflow.inbox.commands";

/// Shared result type for agent durable inbox operations.
pub type AgentInboxResult<T> = Result<T, AgentInboxError>;

/// Agent-level durable inbox failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentInboxError {
    /// The command was rejected before durable persistence because it failed
    /// agent command validation.
    Rejected {
        /// Validation failure.
        error: AgentFacadeError,
    },
    /// Serialization of the command envelope failed before durable persistence.
    Serialization {
        /// Serialization failure detail.
        message: String,
    },
    /// Lower-level durable workflow operation failed.
    Workflow {
        /// Workflow substrate failure.
        error: WorkflowError,
    },
}

impl AgentInboxError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Rejected { .. } => "rejected-command",
            Self::Serialization { .. } => "command-serialization",
            Self::Workflow { error } => error.code(),
        }
    }
}

impl Display for AgentInboxError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { error } => write!(f, "agent command rejected: {error}"),
            Self::Serialization { message } => {
                write!(f, "agent command serialization failed: {message}")
            }
            Self::Workflow { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentInboxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected { error } => Some(error),
            Self::Serialization { .. } => None,
            Self::Workflow { error } => Some(error),
        }
    }
}

impl From<AgentFacadeError> for AgentInboxError {
    fn from(error: AgentFacadeError) -> Self {
        Self::Rejected { error }
    }
}

impl From<WorkflowError> for AgentInboxError {
    fn from(error: WorkflowError) -> Self {
        Self::Workflow { error }
    }
}

/// Duplicate source inferred from the existing durable inbox entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentInboxDuplicateReason {
    /// Duplicate matched the durable inbox message id.
    MessageId,
    /// Duplicate matched the durable inbox deduplication key.
    DeduplicationKey,
    /// Duplicate was reported by the substrate but did not match known command
    /// metadata. This should only occur if a lower layer changes semantics.
    Unknown,
}

impl AgentInboxDuplicateReason {
    /// Stable lowercase label for metrics and logs.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::MessageId => "message-id",
            Self::DeduplicationKey => "deduplication-key",
            Self::Unknown => "unknown",
        }
    }
}

/// Agent-level result of accepting a command into the durable inbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentInboxAcceptance {
    /// A new durable inbox entry was persisted.
    Accepted {
        /// Persisted inbox entry.
        entry: InboxEntry,
        /// Store revision after persistence.
        revision: Revision,
    },
    /// An existing durable inbox entry matched the command id or
    /// deduplication key.
    Duplicate {
        /// Existing inbox entry.
        entry: InboxEntry,
        /// Current recovered revision.
        revision: Revision,
        /// Duplicate source inferred from the existing entry.
        reason: AgentInboxDuplicateReason,
    },
}

impl AgentInboxAcceptance {
    /// Returns true when this command created new durable work.
    #[must_use]
    pub const fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted { .. })
    }

    /// Returns true when this command was a duplicate of existing durable work.
    #[must_use]
    pub const fn is_duplicate(&self) -> bool {
        matches!(self, Self::Duplicate { .. })
    }

    /// Inbox entry associated with this acceptance result.
    #[must_use]
    pub const fn entry(&self) -> &InboxEntry {
        match self {
            Self::Accepted { entry, .. } | Self::Duplicate { entry, .. } => entry,
        }
    }

    /// Recovered or persisted revision associated with this result.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        match self {
            Self::Accepted { revision, .. } | Self::Duplicate { revision, .. } => *revision,
        }
    }

    /// Duplicate reason, when the result is a duplicate.
    #[must_use]
    pub const fn duplicate_reason(&self) -> Option<AgentInboxDuplicateReason> {
        match self {
            Self::Accepted { .. } => None,
            Self::Duplicate { reason, .. } => Some(*reason),
        }
    }
}

/// Durable inbox facade for one agent run.
pub struct AgentRunInbox<Store, Clock = SystemWorkflowClock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    inbox: DurableInbox<Store, Clock>,
    metrics: Arc<dyn MetricsRecorder>,
}

impl<Store> AgentRunInbox<Store, SystemWorkflowClock>
where
    Store: DurableStateStore<WorkflowState>,
{
    /// Creates an agent run inbox with the system clock and no-op metrics.
    #[must_use]
    pub fn new(run_id: AgentRunId, store: Store) -> Self {
        Self::with_metrics(run_id, store, Arc::new(NoopMetricsRecorder))
    }

    /// Creates an agent run inbox with the system clock and an explicit metrics
    /// recorder.
    #[must_use]
    pub fn with_metrics(
        run_id: AgentRunId,
        store: Store,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::from_inbox_with_metrics(
            DurableInbox::new(agent_run_workflow_id(&run_id), store),
            metrics,
        )
    }
}

impl<Store, Clock> AgentRunInbox<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Creates an agent run inbox with an explicit clock and no-op metrics.
    #[must_use]
    pub fn with_clock(run_id: AgentRunId, store: Store, clock: Clock) -> Self {
        Self::with_clock_and_metrics(run_id, store, clock, Arc::new(NoopMetricsRecorder))
    }

    /// Creates an agent run inbox with an explicit clock and metrics recorder.
    #[must_use]
    pub fn with_clock_and_metrics(
        run_id: AgentRunId,
        store: Store,
        clock: Clock,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self::from_inbox_with_metrics(
            DurableInbox::with_clock(agent_run_workflow_id(&run_id), store, clock),
            metrics,
        )
    }

    /// Wraps an existing durable inbox with no-op metrics.
    #[must_use]
    pub fn from_inbox(inbox: DurableInbox<Store, Clock>) -> Self {
        Self::from_inbox_with_metrics(inbox, Arc::new(NoopMetricsRecorder))
    }

    /// Wraps an existing durable inbox with an explicit metrics recorder.
    #[must_use]
    pub fn from_inbox_with_metrics(
        inbox: DurableInbox<Store, Clock>,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self { inbox, metrics }
    }

    /// Underlying durable workflow id.
    #[must_use]
    pub fn workflow_id(&self) -> &WorkflowId {
        self.inbox.workflow_id()
    }

    /// Metrics recorder used by this facade.
    #[must_use]
    pub fn metrics(&self) -> Arc<dyn MetricsRecorder> {
        self.metrics.clone()
    }

    /// Accesses the wrapped durable inbox.
    #[must_use]
    pub const fn inner(&self) -> &DurableInbox<Store, Clock> {
        &self.inbox
    }

    /// Mutably accesses the wrapped durable inbox.
    #[must_use]
    pub fn inner_mut(&mut self) -> &mut DurableInbox<Store, Clock> {
        &mut self.inbox
    }

    /// Consumes this facade and returns the wrapped durable inbox.
    #[must_use]
    pub fn into_inner(self) -> DurableInbox<Store, Clock> {
        self.inbox
    }

    /// Recovers the latest durable workflow state.
    pub async fn recover(&mut self) -> AgentInboxResult<&WorkflowState> {
        self.inbox.recover().await.map_err(AgentInboxError::from)
    }

    /// Accepts a first-class agent command into the durable inbox.
    ///
    /// The returned [`AgentInboxAcceptance::Accepted`] value is produced only
    /// after `rakka-workflow::DurableInbox` has persisted the inbox entry.
    pub async fn accept_command(
        &mut self,
        command: AgentCommand,
    ) -> AgentInboxResult<AgentInboxAcceptance> {
        let command_type = command.type_name();
        let message_type = command.message_type();

        if let Err(error) = validate_command(&command) {
            self.record_command_metric(command_type, message_type, "rejected", "none");
            return Err(AgentInboxError::Rejected { error });
        }

        let payload = serde_json::to_vec(&command).map_err(|error| {
            self.record_command_metric(command_type, message_type, "failed", "serialization");
            AgentInboxError::Serialization {
                message: error.to_string(),
            }
        })?;

        let inbox_command =
            InboxCommand::new(command.metadata.command_id.as_str(), message_type, payload)
                .deduplication_key(command.metadata.deduplication_key.as_str());

        let acceptance = self.inbox.accept(inbox_command).await.map_err(|error| {
            self.record_command_metric(command_type, message_type, "failed", error.code());
            AgentInboxError::Workflow { error }
        })?;

        let acceptance = map_acceptance(&command, acceptance);
        match acceptance {
            AgentInboxAcceptance::Accepted { .. } => {
                self.record_command_metric(command_type, message_type, "accepted", "none");
            }
            AgentInboxAcceptance::Duplicate { reason, .. } => {
                self.record_command_metric(
                    command_type,
                    message_type,
                    "duplicate",
                    reason.as_label(),
                );
            }
        }

        Ok(acceptance)
    }

    fn record_command_metric(
        &self,
        command_type: &'static str,
        message_type: &'static str,
        outcome: &'static str,
        detail: &'static str,
    ) {
        self.metrics.increment_counter(
            METRIC_AGENT_INBOX_COMMANDS,
            1,
            &[
                ("command_type", command_type),
                ("message_type", message_type),
                ("outcome", outcome),
                ("detail", detail),
            ],
        );
    }
}

/// Maps an agent run id to the lower-level durable workflow id.
#[must_use]
pub fn agent_run_workflow_id(run_id: &AgentRunId) -> WorkflowId {
    WorkflowId::new(run_id.as_str())
}

fn map_acceptance(command: &AgentCommand, acceptance: InboxAcceptance) -> AgentInboxAcceptance {
    match acceptance {
        InboxAcceptance::Accepted { entry, revision } => {
            AgentInboxAcceptance::Accepted { entry, revision }
        }
        InboxAcceptance::Duplicate { entry, revision } => {
            let reason = duplicate_reason(command, &entry);
            AgentInboxAcceptance::Duplicate {
                entry,
                revision,
                reason,
            }
        }
    }
}

fn duplicate_reason(command: &AgentCommand, entry: &InboxEntry) -> AgentInboxDuplicateReason {
    if entry.message_id().as_str() == command.metadata.command_id.as_str() {
        return AgentInboxDuplicateReason::MessageId;
    }

    if entry
        .deduplication_key()
        .is_some_and(|key| key.as_str() == command.metadata.deduplication_key.as_str())
    {
        return AgentInboxDuplicateReason::DeduplicationKey;
    }

    AgentInboxDuplicateReason::Unknown
}
