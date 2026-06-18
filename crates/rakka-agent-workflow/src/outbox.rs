//! Durable outbox facade for agent workflow effects.
//!
//! This module maps first-class [`AgentEffect`](crate::AgentEffect) values to
//! the lower-level `rakka-workflow` durable outbox. Scheduled effects are
//! visible through [`AgentRunInbox::due_effects`] only after the substrate has
//! persisted the outbox entry.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_persistence::{DurableStateStore, Revision};
use rakka_workflow::{
    OutboxAcceptance, OutboxCommand, OutboxEntry, OutboxTarget, WorkflowClock, WorkflowError,
    WorkflowState, WorkflowTimestamp,
};

use crate::{
    validate_effect_schedule, AgentEffect, AgentEffectMetadata, AgentEffectSchedule,
    AgentEffectStatus, AgentFacadeError, AgentRunInbox, AgentTimestampMillis,
};

/// Counter for agent durable outbox effect scheduling attempts.
pub const METRIC_AGENT_OUTBOX_EFFECTS: &str = "rakka.agent_workflow.outbox.effects";

/// Shared result type for agent durable outbox operations.
pub type AgentOutboxResult<T> = Result<T, AgentOutboxError>;

/// Agent-level durable outbox failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentOutboxError {
    /// The effect was rejected before durable persistence because it failed
    /// agent effect validation.
    Rejected {
        /// Validation failure.
        error: AgentFacadeError,
    },
    /// Serialization of the effect envelope failed before durable persistence.
    Serialization {
        /// Serialization failure detail.
        message: String,
    },
    /// Deserialization of a persisted effect envelope failed.
    Deserialization {
        /// Deserialization failure detail.
        message: String,
    },
    /// Lower-level durable workflow operation failed.
    Workflow {
        /// Workflow substrate failure.
        error: WorkflowError,
    },
}

impl AgentOutboxError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Rejected { .. } => "rejected-effect",
            Self::Serialization { .. } => "effect-serialization",
            Self::Deserialization { .. } => "effect-deserialization",
            Self::Workflow { error } => error.code(),
        }
    }
}

impl Display for AgentOutboxError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected { error } => write!(f, "agent effect rejected: {error}"),
            Self::Serialization { message } => {
                write!(f, "agent effect serialization failed: {message}")
            }
            Self::Deserialization { message } => {
                write!(f, "agent effect deserialization failed: {message}")
            }
            Self::Workflow { error } => Display::fmt(error, f),
        }
    }
}

impl Error for AgentOutboxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected { error } => Some(error),
            Self::Serialization { .. } | Self::Deserialization { .. } => None,
            Self::Workflow { error } => Some(error),
        }
    }
}

impl From<AgentFacadeError> for AgentOutboxError {
    fn from(error: AgentFacadeError) -> Self {
        Self::Rejected { error }
    }
}

impl From<WorkflowError> for AgentOutboxError {
    fn from(error: WorkflowError) -> Self {
        Self::Workflow { error }
    }
}

/// Duplicate source inferred from the existing durable outbox entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentOutboxDuplicateReason {
    /// Duplicate matched the durable outbox message id.
    MessageId,
    /// Duplicate matched the durable outbox deduplication key.
    DeduplicationKey,
    /// Duplicate was reported by the substrate but did not match known effect
    /// metadata. This should only occur if a lower layer changes semantics.
    Unknown,
}

impl AgentOutboxDuplicateReason {
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

/// Agent-level result of scheduling an effect into the durable outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentOutboxAcceptance {
    /// A new durable outbox entry was persisted.
    Scheduled {
        /// Persisted outbox entry.
        entry: OutboxEntry,
        /// Store revision after persistence.
        revision: Revision,
    },
    /// An existing durable outbox entry matched the effect id or
    /// deduplication key.
    Duplicate {
        /// Existing outbox entry.
        entry: OutboxEntry,
        /// Current recovered revision.
        revision: Revision,
        /// Duplicate source inferred from the existing entry.
        reason: AgentOutboxDuplicateReason,
    },
}

impl AgentOutboxAcceptance {
    /// Returns true when this effect created new durable work.
    #[must_use]
    pub const fn is_scheduled(&self) -> bool {
        matches!(self, Self::Scheduled { .. })
    }

    /// Returns true when this effect was a duplicate of existing durable work.
    #[must_use]
    pub const fn is_duplicate(&self) -> bool {
        matches!(self, Self::Duplicate { .. })
    }

    /// Outbox entry associated with this acceptance result.
    #[must_use]
    pub const fn entry(&self) -> &OutboxEntry {
        match self {
            Self::Scheduled { entry, .. } | Self::Duplicate { entry, .. } => entry,
        }
    }

    /// Recovered or persisted revision associated with this result.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        match self {
            Self::Scheduled { revision, .. } | Self::Duplicate { revision, .. } => *revision,
        }
    }

    /// Duplicate reason, when the result is a duplicate.
    #[must_use]
    pub const fn duplicate_reason(&self) -> Option<AgentOutboxDuplicateReason> {
        match self {
            Self::Scheduled { .. } => None,
            Self::Duplicate { reason, .. } => Some(*reason),
        }
    }
}

/// One due durable outbox effect plus its underlying outbox entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDueEffect {
    /// Underlying durable outbox entry.
    pub entry: OutboxEntry,
    /// Deserialized first-class agent effect.
    pub effect: AgentEffect,
}

impl<Store, Clock> AgentRunInbox<Store, Clock>
where
    Store: DurableStateStore<WorkflowState>,
    Clock: WorkflowClock,
{
    /// Schedules a first-class agent effect into the durable outbox.
    ///
    /// The returned [`AgentOutboxAcceptance::Scheduled`] value is produced only
    /// after `rakka-workflow::DurableInbox` has persisted the outbox entry.
    pub async fn schedule_effect(
        &mut self,
        effect: AgentEffect,
    ) -> AgentOutboxResult<AgentOutboxAcceptance> {
        let effect_kind = effect.kind.type_name();
        let message_type = effect.message_type();
        let target_type = effect.target.target_type.clone();

        let command = match agent_effect_to_outbox_command(&effect) {
            Ok(command) => command,
            Err(error) => {
                let detail = match &error {
                    AgentOutboxError::Rejected { .. } => "validation",
                    AgentOutboxError::Serialization { .. } => "serialization",
                    AgentOutboxError::Deserialization { .. } => "deserialization",
                    AgentOutboxError::Workflow { error } => error.code(),
                };
                self.record_effect_metric(
                    effect_kind,
                    message_type,
                    &target_type,
                    "failed",
                    detail,
                );
                return Err(error);
            }
        };

        let acceptance = self
            .inner_mut()
            .schedule_outbox(command)
            .await
            .map_err(|error| {
                self.record_effect_metric(
                    effect_kind,
                    message_type,
                    &target_type,
                    "failed",
                    error.code(),
                );
                AgentOutboxError::Workflow { error }
            })?;

        let acceptance = map_outbox_acceptance(&effect, acceptance);
        match acceptance {
            AgentOutboxAcceptance::Scheduled { .. } => {
                self.record_effect_metric(
                    effect_kind,
                    message_type,
                    &target_type,
                    "scheduled",
                    "none",
                );
            }
            AgentOutboxAcceptance::Duplicate { reason, .. } => {
                self.record_effect_metric(
                    effect_kind,
                    message_type,
                    &target_type,
                    "duplicate",
                    reason.as_label(),
                );
            }
        }

        Ok(acceptance)
    }

    /// Returns due agent effects from the current durable outbox snapshot.
    pub fn due_effects(&self) -> AgentOutboxResult<Vec<AgentDueEffect>> {
        let entries = self.inner().due_outbox().map_err(AgentOutboxError::from)?;
        entries
            .into_iter()
            .map(agent_due_effect_from_entry)
            .collect()
    }

    fn record_effect_metric(
        &self,
        effect_kind: &'static str,
        message_type: &'static str,
        target_type: &str,
        outcome: &'static str,
        detail: &'static str,
    ) {
        self.metrics().increment_counter(
            METRIC_AGENT_OUTBOX_EFFECTS,
            1,
            &[
                ("effect_kind", effect_kind),
                ("message_type", message_type),
                ("target_type", target_type),
                ("outcome", outcome),
                ("detail", detail),
            ],
        );
    }
}

/// Converts a first-class agent effect into a lower-level durable outbox
/// command.
pub fn agent_effect_to_outbox_command(effect: &AgentEffect) -> AgentOutboxResult<OutboxCommand> {
    validate_effect_for_outbox(effect)?;

    let payload = serde_json::to_vec(effect).map_err(|error| AgentOutboxError::Serialization {
        message: error.to_string(),
    })?;

    let mut command = OutboxCommand::new(
        effect.effect_id.as_str(),
        agent_effect_outbox_target(&effect.target),
        effect.message_type(),
        payload,
    )
    .deduplication_key(effect.deduplication_key.as_str());

    if let Some(due_at) = effect.due_at {
        command = command.scheduled_at(agent_timestamp_to_workflow_timestamp(due_at));
    }

    Ok(command)
}

/// Converts an agent effect target into the lower-level durable outbox target.
#[must_use]
pub fn agent_effect_outbox_target(target: &crate::AgentEffectTarget) -> OutboxTarget {
    match target.target_type.as_str() {
        "actor" => OutboxTarget::actor(target.address.as_ref().unwrap_or(&target.name).to_string()),
        "entity" => {
            let entity_type = target
                .attributes
                .get("entity_type")
                .unwrap_or(&target.name)
                .to_string();
            let entity_id = target
                .attributes
                .get("entity_id")
                .or(target.address.as_ref())
                .unwrap_or(&target.name)
                .to_string();
            OutboxTarget::entity(entity_type, entity_id)
        }
        _ => OutboxTarget::application(target.name.clone()),
    }
}

/// Converts an agent timestamp into a lower-level workflow timestamp.
#[must_use]
pub const fn agent_timestamp_to_workflow_timestamp(
    timestamp: AgentTimestampMillis,
) -> WorkflowTimestamp {
    WorkflowTimestamp::from_millis(timestamp.as_millis())
}

fn validate_effect_for_outbox(effect: &AgentEffect) -> AgentOutboxResult<()> {
    let metadata = AgentEffectMetadata {
        effect_id: effect.effect_id.clone(),
        deduplication_key: effect.deduplication_key.clone(),
        idempotency_key: effect.idempotency_key.clone(),
        causation_id: effect.causation_id.clone(),
        correlation_id: effect.correlation_id.clone(),
        telemetry_context: effect.telemetry_context.clone(),
        created_at: effect.created_at,
        due_at: effect.due_at,
        timeout_ms: effect.timeout_ms,
    };
    let schedule = AgentEffectSchedule {
        kind: effect.kind,
        target: effect.target.clone(),
        metadata,
        payload_ref: effect.payload_ref.clone(),
        expected_result_type: effect.expected_result_type.clone(),
    };
    validate_effect_schedule(&schedule)?;

    if effect.status != AgentEffectStatus::Scheduled {
        return Err(AgentOutboxError::Rejected {
            error: AgentFacadeError::InvalidEffect {
                effect_kind: effect.kind,
                field: "status",
                reason: "effect status must be scheduled",
            },
        });
    }

    Ok(())
}

fn agent_due_effect_from_entry(entry: OutboxEntry) -> AgentOutboxResult<AgentDueEffect> {
    let effect = serde_json::from_slice(entry.payload()).map_err(|error| {
        AgentOutboxError::Deserialization {
            message: error.to_string(),
        }
    })?;
    Ok(AgentDueEffect { entry, effect })
}

fn map_outbox_acceptance(
    effect: &AgentEffect,
    acceptance: OutboxAcceptance,
) -> AgentOutboxAcceptance {
    match acceptance {
        OutboxAcceptance::Scheduled { entry, revision } => {
            AgentOutboxAcceptance::Scheduled { entry, revision }
        }
        OutboxAcceptance::Duplicate { entry, revision } => {
            let reason = duplicate_reason(effect, &entry);
            AgentOutboxAcceptance::Duplicate {
                entry,
                revision,
                reason,
            }
        }
    }
}

fn duplicate_reason(effect: &AgentEffect, entry: &OutboxEntry) -> AgentOutboxDuplicateReason {
    if entry.message_id().as_str() == effect.effect_id.as_str() {
        return AgentOutboxDuplicateReason::MessageId;
    }

    if entry
        .deduplication_key()
        .is_some_and(|key| key.as_str() == effect.deduplication_key.as_str())
    {
        return AgentOutboxDuplicateReason::DeduplicationKey;
    }

    AgentOutboxDuplicateReason::Unknown
}
