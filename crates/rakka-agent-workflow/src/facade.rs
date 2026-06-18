//! Command and effect scheduling facade for agent workflows.
//!
//! This module makes durability metadata explicit before later slices attach
//! the facade to the durable inbox and outbox substrate.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    AgentAttributes, AgentCausationId, AgentCommandId, AgentCorrelationId, AgentDeduplicationKey,
    AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectStatus, AgentEffectTarget,
    AgentIdempotencyKey, AgentRunId, AgentTelemetryContext, AgentTenantId, AgentTimestampMillis,
    AgentWorkflowId, ArtifactRef, HumanCheckpointId, PrincipalRef,
};

/// Shared result type for agent facade validation.
pub type AgentFacadeResult<T> = Result<T, AgentFacadeError>;

/// Validation failures returned by the command and effect facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentFacadeError {
    /// Required command metadata is missing or invalid.
    InvalidCommandMetadata {
        /// Metadata field that failed validation.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Command-specific data is missing or invalid.
    InvalidCommand {
        /// Stable command type name.
        command_type: &'static str,
        /// Command field that failed validation.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Required effect metadata is missing or invalid.
    InvalidEffectMetadata {
        /// Metadata field that failed validation.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Effect-specific scheduling data is missing or invalid.
    InvalidEffect {
        /// Stable effect kind.
        effect_kind: AgentEffectKind,
        /// Effect field that failed validation.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
}

impl Display for AgentFacadeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommandMetadata { field, reason } => {
                write!(f, "invalid command metadata field {field}: {reason}")
            }
            Self::InvalidCommand {
                command_type,
                field,
                reason,
            } => write!(f, "invalid {command_type} command field {field}: {reason}"),
            Self::InvalidEffectMetadata { field, reason } => {
                write!(f, "invalid effect metadata field {field}: {reason}")
            }
            Self::InvalidEffect {
                effect_kind,
                field,
                reason,
            } => write!(
                f,
                "invalid {} effect field {field}: {reason}",
                effect_kind.type_name()
            ),
        }
    }
}

impl Error for AgentFacadeError {}

/// Shared durable-boundary metadata used by commands and effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDurabilityMetadata {
    /// Stable durable inbox or outbox deduplication key.
    pub deduplication_key: AgentDeduplicationKey,
    /// Command or event that caused this work.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related commands, effects, logs, and audit events.
    pub correlation_id: AgentCorrelationId,
    /// Optional trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
}

impl AgentDurabilityMetadata {
    /// Creates shared durable-boundary metadata.
    #[must_use]
    pub fn new(
        deduplication_key: AgentDeduplicationKey,
        causation_id: AgentCausationId,
        correlation_id: AgentCorrelationId,
    ) -> Self {
        Self {
            deduplication_key,
            causation_id,
            correlation_id,
            telemetry_context: AgentTelemetryContext::default(),
        }
    }

    /// Sets optional telemetry context.
    #[must_use]
    pub fn telemetry_context(mut self, telemetry_context: AgentTelemetryContext) -> Self {
        self.telemetry_context = telemetry_context;
        self
    }
}

/// Metadata required for every command accepted at the agent boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCommandMetadata {
    /// Workflow definition id the command targets.
    pub workflow_id: AgentWorkflowId,
    /// Durable run id the command targets.
    pub run_id: AgentRunId,
    /// Stable command id used as the durable inbox message id.
    pub command_id: AgentCommandId,
    /// Stable durable inbox deduplication key.
    pub deduplication_key: AgentDeduplicationKey,
    /// Command or event that caused this command.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related commands, effects, logs, and audit events.
    pub correlation_id: AgentCorrelationId,
    /// Tenant or namespace that owns the command.
    pub tenant: AgentTenantId,
    /// Optional trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
    /// Principal that submitted the command, when known.
    pub principal: Option<PrincipalRef>,
    /// Command receipt timestamp.
    pub received_at: AgentTimestampMillis,
}

impl AgentCommandMetadata {
    /// Creates command metadata and validates required durability fields.
    pub fn new(
        workflow_id: AgentWorkflowId,
        run_id: AgentRunId,
        command_id: AgentCommandId,
        durability: AgentDurabilityMetadata,
        tenant: AgentTenantId,
        received_at: AgentTimestampMillis,
    ) -> AgentFacadeResult<Self> {
        let metadata = Self {
            workflow_id,
            run_id,
            command_id,
            deduplication_key: durability.deduplication_key,
            causation_id: durability.causation_id,
            correlation_id: durability.correlation_id,
            tenant,
            telemetry_context: durability.telemetry_context,
            principal: None,
            received_at,
        };
        validate_command_metadata(&metadata)?;
        Ok(metadata)
    }

    /// Sets optional telemetry context.
    #[must_use]
    pub fn telemetry_context(mut self, telemetry_context: AgentTelemetryContext) -> Self {
        self.telemetry_context = telemetry_context;
        self
    }

    /// Sets the command principal.
    #[must_use]
    pub fn principal(mut self, principal: PrincipalRef) -> Self {
        self.principal = Some(principal);
        self
    }
}

/// First-class command kinds accepted by agent workflows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum AgentCommandKind {
    /// Start a new durable run.
    StartRun,
    /// Submit an application-defined signal to an existing run.
    SubmitSignal {
        /// Bounded signal type name.
        signal_type: String,
    },
    /// Continue a paused run after internal work becomes ready.
    ContinueRun,
    /// Report successful completion of a scheduled effect.
    EffectCompleted {
        /// Effect whose result completed.
        effect_id: AgentEffectId,
    },
    /// Report failed completion of a scheduled effect.
    EffectFailed {
        /// Effect whose dispatch failed.
        effect_id: AgentEffectId,
        /// Stable bounded error code.
        error_code: String,
    },
    /// Submit a human decision for an open checkpoint.
    HumanDecisionSubmitted {
        /// Checkpoint being resolved.
        checkpoint_id: HumanCheckpointId,
        /// Stable bounded decision value.
        decision: String,
    },
    /// Notify a run that a durable timer fired.
    TimerFired {
        /// Stable timer id.
        timer_id: String,
    },
    /// Request run cancellation.
    CancelRun,
    /// Request retry of a failed or paused run.
    RetryRun,
    /// Request retention deletion or logical forgetting of a run.
    ForgetRun,
}

impl AgentCommandKind {
    /// Stable command type name used in workflow definitions.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::StartRun => "StartRun",
            Self::SubmitSignal { .. } => "SubmitSignal",
            Self::ContinueRun => "ContinueRun",
            Self::EffectCompleted { .. } => "EffectCompleted",
            Self::EffectFailed { .. } => "EffectFailed",
            Self::HumanDecisionSubmitted { .. } => "HumanDecisionSubmitted",
            Self::TimerFired { .. } => "TimerFired",
            Self::CancelRun => "CancelRun",
            Self::RetryRun => "RetryRun",
            Self::ForgetRun => "ForgetRun",
        }
    }

    /// Stable lower-level message type used by the durable inbox substrate.
    #[must_use]
    pub fn message_type(&self) -> &'static str {
        match self {
            Self::StartRun => "agent.start-run",
            Self::SubmitSignal { .. } => "agent.submit-signal",
            Self::ContinueRun => "agent.continue-run",
            Self::EffectCompleted { .. } => "agent.effect-completed",
            Self::EffectFailed { .. } => "agent.effect-failed",
            Self::HumanDecisionSubmitted { .. } => "agent.human-decision-submitted",
            Self::TimerFired { .. } => "agent.timer-fired",
            Self::CancelRun => "agent.cancel-run",
            Self::RetryRun => "agent.retry-run",
            Self::ForgetRun => "agent.forget-run",
        }
    }
}

/// Command accepted at the agent workflow boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCommand {
    /// Command kind.
    pub kind: AgentCommandKind,
    /// Required durability and telemetry metadata.
    pub metadata: AgentCommandMetadata,
    /// Optional out-of-line command payload.
    pub payload_ref: Option<ArtifactRef>,
    /// Bounded command attributes.
    pub attributes: AgentAttributes,
}

impl AgentCommand {
    /// Creates and validates an agent command.
    pub fn new(kind: AgentCommandKind, metadata: AgentCommandMetadata) -> AgentFacadeResult<Self> {
        let command = Self {
            kind,
            metadata,
            payload_ref: None,
            attributes: BTreeMap::new(),
        };
        validate_command(&command)?;
        Ok(command)
    }

    /// Sets an out-of-line command payload reference.
    #[must_use]
    pub fn payload_ref(mut self, payload_ref: ArtifactRef) -> Self {
        self.payload_ref = Some(payload_ref);
        self
    }

    /// Adds a bounded command attribute.
    pub fn attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> AgentFacadeResult<Self> {
        let key = key.into();
        if is_blank(&key) {
            return Err(AgentFacadeError::InvalidCommand {
                command_type: self.kind.type_name(),
                field: "attributes.key",
                reason: REQUIRED_FIELD,
            });
        }

        self.attributes.insert(key, value.into());
        validate_command(&self)?;
        Ok(self)
    }

    /// Validates this command.
    pub fn validate(&self) -> AgentFacadeResult<()> {
        validate_command(self)
    }

    /// Stable command type name used in workflow definitions.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        self.kind.type_name()
    }

    /// Stable lower-level message type used by the durable inbox substrate.
    #[must_use]
    pub fn message_type(&self) -> &'static str {
        self.kind.message_type()
    }
}

/// Metadata required to schedule an external effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEffectMetadata {
    /// Stable effect id used as the durable outbox message id.
    pub effect_id: AgentEffectId,
    /// Stable durable outbox deduplication key.
    pub deduplication_key: AgentDeduplicationKey,
    /// Stable downstream idempotency key.
    pub idempotency_key: AgentIdempotencyKey,
    /// Command or step that caused this effect.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related work.
    pub correlation_id: AgentCorrelationId,
    /// Optional trace, baggage, and span-link context.
    pub telemetry_context: AgentTelemetryContext,
    /// Creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Due timestamp for first dispatch.
    pub due_at: Option<AgentTimestampMillis>,
    /// Timeout in milliseconds.
    pub timeout_ms: Option<u64>,
}

impl AgentEffectMetadata {
    /// Creates effect metadata and validates required durability fields.
    pub fn new(
        effect_id: AgentEffectId,
        durability: AgentDurabilityMetadata,
        idempotency_key: AgentIdempotencyKey,
        created_at: AgentTimestampMillis,
    ) -> AgentFacadeResult<Self> {
        let metadata = Self {
            effect_id,
            deduplication_key: durability.deduplication_key,
            idempotency_key,
            causation_id: durability.causation_id,
            correlation_id: durability.correlation_id,
            telemetry_context: durability.telemetry_context,
            created_at,
            due_at: None,
            timeout_ms: None,
        };
        validate_effect_metadata(&metadata)?;
        Ok(metadata)
    }

    /// Sets optional telemetry context.
    #[must_use]
    pub fn telemetry_context(mut self, telemetry_context: AgentTelemetryContext) -> Self {
        self.telemetry_context = telemetry_context;
        self
    }

    /// Sets the first due timestamp for dispatch.
    #[must_use]
    pub const fn due_at(mut self, due_at: AgentTimestampMillis) -> Self {
        self.due_at = Some(due_at);
        self
    }

    /// Sets the effect timeout in milliseconds.
    #[must_use]
    pub const fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }
}

/// Validated request to schedule an external effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEffectSchedule {
    /// Effect kind.
    pub kind: AgentEffectKind,
    /// Dispatch target.
    pub target: AgentEffectTarget,
    /// Required durability and telemetry metadata.
    pub metadata: AgentEffectMetadata,
    /// Optional out-of-line request payload.
    pub payload_ref: Option<ArtifactRef>,
    /// Expected result type name.
    pub expected_result_type: Option<String>,
}

impl AgentEffectSchedule {
    /// Creates and validates an effect schedule request.
    pub fn new(
        kind: AgentEffectKind,
        target: AgentEffectTarget,
        metadata: AgentEffectMetadata,
    ) -> AgentFacadeResult<Self> {
        let schedule = Self {
            kind,
            target,
            metadata,
            payload_ref: None,
            expected_result_type: None,
        };
        validate_effect_schedule(&schedule)?;
        Ok(schedule)
    }

    /// Sets an out-of-line request payload reference.
    #[must_use]
    pub fn payload_ref(mut self, payload_ref: ArtifactRef) -> Self {
        self.payload_ref = Some(payload_ref);
        self
    }

    /// Sets the expected result type name.
    pub fn expected_result_type(
        mut self,
        expected_result_type: impl Into<String>,
    ) -> AgentFacadeResult<Self> {
        let expected_result_type = expected_result_type.into();
        if is_blank(&expected_result_type) {
            return Err(AgentFacadeError::InvalidEffect {
                effect_kind: self.kind,
                field: "expected_result_type",
                reason: REQUIRED_FIELD,
            });
        }

        self.expected_result_type = Some(expected_result_type);
        validate_effect_schedule(&self)?;
        Ok(self)
    }

    /// Validates this effect schedule request.
    pub fn validate(&self) -> AgentFacadeResult<()> {
        validate_effect_schedule(self)
    }

    /// Stable lower-level message type used by the durable outbox substrate.
    #[must_use]
    pub const fn message_type(&self) -> &'static str {
        self.kind.message_type()
    }

    /// Converts this validated schedule into a persisted scheduled effect.
    pub fn into_effect(self) -> AgentFacadeResult<AgentEffect> {
        validate_effect_schedule(&self)?;

        Ok(AgentEffect {
            effect_id: self.metadata.effect_id,
            deduplication_key: self.metadata.deduplication_key,
            kind: self.kind,
            target: self.target,
            status: AgentEffectStatus::Scheduled,
            payload_ref: self.payload_ref,
            result_ref: None,
            timeout_ms: self.metadata.timeout_ms,
            idempotency_key: self.metadata.idempotency_key,
            expected_result_type: self.expected_result_type,
            causation_id: self.metadata.causation_id,
            correlation_id: self.metadata.correlation_id,
            telemetry_context: self.metadata.telemetry_context,
            attempt: 0,
            created_at: self.metadata.created_at,
            due_at: self.metadata.due_at,
            last_error_code: None,
        })
    }
}

impl AgentEffect {
    /// Stable lower-level message type used by the durable outbox substrate.
    #[must_use]
    pub const fn message_type(&self) -> &'static str {
        self.kind.message_type()
    }
}

impl AgentEffectKind {
    /// Stable effect type name used in application metadata.
    #[must_use]
    pub const fn type_name(self) -> &'static str {
        match self {
            Self::ModelCall => "ModelCall",
            Self::ToolCall => "ToolCall",
            Self::ProcessCall => "ProcessCall",
            Self::HttpCall => "HttpCall",
            Self::GrpcCall => "GrpcCall",
            Self::StreamPublish => "StreamPublish",
            Self::ArtifactWrite => "ArtifactWrite",
            Self::HumanApprovalRequest => "HumanApprovalRequest",
            Self::Notification => "Notification",
            Self::ChildWorkflowCommand => "ChildWorkflowCommand",
            Self::AuditEvent => "AuditEvent",
        }
    }

    /// Stable lowercase label for telemetry and persisted metadata.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::ModelCall => "model-call",
            Self::ToolCall => "tool-call",
            Self::ProcessCall => "process-call",
            Self::HttpCall => "http-call",
            Self::GrpcCall => "grpc-call",
            Self::StreamPublish => "stream-publish",
            Self::ArtifactWrite => "artifact-write",
            Self::HumanApprovalRequest => "human-approval-request",
            Self::Notification => "notification",
            Self::ChildWorkflowCommand => "child-workflow-command",
            Self::AuditEvent => "audit-event",
        }
    }

    /// Stable lower-level message type used by the durable outbox substrate.
    #[must_use]
    pub const fn message_type(self) -> &'static str {
        match self {
            Self::ModelCall => "agent.effect.model-call",
            Self::ToolCall => "agent.effect.tool-call",
            Self::ProcessCall => "agent.effect.process-call",
            Self::HttpCall => "agent.effect.http-call",
            Self::GrpcCall => "agent.effect.grpc-call",
            Self::StreamPublish => "agent.effect.stream-publish",
            Self::ArtifactWrite => "agent.effect.artifact-write",
            Self::HumanApprovalRequest => "agent.effect.human-approval-request",
            Self::Notification => "agent.effect.notification",
            Self::ChildWorkflowCommand => "agent.effect.child-workflow-command",
            Self::AuditEvent => "agent.effect.audit-event",
        }
    }
}

/// Validates required command metadata.
pub fn validate_command_metadata(metadata: &AgentCommandMetadata) -> AgentFacadeResult<()> {
    require_command_metadata(metadata.workflow_id.as_str(), "workflow_id")?;
    require_command_metadata(metadata.run_id.as_str(), "run_id")?;
    require_command_metadata(metadata.command_id.as_str(), "command_id")?;
    require_command_metadata(metadata.deduplication_key.as_str(), "deduplication_key")?;
    require_command_metadata(metadata.causation_id.as_str(), "causation_id")?;
    require_command_metadata(metadata.correlation_id.as_str(), "correlation_id")?;
    require_command_metadata(metadata.tenant.as_str(), "tenant")?;

    if let Some(principal) = &metadata.principal {
        require_command_metadata(&principal.principal_type, "principal.principal_type")?;
        require_command_metadata(&principal.principal_id, "principal.principal_id")?;
    }

    Ok(())
}

/// Validates a first-class agent command.
pub fn validate_command(command: &AgentCommand) -> AgentFacadeResult<()> {
    validate_command_metadata(&command.metadata)?;

    match &command.kind {
        AgentCommandKind::StartRun
        | AgentCommandKind::ContinueRun
        | AgentCommandKind::CancelRun
        | AgentCommandKind::RetryRun
        | AgentCommandKind::ForgetRun => {}
        AgentCommandKind::SubmitSignal { signal_type } => {
            require_command(command.kind.type_name(), signal_type, "signal_type")?;
        }
        AgentCommandKind::EffectCompleted { effect_id } => {
            require_command(command.kind.type_name(), effect_id.as_str(), "effect_id")?;
        }
        AgentCommandKind::EffectFailed {
            effect_id,
            error_code,
        } => {
            require_command(command.kind.type_name(), effect_id.as_str(), "effect_id")?;
            require_command(command.kind.type_name(), error_code, "error_code")?;
        }
        AgentCommandKind::HumanDecisionSubmitted {
            checkpoint_id,
            decision,
        } => {
            require_command(
                command.kind.type_name(),
                checkpoint_id.as_str(),
                "checkpoint_id",
            )?;
            require_command(command.kind.type_name(), decision, "decision")?;
        }
        AgentCommandKind::TimerFired { timer_id } => {
            require_command(command.kind.type_name(), timer_id, "timer_id")?;
        }
    }

    for key in command.attributes.keys() {
        require_command(command.kind.type_name(), key, "attributes.key")?;
    }

    Ok(())
}

/// Validates required effect metadata.
pub fn validate_effect_metadata(metadata: &AgentEffectMetadata) -> AgentFacadeResult<()> {
    require_effect_metadata(metadata.effect_id.as_str(), "effect_id")?;
    require_effect_metadata(metadata.deduplication_key.as_str(), "deduplication_key")?;
    require_effect_metadata(metadata.idempotency_key.as_str(), "idempotency_key")?;
    require_effect_metadata(metadata.causation_id.as_str(), "causation_id")?;
    require_effect_metadata(metadata.correlation_id.as_str(), "correlation_id")?;
    Ok(())
}

/// Validates an effect schedule request.
pub fn validate_effect_schedule(schedule: &AgentEffectSchedule) -> AgentFacadeResult<()> {
    validate_effect_metadata(&schedule.metadata)?;
    require_effect(
        schedule.kind,
        &schedule.target.target_type,
        "target.target_type",
    )?;
    require_effect(schedule.kind, &schedule.target.name, "target.name")?;

    if let Some(expected_result_type) = &schedule.expected_result_type {
        require_effect(schedule.kind, expected_result_type, "expected_result_type")?;
    }

    for key in schedule.target.attributes.keys() {
        require_effect(schedule.kind, key, "target.attributes.key")?;
    }

    Ok(())
}

const REQUIRED_FIELD: &str = "required field must be non-empty";

fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}

fn require_command_metadata(value: &str, field: &'static str) -> AgentFacadeResult<()> {
    if is_blank(value) {
        Err(AgentFacadeError::InvalidCommandMetadata {
            field,
            reason: REQUIRED_FIELD,
        })
    } else {
        Ok(())
    }
}

fn require_command(
    command_type: &'static str,
    value: &str,
    field: &'static str,
) -> AgentFacadeResult<()> {
    if is_blank(value) {
        Err(AgentFacadeError::InvalidCommand {
            command_type,
            field,
            reason: REQUIRED_FIELD,
        })
    } else {
        Ok(())
    }
}

fn require_effect_metadata(value: &str, field: &'static str) -> AgentFacadeResult<()> {
    if is_blank(value) {
        Err(AgentFacadeError::InvalidEffectMetadata {
            field,
            reason: REQUIRED_FIELD,
        })
    } else {
        Ok(())
    }
}

fn require_effect(
    effect_kind: AgentEffectKind,
    value: &str,
    field: &'static str,
) -> AgentFacadeResult<()> {
    if is_blank(value) {
        Err(AgentFacadeError::InvalidEffect {
            effect_kind,
            field,
            reason: REQUIRED_FIELD,
        })
    } else {
        Ok(())
    }
}
