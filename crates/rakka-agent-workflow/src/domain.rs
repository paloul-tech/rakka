//! Serializable agent workflow domain contracts.
//!
//! This module is the Phase 0.2 data-contract draft. It defines durable shapes
//! and identifier types, but does not implement workflow execution behavior.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($(#[$meta:meta])* $vis:vis $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        $vis struct $name(String);

        impl $name {
            /// Creates a new identifier.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns this identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes this identifier and returns its owned string.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id! {
    /// Stable workflow definition identity.
    pub AgentWorkflowId
}

string_id! {
    /// Stable id for one durable workflow run.
    pub AgentRunId
}

string_id! {
    /// Stable id for one workflow step.
    pub AgentStepId
}

string_id! {
    /// Stable id for one durable effect.
    pub AgentEffectId
}

string_id! {
    /// Stable id for one human checkpoint.
    pub HumanCheckpointId
}

string_id! {
    /// Stable id for one durable timer.
    pub AgentTimerId
}

string_id! {
    /// Stable id for one dispatcher fleet work item.
    pub AgentDispatchId
}

string_id! {
    /// Stable id for one dispatcher fleet worker.
    pub AgentDispatcherWorkerId
}

string_id! {
    /// Stable id for one accepted command.
    pub AgentCommandId
}

string_id! {
    /// Stable key used to deduplicate durable inbox and outbox writes.
    pub AgentDeduplicationKey
}

string_id! {
    /// Stable key supplied to external effect targets for idempotent execution.
    pub AgentIdempotencyKey
}

string_id! {
    /// Stable id describing the command or event that caused another action.
    pub AgentCausationId
}

string_id! {
    /// Stable id used to correlate related commands, effects, logs, and audit events.
    pub AgentCorrelationId
}

string_id! {
    /// Stable id for one durable audit event.
    pub AgentAuditEventId
}

string_id! {
    /// Stable tenant or namespace identifier supplied by application code.
    pub AgentTenantId
}

string_id! {
    /// Stable workflow definition version.
    pub WorkflowDefinitionVersion
}

/// Serialized state schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StateSchemaVersion(u32);

impl StateSchemaVersion {
    /// Creates a schema version from a positive integer.
    #[must_use]
    pub const fn new(version: u32) -> Self {
        Self(version)
    }

    /// Returns the version number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Timestamp represented as Unix epoch milliseconds.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct AgentTimestampMillis(u64);

impl AgentTimestampMillis {
    /// Creates a timestamp from Unix epoch milliseconds.
    #[must_use]
    pub const fn new(millis: u64) -> Self {
        Self(millis)
    }

    /// Returns Unix epoch milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }
}

/// String key-value attributes reserved for bounded metadata.
pub type AgentAttributes = BTreeMap<String, String>;

/// Fields that must not be used as hot metric labels.
pub const FORBIDDEN_HOT_METRIC_FIELDS: &[&str] = &[
    "workflow_id",
    "run_id",
    "entity_id",
    "command_id",
    "effect_id",
    "checkpoint_id",
    "correlation_id",
    "causation_id",
    "deduplication_key",
    "idempotency_key",
    "prompt_text",
    "completion_text",
    "tool_arguments",
    "artifact_uri",
    "full_error_message",
];

/// Fields intended to be safe as bounded metric labels when applications keep
/// their values bounded.
pub const BOUNDED_METRIC_FIELDS: &[&str] = &[
    "workflow_type",
    "definition_version",
    "state_schema_version",
    "status",
    "step_kind",
    "effect_kind",
    "outcome",
    "error_code",
    "retry_attempt_bucket",
    "tenant_tier",
];

/// High-cardinality fields that may appear in traces, logs, audit records, and
/// snapshots, but not in hot metric labels.
pub const TRACE_LOG_AUDIT_ID_FIELDS: &[&str] = &[
    "workflow_id",
    "run_id",
    "command_id",
    "step_id",
    "effect_id",
    "checkpoint_id",
    "audit_event_id",
    "correlation_id",
    "causation_id",
    "deduplication_key",
    "idempotency_key",
];

/// A registered agent workflow definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflow {
    /// Stable workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Human-readable workflow type used as a bounded telemetry dimension.
    pub workflow_type: String,
    /// Workflow definition version.
    pub definition_version: WorkflowDefinitionVersion,
    /// Serialized state schema version expected by this definition.
    pub state_schema_version: StateSchemaVersion,
    /// Optional display name.
    pub display_name: Option<String>,
    /// Bounded lifecycle status labels this workflow may report.
    pub status_labels: Vec<String>,
    /// Accepted command type names.
    pub command_types: Vec<String>,
    /// Step definitions owned by this workflow.
    pub steps: Vec<AgentStep>,
    /// Application-owned payload types used by this workflow.
    pub payload_types: Vec<AgentPayloadDescriptor>,
    /// Optional artifact reference for retry policy details.
    pub retry_policy_ref: Option<ArtifactRef>,
    /// Optional artifact reference for timeout policy details.
    pub timeout_policy_ref: Option<ArtifactRef>,
    /// Optional artifact reference for approval policy details.
    pub approval_policy_ref: Option<ArtifactRef>,
    /// Bounded labels suitable for telemetry when values are controlled.
    pub observability_labels: AgentAttributes,
}

/// Application-owned payload descriptor for workflow inputs, state, commands,
/// effects, or adapter payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPayloadDescriptor {
    /// Stable payload type name.
    pub type_name: String,
    /// Optional content type for serialized payload bytes.
    pub content_type: Option<String>,
    /// Optional schema artifact reference.
    pub schema_ref: Option<ArtifactRef>,
    /// Bounded payload metadata.
    pub attributes: AgentAttributes,
}

impl AgentPayloadDescriptor {
    /// Creates a payload descriptor by explicit type name.
    #[must_use]
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            content_type: None,
            schema_ref: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Creates a descriptor from a Rust type name.
    #[must_use]
    pub fn for_type<T>() -> Self
    where
        T: 'static,
    {
        Self::new(std::any::type_name::<T>())
    }

    /// Sets the serialized content type.
    #[must_use]
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets an opaque schema artifact reference.
    #[must_use]
    pub fn schema_ref(mut self, schema_ref: ArtifactRef) -> Self {
        self.schema_ref = Some(schema_ref);
        self
    }

    /// Adds bounded payload metadata.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// One durable execution of an agent workflow definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunState {
    /// Stable run id.
    pub run_id: AgentRunId,
    /// Workflow definition id used by this run.
    pub workflow_id: AgentWorkflowId,
    /// Tenant or namespace that owns this run.
    pub tenant: Option<AgentTenantId>,
    /// Definition version selected for this run.
    pub definition_version: WorkflowDefinitionVersion,
    /// Serialized state schema version for this run state.
    pub state_schema_version: StateSchemaVersion,
    /// Current lifecycle status.
    pub status: AgentRunStatus,
    /// Current step cursor.
    pub current_step_id: Option<AgentStepId>,
    /// Current attempt count for the current step.
    pub current_attempt: u32,
    /// Input payload reference.
    pub inputs_ref: Option<ArtifactRef>,
    /// Durable application state payload.
    pub state_payload: AgentStatePayload,
    /// Persisted checkpoints associated with the run.
    pub checkpoints: Vec<HumanCheckpoint>,
    /// Persisted effects associated with the run.
    pub pending_effects: Vec<AgentEffect>,
    /// Currently open human checkpoint when status is waiting for human input.
    pub pending_human_checkpoint: Option<HumanCheckpointId>,
    /// Cancellation details when cancellation has been requested.
    pub cancellation: Option<AgentCancellation>,
    /// Run creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Last update timestamp.
    pub updated_at: AgentTimestampMillis,
    /// Terminal timestamp when the run has completed, failed, or cancelled.
    pub completed_at: Option<AgentTimestampMillis>,
}

/// Durable application state payload policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentStatePayload {
    /// No application-owned state payload is stored.
    Empty,
    /// State is stored out of line and referenced by artifact metadata.
    Artifact(ArtifactRef),
    /// Small inline state for tests, fixtures, and tightly bounded metadata.
    Inline(InlineState),
}

/// Small inline state payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineState {
    /// Payload content type.
    pub content_type: String,
    /// Serialized payload bytes.
    pub bytes: Vec<u8>,
    /// Declared payload size in bytes.
    pub size_bytes: u64,
}

/// Run lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRunStatus {
    /// Command has been accepted but the run has not started executing.
    Accepted,
    /// Run is actively executing.
    Running,
    /// Run is waiting for a durable timer.
    WaitingForTimer,
    /// Run is waiting for a human decision.
    WaitingForHuman,
    /// Run is waiting for an external effect result.
    WaitingForEffect,
    /// Cancellation has been requested.
    Cancelling,
    /// Run completed successfully.
    Completed,
    /// Run failed.
    Failed,
    /// Run is executing compensation.
    Compensating,
    /// Run was cancelled.
    Cancelled,
}

impl AgentRunStatus {
    /// Stable lowercase label for telemetry and registry metadata.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::WaitingForTimer => "waiting-for-timer",
            Self::WaitingForHuman => "waiting-for-human",
            Self::WaitingForEffect => "waiting-for-effect",
            Self::Cancelling => "cancelling",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Compensating => "compensating",
            Self::Cancelled => "cancelled",
        }
    }
}

impl HumanCheckpointStatus {
    /// Stable lowercase label for telemetry and snapshots.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Edited => "edited",
            Self::Escalated => "escalated",
            Self::TimedOut => "timed-out",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns true when no further human decision should resolve this status.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Approved | Self::Rejected | Self::Edited | Self::TimedOut | Self::Cancelled
        )
    }
}

/// One resumable workflow step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStep {
    /// Stable step id.
    pub step_id: AgentStepId,
    /// Step kind.
    pub kind: AgentStepKind,
    /// Optional display name.
    pub display_name: Option<String>,
    /// Candidate next step ids.
    pub next_step_ids: Vec<AgentStepId>,
    /// Optional timeout in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Optional artifact reference for step-specific configuration.
    pub config_ref: Option<ArtifactRef>,
    /// Bounded labels suitable for telemetry when values are controlled.
    pub observability_labels: AgentAttributes,
}

/// Supported step categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentStepKind {
    /// Model call step.
    ModelCall,
    /// Tool call step.
    ToolCall,
    /// Planner step.
    Planner,
    /// Branching decision step.
    Branch,
    /// Wait step.
    Wait,
    /// Human checkpoint step.
    HumanCheckpoint,
    /// Child workflow step.
    ChildWorkflow,
    /// Compensation step.
    Compensation,
    /// Terminal step.
    Terminal,
}

/// One external side effect scheduled through durable outbox semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEffect {
    /// Stable effect id.
    pub effect_id: AgentEffectId,
    /// Stable durable outbox deduplication key.
    pub deduplication_key: AgentDeduplicationKey,
    /// Effect kind.
    pub kind: AgentEffectKind,
    /// Dispatch target.
    pub target: AgentEffectTarget,
    /// Current effect status.
    pub status: AgentEffectStatus,
    /// Payload or request artifact reference.
    pub payload_ref: Option<ArtifactRef>,
    /// Result artifact reference, when available.
    pub result_ref: Option<ArtifactRef>,
    /// Timeout in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Idempotency key supplied to the downstream target.
    pub idempotency_key: AgentIdempotencyKey,
    /// Expected result type name.
    pub expected_result_type: Option<String>,
    /// Command or step that caused this effect.
    pub causation_id: AgentCausationId,
    /// Correlation id shared across related work.
    pub correlation_id: AgentCorrelationId,
    /// Trace and baggage context persisted for this effect.
    pub telemetry_context: AgentTelemetryContext,
    /// Current dispatch attempt.
    pub attempt: u32,
    /// Creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Due timestamp for dispatch or retry.
    pub due_at: Option<AgentTimestampMillis>,
    /// Stable error code for the last failure.
    pub last_error_code: Option<String>,
}

/// Supported effect categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentEffectKind {
    /// Model provider request.
    ModelCall,
    /// Tool adapter request.
    ToolCall,
    /// Process actor request.
    ProcessCall,
    /// HTTP request.
    HttpCall,
    /// gRPC request.
    GrpcCall,
    /// Stream publication.
    StreamPublish,
    /// Artifact write.
    ArtifactWrite,
    /// Human approval request.
    HumanApprovalRequest,
    /// Notification request.
    Notification,
    /// Child workflow command.
    ChildWorkflowCommand,
    /// Audit event write.
    AuditEvent,
}

/// Effect lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentEffectStatus {
    /// Effect is scheduled and waiting for dispatch.
    Scheduled,
    /// Dispatch is in progress.
    Dispatching,
    /// Effect completed successfully.
    Completed,
    /// Effect failed and may be retried.
    Failed,
    /// Effect retry is scheduled for a later time.
    RetryScheduled,
    /// Effect retry budget is exhausted.
    Exhausted,
    /// Effect was cancelled.
    Cancelled,
}

/// External effect target descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEffectTarget {
    /// Target category such as `model`, `tool`, `process`, `http`, or `grpc`.
    pub target_type: String,
    /// Stable target name.
    pub name: String,
    /// Optional address, route, or logical endpoint.
    pub address: Option<String>,
    /// Bounded target attributes.
    pub attributes: AgentAttributes,
}

/// A persisted human decision checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanCheckpoint {
    /// Stable checkpoint id.
    pub checkpoint_id: HumanCheckpointId,
    /// Current checkpoint status.
    pub status: HumanCheckpointStatus,
    /// Decision prompt or summary reference.
    pub summary: String,
    /// Available decisions.
    pub available_decisions: Vec<HumanDecisionOption>,
    /// Roles or policy hints required to resolve the checkpoint.
    pub required_roles: Vec<String>,
    /// Due timestamp for timeout or escalation.
    pub due_at: Option<AgentTimestampMillis>,
    /// Escalation target when the checkpoint is overdue.
    pub escalation_target: Option<String>,
    /// Artifact references that provide decision context.
    pub context_artifacts: Vec<ArtifactRef>,
    /// Principal that created the checkpoint.
    pub created_by: Option<PrincipalRef>,
    /// Principal that resolved the checkpoint.
    pub resolved_by: Option<PrincipalRef>,
    /// Creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Resolution timestamp.
    pub resolved_at: Option<AgentTimestampMillis>,
    /// Durable audit event ids associated with this checkpoint.
    pub audit_event_ids: Vec<AgentAuditEventId>,
}

/// Human checkpoint lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HumanCheckpointStatus {
    /// Waiting for a human decision.
    Open,
    /// Approved.
    Approved,
    /// Rejected.
    Rejected,
    /// Edited by a human reviewer.
    Edited,
    /// Escalated.
    Escalated,
    /// Timed out.
    TimedOut,
    /// Cancelled.
    Cancelled,
}

/// One available human decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanDecisionOption {
    /// Stable decision value.
    pub value: String,
    /// Human-readable label.
    pub label: String,
    /// Whether a comment is required when selecting this decision.
    pub requires_comment: bool,
}

/// A principal reference supplied by application authentication code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalRef {
    /// Principal category such as user, service, or automation.
    pub principal_type: String,
    /// Stable principal id.
    pub principal_id: String,
    /// Optional display name.
    pub display_name: Option<String>,
}

/// Reference to out-of-line durable artifact storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Stable artifact id.
    pub artifact_id: String,
    /// Artifact kind.
    pub kind: ArtifactKind,
    /// Application-owned URI or logical storage reference.
    pub uri: String,
    /// Optional checksum for integrity validation.
    pub checksum: Option<String>,
    /// Optional content type.
    pub content_type: Option<String>,
    /// Optional byte length.
    pub byte_len: Option<u64>,
    /// Retention class selected by application policy.
    pub retention_class: Option<String>,
    /// Optional encryption metadata for application-owned storage.
    pub encryption: Option<ArtifactEncryptionRef>,
    /// Redaction status.
    pub redaction: RedactionStatus,
    /// Artifact creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Bounded artifact metadata.
    pub metadata: AgentAttributes,
}

/// Encryption metadata for an artifact reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactEncryptionRef {
    /// Encryption algorithm or policy name.
    pub algorithm: String,
    /// Application-owned key reference, such as a KMS key URI.
    pub key_ref: String,
    /// Bounded encryption context metadata.
    pub context: AgentAttributes,
}

impl ArtifactEncryptionRef {
    /// Creates encryption metadata for an artifact reference.
    #[must_use]
    pub fn new(algorithm: impl Into<String>, key_ref: impl Into<String>) -> Self {
        Self {
            algorithm: algorithm.into(),
            key_ref: key_ref.into(),
            context: AgentAttributes::new(),
        }
    }

    /// Adds bounded encryption context metadata.
    #[must_use]
    pub fn context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }
}

/// Artifact categories expected by agent workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactKind {
    /// Original input payload.
    Input,
    /// Prompt payload.
    Prompt,
    /// Model completion payload.
    Completion,
    /// Generic file.
    File,
    /// Embedding or vector data.
    Embedding,
    /// Tool output.
    ToolOutput,
    /// Screenshot or visual evidence.
    Screenshot,
    /// Runtime log payload.
    Log,
    /// Application-owned state payload.
    State,
    /// Other artifact category.
    Other,
}

impl ArtifactKind {
    /// Stable lowercase label for telemetry, logs, and storage policy.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Prompt => "prompt",
            Self::Completion => "completion",
            Self::File => "file",
            Self::Embedding => "embedding",
            Self::ToolOutput => "tool-output",
            Self::Screenshot => "screenshot",
            Self::Log => "log",
            Self::State => "state",
            Self::Other => "other",
        }
    }
}

/// Redaction status for payloads, artifacts, logs, and audit records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RedactionStatus {
    /// Redaction status is not known.
    Unknown,
    /// Payload is not redacted.
    Unredacted,
    /// Payload has been redacted.
    Redacted,
    /// Payload is represented only by a reference.
    ReferenceOnly,
}

impl RedactionStatus {
    /// Stable lowercase label for telemetry, logs, and storage policy.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Unredacted => "unredacted",
            Self::Redacted => "redacted",
            Self::ReferenceOnly => "reference-only",
        }
    }
}

/// Serializable trace, baggage, and span-link context.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTelemetryContext {
    /// W3C Trace Context `traceparent` value.
    pub trace_parent: Option<String>,
    /// W3C Trace Context `tracestate` value.
    pub trace_state: Option<String>,
    /// Low-cardinality baggage values allowed by application policy.
    pub baggage: AgentAttributes,
    /// Span links for asynchronous resume and retry boundaries.
    pub span_links: Vec<AgentSpanLink>,
}

/// Serializable span link metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSpanLink {
    /// Linked trace id.
    pub trace_id: String,
    /// Linked span id.
    pub span_id: String,
    /// Optional linked trace state.
    pub trace_state: Option<String>,
    /// Bounded link attributes.
    pub attributes: AgentAttributes,
}

/// Durable audit event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAuditEvent {
    /// Stable audit event id.
    pub audit_event_id: AgentAuditEventId,
    /// Audit event kind.
    pub kind: AgentAuditEventKind,
    /// Workflow definition id.
    pub workflow_id: AgentWorkflowId,
    /// Run id.
    pub run_id: AgentRunId,
    /// Workflow definition version.
    pub definition_version: WorkflowDefinitionVersion,
    /// Tenant or namespace associated with the event.
    pub tenant: Option<AgentTenantId>,
    /// Step id associated with the event.
    pub step_id: Option<AgentStepId>,
    /// Effect id associated with the event.
    pub effect_id: Option<AgentEffectId>,
    /// Checkpoint id associated with the event.
    pub checkpoint_id: Option<HumanCheckpointId>,
    /// Command id associated with the event.
    pub command_id: Option<AgentCommandId>,
    /// Causation id.
    pub causation_id: AgentCausationId,
    /// Correlation id.
    pub correlation_id: AgentCorrelationId,
    /// Principal associated with this event.
    pub actor_principal: Option<PrincipalRef>,
    /// Artifact references associated with this event.
    pub artifact_refs: Vec<ArtifactRef>,
    /// Content hashes associated with redacted or reference-only payloads.
    pub content_hashes: AgentAttributes,
    /// Redaction status.
    pub redaction: RedactionStatus,
    /// Telemetry correlation context.
    pub telemetry_context: AgentTelemetryContext,
    /// Event timestamp.
    pub occurred_at: AgentTimestampMillis,
    /// Bounded audit attributes.
    pub attributes: AgentAttributes,
}

/// Audit event categories expected by agent workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAuditEventKind {
    /// Run was created.
    RunCreated,
    /// Workflow definition version was selected.
    WorkflowDefinitionSelected,
    /// Input was accepted.
    InputAccepted,
    /// Model was requested.
    ModelRequested,
    /// Model response was received.
    ModelResponseReceived,
    /// Tool was requested.
    ToolRequested,
    /// Tool response was received.
    ToolResponseReceived,
    /// Artifact was written.
    ArtifactWritten,
    /// Checkpoint was created.
    CheckpointCreated,
    /// Human decision was made.
    HumanDecisionMade,
    /// Policy override occurred.
    PolicyOverride,
    /// Run completed.
    RunCompleted,
    /// Run failed.
    RunFailed,
    /// Run was cancelled.
    RunCancelled,
    /// Retention deletion occurred.
    RetentionDeletion,
}

/// Cancellation metadata for a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCancellation {
    /// Principal that requested cancellation.
    pub requested_by: Option<PrincipalRef>,
    /// Timestamp when cancellation was requested.
    pub requested_at: AgentTimestampMillis,
    /// Stable reason code.
    pub reason_code: String,
    /// Optional human-readable reason summary.
    pub reason_summary: Option<String>,
}
