//! Structured logs and durable audit helpers for agent workflows.
//!
//! Rakka keeps audit records durable and queryable independently from the
//! OpenTelemetry telemetry backend. Structured log events in this module mirror
//! the OpenTelemetry log data model so applications can bridge them into their
//! chosen SDK or collector pipeline.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    bounded_export_attributes, validate_agent_telemetry_context, AgentAttributes, AgentAuditEvent,
    AgentAuditEventId, AgentAuditEventKind, AgentCorrelationId, AgentRunId, AgentTelemetryContext,
    AgentTimestampMillis, AgentTraceContext, AgentTraceError, AgentWorkflowId, ArtifactRef,
    RedactionStatus, AGENT_EXPORT_MAX_ATTRIBUTES, CRATE_NAME,
};

/// Default instrumentation scope for agent workflow log events.
pub const AGENT_LOG_INSTRUMENTATION_SCOPE: &str = CRATE_NAME;

/// Default maximum serialized body size for a hot structured log event.
pub const DEFAULT_AGENT_LOG_BODY_LIMIT_BYTES: usize = 4 * 1024;

/// Stable attribute key for workflow definition id.
pub const AGENT_LOG_ATTR_WORKFLOW_ID: &str = "workflow_id";

/// Stable attribute key for workflow type.
pub const AGENT_LOG_ATTR_WORKFLOW_TYPE: &str = "workflow_type";

/// Stable attribute key for workflow definition version.
pub const AGENT_LOG_ATTR_DEFINITION_VERSION: &str = "definition_version";

/// Stable attribute key for run id.
pub const AGENT_LOG_ATTR_RUN_ID: &str = "run_id";

/// Stable attribute key for tenant id.
pub const AGENT_LOG_ATTR_TENANT_ID: &str = "tenant_id";

/// Stable attribute key for step id.
pub const AGENT_LOG_ATTR_STEP_ID: &str = "step_id";

/// Stable attribute key for effect id.
pub const AGENT_LOG_ATTR_EFFECT_ID: &str = "effect_id";

/// Stable attribute key for checkpoint id.
pub const AGENT_LOG_ATTR_CHECKPOINT_ID: &str = "checkpoint_id";

/// Stable attribute key for command id.
pub const AGENT_LOG_ATTR_COMMAND_ID: &str = "command_id";

/// Stable attribute key for audit event id.
pub const AGENT_LOG_ATTR_AUDIT_EVENT_ID: &str = "audit_event_id";

/// Stable attribute key for causation id.
pub const AGENT_LOG_ATTR_CAUSATION_ID: &str = "causation_id";

/// Stable attribute key for correlation id.
pub const AGENT_LOG_ATTR_CORRELATION_ID: &str = "correlation_id";

/// Stable attribute key for redaction status.
pub const AGENT_LOG_ATTR_REDACTION: &str = "redaction";

/// Stable attribute key for audit event kind.
pub const AGENT_LOG_ATTR_AUDIT_KIND: &str = "audit_kind";

/// Shared result type for structured logs and durable audit helpers.
pub type AgentAuditResult<T> = Result<T, AgentAuditError>;

/// Boxed future returned by audit sinks.
pub type AgentAuditSinkFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentAuditResult<T>> + Send + 'a>>;

/// Structured log and durable audit errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentAuditError {
    /// A structured log event failed validation.
    InvalidLogEvent {
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A durable audit event failed validation.
    InvalidAuditEvent {
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A redaction policy check failed.
    RedactionPolicy {
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Trace context on a log or audit event failed validation.
    TraceContext {
        /// Trace-context failure.
        error: AgentTraceError,
    },
    /// Audit sink storage failed.
    Sink {
        /// Stable bounded failure message.
        message: String,
    },
}

impl AgentAuditError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidLogEvent { .. } => "invalid-log-event",
            Self::InvalidAuditEvent { .. } => "invalid-audit-event",
            Self::RedactionPolicy { .. } => "audit-redaction-policy",
            Self::TraceContext { .. } => "audit-trace-context",
            Self::Sink { .. } => "audit-sink",
        }
    }
}

impl Display for AgentAuditError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLogEvent { field, reason } => {
                write!(f, "invalid log event field {field}: {reason}")
            }
            Self::InvalidAuditEvent { field, reason } => {
                write!(f, "invalid audit event field {field}: {reason}")
            }
            Self::RedactionPolicy { field, reason } => {
                write!(f, "redaction policy rejected {field}: {reason}")
            }
            Self::TraceContext { error } => Display::fmt(error, f),
            Self::Sink { message } => write!(f, "audit sink failed: {message}"),
        }
    }
}

impl Error for AgentAuditError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::TraceContext { error } => Some(error),
            Self::InvalidLogEvent { .. }
            | Self::InvalidAuditEvent { .. }
            | Self::RedactionPolicy { .. }
            | Self::Sink { .. } => None,
        }
    }
}

impl From<AgentTraceError> for AgentAuditError {
    fn from(error: AgentTraceError) -> Self {
        Self::TraceContext { error }
    }
}

/// OpenTelemetry severity bands used by agent workflow logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentLogSeverity {
    /// Fine-grained trace event.
    Trace,
    /// Debugging event.
    Debug,
    /// Informational event.
    Info,
    /// Warning event.
    Warn,
    /// Error event.
    Error,
    /// Fatal event.
    Fatal,
}

impl AgentLogSeverity {
    /// OpenTelemetry severity number for this severity band.
    #[must_use]
    pub const fn severity_number(self) -> u8 {
        match self {
            Self::Trace => 1,
            Self::Debug => 5,
            Self::Info => 9,
            Self::Warn => 13,
            Self::Error => 17,
            Self::Fatal => 21,
        }
    }

    /// OpenTelemetry severity text for this severity band.
    #[must_use]
    pub const fn severity_text(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Fatal => "FATAL",
        }
    }
}

/// Instrumentation scope carried by an agent workflow log event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentInstrumentationScope {
    /// Scope name, usually crate, module, or instrumentation library name.
    pub name: String,
    /// Optional scope version.
    pub version: Option<String>,
    /// Optional OpenTelemetry schema URL.
    pub schema_url: Option<String>,
    /// Scope attributes.
    pub attributes: AgentAttributes,
}

impl AgentInstrumentationScope {
    /// Creates an instrumentation scope with the supplied name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: None,
            schema_url: None,
            attributes: AgentAttributes::new(),
        }
    }

    /// Sets a scope version.
    #[must_use]
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Sets a schema URL.
    #[must_use]
    pub fn schema_url(mut self, schema_url: impl Into<String>) -> Self {
        self.schema_url = Some(schema_url.into());
        self
    }

    /// Adds a scope attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

impl Default for AgentInstrumentationScope {
    fn default() -> Self {
        Self::new(AGENT_LOG_INSTRUMENTATION_SCOPE)
    }
}

/// OpenTelemetry-compatible structured log event for agent workflow lifecycle events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentLogEvent {
    /// Event name identifying the class of log record.
    pub event_name: String,
    /// Time when the event occurred.
    pub timestamp: AgentTimestampMillis,
    /// Time when the event was observed by the log pipeline.
    pub observed_timestamp: AgentTimestampMillis,
    /// Trace id associated with this event, when present.
    pub trace_id: Option<String>,
    /// Span id associated with this event, when present.
    pub span_id: Option<String>,
    /// W3C trace flags associated with this event, when present.
    pub trace_flags: Option<String>,
    /// OpenTelemetry severity text.
    pub severity_text: String,
    /// OpenTelemetry severity number.
    pub severity_number: u8,
    /// Structured log body represented as JSON-compatible AnyValue.
    pub body: Option<Value>,
    /// Resource attributes for the entity producing telemetry.
    pub resource: AgentAttributes,
    /// Instrumentation scope that emitted the event.
    pub instrumentation_scope: AgentInstrumentationScope,
    /// Log attributes for this specific event occurrence.
    pub attributes: AgentAttributes,
    /// Redaction status for body and artifact references.
    pub redaction: RedactionStatus,
    /// Artifact references associated with the event body.
    pub artifact_refs: Vec<ArtifactRef>,
}

impl AgentLogEvent {
    /// Creates a structured log event with OpenTelemetry severity fields.
    #[must_use]
    pub fn new(
        event_name: impl Into<String>,
        severity: AgentLogSeverity,
        timestamp: AgentTimestampMillis,
        observed_timestamp: AgentTimestampMillis,
    ) -> Self {
        Self {
            event_name: event_name.into(),
            timestamp,
            observed_timestamp,
            trace_id: None,
            span_id: None,
            trace_flags: None,
            severity_text: severity.severity_text().to_string(),
            severity_number: severity.severity_number(),
            body: None,
            resource: AgentAttributes::new(),
            instrumentation_scope: AgentInstrumentationScope::default(),
            attributes: AgentAttributes::new(),
            redaction: RedactionStatus::ReferenceOnly,
            artifact_refs: Vec::new(),
        }
    }

    /// Adds trace correlation from a durable telemetry context.
    pub fn telemetry_context(
        mut self,
        telemetry_context: &AgentTelemetryContext,
    ) -> AgentAuditResult<Self> {
        apply_trace_context(&mut self, telemetry_context)?;
        Ok(self)
    }

    /// Sets a structured log body.
    #[must_use]
    pub fn body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }

    /// Sets resource attributes.
    #[must_use]
    pub fn resource(mut self, resource: AgentAttributes) -> Self {
        self.resource = resource;
        self
    }

    /// Sets instrumentation scope.
    #[must_use]
    pub fn instrumentation_scope(
        mut self,
        instrumentation_scope: AgentInstrumentationScope,
    ) -> Self {
        self.instrumentation_scope = instrumentation_scope;
        self
    }

    /// Sets redaction status.
    #[must_use]
    pub const fn redaction(mut self, redaction: RedactionStatus) -> Self {
        self.redaction = redaction;
        self
    }

    /// Adds an event attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Adds an artifact reference associated with this log event.
    #[must_use]
    pub fn artifact_ref(mut self, artifact_ref: ArtifactRef) -> Self {
        self.artifact_refs.push(artifact_ref);
        self
    }
}

/// Redaction policy hooks for structured logs and durable audit records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentRedactionPolicy {
    /// Whether unredacted structured log bodies are allowed.
    pub allow_unredacted_log_body: bool,
    /// Maximum serialized body size for structured log events.
    pub max_log_body_bytes: usize,
    /// Whether redacted/reference-only audit records must carry an artifact or content hash.
    pub require_audit_reference_or_hash: bool,
}

impl AgentRedactionPolicy {
    /// Creates the default redaction policy.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            allow_unredacted_log_body: false,
            max_log_body_bytes: DEFAULT_AGENT_LOG_BODY_LIMIT_BYTES,
            require_audit_reference_or_hash: true,
        }
    }

    /// Sets whether unredacted structured log bodies are allowed.
    #[must_use]
    pub const fn allow_unredacted_log_body(mut self, allow: bool) -> Self {
        self.allow_unredacted_log_body = allow;
        self
    }

    /// Sets the structured log body byte limit.
    #[must_use]
    pub const fn max_log_body_bytes(mut self, max_log_body_bytes: usize) -> Self {
        self.max_log_body_bytes = max_log_body_bytes;
        self
    }

    /// Sets whether redacted/reference-only audit records require artifact or hash evidence.
    #[must_use]
    pub const fn require_audit_reference_or_hash(mut self, require: bool) -> Self {
        self.require_audit_reference_or_hash = require;
        self
    }
}

impl Default for AgentRedactionPolicy {
    fn default() -> Self {
        Self::new()
    }
}

/// Durable audit write status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAuditWriteStatus {
    /// Event was recorded.
    Recorded,
    /// Event was already present and was not recorded again.
    Duplicate,
}

/// Result metadata returned by durable audit sinks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAuditAcceptance {
    /// Audit event id.
    pub audit_event_id: AgentAuditEventId,
    /// Durable write status.
    pub status: AgentAuditWriteStatus,
}

/// Query used to read audit records independently from telemetry backends.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentAuditQuery {
    /// Optional workflow definition id filter.
    pub workflow_id: Option<AgentWorkflowId>,
    /// Optional run id filter.
    pub run_id: Option<AgentRunId>,
    /// Optional correlation id filter.
    pub correlation_id: Option<AgentCorrelationId>,
    /// Optional audit event kind filter.
    pub kind: Option<AgentAuditEventKind>,
    /// Optional inclusive lower timestamp bound.
    pub occurred_at_or_after: Option<AgentTimestampMillis>,
    /// Optional inclusive upper timestamp bound.
    pub occurred_at_or_before: Option<AgentTimestampMillis>,
    /// Optional maximum number of events to return.
    pub limit: Option<usize>,
}

impl AgentAuditQuery {
    /// Creates an empty audit query.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            workflow_id: None,
            run_id: None,
            correlation_id: None,
            kind: None,
            occurred_at_or_after: None,
            occurred_at_or_before: None,
            limit: None,
        }
    }

    /// Filters by workflow id.
    #[must_use]
    pub fn workflow_id(mut self, workflow_id: AgentWorkflowId) -> Self {
        self.workflow_id = Some(workflow_id);
        self
    }

    /// Filters by run id.
    #[must_use]
    pub fn run_id(mut self, run_id: AgentRunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Filters by correlation id.
    #[must_use]
    pub fn correlation_id(mut self, correlation_id: AgentCorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Filters by audit event kind.
    #[must_use]
    pub const fn kind(mut self, kind: AgentAuditEventKind) -> Self {
        self.kind = Some(kind);
        self
    }

    /// Sets an inclusive lower timestamp bound.
    #[must_use]
    pub const fn occurred_at_or_after(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.occurred_at_or_after = Some(timestamp);
        self
    }

    /// Sets an inclusive upper timestamp bound.
    #[must_use]
    pub const fn occurred_at_or_before(mut self, timestamp: AgentTimestampMillis) -> Self {
        self.occurred_at_or_before = Some(timestamp);
        self
    }

    /// Limits the number of returned events.
    #[must_use]
    pub const fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Durable audit sink abstraction.
pub trait AgentAuditSink {
    /// Records one audit event durably.
    fn record_audit_event<'a>(
        &'a mut self,
        event: AgentAuditEvent,
    ) -> AgentAuditSinkFuture<'a, AgentAuditAcceptance>;

    /// Returns one audit event by id.
    fn get_audit_event<'a>(
        &'a self,
        audit_event_id: AgentAuditEventId,
    ) -> AgentAuditSinkFuture<'a, Option<AgentAuditEvent>>;

    /// Queries audit events without depending on a telemetry backend.
    fn query_audit_events<'a>(
        &'a self,
        query: AgentAuditQuery,
    ) -> AgentAuditSinkFuture<'a, Vec<AgentAuditEvent>>;
}

/// In-memory audit sink for deterministic tests and examples.
#[derive(Debug, Clone)]
pub struct InMemoryAgentAuditSink {
    events: Vec<AgentAuditEvent>,
    redaction_policy: AgentRedactionPolicy,
}

impl InMemoryAgentAuditSink {
    /// Creates an empty in-memory audit sink.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            events: Vec::new(),
            redaction_policy: AgentRedactionPolicy::new(),
        }
    }

    /// Creates an empty in-memory audit sink with a redaction policy.
    #[must_use]
    pub const fn with_redaction_policy(redaction_policy: AgentRedactionPolicy) -> Self {
        Self {
            events: Vec::new(),
            redaction_policy,
        }
    }

    /// Returns all recorded audit events.
    #[must_use]
    pub fn events(&self) -> &[AgentAuditEvent] {
        &self.events
    }
}

impl Default for InMemoryAgentAuditSink {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentAuditSink for InMemoryAgentAuditSink {
    fn record_audit_event<'a>(
        &'a mut self,
        event: AgentAuditEvent,
    ) -> AgentAuditSinkFuture<'a, AgentAuditAcceptance> {
        Box::pin(async move {
            validate_agent_audit_event(&event, self.redaction_policy)?;
            if self
                .events
                .iter()
                .any(|stored| stored.audit_event_id == event.audit_event_id)
            {
                return Ok(AgentAuditAcceptance {
                    audit_event_id: event.audit_event_id,
                    status: AgentAuditWriteStatus::Duplicate,
                });
            }

            let audit_event_id = event.audit_event_id.clone();
            self.events.push(event);
            Ok(AgentAuditAcceptance {
                audit_event_id,
                status: AgentAuditWriteStatus::Recorded,
            })
        })
    }

    fn get_audit_event<'a>(
        &'a self,
        audit_event_id: AgentAuditEventId,
    ) -> AgentAuditSinkFuture<'a, Option<AgentAuditEvent>> {
        Box::pin(async move {
            Ok(self
                .events
                .iter()
                .find(|event| event.audit_event_id == audit_event_id)
                .cloned())
        })
    }

    fn query_audit_events<'a>(
        &'a self,
        query: AgentAuditQuery,
    ) -> AgentAuditSinkFuture<'a, Vec<AgentAuditEvent>> {
        Box::pin(async move {
            let limit = query.limit.unwrap_or(usize::MAX);
            Ok(self
                .events
                .iter()
                .filter(|event| audit_event_matches_query(event, &query))
                .take(limit)
                .cloned()
                .collect())
        })
    }
}

/// Returns the stable lowercase label for an audit event kind.
#[must_use]
pub const fn agent_audit_event_kind_label(kind: AgentAuditEventKind) -> &'static str {
    match kind {
        AgentAuditEventKind::RunCreated => "run-created",
        AgentAuditEventKind::WorkflowDefinitionSelected => "workflow-definition-selected",
        AgentAuditEventKind::InputAccepted => "input-accepted",
        AgentAuditEventKind::ModelRequested => "model-requested",
        AgentAuditEventKind::ModelResponseReceived => "model-response-received",
        AgentAuditEventKind::ToolRequested => "tool-requested",
        AgentAuditEventKind::ToolResponseReceived => "tool-response-received",
        AgentAuditEventKind::ArtifactWritten => "artifact-written",
        AgentAuditEventKind::CheckpointCreated => "checkpoint-created",
        AgentAuditEventKind::HumanDecisionMade => "human-decision-made",
        AgentAuditEventKind::PolicyOverride => "policy-override",
        AgentAuditEventKind::RunCompleted => "run-completed",
        AgentAuditEventKind::RunFailed => "run-failed",
        AgentAuditEventKind::RunCancelled => "run-cancelled",
        AgentAuditEventKind::RetentionDeletion => "retention-deletion",
    }
}

/// Returns the OpenTelemetry event name used for audit-derived log events.
#[must_use]
pub fn agent_audit_log_event_name(kind: AgentAuditEventKind) -> String {
    format!(
        "rakka.agent_workflow.audit.{}",
        agent_audit_event_kind_label(kind)
    )
}

/// How many correlation attributes [`agent_log_event_from_audit_event`] adds of
/// its own: eight always, and up to five more when the audit event carries the
/// optional identities.
///
/// Reserved out of the export bound so an application's own attributes can
/// never crowd out the identities the log record exists to carry.
const AGENT_AUDIT_LOG_CORRELATION_ATTRIBUTES: usize = 13;

/// Builds a structured log event from a durable audit event.
pub fn agent_log_event_from_audit_event(
    audit_event: &AgentAuditEvent,
    observed_timestamp: AgentTimestampMillis,
) -> AgentAuditResult<AgentLogEvent> {
    validate_agent_telemetry_context(&audit_event.telemetry_context)?;
    let severity = agent_log_severity_for_audit_kind(audit_event.kind);
    let mut event = AgentLogEvent::new(
        agent_audit_log_event_name(audit_event.kind),
        severity,
        audit_event.occurred_at,
        observed_timestamp,
    )
    .telemetry_context(&audit_event.telemetry_context)?
    .redaction(audit_event.redaction)
    .body(json!({
        "audit_event_id": audit_event.audit_event_id.as_str(),
        "kind": agent_audit_event_kind_label(audit_event.kind),
        "redaction": audit_event.redaction.as_label(),
    }));

    event.artifact_refs = audit_event.artifact_refs.clone();
    // Bounded on the way out, not refused on the way in. A durable audit event
    // bounds no attribute value and counts no attributes — by design, since
    // 17.1 makes telemetry never a correctness input and an audit write must
    // not fail over what a log record could carry. Copying them verbatim
    // therefore let an event already in the store derive a log record that
    // `validate_agent_log_event` refuses, permanently. What cannot be exported
    // is dropped here instead; the durable audit event keeps all of it.
    //
    // The reserve is the point of the arithmetic: the correlation identities
    // `add_audit_attributes` writes are the ones 17.13 asks a structured log to
    // carry, so they must never be the attributes an over-full application set
    // crowds out.
    event.attributes = bounded_export_attributes(
        audit_event.attributes.clone(),
        AGENT_EXPORT_MAX_ATTRIBUTES.saturating_sub(AGENT_AUDIT_LOG_CORRELATION_ATTRIBUTES),
    );
    add_audit_attributes(&mut event.attributes, audit_event);
    Ok(event)
}

/// Validates a structured log event against trace and redaction policy.
pub fn validate_agent_log_event(
    event: &AgentLogEvent,
    redaction_policy: AgentRedactionPolicy,
) -> AgentAuditResult<()> {
    if is_blank(&event.event_name) {
        return Err(AgentAuditError::InvalidLogEvent {
            field: "event_name",
            reason: "required",
        });
    }
    if is_blank(&event.severity_text) {
        return Err(AgentAuditError::InvalidLogEvent {
            field: "severity_text",
            reason: "required",
        });
    }
    if event.severity_number > AgentLogSeverity::Fatal.severity_number() + 3 {
        return Err(AgentAuditError::InvalidLogEvent {
            field: "severity_number",
            reason: "must be within OpenTelemetry severity range",
        });
    }
    validate_log_trace_fields(event)?;
    validate_log_redaction(event, redaction_policy)?;
    // The log record's attribute set had no guard at all, which made it the
    // one export surface where an unbounded or multi-line value could ride
    // out under a well-formed record. These are generic bounds, not a
    // redaction policy: which keys a log may carry is the emitting domain's
    // decision, made before the record is built.
    crate::otlp::validate_export_attributes("log.attributes", &event.attributes).map_err(|_| {
        AgentAuditError::InvalidLogEvent {
            field: "attributes",
            reason: "must be bounded, single-line, and non-blank keyed",
        }
    })?;
    crate::otlp::validate_export_attributes("log.resource", &event.resource).map_err(|_| {
        AgentAuditError::InvalidLogEvent {
            field: "resource",
            reason: "must be bounded, single-line, and non-blank keyed",
        }
    })?;
    Ok(())
}

/// Validates a durable audit event against trace and redaction policy.
pub fn validate_agent_audit_event(
    event: &AgentAuditEvent,
    redaction_policy: AgentRedactionPolicy,
) -> AgentAuditResult<()> {
    if is_blank(event.audit_event_id.as_str()) {
        return Err(AgentAuditError::InvalidAuditEvent {
            field: "audit_event_id",
            reason: "required",
        });
    }
    if is_blank(event.workflow_id.as_str()) {
        return Err(AgentAuditError::InvalidAuditEvent {
            field: "workflow_id",
            reason: "required",
        });
    }
    if is_blank(event.run_id.as_str()) {
        return Err(AgentAuditError::InvalidAuditEvent {
            field: "run_id",
            reason: "required",
        });
    }
    validate_agent_telemetry_context(&event.telemetry_context)?;
    validate_audit_redaction(event, redaction_policy)?;
    Ok(())
}

fn agent_log_severity_for_audit_kind(kind: AgentAuditEventKind) -> AgentLogSeverity {
    match kind {
        AgentAuditEventKind::RunFailed => AgentLogSeverity::Error,
        AgentAuditEventKind::RunCancelled
        | AgentAuditEventKind::PolicyOverride
        | AgentAuditEventKind::RetentionDeletion => AgentLogSeverity::Warn,
        AgentAuditEventKind::RunCreated
        | AgentAuditEventKind::WorkflowDefinitionSelected
        | AgentAuditEventKind::InputAccepted
        | AgentAuditEventKind::ModelRequested
        | AgentAuditEventKind::ModelResponseReceived
        | AgentAuditEventKind::ToolRequested
        | AgentAuditEventKind::ToolResponseReceived
        | AgentAuditEventKind::ArtifactWritten
        | AgentAuditEventKind::CheckpointCreated
        | AgentAuditEventKind::HumanDecisionMade
        | AgentAuditEventKind::RunCompleted => AgentLogSeverity::Info,
    }
}

fn apply_trace_context(
    event: &mut AgentLogEvent,
    telemetry_context: &AgentTelemetryContext,
) -> AgentAuditResult<()> {
    validate_agent_telemetry_context(telemetry_context)?;
    if let Some(trace_parent) = &telemetry_context.trace_parent {
        let trace_context = AgentTraceContext::from_trace_parent(
            trace_parent,
            telemetry_context.trace_state.as_deref(),
        )?;
        event.trace_id = Some(trace_context.trace_id);
        event.span_id = Some(trace_context.span_id);
        event.trace_flags = Some(trace_context.trace_flags);
    }
    Ok(())
}

fn add_audit_attributes(attributes: &mut AgentAttributes, audit_event: &AgentAuditEvent) {
    attributes.insert(
        AGENT_LOG_ATTR_AUDIT_EVENT_ID.to_string(),
        audit_event.audit_event_id.as_str().to_string(),
    );
    attributes.insert(
        AGENT_LOG_ATTR_AUDIT_KIND.to_string(),
        agent_audit_event_kind_label(audit_event.kind).to_string(),
    );
    attributes.insert(
        AGENT_LOG_ATTR_WORKFLOW_ID.to_string(),
        audit_event.workflow_id.as_str().to_string(),
    );
    attributes.insert(
        AGENT_LOG_ATTR_RUN_ID.to_string(),
        audit_event.run_id.as_str().to_string(),
    );
    attributes.insert(
        AGENT_LOG_ATTR_DEFINITION_VERSION.to_string(),
        audit_event.definition_version.as_str().to_string(),
    );
    attributes.insert(
        AGENT_LOG_ATTR_CAUSATION_ID.to_string(),
        audit_event.causation_id.as_str().to_string(),
    );
    attributes.insert(
        AGENT_LOG_ATTR_CORRELATION_ID.to_string(),
        audit_event.correlation_id.as_str().to_string(),
    );
    attributes.insert(
        AGENT_LOG_ATTR_REDACTION.to_string(),
        audit_event.redaction.as_label().to_string(),
    );

    if let Some(tenant) = &audit_event.tenant {
        attributes.insert(
            AGENT_LOG_ATTR_TENANT_ID.to_string(),
            tenant.as_str().to_string(),
        );
    }
    if let Some(step_id) = &audit_event.step_id {
        attributes.insert(
            AGENT_LOG_ATTR_STEP_ID.to_string(),
            step_id.as_str().to_string(),
        );
    }
    if let Some(effect_id) = &audit_event.effect_id {
        attributes.insert(
            AGENT_LOG_ATTR_EFFECT_ID.to_string(),
            effect_id.as_str().to_string(),
        );
    }
    if let Some(checkpoint_id) = &audit_event.checkpoint_id {
        attributes.insert(
            AGENT_LOG_ATTR_CHECKPOINT_ID.to_string(),
            checkpoint_id.as_str().to_string(),
        );
    }
    if let Some(command_id) = &audit_event.command_id {
        attributes.insert(
            AGENT_LOG_ATTR_COMMAND_ID.to_string(),
            command_id.as_str().to_string(),
        );
    }
}

fn validate_log_trace_fields(event: &AgentLogEvent) -> AgentAuditResult<()> {
    match (&event.trace_id, &event.span_id) {
        (Some(trace_id), Some(span_id)) => {
            let trace_flags = event.trace_flags.as_deref().unwrap_or("00");
            AgentTraceContext::new(
                trace_id.clone(),
                span_id.clone(),
                trace_flags.to_string(),
                None,
            )?;
            Ok(())
        }
        (None, None) => {
            if event.trace_flags.is_some() {
                return Err(AgentAuditError::InvalidLogEvent {
                    field: "trace_flags",
                    reason: "requires trace_id and span_id",
                });
            }
            Ok(())
        }
        (Some(_), None) => Err(AgentAuditError::InvalidLogEvent {
            field: "span_id",
            reason: "required when trace_id is present",
        }),
        (None, Some(_)) => Err(AgentAuditError::InvalidLogEvent {
            field: "trace_id",
            reason: "required when span_id is present",
        }),
    }
}

fn validate_log_redaction(
    event: &AgentLogEvent,
    redaction_policy: AgentRedactionPolicy,
) -> AgentAuditResult<()> {
    if event.redaction == RedactionStatus::Unknown {
        return Err(AgentAuditError::RedactionPolicy {
            field: "redaction",
            reason: "must be explicit",
        });
    }
    if event.redaction == RedactionStatus::Unredacted
        && event.body.is_some()
        && !redaction_policy.allow_unredacted_log_body
    {
        return Err(AgentAuditError::RedactionPolicy {
            field: "body",
            reason: "unredacted log bodies are disabled",
        });
    }
    if let Some(body) = &event.body {
        let body_len = serde_json::to_vec(body)
            .map_err(|error| AgentAuditError::Sink {
                message: error.to_string(),
            })?
            .len();
        if body_len > redaction_policy.max_log_body_bytes {
            return Err(AgentAuditError::RedactionPolicy {
                field: "body",
                reason: "serialized body exceeds configured limit",
            });
        }
    }
    Ok(())
}

fn validate_audit_redaction(
    event: &AgentAuditEvent,
    redaction_policy: AgentRedactionPolicy,
) -> AgentAuditResult<()> {
    if event.redaction == RedactionStatus::Unknown {
        return Err(AgentAuditError::RedactionPolicy {
            field: "redaction",
            reason: "must be explicit",
        });
    }
    if matches!(
        event.redaction,
        RedactionStatus::Redacted | RedactionStatus::ReferenceOnly
    ) && redaction_policy.require_audit_reference_or_hash
        && event.artifact_refs.is_empty()
        && event.content_hashes.is_empty()
    {
        return Err(AgentAuditError::RedactionPolicy {
            field: "artifact_refs",
            reason: "redacted audit records require artifact references or content hashes",
        });
    }
    Ok(())
}

fn audit_event_matches_query(event: &AgentAuditEvent, query: &AgentAuditQuery) -> bool {
    if let Some(workflow_id) = &query.workflow_id {
        if &event.workflow_id != workflow_id {
            return false;
        }
    }
    if let Some(run_id) = &query.run_id {
        if &event.run_id != run_id {
            return false;
        }
    }
    if let Some(correlation_id) = &query.correlation_id {
        if &event.correlation_id != correlation_id {
            return false;
        }
    }
    if let Some(kind) = query.kind {
        if event.kind != kind {
            return false;
        }
    }
    if let Some(from) = query.occurred_at_or_after {
        if event.occurred_at < from {
            return false;
        }
    }
    if let Some(to) = query.occurred_at_or_before {
        if event.occurred_at > to {
            return false;
        }
    }
    true
}

fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}
