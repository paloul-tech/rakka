//! Workflow metric instruments and bounded-label helpers.
//!
//! These helpers keep agent workflow metrics aligned with Rakka's current
//! metrics recorder while preserving OpenTelemetry-friendly names, instrument
//! kinds, and bounded attributes.

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_core::{MetricAttributes, MetricKind, MetricsRecorder};

use crate::{
    adapters::{
        METRIC_AGENT_MODEL_ADAPTER_CALLS, METRIC_AGENT_MODEL_ADAPTER_LATENCY_MS,
        METRIC_AGENT_MODEL_ADAPTER_TOKENS, METRIC_AGENT_TOOL_ADAPTER_CALLS,
        METRIC_AGENT_TOOL_ADAPTER_LATENCY_MS,
    },
    checkpoints::{METRIC_AGENT_HUMAN_CHECKPOINTS, METRIC_AGENT_HUMAN_WAIT_LATENCY_MS},
    dispatcher::{
        METRIC_AGENT_DISPATCHER_BACKLOG, METRIC_AGENT_DISPATCHER_FLEET,
        METRIC_AGENT_DISPATCHER_IN_FLIGHT,
    },
    inbox::METRIC_AGENT_INBOX_COMMANDS,
    outbox::METRIC_AGENT_OUTBOX_EFFECTS,
    timers::METRIC_AGENT_TIMERS,
    BOUNDED_METRIC_FIELDS, FORBIDDEN_HOT_METRIC_FIELDS,
};

/// Counter for durable run state transitions.
pub const METRIC_AGENT_RUN_TRANSITIONS: &str = "rakka.agent_workflow.run.transitions";

/// Counter for workflow step transitions.
pub const METRIC_AGENT_STEP_TRANSITIONS: &str = "rakka.agent_workflow.step.transitions";

/// Counter for durable recovery attempts.
pub const METRIC_AGENT_RECOVERY_EVENTS: &str = "rakka.agent_workflow.recovery.events";

/// Histogram for durable recovery latency in milliseconds.
pub const METRIC_AGENT_RECOVERY_LATENCY_MS: &str = "rakka.agent_workflow.recovery.latency_ms";

/// Gauge for how late timer firing was when observed by a scanner.
pub const METRIC_AGENT_TIMERS_LATE_BY_MS: &str = "rakka.agent_workflow.timers.late_by_ms";

/// Maximum byte length for one hot metric attribute value.
pub const AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES: usize = 96;

/// Workflow metric attribute key for operation names.
pub const AGENT_METRIC_ATTR_OPERATION: &str = "operation";

/// Workflow metric attribute key for command type names.
pub const AGENT_METRIC_ATTR_COMMAND_TYPE: &str = "command_type";

/// Workflow metric attribute key for substrate message type names.
pub const AGENT_METRIC_ATTR_MESSAGE_TYPE: &str = "message_type";

/// Workflow metric attribute key for workflow type names.
pub const AGENT_METRIC_ATTR_WORKFLOW_TYPE: &str = "workflow_type";

/// Workflow metric attribute key for workflow definition versions.
pub const AGENT_METRIC_ATTR_DEFINITION_VERSION: &str = "definition_version";

/// Workflow metric attribute key for lifecycle status labels.
pub const AGENT_METRIC_ATTR_STATUS: &str = "status";

/// Workflow metric attribute key for step kind labels.
pub const AGENT_METRIC_ATTR_STEP_KIND: &str = "step_kind";

/// Workflow metric attribute key for transition labels.
pub const AGENT_METRIC_ATTR_TRANSITION: &str = "transition";

/// Workflow metric attribute key for effect kind labels.
pub const AGENT_METRIC_ATTR_EFFECT_KIND: &str = "effect_kind";

/// Workflow metric attribute key for target class labels.
pub const AGENT_METRIC_ATTR_TARGET_CLASS: &str = "target_class";

/// Workflow metric attribute key for timer status labels.
pub const AGENT_METRIC_ATTR_TIMER_STATUS: &str = "timer_status";

/// Workflow metric attribute key for checkpoint status labels.
pub const AGENT_METRIC_ATTR_CHECKPOINT_STATUS: &str = "checkpoint_status";

/// Workflow metric attribute key for adapter kind labels.
pub const AGENT_METRIC_ATTR_ADAPTER_KIND: &str = "adapter_kind";

/// Workflow metric attribute key for artifact kind labels.
pub const AGENT_METRIC_ATTR_ARTIFACT_KIND: &str = "artifact_kind";

/// Workflow metric attribute key for retry attempt buckets.
pub const AGENT_METRIC_ATTR_RETRY_ATTEMPT_BUCKET: &str = "retry_attempt_bucket";

/// Workflow metric attribute key for outcome labels.
pub const AGENT_METRIC_ATTR_OUTCOME: &str = "outcome";

/// Workflow metric attribute key for bounded detail labels.
pub const AGENT_METRIC_ATTR_DETAIL: &str = "detail";

/// Workflow metric attribute key for stable error codes.
pub const AGENT_METRIC_ATTR_ERROR_CODE: &str = "error_code";

/// Workflow metric attribute key for tenant tier labels.
pub const AGENT_METRIC_ATTR_TENANT_TIER: &str = "tenant_tier";

/// Workflow metric attribute key for redaction status labels.
pub const AGENT_METRIC_ATTR_REDACTION: &str = "redaction";

/// Bounded metric attribute keys accepted by agent workflow helpers.
pub const AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES: &[&str] = &[
    AGENT_METRIC_ATTR_OPERATION,
    AGENT_METRIC_ATTR_COMMAND_TYPE,
    AGENT_METRIC_ATTR_MESSAGE_TYPE,
    AGENT_METRIC_ATTR_WORKFLOW_TYPE,
    AGENT_METRIC_ATTR_DEFINITION_VERSION,
    "state_schema_version",
    AGENT_METRIC_ATTR_STATUS,
    AGENT_METRIC_ATTR_STEP_KIND,
    AGENT_METRIC_ATTR_TRANSITION,
    AGENT_METRIC_ATTR_EFFECT_KIND,
    AGENT_METRIC_ATTR_TARGET_CLASS,
    AGENT_METRIC_ATTR_TIMER_STATUS,
    AGENT_METRIC_ATTR_CHECKPOINT_STATUS,
    AGENT_METRIC_ATTR_ADAPTER_KIND,
    AGENT_METRIC_ATTR_ARTIFACT_KIND,
    AGENT_METRIC_ATTR_RETRY_ATTEMPT_BUCKET,
    AGENT_METRIC_ATTR_OUTCOME,
    AGENT_METRIC_ATTR_DETAIL,
    AGENT_METRIC_ATTR_ERROR_CODE,
    AGENT_METRIC_ATTR_TENANT_TIER,
    AGENT_METRIC_ATTR_REDACTION,
];

const ADDITIONAL_FORBIDDEN_METRIC_ATTRIBUTE_KEYS: &[&str] = &[
    "shard_id",
    "message_id",
    "artifact_id",
    "prompt",
    "completion",
    "tool_output",
    "error_message",
    "stacktrace",
];

/// Workflow metric instrument definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentMetricInstrument {
    /// Stable metric name.
    pub name: &'static str,
    /// Instrument kind.
    pub kind: MetricKind,
    /// UCUM-compatible unit label where possible.
    pub unit: &'static str,
    /// Human-readable instrument description.
    pub description: &'static str,
}

impl AgentMetricInstrument {
    /// Creates a metric instrument definition.
    #[must_use]
    pub const fn new(
        name: &'static str,
        kind: MetricKind,
        unit: &'static str,
        description: &'static str,
    ) -> Self {
        Self {
            name,
            kind,
            unit,
            description,
        }
    }
}

/// Stable agent workflow metric instruments.
pub const AGENT_WORKFLOW_METRIC_INSTRUMENTS: &[AgentMetricInstrument] = &[
    AgentMetricInstrument::new(
        METRIC_AGENT_INBOX_COMMANDS,
        MetricKind::Counter,
        "{command}",
        "Durable agent inbox command acceptance attempts.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_OUTBOX_EFFECTS,
        MetricKind::Counter,
        "{effect}",
        "Durable agent outbox effect scheduling attempts.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_RUN_TRANSITIONS,
        MetricKind::Counter,
        "{transition}",
        "Durable agent run state transitions.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_STEP_TRANSITIONS,
        MetricKind::Counter,
        "{transition}",
        "Workflow step transitions.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_HUMAN_CHECKPOINTS,
        MetricKind::Counter,
        "{checkpoint}",
        "Human checkpoint operations.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_HUMAN_WAIT_LATENCY_MS,
        MetricKind::Histogram,
        "ms",
        "Human checkpoint wait latency.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_MODEL_ADAPTER_CALLS,
        MetricKind::Counter,
        "{call}",
        "Model adapter calls.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_MODEL_ADAPTER_LATENCY_MS,
        MetricKind::Histogram,
        "ms",
        "Model adapter latency.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_MODEL_ADAPTER_TOKENS,
        MetricKind::Histogram,
        "{token}",
        "Model adapter token usage.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_TOOL_ADAPTER_CALLS,
        MetricKind::Counter,
        "{call}",
        "Tool adapter calls.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_TOOL_ADAPTER_LATENCY_MS,
        MetricKind::Histogram,
        "ms",
        "Tool adapter latency.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_TIMERS,
        MetricKind::Counter,
        "{timer}",
        "Durable timer firing attempts.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_TIMERS_LATE_BY_MS,
        MetricKind::Gauge,
        "ms",
        "Observed timer lateness.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_DISPATCHER_FLEET,
        MetricKind::Counter,
        "{dispatch}",
        "Dispatcher fleet operations.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_DISPATCHER_BACKLOG,
        MetricKind::Gauge,
        "{effect}",
        "Dispatcher backlog by target class.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_DISPATCHER_IN_FLIGHT,
        MetricKind::Gauge,
        "{effect}",
        "Dispatcher in-flight work by target class.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_RECOVERY_EVENTS,
        MetricKind::Counter,
        "{recovery}",
        "Durable recovery attempts.",
    ),
    AgentMetricInstrument::new(
        METRIC_AGENT_RECOVERY_LATENCY_MS,
        MetricKind::Histogram,
        "ms",
        "Durable recovery latency.",
    ),
];

/// Errors returned by agent workflow metric helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentMetricError {
    /// Attribute key is forbidden or not part of the bounded key set.
    UnboundedAttributeKey {
        /// Rejected attribute key.
        key: String,
    },
    /// Attribute value is too large for a hot metric label.
    AttributeValueTooLarge {
        /// Attribute key.
        key: String,
        /// Observed byte length.
        value_len: usize,
        /// Configured byte limit.
        limit: usize,
    },
    /// Attribute value contains content that should be kept in logs, traces, or audit.
    UnboundedAttributeValue {
        /// Attribute key.
        key: String,
        /// Stable validation reason.
        reason: &'static str,
    },
}

impl AgentMetricError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnboundedAttributeKey { .. } => "unbounded-metric-attribute-key",
            Self::AttributeValueTooLarge { .. } => "metric-attribute-value-too-large",
            Self::UnboundedAttributeValue { .. } => "unbounded-metric-attribute-value",
        }
    }
}

impl Display for AgentMetricError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnboundedAttributeKey { key } => {
                write!(f, "metric attribute key {key} is not bounded")
            }
            Self::AttributeValueTooLarge {
                key,
                value_len,
                limit,
            } => write!(
                f,
                "metric attribute {key} has {value_len} bytes, exceeding limit {limit}"
            ),
            Self::UnboundedAttributeValue { key, reason } => {
                write!(f, "metric attribute {key} has unbounded value: {reason}")
            }
        }
    }
}

impl Error for AgentMetricError {}

/// Shared result type for agent workflow metric helpers.
pub type AgentMetricResult<T> = Result<T, AgentMetricError>;

/// Returns a metric instrument definition by name.
#[must_use]
pub fn agent_metric_instrument(name: &str) -> Option<&'static AgentMetricInstrument> {
    AGENT_WORKFLOW_METRIC_INSTRUMENTS
        .iter()
        .find(|instrument| instrument.name == name)
}

/// Returns true when an attribute key is accepted for hot metrics.
#[must_use]
pub fn is_bounded_agent_metric_attribute(key: &str) -> bool {
    AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES.contains(&key) || BOUNDED_METRIC_FIELDS.contains(&key)
}

/// Returns true when an attribute key is forbidden for hot metrics.
#[must_use]
pub fn is_forbidden_agent_metric_attribute(key: &str) -> bool {
    FORBIDDEN_HOT_METRIC_FIELDS.contains(&key)
        || ADDITIONAL_FORBIDDEN_METRIC_ATTRIBUTE_KEYS.contains(&key)
}

/// Validates hot metric attributes before recording.
pub fn validate_agent_metric_attributes(attributes: MetricAttributes<'_>) -> AgentMetricResult<()> {
    for (key, value) in attributes {
        if is_forbidden_agent_metric_attribute(key) || !is_bounded_agent_metric_attribute(key) {
            return Err(AgentMetricError::UnboundedAttributeKey {
                key: (*key).to_string(),
            });
        }
        if value.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
            return Err(AgentMetricError::AttributeValueTooLarge {
                key: (*key).to_string(),
                value_len: value.len(),
                limit: AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES,
            });
        }
        if value.contains('\n') || value.contains('\r') {
            return Err(AgentMetricError::UnboundedAttributeValue {
                key: (*key).to_string(),
                reason: "metric label values must be single-line bounded labels",
            });
        }
    }
    Ok(())
}

/// Records a counter after validating attributes.
pub fn record_agent_counter(
    metrics: &dyn MetricsRecorder,
    name: &str,
    value: u64,
    attributes: MetricAttributes<'_>,
) -> AgentMetricResult<()> {
    validate_agent_metric_attributes(attributes)?;
    metrics.increment_counter(name, value, attributes);
    Ok(())
}

/// Records a gauge after validating attributes.
pub fn record_agent_gauge(
    metrics: &dyn MetricsRecorder,
    name: &str,
    value: f64,
    attributes: MetricAttributes<'_>,
) -> AgentMetricResult<()> {
    validate_agent_metric_attributes(attributes)?;
    metrics.record_gauge(name, value, attributes);
    Ok(())
}

/// Records a histogram observation after validating attributes.
pub fn record_agent_histogram(
    metrics: &dyn MetricsRecorder,
    name: &str,
    value: f64,
    attributes: MetricAttributes<'_>,
) -> AgentMetricResult<()> {
    validate_agent_metric_attributes(attributes)?;
    metrics.record_histogram(name, value, attributes);
    Ok(())
}
