//! OpenTelemetry trace context helpers for durable agent workflows.
//!
//! The helpers in this module keep Rakka's durable metadata aligned with W3C
//! Trace Context. They do not install or own an OpenTelemetry SDK; instead they
//! validate and move context across Rakka's inbox, outbox, timer, adapter, and
//! human-checkpoint contracts so applications can attach spans at their chosen
//! telemetry boundary.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{AgentAttributes, AgentSpanLink, AgentTelemetryContext};

/// W3C Trace Context header used to propagate trace id, parent span id, and flags.
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// W3C Trace Context header used to propagate vendor trace state.
pub const TRACESTATE_HEADER: &str = "tracestate";

const SUPPORTED_TRACEPARENT_VERSION: &str = "00";
const TRACE_ID_LEN: usize = 32;
const SPAN_ID_LEN: usize = 16;
const TRACE_FLAGS_LEN: usize = 2;
const TRACEPARENT_PARTS: usize = 4;
const TRACESTATE_MAX_LEN: usize = 512;
const TRACESTATE_MAX_MEMBERS: usize = 32;
const TRACESTATE_KEY_MAX_LEN: usize = 256;
const TRACESTATE_VALUE_MAX_LEN: usize = 256;

#[derive(Debug, Clone, Copy)]
enum TraceIdValidationTarget {
    TraceParent,
    SpanLink,
}

impl TraceIdValidationTarget {
    fn error(self, field: &'static str, reason: &'static str) -> AgentTraceError {
        match self {
            Self::TraceParent => AgentTraceError::InvalidTraceParent { field, reason },
            Self::SpanLink => AgentTraceError::InvalidSpanLink { field, reason },
        }
    }
}

/// Shared result type for agent workflow trace-context helpers.
pub type AgentTraceResult<T> = Result<T, AgentTraceError>;

/// Trace-context validation and propagation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTraceError {
    /// A helper that requires trace context was called without `traceparent`.
    MissingTraceParent,
    /// A `traceparent` value failed validation.
    InvalidTraceParent {
        /// Field that failed validation.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A `tracestate` value failed validation.
    InvalidTraceState {
        /// Stable validation reason.
        reason: &'static str,
    },
    /// A span link failed validation.
    InvalidSpanLink {
        /// Field that failed validation.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
}

impl AgentTraceError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingTraceParent => "missing-traceparent",
            Self::InvalidTraceParent { .. } => "invalid-traceparent",
            Self::InvalidTraceState { .. } => "invalid-tracestate",
            Self::InvalidSpanLink { .. } => "invalid-span-link",
        }
    }
}

impl Display for AgentTraceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingTraceParent => f.write_str("traceparent is required"),
            Self::InvalidTraceParent { field, reason } => {
                write!(f, "invalid traceparent field {field}: {reason}")
            }
            Self::InvalidTraceState { reason } => write!(f, "invalid tracestate: {reason}"),
            Self::InvalidSpanLink { field, reason } => {
                write!(f, "invalid span link field {field}: {reason}")
            }
        }
    }
}

impl Error for AgentTraceError {}

/// Parsed W3C trace context carried by a durable workflow boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTraceContext {
    /// W3C Trace Context version.
    pub version: String,
    /// W3C trace id.
    pub trace_id: String,
    /// W3C parent id field, represented as the current span id for propagation.
    pub span_id: String,
    /// W3C trace flags.
    pub trace_flags: String,
    /// Optional W3C tracestate value.
    pub trace_state: Option<String>,
}

impl AgentTraceContext {
    /// Creates a validated trace context from parsed W3C parts.
    pub fn new(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
        trace_flags: impl Into<String>,
        trace_state: Option<String>,
    ) -> AgentTraceResult<Self> {
        let context = Self {
            version: SUPPORTED_TRACEPARENT_VERSION.to_string(),
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            trace_flags: trace_flags.into(),
            trace_state,
        };
        context.validate()?;
        Ok(context)
    }

    /// Parses and validates a W3C `traceparent` value with optional `tracestate`.
    pub fn from_trace_parent(
        trace_parent: &str,
        trace_state: Option<&str>,
    ) -> AgentTraceResult<Self> {
        let parts = trace_parent.split('-').collect::<Vec<_>>();
        if parts.len() != TRACEPARENT_PARTS {
            return Err(AgentTraceError::InvalidTraceParent {
                field: "traceparent",
                reason: "must contain version, trace id, span id, and flags",
            });
        }

        let trace_state = trace_state.map(validate_trace_state).transpose()?;
        let context = Self {
            version: parts[0].to_string(),
            trace_id: parts[1].to_string(),
            span_id: parts[2].to_string(),
            trace_flags: parts[3].to_string(),
            trace_state,
        };
        context.validate()?;
        Ok(context)
    }

    /// Returns the W3C `traceparent` header value.
    #[must_use]
    pub fn trace_parent(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.version, self.trace_id, self.span_id, self.trace_flags
        )
    }

    /// Returns true when the sampled bit is set in W3C trace flags.
    #[must_use]
    pub fn is_sampled(&self) -> bool {
        u8::from_str_radix(&self.trace_flags, 16)
            .map(|flags| flags & 1 == 1)
            .unwrap_or(false)
    }

    /// Creates the next synchronous propagation context with a new span id.
    pub fn child(&self, child_span_id: impl Into<String>) -> AgentTraceResult<Self> {
        Self::new(
            self.trace_id.clone(),
            child_span_id.into(),
            self.trace_flags.clone(),
            self.trace_state.clone(),
        )
    }

    /// Converts this parsed context to the durable telemetry envelope.
    #[must_use]
    pub fn to_telemetry_context(&self) -> AgentTelemetryContext {
        AgentTelemetryContext {
            trace_parent: Some(self.trace_parent()),
            trace_state: self.trace_state.clone(),
            baggage: AgentAttributes::new(),
            span_links: Vec::new(),
        }
    }

    /// Converts this parsed context to span-link metadata.
    #[must_use]
    pub fn to_span_link(&self, attributes: AgentAttributes) -> AgentSpanLink {
        AgentSpanLink {
            trace_id: self.trace_id.clone(),
            span_id: self.span_id.clone(),
            trace_state: self.trace_state.clone(),
            attributes,
        }
    }

    /// Validates the trace context parts.
    pub fn validate(&self) -> AgentTraceResult<()> {
        validate_trace_parent_version(&self.version)?;
        validate_trace_id(&self.trace_id, TraceIdValidationTarget::TraceParent)?;
        validate_span_id(&self.span_id, TraceIdValidationTarget::TraceParent)?;
        validate_trace_flags(&self.trace_flags)?;
        if let Some(trace_state) = &self.trace_state {
            validate_trace_state(trace_state)?;
        }
        Ok(())
    }
}

/// Parses and validates a W3C trace context from raw header values.
pub fn parse_agent_trace_context(
    trace_parent: &str,
    trace_state: Option<&str>,
) -> AgentTraceResult<AgentTraceContext> {
    AgentTraceContext::from_trace_parent(trace_parent, trace_state)
}

/// Validates the trace context carried by a durable telemetry envelope.
pub fn validate_agent_telemetry_context(context: &AgentTelemetryContext) -> AgentTraceResult<()> {
    if let Some(trace_parent) = &context.trace_parent {
        AgentTraceContext::from_trace_parent(trace_parent, context.trace_state.as_deref())?;
    } else if context.trace_state.is_some() {
        return Err(AgentTraceError::MissingTraceParent);
    }

    for link in &context.span_links {
        validate_agent_span_link(link)?;
    }

    Ok(())
}

/// Injects trace context into a lowercase W3C text-map carrier.
///
/// Existing `traceparent` and `tracestate` carrier keys are removed
/// case-insensitively before the current values are inserted.
pub fn inject_agent_trace_context(
    context: &AgentTelemetryContext,
    carrier: &mut AgentAttributes,
) -> AgentTraceResult<()> {
    remove_case_insensitive(carrier, TRACEPARENT_HEADER);
    remove_case_insensitive(carrier, TRACESTATE_HEADER);

    let Some(trace_parent) = &context.trace_parent else {
        return Ok(());
    };

    let trace_context =
        AgentTraceContext::from_trace_parent(trace_parent, context.trace_state.as_deref())?;
    carrier.insert(TRACEPARENT_HEADER.to_string(), trace_context.trace_parent());
    if let Some(trace_state) = trace_context.trace_state {
        carrier.insert(TRACESTATE_HEADER.to_string(), trace_state);
    }
    Ok(())
}

/// Extracts trace context from a W3C text-map carrier.
///
/// Header keys are matched case-insensitively. Missing `traceparent` returns
/// `Ok(None)` so callers can accept untraced ingress without special casing.
pub fn extract_agent_trace_context(
    carrier: &AgentAttributes,
) -> AgentTraceResult<Option<AgentTelemetryContext>> {
    let Some(trace_parent) = get_case_insensitive(carrier, TRACEPARENT_HEADER) else {
        return Ok(None);
    };
    let trace_state = get_case_insensitive(carrier, TRACESTATE_HEADER);
    let trace_context = AgentTraceContext::from_trace_parent(trace_parent, trace_state)?;
    Ok(Some(trace_context.to_telemetry_context()))
}

/// Creates a synchronous child telemetry context.
///
/// The new context preserves the trace id, flags, tracestate, and baggage, but
/// uses `child_span_id` as the next propagated span id and does not add span
/// links.
pub fn agent_child_telemetry_context(
    context: &AgentTelemetryContext,
    child_span_id: impl Into<String>,
) -> AgentTraceResult<AgentTelemetryContext> {
    let parent = require_agent_trace_context(context)?;
    let mut child = parent.child(child_span_id)?.to_telemetry_context();
    child.baggage = context.baggage.clone();
    Ok(child)
}

/// Creates a durable-resume telemetry context and links back to the parked span.
///
/// Use this for timers, retries, human decisions, callbacks, and recovery
/// paths where elapsed wall-clock time or another process breaks the normal
/// parent-child span relationship.
pub fn agent_durable_resume_telemetry_context(
    context: &AgentTelemetryContext,
    resume_span_id: impl Into<String>,
    link_attributes: AgentAttributes,
) -> AgentTraceResult<AgentTelemetryContext> {
    let parked = require_agent_trace_context(context)?;
    let mut resumed = parked.child(resume_span_id)?.to_telemetry_context();
    resumed.baggage = context.baggage.clone();
    resumed
        .span_links
        .push(parked.to_span_link(link_attributes));
    Ok(resumed)
}

/// Converts the durable telemetry envelope to a parsed trace context.
pub fn require_agent_trace_context(
    context: &AgentTelemetryContext,
) -> AgentTraceResult<AgentTraceContext> {
    let Some(trace_parent) = &context.trace_parent else {
        return Err(AgentTraceError::MissingTraceParent);
    };
    AgentTraceContext::from_trace_parent(trace_parent, context.trace_state.as_deref())
}

/// Validates span-link metadata.
pub fn validate_agent_span_link(link: &AgentSpanLink) -> AgentTraceResult<()> {
    validate_trace_id(&link.trace_id, TraceIdValidationTarget::SpanLink)?;
    validate_span_id(&link.span_id, TraceIdValidationTarget::SpanLink)?;
    if let Some(trace_state) = &link.trace_state {
        validate_trace_state(trace_state)?;
    }
    Ok(())
}

fn validate_trace_parent_version(version: &str) -> AgentTraceResult<()> {
    if version != SUPPORTED_TRACEPARENT_VERSION {
        return Err(AgentTraceError::InvalidTraceParent {
            field: "version",
            reason: "unsupported traceparent version",
        });
    }
    Ok(())
}

fn validate_trace_id(trace_id: &str, target: TraceIdValidationTarget) -> AgentTraceResult<()> {
    if trace_id.len() != TRACE_ID_LEN {
        return Err(target.error("trace_id", "must be 32 lowercase hexadecimal characters"));
    }
    if !is_lower_hex(trace_id) {
        return Err(target.error("trace_id", "must use lowercase hexadecimal characters"));
    }
    if is_all_zero(trace_id) {
        return Err(target.error("trace_id", "must not be all zeros"));
    }
    Ok(())
}

fn validate_span_id(span_id: &str, target: TraceIdValidationTarget) -> AgentTraceResult<()> {
    if span_id.len() != SPAN_ID_LEN {
        return Err(target.error("span_id", "must be 16 lowercase hexadecimal characters"));
    }
    if !is_lower_hex(span_id) {
        return Err(target.error("span_id", "must use lowercase hexadecimal characters"));
    }
    if is_all_zero(span_id) {
        return Err(target.error("span_id", "must not be all zeros"));
    }
    Ok(())
}

fn validate_trace_flags(trace_flags: &str) -> AgentTraceResult<()> {
    if trace_flags.len() != TRACE_FLAGS_LEN {
        return Err(AgentTraceError::InvalidTraceParent {
            field: "trace_flags",
            reason: "must be two lowercase hexadecimal characters",
        });
    }
    if !is_lower_hex(trace_flags) {
        return Err(AgentTraceError::InvalidTraceParent {
            field: "trace_flags",
            reason: "must use lowercase hexadecimal characters",
        });
    }
    Ok(())
}

fn validate_trace_state(trace_state: &str) -> AgentTraceResult<String> {
    if trace_state.is_empty() {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "must not be empty",
        });
    }
    if trace_state.len() > TRACESTATE_MAX_LEN {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "must be at most 512 bytes",
        });
    }
    if !trace_state.is_ascii() || trace_state.bytes().any(|byte| byte < b' ' || byte == 0x7f) {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "must use visible ASCII characters",
        });
    }

    let members = trace_state.split(',').collect::<Vec<_>>();
    if members.len() > TRACESTATE_MAX_MEMBERS {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "must contain at most 32 list members",
        });
    }

    let mut keys = BTreeSet::new();
    for member in members {
        let member = member.trim();
        if member.is_empty() {
            return Err(AgentTraceError::InvalidTraceState {
                reason: "must not contain empty list members",
            });
        }
        let Some((key, value)) = member.split_once('=') else {
            return Err(AgentTraceError::InvalidTraceState {
                reason: "members must contain key=value",
            });
        };
        validate_trace_state_key(key)?;
        validate_trace_state_value(value)?;
        if !keys.insert(key.to_string()) {
            return Err(AgentTraceError::InvalidTraceState {
                reason: "member keys must be unique",
            });
        }
    }

    Ok(trace_state.to_string())
}

fn validate_trace_state_key(key: &str) -> AgentTraceResult<()> {
    if key.is_empty() || key.len() > TRACESTATE_KEY_MAX_LEN {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "member keys must be 1 to 256 bytes",
        });
    }
    if key.bytes().any(|byte| {
        !matches!(
            byte,
            b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'*' | b'/' | b'@'
        )
    }) {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "member keys must use lowercase tracestate characters",
        });
    }
    Ok(())
}

fn validate_trace_state_value(value: &str) -> AgentTraceResult<()> {
    if value.is_empty() || value.len() > TRACESTATE_VALUE_MAX_LEN {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "member values must be 1 to 256 bytes",
        });
    }
    if value.bytes().any(|byte| byte == b',' || byte == b'=') {
        return Err(AgentTraceError::InvalidTraceState {
            reason: "member values must not contain separators",
        });
    }
    Ok(())
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_all_zero(value: &str) -> bool {
    value.bytes().all(|byte| byte == b'0')
}

fn get_case_insensitive<'a>(carrier: &'a AgentAttributes, key: &str) -> Option<&'a str> {
    carrier
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

fn remove_case_insensitive(carrier: &mut AgentAttributes, key: &str) {
    let matching_keys = carrier
        .keys()
        .filter(|candidate| candidate.eq_ignore_ascii_case(key))
        .cloned()
        .collect::<Vec<_>>();
    for matching_key in matching_keys {
        carrier.remove(&matching_key);
    }
}
