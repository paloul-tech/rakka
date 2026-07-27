//! Structured decision events, spans, and metrics.
//!
//! Owns the bounded trace segments with persisted W3C context and links across
//! every durable boundary — no span stays open across a wait — the structured
//! decision events that explain why the runtime did what it did, and the
//! bounded metric set, which never carries an identifier in a label.
//!
//! Content capture is disabled by default. Runtime events are observability and
//! never the correctness source: the durable run, inbox, and outbox state is.
//! An operational answer must therefore stay correct with telemetry entirely
//! unavailable, which is what [`crate::query`] guarantees.
//!
//! Specification: section 17. Filled by slice 1.13, reusing the existing
//! `rakka-agent-workflow` trace-context and OTLP substrate.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use rakka_agent_workflow::{
    validate_agent_span_link, validate_agent_telemetry_context, AgentCausationId,
    AgentCorrelationId, AgentTelemetryContext, AgentTimestampMillis, StateSchemaVersion,
};
use rakka_core::{MetricAttributes, MetricsRecorder};
use serde::{Deserialize, Serialize};

use crate::definition::{AgentEffectSafetyClass, AgentRevisionNumber, AgentToolId};
use crate::identity::{
    AgentGoalId, AgentOperationId, AgentOperationKind, AgentRunScope, AgentTaskId,
};
use crate::loop_runtime::AgentLoopPhase;
use crate::memory::AgentContextSnapshotRef;
use crate::schema::{
    AgentRecordKind, VersionedAgentRecord, CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION,
};

/// Most span links one persisted telemetry context may carry.
///
/// Links accumulate where causality genuinely branches — a regenerated effect
/// links its prior attempt, a resume links the span that parked — and every
/// source of accumulation is already bounded (generations by reconciliation,
/// checkpoints per effect). The cap is a backstop that keeps a durable record
/// bounded even if a caller loops: the *newest* links are kept, because a
/// resume links backwards and the most recent causes are the ones an operator
/// walks first.
pub const AGENT_TELEMETRY_MAX_SPAN_LINKS: usize = 8;

/// Admits a telemetry context to durable state: strict on write, so reads can
/// be permissive.
///
/// Trace context is observability, never correctness
/// ([specification 17.1](../../../docs/plans/rakka-agent/spec.md)), which is
/// why every durable record reads an *absent* context as "nothing recorded"
/// rather than failing closed. That permissiveness is only safe because this
/// gate keeps malformed values out on the way in
/// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)):
///
/// - a `traceparent`/`tracestate` pair that fails W3C validation is dropped
///   whole, never persisted partially;
/// - each span link is validated independently, so one malformed link does not
///   discard the valid causality next to it;
/// - links are capped at [`AGENT_TELEMETRY_MAX_SPAN_LINKS`], keeping the
///   newest; and
/// - baggage is cleared unconditionally: M1 persists no baggage
///   ([specification 17.15](../../../docs/plans/rakka-agent/spec.md); slice
///   1.13 resolution), and externally received baggage is untrusted.
///
/// The function is total — it returns whatever bounded, valid subset the input
/// held, down to the empty context — because a boundary that *refused* a
/// command over its telemetry would make observability a correctness input.
#[must_use]
pub fn sanitize_agent_telemetry_context(context: AgentTelemetryContext) -> AgentTelemetryContext {
    let mut sanitized = AgentTelemetryContext::default();

    let trace_candidate = AgentTelemetryContext {
        trace_parent: context.trace_parent,
        trace_state: context.trace_state,
        ..AgentTelemetryContext::default()
    };
    if validate_agent_telemetry_context(&trace_candidate).is_ok() {
        sanitized.trace_parent = trace_candidate.trace_parent;
        sanitized.trace_state = trace_candidate.trace_state;
    }

    let mut links: Vec<_> = context
        .span_links
        .into_iter()
        .filter(|link| validate_agent_span_link(link).is_ok())
        .collect();
    if links.len() > AGENT_TELEMETRY_MAX_SPAN_LINKS {
        links.drain(..links.len() - AGENT_TELEMETRY_MAX_SPAN_LINKS);
    }
    sanitized.span_links = links;

    sanitized
}

/// Most decision events one run's sink retains before the oldest are evicted.
///
/// Runtime events are projections with bounded retention
/// ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)); durable
/// run state remains the correctness source. A reader whose cursor predates
/// the retained window gets an explicit
/// [`AgentObservabilityError::ReplayWindowExpired`], never a silent gap.
pub const AGENT_DECISION_EVENT_RETENTION: usize = 256;

/// Longest stable reason code a decision event may carry.
pub const AGENT_DECISION_REASON_MAX_LENGTH: usize = 64;

/// What the loop decided
/// ([specification 17.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The M1 loop produces this set; the coordination kinds (`delegate`,
/// `handoff`, `team-operation`, `moderated-turn`) arrive with their phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDecisionKind {
    /// Take another bounded iteration.
    Continue,
    /// Dispatch the tool calls the model selected.
    CallTools,
    /// Propose the result to the owning task.
    SubmitResult,
    /// Park on a durable wait.
    Wait,
    /// The run reached its completed terminal state.
    Complete,
    /// The run reached its failed terminal state.
    Fail,
    /// Park on an approval checkpoint.
    RequestApproval,
    /// Park on a security-authorization checkpoint.
    RequestAuthorization,
    /// Apply a reconciliation decision to an indeterminate effect.
    Reconcile,
}

impl AgentDecisionKind {
    /// Stable kebab-case label for telemetry and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::CallTools => "call-tools",
            Self::SubmitResult => "submit-result",
            Self::Wait => "wait",
            Self::Complete => "complete",
            Self::Fail => "fail",
            Self::RequestApproval => "request-approval",
            Self::RequestAuthorization => "request-authorization",
            Self::Reconcile => "reconcile",
        }
    }
}

impl Display for AgentDecisionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Who decided ([specification 17.7](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentDecisionSource {
    /// The model's output selected the action.
    Model,
    /// The runtime's own deterministic policy selected it.
    DeterministicPolicy,
    /// A human decision selected it.
    Human,
    /// An authenticated authorization service selected it.
    AuthorizationService,
}

impl AgentDecisionSource {
    /// Stable kebab-case label for telemetry and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::DeterministicPolicy => "deterministic-policy",
            Self::Human => "human",
            Self::AuthorizationService => "authorization-service",
        }
    }
}

impl Display for AgentDecisionSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// What a transition asks the loop to record about one decision.
///
/// The loop supplies everything positional — turn, phase, sequence, revisions,
/// context snapshot, telemetry — when it records the draft, so a call site
/// states only what it decided and why.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentDecisionDraft {
    kind: AgentDecisionKind,
    source: AgentDecisionSource,
    slot: String,
    selected_tools: Vec<AgentToolId>,
    safety_class: Option<AgentEffectSafetyClass>,
    reason_code: Option<String>,
}

impl AgentDecisionDraft {
    /// A decision of `kind` by `source`, deduplicated within its turn by
    /// `slot`.
    pub fn new(
        kind: AgentDecisionKind,
        source: AgentDecisionSource,
        slot: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            source,
            slot: slot.into(),
            selected_tools: Vec::new(),
            safety_class: None,
            reason_code: None,
        }
    }

    /// The tool classes the decision selected. Bounded by construction: a
    /// model turn holds at most
    /// [`crate::model::AGENT_MODEL_MAX_TOOL_CALLS`] calls.
    #[must_use]
    pub fn with_selected_tools(mut self, tools: Vec<AgentToolId>) -> Self {
        self.selected_tools = tools;
        self
    }

    /// The safety class of the effect the decision gates or commits.
    #[must_use]
    pub const fn with_safety_class(mut self, class: AgentEffectSafetyClass) -> Self {
        self.safety_class = Some(class);
        self
    }

    /// A stable reason code, truncated at
    /// [`AGENT_DECISION_REASON_MAX_LENGTH`]: a code is a label, and an
    /// unbounded label is how identifiers and messages leak into telemetry.
    #[must_use]
    pub fn with_reason_code(mut self, code: impl Into<String>) -> Self {
        let mut code = code.into();
        code.truncate(AGENT_DECISION_REASON_MAX_LENGTH);
        self.reason_code = Some(code);
        self
    }

    /// Dedup discriminator of this decision within its turn.
    #[must_use]
    pub fn slot(&self) -> &str {
        &self.slot
    }
}

/// One durable agent-loop decision, as a bounded projection record
/// ([specification 17.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The event explains a transition that already committed; it is emitted only
/// after that transition persisted, and duplicate processing resolves to one
/// logical event per decision through the derived operation id
/// ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)). It never
/// carries model text, tool payloads, memory content, hidden reasoning, or a
/// credential — bounded labels, revisions, and references only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentDecisionEvent {
    schema_version: StateSchemaVersion,
    /// The run that decided.
    pub scope: AgentRunScope,
    /// The task the run serves.
    pub task: Option<AgentTaskId>,
    /// The goal the run contributes to, when any.
    pub goal: Option<AgentGoalId>,
    /// Derived identity every replay of this decision resolves to.
    pub operation_id: AgentOperationId,
    /// Per-run monotonic sequence.
    pub sequence: u64,
    /// The turn that decided.
    pub turn: u64,
    /// The loop phase the decision was made in.
    pub phase: AgentLoopPhase,
    /// What was decided.
    pub kind: AgentDecisionKind,
    /// Who decided.
    pub source: AgentDecisionSource,
    /// The tool classes the decision selected.
    pub selected_tools: Vec<AgentToolId>,
    /// The definition revision in force.
    pub definition_revision: AgentRevisionNumber,
    /// The settings revision in force.
    pub settings_revision: AgentRevisionNumber,
    /// The immutable context snapshot the turn was prepared against, when one
    /// exists.
    pub context_snapshot: Option<AgentContextSnapshotRef>,
    /// The safety class of the effect the decision gates or commits.
    pub safety_class: Option<AgentEffectSafetyClass>,
    /// Stable bounded reason code, when the decision carries one.
    pub reason_code: Option<String>,
    /// Causation id: the decision's own derived operation id.
    pub causation_id: AgentCausationId,
    /// Correlation id shared by every record of the run.
    pub correlation_id: AgentCorrelationId,
    /// Trace context of the segment that committed the decision.
    pub telemetry: AgentTelemetryContext,
    /// When the deciding transition committed.
    pub occurred_at: AgentTimestampMillis,
}

impl AgentDecisionEvent {
    /// Assembles the event the loop owes for one recorded draft.
    ///
    /// Everything positional comes from the loop's own durable state; the
    /// operation id is *derived* from the run, the turn, and the draft's slot,
    /// so a re-driven transition resolves to the same event.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn assemble(
        scope: &AgentRunScope,
        task: Option<AgentTaskId>,
        goal: Option<AgentGoalId>,
        sequence: u64,
        turn: u64,
        phase: AgentLoopPhase,
        definition_revision: AgentRevisionNumber,
        settings_revision: AgentRevisionNumber,
        context_snapshot: Option<AgentContextSnapshotRef>,
        telemetry: AgentTelemetryContext,
        draft: AgentDecisionDraft,
        occurred_at: AgentTimestampMillis,
    ) -> Result<Self, crate::identity::AgentIdentityError> {
        let operation_id = AgentOperationId::new(
            AgentOperationKind::Command,
            [
                scope.tenant().as_str(),
                scope.agent().as_str(),
                scope.run().as_str(),
                "decision",
                &turn.to_string(),
                draft.slot(),
            ],
        )?;
        Ok(Self {
            schema_version: CURRENT_AGENT_DECISION_EVENT_SCHEMA_VERSION,
            scope: scope.clone(),
            task,
            goal,
            causation_id: AgentCausationId::new(operation_id.as_str()),
            correlation_id: AgentCorrelationId::new(scope.key()),
            operation_id,
            sequence,
            turn,
            phase,
            kind: draft.kind,
            source: draft.source,
            selected_tools: draft.selected_tools,
            definition_revision,
            settings_revision,
            context_snapshot,
            safety_class: draft.safety_class,
            reason_code: draft.reason_code,
            telemetry,
            occurred_at,
        })
    }
}

impl VersionedAgentRecord for AgentDecisionEvent {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::DecisionEvent;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// Result type of the decision-event sink.
pub type AgentObservabilityResult<T> = Result<T, AgentObservabilityError>;

/// Boxed future of the decision-event sink, matching the crate's store idiom.
pub type AgentObservabilityFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentObservabilityResult<T>> + Send + 'a>>;

/// Whether an append was the first for its operation id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentDecisionWriteStatus {
    /// The event was newly retained.
    Accepted,
    /// The operation id was already retained; nothing was written.
    Duplicate,
}

/// Where decision events go after the transition that decided them committed
/// ([specification 17.13](../../../docs/plans/rakka-agent/spec.md)).
///
/// The sink is a projection, never the correctness source: the run flushes its
/// owed events *after* its own durable transition, retries an unavailable sink
/// on the next settle pass, and never fails a transition over it. Appends
/// deduplicate on the event's derived operation id.
pub trait AgentDecisionEventSink: Send + Sync + 'static {
    /// Stable backend label for diagnostics.
    fn backend_name(&self) -> &'static str;

    /// Appends one event, deduplicating on its operation id.
    fn append<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        event: &'a AgentDecisionEvent,
    ) -> AgentObservabilityFuture<'a, AgentDecisionWriteStatus>;

    /// Reads retained events with sequence strictly greater than `after`, in
    /// sequence order, up to `limit`.
    ///
    /// A cursor that predates the retained window fails with
    /// [`AgentObservabilityError::ReplayWindowExpired`] so a reader resyncs
    /// from authoritative state instead of silently missing events.
    fn read<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        after: u64,
        limit: usize,
    ) -> AgentObservabilityFuture<'a, Vec<AgentDecisionEvent>>;
}

/// Decision-event sink and read errors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentObservabilityError {
    /// The read cursor predates the retained window; resync from durable state.
    ReplayWindowExpired {
        /// Oldest sequence still retained, if any event is.
        oldest_retained: Option<u64>,
    },
    /// The sink rejected or failed the operation.
    Sink {
        /// Stable machine-readable code.
        code: String,
        /// Human-readable detail.
        message: String,
    },
    /// A metric label key is outside both bounded vocabularies, or forbidden.
    UnboundedMetricLabel {
        /// The offending key.
        key: String,
    },
}

impl AgentObservabilityError {
    /// Stable machine-readable error code.
    #[must_use]
    pub fn code(&self) -> &str {
        match self {
            Self::ReplayWindowExpired { .. } => "decision-replay-window-expired",
            Self::Sink { code, .. } => code,
            Self::UnboundedMetricLabel { .. } => "metric-label-unbounded",
        }
    }
}

impl Display for AgentObservabilityError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplayWindowExpired { oldest_retained } => write!(
                f,
                "the decision-event cursor predates the retained window (oldest retained: {oldest_retained:?}); resync from authoritative state"
            ),
            Self::Sink { code, message } => write!(f, "decision-event sink failed ({code}): {message}"),
            Self::UnboundedMetricLabel { key } => write!(
                f,
                "metric label key {key:?} is outside the bounded vocabularies; identifiers and content never label a metric"
            ),
        }
    }
}

impl Error for AgentObservabilityError {}

/// In-memory decision-event sink for tests and examples.
///
/// It keeps the contract a durable sink must keep: appends deduplicate on the
/// operation id, retention is bounded per run at
/// [`AGENT_DECISION_EVENT_RETENTION`] evicting the oldest, and a read from an
/// evicted cursor fails with an explicit expired-window error.
#[derive(Debug, Default)]
pub struct InMemoryAgentDecisionEventSink {
    events: Mutex<BTreeMap<String, Vec<AgentDecisionEvent>>>,
}

impl InMemoryAgentDecisionEventSink {
    /// Creates an empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every retained event for `scope`, in sequence order.
    #[must_use]
    pub fn events(&self, scope: &AgentRunScope) -> Vec<AgentDecisionEvent> {
        self.events
            .lock()
            .expect("the decision sink lock is not poisoned")
            .get(scope.key().as_str())
            .cloned()
            .unwrap_or_default()
    }
}

impl AgentDecisionEventSink for InMemoryAgentDecisionEventSink {
    fn backend_name(&self) -> &'static str {
        "in-memory-decision-events"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        event: &'a AgentDecisionEvent,
    ) -> AgentObservabilityFuture<'a, AgentDecisionWriteStatus> {
        Box::pin(async move {
            let mut events = self
                .events
                .lock()
                .expect("the decision sink lock is not poisoned");
            let retained = events.entry(scope.key().to_string()).or_default();
            if retained
                .iter()
                .any(|held| held.operation_id == event.operation_id)
            {
                return Ok(AgentDecisionWriteStatus::Duplicate);
            }
            retained.push(event.clone());
            retained.sort_by_key(|held| held.sequence);
            if retained.len() > AGENT_DECISION_EVENT_RETENTION {
                let excess = retained.len() - AGENT_DECISION_EVENT_RETENTION;
                retained.drain(..excess);
            }
            Ok(AgentDecisionWriteStatus::Accepted)
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        after: u64,
        limit: usize,
    ) -> AgentObservabilityFuture<'a, Vec<AgentDecisionEvent>> {
        Box::pin(async move {
            let events = self
                .events
                .lock()
                .expect("the decision sink lock is not poisoned");
            let retained = events.get(scope.key().as_str());
            let oldest = retained.and_then(|held| held.first().map(|event| event.sequence));
            if let Some(oldest) = oldest {
                // A cursor of N promises the reader has seen sequence N; if the
                // oldest retained is N+2 or later, something between was evicted.
                if after.saturating_add(1) < oldest {
                    return Err(AgentObservabilityError::ReplayWindowExpired {
                        oldest_retained: Some(oldest),
                    });
                }
            }
            Ok(retained
                .map(|held| {
                    held.iter()
                        .filter(|event| event.sequence > after)
                        .take(limit)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default())
        })
    }
}

/// Counter: agent-loop decisions, labeled by bounded kind and source.
pub const METRIC_AGENT_DECISIONS: &str = "rakka.agent.decisions";

/// Counter: committed loop transitions, labeled by the phase they advanced
/// from.
pub const METRIC_AGENT_RUN_TRANSITIONS: &str = "rakka.agent.run.transitions";

/// Counter: resolved effect generations, labeled by effect kind, safety
/// class, and terminal outcome — `indeterminate` included, which is the
/// alerting signal [specification 17.9](../../../docs/plans/rakka-agent/spec.md)
/// requires.
pub const METRIC_AGENT_EFFECT_OUTCOMES: &str = "rakka.agent.effect.outcomes";

/// Counter: run recoveries, labeled by outcome.
pub const METRIC_AGENT_RECOVERY_EVENTS: &str = "rakka.agent.recovery.events";

/// Gauge: the run's durable count of decision events its bounded ring
/// dropped — the visibility
/// [specification 17.1](../../../docs/plans/rakka-agent/spec.md) requires of
/// telemetry loss.
pub const METRIC_AGENT_DECISION_DROPS: &str = "rakka.agent.decision.drops";

/// Counter: telemetry flush attempts a sink refused, labeled by signal.
pub const METRIC_AGENT_TELEMETRY_FLUSH_FAILURES: &str = "rakka.agent.telemetry.flush.failures";

/// Counter: private-memory retrievals run by snapshot assembly, labeled by
/// bounded backend name and outcome (`retrieved` / `degraded` / `skipped`)
/// ([specification 17.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// Raw query text, returned records, and embeddings are never labels — the
/// snapshot carries the bounded retrieval record; this counts.
pub const METRIC_AGENT_MEMORY_RETRIEVALS: &str = "rakka.agent.memory.retrievals";

/// Counter: memory-ingress guardrail outcomes on retrieved records, labeled
/// by bounded outcome (`blocked` / `transformed` / `reported` /
/// `checkpoint-refused` / `rejected`)
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md),
/// [17.10](../../../docs/plans/rakka-agent/spec.md)).
///
/// Blocked and checkpoint-refused records are deliberately *not* recorded on
/// the snapshot — absence is the decision, and a reason code for absent
/// content would leak what was blocked into model-adjacent data — so this
/// counter is their bounded visibility.
pub const METRIC_AGENT_MEMORY_INGRESS_OUTCOMES: &str = "rakka.agent.memory.ingress.outcomes";

/// Label keys the agent domain adds to the substrate's bounded vocabulary.
///
/// The metric-vocabulary boundary is by layer (slice 1.13 resolution): the
/// substrate keeps measuring the substrate under `rakka.agent_workflow.*`
/// with its own bounded keys, and the agent domain measures its durable
/// transitions under `rakka.agent.*` with these. Every value recorded under
/// them comes from a closed `as_label()` vocabulary, never an identifier.
pub const AGENT_METRIC_FIELDS: &[&str] = &[
    "backend",
    "decision_kind",
    "decision_source",
    "phase",
    "safety_class",
    "signal",
];

/// Accepts a label set for an agent-domain metric, or fails closed.
///
/// The substrate's forbidden-field guard is reused unchanged — an identifier
/// or content field is rejected no matter what — and a key must belong to
/// either the substrate's bounded vocabulary or [`AGENT_METRIC_FIELDS`]
/// ([specification 17.12](../../../docs/plans/rakka-agent/spec.md)).
pub fn validate_agent_domain_metric_attributes(
    attributes: MetricAttributes<'_>,
) -> AgentObservabilityResult<()> {
    for (key, value) in attributes {
        if rakka_agent_workflow::is_forbidden_agent_metric_attribute(key)
            || (!rakka_agent_workflow::is_bounded_agent_metric_attribute(key)
                && !AGENT_METRIC_FIELDS.contains(key))
        {
            return Err(AgentObservabilityError::UnboundedMetricLabel {
                key: (*key).to_string(),
            });
        }
        // The substrate's *value* guard (length bound, single-line) applies to
        // every hot metric regardless of which layer owns the key — a bounded
        // key must not smuggle an unbounded or multi-line value — so it is
        // reused here rather than left to the substrate's key guard alone.
        if rakka_agent_workflow::validate_agent_metric_attribute_value(key, value).is_err() {
            return Err(AgentObservabilityError::UnboundedMetricLabel {
                key: (*key).to_string(),
            });
        }
    }
    Ok(())
}

/// Records an agent-domain counter after validating its labels.
///
/// A validation failure is returned, never recorded — and a call site ignores
/// it rather than failing the run, because metrics are telemetry and telemetry
/// is never a correctness input
/// ([specification 17.1](../../../docs/plans/rakka-agent/spec.md)). The real
/// guard is the unit test that walks every instrument's label keys through
/// the validator.
pub fn record_agent_domain_counter(
    metrics: &dyn MetricsRecorder,
    name: &str,
    value: u64,
    attributes: MetricAttributes<'_>,
) -> AgentObservabilityResult<()> {
    validate_agent_domain_metric_attributes(attributes)?;
    metrics.increment_counter(name, value, attributes);
    Ok(())
}

/// Records an agent-domain gauge after validating its labels.
pub fn record_agent_domain_gauge(
    metrics: &dyn MetricsRecorder,
    name: &str,
    value: f64,
    attributes: MetricAttributes<'_>,
) -> AgentObservabilityResult<()> {
    validate_agent_domain_metric_attributes(attributes)?;
    metrics.record_gauge(name, value, attributes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use rakka_agent_workflow::{AgentAttributes, AgentSpanLink};

    use super::*;

    const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn link(span_id: &str) -> AgentSpanLink {
        AgentSpanLink {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: span_id.to_string(),
            trace_state: None,
            attributes: AgentAttributes::new(),
        }
    }

    #[test]
    fn a_valid_context_survives_with_its_baggage_cleared() {
        let mut context = AgentTelemetryContext {
            trace_parent: Some(TRACE_PARENT.to_string()),
            trace_state: Some("vendor=value".to_string()),
            ..AgentTelemetryContext::default()
        };
        context
            .baggage
            .insert("tenant".to_string(), "acme".to_string());
        context.span_links.push(link("00f067aa0ba902b7"));

        let sanitized = sanitize_agent_telemetry_context(context);

        assert_eq!(sanitized.trace_parent.as_deref(), Some(TRACE_PARENT));
        assert_eq!(sanitized.trace_state.as_deref(), Some("vendor=value"));
        assert_eq!(sanitized.span_links.len(), 1);
        assert!(sanitized.baggage.is_empty(), "M1 persists no baggage");
    }

    #[test]
    fn a_malformed_trace_parent_is_dropped_whole_without_touching_valid_links() {
        let context = AgentTelemetryContext {
            trace_parent: Some("not-a-traceparent".to_string()),
            trace_state: Some("vendor=value".to_string()),
            span_links: vec![link("00f067aa0ba902b7")],
            ..AgentTelemetryContext::default()
        };

        let sanitized = sanitize_agent_telemetry_context(context);

        assert!(sanitized.trace_parent.is_none());
        assert!(
            sanitized.trace_state.is_none(),
            "tracestate must not outlive the traceparent it rode with"
        );
        assert_eq!(sanitized.span_links.len(), 1);
    }

    #[test]
    fn a_malformed_link_is_filtered_while_the_rest_survive() {
        let mut bad = link("00f067aa0ba902b7");
        bad.span_id = "short".to_string();
        let context = AgentTelemetryContext {
            span_links: vec![bad, link("00f067aa0ba902b8")],
            ..AgentTelemetryContext::default()
        };

        let sanitized = sanitize_agent_telemetry_context(context);

        assert_eq!(sanitized.span_links.len(), 1);
        assert_eq!(sanitized.span_links[0].span_id, "00f067aa0ba902b8");
    }

    #[test]
    fn links_are_capped_keeping_the_newest() {
        let links: Vec<_> = (0..AGENT_TELEMETRY_MAX_SPAN_LINKS + 3)
            .map(|index| link(&format!("00f067aa0ba9{index:04x}")))
            .collect();
        let newest = links.last().expect("links are non-empty").clone();
        let context = AgentTelemetryContext {
            span_links: links,
            ..AgentTelemetryContext::default()
        };

        let sanitized = sanitize_agent_telemetry_context(context);

        assert_eq!(sanitized.span_links.len(), AGENT_TELEMETRY_MAX_SPAN_LINKS);
        assert_eq!(
            sanitized.span_links.last(),
            Some(&newest),
            "the newest links are the ones a resume walks first"
        );
    }

    #[test]
    fn the_empty_context_is_a_fixed_point() {
        assert_eq!(
            sanitize_agent_telemetry_context(AgentTelemetryContext::default()),
            AgentTelemetryContext::default()
        );
    }

    #[test]
    fn every_instrument_label_key_passes_the_bounded_guard() {
        // The closed set of label keys every rakka.agent.* instrument records.
        // A key added to an instrument must be added here, and it must pass.
        let recorded: &[&str] = &[
            "decision_kind",
            "decision_source",
            "phase",
            "effect_kind",
            "safety_class",
            "outcome",
            "signal",
        ];
        for key in recorded {
            assert!(
                validate_agent_domain_metric_attributes(&[(key, "value")]).is_ok(),
                "{key} must be accepted"
            );
        }
    }

    #[test]
    fn an_identifier_or_unknown_key_is_rejected() {
        for key in ["run_id", "effect_id", "prompt_text", "tool_arguments"] {
            let error = validate_agent_domain_metric_attributes(&[(key, "value")])
                .expect_err("a forbidden key must be rejected");
            assert_eq!(error.code(), "metric-label-unbounded");
        }
        let error = validate_agent_domain_metric_attributes(&[("free_form", "value")])
            .expect_err("an unknown key must be rejected");
        assert_eq!(error.code(), "metric-label-unbounded");
    }

    #[test]
    fn a_bounded_key_carrying_an_unbounded_or_multiline_value_is_rejected() {
        // A bounded key does not license an unbounded value: the value guard
        // applies no matter which layer owns the key.
        let oversized = "x".repeat(200);
        let error = validate_agent_domain_metric_attributes(&[("phase", oversized.as_str())])
            .expect_err("an oversized value must be rejected under a bounded key");
        assert_eq!(error.code(), "metric-label-unbounded");

        let error = validate_agent_domain_metric_attributes(&[("phase", "one\ntwo")])
            .expect_err("a multi-line value must be rejected under a bounded key");
        assert_eq!(error.code(), "metric-label-unbounded");

        // The same key with a short single-line value is accepted.
        validate_agent_domain_metric_attributes(&[("phase", "deciding-continuation")])
            .expect("a bounded key with a bounded value is accepted");
    }

    #[test]
    fn a_rejected_label_set_records_nothing() {
        let recorder = rakka_core::InMemoryMetricsRecorder::new();
        let result =
            record_agent_domain_counter(&recorder, METRIC_AGENT_DECISIONS, 1, &[("run_id", "r-1")]);
        assert!(result.is_err());
        assert!(recorder.snapshot().observations().is_empty());
    }
}
