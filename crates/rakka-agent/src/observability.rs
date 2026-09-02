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

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{
    validate_agent_span_link, validate_agent_telemetry_context, AgentAttributes, AgentCausationId,
    AgentCorrelationId, AgentTelemetryContext, AgentTimestampMillis, StateSchemaVersion,
};
use rakka_core::{MetricAttributes, MetricKind, MetricsRecorder, OpenTelemetryInstrumentView};
use serde::{Deserialize, Serialize};

use crate::definition::{AgentEffectSafetyClass, AgentRevisionNumber, AgentToolId};
use crate::identity::{
    AgentGoalId, AgentOperationId, AgentOperationKind, AgentRunScope, AgentTaskId, AgentTaskScope,
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

    // A link's *attributes* are bounded here too, not only its ids. The export
    // record copies the persisted links verbatim, and one over-long or
    // multi-line link attribute makes every span closed under this context
    // unexportable for the life of the run — a durable write poisoning a
    // telemetry read, which is the inversion 17.1 forbids.
    let mut links: Vec<_> = context
        .span_links
        .into_iter()
        .filter(|link| validate_agent_span_link(link).is_ok())
        .map(|mut link| {
            link.attributes = rakka_agent_workflow::bounded_export_attributes(
                link.attributes,
                rakka_agent_workflow::AGENT_EXPORT_MAX_ATTRIBUTES,
            );
            link
        })
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
    /// Commit a goal-evaluation effect.
    Evaluate,
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
            Self::Evaluate => "evaluate",
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

/// One bounded page of retained decision events.
///
/// `has_more` is the sink's own explicit answer, never inferred from the page
/// size: the read contract only promises *up to* `limit` events, so a page
/// shorter than the limit proves nothing about what the sink still retains —
/// a reader that guessed from the length would silently stop short of the
/// retained tail.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct AgentDecisionEventPage {
    /// The events, contiguous from the cursor, oldest first.
    pub events: Vec<AgentDecisionEvent>,
    /// Whether the sink retains more events past this page — cut by the
    /// limit, or by a hole in the stream the next read will be refused at.
    pub has_more: bool,
}

impl AgentDecisionEventPage {
    /// A page holding `events`, with `has_more` as the sink's explicit answer.
    #[must_use]
    pub const fn new(events: Vec<AgentDecisionEvent>, has_more: bool) -> Self {
        Self { events, has_more }
    }
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
    /// sequence order, up to `limit`, reporting explicitly whether more are
    /// retained past the page.
    ///
    /// The page MUST be contiguous from `after + 1`. The run's outbox drops
    /// its oldest unflushed event after that event consumed a sequence, so a
    /// hole can sit anywhere in the stream; an implementation MUST stop the
    /// page *before* such a hole — reporting `has_more` so the reader comes
    /// back — rather than page across it or discard the deliverable prefix,
    /// whose depth must never depend on the reader's page size.
    ///
    /// A read is refused with
    /// [`AgentObservabilityError::ReplayWindowExpired`] — naming the first
    /// retained sequence past the hole as the floor to resume from — exactly
    /// when nothing contiguous can be delivered: the first retained event
    /// past `after` is not `after + 1` (the hole sits at the read head), or
    /// `after` lies beyond the newest retained sequence, a cursor this log
    /// never issued that an empty page would silently vouch for. Either way
    /// the reader resyncs from authoritative state instead of silently
    /// missing events.
    fn read<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        after: u64,
        limit: usize,
    ) -> AgentObservabilityFuture<'a, AgentDecisionEventPage>;
}

/// Decision-event sink and read errors.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentObservabilityError {
    /// The read cursor predates the retained window; resync from durable state.
    ReplayWindowExpired {
        /// The floor to resume from: the oldest sequence still retained at or
        /// past the reader's position, when anything is retained there.
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
    ) -> AgentObservabilityFuture<'a, AgentDecisionEventPage> {
        Box::pin(async move {
            let events = self
                .events
                .lock()
                .expect("the decision sink lock is not poisoned");
            let held = events
                .get(scope.key().as_str())
                .map(Vec::as_slice)
                .unwrap_or_default();
            // A cursor past the newest retained sequence is one this log never
            // issued: an empty page would stamp "you are current" over
            // sequences the reader has not seen, and once the log grows past
            // the cursor the reader would resume across them silently.
            let newest = held.last().map_or(0, |event| event.sequence);
            if after > newest {
                return Err(AgentObservabilityError::ReplayWindowExpired {
                    oldest_retained: held.first().map(|event| event.sequence),
                });
            }
            // The outbox is a ring that drops its oldest *unflushed* event
            // after that event already consumed a sequence, so a hole can sit
            // anywhere in the stream. The page is the contiguous prefix from
            // the cursor: a hole at the read head is refused with the floor
            // past it, and a hole further in truncates the page with
            // `has_more` — the reader's next read starts at the hole and gets
            // the refusal — so every retained event is deliverable whatever
            // the reader's page size.
            let mut page: Vec<AgentDecisionEvent> = Vec::new();
            let mut has_more = false;
            let mut expected = after.saturating_add(1);
            for event in held.iter().filter(|event| event.sequence > after) {
                if event.sequence != expected {
                    if page.is_empty() {
                        return Err(AgentObservabilityError::ReplayWindowExpired {
                            oldest_retained: Some(event.sequence),
                        });
                    }
                    has_more = true;
                    break;
                }
                if page.len() == limit {
                    has_more = true;
                    break;
                }
                page.push(event.clone());
                expected = event.sequence.saturating_add(1);
            }
            Ok(AgentDecisionEventPage::new(page, has_more))
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

/// Histogram, milliseconds: how long a committed effect stayed outstanding,
/// from its durable acceptance to its durable result, labeled by effect kind
/// and terminal outcome
/// ([specification 17.9](../../../docs/plans/rakka-agent/spec.md),
/// [17.12](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is the latency the *run* observed: queue wait, dispatch, and the
/// external call together, measured between the effect's own durable
/// timestamps, so the value is the same whichever worker resolved it and
/// whatever clock this process happens to hold. `effect_kind` is what
/// separates a model call from a tool call from an A2A send.
///
/// It is deliberately one instrument rather than a queue/dispatch pair. The
/// effect's `dispatched_at` is stamped when the run hands the effect to the
/// outbox, not when a worker begins an attempt on it, so a pair split there
/// would report the run's own hand-off latency under a name promising queue
/// delay. The dispatcher measures its own attempt separately, from the clock
/// it actually holds.
pub const METRIC_AGENT_EFFECT_OUTSTANDING_DURATION: &str =
    "rakka.agent.effect.outstanding.duration";

/// Histogram, milliseconds: one bounded active turn, from the durable
/// acceptance of the turn's model effect to the transition that folded the
/// turn into the session, labeled by outcome
/// ([specification 17.12](../../../docs/plans/rakka-agent/spec.md)'s active
/// turn duration).
///
/// A turn spans a durable wait — the run is not resident while its model
/// effect is outstanding — so this is deliberately measured between two
/// persisted timestamps rather than across a live segment, which is also why
/// it survives the passivation, recovery, and shard movement the turn may
/// cross.
pub const METRIC_AGENT_TURN_DURATION: &str = "rakka.agent.turn.duration";

/// Histogram, tokens: provider-reported token usage per recorded turn, labeled
/// by `direction` (`input` / `output`)
/// ([specification 17.8](../../../docs/plans/rakka-agent/spec.md),
/// [17.12](../../../docs/plans/rakka-agent/spec.md)).
///
/// Only what the provider actually reported is recorded, **per direction**: a
/// direction reporting zero records nothing rather than a zero, because a zero
/// is a claim about the provider that Rakka has no evidence for.
/// [`crate::model::AgentModelUsage`] carries plain counts and documents an
/// unreported dimension as zero, so at this boundary "the provider said
/// nothing" and "the provider said none" are the same value and neither is
/// evidence. The cost is stated rather than hidden: a turn that genuinely
/// emitted zero completion tokens — a refusal with an empty completion —
/// contributes no `output` sample. Distinguishing the two would take an
/// adapter contract that carries the difference, which the provider adapter in
/// this crate does not have to give.
///
/// Cost is deliberately absent: it is a tenant-specific figure and never a
/// metric label or value here.
pub const METRIC_AGENT_MODEL_TOKENS: &str = "rakka.agent.model.tokens";

/// Counter: run recoveries, labeled by outcome.
pub const METRIC_AGENT_RECOVERY_EVENTS: &str = "rakka.agent.recovery.events";

/// Histogram, milliseconds: how long one run recovery took, labeled by outcome
/// ([specification 17.11](../../../docs/plans/rakka-agent/spec.md),
/// [17.12](../../../docs/plans/rakka-agent/spec.md)).
///
/// This one is genuinely in-process — a durable load, in this process, now —
/// so it is measured by the recovery segment's own monotonic width rather than
/// between persisted timestamps, and it is the cold-activation latency an
/// operator watches after a shard moves.
pub const METRIC_AGENT_RECOVERY_DURATION: &str = "rakka.agent.recovery.duration";

/// Gauge: the run's durable count of decision events its bounded ring
/// dropped — the visibility
/// [specification 17.1](../../../docs/plans/rakka-agent/spec.md) requires of
/// telemetry loss.
pub const METRIC_AGENT_DECISION_DROPS: &str = "rakka.agent.decision.drops";

/// Counter: telemetry flush attempts a sink refused, labeled by signal.
pub const METRIC_AGENT_TELEMETRY_FLUSH_FAILURES: &str = "rakka.agent.telemetry.flush.failures";

/// Gauge: records a bounded telemetry sink is holding, awaiting export.
///
/// The export-path half of the loss visibility
/// [17.12](../../../docs/plans/rakka-agent/spec.md) asks for. A sink that must
/// drop rather than block ([`AgentSegmentSink`]) makes queue depth the leading
/// indicator and the drop counter the lagging one, so both are published, and
/// both are labeled by the sink's own [`AgentSegmentSink::backend_name`] so a
/// deployment running more than one can tell them apart.
pub const METRIC_AGENT_TELEMETRY_EXPORT_QUEUE: &str = "rakka.agent.telemetry.export.queue";

/// Counter: records a bounded telemetry sink dropped at capacity.
///
/// Distinct from [`METRIC_AGENT_TELEMETRY_EXPORT_UNMAPPABLE`] on purpose: this
/// one says the pipeline is behind, that one says a record could never have
/// been sent. An operator's response to them is not the same.
pub const METRIC_AGENT_TELEMETRY_EXPORT_DROPS: &str = "rakka.agent.telemetry.export.drops";

/// Counter: records a telemetry sink refused because they could not be mapped.
///
/// A record that fails its convention mapping or its export validation never
/// enters the buffer, so it is neither queued nor dropped — and without its
/// own counter it would leave no trace at all.
pub const METRIC_AGENT_TELEMETRY_EXPORT_UNMAPPABLE: &str =
    "rakka.agent.telemetry.export.unmappable";

/// The `signal` label values **this crate** writes onto
/// [`METRIC_AGENT_TELEMETRY_FLUSH_FAILURES`] and the export instruments above.
///
/// Bounded and enumerated because 17.12 requires labels to be bounded *and*
/// documented, and because a free-form signal name is how a bounded label
/// becomes an unbounded one. `decision-events` is the durable decision sink's
/// refusal; `spans` is a bounded segment sink's buffer.
///
/// Deliberately **not** a union with the application boundary's signals. The
/// OTLP exporter lives in the deploying binary
/// ([17.17](../../../docs/plans/rakka-agent/spec.md)), so its per-signal
/// values are its own to declare and its own to hold to a bijection — the same
/// separation `rakka_a2a::agents::A2A_INGRESS_ERROR_TYPE` keeps from
/// [`AGENT_SEGMENT_ERROR_TYPES`], and for the same reason: a vocabulary this
/// crate lists but never writes is a promise nothing keeps.
pub const AGENT_TELEMETRY_SIGNALS: &[&str] = &["decision-events", "spans"];

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

/// Counter: wake dispositions the continuous controller recorded, labeled by
/// bounded disposition outcome
/// ([specification 17.12](../../../docs/plans/rakka-agent/spec.md)'s
/// accepted/duplicate/stale/coalesced/missed/late family). Emitted only
/// after the durable transition committed, never for a replayed delivery;
/// the controller's durable counters remain the exact record.
pub const METRIC_AGENT_WAKE_DISPOSITIONS: &str = "rakka.agent.wake.dispositions";

/// Counter: epoch admissions and results, labeled by bounded outcome
/// (`admitted` / `completed` / `failed` / `cancelled`).
pub const METRIC_AGENT_EPOCHS: &str = "rakka.agent.epochs";

/// Counter: continuous-goal lifecycle transitions, labeled by the bounded
/// transition (`suspended` / `resumed` / `renewed` / `expired` / `retired`).
///
/// Status-changing transitions are counted as the difference of the goal's
/// lifecycle status across the committed transition, so observed flips —
/// expiry, retirement by policy, escalation into suspension — count exactly
/// like the commanded ones, from whatever transition first recorded them.
/// `renewed` is the one transition that leaves the status unchanged and is
/// counted from its command.
pub const METRIC_AGENT_GOAL_LIFECYCLE: &str = "rakka.agent.goal.lifecycle";

/// Counter: goal-contract status transitions
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)), labeled by
/// the status arrived at.
///
/// This is the *contract* status of `AgentGoalStatus` —
/// proposed/active/waiting/satisfied/unsatisfied/failed/cancelled/expired —
/// deliberately distinct from [`METRIC_AGENT_GOAL_LIFECYCLE`], which counts
/// the continuous admission gate. Transitions are counted as the difference of
/// the goal record's status across the committed transition, so projected and
/// policy-driven moves count exactly like commanded ones.
pub const METRIC_AGENT_GOAL_STATUS: &str = "rakka.agent.goal.status";

/// Counter: stagnation-threshold trips the wake controller detected, labeled
/// by bounded trigger (`repeated-result` / `no-progress`)
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Counted as the difference of the controller's durable stagnation counters
/// across the committed transition — the admitted-epoch idiom — so a replayed
/// settlement emits nothing. A `Continue` action produces no status flip, and
/// this counter is its only metric visibility.
pub const METRIC_AGENT_GOAL_STAGNATION: &str = "rakka.agent.goal.stagnation";

/// Counter: delegated children's terminal results accepted or refused at the
/// parent run's door, labeled by bounded `outcome`
/// ([specification 8.4](../../../docs/plans/rakka-agent/spec.md)).
pub const METRIC_AGENT_DELEGATION_RESULTS: &str = "rakka.agent.delegation.results";

/// Counter: handoff resolutions accepted or refused at the source run's door,
/// labeled by bounded `outcome`
/// ([specification 8.9](../../../docs/plans/rakka-agent/spec.md)).
pub const METRIC_AGENT_HANDOFF_RESULTS: &str = "rakka.agent.handoff.results";

/// Counter: fan-in groups resolved, labeled by the bounded resolution code
/// (`all-settled` / `any-satisfied` / `quorum-satisfied` / `unsatisfiable` /
/// `timed-out`) as `outcome`
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
pub const METRIC_AGENT_FAN_IN_RESOLUTIONS: &str = "rakka.agent.fan_in.resolutions";

/// Counter: child workflow runs' terminal results accepted at the parent
/// run's door, labeled by bounded `outcome`
/// ([specification 8.6](../../../docs/plans/rakka-agent/spec.md)). A refused
/// delivery is a non-committing error and is not counted here.
pub const METRIC_AGENT_WORKFLOW_RESULTS: &str = "rakka.agent.workflow.results";

/// Counter: team board and lifecycle operations committed at the team
/// entity's door, labeled by bounded `operation` (the command's closed
/// label, plus the out-of-band `expire` for the lazy expiry flip and
/// `close` for a task's terminal notice closing its board entry) and
/// `outcome` (`applied` / `refused` / `activated` / `reopened`)
/// ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)). A
/// duplicate command answered from the operation log records nothing, so a
/// replay never double-counts.
pub const METRIC_AGENT_TEAM_OPERATIONS: &str = "rakka.agent.team.operations";

/// Counter: moderated-conversation turn and lifecycle operations committed at
/// the conversation entity's door, labeled by bounded `operation` (the
/// command's closed label) and `outcome` (`applied` / `refused`)
/// ([specification 8.11](../../../docs/plans/rakka-agent/spec.md)). A
/// duplicate command answered from the operation log — including a
/// past-window replay echoed from the turn ledger — records nothing, so a
/// replay never double-counts. The bounded `mode` label is owed to the slice
/// that lands the model-visible moderation tool.
pub const METRIC_AGENT_MODERATION_TURNS: &str = "rakka.agent.moderation.turns";

/// Counter: authenticated human-result submissions decided at the task
/// entity's door, labeled by bounded `outcome`
/// (`accepted` / `rejected` / `exhausted`)
/// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)). Counted
/// as the difference of the task's durable result cells across the committed
/// transition — the admitted-epoch idiom — so duplicates, durable echoes,
/// and non-committing refusals record nothing.
pub const METRIC_AGENT_HUMAN_RESULTS: &str = "rakka.agent.human.results";

/// Counter: dependency outcomes durably applied at the dependent task's
/// door, labeled by bounded `outcome` (`completed` / `failed` / `cancelled`)
/// ([specification 9.2](../../../docs/plans/rakka-agent/spec.md)). Counted
/// as the difference of the task's resolved-edge count across the committed
/// transition, whichever path — registry exchange or application relay —
/// resolved the edge; a replayed or conflicting delivery records nothing.
pub const METRIC_AGENT_DEPENDENCY_OUTCOMES: &str = "rakka.agent.dependency.outcomes";

/// Counter: exchange replies a settle pass could not settle, labeled by
/// bounded `operation` (the [`crate::choreography::AgentExchangeKind`] label)
/// and `error_code` (the receiver's stable refusal code)
/// ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
///
/// The receiver *answered*; its answer is one no re-drive can settle until a
/// different receiver answers — an upstream that gets created, an owner
/// upgraded past the kind. The courier deliberately does not fail the pass
/// over it, since one unanswerable envelope must not wedge every other
/// exchange the entity owes, so this counter is what keeps a durably wedged
/// entity distinguishable from a healthy one. It is emitted per pass, not per
/// durable transition: a standing wedge counts on every sweep, which is what
/// makes it alertable as a rate. The exchange that is stuck, and since when,
/// are on the journal's own pending record.
pub const METRIC_AGENT_EXCHANGE_UNSETTLEABLE: &str = "rakka.agent.exchange.unsettleable";

/// Emits one [`METRIC_AGENT_EXCHANGE_UNSETTLEABLE`] count per refusal a settle
/// pass could not settle.
///
/// Every entity that drives the courier calls this with its own pass report,
/// so a durably wedged exchange is measured wherever it happens rather than
/// only where someone remembered to look for it.
pub fn record_unsettleable_exchanges(
    metrics: &dyn MetricsRecorder,
    unsettleable: &[crate::choreography::AgentExchangeUnsettleable],
) {
    for refusal in unsettleable {
        record_agent_domain_counter(
            metrics,
            METRIC_AGENT_EXCHANGE_UNSETTLEABLE,
            1,
            &[
                ("operation", refusal.kind.as_label()),
                ("error_code", refusal.code.as_str()),
            ],
        )
        .ok();
    }
}

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
    "direction",
    "operation",
    "outcome",
    "phase",
    "safety_class",
    "signal",
    "transition",
    "trigger",
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

/// Records an agent-domain histogram observation after validating its labels.
pub fn record_agent_domain_histogram(
    metrics: &dyn MetricsRecorder,
    name: &str,
    value: f64,
    attributes: MetricAttributes<'_>,
) -> AgentObservabilityResult<()> {
    validate_agent_domain_metric_attributes(attributes)?;
    metrics.record_histogram(name, value, attributes);
    Ok(())
}

/// Records the millisecond distance between two timestamps as a histogram
/// observation.
///
/// Both endpoints come from the caller's injected clock — the same
/// `AgentTimestampMillis` every transition and every dispatch attempt already
/// carries — rather than from a wall-clock `Instant`. That keeps a duration
/// deterministic under the frozen clocks the test suite drives, and it is the
/// only way a duration spanning a durable boundary (a queued effect, a parked
/// wait, an epoch) can be measured at all, since those endpoints are persisted
/// timestamps and not moments this process lived through.
///
/// A non-monotonic pair records nothing rather than a negative duration: a
/// clock that went backwards is a deployment fault, and a negative sample
/// would corrupt every aggregate that reads the series.
pub fn record_agent_domain_duration(
    metrics: &dyn MetricsRecorder,
    name: &str,
    start: AgentTimestampMillis,
    end: AgentTimestampMillis,
    attributes: MetricAttributes<'_>,
) -> AgentObservabilityResult<()> {
    let Some(elapsed) = end.as_millis().checked_sub(start.as_millis()) else {
        return Ok(());
    };
    record_agent_domain_histogram(metrics, name, elapsed as f64, attributes)
}

/// Latency bucket boundaries, in milliseconds, shared by every agent-domain
/// duration instrument.
///
/// One ladder rather than a bespoke set per instrument: the agent domain's
/// durations span the same range for the same reason — a bounded in-process
/// segment at the bottom, a durable round trip in the middle, a human or an
/// external system at the top — and a single ladder is what lets a dashboard
/// compare a model call against the wait it caused without re-bucketing.
pub const AGENT_LATENCY_BUCKETS_MS: &[f64] = &[
    1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 30_000.0,
    60_000.0, 300_000.0, 900_000.0,
];

/// Count bucket boundaries shared by agent-domain distribution instruments
/// that measure a quantity rather than a duration — token counts, record
/// counts.
pub const AGENT_COUNT_BUCKETS: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 5_000.0, 10_000.0, 50_000.0,
    100_000.0,
];

/// One agent-domain metric instrument: its stable name, kind, unit, bounded
/// label keys, and — for a distribution — its bucket boundaries.
///
/// [Specification 17.12](../../../docs/plans/rakka-agent/spec.md) requires
/// metric labels to be bounded **and documented**. A prose table alone drifts
/// from the code the moment a call site changes, so the catalogue is data:
/// [`AGENT_DOMAIN_METRIC_INSTRUMENTS`] is walked by the test that puts every
/// label key through [`validate_agent_domain_metric_attributes`], and it is
/// what supplies unit and bucket semantics to the OTLP bridge, which sees only
/// raw observations and could not otherwise know them.
///
/// This mirrors the substrate's `AgentMetricInstrument`, extended with the two
/// things the agent domain needs and the substrate's shape lacks: the bounded
/// label keys, and the bucket boundaries.
// `Eq` is deliberately absent: the bucket boundaries are `f64`, which has no
// total equality. `PartialEq` is what the catalogue tests need.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AgentDomainMetricInstrument {
    /// Stable metric name.
    pub name: &'static str,
    /// Instrument kind.
    pub kind: MetricKind,
    /// UCUM-compatible unit label where one exists, otherwise an annotation
    /// in the `{thing}` form the conventions use for dimensionless counts.
    pub unit: &'static str,
    /// The bounded label keys this instrument records, every one of which is
    /// in [`AGENT_METRIC_FIELDS`] or the substrate's bounded vocabulary.
    pub labels: &'static [&'static str],
    /// Explicit bucket boundaries for a distribution; empty for counters and
    /// gauges.
    pub buckets: &'static [f64],
    /// Human-readable description.
    pub description: &'static str,
}

impl AgentDomainMetricInstrument {
    /// Defines one agent-domain instrument.
    #[must_use]
    pub const fn new(
        name: &'static str,
        kind: MetricKind,
        unit: &'static str,
        labels: &'static [&'static str],
        buckets: &'static [f64],
        description: &'static str,
    ) -> Self {
        Self {
            name,
            kind,
            unit,
            labels,
            buckets,
            description,
        }
    }

    /// The borrowed view the OTLP bridge needs to carry this instrument's unit
    /// and buckets into an export.
    #[must_use]
    pub const fn as_export_view(&self) -> OpenTelemetryInstrumentView<'static> {
        OpenTelemetryInstrumentView {
            name: self.name,
            unit: self.unit,
            bucket_boundaries: self.buckets,
        }
    }
}

/// Every metric the agent domain records, with its unit, bounded labels, and
/// buckets.
///
/// This is the documented catalogue
/// [specification 17.12](../../../docs/plans/rakka-agent/spec.md) requires,
/// kept as data so it cannot drift from the call sites: `tests/metric_catalogue.rs`
/// asserts that every name recorded anywhere in the crate appears here and
/// that every entry's labels pass the bounded-label guard.
///
/// The gauges 17.12 asks for that measure the *substrate* rather than the agent
/// domain — resident entities, inbox and outbox backlog, mailbox and stream
/// pressure, shard-ownership distribution — are deliberately absent: they are
/// published by `rakka.agent_workflow.*` and `rakka.*` instruments already, and
/// a second name for one number is two catalogues that drift. The prose
/// catalogue in `docs/rakka-agent-observability-catalogue.md` names the
/// providing instrument for each of those rows.
pub const AGENT_DOMAIN_METRIC_INSTRUMENTS: &[AgentDomainMetricInstrument] = &[
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_DECISIONS,
        MetricKind::Counter,
        "{decision}",
        &["decision_kind", "decision_source"],
        &[],
        "Agent-loop decisions durably retained by a decision sink.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_RUN_TRANSITIONS,
        MetricKind::Counter,
        "{transition}",
        &["phase"],
        &[],
        "Committed loop transitions, by the phase they advanced from.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_EFFECT_OUTCOMES,
        MetricKind::Counter,
        "{effect}",
        &["effect_kind", "safety_class", "outcome"],
        &[],
        "Resolved effect generations, including indeterminate outcomes.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_EFFECT_OUTSTANDING_DURATION,
        MetricKind::Histogram,
        "ms",
        &["effect_kind", "outcome"],
        AGENT_LATENCY_BUCKETS_MS,
        "Durable acceptance to durable result, as the run observed it.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_TURN_DURATION,
        MetricKind::Histogram,
        "ms",
        &["outcome"],
        AGENT_LATENCY_BUCKETS_MS,
        "One bounded active turn, across its durable model round trip.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_MODEL_TOKENS,
        MetricKind::Histogram,
        "{token}",
        &["direction"],
        AGENT_COUNT_BUCKETS,
        "Provider-reported token usage per recorded turn, by direction.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_RECOVERY_EVENTS,
        MetricKind::Counter,
        "{recovery}",
        &["outcome"],
        &[],
        "Run recoveries after restart, passivation, or shard movement.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_RECOVERY_DURATION,
        MetricKind::Histogram,
        "ms",
        &["outcome"],
        AGENT_LATENCY_BUCKETS_MS,
        "One run recovery, measured in the process that performed it.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_DECISION_DROPS,
        MetricKind::Gauge,
        "{decision}",
        &[],
        &[],
        "Decision events a run's bounded outbox ring dropped.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_TELEMETRY_FLUSH_FAILURES,
        MetricKind::Counter,
        "{failure}",
        &["signal"],
        &[],
        "Telemetry flush attempts a sink refused, by signal.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_TELEMETRY_EXPORT_QUEUE,
        MetricKind::Gauge,
        "{record}",
        &["backend", "signal"],
        &[],
        "Records a bounded telemetry sink is holding, awaiting export.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_TELEMETRY_EXPORT_DROPS,
        MetricKind::Counter,
        "{record}",
        &["backend", "signal"],
        &[],
        "Records a bounded telemetry sink dropped at capacity.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_TELEMETRY_EXPORT_UNMAPPABLE,
        MetricKind::Counter,
        "{record}",
        &["backend"],
        &[],
        "Records a telemetry sink refused as unmappable.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_MEMORY_RETRIEVALS,
        MetricKind::Counter,
        "{retrieval}",
        &["backend", "outcome"],
        &[],
        "Private-memory retrievals run by context-snapshot assembly.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_MEMORY_INGRESS_OUTCOMES,
        MetricKind::Counter,
        "{record}",
        &["outcome"],
        &[],
        "Memory-ingress guardrail outcomes on retrieved records.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_WAKE_DISPOSITIONS,
        MetricKind::Counter,
        "{wake}",
        &["outcome", "trigger"],
        &[],
        "Wake dispositions the continuous controller durably recorded.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_EPOCHS,
        MetricKind::Counter,
        "{epoch}",
        &["outcome"],
        &[],
        "Continuous-goal epoch admissions and results.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_GOAL_LIFECYCLE,
        MetricKind::Counter,
        "{transition}",
        &["transition"],
        &[],
        "Continuous-goal lifecycle transitions at the admission gate.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_GOAL_STATUS,
        MetricKind::Counter,
        "{transition}",
        &["transition"],
        &[],
        "Goal-contract status transitions, by the status arrived at.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_GOAL_STAGNATION,
        MetricKind::Counter,
        "{trip}",
        &["trigger"],
        &[],
        "Stagnation-threshold trips the wake controller detected.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_DELEGATION_RESULTS,
        MetricKind::Counter,
        "{result}",
        &["outcome"],
        &[],
        "Delegated children's terminal results decided at the parent's door.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_HANDOFF_RESULTS,
        MetricKind::Counter,
        "{result}",
        &["outcome"],
        &[],
        "Handoff resolutions decided at the source run's door.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_FAN_IN_RESOLUTIONS,
        MetricKind::Counter,
        "{group}",
        &["outcome"],
        &[],
        "Fan-in groups resolved, by bounded resolution code.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_WORKFLOW_RESULTS,
        MetricKind::Counter,
        "{result}",
        &["outcome"],
        &[],
        "Child workflow runs' terminal results accepted at the parent's door.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_TEAM_OPERATIONS,
        MetricKind::Counter,
        "{operation}",
        &["operation", "outcome"],
        &[],
        "Team board and lifecycle operations committed at the team entity.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_MODERATION_TURNS,
        MetricKind::Counter,
        "{operation}",
        &["operation", "outcome"],
        &[],
        "Moderated-conversation turn and lifecycle operations.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_HUMAN_RESULTS,
        MetricKind::Counter,
        "{result}",
        &["outcome"],
        &[],
        "Authenticated human-result submissions decided at the task entity.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_DEPENDENCY_OUTCOMES,
        MetricKind::Counter,
        "{edge}",
        &["outcome"],
        &[],
        "Dependency outcomes durably applied at the dependent task.",
    ),
    AgentDomainMetricInstrument::new(
        METRIC_AGENT_EXCHANGE_UNSETTLEABLE,
        MetricKind::Counter,
        "{refusal}",
        &["operation", "error_code"],
        &[],
        "Exchange replies a settle pass could not settle, per pass.",
    ),
    AgentDomainMetricInstrument::new(
        crate::wake_scanner::METRIC_AGENT_WAKES,
        MetricKind::Counter,
        "{wake}",
        &["outcome", "trigger"],
        &[],
        "Wake delivery attempts made by the shared scanner.",
    ),
];

/// One bounded operation the runtime observed, named by a closed class.
///
/// This is Rakka's own stable vocabulary for the span rows of
/// [specification 17.6](../../../docs/plans/rakka-agent/spec.md), and it is
/// deliberately *not* the OpenTelemetry GenAI vocabulary:
/// [17.20](../../../docs/plans/rakka-agent/spec.md) requires the agent domain
/// to keep an internal vocabulary of its own and to put the convention mapping
/// behind the `otel` feature. So the loop, the entities, and the dispatcher
/// emit these unconditionally — a `--no-default-features` build measures the
/// same operations — and [`crate::otel`] is the only place they become
/// `invoke_agent`, `execute_tool`, and the rest.
///
/// Every payload is a bounded class from a configured registry — a telemetry
/// name, a tool name, a model profile — never a raw identifier, user input, or
/// argument text ([17.6](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentSegmentOperation {
    /// A2A protocol ingress, bracketing context extraction and durable
    /// acceptance. The class is the bounded A2A operation label.
    A2aIngress {
        /// The bounded A2A operation class.
        operation: String,
    },
    /// One bounded loop transition that produced a decision.
    Decide {
        /// The loop phase the transition advanced from.
        phase: &'static str,
    },
    /// One bounded active invocation of an agent.
    InvokeAgent {
        /// The bounded configured agent telemetry or template name, when the
        /// deployment configured one.
        ///
        /// Absent is the normal case inside Rakka, and deliberately so:
        /// [17.6](../../../docs/plans/rakka-agent/spec.md) forbids a span name
        /// to embed an agent identifier, and `AgentId` is an identifier however
        /// bounded it is. The agent's identity rides the segment's
        /// [`AgentSegmentIdentity`] instead, which is where
        /// [17.3](../../../docs/plans/rakka-agent/spec.md) puts it — an
        /// access-controlled attribute, never a name.
        agent_name: Option<String>,
    },
    /// Durable acceptance of an effect into the outbox.
    EffectSchedule {
        /// The bounded effect kind.
        effect_kind: &'static str,
    },
    /// One dispatcher attempt on a scheduled effect.
    EffectDispatch {
        /// The bounded effect kind.
        effect_kind: &'static str,
    },
    /// The dispatch-time authority decision for an effect.
    ToolAuthorize {
        /// The bounded effect kind the grant was sought for.
        effect_kind: &'static str,
    },
    /// A model/provider call executed through the model adapter.
    ModelInference {
        /// The bounded model profile the request pinned, when the deployment
        /// configured one.
        ///
        /// `None` is the default configuration, not an error: a deployment
        /// that names no profile leaves the adapter to its own default. It was
        /// a `String` filled with `""` by an `unwrap_or_default`, which is how
        /// the convention span name came out as `"chat "` — a class differing
        /// from `chat` by an invisible character, which backends group
        /// separately.
        model_profile: Option<String>,
    },
    /// Application-level execution of a named tool.
    ExecuteTool {
        /// The tool name from the bounded registry.
        tool_name: String,
    },
    /// An outbound A2A call to a peer agent.
    DelegateToPeer {
        /// The bounded peer or skill class.
        peer_class: String,
    },
    /// A durable workflow-tool invocation.
    WorkflowInvoke {
        /// The bounded workflow class.
        workflow_class: String,
    },
    /// Goal progress/evidence evaluation.
    GoalEvaluate,
    /// Task result validation against its bounded rule set.
    ValidateTaskResult,
    /// A same-task transfer of responsibility.
    Handoff,
    /// A team board claim or message operation.
    TeamOperation {
        /// The bounded team operation label.
        operation: String,
    },
    /// One moderated-conversation turn transition.
    ModerationTurn {
        /// The bounded conversation operation label.
        operation: String,
    },
    /// Continuous wake/epoch admission.
    WakeAdmit,
    /// A fail-closed autonomy admission check.
    AutonomyAdmit,
    /// A dispatch-time budget reservation.
    BudgetReserve,
    /// A terminal budget settlement.
    BudgetSettle,
    /// A short-term, private, or communal memory operation.
    MemoryOperation {
        /// The bounded memory tier.
        tier: &'static str,
    },
    /// A retrieval against an authorized knowledge space.
    Retrieval {
        /// The bounded backend class.
        backend: String,
    },
    /// Opening a durable checkpoint; the segment ends at the durable park.
    CheckpointOpen,
    /// Resuming a run after a durable wait.
    RunResume,
    /// Recovering a run after restart, passivation, or shard movement.
    RunRecover,
}

impl AgentSegmentOperation {
    /// Stable kebab-case class label, for logs and bounded metric labels.
    ///
    /// The label names the *class*, never its payload: an operation's bounded
    /// class is low-cardinality and a tool or model name is not, so a label
    /// built from this can ride a metric while the payload rides a span.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::A2aIngress { .. } => "a2a-ingress",
            Self::Decide { .. } => "decide",
            Self::InvokeAgent { .. } => "invoke-agent",
            Self::EffectSchedule { .. } => "effect-schedule",
            Self::EffectDispatch { .. } => "effect-dispatch",
            Self::ToolAuthorize { .. } => "tool-authorize",
            Self::ModelInference { .. } => "model-inference",
            Self::ExecuteTool { .. } => "execute-tool",
            Self::DelegateToPeer { .. } => "delegate-to-peer",
            Self::WorkflowInvoke { .. } => "workflow-invoke",
            Self::GoalEvaluate => "goal-evaluate",
            Self::ValidateTaskResult => "validate-task-result",
            Self::Handoff => "handoff",
            Self::TeamOperation { .. } => "team-operation",
            Self::ModerationTurn { .. } => "moderation-turn",
            Self::WakeAdmit => "wake-admit",
            Self::AutonomyAdmit => "autonomy-admit",
            Self::BudgetReserve => "budget-reserve",
            Self::BudgetSettle => "budget-settle",
            Self::MemoryOperation { .. } => "memory-operation",
            Self::Retrieval { .. } => "retrieval",
            Self::CheckpointOpen => "checkpoint-open",
            Self::RunResume => "run-resume",
            Self::RunRecover => "run-recover",
        }
    }
}

/// How a bounded operation ended.
///
/// `Unset` is not "unknown so far": a segment is only ever recorded once it
/// has ended, so `Unset` means the operation carries no success or failure
/// judgement at all — the OpenTelemetry default for a span whose instrument
/// does not classify the outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AgentSegmentOutcome {
    /// The operation ended without a success or failure judgement.
    #[default]
    Unset,
    /// The operation succeeded.
    Ok,
    /// The operation failed.
    Error,
}

impl AgentSegmentOutcome {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

/// The durable identities a segment may carry, under the caller's telemetry
/// access policy ([specification 17.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Identifiers may appear in access-controlled traces and logs and must never
/// label a metric or ride baggage. Where a tenant's policy requires a
/// pseudonym instead of a raw identifier, the substitution belongs to the sink
/// that exports the segment, and the reversible mapping stays outside the
/// telemetry backend.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentSegmentIdentity {
    /// The agent this operation belongs to.
    pub agent: Option<String>,
    /// The run — the durable session identity.
    pub run: Option<String>,
    /// The task.
    pub task: Option<String>,
    /// The goal.
    pub goal: Option<String>,
    /// The delegation.
    pub delegation: Option<String>,
}

impl AgentSegmentIdentity {
    /// The identity of one run scope.
    #[must_use]
    pub fn of_run(scope: &AgentRunScope) -> Self {
        Self {
            agent: Some(scope.agent().as_str().to_string()),
            run: Some(scope.run().as_str().to_string()),
            ..Self::default()
        }
    }

    /// The identity of one task scope.
    ///
    /// A task is tenant-scoped rather than agent-scoped — a task outlives the
    /// agent assigned to it, and may be reassigned — so the agent is absent
    /// here rather than guessed from the caller.
    #[must_use]
    pub fn of_task(scope: &AgentTaskScope) -> Self {
        Self {
            task: Some(scope.task().as_str().to_string()),
            ..Self::default()
        }
    }
}

/// One ended bounded operation: what it was, when it ran, how it ended, and
/// the durable trace context it belongs to.
///
/// A segment is *closed*, never open. The runtime never holds a span object
/// across a durable wait
/// ([17.4](../../../docs/plans/rakka-agent/spec.md)), and the boundaries are
/// never persisted: stamping them into a durable record would make a telemetry
/// change a state migration, which
/// [17.20](../../../docs/plans/rakka-agent/spec.md) forbids. What *is*
/// persisted is the trace context the segment carries, which is what lets a
/// resume link back to the operation that parked.
// `Eq` is absent because [`AgentDecisionEvent`] derives only `PartialEq`, as
// does [`AgentDecisionEventPage`]; a segment carrying one follows.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentTelemetrySegment {
    /// The bounded operation class.
    pub operation: AgentSegmentOperation,
    /// When the operation began.
    pub start: AgentTimestampMillis,
    /// When the operation ended.
    pub end: AgentTimestampMillis,
    /// How it ended.
    pub outcome: AgentSegmentOutcome,
    /// The stable low-cardinality error type, on an error outcome.
    pub error_type: Option<&'static str>,
    /// The stable Rakka error code, on an error outcome.
    pub error_code: Option<String>,
    /// The durable identities, under the caller's access policy.
    pub identity: AgentSegmentIdentity,
    /// Bounded attributes from closed vocabularies. Never content, never a
    /// credential, never an unbounded message.
    pub attributes: AgentAttributes,
    /// The durable decisions the operation committed, when it committed any.
    ///
    /// Carried as the domain's own [`AgentDecisionEvent`] rather than as a
    /// convention span event, so the ungated path stays free of the GenAI
    /// vocabulary and the mapping stays in one place
    /// ([specification 17.7](../../../docs/plans/rakka-agent/spec.md): a
    /// durable decision produces a correlated decision span *or span event*).
    ///
    /// A run populates this only when it is already recording decisions —
    /// that is, when a decision sink is wired. The coupling is deliberate: a
    /// decision is a durable record, and letting a *telemetry* sink switch on
    /// a durable write would invert
    /// [17.1](../../../docs/plans/rakka-agent/spec.md), which makes telemetry
    /// never a correctness input. Segments carry the decisions a run was
    /// already committing, and none it was not.
    pub decisions: Vec<AgentDecisionEvent>,
    /// Provider-reported token usage, when the operation was a model call
    /// that reported some.
    ///
    /// Only what the provider actually reported
    /// ([17.8](../../../docs/plans/rakka-agent/spec.md): never invent token
    /// usage), and absent rather than zeroed when it reported none.
    pub usage: Option<crate::model::AgentModelUsage>,
    /// The durable trace context the operation belongs to.
    pub telemetry: AgentTelemetryContext,
}

impl AgentTelemetrySegment {
    /// A segment that ended without a success or failure judgement.
    #[must_use]
    pub fn new(
        operation: AgentSegmentOperation,
        start: AgentTimestampMillis,
        end: AgentTimestampMillis,
    ) -> Self {
        Self {
            operation,
            start,
            end,
            outcome: AgentSegmentOutcome::Unset,
            error_type: None,
            error_code: None,
            identity: AgentSegmentIdentity::default(),
            attributes: AgentAttributes::new(),
            decisions: Vec::new(),
            usage: None,
            telemetry: AgentTelemetryContext::default(),
        }
    }

    /// Marks the operation successful.
    #[must_use]
    pub fn ok(mut self) -> Self {
        self.outcome = AgentSegmentOutcome::Ok;
        self
    }

    /// Marks the operation failed, with a stable low-cardinality type and the
    /// stable Rakka code.
    ///
    /// The code is bounded on the way in, because an error code is a stable
    /// short string and an unbounded one here would be an error *message*
    /// wearing a code's name — the exact substitution
    /// [17.6](../../../docs/plans/rakka-agent/spec.md) forbids as a grouping
    /// attribute.
    #[must_use]
    pub fn failed(mut self, error_type: &'static str, code: impl AsRef<str>) -> Self {
        self.outcome = AgentSegmentOutcome::Error;
        self.error_type = Some(error_type);
        let code = code.as_ref();
        let bounded = code
            .char_indices()
            .take_while(|(index, character)| {
                index + character.len_utf8() <= AGENT_SEGMENT_ERROR_CODE_MAX_LENGTH
            })
            .map(|(_, character)| character)
            .collect::<String>();
        self.error_code = Some(bounded);
        self
    }

    /// Attaches the durable identities.
    #[must_use]
    pub fn identity(mut self, identity: AgentSegmentIdentity) -> Self {
        self.identity = identity;
        self
    }

    /// Attaches the durable trace context.
    #[must_use]
    pub fn telemetry(mut self, telemetry: AgentTelemetryContext) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Adds one bounded attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Attaches the durable decisions the operation committed.
    #[must_use]
    pub fn decisions(mut self, decisions: Vec<AgentDecisionEvent>) -> Self {
        self.decisions = decisions;
        self
    }

    /// Attaches provider-reported token usage.
    ///
    /// Usage that reports no tokens at all is dropped rather than attached: a
    /// zero is a claim about the provider there is no evidence for.
    #[must_use]
    pub fn usage(mut self, usage: crate::model::AgentModelUsage) -> Self {
        self.usage = (usage.total_tokens() > 0).then_some(usage);
        self
    }

    /// The operation's duration in milliseconds, or `None` when the clock ran
    /// backwards between the endpoints.
    #[must_use]
    pub fn duration_ms(&self) -> Option<u64> {
        self.end.as_millis().checked_sub(self.start.as_millis())
    }
}

/// The longest stable error code a segment carries.
pub const AGENT_SEGMENT_ERROR_CODE_MAX_LENGTH: usize = 96;

/// Every stable `error.type` the **agent domain** writes onto a failed
/// segment.
///
/// [Specification 17.6](../../../docs/plans/rakka-agent/spec.md) makes
/// `error.type` a grouping attribute, and 17.16 asks a retention policy to
/// select on it — so operators write Collector rules against these strings and
/// they are a compatibility surface, not a diagnostic detail. `failed` takes a
/// `&'static str`, which gives the compiler nothing to check, so the
/// vocabulary is data here and `tests/metric_catalogue.rs` holds the call
/// sites to it in both directions: a new type that is not listed fails the
/// suite, and a listed type nothing writes fails it too.
///
/// The A2A edge writes one more of its own,
/// `rakka_a2a::agents::A2A_INGRESS_ERROR_TYPE`, because the protocol adapter
/// owns the ingress span the agent domain defers to it. It is not in this list
/// because this crate does not write it and could not keep it honest.
pub const AGENT_SEGMENT_ERROR_TYPES: &[&str] = &[
    "rakka.agent.authority",
    "rakka.agent.dispatch",
    "rakka.agent.effect",
    "rakka.agent.model",
    "rakka.agent.outbox",
    "rakka.agent.recovery",
    "rakka.agent.tool",
];

/// Segment attribute: the resolved status of the effect an operation settled.
///
/// This is the key a retention policy selects `indeterminate` on
/// ([specification 17.9](../../../docs/plans/rakka-agent/spec.md): an
/// indeterminate transition must be an important event suitable for
/// tail-sampling retention).
pub const SEGMENT_ATTR_EFFECT_STATUS: &str = "rakka.agent.effect.status";

/// Segment attribute: which dispatch attempt this was, so a retention policy
/// can select excessive retry.
pub const SEGMENT_ATTR_EFFECT_ATTEMPT: &str = "rakka.agent.effect.attempt";

/// Segment attribute: the settings revision in force, so a retention policy
/// can select a newly deployed version under investigation.
pub const SEGMENT_ATTR_SETTINGS_REVISION: &str = "rakka.agent.settings_revision";

/// Segment attribute: how many loop transitions one resident slice advanced.
pub const SEGMENT_ATTR_LOOP_TRANSITIONS: &str = "rakka.agent.loop.transitions";

/// Segment attribute: the bounded checkpoint kind a park opened.
pub const SEGMENT_ATTR_CHECKPOINT_KIND: &str = "rakka.agent.checkpoint.kind";

/// Measures one *live* bounded operation — a transition, a dispatch attempt,
/// a provider call — and closes it into a segment.
///
/// Two clocks, on purpose. The segment is *anchored* to the caller's injected
/// `AgentTimestampMillis`, so a segment's position in time agrees with the
/// durable records around it and stays deterministic under the frozen clocks
/// the suite drives. Its *width* comes from a monotonic [`std::time::Instant`],
/// because a
/// live operation has exactly one injected timestamp — a transition receives
/// one `now` and commits under it — and deriving a width from a single value
/// would report every live operation as instantaneous.
///
/// This is the opposite trade from
/// [`record_agent_domain_duration`], and deliberately so: a duration spanning a
/// durable boundary has two persisted endpoints and must use them, while a
/// duration inside one process has none and must measure itself.
#[derive(Debug)]
pub struct AgentSegmentTimer {
    anchor: AgentTimestampMillis,
    started: std::time::Instant,
}

impl AgentSegmentTimer {
    /// Starts measuring, anchored at the caller's current timestamp.
    #[must_use]
    pub fn start(anchor: AgentTimestampMillis) -> Self {
        Self {
            anchor,
            started: std::time::Instant::now(),
        }
    }

    /// Closes the measurement into a segment for the given operation.
    #[must_use]
    pub fn close(self, operation: AgentSegmentOperation) -> AgentTelemetrySegment {
        let elapsed = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        AgentTelemetrySegment::new(
            operation,
            self.anchor,
            AgentTimestampMillis::new(self.anchor.as_millis().saturating_add(elapsed)),
        )
    }
}

/// Receives bounded operation segments as they end.
///
/// Deliberately synchronous and infallible, unlike
/// [`AgentDecisionEventSink`]. A segment is closed inside the dispatch
/// attempt and at entity command boundaries, where there is no place to await
/// and nothing that may fail the operation: telemetry is never a correctness
/// input ([17.1](../../../docs/plans/rakka-agent/spec.md)), so a sink that
/// cannot keep up must drop and count, never block or error. Buffering,
/// batching, and export belong to the implementation — see
/// [`crate::otel::AgentGenAiSpanExporter`], which is the one that turns these
/// into OTLP span records.
pub trait AgentSegmentSink: Send + Sync {
    /// The bounded backend class, for the flush-failure metric's `signal`.
    fn backend_name(&self) -> &'static str;

    /// Records one ended segment.
    fn record(&self, segment: &AgentTelemetrySegment);
}

/// The most segments an [`InMemoryAgentSegmentSink`] retains before it starts
/// dropping the oldest.
///
/// Twice the span exporter's buffer, because a segment is retained until a
/// test reads it rather than until the next flush, and a whole run's worth
/// should fit without the bound ever being reached.
pub const DEFAULT_AGENT_SEGMENT_SINK_CAPACITY: usize = 1024;

/// In-memory segment sink for deterministic tests.
///
/// **Bounded, like every sink.** The trait above states that
/// [17.1](../../../docs/plans/rakka-agent/spec.md) forbids unbounded
/// in-process queues and that a sink which cannot keep up must drop and
/// count; this one used to push into an unbounded `Vec`, six lines below the
/// paragraph saying it must not. That was not merely an inconsistent test
/// helper: the only *other* implementation in the workspace is
/// [`crate::otel::AgentGenAiSpanExporter`], which is behind the `otel`
/// feature, so under `--no-default-features` this was the only thing a
/// deployment could pass to `with_segments` — and every loop transition,
/// dispatch attempt, model call, tool call and A2A ingress appended a cloned
/// segment, with nothing draining it, until the node ran out of memory.
///
/// It is a ring: at capacity the oldest segment is dropped and counted, the
/// same rule and the same direction as the exporter's buffer.
pub struct InMemoryAgentSegmentSink {
    segments: Mutex<VecDeque<AgentTelemetrySegment>>,
    capacity: usize,
    dropped: AtomicU64,
    metrics: Option<Arc<dyn MetricsRecorder>>,
    health: AgentSegmentSinkHealth,
}

impl fmt::Debug for InMemoryAgentSegmentSink {
    /// Hand-written because a `MetricsRecorder` is a caller-supplied trait
    /// object with no `Debug` bound, and adding one to the substrate's
    /// recorder trait to satisfy a test helper would be the tail wagging the
    /// dog. Prints what a reader of a sink actually wants: how much it holds
    /// and how much it lost.
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryAgentSegmentSink")
            .field(
                "retained",
                &self.segments.lock().map(|segments| segments.len()).ok(),
            )
            .field("capacity", &self.capacity)
            .field("dropped", &self.dropped())
            .field("metrics", &self.metrics.is_some())
            .finish()
    }
}

impl Default for InMemoryAgentSegmentSink {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryAgentSegmentSink {
    /// Creates an empty sink retaining
    /// [`DEFAULT_AGENT_SEGMENT_SINK_CAPACITY`] segments.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_AGENT_SEGMENT_SINK_CAPACITY)
    }

    /// Creates an empty sink with an explicit bound.
    ///
    /// A capacity of zero is raised to one: a ring that can hold nothing
    /// would drop every segment while reporting a healthy sink.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            segments: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
            dropped: AtomicU64::new(0),
            metrics: None,
            health: AgentSegmentSinkHealth::new(),
        }
    }

    /// Publishes this sink's export health to `metrics`.
    ///
    /// **What each signal costs, and why the gauge is not the leading
    /// indicator here.** A sink has no flush point of its own, so `record` is
    /// the only place publication can happen. The queue gauge is written only
    /// when the depth *changes*: it traces the ring filling and then goes
    /// quiet, because a ring that evicts rather than blocks sits at exactly
    /// `capacity` from its first eviction onward. Publishing it only on a drop
    /// — which is what this did — meant it was only ever observed at that one
    /// constant, and could never show a sink filling up. The **drop counter**
    /// is the leading indicator, and its write is not avoidable: on a
    /// saturated ring every record evicts one, and each eviction is a distinct
    /// loss that a sampled counter would under-report.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Every segment retained, in the order they ended.
    #[must_use]
    pub fn segments(&self) -> Vec<AgentTelemetrySegment> {
        self.segments
            .lock()
            .map(|segments| segments.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// The bounded operation labels retained, in order.
    #[must_use]
    pub fn operations(&self) -> Vec<&'static str> {
        self.segments
            .lock()
            .map(|segments| {
                segments
                    .iter()
                    .map(|segment| segment.operation.as_label())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// How many segments the bound dropped.
    ///
    /// A non-zero count is what makes the loss visible rather than silent —
    /// and, in a test, what says an assertion is reading a truncated history.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::SeqCst)
    }

    /// Publishes queue depth and the drop diff, when a recorder is wired.
    ///
    /// Called with the sink's lock already released: a metrics recorder is a
    /// caller-supplied trait object, and holding a telemetry lock across one
    /// is how a telemetry sink becomes a correctness input.
    fn publish_health(&self, queued: Option<usize>) {
        let Some(metrics) = self.metrics.as_ref() else {
            return;
        };
        self.health.publish(
            metrics.as_ref(),
            self.backend_name(),
            queued,
            self.dropped(),
            0,
        );
    }
}

impl AgentSegmentSink for InMemoryAgentSegmentSink {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn record(&self, segment: &AgentTelemetrySegment) {
        let Ok(mut segments) = self.segments.lock() else {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            // No depth, rather than a depth of zero. Poisoning is sticky, so
            // this sink will now drop every record it is handed for the life
            // of the process — and `0` is the one reading that would have an
            // operator see a drained, healthy queue while that happened. The
            // drop still counts; the gauge keeps the last depth it could
            // actually observe.
            self.publish_health(None);
            return;
        };
        while segments.len() >= self.capacity {
            segments.pop_front();
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
        segments.push_back(segment.clone());
        let queued = segments.len();
        drop(segments);
        self.publish_health(Some(queued));
    }
}

/// Publishes one bounded telemetry sink's export health.
///
/// [17.12](../../../docs/plans/rakka-agent/spec.md) asks for telemetry export
/// queue, drops, and failures, and until this slice the only recording site in
/// the crate was the durable decision sink's refusal. A segment sink drops
/// rather than blocks — that is the rule [`AgentSegmentSink`] states — so the
/// loss it takes is invisible unless something publishes it.
///
/// **Counters are published as diffs, and that is the whole reason this is a
/// type rather than a function.** A sink's own `dropped()` / `unmappable()`
/// counters are cumulative, and `MetricsRecorder::increment_counter` *adds*,
/// so handing it the cumulative value on every publish would report the
/// triangular sum of the loss instead of the loss. This holds the last
/// published value and increments by the difference — the same rule
/// `advance_loop` follows for segment additions, and for the same reason: what
/// a periodic reader owes a counter is what changed, not what stands.
#[derive(Debug)]
pub struct AgentSegmentSinkHealth {
    published_queue: AtomicU64,
    published_drops: AtomicU64,
    published_unmappable: AtomicU64,
}

impl Default for AgentSegmentSinkHealth {
    fn default() -> Self {
        Self {
            // `0` is a legal queue depth, so "nothing published yet" needs a
            // value no ring can hold. Without it a sink whose first observed
            // depth is zero would be indistinguishable from one that had
            // already published zero, and would skip the write.
            published_queue: AtomicU64::new(u64::MAX),
            published_drops: AtomicU64::new(0),
            published_unmappable: AtomicU64::new(0),
        }
    }
}

impl AgentSegmentSinkHealth {
    /// Creates a publisher that has published nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Publishes queue depth as a gauge and the two loss counters as diffs.
    ///
    /// `queued` is `None` when the caller cannot observe its own depth — a
    /// poisoned lock, and nothing else. It is not the same as `Some(0)`, and
    /// reporting the two alike is how a sink that is dropping every record
    /// comes to read as an empty, healthy queue.
    ///
    /// The `signal` label is written here as the literal `"spans"` rather than
    /// taken as a parameter. A segment sink has exactly one signal, so a
    /// parameter would have been speculative generality — and it would have
    /// put the value somewhere no bijection scan could pair it with its key,
    /// which is how a bounded label vocabulary quietly stops being bounded.
    /// The application boundary's per-signal values are its own
    /// ([`AGENT_TELEMETRY_SIGNALS`]).
    pub fn publish(
        &self,
        metrics: &dyn MetricsRecorder,
        backend: &'static str,
        queued: Option<usize>,
        dropped: u64,
        unmappable: u64,
    ) {
        if let Some(queued) = queued {
            // Only when the depth moved. A ring that evicts rather than blocks
            // sits at exactly `capacity` from its first eviction onward, so
            // re-writing it per record buys an operator nothing and puts a
            // gauge write with label validation on the hot path of every loop
            // transition, dispatch attempt, model call and A2A request.
            //
            // `swap`, not the `fetch_max` the counters use, because a depth
            // legitimately falls and a gauge is last-write-wins. The two
            // differ in what a race costs: a rewound *counter* watermark
            // republishes an interval and is wrong forever after, while two
            // publishers racing here can at worst leave the gauge one step
            // behind until the depth next changes. In the steady state they
            // cannot even do that — every publisher on a saturated ring
            // observes `capacity` and agrees.
            let queued = queued as u64;
            if self.published_queue.swap(queued, Ordering::SeqCst) != queued {
                record_agent_domain_gauge(
                    metrics,
                    METRIC_AGENT_TELEMETRY_EXPORT_QUEUE,
                    queued as f64,
                    &[("backend", backend), ("signal", "spans")],
                )
                .ok();
            }
        }
        let new_drops = Self::advance(&self.published_drops, dropped);
        if new_drops > 0 {
            record_agent_domain_counter(
                metrics,
                METRIC_AGENT_TELEMETRY_EXPORT_DROPS,
                new_drops,
                &[("backend", backend), ("signal", "spans")],
            )
            .ok();
        }
        let new_unmappable = Self::advance(&self.published_unmappable, unmappable);
        if new_unmappable > 0 {
            record_agent_domain_counter(
                metrics,
                METRIC_AGENT_TELEMETRY_EXPORT_UNMAPPABLE,
                new_unmappable,
                &[("backend", backend)],
            )
            .ok();
        }
    }

    /// Advances one cumulative watermark, returning how far it advanced.
    ///
    /// **Advance-only, and that is a correctness property rather than a
    /// micro-optimisation.** A `swap` is atomic by itself, but the pair
    /// *(read the sink's counter, swap the watermark)* is not, and every
    /// driver of a run shares one `Arc<dyn AgentSegmentSink>` across threads.
    /// Two publishers can therefore swap out of order: the one that observed
    /// the higher count lands first and reports it, then the one that observed
    /// the lower count **rewinds** the watermark to a value already published.
    /// The next publish diffs against that rewound value and counts the
    /// interval a second time — ten increments for eight real drops, which is
    /// exactly the over-report the diff exists to prevent. With `fetch_max` a
    /// late, lower observation owes nothing and moves nothing, and the total
    /// published can never exceed the highest count observed.
    ///
    /// The `load` in front is a fast path, not a second opinion: it can only
    /// skip when this observation is already covered, which is precisely when
    /// there is nothing to publish. Every eviction on a saturated ring takes
    /// it for `unmappable`, which a segment sink never raises above zero.
    fn advance(watermark: &AtomicU64, observed: u64) -> u64 {
        if observed <= watermark.load(Ordering::SeqCst) {
            return 0;
        }
        observed.saturating_sub(watermark.fetch_max(observed, Ordering::SeqCst))
    }
}

/// Returns the agent-domain instrument definition for a metric name.
#[must_use]
pub fn agent_domain_metric_instrument(name: &str) -> Option<&'static AgentDomainMetricInstrument> {
    AGENT_DOMAIN_METRIC_INSTRUMENTS
        .iter()
        .find(|instrument| instrument.name == name)
}

/// The whole catalogue as OTLP bridge instrument views, so an export carries
/// every agent-domain unit and bucket boundary.
#[must_use]
pub fn agent_domain_instrument_views() -> Vec<OpenTelemetryInstrumentView<'static>> {
    AGENT_DOMAIN_METRIC_INSTRUMENTS
        .iter()
        .map(AgentDomainMetricInstrument::as_export_view)
        .collect()
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
        // Walked from the catalogue, not from a hand-maintained copy of it.
        // The copy this replaced had gone stale in exactly the way a copy
        // does: it listed eight keys while `backend`, `transition`, `trigger`,
        // and `error_code` were being recorded, so it asserted nothing about
        // four of the twelve keys in use. Reading the catalogue makes the
        // omission impossible — a new label key is only recordable once the
        // instrument declares it, and the instrument's declaration is what
        // this walks.
        assert!(!AGENT_DOMAIN_METRIC_INSTRUMENTS.is_empty());
        for instrument in AGENT_DOMAIN_METRIC_INSTRUMENTS {
            for key in instrument.labels {
                assert!(
                    validate_agent_domain_metric_attributes(&[(key, "value")]).is_ok(),
                    "{key} on {} must be accepted",
                    instrument.name
                );
            }
        }
    }

    #[test]
    fn the_catalogue_is_well_formed() {
        let mut names = std::collections::BTreeSet::new();
        for instrument in AGENT_DOMAIN_METRIC_INSTRUMENTS {
            assert!(
                instrument.name.starts_with("rakka.agent."),
                "unexpected metric namespace: {}",
                instrument.name
            );
            assert!(
                names.insert(instrument.name),
                "duplicate metric instrument: {}",
                instrument.name
            );
            assert!(
                !instrument.unit.trim().is_empty(),
                "{} declares no unit",
                instrument.name
            );
            assert!(
                !instrument.description.trim().is_empty(),
                "{} declares no description",
                instrument.name
            );
            // Buckets are a distribution's property; a counter or gauge that
            // declared them would export a data point nothing can read.
            if instrument.kind == MetricKind::Histogram {
                assert!(
                    !instrument.buckets.is_empty(),
                    "{} is a histogram with no buckets",
                    instrument.name
                );
                assert!(
                    instrument.buckets.windows(2).all(|pair| pair[0] < pair[1]),
                    "{}'s buckets must ascend",
                    instrument.name
                );
            } else {
                assert!(
                    instrument.buckets.is_empty(),
                    "{} is not a distribution and must declare no buckets",
                    instrument.name
                );
            }
        }
    }

    #[test]
    fn the_export_view_carries_the_unit_and_the_buckets() {
        let views = agent_domain_instrument_views();
        assert_eq!(views.len(), AGENT_DOMAIN_METRIC_INSTRUMENTS.len());
        let decisions = agent_domain_metric_instrument(METRIC_AGENT_DECISIONS)
            .expect("the decision counter is catalogued");
        let view = decisions.as_export_view();
        assert_eq!(view.name, METRIC_AGENT_DECISIONS);
        assert_eq!(view.unit, decisions.unit);
        assert!(view.bucket_boundaries.is_empty());
    }

    #[test]
    fn a_duration_records_the_distance_and_a_backwards_clock_records_nothing() {
        let metrics = rakka_core::InMemoryMetricsRecorder::new();
        record_agent_domain_duration(
            &metrics,
            METRIC_AGENT_DECISIONS,
            AgentTimestampMillis::new(10),
            AgentTimestampMillis::new(35),
            &[],
        )
        .expect("the labels are bounded");
        record_agent_domain_duration(
            &metrics,
            METRIC_AGENT_DECISIONS,
            AgentTimestampMillis::new(35),
            AgentTimestampMillis::new(10),
            &[],
        )
        .expect("a backwards pair is not an error");

        let snapshot = metrics.snapshot();
        let observations = snapshot.observations_named(METRIC_AGENT_DECISIONS);
        assert_eq!(
            observations.len(),
            1,
            "a backwards clock records nothing rather than a negative sample"
        );
        assert_eq!(observations[0].value(), 25.0);
        assert_eq!(observations[0].kind(), MetricKind::Histogram);
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

    /// A poisoned sink reports no depth, rather than an empty queue.
    ///
    /// Poisoning is sticky: once a panic has been taken while the ring's lock
    /// was held, this sink drops every record it is ever handed again. The arm
    /// used to publish a depth of `0` on that path, so the one metric an
    /// operator would consult to see a backing-up pipeline reported a drained,
    /// healthy queue for a sink that had stopped working entirely — the exact
    /// opposite reading. Nothing outside this crate can reach the private lock
    /// to poison it, which is why this lives beside the code rather than in
    /// `tests/telemetry_segments.rs`.
    #[test]
    fn a_poisoned_sink_publishes_no_queue_depth() {
        let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());
        let sink =
            Arc::new(InMemoryAgentSegmentSink::with_capacity(4).with_metrics(metrics.clone()));
        let segment = |at: u64| {
            AgentTelemetrySegment::new(
                AgentSegmentOperation::Decide { phase: "propose" },
                rakka_agent_workflow::AgentTimestampMillis::new(at),
                rakka_agent_workflow::AgentTimestampMillis::new(at + 1),
            )
            .ok()
        };

        // One healthy record, so there is a real depth on record to contrast
        // the poisoned publish against.
        sink.record(&segment(1));

        let poisoner = Arc::clone(&sink);
        let _panicked = std::thread::spawn(move || {
            let _held = poisoner
                .segments
                .lock()
                .expect("the lock is not yet poisoned");
            panic!("poisoning the ring");
        })
        .join();
        assert!(sink.segments.lock().is_err(), "the lock is poisoned");

        sink.record(&segment(2));
        assert_eq!(sink.dropped(), 1, "the record was dropped, and counted");

        let snapshot = metrics.snapshot();
        let depths: Vec<u64> = snapshot
            .observations_named(METRIC_AGENT_TELEMETRY_EXPORT_QUEUE)
            .iter()
            .map(|observation| observation.value() as u64)
            .collect();
        assert_eq!(
            depths,
            vec![1],
            "the poisoned publish adds no depth of its own; the gauge holds the \
             last one this sink could actually observe"
        );
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
