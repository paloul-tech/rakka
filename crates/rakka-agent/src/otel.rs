//! OpenTelemetry GenAI convention mapping (`otel` feature).
//!
//! Owns the mapping from the stable Rakka telemetry domain of
//! [`crate::observability`] to a pinned, reviewed OpenTelemetry GenAI
//! semantic-convention revision, composed additively over the existing
//! `rakka-agent-workflow` OTLP bridge — which gained span kind, status,
//! events, and instrumentation scope for it, because a bridge that silently
//! dropped a required field could not claim convention compliance
//! ([specification 17.17](../../../docs/plans/rakka-agent/spec.md)). It does
//! not own application exporter credentials and does not install a global SDK
//! into the core runtime.
//!
//! The convention revision is **pinned** ([`AGENT_GENAI_CONVENTION_REVISION`])
//! and recorded in the instrumentation scope of every batch
//! ([specification 17.2](../../../docs/plans/rakka-agent/spec.md),
//! [17.20](../../../docs/plans/rakka-agent/spec.md)): the GenAI conventions
//! were Development-status when the specification was written, so an upgrade
//! is an explicit adapter compatibility review — span names and kinds, metric
//! names and units, required attributes, content-capture rules — and must not
//! by itself require a durable agent-state migration. The Rakka-internal
//! vocabulary (`rakka.agent.*` names, `as_label()` values) stays stable
//! either way; this module is the only place the two meet.
//!
//! Span names carry bounded classes from configured registries — an agent
//! telemetry name, a tool name, a model name — never raw goal/task/run ids,
//! user input, or argument text
//! ([specification 17.6](../../../docs/plans/rakka-agent/spec.md)). Identity
//! attributes are supplied by the caller under its tenant's telemetry access
//! policy: where policy requires a pseudonym, the caller passes the pseudonym,
//! and the reversible mapping stays outside the telemetry backend
//! ([specification 17.3](../../../docs/plans/rakka-agent/spec.md)).

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{
    bounded_export_attributes, AgentAttributes, AgentLogEvent, AgentOtelInstrumentationScope,
    AgentOtelResource, AgentOtelSpanEvent, AgentOtelSpanExport, AgentOtelSpanKind,
    AgentOtelSpanStatus, AgentOtlpBridgeExport, AgentOtlpExporterConfig, AgentOtlpResult,
    AgentTelemetryContext, AgentTimestampMillis, AGENT_EXPORT_MAX_ATTRIBUTES,
};
use rakka_core::{MetricsRecorder, MetricsSnapshot};

use crate::model::AgentModelUsage;
use crate::observability::{
    AgentDecisionEvent, AgentSegmentOperation, AgentSegmentOutcome, AgentSegmentSink,
    AgentSegmentSinkHealth, AgentTelemetrySegment,
};

/// The single source of the pinned revision literal, so the bare revision
/// ([`AGENT_GENAI_CONVENTION_REVISION`]) and the schema URL
/// ([`AGENT_GENAI_SCHEMA_URL`]) are built from one string and can never drift.
macro_rules! genai_convention_revision {
    () => {
        "1.36.0"
    };
}

/// The reviewed OpenTelemetry semantic-convention revision this adapter maps
/// to. An upgrade requires the [specification 17.20](../../../docs/plans/rakka-agent/spec.md)
/// compatibility review; it is never bumped as a side effect.
pub const AGENT_GENAI_CONVENTION_REVISION: &str = genai_convention_revision!();

/// The schema URL pinning [`AGENT_GENAI_CONVENTION_REVISION`], built from the
/// same literal so the URL and the revision stay in lockstep.
pub const AGENT_GENAI_SCHEMA_URL: &str = concat!(
    "https://opentelemetry.io/schemas/",
    genai_convention_revision!()
);

/// Instrumentation scope name for agent telemetry.
pub const AGENT_OTEL_SCOPE_NAME: &str = "rakka.agent";

/// Instrumentation scope version: this crate's version.
pub const AGENT_OTEL_SCOPE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// GenAI attribute: the agent's identity (or its policy pseudonym).
pub const ATTR_GEN_AI_AGENT_ID: &str = "gen_ai.agent.id";
/// GenAI attribute: the bounded configured agent telemetry/template name.
pub const ATTR_GEN_AI_AGENT_NAME: &str = "gen_ai.agent.name";
/// GenAI attribute: the agent definition revision.
pub const ATTR_GEN_AI_AGENT_VERSION: &str = "gen_ai.agent.version";
/// GenAI attribute: the session identity — `AgentRunId`.
pub const ATTR_GEN_AI_CONVERSATION_ID: &str = "gen_ai.conversation.id";
/// GenAI attribute: the well-known operation name.
pub const ATTR_GEN_AI_OPERATION_NAME: &str = "gen_ai.operation.name";
/// GenAI attribute: the provider name.
pub const ATTR_GEN_AI_PROVIDER_NAME: &str = "gen_ai.provider.name";
/// GenAI attribute: the model the request asked for.
///
/// Rakka's model adapter contract is provider-neutral, so the value is the
/// deployment's bounded *model profile* — the name it configured and the run
/// pinned — never a provider's internal model string, which Rakka does not
/// see. The convention's span name for a chat span is
/// `{gen_ai.operation.name} {gen_ai.request.model}`, so a mapping that
/// produced that name without this attribute named a dimension no query could
/// group by.
pub const ATTR_GEN_AI_REQUEST_MODEL: &str = "gen_ai.request.model";
/// GenAI attribute: the tool name from the bounded registry.
pub const ATTR_GEN_AI_TOOL_NAME: &str = "gen_ai.tool.name";
/// GenAI attribute: the tool type from the bounded registry.
pub const ATTR_GEN_AI_TOOL_TYPE: &str = "gen_ai.tool.type";
/// GenAI attribute: input token usage.
pub const ATTR_GEN_AI_USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";
/// GenAI attribute: output token usage.
pub const ATTR_GEN_AI_USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";

/// Restricted Rakka attribute: `AgentGoalId`. Traces and logs only, never a
/// metric label or baggage.
pub const ATTR_RAKKA_AGENT_GOAL_ID: &str = "rakka.agent.goal.id";
/// Restricted Rakka attribute: `AgentTaskId`.
pub const ATTR_RAKKA_AGENT_TASK_ID: &str = "rakka.agent.task.id";
/// Restricted Rakka attribute: `AgentDelegationId`.
pub const ATTR_RAKKA_AGENT_DELEGATION_ID: &str = "rakka.agent.delegation.id";
/// Rakka attribute: the settings revision in force.
pub const ATTR_RAKKA_AGENT_SETTINGS_REVISION: &str =
    crate::observability::SEGMENT_ATTR_SETTINGS_REVISION;
/// Rakka attribute: the turn index.
pub const ATTR_RAKKA_AGENT_TURN_INDEX: &str = "rakka.agent.turn.index";
/// Rakka attribute: the loop phase label.
pub const ATTR_RAKKA_AGENT_LOOP_PHASE: &str = "rakka.agent.loop.phase";
/// Rakka attribute: the decision kind label.
pub const ATTR_RAKKA_AGENT_DECISION_KIND: &str = "rakka.agent.decision.kind";
/// Rakka attribute: the decision source label.
pub const ATTR_RAKKA_AGENT_DECISION_SOURCE: &str = "rakka.agent.decision.source";
/// Rakka attribute: the effect safety-class label.
pub const ATTR_RAKKA_AGENT_EFFECT_SAFETY: &str = "rakka.agent.effect.safety";
/// Rakka attribute: the effect status label.
///
/// Aliased to the ungated segment key rather than repeating the string. The
/// emitting path is not feature-gated and the convention mapping is, so the
/// two would otherwise be separate literals for one key, able to drift
/// without anything noticing — which is how an attribute ends up declared on
/// one side and written on the other under a different name.
pub const ATTR_RAKKA_AGENT_EFFECT_STATUS: &str = crate::observability::SEGMENT_ATTR_EFFECT_STATUS;
/// Rakka attribute: the stable decision reason code.
pub const ATTR_RAKKA_AGENT_DECISION_REASON: &str = "rakka.agent.decision.reason";
/// Standard error attribute: the stable low-cardinality error type.
pub const ATTR_ERROR_TYPE: &str = "error.type";
/// Rakka attribute: the stable Rakka error code.
pub const ATTR_RAKKA_ERROR_CODE: &str = "rakka.error.code";

/// Rakka attribute: the bounded effect kind a dispatch segment names.
pub const ATTR_RAKKA_AGENT_EFFECT_KIND: &str = "rakka.agent.effect.kind";
/// Rakka attribute: which dispatch attempt a segment describes.
pub const ATTR_RAKKA_AGENT_EFFECT_ATTEMPT: &str = crate::observability::SEGMENT_ATTR_EFFECT_ATTEMPT;
/// Rakka attribute: the bounded checkpoint kind a park opened.
pub const ATTR_RAKKA_AGENT_CHECKPOINT_KIND: &str =
    crate::observability::SEGMENT_ATTR_CHECKPOINT_KIND;
/// Rakka attribute: the bounded A2A operation class of an ingress segment.
pub const ATTR_RAKKA_AGENT_A2A_OPERATION: &str = "rakka.agent.a2a.operation";
/// Rakka attribute: the bounded memory tier of a memory segment.
pub const ATTR_RAKKA_AGENT_MEMORY_TIER: &str = "rakka.agent.memory.tier";
/// Rakka attribute: how many loop transitions one resident slice advanced.
pub const ATTR_RAKKA_AGENT_LOOP_TRANSITIONS: &str =
    crate::observability::SEGMENT_ATTR_LOOP_TRANSITIONS;

/// The span event name a mapped loop decision is emitted under.
pub const AGENT_DECISION_SPAN_EVENT: &str = "rakka.agent.decide";

/// Every attribute key this adapter may put on an exported span or log
/// record.
///
/// [Specification 17.14](../../../docs/plans/rakka-agent/spec.md) asks the
/// application to minimize before export and treats Collector processors as
/// defence in depth, not as the first line — and
/// [17.15](../../../docs/plans/rakka-agent/spec.md) makes received baggage
/// untrusted. There is a bounded-label validator for metrics
/// ([`crate::observability::validate_agent_domain_metric_attributes`]) and,
/// before this, none for spans, so anything a caller's context happened to
/// carry reached a span record intact.
///
/// This is that counterpart: a closed allowlist rather than a denylist,
/// because a denylist is a guess about what content will be called next time,
/// and the adapter knows exactly which keys it writes.
pub const AGENT_SPAN_ATTRIBUTE_KEYS: &[&str] = &[
    ATTR_ERROR_TYPE,
    ATTR_GEN_AI_AGENT_ID,
    ATTR_GEN_AI_AGENT_NAME,
    ATTR_GEN_AI_AGENT_VERSION,
    ATTR_GEN_AI_CONVERSATION_ID,
    ATTR_GEN_AI_OPERATION_NAME,
    ATTR_GEN_AI_PROVIDER_NAME,
    ATTR_GEN_AI_REQUEST_MODEL,
    ATTR_GEN_AI_TOOL_NAME,
    ATTR_GEN_AI_TOOL_TYPE,
    ATTR_GEN_AI_USAGE_INPUT_TOKENS,
    ATTR_GEN_AI_USAGE_OUTPUT_TOKENS,
    ATTR_RAKKA_AGENT_A2A_OPERATION,
    ATTR_RAKKA_AGENT_CHECKPOINT_KIND,
    ATTR_RAKKA_AGENT_DECISION_KIND,
    ATTR_RAKKA_AGENT_DECISION_REASON,
    ATTR_RAKKA_AGENT_DECISION_SOURCE,
    ATTR_RAKKA_AGENT_DELEGATION_ID,
    ATTR_RAKKA_AGENT_EFFECT_ATTEMPT,
    ATTR_RAKKA_AGENT_EFFECT_KIND,
    ATTR_RAKKA_AGENT_EFFECT_SAFETY,
    ATTR_RAKKA_AGENT_EFFECT_STATUS,
    ATTR_RAKKA_AGENT_GOAL_ID,
    ATTR_RAKKA_AGENT_LOOP_PHASE,
    ATTR_RAKKA_AGENT_LOOP_TRANSITIONS,
    ATTR_RAKKA_AGENT_MEMORY_TIER,
    ATTR_RAKKA_AGENT_SETTINGS_REVISION,
    ATTR_RAKKA_AGENT_TASK_ID,
    ATTR_RAKKA_AGENT_TURN_INDEX,
    ATTR_RAKKA_ERROR_CODE,
];

/// The most bytes one exported span attribute value may carry.
///
/// Bounding is not sanitizing: a key outside
/// [`AGENT_SPAN_ATTRIBUTE_KEYS`] is refused however short its value, and this
/// bound exists so an allowlisted key cannot smuggle an unbounded payload
/// under a name the allowlist trusts.
pub const AGENT_SPAN_ATTRIBUTE_VALUE_MAX_BYTES: usize = 256;

/// Every attribute key this adapter may put on an exported **log** record.
///
/// A superset of [`AGENT_SPAN_ATTRIBUTE_KEYS`] rather than the same list. A
/// log record legitimately carries the substrate's durable correlation
/// vocabulary — the identities specification 17.13 asks a structured log to
/// carry, which are exactly the identities 17.12 forbids on a *metric* — so
/// applying the span list to logs would strip the audit trail while claiming
/// to redact it.
///
/// The identities here are the ones that belong on an access-controlled log
/// under 17.3; content, credentials, prompts, completions, tool payloads, and
/// memory records appear on neither list and reach neither surface.
pub const AGENT_LOG_ATTRIBUTE_KEYS: &[&str] = &[
    rakka_agent_workflow::AGENT_LOG_ATTR_AUDIT_EVENT_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_AUDIT_KIND,
    rakka_agent_workflow::AGENT_LOG_ATTR_CAUSATION_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_CHECKPOINT_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_COMMAND_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_CORRELATION_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_DEFINITION_VERSION,
    rakka_agent_workflow::AGENT_LOG_ATTR_EFFECT_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_REDACTION,
    rakka_agent_workflow::AGENT_LOG_ATTR_RUN_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_STEP_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_TENANT_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_WORKFLOW_ID,
    rakka_agent_workflow::AGENT_LOG_ATTR_WORKFLOW_TYPE,
];

/// Whether a key is one this adapter may export on a span.
#[must_use]
pub fn is_agent_span_attribute(key: &str) -> bool {
    AGENT_SPAN_ATTRIBUTE_KEYS.contains(&key)
}

/// Whether a key is one this adapter may export on a log record.
#[must_use]
pub fn is_agent_log_attribute(key: &str) -> bool {
    is_agent_span_attribute(key) || AGENT_LOG_ATTRIBUTE_KEYS.contains(&key)
}

/// Keeps only what a log record may carry.
#[must_use]
fn allowlisted_log(attributes: AgentAttributes) -> AgentAttributes {
    attributes
        .into_iter()
        .filter(|(key, value)| {
            is_agent_log_attribute(key)
                && !value.is_empty()
                && value.len() <= AGENT_SPAN_ATTRIBUTE_VALUE_MAX_BYTES
                && !value.contains('\n')
                && !value.contains('\r')
        })
        .collect()
}

/// Keeps what a log record's **resource** may carry.
///
/// The attribute allowlist deliberately does not run here. A resource is not
/// the record's attribute set: it is the identity of the service that emitted
/// it, and the OpenTelemetry keys that carry that identity —
/// [`OTEL_RESOURCE_SERVICE_NAME`](rakka_agent_workflow::OTEL_RESOURCE_SERVICE_NAME)
/// and its siblings — are on neither the span nor the log vocabulary, because
/// neither vocabulary is about resources. Filtering a resource through them
/// deleted **every** key, so records reached the Collector with an empty
/// resource and nothing to attribute them to, while the batch-level
/// [`AgentOtelResource`] beside them travelled unfiltered. Two policies for
/// one kind of data, and the stricter of them applied to the copy that
/// carries the emitter's name.
///
/// What still applies are the generic value bounds, at the width the export
/// validator itself enforces: an oversized or multi-line value is dropped
/// here rather than failing the whole batch there. Bounding is not
/// sanitizing, and a resource needs the first and not the second.
#[must_use]
fn bounded_resource(resource: AgentAttributes) -> AgentAttributes {
    bounded_export_attributes(resource, AGENT_EXPORT_MAX_ATTRIBUTES)
}

/// Applies the log allowlist to one record, in place.
///
/// Filtering rather than refusing, for the same reason the span path filters:
/// a record carrying an unknown key is still worth exporting without it, and
/// dropping the whole record would turn a telemetry mistake into a lost audit
/// correlation.
///
/// The record's attributes pass the allowlist; its resource passes the
/// generic value bounds instead, which is a different question with a
/// different answer — see `bounded_resource`.
#[must_use]
pub fn allowlist_agent_log(mut log: AgentLogEvent) -> AgentLogEvent {
    log.attributes = allowlisted_log(log.attributes);
    log.resource = bounded_resource(log.resource);
    log
}

/// Accepts an exported span or log attribute set, or names the first key it
/// refuses.
///
/// A value is refused when it is empty, exceeds
/// [`AGENT_SPAN_ATTRIBUTE_VALUE_MAX_BYTES`], or carries a line break — the
/// same value rules the metric guard applies, for the same reason.
pub fn validate_agent_span_attributes(attributes: &AgentAttributes) -> Result<(), String> {
    for (key, value) in attributes {
        if !is_agent_span_attribute(key) {
            return Err(key.clone());
        }
        if value.is_empty()
            || value.len() > AGENT_SPAN_ATTRIBUTE_VALUE_MAX_BYTES
            || value.contains('\n')
            || value.contains('\r')
        {
            return Err(key.clone());
        }
    }
    Ok(())
}

/// Keeps only what [`validate_agent_span_attributes`] accepts.
///
/// The exporter filters rather than fails: a segment carrying an unknown key
/// is still worth exporting without it, and refusing the whole span would
/// make a telemetry mistake into a loss of the operation's record.
#[must_use]
fn allowlisted(attributes: AgentAttributes) -> AgentAttributes {
    attributes
        .into_iter()
        .filter(|(key, value)| {
            let mut single = AgentAttributes::new();
            single.insert(key.clone(), value.clone());
            validate_agent_span_attributes(&single).is_ok()
        })
        .collect()
}

/// The instrumentation scope every agent export batch is stamped with.
#[must_use]
pub fn agent_instrumentation_scope() -> AgentOtelInstrumentationScope {
    AgentOtelInstrumentationScope {
        name: AGENT_OTEL_SCOPE_NAME.to_string(),
        version: AGENT_OTEL_SCOPE_VERSION.to_string(),
        schema_url: Some(AGENT_GENAI_SCHEMA_URL.to_string()),
    }
}

/// The M1 rows of the required span model
/// ([specification 17.6](../../../docs/plans/rakka-agent/spec.md)): each
/// operation's stable span name and kind.
///
/// GenAI names are used where the reviewed convention describes the operation
/// (`invoke_agent`, `execute_tool`, the model-inference `{operation} {model}`
/// form); Rakka's durable/runtime operations use stable `rakka.agent.*`
/// names. Every embedded value is a bounded class from a configured registry;
/// the A2A ingress `SERVER` span is the protocol adapter's and is not built
/// here. The coordination rows (handoff, team, moderation, goal evaluation,
/// wake admission) arrive with their phases.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentGenAiOperation {
    /// One bounded active invocation of an agent.
    InvokeAgent {
        /// The bounded configured agent telemetry name, when one exists.
        ///
        /// Absent is the normal case: an `AgentId` is an identifier however
        /// bounded it is, and [17.6](../../../docs/plans/rakka-agent/spec.md)
        /// forbids one in a span name. The identity rides
        /// [`ATTR_GEN_AI_AGENT_ID`] instead.
        agent_name: Option<String>,
    },
    /// Protocol ingress for one A2A operation, extracting context before
    /// durable acceptance.
    A2aIngress {
        /// The bounded A2A operation class.
        operation: String,
    },
    /// Continuous wake and epoch admission.
    WakeAdmit,
    /// An outbound A2A call to a peer agent.
    DelegateToPeer {
        /// The bounded peer or skill class.
        peer_class: String,
    },
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
    ModerationTurn,
    /// A durable workflow-tool invocation.
    WorkflowInvoke,
    /// Goal progress and evidence evaluation.
    GoalEvaluate,
    /// A short-term, private, or communal memory operation.
    MemoryOperation,
    /// A retrieval against an authorized knowledge space.
    Retrieval {
        /// The bounded data-source or backend class.
        data_source: String,
    },
    /// A general loop decision.
    Decide,
    /// A model/provider call executed through the model adapter.
    ModelInference {
        /// The well-known GenAI operation name (usually `chat`).
        operation: String,
        /// The bounded requested model profile, when the deployment configured
        /// one.
        ///
        /// Absent is a normal case, and the *default* one: a deployment that
        /// names no profile leaves the adapter to its own default. The
        /// convention's span name is `{operation} {model}`, so an absent model
        /// makes the name the bare operation — as
        /// [`Self::InvokeAgent`] already does for an absent agent name —
        /// rather than the operation followed by a space.
        model: Option<String>,
    },
    /// Durable acceptance of an effect into the outbox.
    EffectSchedule,
    /// One dispatcher attempt for a scheduled effect.
    EffectDispatch,
    /// The dispatch-time tool authority decision.
    ToolAuthorize,
    /// Application-level execution of a named tool.
    ExecuteTool {
        /// The tool name from the bounded registry.
        tool_name: String,
    },
    /// Opening a durable checkpoint; the span ends at the durable park.
    CheckpointOpen,
    /// Resuming a run after a durable wait.
    RunResume,
    /// Recovering a run after restart, passivation, or shard movement.
    RunRecover,
    /// A fail-closed autonomy admission check.
    AutonomyAdmit,
    /// A dispatch-time budget reservation.
    BudgetReserve,
    /// A terminal budget settlement.
    BudgetSettle,
}

impl AgentGenAiOperation {
    /// The span name of the operation.
    #[must_use]
    pub fn span_name(&self) -> String {
        match self {
            Self::InvokeAgent { agent_name } => match agent_name {
                Some(agent_name) => format!("invoke_agent {agent_name}"),
                None => "invoke_agent".to_string(),
            },
            Self::A2aIngress { .. } => "rakka.agent.a2a.ingress".to_string(),
            Self::WakeAdmit => "rakka.agent.wake.admit".to_string(),
            Self::DelegateToPeer { peer_class } => format!("invoke_agent {peer_class}"),
            Self::ValidateTaskResult => "rakka.agent.task.validate_result".to_string(),
            Self::Handoff => "rakka.agent.handoff".to_string(),
            Self::TeamOperation { operation } => format!("rakka.agent.team.{operation}"),
            Self::ModerationTurn => "rakka.agent.moderation.turn".to_string(),
            Self::WorkflowInvoke => "rakka.agent.workflow.invoke".to_string(),
            Self::GoalEvaluate => "rakka.agent.goal.evaluate".to_string(),
            Self::MemoryOperation => "rakka.agent.memory.operation".to_string(),
            Self::Retrieval { data_source } => format!("retrieval {data_source}"),
            Self::Decide => "rakka.agent.decide".to_string(),
            Self::ModelInference { operation, model } => match model {
                Some(model) => format!("{operation} {model}"),
                None => operation.clone(),
            },
            Self::EffectSchedule => "rakka.agent.effect.schedule".to_string(),
            Self::EffectDispatch => "rakka.agent.effect.dispatch".to_string(),
            Self::ToolAuthorize => "rakka.agent.tool.authorize".to_string(),
            Self::ExecuteTool { tool_name } => format!("execute_tool {tool_name}"),
            Self::CheckpointOpen => "rakka.agent.checkpoint.open".to_string(),
            Self::RunResume => "rakka.agent.run.resume".to_string(),
            Self::RunRecover => "rakka.agent.run.recover".to_string(),
            Self::AutonomyAdmit => "rakka.agent.autonomy.admit".to_string(),
            Self::BudgetReserve => "rakka.agent.budget.reserve".to_string(),
            Self::BudgetSettle => "rakka.agent.budget.settle".to_string(),
        }
    }

    /// The value this operation contributes to `gen_ai.operation.name`.
    ///
    /// Where the reviewed convention defines a well-known value it is used;
    /// where it does not — every durable Rakka operation — the stable
    /// `rakka.agent.*` class is used instead, because inventing a
    /// convention value would make an upgrade's compatibility review
    /// impossible to perform honestly.
    #[must_use]
    pub fn operation_name(&self) -> String {
        match self {
            Self::InvokeAgent { .. } | Self::DelegateToPeer { .. } => "invoke_agent".to_string(),
            Self::ModelInference { operation, .. } => operation.clone(),
            Self::ExecuteTool { .. } => "execute_tool".to_string(),
            Self::Retrieval { .. } => "retrieval".to_string(),
            other => other.span_name(),
        }
    }

    /// The span kind of the operation.
    ///
    /// The schedule/dispatch pair is `PRODUCER`/`CONSUMER` because the durable
    /// outbox boundary is an asynchronous handoff; the model call is `CLIENT`
    /// toward the provider; everything else in M1 is `INTERNAL`.
    #[must_use]
    pub const fn span_kind(&self) -> AgentOtelSpanKind {
        match self {
            Self::A2aIngress { .. } => AgentOtelSpanKind::Server,
            Self::ModelInference { .. } | Self::DelegateToPeer { .. } => AgentOtelSpanKind::Client,
            Self::EffectSchedule | Self::Handoff => AgentOtelSpanKind::Producer,
            Self::EffectDispatch => AgentOtelSpanKind::Consumer,
            Self::InvokeAgent { .. }
            | Self::Decide
            | Self::ToolAuthorize
            | Self::ExecuteTool { .. }
            | Self::CheckpointOpen
            | Self::RunResume
            | Self::RunRecover
            | Self::AutonomyAdmit
            | Self::BudgetReserve
            | Self::BudgetSettle
            | Self::WakeAdmit
            | Self::ValidateTaskResult
            | Self::TeamOperation { .. }
            | Self::ModerationTurn
            | Self::WorkflowInvoke
            | Self::GoalEvaluate
            | Self::MemoryOperation
            | Self::Retrieval { .. } => AgentOtelSpanKind::Internal,
        }
    }

    /// Builds the bridge span record for one bounded segment of this
    /// operation, with the name and kind set and the persisted context's
    /// links carried over.
    pub fn span(
        &self,
        start_time: AgentTimestampMillis,
        end_time: AgentTimestampMillis,
        telemetry: &AgentTelemetryContext,
    ) -> AgentOtlpResult<AgentOtelSpanExport> {
        Ok(AgentOtelSpanExport::from_telemetry_context(
            self.span_name(),
            start_time,
            end_time,
            telemetry,
        )?
        .kind(self.span_kind()))
    }
}

/// Maps one Rakka bounded operation class to its convention row.
///
/// This is the whole point of the module and, before slice 6.3a, the thing it
/// had no caller for: the operations above were reachable only from the
/// module's own tests. The mapping is total over
/// [`AgentSegmentOperation`], matched without a wildcard, so a class added to
/// the Rakka vocabulary fails to compile until its convention row is decided
/// — which is the [17.20](../../../docs/plans/rakka-agent/spec.md) review
/// happening at the compiler rather than in a checklist.
#[must_use]
pub fn genai_operation(operation: &AgentSegmentOperation) -> AgentGenAiOperation {
    match operation {
        AgentSegmentOperation::A2aIngress { operation } => AgentGenAiOperation::A2aIngress {
            operation: operation.clone(),
        },
        AgentSegmentOperation::Decide { .. } => AgentGenAiOperation::Decide,
        AgentSegmentOperation::InvokeAgent { agent_name } => AgentGenAiOperation::InvokeAgent {
            agent_name: agent_name.clone(),
        },
        AgentSegmentOperation::ModelInference { model_profile } => {
            AgentGenAiOperation::ModelInference {
                // The well-known GenAI operation name for a completion; Rakka's
                // model adapter contract is a single bounded turn, which is
                // what `chat` describes.
                operation: GEN_AI_OPERATION_CHAT.to_string(),
                model: model_profile.clone(),
            }
        }
        AgentSegmentOperation::EffectSchedule { .. } => AgentGenAiOperation::EffectSchedule,
        AgentSegmentOperation::EffectDispatch { .. } => AgentGenAiOperation::EffectDispatch,
        AgentSegmentOperation::ToolAuthorize { .. } => AgentGenAiOperation::ToolAuthorize,
        AgentSegmentOperation::ExecuteTool { tool_name } => AgentGenAiOperation::ExecuteTool {
            tool_name: tool_name.clone(),
        },
        AgentSegmentOperation::DelegateToPeer { peer_class } => {
            AgentGenAiOperation::DelegateToPeer {
                peer_class: peer_class.clone(),
            }
        }
        AgentSegmentOperation::WorkflowInvoke { .. } => AgentGenAiOperation::WorkflowInvoke,
        AgentSegmentOperation::GoalEvaluate => AgentGenAiOperation::GoalEvaluate,
        AgentSegmentOperation::ValidateTaskResult => AgentGenAiOperation::ValidateTaskResult,
        AgentSegmentOperation::Handoff => AgentGenAiOperation::Handoff,
        AgentSegmentOperation::TeamOperation { operation } => AgentGenAiOperation::TeamOperation {
            operation: operation.clone(),
        },
        AgentSegmentOperation::ModerationTurn { .. } => AgentGenAiOperation::ModerationTurn,
        AgentSegmentOperation::WakeAdmit => AgentGenAiOperation::WakeAdmit,
        AgentSegmentOperation::AutonomyAdmit => AgentGenAiOperation::AutonomyAdmit,
        AgentSegmentOperation::BudgetReserve => AgentGenAiOperation::BudgetReserve,
        AgentSegmentOperation::BudgetSettle => AgentGenAiOperation::BudgetSettle,
        AgentSegmentOperation::MemoryOperation { .. } => AgentGenAiOperation::MemoryOperation,
        AgentSegmentOperation::Retrieval { backend } => AgentGenAiOperation::Retrieval {
            data_source: backend.clone(),
        },
        AgentSegmentOperation::CheckpointOpen => AgentGenAiOperation::CheckpointOpen,
        AgentSegmentOperation::RunResume => AgentGenAiOperation::RunResume,
        AgentSegmentOperation::RunRecover => AgentGenAiOperation::RunRecover,
    }
}

/// The well-known GenAI operation name a Rakka model call maps to.
pub const GEN_AI_OPERATION_CHAT: &str = "chat";

/// The provider class Rakka reports when the deployment named none.
///
/// [17.8](../../../docs/plans/rakka-agent/spec.md) asks for the provider when
/// it is "supplied by the provider or safely known". Rakka's model adapter
/// contract is provider-neutral by design — the deploying application supplies
/// the concrete client — so the honest value is the adapter boundary itself,
/// never a guess at whoever is behind it.
pub const GEN_AI_PROVIDER_RAKKA_ADAPTER: &str = "rakka.model_adapter";

/// The attributes one segment's bounded class contributes.
fn operation_attributes(segment: &AgentTelemetrySegment) -> AgentAttributes {
    let mut attributes = AgentAttributes::new();
    let genai = genai_operation(&segment.operation);
    attributes.insert(
        ATTR_GEN_AI_OPERATION_NAME.to_string(),
        genai.operation_name().to_string(),
    );
    match &segment.operation {
        AgentSegmentOperation::ModelInference { model_profile } => {
            attributes.insert(
                ATTR_GEN_AI_PROVIDER_NAME.to_string(),
                GEN_AI_PROVIDER_RAKKA_ADAPTER.to_string(),
            );
            // The model profile belongs to `gen_ai.request.model`, which is
            // the key the convention's own span name for a chat span is built
            // from. It used to be written to `gen_ai.agent.version` — the key
            // documented as the *agent definition revision*, and the key
            // `AgentGenAiIdentity` writes that revision to — so one dimension
            // held two unrelated vocabularies and a dashboard grouping by it
            // mixed model profiles with agent revisions.
            if let Some(model_profile) = model_profile {
                attributes.insert(ATTR_GEN_AI_REQUEST_MODEL.to_string(), model_profile.clone());
            }
        }
        AgentSegmentOperation::ExecuteTool { tool_name } => {
            attributes.insert(ATTR_GEN_AI_TOOL_NAME.to_string(), tool_name.clone());
            attributes.insert(
                ATTR_GEN_AI_TOOL_TYPE.to_string(),
                GEN_AI_TOOL_TYPE_FUNCTION.to_string(),
            );
        }
        AgentSegmentOperation::EffectSchedule { effect_kind }
        | AgentSegmentOperation::EffectDispatch { effect_kind }
        | AgentSegmentOperation::ToolAuthorize { effect_kind } => {
            attributes.insert(
                ATTR_RAKKA_AGENT_EFFECT_KIND.to_string(),
                (*effect_kind).to_string(),
            );
        }
        AgentSegmentOperation::A2aIngress { operation } => {
            attributes.insert(
                ATTR_RAKKA_AGENT_A2A_OPERATION.to_string(),
                operation.clone(),
            );
        }
        AgentSegmentOperation::Decide { phase } => {
            attributes.insert(
                ATTR_RAKKA_AGENT_LOOP_PHASE.to_string(),
                (*phase).to_string(),
            );
        }
        AgentSegmentOperation::MemoryOperation { tier } => {
            attributes.insert(
                ATTR_RAKKA_AGENT_MEMORY_TIER.to_string(),
                (*tier).to_string(),
            );
        }
        _ => {}
    }
    attributes
}

/// The tool type Rakka reports for a dispatched tool call.
pub const GEN_AI_TOOL_TYPE_FUNCTION: &str = "function";

/// Builds the convention span record for one ended segment.
///
/// Status and error mapping happen here and nowhere else: the segment's
/// outcome becomes the span status, and a failure's stable type and Rakka code
/// become [`ATTR_ERROR_TYPE`] and [`ATTR_RAKKA_ERROR_CODE`] rather than an
/// unbounded message, as [17.6](../../../docs/plans/rakka-agent/spec.md)
/// requires. Every attribute passes the allowlist before it reaches the
/// record, so an attribute outside
/// [`AGENT_SPAN_ATTRIBUTE_KEYS`] cannot be exported however it got onto the
/// segment or into the context the segment carries.
pub fn segment_span(segment: &AgentTelemetrySegment) -> AgentOtlpResult<AgentOtelSpanExport> {
    let genai = genai_operation(&segment.operation);
    let mut span = genai.span(segment.start, segment.end, &segment.telemetry)?;

    let mut attributes = operation_attributes(segment);
    attributes.extend(segment.attributes.clone());
    attributes.extend(identity_of(&segment.identity).attributes());
    match segment.outcome {
        AgentSegmentOutcome::Ok => span.status = AgentOtelSpanStatus::Ok,
        AgentSegmentOutcome::Error => {
            span.status = AgentOtelSpanStatus::Error;
            if let Some(error_type) = segment.error_type {
                attributes.insert(ATTR_ERROR_TYPE.to_string(), error_type.to_string());
            }
            if let Some(code) = &segment.error_code {
                attributes.insert(ATTR_RAKKA_ERROR_CODE.to_string(), code.clone());
            }
        }
        AgentSegmentOutcome::Unset => span.status = AgentOtelSpanStatus::Unset,
    }

    // Provider-reported usage, when the segment carried it. `usage_attributes`
    // maps only what the provider actually reported, which is why the segment
    // holds an `Option` rather than a zeroed struct.
    if let Some(usage) = &segment.usage {
        attributes.extend(usage_attributes(usage));
    }

    // The bridge copies nothing into attributes on its own any more, so this
    // set is exactly what the adapter decided to export.
    span.attributes = allowlisted(attributes);

    // Every durable decision the operation committed becomes a bounded span
    // event ([specification 17.7](../../../docs/plans/rakka-agent/spec.md)
    // allows a decision span *or* span event; an event on the transition that
    // made the decision is the one that needs no correlation to be useful).
    for decision in &segment.decisions {
        let mut event = decision_span_event(decision);
        event.attributes = allowlisted(event.attributes);
        span = span.event(event);
    }

    // The record is complete, so its span id is re-derived over everything it
    // carries rather than over the name and time window alone. Without this a
    // run's `decide` spans — same name, same operation, same millisecond —
    // would be one span to a backend. Two records that are *still* identical
    // are separated by the emitting sink, which knows they are two.
    Ok(span.with_derived_span_id(&[]))
}

/// The convention identity of one segment's durable identities.
fn identity_of(identity: &crate::observability::AgentSegmentIdentity) -> AgentGenAiIdentity {
    AgentGenAiIdentity {
        agent_id: identity.agent.clone(),
        agent_name: None,
        agent_version: None,
        conversation_id: identity.run.clone(),
        goal_id: identity.goal.clone(),
        task_id: identity.task.clone(),
        delegation_id: identity.delegation.clone(),
    }
}

/// The identity attributes of one session, mapped per
/// [specification 17.3](../../../docs/plans/rakka-agent/spec.md).
///
/// Values are supplied by the caller under its telemetry access policy: raw
/// identifiers where policy allows them, stable scoped pseudonyms where it
/// does not. The reversible mapping, if any, never enters telemetry. These
/// attributes belong on access-controlled traces and logs; none of them may
/// label a metric or ride baggage.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentGenAiIdentity {
    /// `AgentId` or its pseudonym.
    pub agent_id: Option<String>,
    /// The bounded configured agent telemetry/template name.
    pub agent_name: Option<String>,
    /// The agent definition revision.
    pub agent_version: Option<String>,
    /// `AgentRunId` or its pseudonym — the session identity.
    pub conversation_id: Option<String>,
    /// `AgentGoalId` or its pseudonym.
    pub goal_id: Option<String>,
    /// `AgentTaskId` or its pseudonym.
    pub task_id: Option<String>,
    /// `AgentDelegationId` or its pseudonym.
    pub delegation_id: Option<String>,
}

impl AgentGenAiIdentity {
    /// The convention attributes this identity maps to.
    #[must_use]
    pub fn attributes(&self) -> AgentAttributes {
        let mut attributes = AgentAttributes::new();
        let mut put = |key: &str, value: &Option<String>| {
            if let Some(value) = value {
                attributes.insert(key.to_string(), value.clone());
            }
        };
        put(ATTR_GEN_AI_AGENT_ID, &self.agent_id);
        put(ATTR_GEN_AI_AGENT_NAME, &self.agent_name);
        put(ATTR_GEN_AI_AGENT_VERSION, &self.agent_version);
        put(ATTR_GEN_AI_CONVERSATION_ID, &self.conversation_id);
        put(ATTR_RAKKA_AGENT_GOAL_ID, &self.goal_id);
        put(ATTR_RAKKA_AGENT_TASK_ID, &self.task_id);
        put(ATTR_RAKKA_AGENT_DELEGATION_ID, &self.delegation_id);
        attributes
    }
}

/// Maps one structured loop decision to its bounded span event
/// ([specification 17.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// The event carries the decision's bounded labels and revisions — kind,
/// source, turn, phase, safety class, reason code — and nothing else: no
/// identifier (identity rides the enclosing span under the caller's access
/// policy), no model text, no tool payload, no hidden reasoning.
#[must_use]
pub fn decision_span_event(decision: &AgentDecisionEvent) -> AgentOtelSpanEvent {
    let mut attributes = AgentAttributes::new();
    attributes.insert(
        ATTR_RAKKA_AGENT_DECISION_KIND.to_string(),
        decision.kind.as_label().to_string(),
    );
    attributes.insert(
        ATTR_RAKKA_AGENT_DECISION_SOURCE.to_string(),
        decision.source.as_label().to_string(),
    );
    attributes.insert(
        ATTR_RAKKA_AGENT_TURN_INDEX.to_string(),
        decision.turn.to_string(),
    );
    attributes.insert(
        ATTR_RAKKA_AGENT_LOOP_PHASE.to_string(),
        decision.phase.as_label().to_string(),
    );
    attributes.insert(
        ATTR_RAKKA_AGENT_SETTINGS_REVISION.to_string(),
        decision.settings_revision.to_string(),
    );
    if let Some(class) = decision.safety_class {
        attributes.insert(
            ATTR_RAKKA_AGENT_EFFECT_SAFETY.to_string(),
            class.as_label().to_string(),
        );
    }
    if let Some(reason) = &decision.reason_code {
        attributes.insert(ATTR_RAKKA_AGENT_DECISION_REASON.to_string(), reason.clone());
    }
    AgentOtelSpanEvent {
        name: AGENT_DECISION_SPAN_EVENT.to_string(),
        time: decision.occurred_at,
        attributes,
    }
}

/// Maps provider-reported token usage to the convention's usage attributes.
///
/// Only what the provider actually reported is mapped
/// ([specification 17.8](../../../docs/plans/rakka-agent/spec.md): never
/// invent token usage); cost stays out of standard attributes and out of
/// metric labels.
///
/// A direction reporting zero is *omitted* rather than written as `"0"`, for
/// the reason the token histogram skips it: [`AgentModelUsage`] carries plain
/// counts, so at this boundary "the provider said nothing" and "the provider
/// said none" are the same value, and writing one of them as a figure claims
/// evidence there is none. Both convention attributes are optional, so an
/// absent one is the convention-correct way to say so.
#[must_use]
pub fn usage_attributes(usage: &AgentModelUsage) -> AgentAttributes {
    let mut attributes = AgentAttributes::new();
    for (key, tokens) in [
        (ATTR_GEN_AI_USAGE_INPUT_TOKENS, usage.input_tokens),
        (ATTR_GEN_AI_USAGE_OUTPUT_TOKENS, usage.output_tokens),
    ] {
        if tokens > 0 {
            attributes.insert(key.to_string(), tokens.to_string());
        }
    }
    attributes
}

/// How many mapped spans an exporter buffers before it starts dropping.
pub const DEFAULT_AGENT_SPAN_BUFFER_CAPACITY: usize = 512;

/// The production call site of this module: a segment sink that maps every
/// ended operation into a convention span record and buffers it for export.
///
/// The `otel` module used to have none. Its operations, its identity mapping,
/// and its decision event were reachable only from its own test block, and
/// nothing in the workspace built an [`AgentOtlpBridgeExport`] outside tests —
/// the same shape slice 6.1 found in the five sharding registrations, with the
/// same corrective: a call site on the path a run actually takes.
///
/// Wire it with `with_segments` on a run entity, its sharding settings, or the
/// dispatcher, and the loop, the entities, and the dispatch attempts all flow
/// through here.
///
/// **Bounded, lossy, and never blocking.** [17.1](../../../docs/plans/rakka-agent/spec.md)
/// requires export to be bounded and forbids unbounded in-process queues, and
/// [`AgentSegmentSink::record`] may neither block nor fail. So the buffer is a
/// ring: at capacity the *oldest* span is dropped and counted, the way a run's
/// decision outbox drops its oldest owed event. Dropping the oldest rather
/// than the newest keeps the most recent operations — the ones an incident is
/// usually about — and the drop count is what makes the loss visible.
///
/// The exporter owns no OTLP transport, no credentials, and no SDK: it
/// produces the serializable bridge record and the application boundary sends
/// it ([17.17](../../../docs/plans/rakka-agent/spec.md)).
pub struct AgentGenAiSpanExporter {
    spans: Mutex<VecDeque<AgentOtelSpanExport>>,
    capacity: usize,
    dropped: AtomicU64,
    unmappable: AtomicU64,
    emitted: AtomicU64,
    metrics: Option<Arc<dyn MetricsRecorder>>,
    health: AgentSegmentSinkHealth,
}

impl fmt::Debug for AgentGenAiSpanExporter {
    /// Hand-written for the same reason
    /// [`crate::observability::InMemoryAgentSegmentSink`]'s is: a
    /// `MetricsRecorder` is a caller-supplied trait object with no `Debug`
    /// bound.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentGenAiSpanExporter")
            .field("buffered", &self.buffered())
            .field("capacity", &self.capacity)
            .field("dropped", &self.dropped())
            .field("unmappable", &self.unmappable())
            .field("metrics", &self.metrics.is_some())
            .finish()
    }
}

impl Default for AgentGenAiSpanExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentGenAiSpanExporter {
    /// An exporter buffering [`DEFAULT_AGENT_SPAN_BUFFER_CAPACITY`] spans.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_AGENT_SPAN_BUFFER_CAPACITY)
    }

    /// An exporter with an explicit buffer bound.
    ///
    /// A capacity of zero is raised to one: a ring that can hold nothing would
    /// drop every span while reporting a healthy pipeline.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            spans: Mutex::new(VecDeque::new()),
            capacity: capacity.max(1),
            dropped: AtomicU64::new(0),
            unmappable: AtomicU64::new(0),
            emitted: AtomicU64::new(0),
            metrics: None,
            health: AgentSegmentSinkHealth::new(),
        }
    }

    /// Publishes this exporter's buffer depth and loss to `metrics`.
    ///
    /// Publication happens on [`Self::bridge_export`] — the exporter's one
    /// natural periodic point — rather than on `record`, so the three metric
    /// writes ride the flush and not every closed segment. That also makes the
    /// gauge read the depth the flush is about to clear, which is the number
    /// an operator sizing a buffer wants.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// How many mapped spans are waiting to be exported.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.spans.lock().map(|spans| spans.len()).unwrap_or(0)
    }

    /// How many mapped spans the bounded buffer dropped.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::SeqCst)
    }

    /// How many segments produced no exportable span.
    ///
    /// A segment whose durable context carries no `traceparent` has no trace
    /// to belong to, and inventing one would fabricate a causal claim. Those
    /// segments are counted here rather than exported under a made-up trace,
    /// and so are the records that map but cannot pass
    /// [`AgentOtelSpanExport::validate`] — they are refused at the door
    /// rather than admitted to a buffer that is only cleared on a successful
    /// flush, where one of them would strand every span queued behind it.
    #[must_use]
    pub fn unmappable(&self) -> u64 {
        self.unmappable.load(Ordering::SeqCst)
    }

    /// Takes every buffered span, leaving the buffer empty.
    #[must_use]
    pub fn drain(&self) -> Vec<AgentOtelSpanExport> {
        self.spans
            .lock()
            .map(|mut spans| spans.drain(..).collect())
            .unwrap_or_default()
    }

    /// Builds one OTLP bridge export from the buffered spans, the given
    /// metrics snapshot, and the given logs.
    ///
    /// The batch is stamped with [`agent_instrumentation_scope`], so the
    /// pinned convention revision travels with the data
    /// ([17.2](../../../docs/plans/rakka-agent/spec.md)), and the metrics
    /// carry the agent catalogue's units and buckets rather than being
    /// exported unitless.
    pub fn bridge_export(
        &self,
        exporter: AgentOtlpExporterConfig,
        resource: AgentOtelResource,
        metrics: &MetricsSnapshot,
        logs: Vec<AgentLogEvent>,
    ) -> AgentOtlpResult<AgentOtlpBridgeExport> {
        if let Some(metrics) = self.metrics.as_ref() {
            self.health.publish(
                metrics.as_ref(),
                self.backend_name(),
                self.buffered(),
                self.dropped(),
                self.unmappable(),
            );
        }
        let instruments = crate::observability::agent_domain_instrument_views();
        // Logs pass the allowlist here rather than at their emitter, because
        // this is the boundary they leave Rakka at and a caller may hand in
        // records this crate did not build.
        let logs = logs.into_iter().map(allowlist_agent_log).collect();

        // The buffer is read under its lock and emptied only once the batch
        // has been built. Passing `self.drain()` as an argument emptied it
        // *before* the callee's validation ran, so a blank endpoint or one
        // caller-supplied log that failed its bounds destroyed up to a full
        // buffer of already-mapped spans — unrecoverably, and while
        // `buffered()` and `dropped()` both reported a clean pipeline. Now a
        // failed flush leaves the spans where they were, and the next flush
        // with a working configuration ships them.
        let mut buffered = self.spans.lock().ok();
        let staged = buffered
            .as_ref()
            .map(|spans| spans.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        let export = AgentOtlpBridgeExport::from_signals_with_instruments(
            exporter,
            resource,
            metrics,
            &instruments,
            staged,
            logs,
        )?
        .with_scope(agent_instrumentation_scope());
        if let Some(spans) = buffered.as_mut() {
            spans.clear();
        }
        Ok(export)
    }
}

impl AgentSegmentSink for AgentGenAiSpanExporter {
    fn backend_name(&self) -> &'static str {
        "otlp-bridge"
    }

    fn record(&self, segment: &AgentTelemetrySegment) {
        let Ok(span) = segment_span(segment) else {
            self.unmappable.fetch_add(1, Ordering::SeqCst);
            return;
        };
        // Two closed operations can be identical in every mapped field: a run
        // schedules two different effects, of the same kind, in the same
        // millisecond, and the effect's identity is not a span attribute. The
        // sink is the one thing that knows they are two records rather than
        // one, so its emission ordinal is what separates their ids.
        let emission = self.emitted.fetch_add(1, Ordering::SeqCst).to_string();
        let span = span.with_derived_span_id(&[emission.as_str()]);
        // A record that cannot pass export validation must not enter the ring
        // at all. The buffer is now cleared only on a *successful* flush, so
        // admitting one would fail every later flush and strand every span
        // queued behind it — the mapping half of the same failure the drain
        // ordering used to cause.
        if span.validate().is_err() {
            self.unmappable.fetch_add(1, Ordering::SeqCst);
            return;
        }
        let Ok(mut spans) = self.spans.lock() else {
            self.dropped.fetch_add(1, Ordering::SeqCst);
            return;
        };
        while spans.len() >= self.capacity {
            spans.pop_front();
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
        spans.push_back(span);
    }
}

#[cfg(test)]
mod tests {
    use rakka_agent_workflow::AgentOtelSpanStatus;

    use super::*;

    #[test]
    fn the_scope_pins_the_reviewed_convention_revision() {
        let scope = agent_instrumentation_scope();
        assert_eq!(scope.name, AGENT_OTEL_SCOPE_NAME);
        assert!(!scope.version.is_empty());
        scope.validate().expect("the scope is valid");
        let schema_url = scope.schema_url.expect("the schema url is pinned");
        assert!(
            schema_url.ends_with(AGENT_GENAI_CONVENTION_REVISION),
            "the schema url and the pinned revision must agree"
        );
    }

    #[test]
    fn the_m1_span_rows_use_the_required_names_and_kinds() {
        let rows: &[(AgentGenAiOperation, &str, AgentOtelSpanKind)] = &[
            (
                AgentGenAiOperation::InvokeAgent {
                    agent_name: Some("support".to_string()),
                },
                "invoke_agent support",
                AgentOtelSpanKind::Internal,
            ),
            // Rakka itself supplies no name — an `AgentId` is an identifier
            // however bounded it is, and 17.6 forbids one in a span name — so
            // the nameless form is the one a run actually produces.
            (
                AgentGenAiOperation::InvokeAgent { agent_name: None },
                "invoke_agent",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::Decide,
                "rakka.agent.decide",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::ModelInference {
                    operation: "chat".to_string(),
                    model: Some("gpt-x".to_string()),
                },
                "chat gpt-x",
                AgentOtelSpanKind::Client,
            ),
            (
                AgentGenAiOperation::EffectSchedule,
                "rakka.agent.effect.schedule",
                AgentOtelSpanKind::Producer,
            ),
            (
                AgentGenAiOperation::EffectDispatch,
                "rakka.agent.effect.dispatch",
                AgentOtelSpanKind::Consumer,
            ),
            (
                AgentGenAiOperation::ToolAuthorize,
                "rakka.agent.tool.authorize",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::ExecuteTool {
                    tool_name: "lookup".to_string(),
                },
                "execute_tool lookup",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::CheckpointOpen,
                "rakka.agent.checkpoint.open",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::RunResume,
                "rakka.agent.run.resume",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::RunRecover,
                "rakka.agent.run.recover",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::AutonomyAdmit,
                "rakka.agent.autonomy.admit",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::BudgetReserve,
                "rakka.agent.budget.reserve",
                AgentOtelSpanKind::Internal,
            ),
            (
                AgentGenAiOperation::BudgetSettle,
                "rakka.agent.budget.settle",
                AgentOtelSpanKind::Internal,
            ),
        ];
        for (operation, name, kind) in rows {
            assert_eq!(&operation.span_name(), name);
            assert_eq!(&operation.span_kind(), kind);
        }
    }

    #[test]
    fn a_span_builds_over_the_persisted_context_with_kind_and_links() {
        let telemetry = AgentTelemetryContext {
            trace_parent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            ..AgentTelemetryContext::default()
        };
        let span = AgentGenAiOperation::EffectDispatch
            .span(
                AgentTimestampMillis::new(1),
                AgentTimestampMillis::new(2),
                &telemetry,
            )
            .expect("the span builds");
        assert_eq!(span.kind, AgentOtelSpanKind::Consumer);
        assert_eq!(span.status, AgentOtelSpanStatus::Unset);
        assert_eq!(span.trace_id, "0af7651916cd43dd8448eb211c80319c");
        span.validate().expect("the span record is valid");
    }

    #[test]
    fn a_decision_maps_to_a_bounded_span_event_without_identifiers() {
        let scope = crate::identity::AgentRunScope::new(
            crate::identity::TenantId::new("acme"),
            crate::identity::AgentId::new("support").expect("the agent id is valid"),
            crate::identity::AgentRunId::new("run-1").expect("the run id is valid"),
        )
        .expect("the scope is valid");
        let decision = AgentDecisionEvent::assemble(
            &scope,
            None,
            None,
            1,
            1,
            crate::loop_runtime::AgentLoopPhase::DecidingContinuation,
            crate::definition::AgentRevisionNumber::INITIAL,
            crate::definition::AgentRevisionNumber::INITIAL,
            None,
            AgentTelemetryContext::default(),
            crate::observability::AgentDecisionDraft::new(
                crate::observability::AgentDecisionKind::SubmitResult,
                crate::observability::AgentDecisionSource::Model,
                "proposal",
            )
            .with_reason_code("proposal-submitted"),
            AgentTimestampMillis::new(7),
        )
        .expect("the decision assembles");

        let event = decision_span_event(&decision);
        assert_eq!(event.name, AGENT_DECISION_SPAN_EVENT);
        assert_eq!(event.time, AgentTimestampMillis::new(7));
        assert_eq!(
            event
                .attributes
                .get(ATTR_RAKKA_AGENT_DECISION_KIND)
                .map(String::as_str),
            Some("submit-result")
        );
        assert_eq!(
            event
                .attributes
                .get(ATTR_RAKKA_AGENT_DECISION_SOURCE)
                .map(String::as_str),
            Some("model")
        );
        // Identity rides the enclosing span under the caller's access policy;
        // the event itself names no id, and no key smuggles content.
        for key in event.attributes.keys() {
            assert!(
                key.starts_with("rakka.agent."),
                "{key} is outside the bounded decision-event vocabulary"
            );
            assert!(!key.ends_with(".id"), "{key} must not carry an identifier");
        }
    }

    #[test]
    fn the_identity_maps_to_the_convention_keys() {
        let identity = AgentGenAiIdentity {
            agent_id: Some("agent-1".to_string()),
            agent_name: Some("support".to_string()),
            agent_version: Some("3".to_string()),
            conversation_id: Some("run-1".to_string()),
            goal_id: None,
            task_id: Some("ticket-1".to_string()),
            delegation_id: None,
        };
        let attributes = identity.attributes();
        assert_eq!(
            attributes.get(ATTR_GEN_AI_AGENT_ID).map(String::as_str),
            Some("agent-1")
        );
        assert_eq!(
            attributes
                .get(ATTR_GEN_AI_CONVERSATION_ID)
                .map(String::as_str),
            Some("run-1")
        );
        assert_eq!(
            attributes.get(ATTR_RAKKA_AGENT_TASK_ID).map(String::as_str),
            Some("ticket-1")
        );
        assert!(
            !attributes.contains_key(ATTR_RAKKA_AGENT_GOAL_ID),
            "an absent identity maps to an absent attribute, never an empty one"
        );
    }
}
