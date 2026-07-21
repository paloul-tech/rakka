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

use rakka_agent_workflow::{
    AgentAttributes, AgentOtelInstrumentationScope, AgentOtelSpanEvent, AgentOtelSpanExport,
    AgentOtelSpanKind, AgentOtlpResult, AgentTelemetryContext, AgentTimestampMillis,
};

use crate::model::AgentModelUsage;
use crate::observability::AgentDecisionEvent;

/// The reviewed OpenTelemetry semantic-convention revision this adapter maps
/// to. An upgrade requires the [specification 17.20](../../../docs/plans/rakka-agent/spec.md)
/// compatibility review; it is never bumped as a side effect.
pub const AGENT_GENAI_CONVENTION_REVISION: &str = "1.36.0";

/// The schema URL pinning [`AGENT_GENAI_CONVENTION_REVISION`].
pub const AGENT_GENAI_SCHEMA_URL: &str = "https://opentelemetry.io/schemas/1.36.0";

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
pub const ATTR_RAKKA_AGENT_SETTINGS_REVISION: &str = "rakka.agent.settings_revision";
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
pub const ATTR_RAKKA_AGENT_EFFECT_STATUS: &str = "rakka.agent.effect.status";
/// Rakka attribute: the stable decision reason code.
pub const ATTR_RAKKA_AGENT_DECISION_REASON: &str = "rakka.agent.decision.reason";
/// Standard error attribute: the stable low-cardinality error type.
pub const ATTR_ERROR_TYPE: &str = "error.type";
/// Rakka attribute: the stable Rakka error code.
pub const ATTR_RAKKA_ERROR_CODE: &str = "rakka.error.code";

/// The span event name a mapped loop decision is emitted under.
pub const AGENT_DECISION_SPAN_EVENT: &str = "rakka.agent.decide";

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
    /// One bounded active turn of a named agent.
    InvokeAgent {
        /// The bounded configured agent telemetry name.
        agent_name: String,
    },
    /// A general loop decision.
    Decide,
    /// A model/provider call executed through the model adapter.
    ModelInference {
        /// The well-known GenAI operation name (usually `chat`).
        operation: String,
        /// The bounded requested model name.
        model: String,
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
            Self::InvokeAgent { agent_name } => format!("invoke_agent {agent_name}"),
            Self::Decide => "rakka.agent.decide".to_string(),
            Self::ModelInference { operation, model } => format!("{operation} {model}"),
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

    /// The span kind of the operation.
    ///
    /// The schedule/dispatch pair is `PRODUCER`/`CONSUMER` because the durable
    /// outbox boundary is an asynchronous handoff; the model call is `CLIENT`
    /// toward the provider; everything else in M1 is `INTERNAL`.
    #[must_use]
    pub const fn span_kind(&self) -> AgentOtelSpanKind {
        match self {
            Self::ModelInference { .. } => AgentOtelSpanKind::Client,
            Self::EffectSchedule => AgentOtelSpanKind::Producer,
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
            | Self::BudgetSettle => AgentOtelSpanKind::Internal,
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
#[must_use]
pub fn usage_attributes(usage: &AgentModelUsage) -> AgentAttributes {
    let mut attributes = AgentAttributes::new();
    attributes.insert(
        ATTR_GEN_AI_USAGE_INPUT_TOKENS.to_string(),
        usage.input_tokens.to_string(),
    );
    attributes.insert(
        ATTR_GEN_AI_USAGE_OUTPUT_TOKENS.to_string(),
        usage.output_tokens.to_string(),
    );
    attributes
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
                    agent_name: "support".to_string(),
                },
                "invoke_agent support",
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
                    model: "gpt-x".to_string(),
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
