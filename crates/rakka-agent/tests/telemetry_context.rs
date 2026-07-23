//! The trace-context schema retrofit: durable boundary records carry W3C
//! context additively, defaulted, and never fail-closed.
//!
//! Specification: sections 17.1 and 17.5; the slice 1.13 retrofit resolutions.
//! Telemetry is observability, never correctness, so a record persisted before
//! the retrofit must decode to the empty context inside the *unchanged* v1
//! schema window — no version bump, no migration — and a context-less record
//! must behave identically through the effect path (the schema half of
//! scenario 23). The write side is strict where the read side is permissive:
//! malformed context is dropped at the boundary, never persisted.

use rakka_agent::{
    AgentCheckpoint, AgentCheckpointKind, AgentEffectSpec, AgentEntityAddress,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentId, AgentLoopState,
    AgentOperationId, AgentOperationKind, AgentRecordKind, AgentRevisionNumber, AgentRunEffect,
    AgentRunEffectRequest, AgentRunId, AgentRunScope, AgentSchemaPolicy, AgentTaskId,
    AgentTaskScope, AgentToolCallId, AgentToolCallRequest, AgentToolId, TenantId,
    ATTR_AGENT_TELEMETRY_LINK_KIND, LINK_KIND_SUPERSEDED_GENERATION,
};
use rakka_agent_workflow::{
    AgentCorrelationId, AgentTelemetryContext, AgentTimestampMillis, HumanCheckpointId,
    PrincipalRef, StateSchemaVersion,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

const TENANT: &str = "acme";
const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn stamped_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(TRACE_PARENT.to_string()),
        trace_state: Some("vendor=value".to_string()),
        ..AgentTelemetryContext::default()
    }
}

fn scope() -> AgentRunScope {
    AgentRunScope::new(
        TenantId::new(TENANT),
        AgentId::new("support-agent").expect("the agent id is valid"),
        AgentRunId::new("run-1").expect("the run id is valid"),
    )
    .expect("the scope is valid")
}

fn tool_effect() -> AgentRunEffect {
    let call = AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("the call id is valid"),
        AgentToolId::new("charge-card").expect("the tool id is valid"),
        serde_json::json!({ "amount": 42 }),
    )
    .expect("the call is bounded");
    AgentRunEffect::new(
        &scope(),
        1,
        0,
        AgentRunEffectRequest::Tool {
            call: Box::new(call),
        },
        &AgentEffectSpec::non_idempotent(),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the effect derives")
}

fn envelope() -> AgentExchangeEnvelope {
    let initiator = AgentEntityAddress::Run(scope());
    let target = AgentEntityAddress::Task(
        AgentTaskScope::new(
            TenantId::new(TENANT),
            AgentTaskId::new("ticket-1").expect("the task id is valid"),
        )
        .expect("the task scope is valid"),
    );
    let operation_id = AgentOperationId::new(AgentOperationKind::Command, [TENANT, "exchange-1"])
        .expect("the operation id derives");
    let correlation_id = AgentCorrelationId::new(operation_id.as_str());
    AgentExchangeEnvelope::new(
        operation_id,
        AgentExchangeKind::ALL[0],
        initiator,
        target,
        AgentExchangePayload::encode("probe-payload", &serde_json::json!({ "n": 1 }))
            .expect("the payload encodes"),
        correlation_id,
        AgentTimestampMillis::new(1),
    )
    .expect("the envelope is valid")
}

fn checkpoint() -> AgentCheckpoint {
    AgentCheckpoint::open(
        HumanCheckpointId::new("ck-1"),
        AgentCheckpointKind::Approval,
        scope(),
        &tool_effect(),
        "Decide whether to charge the card.",
        PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "approver".to_string(),
            display_name: None,
        },
        AgentTimestampMillis::new(1),
    )
    .expect("the checkpoint opens")
}

fn loop_state() -> AgentLoopState {
    AgentLoopState::started(
        AgentTaskId::new("ticket-1").expect("the task id is valid"),
        None,
        AgentRevisionNumber::INITIAL,
        AgentRevisionNumber::INITIAL,
        AgentRevisionNumber::INITIAL,
        rakka_agent::AgentRunBudget::allocate(
            rakka_agent::AgentBudgetGrant::new(
                rakka_agent::AgentBudgetAllocation::unbounded(),
                rakka_agent::AgentBudgetLimits::unbounded(),
            ),
            AgentTimestampMillis::new(1),
        ),
    )
}

/// Re-encodes `stamped` the way a pre-retrofit binary wrote it — without the
/// `telemetry` field — and decodes it with this binary.
fn decode_pre_retrofit<R>(stamped: &R) -> R
where
    R: Serialize + DeserializeOwned,
{
    let mut value = serde_json::to_value(stamped).expect("the record serializes");
    let object = value.as_object_mut().expect("the record is an object");
    assert!(
        object.remove("telemetry").is_some(),
        "the retrofit field is present on the wire"
    );
    serde_json::from_value(value).expect("a pre-retrofit encoding decodes")
}

#[test]
fn no_record_kind_bumped_its_schema_version_for_the_retrofit() {
    for kind in [
        AgentRecordKind::ExchangeEnvelope,
        AgentRecordKind::RunEffect,
        AgentRecordKind::Checkpoint,
        AgentRecordKind::LoopState,
    ] {
        assert_eq!(
            kind.current_schema_version(),
            StateSchemaVersion::new(1),
            "{kind}: the telemetry field is additive, not a reinterpretation"
        );
    }
}

#[test]
fn a_pre_retrofit_envelope_decodes_to_the_empty_context_inside_the_v1_window() {
    let stamped = envelope().with_telemetry(stamped_context());
    assert_eq!(
        stamped.telemetry().trace_parent.as_deref(),
        Some(TRACE_PARENT)
    );

    let decoded = decode_pre_retrofit(&stamped);
    assert_eq!(decoded, envelope(), "absent context reads as none recorded");
    AgentSchemaPolicy::default()
        .check(
            AgentRecordKind::ExchangeEnvelope,
            StateSchemaVersion::new(1),
        )
        .expect("the v1 window still accepts the record");
}

#[test]
fn a_pre_retrofit_effect_decodes_to_the_empty_context_and_replays_identically() {
    let mut stamped = tool_effect();
    stamped.telemetry = stamped_context();

    let decoded = decode_pre_retrofit(&stamped);
    assert_eq!(
        decoded,
        tool_effect(),
        "absent context reads as none recorded"
    );

    // The schema half of scenario 23: a context-less record behaves
    // identically through the effect path — the dispatch ticket it projects is
    // byte-for-byte the ticket the same effect projected before the retrofit.
    assert_eq!(
        decoded.to_workflow_effect(&scope()),
        tool_effect().to_workflow_effect(&scope()),
    );
}

#[test]
fn a_pre_retrofit_checkpoint_decodes_to_the_empty_context() {
    let stamped = checkpoint().with_telemetry(stamped_context());
    let decoded = decode_pre_retrofit(&stamped);
    assert_eq!(
        decoded,
        checkpoint(),
        "absent context reads as none recorded"
    );
}

#[test]
fn a_pre_retrofit_loop_state_decodes_to_the_empty_context() {
    let state = loop_state();
    let decoded = decode_pre_retrofit(&state);
    assert_eq!(decoded, state, "absent context reads as none recorded");
}

#[test]
fn the_dispatch_ticket_forwards_the_scheduling_segments_context() {
    let mut effect = tool_effect();
    effect.telemetry = stamped_context();

    let ticket = effect.to_workflow_effect(&scope());
    assert_eq!(
        ticket.telemetry_context, effect.telemetry,
        "the dispatcher's consumer span links to the scheduling segment"
    );

    let unstamped = tool_effect().to_workflow_effect(&scope());
    assert_eq!(
        unstamped.telemetry_context,
        AgentTelemetryContext::default()
    );
}

#[test]
fn a_new_generation_links_the_superseded_one_and_starts_parentless() {
    let mut effect = tool_effect();
    effect.telemetry = stamped_context();

    effect
        .begin_next_generation(&scope(), AgentTimestampMillis::new(2))
        .expect("the next generation begins");

    assert!(
        effect.telemetry.trace_parent.is_none(),
        "a reconciled re-dispatch is caused by the decision, not the superseded segment"
    );
    assert_eq!(effect.telemetry.span_links.len(), 1);
    let link = &effect.telemetry.span_links[0];
    assert_eq!(link.trace_id, "0af7651916cd43dd8448eb211c80319c");
    assert_eq!(link.span_id, "b7ad6b7169203331");
    assert_eq!(
        link.attributes.get(ATTR_AGENT_TELEMETRY_LINK_KIND),
        Some(&LINK_KIND_SUPERSEDED_GENERATION.to_string()),
    );
}

#[test]
fn a_new_generation_of_a_contextless_effect_stays_contextless() {
    let mut effect = tool_effect();
    effect
        .begin_next_generation(&scope(), AgentTimestampMillis::new(2))
        .expect("the next generation begins");
    assert_eq!(effect.telemetry, AgentTelemetryContext::default());
}

#[test]
fn the_write_gate_is_strict_where_the_read_is_permissive() {
    let mut malformed = AgentTelemetryContext {
        trace_parent: Some("not-a-traceparent".to_string()),
        ..AgentTelemetryContext::default()
    };
    malformed
        .baggage
        .insert("tenant".to_string(), "acme".to_string());

    let envelope = envelope().with_telemetry(malformed.clone());
    assert_eq!(
        envelope.telemetry(),
        &AgentTelemetryContext::default(),
        "malformed context is dropped at the boundary, never persisted"
    );

    let checkpoint = checkpoint().with_telemetry(malformed);
    assert_eq!(checkpoint.telemetry, AgentTelemetryContext::default());
}
