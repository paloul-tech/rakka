//! The decision-event vocabulary and its fields (design sections 5.1-5.3).
//!
//! Every kind and outcome label equals its serde label, the first vocabulary
//! writes exactly the ten kinds an older reader decodes, and an event that
//! carries none of the new fields serializes without their keys.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    load_agent_run_state, AgentDecisionEventVocabulary, AgentDecisionKind, AgentDecisionOutcome,
    AgentModelTurn, AgentModelUsage, AgentSchemaPolicy, AgentTaskContent, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, InMemoryAgentDecisionEventSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

const OLD_KINDS: [(AgentDecisionKind, &str); 10] = [
    (AgentDecisionKind::Continue, "continue"),
    (AgentDecisionKind::CallTools, "call-tools"),
    (AgentDecisionKind::SubmitResult, "submit-result"),
    (AgentDecisionKind::Wait, "wait"),
    (AgentDecisionKind::Complete, "complete"),
    (AgentDecisionKind::Fail, "fail"),
    (AgentDecisionKind::RequestApproval, "request-approval"),
    (
        AgentDecisionKind::RequestAuthorization,
        "request-authorization",
    ),
    (AgentDecisionKind::Reconcile, "reconcile"),
    (AgentDecisionKind::Evaluate, "evaluate"),
];

const NEW_KINDS: [(AgentDecisionKind, &str); 6] = [
    (
        AgentDecisionKind::RequestReconciliation,
        "request-reconciliation",
    ),
    (AgentDecisionKind::ResolveCheckpoint, "resolve-checkpoint"),
    (AgentDecisionKind::EscalateCheckpoint, "escalate-checkpoint"),
    (AgentDecisionKind::Cancel, "cancel"),
    (AgentDecisionKind::Handoff, "handoff"),
    (AgentDecisionKind::Supersede, "supersede"),
];

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": "ticket" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 3,
            ..Default::default()
        })
}

/// The two-turn flow: a tool call on turn one, a proposal on turn two.
fn two_turn_dispatcher() -> ScriptedDispatcher<DeterministicModelAdapter> {
    ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    )
}

#[test]
fn every_kind_label_is_its_serde_label() {
    for (kind, label) in OLD_KINDS.iter().chain(NEW_KINDS.iter()) {
        assert_eq!(kind.as_label(), *label);
        assert_eq!(
            serde_json::to_value(kind).expect("kind serializes"),
            serde_json::json!(label)
        );
    }
}

#[test]
fn every_outcome_label_is_its_serde_label() {
    for (outcome, label) in [
        (AgentDecisionOutcome::Granted, "granted"),
        (AgentDecisionOutcome::Denied, "denied"),
        (AgentDecisionOutcome::Expired, "expired"),
        (AgentDecisionOutcome::Cancelled, "cancelled"),
        (
            AgentDecisionOutcome::ConfirmedExecuted,
            "confirmed-executed",
        ),
        (
            AgentDecisionOutcome::ConfirmedNotExecuted,
            "confirmed-not-executed",
        ),
        (AgentDecisionOutcome::Compensate, "compensate"),
        (AgentDecisionOutcome::Abandon, "abandon"),
    ] {
        assert_eq!(outcome.as_label(), label);
        assert_eq!(outcome.to_string(), label);
        assert_eq!(
            serde_json::to_value(outcome).expect("outcome serializes"),
            serde_json::json!(label)
        );
    }
}

#[test]
fn version_one_writes_exactly_the_ten_kinds_a_bc0c224_reader_decodes() {
    for (kind, _) in OLD_KINDS {
        assert!(AgentDecisionEventVocabulary::Version1.writes(kind));
    }
    for (kind, _) in NEW_KINDS {
        assert!(!AgentDecisionEventVocabulary::Version1.writes(kind));
    }
    for (kind, _) in OLD_KINDS.iter().chain(NEW_KINDS.iter()) {
        assert!(AgentDecisionEventVocabulary::Version2.writes(*kind));
    }
    assert_eq!(
        AgentDecisionEventVocabulary::default(),
        AgentDecisionEventVocabulary::Version1
    );
    assert_eq!(
        AgentDecisionEventVocabulary::Version1
            .schema_version()
            .get(),
        1
    );
    assert_eq!(
        AgentDecisionEventVocabulary::Version2
            .schema_version()
            .get(),
        2
    );
}

#[tokio::test]
async fn an_event_without_the_new_fields_serializes_without_their_keys() {
    let sink = Arc::new(InMemoryAgentDecisionEventSink::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    );
    let fx = Fixture::new(dispatcher).with_decision_events(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let events = sink.events(&run_scope());
    assert!(!events.is_empty());
    for event in events {
        let value = serde_json::to_value(&event).expect("event serializes");
        for key in ["checkpoint", "effect", "outcome", "run_status"] {
            assert!(value.get(key).is_none(), "{key} must be absent");
        }
    }
}

/// A vocabulary without a sink records nothing: every transition still queues
/// its drafts, and the seal discards them, so the persisted record owes no
/// event and has assigned no sequence.
#[tokio::test]
async fn an_unwired_run_keeps_no_decisions_in_its_record() {
    let fx = Fixture::new(two_turn_dispatcher())
        .with_decision_event_vocabulary(AgentDecisionEventVocabulary::Version2);
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    assert!(
        state
            .status()
            .is_some_and(rakka_agent::AgentRunStatus::is_terminal),
        "the flow ran to its end, so it made every decision it would record"
    );
    let loop_state = state.loop_state().expect("the run has a loop state");
    assert!(loop_state.decision_outbox().is_empty());
    assert_eq!(loop_state.decision_sequence(), 0);
}

/// The `iterate` decision is queued before `begin_turn` advances the turn
/// and clears the context, and sealed after: it keeps the turn and the
/// context snapshot it was decided in.
#[tokio::test]
async fn iterate_keeps_the_turn_and_context_it_was_decided_in() {
    let sink = Arc::new(InMemoryAgentDecisionEventSink::new());
    let fx = Fixture::new(two_turn_dispatcher()).with_decision_events(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let events = sink.events(&run_scope());
    assert_eq!(events.len(), 4, "start, call-tools, iterate, proposal");
    assert_eq!(events[2].kind, AgentDecisionKind::Continue);
    assert_eq!(events[2].turn, 1);
    assert!(events[2].context_snapshot.is_some());
    assert_eq!(events[2].context_snapshot, events[1].context_snapshot);
}
