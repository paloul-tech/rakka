//! Structured decision events, driven through the run entity.
//!
//! Specification: sections 17.7 and 17.13. A run wired with a decision-event
//! sink records why the loop did what it did — one bounded event per durable
//! decision, in the same compare-and-set as the deciding transition — and the
//! settle pass flushes them to the sink only after those transitions
//! committed. Replayed processing resolves to one logical event per decision:
//! the identity is derived from the run, the turn, and the decision's slot,
//! and the sink deduplicates on it. This is the decision half of scenario 21's
//! session view; the events never carry model text, tool payloads, or
//! credentials.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    load_agent_run_state, AgentDecisionKind, AgentDecisionSource, AgentModelTurn, AgentModelUsage,
    AgentSchemaPolicy, AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentDecisionEventSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

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
        })
}

/// A run that calls a tool on its first turn and proposes on its second emits
/// the decision sequence that explains it — start, call-tools, iterate,
/// submit-result — each event bounded, sequenced, and attributed to its source,
/// and a re-driven pump never emits a decision twice.
#[tokio::test]
async fn a_run_emits_its_decision_sequence_exactly_once() {
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

    let scope = run_scope();
    let events = sink.events(&scope);
    let kinds: Vec<AgentDecisionKind> = events.iter().map(|event| event.kind).collect();
    assert_eq!(
        kinds,
        vec![
            AgentDecisionKind::Continue,     // the opening deterministic start
            AgentDecisionKind::CallTools,    // the model's turn-one fan-out
            AgentDecisionKind::Continue,     // the model's decision to iterate
            AgentDecisionKind::SubmitResult, // the model's turn-two proposal
        ],
    );

    // Each event is bounded, sequenced, and attributed.
    let sequences: Vec<u64> = events.iter().map(|event| event.sequence).collect();
    assert_eq!(sequences, vec![1, 2, 3, 4], "per-run monotonic sequence");
    let turns: Vec<u64> = events.iter().map(|event| event.turn).collect();
    assert_eq!(turns, vec![1, 1, 1, 2]);
    assert_eq!(events[0].source, AgentDecisionSource::DeterministicPolicy);
    assert_eq!(events[1].source, AgentDecisionSource::Model);
    assert_eq!(
        events[1]
            .selected_tools
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        vec!["lookup".to_string()],
        "the selected tool classes ride the call-tools decision"
    );
    assert_eq!(events[3].source, AgentDecisionSource::Model);
    for event in &events {
        assert_eq!(event.task.as_ref(), Some(task_scope().task()));
        assert_eq!(event.scope, scope);
    }

    // A re-driven pump — what a recovery sweep does — emits nothing twice: the
    // events were flushed and cleared, and every decision's derived identity
    // already answered.
    fx.pump().await.expect("the re-driven pump is harmless");
    assert_eq!(sink.events(&scope).len(), 4);

    // The flushed outbox is empty in durable state: the run owes nothing, so a
    // passivated run holds no unflushed telemetry.
    let state = load_agent_run_state(&fx.runs, &scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let owed = state
        .loop_state()
        .map(|loop_state| loop_state.decision_outbox().len())
        .unwrap_or_default();
    assert_eq!(owed, 0, "every owed decision was flushed");
}
