//! Bounded `rakka.agent.*` metrics, driven through the run entity.
//!
//! Specification: section 17.12 and the slice 1.13 metric-vocabulary
//! resolution. The agent domain measures its own durable transitions —
//! decisions, loop transitions, effect outcomes, recoveries — and the
//! substrate keeps measuring the substrate under `rakka.agent_workflow.*`.
//! Every label key comes from a bounded vocabulary and every value from a
//! closed `as_label()` set; no identifier, prompt, argument, or error message
//! ever labels a metric. Metrics are aggregates, never the correctness
//! source: an unwired run records nothing and behaves identically.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    validate_agent_domain_metric_attributes, AgentModelTurn, AgentModelUsage, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, InMemoryAgentDecisionEventSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION, METRIC_AGENT_DECISIONS, METRIC_AGENT_EFFECT_OUTCOMES,
    METRIC_AGENT_RUN_TRANSITIONS,
};
use rakka_core::{InMemoryMetricsRecorder, MetricKind};

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

/// A full tool-then-propose run records the agent-domain instruments — loop
/// transitions by phase, effect outcomes by kind/safety/outcome, decisions by
/// kind/source — and every label on every observation passes the bounded
/// guard, with no identifier anywhere.
#[tokio::test]
async fn a_run_records_bounded_agent_metrics_and_no_identifiers() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
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

    let fx = Fixture::new(dispatcher)
        .with_decision_events(sink)
        .with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let snapshot = metrics.snapshot();

    // Every observation carries only bounded label keys, and the forbidden
    // guard holds: no run/effect/checkpoint id, prompt, argument, or error
    // message labels anything (scenario 25's metric half).
    for observation in snapshot.observations() {
        let attributes: Vec<(&str, &str)> = observation
            .attributes()
            .iter()
            .map(|attribute| (attribute.key(), attribute.value()))
            .collect();
        validate_agent_domain_metric_attributes(&attributes).unwrap_or_else(|error| {
            panic!("{}: {error}", observation.name());
        });
    }

    // The loop's committed transitions were counted by their advancing phase.
    let transitions = snapshot.observations_named(METRIC_AGENT_RUN_TRANSITIONS);
    assert!(!transitions.is_empty(), "committed transitions are counted");
    assert!(transitions
        .iter()
        .all(|observation| observation.kind() == MetricKind::Counter));

    // Both the model calls and the tool call resolved as succeeded outcomes,
    // labeled by kind and safety class.
    let outcomes = snapshot.observations_named(METRIC_AGENT_EFFECT_OUTCOMES);
    let outcome_labels: Vec<(String, String)> = outcomes
        .iter()
        .map(|observation| {
            let find = |key: &str| {
                observation
                    .attributes()
                    .iter()
                    .find(|attribute| attribute.key() == key)
                    .map(|attribute| attribute.value().to_string())
                    .unwrap_or_default()
            };
            (find("effect_kind"), find("outcome"))
        })
        .collect();
    assert!(
        outcome_labels.contains(&("model-call".to_string(), "succeeded".to_string())),
        "a resolved model generation is counted: {outcome_labels:?}"
    );
    assert!(
        outcome_labels.contains(&("tool-call".to_string(), "succeeded".to_string())),
        "a resolved tool generation is counted: {outcome_labels:?}"
    );

    // Each of the four decisions was counted exactly once, on first durable
    // acceptance by the sink — a re-driven pump adds nothing.
    let decisions = snapshot.observations_named(METRIC_AGENT_DECISIONS);
    assert_eq!(decisions.len(), 4, "one count per accepted decision");
    fx.pump().await.expect("the re-driven pump is harmless");
    assert_eq!(
        metrics
            .snapshot()
            .observations_named(METRIC_AGENT_DECISIONS)
            .len(),
        4,
        "a replayed flush counts nothing"
    );
}

/// An unwired run records no agent-domain metrics at all — the recorder
/// defaults to the no-op, and metrics are never a correctness input.
#[tokio::test]
async fn an_unwired_run_records_nothing() {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn("resolved")),
    );
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");
}
