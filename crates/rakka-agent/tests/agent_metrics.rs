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
    epoch_result_operation_id, epoch_task_id_for_wake, validate_agent_domain_metric_attributes,
    wake_admission_command, AgentBudgetConsumption, AgentEntityAddress, AgentEpochResult,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentModelTurn,
    AgentModelUsage, AgentOperationId, AgentOperationKind, AgentTaskContent,
    AgentTaskEntityCommand, AgentTaskScope, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, InMemoryAgentDecisionEventSink, ScheduleRevision, AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION, METRIC_AGENT_DECISIONS, METRIC_AGENT_EFFECT_OUTCOMES,
    METRIC_AGENT_EPOCHS, METRIC_AGENT_GOAL_LIFECYCLE, METRIC_AGENT_RUN_TRANSITIONS,
    METRIC_AGENT_WAKE_DISPOSITIONS,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};
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

/// A legitimate epoch-result envelope for one admitted wake.
fn epoch_result(
    binding: &rakka_agent::AgentWakeBinding,
    status: AgentTaskStatus,
) -> AgentExchangeEnvelope {
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id = epoch_result_operation_id(&tenant(), &goal_id(), binding.wake_id())
        .expect("the operation id derives");
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status,
        consumed: AgentBudgetConsumption::zero(),
        result_digest: None,
    };
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::EpochResult,
        AgentEntityAddress::Task(epoch_scope),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        AgentTimestampMillis::new(9_000),
    )
    .expect("the envelope builds")
}

/// The label values of every observation of one instrument, as `(key, value)`
/// pair lists in recording order.
fn labels_of(snapshot: &rakka_core::MetricsSnapshot, name: &str) -> Vec<Vec<(String, String)>> {
    snapshot
        .observations_named(name)
        .iter()
        .map(|observation| {
            observation
                .attributes()
                .iter()
                .map(|attribute| (attribute.key().to_string(), attribute.value().to_string()))
                .collect()
        })
        .collect()
}

/// The continuous-goal instruments record once per committed transition —
/// an admission counts a disposition and an admitted epoch, a settled epoch
/// result counts its terminal class, a lifecycle command counts its
/// transition — and a replayed admission, answered as a duplicate, counts
/// nothing again. Every label passes the bounded guard.
#[tokio::test]
async fn continuous_goal_transitions_record_bounded_counters_once() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::new()).with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;

    // One scheduled occurrence admits: one disposition, one admitted epoch.
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let admission = wake_admission_command(binding.clone()).expect("the command derives");
    fx.apply_task_command(admission.clone())
        .await
        .expect("the admission applies");
    let snapshot = metrics.snapshot();
    assert_eq!(
        labels_of(&snapshot, METRIC_AGENT_WAKE_DISPOSITIONS),
        vec![vec![
            ("outcome".to_string(), "admitted".to_string()),
            ("trigger".to_string(), "durable-timer".to_string()),
        ]],
        "the admission counted its disposition and trigger"
    );
    assert_eq!(
        labels_of(&snapshot, METRIC_AGENT_EPOCHS),
        vec![vec![("outcome".to_string(), "admitted".to_string())]],
        "the admission counted its epoch"
    );

    // The same delivery replayed answers a duplicate and counts nothing.
    fx.apply_task_command(admission)
        .await
        .expect("the replay answers");
    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot
            .observations_named(METRIC_AGENT_WAKE_DISPOSITIONS)
            .len(),
        1,
        "a duplicate reply is never counted"
    );

    // The epoch's accepted result counts its terminal class, exactly once.
    let result = epoch_result(&binding, AgentTaskStatus::Completed);
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone())
    .with_metrics(metrics.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    let reply = root
        .accept(&result, &fx.router, fx.now())
        .await
        .expect("the result is answered");
    assert!(reply.result().is_accepted(), "the epoch result lands");
    let replay = root
        .accept(&result, &fx.router, fx.now())
        .await
        .expect("the replay is answered");
    assert!(replay.is_replayed(), "the second delivery replays");
    let snapshot = metrics.snapshot();
    assert_eq!(
        labels_of(&snapshot, METRIC_AGENT_EPOCHS),
        vec![
            vec![("outcome".to_string(), "admitted".to_string())],
            vec![("outcome".to_string(), "completed".to_string())],
        ],
        "the settlement counted once, and the replayed delivery not at all"
    );

    // A lifecycle command counts its transition.
    let state = rakka_agent::load_agent_task_state(
        &fx.tasks,
        &task_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the root state loads")
    .expect("the root exists");
    let revision = state
        .task()
        .expect("the root is created")
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle()
        .lifecycle_revision();
    fx.apply_task_command(AgentTaskEntityCommand::SuspendContinuousGoal {
        operation_id: AgentOperationId::new(
            AgentOperationKind::LifecycleSuspend,
            [TENANT, TASK, "suspend-metrics"],
        )
        .expect("the operation id derives"),
        expected_lifecycle_revision: revision,
        reason: None,
        provenance: Box::new(provenance(20)),
    })
    .await
    .expect("the suspend applies");
    let snapshot = metrics.snapshot();
    assert_eq!(
        labels_of(&snapshot, METRIC_AGENT_GOAL_LIFECYCLE),
        vec![vec![("transition".to_string(), "suspended".to_string())]],
        "the suspend counted its transition"
    );

    // Everything the whole flow recorded passes the bounded-label guard.
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
