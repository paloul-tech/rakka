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

use std::sync::atomic::Ordering;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    epoch_result_operation_id, epoch_task_id_for_wake, validate_agent_domain_metric_attributes,
    wake_admission_command, AgentBudgetConsumption, AgentEntityAddress, AgentEpochResult,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentModelTurn,
    AgentModelUsage, AgentOperationId, AgentOperationKind, AgentTaskContent,
    AgentTaskEntityCommand, AgentTaskScope, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, AgentWakeBackoffPolicy, AgentWakeLifecyclePolicy, InMemoryAgentDecisionEventSink,
    ScheduleRevision, AGENT_EPOCH_RESULT_PAYLOAD_TYPE, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
    METRIC_AGENT_DECISIONS, METRIC_AGENT_EFFECT_OUTCOMES, METRIC_AGENT_EPOCHS,
    METRIC_AGENT_GOAL_LIFECYCLE, METRIC_AGENT_RUN_TRANSITIONS, METRIC_AGENT_WAKE_DISPOSITIONS,
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

/// A legitimate epoch-result envelope carrying the accepted result's
/// fingerprint, for the stagnation detector.
fn epoch_result_with_digest(
    binding: &rakka_agent::AgentWakeBinding,
    status: AgentTaskStatus,
    result_digest: Option<rakka_agent::AgentContentDigest>,
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
        result_digest,
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

/// Stagnation trips count as the difference of the controller's durable
/// counters across the committed settlement, labeled by bounded trigger — and
/// a replayed settlement, answered from the journal, counts nothing. Under a
/// `Continue` action no status flips, so this counter is the trip's only
/// metric visibility.
#[tokio::test]
async fn stagnation_trips_count_once_per_settled_epoch() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::new()).with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(
            goal_spec_with_stagnation(2, rakka_agent::AgentGoalStagnationAction::Continue),
            true,
        ),
    ))
    .await
    .expect("the creation applies");

    let digest = rakka_agent::AgentContentDigest::of_json(&serde_json::json!({
        "answer": "same"
    }));
    let mut last = None;
    for due in [5_u64, 10] {
        let binding = scheduled_wake_binding(due, ScheduleRevision::INITIAL);
        fx.apply_task_command(
            wake_admission_command(binding.clone()).expect("the command derives"),
        )
        .await
        .expect("the admission applies");
        let result =
            epoch_result_with_digest(&binding, AgentTaskStatus::Completed, Some(digest.clone()));
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
        last = Some(result);
    }

    // The first identical completion started the streak; the second tripped
    // the threshold: exactly one observation, on the bounded trigger label.
    assert_eq!(
        labels_of(
            &metrics.snapshot(),
            rakka_agent::METRIC_AGENT_GOAL_STAGNATION
        ),
        vec![vec![("trigger".to_string(), "repeated-result".to_string())]],
        "one trip counted once"
    );

    // A redelivered settlement answers from the journal and moves nothing.
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone())
    .with_metrics(metrics.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    root.accept(&last.expect("an envelope was sent"), &fx.router, fx.now())
        .await
        .expect("the replay is answered");
    assert_eq!(
        labels_of(
            &metrics.snapshot(),
            rakka_agent::METRIC_AGENT_GOAL_STAGNATION
        )
        .len(),
        1,
        "the replay counted nothing"
    );
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

/// Observed lifecycle flips count exactly like commanded ones: the counter
/// is the difference of the goal's lifecycle status across the committed
/// transition, so an escalation auto-suspend on the exchange path and an
/// expiry observed by a delivery both emit — and a commanded transition
/// emits exactly once, never doubled by its command label.
#[tokio::test]
async fn observed_lifecycle_flips_count_their_transitions() {
    // Escalation: one failure auto-suspends inside the epoch-result
    // acceptance — no command anywhere near it.
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let escalating = wake_policy()
        .with_failure_backoff(AgentWakeBackoffPolicy {
            escalate_after_failures: Some(1),
            ..AgentWakeBackoffPolicy::DEFAULT
        })
        .expect("the backoff policy is valid");
    let fx = Fixture::new(ScriptedDispatcher::new()).with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(escalating))
        .await;
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone())
    .with_metrics(metrics.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    root.accept(
        &epoch_result(&binding, AgentTaskStatus::Failed),
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the failed result lands");
    assert_eq!(
        labels_of(&metrics.snapshot(), METRIC_AGENT_GOAL_LIFECYCLE),
        vec![vec![("transition".to_string(), "suspended".to_string())]],
        "the escalation auto-suspend counted its observed transition"
    );

    // The commanded resume and retire count exactly once each — the status
    // difference is the one emitter, so trimming the command labels lost
    // nothing and doubled nothing.
    let revision = |state: &rakka_agent::AgentTaskState| {
        state
            .task()
            .expect("the root is created")
            .wake_controller
            .as_ref()
            .expect("the controller exists")
            .lifecycle()
            .lifecycle_revision()
    };
    let state = rakka_agent::load_agent_task_state(
        &fx.tasks,
        &task_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the root state loads")
    .expect("the root exists");
    fx.apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
        operation_id: AgentOperationId::new(
            AgentOperationKind::LifecycleResume,
            [TENANT, TASK, "resume-metrics"],
        )
        .expect("the operation id derives"),
        expected_lifecycle_revision: revision(&state),
        provenance: Box::new(provenance(21)),
    })
    .await
    .expect("the resume applies");
    let state = rakka_agent::load_agent_task_state(
        &fx.tasks,
        &task_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the root state loads")
    .expect("the root exists");
    fx.apply_task_command(AgentTaskEntityCommand::RetireContinuousGoal {
        operation_id: AgentOperationId::new(
            AgentOperationKind::LifecycleTerminate,
            [TENANT, TASK, "retire-metrics"],
        )
        .expect("the operation id derives"),
        expected_lifecycle_revision: revision(&state),
        provenance: Box::new(provenance(22)),
    })
    .await
    .expect("the retire applies");
    assert_eq!(
        labels_of(&metrics.snapshot(), METRIC_AGENT_GOAL_LIFECYCLE),
        vec![
            vec![("transition".to_string(), "suspended".to_string())],
            vec![("transition".to_string(), "resumed".to_string())],
            vec![("transition".to_string(), "retired".to_string())],
        ],
        "each commanded transition counted exactly once"
    );

    // Expiry: a delivery past the policy's expiry is the transition that
    // observes the flip — the goal was created, which counts nothing, and
    // then expired without any command.
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let expiring = wake_policy()
        .with_lifecycle(AgentWakeLifecyclePolicy {
            expires_at: Some(AgentTimestampMillis::new(500)),
            ..AgentWakeLifecyclePolicy::DEFAULT
        })
        .expect("the lifecycle policy is valid");
    let fx = Fixture::new(ScriptedDispatcher::new()).with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(expiring))
        .await;
    fx.clock.store(1_000, Ordering::SeqCst);
    fx.apply_task_command(
        wake_admission_command(scheduled_wake_binding(600, ScheduleRevision::INITIAL))
            .expect("the command derives"),
    )
    .await
    .expect("the late delivery is dispositioned");
    assert_eq!(
        labels_of(&metrics.snapshot(), METRIC_AGENT_GOAL_LIFECYCLE),
        vec![vec![("transition".to_string(), "expired".to_string())]],
        "the observed expiry counted, and creation counted nothing"
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

/// Goal-contract status transitions count by status difference across the
/// committed transition — institution, decision, and policy-driven moves
/// alike — under the `rakka.agent.goal.status` counter, which is distinct
/// from the admission gate's `rakka.agent.goal.lifecycle`.
#[tokio::test]
async fn goal_contract_status_transitions_count_by_status_difference() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::new()).with_metrics(metrics.clone());
    fx.instantiate_agent().await;

    // Institution arrives at `Active`: one transition, counted once.
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");
    assert_eq!(
        labels_of(&metrics.snapshot(), rakka_agent::METRIC_AGENT_GOAL_STATUS),
        vec![vec![("transition".to_string(), "active".to_string())]],
        "the institution counted its arrival at active"
    );

    // A terminal decision counts its arrival; the duplicate answers from the
    // record and emits nothing.
    let cancel = AgentTaskEntityCommand::RecordGoalDecision {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Command,
            [TENANT, TASK, "goal-cancel-metrics"],
        )
        .expect("the operation id derives"),
        decision: Box::new(rakka_agent::AgentGoalDecision {
            reason: rakka_agent::AgentGoalTerminalReason::CancellationRequested {
                reason: "operator".to_string(),
            },
            evaluation: None,
            provenance: Some(Box::new(provenance(30))),
            expected_status_revision: rakka_agent::AgentRevisionNumber::INITIAL,
        }),
    };
    fx.apply_task_command(cancel.clone())
        .await
        .expect("the decision applies");
    fx.apply_task_command(cancel)
        .await
        .expect("the replay answers");
    let labels = labels_of(&metrics.snapshot(), rakka_agent::METRIC_AGENT_GOAL_STATUS);
    assert_eq!(
        labels,
        vec![
            vec![("transition".to_string(), "active".to_string())],
            vec![("transition".to_string(), "cancelled".to_string())],
        ],
        "the decision counted once and its replay counted nothing"
    );
}

/// Team board operations count once per durable transition under bounded
/// `operation`/`outcome` labels, a duplicate command counts nothing, and
/// every observation still passes the bounded guard
/// ([specification 8.10 and 17.12](../../../docs/plans/rakka-agent/spec.md)).
#[tokio::test]
async fn team_operations_count_once_under_bounded_labels() {
    use rakka_agent::{
        AgentGoalId, AgentRevisionNumber, AgentTeamCreation, AgentTeamEntityCommand, AgentTeamId,
        AgentTeamPolicy, AgentTeamScope, METRIC_AGENT_TEAM_OPERATIONS,
    };

    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
    .with_metrics(metrics.clone());

    let scope = AgentTeamScope::new(
        tenant(),
        AgentTeamId::new("metrics-team").expect("the team id is valid"),
    )
    .expect("the team scope is valid");
    let op = |discriminator: &str| {
        AgentOperationId::new(
            AgentOperationKind::TeamOperation,
            [TENANT, "metrics-team", discriminator],
        )
        .expect("the operation id derives")
    };
    let create = AgentTeamEntityCommand::Create {
        operation_id: op("create"),
        creation: Box::new(AgentTeamCreation {
            leader: agent_id(),
            root_goal: AgentGoalId::new("metrics-goal").expect("the goal id is valid"),
            policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
            members: Default::default(),
        }),
    };
    fx.apply_team_command_at(&scope, create.clone())
        .await
        .expect("the team creates");
    // The duplicate answers from the operation log and counts nothing.
    fx.apply_team_command_at(&scope, create)
        .await
        .expect("the replay answers");
    // A domain refusal counts as refused.
    fx.apply_team_command_at(
        &scope,
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-foreign"),
            task: rakka_agent::AgentTaskId::new("board-1").expect("the task id is valid"),
            posted_by: rakka_agent::AgentId::new("outsider").expect("the member id is valid"),
        },
    )
    .await
    .expect_err("a non-member post refuses");

    let snapshot = metrics.snapshot();
    for observation in snapshot.observations_named(METRIC_AGENT_TEAM_OPERATIONS) {
        assert_eq!(observation.kind(), MetricKind::Counter);
        let attributes: Vec<(&str, &str)> = observation
            .attributes()
            .iter()
            .map(|attribute| (attribute.key(), attribute.value()))
            .collect();
        validate_agent_domain_metric_attributes(&attributes)
            .expect("every team-operation label is bounded");
    }
    let labels = labels_of(&snapshot, METRIC_AGENT_TEAM_OPERATIONS);
    assert_eq!(
        labels,
        vec![
            vec![
                ("operation".to_string(), "create".to_string()),
                ("outcome".to_string(), "applied".to_string()),
            ],
            vec![
                ("operation".to_string(), "post".to_string()),
                ("outcome".to_string(), "refused".to_string()),
            ],
        ],
        "one observation per durable decision, none for the replay"
    );
}

/// A task's terminal notice closing its board entry counts `close`/`applied`
/// once at the team's accept boundary, and a replayed delivery — answered
/// from the applied log — counts nothing
/// ([specification 8.10 and 17.12](../../../docs/plans/rakka-agent/spec.md)).
#[tokio::test]
async fn a_terminal_notice_close_counts_once() {
    use rakka_agent::{
        team_terminal_notice_operation_id, AgentGoalId, AgentRevisionNumber, AgentTeamCreation,
        AgentTeamEntityCommand, AgentTeamId, AgentTeamPolicy, AgentTeamScope,
        AgentTeamTerminalNotice, AGENT_TEAM_TERMINAL_NOTICE_PAYLOAD_TYPE,
        METRIC_AGENT_TEAM_OPERATIONS,
    };

    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
    .with_metrics(metrics.clone());

    let scope = AgentTeamScope::new(
        tenant(),
        AgentTeamId::new("metrics-team").expect("the team id is valid"),
    )
    .expect("the team scope is valid");
    let op = |discriminator: &str| {
        AgentOperationId::new(
            AgentOperationKind::TeamOperation,
            [TENANT, "metrics-team", discriminator],
        )
        .expect("the operation id derives")
    };
    let mut members: std::collections::BTreeMap<
        rakka_agent::AgentId,
        std::collections::BTreeSet<rakka_agent::AgentCapabilityId>,
    > = Default::default();
    members.insert(agent_id(), Default::default());
    fx.apply_team_command_at(
        &scope,
        AgentTeamEntityCommand::Create {
            operation_id: op("create"),
            creation: Box::new(AgentTeamCreation {
                leader: agent_id(),
                root_goal: AgentGoalId::new("metrics-goal").expect("the goal id is valid"),
                policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
                members,
            }),
        },
    )
    .await
    .expect("the team creates");
    fx.apply_team_command_at(
        &scope,
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post"),
            task: task_scope().task().clone(),
            posted_by: agent_id(),
        },
    )
    .await
    .expect("the post applies");

    let notice = AgentTeamTerminalNotice {
        task: task_scope(),
        status: AgentTaskStatus::Cancelled,
        terminal_reason: "cancellation-requested".to_string(),
    };
    let operation = team_terminal_notice_operation_id(&tenant(), scope.team(), task_scope().task())
        .expect("the operation id derives");
    let envelope = AgentExchangeEnvelope::new(
        operation.clone(),
        AgentExchangeKind::TeamTerminalNotice,
        AgentEntityAddress::Task(task_scope()),
        AgentEntityAddress::Team(scope.clone()),
        AgentExchangePayload::encode(AGENT_TEAM_TERMINAL_NOTICE_PAYLOAD_TYPE, &notice)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation.as_str()),
        fx.now(),
    )
    .expect("the envelope builds");

    let mut team = rakka_agent::AgentTeamEntityStore::new(
        scope.clone(),
        fx.teams.clone(),
        fx.team_history.clone(),
    )
    .with_metrics(metrics.clone());
    team.recover(fx.now()).await.expect("the team recovers");
    team.accept(&envelope, &fx.router, fx.now())
        .await
        .expect("the notice applies");
    // The replay answers from the applied log and counts nothing.
    team.accept(&envelope, &fx.router, fx.now())
        .await
        .expect("the replay answers");

    let snapshot = metrics.snapshot();
    for observation in snapshot.observations_named(METRIC_AGENT_TEAM_OPERATIONS) {
        assert_eq!(observation.kind(), MetricKind::Counter);
        let attributes: Vec<(&str, &str)> = observation
            .attributes()
            .iter()
            .map(|attribute| (attribute.key(), attribute.value()))
            .collect();
        validate_agent_domain_metric_attributes(&attributes)
            .expect("every team-operation label is bounded");
    }
    let labels = labels_of(&snapshot, METRIC_AGENT_TEAM_OPERATIONS);
    let closes = labels
        .iter()
        .filter(|label| {
            label.contains(&("operation".to_string(), "close".to_string()))
                && label.contains(&("outcome".to_string(), "applied".to_string()))
        })
        .count();
    assert_eq!(
        closes, 1,
        "one close per durable application, none for the replay"
    );
}

/// Moderation turn operations count once per durable decision under bounded
/// `operation`/`outcome` labels, a duplicate — or a past-window ledger echo
/// — counts nothing, and every observation still passes the bounded guard
/// ([specification 8.11 and 17.12](../../../docs/plans/rakka-agent/spec.md)).
#[tokio::test]
async fn moderation_turns_count_once_under_bounded_labels() {
    use rakka_agent::{
        conversation_turn_content_digest, conversation_turn_operation_id, AgentBudgetConsumption,
        AgentConversationCompletionRule, AgentConversationCreation, AgentConversationEntityCommand,
        AgentConversationId, AgentConversationMode, AgentConversationScope,
        AgentConversationTurnSubmit, AgentModerationPolicy, AgentRevisionNumber,
        METRIC_AGENT_MODERATION_TURNS,
    };

    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
    .with_metrics(metrics.clone());

    let conversation =
        AgentConversationId::new("metrics-conversation").expect("the conversation id is valid");
    let scope = AgentConversationScope::new(tenant(), conversation.clone())
        .expect("the conversation scope is valid");
    let participant = rakka_agent::AgentId::new("speaker").expect("the participant id is valid");
    // The turn door reads the speaker's definition, so the roster's members
    // are instantiated with the `Moderation` capability their turns spend.
    fx.instantiate_conversation_participants(&[common::AGENT, "speaker"])
        .await;
    let create = AgentConversationEntityCommand::Create {
        operation_id: rakka_agent::conversation_create_operation_id(&tenant(), &conversation)
            .expect("the operation id derives"),
        creation: Box::new(AgentConversationCreation {
            moderator: agent_id(),
            participants: vec![participant.clone()],
            mode: AgentConversationMode::RoundRobin,
            completion: AgentConversationCompletionRule::ModeratorDecides,
            policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
            task: rakka_agent::AgentTaskId::new("metrics-task").expect("the task id is valid"),
            tokens: None,
            max_wall_clock_millis: None,
            transcript_ref: None,
        }),
    };
    fx.apply_conversation_command_at(&scope, create.clone())
        .await
        .expect("the conversation creates");
    // The duplicate answers from the operation log and counts nothing.
    fx.apply_conversation_command_at(&scope, create)
        .await
        .expect("the replay answers");
    // A domain refusal counts as refused.
    let submit =
        |speaker: &rakka_agent::AgentId, body: &str| AgentConversationEntityCommand::SubmitTurn {
            operation_id: conversation_turn_operation_id(
                &tenant(),
                &conversation,
                0,
                0,
                speaker,
                &conversation_turn_content_digest(body, None),
            )
            .expect("the operation id derives"),
            submit: Box::new(AgentConversationTurnSubmit {
                round: 0,
                turn: 0,
                participant: speaker.clone(),
                body: body.to_string(),
                direction: None,
                usage: AgentBudgetConsumption::zero(),
            }),
        };
    let stranger = rakka_agent::AgentId::new("stranger").expect("the id is valid");
    fx.apply_conversation_command_at(&scope, submit(&stranger, "barging in"))
        .await
        .expect_err("a non-participant turn refuses");
    fx.apply_conversation_command_at(&scope, submit(&participant, "an opening"))
        .await
        .expect("the turn records");
    // The replayed turn answers from the operation log and counts nothing.
    fx.apply_conversation_command_at(&scope, submit(&participant, "an opening"))
        .await
        .expect("the replay answers");

    let snapshot = metrics.snapshot();
    for observation in snapshot.observations_named(METRIC_AGENT_MODERATION_TURNS) {
        assert_eq!(observation.kind(), MetricKind::Counter);
        let attributes: Vec<(&str, &str)> = observation
            .attributes()
            .iter()
            .map(|attribute| (attribute.key(), attribute.value()))
            .collect();
        validate_agent_domain_metric_attributes(&attributes)
            .expect("every moderation-turn label is bounded");
    }
    let labels = labels_of(&snapshot, METRIC_AGENT_MODERATION_TURNS);
    assert_eq!(
        labels,
        vec![
            vec![
                ("operation".to_string(), "create".to_string()),
                ("outcome".to_string(), "applied".to_string()),
            ],
            vec![
                ("operation".to_string(), "turn".to_string()),
                ("outcome".to_string(), "refused".to_string()),
            ],
            vec![
                ("operation".to_string(), "turn".to_string()),
                ("outcome".to_string(), "applied".to_string()),
            ],
        ],
        "one observation per durable decision, none for the replays"
    );
}
