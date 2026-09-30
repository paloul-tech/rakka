//! Bounded behaviour under repetition: the soak half of slice 6.1.
//!
//! Every other test in this crate proves one flow once. What none of them
//! reaches is the shape of a deployment that has been up for a week: an agent
//! that has served thousands of tasks, a metric registry that has recorded
//! thousands of transitions, a store that has accumulated thousands of records.
//! [Specification 9.6](../../docs/plans/rakka-agent/spec.md) bounds the
//! materialized record of one task; what this file bounds is the *agent*, which
//! is the entity that actually lives long.
//!
//! The claim is not throughput. It is that nothing grows that should not:
//!
//! - the agent's durable record is the same size after N tasks as after one —
//!   an agent that remembered its tasks would eventually be unloadable;
//! - the metric series set is fixed, so no identifier reaches a label
//!   ([specification 17.12](../../docs/plans/rakka-agent/spec.md));
//! - each task's own materialized record stays inside its configured bound;
//! - every exchange journal settles empty, so no entity accumulates work it
//!   believes it still owes;
//! - the external system is invoked exactly once per task, so repetition
//!   creates no duplicate work.
//!
//! The default iteration count is small enough to belong in ordinary
//! validation. `RAKKA_AGENT_SOAK_ITERATIONS` scales it for a long local or CI
//! run; the invariants are the same either way, which is the point of asserting
//! properties rather than timings.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    run_id_for_assignment, AgentAssignmentGeneration, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentRunScope, AgentRunStatus, AgentTaskContent, AgentTaskCreation,
    AgentTaskEntityCommand, AgentTaskId, AgentTaskScope, AgentTaskStatus, TenantId,
    AGENT_TASK_MATERIALIZED_MAX_BYTES, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_core::InMemoryMetricsRecorder;

/// How many tasks the soak drives. Deliberately small by default; the shape of
/// the assertion does not change with the count.
fn iterations() -> usize {
    std::env::var("RAKKA_AGENT_SOAK_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(24)
}

fn closing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Resolved.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

fn scopes_for(index: usize) -> (AgentTaskScope, AgentRunScope) {
    let task = AgentTaskId::new(format!("ticket-{index}")).expect("the task id is valid");
    let task_scope =
        AgentTaskScope::new(TenantId::new(TENANT), task.clone()).expect("the task scope is valid");
    let run = run_id_for_assignment(&task, AgentAssignmentGeneration::new(1))
        .expect("the run id derives");
    let run_scope =
        AgentRunScope::new(TenantId::new(TENANT), agent_id(), run).expect("the run scope is valid");
    (task_scope, run_scope)
}

/// The distinct `(name, attributes)` series the recorder has ever emitted.
///
/// Cardinality, not volume: a soak is expected to record more *observations*
/// every iteration, and expected to record them under the same finite set of
/// series. A raw id in a label shows up here as a set that never stops growing.
fn metric_series(recorder: &InMemoryMetricsRecorder) -> BTreeSet<String> {
    recorder
        .snapshot()
        .observations()
        .iter()
        .map(|observation| {
            let mut attributes: Vec<String> = observation
                .attributes()
                .iter()
                .map(|attribute| format!("{}={}", attribute.key(), attribute.value()))
                .collect();
            attributes.sort();
            format!("{}{{{}}}", observation.name(), attributes.join(","))
        })
        .collect()
}

/// The serialized size of the agent's durable record.
async fn agent_state_bytes(fixture: &Fixture) -> usize {
    use rakka_persistence::DurableStateStore;

    let record = fixture
        .agents
        .load(&agent_scope().persistence_id())
        .await
        .expect("the agent record loads")
        .expect("the agent exists");
    serde_json::to_vec(&record.state)
        .expect("the agent state serializes")
        .len()
}

/// The serialized size of one task's materialized record.
async fn task_state_bytes(fixture: &Fixture, scope: &AgentTaskScope) -> usize {
    use rakka_persistence::DurableStateStore;

    let record = fixture
        .tasks
        .load(&scope.persistence_id())
        .await
        .expect("the task record loads")
        .expect("the task exists");
    serde_json::to_vec(&record.state)
        .expect("the task state serializes")
        .len()
}

/// Creates one task for this iteration, assigned to the fixture's one agent.
async fn create_task_at(fixture: &Fixture, scope: &AgentTaskScope) {
    fixture
        .apply_task_command_at(
            scope,
            AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, scope.task().as_str(), "1"],
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentTaskCreation {
                    definition: task_definition(),
                    input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                        .expect("the input is inline-bounded"),
                    assignee: Some(agent_id()),
                    team: None,
                    goal: None,
                    goal_mode: Default::default(),
                    goal_spec: None,
                    parent: None,
                    dependencies: Vec::new(),
                    escrow: None,
                    wake: None,
                    telemetry: Default::default(),
                    delegation: None,
                }),
            },
        )
        .await
        .expect("the task creates");
}

/// Drives one task and its run to rest, and returns nothing owed anywhere.
///
/// The exit condition *is* an assertion: the loop leaves only when both
/// entities report no outstanding exchange and no further transition, so a run
/// that quietly accumulated owed work would hang here rather than pass.
async fn drive_to_rest(fixture: &Fixture, task: &AgentTaskScope, run: &AgentRunScope) {
    for _round in 0..64 {
        let task_progress = fixture
            .settle_task_at(task)
            .await
            .expect("the task settles");

        let now = fixture.now();
        let mut entity = fixture.run_at(run);
        let (run_progress, answered, terminal) = match entity.recover(now).await {
            // The run does not exist until the creation exchange lands.
            Err(_) => (Default::default(), 0, false),
            Ok(_) => {
                let progress = entity
                    .settle_side_effects(&fixture.router, fixture.now())
                    .await
                    .expect("the run settles");
                let answered = fixture
                    .dispatcher
                    .drive(&mut entity, &fixture.router, fixture.now())
                    .await
                    .expect("the dispatcher drives");
                let terminal = entity
                    .state()
                    .ok()
                    .and_then(|state| state.status())
                    .is_some_and(AgentRunStatus::is_terminal);
                (progress, answered, terminal)
            }
        };

        if terminal
            && answered == 0
            && task_progress.outstanding == 0
            && task_progress.settled == 0
            && task_progress.failed == 0
            && run_progress.outstanding == 0
            && run_progress.transitions == 0
        {
            return;
        }
    }
    panic!("the task did not come to rest");
}

#[tokio::test]
async fn an_agent_serving_many_tasks_stays_bounded() {
    let iterations = iterations();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        // Keyed by turn number, so every task's single turn resolves to the
        // same script however many tasks have run before it.
        DeterministicModelAdapter::new().with_turn_for(1, closing_turn()),
    ))
    .with_metrics(metrics.clone());
    fixture.instantiate_agent().await;

    let mut first_agent_bytes = None;
    let mut first_series = None;

    for index in 0..iterations {
        let (task, run) = scopes_for(index);
        create_task_at(&fixture, &task).await;
        drive_to_rest(&fixture, &task, &run).await;

        // The work itself succeeded — a soak that quietly stopped doing work
        // would satisfy every bound below.
        let state = rakka_agent::load_agent_task_state(
            &fixture.tasks,
            &task,
            &rakka_agent::AgentSchemaPolicy::default(),
        )
        .await
        .expect("the task state loads")
        .expect("the task exists");
        let snapshot = state.snapshot().expect("the task snapshot derives");
        assert_eq!(
            snapshot.status,
            AgentTaskStatus::Completed,
            "iteration {index}: the task completed"
        );

        // Each task's own materialized record stays inside its configured
        // bound, however many tasks came before it.
        let bytes = task_state_bytes(&fixture, &task).await;
        assert!(
            bytes <= AGENT_TASK_MATERIALIZED_MAX_BYTES,
            "iteration {index}: the task record is {bytes} bytes, over the \
             {AGENT_TASK_MATERIALIZED_MAX_BYTES}-byte bound"
        );

        // The long-lived entity: an agent that remembered its tasks would grow
        // here, and would eventually be unloadable.
        let agent_bytes = agent_state_bytes(&fixture).await;
        match first_agent_bytes {
            None => first_agent_bytes = Some(agent_bytes),
            Some(first) => assert_eq!(
                agent_bytes, first,
                "iteration {index}: the agent record grew from {first} to {agent_bytes} bytes \
                 while serving tasks"
            ),
        }

        // Cardinality: the series set is fixed from the first completed task on.
        let series = metric_series(&metrics);
        match &first_series {
            None => first_series = Some(series),
            Some(first) => {
                let added: Vec<&String> = series.difference(first).collect();
                assert!(
                    added.is_empty(),
                    "iteration {index}: new metric series appeared, which is what a raw \
                     identifier in a label looks like: {added:?}"
                );
            }
        }
    }

    // Repetition created no duplicate work: one model turn per task, and no
    // task was driven twice.
    assert_eq!(
        fixture.dispatcher.model_calls(),
        iterations,
        "one model call per task"
    );
    assert!(
        first_series.is_some_and(|series| !series.is_empty()),
        "the soak recorded metrics at all"
    );
}
