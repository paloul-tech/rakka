//! A goal's control task that has ended still settles the results of the
//! epochs it admitted (issue #181).
//!
//! A continuous goal's control task can end while an epoch is still out: a
//! person submits its result, or a policy ends the goal. The epoch goes on to
//! its own end and then sends its result. That result used to be refused
//! `task-terminal`, and nothing sent it again: the goal's ledger kept the
//! epoch's budget out for good, never counted what the epoch spent, and its
//! wake controller kept showing the occurrence as active.
//!
//! The ended control task now takes such a result as a settlement. The budget
//! returns, the spend is counted and the wake's place is released. No status
//! moves and nothing is admitted.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_run_state, load_agent_task_state, AgentBudgetDimension, AgentModelTurn,
    AgentRevisionNumber, AgentRunEffectKind, AgentRunEffectStatus, AgentRunScope,
    AgentSchemaPolicy, AgentTaskContent, AgentTaskEntityCommand, AgentTaskScope, AgentTaskStatus,
    AgentWakeDisposition, AgentWakeScanOutcome, ScheduleRevision,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::{continuous_goal_mode, epoch_scopes_for, task_scope, wake_policy, Fixture};

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Observed the nightly state.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "reconciled" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// Delivers an occurrence due at `due_at`: its epoch's scopes, and how the
/// controller dispositioned it.
async fn deliver(
    fx: &Fixture,
    due_at: u64,
) -> (AgentTaskScope, AgentRunScope, AgentWakeDisposition) {
    let binding = fx.schedule_wake(due_at, ScheduleRevision::INITIAL).await;
    fx.clock.store(due_at + 1_000, Ordering::SeqCst);
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the scan pass runs");
    assert_eq!(scan.outcomes.len(), 1, "one occurrence was due");
    let AgentWakeScanOutcome::Dispositioned { disposition, .. } = &scan.outcomes[0] else {
        panic!(
            "the controller dispositions the occurrence, got {:?}",
            scan.outcomes[0]
        );
    };
    let (epoch, run) = epoch_scopes_for(binding.wake_id());
    (epoch, run, disposition.clone())
}

/// A continuous goal whose first epoch is running: its run holds its model
/// call, and the goal's ledger holds the epoch's budget out.
async fn a_goal_with_an_epoch_running() -> (Fixture, AgentTaskScope, AgentRunScope) {
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn()));
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let (epoch, run, disposition) = deliver(&fx, 5).await;
    assert!(disposition.is_admission());
    for _round in 0..16 {
        for scope in [task_scope(), epoch.clone()] {
            fx.settle_task_at(&scope).await.expect("the task settles");
        }
        let mut entity = fx.run_at(&run);
        if entity.recover(fx.now()).await.is_ok() {
            entity
                .settle_side_effects(&fx.router, fx.now())
                .await
                .expect("the run settles");
        }
        let sent = load_agent_run_state(&fx.runs, &run, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .and_then(|state| state.loop_state().map(|held| held.effects().to_vec()))
            .unwrap_or_default()
            .iter()
            .any(|effect| {
                effect.kind() == AgentRunEffectKind::ModelCall
                    && effect.status == AgentRunEffectStatus::Ready
            });
        if sent {
            assert_eq!(
                task_at(&fx, &epoch).await,
                Some((AgentTaskStatus::InProgress, 1))
            );
            return (fx, epoch, run);
        }
    }
    panic!("the epoch's run never sent its model call");
}

/// A person's result for the goal's control task, submitted as an operator.
fn a_persons_result() -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::SubmitHumanResult {
        operation_id: rakka_agent::human_result_operation_id(
            &common::tenant(),
            task_scope().task(),
            "done",
        )
        .expect("the operation id derives"),
        submission: Box::new(rakka_agent::AgentHumanResultSubmission {
            principal: "user:operator".to_string(),
            definition_id: common::task_definition_id(),
            definition_version: AgentRevisionNumber::INITIAL,
            result_schema: common::schema("ticket-result"),
            content: AgentTaskContent::inline(serde_json::json!({ "answer": "closed by hand" }))
                .expect("the content is inline-bounded"),
            evidence: Vec::new(),
            causation_id: rakka_agent_workflow::AgentCausationId::new("cause-done"),
            submitted_at: rakka_agent_workflow::AgentTimestampMillis::new(0),
        }),
    }
}

/// A task's status and how many children of its ledger are out.
async fn task_at(fx: &Fixture, scope: &AgentTaskScope) -> Option<(AgentTaskStatus, usize)> {
    load_agent_task_state(&fx.tasks, scope, &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .and_then(|state| {
            state
                .task()
                .map(|task| (task.status, task.escrow.outstanding().count()))
        })
}

/// The controller's admitted and released counts, how many occurrences it
/// holds active and parked, and the loop iterations the goal's ledger counts
/// as spent.
async fn goal(fx: &Fixture) -> ((u64, u64, usize, usize), u64) {
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let task = root.task().expect("the root is created");
    let controller = task
        .wake_controller
        .as_ref()
        .expect("the controller exists");
    (
        (
            controller.counters().admitted,
            controller.counters().released,
            controller.active().len(),
            controller.pending().len(),
        ),
        task.escrow
            .consumed()
            .get(AgentBudgetDimension::LoopIterations),
    )
}

/// The reply the epoch task holds to the result it sent its goal: the
/// exchange's status, as the journal records it.
async fn epoch_results_reply(fx: &Fixture, epoch: &AgentTaskScope) -> Option<String> {
    let state = load_agent_task_state(&fx.tasks, epoch, &AgentSchemaPolicy::default())
        .await
        .expect("the epoch state loads")
        .expect("the epoch exists");
    let journal = serde_json::to_value(&state).expect("the state serializes")["journal"].take();
    journal["settled"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["kind"] == "epoch-result")
        .map(|entry| match &entry["result"]["status"] {
            serde_json::Value::String(status) => status.clone(),
            other => other.to_string(),
        })
}

/// A person completes the goal's control task while an epoch is running. The
/// epoch finishes, and its result is taken: the budget returns to the ended
/// goal's ledger, the spend is counted, and the wake's place is released.
#[tokio::test]
async fn an_ended_goal_settles_the_result_of_an_epoch_that_was_running() {
    let (fx, epoch, run) = a_goal_with_an_epoch_running().await;

    fx.apply_task_command(a_persons_result())
        .await
        .expect("the person's result applies");
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 1)),
        "the control task is ended over the epoch's budget"
    );

    fx.pump_epoch(&epoch, &run)
        .await
        .expect("the epoch converges");

    assert_eq!(
        task_at(&fx, &epoch).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert_eq!(
        epoch_results_reply(&fx, &epoch).await.as_deref(),
        Some("accepted"),
        "the ended control task took the epoch's result"
    );
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 0)),
        "the budget is back, and the control task's status did not move"
    );
    assert_eq!(
        goal(&fx).await,
        ((1, 1, 0, 0), 1),
        "the wake's place is released, and the epoch's one iteration is counted"
    );
}

/// The place an ended goal's epoch frees is not taken. An occurrence that
/// was parked behind the epoch stays parked: an ended goal admits nothing.
#[tokio::test]
async fn an_ended_goal_promotes_nothing_into_the_place_its_epoch_frees() {
    let (fx, epoch, run) = a_goal_with_an_epoch_running().await;
    let (parked_epoch, _, disposition) = deliver(&fx, 1_000_005).await;
    assert!(
        matches!(disposition, AgentWakeDisposition::Coalesced { .. }),
        "the second occurrence waits behind the running epoch, got {disposition:?}"
    );
    fx.apply_task_command(a_persons_result())
        .await
        .expect("the person's result applies");

    fx.pump_epoch(&epoch, &run)
        .await
        .expect("the epoch converges");

    assert_eq!(
        goal(&fx).await,
        ((1, 1, 0, 1), 1),
        "the result is settled, and the parked occurrence is not admitted"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the control task settles");
    assert_eq!(task_at(&fx, &parked_epoch).await, None);
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 0))
    );
}

/// Only a continuous goal's control task has epochs to settle. An ended task
/// that is not one still refuses an epoch's result, as before.
#[tokio::test]
async fn an_ended_task_that_is_no_continuous_goal_still_refuses_an_epochs_result() {
    use rakka_agent::{
        epoch_result_operation_id, epoch_task_id_for_wake, AgentBudgetConsumption,
        AgentEntityAddress, AgentEpochResult, AgentExchangeEnvelope, AgentExchangeKind,
        AgentExchangePayload, AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
    };
    use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};

    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    // The same human-owned task, with a finite goal mode: it admits no epoch.
    fx.apply_task_command(common::continuous_control_creation_command(
        Default::default(),
    ))
    .await
    .expect("the task is created");
    fx.apply_task_command(a_persons_result())
        .await
        .expect("the person's result applies");
    assert_eq!(
        task_at(&fx, &task_scope()).await.map(|(status, _)| status),
        Some(AgentTaskStatus::Completed)
    );

    // A result that derives rightly from its wake, so nothing but the task's
    // own state can refuse it.
    let binding = common::scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(common::tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id =
        epoch_result_operation_id(&common::tenant(), &common::goal_id(), binding.wake_id())
            .expect("the operation id derives");
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status: AgentTaskStatus::Completed,
        consumed: AgentBudgetConsumption::zero(),
        result_digest: None,
    };
    let envelope = AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::EpochResult,
        AgentEntityAddress::Task(epoch_scope),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_EPOCH_RESULT_PAYLOAD_TYPE, &result)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        AgentTimestampMillis::new(9_000),
    )
    .expect("the envelope builds");
    let mut task = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    task.recover(AgentTimestampMillis::new(9_001))
        .await
        .expect("the task recovers");
    let reply = task
        .accept(&envelope, &fx.router, AgentTimestampMillis::new(9_002))
        .await
        .expect("the result is answered");

    assert_eq!(
        reply.result().status().rejection_code(),
        Some("task-terminal"),
        "got {:?}",
        reply.result()
    );
}
