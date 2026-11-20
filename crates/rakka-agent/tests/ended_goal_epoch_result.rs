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
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    epoch_result_operation_id, epoch_task_id_for_wake, load_agent_run_state, load_agent_task_state,
    wake_admission_command, AgentBudgetConsumption, AgentBudgetDimension, AgentEntityAddress,
    AgentEpochResult, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentExchangeReply, AgentModelTurn, AgentRevisionNumber, AgentRunEffectKind,
    AgentRunEffectStatus, AgentRunScope, AgentSchemaPolicy, AgentTaskContent,
    AgentTaskEntityCommand, AgentTaskHistoryKind, AgentTaskScope, AgentTaskStatus,
    AgentWakeBinding, AgentWakeDisposition, AgentWakeOutcome, AgentWakeScanOutcome,
    ScheduleRevision, AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE, AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION, METRIC_AGENT_EPOCHS,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};
use rakka_core::InMemoryMetricsRecorder;

mod common;

use common::{
    continuous_goal_mode, epoch_scopes_for, scheduled_wake_binding, task_scope, wake_policy,
    Fixture,
};

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

/// The result the epoch of `binding`'s occurrence would send its goal: its
/// outcome, and the loop iterations it spent.
fn epoch_result_for(
    binding: &AgentWakeBinding,
    status: AgentTaskStatus,
    loop_iterations: u64,
) -> AgentExchangeEnvelope {
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(common::tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id =
        epoch_result_operation_id(&common::tenant(), &common::goal_id(), binding.wake_id())
            .expect("the operation id derives");
    let mut consumed = AgentBudgetConsumption::zero();
    consumed.add(AgentBudgetDimension::LoopIterations, loop_iterations);
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status,
        consumed,
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

/// Hands the control task an epoch's result, and returns its reply.
async fn hand_the_goal(
    fx: &Fixture,
    envelope: &AgentExchangeEnvelope,
    metrics: Option<Arc<InMemoryMetricsRecorder>>,
) -> AgentExchangeReply {
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    if let Some(metrics) = metrics {
        root = root.with_metrics(metrics);
    }
    root.recover(fx.now()).await.expect("the root recovers");
    root.accept(envelope, &fx.router, fx.now())
        .await
        .expect("the result is answered")
}

/// What the reply to an epoch's result says was released: the wake, and the
/// occurrence admitted into its place.
fn released_by(reply: &AgentExchangeReply) -> Option<(String, Option<String>)> {
    let outcome: Option<AgentWakeOutcome> = reply
        .result()
        .payload()
        .decode(AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE)
        .expect("the outcome decodes");
    match outcome {
        Some(AgentWakeOutcome::Release(release)) => Some((
            release.released.to_string(),
            release.admitted_next.map(|next| next.to_string()),
        )),
        Some(other) => panic!("an epoch's result releases, got {other:?}"),
        None => None,
    }
}

/// The control task's history: each entry's kind and detail.
async fn history_of_the_goal(fx: &Fixture) -> Vec<(AgentTaskHistoryKind, String)> {
    let mut entries = Vec::new();
    let mut cursor = Some(rakka_agent::AgentTaskHistoryCursor::start());
    while let Some(position) = cursor {
        let page = rakka_agent::AgentTaskHistoryStore::read(&fx.history, &task_scope(), position)
            .await
            .expect("the history reads");
        entries.extend(
            page.entries
                .iter()
                .map(|entry| (entry.kind, entry.detail.clone())),
        );
        cursor = page.next;
    }
    entries
}

/// The control task's failure streak, and when its record last changed.
async fn streak_and_stamp(fx: &Fixture) -> (u32, AgentTimestampMillis) {
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let streak = root
        .task()
        .and_then(|task| task.wake_controller.as_ref())
        .expect("the controller exists")
        .lifecycle()
        .consecutive_failures();
    (streak, root.updated_at())
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
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let reply = hand_the_goal(
        &fx,
        &epoch_result_for(&binding, AgentTaskStatus::Completed, 0),
        None,
    )
    .await;

    assert_eq!(
        reply.result().status().rejection_code(),
        Some("task-terminal"),
        "got {:?}",
        reply.result()
    );
}

/// A human-owned control task with one occurrence admitted, ended by a
/// person's result: the occurrence's binding, and the recorder its metrics go
/// to.
async fn an_ended_goal_with_an_epoch_out(
) -> (Fixture, AgentWakeBinding, Arc<InMemoryMetricsRecorder>) {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::new()).with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    fx.clock.store(50_000, Ordering::SeqCst);
    fx.apply_task_command(a_persons_result())
        .await
        .expect("the person's result applies");
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 1))
    );
    (fx, binding, metrics)
}

/// The outcomes the epoch metric was emitted with, in order.
fn epoch_outcomes(metrics: &InMemoryMetricsRecorder) -> Vec<String> {
    metrics
        .snapshot()
        .observations_named(METRIC_AGENT_EPOCHS)
        .iter()
        .flat_map(|observation| {
            observation
                .attributes()
                .iter()
                .filter(|attribute| attribute.key() == "outcome")
                .map(|attribute| attribute.value().to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// What an ended goal takes from a failed epoch, and what it does not. The
/// settlement is recorded: the reply names the released wake and no
/// successor, one `epoch-settled` entry joins the history, the epoch metric
/// counts the failure, and the record's stamp moves. The failure is not
/// counted toward the goal's streak, which a live goal would have grown.
#[tokio::test]
async fn an_ended_goal_records_a_failed_epochs_settlement_and_counts_no_failure() {
    let (fx, binding, metrics) = an_ended_goal_with_an_epoch_out().await;
    let ended_at = streak_and_stamp(&fx).await.1;
    fx.clock.store(90_000, Ordering::SeqCst);

    let reply = hand_the_goal(
        &fx,
        &epoch_result_for(&binding, AgentTaskStatus::Failed, 1),
        Some(metrics.clone()),
    )
    .await;

    assert!(reply.result().is_accepted(), "got {:?}", reply.result());
    assert_eq!(
        released_by(&reply),
        Some((binding.wake_id().to_string(), None)),
        "the reply names the released wake, and no occurrence admitted after it"
    );
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert_eq!(goal(&fx).await, ((1, 1, 0, 0), 1));
    let (streak, stamped) = streak_and_stamp(&fx).await;
    assert_eq!(streak, 0, "an ended goal counts no failure streak");
    assert!(
        stamped > ended_at,
        "the settlement is a change to the record: {stamped:?} after {ended_at:?}"
    );
    let settled: Vec<_> = history_of_the_goal(&fx)
        .await
        .into_iter()
        .filter(|(kind, _)| *kind == AgentTaskHistoryKind::EpochSettled)
        .collect();
    assert_eq!(
        settled,
        vec![(
            AgentTaskHistoryKind::EpochSettled,
            format!("failed {}", binding.wake_id())
        )]
    );
    assert_eq!(
        epoch_outcomes(&metrics),
        vec!["admitted".to_string(), "failed".to_string()]
    );
}

/// The same failed result on a goal that has not ended grows its streak. This
/// is what the test above is measured against.
#[tokio::test]
async fn a_live_goal_counts_the_failure_an_ended_goal_does_not() {
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");

    let reply = hand_the_goal(
        &fx,
        &epoch_result_for(&binding, AgentTaskStatus::Failed, 1),
        None,
    )
    .await;

    assert!(reply.result().is_accepted(), "got {:?}", reply.result());
    assert_eq!(streak_and_stamp(&fx).await.0, 1);
}

/// An epoch whose wake was released by hand before the goal ended still
/// returns its budget to the ended goal. There is no place left to release,
/// and the reply says so. The same result delivered again is answered from
/// the record and changes nothing.
#[tokio::test]
async fn an_ended_goal_settles_an_epoch_whose_wake_was_released_by_hand() {
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    fx.apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
        operation_id: rakka_agent::AgentOperationId::new(
            rakka_agent::AgentOperationKind::Command,
            [common::TENANT, common::TASK, "manual-release"],
        )
        .expect("the operation id derives"),
        wake: binding.wake_id().clone(),
    })
    .await
    .expect("the manual release applies");
    fx.apply_task_command(a_persons_result())
        .await
        .expect("the person's result applies");
    assert_eq!(goal(&fx).await, ((1, 1, 0, 0), 0));
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 1)),
        "the released epoch still holds its budget"
    );
    let envelope = epoch_result_for(&binding, AgentTaskStatus::Completed, 1);

    let reply = hand_the_goal(&fx, &envelope, None).await;

    assert!(reply.result().is_accepted(), "got {:?}", reply.result());
    assert_eq!(released_by(&reply), None, "no place was left to release");
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert_eq!(goal(&fx).await, ((1, 1, 0, 0), 1));

    let again = hand_the_goal(&fx, &envelope, None).await;

    assert!(
        again.is_replayed(),
        "the second delivery is answered from the record"
    );
    assert!(again.result().is_accepted());
    assert_eq!(goal(&fx).await, ((1, 1, 0, 0), 1));
    assert_eq!(
        history_of_the_goal(&fx)
            .await
            .into_iter()
            .filter(|(kind, _)| *kind == AgentTaskHistoryKind::EpochSettled)
            .count(),
        1
    );
}

/// An agent-owned continuous goal has a run of its own. When the goal accepts
/// that run's result while an epoch is still out, the goal is ended over the
/// epoch, as it is by a person's result. The epoch's result is settled the
/// same way.
#[tokio::test]
async fn a_goal_ended_by_its_own_runs_result_settles_its_epochs_result() {
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(proposing_turn())
            .with_turn(proposing_turn()),
    );
    fx.instantiate_agent().await;
    fx.create_continuous_task_with_mode(continuous_goal_mode(wake_policy()))
        .await;
    let (epoch, run, disposition) = deliver(&fx, 5).await;
    assert!(disposition.is_admission(), "got {disposition:?}");
    let mut sent = false;
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
        sent = load_agent_run_state(&fx.runs, &run, &AgentSchemaPolicy::default())
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
            break;
        }
    }
    assert!(sent, "the epoch's run sent its model call");

    fx.pump().await.expect("the goal's own run converges");
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 1)),
        "the goal accepted its own run's result over the epoch that is out"
    );

    fx.pump_epoch(&epoch, &run)
        .await
        .expect("the epoch converges");

    assert_eq!(
        epoch_results_reply(&fx, &epoch).await.as_deref(),
        Some("accepted")
    );
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert_eq!(
        goal(&fx).await,
        ((1, 1, 0, 0), 2),
        "the place is released, and the goal counts its own run's iteration and the epoch's"
    );
}

/// A control task can itself be a delegated child, which owes its parent a
/// result once it has ended and its ledger is closed. Where it ended over an
/// epoch, the epoch's result is what closes the ledger, so that settlement
/// owes the parent its result.
#[tokio::test]
async fn an_ended_delegated_goal_reports_to_its_parent_when_its_epoch_settles() {
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    let parent_task = rakka_agent::AgentTaskId::new("parent-task").expect("the id is valid");
    let mut command =
        common::continuous_control_creation_command(continuous_goal_mode(wake_policy()));
    let AgentTaskEntityCommand::Create { creation, .. } = &mut command else {
        panic!("the helper builds a creation");
    };
    creation.delegation = Some(Box::new(rakka_agent::AgentTaskDelegationProvenance {
        environments: Default::default(),
        knowledge_spaces: Default::default(),
        read_capabilities: Default::default(),
        delegation: rakka_agent::AgentDelegationId::new("delegation-1").expect("the id is valid"),
        parent_task: parent_task.clone(),
        parent_run: AgentRunScope::new(
            common::tenant(),
            rakka_agent::AgentId::new("coordinator").expect("the id is valid"),
            rakka_agent::AgentRunId::new("parent-task-gen-1").expect("the id is valid"),
        )
        .expect("the scope is valid"),
        lineage: Vec::new(),
        ancestors: Vec::new(),
        depth: 1,
        requested_skill: rakka_agent::AgentCapabilityId::new("billing").expect("the id is valid"),
        capability_scopes: Default::default(),
        credential_bindings: Vec::new(),
        result_schema: None,
        budget: None,
        deadline: None,
    }));
    creation.parent = Some(parent_task);
    fx.apply_task_command(command)
        .await
        .expect("the delegated control task is created");
    let binding = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    fx.apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    fx.apply_task_command(a_persons_result())
        .await
        .expect("the person's result applies");
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 1))
    );
    assert!(
        !goal_has_sent(&fx, "delegation-result").await,
        "the ledger is open, so the parent is not told yet"
    );

    let reply = hand_the_goal(
        &fx,
        &epoch_result_for(&binding, AgentTaskStatus::Completed, 1),
        None,
    )
    .await;

    assert!(reply.result().is_accepted(), "got {:?}", reply.result());
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert!(
        goal_has_sent(&fx, "delegation-result").await,
        "the epoch's result closed the ledger, and the parent is told"
    );
}

/// Whether the control task has sent, or still owes, an exchange of this kind.
async fn goal_has_sent(fx: &Fixture, kind: &str) -> bool {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task exists");
    let journal = serde_json::to_value(&state).expect("the state serializes")["journal"].take();
    let sent = |list: &str, pointer: &str| {
        journal[list].as_array().is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry.pointer(pointer).and_then(|found| found.as_str()) == Some(kind))
        })
    };
    sent("pending", "/envelope/kind") || sent("settled", "/kind")
}
