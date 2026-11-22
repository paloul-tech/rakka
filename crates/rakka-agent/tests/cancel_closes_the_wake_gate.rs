//! A cancel closes a continuous control task's wake gate, whether or not the
//! task holds a goal record (issue #198).
//!
//! A control task with no goal record (a continuous goal mode, no goal
//! contract) used to keep its gate `Active` when it was asked to cancel. The
//! cancellation finalizes only once the task's ledger has nothing out, and
//! each epoch's result released its occurrence and admitted the next parked
//! one, whose epoch was then created and run. So a cancelled task went on
//! starting epochs for as long as occurrences kept coming. A control task
//! with a goal record did not: its cancel decides the goal and retires the
//! gate.
//!
//! The request now retires the gate in either case. Every transition that
//! can admit on a cancelling task (an epoch's result, a delivery, an
//! operator's release and an operator's resume) also retires it first, so a
//! record an earlier build left cancelling with its gate open admits nothing
//! either.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    delegation_cancel_operation_id, epoch_result_operation_id, epoch_task_id_for_wake,
    load_agent_run_state, load_agent_task_state, wake_admission_command, AgentBudgetConsumption,
    AgentBudgetDimension, AgentDelegationCancelRequest, AgentEntityAddress, AgentEpochResult,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentExchangeReply,
    AgentGoalLifecycleStatus, AgentGoalStatus, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentRevisionNumber, AgentRunEffectKind, AgentRunEffectStatus,
    AgentRunScope, AgentSchemaPolicy, AgentTaskContent, AgentTaskEntityCommand,
    AgentTaskEntityReply, AgentTaskError, AgentTaskHistoryKind, AgentTaskScope, AgentTaskState,
    AgentTaskStatus, AgentWakeBinding, AgentWakeDisposition, AgentWakeOutcome,
    AgentWakeScanOutcome, ScheduleRevision, AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE,
    AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE, AGENT_EPOCH_RESULT_PAYLOAD_TYPE,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION, METRIC_AGENT_GOAL_LIFECYCLE,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::DurableStateStore;

mod common;

use common::{
    continuous_control_creation_command, continuous_goal_control_creation_command,
    continuous_goal_mode, epoch_scopes_for, goal_spec, goal_spec_draft, provenance,
    scheduled_wake_binding, task_scope, wake_policy, Fixture, TASK, TENANT,
};

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Observed the nightly state.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "reconciled" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A fixture whose model answers `turns` proposing turns. A build that
/// admits after the cancel runs more epochs than one that does not.
fn fixture() -> Fixture {
    let mut dispatcher = ScriptedDispatcher::new();
    for _ in 0..4 {
        dispatcher = dispatcher.with_turn(proposing_turn());
    }
    Fixture::new(dispatcher)
}

fn operation(kind: AgentOperationKind, step: &str) -> AgentOperationId {
    AgentOperationId::new(kind, [TENANT, TASK, step]).expect("the operation id derives")
}

fn cancel_command() -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Cancel {
        operation_id: operation(AgentOperationKind::Cancellation, "root-cancel"),
        reason: "operator".to_string(),
    }
}

/// One delivered occurrence: its binding, its epoch's scopes, and how the
/// controller dispositioned it.
struct Occurrence {
    binding: AgentWakeBinding,
    epoch: AgentTaskScope,
    run: AgentRunScope,
    disposition: AgentWakeDisposition,
    marked_terminal: bool,
}

/// Parks one occurrence due at `due_at`, moves the clock past it, and has
/// the scanner deliver it.
async fn deliver(fx: &Fixture, due_at: u64) -> Occurrence {
    let binding = fx.schedule_wake(due_at, ScheduleRevision::INITIAL).await;
    fx.clock.store(due_at + 1_000, Ordering::SeqCst);
    let scan = fx
        .wake_scanner()
        .scan_due()
        .await
        .expect("the scan pass runs");
    assert_eq!(scan.outcomes.len(), 1, "one occurrence was due");
    let AgentWakeScanOutcome::Dispositioned {
        disposition,
        marked,
        ..
    } = &scan.outcomes[0]
    else {
        panic!(
            "the controller dispositions the occurrence, got {:?}",
            scan.outcomes[0]
        );
    };
    let (epoch, run) = epoch_scopes_for(binding.wake_id());
    Occurrence {
        binding,
        epoch,
        run,
        disposition: disposition.clone(),
        marked_terminal: marked.is_terminal(),
    }
}

/// Drives an admitted epoch until its run holds its model call, and no
/// further.
async fn run_until_its_model_call_is_out(fx: &Fixture, occurrence: &Occurrence) {
    for _round in 0..16 {
        for scope in [task_scope(), occurrence.epoch.clone()] {
            fx.settle_task_at(&scope).await.expect("the task settles");
        }
        let mut entity = fx.run_at(&occurrence.run);
        if entity.recover(fx.now()).await.is_ok() {
            entity
                .settle_side_effects(&fx.router, fx.now())
                .await
                .expect("the run settles");
        }
        let out = load_agent_run_state(&fx.runs, &occurrence.run, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .and_then(|state| state.loop_state().map(|held| held.effects().to_vec()))
            .unwrap_or_default()
            .iter()
            .any(|effect| {
                effect.kind() == AgentRunEffectKind::ModelCall
                    && effect.status == AgentRunEffectStatus::Ready
            });
        if out {
            return;
        }
    }
    panic!("the epoch's run never sent its model call");
}

/// The control task a test starts from.
#[derive(Clone, Copy)]
enum ControlTask {
    /// The issue's: no goal record, the fixture's bounded definition.
    NoGoalRecord,
    /// No goal record, and a ledger with no ceiling. The bounded ledger
    /// cannot pay for a second epoch while A holds its budget, so only here
    /// can a release by hand promote B.
    NoGoalRecordUnbounded,
    /// A goal record, active.
    GoalRecord,
}

/// A control task whose first epoch A is running, with occurrence B parked
/// behind it.
async fn an_epoch_out_and_one_parked(
    fx: &Fixture,
    control_task: ControlTask,
) -> (Occurrence, Occurrence) {
    create_the_control_task(fx, control_task).await;
    let a = deliver(fx, 5).await;
    assert!(
        matches!(a.disposition, AgentWakeDisposition::Admitted { .. }),
        "got {:?}",
        a.disposition
    );
    run_until_its_model_call_is_out(fx, &a).await;
    let b = deliver(fx, 2_005).await;
    assert!(
        matches!(b.disposition, AgentWakeDisposition::Coalesced { .. }),
        "got {:?}",
        b.disposition
    );
    (a, b)
}

/// Creates the fixture's control task.
async fn create_the_control_task(fx: &Fixture, control_task: ControlTask) {
    fx.instantiate_agent().await;
    let command = match control_task {
        ControlTask::NoGoalRecord => {
            continuous_control_creation_command(continuous_goal_mode(wake_policy()))
        }
        ControlTask::NoGoalRecordUnbounded => {
            let mut command =
                continuous_control_creation_command(continuous_goal_mode(wake_policy()));
            let AgentTaskEntityCommand::Create { creation, .. } = &mut command else {
                panic!("the helper builds a creation");
            };
            creation.definition = common::unbounded_task_definition()
                .with_ownership(rakka_agent::AgentTaskOwnership::Human);
            command
        }
        ControlTask::GoalRecord => continuous_goal_control_creation_command(
            continuous_goal_mode(wake_policy()),
            goal_spec_draft(goal_spec(), true),
        ),
    };
    fx.apply_task_command(command)
        .await
        .expect("the control task is created");
}

/// The control task's stored state.
async fn root(fx: &Fixture) -> AgentTaskState {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists")
}

/// The control task's status, whether its cancellation is recorded, and how
/// many children of its ledger are out.
async fn task_line(fx: &Fixture) -> (AgentTaskStatus, bool, usize) {
    let state = root(fx).await;
    let task = state.task().expect("the root is created");
    (
        task.status,
        task.cancellation.is_some(),
        task.escrow.outstanding().count(),
    )
}

/// The gate: its status and lifecycle revision.
async fn gate(fx: &Fixture) -> (AgentGoalLifecycleStatus, AgentRevisionNumber) {
    let state = root(fx).await;
    let lifecycle = state
        .task()
        .and_then(|task| task.wake_controller.as_ref())
        .expect("the controller exists")
        .lifecycle()
        .clone();
    (lifecycle.status(), lifecycle.lifecycle_revision())
}

/// The controller's admitted and released counts, and how many occurrences
/// it holds active and parked.
async fn controller(fx: &Fixture) -> (u64, u64, usize, usize) {
    let state = root(fx).await;
    let controller = state
        .task()
        .and_then(|task| task.wake_controller.as_ref())
        .expect("the controller exists");
    (
        controller.counters().admitted,
        controller.counters().released,
        controller.active().len(),
        controller.pending().len(),
    )
}

/// Whether an epoch task exists at `scope`.
async fn exists(fx: &Fixture, scope: &AgentTaskScope) -> bool {
    load_agent_task_state(&fx.tasks, scope, &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .is_some()
}

/// What the reply an epoch task holds to its result names: the wake
/// released, and the occurrence admitted into its place.
async fn reply_to_the_result_of(fx: &Fixture, epoch: &AgentTaskScope) -> (String, Option<String>) {
    let state = load_agent_task_state(&fx.tasks, epoch, &AgentSchemaPolicy::default())
        .await
        .expect("the epoch state loads")
        .expect("the epoch exists");
    let journal = serde_json::to_value(&state).expect("the state serializes")["journal"].take();
    let entry = journal["settled"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["kind"] == "epoch-result")
        .cloned()
        .expect("the epoch's result was answered");
    let result: rakka_agent::AgentExchangeResult =
        serde_json::from_value(entry["result"].clone()).expect("the result deserializes");
    assert!(result.is_accepted(), "got {result:?}");
    released_in(result.payload())
}

/// The release an epoch result's reply payload names.
fn released_in(payload: &AgentExchangePayload) -> (String, Option<String>) {
    let outcome: Option<AgentWakeOutcome> = payload
        .decode(AGENT_EPOCH_RESULT_OUTCOME_PAYLOAD_TYPE)
        .expect("the outcome decodes");
    match outcome {
        Some(AgentWakeOutcome::Release(release)) => (
            release.released.to_string(),
            release.admitted_next.map(|next| next.to_string()),
        ),
        other => panic!("an epoch's result releases its wake, got {other:?}"),
    }
}

/// The control task's history entries of the kinds this change concerns,
/// from its cancellation request on.
async fn history_from_the_request(fx: &Fixture) -> Vec<(AgentTaskHistoryKind, String)> {
    let mut entries = Vec::new();
    let mut cursor = Some(rakka_agent::AgentTaskHistoryCursor::start());
    while let Some(position) = cursor {
        let page = rakka_agent::AgentTaskHistoryStore::read(&fx.history, &task_scope(), position)
            .await
            .expect("the history reads");
        entries.extend(page.entries.iter().filter_map(|entry| {
            matches!(
                entry.kind,
                AgentTaskHistoryKind::CancellationRequested
                    | AgentTaskHistoryKind::EpochAdmitted
                    | AgentTaskHistoryKind::EpochSettled
                    | AgentTaskHistoryKind::WakeDispositioned
                    | AgentTaskHistoryKind::GoalDecided
                    | AgentTaskHistoryKind::GoalRetired
                    | AgentTaskHistoryKind::GoalSuspended
            )
            .then(|| (entry.kind, entry.detail.clone()))
        }));
        cursor = page.next;
    }
    let start = entries
        .iter()
        .position(|(kind, _)| *kind == AgentTaskHistoryKind::CancellationRequested)
        .expect("the request is recorded");
    entries.split_off(start)
}

/// The kinds alone.
fn kinds(entries: &[(AgentTaskHistoryKind, String)]) -> Vec<AgentTaskHistoryKind> {
    entries.iter().map(|(kind, _)| *kind).collect()
}

/// The code a command was refused with, whichever way the refusal came.
fn refusal(reply: Result<AgentTaskEntityReply, AgentTaskError>) -> Option<String> {
    match reply {
        Ok(AgentTaskEntityReply::Rejected { code, .. }) => Some(code),
        Err(error) => Some(error.code().to_string()),
        Ok(_) => None,
    }
}

/// The release an `Applied` reply's wake outcome names.
fn released_by_command(reply: AgentTaskEntityReply) -> (String, Option<String>) {
    let AgentTaskEntityReply::Applied { outcome } = reply else {
        panic!("the command applies, got {reply:?}");
    };
    match outcome.wake {
        Some(AgentWakeOutcome::Release(release)) => (
            release.released.to_string(),
            release.admitted_next.map(|next| next.to_string()),
        ),
        other => panic!("the command releases its wake, got {other:?}"),
    }
}

/// The stored gate (the controller's lifecycle record), as JSON.
async fn stored_gate(fx: &Fixture) -> serde_json::Value {
    let state = root(fx).await;
    serde_json::to_value(&state).expect("the state serializes")["task"]["wake_controller"]
        ["lifecycle"]
        .clone()
}

/// Writes `lifecycle` back as the stored gate. An earlier build's cancel
/// changed nothing on the controller, so restoring the gate as it was before
/// the cancel makes the record that build wrote.
async fn restore_the_gate(fx: &Fixture, lifecycle: serde_json::Value) {
    let id = task_scope().persistence_id();
    let record = fx
        .tasks
        .load(&id)
        .await
        .expect("the task record loads")
        .expect("the task record exists");
    let mut value = serde_json::to_value(&record.state).expect("the task state serializes");
    value["task"]["wake_controller"]["lifecycle"] = lifecycle;
    let rewritten = serde_json::from_value(value).expect("the rewritten record deserializes");
    fx.tasks
        .compare_and_set(&id, record.revision, rewritten)
        .await
        .expect("the rewritten record persists");
}

/// The issue's setup, as an earlier build left it: the cancel recorded, the
/// gate still as it was before the cancel.
async fn an_earlier_builds_cancelling_record(fx: &Fixture, before_the_cancel: serde_json::Value) {
    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");
    restore_the_gate(fx, before_the_cancel).await;
    let (_, cancelling, out) = task_line(fx).await;
    assert!(cancelling, "the cancellation is recorded");
    assert_eq!(out, 1, "epoch A is out");
    assert_ne!(
        gate(fx).await.0,
        AgentGoalLifecycleStatus::Retired,
        "the gate is open, as the earlier build left it"
    );
}

/// The cancel closes the gate of a control task that holds no goal record,
/// in the transition that records the request. Nothing else on the
/// controller moves: epoch A is still out and B is still parked. No history
/// entry stands for the closing beyond the request's own.
#[tokio::test]
async fn a_cancel_retires_the_gate_of_a_control_task_with_no_goal_record() {
    let fx = fixture();
    let _ = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecord).await;
    assert_eq!(
        gate(&fx).await,
        (
            AgentGoalLifecycleStatus::Active,
            AgentRevisionNumber::INITIAL
        )
    );
    assert_eq!(controller(&fx).await, (1, 0, 1, 1));

    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");

    assert_eq!(
        task_line(&fx).await,
        (AgentTaskStatus::WaitingForInput, true, 1),
        "the cancel waits for epoch A"
    );
    assert_eq!(
        gate(&fx).await,
        (
            AgentGoalLifecycleStatus::Retired,
            AgentRevisionNumber::INITIAL.next()
        ),
        "the request retires the gate"
    );
    assert_eq!(controller(&fx).await, (1, 0, 1, 1));
    assert_eq!(
        kinds(&history_from_the_request(&fx).await),
        vec![AgentTaskHistoryKind::CancellationRequested]
    );
}

/// The issue's measurement, which this change reverses. Epoch A's result
/// releases A and admits nothing in its place, B's epoch is never created,
/// and the task ends `Cancelled` once A has settled.
#[tokio::test]
async fn the_cancelling_tasks_epoch_result_admits_nothing_and_the_cancel_completes() {
    let fx = fixture();
    let (a, b) = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecord).await;
    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");

    fx.pump_epoch(&a.epoch, &a.run)
        .await
        .expect("epoch A converges");

    assert_eq!(
        reply_to_the_result_of(&fx, &a.epoch).await,
        (a.binding.wake_id().to_string(), None),
        "A's result admits nothing"
    );
    for _ in 0..3 {
        fx.settle_task_at(&task_scope())
            .await
            .expect("the root settles");
    }
    assert!(!exists(&fx, &b.epoch).await, "B's epoch is never created");
    assert_eq!(task_line(&fx).await, (AgentTaskStatus::Cancelled, true, 0));
    assert_eq!(controller(&fx).await, (1, 1, 0, 1), "B stays parked");
    let history = history_from_the_request(&fx).await;
    assert_eq!(
        kinds(&history),
        vec![
            AgentTaskHistoryKind::CancellationRequested,
            AgentTaskHistoryKind::EpochSettled,
        ],
        "nothing is admitted after the request: {history:?}"
    );
}

/// An occurrence delivered while the cancelling task waits for its epoch is
/// barred by the retired gate, and the scanner marks its timer entry
/// terminal. No epoch is created for it.
#[tokio::test]
async fn an_occurrence_delivered_while_the_cancel_waits_is_barred() {
    let fx = fixture();
    let _ = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecord).await;
    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");

    let c = deliver(&fx, 4_005).await;

    assert!(
        matches!(
            c.disposition,
            AgentWakeDisposition::Barred {
                status: AgentGoalLifecycleStatus::Retired,
                ..
            }
        ),
        "got {:?}",
        c.disposition
    );
    assert!(c.marked_terminal, "the scanner does not deliver it again");
    assert_eq!(
        controller(&fx).await,
        (1, 0, 1, 1),
        "B is still the one parked"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert!(!exists(&fx, &c.epoch).await);
}

/// An operator's release of the cancelling task's wake frees the place and
/// admits nothing into it.
#[tokio::test]
async fn an_operators_release_on_the_cancelling_task_admits_nothing() {
    let fx = fixture();
    let (a, b) = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecordUnbounded).await;
    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");

    let reply = fx
        .apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
            operation_id: operation(AgentOperationKind::Command, "manual-release"),
            wake: a.binding.wake_id().clone(),
        })
        .await
        .expect("the release applies");

    assert_eq!(
        released_by_command(reply),
        (a.binding.wake_id().to_string(), None)
    );
    assert_eq!(controller(&fx).await, (1, 1, 0, 1));
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert!(!exists(&fx, &b.epoch).await);
}

/// A cancel that finds the gate suspended by an operator retires it too, and
/// the operator's resume is then refused: it would have admitted the parked
/// occurrence.
#[tokio::test]
async fn a_cancel_that_finds_the_gate_suspended_retires_it_and_no_resume_admits() {
    let fx = fixture();
    let b = a_suspended_gate_with_an_epoch_out_and_one_parked(&fx).await;

    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");

    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Retired);
    let refused = refusal(fx.apply_task_command(resume_command(&fx).await).await);
    assert_eq!(refused.as_deref(), Some("wake-lifecycle-terminal"));
    assert_eq!(controller(&fx).await, (1, 0, 1, 1), "B is not admitted");
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert!(!exists(&fx, &b.epoch).await);
}

/// Epoch A out, the gate suspended by an operator, and B parked by the
/// suspension, on a ledger that could pay for B while A is out: a resume
/// would admit B. Returns B.
async fn a_suspended_gate_with_an_epoch_out_and_one_parked(fx: &Fixture) -> Occurrence {
    create_the_control_task(fx, ControlTask::NoGoalRecordUnbounded).await;
    let a = deliver(fx, 5).await;
    run_until_its_model_call_is_out(fx, &a).await;
    fx.apply_task_command(AgentTaskEntityCommand::SuspendContinuousGoal {
        operation_id: operation(AgentOperationKind::LifecycleSuspend, "suspend"),
        expected_lifecycle_revision: gate(fx).await.1,
        reason: Some("maintenance".to_string()),
        provenance: Box::new(provenance(11)),
    })
    .await
    .expect("the suspension applies");
    let b = deliver(fx, 2_005).await;
    assert!(
        matches!(b.disposition, AgentWakeDisposition::SuspendedParked { .. }),
        "got {:?}",
        b.disposition
    );
    assert_eq!(controller(fx).await, (1, 0, 1, 1));
    b
}

/// The operator's resume, at the gate's current revision.
async fn resume_command(fx: &Fixture) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::ResumeContinuousGoal {
        operation_id: operation(AgentOperationKind::LifecycleResume, "resume"),
        expected_lifecycle_revision: gate(fx).await.1,
        provenance: Box::new(provenance(12)),
    }
}

/// Once the cancel retired the gate, the operator's gate commands are
/// refused as on any retired gate, the issue's workaround among them.
#[tokio::test]
async fn the_gate_commands_are_refused_once_the_cancel_retired_the_gate() {
    let fx = fixture();
    let _ = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecord).await;
    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");
    let revision = gate(&fx).await.1;

    let commands = [
        AgentTaskEntityCommand::SuspendContinuousGoal {
            operation_id: operation(AgentOperationKind::LifecycleSuspend, "suspend"),
            expected_lifecycle_revision: revision,
            reason: None,
            provenance: Box::new(provenance(11)),
        },
        AgentTaskEntityCommand::RenewContinuousGoal {
            operation_id: operation(AgentOperationKind::Command, "renew"),
            expected_lifecycle_revision: revision,
            new_expires_at: AgentTimestampMillis::new(900_000),
            provenance: Box::new(provenance(12)),
        },
        AgentTaskEntityCommand::RetireContinuousGoal {
            operation_id: operation(AgentOperationKind::LifecycleTerminate, "retire"),
            expected_lifecycle_revision: revision,
            provenance: Box::new(provenance(13)),
        },
    ];
    for command in commands {
        assert_eq!(
            refusal(fx.apply_task_command(command).await).as_deref(),
            Some("wake-lifecycle-terminal")
        );
    }
    assert_eq!(gate(&fx).await.1, revision, "nothing moved the gate");
}

/// The lifecycle metric counts the retirement once, from the transition
/// that recorded the request, and nothing after it counts another.
#[tokio::test]
async fn the_lifecycle_metric_counts_the_retirement_once() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = fixture().with_metrics(metrics.clone());
    let (a, _) = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecord).await;
    assert_eq!(retirements(&metrics), 0);

    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");
    assert_eq!(retirements(&metrics), 1);

    fx.pump_epoch(&a.epoch, &a.run)
        .await
        .expect("epoch A converges");
    assert_eq!(task_line(&fx).await.0, AgentTaskStatus::Cancelled);
    assert_eq!(retirements(&metrics), 1);
}

/// How many lifecycle transitions to `retired` the metric counted.
fn retirements(metrics: &InMemoryMetricsRecorder) -> usize {
    metrics
        .snapshot()
        .observations_named(METRIC_AGENT_GOAL_LIFECYCLE)
        .iter()
        .filter(|observation| {
            observation
                .attributes()
                .iter()
                .any(|attribute| attribute.key() == "transition" && attribute.value() == "retired")
        })
        .count()
}

/// A delegated control task has no goal record: creation refuses a delegated
/// child a goal contract, not a continuous mode. Its parent run's cancel
/// reaches it as a `DelegationCancel`, which takes the same request, so its
/// gate closes too. Epoch A's result admits nothing, and the child tells its
/// parent once A has settled.
#[tokio::test]
async fn a_delegated_control_task_cancelled_by_its_parent_admits_nothing_further() {
    let fx = Fixture::new(ScriptedDispatcher::new());
    fx.instantiate_agent().await;
    let parent_run = AgentRunScope::new(
        common::tenant(),
        rakka_agent::AgentId::new("coordinator").expect("the id is valid"),
        rakka_agent::AgentRunId::new("parent-task-gen-1").expect("the id is valid"),
    )
    .expect("the scope is valid");
    let delegation = rakka_agent::AgentDelegationId::new("delegation-1").expect("the id is valid");
    create_a_delegated_control_task(&fx, &parent_run, &delegation).await;
    let a = scheduled_wake_binding(5, ScheduleRevision::INITIAL);
    let b = scheduled_wake_binding(2_005, ScheduleRevision::INITIAL);
    for binding in [&a, &b] {
        fx.apply_task_command(
            wake_admission_command(binding.clone()).expect("the command derives"),
        )
        .await
        .expect("the admission applies");
    }
    assert_eq!(controller(&fx).await, (1, 0, 1, 1));

    let reply = hand_the_root(&fx, &delegation_cancel(&delegation, &parent_run, &fx)).await;
    assert!(reply.result().is_accepted(), "got {:?}", reply.result());
    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Retired);

    let reply = hand_the_root(&fx, &epoch_result_for(&a)).await;
    assert!(reply.result().is_accepted(), "got {:?}", reply.result());
    assert_eq!(
        released_in(reply.result().payload()),
        (a.wake_id().to_string(), None),
        "A's result admits nothing"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert_eq!(task_line(&fx).await, (AgentTaskStatus::Cancelled, true, 0));
    assert!(
        root_has_sent(&fx, "delegation-result").await,
        "the parent is told"
    );
}

/// Creates the fixture's control task as a child delegated by `parent_run`.
async fn create_a_delegated_control_task(
    fx: &Fixture,
    parent_run: &AgentRunScope,
    delegation: &rakka_agent::AgentDelegationId,
) {
    let parent_task = rakka_agent::AgentTaskId::new("parent-task").expect("the id is valid");
    let mut command = continuous_control_creation_command(continuous_goal_mode(wake_policy()));
    let AgentTaskEntityCommand::Create { creation, .. } = &mut command else {
        panic!("the helper builds a creation");
    };
    creation.delegation = Some(Box::new(rakka_agent::AgentTaskDelegationProvenance {
        environments: Default::default(),
        knowledge_spaces: Default::default(),
        read_capabilities: Default::default(),
        delegation: delegation.clone(),
        parent_task: parent_task.clone(),
        parent_run: parent_run.clone(),
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
}

/// The cancel the parent run sends its delegated child.
fn delegation_cancel(
    delegation: &rakka_agent::AgentDelegationId,
    parent_run: &AgentRunScope,
    fx: &Fixture,
) -> AgentExchangeEnvelope {
    let operation_id =
        delegation_cancel_operation_id(&common::tenant(), delegation).expect("derives");
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::DelegationCancel,
        AgentEntityAddress::Run(parent_run.clone()),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(
            AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE,
            &AgentDelegationCancelRequest {
                delegation: delegation.clone(),
                child_task: task_scope().task().clone(),
                reason: "parent-cancelled".to_string(),
            },
        )
        .expect("the payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        fx.now(),
    )
    .expect("the envelope builds")
}

/// The result the epoch of `binding`'s occurrence would send its goal: a
/// completion that spent one loop iteration.
fn epoch_result_for(binding: &AgentWakeBinding) -> AgentExchangeEnvelope {
    let epoch_task = epoch_task_id_for_wake(binding.wake_id()).expect("the epoch derives");
    let epoch_scope =
        AgentTaskScope::new(common::tenant(), epoch_task.clone()).expect("the scope is valid");
    let operation_id =
        epoch_result_operation_id(&common::tenant(), &common::goal_id(), binding.wake_id())
            .expect("the operation id derives");
    let mut consumed = AgentBudgetConsumption::zero();
    consumed.add(AgentBudgetDimension::LoopIterations, 1);
    let result = AgentEpochResult {
        wake: binding.wake_id().clone(),
        task: epoch_task,
        status: AgentTaskStatus::Completed,
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

/// Hands the control task an exchange, and returns its reply.
async fn hand_the_root(fx: &Fixture, envelope: &AgentExchangeEnvelope) -> AgentExchangeReply {
    let mut root = rakka_agent::AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    )
    .with_wake_timers(fx.rewake_parker.clone());
    root.recover(fx.now()).await.expect("the root recovers");
    root.accept(envelope, &fx.router, fx.now())
        .await
        .expect("the exchange is answered")
}

/// Whether the control task has sent, or still owes, an exchange of this kind.
async fn root_has_sent(fx: &Fixture, kind: &str) -> bool {
    let state = root(fx).await;
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

/// A control task with a goal record, measured the same way, is the case
/// the issue contrasts: its cancel decides the goal and retires the gate, and
/// A's result admits nothing. It was right before this change and is
/// unchanged by it, goal reason included.
#[tokio::test]
async fn a_control_task_with_a_goal_record_is_unchanged() {
    let fx = fixture();
    let (a, b) = an_epoch_out_and_one_parked(&fx, ControlTask::GoalRecord).await;

    fx.apply_task_command(cancel_command())
        .await
        .expect("the cancel applies");

    assert_eq!(
        gate(&fx).await,
        (
            AgentGoalLifecycleStatus::Retired,
            AgentRevisionNumber::INITIAL.next()
        )
    );
    let state = root(&fx).await;
    let goal = state
        .task()
        .and_then(|task| task.goal_state.as_deref())
        .expect("the goal record exists");
    assert_eq!(goal.status(), AgentGoalStatus::Cancelled);
    assert_eq!(
        goal.terminal().map(|decision| decision.reason.code()),
        Some("root-task-cancelled")
    );
    fx.pump_epoch(&a.epoch, &a.run)
        .await
        .expect("epoch A converges");
    assert_eq!(
        reply_to_the_result_of(&fx, &a.epoch).await,
        (a.binding.wake_id().to_string(), None)
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert!(!exists(&fx, &b.epoch).await);
    assert_eq!(task_line(&fx).await, (AgentTaskStatus::Cancelled, true, 0));
}

/// A record an earlier build left cancelling with its gate open is closed by
/// the epoch's result that reaches it first: A's result admits nothing, and
/// the task ends `Cancelled`. The closing is not recorded as an observed
/// lifecycle change.
#[tokio::test]
async fn an_earlier_builds_open_gate_is_closed_by_an_epochs_result() {
    let fx = fixture();
    let (a, b) = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecord).await;
    let before = stored_gate(&fx).await;
    an_earlier_builds_cancelling_record(&fx, before).await;

    fx.pump_epoch(&a.epoch, &a.run)
        .await
        .expect("epoch A converges");

    assert_eq!(
        reply_to_the_result_of(&fx, &a.epoch).await,
        (a.binding.wake_id().to_string(), None),
        "A's result admits nothing"
    );
    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Retired);
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert!(!exists(&fx, &b.epoch).await);
    assert_eq!(task_line(&fx).await, (AgentTaskStatus::Cancelled, true, 0));
    assert_eq!(
        kinds(&history_from_the_request(&fx).await),
        vec![
            AgentTaskHistoryKind::CancellationRequested,
            AgentTaskHistoryKind::EpochSettled,
        ]
    );
}

/// The same record is closed by a delivery that reaches it first: the
/// occurrence is barred.
#[tokio::test]
async fn an_earlier_builds_open_gate_is_closed_by_a_delivery() {
    let fx = fixture();
    let _ = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecord).await;
    let before = stored_gate(&fx).await;
    an_earlier_builds_cancelling_record(&fx, before).await;

    let c = deliver(&fx, 4_005).await;

    assert!(
        matches!(
            c.disposition,
            AgentWakeDisposition::Barred {
                status: AgentGoalLifecycleStatus::Retired,
                ..
            }
        ),
        "got {:?}",
        c.disposition
    );
    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Retired);
    assert_eq!(controller(&fx).await, (1, 0, 1, 1));
    assert!(
        !kinds(&history_from_the_request(&fx).await).contains(&AgentTaskHistoryKind::GoalRetired)
    );
}

/// The same record is closed by an operator's release that reaches it
/// first: the release admits nothing.
#[tokio::test]
async fn an_earlier_builds_open_gate_is_closed_by_an_operators_release() {
    let fx = fixture();
    let (a, b) = an_epoch_out_and_one_parked(&fx, ControlTask::NoGoalRecordUnbounded).await;
    let before = stored_gate(&fx).await;
    an_earlier_builds_cancelling_record(&fx, before).await;

    let reply = fx
        .apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
            operation_id: operation(AgentOperationKind::Command, "manual-release"),
            wake: a.binding.wake_id().clone(),
        })
        .await
        .expect("the release applies");

    assert_eq!(
        released_by_command(reply),
        (a.binding.wake_id().to_string(), None)
    );
    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Retired);
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert!(!exists(&fx, &b.epoch).await);
    assert!(
        !kinds(&history_from_the_request(&fx).await).contains(&AgentTaskHistoryKind::GoalRetired)
    );
}

/// A record an earlier build left cancelling with its gate suspended: the
/// operator's resume, the first to reach it, is refused rather than
/// admitting the occurrence the suspension parked.
#[tokio::test]
async fn an_earlier_builds_suspended_gate_is_closed_by_a_resume() {
    let fx = fixture();
    let b = a_suspended_gate_with_an_epoch_out_and_one_parked(&fx).await;
    let before = stored_gate(&fx).await;
    an_earlier_builds_cancelling_record(&fx, before).await;
    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Suspended);

    let refused = refusal(fx.apply_task_command(resume_command(&fx).await).await);

    assert_eq!(refused.as_deref(), Some("wake-lifecycle-terminal"));
    assert_eq!(controller(&fx).await, (1, 0, 1, 1), "B is not admitted");
    fx.settle_task_at(&task_scope())
        .await
        .expect("the root settles");
    assert!(!exists(&fx, &b.epoch).await);
}
