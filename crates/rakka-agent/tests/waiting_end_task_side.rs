//! What stands above a task while its run's end waits (issues #123, #172
//! and #173).
//!
//! A run whose end is decided while it holds a call it sent does not end yet:
//! it waits for that call, and ends when the call is known. Its task is
//! decided all the same, so for that while the task is terminal and its run
//! is not.
//!
//! A task sends what settles its budget upward on its ledger, not on its
//! status: an epoch's result to its wake controller, and the end of its own
//! cancellation. The run returns its budget only when it ends, and until then
//! neither is sent. These tests hold what a continuous goal sees in that
//! window: the wake that admitted the epoch stays held, a later occurrence
//! waits behind it, an operator can still release it by hand, and a cancel of
//! the goal completes only when the run has ended.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    evaluation_operation_id, load_agent_run_state, load_agent_task_state, AgentBudgetAllocation,
    AgentBudgetDimension, AgentCancellationProgress, AgentCheckpointDecision, AgentCheckpointKind,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentReconciliationDecision,
    AgentRevisionNumber, AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunScope, AgentRunStatus, AgentSchemaPolicy,
    AgentTaskContent, AgentTaskEntityCommand, AgentTaskScope, AgentTaskStatus,
    AgentWakeDisposition, AgentWakeId, AgentWakePolicy, AgentWakeScanOutcome, AgentWakeTriggerKind,
    ScheduleRevision, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTelemetryContext, PrincipalRef};

mod common;

use common::{
    agent_scope, continuous_goal_mode, epoch_scopes_for, goal_evaluation_request, goal_id,
    task_scope, wake_policy, Fixture, AGENT, TASK, TENANT,
};

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Observed the nightly state.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "reconciled" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A wake policy whose every epoch is granted one loop iteration, so two
/// epochs can hold budget at once. The fixture's own policy leaves that
/// dimension open, and an open request takes all the goal has left.
fn one_iteration_epochs() -> AgentWakePolicy {
    let mut budget = AgentBudgetAllocation::unbounded();
    budget.set(AgentBudgetDimension::ModelCalls, Some(8));
    budget.set(AgentBudgetDimension::LoopIterations, Some(1));
    AgentWakePolicy::new([AgentWakeTriggerKind::DurableTimer], budget, Some(60_000))
        .expect("the wake policy is valid")
}

/// A continuous goal whose first epoch is accepted while its run still holds
/// a goal evaluation it sent.
struct World {
    fx: Fixture,
    wake: AgentWakeId,
    epoch: AgentTaskScope,
    run: AgentRunScope,
    evaluation: AgentRunEffect,
}

async fn settle_tasks(fx: &Fixture, epoch: &AgentTaskScope) {
    for scope in [task_scope(), epoch.clone()] {
        fx.settle_task_at(&scope).await.expect("the task settles");
    }
}

/// One settle pass of the run, with no dispatcher behind it: a call the run
/// sends stays out until a test reports its result.
async fn settle_run(fx: &Fixture, run: &AgentRunScope) {
    let mut entity = fx.run_at(run);
    let now = fx.now();
    if entity.recover(now).await.is_ok() {
        entity
            .settle_side_effects(&fx.router, now)
            .await
            .expect("the run settles");
    }
}

async fn settle_all(fx: &Fixture, epoch: &AgentTaskScope, run: &AgentRunScope) {
    for _round in 0..4 {
        settle_tasks(fx, epoch).await;
        settle_run(fx, run).await;
    }
}

async fn apply_run(fx: &Fixture, run: &AgentRunScope, command: AgentRunEntityCommand) {
    let mut entity = fx.run_at(run);
    entity.recover(fx.now()).await.expect("the run recovers");
    entity
        .apply(command, &fx.router, fx.now())
        .await
        .expect("the command applies");
}

async fn effects_of(fx: &Fixture, run: &AgentRunScope) -> Vec<AgentRunEffect> {
    load_agent_run_state(&fx.runs, run, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .and_then(|state| state.loop_state().map(|held| held.effects().to_vec()))
        .unwrap_or_default()
}

/// Reports `outcome` as the first attempt's result of `effect`.
async fn record(
    fx: &Fixture,
    run: &AgentRunScope,
    effect: &AgentRunEffect,
    outcome: AgentRunEffectOutcome,
) {
    apply_run(
        fx,
        run,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(run)
                .expect("the result operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(outcome),
        },
    )
    .await;
}

/// The result of a call the dispatcher gave up before invoking it.
fn never_invoked() -> AgentRunEffectOutcome {
    AgentRunEffectOutcome::Cancelled {
        reason: "run-cancelled".to_string(),
    }
}

/// The result of a call whose outcome nobody knows.
fn unknown() -> AgentRunEffectOutcome {
    AgentRunEffectOutcome::indeterminate("connection-lost", "the evaluator died")
}

async fn accepted_epoch_with_an_evaluation_out(policy: AgentWakePolicy) -> World {
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(proposing_turn())
            .with_turn(proposing_turn()),
    );
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(policy))
        .await;
    let binding = fx.schedule_wake(5, ScheduleRevision::INITIAL).await;
    let (epoch, run) = epoch_scopes_for(binding.wake_id());
    fx.clock.fetch_add(1_000, Ordering::SeqCst);
    fx.wake_scanner()
        .scan_due()
        .await
        .expect("the scan pass runs");

    let mut model = None;
    for _round in 0..16 {
        settle_tasks(&fx, &epoch).await;
        settle_run(&fx, &run).await;
        model = effects_of(&fx, &run).await.into_iter().find(|effect| {
            effect.kind() == AgentRunEffectKind::ModelCall
                && effect.status == AgentRunEffectStatus::Ready
        });
        if model.is_some() {
            break;
        }
    }
    let model = model.expect("the epoch's run sent its model call");

    // An epoch contributes to the goal its controller coordinates, so its run
    // is bound to that goal.
    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.goal = goal_id();
    apply_run(
        &fx,
        &run,
        AgentRunEntityCommand::EvaluateGoal {
            operation_id: evaluation_operation_id(&run, "1").expect("the operation id derives"),
            evaluation: Box::new(request),
        },
    )
    .await;
    let evaluation = effects_of(&fx, &run)
        .await
        .into_iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::GoalEvaluationCall)
        .expect("the run holds its goal evaluation");
    assert_eq!(
        evaluation.status,
        AgentRunEffectStatus::Ready,
        "the evaluation was sent"
    );

    let turn = fx.dispatcher.answer(&model).await;
    record(&fx, &run, &model, turn).await;
    settle_all(&fx, &epoch, &run).await;

    let world = World {
        fx,
        wake: binding.wake_id().clone(),
        epoch,
        run,
        evaluation,
    };
    assert_eq!(
        world.epoch_task().await,
        (AgentTaskStatus::Completed, 1),
        "the epoch is accepted, and its run still holds the budget it was given"
    );
    assert_eq!(
        world.run_standing().await,
        (
            AgentRunStatus::Cancelling,
            Some("result-accepted".to_string())
        ),
        "the run waits for the evaluation it sent"
    );
    world
}

impl World {
    /// The epoch task's status and how many children of its ledger are out.
    async fn epoch_task(&self) -> (AgentTaskStatus, usize) {
        task_at(&self.fx, &self.epoch)
            .await
            .expect("the epoch task exists")
    }

    /// The epoch run's status and the code of the reason it ends under.
    async fn run_standing(&self) -> (AgentRunStatus, Option<String>) {
        let state = load_agent_run_state(&self.fx.runs, &self.run, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .expect("the run exists");
        let run = state.run().expect("the run is created");
        (
            run.status,
            run.terminal_reason
                .as_ref()
                .map(|reason| reason.code().to_string()),
        )
    }

    /// The controller's admitted and released counts, and how many
    /// occurrences it holds active and parked.
    async fn controller(&self) -> (u64, u64, usize, usize) {
        let root =
            load_agent_task_state(&self.fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
                .await
                .expect("the root state loads")
                .expect("the root exists");
        let controller = root
            .task()
            .expect("the root is created")
            .wake_controller
            .as_ref()
            .expect("the controller exists");
        (
            controller.counters().admitted,
            controller.counters().released,
            controller.active().len(),
            controller.pending().len(),
        )
    }

    /// How many children of the goal's own ledger are out.
    async fn goal_children_out(&self) -> usize {
        task_at(&self.fx, &task_scope())
            .await
            .expect("the root exists")
            .1
    }

    /// The effect attempts the goal's own ledger counts as spent.
    async fn goal_attempts_spent(&self) -> u64 {
        load_agent_task_state(&self.fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the root state loads")
            .expect("the root exists")
            .task()
            .expect("the root is created")
            .escrow
            .consumed()
            .get(AgentBudgetDimension::EffectAttempts)
    }

    /// The run's evaluation comes back with an outcome nobody knows, and the
    /// run parks on it.
    async fn park_on_the_evaluation(&self) {
        record(&self.fx, &self.run, &self.evaluation, unknown()).await;
        settle_all(&self.fx, &self.epoch, &self.run).await;
        assert_eq!(
            self.run_standing().await,
            (
                AgentRunStatus::WaitingForReconciliation,
                Some("result-accepted".to_string())
            ),
            "the run is parked on the unknown outcome, under the end it was given"
        );
    }

    /// A person answers the run's park: the call is given up as failed.
    async fn answer_the_park(&self) {
        let checkpoint =
            load_agent_run_state(&self.fx.runs, &self.run, &AgentSchemaPolicy::default())
                .await
                .expect("the run state loads")
                .and_then(|state| {
                    state.loop_state().and_then(|held| {
                        held.open_checkpoints()
                            .iter()
                            .find(|checkpoint| {
                                checkpoint.kind
                                    == AgentCheckpointKind::IndeterminateEffectReconciliation
                            })
                            .cloned()
                    })
                })
                .expect("the park holds its checkpoint");
        apply_run(
            &self.fx,
            &self.run,
            AgentRunEntityCommand::ResolveCheckpoint {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::CheckpointResolution,
                    &agent_scope(),
                    "answer",
                )
                .expect("the operation id derives"),
                checkpoint_id: checkpoint.checkpoint_id.clone(),
                resolver: PrincipalRef {
                    principal_type: "user".to_string(),
                    principal_id: "operator".to_string(),
                    display_name: None,
                },
                decision: Box::new(AgentCheckpointDecision::Reconciliation(
                    AgentReconciliationDecision::AbandonAndFail,
                )),
                telemetry: AgentTelemetryContext::default(),
            },
        )
        .await;
    }

    /// A later occurrence of the schedule comes due and is delivered. Returns
    /// its epoch's scopes and how the controller dispositioned it.
    async fn a_later_occurrence(
        &self,
        due_at: u64,
    ) -> (AgentTaskScope, AgentRunScope, AgentWakeDisposition) {
        let binding = self
            .fx
            .schedule_wake(due_at, ScheduleRevision::INITIAL)
            .await;
        self.fx.clock.store(due_at + 100, Ordering::SeqCst);
        let scan = self
            .fx
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

/// An accepted epoch whose run waits for a call still holds the wake that
/// admitted it. The epoch's result goes to the controller when the run has
/// ended and returned its budget, and carries what the run spent while it
/// waited. Until then a later occurrence is parked behind it, and it is
/// admitted the moment the result releases the wake.
#[tokio::test]
async fn an_accepted_epoch_holds_its_wake_until_its_run_ends() {
    let world = accepted_epoch_with_an_evaluation_out(wake_policy()).await;
    assert_eq!(
        world.controller().await,
        (1, 0, 1, 0),
        "the wake is still active: the controller has not been told"
    );

    let (next_epoch, next_run, disposition) = world.a_later_occurrence(2_000_000).await;
    assert!(
        matches!(
            disposition,
            AgentWakeDisposition::Coalesced { replaced: None, .. }
        ),
        "the later occurrence is parked behind the held wake, got {disposition:?}"
    );
    settle_tasks(&world.fx, &world.epoch).await;
    assert_eq!(world.controller().await, (1, 0, 1, 1));
    assert_eq!(
        task_at(&world.fx, &next_epoch).await,
        None,
        "no second epoch exists while the first holds the wake"
    );

    // The dispatcher gives the evaluation up, and the run ends.
    record(&world.fx, &world.run, &world.evaluation, never_invoked()).await;
    world
        .fx
        .pump_epoch(&world.epoch, &world.run)
        .await
        .expect("the first epoch converges");
    assert_eq!(
        world.run_standing().await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string())
        )
    );
    assert_eq!(world.epoch_task().await, (AgentTaskStatus::Completed, 0));
    assert_eq!(
        world.controller().await,
        (2, 1, 1, 0),
        "the result released the wake and the parked occurrence took its place"
    );

    world
        .fx
        .pump_epoch(&next_epoch, &next_run)
        .await
        .expect("the second epoch converges");
    assert_eq!(world.controller().await, (2, 2, 0, 0));
    assert_eq!(
        world.goal_children_out().await,
        0,
        "the goal's ledger is closed"
    );
}

/// An accepted epoch whose run is parked on an unknown outcome holds its wake
/// for as long as the park is open: until a person answers it. Occurrences
/// that come due meanwhile share the one parking place, the newer replacing
/// the older.
#[tokio::test]
async fn a_parked_accepted_epoch_holds_its_wake_until_the_park_is_answered() {
    let world = accepted_epoch_with_an_evaluation_out(wake_policy()).await;
    world.park_on_the_evaluation().await;
    assert_eq!(world.epoch_task().await, (AgentTaskStatus::Completed, 1));
    assert_eq!(world.controller().await, (1, 0, 1, 0));

    let (_, _, second) = world.a_later_occurrence(2_000_000).await;
    assert!(
        matches!(
            second,
            AgentWakeDisposition::Coalesced { replaced: None, .. }
        ),
        "got {second:?}"
    );
    let (_, _, third) = world.a_later_occurrence(4_000_000).await;
    assert!(
        matches!(
            third,
            AgentWakeDisposition::Coalesced {
                replaced: Some(_),
                ..
            }
        ),
        "the third occurrence replaces the second in the one parking place, got {third:?}"
    );
    settle_all(&world.fx, &world.epoch, &world.run).await;
    assert_eq!(
        world.controller().await,
        (1, 0, 1, 1),
        "nothing moves while the park is open"
    );

    world.answer_the_park().await;
    world
        .fx
        .pump_epoch(&world.epoch, &world.run)
        .await
        .expect("the first epoch converges");
    assert_eq!(
        world.run_standing().await,
        (
            AgentRunStatus::Completed,
            Some("result-accepted".to_string())
        )
    );
    assert_eq!(world.epoch_task().await, (AgentTaskStatus::Completed, 0));
    assert_eq!(
        world.controller().await,
        (2, 1, 1, 0),
        "the answer ended the run, and its result released the wake"
    );
    assert_eq!(
        world.goal_attempts_spent().await,
        2,
        "the epoch's result carried the run's whole spend: the model call's attempt, and the \
         evaluation's, which was still out when the epoch was accepted"
    );
}

/// An operator need not wait for the park. Releasing the wake by hand lets the
/// next epoch run while the first is still parked. The first epoch's result
/// comes at last when its park is answered: it returns the budget the goal
/// gave that run, and it releases nothing, though by then another occurrence
/// is active and one more is parked behind it.
///
/// The epochs here are each granted one loop iteration. Under a wake policy
/// that leaves a dimension open, the parked epoch holds all the goal has in
/// it, and the epoch admitted beside it is created with none (issue #179).
#[tokio::test]
async fn an_operator_can_release_the_wake_a_parked_epoch_holds() {
    let world = accepted_epoch_with_an_evaluation_out(one_iteration_epochs()).await;
    world.park_on_the_evaluation().await;
    let (second_epoch, second_run, _) = world.a_later_occurrence(2_000_000).await;

    world
        .fx
        .apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Command,
                [TENANT, TASK, "manual-release"],
            )
            .expect("the operation id derives"),
            wake: world.wake.clone(),
        })
        .await
        .expect("the manual release applies");
    assert_eq!(
        world.controller().await,
        (2, 1, 1, 0),
        "the parked occurrence took the released place"
    );

    world
        .fx
        .pump_epoch(&second_epoch, &second_run)
        .await
        .expect("the second epoch converges");
    assert_eq!(
        task_at(&world.fx, &second_epoch).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert_eq!(world.controller().await, (2, 2, 0, 0));
    assert_eq!(
        world.run_standing().await.0,
        AgentRunStatus::WaitingForReconciliation,
        "the first epoch's run is still parked"
    );
    assert_eq!(
        world.goal_children_out().await,
        1,
        "and the goal still holds the budget it gave that run"
    );

    // A third occurrence is admitted and a fourth parked behind it, so a
    // release the late result should not make would show.
    world.a_later_occurrence(4_000_000).await;
    world.a_later_occurrence(6_000_000).await;
    assert_eq!(
        world.controller().await,
        (3, 2, 1, 1),
        "the third occurrence is active and the fourth parked"
    );
    assert_eq!(world.goal_children_out().await, 2);

    world.answer_the_park().await;
    world
        .fx
        .pump_epoch(&world.epoch, &world.run)
        .await
        .expect("the first epoch converges");
    assert_eq!(world.run_standing().await.0, AgentRunStatus::Completed);
    assert_eq!(
        world.goal_children_out().await,
        1,
        "the late result returned the first epoch's budget"
    );
    assert_eq!(
        world.controller().await,
        (3, 2, 1, 1),
        "and released nothing: the third occurrence is still active, the fourth still parked"
    );
}

/// A goal cancelled while an accepted epoch's run is parked does not finish
/// cancelling until the park is answered: the goal's ledger still holds the
/// budget that run was given, and a closed ledger is the goal's proof that
/// nothing it started is still running. A `Cancel` sent to the parked run
/// itself changes neither what it waits for nor the end it was given.
#[tokio::test]
async fn a_goal_cancelled_over_a_parked_accepted_epoch_ends_when_the_park_is_answered() {
    let world = accepted_epoch_with_an_evaluation_out(wake_policy()).await;
    world.park_on_the_evaluation().await;

    world
        .fx
        .apply_task_command(AgentTaskEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, TASK, "goal-cancel"],
            )
            .expect("the operation id derives"),
            reason: "operator".to_string(),
        })
        .await
        .expect("the goal's cancel applies");
    settle_all(&world.fx, &world.epoch, &world.run).await;
    let goal = world.fx.task_snapshot().await;
    assert!(
        !goal.status.is_terminal(),
        "the goal has not finished cancelling, got {:?}",
        goal.status
    );
    assert_eq!(
        AgentCancellationProgress::derive_task(&goal),
        AgentCancellationProgress::Propagating
    );

    apply_run(
        &world.fx,
        &world.run,
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "run-cancel"],
            )
            .expect("the operation id derives"),
            reason: "operator".to_string(),
        },
    )
    .await;
    settle_all(&world.fx, &world.epoch, &world.run).await;
    assert_eq!(
        world.run_standing().await,
        (
            AgentRunStatus::WaitingForReconciliation,
            Some("result-accepted".to_string())
        ),
        "a cancel does not shorten the park or change the end"
    );
    assert!(!world.fx.task_snapshot().await.status.is_terminal());

    world.answer_the_park().await;
    world
        .fx
        .pump_epoch(&world.epoch, &world.run)
        .await
        .expect("the epoch converges");
    settle_all(&world.fx, &world.epoch, &world.run).await;
    let goal = world.fx.task_snapshot().await;
    assert_eq!(goal.status, AgentTaskStatus::Cancelled);
    assert_eq!(
        AgentCancellationProgress::derive_task(&goal),
        AgentCancellationProgress::Completed
    );
    assert_eq!(world.controller().await, (1, 1, 0, 0));
}
