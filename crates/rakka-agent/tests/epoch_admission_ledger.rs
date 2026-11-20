//! A continuous goal admits an epoch only when its own ledger can pay for it
//! (issue #179).
//!
//! An epoch's budget is debited from the goal's ledger when its wake is
//! admitted, narrowed to what the goal has left. Where the goal had nothing
//! left in a dimension the epoch's run needs, the epoch was created with
//! nothing in it: its task could never be assigned, and it held the wake for
//! good, while the goal's exhaustion policy was never asked.
//!
//! Admission now asks the ledger first. An occurrence the goal cannot pay for
//! is parked, not admitted. Where a child still out holds the dimension, it
//! waits for that child's return. Where nothing could refill it, the goal's
//! exhaustion policy decides, as it does for a task refused its own run.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_run_state, load_agent_task_state, AgentBudgetAllocation, AgentBudgetDimension,
    AgentGoalExhaustionAction, AgentGoalLifecycleStatus, AgentGoalStatus, AgentGoalWaitReason,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRunEffectKind, AgentRunEffectStatus,
    AgentRunScope, AgentSchemaPolicy, AgentTaskContent, AgentTaskEntityCommand, AgentTaskScope,
    AgentTaskStatus, AgentWakeDisposition, AgentWakeId, AgentWakeScanOutcome, ScheduleRevision,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::{
    continuous_goal_control_creation_command, continuous_goal_mode, epoch_scopes_for, goal_spec,
    goal_spec_draft, provenance, scheduled_wake_binding, task_scope, wake_policy, Fixture, TASK,
    TENANT,
};

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Observed the nightly state.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "reconciled" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A fixture whose dispatcher answers `epochs` epochs, each with a proposal
/// on its first turn.
fn fixture(epochs: usize) -> Fixture {
    let mut dispatcher = ScriptedDispatcher::new();
    for _epoch in 0..epochs {
        dispatcher = dispatcher.with_turn(proposing_turn());
    }
    Fixture::new(dispatcher)
}

fn operation(step: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
        .expect("the operation id derives")
}

/// A human-owned continuous control task under a goal contract that allows
/// `loop_iterations` in all, with the fixture's wake policy: an epoch asks
/// for eight model calls and leaves loop iterations open.
async fn goal_with(fx: &Fixture, loop_iterations: u64, action: AgentGoalExhaustionAction) {
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(loop_iterations),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = action;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal's control task is created");
}

/// One occurrence of the schedule: the wake, its epoch's scopes, and how the
/// controller dispositioned it.
struct Occurrence {
    wake: AgentWakeId,
    epoch: AgentTaskScope,
    run: AgentRunScope,
    disposition: AgentWakeDisposition,
}

/// Schedules an occurrence due at `due_at`, moves the clock past it and
/// delivers it.
async fn deliver(fx: &Fixture, due_at: u64) -> Occurrence {
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
    Occurrence {
        wake: binding.wake_id().clone(),
        epoch,
        run,
        disposition: disposition.clone(),
    }
}

/// Delivers an occurrence and runs its epoch to the end.
async fn run_an_epoch(fx: &Fixture, due_at: u64) -> Occurrence {
    let occurrence = deliver(fx, due_at).await;
    assert!(
        occurrence.disposition.is_admission(),
        "the goal can pay for this epoch, got {:?}",
        occurrence.disposition
    );
    fx.pump_epoch(&occurrence.epoch, &occurrence.run)
        .await
        .expect("the epoch converges");
    assert_eq!(
        task_at(fx, &occurrence.epoch).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    occurrence
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

/// The controller's admitted and released counts, and how many occurrences
/// it holds active and parked.
async fn controller(fx: &Fixture) -> (u64, u64, usize, usize) {
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
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

/// The admission gate's status and the reason it is suspended under.
async fn gate(fx: &Fixture) -> (AgentGoalLifecycleStatus, Option<String>) {
    let root = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists");
    let lifecycle = root
        .task()
        .expect("the root is created")
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle();
    (
        lifecycle.status(),
        lifecycle.suspended_reason().map(str::to_string),
    )
}

/// Drives an admitted occurrence's epoch until its run holds its model call,
/// and no further: the epoch is running, with the budget it was given.
async fn run_until_its_model_call_is_out(fx: &Fixture, occurrence: &Occurrence) {
    for _round in 0..16 {
        for scope in [task_scope(), occurrence.epoch.clone()] {
            fx.settle_task_at(&scope).await.expect("the task settles");
        }
        let mut run = fx.run_at(&occurrence.run);
        if run.recover(fx.now()).await.is_ok() {
            run.settle_side_effects(&fx.router, fx.now())
                .await
                .expect("the run settles");
        }
        let sent = load_agent_run_state(&fx.runs, &occurrence.run, &AgentSchemaPolicy::default())
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
            return;
        }
    }
    panic!("the epoch's run never sent its model call");
}

/// The revision of the admission gate's lifecycle.
async fn gate_revision(fx: &Fixture) -> rakka_agent::AgentRevisionNumber {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists")
        .task()
        .expect("the root is created")
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle()
        .lifecycle_revision()
}

async fn goal_record(fx: &Fixture) -> rakka_agent::AgentGoalState {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the root state loads")
        .expect("the root exists")
        .task()
        .expect("the root is created")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .clone()
}

/// A goal that has spent all it had in a dimension parks at the next
/// occurrence, as its policy says. No epoch is created, the wake is not
/// held, and the occurrence waits. A resume that tops the goal up admits it.
#[tokio::test]
async fn a_goal_out_of_budget_parks_at_the_occurrence_it_cannot_pay_for() {
    let fx = fixture(3);
    goal_with(&fx, 2, AgentGoalExhaustionAction::Park).await;
    run_an_epoch(&fx, 5).await;
    run_an_epoch(&fx, 1_000_005).await;
    assert_eq!(controller(&fx).await, (2, 2, 0, 0));
    assert_eq!(
        (goal_record(&fx).await.status(), gate(&fx).await.0),
        (AgentGoalStatus::Active, AgentGoalLifecycleStatus::Active),
        "the goal has spent all it had, and is not parked until an occurrence needs more"
    );

    let third = deliver(&fx, 2_000_005).await;

    assert!(
        matches!(
            third.disposition,
            AgentWakeDisposition::Deferred {
                dimension: AgentBudgetDimension::LoopIterations,
                ..
            }
        ),
        "the occurrence is parked on the dimension the goal is out of, got {:?}",
        third.disposition
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the control task settles");
    assert_eq!(
        task_at(&fx, &third.epoch).await,
        None,
        "no epoch is created that could never run"
    );
    assert_eq!(
        controller(&fx).await,
        (2, 2, 0, 1),
        "nothing is admitted, and the occurrence is parked"
    );
    assert_eq!(
        task_at(&fx, &task_scope()).await.map(|(_, out)| out),
        Some(0),
        "the goal's ledger holds nothing out"
    );
    let goal = goal_record(&fx).await;
    assert_eq!(goal.status(), AgentGoalStatus::Waiting);
    let Some(AgentGoalWaitReason::BudgetExhausted { exhaustion }) = goal.wait() else {
        panic!("expected a budget park, got {:?}", goal.wait());
    };
    assert_eq!(exhaustion.dimension, AgentBudgetDimension::LoopIterations);
    assert_eq!(
        gate(&fx).await,
        (
            AgentGoalLifecycleStatus::Suspended,
            Some("budget-exhausted".to_string())
        ),
        "the park closed admission"
    );

    // One resume widens the ledger, reactivates the goal and admits the
    // occurrence that waited.
    fx.apply_task_command(AgentTaskEntityCommand::ResumeGoal {
        operation_id: operation("resume"),
        expected_status_revision: goal.status_revision(),
        top_up: Some(Box::new(AgentBudgetAllocation {
            loop_iterations: Some(1),
            ..AgentBudgetAllocation::unbounded()
        })),
        provenance: Box::new(provenance(91)),
    })
    .await
    .expect("the resume applies");
    assert_eq!(goal_record(&fx).await.status(), AgentGoalStatus::Active);
    assert_eq!(
        controller(&fx).await,
        (3, 2, 1, 0),
        "the parked occurrence is admitted on the topped-up ledger"
    );
    fx.pump_epoch(&third.epoch, &third.run)
        .await
        .expect("the third epoch converges");
    assert_eq!(
        task_at(&fx, &third.epoch).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert_eq!(controller(&fx).await, (3, 3, 0, 0));
}

/// Under a terminate policy the goal ends, and its control task with it, at
/// the occurrence it cannot pay for. No epoch is created.
#[tokio::test]
async fn a_terminate_policy_ends_the_goal_at_the_occurrence_it_cannot_pay_for() {
    let fx = fixture(2);
    goal_with(&fx, 1, AgentGoalExhaustionAction::Terminate).await;
    run_an_epoch(&fx, 5).await;

    let second = deliver(&fx, 1_000_005).await;

    assert_eq!(task_at(&fx, &second.epoch).await, None);
    assert!(
        goal_record(&fx).await.status().is_terminal(),
        "the goal ended, got {:?}",
        goal_record(&fx).await.status()
    );
    assert_eq!(
        task_at(&fx, &task_scope()).await,
        Some((AgentTaskStatus::Failed, 0)),
        "the control task ended with its goal, with nothing out"
    );
}

/// A control task with no goal contract has no policy to ask. The occurrence
/// it cannot pay for is parked all the same: no epoch is created, and
/// nothing is held.
#[tokio::test]
async fn a_control_task_with_no_goal_contract_parks_the_occurrence_it_cannot_pay_for() {
    let fx = fixture(4);
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    // The task's definition allows three loop iterations in all.
    run_an_epoch(&fx, 5).await;
    run_an_epoch(&fx, 1_000_005).await;
    run_an_epoch(&fx, 2_000_005).await;

    let fourth = deliver(&fx, 3_000_005).await;

    assert!(
        matches!(
            fourth.disposition,
            AgentWakeDisposition::Deferred {
                dimension: AgentBudgetDimension::LoopIterations,
                ..
            }
        ),
        "got {:?}",
        fourth.disposition
    );
    assert_eq!(task_at(&fx, &fourth.epoch).await, None);
    assert_eq!(controller(&fx).await, (3, 3, 0, 1));
    assert_eq!(
        task_at(&fx, &task_scope()).await.map(|(_, out)| out),
        Some(0)
    );

    // A later occurrence takes the one parking place, and still nothing is
    // admitted.
    let fifth = deliver(&fx, 4_000_005).await;
    assert_eq!(task_at(&fx, &fifth.epoch).await, None);
    assert_eq!(controller(&fx).await, (3, 3, 0, 1));
}

/// An occurrence whose epoch the goal cannot pay for yet, because an epoch
/// still out holds the budget, waits for that epoch's return. Here an
/// operator released the running epoch's wake by hand, so the place is free
/// and only the ledger stands in the way. The occurrence is admitted when the
/// first epoch's result returns its budget, and then it runs. The goal's
/// exhaustion policy is not asked: the budget is on its way back.
#[tokio::test]
async fn an_occurrence_waits_for_the_budget_a_running_epoch_holds() {
    let fx = fixture(2);
    goal_with(&fx, 3, AgentGoalExhaustionAction::Park).await;
    let first = deliver(&fx, 5).await;
    assert!(first.disposition.is_admission());
    run_until_its_model_call_is_out(&fx, &first).await;
    assert_eq!(
        task_at(&fx, &first.epoch).await,
        Some((AgentTaskStatus::InProgress, 1))
    );
    let second = deliver(&fx, 1_000_005).await;
    assert_eq!(controller(&fx).await, (1, 0, 1, 1));

    fx.apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
        operation_id: operation("manual-release"),
        wake: first.wake.clone(),
    })
    .await
    .expect("the manual release applies");

    assert_eq!(
        controller(&fx).await,
        (1, 1, 0, 1),
        "the place is free, and the parked occurrence still waits: the goal cannot pay for it"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the control task settles");
    assert_eq!(task_at(&fx, &second.epoch).await, None);
    assert_eq!(
        (goal_record(&fx).await.status(), gate(&fx).await.0),
        (AgentGoalStatus::Active, AgentGoalLifecycleStatus::Active),
        "the goal is not parked for a budget an epoch still out will return"
    );

    fx.pump_epoch(&first.epoch, &first.run)
        .await
        .expect("the first epoch converges");

    assert_eq!(
        controller(&fx).await,
        (2, 1, 1, 0),
        "the first epoch's result returned its budget, and the occurrence is admitted"
    );
    fx.pump_epoch(&second.epoch, &second.run)
        .await
        .expect("the second epoch converges");
    assert_eq!(
        task_at(&fx, &second.epoch).await,
        Some((AgentTaskStatus::Completed, 0))
    );
    assert_eq!(controller(&fx).await, (2, 2, 0, 0));
    assert_eq!(
        task_at(&fx, &task_scope()).await.map(|(_, out)| out),
        Some(0)
    );
}

/// A delivery can promote a parked occurrence and admit the fresh one in the
/// same transition, where the policy allows two epochs at once. The goal's
/// ledger is read again after the first epoch is debited, so the second is
/// not admitted on budget the first has just taken.
///
/// Here the goal has one loop iteration left and the goal window is full, so
/// the third occurrence parks on the window. An hour on, the window has
/// turned. The fourth occurrence's delivery promotes the third, which takes
/// the last iteration, and the fourth is parked on the ledger.
#[tokio::test]
async fn two_admissions_in_one_transition_do_not_count_on_the_same_budget() {
    let fx = fixture(3);
    fx.instantiate_agent().await;
    let mut window = AgentBudgetAllocation::unbounded();
    window.set(AgentBudgetDimension::ModelCalls, Some(20));
    let policy = wake_policy()
        .with_parallel_epochs(
            2,
            rakka_agent::AgentPolicyRef::new("first-result-wins").expect("the ref is valid"),
        )
        .expect("the parallel policy is valid")
        .with_goal_window(rakka_agent::AgentGoalWindowCeiling {
            window: rakka_agent::AgentBudgetWindow::Rolling {
                length_millis: 3_600_000,
            },
            ceiling: window,
        })
        .expect("the windowed policy is valid");
    fx.create_continuous_control_task(continuous_goal_mode(policy))
        .await;
    // Two epochs of eight model calls fill the twenty-call window, and leave
    // the goal one of its three loop iterations.
    run_an_epoch(&fx, 5).await;
    run_an_epoch(&fx, 1_000_005).await;
    let third = deliver(&fx, 2_000_005).await;
    assert!(
        matches!(
            third.disposition,
            AgentWakeDisposition::Deferred {
                dimension: AgentBudgetDimension::ModelCalls,
                ..
            }
        ),
        "the window refuses the third, got {:?}",
        third.disposition
    );
    assert_eq!(controller(&fx).await, (2, 2, 0, 1));

    // The fourth is delivered by command, past the window's turn, so that no
    // re-wake promotes the third in a transition of its own first.
    let fourth = scheduled_wake_binding(6_000_005, ScheduleRevision::INITIAL);
    fx.clock.store(6_001_005, Ordering::SeqCst);
    let reply = fx
        .apply_task_command(
            rakka_agent::wake_admission_command(fourth.clone()).expect("the command derives"),
        )
        .await
        .expect("the delivery applies");
    let rakka_agent::AgentTaskEntityReply::Applied { outcome } = reply else {
        panic!("the delivery applies, got {reply:?}");
    };
    let Some(rakka_agent::AgentWakeOutcome::Disposition(disposition)) = outcome.wake else {
        panic!("the outcome records a disposition, got {:?}", outcome.wake);
    };

    assert!(
        matches!(
            disposition,
            AgentWakeDisposition::Deferred {
                dimension: AgentBudgetDimension::LoopIterations,
                ..
            }
        ),
        "the promoted occurrence took the last iteration, so the fresh one waits, got \
         {disposition:?}"
    );
    assert_eq!(
        controller(&fx).await,
        (3, 2, 1, 1),
        "the third is admitted and the fourth parked"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the control task settles");
    let (fourth_epoch, _) = epoch_scopes_for(fourth.wake_id());
    assert_eq!(task_at(&fx, &fourth_epoch).await, None);
    fx.pump_epoch(&third.epoch, &third.run)
        .await
        .expect("the third epoch converges");
    assert_eq!(
        task_at(&fx, &third.epoch).await,
        Some((AgentTaskStatus::Completed, 0)),
        "the promoted epoch had the budget to run"
    );
}

/// The usual way a goal runs out: the next occurrence comes due while the
/// last epoch the goal can pay for is still running, and waits behind it.
/// That epoch's result frees the place and leaves nothing to pay with. The
/// occurrence is not promoted into an epoch with no budget, and the goal
/// parks.
#[tokio::test]
async fn an_occurrence_behind_the_last_epoch_the_goal_can_pay_for_parks_the_goal() {
    let fx = fixture(1);
    goal_with(&fx, 1, AgentGoalExhaustionAction::Park).await;
    let first = deliver(&fx, 5).await;
    assert!(first.disposition.is_admission());
    run_until_its_model_call_is_out(&fx, &first).await;
    let second = deliver(&fx, 1_000_005).await;
    assert!(
        matches!(second.disposition, AgentWakeDisposition::Coalesced { .. }),
        "the second waits behind the running epoch, got {:?}",
        second.disposition
    );

    fx.pump_epoch(&first.epoch, &first.run)
        .await
        .expect("the first epoch converges");

    assert_eq!(
        controller(&fx).await,
        (1, 1, 0, 1),
        "the result released the wake and promoted nothing"
    );
    assert_eq!(task_at(&fx, &second.epoch).await, None);
    assert_eq!(goal_record(&fx).await.status(), AgentGoalStatus::Waiting);
    assert_eq!(
        gate(&fx).await,
        (
            AgentGoalLifecycleStatus::Suspended,
            Some("budget-exhausted".to_string())
        )
    );
}

/// An operator's resume of the admission gate promotes the occurrence that
/// was parked while the gate was closed, but not into an epoch the goal
/// cannot pay for.
#[tokio::test]
async fn a_gate_resume_admits_nothing_the_goal_cannot_pay_for() {
    let fx = fixture(3);
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    run_an_epoch(&fx, 5).await;
    run_an_epoch(&fx, 1_000_005).await;
    run_an_epoch(&fx, 2_000_005).await;
    fx.apply_task_command(AgentTaskEntityCommand::SuspendContinuousGoal {
        operation_id: operation("suspend"),
        expected_lifecycle_revision: gate_revision(&fx).await,
        reason: None,
        provenance: Box::new(provenance(90)),
    })
    .await
    .expect("the suspension applies");
    let fourth = deliver(&fx, 3_000_005).await;
    assert_eq!(
        controller(&fx).await,
        (3, 3, 0, 1),
        "the occurrence is parked behind the closed gate, got {:?}",
        fourth.disposition
    );

    fx.apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
        operation_id: operation("resume-gate"),
        expected_lifecycle_revision: gate_revision(&fx).await,
        provenance: Box::new(provenance(91)),
    })
    .await
    .expect("the resume applies");

    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Active);
    assert_eq!(
        controller(&fx).await,
        (3, 3, 0, 1),
        "the gate is open, and the goal still cannot pay for the occurrence"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the control task settles");
    assert_eq!(task_at(&fx, &fourth.epoch).await, None);
}

/// Activating a proposed goal lifts the gate its proposal closed, and
/// promotes the occurrence that waited, but not into an epoch the goal
/// cannot pay for.
#[tokio::test]
async fn activating_a_goal_admits_nothing_it_cannot_pay_for() {
    let fx = fixture(1);
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(0),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = AgentGoalExhaustionAction::Park;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(spec, false),
    ))
    .await
    .expect("the proposed goal's control task is created");
    let first = deliver(&fx, 5).await;
    assert_eq!(
        controller(&fx).await,
        (0, 0, 0, 1),
        "the occurrence is parked behind the proposal's gate, got {:?}",
        first.disposition
    );

    fx.apply_task_command(AgentTaskEntityCommand::ActivateGoal {
        operation_id: operation("activate"),
        expected_status_revision: goal_record(&fx).await.status_revision(),
        provenance: Box::new(provenance(92)),
    })
    .await
    .expect("the activation applies");

    assert_eq!(goal_record(&fx).await.status(), AgentGoalStatus::Active);
    assert_eq!(
        controller(&fx).await,
        (0, 0, 0, 1),
        "the goal is active, and has nothing to pay for the occurrence with"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the control task settles");
    assert_eq!(task_at(&fx, &first.epoch).await, None);
}

/// An epoch whose wake was released by hand returns its budget with its
/// result, and that result retries the occurrence that waited. It promotes it
/// only if the goal can now pay. Here the epoch spent the goal's one
/// iteration, so its result returns nothing: the occurrence stays parked, no
/// epoch is created, and the goal parks.
#[tokio::test]
async fn a_late_result_that_returns_nothing_promotes_nothing() {
    let fx = fixture(1);
    goal_with(&fx, 1, AgentGoalExhaustionAction::Park).await;
    let first = deliver(&fx, 5).await;
    assert!(first.disposition.is_admission());
    run_until_its_model_call_is_out(&fx, &first).await;
    let second = deliver(&fx, 1_000_005).await;
    fx.apply_task_command(AgentTaskEntityCommand::CompleteWakeOccurrence {
        operation_id: operation("manual-release"),
        wake: first.wake.clone(),
    })
    .await
    .expect("the manual release applies");
    assert_eq!(
        (controller(&fx).await, goal_record(&fx).await.status()),
        ((1, 1, 0, 1), AgentGoalStatus::Active),
        "the occurrence waits for the budget the released epoch still holds"
    );

    fx.pump_epoch(&first.epoch, &first.run)
        .await
        .expect("the first epoch converges");

    assert_eq!(
        controller(&fx).await,
        (1, 1, 0, 1),
        "the late result returned nothing, and promoted nothing"
    );
    assert_eq!(task_at(&fx, &second.epoch).await, None);
    assert_eq!(goal_record(&fx).await.status(), AgentGoalStatus::Waiting);
    assert_eq!(
        gate(&fx).await,
        (
            AgentGoalLifecycleStatus::Suspended,
            Some("budget-exhausted".to_string())
        )
    );
}

/// A resume that tops up the dimension the goal parked on, and leaves
/// another one empty, admits nothing: the occurrence that waited is not
/// promoted into an epoch with no model calls. The policy is not asked again
/// inside the resume, so the goal reads `Active` until the next delivery or
/// result asks.
#[tokio::test]
async fn a_resume_that_relieves_one_dimension_admits_nothing() {
    let fx = fixture(2);
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(1),
        model_calls: Some(1),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = AgentGoalExhaustionAction::Park;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal's control task is created");
    run_an_epoch(&fx, 5).await;
    let second = deliver(&fx, 1_000_005).await;
    let goal = goal_record(&fx).await;
    let Some(AgentGoalWaitReason::BudgetExhausted { exhaustion }) = goal.wait() else {
        panic!("expected a budget park, got {:?}", goal.wait());
    };
    let mut top_up = AgentBudgetAllocation::unbounded();
    for dimension in AgentBudgetDimension::CONSERVED {
        top_up.set(dimension, Some(0));
    }
    top_up.set(exhaustion.dimension, Some(1));

    fx.apply_task_command(AgentTaskEntityCommand::ResumeGoal {
        operation_id: operation("resume"),
        expected_status_revision: goal.status_revision(),
        top_up: Some(Box::new(top_up)),
        provenance: Box::new(provenance(91)),
    })
    .await
    .expect("the resume applies");

    assert_eq!(goal_record(&fx).await.status(), AgentGoalStatus::Active);
    assert_eq!(
        controller(&fx).await,
        (1, 1, 0, 1),
        "one dimension is relieved and another is still empty: nothing is admitted"
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("the control task settles");
    assert_eq!(task_at(&fx, &second.epoch).await, None);
}

/// A wake policy that itself grants nothing in a dimension is not the goal's
/// exhaustion: the goal's ledger can pay all the policy asks of it. The goal
/// is not ended for it, and admission goes on as it did.
#[tokio::test]
async fn a_wake_policys_own_zero_is_not_read_as_the_goals_exhaustion() {
    let fx = fixture(1);
    fx.instantiate_agent().await;
    let mut budget = AgentBudgetAllocation::unbounded();
    budget.set(AgentBudgetDimension::ModelCalls, Some(8));
    budget.set(AgentBudgetDimension::ToolCalls, Some(0));
    let policy = rakka_agent::AgentWakePolicy::new(
        [rakka_agent::AgentWakeTriggerKind::DurableTimer],
        budget,
        Some(60_000),
    )
    .expect("the wake policy is valid");
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(3),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = AgentGoalExhaustionAction::Terminate;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(policy),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal's control task is created");

    let first = deliver(&fx, 5).await;

    assert!(
        first.disposition.is_admission(),
        "the goal can pay what the policy asks, got {:?}",
        first.disposition
    );
    assert_eq!(goal_record(&fx).await.status(), AgentGoalStatus::Active);
    assert_eq!(
        task_at(&fx, &task_scope()).await.map(|(status, _)| status),
        Some(AgentTaskStatus::WaitingForInput)
    );
}

/// An agent-owned continuous goal has a run of its own, whose budget is a
/// child of the same ledger. Where that run holds all of a dimension, the
/// goal cannot pay for an epoch, and its own run is no relief: its budget
/// comes back only when the task ends. So the policy is asked at the first
/// occurrence.
#[tokio::test]
async fn an_agent_owned_goal_whose_own_run_holds_the_budget_asks_its_policy() {
    let fx = fixture(1);
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(3),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = AgentGoalExhaustionAction::Park;
    fx.apply_task_command(AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(AgentOperationKind::TaskCreation, [TENANT, TASK, "1"])
            .expect("the operation id derives"),
        creation: Box::new(rakka_agent::AgentTaskCreation {
            definition: common::task_definition(),
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(common::agent_id()),
            team: None,
            goal: Some(common::goal_id()),
            goal_mode: continuous_goal_mode(wake_policy()),
            goal_spec: Some(Box::new(goal_spec_draft(spec, true))),
            parent: None,
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        }),
    })
    .await
    .expect("the agent-owned goal is created");
    assert_eq!(
        task_at(&fx, &task_scope()).await.map(|(_, out)| out),
        Some(1),
        "the goal's own run holds its budget"
    );

    let first = deliver(&fx, 5).await;

    assert!(
        matches!(
            first.disposition,
            AgentWakeDisposition::Deferred {
                dimension: AgentBudgetDimension::LoopIterations,
                ..
            }
        ),
        "got {:?}",
        first.disposition
    );
    assert_eq!(task_at(&fx, &first.epoch).await, None);
    assert_eq!(goal_record(&fx).await.status(), AgentGoalStatus::Waiting);
    assert_eq!(
        gate(&fx).await,
        (
            AgentGoalLifecycleStatus::Suspended,
            Some("budget-exhausted".to_string())
        )
    );
}

/// A goal that is only proposed cannot be ended or parked for budget. Where
/// an operator opened its gate, the occurrence it cannot pay for is parked
/// and the policy is not asked: the delivery is answered, not refused.
#[tokio::test]
async fn a_proposed_goal_whose_gate_was_opened_is_not_asked_its_policy() {
    let fx = fixture(1);
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(0),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = AgentGoalExhaustionAction::Terminate;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(spec, false),
    ))
    .await
    .expect("the proposed goal's control task is created");
    fx.apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
        operation_id: operation("open-gate"),
        expected_lifecycle_revision: gate_revision(&fx).await,
        provenance: Box::new(provenance(90)),
    })
    .await
    .expect("the operator opens the gate");
    assert_eq!(gate(&fx).await.0, AgentGoalLifecycleStatus::Active);

    let first = deliver(&fx, 5).await;

    assert!(
        matches!(
            first.disposition,
            AgentWakeDisposition::Deferred {
                dimension: AgentBudgetDimension::LoopIterations,
                ..
            }
        ),
        "got {:?}",
        first.disposition
    );
    assert_eq!(task_at(&fx, &first.epoch).await, None);
    assert_eq!(
        goal_record(&fx).await.status(),
        AgentGoalStatus::Proposed,
        "the goal is not decided"
    );
    assert_eq!(
        task_at(&fx, &task_scope()).await.map(|(status, _)| status),
        Some(AgentTaskStatus::WaitingForInput)
    );
}

/// Where the policy allows two epochs at once and leaves a dimension open,
/// the first epoch holds all the goal has in it. The second occurrence finds
/// a free place and a ledger that cannot pay, and waits. The goal is not
/// parked for it: the budget is on its way back with the first epoch's
/// result, which admits the occurrence.
#[tokio::test]
async fn a_second_parallel_epoch_waits_for_the_first_without_parking_the_goal() {
    let fx = fixture(2);
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(3),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = AgentGoalExhaustionAction::Park;
    let policy = wake_policy()
        .with_parallel_epochs(
            2,
            rakka_agent::AgentPolicyRef::new("first-result-wins").expect("the ref is valid"),
        )
        .expect("the parallel policy is valid");
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(policy),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal's control task is created");
    let first = deliver(&fx, 5).await;
    assert!(first.disposition.is_admission());
    run_until_its_model_call_is_out(&fx, &first).await;

    let second = deliver(&fx, 1_000_005).await;

    assert!(
        matches!(
            second.disposition,
            AgentWakeDisposition::Deferred {
                dimension: AgentBudgetDimension::LoopIterations,
                ..
            }
        ),
        "a place is free and the ledger cannot pay, got {:?}",
        second.disposition
    );
    assert_eq!(controller(&fx).await, (1, 0, 1, 1));
    assert_eq!(
        (goal_record(&fx).await.status(), gate(&fx).await.0),
        (AgentGoalStatus::Active, AgentGoalLifecycleStatus::Active),
        "the goal is not parked for a budget the first epoch will return"
    );

    fx.pump_epoch(&first.epoch, &first.run)
        .await
        .expect("the first epoch converges");
    assert_eq!(
        controller(&fx).await,
        (2, 1, 1, 0),
        "the first epoch's result admitted the occurrence that waited"
    );
    fx.pump_epoch(&second.epoch, &second.run)
        .await
        .expect("the second epoch converges");
    assert_eq!(
        task_at(&fx, &second.epoch).await,
        Some((AgentTaskStatus::Completed, 0))
    );
}
