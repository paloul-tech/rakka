//! A task's decisions answer a replay `Duplicate` after the operation window
//! has moved past them (issue #96, ask 2).
//!
//! The task remembers its last 64 operation ids. Its creation, its
//! cancellation, and every command fenced on a goal status, criteria, wake
//! lifecycle, or schedule revision are also recorded in the task's decision
//! ledger, which only those write, so a client that lost the answer to one
//! and re-posts it later is answered with the original outcome rather than
//! the refusal a first attempt would meet (`task-already-created`,
//! `task-terminal`, `goal-*`, `wake-*`, `task-schedule-not-monotonic`).
//!
//! Each test commits a decision, then rewrites the stored record as if 64
//! later operations had evicted its id ([`forget_operations`]).

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, AgentBudgetAllocation, AgentBudgetCeilings, AgentGoalCriteriaSource,
    AgentGoalDecision, AgentGoalStatus, AgentGoalTerminalReason, AgentOperationId,
    AgentOperationKind, AgentPolicyRef, AgentRevisionNumber, AgentSchemaPolicy, AgentTaskContent,
    AgentTaskCreation, AgentTaskDefinition, AgentTaskDependencyDeclaration,
    AgentTaskDependencyOutcome, AgentTaskEntityCommand, AgentTaskEntityReply, AgentTaskError,
    AgentTaskId, AgentTaskOutcome, AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId,
    AgentTaskStatus, AgentWakeLifecyclePolicy, ScheduleRevision,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::{
    agent_id, continuous_goal_mode, forget_operations, goal_id, goal_spec, goal_spec_draft,
    goal_task_creation_command, provenance, schema, task_definition, task_definition_id,
    task_scope, wake_policy, Fixture, TASK, TENANT,
};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

fn operation(step: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
        .expect("the operation id derives")
}

fn applied(result: Result<AgentTaskEntityReply, AgentTaskError>) -> AgentTaskOutcome {
    match result {
        Ok(AgentTaskEntityReply::Applied { outcome }) => outcome,
        other => panic!("expected the command to apply, got {other:?}"),
    }
}

/// Rewrites the task's record as if 64 later operations had moved its window
/// past every id it holds; `forget_ledger` also drops the ledger, as a record
/// written before the ledger existed.
async fn forget_window(fx: &Fixture, forget_ledger: bool) {
    forget_operations(&fx.tasks, &task_scope().persistence_id(), forget_ledger).await;
}

/// Re-posts `command` and asserts it converges on the outcome it committed.
async fn assert_replay_converges(
    fx: &Fixture,
    command: AgentTaskEntityCommand,
    outcome: AgentTaskOutcome,
) {
    match fx.apply_task_command(command).await {
        Ok(AgentTaskEntityReply::Duplicate { outcome: replayed }) => {
            assert_eq!(replayed, outcome, "the replay answers the original outcome");
        }
        other => panic!("expected the replay to answer Duplicate, got {other:?}"),
    }
}

async fn ledger_len(fx: &Fixture) -> usize {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists")
        .decision_ledger()
        .len()
}

#[tokio::test]
async fn a_replayed_creation_answers_duplicate_after_the_window_has_moved_past_it() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let create = goal_task_creation_command(task_definition(), goal_spec_draft(goal_spec(), true));
    assert!(create.records_decision());
    let outcome = applied(fx.apply_task_command(create.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, create, outcome).await;
}

#[tokio::test]
async fn a_replayed_cancellation_of_a_task_it_ended_answers_duplicate_rather_than_task_terminal() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");
    let cancel = AgentTaskEntityCommand::Cancel {
        operation_id: operation("cancel"),
        reason: "operator".to_string(),
    };
    assert!(cancel.records_decision());
    let outcome = applied(fx.apply_task_command(cancel.clone()).await);
    fx.pump().await.expect("the wind-down drives");
    fx.pump().await.expect("the settlement drives");
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    assert_eq!(
        state.task().expect("the task is created").status,
        AgentTaskStatus::Cancelled
    );
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, cancel, outcome).await;
    assert_eq!(
        ledger_len(&fx).await,
        2,
        "the creation and the cancellation"
    );
}

#[tokio::test]
async fn a_decision_the_ledger_no_longer_holds_meets_the_refusal_a_first_attempt_would() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let create = goal_task_creation_command(task_definition(), goal_spec_draft(goal_spec(), true));
    fx.apply_task_command(create.clone())
        .await
        .expect("the creation applies");
    forget_window(&fx, true).await;

    let refusal = fx
        .apply_task_command(create)
        .await
        .expect_err("the replay is refused");
    assert_eq!(refusal.code(), "task-already-created");
}

#[test]
fn only_decisions_are_remembered_past_the_window() {
    let upstream = || AgentTaskId::new("upstream").expect("the task id is valid");
    assert!(!AgentTaskEntityCommand::Describe.records_decision());
    assert!(!AgentTaskEntityCommand::DeclareDependency {
        operation_id: operation("declare"),
        declaration: Box::new(AgentTaskDependencyDeclaration::new(upstream())),
    }
    .records_decision());
    assert!(!AgentTaskEntityCommand::RecordDependencyOutcome {
        operation_id: operation("resolve"),
        dependency: upstream(),
        outcome: AgentTaskDependencyOutcome::Completed,
    }
    .records_decision());
}

// ---------------------------------------------------------------------------
// Goal commands, fenced on the goal's status or criteria revision.
// ---------------------------------------------------------------------------

async fn create_goal_task(fx: &Fixture, activate: bool) {
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), activate),
    ))
    .await
    .expect("the creation applies");
}

async fn goal_status_revision(fx: &Fixture) -> AgentRevisionNumber {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists")
        .task()
        .expect("the task is created")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .status_revision()
}

#[tokio::test]
async fn a_replayed_goal_activation_answers_duplicate_rather_than_a_stale_revision() {
    let fx = fixture();
    create_goal_task(&fx, false).await;
    let activate = AgentTaskEntityCommand::ActivateGoal {
        operation_id: operation("activate"),
        expected_status_revision: AgentRevisionNumber::INITIAL,
        provenance: Box::new(provenance(10)),
    };
    assert!(activate.records_decision());
    let outcome = applied(fx.apply_task_command(activate.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, activate, outcome).await;
}

#[tokio::test]
async fn a_replayed_goal_decision_answers_duplicate_rather_than_goal_terminal() {
    let fx = fixture();
    create_goal_task(&fx, true).await;
    let decide = AgentTaskEntityCommand::RecordGoalDecision {
        operation_id: operation("cancel-goal"),
        decision: Box::new(AgentGoalDecision {
            reason: AgentGoalTerminalReason::CancellationRequested {
                reason: "operator".to_string(),
            },
            evaluation: None,
            provenance: Some(Box::new(provenance(21))),
            expected_status_revision: AgentRevisionNumber::INITIAL,
        }),
    };
    assert!(decide.records_decision());
    let outcome = applied(fx.apply_task_command(decide.clone()).await);
    assert_eq!(
        outcome
            .goal
            .as_ref()
            .expect("the goal outcome rides")
            .status,
        AgentGoalStatus::Cancelled
    );
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, decide, outcome).await;
}

#[tokio::test]
async fn a_replayed_criteria_revision_answers_duplicate_rather_than_a_stale_revision() {
    let fx = fixture();
    create_goal_task(&fx, true).await;
    let revise = AgentTaskEntityCommand::ReviseGoalCriteria {
        operation_id: operation("revise"),
        expected_criteria_revision: AgentRevisionNumber::INITIAL,
        source: AgentGoalCriteriaSource::Policy(
            AgentPolicyRef::new("ticket-resolved-v2").expect("the policy ref is valid"),
        ),
        digest: None,
        provenance: Box::new(provenance(80)),
    };
    assert!(revise.records_decision());
    let outcome = applied(fx.apply_task_command(revise.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, revise, outcome).await;
}

/// A definition whose ceilings leave headroom above the goal's allocation, so
/// a resume's widening has room to grow into.
fn roomy_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve one customer support ticket under the goal's allocation.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(10),
        ..AgentBudgetCeilings::unbounded()
    })
}

#[tokio::test]
async fn a_replayed_goal_resume_answers_duplicate_rather_than_a_stale_revision() {
    // An agent-owned continuous goal with no loop allocation parks `Waiting`
    // at creation: the first assignment refusal is permanent.
    let fx = fixture();
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(0),
        ..AgentBudgetAllocation::unbounded()
    };
    let parked = applied(
        fx.apply_task_command(AgentTaskEntityCommand::Create {
            operation_id: AgentOperationId::new(
                AgentOperationKind::TaskCreation,
                [TENANT, TASK, "1"],
            )
            .expect("operation id should be derivable"),
            creation: Box::new(AgentTaskCreation {
                definition: roomy_definition(),
                input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                    .expect("the input is inline-bounded"),
                assignee: Some(agent_id()),
                team: None,
                goal: Some(goal_id()),
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
        .await,
    );
    assert_eq!(
        parked.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Waiting
    );

    let resume = AgentTaskEntityCommand::ResumeGoal {
        operation_id: operation("resume"),
        expected_status_revision: goal_status_revision(&fx).await,
        top_up: Some(Box::new(AgentBudgetAllocation {
            loop_iterations: Some(2),
            ..AgentBudgetAllocation::unbounded()
        })),
        provenance: Box::new(provenance(91)),
    };
    assert!(resume.records_decision());
    let outcome = applied(fx.apply_task_command(resume.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, resume, outcome).await;
}

// ---------------------------------------------------------------------------
// Wake-lifecycle commands, fenced on the lifecycle revision, and the schedule
// update, fenced on the schedule revision.
// ---------------------------------------------------------------------------

async fn lifecycle_revision(fx: &Fixture) -> AgentRevisionNumber {
    load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists")
        .task()
        .expect("the task is created")
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle()
        .lifecycle_revision()
}

fn lifecycle_operation(kind: AgentOperationKind, step: &str) -> AgentOperationId {
    AgentOperationId::new(kind, [TENANT, TASK, step]).expect("the operation id derives")
}

fn suspend(revision: AgentRevisionNumber) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::SuspendContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::LifecycleSuspend, "suspend-1"),
        expected_lifecycle_revision: revision,
        reason: Some("maintenance window".to_string()),
        provenance: Box::new(provenance(10)),
    }
}

#[tokio::test]
async fn a_replayed_suspension_answers_duplicate_rather_than_a_stale_lifecycle_revision() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let command = suspend(lifecycle_revision(&fx).await);
    assert!(command.records_decision());
    let outcome = applied(fx.apply_task_command(command.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, command, outcome).await;
}

#[tokio::test]
async fn a_replayed_lifecycle_resume_answers_duplicate_rather_than_a_stale_lifecycle_revision() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    applied(
        fx.apply_task_command(suspend(lifecycle_revision(&fx).await))
            .await,
    );
    let resume = AgentTaskEntityCommand::ResumeContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::LifecycleResume, "resume-1"),
        expected_lifecycle_revision: lifecycle_revision(&fx).await,
        provenance: Box::new(provenance(11)),
    };
    assert!(resume.records_decision());
    let outcome = applied(fx.apply_task_command(resume.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, resume, outcome).await;
    assert_eq!(ledger_len(&fx).await, 3, "creation, suspension, resume");
}

#[tokio::test]
async fn a_replayed_renewal_answers_duplicate_rather_than_a_stale_lifecycle_revision() {
    // Renewal needs an effective expiry to extend; the default lifecycle
    // policy has none, and requires no renewal window.
    let fx = fixture();
    fx.instantiate_agent().await;
    let policy = wake_policy()
        .with_lifecycle(AgentWakeLifecyclePolicy {
            expires_at: Some(AgentTimestampMillis::new(50_000)),
            ..AgentWakeLifecyclePolicy::DEFAULT
        })
        .expect("the lifecycle policy is valid");
    fx.create_continuous_control_task(continuous_goal_mode(policy))
        .await;
    let renew = AgentTaskEntityCommand::RenewContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::Command, "renew-1"),
        expected_lifecycle_revision: lifecycle_revision(&fx).await,
        new_expires_at: AgentTimestampMillis::new(60_000),
        provenance: Box::new(provenance(12)),
    };
    assert!(renew.records_decision());
    let outcome = applied(fx.apply_task_command(renew.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, renew, outcome).await;
}

#[tokio::test]
async fn a_replayed_retirement_answers_duplicate_rather_than_wake_lifecycle_terminal() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;
    let retire = AgentTaskEntityCommand::RetireContinuousGoal {
        operation_id: lifecycle_operation(AgentOperationKind::LifecycleTerminate, "retire-1"),
        expected_lifecycle_revision: lifecycle_revision(&fx).await,
        provenance: Box::new(provenance(13)),
    };
    assert!(retire.records_decision());
    let outcome = applied(fx.apply_task_command(retire.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, retire, outcome).await;
}

#[tokio::test]
async fn a_replayed_schedule_update_answers_duplicate_rather_than_not_monotonic() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_continuous_task().await;
    let update = AgentTaskEntityCommand::UpdateContinuousSchedule {
        operation_id: lifecycle_operation(AgentOperationKind::SettingsUpdate, "rev-2"),
        schedule_revision: ScheduleRevision::new(2),
        wake_policy: None,
    };
    assert!(update.records_decision());
    let outcome = applied(fx.apply_task_command(update.clone()).await);
    forget_window(&fx, false).await;

    assert_replay_converges(&fx, update, outcome).await;
}
