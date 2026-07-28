//! The goal contract and its lifecycle at the entity surface.
//!
//! Specification: section 8.1 (the `AgentGoalSpec` contract, the
//! `AgentGoalStatus` lifecycle, and the `Unsatisfied`/`Failed` distinction)
//! and section 6.3 (the root `AgentTaskEntity` coordinates; the goal identity
//! defaults to the root task's value while the types stay distinct). Every
//! entity here is rebuilt from durable state per call — the `Fixture`
//! convention — so every command already arrives after a restart.

use std::sync::atomic::Ordering;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, AgentGoalDecision, AgentGoalId, AgentGoalStatus,
    AgentGoalTerminalReason, AgentGoalWaitReason, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentSchemaPolicy, AgentTaskEntityCommand, AgentTaskEntityReply,
    AgentTaskError, AgentTaskStatus,
};

mod common;

use common::{
    continuous_goal_control_creation_command, continuous_goal_mode, goal_evaluation, goal_spec,
    goal_spec_draft, goal_task_creation_command, provenance, task_definition, task_scope,
    wake_policy, Fixture, TASK, TENANT,
};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

fn operation(step: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
        .expect("the operation id derives")
}

async fn snapshot(fx: &Fixture) -> rakka_agent::AgentTaskSnapshot {
    match fx
        .apply_task_command(AgentTaskEntityCommand::Describe)
        .await
        .expect("describe answers")
    {
        AgentTaskEntityReply::Snapshot(Some(snapshot)) => *snapshot,
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

fn refused_code(result: Result<AgentTaskEntityReply, AgentTaskError>) -> String {
    match result {
        Err(error) => error.code().to_string(),
        Ok(other) => panic!("expected a refusal, got {other:?}"),
    }
}

fn applied(result: Result<AgentTaskEntityReply, AgentTaskError>) -> rakka_agent::AgentTaskOutcome {
    match result {
        Ok(AgentTaskEntityReply::Applied { outcome }) => outcome,
        other => panic!("expected the command to apply, got {other:?}"),
    }
}

fn cancellation(expected: AgentRevisionNumber, at: u64) -> AgentGoalDecision {
    AgentGoalDecision {
        reason: AgentGoalTerminalReason::CancellationRequested {
            reason: "operator".to_string(),
        },
        evaluation: None,
        provenance: Some(Box::new(provenance(at))),
        expected_status_revision: expected,
    }
}

#[tokio::test]
async fn creation_institutes_the_goal_under_the_derived_identity() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let outcome = applied(
        fx.apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(goal_spec(), true),
        ))
        .await,
    );
    // The goal outcome rides the task outcome, active from the creating
    // transition — creating the root task is the authorization to work.
    let goal = outcome.goal.expect("the outcome carries the goal");
    assert_eq!(goal.status, AgentGoalStatus::Active);

    let view = snapshot(&fx).await;
    // No explicit binding was given, so the identity is derived from the root
    // task's own value — open decision 14's resolved default — while the
    // types stay distinct.
    assert_eq!(
        view.goal,
        Some(AgentGoalId::for_root_task(task_scope().task()))
    );
    let goal = view.goal_state.expect("the snapshot carries the goal view");
    assert_eq!(goal.status, AgentGoalStatus::Active);
    assert_eq!(goal.spec_revision, AgentRevisionNumber::INITIAL);
    assert_eq!(goal.criteria_revision, AgentRevisionNumber::INITIAL);
    // The creation decided the assignment in the same compare-and-set, the
    // run accepted, and the coordinator run is derived from the assignment.
    assert_eq!(view.status, AgentTaskStatus::InProgress);
    assert!(goal.coordinator.is_some(), "the coordinator run is derived");
}

#[tokio::test]
async fn a_proposed_goal_spends_nothing_until_activated() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), false),
    ))
    .await
    .expect("the creation applies");

    let view = snapshot(&fx).await;
    let goal = view.goal_state.expect("the goal view exists");
    assert_eq!(goal.status, AgentGoalStatus::Proposed);
    // The task is created and its agent is admitted, but the proposed goal
    // gates the assignment: nothing spends before activation.
    assert_eq!(view.status, AgentTaskStatus::Created);
    assert!(view.assignment.is_none(), "no assignment before activation");

    // A settle sweep moves nothing either.
    fx.settle_task_at(&task_scope())
        .await
        .expect("the sweep settles");
    assert!(snapshot(&fx).await.assignment.is_none());

    let activate = AgentTaskEntityCommand::ActivateGoal {
        operation_id: operation("activate"),
        expected_status_revision: AgentRevisionNumber::INITIAL,
        provenance: Box::new(provenance(10)),
    };
    let outcome = applied(fx.apply_task_command(activate.clone()).await);
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Active
    );
    // Activation made the task assignable, and the assignment committed with
    // it in the same compare-and-set.
    assert_eq!(outcome.status, AgentTaskStatus::Assigned);

    // A replayed activation answers from the operation log, not a second
    // transition.
    let reply = fx
        .apply_task_command(activate)
        .await
        .expect("the replay answers");
    assert!(
        matches!(reply, AgentTaskEntityReply::Duplicate { .. }),
        "a replay is a duplicate, got {reply:?}"
    );

    // A second activation under a fresh operation id is refused: the goal is
    // no longer proposed.
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::ActivateGoal {
            operation_id: operation("activate-again"),
            expected_status_revision: AgentRevisionNumber::INITIAL.next(),
            provenance: Box::new(provenance(11)),
        })
        .await;
    assert_eq!(refused_code(refusal), "goal-not-proposed");
}

#[tokio::test]
async fn terminal_decisions_are_fenced_deduplicated_and_absorbing() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");

    // A stale expected revision is fenced.
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("cancel-stale"),
            decision: Box::new(cancellation(AgentRevisionNumber::new(9), 20)),
        })
        .await;
    assert_eq!(refused_code(refusal), "goal-stale-status-revision");

    let cancel = AgentTaskEntityCommand::RecordGoalDecision {
        operation_id: operation("cancel"),
        decision: Box::new(cancellation(AgentRevisionNumber::INITIAL, 21)),
    };
    let outcome = applied(fx.apply_task_command(cancel.clone()).await);
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Cancelled
    );

    // The replay answers from the record; a fresh decision is refused by the
    // absorbing terminal status.
    let reply = fx
        .apply_task_command(cancel)
        .await
        .expect("the replay answers");
    assert!(matches!(reply, AgentTaskEntityReply::Duplicate { .. }));
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("cancel-after-terminal"),
            decision: Box::new(cancellation(AgentRevisionNumber::INITIAL.next(), 22)),
        })
        .await;
    assert_eq!(refused_code(refusal), "goal-terminal");

    let view = snapshot(&fx).await;
    let goal = view.goal_state.expect("the goal view exists");
    assert_eq!(goal.status, AgentGoalStatus::Cancelled);
    assert_eq!(
        goal.terminal.map(|reason| reason.code()),
        Some("cancellation-requested")
    );
}

#[tokio::test]
async fn satisfaction_requires_an_evaluation_of_the_current_criteria() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");

    // A model or operator may propose satisfaction, but the declaration alone
    // is not sufficient (specification 8.3): without an evaluation reference
    // the decision is refused at the entity surface.
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("satisfy-bare"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: None,
                provenance: Some(Box::new(provenance(30))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await;
    assert_eq!(refused_code(refusal), "goal-decision-without-evaluation");

    // An evaluation of another criteria revision is stale.
    let mut stale = goal_evaluation();
    stale.criteria_revision = AgentRevisionNumber::new(7);
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("satisfy-stale"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(stale)),
                provenance: Some(Box::new(provenance(31))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await;
    assert_eq!(refused_code(refusal), "goal-evaluation-stale");

    // An evaluation of the criteria revision in force is accepted.
    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("satisfy"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(goal_evaluation())),
                provenance: Some(Box::new(provenance(32))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await,
    );
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Satisfied
    );
}

#[tokio::test]
async fn an_oversized_decision_payload_is_refused_whole() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");

    // The reason strings are truncated by the goal record itself, but the
    // evaluation reference carries caller-sized artifact fields — a payload
    // the bounded task record cannot hold refuses the whole decision, and
    // the goal stays decidable.
    let mut evaluation = goal_evaluation();
    evaluation.evidence = Some(rakka_agent_workflow::ArtifactRef {
        artifact_id: "evidence-1".to_string(),
        kind: rakka_agent_workflow::ArtifactKind::File,
        uri: format!("s3://evidence/{}", "x".repeat(64 * 1024)),
        checksum: None,
        content_type: None,
        byte_len: None,
        retention_class: None,
        encryption: None,
        redaction: rakka_agent_workflow::RedactionStatus::Unredacted,
        created_at: rakka_agent_workflow::AgentTimestampMillis::new(1),
        metadata: rakka_agent_workflow::AgentAttributes::default(),
    });
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("satisfy-oversized"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(evaluation)),
                provenance: Some(Box::new(provenance(35))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await;
    assert_eq!(refused_code(refusal), "task-state-too-large");

    // Nothing was persisted: the goal is still active, and a bounded
    // decision under the same expected revision still applies.
    let view = snapshot(&fx).await;
    assert_eq!(
        view.goal_state.expect("the goal view exists").status,
        AgentGoalStatus::Active
    );
    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("satisfy-bounded"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(goal_evaluation())),
                provenance: Some(Box::new(provenance(36))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await,
    );
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Satisfied
    );
}

#[tokio::test]
async fn an_unsatisfied_decision_records_the_evaluator_not_a_failure() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");

    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("unsatisfied"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaNotMet,
                evaluation: Some(Box::new(goal_evaluation())),
                provenance: Some(Box::new(provenance(40))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await,
    );
    // `Unsatisfied` is an evaluator or policy decision that the criteria were
    // not met; `Failed` would be an execution failure — the two statuses stay
    // distinct in the record.
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Unsatisfied
    );
    let view = snapshot(&fx).await;
    assert_eq!(
        view.goal_state
            .expect("the goal view exists")
            .terminal
            .map(|reason| reason.code()),
        Some("criteria-not-met")
    );
}

#[tokio::test]
async fn cancelling_the_root_task_takes_the_goal_with_it() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");

    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::Cancel {
            operation_id: operation("cancel-task"),
            reason: "operator".to_string(),
        })
        .await,
    );
    assert_eq!(outcome.status, AgentTaskStatus::Cancelled);
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Cancelled
    );
    let view = snapshot(&fx).await;
    assert_eq!(
        view.goal_state
            .expect("the goal view exists")
            .terminal
            .map(|reason| reason.code()),
        Some("root-task-cancelled")
    );
}

#[tokio::test]
async fn retiring_the_continuous_gate_cancels_the_contract_with_the_retired_reason() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");

    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::RetireContinuousGoal {
            operation_id: operation("retire"),
            expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
            provenance: Box::new(provenance(50)),
        })
        .await,
    );
    // Specification 8.1 has no `Retired` status: retirement is an authorized
    // stop, so the contract ends `Cancelled` with the structured reason on
    // record — a reason, not a new top-level status (specification 9.7).
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Cancelled
    );
    let view = snapshot(&fx).await;
    let goal = view.goal_state.expect("the goal view exists");
    assert!(matches!(
        goal.terminal,
        Some(AgentGoalTerminalReason::Retired)
    ));
}

#[tokio::test]
async fn suspending_the_admission_gate_parks_the_goal_and_resume_lifts_it() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(goal_spec(), true),
    ))
    .await
    .expect("the creation applies");

    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::SuspendContinuousGoal {
            operation_id: operation("suspend"),
            expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
            reason: Some("maintenance".to_string()),
            provenance: Box::new(provenance(60)),
        })
        .await,
    );
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Waiting
    );
    let view = snapshot(&fx).await;
    assert!(matches!(
        view.goal_state.expect("the goal view exists").wait,
        Some(AgentGoalWaitReason::AdmissionSuspended)
    ));

    // The goal door does not own an admission suspension: its resume is
    // refused toward the gate's own command.
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::ResumeGoal {
            operation_id: operation("resume-goal-wrong-door"),
            expected_status_revision: AgentRevisionNumber::INITIAL.next(),
            top_up: None,
            provenance: Box::new(provenance(61)),
        })
        .await;
    assert_eq!(refused_code(refusal), "task-goal-wait-owned-elsewhere");

    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: operation("resume"),
            expected_lifecycle_revision: AgentRevisionNumber::INITIAL.next(),
            provenance: Box::new(provenance(62)),
        })
        .await,
    );
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Active
    );
}

#[tokio::test]
async fn a_passed_deadline_expires_the_goal_from_the_settle_pass() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.deadline = Some(rakka_agent_workflow::AgentTimestampMillis::new(5_000));
    // Human-owned, so no run machinery is in flight when the deadline passes.
    fx.apply_task_command(continuous_goal_control_creation_command(
        continuous_goal_mode(wake_policy()),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the creation applies");

    // The deadline passes while the goal is fully passivated; the next settle
    // sweep — a recovery pass, not a command — observes it durably.
    fx.clock.store(6_000, Ordering::SeqCst);
    fx.settle_task_at(&task_scope())
        .await
        .expect("the sweep settles");

    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    let task = state.task().expect("the task is created");
    let goal = task.goal_state.as_deref().expect("the goal record exists");
    assert_eq!(goal.status(), AgentGoalStatus::Expired);
    assert_eq!(
        goal.terminal().map(|decision| decision.reason.code()),
        Some("deadline-expired")
    );
    // The admission gate closed with the contract.
    assert!(task
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle()
        .status()
        .is_terminal());

    // Terminal is absorbing: a later activation is refused.
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::ActivateGoal {
            operation_id: operation("activate-expired"),
            expected_status_revision: AgentRevisionNumber::INITIAL.next(),
            provenance: Box::new(provenance(70)),
        })
        .await;
    assert_eq!(refused_code(refusal), "goal-terminal");
}
