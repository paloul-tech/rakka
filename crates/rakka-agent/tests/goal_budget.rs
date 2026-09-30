//! The goal-scope budget-exhaustion policy: park, escalate, terminate, and
//! the one resume door that reactivates what a park stopped.
//!
//! Specification: section 8.1 ("durability does not authorize unbounded
//! compute: a budget or progress limit MUST park, escalate, or terminate
//! according to policy") and section 9.7 (hard ceilings deterministically
//! park, escalate, or terminate per persisted policy; the goal allocation
//! sits between the definition ceiling and the task's runs). The run-side
//! behavior — park, ask, stop on a zero grant — is scenario 52's, proven in
//! `escrow_ledger.rs` and unchanged here; what this file proves is the goal
//! scope's own reaction to the exhaustion it observes.

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, AgentBudgetAllocation, AgentBudgetCeilings, AgentBudgetDimension,
    AgentGoalExhaustionAction, AgentGoalLifecycleStatus, AgentGoalStatus, AgentGoalWaitReason,
    AgentOperationId, AgentOperationKind, AgentRunStatus, AgentSchemaPolicy, AgentTaskContent,
    AgentTaskCreation, AgentTaskDefinition, AgentTaskEntityCommand, AgentTaskEntityReply,
    AgentTaskError, AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskStatus,
    AgentTaskTerminalReason,
};

mod common;

use common::{
    agent_id, continuous_goal_mode, goal_id, goal_spec, goal_spec_draft,
    goal_task_creation_command, provenance, schema, task_definition_id, task_scope, wake_policy,
    Fixture, TASK, TENANT,
};

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

fn operation(step: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
        .expect("the operation id derives")
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

/// The fixture goal holding three loop iterations — all of which the first
/// run is escrowed, so its top-up ask is answered with nothing.
fn exhausted_goal(action: AgentGoalExhaustionAction) -> rakka_agent::AgentGoalSpec {
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(3),
        ..AgentBudgetAllocation::unbounded()
    };
    spec.exhaustion.default = action;
    spec
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

async fn goal_record(fx: &Fixture) -> rakka_agent::AgentGoalState {
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    state
        .task()
        .expect("the task is created")
        .goal_state
        .as_deref()
        .expect("the goal record exists")
        .clone()
}

#[tokio::test]
async fn a_zero_grant_top_up_parks_the_goal_and_a_topped_up_resume_reactivates_it() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        roomy_definition(),
        goal_spec_draft(exhausted_goal(AgentGoalExhaustionAction::Park), true),
    ))
    .await
    .expect("the creation applies");
    fx.pump()
        .await
        .expect("the run asks, is refused, and stops");

    // The run's half is unchanged: it stops with its original exhaustion.
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);

    // What is new: the goal observed its own ceiling in the same transition
    // that answered the ask with nothing, and its policy parked it.
    let goal = goal_record(&fx).await;
    assert_eq!(goal.status(), AgentGoalStatus::Waiting);
    let Some(AgentGoalWaitReason::BudgetExhausted { exhaustion }) = goal.wait() else {
        panic!("expected a budget park, got {:?}", goal.wait());
    };
    assert_eq!(exhaustion.dimension, AgentBudgetDimension::LoopIterations);

    // A parked goal spends nothing: no second assignment generation is
    // consumed by any number of settle sweeps.
    fx.settle_task_at(&task_scope())
        .await
        .expect("the sweep settles");
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    assert_eq!(state.task().expect("the task is created").assignments, 1);

    // A resume that widens nothing is refused with the exhaustion still in
    // force: resuming into the same wall would only re-park.
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::ResumeGoal {
            operation_id: operation("resume-bare"),
            expected_status_revision: goal.status_revision(),
            top_up: None,
            provenance: Box::new(provenance(80)),
        })
        .await;
    assert_eq!(refused_code(refusal), "task-goal-resume-unrelieved");

    // The owner grants more; the widening stays under the definition ceiling,
    // the goal reactivates, and the next assignment commits with it.
    let resume = AgentTaskEntityCommand::ResumeGoal {
        operation_id: operation("resume"),
        expected_status_revision: goal.status_revision(),
        top_up: Some(Box::new(AgentBudgetAllocation {
            loop_iterations: Some(5),
            ..AgentBudgetAllocation::unbounded()
        })),
        provenance: Box::new(provenance(81)),
    };
    let outcome = applied(fx.apply_task_command(resume.clone()).await);
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Active
    );
    // The task itself stays bound to the failed generation — releasing an
    // accepted run's assignment is reassignment/handoff semantics that later
    // slices own. What the resume proves here is the goal door: the ledger
    // widened, the contract reactivated, and both durably.
    assert_eq!(outcome.status, AgentTaskStatus::InProgress);

    // Replaying the resume answers from the record and widens nothing twice.
    let reply = fx
        .apply_task_command(resume)
        .await
        .expect("the replay answers");
    assert!(matches!(reply, AgentTaskEntityReply::Duplicate { .. }));
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    let escrow = &state.task().expect("the task is created").escrow;
    assert_eq!(
        escrow.allocation().loop_iterations,
        Some(8),
        "one widening: three original plus the five granted, once"
    );
}

#[tokio::test]
async fn a_terminate_policy_fails_the_goal_and_the_root_task_together() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        roomy_definition(),
        goal_spec_draft(exhausted_goal(AgentGoalExhaustionAction::Terminate), true),
    ))
    .await
    .expect("the creation applies");
    fx.pump()
        .await
        .expect("the run asks, is refused, and stops");

    let goal = goal_record(&fx).await;
    assert_eq!(goal.status(), AgentGoalStatus::Failed);
    assert_eq!(
        goal.terminal().map(|decision| decision.reason.code()),
        Some("budget-exhausted")
    );

    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    let task = state.task().expect("the task is created");
    assert_eq!(task.status, AgentTaskStatus::Failed);
    let Some(AgentTaskTerminalReason::GoalBudgetExhausted { exhaustion }) = &task.terminal_reason
    else {
        panic!(
            "expected the goal's exhaustion, got {:?}",
            task.terminal_reason
        );
    };
    assert_eq!(exhaustion.dimension, AgentBudgetDimension::LoopIterations);
}

#[tokio::test]
async fn an_escalate_policy_parks_with_the_escalation_on_record() {
    let fx = fixture();
    fx.instantiate_agent().await;
    let mut spec = exhausted_goal(AgentGoalExhaustionAction::Escalate);
    spec.escalation =
        Some(rakka_agent::AgentPolicyRef::new("page-the-owner").expect("the policy ref is valid"));
    fx.apply_task_command(goal_task_creation_command(
        roomy_definition(),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the creation applies");
    fx.pump()
        .await
        .expect("the run asks, is refused, and stops");

    // Escalation is the durable record plus the same park: goal-scope HITL
    // wiring is a later slice, and until then nothing pretends otherwise.
    let goal = goal_record(&fx).await;
    assert_eq!(goal.status(), AgentGoalStatus::Waiting);
    let Some(AgentGoalWaitReason::Escalated { exhaustion }) = goal.wait() else {
        panic!("expected an escalated park, got {:?}", goal.wait());
    };
    assert_eq!(exhaustion.dimension, AgentBudgetDimension::LoopIterations);
}

#[tokio::test]
async fn a_permanent_assignment_refusal_parks_a_continuous_goal_and_suspends_admission() {
    let fx = fixture();
    fx.instantiate_agent().await;
    // An agent-owned continuous root whose goal holds nothing in the loop
    // dimension: the very first assignment refusal is permanent — zero
    // headroom and no outstanding child whose return could restore any.
    let mut spec = goal_spec();
    spec.allocation = AgentBudgetAllocation {
        loop_iterations: Some(0),
        ..AgentBudgetAllocation::unbounded()
    };
    let creation = AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(AgentOperationKind::TaskCreation, [TENANT, TASK, "1"])
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
    };
    let outcome = applied(fx.apply_task_command(creation).await);
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Waiting
    );

    // The park closed continuous admission in the same compare-and-set, so
    // triggers coalesce and nothing spends.
    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    let task = state.task().expect("the task is created");
    let gate = task
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle();
    assert_eq!(gate.status(), AgentGoalLifecycleStatus::Suspended);
    assert_eq!(gate.suspended_reason(), Some("budget-exhausted"));

    // The gate's own resume does not own a budget park.
    let refusal = fx
        .apply_task_command(AgentTaskEntityCommand::ResumeContinuousGoal {
            operation_id: operation("gate-resume"),
            expected_lifecycle_revision: gate.lifecycle_revision(),
            provenance: Box::new(provenance(90)),
        })
        .await;
    assert_eq!(refused_code(refusal), "task-goal-wait-owned-elsewhere");

    // One resume lifts both: the ledger widens, the goal reactivates, the
    // admission gate resumes, and the assignment commits with it.
    let goal = goal_record(&fx).await;
    let outcome = applied(
        fx.apply_task_command(AgentTaskEntityCommand::ResumeGoal {
            operation_id: operation("resume"),
            expected_status_revision: goal.status_revision(),
            top_up: Some(Box::new(AgentBudgetAllocation {
                loop_iterations: Some(2),
                ..AgentBudgetAllocation::unbounded()
            })),
            provenance: Box::new(provenance(91)),
        })
        .await,
    );
    assert_eq!(
        outcome.goal.expect("the goal outcome rides").status,
        AgentGoalStatus::Active
    );
    assert_eq!(outcome.status, AgentTaskStatus::Assigned);

    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the state exists");
    let gate = state
        .task()
        .expect("the task is created")
        .wake_controller
        .as_ref()
        .expect("the controller exists")
        .lifecycle();
    assert_eq!(gate.status(), AgentGoalLifecycleStatus::Active);
}
