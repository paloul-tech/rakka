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
    load_agent_task_state, AgentOperationId, AgentOperationKind, AgentSchemaPolicy,
    AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome, AgentTaskEntityCommand,
    AgentTaskEntityReply, AgentTaskError, AgentTaskId, AgentTaskOutcome, AgentTaskStatus,
};

mod common;

use common::{
    forget_operations, goal_spec, goal_spec_draft, goal_task_creation_command, task_definition,
    task_scope, Fixture, TASK, TENANT,
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
