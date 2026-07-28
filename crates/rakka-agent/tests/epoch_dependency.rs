//! A dependency failure that terminates an epoch still returns the epoch's
//! result to its controller.
//!
//! The terminal transition of `RecordDependencyOutcome` owes the
//! `EpochResult` exactly as a cancellation does: when the failure closes an
//! epoch whose ledger has nothing outstanding, that transition is the first —
//! and only — observer of the closed ledger. If it forgot to owe the result,
//! the controller's wake would stay active forever and the goal would never
//! admit another occurrence.

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_task_state, wake_admission_command, AgentOperationId, AgentOperationKind,
    AgentSchemaPolicy, AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome,
    AgentTaskEntityCommand, AgentTaskEntityReply, AgentTaskEntityStore, AgentTaskId,
    AgentTaskScope, AgentTaskState, ScheduleRevision,
};

mod common;

use common::{
    continuous_goal_mode, epoch_scopes_for, scheduled_wake_binding, task_scope, wake_policy,
    Fixture, TENANT,
};

async fn state_at(fx: &Fixture, scope: &AgentTaskScope) -> AgentTaskState {
    load_agent_task_state(&fx.tasks, scope, &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the task exists")
}

async fn apply_at(
    fx: &Fixture,
    scope: &AgentTaskScope,
    command: AgentTaskEntityCommand,
) -> AgentTaskEntityReply {
    let mut task = AgentTaskEntityStore::new(
        scope.clone(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    let now = fx.now();
    task.recover(now).await.expect("the task recovers");
    task.apply(command, &fx.router, fx.now())
        .await
        .expect("the command applies")
}

#[tokio::test]
async fn a_failed_dependency_still_returns_the_epoch_result() {
    let fx = Fixture::new(ScriptedDispatcher::new());
    // The agent is deliberately never instantiated: the epoch task's
    // assignment is refused, so no run escrow ever opens, and the dependency
    // failure below is the transition that closes the epoch's ledger.
    fx.create_continuous_control_task(continuous_goal_mode(wake_policy()))
        .await;

    let binding = scheduled_wake_binding(1_000, ScheduleRevision::INITIAL);
    let (epoch_scope, _run_scope) = epoch_scopes_for(binding.wake_id());
    let reply = fx
        .apply_task_command(wake_admission_command(binding.clone()).expect("the command derives"))
        .await
        .expect("the admission applies");
    assert!(matches!(reply, AgentTaskEntityReply::Applied { .. }));
    fx.settle_task_at(&task_scope())
        .await
        .expect("the epoch creation delivers");

    // The epoch exists, unassigned and with nothing outstanding.
    let epoch = state_at(&fx, &epoch_scope).await;
    let epoch_task = epoch.task().expect("the epoch task is created");
    assert!(!epoch_task.status.is_terminal());
    assert_eq!(epoch_task.escrow.outstanding().count(), 0);

    // A declared dependency fails, which terminates the epoch under the
    // default cancel-dependents policy.
    let blocker = AgentTaskId::new("blocker-1").expect("the task id is valid");
    let epoch_task_id = epoch_scope.task().as_str();
    apply_at(
        &fx,
        &epoch_scope,
        AgentTaskEntityCommand::DeclareDependency {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Command,
                [TENANT, epoch_task_id, "declare-blocker"],
            )
            .expect("the operation id derives"),
            declaration: Box::new(AgentTaskDependencyDeclaration::new(blocker.clone())),
        },
    )
    .await;
    apply_at(
        &fx,
        &epoch_scope,
        AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Command,
                [TENANT, epoch_task_id, "blocker-failed"],
            )
            .expect("the operation id derives"),
            dependency: blocker,
            outcome: AgentTaskDependencyOutcome::Failed,
        },
    )
    .await;
    let epoch = state_at(&fx, &epoch_scope).await;
    assert!(epoch.task().expect("the epoch exists").status.is_terminal());

    // The terminal transition owed the epoch result; settling both entities
    // drives it home and the controller releases the wake, keeping the goal
    // alive for its next occurrence.
    for _ in 0..8 {
        let epoch_progress = fx
            .settle_task_at(&epoch_scope)
            .await
            .expect("the epoch settles");
        let root_progress = fx
            .settle_task_at(&task_scope())
            .await
            .expect("the root settles");
        if epoch_progress.outstanding == 0 && root_progress.outstanding == 0 {
            break;
        }
    }

    let root = state_at(&fx, &task_scope()).await;
    let root_task = root.task().expect("the root exists");
    assert!(
        !root_task.status.is_terminal(),
        "completion never terminates the goal"
    );
    let controller = root_task
        .wake_controller
        .as_ref()
        .expect("the controller exists");
    assert!(
        controller.active().is_empty(),
        "the failed epoch's wake is released"
    );
    assert_eq!(controller.counters().released, 1);
    assert_eq!(
        root_task.escrow.outstanding().count(),
        0,
        "the epoch's escrow settled and returned"
    );
}
