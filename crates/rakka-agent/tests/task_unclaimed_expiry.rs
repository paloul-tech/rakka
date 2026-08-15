//! Scenario 42 hardening ([specification 8.10](../../../docs/plans/rakka-agent/spec.md)):
//! a board-governed task that no claim ever names expires at its
//! definition's unclaimed horizon — the bounded replacement for the assignee
//! fail-fast a team creation forgoes. A wrong team id, a task never posted,
//! or a board that expired before any claim surfaces as a cancelled task
//! with its escrow settled, never as a silently parked one.

mod common;

use common::{task_scope, tenant, Fixture, TENANT};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::AgentRevisionNumber;
use rakka_agent::{
    AgentAssignmentStatus, AgentGoalId, AgentId, AgentOperationId, AgentOperationKind, AgentScope,
    AgentTaskContent, AgentTaskCreation, AgentTaskEntityCommand, AgentTaskStatus,
    AgentTaskTerminalReason, AgentTeamCreation, AgentTeamEntityCommand, AgentTeamId,
    AgentTeamPolicy, AgentTeamScope, AGENT_TASK_DEFAULT_MAX_UNCLAIMED_MILLIS,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

const TEAM: &str = "support-team";
const LEADER: &str = "lead";
const MEMBER: &str = "worker-a";

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
}

fn team_scope() -> AgentTeamScope {
    AgentTeamScope::new(
        tenant(),
        AgentTeamId::new(TEAM).expect("the team id is valid"),
    )
    .expect("the team scope is valid")
}

fn member(name: &str) -> AgentId {
    AgentId::new(name).expect("the member id is valid")
}

fn op(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::TeamClaim, [TENANT, TEAM, discriminator])
        .expect("the operation id derives")
}

/// Creates the fixture task on the board posture: team provenance, no
/// assignee — it waits until a claim names one, bounded by its horizon.
async fn create_board_task(fx: &Fixture) {
    fx.apply_task_command_at(
        &task_scope(),
        AgentTaskEntityCommand::Create {
            operation_id: AgentOperationId::new(
                AgentOperationKind::TaskCreation,
                [TENANT, task_scope().task().as_str(), "1"],
            )
            .expect("the operation id derives"),
            creation: Box::new(AgentTaskCreation {
                definition: common::task_definition(),
                input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                    .expect("the input is inline-bounded"),
                assignee: None,
                team: Some(AgentTeamId::new(TEAM).expect("the team id is valid")),
                goal: None,
                goal_mode: Default::default(),
                goal_spec: None,
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: None,
                telemetry: Default::default(),
            }),
        },
    )
    .await
    .expect("the board task creates");
}

#[tokio::test]
async fn an_unclaimed_board_task_expires_at_its_horizon() {
    let fx = fixture();
    create_board_task(&fx).await;

    // Inside the horizon the settle pass leaves the parked task alone and
    // burns no revision doing it.
    fx.clock
        .store(AGENT_TASK_DEFAULT_MAX_UNCLAIMED_MILLIS, Ordering::SeqCst);
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    let task = fx.task_snapshot().await;
    assert!(
        !task.status.is_terminal(),
        "a wait inside the horizon does not expire"
    );

    // Past the horizon the settle pass expires it through the cancellation
    // machinery whole: terminal, reasoned, escrow closed — never parked.
    fx.clock.store(
        AGENT_TASK_DEFAULT_MAX_UNCLAIMED_MILLIS + 1_000,
        Ordering::SeqCst,
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Cancelled);
    assert!(
        matches!(
            task.terminal_reason,
            Some(AgentTaskTerminalReason::CancellationRequested { ref reason })
                if reason == "unclaimed-expired"
        ),
        "the terminal reason names the unclaimed expiry, got {:?}",
        task.terminal_reason
    );

    // The expiry is a terminal like any other, so the eager notice went out
    // to the governing team — which never existed here. `team-not-found` is
    // a definitive refusal: the notice settles instead of re-driving
    // forever against a board that will never answer.
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    let task = fx.task_snapshot().await;
    assert!(
        task.team_terminal_notice_settled,
        "a missing team settles the notice definitively"
    );
}

#[tokio::test]
async fn a_recorded_claim_holds_the_unclaimed_horizon_open() {
    let fx = fixture();
    fx.instantiate_agent_at(
        AgentScope::new(tenant(), member(MEMBER)).expect("the member scope is valid"),
    )
    .await;
    let mut members: BTreeMap<AgentId, BTreeSet<rakka_agent::AgentCapabilityId>> = BTreeMap::new();
    members.insert(member(MEMBER), BTreeSet::new());
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Create {
            operation_id: op("create"),
            creation: Box::new(AgentTeamCreation {
                leader: member(LEADER),
                root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
                policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
                members,
            }),
        },
    )
    .await
    .expect("the team creates");
    create_board_task(&fx).await;
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post"),
            task: task_scope().task().clone(),
            posted_by: member(MEMBER),
        },
    )
    .await
    .expect("the post applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim"),
            task: task_scope().task().clone(),
            member: member(MEMBER),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");

    // The recorded claim took the task off the board-waiting posture: the
    // horizon no longer applies, however late the offer resolves.
    fx.clock.store(
        AGENT_TASK_DEFAULT_MAX_UNCLAIMED_MILLIS + 1_000,
        Ordering::SeqCst,
    );
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    let task = fx.task_snapshot().await;
    assert!(
        !task.status.is_terminal(),
        "a claimed task never expires unclaimed"
    );
    let assignment = task.assignment.expect("the offer resolved");
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    assert_eq!(assignment.agent, member(MEMBER));
}
