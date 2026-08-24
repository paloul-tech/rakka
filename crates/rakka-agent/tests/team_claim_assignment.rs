//! Scenario 42 ([specification 8.10 and 18](../../../docs/plans/rakka-agent/spec.md)):
//! concurrent team members atomically claim a task so only one normal
//! current owner may schedule effects, and stale claim/release/transfer
//! commands fail closed.
//!
//! The board never holds ownership: a claim drives `decide_assignment` on
//! the task entity, whose assignment-generation fence stays the
//! one-normal-owner guarantee, and everything the board stores about an
//! accepted assignment is an observational echo delivered by the
//! claim-result exchange.

mod common;

use common::{task_scope, tenant, Fixture, TENANT};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    team_claim_operation_id, AgentAssignmentGeneration, AgentAssignmentStatus, AgentEntityAddress,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentExchangeTransport,
    AgentGoalId, AgentId, AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentScope,
    AgentTaskContent, AgentTaskCreation, AgentTaskEntityCommand, AgentTaskTeamClaimStatus,
    AgentTeamBoardEntryStatus, AgentTeamClaimAction, AgentTeamClaimCommand, AgentTeamCreation,
    AgentTeamEntityCommand, AgentTeamEntityReply, AgentTeamEntityStore, AgentTeamId,
    AgentTeamPolicy, AgentTeamScope, AGENT_TEAM_CLAIM_PAYLOAD_TYPE,
};
use rakka_agent_workflow::AgentCorrelationId;
use std::collections::{BTreeMap, BTreeSet};

const TEAM: &str = "support-team";
const LEADER: &str = "lead";
const MEMBER_A: &str = "worker-a";
const MEMBER_B: &str = "worker-b";

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

/// Creates the team with the leader and both worker members.
async fn create_team(fx: &Fixture) {
    let mut members: BTreeMap<AgentId, BTreeSet<rakka_agent::AgentCapabilityId>> = BTreeMap::new();
    members.insert(member(MEMBER_A), BTreeSet::new());
    members.insert(member(MEMBER_B), BTreeSet::new());
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
}

/// Creates the fixture task on the board posture: team provenance, no
/// assignee — it waits until a claim names one.
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

async fn post_board_task(fx: &Fixture) {
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post"),
            task: task_scope().task().clone(),
            posted_by: member(MEMBER_A),
        },
    )
    .await
    .expect("the post applies");
}

/// The claim world: team, board task, and both members instantiated.
async fn claimable_world(fx: &Fixture) {
    for name in [MEMBER_A, MEMBER_B] {
        fx.instantiate_team_member_at(
            AgentScope::new(tenant(), member(name)).expect("the member scope is valid"),
        )
        .await;
    }
    create_team(fx).await;
    create_board_task(fx).await;
    post_board_task(fx).await;
}

/// Drives the claim choreography to quiescence: the team's courier delivers
/// the board decision, the task's settle delivers the assignment and the
/// claim result, and the final team settle absorbs it.
async fn settle_claim_round_trip(fx: &Fixture) {
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
}

fn board_entry(snapshot: &rakka_agent::AgentTeamSnapshot) -> &rakka_agent::AgentTeamBoardEntry {
    snapshot
        .board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .expect("the board holds the posted task")
}

#[tokio::test]
async fn a_board_claim_drives_exactly_one_assignment_generation_to_the_claimant() {
    let fx = fixture();
    claimable_world(&fx).await;

    let claimed = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Claim {
                operation_id: op("claim-a"),
                task: task_scope().task().clone(),
                member: member(MEMBER_A),
                expected_epoch: 0,
            },
        )
        .await
        .expect("the claim applies");
    assert!(matches!(claimed, AgentTeamEntityReply::Applied { .. }));

    settle_claim_round_trip(&fx).await;

    // The task is the one owner authority: exactly one generation, accepted
    // by the claimant's run.
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1)
    );
    let assignment = task.assignment.expect("the assignment stands");
    assert_eq!(assignment.agent, member(MEMBER_A));
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    let claim = task.team_claim.expect("the claim provenance stands");
    assert_eq!(claim.status, AgentTaskTeamClaimStatus::Accepted);
    assert!(claim.result_settled, "the claim result settled at the team");

    // The board mirrors the owner without holding ownership.
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    let echo = entry.claim.as_ref().expect("the claim echo stands");
    assert_eq!(echo.member, member(MEMBER_A));
    assert_eq!(
        echo.generation_echo,
        Some(AgentAssignmentGeneration::new(1))
    );
}

#[tokio::test]
async fn concurrent_claims_admit_one_owner_and_the_loser_fails_closed() {
    let fx = fixture();
    claimable_world(&fx).await;

    // Both members observed the open entry at epoch 0. The first decision
    // wins the board's compare-and-set and bumps the epoch.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the first claim applies");

    let loser = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Claim {
                operation_id: op("claim-b"),
                task: task_scope().task().clone(),
                member: member(MEMBER_B),
                expected_epoch: 0,
            },
        )
        .await
        .expect_err("the concurrent claim fails closed");
    assert_eq!(loser.code(), "team-claim-stale-epoch");

    settle_claim_round_trip(&fx).await;
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1),
        "one claim, one generation, one owner"
    );
    assert_eq!(
        task.assignment.expect("the assignment stands").agent,
        member(MEMBER_A)
    );
}

#[tokio::test]
async fn a_stale_owner_write_on_the_team_store_is_rejected() {
    let fx = fixture();
    claimable_world(&fx).await;

    // Two shard owners of one team scope: A recovers and caches a revision,
    // then B recovers and commits the claim first.
    let mut owner_a =
        AgentTeamEntityStore::new(team_scope(), fx.teams.clone(), fx.team_history.clone());
    owner_a.recover(fx.now()).await.expect("owner A recovers");
    let mut owner_b =
        AgentTeamEntityStore::new(team_scope(), fx.teams.clone(), fx.team_history.clone());
    owner_b.recover(fx.now()).await.expect("owner B recovers");
    owner_b
        .apply(
            AgentTeamEntityCommand::Claim {
                operation_id: op("claim-b"),
                task: task_scope().task().clone(),
                member: member(MEMBER_B),
                expected_epoch: 0,
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect("owner B's claim commits");

    // Owner A's write is fenced by the store's revision: nothing it decided
    // reaches durable state.
    let stale = owner_a
        .apply(
            AgentTeamEntityCommand::Claim {
                operation_id: op("claim-a"),
                task: task_scope().task().clone(),
                member: member(MEMBER_A),
                expected_epoch: 0,
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect_err("the stale owner's write is rejected");
    assert!(
        stale.to_string().contains("revision"),
        "the persistence fence rejected the write: {stale}"
    );

    // Re-recovered, owner A sees the durable truth and its claim fails
    // closed on the epoch it can now observe.
    owner_a
        .recover(fx.now())
        .await
        .expect("owner A re-recovers");
    let refused = owner_a
        .apply(
            AgentTeamEntityCommand::Claim {
                operation_id: op("claim-a-retry"),
                task: task_scope().task().clone(),
                member: member(MEMBER_A),
                expected_epoch: 0,
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect_err("the re-based stale claim fails closed");
    assert_eq!(refused.code(), "team-claim-stale-epoch");

    settle_claim_round_trip(&fx).await;
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1)
    );
    assert_eq!(
        task.assignment.expect("the assignment stands").agent,
        member(MEMBER_B)
    );
}

#[tokio::test]
async fn stale_release_and_transfer_commands_fail_closed() {
    let fx = fixture();
    claimable_world(&fx).await;

    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");

    let stale_release = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Release {
                operation_id: op("release-stale"),
                task: task_scope().task().clone(),
                member: member(MEMBER_A),
                expected_epoch: 0,
            },
        )
        .await
        .expect_err("a stale release fails closed");
    assert_eq!(stale_release.code(), "team-claim-stale-epoch");

    let not_holder = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Release {
                operation_id: op("release-foreign"),
                task: task_scope().task().clone(),
                member: member(MEMBER_B),
                expected_epoch: 1,
            },
        )
        .await
        .expect_err("a non-holder release fails closed");
    assert_eq!(not_holder.code(), "team-release-not-holder");

    let unexpired = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Claim {
                operation_id: op("steal-early"),
                task: task_scope().task().clone(),
                member: member(MEMBER_B),
                expected_epoch: 1,
            },
        )
        .await
        .expect_err("a pending claim under a live lease is not stealable");
    assert_eq!(unexpired.code(), "team-claim-not-stealable");

    let stale_transfer = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Transfer {
                operation_id: op("transfer-stale"),
                task: task_scope().task().clone(),
                member: member(MEMBER_A),
                target: member(MEMBER_B),
                expected_epoch: 0,
            },
        )
        .await
        .expect_err("a stale transfer fails closed");
    assert_eq!(stale_transfer.code(), "team-claim-stale-epoch");

    let foreign_target = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Transfer {
                operation_id: op("transfer-foreign"),
                task: task_scope().task().clone(),
                member: member(MEMBER_A),
                target: member("outsider"),
                expected_epoch: 1,
            },
        )
        .await
        .expect_err("a transfer to a non-member fails closed");
    assert_eq!(foreign_target.code(), "team-not-member");
}

#[tokio::test]
async fn a_refused_claim_resolves_through_the_result_and_reopens_the_board() {
    let fx = fixture();
    // The claimant is a member but was never instantiated: the assignment
    // decision's readiness read refuses, and the single-attempt rule
    // resolves the claim rather than parking the task.
    create_team(&fx).await;
    create_board_task(&fx).await;
    post_board_task(&fx).await;

    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-ghost"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies at the board");

    settle_claim_round_trip(&fx).await;

    let task = fx.task_snapshot().await;
    assert!(task.assignment.is_none(), "no generation was accepted");
    let claim = task.team_claim.expect("the claim provenance stands");
    assert!(matches!(
        claim.status,
        AgentTaskTeamClaimStatus::Refused { .. }
    ));
    assert!(claim.result_settled);

    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Open);
    assert!(entry.claim.is_none());
    assert_eq!(
        entry.last_code.as_deref(),
        Some("agent-not-instantiated"),
        "the board carries the refusal code the members rebase on"
    );
}

#[tokio::test]
async fn a_member_without_the_team_capability_is_refused_at_the_assignment_door() {
    // The M5 setup-cannot-widen bullet, with coordination as the dimension
    // ([specification 8.8](../../../docs/plans/rakka-agent/spec.md)). Board
    // membership is trusted application wiring and never an authority source
    // of its own: this claimant is a fully instantiated, perfectly healthy
    // agent, admitted to the task definition and on the roster — everything
    // the pre-5.6 checks looked at — and its *definition* simply never granted
    // the `Team` coordination capability a board claim spends.
    let fx = fixture();
    let scope = AgentScope::new(tenant(), member(MEMBER_A)).expect("the member scope is valid");
    // Instantiated with the plain envelope: task definition yes, coordination
    // capabilities none.
    fx.instantiate_agent_at(scope.clone()).await;
    create_team(&fx).await;
    create_board_task(&fx).await;
    post_board_task(&fx).await;

    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-uncapable"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies at the board");

    settle_claim_round_trip(&fx).await;

    let task = fx.task_snapshot().await;
    assert!(
        task.assignment.is_none(),
        "no generation was bought without the capability"
    );
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::default(),
        "and none was minted, so the ledger is untouched"
    );
    let claim = task.team_claim.expect("the claim provenance stands");
    assert!(matches!(
        claim.status,
        AgentTaskTeamClaimStatus::Refused { .. }
    ));
    assert!(claim.result_settled, "the refusal reached the board");

    // The single-attempt rule: the entry reopens for a member that *is*
    // authorized rather than parking the task in a refusal loop.
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Open);
    assert!(entry.claim.is_none());
    assert_eq!(
        entry.last_code.as_deref(),
        Some("team-coordination-unauthorized"),
        "the board carries the refusal code the members rebase on"
    );

    // The same agent, same task, assigned directly, is untouched: a direct
    // assignment spends no coordination capability, so the door is a door on
    // the claim and not a second admission check.
    let readiness_task = fx.task_snapshot().await;
    assert!(
        readiness_task.last_refusal.is_none(),
        "the claim's refusal is the claim's, not a standing refusal against the agent"
    );
}

#[tokio::test]
async fn an_expired_lease_steal_supersedes_the_pending_claim_before_acceptance() {
    let fx = fixture();
    // Member A never instantiated: its claim can never accept, so the steal
    // window is real. Member B is ready.
    fx.instantiate_team_member_at(
        AgentScope::new(tenant(), member(MEMBER_B)).expect("the member scope is valid"),
    )
    .await;
    create_team(&fx).await;
    create_board_task(&fx).await;
    post_board_task(&fx).await;

    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the first claim applies");

    // The lease lapses; the entry becomes stealable while still pending.
    fx.clock.store(400_000, std::sync::atomic::Ordering::SeqCst);
    let stolen = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Claim {
                operation_id: op("steal-b"),
                task: task_scope().task().clone(),
                member: member(MEMBER_B),
                expected_epoch: 1,
            },
        )
        .await
        .expect("the expired-lease steal applies");
    assert!(matches!(stolen, AgentTeamEntityReply::Applied { .. }));

    settle_claim_round_trip(&fx).await;
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");

    let task = fx.task_snapshot().await;
    let assignment = task.assignment.expect("the winner's assignment stands");
    assert_eq!(assignment.agent, member(MEMBER_B));
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1),
        "the superseded claim never minted a generation"
    );

    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    assert_eq!(
        entry.claim.as_ref().expect("the echo stands").member,
        member(MEMBER_B)
    );
}

#[tokio::test]
async fn a_late_board_decision_defers_to_the_assignment_fence() {
    let fx = fixture();
    claimable_world(&fx).await;
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");
    settle_claim_round_trip(&fx).await;

    // The board itself refuses a steal over an activated claim, lease or no
    // lease: the lease bounds the pending window only.
    fx.clock.store(500_000, std::sync::atomic::Ordering::SeqCst);
    let owned = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Claim {
                operation_id: op("steal-owned"),
                task: task_scope().task().clone(),
                member: member(MEMBER_B),
                expected_epoch: 1,
            },
        )
        .await
        .expect_err("an activated claim is never stealable");
    assert_eq!(owned.code(), "team-claim-already-owned");

    // And even a well-formed decision delivered straight at the task — a
    // reordered courier, a forged board — refuses on the task's own fence:
    // the assignment-generation machinery stays the one-owner guarantee.
    let claim =
        rakka_agent::team_claim_id_for(&team_scope(), task_scope().task(), &member(MEMBER_B), 9)
            .expect("the claim id derives");
    let command = AgentTeamClaimCommand {
        team: team_scope(),
        claim: claim.clone(),
        task: task_scope().task().clone(),
        epoch: 9,
        action: AgentTeamClaimAction::Claim {
            member: member(MEMBER_B),
        },
        policy_revision: AgentRevisionNumber::INITIAL,
        lease_expires_at: rakka_agent_workflow::AgentTimestampMillis::new(600_000),
    };
    let operation = team_claim_operation_id(&tenant(), &claim).expect("the operation derives");
    let envelope = AgentExchangeEnvelope::new(
        operation.clone(),
        AgentExchangeKind::TeamClaim,
        AgentEntityAddress::Team(team_scope()),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_TEAM_CLAIM_PAYLOAD_TYPE, &command)
            .expect("the payload encodes"),
        AgentCorrelationId::new(operation.as_str()),
        fx.now(),
    )
    .expect("the envelope builds");
    let reply = fx
        .router
        .deliver(&envelope)
        .await
        .expect("the delivery reaches the task");
    assert_eq!(
        reply.result().status().rejection_code(),
        Some("team-claim-already-owned"),
        "the task's fence protects the accepted owner"
    );

    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1)
    );
    assert_eq!(
        task.assignment.expect("the assignment stands").agent,
        member(MEMBER_A),
        "only one normal owner may schedule effects"
    );
}

#[tokio::test]
async fn a_release_racing_the_offer_restores_and_the_acceptance_stands() {
    let fx = fixture();
    claimable_world(&fx).await;

    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");
    // The release races the claim's own delivery: both board decisions are
    // outstanding when the courier drives.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Release {
            operation_id: op("release-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 1,
        },
    )
    .await
    .expect("the release applies at the board");

    // One drive delivers both: the claim records and mints its offer; the
    // release finds the generation in flight and refuses definitively.
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(
        entry.status,
        AgentTeamBoardEntryStatus::Pending,
        "the refused release restores the pending claim"
    );
    assert_eq!(
        entry.last_code.as_deref(),
        Some("team-release-assignment-inflight")
    );

    // The offer resolves; the claim stands and activates.
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    assert_eq!(board_entry(&team).status, AgentTeamBoardEntryStatus::Active);
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment.expect("the assignment stands").status,
        AgentAssignmentStatus::Accepted
    );
}

#[tokio::test]
async fn a_claim_against_a_missing_or_foreign_task_closes_the_entry() {
    let fx = fixture();
    fx.instantiate_team_member_at(
        AgentScope::new(tenant(), member(MEMBER_A)).expect("the member scope is valid"),
    )
    .await;
    create_team(&fx).await;
    // A board task that names a task entity that was never created, and one
    // created under a different team's governance.
    let phantom = rakka_agent::AgentTaskId::new("phantom").expect("the task id is valid");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-phantom"),
            task: phantom.clone(),
            posted_by: member(MEMBER_A),
        },
    )
    .await
    .expect("the post applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-phantom"),
            task: phantom.clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies at the board");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");

    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = team
        .board
        .iter()
        .find(|entry| entry.task == phantom)
        .expect("the phantom entry stands");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Done);
    assert_eq!(entry.last_code.as_deref(), Some("team-claim-task-unknown"));
}

#[tokio::test]
async fn replayed_claim_commands_and_deliveries_converge_on_one_claim() {
    let fx = fixture();
    claimable_world(&fx).await;

    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");

    // The replayed command answers from the operation log.
    let replay = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Claim {
                operation_id: op("claim-a"),
                task: task_scope().task().clone(),
                member: member(MEMBER_A),
                expected_epoch: 0,
            },
        )
        .await
        .expect("the replay is answered");
    assert!(matches!(replay, AgentTeamEntityReply::Duplicate { .. }));

    // The claim exchange arrives twice at the task: one recorded claim, one
    // generation, and the second delivery echoes the first's result.
    fx.task_transport
        .inject(rakka_agent::testkit::ExchangeFault::DeliverTwice);
    settle_claim_round_trip(&fx).await;

    let task = fx.task_snapshot().await;
    assert_eq!(task.team_claims, 1, "one claim recorded, however delivered");
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1)
    );
    assert_eq!(
        task.assignment.expect("the assignment stands").agent,
        member(MEMBER_A)
    );
}

#[tokio::test]
async fn a_steal_racing_an_undecided_offer_settles_and_reopens_the_board() {
    let fx = fixture();
    claimable_world(&fx).await;

    // A's claim reaches the task and mints its offer, which stays undecided:
    // the assignment exchange to the run has not been driven yet.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");

    // The lease lapses and B steals the still-pending entry; the task
    // refuses the superseding claim because A's offer is still in flight.
    fx.clock.store(400_000, std::sync::atomic::Ordering::SeqCst);
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("steal-b"),
            task: task_scope().task().clone(),
            member: member(MEMBER_B),
            expected_epoch: 1,
        },
    )
    .await
    .expect("the expired-lease steal applies");
    fx.settle_team_at(&team_scope())
        .await
        .expect("the refused steal settles instead of livelocking");

    // The refusal is definitive and the entry reopens for a retry after the
    // offer resolves — the exact promise the task-side refusal makes.
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Open);
    assert!(entry.claim.is_none());
    assert_eq!(
        entry.last_code.as_deref(),
        Some("team-claim-assignment-inflight")
    );

    // The offer resolves as accepted, and the owner echo fills the reopened
    // entry: the board converges on the owner the fence protected.
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    let echo = entry.claim.as_ref().expect("the owner echo stands");
    assert_eq!(echo.member, member(MEMBER_A));
    assert_eq!(
        echo.generation_echo,
        Some(AgentAssignmentGeneration::new(1))
    );
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment.expect("the assignment stands").agent,
        member(MEMBER_A)
    );
    assert_eq!(task.team_claims, 1, "the refused steal recorded nothing");
}

#[tokio::test]
async fn an_early_owner_echo_survives_the_superseded_steals_refusal() {
    let fx = fixture();
    claimable_world(&fx).await;

    // A's claim records, its offer is driven, and the run accepts: the task
    // durably owns the work and owes the board the activated claim result.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");

    // The activated result has not reached the board yet, so the entry is
    // still pending and the lapsed lease lets B steal it.
    fx.clock.store(400_000, std::sync::atomic::Ordering::SeqCst);
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("steal-b"),
            task: task_scope().task().clone(),
            member: member(MEMBER_B),
            expected_epoch: 1,
        },
    )
    .await
    .expect("the expired-lease steal applies");

    // The superseded claim's activation is delivered first: the owner echo
    // must fill the entry even though the steal's claim currently holds it.
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    assert_eq!(
        entry.claim.as_ref().expect("the echo stands").member,
        member(MEMBER_A)
    );

    // The steal's already-owned refusal settles after the echo and must not
    // clobber it: the board keeps showing who owns the task.
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    let echo = entry.claim.as_ref().expect("the owner echo survives");
    assert_eq!(echo.member, member(MEMBER_A));
    assert_eq!(
        echo.generation_echo,
        Some(AgentAssignmentGeneration::new(1))
    );
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment.expect("the assignment stands").agent,
        member(MEMBER_A)
    );
}

#[tokio::test]
async fn a_release_racing_the_acceptance_refuses_owned_and_the_owner_echo_fills() {
    let fx = fixture();
    claimable_world(&fx).await;

    // A's claim records and its offer is accepted; the activated result is
    // still in flight toward the board when the release decision commits.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Release {
            operation_id: op("release-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 1,
        },
    )
    .await
    .expect("the release applies at the board");

    // The task refuses the release of a settled, accepted claim as owned —
    // ownership leaves the board only through task-side outcomes — and the
    // board marks the entry owned instead of reopening it.
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(
        entry.status,
        AgentTeamBoardEntryStatus::Active,
        "a released acceptance must not reopen the entry"
    );
    assert_eq!(entry.last_code.as_deref(), Some("team-claim-already-owned"));

    // The activated result lands and completes the echo.
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    let echo = entry.claim.as_ref().expect("the owner echo stands");
    assert_eq!(echo.member, member(MEMBER_A));
    assert_eq!(
        echo.generation_echo,
        Some(AgentAssignmentGeneration::new(1))
    );
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment.expect("the assignment stands").status,
        AgentAssignmentStatus::Accepted
    );
}

#[tokio::test]
async fn a_release_outrunning_its_claim_restores_the_pending_entry() {
    let fx = fixture();
    claimable_world(&fx).await;

    // Both decisions are outstanding when the courier drives, and the claim
    // exchange is lost in transit: the release reaches a task that has never
    // seen the claim and refuses it as unknown.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Release {
            operation_id: op("release-a"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 1,
        },
    )
    .await
    .expect("the release applies at the board");
    fx.task_transport
        .inject(rakka_agent::testkit::ExchangeFault::LoseEnvelope);
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");

    // The definitive refusal must not wedge the entry in Releasing: it
    // restores to pending, where the release can be retried and the lease
    // keeps the steal escape hatch open.
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(
        entry.status,
        AgentTeamBoardEntryStatus::Pending,
        "a release the task has not seen the claim for must not wedge the entry"
    );
    assert_eq!(
        entry.claim.as_ref().expect("the claim stands").member,
        member(MEMBER_A)
    );
    assert_eq!(entry.last_code.as_deref(), Some("team-release-unknown"));

    // The lost claim redelivers and the choreography converges normally.
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_task_at(&task_scope())
        .await
        .expect("task settles");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    assert_eq!(board_entry(&team).status, AgentTeamBoardEntryStatus::Active);
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment.expect("the assignment stands").status,
        AgentAssignmentStatus::Accepted
    );
}

#[tokio::test]
async fn pre_team_records_serialize_byte_identically() {
    let fx = fixture();
    fx.instantiate_agent().await;
    fx.create_task().await;

    // A task no board ever touched serializes without any team field, so
    // records persisted before this slice stay byte-identical.
    let snapshot = fx.task_snapshot().await;
    let serialized = serde_json::to_value(&snapshot).expect("the snapshot serializes");
    let object = serialized.as_object().expect("the snapshot is an object");
    assert!(!object.contains_key("team"));
    assert!(!object.contains_key("team_claim"));
    assert!(!object.contains_key("team_claims"));
    assert!(!object.contains_key("team_claim_fence"));

    // A pre-slice revision-only policy payload still decodes, with the
    // bounded defaults filled in.
    let policy: AgentTeamPolicy =
        serde_json::from_value(serde_json::json!({ "revision": 1 })).expect("the shell decodes");
    assert_eq!(policy.max_members, 8);
    assert_eq!(policy.claim_lease_ms, 300_000);
    assert!(policy.tool.is_none());
}

/// A re-posted task must not resurrect an evicted incarnation's claim
/// identity. The claim id hashes `(team, task, member, epoch)` and doubles
/// as the exchange's deduplication key, so an entry whose epoch restarted at
/// zero after Done-eviction would let a same-member re-claim re-derive the
/// exact operation the task's journal already answered — and the journal
/// replays the old definitive refusal instead of re-running arbitration,
/// closing the fresh entry over a task that is now genuinely claimable.
/// The board therefore seeds a re-posted entry above every epoch an evicted
/// entry ever reached.
#[tokio::test]
async fn a_repost_after_eviction_cannot_replay_a_prior_incarnations_refusal() {
    let fx = fixture();
    fx.instantiate_team_member_at(
        AgentScope::new(tenant(), member(MEMBER_A)).expect("the member scope is valid"),
    )
    .await;
    // A one-entry board makes eviction immediate: every post of new work
    // evicts whatever finished before it.
    let mut members: BTreeMap<AgentId, BTreeSet<rakka_agent::AgentCapabilityId>> = BTreeMap::new();
    members.insert(member(MEMBER_A), BTreeSet::new());
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Create {
            operation_id: op("create"),
            creation: Box::new(AgentTeamCreation {
                leader: member(LEADER),
                root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
                policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)
                    .with_max_board_entries(1),
                members,
            }),
        },
    )
    .await
    .expect("the team creates");

    // Incarnation one: the board task is claimed before its task entity
    // exists. The uncreated task refuses `team-claim-task-unknown`
    // definitively, the task's journal memoizes that answer under the
    // claim's derived operation id, and the entry closes Done at epoch 1.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-early"),
            task: task_scope().task().clone(),
            posted_by: member(MEMBER_A),
        },
    )
    .await
    .expect("the early post applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-early"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the early claim applies at the board");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Done);
    assert_eq!(entry.last_code.as_deref(), Some("team-claim-task-unknown"));

    // The task now really exists under this board's governance.
    create_board_task(&fx).await;

    // Unrelated finished work cycles the one slot, evicting the Done entry —
    // the incarnation boundary this test is about.
    let cycler = rakka_agent::AgentTaskId::new("cycler").expect("the task id is valid");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-cycler"),
            task: cycler.clone(),
            posted_by: member(MEMBER_A),
        },
    )
    .await
    .expect("the cycler post evicts the Done entry");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let cycler_entry = team
        .board
        .iter()
        .find(|entry| entry.task == cycler)
        .expect("the cycler entry stands");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-cycler"),
            task: cycler.clone(),
            member: member(MEMBER_A),
            expected_epoch: cycler_entry.claim_epoch,
        },
    )
    .await
    .expect("the cycler claim applies at the board");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");
    fx.settle_team_at(&team_scope())
        .await
        .expect("team settles");

    // Incarnation two: the re-post evicts the cycler and the re-claim must
    // arbitrate at the task — which now accepts — rather than being answered
    // by incarnation one's memoized refusal.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-again"),
            task: task_scope().task().clone(),
            posted_by: member(MEMBER_A),
        },
    )
    .await
    .expect("the re-post applies");
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Open);
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-again"),
            task: task_scope().task().clone(),
            member: member(MEMBER_A),
            expected_epoch: entry.claim_epoch,
        },
    )
    .await
    .expect("the re-claim applies at the board");
    settle_claim_round_trip(&fx).await;

    let task = fx.task_snapshot().await;
    let assignment = task.assignment.expect("the re-claim wins an assignment");
    assert_eq!(assignment.agent, member(MEMBER_A));
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = board_entry(&team);
    assert_eq!(
        entry.status,
        AgentTeamBoardEntryStatus::Active,
        "the fresh claim identity reached arbitration; a replayed refusal \
         would have closed the entry Done with the stale code, got {:?}",
        entry.last_code
    );
}
