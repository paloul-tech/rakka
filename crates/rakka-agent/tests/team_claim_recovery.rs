//! Crash-point sweeps over the claim choreography
//! ([specification 8.10 and 9.8](../../../docs/plans/rakka-agent/spec.md),
//! scenario 42's fault half): owner loss at every durable write of the team
//! and task stores converges on one claim, one assignment generation, one
//! owner — never two, never none stranded.
//!
//! Each iteration builds a fresh world, arms exactly one store at one write,
//! drives to the loss, survives, and re-drives the same operation ids: the
//! deduplicated command inbox, the journal's re-drive, and the claim
//! provenance's echo are what make every window converge.

mod common;

use common::{task_scope, tenant, Fixture, TENANT};
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentAssignmentGeneration, AgentAssignmentStatus, AgentGoalId, AgentId, AgentOperationId,
    AgentOperationKind, AgentRevisionNumber, AgentScope, AgentTaskContent, AgentTaskCreation,
    AgentTaskEntityCommand, AgentTaskTeamClaimStatus, AgentTeamBoardEntryStatus, AgentTeamCreation,
    AgentTeamEntityCommand, AgentTeamId, AgentTeamPolicy, AgentTeamScope,
};
use std::collections::{BTreeMap, BTreeSet};

const TEAM: &str = "support-team";
const MEMBER: &str = "worker-a";

fn team_scope() -> AgentTeamScope {
    AgentTeamScope::new(
        tenant(),
        AgentTeamId::new(TEAM).expect("the team id is valid"),
    )
    .expect("the team scope is valid")
}

fn member() -> AgentId {
    AgentId::new(MEMBER).expect("the member id is valid")
}

fn op(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::TeamClaim, [TENANT, TEAM, discriminator])
        .expect("the operation id derives")
}

fn claim_command() -> AgentTeamEntityCommand {
    AgentTeamEntityCommand::Claim {
        operation_id: op("claim"),
        task: task_scope().task().clone(),
        member: member(),
        expected_epoch: 0,
    }
}

/// Builds the claimable world: an instantiated member, a created team, and
/// the posted board task.
async fn world() -> Fixture {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.instantiate_team_member_at(
        AgentScope::new(tenant(), member()).expect("the member scope is valid"),
    )
    .await;
    let mut members: BTreeMap<AgentId, BTreeSet<rakka_agent::AgentCapabilityId>> = BTreeMap::new();
    members.insert(member(), BTreeSet::new());
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Create {
            operation_id: op("create"),
            creation: Box::new(AgentTeamCreation {
                leader: member(),
                root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
                policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
                members,
            }),
        },
    )
    .await
    .expect("the team creates");
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
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post"),
            task: task_scope().task().clone(),
            posted_by: member(),
        },
    )
    .await
    .expect("the post applies");
    fx
}

/// Drives the claim and every settle pass the round trip needs, tolerating
/// injected crashes: a crashed pass is exactly an owner death mid-flow.
async fn drive_claim(fx: &Fixture) {
    let _ = fx
        .apply_team_command_at(&team_scope(), claim_command())
        .await;
    for _round in 0..4 {
        let _ = fx.settle_team_at(&team_scope()).await;
        let _ = fx.settle_task_at(&task_scope()).await;
    }
}

/// Re-drives after survival until quiescent, then asserts the converged
/// truth: one recorded claim, one accepted generation, an active board
/// entry mirroring it.
async fn assert_converged(fx: &Fixture) {
    // The retried command either applies fresh (the crash preceded its
    // commit) or answers from the operation log — both converge.
    let _ = fx
        .apply_team_command_at(&team_scope(), claim_command())
        .await;
    for _round in 0..6 {
        let _ = fx.settle_team_at(&team_scope()).await;
        let _ = fx.settle_task_at(&task_scope()).await;
    }

    let task = fx.task_snapshot().await;
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1),
        "exactly one generation minted across the loss"
    );
    assert_eq!(task.team_claims, 1, "exactly one claim recorded");
    let assignment = task.assignment.expect("the assignment stands");
    assert_eq!(assignment.agent, member());
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    let claim = task.team_claim.expect("the claim provenance stands");
    assert_eq!(claim.status, AgentTaskTeamClaimStatus::Accepted);
    assert!(claim.result_settled);

    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = team
        .board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .expect("the board holds the task");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    assert_eq!(
        entry.claim.as_ref().expect("the echo stands").member,
        member()
    );
}

/// Counts the durable writes one crash-free claim round trip attempts on
/// each store, so the sweeps below cover every real write and know when
/// they have run past the flow's end.
async fn reference_writes() -> (usize, usize) {
    let fx = world().await;
    fx.teams.reset_writes();
    fx.tasks.reset_writes();
    drive_claim(&fx).await;
    (fx.teams.writes(), fx.tasks.writes())
}

#[tokio::test]
async fn the_claim_converges_across_every_team_store_crash_point() {
    let (team_writes, _) = reference_writes().await;
    assert!(
        team_writes >= 2,
        "the claim flow writes the team store at least twice (claim commit, result settle)"
    );
    for point in 1..=team_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world().await;
            fx.teams.reset_writes();
            fx.teams.crash_at(point, window);
            drive_claim(&fx).await;
            fx.teams.assert_crash_fired(point, window);
            fx.teams.survive();
            assert_converged(&fx).await;
        }
    }
}

#[tokio::test]
async fn the_claim_converges_across_every_task_store_crash_point() {
    let (_, task_writes) = reference_writes().await;
    assert!(
        task_writes >= 2,
        "the claim flow writes the task store at least twice (claim apply, acceptance settle)"
    );
    for point in 1..=task_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world().await;
            fx.tasks.reset_writes();
            fx.tasks.crash_at(point, window);
            drive_claim(&fx).await;
            fx.tasks.assert_crash_fired(point, window);
            fx.tasks.survive();
            assert_converged(&fx).await;
        }
    }
}

#[tokio::test]
async fn a_loss_in_the_committed_but_unsent_window_re_drives_the_same_claim() {
    // The window the journal exists for: the board decision committed —
    // entry pending, exchange owed — and the owner died before anything was
    // sent. Nothing but recovery may deliver it, under the same operation
    // id.
    let fx = world().await;
    fx.apply_team_command_at(&team_scope(), claim_command())
        .await
        .expect("the claim commits");

    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = team
        .board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .expect("the board holds the task");
    assert_eq!(
        entry.status,
        AgentTeamBoardEntryStatus::Pending,
        "the decision committed durably before any delivery"
    );
    let task = fx.task_snapshot().await;
    assert_eq!(task.team_claims, 0, "nothing reached the task yet");

    // Every driver from here is a restart: recovery finds the owed exchange
    // and the round trip converges.
    assert_converged(&fx).await;
}

#[tokio::test]
async fn a_wire_claim_committed_past_a_resident_board_is_driven_by_its_next_settle_sweep() {
    // A board has two writers by construction — the resident sharded entity,
    // which holds one store for its whole residency, and the A2A service,
    // which builds its own store on any node and is how every wire claim
    // arrives. The service normally couriers its own write; if it dies
    // first, the durable-outbox guarantee falls to the board's settle
    // sweeps. A resident that decides it owes nothing from a stale cache
    // performs zero writes, never conflicts, and so never re-reads — before
    // the settle pass re-materialized, this sweep no-opped forever on an
    // otherwise idle board and the claim's owed decision stalled until an
    // unrelated command happened to lose a compare-and-set.
    let fx = world().await;

    // The resident board materializes and goes idle holding a clean cache.
    let mut resident = rakka_agent::AgentTeamEntityStore::new(
        team_scope(),
        fx.teams.clone(),
        fx.team_history.clone(),
    );
    resident
        .recover(fx.now())
        .await
        .expect("the resident loads");

    // The other writer — the A2A service's own store handle — commits the
    // claim and dies before driving its courier.
    let reply = fx
        .apply_team_command_at(&team_scope(), claim_command())
        .await
        .expect("the wire claim commits");
    assert!(matches!(
        reply,
        rakka_agent::AgentTeamEntityReply::Applied { .. }
    ));

    // The resident's own settle sweep — no passivation, no lost race, no
    // mutating command in between — must observe the wire-committed claim
    // and drive its owed decision exchange to the task.
    let progress = resident
        .settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the resident sweep settles");
    assert!(
        progress.settled >= 1,
        "the sweep drove the claim its cache never saw: {progress:?}"
    );

    // The same resident keeps sweeping the round trip to convergence — the
    // claim result the task owes back lands on it the same way.
    for _round in 0..6 {
        let _ = fx.settle_task_at(&task_scope()).await;
        let _ = resident.settle_side_effects(&fx.router, fx.now()).await;
    }
    assert_converged(&fx).await;
}
