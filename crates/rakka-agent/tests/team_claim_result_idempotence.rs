//! A redelivered claim activation is absorbed past the applied window.
//!
//! The board's applied journal absorbs a redelivered `TeamClaimResult`
//! inside its bounded window; past eviction, the durable proof the
//! settlement applied is the entry's filled activation echo — the
//! `apply_team_terminal` Done-echo precedent. Without that guard the
//! re-driven notice re-runs the activation arm and appends a second
//! `ClaimSettled` history row for the same operation id, corrupting the
//! replayable history log ([specification 17.13](../../docs/plans/rakka-agent/spec.md):
//! the log is a compatibility surface, one row per durable transition).

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{tenant, Fixture};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    run_id_for_assignment, team_claim_result_operation_id, AgentAssignmentGeneration,
    AgentCapabilityId, AgentEntityAddress, AgentExchangeEnvelope, AgentExchangeKind,
    AgentExchangePayload, AgentGoalId, AgentId, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentTaskId, AgentTaskScope, AgentTaskStatus, AgentTeamBoardEntryStatus,
    AgentTeamClaimOutcome, AgentTeamClaimResultNotice, AgentTeamCreation, AgentTeamEntityCommand,
    AgentTeamEntityReply, AgentTeamEntityStore, AgentTeamHistoryCursor, AgentTeamHistoryKind,
    AgentTeamHistoryStore as _, AgentTeamId, AgentTeamPolicy, AgentTeamScope,
    AgentTeamTerminalNotice, AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE,
    AGENT_TEAM_TERMINAL_NOTICE_PAYLOAD_TYPE,
};
use rakka_agent_workflow::AgentCorrelationId;

const TEAM: &str = "support-team";
const MEMBER: &str = "worker-a";
const LEADER: &str = "team-leader";

fn task_scope_for(task: &AgentTaskId) -> AgentTaskScope {
    AgentTaskScope::new(tenant(), task.clone()).expect("the task scope is valid")
}

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

fn member_id(name: &str) -> AgentId {
    AgentId::new(name).expect("the member id is valid")
}

fn op(discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(
        AgentOperationKind::TeamOperation,
        [tenant().as_str(), TEAM, discriminator],
    )
    .expect("the operation id derives")
}

async fn count_claim_settled(fx: &Fixture) -> usize {
    let mut count = 0;
    let mut cursor = AgentTeamHistoryCursor::start();
    loop {
        let page = fx
            .team_history
            .read(&team_scope(), cursor)
            .await
            .expect("the history reads");
        count += page
            .entries
            .iter()
            .filter(|entry| entry.kind == AgentTeamHistoryKind::ClaimSettled)
            .count();
        match page.next {
            Some(next) => cursor = next,
            None => break,
        }
    }
    count
}

/// The task's `Activated` notice applies once; its redelivery after the
/// applied window evicted the operation is absorbed by the entry's filled
/// echo — one `ClaimSettled` row, the entry's shape untouched.
#[tokio::test]
async fn a_redelivered_activation_records_no_second_settlement() {
    let fx = fixture();
    let mut members: BTreeMap<AgentId, BTreeSet<AgentCapabilityId>> = BTreeMap::new();
    members.insert(member_id(MEMBER), BTreeSet::new());
    let reply = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Create {
                operation_id: op("create"),
                creation: Box::new(AgentTeamCreation {
                    leader: member_id(LEADER),
                    root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
                    policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
                    members,
                }),
            },
        )
        .await
        .expect("the team creates");
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));

    let task = AgentTaskId::new("board-1").expect("the task id is valid");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post"),
            task: task.clone(),
            posted_by: member_id(MEMBER),
        },
    )
    .await
    .expect("the post applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim"),
            task: task.clone(),
            member: member_id(MEMBER),
            expected_epoch: 0,
        },
    )
    .await
    .expect("the claim applies at the board");

    // The claim the board minted, read from durable state so the notice
    // names exactly the entry's current claim.
    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = snapshot
        .board
        .iter()
        .find(|entry| entry.task == task)
        .expect("the entry stands");
    let claim = entry
        .claim
        .as_ref()
        .expect("the claim is pending")
        .claim
        .clone();

    // The task's activation notice, exactly as its settle pass would send it.
    let operation =
        team_claim_result_operation_id(&tenant(), &claim).expect("the operation id derives");
    let generation = AgentAssignmentGeneration::new(1);
    let notice = AgentExchangeEnvelope::new(
        operation.clone(),
        AgentExchangeKind::TeamClaimResult,
        AgentEntityAddress::Task(task_scope_for(&task)),
        AgentEntityAddress::Team(team_scope()),
        AgentExchangePayload::encode(
            AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE,
            &AgentTeamClaimResultNotice {
                task: task_scope_for(&task),
                claim: claim.clone(),
                epoch: 1,
                outcome: AgentTeamClaimOutcome::Activated {
                    generation,
                    run: run_id_for_assignment(&task, generation).expect("the run id derives"),
                    member: member_id(MEMBER),
                },
            },
        )
        .expect("the payload encodes"),
        AgentCorrelationId::new(operation.as_str()),
        fx.now(),
    )
    .expect("the envelope is valid");

    let mut team =
        AgentTeamEntityStore::new(team_scope(), fx.teams.clone(), fx.team_history.clone());
    let first = team
        .accept(&notice, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(first.result().is_accepted(), "the activation applies");
    assert_eq!(count_claim_settled(&fx).await, 1);

    // Evict the operation from the applied journal with unrelated exchange
    // traffic: terminal notices for tasks never posted here are absorbed
    // idempotently, and each delivery journals its own operation.
    for index in 0..66 {
        let phantom = AgentTaskId::new(format!("phantom-{index}")).expect("the task id is valid");
        let filler_op = op(&format!("terminal-filler-{index}"));
        let filler = AgentExchangeEnvelope::new(
            filler_op.clone(),
            AgentExchangeKind::TeamTerminalNotice,
            AgentEntityAddress::Task(task_scope_for(&phantom)),
            AgentEntityAddress::Team(team_scope()),
            AgentExchangePayload::encode(
                AGENT_TEAM_TERMINAL_NOTICE_PAYLOAD_TYPE,
                &AgentTeamTerminalNotice {
                    task: task_scope_for(&phantom),
                    status: AgentTaskStatus::Completed,
                    terminal_reason: "result-accepted".to_string(),
                },
            )
            .expect("the payload encodes"),
            AgentCorrelationId::new(filler_op.as_str()),
            fx.now(),
        )
        .expect("the envelope is valid");
        team.accept(&filler, &fx.router, fx.now())
            .await
            .expect("the filler delivery succeeds");
    }

    // The redelivery past the window: the journal no longer answers it, so
    // the arm runs again — and the filled echo absorbs it.
    let second = team
        .accept(&notice, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        !second.is_replayed(),
        "the applied window evicted the operation, so this is a re-run, not an echo"
    );
    assert!(second.result().is_accepted());
    assert_eq!(
        count_claim_settled(&fx).await,
        1,
        "one settlement row per operation, however often the notice re-drives"
    );

    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = snapshot
        .board
        .iter()
        .find(|entry| entry.task == task)
        .expect("the entry stands");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    let board_claim = entry.claim.as_ref().expect("the claim survives");
    assert_eq!(board_claim.generation_echo, Some(generation));
}
