//! The receiver halves of the cross-participant refusal classifiers
//! ([specification 9.5](../../../docs/plans/rakka-agent/spec.md)'s
//! convergence rule): exactly the refusals an exchange's initiator keeps
//! outstanding are the ones its receiver declines to memoize. A refusal
//! born of the receiver's *inability* — a payload this binary cannot decode
//! mid rolling upgrade — therefore re-runs on the next drive instead of
//! answering every re-drive from the journal for the whole applied window,
//! while a definitive answer still memoizes and absorbs duplicates past it.

mod common;

use common::{run_scope, task_scope, tenant, Fixture};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    handoff_id_for, handoff_result_operation_id, team_claim_id_for, team_claim_operation_id,
    team_claim_result_operation_id, AgentEntityAddress, AgentExchangeEnvelope, AgentExchangeKind,
    AgentExchangePayload, AgentHandoffResolutionNotice, AgentHandoffResultNotice, AgentId,
    AgentOperationId, AgentRevisionNumber, AgentTaskEntityStore, AgentTeamClaimAction,
    AgentTeamClaimCommand, AgentTeamClaimOutcome, AgentTeamClaimResultNotice, AgentTeamEntityStore,
    AgentTeamId, AgentTeamScope, AGENT_HANDOFF_RESULT_PAYLOAD_TYPE, AGENT_TEAM_CLAIM_PAYLOAD_TYPE,
    AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE,
};
use rakka_agent_workflow::{AgentCorrelationId, AgentTimestampMillis};

const TEAM: &str = "support-team";
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

fn member() -> AgentId {
    AgentId::new(MEMBER).expect("the member id is valid")
}

fn envelope(
    fx: &Fixture,
    operation: &AgentOperationId,
    kind: AgentExchangeKind,
    initiator: AgentEntityAddress,
    target: AgentEntityAddress,
    payload: AgentExchangePayload,
) -> AgentExchangeEnvelope {
    AgentExchangeEnvelope::new(
        operation.clone(),
        kind,
        initiator,
        target,
        payload,
        AgentCorrelationId::new(operation.as_str()),
        fx.now(),
    )
    .expect("the envelope is valid")
}

/// A handoff result the run's binary cannot decode is refused
/// `handoff-result-undecodable` and — the receiver half of the shared
/// classifier — never memoized: the re-driven operation re-runs the arm, so
/// a binary that can read the notice answers it definitively, and *that*
/// answer is what absorbs duplicates from the journal.
#[tokio::test]
async fn an_undecodable_handoff_result_is_not_memoized_by_the_run() {
    let fx = fixture();
    let handoff = handoff_id_for(&run_scope(), 1, 0).expect("the handoff id derives");
    let operation =
        handoff_result_operation_id(&tenant(), &handoff).expect("the operation id derives");
    let unreadable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::HandoffResult,
        AgentEntityAddress::Task(task_scope()),
        AgentEntityAddress::Run(run_scope()),
        AgentExchangePayload::empty(AGENT_HANDOFF_RESULT_PAYLOAD_TYPE),
    );

    let mut run = fx.run_at(&run_scope());
    let first = run
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert_eq!(
        first.result().status().rejection_code(),
        Some("handoff-result-undecodable"),
        "the inability refuses"
    );

    // The re-drive after the upgrade: the same operation, now readable. An
    // unmemoized inability re-runs the arm, which answers definitively —
    // this run was never assigned, so it holds no handoff.
    let readable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::HandoffResult,
        AgentEntityAddress::Task(task_scope()),
        AgentEntityAddress::Run(run_scope()),
        AgentExchangePayload::encode(
            AGENT_HANDOFF_RESULT_PAYLOAD_TYPE,
            &AgentHandoffResultNotice {
                task: task_scope(),
                handoff,
                resolution: AgentHandoffResolutionNotice::Refused {
                    code: "handoff-target-refused".to_string(),
                },
            },
        )
        .expect("the payload encodes"),
    );
    let second = run
        .accept(&readable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        !second.is_replayed(),
        "an inability must not answer the re-drive from the journal"
    );
    assert_eq!(
        second.result().status().rejection_code(),
        Some("handoff-not-held"),
        "the re-run arm answers definitively"
    );

    // The definitive answer memoizes: even a delivery this binary cannot
    // read is now answered from the journal, so duplicates stay absorbed
    // past the applied window's whole life.
    let third = run
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        third.is_replayed(),
        "a definitive refusal absorbs duplicates"
    );
    assert_eq!(
        third.result().status().rejection_code(),
        Some("handoff-not-held")
    );
}

/// A team-claim command the task's binary cannot decode is refused with the
/// decoding code and never memoized: the board's re-driven decision re-runs
/// the arm, so a binary that can read it reaches arbitration and answers a
/// code the board's classifier settles.
#[tokio::test]
async fn an_undecodable_team_claim_is_not_memoized_by_the_task() {
    let fx = fixture();
    let claim =
        team_claim_id_for(&team_scope(), task_scope().task(), &member(), 1).expect("the claim id");
    let operation = team_claim_operation_id(&tenant(), &claim).expect("the operation id derives");
    let unreadable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::TeamClaim,
        AgentEntityAddress::Team(team_scope()),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::empty(AGENT_TEAM_CLAIM_PAYLOAD_TYPE),
    );

    let mut task = AgentTaskEntityStore::new(
        task_scope(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    let first = task
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert_eq!(
        first.result().status().rejection_code(),
        Some("exchange-payload-decoding"),
        "the inability refuses"
    );

    // The re-drive, readable: arbitration runs and answers a definitive
    // code — no task exists under this scope.
    let readable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::TeamClaim,
        AgentEntityAddress::Team(team_scope()),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(
            AGENT_TEAM_CLAIM_PAYLOAD_TYPE,
            &AgentTeamClaimCommand {
                team: team_scope(),
                claim: claim.clone(),
                task: task_scope().task().clone(),
                epoch: 1,
                action: AgentTeamClaimAction::Claim { member: member() },
                policy_revision: AgentRevisionNumber::INITIAL,
                lease_expires_at: AgentTimestampMillis::new(60_000),
            },
        )
        .expect("the payload encodes"),
    );
    let second = task
        .accept(&readable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        !second.is_replayed(),
        "an inability must not answer the re-drive from the journal"
    );
    assert_eq!(
        second.result().status().rejection_code(),
        Some("team-claim-task-unknown"),
        "the re-run arm reaches arbitration"
    );

    // The definitive answer memoizes and absorbs duplicates.
    let third = task
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        third.is_replayed(),
        "a definitive refusal absorbs duplicates"
    );
    assert_eq!(
        third.result().status().rejection_code(),
        Some("team-claim-task-unknown")
    );
}

/// A claim-result notice the team's binary cannot decode is refused with
/// the decoding code and never memoized: the task's re-driven owed result
/// re-runs the arm, so a binary that can read it answers a code the task's
/// classifier settles — and the exchange converges instead of the task
/// re-driving forever against a stale journal echo.
#[tokio::test]
async fn an_undecodable_team_claim_result_is_not_memoized_by_the_team() {
    let fx = fixture();
    let claim =
        team_claim_id_for(&team_scope(), task_scope().task(), &member(), 1).expect("the claim id");
    let operation =
        team_claim_result_operation_id(&tenant(), &claim).expect("the operation id derives");
    let unreadable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::TeamClaimResult,
        AgentEntityAddress::Task(task_scope()),
        AgentEntityAddress::Team(team_scope()),
        AgentExchangePayload::empty(AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE),
    );

    let mut team =
        AgentTeamEntityStore::new(team_scope(), fx.teams.clone(), fx.team_history.clone());
    let first = team
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert_eq!(
        first.result().status().rejection_code(),
        Some("exchange-payload-decoding"),
        "the inability refuses"
    );

    // The re-drive, readable: the arm answers definitively — no team
    // exists under this scope.
    let readable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::TeamClaimResult,
        AgentEntityAddress::Task(task_scope()),
        AgentEntityAddress::Team(team_scope()),
        AgentExchangePayload::encode(
            AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE,
            &AgentTeamClaimResultNotice {
                task: task_scope(),
                claim,
                epoch: 1,
                outcome: AgentTeamClaimOutcome::Refused {
                    code: "team-claim-task-unknown".to_string(),
                },
            },
        )
        .expect("the payload encodes"),
    );
    let second = team
        .accept(&readable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        !second.is_replayed(),
        "an inability must not answer the re-drive from the journal"
    );
    assert_eq!(
        second.result().status().rejection_code(),
        Some("team-not-found"),
        "the re-run arm answers definitively"
    );

    // The definitive answer memoizes and absorbs duplicates.
    let third = team
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        third.is_replayed(),
        "a definitive refusal absorbs duplicates"
    );
    assert_eq!(
        third.result().status().rejection_code(),
        Some("team-not-found")
    );
}
