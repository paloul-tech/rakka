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
    AgentTeamId, AgentTeamScope, AGENT_BUDGET_RETURN_PAYLOAD_TYPE,
    AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE, AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE,
    AGENT_DELEGATION_RESULT_PAYLOAD_TYPE, AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN,
    AGENT_HANDOFF_RESULT_PAYLOAD_TYPE, AGENT_RUN_CANCEL_PAYLOAD_TYPE,
    AGENT_TEAM_CLAIM_PAYLOAD_TYPE, AGENT_TEAM_CLAIM_RESULT_PAYLOAD_TYPE,
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

/// Every cross-participant exchange classifies one refusal code identically
/// at both ends.
///
/// The five kinds below are the ones whose two ends are *different*
/// participants, so neither inherits the other's arm — the task↔task kinds
/// share one `check_settle` by construction. Each pair consults one shared
/// classifier, which is what makes the property hold by construction rather
/// than by two lists agreeing today: exactly the refusals the initiator
/// keeps outstanding are the ones the receiver declines to memoize.
///
/// The `delegation-result-early` row is the in-version wedge — no rolling
/// upgrade needed. A child that terminalizes before its parent's own send
/// receipt settles the delegation cell is refused *because the parent is not
/// ready yet*; memoizing that at the run answered every later re-drive from
/// the journal while the child's initiator arm kept the exchange outstanding,
/// so the parent's fan-in member — and the goal behind it — waited forever.
#[test]
fn every_cross_participant_kind_classifies_alike_at_both_ends() {
    use rakka_agent::{
        AgentExchangeParticipant, AgentExchangeResult, AgentRunParticipant, AgentTaskParticipant,
    };

    // (kind, payload type, initiator address, receiver address, whether the
    // task participant is the initiator, and the codes with their verdicts).
    let cases: Vec<(
        AgentExchangeKind,
        &str,
        AgentEntityAddress,
        AgentEntityAddress,
        bool,
        Vec<(&str, bool)>,
    )> = vec![
        (
            AgentExchangeKind::DelegationResult,
            AGENT_DELEGATION_RESULT_PAYLOAD_TYPE,
            AgentEntityAddress::Task(task_scope()),
            AgentEntityAddress::Run(run_scope()),
            true,
            vec![
                ("delegation-result-unknown-run", true),
                ("delegation-result-unknown-delegation", true),
                ("delegation-result-forged", true),
                ("delegation-result-not-owned", true),
                ("delegation-result-early", false),
                ("delegation-result-undecodable", false),
                ("unsupported-exchange", false),
            ],
        ),
        (
            AgentExchangeKind::RunCancel,
            AGENT_RUN_CANCEL_PAYLOAD_TYPE,
            AgentEntityAddress::Task(task_scope()),
            AgentEntityAddress::Run(run_scope()),
            true,
            vec![
                ("run-cancel-forged", true),
                ("run-cancel-unassigned", true),
                ("run-cancel-stale-generation", true),
                ("run-cancel-undecodable", false),
                ("run-cancel-failed", false),
                ("unsupported-exchange", false),
            ],
        ),
        (
            AgentExchangeKind::DelegationCancel,
            AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE,
            AgentEntityAddress::Run(run_scope()),
            AgentEntityAddress::Task(task_scope()),
            false,
            vec![
                ("delegation-cancel-forged", true),
                ("delegation-cancel-not-delegated", true),
                ("delegation-cancel-undecodable", false),
                ("unsupported-exchange", false),
            ],
        ),
        (
            AgentExchangeKind::BudgetSettlement,
            AGENT_BUDGET_SETTLEMENT_PAYLOAD_TYPE,
            AgentEntityAddress::Run(run_scope()),
            AgentEntityAddress::Task(task_scope()),
            false,
            vec![
                (AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN, true),
                ("task-state-too-large", false),
                ("exchange-payload-decoding", false),
                ("unsupported-exchange", false),
            ],
        ),
        (
            AgentExchangeKind::BudgetReturn,
            AGENT_BUDGET_RETURN_PAYLOAD_TYPE,
            AgentEntityAddress::Run(run_scope()),
            AgentEntityAddress::Task(task_scope()),
            false,
            vec![
                (AGENT_ESCROW_REFUSAL_CHILD_UNKNOWN, true),
                ("task-state-too-large", false),
                ("exchange-payload-decoding", false),
                ("unsupported-exchange", false),
            ],
        ),
    ];

    let fx = fixture();
    let task = AgentTaskParticipant;
    let run = AgentRunParticipant;
    for (kind, payload_type, initiator, receiver, task_initiates, codes) in cases {
        let operation = AgentOperationId::new(
            rakka_agent::AgentOperationKind::Command,
            [tenant().as_str(), &format!("{kind}")],
        )
        .expect("the operation id derives");
        let carrier = envelope(
            &fx,
            &operation,
            kind,
            initiator,
            receiver,
            AgentExchangePayload::empty(payload_type),
        );
        for (code, settles) in codes {
            let refusal = AgentExchangeResult::rejected(
                code,
                "refused",
                AgentExchangePayload::empty(payload_type),
            );
            let by_task = task.check_settle(&carrier, &refusal).is_ok();
            let by_run = run.check_settle(&carrier, &refusal).is_ok();
            let (as_initiator, as_receiver) = if task_initiates {
                (by_task, by_run)
            } else {
                (by_run, by_task)
            };
            assert_eq!(
                as_initiator, settles,
                "{kind}/{code}: the initiator's verdict"
            );
            assert_eq!(
                as_receiver, settles,
                "{kind}/{code}: the receiver must classify exactly as the initiator does, or \
                 the host memoizes a refusal the initiator keeps outstanding and the exchange \
                 can never converge"
            );
        }
    }
}

/// A delegation result the run's binary cannot decode is refused and never
/// memoized: the child's re-driven report re-runs the arm, so a binary that
/// can read it answers definitively — and *that* answer absorbs duplicates.
#[tokio::test]
async fn an_undecodable_delegation_result_is_not_memoized_by_the_run() {
    let fx = fixture();
    let delegation = rakka_agent::delegation_id_for(&run_scope(), 1, 0).expect("the id derives");
    let operation = rakka_agent::delegation_result_operation_id(&tenant(), &delegation)
        .expect("the operation id derives");
    let child = rakka_agent::AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new("child-1").expect("the task id is valid"),
    )
    .expect("the child scope is valid");
    let unreadable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::DelegationResult,
        AgentEntityAddress::Task(child.clone()),
        AgentEntityAddress::Run(run_scope()),
        AgentExchangePayload::empty(AGENT_DELEGATION_RESULT_PAYLOAD_TYPE),
    );

    let mut run = fx.run_at(&run_scope());
    let first = run
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert_eq!(
        first.result().status().rejection_code(),
        Some("delegation-result-undecodable"),
        "the inability refuses"
    );

    // The re-drive after the upgrade: readable, and the arm answers
    // definitively — this run was never assigned, so it owns no delegations.
    let readable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::DelegationResult,
        AgentEntityAddress::Task(child.clone()),
        AgentEntityAddress::Run(run_scope()),
        AgentExchangePayload::encode(
            AGENT_DELEGATION_RESULT_PAYLOAD_TYPE,
            &rakka_agent::AgentDelegationReport {
                delegation: delegation.clone(),
                child_task: child.task().clone(),
                child_run: None,
                status: rakka_agent::AgentTaskStatus::Completed,
                terminal_reason: None,
                result_digest: None,
                descendants_created: 0,
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
        Some("delegation-result-unknown-run"),
        "the re-run arm answers definitively"
    );

    let third = run
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        third.is_replayed(),
        "a definitive refusal absorbs duplicates"
    );
}

/// A delegation-cancel the child task's binary cannot decode is refused and
/// never memoized, so the parent's subtree cancellation converges on the
/// re-drive instead of quiescing against a child that never learned it was
/// cancelled.
#[tokio::test]
async fn an_undecodable_delegation_cancel_is_not_memoized_by_the_task() {
    let fx = fixture();
    let delegation = rakka_agent::delegation_id_for(&run_scope(), 1, 0).expect("the id derives");
    let operation = rakka_agent::delegation_cancel_operation_id(&tenant(), &delegation)
        .expect("the operation id derives");
    let unreadable = envelope(
        &fx,
        &operation,
        AgentExchangeKind::DelegationCancel,
        AgentEntityAddress::Run(run_scope()),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::empty(AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE),
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
        Some("delegation-cancel-undecodable"),
        "the inability refuses"
    );
    let second = task
        .accept(&unreadable, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    assert!(
        !second.is_replayed(),
        "an inability must not answer the re-drive from the journal"
    );
    assert_eq!(
        second.result().status().rejection_code(),
        Some("delegation-cancel-undecodable"),
        "the arm re-runs and answers for itself"
    );
}

/// A delivery that changes nothing costs no durable revision.
///
/// The receiver's half of the convergence rule deliberately leaves an
/// inability unmemoized, so the courier re-drives it on every settle pass for
/// as long as the exchange stands. Persisting the byte-identical state each
/// time would bill that standing exchange one revision per pass for the life
/// of both entities — and every spurious bump would conflict the entity's
/// other writer in the documented two-writer topology. The canonical repro is
/// the documented `Blocked` posture: a dependent registering against an
/// upstream that does not exist yet.
#[tokio::test]
async fn a_delivery_that_changes_nothing_burns_no_revision() {
    use rakka_persistence::DurableStateStore;

    let fx = fixture();
    let upstream = rakka_agent::AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new("never-created").expect("the task id is valid"),
    )
    .expect("the upstream scope is valid");
    let operation = rakka_agent::dependency_registration_operation_id(
        &tenant(),
        upstream.task(),
        task_scope().task(),
    )
    .expect("the operation id derives");
    let registration = envelope(
        &fx,
        &operation,
        AgentExchangeKind::DependencyRegistration,
        AgentEntityAddress::Task(task_scope()),
        AgentEntityAddress::Task(upstream.clone()),
        AgentExchangePayload::encode(
            rakka_agent::AGENT_DEPENDENCY_REGISTRATION_PAYLOAD_TYPE,
            &rakka_agent::AgentDependencyRegistration {
                dependent: task_scope(),
                upstream: upstream.task().clone(),
                policy: rakka_agent::AgentDependencyFailurePolicy::CancelDependents,
            },
        )
        .expect("the payload encodes"),
    );

    let revision = || async {
        DurableStateStore::load(&fx.tasks, &upstream.persistence_id())
            .await
            .expect("the record loads")
            .map(|record| record.revision)
    };

    let mut receiver = AgentTaskEntityStore::new(
        upstream.clone(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    for pass in 0..4 {
        let reply = receiver
            .accept(&registration, &fx.router, fx.now())
            .await
            .expect("the delivery succeeds");
        assert_eq!(
            reply.result().status().rejection_code(),
            Some("task-not-created"),
            "pass {pass}: the upstream cannot answer yet"
        );
        assert!(
            !reply.is_replayed(),
            "pass {pass}: the inability stays unmemoized, so the arm re-runs"
        );
        assert_eq!(
            revision().await,
            None,
            "pass {pass}: a refusal that recorded nothing must not write — least of all \
             materialize a durable record for a task that does not exist"
        );
    }
}
