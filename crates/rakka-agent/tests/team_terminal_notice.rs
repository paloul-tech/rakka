//! The task → team terminal notice
//! ([specification 8.10 and 9.8](../../../docs/plans/rakka-agent/spec.md)):
//! a terminal board-governed task closes its board entry eagerly, without a
//! member's claim attempt — the lazy close this exchange replaces — and the
//! close's epoch bump absorbs every stale board decision still in flight.
//!
//! The sweeps copy the claim-recovery discipline: each iteration builds a
//! fresh world, arms exactly one store at one write, drives to the loss,
//! survives, and re-drives the same operation ids.

mod common;

use common::{task_scope, tenant, Fixture, TENANT};
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentGoalId, AgentId, AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentScope,
    AgentTaskContent, AgentTaskCreation, AgentTaskEntityCommand, AgentTaskStatus,
    AgentTeamBoardEntryStatus, AgentTeamCreation, AgentTeamEntityCommand, AgentTeamHistoryKind,
    AgentTeamId, AgentTeamPolicy, AgentTeamScope,
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

fn cancel_command() -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Cancel {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Cancellation,
            [TENANT, task_scope().task().as_str(), "operator"],
        )
        .expect("the operation id derives"),
        reason: "no longer needed".to_string(),
    }
}

/// Builds the world: an instantiated member, a created team, and a created
/// board task — posted onto the board unless the test wants it unposted.
async fn world(post: bool) -> Fixture {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.instantiate_agent_at(
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
    if post {
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
    }
    fx
}

/// Drives both entities and the claimant's run until the flow at hand
/// rests, tolerating injected crashes: a crashed pass is exactly an owner
/// death mid-flow.
async fn drive(fx: &Fixture) {
    for _round in 0..6 {
        let _ = fx.settle_task_at(&task_scope()).await;
        let run_id = rakka_agent::run_id_for_assignment(
            task_scope().task(),
            rakka_agent::AgentAssignmentGeneration::new(1),
        )
        .expect("the run id derives");
        if let Ok(scope) = rakka_agent::AgentRunScope::new(tenant(), member(), run_id) {
            let mut run = fx.run_at(&scope);
            if run.recover(fx.now()).await.is_ok() {
                let _ = run.settle_side_effects(&fx.router, fx.now()).await;
                let _ = fx.dispatcher.drive(&mut run, &fx.router, fx.now()).await;
                let _ = run.settle_side_effects(&fx.router, fx.now()).await;
            }
        }
        let _ = fx.settle_team_at(&team_scope()).await;
    }
}

/// Drives the claim round trip to its activated rest state.
async fn activate_claim(fx: &Fixture) {
    let _ = fx
        .apply_team_command_at(&team_scope(), claim_command())
        .await;
    drive(fx).await;
}

/// The board entry for the world's one task.
async fn entry(fx: &Fixture) -> Option<rakka_agent::AgentTeamBoardEntry> {
    let team = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    team.board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .cloned()
}

/// How many team-history rows of one kind the sink holds.
async fn team_history_count(fx: &Fixture, kind: AgentTeamHistoryKind) -> usize {
    let mut count = 0;
    let mut cursor = Some(rakka_agent::AgentTeamHistoryCursor::start());
    while let Some(position) = cursor {
        let page =
            rakka_agent::AgentTeamHistoryStore::read(&fx.team_history, &team_scope(), position)
                .await
                .expect("the team history reads");
        count += page
            .entries
            .iter()
            .filter(|entry| entry.kind == kind)
            .count();
        cursor = page.next;
    }
    count
}

/// Re-drives after survival until quiescent, then asserts the converged
/// truth of the terminal flow over an activated claim: the entry closed
/// once, under a bumped epoch, with the notice marker settled.
async fn assert_converged(fx: &Fixture) {
    // The retried command either applies fresh (the crash preceded its
    // commit) or answers from the operation log — both converge.
    let _ = fx
        .apply_task_command_at(&task_scope(), cancel_command())
        .await;
    drive(fx).await;
    drive(fx).await;

    let task = fx.task_snapshot().await;
    assert!(task.status.is_terminal(), "the task terminalized");
    assert!(
        task.team_terminal_notice_settled,
        "the notice marker settled"
    );

    let entry = entry(fx).await.expect("the board holds the entry");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Done);
    assert!(entry.claim.is_none(), "the close cleared any claim");
    assert_eq!(
        entry.last_code.as_deref(),
        Some("cancellation-requested"),
        "the entry echoes the task's terminal-reason code"
    );
    assert_eq!(
        team_history_count(fx, AgentTeamHistoryKind::TaskClosed).await,
        1,
        "exactly one close recorded across the loss"
    );
}

#[tokio::test]
async fn a_terminal_task_closes_its_active_board_entry_without_a_claim_attempt() {
    // The headline: the entry is Active — a member owns the task — and the
    // task ends. Before this exchange the entry stayed live-looking until
    // some member's claim attempt was refused; now the terminal transition
    // itself closes it.
    let fx = world(true).await;
    activate_claim(&fx).await;
    let before = entry(&fx).await.expect("the board holds the entry");
    assert_eq!(before.status, AgentTeamBoardEntryStatus::Active);
    let epoch_before = before.claim_epoch;

    fx.apply_task_command_at(&task_scope(), cancel_command())
        .await
        .expect("the cancel applies");
    drive(&fx).await;

    let after = entry(&fx).await.expect("the board holds the entry");
    assert_eq!(after.status, AgentTeamBoardEntryStatus::Done);
    assert!(after.claim.is_none());
    assert_eq!(
        after.last_code.as_deref(),
        Some("cancellation-requested"),
        "the close carries the terminal reason, not a claim-refusal code"
    );
    assert!(
        after.claim_epoch > epoch_before,
        "the close is a board decision: it bumps the entry's claim epoch"
    );
    let task = fx.task_snapshot().await;
    assert!(task.status.is_terminal());
    assert!(task.team_terminal_notice_settled);
    assert_eq!(
        team_history_count(&fx, AgentTeamHistoryKind::TaskClosed).await,
        1
    );
}

#[tokio::test]
async fn an_unclaimed_board_entry_closes_when_its_task_ends() {
    // No claim ever existed — the purest form of "without a claim attempt".
    let fx = world(true).await;
    fx.apply_task_command_at(&task_scope(), cancel_command())
        .await
        .expect("the cancel applies");
    drive(&fx).await;

    let after = entry(&fx).await.expect("the board holds the entry");
    assert_eq!(after.status, AgentTeamBoardEntryStatus::Done);
    assert!(after.claim.is_none());
    assert!(fx.task_snapshot().await.team_terminal_notice_settled);
}

#[tokio::test]
async fn a_notice_for_a_task_never_posted_settles_without_a_board_write() {
    // The task carries team provenance but was never posted: the notice is
    // still owed — the task cannot know — and the board answers it
    // idempotently with nothing to close.
    let fx = world(false).await;
    fx.apply_task_command_at(&task_scope(), cancel_command())
        .await
        .expect("the cancel applies");
    drive(&fx).await;

    assert!(entry(&fx).await.is_none(), "nothing was ever posted");
    assert!(
        fx.task_snapshot().await.team_terminal_notice_settled,
        "the accepted no-op settles the marker"
    );
    assert_eq!(
        team_history_count(&fx, AgentTeamHistoryKind::TaskClosed).await,
        0,
        "an idempotent no-op records no close"
    );
}

#[tokio::test]
async fn a_stale_release_reply_after_the_eager_close_is_absorbed() {
    // The closed-entry regression pin. `settle_claim_action`'s
    // `(Release, "team-claim-already-owned")` arm restores an entry Active,
    // so a reply arriving after the eager close would resurrect a `Done`
    // entry as a live-looking one — permanently, since claim, release, and
    // transfer all refuse a closed entry and the terminal notice is owed
    // once. Two guards cover it now: `Done` is absorbing under
    // `settle_claim_action`, and the close bumps the epoch. The interleaving
    // is what this test builds; every precondition it needs is asserted,
    // because each one silently unbuilds it.
    let fx = world(true).await;

    // The claim records at the task — entry Pending, the assignment offer
    // still in flight — which is the only window a release is legal in.
    fx.apply_team_command_at(&team_scope(), claim_command())
        .await
        .expect("the claim records");
    let _ = fx.settle_team_at(&team_scope()).await;
    let pending = entry(&fx).await.expect("the entry stands");
    assert_eq!(pending.status, AgentTeamBoardEntryStatus::Pending);

    // The release commits its board decision and owes its exchange — not
    // yet driven, because only the team's own settle pass drives it.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Release {
            operation_id: op("release"),
            task: task_scope().task().clone(),
            member: member(),
            expected_epoch: pending.claim_epoch,
        },
    )
    .await
    .expect("the release records");
    let releasing = entry(&fx).await.expect("the entry stands");
    assert_eq!(
        releasing.status,
        AgentTeamBoardEntryStatus::Releasing,
        "the release decision committed, so its exchange is owed and undriven \
         — the interleaving this pin needs"
    );

    // The offer accepts, the task terminalizes, and its notice lands —
    // the entry closes under a bumped epoch while the release is still in
    // flight in the team's journal.
    fx.apply_task_command_at(&task_scope(), cancel_command())
        .await
        .expect("the cancel applies");
    for _round in 0..6 {
        let _ = fx.settle_task_at(&task_scope()).await;
        let run_id = rakka_agent::run_id_for_assignment(
            task_scope().task(),
            rakka_agent::AgentAssignmentGeneration::new(1),
        )
        .expect("the run id derives");
        if let Ok(scope) = rakka_agent::AgentRunScope::new(tenant(), member(), run_id) {
            let mut run = fx.run_at(&scope);
            if run.recover(fx.now()).await.is_ok() {
                let _ = run.settle_side_effects(&fx.router, fx.now()).await;
                let _ = fx.dispatcher.drive(&mut run, &fx.router, fx.now()).await;
                let _ = run.settle_side_effects(&fx.router, fx.now()).await;
            }
        }
    }
    let closed = entry(&fx).await.expect("the board holds the entry");
    assert_eq!(closed.status, AgentTeamBoardEntryStatus::Done);
    // The claim reached the task and was *accepted* — the precondition the
    // arm under test needs. Had it resolved through `resolve_team_claim_refusal`
    // instead, `release_team_claim` would take its settled-claim early `Ok`,
    // the team's accepted-Release branch would require `Releasing` and find
    // `Done`, and both assertions below would pass against a guard that had
    // been deleted.
    assert!(
        matches!(
            fx.task_snapshot()
                .await
                .team_claim
                .as_deref()
                .map(|claim| &claim.status),
            Some(rakka_agent::AgentTaskTeamClaimStatus::Accepted)
        ),
        "the release answers `team-claim-already-owned`, the one arm the \
         guards below cover"
    );

    // Now the release's stale reply is delivered and settles — and changes
    // nothing. The pass must actually settle it: a reply that never arrived
    // would leave every assertion below trivially true.
    let mut settled = 0;
    for _round in 0..3 {
        if let Ok(progress) = fx.settle_team_at(&team_scope()).await {
            settled += progress.settled;
        }
        let _ = fx.settle_task_at(&task_scope()).await;
    }
    assert!(
        settled >= 1,
        "the outstanding release exchange really did drain onto the closed entry"
    );
    let after = entry(&fx).await.expect("the board holds the entry");
    assert_eq!(
        after.status,
        AgentTeamBoardEntryStatus::Done,
        "and `settle_claim_action` declines to touch a closed entry at all"
    );
    assert!(after.claim.is_none());
    assert_eq!(
        after.claim_epoch, closed.claim_epoch,
        "nothing ran: not even the arm's `last_code` write"
    );
    assert_eq!(
        after.last_code, closed.last_code,
        "the entry still carries the terminal reason the close echoed"
    );
}

#[tokio::test]
async fn a_settled_notice_burns_no_revision_on_later_sweeps() {
    // The two-guard rule past the journal window: once the marker settled,
    // the settle-pass twin's would-advance probe derives nothing and writes
    // nothing.
    let fx = world(true).await;
    fx.apply_task_command_at(&task_scope(), cancel_command())
        .await
        .expect("the cancel applies");
    drive(&fx).await;
    assert!(fx.task_snapshot().await.team_terminal_notice_settled);

    fx.tasks.reset_writes();
    let _ = fx.settle_task_at(&task_scope()).await;
    let _ = fx.settle_task_at(&task_scope()).await;
    assert_eq!(
        fx.tasks.writes(),
        0,
        "a healthy sweep over a settled notice burns no revision"
    );
}

#[tokio::test]
async fn a_loss_in_the_committed_but_unsent_window_re_drives_the_same_notice() {
    // The window the journal exists for: the terminal committed — status
    // flipped, notice owed — and the delivery was lost. Nothing but a later
    // drive may deliver it, under the same operation id. The command path
    // drives its own courier, so the lost delivery is injected.
    let fx = world(true).await;
    fx.team_transport
        .inject(rakka_agent::testkit::ExchangeFault::LoseEnvelope);
    fx.apply_task_command_at(&task_scope(), cancel_command())
        .await
        .expect("the cancel applies");

    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Cancelled);
    assert!(
        !task.team_terminal_notice_settled,
        "the notice is owed, not settled — nothing was delivered yet"
    );
    let before = entry(&fx).await.expect("the board holds the entry");
    assert_ne!(
        before.status,
        AgentTeamBoardEntryStatus::Done,
        "the board has not heard yet"
    );

    // Every driver from here is a restart: recovery finds the owed exchange
    // and the close converges.
    drive(&fx).await;
    let after = entry(&fx).await.expect("the board holds the entry");
    assert_eq!(after.status, AgentTeamBoardEntryStatus::Done);
    assert!(fx.task_snapshot().await.team_terminal_notice_settled);
    assert_eq!(
        team_history_count(&fx, AgentTeamHistoryKind::TaskClosed).await,
        1
    );
}

/// Counts the durable writes one crash-free terminal flow attempts on each
/// store, so the sweeps below cover every real write and know when they
/// have run past the flow's end.
async fn reference_writes() -> (usize, usize) {
    let fx = world(true).await;
    activate_claim(&fx).await;
    fx.teams.reset_writes();
    fx.tasks.reset_writes();
    let _ = fx
        .apply_task_command_at(&task_scope(), cancel_command())
        .await;
    drive(&fx).await;
    (fx.teams.writes(), fx.tasks.writes())
}

#[tokio::test]
async fn the_close_converges_across_every_team_store_crash_point() {
    let (team_writes, _) = reference_writes().await;
    assert!(
        team_writes >= 1,
        "the terminal flow writes the team store at least once (the close)"
    );
    for point in 1..=team_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world(true).await;
            activate_claim(&fx).await;
            fx.teams.reset_writes();
            fx.teams.crash_at(point, window);
            let _ = fx
                .apply_task_command_at(&task_scope(), cancel_command())
                .await;
            drive(&fx).await;
            fx.teams.assert_crash_fired(point, window);
            fx.teams.survive();
            assert_converged(&fx).await;
        }
    }
}

#[tokio::test]
async fn the_close_converges_across_every_task_store_crash_point() {
    let (_, task_writes) = reference_writes().await;
    assert!(
        task_writes >= 2,
        "the terminal flow writes the task store at least twice (terminal commit, notice settle)"
    );
    for point in 1..=task_writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world(true).await;
            activate_claim(&fx).await;
            fx.tasks.reset_writes();
            fx.tasks.crash_at(point, window);
            let _ = fx
                .apply_task_command_at(&task_scope(), cancel_command())
                .await;
            drive(&fx).await;
            fx.tasks.assert_crash_fired(point, window);
            fx.tasks.survive();
            assert_converged(&fx).await;
        }
    }
}
