//! Team entity lifecycle: creation, fenced membership, the bounded message
//! ring, disband, lazy expiry, and the audit history
//! ([specification 8.10 and 17.13](../../../docs/plans/rakka-agent/spec.md),
//! scenario 42's board half).
//!
//! Every command rebuilds the entity from durable state — each call is
//! already a restart — and every fence is proven by the stale command
//! failing closed with its stable code.

mod common;

use common::{tenant, Fixture};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentCapabilityId, AgentGoalId, AgentId, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentTeamCreation, AgentTeamEntityCommand, AgentTeamEntityReply,
    AgentTeamHistoryCursor, AgentTeamHistoryStore, AgentTeamId, AgentTeamPolicy, AgentTeamScope,
    AgentTeamStatus,
};
use std::collections::{BTreeMap, BTreeSet};

const TEAM: &str = "support-team";
const LEADER: &str = "lead";
const MEMBER: &str = "worker";

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

fn creation(policy: AgentTeamPolicy) -> AgentTeamCreation {
    let mut members: BTreeMap<AgentId, BTreeSet<AgentCapabilityId>> = BTreeMap::new();
    members.insert(member_id(MEMBER), BTreeSet::new());
    AgentTeamCreation {
        leader: member_id(LEADER),
        root_goal: AgentGoalId::new("quarterly-support").expect("the goal id is valid"),
        policy,
        members,
    }
}

fn create_command(policy: AgentTeamPolicy) -> AgentTeamEntityCommand {
    AgentTeamEntityCommand::Create {
        operation_id: op("create"),
        creation: Box::new(creation(policy)),
    }
}

async fn created_fixture(policy: AgentTeamPolicy) -> Fixture {
    let fx = fixture();
    let reply = fx
        .apply_team_command_at(&team_scope(), create_command(policy))
        .await
        .expect("the team creates");
    assert!(matches!(reply, AgentTeamEntityReply::Applied { .. }));
    fx
}

#[tokio::test]
async fn a_team_creates_once_and_a_replayed_creation_echoes_the_outcome() {
    let fx = created_fixture(AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)).await;

    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the created team snapshots");
    assert_eq!(snapshot.status, AgentTeamStatus::Active);
    assert_eq!(snapshot.leader, member_id(LEADER));
    // The leader joins whether or not the creation listed it.
    assert!(snapshot.members.contains_key(&member_id(LEADER)));
    assert!(snapshot.members.contains_key(&member_id(MEMBER)));
    assert_eq!(snapshot.members.len(), 2);

    // The replayed creation answers from the operation log: one team, one
    // creation, the original outcome.
    let replay = fx
        .apply_team_command_at(
            &team_scope(),
            create_command(AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)),
        )
        .await
        .expect("the replay is answered");
    let AgentTeamEntityReply::Duplicate { outcome } = replay else {
        panic!("a replayed creation answers Duplicate, got {replay:?}");
    };
    assert_eq!(outcome.members, 2);
}

#[tokio::test]
async fn membership_changes_fence_on_the_lifecycle_revision() {
    let fx = created_fixture(AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)).await;

    let joined = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::AddMember {
                operation_id: op("join-1"),
                member: member_id("newcomer"),
                capability_scopes: BTreeSet::new(),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(common::provenance(10)),
            },
        )
        .await
        .expect("the join applies");
    let AgentTeamEntityReply::Applied { outcome } = joined else {
        panic!("the join applies, got {joined:?}");
    };
    assert_eq!(outcome.members, 3);
    assert_eq!(
        outcome.lifecycle_revision,
        AgentRevisionNumber::INITIAL.next()
    );

    // The same expectation again is stale: the revision moved.
    let stale = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::AddMember {
                operation_id: op("join-2"),
                member: member_id("straggler"),
                capability_scopes: BTreeSet::new(),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(common::provenance(11)),
            },
        )
        .await
        .expect_err("a stale membership change fails closed");
    assert_eq!(stale.code(), "team-stale-lifecycle-revision");

    // The leader is immovable; a plain member leaves under the fence.
    let leader = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::RemoveMember {
                operation_id: op("leave-leader"),
                member: member_id(LEADER),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL.next(),
                provenance: Box::new(common::provenance(12)),
            },
        )
        .await
        .expect_err("the leader cannot be removed");
    assert_eq!(leader.code(), "team-leader-immovable");

    let left = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::RemoveMember {
                operation_id: op("leave-1"),
                member: member_id("newcomer"),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL.next(),
                provenance: Box::new(common::provenance(13)),
            },
        )
        .await
        .expect("the leave applies");
    let AgentTeamEntityReply::Applied { outcome } = left else {
        panic!("the leave applies, got {left:?}");
    };
    assert_eq!(outcome.members, 2);
}

#[tokio::test]
async fn membership_and_board_ceilings_fail_closed() {
    let policy = AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)
        .with_max_members(2)
        .with_max_board_entries(1);
    let fx = created_fixture(policy).await;

    // Two members exist (leader + worker); the ceiling refuses a third.
    let full = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::AddMember {
                operation_id: op("join-over"),
                member: member_id("overflow"),
                capability_scopes: BTreeSet::new(),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(common::provenance(20)),
            },
        )
        .await
        .expect_err("the membership ceiling fails closed");
    assert_eq!(full.code(), "team-members-exhausted");

    let posted = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::PostTask {
                operation_id: op("post-1"),
                task: rakka_agent::AgentTaskId::new("board-1").expect("the task id is valid"),
                posted_by: member_id(MEMBER),
            },
        )
        .await
        .expect("the first post applies");
    assert!(matches!(posted, AgentTeamEntityReply::Applied { .. }));

    let duplicate = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::PostTask {
                operation_id: op("post-1-again"),
                task: rakka_agent::AgentTaskId::new("board-1").expect("the task id is valid"),
                posted_by: member_id(MEMBER),
            },
        )
        .await
        .expect_err("a task posts once");
    assert_eq!(duplicate.code(), "team-task-already-posted");

    let full_board = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::PostTask {
                operation_id: op("post-2"),
                task: rakka_agent::AgentTaskId::new("board-2").expect("the task id is valid"),
                posted_by: member_id(MEMBER),
            },
        )
        .await
        .expect_err("the board ceiling fails closed");
    assert_eq!(full_board.code(), "team-board-exhausted");
}

#[tokio::test]
async fn the_message_ring_bounds_its_size_and_counts_its_drops() {
    let policy = AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)
        .with_max_messages(2)
        .with_max_message_bytes(16);
    let fx = created_fixture(policy).await;

    let outsider = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::AppendMessage {
                operation_id: op("msg-outsider"),
                from: member_id("outsider"),
                to: None,
                body: "hello".to_string(),
            },
        )
        .await
        .expect_err("a non-member cannot post to the ring");
    assert_eq!(outsider.code(), "team-not-member");

    let oversized = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::AppendMessage {
                operation_id: op("msg-oversized"),
                from: member_id(MEMBER),
                to: None,
                body: "x".repeat(64),
            },
        )
        .await
        .expect_err("an oversized body fails closed");
    assert_eq!(oversized.code(), "team-message-too-large");

    for index in 0..3u32 {
        let applied = fx
            .apply_team_command_at(
                &team_scope(),
                AgentTeamEntityCommand::AppendMessage {
                    operation_id: op(&format!("msg-{index}")),
                    from: member_id(MEMBER),
                    to: Some(member_id(LEADER)),
                    body: format!("note {index}"),
                },
            )
            .await
            .expect("the append applies");
        assert!(matches!(applied, AgentTeamEntityReply::Applied { .. }));
    }

    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    // The bounded ring dropped the oldest and said so — bounded loss, never
    // silent.
    assert_eq!(snapshot.messages.len(), 2);
    assert_eq!(snapshot.messages_dropped, 1);
    assert_eq!(
        snapshot
            .messages
            .iter()
            .map(|message| message.sequence)
            .collect::<Vec<_>>(),
        vec![2, 3],
        "the ring keeps the newest messages in sequence order"
    );
}

#[tokio::test]
async fn disband_fences_on_the_lifecycle_revision_and_closes_the_team() {
    let fx = created_fixture(AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)).await;

    let stale = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Disband {
                operation_id: op("disband-stale"),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL.next(),
                provenance: Box::new(common::provenance(30)),
                reason: "done".to_string(),
            },
        )
        .await
        .expect_err("a stale disband fails closed");
    assert_eq!(stale.code(), "team-stale-lifecycle-revision");

    let disbanded = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::Disband {
                operation_id: op("disband"),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(common::provenance(31)),
                reason: "done".to_string(),
            },
        )
        .await
        .expect("the disband applies");
    let AgentTeamEntityReply::Applied { outcome } = disbanded else {
        panic!("the disband applies, got {disbanded:?}");
    };
    assert_eq!(outcome.status, AgentTeamStatus::Disbanded);

    // The board is read-only history now: every mutating command refuses.
    let post = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::PostTask {
                operation_id: op("post-after"),
                task: rakka_agent::AgentTaskId::new("late").expect("the task id is valid"),
                posted_by: member_id(MEMBER),
            },
        )
        .await
        .expect_err("a disbanded team accepts no board command");
    assert_eq!(post.code(), "team-disbanded");
}

#[tokio::test]
async fn a_passed_expiry_horizon_refuses_commands_and_the_settle_pass_records_the_flip() {
    let policy = AgentTeamPolicy::new(AgentRevisionNumber::INITIAL).with_expiry_after_ms(10);
    let fx = created_fixture(policy).await;

    // Advance the fixture clock well past the horizon.
    fx.clock.store(10_000, std::sync::atomic::Ordering::SeqCst);

    // The command refuses purely — no timer fired, nothing flipped yet.
    let refused = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::PostTask {
                operation_id: op("post-late"),
                task: rakka_agent::AgentTaskId::new("late").expect("the task id is valid"),
                posted_by: member_id(MEMBER),
            },
        )
        .await
        .expect_err("an expired team refuses");
    assert_eq!(refused.code(), "team-expired");
    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    assert_eq!(
        snapshot.status,
        AgentTeamStatus::Active,
        "the refusal is pure; the flip belongs to the settle pass"
    );

    // The settle pass observes the horizon durably, once.
    let progress = fx
        .settle_team_at(&team_scope())
        .await
        .expect("the settle pass runs");
    assert!(progress.expiry_observed);
    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    assert_eq!(snapshot.status, AgentTeamStatus::Expired);

    let progress = fx
        .settle_team_at(&team_scope())
        .await
        .expect("the second settle pass runs");
    assert!(
        !progress.expiry_observed,
        "a second sweep burns no revision over an already-expired team"
    );
}

#[tokio::test]
async fn the_audit_trail_is_history_recorded_once_per_transition() {
    let fx = created_fixture(AgentTeamPolicy::new(AgentRevisionNumber::INITIAL)).await;

    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::AddMember {
            operation_id: op("join-audit"),
            member: member_id("newcomer"),
            capability_scopes: BTreeSet::new(),
            expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
            provenance: Box::new(common::provenance(40)),
        },
    )
    .await
    .expect("the join applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-audit"),
            task: rakka_agent::AgentTaskId::new("board-1").expect("the task id is valid"),
            posted_by: member_id(MEMBER),
        },
    )
    .await
    .expect("the post applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::AppendMessage {
            operation_id: op("msg-audit"),
            from: member_id(MEMBER),
            to: None,
            body: "note".to_string(),
        },
    )
    .await
    .expect("the append applies");
    // A replayed command records no second history row.
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::AppendMessage {
            operation_id: op("msg-audit"),
            from: member_id(MEMBER),
            to: None,
            body: "note".to_string(),
        },
    )
    .await
    .expect("the replay is answered");

    let mut kinds = Vec::new();
    let mut cursor = AgentTeamHistoryCursor::start();
    loop {
        let page = fx
            .team_history
            .read(&team_scope(), cursor)
            .await
            .expect("the history reads");
        kinds.extend(page.entries.iter().map(|entry| entry.kind));
        match page.next {
            Some(next) => cursor = next,
            None => break,
        }
    }
    assert_eq!(
        kinds.iter().map(|kind| kind.as_label()).collect::<Vec<_>>(),
        vec![
            "team-created",
            "team-member-joined",
            "team-task-posted",
            "team-message-appended",
        ],
        "one ordered row per durable transition, none for the replay"
    );
}

#[tokio::test]
async fn done_entries_are_evicted_before_the_ceiling_refuses_a_post() {
    // A one-entry board: the smallest ceiling makes exhaustion immediate.
    let fx = created_fixture(
        AgentTeamPolicy::new(AgentRevisionNumber::INITIAL).with_max_board_entries(1),
    )
    .await;

    // A claim against a task entity that was never created closes the entry
    // as Done — a settled fact that must not hold the ceiling forever.
    let phantom = rakka_agent::AgentTaskId::new("phantom").expect("the task id is valid");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-phantom"),
            task: phantom.clone(),
            posted_by: member_id(MEMBER),
        },
    )
    .await
    .expect("the post applies");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::Claim {
            operation_id: op("claim-phantom"),
            task: phantom.clone(),
            member: member_id(MEMBER),
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
    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    let entry = snapshot
        .board
        .iter()
        .find(|entry| entry.task == phantom)
        .expect("the phantom entry stands");
    assert_eq!(entry.status, rakka_agent::AgentTeamBoardEntryStatus::Done);

    // The full board evicts its Done entry instead of refusing every future
    // post: a long-lived team is never exhausted by its own finished work.
    let next = rakka_agent::AgentTaskId::new("board-next").expect("the task id is valid");
    fx.apply_team_command_at(
        &team_scope(),
        AgentTeamEntityCommand::PostTask {
            operation_id: op("post-next"),
            task: next.clone(),
            posted_by: member_id(MEMBER),
        },
    )
    .await
    .expect("the Done entry yields its slot to live work");
    let snapshot = fx
        .team_snapshot_at(&team_scope())
        .await
        .expect("the team snapshots");
    assert_eq!(snapshot.board.len(), 1);
    assert_eq!(snapshot.board[0].task, next);

    // A live entry is never evicted: the ceiling still refuses over open work.
    let refused = fx
        .apply_team_command_at(
            &team_scope(),
            AgentTeamEntityCommand::PostTask {
                operation_id: op("post-over"),
                task: rakka_agent::AgentTaskId::new("board-over").expect("the task id is valid"),
                posted_by: member_id(MEMBER),
            },
        )
        .await
        .expect_err("a full board of live work still refuses");
    assert_eq!(refused.code(), "team-board-exhausted");
}
