//! Conversation entity lifecycle: trusted creation, the creation-time policy
//! arithmetic, the moderator's fenced early end, creation-fixed budgets, the
//! bounded transcript ring, lazy deadline expiry, and the compatibility pins
//! ([specification 8.11 and 17.13](../../../docs/plans/rakka-agent/spec.md),
//! scenario 43's lifecycle half).
//!
//! Every command rebuilds the entity from durable state — each call is
//! already a restart — and every fence is proven by the stale command
//! failing closed with its stable code.

mod common;

use std::sync::atomic::Ordering;

use common::{tenant, Fixture};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_end_operation_id, conversation_turn_content_digest,
    conversation_turn_operation_id, AgentBudgetConsumption, AgentConversationCompletionRule,
    AgentConversationCreation, AgentConversationDirection, AgentConversationEntityCommand,
    AgentConversationEntityReply, AgentConversationId, AgentConversationMode,
    AgentConversationScope, AgentConversationStatus, AgentConversationTerminalReason,
    AgentConversationTurnSubmit, AgentId, AgentModerationPolicy, AgentOperationId,
    AgentOperationKind, AgentRevisionNumber, AgentTaskId,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

const CONVERSATION: &str = "design-review";
const MODERATOR: &str = "moderator";
const TASK: &str = "review-task";

/// The fixture with this file's whole cast instantiated as
/// moderation-capable agents: the roster admits a speaker to *this*
/// conversation, its definition admits it to moderated work at all, and the
/// turn door reads both.
async fn fixture() -> Fixture {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.instantiate_conversation_participants(&[
        MODERATOR, "alpha", "beta", "gamma", "p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8",
        "p9",
    ])
    .await;
    fx
}

fn conversation_scope() -> AgentConversationScope {
    AgentConversationScope::new(
        tenant(),
        AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
    )
    .expect("the conversation scope is valid")
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name).expect("the agent id is valid")
}

fn provenance(at: u64) -> rakka_agent::AgentRevisionProvenance {
    rakka_agent::AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator-7".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn creation(policy: AgentModerationPolicy, participants: &[&str]) -> AgentConversationCreation {
    AgentConversationCreation {
        moderator: agent(MODERATOR),
        participants: participants.iter().map(|name| agent(name)).collect(),
        mode: AgentConversationMode::RoundRobin,
        completion: AgentConversationCompletionRule::ModeratorDecides,
        policy,
        task: AgentTaskId::new(TASK).expect("the task id is valid"),
        tokens: None,
        max_wall_clock_millis: None,
        transcript_ref: Some("artifact://transcripts/design-review".to_string()),
    }
}

fn create_op() -> AgentOperationId {
    rakka_agent::conversation_create_operation_id(
        &tenant(),
        &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
    )
    .expect("the operation id derives")
}

fn create_command(creation: AgentConversationCreation) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::Create {
        operation_id: create_op(),
        creation: Box::new(creation),
    }
}

fn submit_command(
    round: u64,
    turn: u32,
    participant: &str,
    body: &str,
    tokens: u64,
) -> AgentConversationEntityCommand {
    let mut usage = AgentBudgetConsumption::zero();
    usage.tokens = tokens;
    AgentConversationEntityCommand::SubmitTurn {
        operation_id: conversation_turn_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
            round,
            turn,
            &agent(participant),
            &conversation_turn_content_digest(body, None),
        )
        .expect("the operation id derives"),
        submit: Box::new(AgentConversationTurnSubmit {
            round,
            turn,
            participant: agent(participant),
            body: body.to_string(),
            direction: None,
            usage,
        }),
    }
}

fn end_command(expected_round: u64, reason: &str) -> AgentConversationEntityCommand {
    end_command_by(MODERATOR, expected_round, reason)
}

fn end_command_by(
    moderator: &str,
    expected_round: u64,
    reason: &str,
) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::EndEarly {
        operation_id: conversation_end_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
            expected_round,
            reason,
        )
        .expect("the operation id derives"),
        moderator: agent(moderator),
        expected_round,
        reason: reason.to_string(),
        provenance: Box::new(provenance(1)),
    }
}

async fn created_fixture(creation: AgentConversationCreation) -> Fixture {
    let fx = fixture().await;
    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), create_command(creation))
        .await
        .expect("the conversation creates");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    fx
}

#[tokio::test]
async fn a_conversation_creates_once_and_a_replayed_creation_echoes_the_outcome() {
    let policy = AgentModerationPolicy::new(AgentRevisionNumber::INITIAL);
    let fx = created_fixture(creation(policy.clone(), &["alpha", "beta"])).await;

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the created conversation snapshots");
    assert_eq!(snapshot.status, AgentConversationStatus::Active);
    assert_eq!(snapshot.moderator, agent(MODERATOR));
    assert_eq!(snapshot.participants, vec![agent("alpha"), agent("beta")]);
    assert_eq!(snapshot.round, 0);
    assert_eq!(snapshot.turn_in_round, 0);
    assert_eq!(snapshot.current_speaker, Some(agent("alpha")));
    assert_eq!(
        snapshot.transcript_ref.as_deref(),
        Some("artifact://transcripts/design-review")
    );

    let replay = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(policy, &["alpha", "beta"])),
        )
        .await
        .expect("the replay is answered");
    let AgentConversationEntityReply::Duplicate { outcome } = replay else {
        panic!("a replayed creation answers Duplicate, got {replay:?}");
    };
    assert_eq!(outcome.status, AgentConversationStatus::Active);
    assert_eq!(outcome.turns_recorded, 0);
}

#[tokio::test]
async fn an_invalid_roster_or_transcript_reference_refuses_at_creation() {
    let policy = || AgentModerationPolicy::new(AgentRevisionNumber::INITIAL);

    let empty = fixture()
        .await
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(policy(), &[])),
        )
        .await
        .expect_err("an empty roster refuses");
    assert_eq!(empty.code(), "conversation-participants-invalid");

    let repeated = fixture()
        .await
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(policy(), &["alpha", "alpha"])),
        )
        .await
        .expect_err("a repeated participant refuses");
    assert_eq!(repeated.code(), "conversation-participants-invalid");

    let over_cap = fixture()
        .await
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(
                policy(),
                &["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"],
            )),
        )
        .await
        .expect_err("a roster over the hard cap refuses");
    assert_eq!(over_cap.code(), "conversation-participants-invalid");

    // A round-robin round is one turn per roster member: a roster longer
    // than the turn ceiling could never complete a round.
    let unroundable = fixture()
        .await
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(
                policy().with_max_turns_per_round(2),
                &["p1", "p2", "p3"],
            )),
        )
        .await
        .expect_err("a roster past the round ceiling refuses");
    assert_eq!(unroundable.code(), "conversation-participants-invalid");

    let mut oversized_ref = creation(policy(), &["alpha"]);
    oversized_ref.transcript_ref = Some("r".repeat(300));
    let oversized = fixture()
        .await
        .apply_conversation_command_at(&conversation_scope(), create_command(oversized_ref))
        .await
        .expect_err("an oversized transcript reference refuses");
    assert_eq!(oversized.code(), "conversation-transcript-ref-invalid");
}

/// A history sink that is down, so the pending outbox reaches its capacity —
/// the only way the persisted state ever holds a full one.
#[derive(Clone, Default)]
struct UnavailableHistory;

impl rakka_agent::AgentConversationHistoryStore for UnavailableHistory {
    fn backend_name(&self) -> &'static str {
        "unavailable"
    }

    fn append<'a>(
        &'a self,
        _scope: &'a AgentConversationScope,
        _entry: &'a rakka_agent::AgentConversationHistoryEntry,
    ) -> rakka_agent::AgentConversationHistoryFuture<'a, ()> {
        Box::pin(async move { Err(unavailable()) })
    }

    fn read<'a>(
        &'a self,
        _scope: &'a AgentConversationScope,
        _cursor: rakka_agent::AgentConversationHistoryCursor,
    ) -> rakka_agent::AgentConversationHistoryFuture<'a, rakka_agent::AgentConversationHistoryPage>
    {
        Box::pin(async move { Err(unavailable()) })
    }
}

fn unavailable() -> rakka_agent::AgentConversationError {
    rakka_agent::AgentConversationError::Choreography(Box::new(
        rakka_agent::AgentChoreographyError::Persistence(rakka_persistence::DurableError::Store {
            backend: "unavailable",
            message: "the history sink is down".to_string(),
        }),
    ))
}

#[tokio::test]
async fn the_creation_arithmetic_upper_bounds_what_the_state_guard_measures() {
    // The whole point of the creation-time reserve: it must be an upper
    // bound on every byte the in-flight guard can ever measure. If it is
    // not, a policy passes the door and the protocol wedges mid-round on
    // `conversation-state-too-large`, with the early end as its only exit.
    // Checked here by *saturating* the maxed-out policy — a full ledger, a
    // full ring of maximally escape-expensive bodies, a full operation log,
    // and a full history outbox — and comparing the real serialized size to
    // the reserve creation charged for it.
    let maxed = AgentModerationPolicy::new(AgentRevisionNumber::INITIAL)
        .with_max_rounds(16)
        .with_max_turns_per_round(16)
        .with_max_messages(16)
        .with_max_message_bytes(1024);

    let reserved = 16 * 16 * rakka_agent::AGENT_CONVERSATION_TURN_RECORD_RESERVE_BYTES
        + 16 * (1024 + rakka_agent::AGENT_CONVERSATION_MESSAGE_RECORD_RESERVE_BYTES)
        + rakka_agent::AGENT_CONVERSATION_OPERATION_LOG_CAPACITY
            * rakka_agent::AGENT_CONVERSATION_OPERATION_LOG_ENTRY_RESERVE_BYTES
        + rakka_agent::AGENT_CONVERSATION_PENDING_HISTORY_CAPACITY
            * rakka_agent::AGENT_CONVERSATION_HISTORY_ENTRY_RESERVE_BYTES
        + rakka_agent::AGENT_CONVERSATION_FIXED_OVERHEAD_BYTES;
    let bound = rakka_agent::AGENT_CONVERSATION_MATERIALIZED_MAX_BYTES
        - rakka_agent::AGENT_CONVERSATION_STATE_GROWTH_RESERVE_BYTES;
    assert!(
        reserved <= bound,
        "every policy the hard caps admit must fit: reserved {reserved} against bound {bound}"
    );

    // The maxed policy therefore creates rather than refusing — the wedge is
    // impossible by construction, not merely turned away at the door.
    let roster: Vec<String> = (0..8).map(|index| format!("p{index}")).collect();
    let names: Vec<&str> = roster.iter().map(String::as_str).collect();
    let fx = created_fixture(creation(maxed, &names)).await;

    // Now saturate every bounded collection at once. Bodies are all quotes:
    // each byte escapes to two, so a 512-character body stores as the full
    // 1024-byte ceiling — the most expensive body the policy admits.
    let body = "\"".repeat(512);
    // Follow the cursor rather than assuming it: the round closes on its
    // own, and a coordinate behind the cursor would be answered by the
    // ledger instead of recording anything.
    for _ in 0..256 {
        let Some(snapshot) = fx.conversation_snapshot_at(&conversation_scope()).await else {
            break;
        };
        let Some(speaker) = snapshot.current_speaker.clone() else {
            break;
        };
        // Two rounds are left for the degraded stretch below, which is the
        // only way the pending outbox ever reaches its capacity.
        if snapshot.round >= 14 {
            break;
        }
        if fx
            .apply_conversation_command_at(
                &conversation_scope(),
                submit_command(
                    snapshot.round,
                    snapshot.turn_in_round,
                    speaker.as_str(),
                    &body,
                    1,
                ),
            )
            .await
            .is_err()
        {
            break;
        }
    }

    // The ledger, ring, and operation log are now as full as this policy
    // allows. The outbox is not — the fixture's sink drains it — so the last
    // stretch runs against a sink that is down, which is the only way the
    // pending history reaches its capacity.
    let mut degraded = rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        fx.agents.clone(),
        UnavailableHistory,
    );
    for _ in 0..64 {
        let Some(snapshot) = fx.conversation_snapshot_at(&conversation_scope()).await else {
            break;
        };
        let Some(speaker) = snapshot.current_speaker.clone() else {
            break;
        };
        let command = submit_command(
            snapshot.round,
            snapshot.turn_in_round,
            speaker.as_str(),
            &body,
            1,
        );
        let _ = degraded.apply(command, &fx.router, fx.now()).await;
    }

    let serialized = fx
        .conversation_state_bytes(&conversation_scope())
        .await
        .expect("the persisted state serializes");
    // The measurement is only worth anything if the state really is
    // saturated, so assert that before comparing sizes.
    let saturated = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the saturated conversation snapshots");
    assert_eq!(saturated.turns.len(), 128, "the ledger filled");
    assert_eq!(saturated.messages.len(), 16, "the ring filled");
    assert!(
        serialized > rakka_agent::AGENT_CONVERSATION_FIXED_OVERHEAD_BYTES,
        "a state this size is a real measurement, not an empty one: {serialized}"
    );
    assert!(
        serialized <= reserved,
        "the saturated state must fit the reserve creation charged: {serialized} > {reserved}"
    );
    assert!(
        serialized <= bound,
        "and it must fit the guard's own bound: {serialized} > {bound}"
    );
}

#[tokio::test]
async fn the_moderator_ends_early_under_policy_and_the_round_is_the_fence() {
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "beta"],
    ))
    .await;

    // A stale round refuses before anything flips.
    let stale = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(5, "premature"))
        .await
        .expect_err("an end against a future round refuses");
    assert_eq!(stale.code(), "conversation-end-stale-round");

    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "consensus reached"))
        .await
        .expect("the end applies");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the ended conversation snapshots");
    assert_eq!(snapshot.status, AgentConversationStatus::Ended);
    assert_eq!(
        snapshot.terminal_reason,
        Some(AgentConversationTerminalReason::ModeratorEnded)
    );
    assert!(snapshot.ended_at.is_some());

    // The replayed end answers from the operation log.
    let replay = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "consensus reached"))
        .await
        .expect("the replay is answered");
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));

    // A *distinct* end decision — a later round, hence a new operation —
    // finds the terminal state absorbing.
    let second = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(1, "again"))
        .await
        .expect_err("a second distinct end refuses");
    assert_eq!(second.code(), "conversation-ended");

    // And a turn after the end refuses under the terminal code.
    let turn = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "alpha", "late thought", 0),
        )
        .await
        .expect_err("a turn after the end refuses");
    assert_eq!(turn.code(), "conversation-ended");
}

#[tokio::test]
async fn only_the_moderators_end_terminalizes_the_conversation() {
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "beta"],
    ))
    .await;

    // A roster participant may speak but may not end: specification 8.11
    // grants the early end to the moderator alone.
    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            end_command_by("alpha", 0, "i am done"),
        )
        .await
        .expect_err("a participant's end refuses");
    assert_eq!(refused.code(), "conversation-end-not-moderator");

    // An agent outside the conversation entirely refuses under the same
    // fence — the durable moderator is the only admitted claim.
    let stranger = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            end_command_by("outsider", 0, "on your behalf"),
        )
        .await
        .expect_err("a stranger's end refuses");
    assert_eq!(stranger.code(), "conversation-end-not-moderator");

    // Nothing flipped: the conversation is still live and still the
    // moderator's to end.
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the live conversation snapshots");
    assert_eq!(snapshot.status, AgentConversationStatus::Active);
    assert_eq!(snapshot.terminal_reason, None);

    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "consensus reached"))
        .await
        .expect("the moderator's end applies");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
}

#[tokio::test]
async fn an_end_forbidden_by_policy_refuses() {
    // Forbidding the early end is admissible as long as some other road to
    // a terminal state remains — here the wall-clock deadline.
    let mut forbidden = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).without_early_end(),
        &["alpha"],
    );
    forbidden.max_wall_clock_millis = Some(60_000);
    let fx = created_fixture(forbidden).await;
    let refused = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "premature"))
        .await
        .expect_err("the policy forbids the early end");
    assert_eq!(refused.code(), "conversation-end-not-permitted");
}

#[tokio::test]
async fn the_early_end_records_who_ended_it_why_and_bounds_the_reason() {
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "beta"],
    ))
    .await;

    // The reason is caller-supplied free text on a durable append, so it is
    // bounded at the ceiling the constant advertises — not silently
    // truncated at twice it.
    let oversized = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            end_command(
                0,
                &"r".repeat(rakka_agent::AGENT_CONVERSATION_REASON_MAX_BYTES + 1),
            ),
        )
        .await
        .expect_err("an over-long reason refuses");
    assert_eq!(oversized.code(), "conversation-reason-too-large");
    assert_eq!(
        fx.conversation_snapshot_at(&conversation_scope())
            .await
            .expect("the conversation snapshots")
            .status,
        AgentConversationStatus::Active,
        "the refused end terminalized nothing"
    );

    fx.apply_conversation_command_at(&conversation_scope(), end_command(0, "consensus reached"))
        .await
        .expect("the end applies");

    // The audit trail answers who terminalized the conversation, against
    // which round, and why — each in its own field, so `detail` is the
    // stable terminal code and nothing has to be inferred from it.
    let page = rakka_agent::AgentConversationHistoryStore::read(
        &fx.conversation_history,
        &conversation_scope(),
        rakka_agent::AgentConversationHistoryCursor::start(),
    )
    .await
    .expect("the history reads");
    let ended = page
        .entries
        .iter()
        .find(|entry| entry.kind == rakka_agent::AgentConversationHistoryKind::Ended)
        .expect("the early end is audited");
    assert_eq!(ended.principal.as_deref(), Some("user:operator-7"));
    assert_eq!(ended.participant.as_ref(), Some(&agent(MODERATOR)));
    assert_eq!(ended.round, Some(0));
    assert_eq!(ended.reason.as_deref(), Some("consensus reached"));
    assert_eq!(
        ended.detail,
        AgentConversationTerminalReason::ModeratorEnded.code(),
        "detail is the stable terminal code, never the caller's free text"
    );
}

#[tokio::test]
async fn an_end_regenerated_with_a_different_reason_is_not_absorbed_as_a_duplicate() {
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "beta"],
    ))
    .await;

    fx.apply_conversation_command_at(&conversation_scope(), end_command(0, "consensus reached"))
        .await
        .expect("the end applies");

    // The identical redelivery converges from the operation log.
    let replay = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "consensus reached"))
        .await
        .expect("the identical redelivery is answered");
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));

    // A *regenerated* end at the same round carries different reasoning, so
    // it is a different decision: it derives its own operation id, misses
    // the log, and meets the terminal guard — rather than being answered
    // `Duplicate` while the audited reason stays the first attempt's.
    let regenerated = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "deadline exceeded"))
        .await
        .expect_err("a regenerated end refuses loudly");
    assert_eq!(regenerated.code(), "conversation-ended");

    // And the audited reason is still the one that actually decided it.
    let page = rakka_agent::AgentConversationHistoryStore::read(
        &fx.conversation_history,
        &conversation_scope(),
        rakka_agent::AgentConversationHistoryCursor::start(),
    )
    .await
    .expect("the history reads");
    let ends: Vec<_> = page
        .entries
        .iter()
        .filter(|entry| entry.kind == rakka_agent::AgentConversationHistoryKind::Ended)
        .collect();
    assert_eq!(ends.len(), 1, "one end, one audit entry");
    assert_eq!(ends[0].reason.as_deref(), Some("consensus reached"));
}

#[tokio::test]
async fn a_creation_replayed_with_different_content_refuses_rather_than_echoing() {
    // The turn identity's discipline applied to creation: same record,
    // converge; different record, refuse. A content-blind id would answer a
    // second creation `Duplicate` with the outcome of a conversation it does
    // not describe.
    let fx = fixture().await;
    let first = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "beta"],
    );
    let content_op = |creation: &AgentConversationCreation| {
        rakka_agent::conversation_create_content_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
            creation,
        )
        .expect("the operation id derives")
    };

    fx.apply_conversation_command_at(
        &conversation_scope(),
        AgentConversationEntityCommand::Create {
            operation_id: content_op(&first),
            creation: Box::new(first.clone()),
        },
    )
    .await
    .expect("the conversation creates");

    // The identical creation re-derives the identical operation and echoes.
    let replay = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            AgentConversationEntityCommand::Create {
                operation_id: content_op(&first),
                creation: Box::new(first.clone()),
            },
        )
        .await
        .expect("the identical replay is answered");
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));

    // A different roster is a different creation, so it derives a different
    // operation and meets the entity's own guard.
    let second = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "gamma"],
    );
    assert_ne!(content_op(&first), content_op(&second));
    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            AgentConversationEntityCommand::Create {
                operation_id: content_op(&second),
                creation: Box::new(second),
            },
        )
        .await
        .expect_err("a different creation refuses");
    assert_eq!(refused.code(), "conversation-already-created");

    // The roster the conversation actually has is the first one.
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.participants, vec![agent("alpha"), agent("beta")]);
}

#[tokio::test]
async fn a_configuration_with_no_reachable_terminal_state_refuses_at_creation() {
    // Under the moderator-decides rule the round ceiling only parks the
    // cursor, so the early end is the sole exit. Forbidding it without a
    // deadline leaves a conversation that can never terminalize — the
    // governing task would wait on a signal that cannot come, so the door
    // refuses it beside the other wedge guards.
    let unreachable = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).without_early_end(),
        &["alpha", "beta"],
    );
    let refused = fixture()
        .await
        .apply_conversation_command_at(&conversation_scope(), create_command(unreachable))
        .await
        .expect_err("a configuration with no terminal state refuses");
    assert_eq!(refused.code(), "conversation-completion-unreachable");

    // Each of the three ways out makes it admissible again: the early end…
    let with_end = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "beta"],
    );
    fixture()
        .await
        .apply_conversation_command_at(&conversation_scope(), create_command(with_end))
        .await
        .expect("the early end is a road to terminal");

    // …a deadline…
    let mut with_deadline = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).without_early_end(),
        &["alpha", "beta"],
    );
    with_deadline.max_wall_clock_millis = Some(60_000);
    fixture()
        .await
        .apply_conversation_command_at(&conversation_scope(), create_command(with_deadline))
        .await
        .expect("a deadline is a road to terminal");

    // …and the all-rounds completion rule, which ends the conversation in
    // the same commit that completes its final round.
    let mut all_rounds = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).without_early_end(),
        &["alpha", "beta"],
    );
    all_rounds.completion = AgentConversationCompletionRule::AllRounds;
    fixture()
        .await
        .apply_conversation_command_at(&conversation_scope(), create_command(all_rounds))
        .await
        .expect("completing every round is a road to terminal");
}

#[tokio::test]
async fn a_parked_conversation_names_no_next_speaker() {
    // The round ceiling parks the cursor with the conversation still
    // active. Naming a speaker there would send that speaker into
    // `conversation-rounds-exhausted` forever, and a driver polling
    // `current_speaker` would retry instead of routing the moderator to its
    // early end.
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL)
            .with_max_rounds(1)
            .with_max_turns_per_round(2),
        &["alpha", "beta"],
    ))
    .await;

    for (turn, speaker) in ["alpha", "beta"].into_iter().enumerate() {
        fx.apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, turn as u32, speaker, "statement", 0),
        )
        .await
        .expect("the turn records");
    }

    let parked = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the parked conversation snapshots");
    assert_eq!(parked.status, AgentConversationStatus::Active);
    assert_eq!(parked.round, 1, "the cursor parked at the round ceiling");
    assert_eq!(
        parked.current_speaker, None,
        "a parked cursor owns nothing, so the projection names nobody"
    );

    // And the refusal the projection is now honest about.
    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(1, 0, "alpha", "another", 0),
        )
        .await
        .expect_err("a turn past the round ceiling refuses");
    assert_eq!(refused.code(), "conversation-rounds-exhausted");

    // The moderator's early end is the move that remains, and it lands.
    fx.apply_conversation_command_at(&conversation_scope(), end_command(1, "we are done"))
        .await
        .expect("the moderator's end still lands on a parked conversation");
}

#[tokio::test]
async fn the_message_ceiling_measures_what_the_body_costs_to_store() {
    // `max_message_bytes` governs the ring's contribution to the serialized
    // state, so it has to measure the body the way the state does. Charging
    // the raw length while the bound measured the escaped one is what let a
    // policy pass the door and still blow the bound mid-round.
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_message_bytes(64),
        &["alpha", "beta"],
    ))
    .await;

    // Plain text is unaffected: 64 characters cost 64 bytes stored.
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "alpha", &"a".repeat(64), 0),
        )
        .await
        .expect("a plain body at the ceiling records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));

    // The same 64 characters as quotes cost 128 stored, and are refused —
    // with the stored cost, not the typed one, in the refusal.
    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 1, "beta", &"\"".repeat(64), 0),
        )
        .await
        .expect_err("an escape-expensive body over the stored ceiling refuses");
    assert_eq!(refused.code(), "conversation-message-too-large");
    assert!(
        refused.to_string().contains("128"),
        "the refusal reports the stored cost: {refused}"
    );

    // And half as many quotes fit exactly.
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 1, "beta", &"\"".repeat(32), 0),
        )
        .await
        .expect("an escaped body at the stored ceiling records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
}

#[tokio::test]
async fn an_implausible_usage_report_refuses_before_it_can_spend_the_shared_grant() {
    // The reported spend is the speaker's own claim about its own run, and
    // the grant it draws down belongs to every participant. Bounding the
    // claim is what keeps one turn from exhausting the conversation for
    // everyone — exhaustion refuses rather than parks, so a poisoned total
    // leaves no reachable progress at all.
    let mut creation = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_turn_tokens(500),
        &["alpha", "beta"],
    );
    creation.tokens = Some(1_000);
    let fx = created_fixture(creation).await;

    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "alpha", "an opening", u64::MAX),
        )
        .await
        .expect_err("an implausible report refuses");
    assert_eq!(refused.code(), "conversation-turn-usage-too-large");

    // Nothing was spent and nothing was recorded: the refusal precedes the
    // accounting, so the grant is intact for every other participant.
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.budgets.consumed.tokens, 0);
    assert!(snapshot.turns.is_empty());
    assert_eq!(snapshot.current_speaker, Some(agent("alpha")));

    // A report at the ceiling still lands, and overshooting what *remains*
    // stays legal below it — that spend already happened.
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "alpha", "an opening", 500),
        )
        .await
        .expect("a report at the ceiling records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.budgets.consumed.tokens, 500);

    // And the spend is attributed: the audit trail names who reported it,
    // so an exhausted conversation is never an anonymous total.
    let page = rakka_agent::AgentConversationHistoryStore::read(
        &fx.conversation_history,
        &conversation_scope(),
        rakka_agent::AgentConversationHistoryCursor::start(),
    )
    .await
    .expect("the history reads");
    let turn_entry = page
        .entries
        .iter()
        .find(|entry| entry.kind == rakka_agent::AgentConversationHistoryKind::TurnRecorded)
        .expect("the recorded turn is audited");
    assert_eq!(turn_entry.participant.as_ref(), Some(&agent("alpha")));
    assert_eq!(turn_entry.detail, "tokens=500");
}

#[tokio::test]
async fn token_exhaustion_refuses_the_next_turn_and_never_parks() {
    let mut creation = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha", "beta"],
    );
    creation.tokens = Some(100);
    let fx = created_fixture(creation).await;

    // The overshooting turn records whole: the spend already happened in
    // the speaker's run, and refusing it would lose the content without
    // recovering the tokens.
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "alpha", "a long opening statement", 150),
        )
        .await
        .expect("the overshooting turn records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));

    // Exhaustion bites at the next door — refuse, never park.
    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 1, "beta", "a reply", 0),
        )
        .await
        .expect_err("the exhausted budget refuses the next turn");
    assert_eq!(refused.code(), "tokens");

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(
        snapshot.status,
        AgentConversationStatus::Active,
        "exhaustion refuses, never parks or terminalizes"
    );
    assert_eq!(snapshot.budgets.consumed.tokens, 150);

    // The moderator's early end — whose result rides the run-side doors —
    // is the application's move, and it still lands.
    let ended = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "budget spent"))
        .await
        .expect("the end applies");
    assert!(matches!(
        ended,
        AgentConversationEntityReply::Applied { .. }
    ));
}

#[tokio::test]
async fn the_deadline_refuses_before_the_flip_and_the_sweep_flips_once() {
    let mut creation = creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["alpha"],
    );
    creation.max_wall_clock_millis = Some(50);
    let fx = created_fixture(creation).await;

    fx.clock.fetch_add(1_000, Ordering::SeqCst);

    // The horizon refuses before the durable flip: the refusal must not
    // depend on whether the settle pass has run yet.
    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "alpha", "too late", 0),
        )
        .await
        .expect_err("the passed deadline refuses");
    assert_eq!(refused.code(), "conversation-expired");
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(
        snapshot.status,
        AgentConversationStatus::Active,
        "the flip belongs to the settle pass, not the refusal"
    );

    // The settle pass owns the flip, exactly once; a second sweep skips the
    // write entirely.
    let progress = fx
        .settle_conversation_at(&conversation_scope())
        .await
        .expect("the settle pass runs");
    assert!(progress.expiry_observed);
    let again = fx
        .settle_conversation_at(&conversation_scope())
        .await
        .expect("the second settle pass runs");
    assert!(!again.expiry_observed, "the flip is absorbing");

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the expired conversation snapshots");
    assert_eq!(snapshot.status, AgentConversationStatus::Expired);
    assert_eq!(
        snapshot.terminal_reason,
        Some(AgentConversationTerminalReason::Expired)
    );

    let still_refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "alpha", "too late", 0),
        )
        .await
        .expect_err("the expired conversation refuses");
    assert_eq!(still_refused.code(), "conversation-expired");
}

#[tokio::test]
async fn the_ring_drops_oldest_and_deduplication_never_rides_it() {
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_messages(2),
        &["p1", "p2", "p3"],
    ))
    .await;

    for (turn, (speaker, body)) in [("p1", "first"), ("p2", "second"), ("p3", "third")]
        .into_iter()
        .enumerate()
    {
        let reply = fx
            .apply_conversation_command_at(
                &conversation_scope(),
                submit_command(0, turn as u32, speaker, body, 0),
            )
            .await
            .expect("the turn records");
        assert!(matches!(
            reply,
            AgentConversationEntityReply::Applied { .. }
        ));
    }

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.turns.len(), 3, "the ledger keeps every turn");
    assert_eq!(snapshot.messages.len(), 2, "the ring holds its ceiling");
    assert_eq!(snapshot.messages_dropped, 1, "the drop is visible");
    assert_eq!(snapshot.messages[0].body, "second");

    // The turn the ring dropped still answers idempotently: deduplication
    // rides the operation log and the ledger, never the ring.
    let replay = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit_command(0, 0, "p1", "first", 0),
        )
        .await
        .expect("the replay is answered");
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.turns.len(), 3, "the replay recorded nothing");
}

#[tokio::test]
async fn a_pre_slice_policy_decodes_with_defaults_and_the_identity_pins_hold() {
    // The 5.1 revision-only shell still decodes, with every ceiling at its
    // serde default — the cross-version contract of the policy payload.
    let decoded: AgentModerationPolicy =
        serde_json::from_value(serde_json::json!({ "revision": 1 }))
            .expect("the revision-only shell decodes");
    assert_eq!(decoded.revision, AgentRevisionNumber::INITIAL);
    assert_eq!(decoded.max_rounds, 4);
    assert_eq!(decoded.max_turns_per_round, 8);
    assert_eq!(decoded.max_messages, 8);
    assert_eq!(decoded.max_message_bytes, 1024);
    assert_eq!(
        decoded.max_turn_tokens,
        Some(rakka_agent::AGENT_CONVERSATION_DEFAULT_MAX_TURN_TOKENS),
        "a policy stored before the per-turn ceiling existed decodes into it"
    );
    assert!(decoded.moderator_may_end_early);
    assert!(decoded.tool.is_none());

    // The operation-kind labels are persisted compatibility surface.
    assert_eq!(
        AgentOperationKind::ConversationTurn.as_label(),
        "conversation-turn"
    );
    assert_eq!(
        AgentOperationKind::ConversationOperation.as_label(),
        "conversation-operation"
    );

    // The turn operation id is pure over its logical coordinates: stable
    // across derivations, sensitive to every input — including the body,
    // whose digest is what keeps a regenerated submission from silently
    // aliasing the recorded turn.
    let conversation = AgentConversationId::new(CONVERSATION).expect("the id is valid");
    let derive = |round, turn, participant: &str, body: &str| {
        conversation_turn_operation_id(
            &tenant(),
            &conversation,
            round,
            turn,
            &agent(participant),
            &conversation_turn_content_digest(body, None),
        )
        .expect("the operation id derives")
    };
    assert_eq!(
        derive(0, 0, "alpha", "hello"),
        derive(0, 0, "alpha", "hello")
    );
    assert_ne!(
        derive(0, 0, "alpha", "hello"),
        derive(0, 0, "alpha", "other")
    );
    assert_ne!(
        derive(0, 0, "alpha", "hello"),
        derive(0, 1, "alpha", "hello")
    );
    assert_ne!(
        derive(0, 0, "alpha", "hello"),
        derive(0, 0, "beta", "hello")
    );

    // The direction is content: the same words steering the protocol
    // differently are different decisions, and each must derive its own
    // identity or a regenerated one would be absorbed as a duplicate.
    let directed = |body: &str, direction: Option<AgentConversationDirection>| {
        conversation_turn_operation_id(
            &tenant(),
            &conversation,
            0,
            0,
            &agent(MODERATOR),
            &conversation_turn_content_digest(body, direction.as_ref()),
        )
        .expect("the operation id derives")
    };
    let close = || Some(AgentConversationDirection::CloseRound);
    let designate = |name: &str| Some(AgentConversationDirection::Designate(agent(name)));
    assert_eq!(directed("next", close()), directed("next", close()));
    assert_ne!(directed("next", close()), directed("next", None));
    assert_ne!(
        directed("next", close()),
        directed("next", designate("beta"))
    );
    assert_ne!(directed("next", designate("beta")), directed("next", None));
    assert_ne!(
        directed("next", designate("beta")),
        directed("next", designate("gamma"))
    );

    // The golden vectors: a persisted operation id must re-derive byte for
    // byte forever, one per direction shape.
    assert_eq!(
        derive(2, 3, "alpha", "hello").as_str(),
        "conversation-turn/acme/design-review/2/3/alpha/\
         7854c55b74459b37aa6fb941d194edd619b425091d8442d9d9c44a33a48fcb72"
    );
    assert_eq!(
        directed("next", close()).as_str(),
        "conversation-turn/acme/design-review/0/0/moderator/\
         829398ee019816a08ad58fcedb4b55a2a695158a3b9c3c0d8c9d360fcab0436c"
    );
    assert_eq!(
        directed("next", designate("beta")).as_str(),
        "conversation-turn/acme/design-review/0/0/moderator/\
         3723686f2b03ee546b9980be44391adac55c617054f848eb8669f1343083f83d"
    );
}

#[tokio::test]
async fn an_early_end_from_a_moderator_without_the_moderation_capability_refuses() {
    // The early end is the one terminalizing operation a caller can reach,
    // and it is wire-reachable through the A2A `end` verb — so it passes the
    // same authority door as the turn
    // ([specification 8.8](../../../docs/plans/rakka-agent/spec.md)). The
    // moderator here is the conversation's durable moderator, at the right
    // round, under a policy that permits the early end; only its envelope
    // changed. Without the door, an agent refused every turn — the designate
    // and close-round that ride `SubmitTurn` included — could still
    // unilaterally terminalize the conversation and unblock the governing
    // task's review gate.
    let policy = AgentModerationPolicy::new(AgentRevisionNumber::INITIAL);
    let fx = created_fixture(creation(policy, &["alpha", "beta"])).await;

    // Republish the moderator without the capability, leaving its task
    // definitions, its lifecycle, and the conversation's record untouched:
    // the door re-derives against the definition now in force.
    let scope =
        rakka_agent::AgentScope::new(tenant(), agent(MODERATOR)).expect("the agent scope is valid");
    let mut narrowed = rakka_agent::AgentAuthorityEnvelope::empty();
    narrowed
        .task_definitions
        .insert(common::task_definition_id());
    let definition = rakka_agent::AgentDefinition::new(
        rakka_agent::AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
        "Resolves customer support tickets end to end.",
        narrowed,
    )
    .expect("the agent definition is valid");
    let mut entity = rakka_agent::AgentEntityStore::new(scope.clone(), fx.agents.clone());
    entity.recover().await.expect("the agent recovers");
    entity
        .apply(rakka_agent::AgentEntityCommand::PublishDefinition {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::DefinitionUpdate,
                &scope,
                "2",
            )
            .expect("the operation id derives"),
            definition: Box::new(definition),
            provenance: Box::new(provenance(2)),
        })
        .await
        .expect("the narrowing definition publishes");

    let refused = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "calling it"))
        .await
        .expect_err("the end door refuses a moderator its definition no longer admits");
    assert_eq!(refused.code(), "conversation-moderation-unauthorized");

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(
        snapshot.status,
        AgentConversationStatus::Active,
        "the refusal terminalized nothing"
    );
}
