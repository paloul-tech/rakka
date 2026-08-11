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

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
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
        )
        .expect("the operation id derives"),
        moderator: agent(moderator),
        expected_round,
        reason: reason.to_string(),
        provenance: Box::new(provenance(1)),
    }
}

async fn created_fixture(creation: AgentConversationCreation) -> Fixture {
    let fx = fixture();
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
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(policy(), &[])),
        )
        .await
        .expect_err("an empty roster refuses");
    assert_eq!(empty.code(), "conversation-participants-invalid");

    let repeated = fixture()
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(policy(), &["alpha", "alpha"])),
        )
        .await
        .expect_err("a repeated participant refuses");
    assert_eq!(repeated.code(), "conversation-participants-invalid");

    let over_cap = fixture()
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
        .apply_conversation_command_at(&conversation_scope(), create_command(oversized_ref))
        .await
        .expect_err("an oversized transcript reference refuses");
    assert_eq!(oversized.code(), "conversation-transcript-ref-invalid");
}

#[tokio::test]
async fn an_unfittable_policy_refuses_at_creation() {
    // Maxed everything: 16 rounds x 16 turns x 128 reserved bytes alone
    // exceeds the state bound, so a conversation under this policy could
    // wedge mid-round on the state-bounds guard — the door is where the
    // arithmetic must hold.
    let maxed = AgentModerationPolicy::new(AgentRevisionNumber::INITIAL)
        .with_max_rounds(16)
        .with_max_turns_per_round(16)
        .with_max_messages(16)
        .with_max_message_bytes(1024);
    let refused = fixture()
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(creation(maxed, &["alpha"])),
        )
        .await
        .expect_err("the unfittable policy refuses");
    assert_eq!(refused.code(), "conversation-policy-too-large");
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
    let fx = created_fixture(creation(
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).without_early_end(),
        &["alpha"],
    ))
    .await;
    let refused = fx
        .apply_conversation_command_at(&conversation_scope(), end_command(0, "premature"))
        .await
        .expect_err("the policy forbids the early end");
    assert_eq!(refused.code(), "conversation-end-not-permitted");
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
