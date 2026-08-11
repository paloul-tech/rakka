//! Turn ownership, ordering, and the layered deduplication of the moderated
//! turn protocol
//! ([specification 8.11](../../../docs/plans/rakka-agent/spec.md),
//! scenario 43's ordering half): only the current authorized participant may
//! submit, a duplicate converges — from the operation log inside its window
//! and from the dense turn ledger past it, even after the conversation ended
//! — and a regenerated or superseded submission refuses loudly.
//!
//! Every command rebuilds the entity from durable state — each call is
//! already a restart.

mod common;

use common::{tenant, Fixture};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_turn_body_digest, conversation_turn_operation_id, AgentBudgetConsumption,
    AgentConversationCompletionRule, AgentConversationCreation, AgentConversationDirection,
    AgentConversationEntityCommand, AgentConversationEntityReply, AgentConversationId,
    AgentConversationMode, AgentConversationScope, AgentConversationStatus,
    AgentConversationTerminalReason, AgentConversationTurnSubmit, AgentId, AgentModerationPolicy,
    AgentRevisionNumber, AgentTaskId,
};

const CONVERSATION: &str = "panel-debate";
const MODERATOR: &str = "moderator";

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

fn create_command(
    mode: AgentConversationMode,
    completion: AgentConversationCompletionRule,
    policy: AgentModerationPolicy,
    participants: &[&str],
) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::Create {
        operation_id: rakka_agent::conversation_create_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
        )
        .expect("the operation id derives"),
        creation: Box::new(AgentConversationCreation {
            moderator: agent(MODERATOR),
            participants: participants.iter().map(|name| agent(name)).collect(),
            mode,
            completion,
            policy,
            task: AgentTaskId::new("debate-task").expect("the task id is valid"),
            tokens: None,
            max_wall_clock_millis: None,
            transcript_ref: None,
        }),
    }
}

fn submit(
    round: u64,
    turn: u32,
    participant: &str,
    body: &str,
    direction: Option<AgentConversationDirection>,
) -> AgentConversationEntityCommand {
    AgentConversationEntityCommand::SubmitTurn {
        operation_id: conversation_turn_operation_id(
            &tenant(),
            &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
            round,
            turn,
            &agent(participant),
            &conversation_turn_body_digest(body),
        )
        .expect("the operation id derives"),
        submit: Box::new(AgentConversationTurnSubmit {
            round,
            turn,
            participant: agent(participant),
            body: body.to_string(),
            direction,
            usage: AgentBudgetConsumption::zero(),
        }),
    }
}

async fn created(
    mode: AgentConversationMode,
    completion: AgentConversationCompletionRule,
    policy: AgentModerationPolicy,
    participants: &[&str],
) -> Fixture {
    let fx = fixture();
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            create_command(mode, completion, policy, participants),
        )
        .await
        .expect("the conversation creates");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    fx
}

#[tokio::test]
async fn a_round_robin_conversation_advances_ownership_in_roster_order() {
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2", "p3"],
    )
    .await;

    for (turn, speaker) in ["p1", "p2", "p3"].into_iter().enumerate() {
        let snapshot = fx
            .conversation_snapshot_at(&conversation_scope())
            .await
            .expect("the conversation snapshots");
        assert_eq!(snapshot.current_speaker, Some(agent(speaker)));
        let reply = fx
            .apply_conversation_command_at(
                &conversation_scope(),
                submit(0, turn as u32, speaker, "position", None),
            )
            .await
            .expect("the turn records");
        assert!(matches!(
            reply,
            AgentConversationEntityReply::Applied { .. }
        ));
    }

    // The round closed and ownership returned to the head of the roster.
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.round, 1);
    assert_eq!(snapshot.turn_in_round, 0);
    assert_eq!(snapshot.current_speaker, Some(agent("p1")));
    assert_eq!(snapshot.turns.len(), 3);
}

#[tokio::test]
async fn only_the_current_participant_may_submit() {
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2"],
    )
    .await;

    let wrong_member = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, "p2", "out of turn", None),
        )
        .await
        .expect_err("a roster member out of turn refuses");
    assert_eq!(wrong_member.code(), "conversation-not-your-turn");

    let stranger = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, "intruder", "hello", None),
        )
        .await
        .expect_err("a non-participant refuses");
    assert_eq!(stranger.code(), "conversation-not-participant");

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert!(snapshot.turns.is_empty(), "a refusal records nothing");
    assert_eq!(snapshot.turn_in_round, 0);
}

#[tokio::test]
async fn a_future_turn_refuses_and_the_corrected_submit_still_lands() {
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2"],
    )
    .await;

    let early = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 1, "p2", "jumping ahead", None),
        )
        .await
        .expect_err("a future coordinate refuses");
    assert_eq!(early.code(), "conversation-turn-out-of-order");

    // Nothing was recorded for the refused coordinate, so the in-order
    // submissions arrive untainted — including the very turn that refused.
    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "opening", None))
        .await
        .expect("the in-order turn records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 1, "p2", "jumping ahead", None),
        )
        .await
        .expect("the corrected turn records under the same operation id");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
}

#[tokio::test]
async fn a_duplicate_submit_in_window_answers_the_original_outcome() {
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2"],
    )
    .await;

    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "opening", None))
        .await
        .expect("the turn records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));

    let replay = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "opening", None))
        .await
        .expect("the replay is answered");
    let AgentConversationEntityReply::Duplicate { outcome } = replay else {
        panic!("a replayed turn answers Duplicate, got {replay:?}");
    };
    assert_eq!(outcome.turns_recorded, 1, "no second turn recorded");
}

#[tokio::test]
async fn a_past_window_replay_echoes_the_recorded_turn_even_after_the_end() {
    // 14 rounds x 5 participants = 70 turns: enough to evict the first
    // turn's operation from the bounded 64-entry log, and — under the
    // all-rounds completion — to end the conversation, so the echo is
    // proven past the window *and* past the terminal guard at once.
    let roster = ["p1", "p2", "p3", "p4", "p5"];
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::AllRounds,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL)
            .with_max_rounds(14)
            .with_max_turns_per_round(5)
            .with_max_message_bytes(256),
        &roster,
    )
    .await;

    for round in 0..14u64 {
        for (turn, speaker) in roster.into_iter().enumerate() {
            let body = format!("round {round} statement from {speaker}");
            let reply = fx
                .apply_conversation_command_at(
                    &conversation_scope(),
                    submit(round, turn as u32, speaker, &body, None),
                )
                .await
                .expect("the turn records");
            assert!(matches!(
                reply,
                AgentConversationEntityReply::Applied { .. }
            ));
        }
    }
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.status, AgentConversationStatus::Ended);
    assert_eq!(
        snapshot.terminal_reason,
        Some(AgentConversationTerminalReason::RoundsComplete),
        "completing the final round ends the conversation in the same commit"
    );
    assert_eq!(snapshot.turns.len(), 70);

    // The redelivery of the very first turn — identical coordinate,
    // identical bytes — converges from the ledger, past the operation-log
    // window and past the terminal guard.
    let replay = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, "p1", "round 0 statement from p1", None),
        )
        .await
        .expect("the past-window replay is answered");
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.turns.len(), 70, "the echo recorded nothing");

    // A *regenerated* submission — same coordinate, different content — is
    // a new, illegal decision: echoing the recorded turn would silently
    // persuade the speaker its new content was recorded.
    let regenerated = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, "p1", "a rewritten opening", None),
        )
        .await
        .expect_err("a regenerated submission refuses loudly");
    assert_eq!(regenerated.code(), "conversation-turn-content-mismatch");

    // A different speaker claiming the recorded coordinate was superseded
    // by the protocol's own advance.
    let superseded = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, "p2", "round 0 statement from p1", None),
        )
        .await
        .expect_err("a foreign speaker's claim refuses");
    assert_eq!(superseded.code(), "conversation-turn-superseded");
}

#[tokio::test]
async fn moderator_directed_designation_owns_the_next_turn() {
    let fx = created(
        AgentConversationMode::ModeratorDirected,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_turns_per_round(3),
        &["p1", "p2"],
    )
    .await;

    // A moderator turn must direct what follows.
    let undirected = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, MODERATOR, "who wants to start?", None),
        )
        .await
        .expect_err("a directionless moderator turn refuses");
    assert_eq!(undirected.code(), "conversation-direction-required");

    // A designation of someone off the roster refuses.
    let unknown = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(
                0,
                0,
                MODERATOR,
                "over to you",
                Some(AgentConversationDirection::Designate(agent("intruder"))),
            ),
        )
        .await
        .expect_err("an off-roster designation refuses");
    assert_eq!(unknown.code(), "conversation-designate-unknown");

    // The designation is the durable owner fact: p1 owns the next turn.
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(
                0,
                0,
                MODERATOR,
                "p1, your opening",
                Some(AgentConversationDirection::Designate(agent("p1"))),
            ),
        )
        .await
        .expect("the designating turn records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.designated, Some(agent("p1")));
    assert_eq!(snapshot.current_speaker, Some(agent("p1")));

    // The undesignated participant does not own it.
    let wrong = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 1, "p2", "interjecting", None),
        )
        .await
        .expect_err("the undesignated participant refuses");
    assert_eq!(wrong.code(), "conversation-not-your-turn");

    // A participant turn may not carry a direction.
    let directed = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(
                0,
                1,
                "p1",
                "my opening",
                Some(AgentConversationDirection::CloseRound),
            ),
        )
        .await
        .expect_err("a directed participant turn refuses");
    assert_eq!(directed.code(), "conversation-direction-forbidden");

    // The designated turn lands and ownership returns to the moderator.
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 1, "p1", "my opening", None),
        )
        .await
        .expect("the designated turn records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.designated, None);
    assert_eq!(snapshot.current_speaker, Some(agent(MODERATOR)));

    // At the rim only closing the round is accepted: a designation would
    // land its turn past the ceiling.
    let rim = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(
                0,
                2,
                MODERATOR,
                "one more?",
                Some(AgentConversationDirection::Designate(agent("p2"))),
            ),
        )
        .await
        .expect_err("a designation at the rim refuses");
    assert_eq!(rim.code(), "conversation-turns-exhausted");
    let reply = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(
                0,
                2,
                MODERATOR,
                "closing the round",
                Some(AgentConversationDirection::CloseRound),
            ),
        )
        .await
        .expect("the closing turn records");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.round, 1);
    assert_eq!(snapshot.turn_in_round, 0);
    assert_eq!(snapshot.current_speaker, Some(agent(MODERATOR)));
}

#[tokio::test]
async fn rounds_complete_ends_or_exhausts_by_completion_rule() {
    // All-rounds: completing the final permitted round ends the
    // conversation in the same compare-and-set — completion beats
    // exhaustion, so no turn ever refuses rounds-exhausted here.
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::AllRounds,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_rounds(1),
        &["p1", "p2"],
    )
    .await;
    for (turn, speaker) in ["p1", "p2"].into_iter().enumerate() {
        let reply = fx
            .apply_conversation_command_at(
                &conversation_scope(),
                submit(0, turn as u32, speaker, "the one round", None),
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
    assert_eq!(snapshot.status, AgentConversationStatus::Ended);
    assert_eq!(
        snapshot.terminal_reason,
        Some(AgentConversationTerminalReason::RoundsComplete)
    );

    // Moderator-decides: the cursor parks at the ceiling, the status stays
    // active, and further turns refuse under the stable code — never a
    // silent park; the early end is the moderator's move.
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_rounds(1),
        &["p1", "p2"],
    )
    .await;
    for (turn, speaker) in ["p1", "p2"].into_iter().enumerate() {
        let reply = fx
            .apply_conversation_command_at(
                &conversation_scope(),
                submit(0, turn as u32, speaker, "the one round", None),
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
    assert_eq!(snapshot.status, AgentConversationStatus::Active);
    assert_eq!(snapshot.round, 1);
    let exhausted = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(1, 0, "p1", "another round?", None),
        )
        .await
        .expect_err("the parked cursor refuses further turns");
    assert_eq!(exhausted.code(), "conversation-rounds-exhausted");
}
