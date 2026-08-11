//! Crash-point sweeps over the turn protocol
//! ([specification 8.11 and 9.8](../../../docs/plans/rakka-agent/spec.md),
//! scenario 43's fault half): owner loss at every durable write of the
//! conversation store converges on one ledger record per coordinate, one
//! cursor advance per turn, and one usage charge — never a duplicated turn,
//! never a double-charged budget.
//!
//! Each iteration builds a fresh world, arms exactly one store at one write,
//! drives to the loss, survives, and re-drives the same operation ids: the
//! deduplicated command inbox, the dense turn ledger's echo, and the
//! idempotent slot-keyed history appends are what make every window
//! converge.

mod common;

use common::{tenant, Fixture};
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_turn_body_digest, conversation_turn_operation_id, AgentBudgetConsumption,
    AgentConversationCompletionRule, AgentConversationCreation, AgentConversationEntityCommand,
    AgentConversationId, AgentConversationMode, AgentConversationScope, AgentConversationStatus,
    AgentConversationTurnSubmit, AgentId, AgentModerationPolicy, AgentRevisionNumber, AgentTaskId,
};

const CONVERSATION: &str = "recovery-review";

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

fn submit(round: u64, turn: u32, participant: &str, body: &str) -> AgentConversationEntityCommand {
    let mut usage = AgentBudgetConsumption::zero();
    usage.tokens = 10;
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
            direction: None,
            usage,
        }),
    }
}

/// Builds the conversational world: a created two-participant round-robin
/// conversation with a token grant.
async fn world() -> Fixture {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.apply_conversation_command_at(
        &conversation_scope(),
        AgentConversationEntityCommand::Create {
            operation_id: rakka_agent::conversation_create_operation_id(
                &tenant(),
                &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
            )
            .expect("the operation id derives"),
            creation: Box::new(AgentConversationCreation {
                moderator: agent("moderator"),
                participants: vec![agent("p1"), agent("p2")],
                mode: AgentConversationMode::RoundRobin,
                completion: AgentConversationCompletionRule::ModeratorDecides,
                policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
                task: AgentTaskId::new("recovery-task").expect("the task id is valid"),
                tokens: Some(1_000),
                max_wall_clock_millis: None,
                transcript_ref: Some("artifact://transcripts/recovery".to_string()),
            }),
        },
    )
    .await
    .expect("the conversation creates");
    fx
}

/// Drives one full round — both turns and the settle passes that flush what
/// each committed — tolerating injected crashes: a crashed pass is exactly
/// an owner death mid-flow.
async fn drive_round(fx: &Fixture) {
    let _ = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "opening"))
        .await;
    let _ = fx.settle_conversation_at(&conversation_scope()).await;
    let _ = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 1, "p2", "reply"))
        .await;
    let _ = fx.settle_conversation_at(&conversation_scope()).await;
}

/// Re-drives after survival until quiescent, then asserts the converged
/// truth: one ledger record per coordinate, the cursor advanced exactly one
/// round, the usage charged exactly once, and no duplicated history slot.
async fn assert_converged(fx: &Fixture) {
    // The retried commands either apply fresh (the crash preceded their
    // commit) or answer from the operation log and the ledger — both
    // converge.
    let _ = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "opening"))
        .await;
    let _ = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 1, "p2", "reply"))
        .await;
    for _round in 0..3 {
        let _ = fx.settle_conversation_at(&conversation_scope()).await;
    }

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.status, AgentConversationStatus::Active);
    assert_eq!(snapshot.turns.len(), 2, "one ledger record per coordinate");
    assert_eq!(snapshot.round, 1, "the round advanced exactly once");
    assert_eq!(snapshot.turn_in_round, 0);
    assert_eq!(snapshot.current_speaker, Some(agent("p1")));
    assert_eq!(
        snapshot.budgets.consumed.tokens, 20,
        "each turn's usage charged exactly once across the loss"
    );
    assert_eq!(
        snapshot.transcript_ref.as_deref(),
        Some("artifact://transcripts/recovery"),
        "the transcript reference survives every window"
    );

    // The idempotent slot-keyed history converged too: created, two turns,
    // one round advance — each sequence occupied exactly once, and a
    // re-driven flush of a different entry at an occupied slot would have
    // failed the drive loudly.
    assert_eq!(fx.conversation_history.len(&conversation_scope()), 4);
}

/// Counts the durable writes one crash-free round attempts on the
/// conversation store, so the sweep below covers every real write and knows
/// when it has run past the flow's end.
async fn reference_writes() -> usize {
    let fx = world().await;
    fx.conversations.reset_writes();
    drive_round(&fx).await;
    fx.conversations.writes()
}

#[tokio::test]
async fn the_round_converges_across_every_conversation_store_crash_point() {
    let writes = reference_writes().await;
    assert!(
        writes >= 2,
        "the round writes the conversation store at least twice (one commit per turn)"
    );
    for point in 1..=writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world().await;
            fx.conversations.reset_writes();
            fx.conversations.crash_at(point, window);
            drive_round(&fx).await;
            fx.conversations.assert_crash_fired(point, window);
            fx.conversations.survive();
            assert_converged(&fx).await;
        }
    }
}

#[tokio::test]
async fn a_loss_between_the_commit_and_the_history_flush_re_flushes_the_same_slots() {
    // The window the pending-history outbox exists for: the turn committed
    // — ledger record, cursor advance, owed history — and the owner died
    // before the flush. Recovery flushes the identical entries to the
    // identical slots.
    let fx = world().await;
    fx.apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "opening"))
        .await
        .expect("the turn commits");
    // No settle ran: whatever the apply-path flush left behind, the next
    // drive is a restart that owes at most the same idempotent appends.
    assert_converged(&fx).await;
}

#[tokio::test]
async fn a_resident_store_that_loses_a_compare_and_set_recovers_and_keeps_serving() {
    // A conversation has two writers by construction — the resident sharded
    // entity, which holds one store for its whole residency, and the A2A
    // service, which builds its own store on any node. The loser of a race
    // between them must reload the authoritative record, not answer
    // `exchange-not-recovered` until it happens to passivate.
    let fx = world().await;
    let mut resident = rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        fx.conversation_history.clone(),
    );
    resident
        .recover(fx.now())
        .await
        .expect("the resident loads");

    // The other writer commits the round's first turn — the same turn the
    // resident is about to be handed — so the resident's cached revision is
    // now stale.
    fx.apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "opening"))
        .await
        .expect("the other writer's turn commits");

    // Computed against the stale cache the turn is legitimate, so the
    // resident reaches its compare-and-set and loses it; the host drops the
    // record the transition was computed from.
    let conflict = resident
        .apply(submit(0, 0, "p1", "opening"), &fx.router, fx.now())
        .await
        .expect_err("the stale resident loses the compare-and-set");
    assert!(
        !conflict.is_domain_refusal(),
        "a lost race is an infrastructure fault, not a decision: {conflict}"
    );

    // The retry is the whole point: the same resident store reloads the
    // authoritative record and answers the redelivery from the ledger.
    // Before the fix this answered `exchange-not-recovered` for the rest of
    // the entity's residency.
    let reply = resident
        .apply(submit(0, 0, "p1", "opening"), &fx.router, fx.now())
        .await
        .expect("the resident recovers and serves the retry");
    assert!(matches!(
        reply,
        rakka_agent::AgentConversationEntityReply::Duplicate { .. }
    ));

    // And it keeps serving live traffic, not just replays.
    let reply = resident
        .apply(submit(0, 1, "p2", "reply"), &fx.router, fx.now())
        .await
        .expect("the recovered resident serves the next turn");
    assert!(matches!(
        reply,
        rakka_agent::AgentConversationEntityReply::Applied { .. }
    ));

    // And the conversation converged exactly as an uncontended round does.
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.turns.len(), 2, "one ledger record per coordinate");
    assert_eq!(snapshot.round, 1, "the round advanced exactly once");
    assert_eq!(snapshot.budgets.consumed.tokens, 20);
}
