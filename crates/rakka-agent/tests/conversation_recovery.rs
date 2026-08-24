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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::{tenant, Fixture};
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_turn_content_digest, conversation_turn_operation_id, AgentBudgetConsumption,
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

/// Builds the conversational world: a created two-participant round-robin
/// conversation with a token grant.
async fn world() -> Fixture {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.instantiate_conversation_participants(&["moderator", "p1", "p2"])
        .await;
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

/// A history sink that is down until it is healed, then behaves normally.
///
/// The only way to *create* the committed-but-unflushed window on purpose:
/// with a working sink the apply path drains the outbox on every command, so
/// the window this test is named for would never exist.
#[derive(Clone)]
struct HealableHistory {
    inner: rakka_agent::InMemoryAgentConversationHistoryStore,
    down: Arc<AtomicBool>,
}

impl HealableHistory {
    fn down() -> Self {
        Self {
            inner: rakka_agent::InMemoryAgentConversationHistoryStore::new(),
            down: Arc::new(AtomicBool::new(true)),
        }
    }

    fn heal(&self) {
        self.down.store(false, Ordering::SeqCst);
    }

    fn unavailable() -> rakka_agent::AgentConversationError {
        rakka_agent::AgentConversationError::Choreography(Box::new(
            rakka_agent::AgentChoreographyError::Persistence(
                rakka_persistence::DurableError::Store {
                    backend: "healable",
                    message: "the history sink is down".to_string(),
                },
            ),
        ))
    }
}

impl rakka_agent::AgentConversationHistoryStore for HealableHistory {
    fn backend_name(&self) -> &'static str {
        "healable"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentConversationScope,
        entry: &'a rakka_agent::AgentConversationHistoryEntry,
    ) -> rakka_agent::AgentConversationHistoryFuture<'a, ()> {
        Box::pin(async move {
            if self.down.load(Ordering::SeqCst) {
                return Err(Self::unavailable());
            }
            self.inner.append(scope, entry).await
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentConversationScope,
        cursor: rakka_agent::AgentConversationHistoryCursor,
    ) -> rakka_agent::AgentConversationHistoryFuture<'a, rakka_agent::AgentConversationHistoryPage>
    {
        Box::pin(async move {
            if self.down.load(Ordering::SeqCst) {
                return Err(Self::unavailable());
            }
            self.inner.read(scope, cursor).await
        })
    }
}

#[tokio::test]
async fn a_loss_between_the_commit_and_the_history_flush_re_flushes_the_same_slots() {
    // The window the pending-history outbox exists for: the turn committed
    // — ledger record, cursor advance, owed history — and the flush did not
    // land. Recovery flushes the identical entries to the identical slots.
    let fx = world().await;
    let history = HealableHistory::down();
    let mut store = rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        fx.agents.clone(),
        history.clone(),
    );

    // The turn commits and the flush fails, so the window is real rather
    // than assumed.
    let _ = store
        .apply(submit(0, 0, "p1", "opening"), &fx.router, fx.now())
        .await;
    let owed = fx
        .conversation_pending_history(&conversation_scope())
        .await
        .expect("the state loads");
    assert!(owed > 0, "the committed turn left history owed to the sink");

    // The sink comes back. A settle pass — the restart's own work — flushes
    // exactly what was owed, to the slots the transition assigned.
    history.heal();
    let mut recovered = rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        fx.agents.clone(),
        history.clone(),
    );
    recovered
        .recover(fx.now())
        .await
        .expect("the restarted entity recovers");
    recovered
        .settle_side_effects(&fx.router, fx.now())
        .await
        .expect("the owed history flushes");
    assert_eq!(
        fx.conversation_pending_history(&conversation_scope())
            .await
            .expect("the state loads"),
        0,
        "the outbox drained"
    );

    // Re-driving is idempotent: the same slots, appended once each. A
    // re-flush that wrote a *different* entry at an occupied sequence would
    // fail closed rather than overwrite.
    recovered
        .settle_side_effects(&fx.router, fx.now())
        .await
        .expect("a second settle owes nothing");
    // This sink only ever saw what the degraded store owed it — the creation
    // was flushed to the fixture's own sink before the outage — so it holds a
    // *partial* window, and a reader starting from the beginning is told so
    // rather than handed a log quietly missing its first entry.
    let floor = match rakka_agent::AgentConversationHistoryStore::read(
        &history,
        &conversation_scope(),
        rakka_agent::AgentConversationHistoryCursor::start(),
    )
    .await
    {
        Err(rakka_agent::AgentConversationError::HistoryWindowExpired {
            oldest_retained: Some(floor),
        }) => floor,
        other => panic!("a partial window answers its floor, not a short page: {other:?}"),
    };

    let page = rakka_agent::AgentConversationHistoryStore::read(
        &history,
        &conversation_scope(),
        rakka_agent::AgentConversationHistoryCursor::start().resuming_at(floor),
    )
    .await
    .expect("the history reads from the floor it reported");
    let sequences: Vec<u64> = page
        .entries
        .iter()
        .map(|entry| entry.sequence.get())
        .collect();
    let mut unique = sequences.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        sequences.len(),
        unique.len(),
        "each sequence occupied exactly once: {sequences:?}"
    );
    assert!(
        page.entries
            .iter()
            .any(|entry| entry.kind == rakka_agent::AgentConversationHistoryKind::TurnRecorded),
        "the turn committed during the outage reached the sink once it healed: {sequences:?}"
    );
}

#[tokio::test]
async fn the_history_pages_through_its_cursor_without_gaps_or_repeats() {
    // The read cursor is the only way an operator gets at the audit trail,
    // and its paging was never exercised: a page size smaller than the
    // history is the case that matters.
    let fx = world().await;
    for round in 0..3u64 {
        for (turn, speaker) in ["p1", "p2"].into_iter().enumerate() {
            fx.apply_conversation_command_at(
                &conversation_scope(),
                submit(round, turn as u32, speaker, "statement"),
            )
            .await
            .expect("the turn records");
        }
    }
    let _ = fx.settle_conversation_at(&conversation_scope()).await;

    let total = fx.conversation_history.len(&conversation_scope());
    assert!(total > 3, "enough history to need more than one page");

    let mut cursor = rakka_agent::AgentConversationHistoryCursor::start().with_limit(2);
    let mut seen: Vec<u64> = Vec::new();
    for _ in 0..64 {
        let page = rakka_agent::AgentConversationHistoryStore::read(
            &fx.conversation_history,
            &conversation_scope(),
            cursor,
        )
        .await
        .expect("the page reads");
        assert!(page.entries.len() <= 2, "the page honors its limit");
        seen.extend(page.entries.iter().map(|entry| entry.sequence.get()));
        match page.next {
            Some(next) => cursor = next,
            None => break,
        }
    }

    assert_eq!(seen.len(), total, "paging saw every entry exactly once");
    let mut ordered = seen.clone();
    ordered.sort_unstable();
    ordered.dedup();
    assert_eq!(seen, ordered, "in sequence order, with no gaps or repeats");
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
        fx.agents.clone(),
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
