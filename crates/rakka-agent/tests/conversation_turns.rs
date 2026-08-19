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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::{tenant, Fixture};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    conversation_turn_content_digest, conversation_turn_operation_id, AgentBudgetConsumption,
    AgentConversationCompletionRule, AgentConversationCreation, AgentConversationDirection,
    AgentConversationEntityCommand, AgentConversationEntityReply, AgentConversationId,
    AgentConversationMode, AgentConversationScope, AgentConversationStatus,
    AgentConversationTerminalReason, AgentConversationTurnSubmit, AgentId, AgentModerationPolicy,
    AgentRevisionNumber, AgentScope, AgentTaskId,
};
use rakka_persistence::{
    DurableError, DurableStateStore, PersistenceId, Revision, StateRecord, StoreFuture,
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
            &conversation_turn_content_digest(body, direction.as_ref()),
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
    // The roster admits these speakers here; their definitions are what admit
    // them to moderated work at all, and the turn door reads both.
    let mut roster = vec![MODERATOR];
    roster.extend_from_slice(participants);
    fx.instantiate_conversation_participants(&roster).await;
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
async fn a_speaker_without_the_moderation_capability_is_refused_at_the_turn_door() {
    // The M5 setup-cannot-widen bullet at the conversation's own door
    // ([specification 8.8](../../../docs/plans/rakka-agent/spec.md)). The
    // roster and the envelope answer different questions, and both must say
    // yes: `p1` here is the roster's current speaker, at the right coordinate,
    // inside every budget — and its definition never granted `Moderation`.
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2"],
    )
    .await;

    // Republish `p1` without the capability, leaving everything else — its
    // task definitions, its lifecycle, the roster — exactly as it was. A
    // republish rather than a different fixture on purpose: the door must
    // re-derive against the definition *now in force*, so an agent that was
    // authorized when the conversation was created and is not any more stops
    // speaking, without the conversation having had to notice the update.
    let scope = AgentScope::new(tenant(), AgentId::new("p1").expect("the agent id is valid"))
        .expect("the agent scope is valid");
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
            provenance: Box::new(common::provenance(2)),
        })
        .await
        .expect("the narrowing definition publishes");

    let refused = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, "p1", "speaking without authority", None),
        )
        .await
        .expect_err("the turn door refuses a speaker its definition never admitted");
    assert_eq!(refused.code(), "conversation-moderation-unauthorized");

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert!(snapshot.turns.is_empty(), "a refusal records nothing");
    assert_eq!(snapshot.turn_in_round, 0, "and moves no cursor");
    assert_eq!(
        snapshot.current_speaker,
        Some(agent("p1")),
        "the roster is unchanged: the refusal is about authority, not membership"
    );

    // An agent with no durable record at all is refused the same way, because
    // no record means no definition means no grant.
    let ghosted = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, "intruder", "hello", None),
        )
        .await
        .expect_err("a non-participant still refuses on the roster first");
    assert_eq!(
        ghosted.code(),
        "conversation-not-participant",
        "the roster answers before the envelope: an outsider is not told whether it could have \
         spoken had it been admitted"
    );
}

#[tokio::test]
async fn a_suspended_speaker_is_refused_at_the_turn_door() {
    // Lifecycle is part of the authority the door reads
    // ([specification 8.8](../../../docs/plans/rakka-agent/spec.md)) — the
    // rule every sibling envelope door already holds: suspension withdraws
    // assignments, board claims, and effect dispatch immediately, and the
    // moderated turn must not be the one surface that keeps listening.
    // `p1` here is the roster's current speaker, at the right coordinate,
    // with its `Moderation` grant intact — only its lifecycle changed.
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2"],
    )
    .await;

    let scope = AgentScope::new(tenant(), agent("p1")).expect("the agent scope is valid");
    let mut entity = rakka_agent::AgentEntityStore::new(scope.clone(), fx.agents.clone());
    entity.recover().await.expect("the agent recovers");
    entity
        .apply(rakka_agent::AgentEntityCommand::Suspend {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::LifecycleCommand,
                &scope,
                "suspend-1",
            )
            .expect("the operation id derives"),
            expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
            provenance: Box::new(common::provenance(2)),
        })
        .await
        .expect("the suspension applies");

    let refused = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "position", None))
        .await
        .expect_err("the turn door refuses a suspended speaker");
    assert_eq!(refused.code(), "conversation-moderation-unauthorized");

    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert!(snapshot.turns.is_empty(), "a refusal records nothing");

    // Resuming restores exactly what suspension withdrew: the same turn —
    // same operation id, same digest — now lands, because a rejected
    // transition left no trace to collide with.
    let mut entity = rakka_agent::AgentEntityStore::new(scope.clone(), fx.agents.clone());
    entity.recover().await.expect("the agent recovers");
    entity
        .apply(rakka_agent::AgentEntityCommand::Resume {
            operation_id: rakka_agent::AgentOperationId::for_agent(
                rakka_agent::AgentOperationKind::LifecycleCommand,
                &scope,
                "resume-1",
            )
            .expect("the operation id derives"),
            expected_lifecycle_revision: AgentRevisionNumber::new(2),
            provenance: Box::new(common::provenance(3)),
        })
        .await
        .expect("the resume applies");
    let reply = fx
        .apply_conversation_command_at(&conversation_scope(), submit(0, 0, "p1", "position", None))
        .await
        .expect("the resumed speaker's turn lands");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
}

/// The fixture's agents store behind an outage switch: reads fail while the
/// switch is thrown, writes pass through untouched. The moderation door's
/// cross-entity load is the conversation's only read of this store.
#[derive(Clone)]
struct OutageAgents {
    inner: common::AgentStore,
    down: Arc<AtomicBool>,
}

impl DurableStateStore<rakka_agent::AgentEntityState> for OutageAgents {
    fn backend_name(&self) -> &'static str {
        "outage-agents"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<rakka_agent::AgentEntityState>>> {
        if self.down.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(DurableError::store(
                    "outage-agents",
                    "the agents store is unreachable",
                ))
            });
        }
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: rakka_agent::AgentEntityState,
    ) -> StoreFuture<'a, StateRecord<rakka_agent::AgentEntityState>> {
        self.inner
            .compare_and_set(persistence_id, expected_revision, state)
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        self.inner.delete(persistence_id, expected_revision)
    }
}

fn outage_resident(
    fx: &Fixture,
    down: &Arc<AtomicBool>,
) -> rakka_agent::AgentConversationEntityStore<
    common::ConversationStore,
    OutageAgents,
    rakka_agent::InMemoryAgentConversationHistoryStore,
> {
    rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        OutageAgents {
            inner: fx.agents.clone(),
            down: down.clone(),
        },
        fx.conversation_history.clone(),
    )
}

#[tokio::test]
async fn an_agents_store_read_fault_answers_as_a_retryable_error_not_a_wire_refusal() {
    // The moderation door's cross-entity load failing is a read fault the
    // very next attempt may serve — the variant's own contract says the
    // caller may retry it. A domain refusal it is not: the A2A surface maps
    // refusals to a definitive wire `Rejected`, and a conforming caller
    // abandons a rejected turn — which, before the classification fix,
    // abandoned a turn an agents-store blip refused and the next attempt
    // would have landed.
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2"],
    )
    .await;
    let down = Arc::new(AtomicBool::new(true));
    let mut resident = outage_resident(&fx, &down);
    resident
        .recover(fx.now())
        .await
        .expect("the conversation loads");

    // Every local guard admits this turn — right speaker, right coordinate
    // — so it reaches the read, and the outage surfaces as the fault.
    let fault = resident
        .apply(submit(0, 0, "p1", "opening", None), &fx.router, fx.now())
        .await
        .expect_err("the read fault surfaces");
    assert_eq!(fault.code(), "conversation-participant-record-unreadable");
    assert!(
        !fault.is_domain_refusal(),
        "a read fault is retryable infrastructure, never a decision: {fault}"
    );

    // The very next attempt after the store heals lands the same turn — the
    // retry the classification exists to keep possible.
    down.store(false, Ordering::SeqCst);
    let reply = resident
        .apply(submit(0, 0, "p1", "opening", None), &fx.router, fx.now())
        .await
        .expect("the healed retry lands");
    assert!(matches!(
        reply,
        AgentConversationEntityReply::Applied { .. }
    ));
}

#[tokio::test]
async fn local_guards_answer_definitively_while_the_agents_store_is_down() {
    // The conversation's own guards precede the cross-entity moderation
    // read, so a turn a local guard refuses gets its definitive answer from
    // the conversation's own record alone — and never pays the durable
    // load. Before the reorder, every refusal below surfaced as the
    // retryable `conversation-participant-record-unreadable`, and a
    // well-behaved caller retried forever against turns that can never
    // land.
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
        &["p1", "p2"],
    )
    .await;
    let down = Arc::new(AtomicBool::new(true));
    let mut resident = outage_resident(&fx, &down);
    resident
        .recover(fx.now())
        .await
        .expect("the conversation loads");

    let stranger = resident
        .apply(
            submit(0, 0, "intruder", "hello", None),
            &fx.router,
            fx.now(),
        )
        .await
        .expect_err("a non-participant refuses from the roster alone");
    assert_eq!(stranger.code(), "conversation-not-participant");

    let wrong_owner = resident
        .apply(submit(0, 0, "p2", "cutting in", None), &fx.router, fx.now())
        .await
        .expect_err("the owner fence refuses from the cursor alone");
    assert_eq!(wrong_owner.code(), "conversation-not-your-turn");

    let early = resident
        .apply(submit(1, 0, "p1", "previewing", None), &fx.router, fx.now())
        .await
        .expect_err("the coordinate fence refuses from the cursor alone");
    assert_eq!(early.code(), "conversation-turn-out-of-order");

    // The moderator ends the conversation through the healthy fixture path,
    // and a fresh resident materialized after the end — agents store still
    // down — answers the terminal fence, not the read fault.
    let ended = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            AgentConversationEntityCommand::EndEarly {
                operation_id: rakka_agent::conversation_end_operation_id(
                    &tenant(),
                    &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
                    0,
                    "wrapped",
                )
                .expect("the operation id derives"),
                moderator: agent(MODERATOR),
                expected_round: 0,
                reason: "wrapped".to_string(),
                provenance: Box::new(common::provenance(9)),
            },
        )
        .await
        .expect("the moderator ends the conversation");
    assert!(matches!(
        ended,
        AgentConversationEntityReply::Applied { .. }
    ));

    let mut resident = outage_resident(&fx, &down);
    resident
        .recover(fx.now())
        .await
        .expect("the ended conversation loads");
    let terminal = resident
        .apply(submit(0, 0, "p1", "opening", None), &fx.router, fx.now())
        .await
        .expect_err("the terminal fence refuses from the conversation's own record");
    assert_eq!(terminal.code(), "conversation-ended");
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

/// A history sink that is simply down: every append fails as an unavailable
/// backend, distinguishably from the entity's own outbox refusal.
#[derive(Clone, Default)]
struct UnavailableHistory;

impl UnavailableHistory {
    fn unavailable() -> rakka_agent::AgentConversationError {
        rakka_agent::AgentConversationError::Choreography(Box::new(
            rakka_agent::AgentChoreographyError::Persistence(
                rakka_persistence::DurableError::Store {
                    backend: "unavailable",
                    message: "the history sink is down".to_string(),
                },
            ),
        ))
    }
}

impl rakka_agent::AgentConversationHistoryStore for UnavailableHistory {
    fn backend_name(&self) -> &'static str {
        "unavailable"
    }

    fn append<'a>(
        &'a self,
        _scope: &'a AgentConversationScope,
        _entry: &'a rakka_agent::AgentConversationHistoryEntry,
    ) -> rakka_agent::AgentConversationHistoryFuture<'a, ()> {
        Box::pin(async move { Err(Self::unavailable()) })
    }

    fn read<'a>(
        &'a self,
        _scope: &'a AgentConversationScope,
        _cursor: rakka_agent::AgentConversationHistoryCursor,
    ) -> rakka_agent::AgentConversationHistoryFuture<'a, rakka_agent::AgentConversationHistoryPage>
    {
        Box::pin(async move { Err(Self::unavailable()) })
    }
}

#[tokio::test]
async fn a_redelivered_turn_converges_while_the_history_sink_is_down() {
    // History is observability; the durable turn ledger is the truth. A
    // redelivery whose coordinate is already in the ledger writes nothing at
    // all, so it must converge even while the sink is unavailable — the
    // retrying caller has no other way to learn its turn landed.
    let roster = ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"];
    let fx = created(
        AgentConversationMode::RoundRobin,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL)
            .with_max_rounds(16)
            .with_max_turns_per_round(8)
            .with_max_messages(1)
            .with_max_message_bytes(64),
        &roster,
    )
    .await;

    // Nine rounds of eight is 72 operations: enough to evict the very first
    // turn from the bounded 64-entry log, so its redelivery is answered by
    // the dense ledger rather than the operation log.
    let body = |round: u64, speaker: &str| format!("r{round} {speaker}");
    for round in 0..9u64 {
        for (turn, speaker) in roster.into_iter().enumerate() {
            fx.apply_conversation_command_at(
                &conversation_scope(),
                submit(round, turn as u32, speaker, &body(round, speaker), None),
            )
            .await
            .expect("the turn records");
        }
    }

    // The same durable conversation, now served by an entity whose history
    // sink is down. Each committed turn owes entries the flush cannot
    // deliver, so the pending outbox fills.
    let mut degraded = rakka_agent::AgentConversationEntityStore::new(
        conversation_scope(),
        fx.conversations.clone(),
        fx.agents.clone(),
        UnavailableHistory,
    );
    for round in 9..16u64 {
        for (turn, speaker) in roster.into_iter().enumerate() {
            // Early attempts commit and fail only at the flush; once the
            // outbox has no headroom the guard refuses before the commit.
            let command = submit(round, turn as u32, speaker, &body(round, speaker), None);
            let _ = degraded.apply(command, &fx.router, fx.now()).await;
        }
    }

    // The contrast that proves the ordering, at this exact moment on this
    // exact store. A *new* turn at the cursor the protocol actually holds
    // is refused, and records nothing — the outbox guard turned it away
    // before any commit.
    let parked = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    let speaker = parked
        .current_speaker
        .clone()
        .expect("the parked conversation still names its next speaker");
    let fresh = degraded
        .apply(
            submit(
                parked.round,
                parked.turn_in_round,
                speaker.as_str(),
                "a new decision",
                None,
            ),
            &fx.router,
            fx.now(),
        )
        .await
        .expect_err("a new turn refuses while the sink is down");
    assert!(
        !fresh.is_domain_refusal() || fresh.code() == "conversation-history-backlog",
        "the refusal is the history outbox, not a protocol decision: {fresh}"
    );
    let after = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(
        after.turns.len(),
        parked.turns.len(),
        "the refused turn committed nothing"
    );

    // …while the redelivery of a turn the ledger already holds converges,
    // because it writes nothing and owes the sink nothing.
    let replay = degraded
        .apply(
            submit(0, 0, "p1", &body(0, "p1"), None),
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the past-window redelivery converges through the outage");
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));

    // And a regenerated submission still refuses loudly through the outage —
    // the echo answers honestly, it does not wave everything through.
    let regenerated = degraded
        .apply(
            submit(0, 0, "p1", "a rewritten opening", None),
            &fx.router,
            fx.now(),
        )
        .await
        .expect_err("a regenerated submission refuses through the outage too");
    assert_eq!(regenerated.code(), "conversation-turn-content-mismatch");
}

#[tokio::test]
async fn a_round_never_records_more_turns_than_its_ceiling_declares() {
    // The ceiling is a ceiling on records. Enforcing it only where the
    // moderator designates let a round record one turn more than the policy
    // declared — billing a turn the operator never admitted, and eroding
    // the per-round ledger reserve the creation arithmetic holds.
    for max_turns in [2u32, 3, 4, 8] {
        let fx = created(
            AgentConversationMode::ModeratorDirected,
            AgentConversationCompletionRule::ModeratorDecides,
            AgentModerationPolicy::new(AgentRevisionNumber::INITIAL)
                .with_max_rounds(2)
                .with_max_turns_per_round(max_turns),
            &["p1", "p2"],
        )
        .await;

        // Drive one round to its close, taking whichever move the protocol
        // admits: designate while there is room for the closing turn, close
        // the round once there is not.
        for _ in 0..(max_turns + 4) {
            let snapshot = fx
                .conversation_snapshot_at(&conversation_scope())
                .await
                .expect("the conversation snapshots");
            if snapshot.round > 0 {
                break;
            }
            let Some(speaker) = snapshot.current_speaker.clone() else {
                break;
            };
            let moderator = speaker == agent(MODERATOR);
            let direction = if !moderator {
                None
            } else if fx
                .apply_conversation_command_at(
                    &conversation_scope(),
                    submit(
                        snapshot.round,
                        snapshot.turn_in_round,
                        speaker.as_str(),
                        "designating",
                        Some(AgentConversationDirection::Designate(agent("p1"))),
                    ),
                )
                .await
                .is_ok()
            {
                continue;
            } else {
                Some(AgentConversationDirection::CloseRound)
            };
            if fx
                .apply_conversation_command_at(
                    &conversation_scope(),
                    submit(
                        snapshot.round,
                        snapshot.turn_in_round,
                        speaker.as_str(),
                        "speaking",
                        direction,
                    ),
                )
                .await
                .is_err()
            {
                break;
            }
        }

        let snapshot = fx
            .conversation_snapshot_at(&conversation_scope())
            .await
            .expect("the conversation snapshots");
        let in_round = snapshot
            .turns
            .iter()
            .filter(|record| record.round == 0)
            .count();
        assert!(
            in_round <= max_turns as usize,
            "a round declared {max_turns} turns and recorded {in_round}"
        );
        assert_eq!(snapshot.round, 1, "and the round still closed");
    }
}

#[tokio::test]
async fn a_turn_regenerated_with_a_different_direction_refuses_at_both_layers() {
    // The direction is content: the same words that designate a speaker and
    // the same words that close the round are different decisions, and a
    // durable redelivery must never absorb one as the other.
    let fx = created(
        AgentConversationMode::ModeratorDirected,
        AgentConversationCompletionRule::ModeratorDecides,
        AgentModerationPolicy::new(AgentRevisionNumber::INITIAL).with_max_turns_per_round(4),
        &["p1", "p2"],
    )
    .await;

    let designate = || Some(AgentConversationDirection::Designate(agent("p1")));
    let close = || Some(AgentConversationDirection::CloseRound);

    fx.apply_conversation_command_at(
        &conversation_scope(),
        submit(0, 0, MODERATOR, "next", designate()),
    )
    .await
    .expect("the designating turn records");

    // In-window: the two directions derive different operation ids, so the
    // regenerated turn is not answered from the operation log at all — it
    // reaches the transition and the ledger refuses it.
    let regenerated = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, MODERATOR, "next", close()),
        )
        .await
        .expect_err("the same words closing the round is a different decision");
    assert_eq!(regenerated.code(), "conversation-turn-content-mismatch");

    // The identical redelivery still converges.
    let replay = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, MODERATOR, "next", designate()),
        )
        .await
        .expect("the identical redelivery is answered");
    assert!(matches!(
        replay,
        AgentConversationEntityReply::Duplicate { .. }
    ));

    // And the designation the protocol actually recorded still stands: the
    // refused close-round changed nothing.
    let snapshot = fx
        .conversation_snapshot_at(&conversation_scope())
        .await
        .expect("the conversation snapshots");
    assert_eq!(snapshot.designated, Some(agent("p1")));
    assert_eq!(snapshot.round, 0, "the refused close-round did not advance");
    assert_eq!(snapshot.turns.len(), 1);

    // Past the operation-log window the dense ledger is the echo, and it
    // fences the direction there too — the digest it stores covers the
    // whole decision, not just the words.
    let designated_again = Some(AgentConversationDirection::Designate(agent("p2")));
    let superseding = fx
        .apply_conversation_command_at(
            &conversation_scope(),
            submit(0, 0, MODERATOR, "next", designated_again),
        )
        .await
        .expect_err("a third direction at the recorded coordinate refuses");
    assert_eq!(superseding.code(), "conversation-turn-content-mismatch");
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
