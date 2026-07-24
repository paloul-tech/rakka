//! Agent-private long-term memory promotion, driven through the run entity.
//!
//! Specification: sections 13.1 and 13.3; the private-memory halves of
//! scenarios 15, 16, and 18 of section 18 (slice 2.1). A deduplicated
//! `PromoteMemory` command commits one idempotent durable `MemoryPromotion`
//! effect in a bounded transition; the dispatcher-side executor reads the
//! selected session entries and upserts private memories under purely derived
//! identities, so any replay converges on one logical write per entry.
//!
//! The store-level halves of the same scenarios live in the `memory` unit
//! tests; these drive the real run entity end to end.

use std::sync::Arc;

use rakka_agent::testkit::{sweep_crash_points, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    promotion_operation_id, AgentMemoryConsolidationTarget, AgentMemoryPromotionRequest,
    AgentModelTurn, AgentModelUsage, AgentPrivateMemoryId, AgentPrivateMemoryKind,
    AgentPrivateMemoryStore, AgentRunEffect, AgentRunEffectKind, AgentRunEntityCommand,
    AgentRunEntityReply, AgentRunMemory, AgentRunStatus, AgentScope, AgentTaskContent,
    AgentTaskEntityStore, InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore,
    InMemorySessionMemoryStore, MemorySequence, PrivateMemoryCursor, SessionMemoryCursor,
    SessionMemoryPage, SessionMemoryPromotionExecutor, SessionMemoryStore,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};

mod common;

use common::*;

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(text)
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
        })
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 3,
        })
}

struct Stores {
    session: Arc<InMemorySessionMemoryStore>,
    snapshots: Arc<InMemoryContextSnapshotStore>,
    private: Arc<InMemoryAgentPrivateMemoryStore>,
}

fn stores() -> Stores {
    Stores {
        session: Arc::new(InMemorySessionMemoryStore::new()),
        snapshots: Arc::new(InMemoryContextSnapshotStore::new()),
        private: Arc::new(InMemoryAgentPrivateMemoryStore::new()),
    }
}

fn requested_by() -> PrincipalRef {
    PrincipalRef {
        principal_type: "service".to_string(),
        principal_id: "memory-curator".to_string(),
        display_name: None,
    }
}

fn promotion(from: u64, to: u64) -> AgentMemoryPromotionRequest {
    AgentMemoryPromotionRequest {
        from_sequence: MemorySequence::new(from),
        to_sequence: MemorySequence::new(to),
        kind: AgentPrivateMemoryKind::Semantic,
        target: None,
        confidence_bps: 9_000,
        requested_by: requested_by(),
    }
}

fn promote_command(request: AgentMemoryPromotionRequest, disc: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::PromoteMemory {
        operation_id: promotion_operation_id(&run_scope(), disc).expect("operation id"),
        promotion: Box::new(request),
    }
}

/// A two-turn scripted world with the promotion executor wired over the
/// fixture's session and private stores.
fn promoting_world() -> (Fixture, Stores) {
    let stores = stores();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("thinking"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_memory_promotion_executor(Arc::new(SessionMemoryPromotionExecutor::new(
        stores.session.clone(),
        stores.private.clone(),
    )));
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    (fx, stores)
}

/// Cranks the run to its current durable wait.
async fn crank(fx: &Fixture) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("settle");
}

/// Applies one command to the run entity; a refused transition surfaces as the
/// entity error.
async fn apply(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, rakka_agent::AgentRunError> {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.apply(command, &fx.router, now).await
}

/// Applies one command that must succeed.
async fn apply_ok(fx: &Fixture, command: AgentRunEntityCommand) -> AgentRunEntityReply {
    apply(fx, command).await.expect("the command applies")
}

/// Reads the session page the promotions select from.
async fn session_page(session: &InMemorySessionMemoryStore) -> SessionMemoryPage {
    session
        .read(&run_scope(), SessionMemoryCursor::start())
        .await
        .expect("read the session")
}

/// The one *outstanding* promotion effect the run currently holds; earlier,
/// already-resolved promotions may linger on the loop state beside it.
async fn promotion_effect(fx: &Fixture) -> AgentRunEffect {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is started");
    let effects: Vec<&AgentRunEffect> = loop_state
        .effects()
        .iter()
        .filter(|effect| {
            effect.kind() == AgentRunEffectKind::MemoryPromotionCall && effect.is_outstanding()
        })
        .collect();
    assert_eq!(effects.len(), 1, "exactly one outstanding promotion effect");
    effects[0].clone()
}

/// Answers only the promotion effect, leaving the model wait untouched, so a
/// test can keep the run live across several promotions.
async fn answer_promotion(fx: &Fixture, effect: &AgentRunEffect) -> AgentRunEntityReply {
    let request = match &effect.request {
        rakka_agent::AgentRunEffectRequest::MemoryPromotion { promotion } => (**promotion).clone(),
        other => panic!("not a promotion effect: {other:?}"),
    };
    let scope = run_scope();
    let now = fx.now();
    let outcome = fx
        .dispatcher
        .promotion_outcome(&scope, effect, &request, now)
        .await;
    apply_ok(
        fx,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&scope)
                .expect("result operation id"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        },
    )
    .await
}

/// Scenario 16 (private half) and the promotion happy path: a deduplicated
/// command promotes the selected entries into the private store, the loop
/// records one bounded receipt, and an unauthorized read reveals nothing
/// (scenario 18, private half).
#[tokio::test]
async fn a_promotion_command_promotes_session_entries_through_the_run() {
    let (fx, stores) = promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;

    // Crank to the turn-one wait, answer it, and crank to the turn-two wait:
    // the session now durably holds the task input and turn one's assistant
    // entry, and the run is still live.
    crank(&fx).await;
    {
        let mut run = fx.run();
        let now = fx.now();
        run.recover(now).await.expect("recover");
        fx.dispatcher
            .drive(&mut run, &fx.router, fx.now())
            .await
            .expect("answer the turn-one model call");
    }
    crank(&fx).await;

    let reply = apply_ok(&fx, promote_command(promotion(1, 2), "policy-1")).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "the promotion applies: {reply:?}"
    );

    // Resolve the promotion before the turn-two proposal can complete the
    // run: a result that raced completion would be refused as terminal (the
    // documented convergence), which is truthful but makes the receipt racy.
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;

    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    // Every selected entry was promoted under its derived identity, with the
    // content, classification, and provenance the entry carried.
    let owner = agent_scope();
    let page = session_page(&stores.session).await;
    assert!(page.entries.len() >= 2);
    let now = AgentTimestampMillis::new(10_000);
    for entry in page.entries.iter().take(2) {
        let memory_id = AgentPrivateMemoryId::derive_promoted(
            &owner,
            &entry.entry_id,
            AgentPrivateMemoryKind::Semantic,
        )
        .expect("derive");
        let memory = stores
            .private
            .get(&owner, &memory_id, now)
            .await
            .expect("get")
            .expect("the promoted memory exists");
        assert_eq!(memory.content, entry.content);
        assert_eq!(memory.classification, entry.classification);
        assert_eq!(memory.source.run.as_ref(), Some(run_scope().run()));
        assert_eq!(memory.source.entry.as_ref(), Some(&entry.entry_id));
        assert!(memory.source.effect.is_some());
    }
    assert_eq!(stores.private.len(&owner), 2);

    // The loop kept one bounded receipt: identities and revisions, no content.
    let mut entity = fx.run();
    let recover_at = fx.now();
    entity.recover(recover_at).await.expect("recover");
    let state = entity.state().expect("state");
    let receipts = state
        .loop_state()
        .expect("the loop is started")
        .memory_promotions();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].promoted.len(), 2);

    // Scenario 18, private half: a sibling agent and a foreign tenant read
    // nothing — not even existence.
    let sibling = AgentScope::new(
        tenant(),
        rakka_agent::AgentId::new("billing-agent").expect("agent id"),
    )
    .expect("scope");
    let foreign = AgentScope::new(rakka_agent::TenantId::new("globex"), agent_id()).expect("scope");
    let promoted_id = receipts[0].promoted[0].memory_id.clone();
    for scope in [&sibling, &foreign] {
        assert!(stores
            .private
            .get(scope, &promoted_id, now)
            .await
            .expect("get")
            .is_none());
        let listed = stores
            .private
            .list(scope, PrivateMemoryCursor::start(), now)
            .await
            .expect("list");
        assert!(listed.memories.is_empty());
    }
}

/// Scenario 16, private half: a replayed command answers its original outcome
/// with one effect, and a replayed result records one receipt and moves no
/// revision.
#[tokio::test]
async fn a_replayed_promotion_command_and_result_write_once() {
    let (fx, stores) = promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    {
        let mut run = fx.run();
        let now = fx.now();
        run.recover(now).await.expect("recover");
        fx.dispatcher
            .drive(&mut run, &fx.router, fx.now())
            .await
            .expect("answer the turn-one model call");
    }
    crank(&fx).await;

    let reply = apply_ok(&fx, promote_command(promotion(1, 2), "policy-1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;

    // The command replay answers the original outcome and commits nothing new.
    let replay = apply_ok(&fx, promote_command(promotion(1, 2), "policy-1")).await;
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "the replayed command deduplicates: {replay:?}"
    );
    let same = promotion_effect(&fx).await;
    assert_eq!(same.effect_id, effect.effect_id, "still exactly one effect");

    // Resolve the promotion, then replay the result: the second delivery
    // deduplicates, one receipt exists, and no revision moved.
    let first = answer_promotion(&fx, &effect).await;
    assert!(matches!(first, AgentRunEntityReply::Applied { .. }));
    let second = answer_promotion(&fx, &effect).await;
    assert!(
        matches!(second, AgentRunEntityReply::Duplicate { .. }),
        "the replayed result deduplicates: {second:?}"
    );

    fx.pump().await.expect("the loop runs to completion");
    let mut entity = fx.run();
    let recover_at = fx.now();
    entity.recover(recover_at).await.expect("recover");
    let state = entity.state().expect("state");
    let receipts = state
        .loop_state()
        .expect("the loop is started")
        .memory_promotions();
    assert_eq!(receipts.len(), 1, "one receipt however often it replayed");

    let owner = agent_scope();
    assert_eq!(stores.private.len(&owner), 2);
    let now = AgentTimestampMillis::new(10_000);
    for reference in &receipts[0].promoted {
        let memory = stores
            .private
            .get(&owner, &reference.memory_id, now)
            .await
            .expect("get")
            .expect("the memory exists");
        assert_eq!(
            memory.revision, reference.revision,
            "the receipt names the revision the store holds"
        );
        assert_eq!(
            memory.revision.get(),
            1,
            "a replay never bumped the revision"
        );
    }
}

/// The adjudicated non-fencing failure semantics: a promotion that fails —
/// here, no executor is wired — records its failure on the effect and the run
/// completes anyway, because memory is never the correctness source
/// (specification 13.1).
#[tokio::test]
async fn a_failed_promotion_does_not_wind_the_run_down() {
    let stores = stores();
    // The dispatcher deliberately has no promotion executor.
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("thinking"))
            .with_turn_for(2, proposing_turn("resolved")),
    );
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;

    // The selection exists (the task input, sequence 1) and the command
    // applies; the effect then fails at dispatch with the executor-missing
    // code.
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "policy-1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));

    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::Completed,
        "a failed promotion never killed the live run"
    );

    // Nothing was promoted and no receipt exists — the failure is on the
    // effect record, not the run's fate.
    assert!(stores.private.is_empty(&agent_scope()));
    let mut entity = fx.run();
    let recover_at = fx.now();
    entity.recover(recover_at).await.expect("recover");
    let state = entity.state().expect("state");
    assert!(state
        .loop_state()
        .expect("the loop is started")
        .memory_promotions()
        .is_empty());
}

/// The refusal table: every malformed or unaffordable promotion is refused in
/// the bounded transition with its stable code, before any effect commits.
#[tokio::test]
async fn promotion_refusals_fail_closed() {
    // Unwired session memory refuses outright.
    {
        let dispatcher = ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new().with_turn_for(1, proposing_turn("resolved")),
        );
        let fx = Fixture::new(dispatcher);
        fx.instantiate_agent().await;
        fx.create_task().await;
        crank(&fx).await;
        let error = apply(&fx, promote_command(promotion(1, 1), "p"))
            .await
            .expect_err("an unwired run refuses the promotion");
        assert_eq!(error.code(), "run-session-memory-unwired");
    }

    let (fx, _stores) = promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;

    // Selection beyond what the run has durably assigned.
    let error = apply(&fx, promote_command(promotion(5, 6), "p1"))
        .await
        .expect_err("an out-of-range selection is refused");
    assert_eq!(error.code(), "run-memory-selection-out-of-range");

    // An inverted selection.
    let error = apply(&fx, promote_command(promotion(2, 1), "p2"))
        .await
        .expect_err("an inverted selection is refused");
    assert_eq!(error.code(), "run-memory-selection-invalid");

    // A selection wider than the bound.
    let error = apply(&fx, promote_command(promotion(1, 200), "p3"))
        .await
        .expect_err("an oversized selection is refused");
    assert_eq!(error.code(), "run-memory-selection-invalid");

    // A consolidation spanning more than one source entry: give the session a
    // second sequence first.
    {
        let mut run = fx.run();
        let now = fx.now();
        run.recover(now).await.expect("recover");
        fx.dispatcher
            .drive(&mut run, &fx.router, fx.now())
            .await
            .expect("answer the turn-one model call");
    }
    crank(&fx).await;
    let mut spanning = promotion(1, 2);
    spanning.target = Some(AgentMemoryConsolidationTarget {
        memory_id: AgentPrivateMemoryId::new("mem-target").expect("memory id"),
        expected_revision: rakka_agent::AgentRevisionNumber::INITIAL,
    });
    let error = apply(&fx, promote_command(spanning, "p4"))
        .await
        .expect_err("a spanning consolidation is refused");
    assert_eq!(error.code(), "run-memory-consolidation-invalid");
}

/// Consolidation is a compare-and-set update of exactly one memory: the first
/// consolidation bumps the revision once, and a stale expectation settles the
/// generation `Failed` without a write and without killing the run.
#[tokio::test]
async fn consolidation_updates_one_memory_under_cas() {
    let (fx, stores) = promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    {
        let mut run = fx.run();
        let now = fx.now();
        run.recover(now).await.expect("recover");
        fx.dispatcher
            .drive(&mut run, &fx.router, fx.now())
            .await
            .expect("answer the turn-one model call");
    }
    crank(&fx).await;

    // Promote the task input (sequence 1) into a fresh memory.
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "p1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;

    let owner = agent_scope();
    let now = AgentTimestampMillis::new(10_000);
    let page = session_page(&stores.session).await;
    let input_entry = &page.entries[0];
    let assistant_entry = &page.entries[1];
    let memory_id = AgentPrivateMemoryId::derive_promoted(
        &owner,
        &input_entry.entry_id,
        AgentPrivateMemoryKind::Semantic,
    )
    .expect("derive");
    let created = stores
        .private
        .get(&owner, &memory_id, now)
        .await
        .expect("get")
        .expect("the memory exists");
    assert_eq!(created.revision.get(), 1);

    // Consolidate turn one's assistant entry (sequence 2) into it at the
    // exact revision.
    let mut consolidate = promotion(2, 2);
    consolidate.target = Some(AgentMemoryConsolidationTarget {
        memory_id: memory_id.clone(),
        expected_revision: created.revision,
    });
    let reply = apply_ok(&fx, promote_command(consolidate, "c1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;

    let updated = stores
        .private
        .get(&owner, &memory_id, now)
        .await
        .expect("get")
        .expect("the memory exists");
    assert_eq!(updated.revision.get(), 2, "the consolidation bumped once");
    assert_eq!(
        updated.content, assistant_entry.content,
        "the consolidated content is the selected entry's"
    );
    assert_eq!(
        updated.created_at, created.created_at,
        "the update carried the original creation time forward"
    );

    // A stale expectation is refused without a write, and the run survives.
    let mut stale = promotion(1, 1);
    stale.target = Some(AgentMemoryConsolidationTarget {
        memory_id: memory_id.clone(),
        expected_revision: created.revision,
    });
    let reply = apply_ok(&fx, promote_command(stale, "c2")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;

    let unmoved = stores
        .private
        .get(&owner, &memory_id, now)
        .await
        .expect("get")
        .expect("the memory exists");
    assert_eq!(unmoved.revision.get(), 2, "the stale writer wrote nothing");

    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::Completed,
        "a refused consolidation never killed the run"
    );
}

/// Scenario 15, private half, across runs: two runs of one agent promoting
/// concurrently derive disjoint memory identities — nothing overwrites — and a
/// repeated promotion of the same entry converges on the existing record.
#[tokio::test]
async fn concurrent_runs_promote_without_stale_overwrite() {
    use rakka_agent::{
        AgentEffectPolicies, AgentMemoryPromotionExecutor, AgentRunEffectRequest, AgentRunId,
        AgentRunScope, MemoryClassification, MemoryEntryId, MemoryEntryRole, MemoryOperationId,
        SessionMemoryEntry,
    };

    let session = Arc::new(InMemorySessionMemoryStore::new());
    let private = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let executor = SessionMemoryPromotionExecutor::new(session.clone(), private.clone());
    let owner = agent_scope();
    let now = AgentTimestampMillis::new(50);

    // Two runs of the same agent, each with its own session.
    let mut effects = Vec::new();
    for run in ["run-a", "run-b"] {
        let scope = AgentRunScope::new(tenant(), agent_id(), AgentRunId::new(run).expect("run id"))
            .expect("scope");
        let entry = SessionMemoryEntry::new(
            MemoryEntryId::derive(&scope, "turn-1-assistant").expect("entry id"),
            MemoryOperationId::derive(&scope, "turn-1-assistant").expect("op id"),
            MemorySequence::new(1),
            MemoryEntryRole::Assistant,
            AgentTaskContent::inline(serde_json::json!({ "from": run })).expect("content"),
            1,
            None,
            MemoryClassification::Unclassified,
            now,
        )
        .expect("the entry is bounded");
        session.append(&scope, &entry).await.expect("append");

        let request = promotion(1, 1);
        let effect = AgentRunEffect::new(
            &scope,
            1,
            0,
            AgentRunEffectRequest::MemoryPromotion {
                promotion: Box::new(request.clone()),
            },
            AgentEffectPolicies::new().spec_for(&AgentRunEffectRequest::MemoryPromotion {
                promotion: Box::new(request.clone()),
            }),
            rakka_agent::AgentRevisionNumber::INITIAL,
            now,
        )
        .expect("the effect commits");
        effects.push((scope, effect, request));
    }

    // Both promotions run concurrently against one shared private store.
    let (left, right) = (&effects[0], &effects[1]);
    let (a, b) = tokio::join!(
        executor.execute(&left.0, &left.1, &left.2, now),
        executor.execute(&right.0, &right.1, &right.2, now),
    );
    let a = a.expect("run-a promotes");
    let b = b.expect("run-b promotes");

    let refs = |finding| match finding {
        rakka_agent::AgentMemoryPromotionFinding::Promoted { promoted } => promoted,
        other => panic!("the promotion succeeded: {other:?}"),
    };
    let (a, b) = (refs(a), refs(b));
    assert_ne!(
        a[0].memory_id, b[0].memory_id,
        "two runs' same-slot entries derive disjoint memories"
    );
    assert_eq!(private.len(&owner), 2);
    for reference in a.iter().chain(b.iter()) {
        let memory = private
            .get(&owner, &reference.memory_id, now)
            .await
            .expect("get")
            .expect("the memory exists");
        assert_eq!(memory.revision.get(), 1, "nothing overwrote anything");
    }

    // A second, distinct promotion of run-a's entry converges on the existing
    // memory: same identity, same revision, no duplicate and no churn.
    let (scope, _first_effect, request) = left;
    let second_effect = AgentRunEffect::new(
        scope,
        2,
        0,
        AgentRunEffectRequest::MemoryPromotion {
            promotion: Box::new(request.clone()),
        },
        AgentEffectPolicies::new().spec_for(&AgentRunEffectRequest::MemoryPromotion {
            promotion: Box::new(request.clone()),
        }),
        rakka_agent::AgentRevisionNumber::INITIAL,
        now,
    )
    .expect("the effect commits");
    let again = refs(
        executor
            .execute(scope, &second_effect, request, now)
            .await
            .expect("the re-promotion converges"),
    );
    assert_eq!(again[0].memory_id, a[0].memory_id);
    assert_eq!(again[0].revision.get(), 1);
    assert_eq!(private.len(&owner), 2, "convergence created nothing");
}

/// The owner-kill sweep over the promotion flow: kill the run's owner at every
/// durable write, on both sides of the compare-and-set, then recover and retry
/// under the retry contract (the same operation id). However the owner died,
/// the run completes, the private store holds exactly the promoted memory at
/// its initial revision, and the loop holds exactly one receipt — the
/// command → effect → upsert → settle chain never double-writes and never
/// loses a promotion once its transition committed (scenario 16, private
/// half, under fault injection).
#[tokio::test]
async fn memory_promotion_survives_any_owner_loss() {
    let build = || {
        let stores = stores();
        let dispatcher = ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new().with_turn_for(1, proposing_turn("resolved")),
        )
        .with_memory_promotion_executor(Arc::new(SessionMemoryPromotionExecutor::new(
            stores.session.clone(),
            stores.private.clone(),
        )));
        let fx = Fixture::new(dispatcher).with_memory(
            AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
                .with_private_store(stores.private.clone()),
        );
        (fx, stores)
    };

    // Retry the promotion until the run is ready for it, mirroring an
    // application's retry loop: the task may still owe the assignment, or the
    // run may not have assigned the selected sequence yet. Returns whether the
    // promotion durably applied; a doomed pass reports `false` instead of
    // panicking, so the sweep can recover and retry.
    async fn try_promote(fx: &Fixture) -> bool {
        for _round in 0..16 {
            let now = fx.now();
            let mut task = AgentTaskEntityStore::new(
                task_scope(),
                fx.tasks.clone(),
                fx.agents.clone(),
                fx.history.clone(),
            );
            if task.recover(now).await.is_ok() {
                let _ = task.settle_side_effects(&fx.router, now).await;
            }
            let mut run = fx.run();
            let now = fx.now();
            if run.recover(now).await.is_err() {
                continue;
            }
            let _ = run.settle_side_effects(&fx.router, now).await;
            let command = promote_command(promotion(1, 1), "sweep");
            match run.apply(command, &fx.router, fx.now()).await {
                Ok(AgentRunEntityReply::Applied { .. })
                | Ok(AgentRunEntityReply::Duplicate { .. }) => {}
                Ok(_) | Err(_) => continue,
            }
            // Resolve the promotion deterministically before the model
            // result can complete the run: a result racing completion is
            // refused as terminal (the documented convergence), which is
            // truthful but would make the receipt racy.
            let _ = run.settle_side_effects(&fx.router, fx.now()).await;
            let Ok(state) = run.state() else { continue };
            let outstanding = state.loop_state().and_then(|loop_state| {
                loop_state
                    .effects()
                    .iter()
                    .find(|effect| {
                        effect.kind() == AgentRunEffectKind::MemoryPromotionCall
                            && effect.is_outstanding()
                    })
                    .cloned()
            });
            let Some(effect) = outstanding else {
                // Already resolved on a prior pass.
                return true;
            };
            let request = match &effect.request {
                rakka_agent::AgentRunEffectRequest::MemoryPromotion { promotion } => {
                    (**promotion).clone()
                }
                other => panic!("not a promotion effect: {other:?}"),
            };
            let scope = run_scope();
            let outcome = fx
                .dispatcher
                .promotion_outcome(&scope, &effect, &request, fx.now())
                .await;
            let result = AgentRunEntityCommand::RecordEffectResult {
                operation_id: effect
                    .result_operation_id(&scope)
                    .expect("result operation id"),
                effect_id: effect.effect_id.clone(),
                generation: effect.generation,
                attempt: effect.attempts.saturating_add(1),
                fence: 0,
                outcome: Box::new(outcome),
            };
            match run.apply(result, &fx.router, fx.now()).await {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
        false
    }

    // The reference flow, uncrashed, counts the durable writes to sweep.
    let (reference, reference_stores) = build();
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;
    assert!(try_promote(&reference).await, "the reference flow promotes");
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    assert_eq!(
        reference_stores.private.len(&agent_scope()),
        1,
        "the reference flow promoted its entry"
    );
    let writes = reference.runs.writes();
    assert!(
        writes >= 5,
        "the promotion flow should make several durable writes, saw {writes}"
    );

    sweep_crash_points(writes, |nth, point| async move {
        let (fx, stores) = build();
        fx.instantiate_agent().await;

        fx.runs.crash_at(nth, point);
        fx.create_task().await;
        // The doomed pass: any step may die at the armed write.
        let _ = try_promote(&fx).await;
        let _ = fx.pump().await;

        fx.runs.assert_crash_fired(nth, point);
        fx.runs.survive();

        // Recovery plus the retry contract: the same operation id, re-applied.
        assert!(
            try_promote(&fx).await,
            "crash {point:?} at write {nth} left the promotion inapplicable"
        );
        fx.pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let run = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            run.status,
            AgentRunStatus::Completed,
            "crash {point:?} at write {nth} should still complete"
        );

        // Exactly one memory at its initial revision: the promotion neither
        // double-wrote nor vanished.
        let owner = agent_scope();
        assert_eq!(
            stores.private.len(&owner),
            1,
            "crash {point:?} at write {nth} duplicated or lost the promotion"
        );
        let now = AgentTimestampMillis::new(1_000_000);
        let listed = stores
            .private
            .list(&owner, PrivateMemoryCursor::start(), now)
            .await
            .expect("list");
        assert_eq!(listed.memories.len(), 1);
        assert_eq!(
            listed.memories[0].revision.get(),
            1,
            "crash {point:?} at write {nth} bumped a revision it never should"
        );

        // Exactly one bounded receipt survived recovery.
        let mut entity = fx.run();
        let recover_at = fx.now();
        entity.recover(recover_at).await.expect("recover");
        let state = entity.state().expect("state");
        let receipts = state
            .loop_state()
            .expect("the loop is started")
            .memory_promotions();
        assert_eq!(
            receipts.len(),
            1,
            "crash {point:?} at write {nth} duplicated or lost the receipt"
        );
        assert_eq!(receipts[0].promoted.len(), 1);
    })
    .await;
}
