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
    agent_operational_snapshot, promotion_operation_id, AgentCheckpoint, AgentCheckpointKind,
    AgentLoopPhase, AgentMemoryConsolidationTarget, AgentMemoryPromotionRequest, AgentModelTurn,
    AgentModelUsage, AgentOperationId, AgentOperationKind, AgentPrivateMemoryId,
    AgentPrivateMemoryKind, AgentPrivateMemoryStore, AgentRunEffect, AgentRunEffectKind,
    AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityReply,
    AgentRunMemory, AgentRunStatus, AgentSchemaPolicy, AgentScope, AgentTaskContent,
    AgentTaskEntityStore, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    AgentToolResult, InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore,
    InMemorySessionMemoryStore, MemoryEntryRole, MemorySequence, PrivateMemoryCursor,
    SessionMemoryCursor, SessionMemoryPage, SessionMemoryPromotionExecutor, SessionMemoryStore,
    AGENT_POST_TERMINAL_MEMORY_WINDOW_DEFAULT_MS, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(text)
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
            ..Default::default()
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
            ..Default::default()
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
        roles: None,
    }
}

fn promote_command(request: AgentMemoryPromotionRequest, disc: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::PromoteMemory {
        operation_id: promotion_operation_id(&run_scope(), disc).expect("operation id"),
        promotion: Box::new(request),
    }
}

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": "ticket" }),
            )
            .expect("the tool call is bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
            ..Default::default()
        })
}

/// A world whose first turn calls a tool, so the session holds one entry of
/// each of the three recorded roles — the task input, the assistant text, the
/// tool result — before turn two's model wait.
fn tool_promoting_world() -> (Fixture, Stores) {
    let stores = stores();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
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
    // Boxed, here and in the helpers below: a debug build copies an awaited
    // future onto its caller's stack, and these tests nest several helpers
    // deep.
    Box::pin(run.settle_side_effects(&fx.router, now))
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
    Box::pin(run.apply(command, &fx.router, now)).await
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
                let _ = Box::pin(task.settle_side_effects(&fx.router, now)).await;
            }
            let mut run = fx.run();
            let now = fx.now();
            if run.recover(now).await.is_err() {
                continue;
            }
            let _ = Box::pin(run.settle_side_effects(&fx.router, now)).await;
            let command = promote_command(promotion(1, 1), "sweep");
            match Box::pin(run.apply(command, &fx.router, fx.now())).await {
                Ok(AgentRunEntityReply::Applied { .. })
                | Ok(AgentRunEntityReply::Duplicate { .. }) => {}
                Ok(_) | Err(_) => continue,
            }
            // Resolve the promotion deterministically before the model
            // result can complete the run: a result racing completion is
            // refused as terminal (the documented convergence), which is
            // truthful but would make the receipt racy.
            let _ = Box::pin(run.settle_side_effects(&fx.router, fx.now())).await;
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
            match Box::pin(run.apply(result, &fx.router, fx.now())).await {
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

/// Answers every dispatched effect except an outstanding promotion, so a test
/// can crank a tool-calling turn to rest while keeping the run live.
async fn drive_turn(fx: &Fixture) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    Box::pin(fx.dispatcher.drive(&mut run, &fx.router, fx.now()))
        .await
        .expect("answer the dispatched effects");
}

/// The promoted memory identity one session entry derives to.
fn promoted_id(entry: &rakka_agent::SessionMemoryEntry) -> AgentPrivateMemoryId {
    AgentPrivateMemoryId::derive_promoted(
        &agent_scope(),
        &entry.entry_id,
        AgentPrivateMemoryKind::Semantic,
    )
    .expect("derive")
}

/// The native role filter (specification 13.3): over a window holding one
/// entry of each recorded role, `roles = Some([ToolResult])` promotes exactly
/// the tool-result entry; `None` promotes every entry and converges on the
/// one already promoted; a filter that selects nothing is a definitive
/// refusal under `memory-promotion-selection-empty` that writes nothing and
/// leaves the run live; a second filtered promotion converges on the same
/// record; and an empty role set is refused at the door.
#[tokio::test]
async fn a_role_filter_promotes_only_the_selected_roles() {
    let (fx, stores) = tool_promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    // Turn one: the model call, then its tool call, then the turn rests and
    // its entries flush; turn two's model call is the live wait.
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;

    let page = session_page(&stores.session).await;
    let roles: Vec<MemoryEntryRole> = page.entries.iter().map(|entry| entry.role).collect();
    assert_eq!(
        roles,
        vec![
            MemoryEntryRole::User,
            MemoryEntryRole::Assistant,
            MemoryEntryRole::ToolResult
        ],
        "the window holds one entry of each recorded role"
    );
    let tool_entry = &page.entries[2];
    let owner = agent_scope();
    let now = AgentTimestampMillis::new(10_000);

    // `Some([ToolResult])` over the whole window promotes the tool entry only.
    let mut filtered = promotion(1, 3);
    filtered.roles = Some(vec![MemoryEntryRole::ToolResult]);
    let reply = apply_ok(&fx, promote_command(filtered, "tools-1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;
    assert_eq!(
        stores.private.len(&owner),
        1,
        "only the tool-result entry promoted"
    );
    let promoted = stores
        .private
        .get(&owner, &promoted_id(tool_entry), now)
        .await
        .expect("get")
        .expect("the tool-result entry's memory exists");
    assert_eq!(promoted.content, tool_entry.content);
    assert_eq!(promoted.source.entry.as_ref(), Some(&tool_entry.entry_id));

    // `None` over the same window promotes the rest and converges on the
    // tool entry's existing record: three memories, none churned.
    let reply = apply_ok(&fx, promote_command(promotion(1, 3), "all-1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;
    assert_eq!(stores.private.len(&owner), 3, "every role promoted once");
    let converged = stores
        .private
        .get(&owner, &promoted_id(tool_entry), now)
        .await
        .expect("get")
        .expect("the tool-result memory still exists");
    assert_eq!(
        converged.revision.get(),
        1,
        "convergence bumped no revision"
    );

    // A filter that selects nothing is refused definitively — on the effect
    // record, under the stable code, with nothing written and the run live.
    let mut empty = promotion(1, 3);
    empty.roles = Some(vec![MemoryEntryRole::Summary]);
    let reply = apply_ok(&fx, promote_command(empty, "summaries-1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;
    let refused = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .effects()
            .iter()
            .find(|held| held.effect_id == effect.effect_id)
            .cloned()
            .expect("the refused effect record survives")
    };
    assert_eq!(refused.status, AgentRunEffectStatus::Failed);
    assert_eq!(
        refused.last_error_code.as_deref(),
        Some("memory-promotion-selection-empty")
    );
    assert_eq!(
        stores.private.len(&owner),
        3,
        "the refused window wrote nothing"
    );
    let live = fx.run_snapshot().await.expect("the run exists");
    assert!(
        !live.status.is_terminal(),
        "the run stays live: {:?}",
        live.status
    );
    assert_eq!(live.terminal_reason, None);

    // A second filtered promotion under a new operation id converges on the
    // same record: the identity is per entry, and the filter changes nothing
    // about it.
    let mut again = promotion(1, 3);
    again.roles = Some(vec![MemoryEntryRole::ToolResult]);
    let reply = apply_ok(&fx, promote_command(again, "tools-2")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;
    assert_eq!(stores.private.len(&owner), 3, "the replay created nothing");
    let receipts = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .memory_promotions()
            .to_vec()
    };
    let last = receipts
        .last()
        .expect("the converged promotion left a receipt");
    assert_eq!(last.promoted.len(), 1);
    assert_eq!(last.promoted[0].memory_id, promoted_id(tool_entry));
    assert_eq!(last.promoted[0].revision.get(), 1);

    // An empty role set is refused at the door, before any effect commits.
    let mut none = promotion(1, 3);
    none.roles = Some(Vec::new());
    let error = apply(&fx, promote_command(none, "none-1"))
        .await
        .expect_err("an empty role set is refused");
    assert_eq!(error.code(), "run-memory-roles-empty");

    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

/// A consolidation still needs exactly one selected entry: a one-entry window
/// whose entry the role filter excludes is refused under the empty-selection
/// code, and the target is not touched.
#[tokio::test]
async fn a_filtered_consolidation_with_no_selected_entry_is_refused() {
    let (fx, stores) = tool_promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;

    // Promote the task input into a fresh memory to consolidate into.
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "p1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;
    let owner = agent_scope();
    let now = AgentTimestampMillis::new(10_000);
    let page = session_page(&stores.session).await;
    let target_id = promoted_id(&page.entries[0]);
    let created = stores
        .private
        .get(&owner, &target_id, now)
        .await
        .expect("get")
        .expect("the memory exists");

    // Consolidate sequence 3 (the tool result) into it, but filter to a role
    // the entry does not have.
    let mut consolidate = promotion(3, 3);
    consolidate.target = Some(AgentMemoryConsolidationTarget {
        memory_id: target_id.clone(),
        expected_revision: created.revision,
    });
    consolidate.roles = Some(vec![MemoryEntryRole::Assistant]);
    let reply = apply_ok(&fx, promote_command(consolidate, "c1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    answer_promotion(&fx, &effect).await;
    let unmoved = stores
        .private
        .get(&owner, &target_id, now)
        .await
        .expect("get")
        .expect("the memory exists");
    assert_eq!(
        unmoved.revision, created.revision,
        "the target was not touched"
    );
    let refused = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .effects()
            .iter()
            .find(|held| held.effect_id == effect.effect_id)
            .cloned()
            .expect("the refused effect record survives")
    };
    assert_eq!(
        refused.last_error_code.as_deref(),
        Some("memory-promotion-selection-empty")
    );

    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

// ===========================================================================
// The post-terminal window: a run that has ended still accepts a promotion
// for a bounded window, its effect rides the ordinary outbox, and the outcome
// lands on the terminal record without moving it (specification 13.3;
// scenario 16's private half, after the run's end).
// ===========================================================================

/// How the world's run ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    Completed,
    Failed,
    Cancelled,
}

impl Ending {
    const ALL: [Self; 3] = [Self::Completed, Self::Failed, Self::Cancelled];

    fn status(self) -> AgentRunStatus {
        match self {
            Self::Completed => AgentRunStatus::Completed,
            Self::Failed => AgentRunStatus::Failed,
            Self::Cancelled => AgentRunStatus::Cancelled,
        }
    }
}

/// A promoting world driven to its terminal status. However the run ended,
/// the session durably holds the task input at sequence one.
async fn ended_world(ending: Ending) -> (Fixture, Stores) {
    ended_world_recording(ending, None).await
}

/// [`ended_world`], optionally wiring a decision-event sink under the
/// vocabulary that writes every kind.
async fn ended_world_recording(
    ending: Ending,
    sink: Option<Arc<CountingDecisionSink>>,
) -> (Fixture, Stores) {
    ended_world_configured(
        ending,
        sink.map(|sink| (sink, rakka_agent::AgentDecisionEventVocabulary::Version2)),
        None,
    )
    .await
}

/// [`ended_world`], optionally wiring a decision-event sink under an explicit
/// vocabulary, and optionally capping the task's effect attempts.
async fn ended_world_configured(
    ending: Ending,
    sink: Option<(
        Arc<CountingDecisionSink>,
        rakka_agent::AgentDecisionEventVocabulary,
    )>,
    attempt_ceiling: Option<u32>,
) -> (Fixture, Stores) {
    ended_world_with_policies(
        ending,
        sink,
        attempt_ceiling,
        rakka_agent::AgentEffectPolicies::default(),
    )
    .await
}

/// [`ended_world_configured`] under explicit effect policies, which carry the
/// checkpoint SLA the run stamps onto the checkpoints it opens.
async fn ended_world_with_policies(
    ending: Ending,
    sink: Option<(
        Arc<CountingDecisionSink>,
        rakka_agent::AgentDecisionEventVocabulary,
    )>,
    attempt_ceiling: Option<u32>,
    policies: rakka_agent::AgentEffectPolicies,
) -> (Fixture, Stores) {
    let stores = stores();
    let adapter = match ending {
        Ending::Failed => {
            DeterministicModelAdapter::new().with_turn_for(1, tool_calling_turn("flaky-tool"))
        }
        Ending::Completed | Ending::Cancelled => DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("thinking"))
            .with_turn_for(2, proposing_turn("resolved")),
    };
    let mut dispatcher =
        ScriptedDispatcher::with_adapter(adapter).with_memory_promotion_executor(Arc::new(
            SessionMemoryPromotionExecutor::new(stores.session.clone(), stores.private.clone()),
        ));
    if ending == Ending::Failed {
        dispatcher =
            dispatcher.with_tool_failure("flaky-tool", "tool-unavailable", "the tool is down");
    }
    let fx = Fixture::with_sink(
        dispatcher,
        rakka_agent::InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(std::sync::atomic::AtomicU64::new(1)),
    )
    .with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    let fx = match sink {
        Some((sink, vocabulary)) => fx
            .with_decision_events(sink)
            .with_decision_event_vocabulary(vocabulary),
        None => fx,
    };
    Box::pin(fx.instantiate_agent()).await;
    match attempt_ceiling {
        Some(ceiling) => {
            Box::pin(fx.create_task_with(task_definition().with_budgets(
                rakka_agent::AgentBudgetCeilings {
                    max_loop_iterations: Some(3),
                    max_effect_attempts: Some(ceiling),
                    ..rakka_agent::AgentBudgetCeilings::unbounded()
                },
            )))
            .await;
        }
        None => Box::pin(fx.create_task()).await,
    }
    if ending == Ending::Cancelled {
        crank(&fx).await;
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        Box::pin(
            run.apply(
                AgentRunEntityCommand::Cancel {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::Cancellation,
                        [common::TENANT, common::AGENT, "1"],
                    )
                    .expect("derivable"),
                    reason: "operator stopped the work".to_string(),
                },
                &fx.router,
                fx.now(),
            ),
        )
        .await
        .expect("the cancel applies");
    }
    Box::pin(fx.pump()).await.expect("the loop runs to its end");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, ending.status(), "the world ended as scripted");
    assert!(
        run.terminal_at.is_some(),
        "the terminal transition stamped the run"
    );
    (fx, stores)
}

/// The one promotion effect the run holds under `effect_id`, re-read from
/// durable state.
async fn promotion_record(fx: &Fixture, effect: &AgentRunEffect) -> AgentRunEffect {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .find(|held| held.effect_id == effect.effect_id)
        .cloned()
        .expect("the effect record survives")
}

/// Records `outcome` against the run's one outstanding promotion, after the
/// settle pass has made it dispatchable, without driving anything else.
async fn resolve_promotion(fx: &Fixture, outcome: AgentRunEffectOutcome) -> AgentRunEffect {
    crank(fx).await;
    let effect = promotion_effect(fx).await;
    let scope = run_scope();
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
    .await;
    promotion_record(fx, &effect).await
}

/// Inside the window, a run that ended `Completed`, `Failed`, or `Cancelled`
/// accepts a promotion: the effect is committed, rides the ordinary outbox,
/// is dispatched by the ordinary pump, and its receipt lands on the terminal
/// record — whose status, phase, terminal reason, and terminal stamp do not
/// move.
#[tokio::test]
async fn a_promotion_is_accepted_inside_the_post_terminal_window() {
    for ending in Ending::ALL {
        let (fx, stores) = ended_world(ending).await;
        let before = fx.run_snapshot().await.expect("the run exists");

        let reply = apply_ok(&fx, promote_command(promotion(1, 1), "post-terminal")).await;
        assert!(
            matches!(reply, AgentRunEntityReply::Applied { .. }),
            "{ending:?}: the post-terminal promotion applies, got {reply:?}"
        );
        fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
            .await
            .unwrap_or_else(|error| panic!("{ending:?}: {error}"));

        let owner = agent_scope();
        assert_eq!(
            stores.private.len(&owner),
            1,
            "{ending:?}: the task input was promoted after the run ended"
        );
        let mut entity = fx.run();
        entity.recover(fx.now()).await.expect("recover");
        let state = entity.state().expect("state");
        let loop_state = state.loop_state().expect("the loop is started");
        assert_eq!(
            loop_state.memory_promotions().len(),
            1,
            "{ending:?}: one receipt"
        );
        assert!(
            loop_state.effects().iter().any(|effect| effect.kind()
                == AgentRunEffectKind::MemoryPromotionCall
                && effect.status == AgentRunEffectStatus::Succeeded),
            "{ending:?}: the promotion effect succeeded"
        );

        let after = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            after.status, before.status,
            "{ending:?}: the status did not move"
        );
        assert_eq!(
            after.phase, before.phase,
            "{ending:?}: the phase did not move"
        );
        assert_eq!(
            after.terminal_reason, before.terminal_reason,
            "{ending:?}: the terminal reason did not move"
        );
        assert_eq!(
            after.terminal_at, before.terminal_at,
            "{ending:?}: the terminal stamp did not move"
        );
    }
}

/// The window is inclusive of its last millisecond and closed one past it:
/// a promotion at exactly `terminal_at + window` is accepted, one at
/// `terminal_at + window + 1` is refused under `run-memory-window-closed`
/// with nothing committed and nothing written.
#[tokio::test]
async fn a_promotion_past_the_window_is_refused_and_commits_nothing() {
    let (fx, stores) = ended_world(Ending::Completed).await;
    let terminal_at = fx
        .run_snapshot()
        .await
        .expect("the run exists")
        .terminal_at
        .expect("stamped")
        .as_millis();
    let window = AGENT_POST_TERMINAL_MEMORY_WINDOW_DEFAULT_MS;

    // `apply` reads the clock once, so storing the boundary is what the
    // transition sees.
    fx.clock
        .store(terminal_at + window, std::sync::atomic::Ordering::SeqCst);
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "boundary")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
        .await
        .expect("the boundary promotion settles");
    assert_eq!(stores.private.len(&agent_scope()), 1);

    fx.clock.store(
        terminal_at + window + 1,
        std::sync::atomic::Ordering::SeqCst,
    );
    let error = apply(&fx, promote_command(promotion(1, 1), "late"))
        .await
        .expect_err("a promotion past the window is refused");
    assert_eq!(error.code(), "run-memory-window-closed");
    let mut entity = fx.run();
    entity.recover(fx.now()).await.expect("recover");
    let promotions = entity
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .filter(|effect| effect.kind() == AgentRunEffectKind::MemoryPromotionCall)
        .count();
    assert_eq!(promotions, 1, "the refused promotion committed no effect");
    assert_eq!(stores.private.len(&agent_scope()), 1, "and wrote nothing");
}

/// A zero window closes the door entirely, under the plain terminal refusal
/// a run answered before the window existed.
#[tokio::test]
async fn a_zero_window_restores_the_terminal_refusal() {
    let (mut fx, stores) = ended_world(Ending::Completed).await;
    fx.policies = fx.policies.clone().with_post_terminal_memory_window_ms(0);
    let error = apply(&fx, promote_command(promotion(1, 1), "closed"))
        .await
        .expect_err("a zero window refuses");
    assert_eq!(error.code(), "run-terminal");
    assert!(stores.private.is_empty(&agent_scope()));
}

/// A terminal record with no terminal stamp — persisted before the stamp
/// existed and never repaired — is outside every window: `updated_at` would
/// be a sliding clock, since every accepted post-terminal command moves it.
#[tokio::test]
async fn a_terminal_record_without_a_stamp_is_outside_the_window() {
    let (fx, stores) = ended_world(Ending::Completed).await;
    let id = run_scope().persistence_id();
    let record = fx
        .runs
        .load(&id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    assert!(
        !value["run"]["terminal_at"].is_null(),
        "the terminal transition stamped the record"
    );
    value["run"]["terminal_at"] = serde_json::Value::Null;
    let unstamped = serde_json::from_value(value).expect("the unstamped record deserializes");
    fx.runs
        .compare_and_set(&id, record.revision, unstamped)
        .await
        .expect("the unstamped record persists");

    let error = apply(&fx, promote_command(promotion(1, 1), "unstamped"))
        .await
        .expect_err("an unstamped terminal record refuses");
    assert_eq!(error.code(), "run-memory-window-closed");
    assert!(stores.private.is_empty(&agent_scope()));
}

/// Replay after the run's end writes once: the command replay answers from
/// the operation log with one effect, and the result replay records one
/// receipt and moves no revision.
#[tokio::test]
async fn a_post_terminal_promotion_replays_once() {
    let (fx, stores) = ended_world(Ending::Completed).await;
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "post-1")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let replay = apply_ok(&fx, promote_command(promotion(1, 1), "post-1")).await;
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "the replayed command deduplicates: {replay:?}"
    );
    crank(&fx).await;
    let effect = promotion_effect(&fx).await;
    let first = answer_promotion(&fx, &effect).await;
    assert!(matches!(first, AgentRunEntityReply::Applied { .. }));
    let second = answer_promotion(&fx, &effect).await;
    assert!(
        matches!(second, AgentRunEntityReply::Duplicate { .. }),
        "the replayed result deduplicates: {second:?}"
    );

    let owner = agent_scope();
    assert_eq!(stores.private.len(&owner), 1);
    let listed = stores
        .private
        .list(
            &owner,
            PrivateMemoryCursor::start(),
            AgentTimestampMillis::new(1_000_000),
        )
        .await
        .expect("list");
    assert_eq!(
        listed.memories[0].revision.get(),
        1,
        "no replay bumped the revision"
    );
    let mut entity = fx.run();
    entity.recover(fx.now()).await.expect("recover");
    let receipts = entity
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .memory_promotions()
        .len();
    assert_eq!(receipts, 1, "one receipt however often it replayed");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
}

/// Once the run's session memory is gone the executor's retryable
/// source-missing failure exhausts the attempt budget; the exhausted effect,
/// being exempt from the wind-down, ends nothing — the terminal record is
/// untouched.
#[tokio::test]
async fn an_exhausted_post_terminal_promotion_leaves_the_terminal_record_untouched() {
    let (fx, stores) = ended_world(Ending::Failed).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "exhausted")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let exhausted = resolve_promotion(
        &fx,
        AgentRunEffectOutcome::exhausted(
            "memory-promotion-source-missing".to_string(),
            "the session rows were purged".to_string(),
        ),
    )
    .await;
    assert_eq!(exhausted.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        exhausted.last_error_code.as_deref(),
        Some("memory-promotion-source-missing")
    );
    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before.status);
    assert_eq!(after.terminal_reason, before.terminal_reason);
    assert_eq!(after.terminal_at, before.terminal_at);
    assert!(stores.private.is_empty(&agent_scope()));
}

/// An ambiguous outcome delivered to a run that has ended parks the
/// promotion `Indeterminate` and opens its reconciliation checkpoint: either
/// resolution path can then answer it. The checkpoint omits `compensate`,
/// which an ended run refuses, and the terminal record does not move.
#[tokio::test]
async fn an_ended_runs_ambiguous_promotion_opens_its_reconciliation_checkpoint() {
    for ending in Ending::ALL {
        let (fx, _stores) = ended_world(ending).await;
        let before = fx.run_snapshot().await.expect("the run exists");
        let reply = apply_ok(&fx, promote_command(promotion(1, 1), "ambiguous")).await;
        assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
        let parked = resolve_promotion(
            &fx,
            AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            ),
        )
        .await;
        assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);

        let mut entity = fx.run();
        entity.recover(fx.now()).await.expect("recover");
        let state = entity.state().expect("state");
        let open = state
            .loop_state()
            .expect("the loop is started")
            .open_checkpoints();
        assert_eq!(open.len(), 1, "{ending:?}: one checkpoint opens");
        assert_eq!(
            open[0].kind,
            AgentCheckpointKind::IndeterminateEffectReconciliation
        );
        assert_eq!(
            open[0].checkpoint_id,
            AgentCheckpoint::id_for_effect(
                &parked.effect_id,
                parked.generation,
                AgentCheckpointKind::IndeterminateEffectReconciliation
            )
        );
        let decisions: Vec<&str> = open[0]
            .allowed_decisions
            .iter()
            .map(|decision| decision.value.as_str())
            .collect();
        assert_eq!(
            decisions,
            [
                "confirmed-completed",
                "confirmed-not-executed",
                "escalate",
                "abandon-and-fail"
            ],
            "{ending:?}: an ended run offers no compensate"
        );

        let snapshot = agent_operational_snapshot(
            &fx.runs,
            &run_scope(),
            &AgentSchemaPolicy::default(),
            fx.now(),
        )
        .await
        .expect("the point query answers")
        .expect("the run exists");
        assert_eq!(snapshot.open_checkpoints.len(), 1, "{ending:?}");

        let after = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(after.status, before.status, "{ending:?}");
        assert_eq!(after.terminal_reason, before.terminal_reason, "{ending:?}");
        assert_eq!(after.terminal_at, before.terminal_at, "{ending:?}");
    }
}

/// The ended run's park records exactly one `request-reconciliation`, in the
/// slot its effect's identity names, and a replay of the same result records
/// no second one.
#[tokio::test]
async fn an_ended_runs_ambiguous_promotion_records_one_reconciliation_open() {
    let sink = CountingDecisionSink::new();
    let (fx, _stores) = ended_world_recording(Ending::Cancelled, Some(sink.clone())).await;
    let scope = run_scope();
    let before = sink.events(&scope);
    assert!(
        !before.is_empty(),
        "the ended run recorded its own decisions"
    );
    let before_snapshot = fx.run_snapshot().await.expect("the run exists");
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "ambiguous")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let parked = resolve_promotion(
        &fx,
        AgentRunEffectOutcome::indeterminate(
            "dispatcher-lost-after-started",
            "the recovery retry was refused",
        ),
    )
    .await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);

    let opens = |sink: &CountingDecisionSink| -> Vec<rakka_agent::AgentDecisionEvent> {
        sink.events(&scope)
            .into_iter()
            .filter(|event| event.kind == rakka_agent::AgentDecisionKind::RequestReconciliation)
            .collect()
    };
    let opened = opens(&sink);
    assert_eq!(opened.len(), 1, "one reconciliation open");
    assert_eq!(
        opened[0].reason_code.as_deref(),
        Some("dispatcher-lost-after-started")
    );
    let slot = format!("ck-open-reconcile-t{}-s{}-g1", parked.turn, parked.slot);
    assert!(
        opened[0].operation_id.as_str().contains(&slot),
        "the open's operation id carries slot {slot}: {}",
        opened[0].operation_id.as_str()
    );
    let count = sink.events(&scope).len();
    assert_eq!(count, before.len() + 1, "the park added exactly one event");

    // A settle pass, then the same result re-applied: nothing more records.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("settle");
    let _ = run
        .apply(
            AgentRunEntityCommand::RecordEffectResult {
                operation_id: parked
                    .result_operation_id(&scope)
                    .expect("result operation id"),
                effect_id: parked.effect_id.clone(),
                generation: parked.generation,
                attempt: parked.attempts,
                fence: 0,
                outcome: Box::new(AgentRunEffectOutcome::indeterminate(
                    "dispatcher-lost-after-started",
                    "the recovery retry was refused",
                )),
            },
            &fx.router,
            fx.now(),
        )
        .await;
    assert_eq!(opens(&sink).len(), 1, "a replay records no second open");
    assert_eq!(sink.events(&scope).len(), count);
    assert_decision_stream_whole(&sink, &scope);

    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before_snapshot.status);
    assert_eq!(after.terminal_reason, before_snapshot.terminal_reason);
    assert_eq!(after.terminal_at, before_snapshot.terminal_at);
}

// ---------------------------------------------------------------------------
// Answering an ended run's parked promotion through
// `ResolveIndeterminateEffect` (issue #120).
// ---------------------------------------------------------------------------

/// Parks a promotion `Indeterminate` on the ended run, opening its
/// reconciliation checkpoint.
async fn park_promotion(fx: &Fixture, discriminator: &str) -> AgentRunEffect {
    let reply = apply_ok(fx, promote_command(promotion(1, 1), discriminator)).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let parked = resolve_promotion(
        fx,
        AgentRunEffectOutcome::indeterminate(
            "dispatcher-lost-after-started",
            "the recovery retry was refused",
        ),
    )
    .await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    parked
}

/// A `ResolveIndeterminateEffect` command under a checkpoint-resolution key.
fn resolve_indeterminate(
    key: &str,
    effect: &AgentRunEffect,
    generation: rakka_agent::AgentEffectGeneration,
    resolution: rakka_agent::AgentEffectResolution,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveIndeterminateEffect {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        effect_id: effect.effect_id.clone(),
        generation,
        resolution: Box::new(resolution),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

/// The checkpoints the ended run holds open.
async fn open_checkpoints(fx: &Fixture) -> Vec<AgentCheckpoint> {
    let mut entity = fx.run();
    entity.recover(fx.now()).await.expect("recover");
    entity
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .open_checkpoints()
        .to_vec()
}

/// Asserts the terminal record is exactly what `before` read.
async fn assert_terminal_unmoved(fx: &Fixture, before: &rakka_agent::AgentRunSnapshot, at: &str) {
    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before.status, "{at}: status");
    assert_eq!(
        after.terminal_reason, before.terminal_reason,
        "{at}: reason"
    );
    assert_eq!(after.terminal_at, before.terminal_at, "{at}: stamp");
}

/// `ConfirmedNotExecuted` on an ended run is a person's assertion that the
/// attempt never ran: the live arm runs, a new generation is minted, and the
/// ordinary outbox flushes it, however long ago the window closed. The
/// terminal record never moves, and a replayed decision answers `Duplicate`.
#[tokio::test]
async fn confirmed_not_executed_re_sends_an_ended_runs_promotion() {
    for ending in Ending::ALL {
        let (fx, stores) = ended_world(ending).await;
        let before = fx.run_snapshot().await.expect("the run exists");
        let terminal_at = before.terminal_at.expect("stamped").as_millis();
        let parked = park_promotion(&fx, "ambiguous").await;
        fx.clock.store(
            terminal_at + AGENT_POST_TERMINAL_MEMORY_WINDOW_DEFAULT_MS + 1,
            std::sync::atomic::Ordering::SeqCst,
        );

        let command = resolve_indeterminate(
            "not-executed",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        );
        let reply = apply_ok(&fx, command.clone()).await;
        assert!(
            matches!(reply, AgentRunEntityReply::Applied { .. }),
            "{ending:?}: {reply:?}"
        );
        let resent = promotion_record(&fx, &parked).await;
        assert_eq!(resent.generation, parked.generation.next(), "{ending:?}");
        assert!(
            matches!(
                resent.status,
                AgentRunEffectStatus::Pending | AgentRunEffectStatus::Ready
            ),
            "{ending:?}: {:?}",
            resent.status
        );
        assert!(open_checkpoints(&fx).await.is_empty(), "{ending:?}");
        assert_terminal_unmoved(&fx, &before, "after the decision").await;

        fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
            .await
            .expect("the re-sent promotion settles");
        let done = promotion_record(&fx, &parked).await;
        assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{ending:?}");
        assert_eq!(stores.private.len(&agent_scope()), 1, "{ending:?}");
        assert_terminal_unmoved(&fx, &before, "after the pump").await;

        let replay = apply_ok(&fx, command).await;
        assert!(
            matches!(replay, AgentRunEntityReply::Duplicate { .. }),
            "{ending:?}: {replay:?}"
        );
        let held = promotion_record(&fx, &parked).await;
        assert_eq!(held.generation, parked.generation.next(), "{ending:?}");
        assert_terminal_unmoved(&fx, &before, "after the replay").await;
    }
}

/// A re-send that finds memory retention has discharged the run's session
/// rows writes nothing and settles the effect `Failed` rather than parking it
/// again. The executor reports the discharged rows as a missing source (a
/// retryable attempt failure), which the in-process driver, having no attempt
/// machinery, surfaces as a failed effect.
#[tokio::test]
async fn a_re_sent_promotion_after_retention_fails_definitively() {
    let (fx, stores) = ended_world(Ending::Completed).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let terminal_at = before.terminal_at.expect("stamped");
    let parked = park_promotion(&fx, "ambiguous").await;
    let purged = stores
        .session
        .purge_run(
            &run_scope(),
            &rakka_agent::SessionRetentionPolicy::bounded_default().with_retain_for_millis(0),
            terminal_at,
            fx.now(),
        )
        .await
        .expect("the purge runs");
    assert!(purged.entries_deleted() > 0, "the session rows were purged");

    apply_ok(
        &fx,
        resolve_indeterminate(
            "not-executed",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await;
    fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
        .await
        .expect("the re-sent promotion settles");
    let done = promotion_record(&fx, &parked).await;
    assert_eq!(done.status, AgentRunEffectStatus::Failed);
    assert_eq!(
        done.last_error_code.as_deref(),
        Some("memory-promotion-attempt-failed")
    );
    assert!(stores.private.is_empty(&agent_scope()));
    assert_terminal_unmoved(&fx, &before, "after the failed re-send").await;
}

/// `ConfirmedCompleted` records the established outcome on the parked
/// generation, as the window's own outcomes land, and mints no generation.
#[tokio::test]
async fn confirmed_completed_records_an_ended_runs_promotion_outcome() {
    for ending in Ending::ALL {
        let (fx, _stores) = ended_world(ending).await;
        let before = fx.run_snapshot().await.expect("the run exists");
        let parked = park_promotion(&fx, "ambiguous").await;
        apply_ok(
            &fx,
            resolve_indeterminate(
                "completed",
                &parked,
                parked.generation,
                rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                    outcome: Box::new(AgentRunEffectOutcome::MemoryPromotion {
                        promoted: Vec::new(),
                    }),
                },
            ),
        )
        .await;
        let done = promotion_record(&fx, &parked).await;
        assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{ending:?}");
        assert_eq!(done.generation, parked.generation, "{ending:?}");
        assert!(open_checkpoints(&fx).await.is_empty(), "{ending:?}");
        assert_terminal_unmoved(&fx, &before, "after the decision").await;
    }
}

/// A re-send the run cannot afford is refused `run-redispatch-unaffordable`:
/// the checkpoint stays open and the effect stays parked at its generation.
#[tokio::test]
async fn an_unaffordable_re_send_on_an_ended_run_keeps_its_checkpoint() {
    // The two model calls spend two attempts; the first promotion reserves
    // its three and settles having made one, so a second reservation of three
    // overflows the ceiling.
    let (fx, _stores) = ended_world_configured(Ending::Completed, None, Some(5)).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let refusal = apply(
        &fx,
        resolve_indeterminate(
            "not-executed",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await
    .expect_err("an unaffordable re-send is refused");
    assert_eq!(refusal.code(), "run-redispatch-unaffordable");

    let held = promotion_record(&fx, &parked).await;
    assert_eq!(held.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(held.generation, parked.generation);
    assert_eq!(open_checkpoints(&fx).await.len(), 1);
    assert_terminal_unmoved(&fx, &before, "after the refusal").await;
}

/// An ended run answers only an outside-the-turn effect it holds; every other
/// target is `run-terminal`, and a held one gets the refusals a live run
/// gives.
#[tokio::test]
async fn an_ended_run_refuses_a_resolution_it_cannot_answer() {
    let (fx, _stores) = ended_world(Ending::Failed).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;

    let mut unknown = parked.clone();
    unknown.effect_id = rakka_agent_workflow::AgentEffectId::new("effect-nobody-holds");
    let refusal = apply(
        &fx,
        resolve_indeterminate(
            "unknown",
            &unknown,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await
    .expect_err("an unknown effect is refused");
    assert_eq!(refusal.code(), "run-terminal");

    // The failed tool effect is held by the run, but is not outside the turn.
    let mut entity = fx.run();
    entity.recover(fx.now()).await.expect("recover");
    let tool = entity
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::ToolCall)
        .cloned()
        .expect("the failed run holds its tool effect");
    let refusal = apply(
        &fx,
        resolve_indeterminate(
            "tool",
            &tool,
            tool.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await
    .expect_err("a tool effect is refused");
    assert_eq!(refusal.code(), "run-terminal");

    let refusal = apply(
        &fx,
        resolve_indeterminate(
            "stale-generation",
            &parked,
            parked.generation.next(),
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await
    .expect_err("a stale generation is refused");
    assert_eq!(refusal.code(), "run-stale-effect-generation");
    assert_eq!(open_checkpoints(&fx).await.len(), 1);

    apply_ok(
        &fx,
        resolve_indeterminate(
            "completed",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(AgentRunEffectOutcome::MemoryPromotion {
                    promoted: Vec::new(),
                }),
            },
        ),
    )
    .await;
    let refusal = apply(
        &fx,
        resolve_indeterminate(
            "second-decision",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await
    .expect_err("a settled effect takes no second decision");
    assert_eq!(refusal.code(), "run-stale-effect-result");
    assert_terminal_unmoved(&fx, &before, "after the refusals").await;
}

/// A re-sent generation that parks again opens its own checkpoint and records
/// its own `request-reconciliation`: two opens in generation order, one
/// `reconcile`, and no hole.
#[tokio::test]
async fn a_re_sent_generation_that_parks_again_opens_its_own_checkpoint() {
    let sink = CountingDecisionSink::new();
    let (fx, _stores) = ended_world_recording(Ending::Completed, Some(sink.clone())).await;
    let scope = run_scope();
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    apply_ok(
        &fx,
        resolve_indeterminate(
            "not-executed",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await;
    let second = resolve_promotion(
        &fx,
        AgentRunEffectOutcome::indeterminate("dispatcher-lost-after-started", "refused again"),
    )
    .await;
    assert_eq!(second.generation, parked.generation.next());
    assert_eq!(second.status, AgentRunEffectStatus::Indeterminate);

    let open = open_checkpoints(&fx).await;
    assert_eq!(
        open.len(),
        1,
        "only the new generation's checkpoint is open"
    );
    assert_eq!(
        open[0].checkpoint_id,
        AgentCheckpoint::id_for_effect(
            &second.effect_id,
            second.generation,
            AgentCheckpointKind::IndeterminateEffectReconciliation
        )
    );
    let events = sink.events(&scope);
    let opens: Vec<_> = events
        .iter()
        .filter(|event| event.kind == rakka_agent::AgentDecisionKind::RequestReconciliation)
        .collect();
    assert_eq!(opens.len(), 2);
    assert!(opens[0].operation_id.as_str().ends_with("-g1"));
    assert!(opens[1].operation_id.as_str().ends_with("-g2"));
    let reconciles = events
        .iter()
        .filter(|event| event.kind == rakka_agent::AgentDecisionKind::Reconcile)
        .count();
    assert_eq!(reconciles, 1);
    assert_decision_stream_whole(&sink, &scope);
    assert_terminal_unmoved(&fx, &before, "after the second park").await;
}

/// A host at the default `Version1` vocabulary records the ended run's
/// `reconcile` and nothing else: `request-reconciliation` is not written
/// under it, and the stream still round-trips through the `bc0c224` shape.
#[tokio::test]
async fn a_version_one_host_records_the_ended_runs_reconcile() {
    let sink = CountingDecisionSink::new();
    let (fx, _stores) = ended_world_configured(
        Ending::Completed,
        Some((
            sink.clone(),
            rakka_agent::AgentDecisionEventVocabulary::Version1,
        )),
        None,
    )
    .await;
    let scope = run_scope();
    let before = fx.run_snapshot().await.expect("the run exists");
    let ended = sink.events(&scope).len();
    assert!(ended > 0, "the ended run recorded its own decisions");
    let parked = park_promotion(&fx, "ambiguous").await;
    apply_ok(
        &fx,
        resolve_indeterminate(
            "completed",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(AgentRunEffectOutcome::MemoryPromotion {
                    promoted: Vec::new(),
                }),
            },
        ),
    )
    .await;
    let events = sink.events(&scope);
    let kinds: Vec<_> = events[ended..].iter().map(|event| event.kind).collect();
    assert_eq!(kinds, [rakka_agent::AgentDecisionKind::Reconcile]);
    assert_bc0c224_round_trips(&events);
    assert_decision_stream_whole(&sink, &scope);
    assert_terminal_unmoved(&fx, &before, "after the decision").await;
}

// ---------------------------------------------------------------------------
// Answering an ended run's parked promotion through `ResolveCheckpoint`
// (issue #120).
// ---------------------------------------------------------------------------

/// A `ResolveCheckpoint` command under a checkpoint-resolution key.
fn resolve_checkpoint_command(
    checkpoint_id: &rakka_agent_workflow::HumanCheckpointId,
    key: &str,
    decision: rakka_agent::AgentCheckpointDecision,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        checkpoint_id: checkpoint_id.clone(),
        resolver: requested_by(),
        decision: Box::new(decision),
        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
    }
}

/// A reconciliation checkpoint decision.
fn reconciliation(
    decision: rakka_agent::AgentReconciliationDecision,
) -> rakka_agent::AgentCheckpointDecision {
    rakka_agent::AgentCheckpointDecision::Reconciliation(decision)
}

/// `ConfirmedCompleted` carrying an empty promotion outcome.
fn confirmed_completed() -> rakka_agent::AgentReconciliationDecision {
    rakka_agent::AgentReconciliationDecision::ConfirmedCompleted {
        resolution: Box::new(rakka_agent::AgentEffectResolution::ConfirmedExecuted {
            outcome: Box::new(AgentRunEffectOutcome::MemoryPromotion {
                promoted: Vec::new(),
            }),
        }),
    }
}

/// The one checkpoint the ended run holds open.
async fn the_open_checkpoint(fx: &Fixture) -> AgentCheckpoint {
    let open = open_checkpoints(fx).await;
    assert_eq!(open.len(), 1, "one checkpoint is open");
    open.into_iter().next().expect("one checkpoint")
}

/// `ConfirmedNotExecuted` through the checkpoint: re-sent under generation 2,
/// which then lands `Succeeded`.
async fn checkpoint_confirms_not_executed(ending: Ending) {
    let (fx, stores) = ended_world(ending).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    let reply = apply_ok(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "not-executed",
            reconciliation(rakka_agent::AgentReconciliationDecision::ConfirmedNotExecuted),
        ),
    )
    .await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{ending:?}: {reply:?}"
    );
    let resent = promotion_record(&fx, &parked).await;
    assert_eq!(resent.generation, parked.generation.next(), "{ending:?}");
    assert!(open_checkpoints(&fx).await.is_empty(), "{ending:?}");
    assert_terminal_unmoved(&fx, &before, "after the re-send decision").await;
    fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
        .await
        .expect("the re-sent promotion settles");
    let done = promotion_record(&fx, &parked).await;
    assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{ending:?}");
    assert_eq!(stores.private.len(&agent_scope()), 1, "{ending:?}");
    assert_terminal_unmoved(&fx, &before, "after the pump").await;
}

/// `ConfirmedCompleted` through the checkpoint: the outcome lands on the same
/// generation.
async fn checkpoint_confirms_completed(ending: Ending) {
    let (fx, _stores) = ended_world(ending).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    apply_ok(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "completed",
            reconciliation(confirmed_completed()),
        ),
    )
    .await;
    let done = promotion_record(&fx, &parked).await;
    assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{ending:?}");
    assert_eq!(done.generation, parked.generation, "{ending:?}");
    assert!(open_checkpoints(&fx).await.is_empty(), "{ending:?}");
    assert_terminal_unmoved(&fx, &before, "after ConfirmedCompleted").await;
}

/// `AbandonAndFail` through the checkpoint: the generation fails, and the
/// run's own reason is untouched.
async fn checkpoint_abandons(ending: Ending) {
    let (fx, _stores) = ended_world(ending).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    apply_ok(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "abandon",
            reconciliation(rakka_agent::AgentReconciliationDecision::AbandonAndFail),
        ),
    )
    .await;
    let done = promotion_record(&fx, &parked).await;
    assert_eq!(done.status, AgentRunEffectStatus::Failed, "{ending:?}");
    assert_eq!(
        done.last_error_code.as_deref(),
        Some("reconciliation-abandoned"),
        "{ending:?}"
    );
    assert_eq!(done.generation, parked.generation, "{ending:?}");
    assert!(open_checkpoints(&fx).await.is_empty(), "{ending:?}");
    assert_terminal_unmoved(&fx, &before, "after AbandonAndFail").await;
}

/// `Escalate` through the checkpoint: it stays open, escalated, and the effect
/// stays parked.
async fn checkpoint_escalates(ending: Ending) {
    let (fx, _stores) = ended_world(ending).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    apply_ok(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "escalate",
            reconciliation(rakka_agent::AgentReconciliationDecision::Escalate),
        ),
    )
    .await;
    let open = the_open_checkpoint(&fx).await;
    assert_eq!(open.checkpoint_id, checkpoint.checkpoint_id, "{ending:?}");
    assert_eq!(
        open.status,
        rakka_agent::AgentCheckpointStatus::Escalated,
        "{ending:?}"
    );
    let held = promotion_record(&fx, &parked).await;
    assert_eq!(
        held.status,
        AgentRunEffectStatus::Indeterminate,
        "{ending:?}"
    );
    assert_eq!(held.generation, parked.generation, "{ending:?}");
    assert_terminal_unmoved(&fx, &before, "after Escalate").await;
}

/// A person's checkpoint decision answers an ended run's parked promotion
/// exactly as `ResolveIndeterminateEffect` does, and the terminal record never
/// moves.
#[tokio::test]
async fn a_checkpoint_decision_answers_an_ended_runs_promotion() {
    for ending in Ending::ALL {
        Box::pin(checkpoint_confirms_not_executed(ending)).await;
        Box::pin(checkpoint_confirms_completed(ending)).await;
        Box::pin(checkpoint_abandons(ending)).await;
        Box::pin(checkpoint_escalates(ending)).await;
    }
}

/// The same refusal through the checkpoint: `ConfirmedNotExecuted` the run
/// cannot afford answers `run-redispatch-unaffordable`, and the refused
/// transition is discarded whole, so the checkpoint (which the decision's arm
/// drops before the core refuses) stays open, the effect stays parked at its
/// generation, and the terminal record is unmoved.
#[tokio::test]
async fn an_unaffordable_re_send_through_the_checkpoint_keeps_it_open() {
    let (fx, _stores) = ended_world_configured(Ending::Completed, None, Some(5)).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    let refusal = apply(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "not-executed",
            reconciliation(rakka_agent::AgentReconciliationDecision::ConfirmedNotExecuted),
        ),
    )
    .await
    .expect_err("an unaffordable re-send is refused");
    assert_eq!(refusal.code(), "run-redispatch-unaffordable");

    let still = the_open_checkpoint(&fx).await;
    assert_eq!(still.checkpoint_id, checkpoint.checkpoint_id);
    assert_eq!(still.status, rakka_agent::AgentCheckpointStatus::Open);
    let held = promotion_record(&fx, &parked).await;
    assert_eq!(held.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(held.generation, parked.generation);
    assert_terminal_unmoved(&fx, &before, "after the refusal").await;
}

/// A compensation is new work the host did not ask for: `Compensate` on an
/// ended run's reconciliation checkpoint is `run-terminal`, and the checkpoint
/// and the parked effect are untouched.
#[tokio::test]
async fn compensate_is_refused_on_an_ended_run() {
    for ending in Ending::ALL {
        let (fx, _stores) = ended_world(ending).await;
        let before = fx.run_snapshot().await.expect("the run exists");
        let parked = park_promotion(&fx, "ambiguous").await;
        let checkpoint = the_open_checkpoint(&fx).await;
        let refusal = apply(
            &fx,
            resolve_checkpoint_command(
                &checkpoint.checkpoint_id,
                "compensate",
                reconciliation(rakka_agent::AgentReconciliationDecision::Compensate {
                    compensation: rakka_agent::AgentCompensationRef::new("refund-charge")
                        .expect("the compensation ref is valid"),
                }),
            ),
        )
        .await
        .expect_err("a compensation is refused on an ended run");
        assert_eq!(refusal.code(), "run-terminal", "{ending:?}");

        let still = the_open_checkpoint(&fx).await;
        assert_eq!(still.checkpoint_id, checkpoint.checkpoint_id, "{ending:?}");
        assert_eq!(
            still.status,
            rakka_agent::AgentCheckpointStatus::Open,
            "{ending:?}: still waiting"
        );
        let held = promotion_record(&fx, &parked).await;
        assert_eq!(
            held.status,
            AgentRunEffectStatus::Indeterminate,
            "{ending:?}"
        );
        assert_terminal_unmoved(&fx, &before, "after the refusal").await;
    }
}

/// The decisions the point query lists on the run's one open checkpoint.
async fn listed_decisions(fx: &Fixture) -> Vec<String> {
    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert_eq!(snapshot.open_checkpoints.len(), 1, "one checkpoint is open");
    snapshot.open_checkpoints[0]
        .allowed_decisions
        .iter()
        .map(|decision| decision.value.clone())
        .collect()
}

/// A reconciliation checkpoint the run opened while live keeps `compensate`
/// in its durable record after the run ends, because a checkpoint is never
/// rewritten. The point query stops listing it once the run has ended: an
/// ended run refuses `Compensate`, so its checkpoint never offers it. The run
/// ends with the checkpoint open through `settle_proposal`'s `Accepted` arm,
/// which terminates without waiting on a promotion, outside the turn.
#[tokio::test]
async fn an_ended_runs_snapshot_lists_no_compensate_on_a_checkpoint_opened_while_live() {
    use rakka_agent::testkit::ExchangeFault;

    let (fx, _stores) = promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;
    // Turn two proposes, and the proposal's deliveries are lost until after
    // the promotion parks, so the run rests on its proposal meanwhile.
    for _ in 0..12 {
        fx.task_transport.inject(ExchangeFault::LoseEnvelope);
    }
    drive_turn(&fx).await;
    crank(&fx).await;
    let resting = fx.run_snapshot().await.expect("the run exists");
    assert!(resting.proposal.is_some(), "the run rests on its proposal");
    let parked = park_promotion(&fx, "ambiguous").await;
    let live = fx.run_snapshot().await.expect("the run exists");
    assert!(
        !live.status.is_terminal(),
        "the promotion parks on a live run"
    );
    let opened = the_open_checkpoint(&fx).await;
    assert_eq!(
        opened.kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    assert_eq!(opened.bound_effect.effect_id, parked.effect_id);
    let all = [
        "confirmed-completed",
        "confirmed-not-executed",
        "compensate",
        "escalate",
        "abandon-and-fail",
    ];
    assert_eq!(
        listed_decisions(&fx).await,
        all,
        "a live run lists them all"
    );

    // The proposal is re-delivered once the lost deliveries run out, and the
    // task's acceptance ends the run with the checkpoint still open.
    for _ in 0..16 {
        fx.pump().await.expect("the loop runs");
        if fx
            .run_snapshot()
            .await
            .expect("the run exists")
            .status
            .is_terminal()
        {
            break;
        }
    }
    let ended = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(ended.status, AgentRunStatus::Completed);
    let held = the_open_checkpoint(&fx).await;
    assert_eq!(held.checkpoint_id, opened.checkpoint_id);
    let recorded: Vec<&str> = held
        .allowed_decisions
        .iter()
        .map(|decision| decision.value.as_str())
        .collect();
    assert_eq!(recorded, all, "the durable record keeps its list");
    assert_eq!(
        listed_decisions(&fx).await,
        [
            "confirmed-completed",
            "confirmed-not-executed",
            "escalate",
            "abandon-and-fail"
        ],
        "the point query drops compensate on an ended run"
    );
}

/// Two decisions for one park through different paths: the checkpoint a
/// `ResolveIndeterminateEffect` dropped is gone, so a decision naming it is
/// `run-terminal` and nothing is re-sent a second time.
#[tokio::test]
async fn a_dropped_checkpoint_on_an_ended_run_answers_run_terminal() {
    let (fx, _stores) = ended_world(Ending::Completed).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    apply_ok(
        &fx,
        resolve_indeterminate(
            "not-executed",
            &parked,
            parked.generation,
            rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
        ),
    )
    .await;
    assert!(open_checkpoints(&fx).await.is_empty());

    let refusal = apply(
        &fx,
        resolve_checkpoint_command(
            &checkpoint.checkpoint_id,
            "late",
            reconciliation(rakka_agent::AgentReconciliationDecision::ConfirmedNotExecuted),
        ),
    )
    .await
    .expect_err("the dropped checkpoint answers nothing");
    assert_eq!(refusal.code(), "run-terminal");
    let held = promotion_record(&fx, &parked).await;
    assert_eq!(held.generation, parked.generation.next());
    assert_terminal_unmoved(&fx, &before, "after the late decision").await;
}

/// What a confirmation of another kind's outcome would move on an ended run:
/// the loop's phase, its pending model turn and its goal-evaluation cell.
async fn loop_picture(
    fx: &Fixture,
) -> (
    AgentLoopPhase,
    Option<AgentModelTurn>,
    Option<rakka_agent::AgentGoalEvaluationCell>,
) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is started");
    (
        loop_state.phase(),
        loop_state.pending_turn().cloned(),
        loop_state.goal_evaluation().cloned(),
    )
}

/// Outcomes that do not settle a parked promotion: a model turn that calls a
/// tool, a goal evaluation's verdict, and a claim append's receipt.
fn foreign_outcomes(effect: &AgentRunEffect) -> Vec<(&'static str, AgentRunEffectOutcome)> {
    let record = rakka_agent::AgentGoalEvaluationRecord::new(
        rakka_agent::goal_evaluation_record_id(
            &run_scope(),
            effect.turn,
            effect.slot,
            effect.generation,
        )
        .expect("the evaluation id derives"),
        rakka_agent::AgentGoalId::new("ship-the-fix").expect("the goal id is valid"),
        rakka_agent::AgentPolicyRef::new("evaluator-v1").expect("the evaluator ref is valid"),
        rakka_agent::AgentGoalEvaluationMethodKind::DeterministicAssertion,
        rakka_agent::AgentRevisionNumber::INITIAL,
        rakka_agent::AgentGoalEvaluationOutcome::Satisfied,
        "criteria-met",
        Vec::new(),
        None,
        effect.effect_id.clone(),
        effect.generation,
        AgentTimestampMillis::new(1),
    )
    .expect("the evaluation record is valid");
    vec![
        (
            "model",
            AgentRunEffectOutcome::Model {
                turn: Box::new(tool_calling_turn("lookup")),
            },
        ),
        (
            "evaluation",
            AgentRunEffectOutcome::Evaluation {
                record: Box::new(record),
            },
        ),
        (
            "claim-append",
            AgentRunEffectOutcome::ClaimAppend {
                claim: rakka_agent::AgentCommunalClaimId::new("claim-elsewhere")
                    .expect("the claim id is valid"),
            },
        ),
    ]
}

/// Confirms each foreign outcome against an ended run's parked promotion,
/// through `ResolveCheckpoint` when `via_checkpoint` holds, else through
/// `ResolveIndeterminateEffect`, and checks each is refused
/// `effect-outcome-mismatch` with nothing moved. Then confirms `settling`, an
/// outcome that does settle the promotion, which the run accepts.
async fn foreign_confirmations_are_refused(
    ending: Ending,
    via_checkpoint: bool,
    settling: AgentRunEffectOutcome,
) {
    let (fx, _stores) = ended_world(ending).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    let picture = loop_picture(&fx).await;
    let confirm = |key: &str, outcome: AgentRunEffectOutcome| {
        let resolution = rakka_agent::AgentEffectResolution::ConfirmedExecuted {
            outcome: Box::new(outcome),
        };
        if via_checkpoint {
            resolve_checkpoint_command(
                &checkpoint.checkpoint_id,
                key,
                reconciliation(
                    rakka_agent::AgentReconciliationDecision::ConfirmedCompleted {
                        resolution: Box::new(resolution),
                    },
                ),
            )
        } else {
            resolve_indeterminate(key, &parked, parked.generation, resolution)
        }
    };

    for (label, outcome) in foreign_outcomes(&parked) {
        let at = format!("{ending:?}, a {label} outcome, via the checkpoint: {via_checkpoint}");
        let refusal = apply(&fx, confirm(label, outcome))
            .await
            .expect_err("a foreign outcome settles nothing");
        assert_eq!(refusal.code(), "effect-outcome-mismatch", "{at}");
        assert_eq!(
            loop_picture(&fx).await,
            picture,
            "{at}: the phase, pending turn and goal evaluation"
        );
        let held = promotion_record(&fx, &parked).await;
        assert_eq!(held.status, AgentRunEffectStatus::Indeterminate, "{at}");
        assert_eq!(held.generation, parked.generation, "{at}");
        assert_eq!(the_open_checkpoint(&fx).await, checkpoint, "{at}");
        assert_terminal_unmoved(&fx, &before, &at).await;
    }

    let resolved = settling.resolved_status();
    apply_ok(&fx, confirm("settling", settling)).await;
    let done = promotion_record(&fx, &parked).await;
    assert_eq!(done.status, resolved, "{ending:?}");
    assert_eq!(done.generation, parked.generation, "{ending:?}");
    assert!(open_checkpoints(&fx).await.is_empty(), "{ending:?}");
    assert_eq!(loop_picture(&fx).await, picture, "{ending:?}");
    assert_terminal_unmoved(&fx, &before, "after the settling confirmation").await;
}

/// `ResolveIndeterminateEffect` on an ended run's parked promotion refuses a
/// `ConfirmedExecuted` whose outcome belongs to another kind of effect.
/// Applied, a model turn would move the ended run's phase and leave it a
/// pending turn, and a goal evaluation would fill its goal-evaluation cell
/// and owe the task an exchange. A definitive failure still settles it.
#[tokio::test]
async fn an_ended_run_refuses_a_confirmation_carrying_another_kinds_outcome() {
    for ending in Ending::ALL {
        Box::pin(foreign_confirmations_are_refused(
            ending,
            false,
            AgentRunEffectOutcome::failed(
                "memory-promotion-attempt-failed",
                "the operator saw the promotion fail",
            ),
        ))
        .await;
    }
}

/// The same refusal through the reconciliation checkpoint's
/// `ConfirmedCompleted`: the refused transition is discarded whole, so the
/// checkpoint the decision's arm drops stays open. An exhausted outcome still
/// settles it.
#[tokio::test]
async fn an_ended_runs_checkpoint_refuses_a_confirmation_carrying_another_kinds_outcome() {
    for ending in Ending::ALL {
        Box::pin(foreign_confirmations_are_refused(
            ending,
            true,
            AgentRunEffectOutcome::exhausted(
                "memory-promotion-attempt-failed",
                "the operator saw the attempts run out",
            ),
        ))
        .await;
    }
}

/// The person's escalation and confirmation each record exactly one decision
/// event, in the effect's own slots; a replay of either answers `Duplicate`
/// and records nothing, and the stream has no hole.
#[tokio::test]
async fn an_ended_runs_checkpoint_decisions_record_their_events() {
    use rakka_agent::AgentDecisionKind as Kind;
    let sink = CountingDecisionSink::new();
    let (fx, _stores) = ended_world_recording(Ending::Completed, Some(sink.clone())).await;
    let scope = run_scope();
    let before = fx.run_snapshot().await.expect("the run exists");
    let ended = sink.events(&scope).len();
    assert!(ended > 0, "the ended run recorded its own decisions");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;

    let escalate = resolve_checkpoint_command(
        &checkpoint.checkpoint_id,
        "escalate",
        reconciliation(rakka_agent::AgentReconciliationDecision::Escalate),
    );
    apply_ok(&fx, escalate.clone()).await;
    let complete = resolve_checkpoint_command(
        &checkpoint.checkpoint_id,
        "completed",
        reconciliation(confirmed_completed()),
    );
    apply_ok(&fx, complete.clone()).await;

    let events = sink.events(&scope);
    let kinds: Vec<_> = events[ended..].iter().map(|event| event.kind).collect();
    assert_eq!(
        kinds,
        [
            Kind::RequestReconciliation,
            Kind::EscalateCheckpoint,
            Kind::Reconcile
        ]
    );
    assert_eq!(events[ended + 1].reason_code.as_deref(), Some("escalated"));
    let reconcile = &events[ended + 2];
    assert_eq!(
        reconcile.outcome,
        Some(rakka_agent::AgentDecisionOutcome::ConfirmedExecuted)
    );
    assert!(
        reconcile.operation_id.as_str().ends_with("-g1"),
        "{}",
        reconcile.operation_id.as_str()
    );
    let recorded = events.len();

    for replay in [escalate, complete] {
        let reply = apply_ok(&fx, replay).await;
        assert!(
            matches!(reply, AgentRunEntityReply::Duplicate { .. }),
            "{reply:?}"
        );
    }
    assert_eq!(
        sink.events(&scope).len(),
        recorded,
        "a replay records nothing"
    );
    let held = promotion_record(&fx, &parked).await;
    assert_eq!(held.status, AgentRunEffectStatus::Succeeded);
    assert_decision_stream_whole(&sink, &scope);
    assert_terminal_unmoved(&fx, &before, "after the replays").await;
}

/// A gated promotion commanded after the run ended opens its gate in the
/// command's own transition (issue #126): a `PromoteMemory` on an ended run,
/// under a spec that requires a checkpoint, records the effect and one
/// `Approval` bound to its generation. The snapshot's view of the approval
/// lists only decisions an ended run answers. Ungranted, the effect is not
/// handed to the dispatcher; a grant dispatches it through the ordinary
/// outbox and the memory lands. The terminal record never moves.
#[tokio::test]
async fn a_gated_promotion_on_an_ended_run_opens_its_approval() {
    let (mut fx, stores) = ended_world(Ending::Completed).await;
    fx.policies = gated_promotion_policies(rakka_agent::AgentCheckpointSla::default());
    let before = fx.run_snapshot().await.expect("the run exists");
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "gated")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    assert_gate_invariant(&fx).await;
    assert_terminal_unmoved(&fx, &before, "after the command").await;

    let effect = promotion_effect(&fx).await;
    assert!(effect.checkpoint_required, "the effect is gated");
    assert_eq!(effect.status, AgentRunEffectStatus::Pending);
    let approval = the_open_checkpoint(&fx).await;
    assert_eq!(approval.kind, AgentCheckpointKind::Approval);
    assert_eq!(approval.bound_effect.effect_id, effect.effect_id);
    assert_eq!(approval.bound_effect.generation, effect.generation);
    let listed = listed_decisions(&fx).await;
    assert_eq!(listed, ["approve", "deny"], "both answer an ended run");
    assert!(!listed.iter().any(|decision| decision == "compensate"));

    // A settle pass does not hand the ungranted effect to the dispatcher.
    crank(&fx).await;
    let held = promotion_record(&fx, &effect).await;
    assert_eq!(held.status, AgentRunEffectStatus::Pending);
    assert_eq!(held.attempts, 0);
    assert_terminal_unmoved(&fx, &before, "after the settle pass").await;

    let reply = apply_ok(&fx, grant_command(&approval.checkpoint_id, "approve")).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_gate_invariant(&fx).await;
    assert!(open_checkpoints(&fx).await.is_empty());
    assert_terminal_unmoved(&fx, &before, "after the grant").await;

    fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
        .await
        .expect("the granted promotion settles");
    let done = promotion_record(&fx, &effect).await;
    assert_eq!(done.status, AgentRunEffectStatus::Succeeded);
    assert_eq!(stores.private.len(&agent_scope()), 1);
    assert_gate_invariant(&fx).await;
    assert_terminal_unmoved(&fx, &before, "after the pump").await;
}

/// The owner-kill sweep over the post-terminal promotion flow: complete the
/// run uncrashed, then kill its owner at every durable write of the
/// promotion, recover, and retry under the same operation id. However the
/// owner died, the private store holds exactly the promoted memory at its
/// initial revision, the loop holds exactly one receipt, and the run is still
/// `Completed` with its stamp untouched.
#[tokio::test]
async fn a_post_terminal_promotion_survives_any_owner_loss() {
    /// Applies the promotion and drives it to rest, reporting whether it
    /// durably applied; a doomed pass reports `false` rather than panicking.
    async fn try_promote(fx: &Fixture) -> bool {
        for _round in 0..16 {
            let mut run = fx.run();
            if run.recover(fx.now()).await.is_err() {
                continue;
            }
            match Box::pin(run.apply(
                promote_command(promotion(1, 1), "sweep"),
                &fx.router,
                fx.now(),
            ))
            .await
            {
                Ok(AgentRunEntityReply::Applied { .. })
                | Ok(AgentRunEntityReply::Duplicate { .. }) => {}
                Ok(_) | Err(_) => continue,
            }
            if Box::pin(run.settle_side_effects(&fx.router, fx.now()))
                .await
                .is_err()
            {
                continue;
            }
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
            match Box::pin(run.apply(result, &fx.router, fx.now())).await {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
        false
    }

    // The reference flow, uncrashed, counts the writes of the post-terminal
    // promotion alone: the run's own life is not what this sweeps.
    let (reference, reference_stores) = ended_world(Ending::Completed).await;
    reference.runs.reset_writes();
    assert!(try_promote(&reference).await, "the reference flow promotes");
    let writes = reference.runs.writes();
    assert!(
        writes >= 3,
        "the post-terminal promotion should make several durable writes, saw {writes}"
    );
    assert_eq!(reference_stores.private.len(&agent_scope()), 1);

    sweep_crash_points(writes, |nth, point| async move {
        let (fx, stores) = ended_world(Ending::Completed).await;
        let stamp = fx.run_snapshot().await.expect("the run exists").terminal_at;

        fx.runs.crash_at(nth, point);
        let _ = try_promote(&fx).await;
        fx.runs.assert_crash_fired(nth, point);
        fx.runs.survive();

        assert!(
            try_promote(&fx).await,
            "crash {point:?} at write {nth} left the promotion inapplicable"
        );

        let run = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            run.status,
            AgentRunStatus::Completed,
            "crash {point:?} at write {nth} moved the terminal status"
        );
        assert_eq!(
            run.terminal_at, stamp,
            "crash {point:?} at write {nth} moved the terminal stamp"
        );
        let owner = agent_scope();
        assert_eq!(
            stores.private.len(&owner),
            1,
            "crash {point:?} at write {nth} duplicated or lost the promotion"
        );
        let listed = stores
            .private
            .list(
                &owner,
                PrivateMemoryCursor::start(),
                AgentTimestampMillis::new(1_000_000),
            )
            .await
            .expect("list");
        assert_eq!(listed.memories[0].revision.get(), 1);
        let mut entity = fx.run();
        entity.recover(fx.now()).await.expect("recover");
        let receipts = entity
            .state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .memory_promotions()
            .len();
        assert_eq!(
            receipts, 1,
            "crash {point:?} at write {nth} duplicated or lost the receipt"
        );
    })
    .await;
}

/// A live run resting on its result proposal has dropped the turn's resolved
/// effects but not begun another turn. A promotion committed there must take
/// a fresh slot rather than re-derive the resolved model call's identity —
/// which the pre-counter allocator did, and which the operation log then
/// answered `Duplicate` forever.
#[tokio::test]
async fn a_promotion_while_the_proposal_is_pending_derives_a_fresh_identity() {
    use rakka_agent::testkit::ExchangeFault;

    let (fx, stores) = promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;
    // Turn two proposes. Every delivery of the proposal is lost, so the
    // task's decision never comes back and the run rests on its proposal
    // with the turn cleared and no next turn begun — the recipe
    // `run_entity.rs` uses to hold a run there.
    for _ in 0..6 {
        fx.task_transport.inject(ExchangeFault::LoseEnvelope);
    }
    drive_turn(&fx).await;
    crank(&fx).await;
    let resting = fx.run_snapshot().await.expect("the run exists");
    assert!(resting.proposal.is_some(), "the run rests on its proposal");
    assert!(
        !resting.status.is_terminal(),
        "the decision has not come back: {:?}",
        resting.status
    );

    let reply = apply_ok(&fx, promote_command(promotion(1, 2), "while-proposed")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    assert_ne!(
        effect.effect_id,
        rakka_agent::effect_id_for(&run_scope(), 2, 0).expect("derives"),
        "the promotion did not reuse the resolved model call's identity"
    );
    crank(&fx).await;
    let answered = answer_promotion(&fx, &effect).await;
    assert!(
        matches!(answered, AgentRunEntityReply::Applied { .. }),
        "the promotion's result is its own, not a replay of the model's: {answered:?}"
    );
    fx.pump().await.expect("the loop runs to completion");
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
    assert_eq!(stores.private.len(&agent_scope()), 2);
}

/// A loop state persisted before the slot counter decodes it as zero; the
/// operation-log floor still allocates a post-terminal promotion a slot the
/// run has not already resolved, so the effect settles instead of answering
/// `Duplicate` from the finished turn's model result.
#[tokio::test]
async fn a_loop_state_persisted_before_the_slot_counter_still_allocates_a_fresh_slot() {
    let (fx, stores) = ended_world(Ending::Completed).await;
    let id = run_scope().persistence_id();
    let record = fx
        .runs
        .load(&id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    let loop_state = value["run"]["loop_state"]
        .as_object_mut()
        .expect("the loop state is an object");
    assert!(
        loop_state.remove("next_slot").is_some(),
        "the record carried the counter"
    );
    let legacy = serde_json::from_value(value).expect("the pre-counter record deserializes");
    fx.runs
        .compare_and_set(&id, record.revision, legacy)
        .await
        .expect("the pre-counter record persists");

    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "legacy")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let effect = promotion_effect(&fx).await;
    assert_ne!(
        effect.effect_id,
        rakka_agent::effect_id_for(&run_scope(), effect.turn, 0).expect("derives"),
        "the floor skipped the resolved model call's slot"
    );
    fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
        .await
        .expect("the legacy-record promotion settles");
    assert_eq!(stores.private.len(&agent_scope()), 1);
    assert_eq!(
        fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
}

// ===========================================================================
// A turn rests on the turn's own effects (gap slice 3): a promotion
// outstanding when the turn's last effect result lands never holds the turn
// open, and its own outcome never rests one. The exposure a consumer's
// sweep found — a promotion committed into a live run whose turn had a tool
// in flight parked the run `AwaitingTools` with nothing outstanding and
// nothing left to rest it.
// ===========================================================================

/// Where the run durably rests: phase, status, turn, and how many effects of
/// any kind are outstanding.
async fn resting_at(fx: &Fixture) -> (AgentLoopPhase, AgentRunStatus, u64, usize) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is started");
    (
        loop_state.phase(),
        state.status().expect("the run exists"),
        loop_state.turn(),
        loop_state.outstanding_effects().count(),
    )
}

/// Every dispatched, still-outstanding effect of `kind` the run holds, in
/// slot order.
async fn dispatched_effects_of(fx: &Fixture, kind: AgentRunEffectKind) -> Vec<AgentRunEffect> {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    let state = run.state().expect("state");
    state
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .filter(|effect| effect.kind() == kind && effect.status == AgentRunEffectStatus::Ready)
        .cloned()
        .collect()
}

/// The one dispatched, still-outstanding effect of `kind` the run holds.
async fn dispatched_effect(fx: &Fixture, kind: AgentRunEffectKind) -> AgentRunEffect {
    let effects = dispatched_effects_of(fx, kind).await;
    assert_eq!(effects.len(), 1, "exactly one dispatched {kind} effect");
    effects[0].clone()
}

/// Records the dispatcher's scripted answer to exactly one effect — a tool
/// call, a delegation send — and nothing else, so a test chooses which
/// result lands first.
async fn answer_effect(fx: &Fixture, effect: &AgentRunEffect) -> AgentRunEntityReply {
    let outcome = fx.dispatcher.answer(effect).await;
    apply_ok(
        fx,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
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

/// A tool-calling world cranked to turn one's tool wait with a promotion
/// committed beside the in-flight tool, both dispatched: the exact state a
/// sweep that promotes live runs produces. Returns the tool effect and the
/// promotion effect.
async fn mid_turn_promoting_world() -> (Fixture, Stores, AgentRunEffect, AgentRunEffect) {
    let (fx, stores) = tool_promoting_world();
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    // The turn-one model call answers with the tool call; the applying
    // transition commits and dispatches the tool effect.
    drive_turn(&fx).await;
    crank(&fx).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (AgentLoopPhase::AwaitingTools, 1),
        "the tool is in flight"
    );

    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "mid-turn")).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "the promotion applies to the live run: {reply:?}"
    );
    crank(&fx).await;
    let tool = dispatched_effect(&fx, AgentRunEffectKind::ToolCall).await;
    let promotion = dispatched_effect(&fx, AgentRunEffectKind::MemoryPromotionCall).await;
    (fx, stores, tool, promotion)
}

/// Proof 1, the wedge a consumer's sweep found: a promotion committed while
/// the turn's tool is in flight, and the tool result lands first. The turn
/// rests on the turn's own effects, so the run goes on to its next model
/// call with the promotion still outstanding; the promotion then resolves on
/// its own and the run completes. Before the fix the tool arm counted the
/// promotion among the effects the turn awaited, declined to rest, and the
/// promotion's own outcome rested nothing: the run parked `AwaitingTools`
/// with zero outstanding effects, permanently.
#[tokio::test]
async fn a_promotion_outstanding_when_the_tool_result_lands_does_not_hold_the_turn_open() {
    let (fx, stores, tool, promotion) = mid_turn_promoting_world().await;

    let reply = answer_effect(&fx, &tool).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let (phase, status, turn, outstanding) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (AgentLoopPhase::AwaitingModel, 2),
        "the tool result rested the turn and the run went on to its next model call \
         (status {status:?}, {outstanding} effect(s) outstanding)"
    );

    let reply = answer_promotion(&fx, &promotion).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        stores.private.len(&agent_scope()),
        1,
        "the promotion landed"
    );
}

/// Proof 2, the control: the promotion resolves first. Its outcome rests
/// nothing — the turn is still waiting on its tool — and the tool result then
/// rests the turn exactly as it always did.
#[tokio::test]
async fn a_promotion_resolving_before_the_tool_result_rests_nothing() {
    let (fx, stores, tool, promotion) = mid_turn_promoting_world().await;

    let reply = answer_promotion(&fx, &promotion).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let (phase, _, turn, outstanding) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn, outstanding),
        (AgentLoopPhase::AwaitingTools, 1, 1),
        "the promotion's outcome moved nothing: the turn still waits on its tool"
    );

    answer_effect(&fx, &tool).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!((phase, turn), (AgentLoopPhase::AwaitingModel, 2));
    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(stores.private.len(&agent_scope()), 1);
}

/// Delivers one child's result exchange to the parent run.
async fn deliver(fx: &Fixture, envelope: &rakka_agent::AgentExchangeEnvelope) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    let reply = Box::pin(run.accept(envelope, &fx.router, now))
        .await
        .expect("the delivery succeeds");
    assert!(reply.result().is_accepted(), "the child result is accepted");
}

/// Proof 4, the same exposure at another gated arm: a promotion committed
/// while a fan-out's delegation sends are in flight, and the last send's
/// receipt lands first. The turn rests on the closed group — `AwaitingChildren`
/// — with the promotion still outstanding; the children's results then
/// resume the run and it completes.
#[tokio::test]
async fn a_promotion_outstanding_when_the_last_delegation_send_lands_does_not_hold_the_turn_open() {
    let stores = stores();
    let executor = SkillNamedExecutor::new();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, fan_out_turn())
            .with_turn_for(2, common::proposing_turn()),
    )
    .with_a2a_send_executor(executor)
    .with_memory_promotion_executor(Arc::new(SessionMemoryPromotionExecutor::new(
        stores.session.clone(),
        stores.private.clone(),
    )));
    let fx = Fixture::new(dispatcher)
        .with_delegation(delegation_config_with_fan_in())
        .with_memory(
            AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
                .with_private_store(stores.private.clone()),
        );
    create_fan_out_task(&fx, None).await;
    crank(&fx).await;
    // The fan-out turn: two sends commit, the await verb closes the group.
    drive_turn(&fx).await;
    crank(&fx).await;
    let sends = dispatched_effects_of(&fx, AgentRunEffectKind::A2aSendCall).await;
    assert_eq!(sends.len(), 2, "both sends are in flight");

    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "mid-fan-out")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    crank(&fx).await;
    let promotion = dispatched_effect(&fx, AgentRunEffectKind::MemoryPromotionCall).await;

    answer_effect(&fx, &sends[0]).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (AgentLoopPhase::AwaitingTools, 1),
        "the second send is still in flight"
    );
    answer_effect(&fx, &sends[1]).await;
    let (phase, status, turn, outstanding) = resting_at(&fx).await;
    assert_eq!(
        (phase, status, turn),
        (AgentLoopPhase::AwaitingChildren, AgentRunStatus::Running, 1),
        "the last receipt rested the turn on the closed group \
         ({outstanding} effect(s) outstanding)"
    );

    let reply = answer_promotion(&fx, &promotion).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let children = committed_children(&fx).await;
    assert_eq!(children.len(), 2);
    for (delegation, child_task) in &children {
        let envelope =
            child_result_envelope(&fx, delegation, child_task, AgentTaskStatus::Completed);
        deliver(&fx, &envelope).await;
    }
    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        stores.private.len(&agent_scope()),
        1,
        "the promotion landed"
    );
}

/// Proof 5, the repair: a record a binary without this fix left wedged —
/// `AwaitingTools`, the tool's result recorded, the promotion resolved,
/// nothing outstanding — is rested by its next settle pass, whether or not
/// any further effect outcome ever arrives. The fixed code can no longer
/// produce the record, so it is written through the state store exactly as
/// the old tool arm and promotion arm left it.
#[tokio::test]
async fn a_record_wedged_before_the_fix_is_rested_by_its_next_settle_pass() {
    let (fx, _stores, tool, _promotion) = mid_turn_promoting_world().await;
    let id = run_scope().persistence_id();
    let record = fx
        .runs
        .load(&id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    let run = value["run"].as_object_mut().expect("the run is an object");
    assert_eq!(run["status"], serde_json::json!("waiting-for-effect"));
    let loop_state = run["loop_state"]
        .as_object_mut()
        .expect("the loop state is an object");
    assert_eq!(loop_state["phase"], serde_json::json!("awaiting-tools"));
    // Both dispatched effects resolved, as the old arms recorded them...
    let mut resolved = 0;
    for effect in loop_state["effects"]
        .as_array_mut()
        .expect("the effects are an array")
    {
        if effect["status"] == serde_json::json!("ready") {
            effect["status"] = serde_json::json!("succeeded");
            resolved += 1;
        }
    }
    assert_eq!(resolved, 2, "the tool and the promotion");
    // ...with the tool's result recorded for the turn, exactly as the old
    // tool arm recorded it before declining to rest the turn.
    let result = AgentToolResult {
        call_id: AgentToolCallId::new("call-1").expect("call id"),
        content: AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
        recorded_at: fx.now(),
        tool: Some(AgentToolId::new("lookup").expect("tool id")),
        effect_id: Some(tool.effect_id.clone()),
    };
    loop_state["tool_results"]
        .as_array_mut()
        .expect("the tool results are an array")
        .push(serde_json::to_value(&result).expect("the tool result serializes"));
    let wedged = serde_json::from_value(value).expect("the wedged record deserializes");
    fx.runs
        .compare_and_set(&id, record.revision, wedged)
        .await
        .expect("the wedged record persists");

    let (phase, status, turn, outstanding) = resting_at(&fx).await;
    assert_eq!(
        (phase, status, turn, outstanding),
        (
            AgentLoopPhase::AwaitingTools,
            AgentRunStatus::WaitingForEffect,
            1,
            0
        ),
        "the record is wedged: nothing outstanding and nothing left to rest it"
    );

    fx.pump()
        .await
        .expect("the settle pass rests the turn and the loop runs on");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::Completed,
        "the wedged run completed after the repair (phase {:?})",
        run.phase
    );
    assert_eq!(fx.dispatcher.model_calls(), 2, "turn two's model call ran");
}

/// Proof 6, the wind-down fence is unchanged: a promotion committed on a
/// live run whose turn has a tool in flight, when the run then winds down,
/// still dispatches. The tool result lands on the winding-down run and rests
/// nothing — a result never resumes a run that is quiescing — the promotion
/// resolves through the fence, and the run ends `Cancelled` with the memory
/// landed. (The dispatcher-side halves — the sweep never cancels an exempt
/// ticket — are `effect_dispatch.rs`'s two `..._survives_the_wind_down_fence`
/// proofs.)
#[tokio::test]
async fn a_promotion_committed_mid_turn_still_dispatches_when_the_run_winds_down() {
    let (fx, stores, tool, promotion) = mid_turn_promoting_world().await;
    apply_ok(
        &fx,
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("derivable"),
            reason: "operator stopped the work".to_string(),
        },
    )
    .await;
    let (_, status, _, _) = resting_at(&fx).await;
    assert_eq!(status, AgentRunStatus::Cancelling);

    answer_effect(&fx, &tool).await;
    let (phase, status, turn, outstanding) = resting_at(&fx).await;
    assert_eq!(
        (phase, status, turn, outstanding),
        (
            AgentLoopPhase::AwaitingTools,
            AgentRunStatus::Cancelling,
            1,
            1
        ),
        "a winding-down run records the result and rests nothing; the promotion is still owed"
    );

    let reply = answer_promotion(&fx, &promotion).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    fx.pump().await.expect("the wind-down settles");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Cancelled);
    assert_eq!(
        stores.private.len(&agent_scope()),
        1,
        "the promotion landed through the fence"
    );
    let mut entity = fx.run();
    let recover_at = fx.now();
    entity.recover(recover_at).await.expect("recover");
    let receipts = entity
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .memory_promotions()
        .len();
    assert_eq!(receipts, 1, "one bounded receipt on the terminal record");
}

// ---------------------------------------------------------------------------
// The park repair and the checkpoint timers on an ended run (issue #120).
// ---------------------------------------------------------------------------

/// Rewrites the stored record checkpoint-less, as a record already stuck in a
/// store, or one a `bc0c224` replica parked mid-roll, reads.
async fn unbind_checkpoints(fx: &Fixture) {
    let id = run_scope().persistence_id();
    let record = fx
        .runs
        .load(&id)
        .await
        .expect("the run record loads")
        .expect("the run record exists");
    let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
    let loop_state = value["run"]["loop_state"]
        .as_object_mut()
        .expect("the loop state is an object");
    loop_state.insert("open_checkpoints".to_string(), serde_json::json!([]));
    loop_state.insert("pending_checkpoint".to_string(), serde_json::Value::Null);
    let unbound = serde_json::from_value(value).expect("the checkpoint-less record deserializes");
    fx.runs
        .compare_and_set(&id, record.revision, unbound)
        .await
        .expect("the checkpoint-less record persists");
}

/// One settle pass: recover, then settle the side effects.
async fn settle_pass(fx: &Fixture) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    Box::pin(run.settle_side_effects(&fx.router, fx.now()))
        .await
        .expect("settle");
}

/// A park an ended run holds without its checkpoint heals on the next settle
/// pass: the pass opens exactly the checkpoint the park would have, once, with
/// no `compensate`, records one `request-reconciliation`, and never moves the
/// terminal record. The person's `ConfirmedCompleted` then settles it.
#[tokio::test]
async fn a_checkpoint_less_park_on_an_ended_run_heals_on_the_next_settle_pass() {
    use rakka_agent::AgentDecisionKind as Kind;
    // The park predates decision events, as a record an older binary left
    // does: the sink joins afterwards, so the repair's open is the first
    // `request-reconciliation` the stream sees.
    let (fx, _stores) = ended_world(Ending::Failed).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    assert_terminal_unmoved(&fx, &before, "after the park").await;
    let sink = CountingDecisionSink::new();
    let fx = fx
        .with_decision_events(sink.clone())
        .with_decision_event_vocabulary(rakka_agent::AgentDecisionEventVocabulary::Version2);
    let scope = run_scope();
    unbind_checkpoints(&fx).await;
    assert!(open_checkpoints(&fx).await.is_empty(), "none is open");
    assert_terminal_unmoved(&fx, &before, "after the rewrite").await;

    settle_pass(&fx).await;
    let open = the_open_checkpoint(&fx).await;
    assert_eq!(
        open.checkpoint_id,
        AgentCheckpoint::id_for_effect(
            &parked.effect_id,
            parked.generation,
            AgentCheckpointKind::IndeterminateEffectReconciliation
        )
    );
    let decisions: Vec<&str> = open
        .allowed_decisions
        .iter()
        .map(|decision| decision.value.as_str())
        .collect();
    assert_eq!(
        decisions,
        [
            "confirmed-completed",
            "confirmed-not-executed",
            "escalate",
            "abandon-and-fail"
        ],
        "the repaired checkpoint offers no compensate"
    );
    assert_terminal_unmoved(&fx, &before, "after the first settle pass").await;

    let recorded = sink.events(&scope);
    let opens: Vec<_> = recorded
        .iter()
        .filter(|event| event.kind == Kind::RequestReconciliation)
        .collect();
    assert_eq!(opens.len(), 1, "exactly one reconciliation open");
    let first: Vec<_> = recorded.iter().map(|event| event.kind).collect();
    assert_eq!(
        first,
        [Kind::RequestReconciliation],
        "the pass records only the open"
    );
    let slot = format!("ck-open-reconcile-t{}-s{}-g1", parked.turn, parked.slot);
    assert!(
        opens[0].operation_id.as_str().contains(&slot),
        "the open carries slot {slot}: {}",
        opens[0].operation_id.as_str()
    );

    settle_pass(&fx).await;
    assert_eq!(
        the_open_checkpoint(&fx).await.checkpoint_id,
        open.checkpoint_id
    );
    assert_eq!(
        sink.events(&scope).len(),
        recorded.len(),
        "a second pass adds nothing"
    );
    assert_terminal_unmoved(&fx, &before, "after the second settle pass").await;

    let complete = resolve_checkpoint_command(
        &open.checkpoint_id,
        "completed",
        reconciliation(confirmed_completed()),
    );
    apply_ok(&fx, complete.clone()).await;
    let done = promotion_record(&fx, &parked).await;
    assert_eq!(done.status, AgentRunEffectStatus::Succeeded);
    assert!(open_checkpoints(&fx).await.is_empty());
    let reply = apply_ok(&fx, complete).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Duplicate { .. }),
        "{reply:?}"
    );
    let events = sink.events(&scope);
    let kinds: Vec<_> = events
        .iter()
        .skip(recorded.len())
        .map(|event| event.kind)
        .collect();
    assert_eq!(
        kinds,
        [Kind::Reconcile],
        "the decision records one reconcile"
    );
    assert_decision_stream_whole(&sink, &scope);
    assert_terminal_unmoved(&fx, &before, "after ConfirmedCompleted").await;
}

/// A timer escalates an ended run's reconciliation checkpoint once its SLA is
/// due, records `escalate-checkpoint` with `sla-due`, and moves nothing of the
/// terminal record.
#[tokio::test]
async fn a_timer_escalates_an_ended_runs_reconciliation_checkpoint() {
    use rakka_agent::AgentDecisionKind as Kind;
    let sink = CountingDecisionSink::new();
    let policies = rakka_agent::AgentEffectPolicies::default().with_checkpoint_sla(
        rakka_agent::AgentCheckpointSla {
            due_after_ms: Some(1_000),
            expire_after_ms: None,
            escalation_target: Some("ops-oncall".to_string()),
        },
    );
    let (fx, _stores) = ended_world_with_policies(
        Ending::Completed,
        Some((
            sink.clone(),
            rakka_agent::AgentDecisionEventVocabulary::Version2,
        )),
        None,
        policies,
    )
    .await;
    let scope = run_scope();
    let before = fx.run_snapshot().await.expect("the run exists");
    let parked = park_promotion(&fx, "ambiguous").await;
    let checkpoint = the_open_checkpoint(&fx).await;
    assert_eq!(checkpoint.status, rakka_agent::AgentCheckpointStatus::Open);
    let due = checkpoint.due_at.expect("the SLA stamped a due time");
    assert_terminal_unmoved(&fx, &before, "after the park").await;
    let fired = sink.events(&scope).len();

    fx.clock
        .store(due.as_millis() + 1, std::sync::atomic::Ordering::SeqCst);
    let timer = AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            "timer",
        )
        .expect("the timer key derives"),
    };
    apply_ok(&fx, timer.clone()).await;
    let escalated = the_open_checkpoint(&fx).await;
    assert_eq!(escalated.checkpoint_id, checkpoint.checkpoint_id);
    assert_eq!(
        escalated.status,
        rakka_agent::AgentCheckpointStatus::Escalated
    );
    let held = promotion_record(&fx, &parked).await;
    assert_eq!(held.status, AgentRunEffectStatus::Indeterminate);
    let events = sink.events(&scope);
    let kinds: Vec<_> = events[fired..].iter().map(|event| event.kind).collect();
    assert_eq!(kinds, [Kind::EscalateCheckpoint]);
    assert_eq!(events[fired].reason_code.as_deref(), Some("sla-due"));
    assert_terminal_unmoved(&fx, &before, "after the timer").await;

    let reply = apply_ok(&fx, timer).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Duplicate { .. }),
        "{reply:?}"
    );
    assert_eq!(
        sink.events(&scope).len(),
        fired + 1,
        "a replay records nothing"
    );
    assert_decision_stream_whole(&sink, &scope);
    assert_terminal_unmoved(&fx, &before, "after the replay").await;
}

// ---------------------------------------------------------------------------
// A gated promotion committed in a tool-calling turn (issue #120).
// ---------------------------------------------------------------------------

/// Policies whose promotion spec requires a checkpoint, under `sla`.
fn gated_promotion_policies(
    sla: rakka_agent::AgentCheckpointSla,
) -> rakka_agent::AgentEffectPolicies {
    rakka_agent::AgentEffectPolicies::new()
        .with_memory_promotion_spec(
            rakka_agent::AgentEffectSpec::idempotent(3)
                .expect("the spec is valid")
                .with_checkpoint_required(),
        )
        .expect("the policies are valid")
        .with_checkpoint_sla(sla)
}

/// A live run whose first turn's model call answers with a tool call, with a
/// gated promotion committed while the run awaited that model call. The
/// promotion's approval opens with the command, outside the turn, and the
/// tool-calling turn opens no second one (issue #126). Returned after the
/// model answered: the tool-call transition has run. With a `sink`, the run
/// records its decision events there under the vocabulary that writes every
/// kind.
async fn gated_promotion_in_a_tool_turn(
    sla: rakka_agent::AgentCheckpointSla,
    sink: Option<Arc<CountingDecisionSink>>,
) -> (Fixture, Stores) {
    let stores = stores();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    )
    .with_memory_promotion_executor(Arc::new(SessionMemoryPromotionExecutor::new(
        stores.session.clone(),
        stores.private.clone(),
    )));
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    let mut fx = match sink {
        Some(sink) => fx
            .with_decision_events(sink)
            .with_decision_event_vocabulary(rakka_agent::AgentDecisionEventVocabulary::Version2),
        None => fx,
    };
    fx.policies = gated_promotion_policies(sla);
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (AgentLoopPhase::AwaitingModel, 1),
        "the run awaits its model call"
    );
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "gated")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    // The model answers with a tool call in the same turn.
    drive_turn(&fx).await;
    (fx, stores)
}

/// A gated promotion opens its approval in the command's own transition, not
/// in the turn that later calls tools (issue #126). The approval exists as
/// soon as `PromoteMemory` applies, while the run still awaits its model
/// call; the tool-calling turn that follows opens no second one and leaves
/// the first untouched. The gate is outside the turn, so after the command
/// and after the turn the run rests exactly where the same world with an
/// ungated promotion rests.
#[tokio::test]
async fn a_gated_promotion_opens_one_approval_at_the_command() {
    // The same world with an ungated promotion: where the run rests after the
    // command, and after the tool-calling turn.
    let (plain, _plain_stores) = live_world(
        Rest::AwaitingModel,
        rakka_agent::AgentEffectPolicies::default(),
    )
    .await;
    apply_ok(&plain, promote_command(promotion(1, 1), "gated")).await;
    assert_gate_invariant(&plain).await;
    let plain_commanded = status_and_phase(&plain).await;
    drive_turn(&plain).await;
    let plain_called = status_and_phase(&plain).await;

    let (fx, _stores) = live_world(
        Rest::AwaitingModel,
        gated_promotion_policies(rakka_agent::AgentCheckpointSla::default()),
    )
    .await;
    let promotion = promote_gated(&fx, "gated").await;
    assert!(promotion.checkpoint_required, "the effect is gated");
    let open = open_checkpoints(&fx).await;
    assert_eq!(open.len(), 1, "the command opened the approval: {open:?}");
    assert_eq!(open[0].kind, AgentCheckpointKind::Approval);
    assert_eq!(open[0].bound_effect.effect_id, promotion.effect_id);
    assert_eq!(open[0].bound_effect.generation, promotion.generation);
    assert_eq!(status_and_phase(&fx).await, plain_commanded);

    // The model answers with a tool call in the same turn.
    drive_turn(&fx).await;
    assert_gate_invariant(&fx).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (AgentLoopPhase::AwaitingTools, 1),
        "the turn called its tool"
    );
    assert_eq!(
        open_checkpoints(&fx).await,
        open,
        "the tool-calling turn opened no second gate and left the first as it was"
    );
    assert_eq!(status_and_phase(&fx).await, plain_called);
}

/// The promotion effect the run holds, whatever its status.
async fn open_promotion(fx: &Fixture) -> AgentRunEffect {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::MemoryPromotionCall)
        .cloned()
        .expect("the promotion is held")
}

/// Pumps the run to its end and checks the gated promotion's approval
/// survives it. The run ends `Completed`: the task accepts the second turn's
/// proposal, and `settle_proposal`'s `Accepted` arm terminates without waiting
/// on the promotion, which is outside the turn. (A run that fails instead
/// winds down `Cancelling` and waits on the pending approval; it never
/// reaches a terminal status while the approval is open.) Returns the run's
/// snapshot and the approval.
async fn end_the_run_leaving_the_approval(
    fx: &Fixture,
) -> (rakka_agent::AgentRunSnapshot, AgentCheckpoint) {
    fx.pump().await.expect("the loop runs to its end");
    let ended = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(ended.status, AgentRunStatus::Completed);
    assert!(ended.terminal_at.is_some());
    let approval = the_open_checkpoint(fx).await;
    assert_eq!(approval.kind, AgentCheckpointKind::Approval);
    let promotion = open_promotion(fx).await;
    assert_eq!(approval.bound_effect.effect_id, promotion.effect_id);
    assert_eq!(promotion.status, AgentRunEffectStatus::Pending);
    assert_eq!(promotion.attempts, 0);
    (ended, approval)
}

/// The decision events the run recorded up to its end, checking that the
/// run's `complete` is the last of them.
fn events_through_the_end(sink: &CountingDecisionSink) -> usize {
    let events = sink.events(&run_scope());
    assert_eq!(
        events.last().map(|event| event.kind),
        Some(rakka_agent::AgentDecisionKind::Complete),
        "the run's end is the last event it recorded"
    );
    events.len()
}

/// A grant on an ended run's gated promotion approval dispatches the
/// promotion without moving the terminal record: the `Granted` arm must not
/// rewrite the status from the checkpoint-wait projection. The grant records
/// exactly one `resolve-checkpoint` after the run's end, the dispatch records
/// nothing, a replayed grant records nothing, and the stream has no hole.
#[tokio::test]
async fn a_granted_gated_promotion_on_an_ended_run_dispatches_without_moving_the_status() {
    use rakka_agent::AgentDecisionKind as Kind;
    let sink = CountingDecisionSink::new();
    let scope = run_scope();
    let (fx, stores) = gated_promotion_in_a_tool_turn(
        rakka_agent::AgentCheckpointSla::default(),
        Some(sink.clone()),
    )
    .await;
    let (before, approval) = end_the_run_leaving_the_approval(&fx).await;
    assert_terminal_unmoved(&fx, &before, "after the run ended").await;
    let ended = events_through_the_end(&sink);

    let grant = resolve_checkpoint_command(
        &approval.checkpoint_id,
        "approve",
        rakka_agent::AgentCheckpointDecision::Approval(
            rakka_agent::AgentApprovalDecision::Approve {
                credential_binding: None,
                expires_at: AgentTimestampMillis::new(u64::MAX / 2),
                allowed_use_count: 1,
            },
        ),
    );
    let reply = apply_ok(&fx, grant.clone()).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert!(open_checkpoints(&fx).await.is_empty());
    assert_terminal_unmoved(&fx, &before, "after the grant").await;

    fx.pump_post_terminal(AgentRunEffectKind::MemoryPromotionCall)
        .await
        .expect("the granted promotion settles");
    let done = open_promotion(&fx).await;
    assert_eq!(done.status, AgentRunEffectStatus::Succeeded);
    assert_eq!(stores.private.len(&agent_scope()), 1);
    assert_terminal_unmoved(&fx, &before, "after the pump").await;

    let events = sink.events(&scope);
    let kinds: Vec<_> = events[ended..].iter().map(|event| event.kind).collect();
    assert_eq!(kinds, [Kind::ResolveCheckpoint]);
    let resolved = &events[ended];
    assert_eq!(
        resolved.outcome,
        Some(rakka_agent::AgentDecisionOutcome::Granted)
    );
    assert_eq!(resolved.reason_code.as_deref(), Some("granted"));
    assert_eq!(
        resolved.source,
        rakka_agent::AgentDecisionSource::Human,
        "a person granted it"
    );

    let replay = apply_ok(&fx, grant).await;
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "{replay:?}"
    );
    assert_eq!(
        sink.events(&scope).len(),
        ended + 1,
        "a replay records nothing"
    );
    assert_decision_stream_whole(&sink, &scope);
    assert_terminal_unmoved(&fx, &before, "after the replay").await;
}

/// A timer expires an ended run's gated promotion approval: the promotion's
/// generation fails, nothing else does, and the terminal record is unmoved.
/// The expiry records exactly one `resolve-checkpoint` after the run's end, a
/// replayed timer records nothing, and the stream has no hole.
#[tokio::test]
async fn a_timer_expires_an_ended_runs_gated_promotion_approval() {
    use rakka_agent::AgentDecisionKind as Kind;
    let sink = CountingDecisionSink::new();
    let scope = run_scope();
    let (fx, _stores) = gated_promotion_in_a_tool_turn(
        rakka_agent::AgentCheckpointSla {
            due_after_ms: None,
            expire_after_ms: Some(1_000),
            escalation_target: None,
        },
        Some(sink.clone()),
    )
    .await;
    let (before, approval) = end_the_run_leaving_the_approval(&fx).await;
    let ended = events_through_the_end(&sink);
    let expires = approval.expires_at.expect("the SLA stamped an expiry");
    let effects_before = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .effects()
            .to_vec()
    };

    fx.clock
        .store(expires.as_millis() + 1, std::sync::atomic::Ordering::SeqCst);
    let timer = AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            "timer",
        )
        .expect("the timer key derives"),
    };
    apply_ok(&fx, timer.clone()).await;

    assert!(
        open_checkpoints(&fx).await.is_empty(),
        "the approval expired"
    );
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let effects_after = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .effects()
        .to_vec();
    for after in &effects_after {
        let was = effects_before
            .iter()
            .find(|held| held.effect_id == after.effect_id)
            .expect("no effect appears");
        if after.kind() == AgentRunEffectKind::MemoryPromotionCall {
            assert_eq!(after.status, AgentRunEffectStatus::Failed);
            assert_eq!(after.generation, was.generation);
        } else {
            assert_eq!(after, was, "only the promotion's generation changes");
        }
    }
    assert_terminal_unmoved(&fx, &before, "after the timer").await;

    let events = sink.events(&scope);
    let kinds: Vec<_> = events[ended..].iter().map(|event| event.kind).collect();
    assert_eq!(kinds, [Kind::ResolveCheckpoint]);
    let expired = &events[ended];
    assert_eq!(
        expired.outcome,
        Some(rakka_agent::AgentDecisionOutcome::Expired)
    );
    assert_eq!(expired.reason_code.as_deref(), Some("expired"));
    assert_eq!(
        expired.source,
        rakka_agent::AgentDecisionSource::DeterministicPolicy,
        "the timer expired it"
    );

    let replay = apply_ok(&fx, timer).await;
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "{replay:?}"
    );
    assert_eq!(
        sink.events(&scope).len(),
        ended + 1,
        "a replay records nothing"
    );
    assert_decision_stream_whole(&sink, &scope);
    assert_terminal_unmoved(&fx, &before, "after the replay").await;
}

// ---------------------------------------------------------------------------
// A gated promotion opens its gate at the command, outside the turn
// (issue #126).
// ---------------------------------------------------------------------------

/// Which gate a deployment's promotion spec requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// A checkpoint alone.
    Checkpoint,
    /// A security authorization alone.
    Authorization,
    /// Both a checkpoint and a security authorization.
    Both,
}

impl Gate {
    const ALL: [Self; 3] = [Self::Checkpoint, Self::Authorization, Self::Both];

    /// The promotion spec that requires this gate.
    fn spec(self) -> rakka_agent::AgentEffectSpec {
        let spec = rakka_agent::AgentEffectSpec::idempotent(3).expect("the spec is valid");
        match self {
            Self::Checkpoint => spec.with_checkpoint_required(),
            Self::Authorization => spec.with_authorization_required(),
            Self::Both => spec
                .with_checkpoint_required()
                .with_authorization_required(),
        }
    }

    /// The checkpoint kind the gate opens: an authorization wins over a
    /// checkpoint, as at every other site that opens a gate.
    fn kind(self) -> AgentCheckpointKind {
        match self {
            Self::Checkpoint => AgentCheckpointKind::Approval,
            Self::Authorization | Self::Both => AgentCheckpointKind::SecurityAuthorization,
        }
    }

    /// The decision event the gate's opening records.
    fn request(self) -> rakka_agent::AgentDecisionKind {
        match self.kind() {
            AgentCheckpointKind::Approval => rakka_agent::AgentDecisionKind::RequestApproval,
            _ => rakka_agent::AgentDecisionKind::RequestAuthorization,
        }
    }
}

/// Policies whose promotion spec requires `gate`, under `sla`.
fn policies_gating(
    gate: Gate,
    sla: rakka_agent::AgentCheckpointSla,
) -> rakka_agent::AgentEffectPolicies {
    rakka_agent::AgentEffectPolicies::new()
        .with_memory_promotion_spec(gate.spec())
        .expect("the policies are valid")
        .with_checkpoint_sla(sla)
}

/// An SLA that expires a gate a minute after it opens, so a test can expire
/// one by moving the clock past it.
fn expiring_sla() -> rakka_agent::AgentCheckpointSla {
    rakka_agent::AgentCheckpointSla {
        due_after_ms: None,
        expire_after_ms: Some(60_000),
        escalation_target: None,
    }
}

/// Where a live run rests when a gated promotion is commanded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rest {
    /// Turn one's model call is in flight.
    AwaitingModel,
    /// Turn one's tool call is in flight.
    AwaitingTools,
    /// Turn two's result proposal awaits the task's decision.
    OnProposal,
}

impl Rest {
    const ALL: [Self; 3] = [Self::AwaitingModel, Self::AwaitingTools, Self::OnProposal];
}

/// A live run under `policies`, driven to `rest`. The session holds the task
/// input at sequence one in every rest.
async fn live_world(rest: Rest, policies: rakka_agent::AgentEffectPolicies) -> (Fixture, Stores) {
    use rakka_agent::testkit::ExchangeFault;

    let (mut fx, stores) = match rest {
        Rest::AwaitingModel | Rest::AwaitingTools => tool_promoting_world(),
        Rest::OnProposal => promoting_world(),
    };
    fx.policies = policies;
    Box::pin(fx.instantiate_agent()).await;
    Box::pin(fx.create_task()).await;
    crank(&fx).await;
    match rest {
        Rest::AwaitingModel => {}
        Rest::AwaitingTools => {
            drive_turn(&fx).await;
            crank(&fx).await;
        }
        Rest::OnProposal => {
            drive_turn(&fx).await;
            crank(&fx).await;
            // Turn two proposes, and the proposal's deliveries are lost for
            // the rest of the test, so the run rests on its proposal.
            for _ in 0..64 {
                fx.task_transport.inject(ExchangeFault::LoseEnvelope);
            }
            drive_turn(&fx).await;
            crank(&fx).await;
        }
    }
    let (phase, _, turn, _) = resting_at(&fx).await;
    match rest {
        Rest::AwaitingModel => assert_eq!((phase, turn), (AgentLoopPhase::AwaitingModel, 1)),
        Rest::AwaitingTools => assert_eq!((phase, turn), (AgentLoopPhase::AwaitingTools, 1)),
        Rest::OnProposal => {
            let resting = fx.run_snapshot().await.expect("the run exists");
            assert!(resting.proposal.is_some(), "the run rests on its proposal");
        }
    }
    (fx, stores)
}

/// The run's status and loop phase.
async fn status_and_phase(fx: &Fixture) -> (AgentRunStatus, AgentLoopPhase) {
    let (phase, status, _, _) = resting_at(fx).await;
    (status, phase)
}

/// Commands a promotion of the task input under `discriminator`, checks the
/// gate invariant, and returns the promotion's effect.
async fn promote_gated(fx: &Fixture, discriminator: &str) -> AgentRunEffect {
    let reply = apply_ok(fx, promote_command(promotion(1, 1), discriminator)).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_gate_invariant(fx).await;
    promotion_effect(fx).await
}

/// The one waiting gate bound to `effect`'s current generation.
async fn the_gate_of(fx: &Fixture, effect: &AgentRunEffect) -> AgentCheckpoint {
    let held = promotion_record(fx, effect).await;
    let gates: Vec<AgentCheckpoint> = open_checkpoints(fx)
        .await
        .into_iter()
        .filter(|checkpoint| {
            checkpoint.status.is_waiting()
                && checkpoint.kind != AgentCheckpointKind::IndeterminateEffectReconciliation
                && checkpoint.bound_effect.effect_id == held.effect_id
                && checkpoint.bound_effect.generation == held.generation
        })
        .collect();
    assert_eq!(gates.len(), 1, "one gate is bound to the effect: {gates:?}");
    gates.into_iter().next().expect("one gate")
}

/// A person's grant of a gate.
fn grant_command(
    checkpoint_id: &rakka_agent_workflow::HumanCheckpointId,
    key: &str,
) -> AgentRunEntityCommand {
    resolve_checkpoint_command(
        checkpoint_id,
        key,
        rakka_agent::AgentCheckpointDecision::Approval(
            rakka_agent::AgentApprovalDecision::Approve {
                credential_binding: None,
                expires_at: AgentTimestampMillis::new(u64::MAX / 2),
                allowed_use_count: 1,
            },
        ),
    )
}

/// A person's denial of a gate.
fn deny_command(
    checkpoint_id: &rakka_agent_workflow::HumanCheckpointId,
    key: &str,
) -> AgentRunEntityCommand {
    resolve_checkpoint_command(
        checkpoint_id,
        key,
        rakka_agent::AgentCheckpointDecision::Approval(rakka_agent::AgentApprovalDecision::Deny {
            reason: "not this memory".to_string(),
        }),
    )
}

/// The durable checkpoint-timer sweep under `key`.
fn timer_command(key: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the timer key derives"),
    }
}

/// The operator's cancellation of the run.
fn cancel_command() -> AgentRunEntityCommand {
    cancel_command_under("1")
}

/// An operator's cancellation of the run under its own operation id, so a
/// second cancel is a new command rather than a replay of the first.
fn cancel_command_under(discriminator: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::Cancel {
        operation_id: AgentOperationId::new(
            AgentOperationKind::Cancellation,
            [TENANT, AGENT, discriminator],
        )
        .expect("derivable"),
        reason: "operator stopped the work".to_string(),
    }
}

/// How a test answers a gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// A person grants it.
    Grant,
    /// A person denies it.
    Deny,
    /// The SLA timer expires it.
    Expire,
}

impl Answer {
    const ALL: [Self; 3] = [Self::Grant, Self::Deny, Self::Expire];
}

/// Answers `gate` as `answer`, and checks the gate invariant after the
/// command. An expiry moves the clock past the gate's deadline first.
async fn answer_gate(fx: &Fixture, gate: &AgentCheckpoint, answer: Answer) {
    let command = match answer {
        Answer::Grant => grant_command(&gate.checkpoint_id, "grant"),
        Answer::Deny => deny_command(&gate.checkpoint_id, "deny"),
        Answer::Expire => {
            let expires = gate.expires_at.expect("the SLA stamped an expiry");
            fx.clock
                .store(expires.as_millis() + 1, std::sync::atomic::Ordering::SeqCst);
            timer_command("expire")
        }
    };
    let reply = apply_ok(fx, command).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{answer:?}: {reply:?}"
    );
    assert_gate_invariant(fx).await;
}

/// A promotion's `PromoteMemory` opens its gate on a live run, in the
/// command's own transition: an `Approval` when the spec requires a
/// checkpoint alone, a `SecurityAuthorization` when it requires an
/// authorization, with or without a checkpoint. The gate binds the effect's
/// generation, and a grant of either kind dispatches the promotion.
#[tokio::test]
async fn a_gated_promotion_by_command_opens_its_gate_on_a_live_run() {
    for gate in Gate::ALL {
        let (fx, stores) = live_world(
            Rest::AwaitingModel,
            policies_gating(gate, rakka_agent::AgentCheckpointSla::default()),
        )
        .await;
        let resting = status_and_phase(&fx).await;
        let promotion = promote_gated(&fx, "gated").await;
        assert_eq!(promotion.status, AgentRunEffectStatus::Pending, "{gate:?}");
        let opened = the_gate_of(&fx, &promotion).await;
        assert_eq!(opened.kind, gate.kind(), "{gate:?}");
        assert_eq!(open_checkpoints(&fx).await.len(), 1, "{gate:?}: one gate");
        assert_eq!(status_and_phase(&fx).await, resting, "{gate:?}");

        answer_gate(&fx, &opened, Answer::Grant).await;
        let granted = promotion_record(&fx, &promotion).await;
        assert_eq!(granted.status, AgentRunEffectStatus::Ready, "{gate:?}");
        answer_promotion(&fx, &granted).await;
        assert_gate_invariant(&fx).await;
        let done = promotion_record(&fx, &promotion).await;
        assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{gate:?}");
        assert_eq!(stores.private.len(&agent_scope()), 1, "{gate:?}");
        assert_eq!(status_and_phase(&fx).await, resting, "{gate:?}");
    }
}

/// A promotion's gate is outside the turn: in each rest a live run holds, the
/// command that opens it, a grant, a denial and an expiry each leave the
/// run's status and phase where they were. A granted promotion dispatches and
/// its receipt lands; a denied or expired one fails, and the run does not
/// wind down.
#[tokio::test]
async fn a_promotion_gate_moves_no_status_or_phase() {
    for rest in Rest::ALL {
        for answer in Answer::ALL {
            let at = format!("{rest:?}, {answer:?}");
            let (fx, stores) =
                live_world(rest, policies_gating(Gate::Checkpoint, expiring_sla())).await;
            let resting = status_and_phase(&fx).await;
            let promotion = promote_gated(&fx, "gated").await;
            assert_eq!(status_and_phase(&fx).await, resting, "{at}: the command");
            let gate = the_gate_of(&fx, &promotion).await;
            assert_eq!(gate.kind, AgentCheckpointKind::Approval, "{at}");

            answer_gate(&fx, &gate, answer).await;
            assert_eq!(status_and_phase(&fx).await, resting, "{at}: the answer");
            assert!(open_checkpoints(&fx).await.is_empty(), "{at}");
            match answer {
                Answer::Grant => {
                    let granted = promotion_record(&fx, &promotion).await;
                    assert_eq!(granted.status, AgentRunEffectStatus::Ready, "{at}");
                    let reply = answer_promotion(&fx, &granted).await;
                    assert!(
                        matches!(reply, AgentRunEntityReply::Applied { .. }),
                        "{at}: {reply:?}"
                    );
                    assert_gate_invariant(&fx).await;
                    let done = promotion_record(&fx, &promotion).await;
                    assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{at}");
                    assert_eq!(stores.private.len(&agent_scope()), 1, "{at}");
                    assert_eq!(status_and_phase(&fx).await, resting, "{at}: the receipt");
                }
                Answer::Deny | Answer::Expire => {
                    let failed = promotion_record(&fx, &promotion).await;
                    assert_eq!(failed.status, AgentRunEffectStatus::Failed, "{at}");
                    let run = fx.run_snapshot().await.expect("the run exists");
                    assert_eq!(run.terminal_reason, None, "{at}: not winding down");
                    assert_ne!(run.status, AgentRunStatus::Cancelling, "{at}");
                }
            }
        }
    }
}

/// A live run whose one tool fails, under `policies`: a gated promotion is
/// commanded while the tool is in flight, and then the tool's failure lands.
/// The run winds down `Cancelling` on the promotion, whose gate is still open.
/// Returns the promotion, its gate, and the winding-down run's snapshot.
async fn failing_world_with_an_open_gate(
    policies: rakka_agent::AgentEffectPolicies,
) -> (
    Fixture,
    Stores,
    AgentRunEffect,
    AgentCheckpoint,
    rakka_agent::AgentRunSnapshot,
) {
    let stores = stores();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(1, tool_calling_turn("flaky-tool")),
    )
    .with_tool_failure("flaky-tool", "tool-unavailable", "the tool is down")
    .with_memory_promotion_executor(Arc::new(SessionMemoryPromotionExecutor::new(
        stores.session.clone(),
        stores.private.clone(),
    )));
    let mut fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    fx.policies = policies;
    fx.instantiate_agent().await;
    fx.create_task().await;
    crank(&fx).await;
    drive_turn(&fx).await;
    crank(&fx).await;
    let tool = dispatched_effect(&fx, AgentRunEffectKind::ToolCall).await;

    let promotion = promote_gated(&fx, "gated").await;
    let gate = the_gate_of(&fx, &promotion).await;
    let reply = answer_effect(&fx, &tool).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    assert_gate_invariant(&fx).await;
    let winding = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(winding.status, AgentRunStatus::Cancelling);
    assert!(
        matches!(
            &winding.terminal_reason,
            Some(rakka_agent::AgentRunTerminalReason::EffectFailed { effect_id, .. })
                if *effect_id == tool.effect_id
        ),
        "the tool's failure winds the run down: {:?}",
        winding.terminal_reason
    );
    assert_eq!(
        the_gate_of(&fx, &promotion).await,
        gate,
        "the wind-down leaves the gate open and as it was"
    );
    (fx, stores, promotion, gate, winding)
}

/// A promotion's gate stays answerable while a failure winds the run down,
/// and answering it moves nothing on the run. A grant leaves the status and
/// phase as they were; once the promotion's receipt lands the run ends under
/// the tool's failure. A denial, and an expiry, settle the promotion `Failed`
/// and end the run under that same reason.
#[tokio::test]
async fn a_promotion_gate_on_a_failing_run_stays_answerable() {
    for answer in Answer::ALL {
        let at = format!("{answer:?}");
        let (fx, stores, promotion, gate, winding) =
            failing_world_with_an_open_gate(policies_gating(Gate::Checkpoint, expiring_sla()))
                .await;
        let moving = status_and_phase(&fx).await;

        answer_gate(&fx, &gate, answer).await;
        match answer {
            Answer::Grant => {
                assert_eq!(status_and_phase(&fx).await, moving, "{at}: the grant");
                let granted = promotion_record(&fx, &promotion).await;
                assert_eq!(granted.status, AgentRunEffectStatus::Ready, "{at}");
                answer_promotion(&fx, &granted).await;
                assert_gate_invariant(&fx).await;
                let done = promotion_record(&fx, &promotion).await;
                assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{at}");
                assert_eq!(stores.private.len(&agent_scope()), 1, "{at}");
            }
            Answer::Deny | Answer::Expire => {
                let failed = promotion_record(&fx, &promotion).await;
                assert_eq!(failed.status, AgentRunEffectStatus::Failed, "{at}");
                assert!(stores.private.is_empty(&agent_scope()), "{at}");
            }
        }
        let ended = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(ended.status, AgentRunStatus::Failed, "{at}");
        assert_eq!(
            ended.terminal_reason, winding.terminal_reason,
            "{at}: the run ends under the reason its wind-down began with"
        );
        assert!(open_checkpoints(&fx).await.is_empty(), "{at}");
    }
}

/// The command that opens a gate records the gate's request, exactly once and
/// naming the checkpoint: `request-approval` for an approval,
/// `request-authorization` for an authorization. A cancel then records the
/// gate's close, as it does for any gate it closes.
#[tokio::test]
async fn a_commanded_gate_records_its_request_event() {
    use rakka_agent::AgentDecisionKind as Kind;
    for gate in [Gate::Checkpoint, Gate::Authorization] {
        let sink = CountingDecisionSink::new();
        let scope = run_scope();
        let (fx, _stores) = tool_promoting_world();
        let mut fx = fx
            .with_decision_events(sink.clone())
            .with_decision_event_vocabulary(rakka_agent::AgentDecisionEventVocabulary::Version2);
        fx.policies = policies_gating(gate, rakka_agent::AgentCheckpointSla::default());
        fx.instantiate_agent().await;
        fx.create_task().await;
        crank(&fx).await;
        let before = sink.events(&scope).len();

        let promoted = promote_gated(&fx, "gated").await;
        let opened = the_gate_of(&fx, &promoted).await;
        let events = sink.events(&scope);
        let recorded: Vec<_> = events[before..].iter().map(|event| event.kind).collect();
        assert_eq!(recorded, [gate.request()], "{gate:?}");
        let request = &events[before];
        assert_eq!(
            request
                .checkpoint
                .as_ref()
                .map(|checkpoint| (&checkpoint.checkpoint_id, checkpoint.kind)),
            Some((&opened.checkpoint_id, gate.kind())),
            "{gate:?}: the event names the gate"
        );
        let replay = apply_ok(&fx, promote_command(promotion(1, 1), "gated")).await;
        assert!(matches!(replay, AgentRunEntityReply::Duplicate { .. }));
        assert_gate_invariant(&fx).await;
        assert_eq!(
            sink.events(&scope).len(),
            before + 1,
            "{gate:?}: a replay records nothing"
        );

        let reply = apply_ok(&fx, cancel_command()).await;
        assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
        assert_gate_invariant(&fx).await;
        let naming: Vec<_> = sink
            .events(&scope)
            .into_iter()
            .filter(|event| {
                event
                    .checkpoint
                    .as_ref()
                    .is_some_and(|checkpoint| checkpoint.checkpoint_id == opened.checkpoint_id)
            })
            .collect();
        assert_eq!(
            naming.iter().map(|event| event.kind).collect::<Vec<_>>(),
            [gate.request(), Kind::ResolveCheckpoint],
            "{gate:?}"
        );
        let close = &naming[1];
        assert_eq!(
            close.outcome,
            Some(rakka_agent::AgentDecisionOutcome::Cancelled),
            "{gate:?}"
        );
        assert_eq!(close.reason_code.as_deref(), Some("cancelled"), "{gate:?}");
        assert_eq!(
            close.source,
            rakka_agent::AgentDecisionSource::DeterministicPolicy,
            "{gate:?}"
        );
        assert_decision_stream_whole(&sink, &scope);
    }
}

/// The tool-calling turn opens no gate for a promotion: the promotion owns
/// its gate. A promotion denied while the run awaited its model call stays
/// `Failed` with no gate when that turn then calls a tool.
#[tokio::test]
async fn a_tool_calling_turn_opens_no_gate_for_a_promotion() {
    let (fx, _stores) = live_world(
        Rest::AwaitingModel,
        gated_promotion_policies(rakka_agent::AgentCheckpointSla::default()),
    )
    .await;
    let promotion = promote_gated(&fx, "gated").await;
    let gate = the_gate_of(&fx, &promotion).await;
    answer_gate(&fx, &gate, Answer::Deny).await;
    assert_eq!(
        promotion_record(&fx, &promotion).await.status,
        AgentRunEffectStatus::Failed
    );
    assert!(open_checkpoints(&fx).await.is_empty());

    // The model answers with a tool call in the same turn.
    drive_turn(&fx).await;
    assert_gate_invariant(&fx).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (AgentLoopPhase::AwaitingTools, 1),
        "the turn called its tool"
    );
    assert!(
        open_checkpoints(&fx).await.is_empty(),
        "the tool-calling turn opened no gate on the denied promotion"
    );
    assert_eq!(
        promotion_record(&fx, &promotion).await.status,
        AgentRunEffectStatus::Failed
    );
}

/// A replayed gated `PromoteMemory` opens no second gate: the second delivery
/// answers `Duplicate` with the one gate still open, and a delivery after the
/// gate was denied answers `Duplicate` and opens none.
#[tokio::test]
async fn a_replayed_gated_promotion_opens_one_gate() {
    let (fx, _stores) = live_world(
        Rest::AwaitingModel,
        gated_promotion_policies(rakka_agent::AgentCheckpointSla::default()),
    )
    .await;
    let command = promote_command(promotion(1, 1), "gated");
    let first = apply_ok(&fx, command.clone()).await;
    assert!(matches!(first, AgentRunEntityReply::Applied { .. }));
    assert_gate_invariant(&fx).await;
    let promotion = promotion_effect(&fx).await;
    let gate = the_gate_of(&fx, &promotion).await;

    let replay = apply_ok(&fx, command.clone()).await;
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "{replay:?}"
    );
    assert_gate_invariant(&fx).await;
    assert_eq!(open_checkpoints(&fx).await, std::slice::from_ref(&gate));
    assert_eq!(promotion_record(&fx, &promotion).await, promotion);

    answer_gate(&fx, &gate, Answer::Deny).await;
    let again = apply_ok(&fx, command).await;
    assert!(
        matches!(again, AgentRunEntityReply::Duplicate { .. }),
        "{again:?}"
    );
    assert_gate_invariant(&fx).await;
    assert!(open_checkpoints(&fx).await.is_empty(), "no gate reopens");
    let held = promotion_record(&fx, &promotion).await;
    assert_eq!(held.status, AgentRunEffectStatus::Failed);
    assert_eq!(held.generation, promotion.generation);
}

/// A granted gated promotion whose outcome came back unknown on a live run,
/// confirmed not executed, is re-sent under a new generation, and that
/// generation's gate opens: the old grant binds the old generation only. The
/// re-sent gate is outside the turn like the first, so the run's status
/// reports no approval or authorization wait for it.
#[tokio::test]
async fn a_re_sent_gated_promotion_on_a_live_run_opens_the_new_generations_gate() {
    for gate in Gate::ALL {
        let (fx, _stores) = live_world(
            Rest::AwaitingModel,
            policies_gating(gate, rakka_agent::AgentCheckpointSla::default()),
        )
        .await;
        let resting = status_and_phase(&fx).await;
        let promotion = promote_gated(&fx, "gated").await;
        let first = the_gate_of(&fx, &promotion).await;
        answer_gate(&fx, &first, Answer::Grant).await;

        let parked = resolve_promotion(
            &fx,
            AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            ),
        )
        .await;
        assert_gate_invariant(&fx).await;
        assert_eq!(
            parked.status,
            AgentRunEffectStatus::Indeterminate,
            "{gate:?}"
        );

        let reply = apply_ok(
            &fx,
            resolve_indeterminate(
                "not-executed",
                &parked,
                parked.generation,
                rakka_agent::AgentEffectResolution::ConfirmedNotExecuted,
            ),
        )
        .await;
        assert!(
            matches!(reply, AgentRunEntityReply::Applied { .. }),
            "{gate:?}: {reply:?}"
        );
        assert_gate_invariant(&fx).await;
        let resent = promotion_record(&fx, &parked).await;
        assert_eq!(resent.generation, parked.generation.next(), "{gate:?}");
        assert_eq!(resent.status, AgentRunEffectStatus::Pending, "{gate:?}");
        let second = the_gate_of(&fx, &resent).await;
        assert_eq!(second.kind, gate.kind(), "{gate:?}");
        assert_eq!(
            second.bound_effect.generation, resent.generation,
            "{gate:?}"
        );

        let status = fx.run_snapshot().await.expect("the run exists").status;
        assert!(
            !matches!(
                status,
                AgentRunStatus::WaitingForApproval | AgentRunStatus::WaitingForAuthorization
            ),
            "{gate:?}: the re-sent gate is not the turn's wait: {status:?}"
        );
        assert_eq!(status_and_phase(&fx).await, resting, "{gate:?}");
    }
}

/// A gated promotion commanded on a run that already holds the most open
/// checkpoints it may is refused `effect-pending-overflow`, whole: no effect is
/// recorded, no attempt is reserved, no checkpoint opens, and a retry is
/// refused the same way rather than answered `Duplicate`. The eight held
/// checkpoints are reconciliation checkpoints on parked promotions, which are
/// not outstanding, so the effect bound is not reached and the refusal is the
/// checkpoint bound's.
#[tokio::test]
async fn a_gated_promotion_at_the_checkpoint_bound_is_refused_whole() {
    let (mut fx, _stores) = live_world(
        Rest::AwaitingModel,
        rakka_agent::AgentEffectPolicies::default(),
    )
    .await;
    for parked in 0..rakka_agent::AGENT_RUN_MAX_PENDING_EFFECTS {
        let reply = apply_ok(
            &fx,
            promote_command(promotion(1, 1), &format!("parked-{parked}")),
        )
        .await;
        assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
        assert_gate_invariant(&fx).await;
        let held = resolve_promotion(
            &fx,
            AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            ),
        )
        .await;
        assert_eq!(held.status, AgentRunEffectStatus::Indeterminate);
        assert_gate_invariant(&fx).await;
    }
    let held = open_checkpoints(&fx).await;
    assert_eq!(held.len(), rakka_agent::AGENT_RUN_MAX_PENDING_EFFECTS);
    assert!(held.iter().all(
        |checkpoint| checkpoint.kind == AgentCheckpointKind::IndeterminateEffectReconciliation
    ));
    let (_, _, _, outstanding) = resting_at(&fx).await;
    assert!(
        outstanding < rakka_agent::AGENT_RUN_MAX_PENDING_EFFECTS,
        "the effect bound is not reached: {outstanding} outstanding"
    );

    /// What a refused command must leave as it was: the effects, the
    /// reserved attempts, the checkpoints and the status.
    async fn picture(
        fx: &Fixture,
    ) -> (
        Vec<AgentRunEffect>,
        u64,
        Vec<AgentCheckpoint>,
        Option<AgentRunStatus>,
    ) {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        let state = run.state().expect("state");
        let loop_state = state.loop_state().expect("the loop is started");
        (
            loop_state.effects().to_vec(),
            loop_state.budget().reserved_attempts(),
            loop_state.open_checkpoints().to_vec(),
            state.status(),
        )
    }
    let before = picture(&fx).await;
    fx.policies = gated_promotion_policies(rakka_agent::AgentCheckpointSla::default());
    let command = promote_command(promotion(1, 1), "gated");
    for attempt in ["the command", "a retry"] {
        let refusal = apply(&fx, command.clone())
            .await
            .expect_err("the checkpoint bound refuses the gated promotion");
        assert_eq!(refusal.code(), "effect-pending-overflow", "{attempt}");
        assert_gate_invariant(&fx).await;
        let after = picture(&fx).await;
        assert_eq!(after.0.len(), before.0.len(), "{attempt}: the effect count");
        assert_eq!(after.1, before.1, "{attempt}: the reserved attempts");
        assert_eq!(after, before, "{attempt}: the record is unmoved");
    }
}

// ---------------------------------------------------------------------------
// A cancel settles the ungranted gated promotion with its gate (issue #126,
// section 5.2.4).
// ---------------------------------------------------------------------------

/// The attempts the run's budget holds reserved.
async fn reserved_attempts(fx: &Fixture) -> u64 {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .budget()
        .reserved_attempts()
}

/// Whether the run holds a grant for `effect`'s current generation.
async fn holds_grant(fx: &Fixture, effect: &AgentRunEffect) -> bool {
    let held = promotion_record(fx, effect).await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .grant_for(&held)
        .is_some()
}

/// The status of the record the run holds for `gate`, `None` once the run
/// dropped it.
async fn gate_status(
    fx: &Fixture,
    gate: &AgentCheckpoint,
) -> Option<rakka_agent::AgentCheckpointStatus> {
    open_checkpoints(fx)
        .await
        .into_iter()
        .find(|held| held.checkpoint_id == gate.checkpoint_id)
        .map(|held| held.status)
}

/// A cancel with a promotion's gate open settles the promotion `Cancelled`
/// with its gate, in the cancel's own transition: the gate is cancelled, the
/// promotion never dispatched, and the attempts it reserved are released, so
/// the run's reservation is what it was before the command. Once the turn's
/// own in-flight work settles, the run ends `Cancelled`. Without the rule the
/// promotion stays `Pending` with no gate and the run stays `Cancelling`,
/// with nothing left that can move it.
#[tokio::test]
async fn a_cancel_settles_a_gated_promotion_with_its_gate() {
    for rest in [Rest::AwaitingModel, Rest::AwaitingTools] {
        for gate in Gate::ALL {
            let at = format!("{rest:?}, {gate:?}");
            let (fx, stores) = live_world(
                rest,
                policies_gating(gate, rakka_agent::AgentCheckpointSla::default()),
            )
            .await;
            let reserved = reserved_attempts(&fx).await;
            let promotion = promote_gated(&fx, "gated").await;
            let opened = the_gate_of(&fx, &promotion).await;
            assert!(
                reserved_attempts(&fx).await > reserved,
                "{at}: the command reserved the promotion's attempts"
            );

            let reply = apply_ok(&fx, cancel_command()).await;
            assert!(
                matches!(reply, AgentRunEntityReply::Applied { .. }),
                "{at}: {reply:?}"
            );
            assert_gate_invariant(&fx).await;
            assert_eq!(
                gate_status(&fx, &opened).await,
                Some(rakka_agent::AgentCheckpointStatus::Cancelled),
                "{at}: the gate is cancelled"
            );
            let cancelled = promotion_record(&fx, &promotion).await;
            assert_eq!(cancelled.status, AgentRunEffectStatus::Cancelled, "{at}");
            assert_eq!(cancelled.attempts, 0, "{at}: it never dispatched");
            assert_eq!(cancelled.generation, promotion.generation, "{at}");
            assert_eq!(
                reserved_attempts(&fx).await,
                reserved,
                "{at}: the promotion's reservation is released"
            );
            let winding = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(
                winding.status,
                AgentRunStatus::Cancelling,
                "{at}: the turn's in-flight work still holds the run"
            );

            drive_turn(&fx).await;
            assert_gate_invariant(&fx).await;
            let ended = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(ended.status, AgentRunStatus::Cancelled, "{at}");
            assert_eq!(ended.terminal_reason, winding.terminal_reason, "{at}");
            assert!(
                stores.private.is_empty(&agent_scope()),
                "{at}: nothing was promoted"
            );
        }
    }
}

/// A cancel leaves a granted promotion alone: holding its grant it is
/// dispatchable, and it rides the outbox as an exempt effect does on a
/// winding-down run. A grant is flushed `Ready` by its own command's settle
/// pass, so the promotion is still `Pending` at a later cancel only when the
/// grant's owner died between the commit and that flush: the run store's
/// crash builds exactly that window, and kills the cancel's owner the same
/// way, so the record shows what the cancel's own transition left. The next
/// settle pass dispatches the promotion, its receipt lands, and the run ends
/// `Cancelled`.
#[tokio::test]
async fn a_cancel_leaves_a_granted_promotion_to_dispatch() {
    use rakka_agent::testkit::CrashPoint;

    let (fx, stores) = live_world(
        Rest::AwaitingModel,
        policies_gating(Gate::Checkpoint, rakka_agent::AgentCheckpointSla::default()),
    )
    .await;
    let promotion = promote_gated(&fx, "gated").await;
    let gate = the_gate_of(&fx, &promotion).await;

    // The grant commits, and its owner dies before the settle pass flushes it.
    fx.runs.crash_at(1, CrashPoint::AfterWrite);
    apply(&fx, grant_command(&gate.checkpoint_id, "grant"))
        .await
        .expect_err("the grant's owner dies once the grant commits");
    fx.runs.assert_crash_fired(1, CrashPoint::AfterWrite);
    fx.runs.survive();
    assert_gate_invariant(&fx).await;
    let granted = promotion_record(&fx, &promotion).await;
    assert_eq!(granted.status, AgentRunEffectStatus::Pending, "unflushed");
    assert!(holds_grant(&fx, &promotion).await, "the grant is held");
    assert_eq!(
        gate_status(&fx, &gate).await,
        None,
        "the grant resolved the gate"
    );

    // The cancel commits, and its owner dies the same way.
    fx.runs.crash_at(1, CrashPoint::AfterWrite);
    apply(&fx, cancel_command())
        .await
        .expect_err("the cancel's owner dies once the cancel commits");
    fx.runs.assert_crash_fired(1, CrashPoint::AfterWrite);
    fx.runs.survive();
    assert_gate_invariant(&fx).await;
    let after = promotion_record(&fx, &promotion).await;
    assert_eq!(
        after.status,
        AgentRunEffectStatus::Pending,
        "the cancel left the granted promotion to dispatch"
    );
    assert!(
        holds_grant(&fx, &promotion).await,
        "the grant is still held"
    );
    let winding = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(winding.status, AgentRunStatus::Cancelling);

    crank(&fx).await;
    assert_gate_invariant(&fx).await;
    let dispatched = promotion_record(&fx, &promotion).await;
    assert_eq!(
        dispatched.status,
        AgentRunEffectStatus::Ready,
        "it dispatches"
    );
    let reply = answer_promotion(&fx, &dispatched).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_gate_invariant(&fx).await;
    let done = promotion_record(&fx, &promotion).await;
    assert_eq!(
        done.status,
        AgentRunEffectStatus::Succeeded,
        "its receipt lands"
    );
    assert_eq!(stores.private.len(&agent_scope()), 1);

    drive_turn(&fx).await;
    assert_gate_invariant(&fx).await;
    let ended = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(ended.status, AgentRunStatus::Cancelled);
    assert_eq!(ended.terminal_reason, winding.terminal_reason);
}

/// A record a binary before the command opened the gate left: a gated
/// promotion `Pending`, ungranted, with no gate at all. The rule reads the
/// effect, not the checkpoint, so a cancel settles it `Cancelled` all the
/// same and releases its attempts, and the run ends `Cancelled`.
#[tokio::test]
async fn a_cancel_settles_a_gated_promotion_that_has_no_gate() {
    let (fx, stores) = live_world(
        Rest::AwaitingModel,
        policies_gating(Gate::Checkpoint, rakka_agent::AgentCheckpointSla::default()),
    )
    .await;
    let reserved = reserved_attempts(&fx).await;
    let promotion = promote_gated(&fx, "gated").await;
    unbind_checkpoints(&fx).await;
    assert!(open_checkpoints(&fx).await.is_empty(), "no gate is held");
    let held = promotion_record(&fx, &promotion).await;
    assert_eq!(held.status, AgentRunEffectStatus::Pending);
    assert!(held.checkpoint_required, "the promotion is gated");
    assert!(!holds_grant(&fx, &promotion).await, "and ungranted");

    let reply = apply_ok(&fx, cancel_command()).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_gate_invariant(&fx).await;
    let cancelled = promotion_record(&fx, &promotion).await;
    assert_eq!(cancelled.status, AgentRunEffectStatus::Cancelled);
    assert_eq!(cancelled.attempts, 0);
    assert_eq!(reserved_attempts(&fx).await, reserved, "the reservation");
    assert!(open_checkpoints(&fx).await.is_empty(), "no gate was opened");

    drive_turn(&fx).await;
    assert_gate_invariant(&fx).await;
    let ended = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(ended.status, AgentRunStatus::Cancelled);
    assert!(stores.private.is_empty(&agent_scope()));
}

/// The rule is scoped to the effects outside the turn. A compensation is
/// exempt from the wind-down fence as a promotion is, and one the deployment
/// gates waits `Pending` with no gate, since nothing opens a compensation's
/// gate (issue #128 owns that). A cancel leaves such a compensation exactly
/// as it was: what a cancel does to it is issue #128's to decide, not this
/// rule's.
#[tokio::test]
async fn a_cancel_leaves_a_gated_compensation_as_it_was() {
    let policies = rakka_agent::AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new("lookup").expect("tool id"),
            rakka_agent::AgentEffectSpec::non_idempotent(),
        )
        .expect("the tool spec is valid")
        .with_compensation_spec(
            rakka_agent::AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the compensation spec is valid");
    let (fx, _stores) = live_world(Rest::AwaitingTools, policies).await;
    let tool = dispatched_effect(&fx, AgentRunEffectKind::ToolCall).await;
    let reply = apply_ok(
        &fx,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: tool
                .result_operation_id(&run_scope())
                .expect("result operation id"),
            effect_id: tool.effect_id.clone(),
            generation: tool.generation,
            attempt: tool.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(AgentRunEffectOutcome::indeterminate(
                "connection-lost",
                "the worker died mid-invocation",
            )),
        },
    )
    .await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    assert_gate_invariant(&fx).await;
    let reconcile = the_open_checkpoint(&fx).await;
    assert_eq!(
        reconcile.kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    let reply = apply_ok(
        &fx,
        resolve_checkpoint_command(
            &reconcile.checkpoint_id,
            "compensate",
            reconciliation(rakka_agent::AgentReconciliationDecision::Compensate {
                compensation: rakka_agent::AgentCompensationRef::new("refund-charge")
                    .expect("the compensation ref is valid"),
            }),
        ),
    )
    .await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_gate_invariant(&fx).await;

    /// The run's one compensation effect.
    async fn compensation(fx: &Fixture) -> AgentRunEffect {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        let state = run.state().expect("state");
        let held: Vec<AgentRunEffect> = state
            .loop_state()
            .expect("the loop is started")
            .effects()
            .iter()
            .filter(|effect| effect.kind() == AgentRunEffectKind::CompensationCall)
            .cloned()
            .collect();
        assert_eq!(held.len(), 1, "one compensation is scheduled");
        held.into_iter().next().expect("one compensation")
    }
    let scheduled = compensation(&fx).await;
    assert_eq!(scheduled.status, AgentRunEffectStatus::Pending);
    assert!(scheduled.checkpoint_required, "the compensation is gated");
    assert!(
        !open_checkpoints(&fx)
            .await
            .iter()
            .any(|checkpoint| checkpoint.status.is_waiting()),
        "and nothing opened its gate"
    );
    let reserved = reserved_attempts(&fx).await;

    let reply = apply_ok(&fx, cancel_command()).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_gate_invariant(&fx).await;
    assert_eq!(
        compensation(&fx).await,
        scheduled,
        "the cancel left the compensation as it was"
    );
    assert_eq!(reserved_attempts(&fx).await, reserved, "the reservation");
}

/// A turn that calls two tools at once: `flaky-tool`, whose call fails, and
/// `lookup`, which answers.
fn two_tool_turn() -> AgentModelTurn {
    let call = |id: &str, tool: &str| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(id).expect("call id"),
            AgentToolId::new(tool).expect("tool id"),
            serde_json::json!({ "query": "ticket" }),
        )
        .expect("the tool call is bounded")
    };
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up two ways.")
        .with_tool_call(call("call-1", "flaky-tool"))
        .with_tool_call(call("call-2", "lookup"))
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
            ..Default::default()
        })
}

/// How a run comes to be winding down before a promotion is commanded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Winding {
    /// An operator's `Cancel`, with turn one's model call in flight.
    Cancelled,
    /// A failed tool, with the same turn's other tool in flight.
    FailedTool,
}

impl Winding {
    const ALL: [Self; 2] = [Self::Cancelled, Self::FailedTool];

    /// The status the run ends with once nothing holds it.
    fn ending(self) -> AgentRunStatus {
        match self {
            Self::Cancelled => AgentRunStatus::Cancelled,
            Self::FailedTool => AgentRunStatus::Failed,
        }
    }
}

/// A live run under `policies`, wound down by `winding` with other work still
/// in flight that holds it `Cancelling`. Returns the reason the wind-down
/// began with.
async fn winding_world(
    winding: Winding,
    policies: rakka_agent::AgentEffectPolicies,
) -> (Fixture, Stores, Option<rakka_agent::AgentRunTerminalReason>) {
    let (fx, stores) = match winding {
        Winding::Cancelled => {
            // `live_world(Rest::AwaitingModel, policies)`, flattened so this
            // frame does not nest a whole `live_world` frame.
            let (mut fx, stores) = tool_promoting_world();
            fx.policies = policies;
            Box::pin(fx.instantiate_agent()).await;
            Box::pin(fx.create_task()).await;
            crank(&fx).await;
            let (phase, _, turn, _) = resting_at(&fx).await;
            assert_eq!((phase, turn), (AgentLoopPhase::AwaitingModel, 1));
            let reply = apply_ok(&fx, cancel_command()).await;
            assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
            assert_gate_invariant(&fx).await;
            (fx, stores)
        }
        Winding::FailedTool => {
            let stores = stores();
            let dispatcher = ScriptedDispatcher::with_adapter(
                DeterministicModelAdapter::new().with_turn_for(1, two_tool_turn()),
            )
            .with_tool_failure("flaky-tool", "tool-unavailable", "the tool is down")
            .with_tool_result(
                "lookup",
                AgentTaskContent::inline(serde_json::json!({ "found": true }))
                    .expect("the tool result is inline-bounded"),
            )
            .with_memory_promotion_executor(Arc::new(
                SessionMemoryPromotionExecutor::new(stores.session.clone(), stores.private.clone()),
            ));
            let mut fx = Fixture::new(dispatcher).with_memory(
                AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
                    .with_private_store(stores.private.clone()),
            );
            fx.policies = policies;
            Box::pin(fx.instantiate_agent()).await;
            Box::pin(fx.create_task()).await;
            crank(&fx).await;
            drive_turn(&fx).await;
            crank(&fx).await;
            let tools = dispatched_effects_of(&fx, AgentRunEffectKind::ToolCall).await;
            assert_eq!(tools.len(), 2, "both tools are in flight");
            let flaky = tools
                .iter()
                .find(|effect| {
                    effect
                        .request
                        .tool_call()
                        .is_some_and(|call| call.tool.as_str() == "flaky-tool")
                })
                .expect("the flaky tool is in flight");
            let reply = answer_effect(&fx, flaky).await;
            assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
            assert_gate_invariant(&fx).await;
            (fx, stores)
        }
    };
    let (_, status, _, outstanding) = resting_at(&fx).await;
    assert_eq!(status, AgentRunStatus::Cancelling, "{winding:?}");
    assert_eq!(outstanding, 1, "{winding:?}: one effect holds the run");
    let reason = fx
        .run_snapshot()
        .await
        .expect("the run exists")
        .terminal_reason;
    assert!(
        reason.is_some(),
        "{winding:?}: the wind-down has its reason"
    );
    (fx, stores, reason)
}

/// How the test closes a gate opened on a winding-down run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Close {
    /// A person grants it.
    Grant,
    /// A further `Cancel` arrives.
    Cancel,
}

/// A gated promotion commanded on a run already winding down, after a cancel
/// or after a failed tool, opens its gate and moves neither the status nor
/// the phase. Once the run's other work settles, the open gate alone holds the
/// run `Cancelling` (section 5.2.4's consequence). A grant then moves nothing
/// either: the promotion dispatches, and its receipt is what ends the run. A
/// further `Cancel` instead settles the promotion `Cancelled` with its gate,
/// and that ends the run. Either way it ends under the reason its wind-down
/// began with.
#[tokio::test]
async fn a_gated_promotion_commanded_while_winding_down_holds_the_run_until_answered() {
    for winding in Winding::ALL {
        for close in [Close::Grant, Close::Cancel] {
            let at = format!("{winding:?}, {close:?}");
            let (fx, stores, reason) = winding_world(
                winding,
                policies_gating(Gate::Checkpoint, rakka_agent::AgentCheckpointSla::default()),
            )
            .await;
            let winding_down = status_and_phase(&fx).await;

            let promotion = promote_gated(&fx, "gated").await;
            assert_eq!(
                status_and_phase(&fx).await,
                winding_down,
                "{at}: the command"
            );
            assert_eq!(promotion.status, AgentRunEffectStatus::Pending, "{at}");
            let gate = the_gate_of(&fx, &promotion).await;

            // The run's other work settles, and the open gate alone holds it.
            drive_turn(&fx).await;
            assert_gate_invariant(&fx).await;
            let (_, _, _, outstanding) = resting_at(&fx).await;
            assert_eq!(outstanding, 1, "{at}: only the promotion is outstanding");
            let held = status_and_phase(&fx).await;
            assert_eq!(
                held.0,
                AgentRunStatus::Cancelling,
                "{at}: the gate holds the run"
            );
            assert_eq!(the_gate_of(&fx, &promotion).await, gate, "{at}");

            match close {
                Close::Grant => {
                    answer_gate(&fx, &gate, Answer::Grant).await;
                    assert_eq!(status_and_phase(&fx).await, held, "{at}: the grant");
                    let granted = promotion_record(&fx, &promotion).await;
                    assert_eq!(granted.status, AgentRunEffectStatus::Ready, "{at}");
                    let reply = answer_promotion(&fx, &granted).await;
                    assert!(
                        matches!(reply, AgentRunEntityReply::Applied { .. }),
                        "{at}: {reply:?}"
                    );
                    assert_gate_invariant(&fx).await;
                    let done = promotion_record(&fx, &promotion).await;
                    assert_eq!(done.status, AgentRunEffectStatus::Succeeded, "{at}");
                    assert_eq!(stores.private.len(&agent_scope()), 1, "{at}");
                }
                Close::Cancel => {
                    let reply = apply_ok(&fx, cancel_command_under("again")).await;
                    assert!(
                        matches!(reply, AgentRunEntityReply::Applied { .. }),
                        "{at}: {reply:?}"
                    );
                    assert_gate_invariant(&fx).await;
                    let cancelled = promotion_record(&fx, &promotion).await;
                    assert_eq!(cancelled.status, AgentRunEffectStatus::Cancelled, "{at}");
                    assert_eq!(cancelled.attempts, 0, "{at}");
                    assert_eq!(
                        gate_status(&fx, &gate).await,
                        Some(rakka_agent::AgentCheckpointStatus::Cancelled),
                        "{at}: the gate is cancelled with it"
                    );
                    assert!(stores.private.is_empty(&agent_scope()), "{at}");
                }
            }
            let ended = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(ended.status, winding.ending(), "{at}");
            assert_eq!(
                ended.terminal_reason, reason,
                "{at}: the run ends under the reason its wind-down began with"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// An ended run's `next_wake` reads only what its timer sweep acts on
// (issue #127).
// ---------------------------------------------------------------------------

/// A world whose first turn calls a checkpoint-required tool beside one that
/// fails, under `sla`, with the promotion spec gated too. The failure fences
/// the gated call, so the run ends `Failed` still holding that call's
/// approval, waiting: a checkpoint an ended run's timer sweep leaves alone.
async fn failed_world_holding_a_tool_approval(
    sla: rakka_agent::AgentCheckpointSla,
) -> (Fixture, Stores) {
    let stores = stores();
    let call = |id: &str, tool: &str| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(id).expect("call id"),
            AgentToolId::new(tool).expect("tool id"),
            serde_json::json!({ "query": "ticket" }),
        )
        .expect("the tool call is bounded")
    };
    let turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me charge the card and look that up.")
        .with_tool_call(call("call-1", "charge-card"))
        .with_tool_call(call("call-2", "flaky-tool"))
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
            ..Default::default()
        });
    let dispatcher =
        ScriptedDispatcher::with_adapter(DeterministicModelAdapter::new().with_turn_for(1, turn))
            .with_tool_failure("flaky-tool", "tool-unavailable", "the tool is down")
            .with_memory_promotion_executor(Arc::new(SessionMemoryPromotionExecutor::new(
                stores.session.clone(),
                stores.private.clone(),
            )));
    let policies = gated_promotion_policies(sla)
        .with_tool_spec(
            AgentToolId::new("charge-card").expect("tool id"),
            rakka_agent::AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the gated tool spec is valid");
    let fx = Fixture::with_sink(
        dispatcher,
        rakka_agent::InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(std::sync::atomic::AtomicU64::new(1)),
    )
    .with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    Box::pin(fx.instantiate_agent()).await;
    Box::pin(fx.create_task()).await;
    Box::pin(fx.pump()).await.expect("the loop runs to its end");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::Failed,
        "the failed call ended the run"
    );
    (fx, stores)
}

/// The run's operational snapshot, observed at the fixture's clock.
async fn operational_snapshot(fx: &Fixture) -> rakka_agent::AgentOperationalSnapshot {
    Box::pin(agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    ))
    .await
    .expect("the point query answers")
    .expect("the run exists")
}

/// Applies the run's checkpoint timer command at exactly `at`, under `key`.
async fn fire_checkpoint_timers_at(
    fx: &Fixture,
    at: AgentTimestampMillis,
    key: &str,
) -> AgentRunEntityReply {
    fx.clock
        .store(at.as_millis(), std::sync::atomic::Ordering::SeqCst);
    apply_ok(
        fx,
        AgentRunEntityCommand::FireCheckpointTimers {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                key,
            )
            .expect("the timer key derives"),
        },
    )
    .await
}

/// An ended run's `next_wake` reads only the checkpoints its timer sweep acts
/// on. The run ended `Failed` holding a tool call's approval, which the sweep
/// leaves waiting on an ended run, so that approval's deadline is never
/// reported, though it is the earliest the run holds. A gated promotion
/// commanded after the end opens a gate the sweep does act on, and `next_wake`
/// is that gate's deadline. The timer at the approval's deadline changes
/// nothing; the timer at each reported deadline changes the record; and once
/// nothing is reported, a timer at any time changes no checkpoint.
///
/// The approval is reached through commands, not by editing the record: the
/// failed sibling call fences the gated call, and nothing closes its approval.
#[tokio::test]
async fn an_ended_runs_next_wake_reads_only_what_its_sweep_acts_on() {
    let sla = rakka_agent::AgentCheckpointSla {
        due_after_ms: Some(1_000),
        expire_after_ms: Some(5_000),
        escalation_target: Some("ops-oncall".to_string()),
    };
    let (fx, _stores) = Box::pin(failed_world_holding_a_tool_approval(sla)).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let approval = the_open_checkpoint(&fx).await;
    assert_eq!(approval.kind, AgentCheckpointKind::Approval);
    assert_eq!(approval.status, rakka_agent::AgentCheckpointStatus::Open);
    let gated_call = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        run.state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .effects()
            .iter()
            .find(|effect| effect.effect_id == approval.bound_effect.effect_id)
            .cloned()
            .expect("the gated call is held")
    };
    assert_eq!(
        gated_call.kind(),
        AgentRunEffectKind::ToolCall,
        "the approval binds an effect that is not outside the turn"
    );
    let skipped = approval
        .next_timer_deadline()
        .expect("on its own, the approval would wake at its due time");
    assert_eq!(Some(skipped), approval.due_at);
    assert_eq!(
        operational_snapshot(&fx).await.next_wake,
        None,
        "the sweep skips the approval, so it wakes nothing"
    );

    let promotion = promote_gated(&fx, "gated").await;
    let gate = the_gate_of(&fx, &promotion).await;
    let gate_due = gate.due_at.expect("the SLA stamped the gate's due time");
    let gate_expires = gate.expires_at.expect("the SLA stamped the gate's expiry");
    assert!(
        skipped < gate_due,
        "the skipped approval's deadline is earlier"
    );
    let snapshot = operational_snapshot(&fx).await;
    assert_eq!(snapshot.open_checkpoints.len(), 2);
    assert_eq!(
        snapshot.next_wake,
        Some(gate_due),
        "the gate's deadline is reported"
    );
    let stored = open_checkpoints(&fx).await;
    for view in &snapshot.open_checkpoints {
        let held = stored
            .iter()
            .find(|held| held.checkpoint_id == view.checkpoint_id)
            .expect("each view names a stored checkpoint");
        assert_eq!(
            (view.due_at, view.expires_at),
            (held.due_at, held.expires_at),
            "the view carries the record's deadlines"
        );
    }

    // The timer at the skipped approval's deadline changes nothing.
    let held = open_checkpoints(&fx).await;
    let reply = fire_checkpoint_timers_at(&fx, skipped, "timer-skipped").await;
    assert!(
        matches!(reply, AgentRunEntityReply::Unchanged { .. }),
        "{reply:?}"
    );
    assert_eq!(open_checkpoints(&fx).await, held);
    assert_terminal_unmoved(&fx, &before, "after the timer at the skipped deadline").await;

    // The timer at each reported deadline changes the record, and only the
    // gate moves.
    let reply = fire_checkpoint_timers_at(&fx, gate_due, "timer-gate-due").await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_eq!(
        the_gate_of(&fx, &promotion).await.status,
        rakka_agent::AgentCheckpointStatus::Escalated
    );
    assert_eq!(
        operational_snapshot(&fx).await.next_wake,
        Some(gate_expires),
        "the escalated gate wakes at its expiry"
    );
    let reply = fire_checkpoint_timers_at(&fx, gate_expires, "timer-gate-expires").await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    assert_eq!(
        promotion_record(&fx, &promotion).await.status,
        AgentRunEffectStatus::Failed,
        "the gate expired and failed its promotion"
    );
    assert!(
        approval
            .expires_at
            .is_some_and(|expiry| expiry < gate_expires),
        "the approval's own expiry has passed too"
    );
    assert_eq!(
        open_checkpoints(&fx).await,
        vec![approval.clone()],
        "the approval is exactly as the run ended"
    );
    assert_eq!(operational_snapshot(&fx).await.next_wake, None);

    // With nothing reported, the timer at any time changes no checkpoint.
    let far = AgentTimestampMillis::new(1_000_000_000);
    let reply = fire_checkpoint_timers_at(&fx, far, "timer-far").await;
    assert!(
        matches!(reply, AgentRunEntityReply::Unchanged { .. }),
        "{reply:?}"
    );
    assert_eq!(open_checkpoints(&fx).await, vec![approval]);
    assert_terminal_unmoved(&fx, &before, "after the timers").await;
}
