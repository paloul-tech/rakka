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
    promotion_operation_id, AgentLoopPhase, AgentMemoryConsolidationTarget,
    AgentMemoryPromotionRequest, AgentModelTurn, AgentModelUsage, AgentOperationId,
    AgentOperationKind, AgentPrivateMemoryId, AgentPrivateMemoryKind, AgentPrivateMemoryStore,
    AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectStatus,
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunMemory, AgentRunStatus, AgentScope,
    AgentTaskContent, AgentTaskEntityStore, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, AgentToolResult, InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore,
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

/// Answers every dispatched effect except an outstanding promotion, so a test
/// can crank a tool-calling turn to rest while keeping the run live.
async fn drive_turn(fx: &Fixture) {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
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
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    if ending == Ending::Cancelled {
        crank(&fx).await;
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
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
        )
        .await
        .expect("the cancel applies");
    }
    fx.pump().await.expect("the loop runs to its end");
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
        AgentRunEffectOutcome::Exhausted {
            code: "memory-promotion-source-missing".to_string(),
            message: "the session rows were purged".to_string(),
        },
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

/// An ambiguous outcome delivered to a run that has ended is recorded on the
/// effect and opens no reconciliation checkpoint: there is no run to park,
/// and the terminal status does not move.
#[tokio::test]
async fn an_indeterminate_post_terminal_outcome_opens_no_checkpoint() {
    let (fx, _stores) = ended_world(Ending::Cancelled).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let reply = apply_ok(&fx, promote_command(promotion(1, 1), "ambiguous")).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let parked = resolve_promotion(
        &fx,
        AgentRunEffectOutcome::Indeterminate {
            code: "dispatcher-lost-after-started".to_string(),
            message: "the recovery retry was refused".to_string(),
        },
    )
    .await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    let mut entity = fx.run();
    entity.recover(fx.now()).await.expect("recover");
    let state = entity.state().expect("state");
    assert!(
        state
            .loop_state()
            .expect("the loop is started")
            .open_checkpoints()
            .is_empty(),
        "no reconciliation checkpoint opens on a run that has ended"
    );
    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        after.status,
        AgentRunStatus::Cancelled,
        "the status did not move"
    );
    assert_eq!(after.status, before.status);
    assert_eq!(after.terminal_at, before.terminal_at);
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
            match run
                .apply(
                    promote_command(promotion(1, 1), "sweep"),
                    &fx.router,
                    fx.now(),
                )
                .await
            {
                Ok(AgentRunEntityReply::Applied { .. })
                | Ok(AgentRunEntityReply::Duplicate { .. }) => {}
                Ok(_) | Err(_) => continue,
            }
            if run.settle_side_effects(&fx.router, fx.now()).await.is_err() {
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
            match run.apply(result, &fx.router, fx.now()).await {
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
    let reply = run
        .accept(envelope, &fx.router, now)
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
