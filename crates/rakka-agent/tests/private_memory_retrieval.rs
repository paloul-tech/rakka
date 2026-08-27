//! Private-memory retrieval through the run entity's snapshot path.
//!
//! Specification: sections 13.3, 13.5, 13.6; scenarios 17 and 18 of section
//! 18, extended to the slice 2.2 retrieval flow. A run wired with a retrieval
//! bundle must, as it cranks its durable loop:
//!
//! - fill each snapshot's private selections through the slice 1.11 assembly
//!   path only — content embedded, budget accounted, retrieval recorded;
//! - select nothing from another agent's or another tenant's memory
//!   (scenario 18); and
//! - keep a retried model input byte-identical under *every* kind of drift —
//!   a concurrent memory update, a tombstone, even a retriever upgrade —
//!   because the persisted snapshot, not the index, is what a retry reads
//!   (scenario 17).
//!
//! A retriever outage degrades the turn to an empty selection instead of
//! stalling the run (specification 13.1); the assembly-level halves of these
//! proofs live in the `retrieval` unit tests, and the store-level scenario 18
//! half in the `memory` unit tests.

use std::sync::Arc;

use rakka_agent::testkit::{
    DeterministicModelAdapter, ScriptedDispatcher, ScriptedPrivateMemoryRetriever,
};
use rakka_agent::{
    AgentContextSnapshotRef, AgentGuardrailChain, AgentId, AgentMemoryRetrieval, AgentModelTurn,
    AgentPrivateMemory, AgentPrivateMemoryId, AgentPrivateMemoryKind, AgentPrivateMemoryStore,
    AgentRevisionNumber, AgentRunMemory, AgentRunStatus, AgentScope, AgentTaskContent,
    ContextSnapshotStore, InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore,
    InMemoryPrivateMemoryRetriever, InMemorySessionMemoryStore, MemoryClassification,
    MemoryOperationId, MemoryTombstoneReason, PrivateMemoryExpectation,
    PrivateMemoryTombstoneRequest, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::*;

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text(text)
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
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

fn private_memory(scope: &AgentScope, name: &str, text: &str) -> AgentPrivateMemory {
    AgentPrivateMemory::new(
        AgentPrivateMemoryId::new(format!("mem-{name}")).expect("memory id"),
        MemoryOperationId::derive_for_agent(scope, format!("create-{name}")).expect("op id"),
        AgentPrivateMemoryKind::Semantic,
        AgentTaskContent::inline(serde_json::json!(text)).expect("content"),
        9_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the memory is bounded")
}

async fn seed(store: &InMemoryAgentPrivateMemoryStore, scope: &AgentScope, name: &str, text: &str) {
    store
        .upsert(
            scope,
            &private_memory(scope, name, text),
            PrivateMemoryExpectation::Absent,
        )
        .await
        .expect("seed upsert");
}

/// A fixture whose run entity retrieves from the given stores through the
/// in-memory reference retriever under an empty (all-allowing) ingress chain.
fn retrieving_world(stores: &Stores) -> Fixture {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("checking the ticket history"))
            .with_turn_for(2, proposing_turn("resolved")),
    );
    let retrieval = AgentMemoryRetrieval::new(
        Arc::new(InMemoryPrivateMemoryRetriever::new(stores.private.clone())),
        stores.private.clone(),
        AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
    );
    Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone())
            .with_retrieval(retrieval),
    )
}

/// A wired retriever fills the snapshot's private selections through the
/// slice 1.11 path: content embedded verbatim, budget accounted, the
/// retrieval query and retriever version recorded, and the whole snapshot
/// still untrusted and digest-verified.
#[tokio::test]
async fn a_wired_retriever_fills_the_snapshot_through_the_snapshot_path() {
    let stores = stores();
    // The task input mentions the ticket; this memory shares that token.
    seed(
        &stores.private,
        &agent_scope(),
        "history",
        "ticket escalation history: customer prefers email",
    )
    .await;

    let fx = retrieving_world(&stores);
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    let scope = run_scope();
    let snapshot = stores
        .snapshots
        .load(
            &scope,
            &AgentContextSnapshotRef::for_turn(&scope, 1).expect("ref"),
        )
        .await
        .expect("load")
        .expect("the turn-one snapshot exists");

    assert_eq!(snapshot.private_memory.len(), 1);
    let selection = &snapshot.private_memory[0];
    assert_eq!(selection.memory_id.as_str(), "mem-history");
    assert_eq!(selection.revision, AgentRevisionNumber::INITIAL);
    assert_eq!(
        selection.content.inline_value(),
        Some(&serde_json::json!(
            "ticket escalation history: customer prefers email"
        )),
        "the exact content used is embedded, not just the identity"
    );
    assert_eq!(selection.content_digest, selection.content.digest());
    assert!(selection.relevance_bps > 0);

    assert_eq!(snapshot.budget.private_memories, 1);
    assert!(snapshot.budget.private_memory_bytes > 0);
    assert_eq!(
        snapshot.ingress_revision,
        Some(AgentRevisionNumber::INITIAL)
    );
    assert!(snapshot.is_untrusted());
    assert_eq!(snapshot.content_digest, snapshot.compute_digest());

    let retrieval = snapshot
        .retrievals
        .iter()
        .find(|retrieval| retrieval.retriever == "in-memory")
        .expect("the private retrieval is recorded beside the session window's");
    assert_eq!(retrieval.retriever_version, AgentRevisionNumber::INITIAL);
    assert!(
        retrieval.query.contains("ticket"),
        "the recorded query derives from the session window"
    );
}

/// Scenario 18, retrieval half: a run's snapshots select only its own agent's
/// memories, whatever a sibling agent or a foreign tenant has stored.
#[tokio::test]
async fn retrieval_is_isolated_by_tenant_and_agent() {
    let stores = stores();
    seed(
        &stores.private,
        &agent_scope(),
        "own",
        "ticket resolution steps",
    )
    .await;
    let sibling = AgentScope::new(tenant(), AgentId::new("billing").expect("agent id"))
        .expect("sibling scope");
    seed(
        &stores.private,
        &sibling,
        "sibling-secret",
        "SIBLING ticket data no other agent may see",
    )
    .await;
    let foreign = AgentScope::new(TenantId::new("rival"), agent_id()).expect("foreign scope");
    seed(
        &stores.private,
        &foreign,
        "foreign-secret",
        "FOREIGN ticket data no other tenant may see",
    )
    .await;

    let fx = retrieving_world(&stores);
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");

    let scope = run_scope();
    for turn in [1, 2] {
        let snapshot = stores
            .snapshots
            .load(
                &scope,
                &AgentContextSnapshotRef::for_turn(&scope, turn).expect("ref"),
            )
            .await
            .expect("load")
            .expect("the snapshot exists");
        for selection in &snapshot.private_memory {
            assert_eq!(
                selection.memory_id.as_str(),
                "mem-own",
                "turn {turn} selected a memory outside the run's agent scope"
            );
            let text = selection
                .content
                .inline_value()
                .expect("inline")
                .to_string();
            assert!(
                !text.contains("SIBLING") && !text.contains("FOREIGN"),
                "turn {turn} leaked cross-scope content into a model input"
            );
        }
    }
}

/// Scenario 17, extended to retrieval — the slice's done-when proof: once a
/// model effect's snapshot is persisted, no store or index drift changes a
/// retried input. A selected memory is CAS-updated, another is tombstoned,
/// and the re-driven settle even runs under an *upgraded* retriever — the
/// snapshot reloads byte-identical, and no second snapshot is minted.
#[tokio::test]
async fn a_model_effect_retry_is_immune_to_index_and_store_drift() {
    let stores = stores();
    let scope_agent = agent_scope();
    seed(
        &stores.private,
        &scope_agent,
        "kept",
        "ticket history alpha",
    )
    .await;
    seed(
        &stores.private,
        &scope_agent,
        "withdrawn",
        "ticket history beta",
    )
    .await;

    let fx = retrieving_world(&stores);
    fx.instantiate_agent().await;
    fx.create_task().await;
    let scope = run_scope();

    // Crank to the turn-one model wait; its snapshot selected both memories.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank to the turn-one wait");
    let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("ref");
    let original = stores
        .snapshots
        .load(&scope, &reference)
        .await
        .expect("load")
        .expect("the turn-one snapshot exists");
    assert_eq!(
        original.private_memory.len(),
        2,
        "both seeded memories were selected"
    );

    // Every kind of drift at once: a concurrent CAS update moves one selected
    // memory to a new revision and new content, the other is withdrawn
    // entirely, ...
    let mut updated = private_memory(&scope_agent, "kept", "ticket history REWRITTEN");
    updated.operation_id =
        MemoryOperationId::derive_for_agent(&scope_agent, "update-kept").expect("op id");
    stores
        .private
        .upsert(
            &scope_agent,
            &updated,
            PrivateMemoryExpectation::Revision(AgentRevisionNumber::INITIAL),
        )
        .await
        .expect("the concurrent update lands");
    stores
        .private
        .tombstone(
            &scope_agent,
            &PrivateMemoryTombstoneRequest {
                memory_id: AgentPrivateMemoryId::new("mem-withdrawn").expect("id"),
                operation_id: MemoryOperationId::derive_for_agent(&scope_agent, "withdraw")
                    .expect("op id"),
                reason: MemoryTombstoneReason::Retracted,
                tombstoned_at: fx.now(),
            },
        )
        .await
        .expect("the withdrawal lands");

    // ... and the re-driven settle — a recovery on another node, after a
    // deploy — runs under an upgraded retriever version.
    let upgraded = AgentMemoryRetrieval::new(
        Arc::new(
            InMemoryPrivateMemoryRetriever::new(stores.private.clone())
                .with_version(AgentRevisionNumber::new(9)),
        ),
        stores.private.clone(),
        AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
    );
    let mut run = rakka_agent::testkit::run_entity(&scope, &fx.runs, &fx.effects)
        .with_effect_policies(fx.policies.clone())
        .with_memory(
            AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
                .with_private_store(stores.private.clone())
                .with_retrieval(upgraded),
        );
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("re-drive the turn-one wait");

    let reused = stores
        .snapshots
        .load(&scope, &reference)
        .await
        .expect("load")
        .expect("the snapshot still exists");
    assert_eq!(
        reused, original,
        "index and store drift changed a retried model input"
    );
    assert_eq!(
        stores.snapshots.len(&scope),
        1,
        "the re-driven settle minted a second snapshot for the turn"
    );
    assert_eq!(
        reused.private_memory[0]
            .content
            .inline_value()
            .expect("inline")
            .as_str(),
        Some("ticket history alpha"),
        "the retried input carries the content the first assembly selected"
    );
}

/// A retriever outage never stalls the run: the turn degrades to an empty
/// private selection, the attempted retrieval is still recorded, and the run
/// completes (specification 13.1 over 13.5's determinism — the degraded
/// selection is then permanent for that turn, by design).
#[tokio::test]
async fn a_retriever_outage_degrades_to_an_empty_selection_without_stalling() {
    let stores = stores();
    seed(
        &stores.private,
        &agent_scope(),
        "unreachable",
        "ticket history behind a dead index",
    )
    .await;

    let scripted = ScriptedPrivateMemoryRetriever::new()
        .with_error(rakka_agent::MemoryError::Backend {
            backend: "scripted".to_string(),
            message: "vector index outage".to_string(),
        })
        .with_error(rakka_agent::MemoryError::Backend {
            backend: "scripted".to_string(),
            message: "vector index outage".to_string(),
        });
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("checking the ticket history"))
            .with_turn_for(2, proposing_turn("resolved")),
    );
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone())
            .with_retrieval(AgentMemoryRetrieval::new(
                Arc::new(scripted.clone()),
                stores.private.clone(),
                AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
            )),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump()
        .await
        .expect("an index outage must not stall the loop");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert!(scripted.calls() >= 2, "each turn attempted its retrieval");

    let scope = run_scope();
    for turn in [1, 2] {
        let snapshot = stores
            .snapshots
            .load(
                &scope,
                &AgentContextSnapshotRef::for_turn(&scope, turn).expect("ref"),
            )
            .await
            .expect("load")
            .expect("the snapshot exists");
        assert!(
            snapshot.private_memory.is_empty(),
            "turn {turn} degraded to an empty selection"
        );
        assert_eq!(snapshot.ingress_revision, None);
        assert!(
            snapshot
                .retrievals
                .iter()
                .any(|retrieval| retrieval.retriever == "scripted"),
            "turn {turn} still recorded the attempted retrieval"
        );
    }
}

/// A run with a private store but no retrieval bundle keeps the session-only
/// snapshot shape — wiring the slice 2.1 store alone changes nothing about a
/// model input until a deployment opts into retrieval.
#[tokio::test]
async fn a_run_without_retrieval_keeps_the_session_only_shape() {
    let stores = stores();
    seed(
        &stores.private,
        &agent_scope(),
        "dormant",
        "ticket history nobody retrieves",
    )
    .await;

    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("checking the ticket history"))
            .with_turn_for(2, proposing_turn("resolved")),
    );
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(stores.session.clone(), stores.snapshots.clone())
            .with_private_store(stores.private.clone()),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");

    let scope = run_scope();
    for turn in [1, 2] {
        let snapshot = stores
            .snapshots
            .load(
                &scope,
                &AgentContextSnapshotRef::for_turn(&scope, turn).expect("ref"),
            )
            .await
            .expect("load")
            .expect("the snapshot exists");
        assert!(snapshot.private_memory.is_empty());
        assert_eq!(snapshot.ingress_revision, None);
        assert_eq!(snapshot.budget.private_memories, 0);
        assert!(
            !snapshot
                .retrievals
                .iter()
                .any(|retrieval| retrieval.retriever != "session-window"),
            "only the session-window read is recorded"
        );
    }
}
