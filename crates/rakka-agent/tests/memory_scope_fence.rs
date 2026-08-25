//! A retriever cannot place into a model context what the addressed scope's
//! authoritative store does not hold.
//!
//! Specification: sections 13.1, 13.5, 13.6, and the retrieval clauses of 16;
//! scenario 18. Retrieval is the one path by which memory reaches a model, and
//! until this slice the record it embedded was the one the *retriever*
//! returned. Every property of that record was re-checked — validity,
//! classification, confidence, tombstone, expiry, duplication — except the one
//! that decides whose memory it is, and the trait documented that gap as
//! unavoidable: an `AgentPrivateMemory` carries no tenant or agent, so a
//! foreign record is indistinguishable from a correct one by inspection.
//!
//! It was avoidable. The assembly holds the authoritative `AgentScope` and the
//! store is scope-addressed, so resolving each ranked identity through the
//! store answers the question by construction — and, in the same step, stops
//! the retriever deciding the content, classification, and confidence that
//! reach the model at all.
//!
//! Why `private_memory_retrieval.rs::retrieval_is_isolated_by_tenant_and_agent`
//! was not enough: it drives `InMemoryPrivateMemoryRetriever`, whose isolation
//! is inherited from the store it scans. It proves the *reference retriever*
//! isolates. It says nothing about whether the *assembly* does — and the
//! assembly is the only layer a third-party vector backend cannot bypass.
//!
//! Every test here drives the real run entity: the settle pass calls
//! `persist_context_snapshots`, which is the one production caller of
//! `assemble_context`.

use std::sync::Arc;

use rakka_agent::testkit::{
    DeterministicModelAdapter, ScriptedDispatcher, ScriptedPrivateMemoryRetriever,
};
use rakka_agent::{
    AgentContextSnapshotRef, AgentGuardrailChain, AgentId, AgentMemoryRetrieval, AgentModelTurn,
    AgentPrivateMemory, AgentPrivateMemoryId, AgentPrivateMemoryKind, AgentPrivateMemoryStore,
    AgentRevisionNumber, AgentRunMemory, AgentScope, AgentTaskContent, ContextSnapshotStore,
    InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    MemoryClassification, MemoryContextSnapshot, MemoryOperationId, MemoryRetrievalOutcome,
    MemoryTombstoneReason, PrivateMemoryExpectation, PrivateMemoryTombstoneRequest,
    RetrievedPrivateMemory, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
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

/// The scope of another tenant entirely.
fn foreign_scope() -> AgentScope {
    AgentScope::new(
        TenantId::new("other-corp"),
        AgentId::new("their-agent").expect("agent id"),
    )
    .expect("the foreign scope is valid")
}

/// A world whose retriever answers a fixed ranking regardless of the scope it
/// is asked about — a hostile, or merely broken, vector backend.
///
/// `ScriptedPrivateMemoryRetriever` ignores its `scope` argument entirely, so
/// it is not a caricature: it is exactly the failure mode the trait doc used
/// to say nothing downstream could catch.
fn hostile_world(
    ranking: MemoryRetrievalOutcome,
    authority: Arc<InMemoryAgentPrivateMemoryStore>,
) -> (Fixture, Arc<InMemoryContextSnapshotStore>) {
    let snapshots = Arc::new(InMemoryContextSnapshotStore::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("checking the ticket history"))
            .with_turn_for(2, proposing_turn("resolved")),
    );
    let fx = Fixture::new(dispatcher).with_memory(
        AgentRunMemory::new(
            Arc::new(InMemorySessionMemoryStore::new()),
            snapshots.clone(),
        )
        .with_private_store(authority.clone())
        .with_retrieval(AgentMemoryRetrieval::new(
            Arc::new(ScriptedPrivateMemoryRetriever::new().with_outcome(ranking)),
            authority,
            AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        )),
    );
    (fx, snapshots)
}

fn ranking(memories: Vec<AgentPrivateMemory>) -> MemoryRetrievalOutcome {
    MemoryRetrievalOutcome {
        memories: memories
            .into_iter()
            .enumerate()
            .map(|(index, memory)| RetrievedPrivateMemory {
                memory,
                relevance_bps: 9_000 - u16::try_from(index).unwrap_or(0) * 100,
                embedding: None,
            })
            .collect(),
        index_watermark: None,
    }
}

/// Drives the run to completion and returns its first turn's snapshot.
async fn first_snapshot(
    fx: &Fixture,
    snapshots: &InMemoryContextSnapshotStore,
) -> MemoryContextSnapshot {
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");
    let scope = run_scope();
    snapshots
        .load(
            &scope,
            &AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference"),
        )
        .await
        .expect("the snapshot loads")
        .expect("the first turn persisted a snapshot")
}

// ---------------------------------------------------------------------------
// Scenario 18: a foreign scope's memory never reaches a model context.
// ---------------------------------------------------------------------------

/// The headline claim. A retriever that answers with another tenant's record
/// gets it dropped, not embedded.
#[tokio::test]
async fn a_foreign_tenants_memory_never_enters_the_snapshot() {
    let foreign = foreign_scope();
    // The foreign record exists — in the foreign tenant's own store, which is
    // not the one this run is wired to. That is the realistic shape: a shared
    // vector index with a wrong predicate, not a fabricated record.
    let leaked = private_memory(&foreign, "their-secret", "FOREIGN-TENANT-CONTENT");

    let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    seed(&authority, &agent_scope(), "ours", "our own renewal terms").await;

    let (fx, snapshots) = hostile_world(ranking(vec![leaked]), authority);
    let snapshot = first_snapshot(&fx, &snapshots).await;

    assert!(
        snapshot.private_memory.is_empty(),
        "a foreign tenant's record reached the model context: {:?}",
        snapshot.private_memory
    );
    let encoded = serde_json::to_string(&snapshot).expect("the snapshot serializes");
    assert!(
        !encoded.contains("FOREIGN-TENANT-CONTENT"),
        "the foreign content is in the snapshot somewhere: {encoded}"
    );
}

/// An identity that exists in no scope at all is dropped the same way.
#[tokio::test]
async fn a_memory_that_exists_in_no_scope_never_enters_the_snapshot() {
    let invented = private_memory(&agent_scope(), "invented", "FABRICATED-CONTENT");
    let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    seed(&authority, &agent_scope(), "ours", "our own renewal terms").await;

    let (fx, snapshots) = hostile_world(ranking(vec![invented]), authority);
    let snapshot = first_snapshot(&fx, &snapshots).await;

    assert!(snapshot.private_memory.is_empty());
    let encoded = serde_json::to_string(&snapshot).expect("the snapshot serializes");
    assert!(!encoded.contains("FABRICATED-CONTENT"));
}

// ---------------------------------------------------------------------------
// The retriever's payload is advisory: the store's record is what is embedded.
// ---------------------------------------------------------------------------

/// A forged copy of a real memory is replaced by the authoritative record.
///
/// This is the half nobody had named as a gap. The retriever returns the right
/// *identity* with the wrong everything else — rewritten content, escalated
/// confidence, a downgraded classification that would slip a sensitive record
/// past the default query. None of it survives.
#[tokio::test]
async fn a_forged_copy_of_a_real_memory_is_replaced_by_the_authoritative_record() {
    let scope = agent_scope();
    let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    seed(&authority, &scope, "ours", "AUTHORITATIVE-CONTENT").await;

    let mut forged = private_memory(&scope, "ours", "FORGED-CONTENT");
    forged.confidence_bps = 10_000;
    forged.classification = MemoryClassification::Unclassified;

    let (fx, snapshots) = hostile_world(ranking(vec![forged]), authority);
    let snapshot = first_snapshot(&fx, &snapshots).await;

    assert_eq!(
        snapshot.private_memory.len(),
        1,
        "the real memory is still selected — the forgery is corrected, not punished"
    );
    let selected = &snapshot.private_memory[0];
    assert_eq!(selected.memory_id.as_str(), "mem-ours");
    assert_eq!(
        selected.confidence_bps, 9_000,
        "the store's confidence decides, not the ranking's"
    );
    let encoded = serde_json::to_string(&snapshot).expect("the snapshot serializes");
    assert!(
        encoded.contains("AUTHORITATIVE-CONTENT"),
        "the snapshot does not carry the store's content: {encoded}"
    );
    assert!(
        !encoded.contains("FORGED-CONTENT"),
        "the snapshot carries the retriever's content: {encoded}"
    );
}

/// A stale ranked revision embeds the authoritative current record.
///
/// A vector index lags its source by construction. Before this fix the
/// snapshot froze whatever the index happened to hold.
#[tokio::test]
async fn a_stale_ranked_revision_embeds_the_authoritative_current_record() {
    let scope = agent_scope();
    let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let original = private_memory(&scope, "ours", "STALE-INDEXED-CONTENT");
    authority
        .upsert(&scope, &original, PrivateMemoryExpectation::Absent)
        .await
        .expect("the original seeds");

    // The store moves on; the index does not.
    let mut updated = private_memory(&scope, "ours", "CURRENT-CONTENT");
    updated.operation_id =
        MemoryOperationId::derive_for_agent(&scope, "update-ours").expect("op id");
    authority
        .upsert(
            &scope,
            &updated,
            PrivateMemoryExpectation::Revision(AgentRevisionNumber::INITIAL),
        )
        .await
        .expect("the update lands");

    let (fx, snapshots) = hostile_world(ranking(vec![original]), authority);
    let snapshot = first_snapshot(&fx, &snapshots).await;

    let encoded = serde_json::to_string(&snapshot).expect("the snapshot serializes");
    assert!(
        encoded.contains("CURRENT-CONTENT"),
        "the snapshot froze the index's stale copy: {encoded}"
    );
    assert!(!encoded.contains("STALE-INDEXED-CONTENT"));
}

/// A ranked identity the store has tombstoned is dropped, not embedded.
#[tokio::test]
async fn a_ranked_identity_the_store_tombstoned_is_dropped_not_embedded() {
    let scope = agent_scope();
    let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let withdrawn = private_memory(&scope, "withdrawn", "WITHDRAWN-CONTENT");
    authority
        .upsert(&scope, &withdrawn, PrivateMemoryExpectation::Absent)
        .await
        .expect("the memory seeds");
    authority
        .tombstone(
            &scope,
            &PrivateMemoryTombstoneRequest {
                memory_id: withdrawn.memory_id.clone(),
                operation_id: MemoryOperationId::derive_for_agent(&scope, "withdraw")
                    .expect("op id"),
                reason: MemoryTombstoneReason::Retracted,
                tombstoned_at: AgentTimestampMillis::new(2),
            },
        )
        .await
        .expect("the withdrawal lands");

    // The index still ranks the pre-withdrawal copy.
    let (fx, snapshots) = hostile_world(ranking(vec![withdrawn]), authority);
    let snapshot = first_snapshot(&fx, &snapshots).await;

    assert!(snapshot.private_memory.is_empty());
    let encoded = serde_json::to_string(&snapshot).expect("the snapshot serializes");
    assert!(
        !encoded.contains("WITHDRAWN-CONTENT"),
        "a withdrawn memory reached a model context: {encoded}"
    );
}

// ---------------------------------------------------------------------------
// The existence-leak clause: an unverifiable retrieval is indistinguishable
// from one that found nothing.
// ---------------------------------------------------------------------------

/// A wholly-foreign ranking produces the same snapshot an empty ranking does.
///
/// Not "produces no selections" — *the same snapshot*, digest included. A
/// difference anywhere in the record would be an oracle: a reader could tell
/// "your retriever named memories you do not own" from "your retriever found
/// nothing", and the first answer is information about another tenant.
#[tokio::test]
async fn an_unverifiable_retrieval_answers_what_an_empty_retrieval_answers() {
    let foreign = foreign_scope();
    let leaked = private_memory(&foreign, "their-secret", "FOREIGN-TENANT-CONTENT");

    let unverifiable = {
        let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
        let (fx, snapshots) = hostile_world(ranking(vec![leaked]), authority);
        first_snapshot(&fx, &snapshots).await
    };
    let empty = {
        let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
        let (fx, snapshots) = hostile_world(ranking(Vec::new()), authority);
        first_snapshot(&fx, &snapshots).await
    };

    assert_eq!(
        unverifiable.private_memory, empty.private_memory,
        "the selections differ"
    );
    assert_eq!(
        unverifiable.budget, empty.budget,
        "the budget accounting differs, which is an oracle"
    );
    assert_eq!(
        unverifiable.content_digest, empty.content_digest,
        "the content digests differ, which is an oracle"
    );
    assert_eq!(
        unverifiable.retrievals, empty.retrievals,
        "the recorded retrieval differs, which is an oracle"
    );
}

// ---------------------------------------------------------------------------
// The drop is counted, and the count carries no identity.
// ---------------------------------------------------------------------------

/// A dropped identity is observable as a bounded metric, not a silent skip.
#[tokio::test]
async fn dropped_identities_are_counted_and_observable() {
    let foreign = foreign_scope();
    let leaked = private_memory(&foreign, "their-secret", "FOREIGN-TENANT-CONTENT");
    let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());

    let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let snapshots = Arc::new(InMemoryContextSnapshotStore::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("checking the ticket history"))
            .with_turn_for(2, proposing_turn("resolved")),
    );
    let fx = Fixture::new(dispatcher)
        .with_metrics(metrics.clone())
        .with_memory(
            AgentRunMemory::new(
                Arc::new(InMemorySessionMemoryStore::new()),
                snapshots.clone(),
            )
            .with_private_store(authority.clone())
            .with_retrieval(AgentMemoryRetrieval::new(
                Arc::new(ScriptedPrivateMemoryRetriever::new().with_outcome(ranking(vec![leaked]))),
                authority,
                AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
            )),
        );
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");

    let rendered = format!("{:?}", metrics.snapshot().observations());
    assert!(
        rendered.contains("unverified"),
        "the drop was not counted anywhere an operator could see it: {rendered}"
    );
    // The count says how many, never which — an identity in a label is the
    // unbounded-cardinality failure specification 16 forbids outright.
    assert!(
        !rendered.contains("their-secret"),
        "the metric carries the dropped identity: {rendered}"
    );
    assert!(
        !rendered.contains("other-corp"),
        "the metric carries the foreign tenant: {rendered}"
    );
}
