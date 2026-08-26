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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    DeterministicModelAdapter, ScriptedDispatcher, ScriptedPrivateMemoryRetriever,
};
use rakka_agent::{
    AgentContextSnapshotRef, AgentGuardrailChain, AgentId, AgentMemoryRetrieval, AgentModelTurn,
    AgentPrivateMemory, AgentPrivateMemoryId, AgentPrivateMemoryKind, AgentPrivateMemoryStore,
    AgentRevisionNumber, AgentRunMemory, AgentScope, AgentTaskContent, ContextSnapshotStore,
    InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    MemoryClassification, MemoryContextSnapshot, MemoryEmbeddingRef, MemoryError, MemoryFuture,
    MemoryOperationId, MemoryRetrievalOutcome, MemoryTombstoneReason, PrivateMemoryCursor,
    PrivateMemoryDeleteRequest, PrivateMemoryExpectation, PrivateMemoryPage,
    PrivateMemoryTombstoneRequest, RetrievedPrivateMemory, SessionMemoryPromotionExecutor,
    TenantId, AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
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

// ---------------------------------------------------------------------------
// What the ranking may decide, and what a ranking may cost.
// ---------------------------------------------------------------------------

/// An authoritative store that counts its point reads and can start failing
/// them, so a test can see how much I/O a ranking buys and what a store fault
/// part-way through a walk costs.
///
/// Everything but `get` delegates: `get` is the whole resolution path.
struct MeteredAuthority {
    inner: Arc<InMemoryAgentPrivateMemoryStore>,
    gets: AtomicUsize,
    fail_after: Option<usize>,
}

impl MeteredAuthority {
    fn over(inner: Arc<InMemoryAgentPrivateMemoryStore>) -> Self {
        Self {
            inner,
            gets: AtomicUsize::new(0),
            fail_after: None,
        }
    }

    /// Answers the first `count` reads and fails every one after them.
    fn failing_after(mut self, count: usize) -> Self {
        self.fail_after = Some(count);
        self
    }

    fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
}

impl AgentPrivateMemoryStore for MeteredAuthority {
    fn backend_name(&self) -> &'static str {
        "metered"
    }

    fn upsert<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory: &'a AgentPrivateMemory,
        expected: PrivateMemoryExpectation,
    ) -> MemoryFuture<'a, AgentPrivateMemory> {
        self.inner.upsert(scope, memory, expected)
    }

    fn get<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory_id: &'a AgentPrivateMemoryId,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentPrivateMemory>> {
        let seen = self.gets.fetch_add(1, Ordering::SeqCst) + 1;
        let failed = self.fail_after.is_some_and(|after| seen > after);
        Box::pin(async move {
            if failed {
                return Err(MemoryError::Backend {
                    backend: "metered".to_string(),
                    message: "the authoritative store is unreachable".to_string(),
                });
            }
            self.inner.get(scope, memory_id, now).await
        })
    }

    fn list<'a>(
        &'a self,
        scope: &'a AgentScope,
        cursor: PrivateMemoryCursor,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, PrivateMemoryPage> {
        self.inner.list(scope, cursor, now)
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryTombstoneRequest,
    ) -> MemoryFuture<'a, AgentPrivateMemory> {
        self.inner.tombstone(scope, request)
    }

    fn delete<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryDeleteRequest,
    ) -> MemoryFuture<'a, ()> {
        self.inner.delete(scope, request)
    }

    fn purge_expired<'a>(
        &'a self,
        scope: &'a AgentScope,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> MemoryFuture<'a, u64> {
        self.inner.purge_expired(scope, now, limit)
    }
}

/// The same world as [`hostile_world`], over an authority a test can meter.
fn metered_world(
    ranking: MemoryRetrievalOutcome,
    authority: Arc<MeteredAuthority>,
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
        .with_retrieval(AgentMemoryRetrieval::new(
            Arc::new(ScriptedPrivateMemoryRetriever::new().with_outcome(ranking)),
            authority,
            AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        )),
    );
    (fx, snapshots)
}

/// A flat ranking of `count` identities that exist in no store, all at one
/// relevance — the shape a drifted or hostile index returns.
fn long_ranking(count: usize) -> MemoryRetrievalOutcome {
    MemoryRetrievalOutcome {
        memories: (0..count)
            .map(|index| RetrievedPrivateMemory {
                memory: private_memory(&agent_scope(), &format!("ghost-{index}"), "nothing"),
                relevance_bps: 5_000,
                embedding: None,
            })
            .collect(),
        index_watermark: None,
    }
}

/// The embedding reference the snapshot carries comes from the authoritative
/// record, not from the ranking payload.
///
/// It was the one field lifted verbatim from the retriever's answer into
/// durable, digest-covered state. `AgentPrivateMemory::validate` bounds the
/// model name and rejects an empty model or zero dimensions; nothing bounded
/// it on this path, and `max_bytes` counts only content, so a ranking could
/// put an arbitrarily large string into an immutable snapshot.
#[tokio::test]
async fn the_snapshot_embeds_the_stores_embedding_not_the_rankings() {
    let scope = agent_scope();
    let authority = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let stored = private_memory(&scope, "ours", "our own renewal terms")
        .with_embedding(MemoryEmbeddingRef {
            model: "trusted-embedder".to_string(),
            dimensions: 8,
            version: AgentRevisionNumber::INITIAL,
        })
        .expect("the stored embedding is bounded");
    authority
        .upsert(&scope, &stored, PrivateMemoryExpectation::Absent)
        .await
        .expect("seed upsert");

    // The ranking names the same identity but claims a different vector.
    let mut ranked = ranking(vec![stored.clone()]);
    ranked.memories[0].embedding = Some(MemoryEmbeddingRef {
        model: "FORGED-EMBEDDER".to_string(),
        dimensions: 4_096,
        version: AgentRevisionNumber::INITIAL,
    });

    let (fx, snapshots) = hostile_world(ranked, authority);
    let snapshot = first_snapshot(&fx, &snapshots).await;

    let selected = snapshot
        .private_memory
        .first()
        .expect("the stored memory was selected");
    assert_eq!(
        selected
            .embedding
            .as_ref()
            .map(|reference| &reference.model),
        Some(&"trusted-embedder".to_string()),
        "the ranking's embedding reference reached the snapshot"
    );
    let encoded = serde_json::to_string(&snapshot).expect("the snapshot serializes");
    assert!(
        !encoded.contains("FORGED-EMBEDDER"),
        "the ranking's own words are in the durable snapshot: {encoded}"
    );
}

/// A ranking's *length* cannot decide how much I/O the settle pass performs.
///
/// The walk stops only at a selection bound, and an identity the store does
/// not hold never grows the selection — so before the resolution ceiling a
/// retriever could hand the owning shard as many sequential point reads as it
/// liked, none of which produced anything.
#[tokio::test]
async fn a_long_ranking_cannot_buy_unbounded_authoritative_reads() {
    let inner = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    seed(&inner, &agent_scope(), "ours", "our own renewal terms").await;
    let authority = Arc::new(MeteredAuthority::over(inner));

    let (fx, snapshots) = metered_world(long_ranking(500), authority.clone());
    let snapshot = first_snapshot(&fx, &snapshots).await;

    assert!(
        authority.gets() <= AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS,
        "a 500-entry ranking bought {} store reads, ceiling {}",
        authority.gets(),
        AGENT_MEMORY_RETRIEVAL_MAX_RESOLUTIONS
    );
    assert!(
        snapshot.private_memory.is_empty(),
        "no ranked identity was held in scope, so nothing may be embedded"
    );
}

/// A store fault part-way through the walk keeps what is already verified.
///
/// The snapshot is immutable and persisted first-writer-wins, so discarding
/// the verified prefix would not degrade *this* assembly — it would blank the
/// run's long-term memory for the rest of its life, on every retry of every
/// later turn.
#[tokio::test]
async fn a_store_fault_mid_walk_keeps_the_verified_prefix() {
    let scope = agent_scope();
    let inner = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    seed(&inner, &scope, "first", "the renewal terms").await;
    seed(&inner, &scope, "second", "the escalation path").await;
    seed(&inner, &scope, "third", "the refund policy").await;
    let authority = Arc::new(MeteredAuthority::over(inner).failing_after(2));

    let ranked = ranking(vec![
        private_memory(&scope, "first", "the renewal terms"),
        private_memory(&scope, "second", "the escalation path"),
        private_memory(&scope, "third", "the refund policy"),
    ]);
    let (fx, snapshots) = metered_world(ranked, authority.clone());
    let snapshot = first_snapshot(&fx, &snapshots).await;

    assert_eq!(
        snapshot.private_memory.len(),
        2,
        "the two records the store answered for were discarded by the third's \
         failure: {:?}",
        snapshot.private_memory
    );
    assert_eq!(
        snapshot.budget.private_memories, 2,
        "the budget must describe what the snapshot carries"
    );
    assert!(
        snapshot.budget.private_memory_bytes > 0,
        "a snapshot carrying selections carries their bytes"
    );
    assert!(
        snapshot.ingress_revision.is_some(),
        "a chain evaluated every embedded record, so the snapshot must name it"
    );
    assert_eq!(authority.gets(), 3, "the walk stopped at the failing read");
}

// ---------------------------------------------------------------------------
// One declaration of the store this agent's long-term memory lives in.
// ---------------------------------------------------------------------------

/// The bundle's authority *is* the run's private store, whatever order the
/// builders ran in, and the promotion executor derives from the same place.
///
/// Naming the two separately is a pairing nothing can check — an
/// `Arc<dyn AgentPrivateMemoryStore>` carries no identity a wiring check could
/// compare — and getting it wrong writes every promoted memory where nothing
/// reads it, signalled only by a counter a hostile retriever also moves.
#[test]
fn the_retrieval_bundles_authority_is_the_runs_private_store() {
    let authority: Arc<dyn AgentPrivateMemoryStore> =
        Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let decoy: Arc<dyn AgentPrivateMemoryStore> = Arc::new(MeteredAuthority::over(Arc::new(
        InMemoryAgentPrivateMemoryStore::new(),
    )));
    let bundle = AgentMemoryRetrieval::new(
        Arc::new(ScriptedPrivateMemoryRetriever::new()),
        authority.clone(),
        AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
    );

    for (label, memory) in [
        (
            "private store first",
            AgentRunMemory::new(
                Arc::new(InMemorySessionMemoryStore::new()),
                Arc::new(InMemoryContextSnapshotStore::new()),
            )
            .with_private_store(decoy.clone())
            .with_retrieval(bundle.clone()),
        ),
        (
            "retrieval first",
            AgentRunMemory::new(
                Arc::new(InMemorySessionMemoryStore::new()),
                Arc::new(InMemoryContextSnapshotStore::new()),
            )
            .with_retrieval(bundle.clone())
            .with_private_store(decoy.clone()),
        ),
    ] {
        let private = memory.private().expect("a private store is wired");
        assert!(
            Arc::ptr_eq(private, &authority),
            "{label}: the run answered the store nothing resolves through"
        );
        let executor = SessionMemoryPromotionExecutor::for_memory(&memory)
            .expect("the bundle names a private store");
        assert!(
            Arc::ptr_eq(executor.private_store(), &authority),
            "{label}: promotions would write a store retrieval never reads"
        );
    }
}

/// With no bundle wired, the explicitly named store is still the answer.
#[test]
fn without_a_bundle_the_named_private_store_is_the_runs_private_store() {
    let named: Arc<dyn AgentPrivateMemoryStore> = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let memory = AgentRunMemory::new(
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    )
    .with_private_store(named.clone());

    assert!(Arc::ptr_eq(
        memory.private().expect("a private store is wired"),
        &named
    ));
    assert!(SessionMemoryPromotionExecutor::for_memory(&memory).is_some());

    let bare = AgentRunMemory::new(
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    );
    assert!(bare.private().is_none());
    assert!(
        SessionMemoryPromotionExecutor::for_memory(&bare).is_none(),
        "a deployment that names no private store does not promote"
    );
}
