//! Private-memory vector retrieval and snapshot-assembly integration.
//!
//! Owns the Rakka-side seam of slice 2.2: the vendor-neutral
//! [`AgentPrivateMemoryRetriever`] trait a vector backend implements (the
//! pgvector adapter lives in `rakka-agent-postgres`), the
//! [`AgentMemoryEmbedder`] seam a deployment supplies, the deterministic
//! retrieval-query derivation, and [`assemble_context`] — the slice 1.11
//! snapshot-assembly path extended to fill a snapshot's private selections.
//!
//! Specification: sections 13.3, 13.5, 13.6, and the retrieval clauses of 16.
//!
//! # Retrieval feeds the model only through the snapshot
//!
//! Retrieval runs during snapshot assembly in the settle pass — never inside a
//! transition, never as a side channel into a model request. Whatever the
//! retriever returns is re-checked fail-closed, evaluated at the
//! [`AgentGuardrailBoundary::MemoryIngress`] boundary, embedded (content, not
//! identity) into the immutable [`MemoryContextSnapshot`], and persisted
//! first-writer-wins. A model-effect retry reads that snapshot back, so index
//! drift, a concurrent memory write, or a re-ranked query can never change a
//! retried model input (scenario 17,
//! [specification 13.5](../../../docs/plans/rakka-agent/spec.md)).
//!
//! # Memory is never the correctness source
//!
//! A retriever outage degrades the turn to an empty private selection rather
//! than stalling the settle pass: the attempted retrieval is still recorded on
//! the snapshot, the degradation is counted, and the run proceeds
//! ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)). The
//! consequence is deliberate: a turn assembled during an outage keeps its
//! empty selection forever, because first-writer-wins determinism is the
//! stronger promise. Session-store failures keep their propagate-and-retry
//! semantics — the session *is* the conversation; private recall is auxiliary.
//!
//! # Embeddings are derived, and nothing here stamps the record
//!
//! The vectors a retrieval backend derives are rebuildable projections of the
//! authoritative [`AgentPrivateMemory`] content, never the only copy
//! ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)). No path in
//! this slice writes [`AgentPrivateMemory::embedding`] back through the store:
//! a compare-and-set stamp from the indexing path would bump the revision the
//! just-derived vector was keyed to and race live runs for no fence the
//! derived row does not already provide. The derived row carries the
//! model/dimension/version metadata, and [`RetrievedPrivateMemory::embedding`]
//! carries it into the snapshot selection. The record field remains for
//! deployment writers that know their embedder configuration.
//!
//! # One chain, two enforcement points
//!
//! [`AgentMemoryRetrieval`] requires its guardrail chain at construction — a
//! deployment with no memory-ingress stages passes an empty chain explicitly,
//! so a wired retriever can never silently skip the boundary. The chain must
//! be the same one the dispatch authority carries, because the authority's
//! coverage check ([`crate::tools::AGENT_EVALUATED_GUARDRAIL_BOUNDARIES`])
//! cannot see this bundle.

use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use rakka_agent_workflow::AgentTimestampMillis;

use crate::definition::AgentRevisionNumber;
use crate::guardrails::{
    AgentGuardrailBoundary, AgentGuardrailChain, AgentGuardrailContext, AgentGuardrailDisposition,
};
use crate::identity::{AgentRunScope, AgentScope};
use crate::memory::{
    assemble_session_context, AgentContextSnapshotRef, AgentPrivateMemory, AgentPrivateMemoryId,
    AgentPrivateMemoryStore, AgentRunMemory, MemoryClassification, MemoryContextSnapshot,
    MemoryEmbeddingRef, MemoryEntryRole, MemoryError, MemoryFuture, PrivateMemoryCursor,
    SnapshotIngressRecord, SnapshotPrivateMemory, SnapshotRetrieval,
    AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES, AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES,
    AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES, AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES,
};
use crate::task::AgentTaskContent;

/// Largest retrieval-query text, in bytes.
///
/// The query is recorded verbatim on the snapshot's [`SnapshotRetrieval`]
/// entry, so it is bounded like every other snapshot field; derivation keeps
/// the tail, because the most recent session content is the informative end.
pub const AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES: usize = 1024;

/// How many recent session entries the default query derivation reads.
pub const AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES: usize = 4;

/// Longest index watermark recorded on a snapshot retrieval, in bytes.
pub const AGENT_MEMORY_INDEX_WATERMARK_MAX_LENGTH: usize = 128;

/// Most authoritative records the in-memory reference retriever scans per
/// retrieval.
///
/// The reference implementation ranks by scanning the store; the scan is
/// bounded so a pathological corpus degrades to a bounded answer instead of an
/// unbounded read. A vector backend ranks in its index and never needs this.
pub const AGENT_MEMORY_RETRIEVAL_SCAN_MAX_ENTRIES: usize = 1024;

// ===========================================================================
// The retrieval seam ([specification 13.3, 13.6]).
// ===========================================================================

/// One bounded private-memory retrieval request.
///
/// Every field is a *pre-ranking* filter
/// ([specification 16](../../../docs/plans/rakka-agent/spec.md): "memory
/// retrieval MUST enforce tenant, agent/knowledge-space, classification, and
/// purpose restrictions before ranking results") — an implementation applies
/// them as query predicates before any distance ordering, even where that
/// costs index performance ([specification 13.6](../../../docs/plans/rakka-agent/spec.md)).
/// The tenant and agent filters travel separately, as the explicit
/// [`AgentScope`] every [`AgentPrivateMemoryRetriever::retrieve`] call
/// addresses. Builders clamp rather than error, like every policy knob in
/// this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRetrievalQuery {
    text: String,
    limit: usize,
    max_bytes: usize,
    min_confidence_bps: u16,
    classifications: BTreeSet<MemoryClassification>,
}

impl MemoryRetrievalQuery {
    /// A query over the given text with the default bounds: at most
    /// [`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES`] results within
    /// [`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES`], any confidence,
    /// unclassified content only.
    ///
    /// Text longer than [`AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES`] is
    /// truncated deterministically at a character boundary, keeping the head.
    #[must_use]
    pub fn new(text: impl Into<String>) -> Self {
        let mut text: String = text.into();
        if text.len() > AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES {
            let mut end = AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text.truncate(end);
        }
        Self {
            text,
            limit: AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES,
            max_bytes: AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES,
            min_confidence_bps: 0,
            classifications: BTreeSet::from([MemoryClassification::Unclassified]),
        }
    }

    /// Sets the result limit, clamped to
    /// `1..=`[`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES`].
    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit.clamp(1, AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES);
        self
    }

    /// Sets the total content byte budget, clamped to
    /// `1..=`[`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES`].
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes.clamp(1, AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES);
        self
    }

    /// Sets the minimum confidence a result must carry, clamped to 10000
    /// basis points.
    #[must_use]
    pub fn with_min_confidence_bps(mut self, min_confidence_bps: u16) -> Self {
        self.min_confidence_bps = min_confidence_bps.min(10_000);
        self
    }

    /// Sets the classifications a result may carry.
    ///
    /// [`MemoryClassification::Redacted`] is never eligible — its bytes were
    /// withheld by policy, so there is nothing a retrieval may hand a model —
    /// and is removed from the set rather than refused, like every other
    /// clamp. An empty remainder means the default: unclassified only.
    #[must_use]
    pub fn with_classifications(mut self, classifications: BTreeSet<MemoryClassification>) -> Self {
        let mut classifications = classifications;
        classifications.remove(&MemoryClassification::Redacted);
        if classifications.is_empty() {
            classifications.insert(MemoryClassification::Unclassified);
        }
        self.classifications = classifications;
        self
    }

    /// The bounded query text.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The result limit.
    #[must_use]
    pub const fn limit(&self) -> usize {
        self.limit
    }

    /// The total content byte budget.
    #[must_use]
    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// The minimum confidence a result must carry, in basis points.
    #[must_use]
    pub const fn min_confidence_bps(&self) -> u16 {
        self.min_confidence_bps
    }

    /// The classifications a result may carry.
    #[must_use]
    pub const fn classifications(&self) -> &BTreeSet<MemoryClassification> {
        &self.classifications
    }

    /// Whether one record passes the query's pre-ranking filters at `now`.
    ///
    /// This is the shared filter table both the reference retriever and the
    /// assembly-side fail-closed re-check apply: live (not tombstoned, not
    /// expired), classification in the allowed set, confidence at or above
    /// the floor.
    #[must_use]
    pub fn admits(&self, memory: &AgentPrivateMemory, now: AgentTimestampMillis) -> bool {
        !memory.is_tombstoned()
            && !memory.is_expired(now)
            && self.classifications.contains(&memory.classification)
            && !memory.classification.is_redacted()
            && memory.confidence_bps >= self.min_confidence_bps
    }
}

/// One retrieved private memory, ranked.
#[derive(Debug, Clone, PartialEq)]
pub struct RetrievedPrivateMemory {
    /// The full authoritative record, as the store holds it. Content comes
    /// from here — a derived vector row is never a content source.
    pub memory: AgentPrivateMemory,
    /// The retriever's deterministic relevance in basis points (0-10000);
    /// ties rank by ascending memory id.
    pub relevance_bps: u16,
    /// Metadata of the derived vector that ranked this memory, when the
    /// backend has one.
    pub embedding: Option<MemoryEmbeddingRef>,
}

/// What one retrieval returned.
#[derive(Debug, Clone, PartialEq)]
pub struct MemoryRetrievalOutcome {
    /// The ranked results, best first; ties by ascending memory id.
    pub memories: Vec<RetrievedPrivateMemory>,
    /// A bounded, content-free description of the index state the retrieval
    /// ranked against, when the backend reports one. It is recorded on the
    /// snapshot ([specification 13.5](../../../docs/plans/rakka-agent/spec.md):
    /// "embedding/index version or watermark when available"), truncated to
    /// [`AGENT_MEMORY_INDEX_WATERMARK_MAX_LENGTH`] at assembly.
    pub index_watermark: Option<String>,
}

/// The vendor-neutral private-memory retriever
/// ([specification 13.6](../../../docs/plans/rakka-agent/spec.md)).
///
/// Implementations rank an agent's private memories against a bounded query,
/// under the contract the query documents: every scope and query filter is
/// applied *before* ranking, a wrong-scope retrieval answers an empty outcome
/// byte-identical to an empty agent's (scenario 18), and an outage surfaces as
/// [`MemoryError::Backend`] — which the assembly path degrades on, because
/// memory is never a correctness source. The trait is object-safe so callers
/// hold `Arc<dyn AgentPrivateMemoryRetriever>`; the in-memory reference
/// implementation lives here, the pgvector adapter in `rakka-agent-postgres`.
///
/// # Scope isolation is the implementation's alone to enforce
///
/// [`assemble_context`] re-checks everything it can about a returned record —
/// record validity, classification, confidence, tombstone, expiry,
/// duplication — but it cannot re-check *scope*: an [`AgentPrivateMemory`]
/// carries no tenant or agent, so a record answered from the wrong scope is
/// indistinguishable from a correct one by the time the assembly sees it.
/// Answering only for the addressed [`AgentScope`] is therefore the one
/// contract clause no downstream layer can catch a violation of, and the one
/// an implementation must prove with its own tests (scenario 18).
pub trait AgentPrivateMemoryRetriever: Send + Sync + 'static {
    /// Stable backend name, used in telemetry and the snapshot's retrieval
    /// record.
    fn backend_name(&self) -> &'static str;

    /// The retriever version recorded in
    /// [`SnapshotRetrieval::retriever_version`], so an upgrade is an explicit
    /// change.
    fn retriever_version(&self) -> AgentRevisionNumber;

    /// Retrieves ranked private memories for one agent scope.
    fn retrieve<'a>(
        &'a self,
        scope: &'a AgentScope,
        query: &'a MemoryRetrievalQuery,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, MemoryRetrievalOutcome>;
}

/// The embedding seam a deployment supplies
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Rakka never ships a production embedder: the deployment binds one — a
/// provider client, a local model — and declares its identity through
/// [`Self::embedding_ref`]. An implementation must be deterministic for its
/// declared reference, and its output length must equal the declared
/// dimension count; adapters fail closed on a mismatch rather than indexing a
/// vector the metadata misdescribes.
pub trait AgentMemoryEmbedder: Send + Sync + 'static {
    /// The static identity of this embedder: model, dimensions, pipeline
    /// version.
    fn embedding_ref(&self) -> MemoryEmbeddingRef;

    /// Embeds one bounded text into a vector of exactly
    /// `embedding_ref().dimensions` finite values.
    fn embed<'a>(&'a self, text: &'a str) -> MemoryFuture<'a, Vec<f32>>;
}

/// The canonical text a memory's content is embedded from, shared by the
/// query and content sides so both vectors come from one deterministic
/// serialization.
///
/// Inline string content embeds as the raw string; other inline values embed
/// as their canonical JSON encoding (object keys sorted). Artifact-backed
/// content and the tombstone's null marker return `None` — this crate never
/// loads artifact bytes, so an artifact-backed memory is not semantically
/// indexable here and an indexer skips it visibly rather than embedding the
/// reference.
#[must_use]
pub fn memory_embedding_text(content: &AgentTaskContent) -> Option<String> {
    match content.inline_value() {
        Some(serde_json::Value::Null) | None => None,
        Some(serde_json::Value::String(text)) => Some(text.clone()),
        Some(value) => Some(value.to_string()),
    }
}

// ===========================================================================
// Query derivation and the retrieval policy.
// ===========================================================================

/// How the settle pass derives a retrieval from an assembled session window.
///
/// Every knob clamps to its bound; the defaults are the constants this module
/// declares. The policy rides the [`AgentMemoryRetrieval`] bundle, so a run
/// without retrieval carries nothing new.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryRetrievalPolicy {
    max_results: usize,
    max_bytes: usize,
    query_entries: usize,
    min_confidence_bps: u16,
    classifications: BTreeSet<MemoryClassification>,
}

impl MemoryRetrievalPolicy {
    /// The default policy: up to
    /// [`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES`] results within
    /// [`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES`], queries derived from the
    /// last [`AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES`] window entries,
    /// any confidence, unclassified content only.
    #[must_use]
    pub fn recent_context() -> Self {
        Self {
            max_results: AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES,
            max_bytes: AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES,
            query_entries: AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES,
            min_confidence_bps: 0,
            classifications: BTreeSet::from([MemoryClassification::Unclassified]),
        }
    }

    /// Sets how many results a retrieval may select, clamped to
    /// `1..=`[`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES`].
    #[must_use]
    pub fn with_max_results(mut self, max_results: usize) -> Self {
        self.max_results = max_results.clamp(1, AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES);
        self
    }

    /// Sets the selection's content byte budget, clamped to
    /// `1..=`[`AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES`].
    ///
    /// Selection *stops* at the first ranked record that would exceed the
    /// budget rather than skipping it, so the selection stays a rank prefix
    /// and a large record can leave the budget under-filled — see
    /// [`assemble_context`].
    #[must_use]
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes.clamp(1, AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES);
        self
    }

    /// Sets how many recent window entries the query derivation reads,
    /// clamped to `1..=`[`AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES`].
    #[must_use]
    pub fn with_query_entries(mut self, query_entries: usize) -> Self {
        self.query_entries = query_entries.clamp(1, AGENT_MEMORY_RETRIEVAL_QUERY_SOURCE_ENTRIES);
        self
    }

    /// Sets the minimum confidence a result must carry, clamped to 10000
    /// basis points.
    #[must_use]
    pub fn with_min_confidence_bps(mut self, min_confidence_bps: u16) -> Self {
        self.min_confidence_bps = min_confidence_bps.min(10_000);
        self
    }

    /// Sets the classifications a retrieval may select, under the same
    /// clamps as [`MemoryRetrievalQuery::with_classifications`].
    #[must_use]
    pub fn with_classifications(mut self, classifications: BTreeSet<MemoryClassification>) -> Self {
        let mut classifications = classifications;
        classifications.remove(&MemoryClassification::Redacted);
        if classifications.is_empty() {
            classifications.insert(MemoryClassification::Unclassified);
        }
        self.classifications = classifications;
        self
    }

    /// How many results a retrieval may select.
    #[must_use]
    pub const fn max_results(&self) -> usize {
        self.max_results
    }

    /// The selection's content byte budget.
    #[must_use]
    pub const fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// How many recent window entries the query derivation reads.
    #[must_use]
    pub const fn query_entries(&self) -> usize {
        self.query_entries
    }

    /// The minimum confidence a result must carry, in basis points.
    #[must_use]
    pub const fn min_confidence_bps(&self) -> u16 {
        self.min_confidence_bps
    }

    /// The classifications a retrieval may select.
    #[must_use]
    pub const fn classifications(&self) -> &BTreeSet<MemoryClassification> {
        &self.classifications
    }
}

impl Default for MemoryRetrievalPolicy {
    fn default() -> Self {
        Self::recent_context()
    }
}

/// Derives the retrieval query one snapshot's assembly runs, from the session
/// window that same assembly selected.
///
/// The derivation is a pure function of the window and the policy: the last
/// [`MemoryRetrievalPolicy::query_entries`] non-summary entries' inline text,
/// oldest first, joined by newlines, truncated to
/// [`AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES`] keeping the tail — the most
/// recent content. Reading the *assembled* window rather than the store is
/// what keeps the query deterministic under snapshot reuse: the window is
/// pinned by the first-writer-wins persist, so a re-assembly race resolves
/// exactly as scenario 17 already resolves session drift. `None` — retrieval
/// skipped — when the window holds no embeddable text.
#[must_use]
pub fn derive_retrieval_query(
    window: &[crate::memory::SnapshotSessionEntry],
    policy: &MemoryRetrievalPolicy,
) -> Option<MemoryRetrievalQuery> {
    let mut recent: Vec<&crate::memory::SnapshotSessionEntry> = window
        .iter()
        .rev()
        .filter(|entry| entry.role != MemoryEntryRole::Summary)
        .take(policy.query_entries())
        .collect();
    recent.reverse();

    let texts: Vec<String> = recent
        .iter()
        .filter_map(|entry| memory_embedding_text(&entry.content))
        .filter(|text| !text.is_empty())
        .collect();
    if texts.is_empty() {
        return None;
    }

    let mut text = texts.join("\n");
    if text.len() > AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES {
        let mut start = text.len() - AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES;
        while !text.is_char_boundary(start) {
            start += 1;
        }
        text = text[start..].to_string();
    }

    Some(
        MemoryRetrievalQuery::new(text)
            .with_limit(policy.max_results())
            .with_max_bytes(policy.max_bytes())
            .with_min_confidence_bps(policy.min_confidence_bps())
            .with_classifications(policy.classifications().clone()),
    )
}

// ===========================================================================
// The retrieval bundle a run's memory carries.
// ===========================================================================

/// The retrieval collaborators one deployment wires into
/// [`AgentRunMemory::with_retrieval`].
///
/// The guardrail chain is a *required* constructor argument: a wired
/// retriever always evaluates whatever chain it holds at the memory-ingress
/// boundary, and a deployment with no ingress stages says so explicitly with
/// an empty chain. An optional chain would be a fail-open — a retriever wired
/// without one would feed a model context no stage ever saw.
#[derive(Clone)]
pub struct AgentMemoryRetrieval {
    retriever: Arc<dyn AgentPrivateMemoryRetriever>,
    guardrails: AgentGuardrailChain,
    policy: MemoryRetrievalPolicy,
}

impl AgentMemoryRetrieval {
    /// Bundles a retriever with the memory-ingress guardrail chain it is
    /// evaluated under, and the default policy.
    #[must_use]
    pub fn new(
        retriever: Arc<dyn AgentPrivateMemoryRetriever>,
        guardrails: AgentGuardrailChain,
    ) -> Self {
        Self {
            retriever,
            guardrails,
            policy: MemoryRetrievalPolicy::recent_context(),
        }
    }

    /// Uses an explicit retrieval policy.
    #[must_use]
    pub fn with_policy(mut self, policy: MemoryRetrievalPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The retriever.
    #[must_use]
    pub fn retriever(&self) -> &dyn AgentPrivateMemoryRetriever {
        self.retriever.as_ref()
    }

    /// The memory-ingress guardrail chain.
    #[must_use]
    pub const fn guardrails(&self) -> &AgentGuardrailChain {
        &self.guardrails
    }

    /// The retrieval policy.
    #[must_use]
    pub const fn policy(&self) -> &MemoryRetrievalPolicy {
        &self.policy
    }
}

impl Debug for AgentMemoryRetrieval {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentMemoryRetrieval")
            .field("retriever", &self.retriever.backend_name())
            .field("guardrails", &self.guardrails.revision())
            .field("policy", &self.policy)
            .finish()
    }
}

// ===========================================================================
// The in-memory reference retriever.
// ===========================================================================

/// The in-memory reference retriever, for tests and single-process
/// deployments.
///
/// It ranks by scanning the authoritative store — bounded by
/// [`AGENT_MEMORY_RETRIEVAL_SCAN_MAX_ENTRIES`] — so scope isolation is the
/// store's own (scenario 18 comes free), and every query filter is applied
/// before ranking, exactly the contract a vector backend implements in SQL.
/// Without an embedder it scores by deterministic token overlap and returns
/// only overlapping memories; with one it scores by cosine similarity over
/// vectors embedded on the fly and returns every admitted candidate ranked,
/// the way a vector index would. It reports no index watermark, honestly:
/// there is no index.
#[derive(Clone)]
pub struct InMemoryPrivateMemoryRetriever {
    store: Arc<dyn AgentPrivateMemoryStore>,
    embedder: Option<Arc<dyn AgentMemoryEmbedder>>,
    version: AgentRevisionNumber,
}

impl InMemoryPrivateMemoryRetriever {
    /// A retriever over the given authoritative store, scoring by token
    /// overlap, at the initial retriever version.
    #[must_use]
    pub fn new(store: Arc<dyn AgentPrivateMemoryStore>) -> Self {
        Self {
            store,
            embedder: None,
            version: AgentRevisionNumber::INITIAL,
        }
    }

    /// Scores by cosine similarity over the embedder's vectors instead of
    /// token overlap.
    #[must_use]
    pub fn with_embedder(mut self, embedder: Arc<dyn AgentMemoryEmbedder>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// Uses an explicit retriever version, so a test can prove version drift
    /// cannot change a retried input.
    #[must_use]
    pub const fn with_version(mut self, version: AgentRevisionNumber) -> Self {
        self.version = version;
        self
    }

    /// Reads up to the scan bound of admitted live records.
    async fn scan(
        &self,
        scope: &AgentScope,
        query: &MemoryRetrievalQuery,
        now: AgentTimestampMillis,
    ) -> Result<Vec<AgentPrivateMemory>, MemoryError> {
        let mut admitted = Vec::new();
        let mut scanned = 0usize;
        let mut cursor =
            PrivateMemoryCursor::start().with_limit(AGENT_PRIVATE_MEMORY_PAGE_MAX_ENTRIES);
        loop {
            let page = self.store.list(scope, cursor, now).await?;
            scanned = scanned.saturating_add(page.memories.len());
            for memory in page.memories {
                if query.admits(&memory, now) {
                    admitted.push(memory);
                }
            }
            match page.next {
                Some(next) if scanned < AGENT_MEMORY_RETRIEVAL_SCAN_MAX_ENTRIES => cursor = next,
                _ => break,
            }
        }
        Ok(admitted)
    }
}

impl Debug for InMemoryPrivateMemoryRetriever {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryPrivateMemoryRetriever")
            .field("store", &self.store.backend_name())
            .field(
                "embedder",
                &self
                    .embedder
                    .as_ref()
                    .map(|embedder| embedder.embedding_ref().model),
            )
            .field("version", &self.version)
            .finish()
    }
}

impl AgentPrivateMemoryRetriever for InMemoryPrivateMemoryRetriever {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn retriever_version(&self) -> AgentRevisionNumber {
        self.version
    }

    fn retrieve<'a>(
        &'a self,
        scope: &'a AgentScope,
        query: &'a MemoryRetrievalQuery,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, MemoryRetrievalOutcome> {
        Box::pin(async move {
            let admitted = self.scan(scope, query, now).await?;

            let mut ranked: Vec<RetrievedPrivateMemory> = match &self.embedder {
                None => {
                    let query_tokens = tokenize(query.text());
                    admitted
                        .into_iter()
                        .filter_map(|memory| {
                            let text = memory_embedding_text(&memory.content)?;
                            let relevance_bps = token_overlap_bps(&query_tokens, &text);
                            (relevance_bps > 0).then_some(RetrievedPrivateMemory {
                                memory,
                                relevance_bps,
                                embedding: None,
                            })
                        })
                        .collect()
                }
                Some(embedder) => {
                    let reference = embedder.embedding_ref();
                    let query_vector = embed_checked(embedder.as_ref(), query.text()).await?;
                    let mut ranked = Vec::with_capacity(admitted.len());
                    for memory in admitted {
                        let Some(text) = memory_embedding_text(&memory.content) else {
                            continue;
                        };
                        let vector = embed_checked(embedder.as_ref(), &text).await?;
                        ranked.push(RetrievedPrivateMemory {
                            memory,
                            relevance_bps: cosine_relevance_bps(&query_vector, &vector),
                            embedding: Some(reference.clone()),
                        });
                    }
                    ranked
                }
            };

            ranked.sort_by(|a, b| {
                b.relevance_bps
                    .cmp(&a.relevance_bps)
                    .then_with(|| a.memory.memory_id.cmp(&b.memory.memory_id))
            });
            ranked.truncate(query.limit());

            Ok(MemoryRetrievalOutcome {
                memories: ranked,
                index_watermark: None,
            })
        })
    }
}

/// Embeds one text and fails closed on a vector the embedder's declared
/// reference misdescribes.
async fn embed_checked(
    embedder: &dyn AgentMemoryEmbedder,
    text: &str,
) -> Result<Vec<f32>, MemoryError> {
    let reference = embedder.embedding_ref();
    let vector = embedder.embed(text).await?;
    if vector.len() != reference.dimensions as usize {
        return Err(MemoryError::InvalidEmbeddingRef {
            message: format!(
                "the embedder {} declares {} dimensions but produced a vector of {}",
                reference.model,
                reference.dimensions,
                vector.len()
            ),
        });
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(MemoryError::InvalidEmbeddingRef {
            message: format!(
                "the embedder {} produced a non-finite vector component",
                reference.model
            ),
        });
    }
    Ok(vector)
}

/// Lowercased alphanumeric tokens of one text, deduplicated.
fn tokenize(text: &str) -> BTreeSet<String> {
    text.split(|character: char| !character.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// Deterministic token-overlap relevance: the fraction of query tokens the
/// text contains, in basis points.
fn token_overlap_bps(query_tokens: &BTreeSet<String>, text: &str) -> u16 {
    if query_tokens.is_empty() {
        return 0;
    }
    let text_tokens = tokenize(text);
    let overlap = query_tokens.intersection(&text_tokens).count();
    let bps = overlap.saturating_mul(10_000) / query_tokens.len();
    u16::try_from(bps.min(10_000)).unwrap_or(10_000)
}

/// Deterministic cosine relevance in basis points: `(cos + 1) / 2 · 10000`,
/// the same mapping a cosine-distance backend documents (`d = 1 − cos`, so
/// `(1 − d/2) · 10000`). A zero-magnitude vector on either side scores zero.
fn cosine_relevance_bps(a: &[f32], b: &[f32]) -> u16 {
    if a.len() != b.len() {
        return 0;
    }
    let mut dot = 0f64;
    let mut norm_a = 0f64;
    let mut norm_b = 0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += f64::from(*x) * f64::from(*y);
        norm_a += f64::from(*x) * f64::from(*x);
        norm_b += f64::from(*y) * f64::from(*y);
    }
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0;
    }
    let cosine = (dot / (norm_a.sqrt() * norm_b.sqrt())).clamp(-1.0, 1.0);
    let bps = ((cosine + 1.0) / 2.0 * 10_000.0).round();
    if bps <= 0.0 {
        0
    } else if bps >= 10_000.0 {
        10_000
    } else {
        // The clamp above bounds the value, so the cast is lossless.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        {
            bps as u16
        }
    }
}

// ===========================================================================
// Snapshot assembly with retrieval ([specification 13.5, 16]).
// ===========================================================================

/// A bounded telemetry summary of one assembly's retrieval; never a
/// correctness input.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetrievalReport {
    /// Whether a retrieval ran (a bundle was wired and a query derived).
    pub attempted: bool,
    /// Whether the retriever failed and the turn degraded to an empty
    /// selection.
    pub degraded: bool,
    /// How many memories the snapshot selected.
    pub selected: usize,
    /// How many returned records the fail-closed re-checks rejected.
    pub rejected: usize,
    /// How many records a memory-ingress stage blocked, including transforms
    /// the boundary cannot apply.
    pub blocked: usize,
    /// How many selections a memory-ingress stage transformed.
    pub transformed: usize,
    /// How many report-only findings the selections carry.
    pub reported: usize,
    /// How many records a `require-checkpoint` outcome dropped fail-closed.
    pub checkpoint_refused: usize,
}

impl RetrievalReport {
    /// The bounded outcome label for metrics: `skipped`, `degraded`, or
    /// `retrieved`.
    #[must_use]
    pub const fn outcome_label(&self) -> &'static str {
        if !self.attempted {
            "skipped"
        } else if self.degraded {
            "degraded"
        } else {
            "retrieved"
        }
    }
}

/// One assembled snapshot, with the retrieval report the caller feeds to
/// telemetry.
#[derive(Debug, Clone, PartialEq)]
pub struct AssembledContext {
    /// The assembled snapshot, ready to persist.
    pub snapshot: MemoryContextSnapshot,
    /// What the retrieval did.
    pub retrieval: RetrievalReport,
}

/// Assembles the immutable context snapshot one model turn is computed from,
/// private selections included.
///
/// This is the slice 1.11 path with the phase-2 private half filled in:
/// [`assemble_session_context`] builds the session window, and — when the
/// bundle carries an [`AgentMemoryRetrieval`] — the derived query is
/// retrieved, every returned record is re-checked fail-closed, evaluated at
/// the memory-ingress boundary, and embedded content-first into the
/// snapshot's private selections under the entry and byte bounds. It performs
/// no write: persistence stays the caller's, through the idempotent
/// first-writer-wins [`crate::memory::ContextSnapshotStore::persist`].
///
/// The re-checks cover every property a record carries; scope is the
/// exception, and stays the retriever's own obligation — see
/// [`AgentPrivateMemoryRetriever`].
///
/// A retriever error degrades the turn — empty selection, attempted retrieval
/// still recorded — instead of failing the assembly
/// ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)); a
/// session-store error propagates, as it always has.
///
/// # The selection is always a rank prefix
///
/// Selection walks the surviving ranked records in order and *stops* at the
/// first one that would exceed [`MemoryRetrievalPolicy::max_results`] or
/// [`MemoryRetrievalPolicy::max_bytes`], rather than skipping it to fit a
/// smaller record ranked below it. Skipping would make the selected set depend
/// on the byte sizes of the records it passed over, so two corpora that rank
/// identically could select different memories; stopping keeps the selection a
/// prefix of the ranking that survived the re-checks and the ingress boundary,
/// which is the property a reader of the snapshot can actually reason about.
/// The cost is deliberate: one large record can leave the byte budget
/// under-filled.
pub async fn assemble_context(
    memory: &AgentRunMemory,
    scope: &AgentRunScope,
    reference: &AgentContextSnapshotRef,
    turn: u64,
    policy_revision: AgentRevisionNumber,
    now: AgentTimestampMillis,
) -> Result<AssembledContext, MemoryError> {
    let mut snapshot = assemble_session_context(
        memory.session(),
        scope,
        reference,
        turn,
        memory.window(),
        policy_revision,
        now,
    )
    .await?;

    let Some(retrieval) = memory.retrieval() else {
        return Ok(AssembledContext {
            snapshot,
            retrieval: RetrievalReport::default(),
        });
    };
    let Some(query) = derive_retrieval_query(&snapshot.session, retrieval.policy()) else {
        return Ok(AssembledContext {
            snapshot,
            retrieval: RetrievalReport::default(),
        });
    };

    let mut report = RetrievalReport {
        attempted: true,
        ..RetrievalReport::default()
    };
    let agent_scope = scope.agent_scope();
    let retriever = retrieval.retriever();

    let outcome = match retriever.retrieve(&agent_scope, &query, now).await {
        Ok(outcome) => outcome,
        Err(_) => {
            // Memory is never the correctness source: the turn degrades to an
            // empty selection, permanently for this snapshot — determinism is
            // the stronger promise ([specification 13.1, 13.5]).
            report.degraded = true;
            snapshot.retrievals.push(SnapshotRetrieval {
                query: query.text().to_string(),
                retriever: retriever.backend_name().to_string(),
                retriever_version: retriever.retriever_version(),
                index_watermark: None,
            });
            snapshot.content_digest = snapshot.compute_digest();
            return Ok(AssembledContext {
                snapshot,
                retrieval: report,
            });
        }
    };

    let chain = retrieval.guardrails();
    let policy = retrieval.policy();
    let mut selections: Vec<SnapshotPrivateMemory> = Vec::new();
    let mut selected_bytes = 0usize;
    let mut seen: BTreeSet<AgentPrivateMemoryId> = BTreeSet::new();

    for retrieved in outcome.memories {
        if selections.len() >= policy.max_results() {
            break;
        }
        let memory_record = retrieved.memory;

        // Fail-closed re-checks: a record the query would not admit, an
        // invalid record, or a duplicate is rejected here even if the
        // retriever returned it. Scope is the one clause these cannot cover —
        // an `AgentPrivateMemory` carries no tenant or agent, so answering
        // only for the addressed scope stays the retriever's own obligation
        // (see `AgentPrivateMemoryRetriever`).
        if memory_record.validate().is_err()
            || !query.admits(&memory_record, now)
            || !seen.insert(memory_record.memory_id.clone())
        {
            report.rejected += 1;
            continue;
        }

        // The memory-ingress boundary: evaluated per record, on the content a
        // transform may rewrite, with identity in the context
        // ([specification 16](../../../docs/plans/rakka-agent/spec.md)).
        let Ok(content_value) = content_as_value(&memory_record.content) else {
            report.rejected += 1;
            continue;
        };
        let context = AgentGuardrailContext::new(AgentGuardrailBoundary::MemoryIngress, scope)
            .with_memory(&memory_record.memory_id);
        let decision = chain.evaluate_bounded(
            &context,
            &content_value,
            AGENT_PRIVATE_MEMORY_INLINE_MAX_BYTES,
        );

        match &decision.disposition {
            AgentGuardrailDisposition::Blocked { .. } => {
                report.blocked += 1;
                continue;
            }
            AgentGuardrailDisposition::CheckpointRequired { .. } => {
                // Fail-closed drop: there is no checkpoint plumbing at
                // snapshot assembly, and parking a model turn on a per-memory
                // grant would make memory a liveness gate
                // ([specification 13.1](../../../docs/plans/rakka-agent/spec.md)).
                report.checkpoint_refused += 1;
                continue;
            }
            AgentGuardrailDisposition::Allowed => {}
        }

        let (content, content_digest) = if decision.transformed {
            if memory_record.content.inline_value().is_none() {
                // A stage cannot rewrite an immutable artifact reference; the
                // deterministic outcome is a drop, the same rule the
                // model-request boundary applies to its synthetic descriptor
                // (`guardrail-transform-unsupported`).
                report.blocked += 1;
                continue;
            }
            let transformed = AgentTaskContent::Inline(decision.content.clone());
            let digest = transformed.digest();
            (transformed, digest)
        } else {
            (
                memory_record.content.clone(),
                memory_record.content_digest.clone(),
            )
        };

        let entry_bytes = content.size_bytes();
        if selected_bytes.saturating_add(entry_bytes) > policy.max_bytes() {
            break;
        }
        selected_bytes += entry_bytes;

        // Outcome accounting follows the budget check, so every count here
        // describes a record the snapshot actually embeds: a transform on a
        // record the budget then stopped at is not a transform the snapshot
        // carries.
        if decision.transformed {
            report.transformed += 1;
        }
        report.reported += decision.reports.len();

        selections.push(SnapshotPrivateMemory {
            memory_id: memory_record.memory_id.clone(),
            revision: memory_record.revision,
            kind: memory_record.kind,
            content,
            content_digest,
            classification: memory_record.classification,
            confidence_bps: memory_record.confidence_bps,
            relevance_bps: retrieved.relevance_bps.min(10_000),
            embedding: retrieved.embedding,
            transforms: decision
                .transforms
                .iter()
                .map(|transform| SnapshotIngressRecord {
                    stage: transform.stage.clone(),
                    revision: transform.revision,
                    reason_code: transform.reason_code.clone(),
                })
                .collect(),
            reports: decision
                .reports
                .iter()
                .map(|finding| SnapshotIngressRecord {
                    stage: finding.stage.clone(),
                    revision: finding.revision,
                    reason_code: finding.reason_code.clone(),
                })
                .collect(),
        });
    }

    report.selected = selections.len();
    snapshot.budget.private_memories = selections.len();
    snapshot.budget.private_memory_bytes = selected_bytes;
    snapshot.private_memory = selections;
    snapshot.ingress_revision = Some(chain.revision());
    snapshot.retrievals.push(SnapshotRetrieval {
        query: query.text().to_string(),
        retriever: retriever.backend_name().to_string(),
        retriever_version: retriever.retriever_version(),
        index_watermark: outcome.index_watermark.map(bounded_watermark),
    });
    snapshot.content_digest = snapshot.compute_digest();

    Ok(AssembledContext {
        snapshot,
        retrieval: report,
    })
}

/// The value the memory-ingress boundary evaluates: the inline content
/// itself, or the serialized artifact reference for reference content.
fn content_as_value(content: &AgentTaskContent) -> Result<serde_json::Value, MemoryError> {
    match content.inline_value() {
        Some(value) => Ok(value.clone()),
        None => content
            .artifact_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| MemoryError::Encoding {
                message: error.to_string(),
            })?
            .ok_or_else(|| MemoryError::Encoding {
                message: "the content carries neither an inline value nor an artifact reference"
                    .to_string(),
            }),
    }
}

/// Truncates an index watermark to its bound, deterministically, at a
/// character boundary.
fn bounded_watermark(watermark: String) -> String {
    if watermark.len() <= AGENT_MEMORY_INDEX_WATERMARK_MAX_LENGTH {
        return watermark;
    }
    let mut end = AGENT_MEMORY_INDEX_WATERMARK_MAX_LENGTH;
    while !watermark.is_char_boundary(end) {
        end -= 1;
    }
    watermark[..end].to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::definition::AgentGuardrailStageId;
    use crate::guardrails::{AgentGuardrail, AgentGuardrailOutcome, AgentGuardrailStage};
    use crate::identity::{AgentId, AgentRunId, TenantId};
    use crate::memory::{
        AgentPrivateMemoryKind, InMemoryAgentPrivateMemoryStore, InMemorySessionMemoryStore,
        MemoryEntryId, MemoryOperationId, MemoryTombstoneReason, PrivateMemoryExpectation,
        PrivateMemoryTombstoneRequest, SessionMemoryEntry, SnapshotSessionEntry,
    };
    use crate::testkit::{DeterministicEmbedder, ScriptedPrivateMemoryRetriever};

    fn run_scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support").expect("agent id"),
            AgentRunId::new("run-1").expect("run id"),
        )
        .expect("run scope")
    }

    fn window_entry(slot: &str, role: MemoryEntryRole, text: &str) -> SnapshotSessionEntry {
        let scope = run_scope();
        let content = AgentTaskContent::inline(json!(text)).expect("content");
        let digest = content.digest();
        SnapshotSessionEntry {
            entry_id: MemoryEntryId::derive(&scope, slot).expect("entry id"),
            sequence: crate::memory::MemorySequence::new(1),
            role,
            content,
            content_digest: digest,
            classification: MemoryClassification::Unclassified,
        }
    }

    fn private_memory(scope: &AgentScope, name: &str, text: &str) -> AgentPrivateMemory {
        AgentPrivateMemory::new(
            AgentPrivateMemoryId::new(format!("mem-{name}")).expect("memory id"),
            MemoryOperationId::derive_for_agent(scope, format!("create-{name}"))
                .expect("operation id"),
            AgentPrivateMemoryKind::Semantic,
            AgentTaskContent::inline(json!(text)).expect("content"),
            9_000,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(1),
        )
        .expect("the memory is bounded")
    }

    async fn seeded_store(
        scope: &AgentScope,
        memories: &[(&str, &str)],
    ) -> Arc<InMemoryAgentPrivateMemoryStore> {
        let store = Arc::new(InMemoryAgentPrivateMemoryStore::new());
        for (name, text) in memories {
            store
                .upsert(
                    scope,
                    &private_memory(scope, name, text),
                    PrivateMemoryExpectation::Absent,
                )
                .await
                .expect("seed upsert");
        }
        store
    }

    fn now() -> AgentTimestampMillis {
        AgentTimestampMillis::new(1_000)
    }

    // ------------------------------------------------------------------
    // Query derivation.
    // ------------------------------------------------------------------

    #[test]
    fn query_derivation_is_deterministic_and_skips_summaries() {
        let window = vec![
            window_entry("a", MemoryEntryRole::User, "renew the contract"),
            window_entry("b", MemoryEntryRole::Summary, "SUMMARIZED"),
            window_entry(
                "c",
                MemoryEntryRole::Assistant,
                "checking the renewal terms",
            ),
        ];
        let policy = MemoryRetrievalPolicy::recent_context().with_query_entries(2);

        let first = derive_retrieval_query(&window, &policy).expect("query derives");
        let second = derive_retrieval_query(&window, &policy).expect("query derives again");
        assert_eq!(first, second, "derivation is a pure function of the window");
        assert_eq!(
            first.text(),
            "renew the contract\nchecking the renewal terms"
        );
        assert!(
            !first.text().contains("SUMMARIZED"),
            "summaries never enter the query"
        );
    }

    #[test]
    fn query_derivation_keeps_the_tail_at_a_character_boundary() {
        let long = format!(
            "{}é-tail-end",
            "x".repeat(AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES)
        );
        let window = vec![window_entry("a", MemoryEntryRole::User, &long)];
        let query = derive_retrieval_query(&window, &MemoryRetrievalPolicy::recent_context())
            .expect("query derives");
        assert!(query.text().len() <= AGENT_MEMORY_RETRIEVAL_QUERY_MAX_BYTES);
        assert!(
            query.text().ends_with("-tail-end"),
            "the tail — the most recent content — is what survives truncation"
        );
    }

    #[test]
    fn query_derivation_skips_an_empty_window() {
        assert_eq!(
            derive_retrieval_query(&[], &MemoryRetrievalPolicy::recent_context()),
            None
        );
        // Artifact-backed content carries no embeddable text either.
        let artifact_only = vec![window_entry("a", MemoryEntryRole::User, "")];
        assert_eq!(
            derive_retrieval_query(&artifact_only, &MemoryRetrievalPolicy::recent_context()),
            None,
            "a window with no embeddable text derives no query"
        );
    }

    #[test]
    fn query_and_policy_knobs_clamp_instead_of_erroring() {
        let query = MemoryRetrievalQuery::new("q")
            .with_limit(0)
            .with_max_bytes(usize::MAX)
            .with_min_confidence_bps(u16::MAX)
            .with_classifications(std::collections::BTreeSet::from([
                MemoryClassification::Redacted,
            ]));
        assert_eq!(query.limit(), 1);
        assert_eq!(query.max_bytes(), AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_BYTES);
        assert_eq!(query.min_confidence_bps(), 10_000);
        assert_eq!(
            query.classifications(),
            &std::collections::BTreeSet::from([MemoryClassification::Unclassified]),
            "redacted is never eligible, and an empty remainder means the default"
        );

        let policy = MemoryRetrievalPolicy::recent_context()
            .with_max_results(usize::MAX)
            .with_query_entries(0);
        assert_eq!(
            policy.max_results(),
            AGENT_SNAPSHOT_PRIVATE_MEMORY_MAX_ENTRIES
        );
        assert_eq!(policy.query_entries(), 1);
    }

    #[test]
    fn embedding_text_is_canonical() {
        let text = AgentTaskContent::inline(json!("plain text")).expect("content");
        assert_eq!(memory_embedding_text(&text).as_deref(), Some("plain text"));

        let object = AgentTaskContent::inline(json!({"b": 2, "a": 1})).expect("content");
        assert_eq!(
            memory_embedding_text(&object).as_deref(),
            Some(r#"{"a":1,"b":2}"#),
            "object keys serialize sorted, so the embedding text is canonical"
        );

        let null = AgentTaskContent::inline(json!(null)).expect("content");
        assert_eq!(memory_embedding_text(&null), None);
    }

    // ------------------------------------------------------------------
    // The in-memory reference retriever.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn token_overlap_ranks_deterministically_with_identity_tiebreak() {
        let scope = run_scope().agent_scope();
        let store = seeded_store(
            &scope,
            &[
                ("full", "the renewal contract terms"),
                ("partial", "contract signature"),
                ("tie-b", "renewal"),
                ("tie-a", "renewal"),
                ("noise", "lunch menu"),
            ],
        )
        .await;
        let retriever = InMemoryPrivateMemoryRetriever::new(store);
        let query = MemoryRetrievalQuery::new("renewal contract terms");

        let outcome = retriever
            .retrieve(&scope, &query, now())
            .await
            .expect("retrieval");
        let names: Vec<&str> = outcome
            .memories
            .iter()
            .map(|retrieved| retrieved.memory.memory_id.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["mem-full", "mem-partial", "mem-tie-a", "mem-tie-b"],
            "ranked by overlap descending, ties by ascending memory id, zero overlap excluded"
        );
        assert_eq!(outcome.index_watermark, None, "no index, no watermark");
    }

    #[tokio::test]
    async fn filters_apply_before_ranking() {
        let scope = run_scope().agent_scope();
        let store = Arc::new(InMemoryAgentPrivateMemoryStore::new());
        let mut sensitive = private_memory(&scope, "sensitive", "renewal terms exactly");
        sensitive.classification = MemoryClassification::Sensitive;
        let mut hesitant = private_memory(&scope, "hesitant", "renewal terms exactly");
        hesitant.confidence_bps = 100;
        for memory in [
            &sensitive,
            &hesitant,
            &private_memory(&scope, "plain", "renewal terms"),
        ] {
            store
                .upsert(&scope, memory, PrivateMemoryExpectation::Absent)
                .await
                .expect("seed");
        }

        let retriever = InMemoryPrivateMemoryRetriever::new(store);
        let query = MemoryRetrievalQuery::new("renewal terms").with_min_confidence_bps(5_000);
        let outcome = retriever
            .retrieve(&scope, &query, now())
            .await
            .expect("retrieval");
        let names: Vec<&str> = outcome
            .memories
            .iter()
            .map(|retrieved| retrieved.memory.memory_id.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["mem-plain"],
            "a nearer sensitive or low-confidence record is filtered before ranking, \
             never merely outranked"
        );
    }

    #[tokio::test]
    async fn cross_scope_retrieval_is_byte_identical_to_an_empty_agent() {
        let owner = run_scope().agent_scope();
        let store = seeded_store(&owner, &[("secret", "the launch date is friday")]).await;
        let retriever = InMemoryPrivateMemoryRetriever::new(store);

        let sibling = AgentScope::new(
            TenantId::new("acme"),
            AgentId::new("billing").expect("agent id"),
        )
        .expect("scope");
        let foreign = AgentScope::new(
            TenantId::new("rival"),
            AgentId::new("support").expect("agent id"),
        )
        .expect("scope");
        let empty_agent = AgentScope::new(
            TenantId::new("acme"),
            AgentId::new("brand-new").expect("agent id"),
        )
        .expect("scope");

        let query = MemoryRetrievalQuery::new("launch date friday");
        let baseline = retriever
            .retrieve(&empty_agent, &query, now())
            .await
            .expect("empty-agent retrieval");
        for scope in [&sibling, &foreign] {
            let outcome = retriever
                .retrieve(scope, &query, now())
                .await
                .expect("cross-scope retrieval");
            assert_eq!(
                outcome, baseline,
                "a wrong-scope retrieval reveals nothing, not even existence"
            );
        }
    }

    #[tokio::test]
    async fn a_tombstoned_memory_is_never_retrieved() {
        let scope = run_scope().agent_scope();
        let store = seeded_store(&scope, &[("withdrawn", "renewal terms")]).await;
        store
            .tombstone(
                &scope,
                &PrivateMemoryTombstoneRequest {
                    memory_id: AgentPrivateMemoryId::new("mem-withdrawn").expect("id"),
                    operation_id: MemoryOperationId::derive_for_agent(&scope, "tombstone")
                        .expect("op id"),
                    reason: MemoryTombstoneReason::Retracted,
                    tombstoned_at: now(),
                },
            )
            .await
            .expect("tombstone");

        let retriever = InMemoryPrivateMemoryRetriever::new(store);
        let outcome = retriever
            .retrieve(&scope, &MemoryRetrievalQuery::new("renewal terms"), now())
            .await
            .expect("retrieval");
        assert!(outcome.memories.is_empty());
    }

    #[tokio::test]
    async fn an_embedder_ranks_by_cosine_and_a_dimension_mismatch_fails_closed() {
        let scope = run_scope().agent_scope();
        let store = seeded_store(
            &scope,
            &[
                ("near", "renewal contract terms"),
                ("far", "lunch menu soup"),
            ],
        )
        .await;
        let retriever = InMemoryPrivateMemoryRetriever::new(store.clone())
            .with_embedder(Arc::new(DeterministicEmbedder::new()));
        let outcome = retriever
            .retrieve(
                &scope,
                &MemoryRetrievalQuery::new("renewal contract terms"),
                now(),
            )
            .await
            .expect("retrieval");
        assert_eq!(
            outcome.memories[0].memory.memory_id.as_str(),
            "mem-near",
            "the cosine-nearest memory ranks first"
        );
        assert_eq!(
            outcome.memories[0]
                .embedding
                .as_ref()
                .map(|reference| reference.model.as_str()),
            Some(DeterministicEmbedder::MODEL),
            "the embedder's identity rides each retrieved item"
        );

        // An embedder whose output length contradicts its declaration fails
        // closed rather than ranking under a misdescribed geometry.
        struct LyingEmbedder;
        impl AgentMemoryEmbedder for LyingEmbedder {
            fn embedding_ref(&self) -> MemoryEmbeddingRef {
                MemoryEmbeddingRef {
                    model: "liar".to_string(),
                    dimensions: 8,
                    version: AgentRevisionNumber::INITIAL,
                }
            }
            fn embed<'a>(&'a self, _text: &'a str) -> MemoryFuture<'a, Vec<f32>> {
                Box::pin(async move { Ok(vec![1.0; 3]) })
            }
        }
        let lying =
            InMemoryPrivateMemoryRetriever::new(store).with_embedder(Arc::new(LyingEmbedder));
        let error = lying
            .retrieve(&scope, &MemoryRetrievalQuery::new("renewal"), now())
            .await
            .expect_err("the mismatch fails closed");
        assert_eq!(error.code(), "memory-embedding-invalid");
    }

    // ------------------------------------------------------------------
    // Snapshot assembly with retrieval.
    // ------------------------------------------------------------------

    /// One scripted memory-ingress rule.
    struct ScriptedRule(AgentGuardrailOutcome);
    impl AgentGuardrail for ScriptedRule {
        fn evaluate(
            &self,
            _context: &AgentGuardrailContext<'_>,
            _content: &serde_json::Value,
        ) -> AgentGuardrailOutcome {
            self.0.clone()
        }
    }

    /// A rule that blocks exactly one memory id.
    struct BlockOne(&'static str);
    impl AgentGuardrail for BlockOne {
        fn evaluate(
            &self,
            context: &AgentGuardrailContext<'_>,
            _content: &serde_json::Value,
        ) -> AgentGuardrailOutcome {
            if context
                .memory
                .is_some_and(|memory| memory.as_str() == self.0)
            {
                AgentGuardrailOutcome::Block {
                    reason_code: "scripted-block".to_string(),
                    evidence: None,
                }
            } else {
                AgentGuardrailOutcome::Allow
            }
        }
    }

    fn ingress_chain(outcome: AgentGuardrailOutcome) -> AgentGuardrailChain {
        ingress_chain_with(Arc::new(ScriptedRule(outcome)))
    }

    fn ingress_chain_with(rule: Arc<dyn AgentGuardrail>) -> AgentGuardrailChain {
        AgentGuardrailChain::new(AgentRevisionNumber::new(7))
            .with_stage(
                AgentGuardrailStage::new(
                    AgentGuardrailStageId::new("ingress-stage").expect("stage id"),
                    AgentRevisionNumber::new(2),
                    rule,
                )
                .at_boundary(AgentGuardrailBoundary::MemoryIngress),
            )
            .expect("the stage registers")
    }

    async fn memory_with_session(
        retrieval: Option<AgentMemoryRetrieval>,
    ) -> (AgentRunMemory, AgentRunScope) {
        let scope = run_scope();
        let session = Arc::new(InMemorySessionMemoryStore::new());
        let entry = SessionMemoryEntry::new(
            MemoryEntryId::derive(&scope, "turn-1").expect("entry id"),
            MemoryOperationId::derive(&scope, "turn-1").expect("op id"),
            crate::memory::MemorySequence::new(1),
            MemoryEntryRole::User,
            AgentTaskContent::inline(json!("what are the renewal contract terms"))
                .expect("content"),
            1,
            None,
            MemoryClassification::Unclassified,
            now(),
        )
        .expect("entry");
        crate::memory::SessionMemoryStore::append(session.as_ref(), &scope, &entry)
            .await
            .expect("append");

        let mut memory = AgentRunMemory::new(
            session,
            Arc::new(crate::memory::InMemoryContextSnapshotStore::new()),
        );
        if let Some(retrieval) = retrieval {
            memory = memory.with_retrieval(retrieval);
        }
        (memory, scope)
    }

    async fn assembled_with_chain(
        chain: AgentGuardrailChain,
        memories: &[(&str, &str)],
    ) -> AssembledContext {
        let scope = run_scope();
        let store = seeded_store(&scope.agent_scope(), memories).await;
        let retrieval =
            AgentMemoryRetrieval::new(Arc::new(InMemoryPrivateMemoryRetriever::new(store)), chain);
        let (memory, scope) = memory_with_session(Some(retrieval)).await;
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        assemble_context(
            &memory,
            &scope,
            &reference,
            1,
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("assembly")
    }

    #[tokio::test]
    async fn assembly_fills_selections_through_the_snapshot_path() {
        let assembled = assembled_with_chain(
            AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
            &[("terms", "the renewal contract terms are net-30")],
        )
        .await;
        let snapshot = &assembled.snapshot;

        assert_eq!(snapshot.private_memory.len(), 1);
        let selection = &snapshot.private_memory[0];
        assert_eq!(selection.memory_id.as_str(), "mem-terms");
        assert_eq!(selection.revision, AgentRevisionNumber::INITIAL);
        assert_eq!(selection.content_digest, selection.content.digest());
        assert_eq!(snapshot.budget.private_memories, 1);
        assert!(snapshot.budget.private_memory_bytes > 0);
        assert_eq!(
            snapshot.ingress_revision,
            Some(AgentRevisionNumber::INITIAL),
            "the chain revision the selection was evaluated under is recorded"
        );
        assert!(snapshot.is_untrusted());
        assert_eq!(snapshot.content_digest, snapshot.compute_digest());

        let retrieval = snapshot
            .retrievals
            .iter()
            .find(|retrieval| retrieval.retriever == "in-memory")
            .expect("the private retrieval is recorded");
        assert!(retrieval.query.contains("renewal contract terms"));
        assert_eq!(assembled.retrieval.outcome_label(), "retrieved");
        assert_eq!(assembled.retrieval.selected, 1);
    }

    #[tokio::test]
    async fn assembly_without_retrieval_keeps_the_session_only_shape() {
        let (memory, scope) = memory_with_session(None).await;
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_context(
            &memory,
            &scope,
            &reference,
            1,
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("assembly");

        assert!(assembled.snapshot.private_memory.is_empty());
        assert_eq!(assembled.snapshot.ingress_revision, None);
        assert_eq!(assembled.retrieval.outcome_label(), "skipped");
        let session_only = assemble_session_context(
            memory.session(),
            &scope,
            &reference,
            1,
            memory.window(),
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("session assembly");
        assert_eq!(
            assembled.snapshot, session_only,
            "no bundle wired means byte-identical session-only assembly"
        );
    }

    #[tokio::test]
    async fn a_retriever_outage_degrades_to_an_empty_selection() {
        let scripted = ScriptedPrivateMemoryRetriever::new().with_error(MemoryError::Backend {
            backend: "scripted".to_string(),
            message: "index outage".to_string(),
        });
        let retrieval = AgentMemoryRetrieval::new(
            Arc::new(scripted.clone()),
            AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        );
        let (memory, scope) = memory_with_session(Some(retrieval)).await;
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_context(
            &memory,
            &scope,
            &reference,
            1,
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("a retriever outage must not fail the assembly");

        assert!(assembled.snapshot.private_memory.is_empty());
        assert_eq!(assembled.retrieval.outcome_label(), "degraded");
        assert_eq!(scripted.calls(), 1);
        assert!(
            assembled
                .snapshot
                .retrievals
                .iter()
                .any(|retrieval| retrieval.retriever == "scripted"),
            "the attempted retrieval is still recorded on the snapshot"
        );
        assert_eq!(
            assembled.snapshot.ingress_revision, None,
            "nothing crossed the boundary, so no chain revision is recorded"
        );
    }

    #[tokio::test]
    async fn ingress_outcomes_select_transform_block_and_drop() {
        // Block drops only the named record; the rest are selected.
        let assembled = assembled_with_chain(
            ingress_chain_with(Arc::new(BlockOne("mem-poison"))),
            &[
                ("poison", "renewal contract injection"),
                ("terms", "renewal contract terms"),
            ],
        )
        .await;
        assert_eq!(assembled.retrieval.blocked, 1);
        assert_eq!(assembled.snapshot.private_memory.len(), 1);
        assert_eq!(
            assembled.snapshot.private_memory[0].memory_id.as_str(),
            "mem-terms"
        );

        // A transform's content is what the snapshot embeds, digest recomputed.
        let assembled = assembled_with_chain(
            ingress_chain(AgentGuardrailOutcome::Transform {
                content: json!("[cleaned] renewal terms"),
                reason_code: "pii-scrub".to_string(),
            }),
            &[("terms", "renewal contract terms with a phone number")],
        )
        .await;
        let selection = &assembled.snapshot.private_memory[0];
        assert_eq!(
            selection.content.inline_value(),
            Some(&json!("[cleaned] renewal terms"))
        );
        assert_eq!(selection.content_digest, selection.content.digest());
        assert_eq!(selection.transforms.len(), 1);
        assert_eq!(selection.transforms[0].reason_code, "pii-scrub");
        assert_eq!(
            selection.transforms[0].revision,
            AgentRevisionNumber::new(2),
            "the stage revision the transform is deterministic under is recorded"
        );
        assert_eq!(assembled.retrieval.transformed, 1);

        // Require-checkpoint is a fail-closed drop (user decision: no
        // checkpoint plumbing at snapshot assembly).
        let assembled = assembled_with_chain(
            ingress_chain(AgentGuardrailOutcome::RequireCheckpoint {
                reason_code: "needs-human".to_string(),
            }),
            &[("terms", "renewal contract terms")],
        )
        .await;
        assert!(assembled.snapshot.private_memory.is_empty());
        assert_eq!(assembled.retrieval.checkpoint_refused, 1);

        // Report-only selects and records the finding.
        let assembled = assembled_with_chain(
            ingress_chain(AgentGuardrailOutcome::ReportOnly {
                reason_code: "watchlist".to_string(),
                evidence: None,
            }),
            &[("terms", "renewal contract terms")],
        )
        .await;
        assert_eq!(assembled.snapshot.private_memory.len(), 1);
        assert_eq!(
            assembled.snapshot.private_memory[0].reports[0].reason_code,
            "watchlist"
        );
        assert_eq!(assembled.retrieval.reported, 1);
        assert_eq!(
            assembled.snapshot.ingress_revision,
            Some(AgentRevisionNumber::new(7)),
            "the evaluated chain's revision is recorded"
        );
    }

    #[tokio::test]
    async fn selection_bounds_stop_at_the_entry_limit() {
        let scope = run_scope();
        let store = seeded_store(
            &scope.agent_scope(),
            &[
                ("a", "renewal terms alpha"),
                ("b", "renewal terms beta"),
                ("c", "renewal terms gamma"),
            ],
        )
        .await;
        let retrieval = AgentMemoryRetrieval::new(
            Arc::new(InMemoryPrivateMemoryRetriever::new(store)),
            AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        )
        .with_policy(MemoryRetrievalPolicy::recent_context().with_max_results(2));
        let (memory, scope) = memory_with_session(Some(retrieval)).await;
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_context(
            &memory,
            &scope,
            &reference,
            1,
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("assembly");
        assert_eq!(
            assembled.snapshot.private_memory.len(),
            2,
            "the ranked selection stops at the policy's entry bound"
        );
    }

    #[tokio::test]
    async fn selection_stops_at_the_byte_budget_instead_of_skipping() {
        // The byte budget is a rank prefix, not a knapsack: the record that
        // overflows it ends the selection, and a smaller record ranked below
        // that one is not pulled up to fill the remaining budget. Skipping
        // would make the selected set depend on the sizes of the records it
        // passed over.
        let scope = run_scope().agent_scope();
        let head = private_memory(&scope, "alpha", "renewal contract terms");
        let overflowing = private_memory(&scope, "big", &"renewal contract terms ".repeat(20));
        let tail = private_memory(&scope, "gamma", "renewal contract terms");
        let head_bytes = head.content.size_bytes();
        let tail_bytes = tail.content.size_bytes();
        // A budget that fits the head and the tail together, but not the
        // record ranked between them.
        let budget = head_bytes + tail_bytes;
        assert!(
            head_bytes + overflowing.content.size_bytes() > budget,
            "the middle record must be the one that overflows the budget"
        );

        let outcome = MemoryRetrievalOutcome {
            memories: vec![
                RetrievedPrivateMemory {
                    memory: head,
                    relevance_bps: 9_000,
                    embedding: None,
                },
                RetrievedPrivateMemory {
                    memory: overflowing,
                    relevance_bps: 8_000,
                    embedding: None,
                },
                RetrievedPrivateMemory {
                    memory: tail,
                    relevance_bps: 7_000,
                    embedding: None,
                },
            ],
            index_watermark: None,
        };
        let retrieval = AgentMemoryRetrieval::new(
            Arc::new(ScriptedPrivateMemoryRetriever::new().with_outcome(outcome)),
            AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        )
        .with_policy(MemoryRetrievalPolicy::recent_context().with_max_bytes(budget));
        let (memory, scope) = memory_with_session(Some(retrieval)).await;
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_context(
            &memory,
            &scope,
            &reference,
            1,
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("assembly");

        assert_eq!(
            assembled
                .snapshot
                .private_memory
                .iter()
                .map(|selection| selection.memory_id.as_str())
                .collect::<Vec<_>>(),
            vec!["mem-alpha"],
            "the selection stopped at the overflowing record rather than skipping to mem-gamma"
        );
        assert_eq!(
            assembled.snapshot.budget.private_memory_bytes, head_bytes,
            "the budget accounts exactly the bytes the snapshot embeds"
        );
        assert_eq!(assembled.snapshot.budget.private_memories, 1);
        assert_eq!(assembled.retrieval.selected, 1);
    }

    #[tokio::test]
    async fn a_transform_the_byte_budget_stops_at_is_not_counted() {
        // Outcome accounting describes the snapshot: a record whose transform
        // ran but whose bytes the budget then refused is not a transform the
        // snapshot carries, so it must not be counted as one.
        let replacement = json!(format!("[cleaned] {}", "renewal terms ".repeat(20)));
        let transformed_bytes = AgentTaskContent::Inline(replacement.clone()).size_bytes();
        let scope = run_scope();
        let store = seeded_store(
            &scope.agent_scope(),
            // Identical text, so relevance ties and the rank order is the
            // documented ascending-memory-id tiebreak.
            &[
                ("alpha", "renewal contract terms"),
                ("beta", "renewal contract terms"),
            ],
        )
        .await;
        let retrieval = AgentMemoryRetrieval::new(
            Arc::new(InMemoryPrivateMemoryRetriever::new(store)),
            ingress_chain(AgentGuardrailOutcome::Transform {
                content: replacement,
                reason_code: "pii-scrub".to_string(),
            }),
        )
        // Room for exactly one transformed record.
        .with_policy(MemoryRetrievalPolicy::recent_context().with_max_bytes(transformed_bytes));
        let (memory, scope) = memory_with_session(Some(retrieval)).await;
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_context(
            &memory,
            &scope,
            &reference,
            1,
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("assembly");

        assert_eq!(assembled.snapshot.private_memory.len(), 1);
        assert_eq!(assembled.snapshot.private_memory[0].transforms.len(), 1);
        assert_eq!(
            assembled.retrieval.transformed, 1,
            "the second record's transform was refused by the budget, not embedded"
        );
        assert_eq!(assembled.retrieval.selected, 1);
    }

    #[tokio::test]
    async fn a_misbehaving_adapter_cannot_push_an_inadmissible_record() {
        // The scripted retriever returns a record the query never admits — a
        // sensitive classification under a default query — plus a duplicate;
        // the fail-closed re-checks reject both.
        let scope = run_scope().agent_scope();
        let mut sneaky = private_memory(&scope, "sneaky", "renewal contract terms");
        sneaky.classification = MemoryClassification::Sensitive;
        let honest = private_memory(&scope, "honest", "renewal contract terms");
        let outcome = MemoryRetrievalOutcome {
            memories: vec![
                RetrievedPrivateMemory {
                    memory: sneaky,
                    relevance_bps: 10_000,
                    embedding: None,
                },
                RetrievedPrivateMemory {
                    memory: honest.clone(),
                    relevance_bps: 9_000,
                    embedding: None,
                },
                RetrievedPrivateMemory {
                    memory: honest,
                    relevance_bps: 8_000,
                    embedding: None,
                },
            ],
            index_watermark: Some("w".repeat(AGENT_MEMORY_INDEX_WATERMARK_MAX_LENGTH * 2)),
        };
        let retrieval = AgentMemoryRetrieval::new(
            Arc::new(ScriptedPrivateMemoryRetriever::new().with_outcome(outcome)),
            AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        );
        let (memory, scope) = memory_with_session(Some(retrieval)).await;
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_context(
            &memory,
            &scope,
            &reference,
            1,
            AgentRevisionNumber::INITIAL,
            now(),
        )
        .await
        .expect("assembly");

        assert_eq!(assembled.snapshot.private_memory.len(), 1);
        assert_eq!(
            assembled.snapshot.private_memory[0].memory_id.as_str(),
            "mem-honest"
        );
        assert_eq!(assembled.retrieval.rejected, 2);
        let recorded = assembled
            .snapshot
            .retrievals
            .iter()
            .find(|retrieval| retrieval.retriever == "scripted")
            .expect("recorded retrieval");
        assert_eq!(
            recorded.index_watermark.as_ref().map(String::len),
            Some(AGENT_MEMORY_INDEX_WATERMARK_MAX_LENGTH),
            "the watermark is truncated to its bound"
        );
    }
}
