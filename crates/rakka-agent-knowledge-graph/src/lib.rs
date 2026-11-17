//! Database-agnostic communal knowledge graph for the Rakka agent domain
//! ([specification 13.4](../../docs/plans/rakka-agent/spec.md),
//! [13.6](../../docs/plans/rakka-agent/spec.md); slice 2.3 of the
//! implementation plan).
//!
//! Agents collaborating in a `(TenantId, KnowledgeSpaceId)` knowledge space
//! append provenance-bearing **claims** rather than overwriting canonical
//! facts; conflicting claims coexist under the
//! `Proposed`/`Verified`/`Disputed`/`Retracted` trust lattice, whose
//! transitions are append-only audit records. The
//! [`KnowledgeGraphStore`] SPI is portable by construction — no vendor
//! client, query language (SQL, Cypher, SPARQL), or vendor identifier appears
//! in any public type, and every implementation must pass the
//! [`conformance`] suite unchanged (scenario 20), which is what keeps claim
//! identity, idempotency, provenance, trust, and authorization semantics
//! backend-independent. Promotion of consequential claims to `Verified` is
//! gated through the slice 1.10 checkpoint grants of `rakka-agent`
//! ([`promotion`]).
//!
//! # Module map
//!
//! - [`scope`] — the `(TenantId, KnowledgeSpaceId)` scope every operation is
//!   addressed through (open decision 2: tenant/organization boundary, no
//!   implicit cross-tenant graph).
//! - [`claim`] — claim records, identities and derivations, statement shape,
//!   provenance, access, and the trust lattice (open decision 3: claims are
//!   born `Proposed`, structurally).
//! - [`transition`] — append-only trust-transition records, requests, and
//!   outcomes.
//! - [`tombstone`] — the record a claim carries when its content was
//!   withdrawn, and the stub's fixed markers.
//! - [`promotion`] — the HITL/policy promotion gate over
//!   `rakka-agent` checkpoint grants.
//! - [`retention`] — the per-space retention policy (a window per trust
//!   status, a tombstone window, a legal hold) and the bounded sweep that
//!   discharges it over any backend.
//! - [`search`] — the keyword search a store may offer: the query, the
//!   shared word rule, the score and the ranking order.
//! - [`semantic`] — the meaning-based search a store may offer: the text a
//!   claim is embedded from, the validated embedding, the query, and the one
//!   mapping from cosine similarity to a score.
//! - [`embedding_pass`] — the pass a deployment calls to embed the claims of
//!   a space that have no embedding yet.
//! - [`communal_retrieval`] — the communal read ports of `rakka-agent`,
//!   served from a store.
//! - [`store`] — the portable [`KnowledgeGraphStore`] SPI, bounded
//!   query/traversal types, capability reporting, and the in-memory
//!   reference implementation.
//! - [`error`] — the stable error vocabulary and the crate-owned schema
//!   window.
//! - [`conformance`] — the backend conformance harness (ungated test
//!   support, like `rakka_agent::testkit`).
//!
//! # Ownership boundary
//!
//! This crate depends on `rakka-agent` for identities, classification,
//! digests, and checkpoint grants; `rakka-agent` never depends back on it.
//! Concrete backend bindings (relational, property-graph, RDF, embedded, or
//! managed) live in separately versioned crates or application code
//! ([specification 19](../../docs/plans/rakka-agent/spec.md)); production
//! durability claims are never based on the in-memory implementation.
//!
//! # Reading claims into a model context
//!
//! [`communal_retrieval`] implements the two communal read ports
//! `rakka-agent` declares. [`KnowledgeGraphCommunalRetriever`] ranks a
//! space's claims by keyword [`search`](KnowledgeGraphStore::search), or,
//! given an embedder and a store that can, by meaning
//! ([`search_semantic`](KnowledgeGraphStore::search_semantic)), and
//! [`KnowledgeGraphCommunalAuthority`] answers the claim a ranking named.
//! Context assembly in `rakka-agent` decides on the record the authority
//! answers, which is where a claim's trust status, classification and
//! required read capabilities are enforced for a reading run. The store SPI
//! itself still enforces none of them on `get` or `query`: those are
//! operations an application calls, and the access set stays carried,
//! queryable data there.
//!
//! # Non-goals, still
//!
//! No metrics counters, no sharded knowledge-space entity, no agent-facing
//! erasure path (tombstone, delete and the retention sweep are operations an
//! application calls on the store; no run effect, command or tool reaches
//! them), no bulk or whole-space erasure, no approximate index and no
//! relevance floor for the meaning-based search, no loop that calls the
//! embedding pass, and no cross-tenant federation. A store never calls an
//! embedding model: the retriever, the append executor and the pass do, with
//! the embedder a deployment wires. The run-entity claim-append effect
//! this crate once deferred landed with scenario 33 ([`append_executor`]).

pub mod append_executor;
pub mod claim;
pub mod communal_retrieval;
pub mod conformance;
pub mod embedding_pass;
pub mod error;
pub mod goal_claim_source;
pub mod promotion;
pub mod retention;
pub mod scope;
pub mod search;
pub mod semantic;
pub mod store;
pub mod tombstone;
pub mod transition;

pub use append_executor::KnowledgeGraphClaimAppendExecutor;
pub use claim::{
    Claim, ClaimAccess, ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
    ClaimProvenance, ClaimRecord, ClaimTrustStatus, CLAIM_LIVE_SCHEMA_VERSION,
    CLAIM_MAX_ACL_CAPABILITIES, CLAIM_MAX_EVIDENCE_ARTIFACTS, CLAIM_MAX_TRUST_TRANSITIONS,
    CLAIM_OBJECT_INLINE_MAX_BYTES, CURRENT_CLAIM_SCHEMA_VERSION,
};
pub use communal_retrieval::{
    KnowledgeGraphCommunalAuthority, KnowledgeGraphCommunalRanking, KnowledgeGraphCommunalRetriever,
};
pub use embedding_pass::{embed_claims, ClaimEmbeddingPassPage};
pub use error::{check_schema_window, ClaimError, ClaimFuture, ClaimRecordKind, ClaimResult};
pub use goal_claim_source::KnowledgeGraphGoalClaimSource;
pub use promotion::{
    claim_promotion_binding, claim_promotion_effect_id, validate_promotion, ClaimPromotionEvidence,
    ClaimPromotionPolicy,
};
pub use retention::{
    discharge_knowledge_space_retention, KnowledgeSpaceRetentionPolicy,
    KnowledgeSpaceRetentionReport, KnowledgeSpaceRetentionSweep,
    KNOWLEDGE_SPACE_RETENTION_DEFAULT_MAX_BATCH,
};
pub use scope::KnowledgeSpaceScope;
pub use search::{
    claim_search_query_tokens, claim_search_relevance_bps, claim_search_tokens,
    rank_claim_search_hits, ClaimSearch, ClaimSearchHit, ClaimSearchReport, CLAIM_SEARCH_MAX_HITS,
    CLAIM_SEARCH_TEXT_MAX_BYTES,
};
pub use semantic::{
    claim_embedding_text, claim_semantic_distance_relevance_bps, claim_semantic_relevance_bps,
    ClaimEmbedding, ClaimEmbeddingPage, ClaimSemanticSearch, CLAIM_EMBEDDING_MAX_DIMENSIONS,
    CLAIM_EMBEDDING_PAGE_MAX_CLAIMS,
};
pub use store::{
    ClaimCursor, ClaimFilter, ClaimPage, ClaimTransitionCursor, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalDirection, ClaimTraversalReport, InMemoryKnowledgeGraphStore,
    KnowledgeGraphCapabilities, KnowledgeGraphCapability, KnowledgeGraphStore,
    CLAIM_PAGE_DEFAULT_LIMIT, CLAIM_PAGE_MAX_ENTRIES, CLAIM_TRANSITION_PAGE_MAX_ENTRIES,
    CLAIM_TRAVERSAL_MAX_DEPTH, CLAIM_TRAVERSAL_MAX_EDGES, CLAIM_TRAVERSAL_MAX_NODES,
};
pub use tombstone::{
    ClaimDeleteExpectation, ClaimDeleteRequest, ClaimTombstone, ClaimTombstoneReason,
    ClaimTombstoneRequest, CLAIM_TOMBSTONE_MARKER,
};
pub use transition::{
    ClaimPromotionReceipt, ClaimTransitionOutcome, ClaimTrustTransition,
    ClaimTrustTransitionRecord, ClaimTrustTransitionRequest, CLAIM_TRANSITION_REASON_MAX_LENGTH,
    CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION,
};
