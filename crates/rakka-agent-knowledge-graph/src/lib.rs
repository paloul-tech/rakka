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
//! - [`promotion`] — the HITL/policy promotion gate over
//!   `rakka-agent` checkpoint grants.
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
//! # Non-goals of this slice
//!
//! No retrieval/context-snapshot path (the `MemoryContextSnapshot` communal
//! selection stays empty until communal retrieval lands), no run-entity
//! claim-append effect (scenario 33 binds at M4; the promotion binding is
//! derived so that effect adopts it unchanged), no metrics counters, no
//! sharded knowledge-space entity, no deletion path (`Retracted` is the
//! auditable withdrawal), no per-claim capability enforcement at read (the
//! access set is carried, queryable data; its enforcement point is the
//! communal retrieval and policy layer), and no cross-tenant federation.

pub mod append_executor;
pub mod claim;
pub mod conformance;
pub mod error;
pub mod goal_claim_source;
pub mod promotion;
pub mod scope;
pub mod store;
pub mod transition;

pub use append_executor::KnowledgeGraphClaimAppendExecutor;
pub use claim::{
    Claim, ClaimAccess, ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate,
    ClaimProvenance, ClaimRecord, ClaimTrustStatus, CLAIM_MAX_ACL_CAPABILITIES,
    CLAIM_MAX_EVIDENCE_ARTIFACTS, CLAIM_MAX_TRUST_TRANSITIONS, CLAIM_OBJECT_INLINE_MAX_BYTES,
    CURRENT_CLAIM_SCHEMA_VERSION,
};
pub use error::{check_schema_window, ClaimError, ClaimFuture, ClaimRecordKind, ClaimResult};
pub use goal_claim_source::KnowledgeGraphGoalClaimSource;
pub use promotion::{
    claim_promotion_binding, claim_promotion_effect_id, validate_promotion, ClaimPromotionEvidence,
    ClaimPromotionPolicy,
};
pub use scope::KnowledgeSpaceScope;
pub use store::{
    ClaimCursor, ClaimFilter, ClaimPage, ClaimTransitionCursor, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalDirection, ClaimTraversalReport, InMemoryKnowledgeGraphStore,
    KnowledgeGraphCapabilities, KnowledgeGraphCapability, KnowledgeGraphStore,
    CLAIM_PAGE_DEFAULT_LIMIT, CLAIM_PAGE_MAX_ENTRIES, CLAIM_TRANSITION_PAGE_MAX_ENTRIES,
    CLAIM_TRAVERSAL_MAX_DEPTH, CLAIM_TRAVERSAL_MAX_EDGES, CLAIM_TRAVERSAL_MAX_NODES,
};
pub use transition::{
    ClaimPromotionReceipt, ClaimTransitionOutcome, ClaimTrustTransition,
    ClaimTrustTransitionRecord, ClaimTrustTransitionRequest, CLAIM_TRANSITION_REASON_MAX_LENGTH,
    CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION,
};
