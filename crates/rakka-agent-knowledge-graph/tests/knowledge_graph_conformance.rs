//! The conformance suite against the in-memory reference implementation.
//!
//! Specification 13.4/13.6; slice 2.3. The graph halves of scenarios 16 and
//! 18 are the `idempotent_append` and `authorization_isolation` clauses —
//! named so below. Slice 2.4 runs the same clauses, unchanged, against a
//! second structurally different backend (scenario 20).

use rakka_agent_knowledge_graph::conformance::{self, ConformanceScopes};
use rakka_agent_knowledge_graph::store::{
    ClaimCursor, ClaimFilter, ClaimPage, ClaimTransitionCursor, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalReport, KnowledgeGraphCapabilities,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimFuture, ClaimId, ClaimPromotionPolicy, ClaimTransitionOutcome,
    ClaimTrustTransitionRequest, InMemoryKnowledgeGraphStore, KnowledgeGraphStore,
    KnowledgeSpaceScope,
};
use rakka_agent_workflow::AgentTimestampMillis;

#[tokio::test]
async fn claim_identity_is_stable_and_reads_back_exactly() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::claim_identity(&store, ConformanceScopes::unique("identity")).await;
}

#[tokio::test]
async fn scenario_16_replayed_graph_writes_are_idempotent() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::idempotent_append(&store, ConformanceScopes::unique("scenario-16")).await;
}

#[tokio::test]
async fn provenance_survives_every_transition() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::provenance_preservation(&store, ConformanceScopes::unique("provenance")).await;
}

#[tokio::test]
async fn queries_filter_by_trust_and_provenance() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::trust_filtering(&store, ConformanceScopes::unique("trust")).await;
}

#[tokio::test]
async fn every_appended_claim_is_born_proposed() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::born_proposed(&store, ConformanceScopes::unique("born-proposed")).await;
}

#[tokio::test]
async fn scenario_18_unauthorized_graph_reads_do_not_reveal_existence() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::authorization_isolation(&store, ConformanceScopes::unique("scenario-18")).await;
}

#[tokio::test]
async fn query_pages_are_bounded_and_cursor_walks_are_exact() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::bounded_queries(&store, ConformanceScopes::unique("bounded-queries")).await;
}

#[tokio::test]
async fn traversal_is_bounded_deterministic_and_explicit_about_cuts() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::bounded_traversal(&store, ConformanceScopes::unique("bounded-traversal")).await;
}

#[tokio::test]
async fn the_transition_table_holds_end_to_end_and_replays_converge() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::transition_legality_and_replay(&store, ConformanceScopes::unique("legality"))
        .await;
}

#[tokio::test]
async fn the_promotion_gate_fails_closed_and_stamps_receipts() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::promotion_gate(&store, ConformanceScopes::unique("gate")).await;
}

#[tokio::test]
async fn the_capability_report_is_coherent() {
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::capability_report_coherence(&store).await;
}

#[tokio::test]
async fn the_whole_contract_passes_as_one_suite() {
    // Exactly what a slice 2.4 backend runs, unchanged.
    let store = InMemoryKnowledgeGraphStore::new();
    conformance::check_knowledge_graph_contract(&store).await;
}

/// Declared traversal depth: two, and honoured — the effective depth of any
/// request is the smaller of the request, this declaration, and the crate cap.
const SHALLOW_DECLARED_DEPTH: u32 = 2;

/// A conformant backend that declares — and serves — a depth tighter than the
/// crate cap, which the SPI explicitly permits.
///
/// It exists so the suite proves what it promises: a backend using the
/// tighter-declaration feature passes the same clauses unchanged. Every
/// operation but `traverse` delegates verbatim; `traverse` clamps to the
/// declaration, which is the whole point.
#[derive(Default)]
struct ShallowKnowledgeGraphStore {
    inner: InMemoryKnowledgeGraphStore,
}

impl KnowledgeGraphStore for ShallowKnowledgeGraphStore {
    fn backend_name(&self) -> &'static str {
        "in-memory-shallow"
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        KnowledgeGraphCapabilities::core().with_max_traversal_depth(SHALLOW_DECLARED_DEPTH)
    }

    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim> {
        self.inner.append(scope, claim)
    }

    fn get<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        self.inner.get(scope, claim_id)
    }

    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        self.inner.query(scope, filter, cursor)
    }

    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        let clamped = traversal
            .clone()
            .with_depth(traversal.depth().min(SHALLOW_DECLARED_DEPTH));
        Box::pin(async move { self.inner.traverse(scope, &clamped).await })
    }

    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        self.inner.transition(scope, request, policy, now)
    }

    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        self.inner.transitions(scope, claim_id, cursor)
    }
}

#[tokio::test]
async fn a_backend_declaring_a_tighter_traversal_depth_passes_the_contract() {
    let store = ShallowKnowledgeGraphStore::default();
    conformance::check_knowledge_graph_contract(&store).await;
}
