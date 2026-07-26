//! The conformance suite against the in-memory reference implementation.
//!
//! Specification 13.4/13.6; slice 2.3. The graph halves of scenarios 16 and
//! 18 are the `idempotent_append` and `authorization_isolation` clauses —
//! named so below. Slice 2.4 runs the same clauses, unchanged, against a
//! second structurally different backend (scenario 20).

use rakka_agent_knowledge_graph::conformance::{self, ConformanceScopes};
use rakka_agent_knowledge_graph::InMemoryKnowledgeGraphStore;

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
