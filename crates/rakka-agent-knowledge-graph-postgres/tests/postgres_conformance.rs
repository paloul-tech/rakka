//! The shared conformance suite against the PostgreSQL backend — unchanged.
//!
//! Specification 13.6; slice 2.4. Scenario 20 is the acceptance gate: every
//! communal graph backend passes the same claim-identity, idempotent-append,
//! provenance, trust-filtering, authorization, and bounded-query conformance
//! suite without changing agent-domain code. The clauses below are byte-level
//! the same calls `crates/rakka-agent-knowledge-graph/tests/
//! knowledge_graph_conformance.rs` makes against the in-memory reference; the
//! only difference is the store handed in.
//!
//! Every test is gated on `RAKKA_POSTGRES_TEST_DSN` and passes silently
//! without it. Scope isolation between concurrent runs against one shared
//! database comes from the suite itself: `ConformanceScopes::unique` mints
//! per-run-namespaced tenants, pinnable via
//! `RAKKA_KNOWLEDGE_GRAPH_CONFORMANCE_RUN`.

use rakka_agent_knowledge_graph::conformance::{self, ConformanceScopes};
use rakka_agent_knowledge_graph_postgres::PostgresKnowledgeGraphStore;
use tokio_postgres::NoTls;

async fn store() -> Option<PostgresKnowledgeGraphStore> {
    let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
        Ok(dsn) => dsn,
        Err(_) => return None,
    };
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("the PostgreSQL test database should connect");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres test connection error: {error}");
        }
    });
    let store = PostgresKnowledgeGraphStore::new(client);
    store.migrate().await.expect("the schema applies");
    Some(store)
}

#[tokio::test]
async fn claim_identity_is_stable_and_reads_back_exactly_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::claim_identity(&store, ConformanceScopes::unique("pg-identity")).await;
}

#[tokio::test]
async fn scenario_16_replayed_graph_writes_are_idempotent_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::idempotent_append(&store, ConformanceScopes::unique("pg-scenario-16")).await;
}

#[tokio::test]
async fn provenance_survives_every_transition_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::provenance_preservation(&store, ConformanceScopes::unique("pg-provenance")).await;
}

#[tokio::test]
async fn queries_filter_by_trust_and_provenance_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::trust_filtering(&store, ConformanceScopes::unique("pg-trust")).await;
}

#[tokio::test]
async fn every_appended_claim_is_born_proposed_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::born_proposed(&store, ConformanceScopes::unique("pg-born-proposed")).await;
}

#[tokio::test]
async fn an_appended_claim_carries_the_identity_its_operation_derives_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::appended_identity_is_derived(
        &store,
        ConformanceScopes::unique("pg-derived-identity"),
    )
    .await;
}

#[tokio::test]
async fn scenario_18_unauthorized_graph_reads_do_not_reveal_existence_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::authorization_isolation(&store, ConformanceScopes::unique("pg-scenario-18")).await;
}

#[tokio::test]
async fn query_pages_are_bounded_and_cursor_walks_are_exact_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::bounded_queries(&store, ConformanceScopes::unique("pg-bounded-queries")).await;
}

#[tokio::test]
async fn traversal_is_bounded_deterministic_and_explicit_about_cuts_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::bounded_traversal(&store, ConformanceScopes::unique("pg-bounded-traversal")).await;
}

#[tokio::test]
async fn the_transition_table_holds_end_to_end_and_replays_converge_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::transition_legality_and_replay(&store, ConformanceScopes::unique("pg-legality"))
        .await;
}

#[tokio::test]
async fn the_promotion_gate_fails_closed_and_stamps_receipts_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::promotion_gate(&store, ConformanceScopes::unique("pg-gate")).await;
}

#[tokio::test]
async fn the_capability_report_is_coherent_when_dsn_is_set() {
    let Some(store) = store().await else { return };
    conformance::capability_report_coherence(&store).await;
}

#[tokio::test]
async fn scenario_20_the_whole_contract_passes_unchanged_when_dsn_is_set() {
    // The M2 acceptance gate: the umbrella the in-memory reference runs, on a
    // structurally different backend, with zero agent-domain change.
    let Some(store) = store().await else { return };
    conformance::check_knowledge_graph_contract(&store).await;
}
