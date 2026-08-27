//! The memory contract, run against the in-memory reference backends.
//!
//! Specification: sections 13.1, 13.2, 13.3, 13.5, 13.6, and the retrieval
//! clauses of 16; scenarios 14, 15, 16, and 18.
//!
//! Every clause lives in `rakka_agent::memory_conformance` so that the
//! PostgreSQL adapter runs the *same* suite unchanged — the shape slice 2.4
//! established for the knowledge graph, and the thing that stops two hand-
//! written copies of one contract drifting apart. Two copies is what existed
//! before this slice: the semantics were asserted once in `memory.rs`'s unit
//! tests and again, by hand, in `rakka-agent-postgres`.
//!
//! Why the existing proofs were not enough, specifically:
//!
//! - `memory.rs`'s `session_memory_is_isolated_by_agent_and_run` uses **one**
//!   tenant and asserts `is_empty()`. `is_empty()` is satisfied by a backend
//!   that answers "empty" *differently* from how it answers an unknown scope,
//!   which is exactly the existence disclosure section 13.1 forbids.
//! - `cross_scope_private_reads_reveal_nothing` reaches a second tenant but
//!   asserts `is_none()`/`is_empty()`, never exercises the write refusals or
//!   `purge_expired`, and never compares against a scope that is genuinely
//!   empty.
//! - The snapshot tier had no isolation test at all — and it is the tier where
//!   a keying bug is most plausible, because `persist` takes no scope argument
//!   and the record's own `scope` field is the whole fence.

use std::sync::Arc;

use rakka_agent::memory_conformance::{
    check_agent_memory_contract, check_context_snapshot_store_contract,
    check_private_memory_retriever_contract, check_private_memory_store_contract,
    check_session_memory_store_contract, private_scope_isolation,
    private_tombstone_and_delete_erasure, private_write_preconditions,
    retriever_answers_authoritative_records, retriever_filters_before_ranking,
    retriever_scope_isolation, session_idempotent_append, session_retention_purge,
    session_scope_isolation, snapshot_immutability, snapshot_scope_isolation,
    MemoryConformanceScopes, StoreOnlySeeder, MEMORY_UNSCOPED_METHODS,
};
use rakka_agent::{
    InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore, InMemoryPrivateMemoryRetriever,
    InMemorySessionMemoryStore,
};

fn session() -> InMemorySessionMemoryStore {
    InMemorySessionMemoryStore::new()
}

fn snapshots() -> InMemoryContextSnapshotStore {
    InMemoryContextSnapshotStore::new()
}

fn private() -> Arc<InMemoryAgentPrivateMemoryStore> {
    Arc::new(InMemoryAgentPrivateMemoryStore::new())
}

// ---------------------------------------------------------------------------
// Session tier.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_replayed_session_append_creates_no_second_entry() {
    session_idempotent_append(&session()).await;
}

#[tokio::test]
async fn every_session_operation_isolates_by_tenant_agent_and_run() {
    session_scope_isolation(&session()).await;
}

#[tokio::test]
async fn session_retention_reports_held_not_yet_due_and_purged() {
    session_retention_purge(&session()).await;
}

// ---------------------------------------------------------------------------
// Snapshot tier.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_persisted_snapshot_is_immutable() {
    snapshot_immutability(&snapshots()).await;
}

#[tokio::test]
async fn every_snapshot_operation_isolates_by_the_records_own_scope() {
    snapshot_scope_isolation(&snapshots()).await;
}

// ---------------------------------------------------------------------------
// Private tier.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn private_writes_honour_their_expectations() {
    private_write_preconditions(private().as_ref()).await;
}

#[tokio::test]
async fn a_withdrawal_erases_the_ledger_that_could_resurrect_it() {
    private_tombstone_and_delete_erasure(private().as_ref()).await;
}

#[tokio::test]
async fn every_private_operation_isolates_by_tenant_and_agent() {
    private_scope_isolation(private().as_ref()).await;
}

// ---------------------------------------------------------------------------
// Retriever.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_retrieval_outside_the_scope_reveals_nothing() {
    let store = private();
    retriever_scope_isolation(
        &InMemoryPrivateMemoryRetriever::new(store.clone()),
        store.as_ref(),
        &StoreOnlySeeder,
    )
    .await;
}

#[tokio::test]
async fn a_bounded_query_still_answers_a_full_page_of_admissible_records() {
    let store = private();
    retriever_filters_before_ranking(
        &InMemoryPrivateMemoryRetriever::new(store.clone()),
        store.as_ref(),
        &StoreOnlySeeder,
    )
    .await;
}

#[tokio::test]
async fn a_ranked_record_matches_the_authoritative_one() {
    let store = private();
    retriever_answers_authoritative_records(
        &InMemoryPrivateMemoryRetriever::new(store.clone()),
        store.as_ref(),
        &StoreOnlySeeder,
    )
    .await;
}

// ---------------------------------------------------------------------------
// The aggregates a backend crate actually calls.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn each_tier_passes_its_own_umbrella() {
    check_session_memory_store_contract(&session()).await;
    check_context_snapshot_store_contract(&snapshots()).await;
    check_private_memory_store_contract(private().as_ref()).await;
    let store = private();
    check_private_memory_retriever_contract(
        &InMemoryPrivateMemoryRetriever::new(store.clone()),
        store.as_ref(),
        &StoreOnlySeeder,
    )
    .await;
}

/// The acceptance shape: one call, zero domain code.
#[tokio::test]
async fn the_whole_memory_contract_passes_as_one_suite() {
    let store = private();
    check_agent_memory_contract(
        &session(),
        &snapshots(),
        store.as_ref(),
        &InMemoryPrivateMemoryRetriever::new(store.clone()),
        &StoreOnlySeeder,
    )
    .await;
}

// ---------------------------------------------------------------------------
// The scopes themselves.
// ---------------------------------------------------------------------------

/// The scope fixture is only as strong as its distinctness.
///
/// `foreign` must differ from `primary` in tenant, agent, *and* run at once —
/// a backend that fenced on only one of the three would otherwise pass by
/// accident — and `empty` must share the primary's tenant, or the comparison
/// would prove tenant isolation twice and scope isolation never.
#[test]
fn the_conformance_scopes_are_distinct_in_the_ways_the_clauses_need() {
    let scopes = MemoryConformanceScopes::unique("self-check");

    assert_ne!(scopes.primary.tenant(), scopes.foreign.tenant());
    assert_ne!(scopes.primary.agent(), scopes.foreign.agent());
    assert_ne!(scopes.primary.run(), scopes.foreign.run());

    assert_eq!(
        scopes.primary.tenant(),
        scopes.empty.tenant(),
        "the empty reference must share the primary's tenant"
    );
    assert_eq!(scopes.primary.agent(), scopes.sibling_run.agent());
    assert_ne!(scopes.primary.run(), scopes.sibling_run.run());
    assert_eq!(scopes.primary.tenant(), scopes.sibling_agent.tenant());
    assert_ne!(scopes.primary.agent(), scopes.sibling_agent.agent());

    // Two calls never collide, so clauses cannot pollute each other — which
    // matters most against a live database shared by a whole test binary.
    let other = MemoryConformanceScopes::unique("self-check");
    assert_ne!(scopes.primary.key(), other.primary.key());

    assert!(
        MEMORY_UNSCOPED_METHODS.contains(&"backend_name"),
        "the unscoped-method list must name what it excuses"
    );
}
