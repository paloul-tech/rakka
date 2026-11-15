//! The shared memory conformance suite against PostgreSQL — unchanged.
//!
//! Specification: sections 13.1, 13.2, 13.3, 13.5, 13.6, and the retrieval
//! clauses of 16; scenarios 14, 15, 16, and 18. The calls below are the same
//! ones `crates/rakka-agent/tests/memory_store_contract.rs` makes against the
//! in-memory reference; the only difference is the backend handed in. That is
//! the point: before this slice each backend's semantics were asserted by a
//! separate hand-written copy, and two copies of one contract drift — the
//! security clauses being exactly the ones a drifted copy loses.
//!
//! Every test is gated on `RAKKA_POSTGRES_TEST_DSN` and passes silently
//! without it. Isolation between concurrent runs against one shared database
//! comes from the suite itself: `MemoryConformanceScopes::unique` mints
//! per-run-namespaced tenants, pinnable via
//! `RAKKA_AGENT_MEMORY_CONFORMANCE_RUN` when a failing run's rows need to be
//! found afterwards.
//!
//! # A skip is announced, and can be made a failure
//!
//! The three retriever clauses need the `vector` extension, which a stock
//! `postgres` image does not carry. A test that skipped and still reported
//! `ok` would be indistinguishable from one that ran — the exact shape this
//! slice exists to refuse elsewhere — so a skip prints the clauses it cost by
//! name, and `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` turns it into a failure for
//! a run that means to prove the retriever arm (CI, or a release check
//! against a `pgvector/pgvector` image).

use std::sync::Arc;

use rakka_agent::memory::{AgentPrivateMemory, MemoryFuture};
use rakka_agent::memory_conformance::{self, MemoryCorpusSeeder};
use rakka_agent::testkit::DeterministicEmbedder;
use rakka_agent::AgentScope;
use rakka_agent_postgres::retrieval::PgvectorPrivateMemoryRetriever;
use rakka_agent_postgres::{
    PostgresAgentPrivateMemoryStore, PostgresContextSnapshotStore, PostgresSessionMemoryStore,
};
use rakka_agent_workflow::AgentTimestampMillis;
use tokio_postgres::{Client, NoTls};

/// A shared client, or `None` when the DSN is unset.
async fn client() -> Option<Arc<Client>> {
    let dsn = std::env::var("RAKKA_POSTGRES_TEST_DSN").ok()?;
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("the PostgreSQL test database should connect");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres test connection error: {error}");
        }
    });
    Some(Arc::new(client))
}

async fn session() -> Option<PostgresSessionMemoryStore> {
    let store = PostgresSessionMemoryStore::from_shared_client(client().await?);
    store.migrate().await.expect("the schema applies");
    Some(store)
}

async fn snapshots() -> Option<PostgresContextSnapshotStore> {
    let store = PostgresContextSnapshotStore::from_shared_client(client().await?);
    store.migrate().await.expect("the schema applies");
    Some(store)
}

async fn private() -> Option<PostgresAgentPrivateMemoryStore> {
    let store = PostgresAgentPrivateMemoryStore::from_shared_client(client().await?);
    store.migrate().await.expect("the schema applies");
    Some(store)
}

/// The pgvector arm: an authoritative store, its retriever, and the seeder
/// that writes the vector row a scan-based backend does not need.
async fn retrieval() -> Option<(
    PostgresAgentPrivateMemoryStore,
    PgvectorPrivateMemoryRetriever,
    PgvectorSeeder,
)> {
    let shared = client().await?;
    let store = PostgresAgentPrivateMemoryStore::from_shared_client(shared.clone());
    store.migrate().await.expect("the schema applies");
    let retriever = PgvectorPrivateMemoryRetriever::from_shared_client(
        shared.clone(),
        Arc::new(DeterministicEmbedder::new()),
    );
    if let Err(error) = retriever.migrate().await {
        pgvector_unavailable(&error.to_string());
        return None;
    }
    let seeder = PgvectorSeeder {
        retriever: PgvectorPrivateMemoryRetriever::from_shared_client(
            shared,
            Arc::new(DeterministicEmbedder::new()),
        ),
    };
    Some((store, retriever, seeder))
}

/// Set to `1` to make a missing `vector` extension a failure rather than a
/// skip.
const PGVECTOR_REQUIRED_ENV: &str = "RAKKA_POSTGRES_PGVECTOR_REQUIRED";

/// Announces — or, when required, refuses — a run without the extension.
///
/// A skip that reported `ok` and printed nothing would be indistinguishable
/// from a clause that ran, which is how a suite quietly stops covering what
/// its name claims. This names the three clauses the skip costs.
fn pgvector_unavailable(detail: &str) {
    let message = format!(
        "the pgvector arm did not run: the `vector` extension is unavailable ({detail}). \
         Not exercised: retriever_scope_isolation, retriever_filters_before_ranking, \
         retriever_answers_authoritative_records. Use a pgvector-enabled image, or set \
         {PGVECTOR_REQUIRED_ENV}=1 to make this a failure."
    );
    assert!(
        std::env::var(PGVECTOR_REQUIRED_ENV).as_deref() != Ok("1"),
        "{message}"
    );
    eprintln!("SKIPPED: {message}");
}

/// Makes a memory rankable by writing its derived vector row.
struct PgvectorSeeder {
    retriever: PgvectorPrivateMemoryRetriever,
}

impl MemoryCorpusSeeder for PgvectorSeeder {
    fn index<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory: &'a AgentPrivateMemory,
    ) -> MemoryFuture<'a, ()> {
        Box::pin(async move {
            self.retriever
                .index_memory(scope, &memory.memory_id, AgentTimestampMillis::new(1))
                .await?;
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Session tier.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_replayed_session_append_creates_no_second_entry_when_dsn_is_set() {
    let Some(store) = session().await else {
        return;
    };
    memory_conformance::session_idempotent_append(&store).await;
}

#[tokio::test]
async fn a_tool_result_entry_keeps_its_tool_and_effect_provenance_when_dsn_is_set() {
    // The entry is JSON-encoded whole into `entry BYTEA`, so the two
    // provenance fields ride the existing column with no DDL; what this holds
    // is that the encoding neither drops nor reorders them.
    let Some(store) = session().await else {
        return;
    };
    memory_conformance::session_tool_provenance_round_trip(&store).await;
}

#[tokio::test]
async fn every_session_operation_isolates_when_dsn_is_set() {
    let Some(store) = session().await else {
        return;
    };
    memory_conformance::session_scope_isolation(&store).await;
}

#[tokio::test]
async fn session_retention_reports_every_outcome_when_dsn_is_set() {
    let Some(store) = session().await else {
        return;
    };
    memory_conformance::session_retention_purge(&store).await;
}

// ---------------------------------------------------------------------------
// Snapshot tier.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_persisted_snapshot_is_immutable_when_dsn_is_set() {
    let Some(store) = snapshots().await else {
        return;
    };
    memory_conformance::snapshot_immutability(&store).await;
}

#[tokio::test]
async fn every_snapshot_operation_isolates_when_dsn_is_set() {
    let Some(store) = snapshots().await else {
        return;
    };
    memory_conformance::snapshot_scope_isolation(&store).await;
}

// ---------------------------------------------------------------------------
// Private tier.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn private_writes_honour_their_expectations_when_dsn_is_set() {
    let Some(store) = private().await else {
        return;
    };
    memory_conformance::private_write_preconditions(&store).await;
}

#[tokio::test]
async fn a_withdrawal_erases_its_ledger_when_dsn_is_set() {
    let Some(store) = private().await else {
        return;
    };
    memory_conformance::private_tombstone_and_delete_erasure(&store).await;
}

#[tokio::test]
async fn every_private_operation_isolates_when_dsn_is_set() {
    let Some(store) = private().await else {
        return;
    };
    memory_conformance::private_scope_isolation(&store).await;
}

// ---------------------------------------------------------------------------
// The pgvector retriever.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_retrieval_outside_the_scope_reveals_nothing_when_dsn_is_set() {
    let Some((store, retriever, seeder)) = retrieval().await else {
        return;
    };
    memory_conformance::retriever_scope_isolation(&retriever, &store, &seeder).await;
}

/// The clause that would have caught slice 2.2's own pgvector defect: a filter
/// applied after the index's `LIMIT` answers a short page.
#[tokio::test]
async fn a_bounded_query_answers_a_full_page_of_admissible_records_when_dsn_is_set() {
    let Some((store, retriever, seeder)) = retrieval().await else {
        return;
    };
    memory_conformance::retriever_filters_before_ranking(&retriever, &store, &seeder).await;
}

#[tokio::test]
async fn a_ranked_record_matches_the_authoritative_one_when_dsn_is_set() {
    let Some((store, retriever, seeder)) = retrieval().await else {
        return;
    };
    memory_conformance::retriever_answers_authoritative_records(&retriever, &store, &seeder).await;
}

// ---------------------------------------------------------------------------
// The acceptance shape.
// ---------------------------------------------------------------------------

/// One call, zero agent-domain code — the shape slice 2.4 established.
#[tokio::test]
async fn the_whole_memory_contract_passes_unchanged_when_dsn_is_set() {
    let (Some(session), Some(snapshots)) = (session().await, snapshots().await) else {
        return;
    };
    match retrieval().await {
        Some((store, retriever, seeder)) => {
            memory_conformance::check_agent_memory_contract(
                &session, &snapshots, &store, &retriever, &seeder,
            )
            .await;
        }
        None => {
            // No pgvector. The three *store* umbrellas still run, which is
            // worth doing — but this is not the whole contract, and saying so
            // is the difference between a partial pass and a claimed one.
            let Some(store) = private().await else {
                return;
            };
            eprintln!(
                "PARTIAL: the whole-contract umbrella ran three of four tiers; the retriever \
                 tier needs the `vector` extension"
            );
            memory_conformance::check_session_memory_store_contract(&session).await;
            memory_conformance::check_context_snapshot_store_contract(&snapshots).await;
            memory_conformance::check_private_memory_store_contract(&store).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Issue #69: the snapshot store and the snapshot's two versions.
//
// A snapshot is version 2 exactly when a communal search ran. This store
// keeps a snapshot as its own encoding and checks its version on load, so no
// code here changed; these proofs hold that a version 2 snapshot round-trips
// through it and that a version 1 snapshot still loads.
// ---------------------------------------------------------------------------

/// A snapshot of one session entry in a scope of its own.
async fn session_snapshot(label: &str) -> rakka_agent::MemoryContextSnapshot {
    use rakka_agent::SessionMemoryStore;

    let scope = memory_conformance::MemoryConformanceScopes::unique(label).primary;
    let session = rakka_agent::InMemorySessionMemoryStore::new();
    let entry = rakka_agent::SessionMemoryEntry::new(
        rakka_agent::MemoryEntryId::derive(&scope, "turn-1-user").expect("the entry id derives"),
        rakka_agent::MemoryOperationId::derive(&scope, "turn-1-user")
            .expect("the operation id derives"),
        rakka_agent::MemorySequence::new(1),
        rakka_agent::MemoryEntryRole::User,
        rakka_agent::AgentTaskContent::inline(serde_json::json!({ "text": "where is my order" }))
            .expect("the content is bounded"),
        1,
        None,
        rakka_agent::MemoryClassification::Unclassified,
        AgentTimestampMillis::new(5),
    )
    .expect("the entry is bounded");
    session
        .append(&scope, &entry)
        .await
        .expect("the entry appends");
    let reference =
        rakka_agent::AgentContextSnapshotRef::for_turn(&scope, 1).expect("the reference derives");
    rakka_agent::assemble_session_context(
        &session,
        &scope,
        &reference,
        1,
        &rakka_agent::SessionWindowPolicy::recent_window(),
        rakka_agent::AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(10),
    )
    .await
    .expect("the snapshot assembles")
}

#[tokio::test]
async fn postgres_a_version_2_snapshot_persists_and_loads_when_dsn_is_set() {
    use rakka_agent::{ContextSnapshotStore, VersionedAgentRecord};

    let Some(store) = snapshots().await else {
        return;
    };
    let mut snapshot = session_snapshot("snapshot-version-2").await;
    let statement = rakka_agent::CommunalClaimStatement {
        subject: "order-42".to_string(),
        predicate: "status".to_string(),
        object: rakka_agent::CommunalClaimObject::Value(
            rakka_agent::AgentTaskContent::inline(serde_json::json!("delayed at customs"))
                .expect("the value is bounded"),
        ),
    };
    let space = rakka_agent::KnowledgeSpaceId::new("space-a").expect("the space id is valid");
    snapshot.set_communal(
        rakka_agent::SnapshotCommunalRetrieval {
            query: "where is my order".to_string(),
            retriever: "postgres".to_string(),
            retriever_version: rakka_agent::AgentRevisionNumber::INITIAL,
            index_watermark: None,
            spaces: vec![space.clone()],
            trust: [rakka_agent::AgentCommunalClaimTrust::Verified].into(),
            classifications: [rakka_agent::MemoryClassification::Unclassified].into(),
            min_confidence_bps: 0,
            index_incomplete: true,
            ingress_revision: Some(rakka_agent::AgentRevisionNumber::new(3)),
            definition_revision: rakka_agent::AgentRevisionNumber::new(2),
            setup_revision: None,
        },
        vec![rakka_agent::SnapshotCommunalClaim {
            claim_id: rakka_agent::AgentCommunalClaimId::new("claim-1")
                .expect("the claim id is valid"),
            space,
            trust: rakka_agent::AgentCommunalClaimTrust::Verified,
            statement_digest: statement.digest(),
            statement,
            provenance: rakka_agent::CommunalClaimProvenance {
                agent: rakka_agent::AgentId::new("scout").expect("the agent id is valid"),
                goal: None,
                task: None,
                run: None,
                delegation: None,
            },
            evidence: Vec::new(),
            created_at: AgentTimestampMillis::new(1),
            classification: rakka_agent::MemoryClassification::Unclassified,
            confidence_bps: 5_000,
            relevance_bps: 7_500,
            transforms: Vec::new(),
            reports: Vec::new(),
        }],
        64,
    );
    assert_eq!(snapshot.schema_version().get(), 2);

    let stored = store
        .persist(&snapshot)
        .await
        .expect("a version 2 snapshot persists");
    assert_eq!(stored, snapshot);
    let loaded = store
        .load(&snapshot.scope, &snapshot.reference)
        .await
        .expect("a version 2 snapshot loads")
        .expect("the snapshot is stored");
    assert_eq!(loaded, snapshot);
    assert_eq!(loaded.schema_version().get(), 2);
    assert_eq!(loaded.communal_claims.len(), 1);
    assert!(loaded
        .communal
        .is_some_and(|communal| communal.index_incomplete));
}

#[tokio::test]
async fn postgres_a_version_1_snapshot_still_loads_when_dsn_is_set() {
    use rakka_agent::{ContextSnapshotStore, VersionedAgentRecord};

    let Some(store) = snapshots().await else {
        return;
    };
    let snapshot = session_snapshot("snapshot-version-1").await;
    assert_eq!(snapshot.schema_version().get(), 1);

    store
        .persist(&snapshot)
        .await
        .expect("a version 1 snapshot persists");
    let loaded = store
        .load(&snapshot.scope, &snapshot.reference)
        .await
        .expect("a version 1 snapshot loads under a binary whose current version is 2")
        .expect("the snapshot is stored");
    assert_eq!(loaded, snapshot);
    assert_eq!(loaded.schema_version().get(), 1);
    assert!(loaded.communal.is_none());
}
