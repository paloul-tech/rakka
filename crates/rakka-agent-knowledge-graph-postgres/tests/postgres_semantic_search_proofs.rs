//! What only a live database can prove about claim embeddings (issue #140).
//!
//! The conformance suite proves the shared meaning-based search semantics on
//! this backend. This file proves what needs real rows, a real catalog and a
//! real *earlier binary*: that a store which does not opt in is untouched,
//! that opting in rewrites nothing, that everything an earlier build still
//! does to an opted-in table is safe, and, above all, that a claim's
//! embedding never outlives the claim's content, whichever binary withdraws
//! it and however late an embedding write arrives.
//!
//! The earlier binary is a store built **without** `with_semantic_search()`,
//! on a client of its own. That is exact, not an approximation: opting in
//! changes none of that store's statements, which the first test here holds
//! for the migration and the crate's review holds for the rest.
//!
//! Every test that reads a database is gated on `RAKKA_POSTGRES_TEST_DSN` and
//! passes silently without it. Every test that needs the `vector` extension
//! announces that it was skipped when the database cannot provide it, and
//! `RAKKA_POSTGRES_PGVECTOR_REQUIRED=1` turns that skip into a failure.

use std::collections::BTreeSet;
use std::sync::Arc;

use rakka_agent::{
    AgentCommunalClaimId, AgentCommunalClaimRetriever, AgentCommunalClaimTrust, AgentId,
    AgentMemoryEmbedder, AgentRevisionNumber, AgentTaskContent, CommunalRetrievalQuery,
    MemoryClassification, MemoryEmbeddingRef,
};
use rakka_agent_knowledge_graph::conformance::{
    conformance_resolver, ConceptEmbedder, ConformanceScopes,
};
use rakka_agent_knowledge_graph::{
    embed_claims, Claim, ClaimDeleteRequest, ClaimEmbedding, ClaimId, ClaimNodeId, ClaimObject,
    ClaimOperationId, ClaimPredicate, ClaimPromotionPolicy, ClaimProvenance, ClaimSearch,
    ClaimSemanticSearch, ClaimTombstoneReason, ClaimTombstoneRequest, ClaimTrustStatus,
    ClaimTrustTransitionRequest, KnowledgeGraphCapability, KnowledgeGraphCommunalRanking,
    KnowledgeGraphCommunalRetriever, KnowledgeGraphStore, KnowledgeSpaceScope,
};
use rakka_agent_knowledge_graph_postgres::{
    PostgresKnowledgeGraphStore, CLAIM_EMBEDDING_WRITE_SQL, EMBEDDING_CHECK_NAME,
    EMBEDDING_TRIGGER_NAME, MIGRATION_SQL, MIGRATION_VALIDATE_SQL,
};
use rakka_agent_workflow::AgentTimestampMillis;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls};

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);

/// Set to `1` to make a missing `vector` extension a failure rather than a
/// skip.
const PGVECTOR_REQUIRED_ENV: &str = "RAKKA_POSTGRES_PGVECTOR_REQUIRED";

/// Serialises this file's `CREATE EXTENSION` across its tests. Test-only, and
/// unrelated to the store's own migration lock.
const EXTENSION_LOCK_KEY: i64 = 982_451_929;

/// The four columns the semantic migration adds.
const EMBEDDING_COLUMNS: [&str; 4] = [
    "embedding",
    "embedding_model",
    "embedding_dimensions",
    "embedding_version",
];

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .expect("the PostgreSQL test database should connect");
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres test connection error: {error}");
        }
    });
    client
}

fn dsn() -> Option<String> {
    std::env::var("RAKKA_POSTGRES_TEST_DSN").ok()
}

async fn extension_available(dsn: &str) -> bool {
    connect(dsn)
        .await
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'vector') \
             AS available",
            &[],
        )
        .await
        .expect("the catalog reads")
        .get("available")
}

/// The DSN of a database that can provide the `vector` extension, with the
/// extension created in the default schema.
///
/// The extension is created here, from a session on the default search path,
/// before any private schema exists. Created first from a session whose path
/// leads with a private schema, it would live in that schema, and dropping
/// the schema would drop the extension under every other test.
///
/// Without the extension the test is announced as skipped, or refused when
/// [`PGVECTOR_REQUIRED_ENV`] is `1`: a skip that reported `ok` and printed
/// nothing would be indistinguishable from a proof that ran.
async fn pgvector_dsn(test: &str) -> Option<String> {
    let dsn = dsn()?;
    if !extension_available(&dsn).await {
        let message = format!(
            "{test} did not run: the `vector` extension is unavailable on this database. Use a \
             pgvector-enabled image, or set {PGVECTOR_REQUIRED_ENV}=1 to make this a failure."
        );
        assert!(
            std::env::var(PGVECTOR_REQUIRED_ENV).as_deref() != Ok("1"),
            "{message}"
        );
        eprintln!("SKIPPED: {message}");
        return None;
    }
    connect(&dsn)
        .await
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock({EXTENSION_LOCK_KEY}); \
             CREATE EXTENSION IF NOT EXISTS vector; COMMIT;"
        ))
        .await
        .expect("the extension is created in the default schema");
    Some(dsn)
}

fn unique_schema(label: &str) -> String {
    format!(
        "rakka_knowledge_graph_{label}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos()
    )
}

/// One private schema: the tables of a test live here and nowhere else.
///
/// Its sessions resolve names in the private schema first and in `public`
/// after it, because that is where the `vector` type and its operators live.
struct PrivateSchema {
    dsn: String,
    name: String,
    setup: Client,
}

impl PrivateSchema {
    async fn create(dsn: &str, label: &str) -> Self {
        let setup = connect(dsn).await;
        let name = unique_schema(label);
        setup
            .batch_execute(&format!("CREATE SCHEMA {name}"))
            .await
            .expect("the private schema is created");
        Self {
            dsn: dsn.to_string(),
            name,
            setup,
        }
    }

    /// A raw client in the schema.
    async fn client(&self) -> Client {
        let client = connect(&self.dsn).await;
        client
            .batch_execute(&format!("SET search_path TO {}, public", self.name))
            .await
            .expect("the session targets the private schema");
        client
    }

    /// A store that did not opt in: the earlier binary, exactly.
    async fn plain_store(&self) -> PostgresKnowledgeGraphStore {
        PostgresKnowledgeGraphStore::new(self.client().await)
    }

    /// A store that opted in, on a client of its own. Not yet migrated.
    async fn semantic_store(&self) -> PostgresKnowledgeGraphStore {
        PostgresKnowledgeGraphStore::new(self.client().await).with_semantic_search()
    }

    /// The schema migrated by a store that opted in.
    async fn migrated(dsn: &str, label: &str) -> (Self, Client, PostgresKnowledgeGraphStore) {
        let schema = Self::create(dsn, label).await;
        let store = schema.semantic_store().await;
        store.migrate().await.expect("the semantic schema applies");
        let raw = schema.client().await;
        (schema, raw, store)
    }

    async fn drop(self) {
        self.setup
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.name))
            .await
            .expect("the private schema is dropped");
    }
}

fn reference(version: u64) -> MemoryEmbeddingRef {
    MemoryEmbeddingRef {
        model: "proof-embedder".to_string(),
        dimensions: 3,
        version: AgentRevisionNumber::new(version),
    }
}

fn embedding(vector: [f32; 3]) -> ClaimEmbedding {
    ClaimEmbedding {
        reference: reference(1),
        vector: vector.to_vec(),
    }
}

fn text_claim(
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    text: &str,
    classification: MemoryClassification,
) -> Claim {
    Claim::new(
        scope,
        ClaimOperationId::derive_append(scope, discriminator).expect("the operation id derives"),
        ClaimNodeId::new("fleetnote").expect("the node id is valid"),
        ClaimPredicate::new("reports").expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::Value::String(text.to_string()))
                .expect("the value is inline-bounded"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        classification,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
}

async fn append(
    store: &PostgresKnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    text: &str,
) -> Claim {
    store
        .append(
            scope,
            &text_claim(
                scope,
                discriminator,
                text,
                MemoryClassification::Unclassified,
            ),
        )
        .await
        .expect("the claim appends")
}

fn tombstone_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
) -> ClaimTombstoneRequest {
    ClaimTombstoneRequest {
        claim_id: claim_id.clone(),
        operation_id: ClaimOperationId::derive_tombstone(scope, claim_id, discriminator)
            .expect("the operation id derives"),
        reason: ClaimTombstoneReason::Withdrawn,
        actor: conformance_resolver(),
        tombstoned_at: AgentTimestampMillis::new(20),
    }
}

/// A search toward `[1, 0, 0]` admitting fresh claims of `classifications`.
fn toward_first_axis(classifications: &[MemoryClassification]) -> ClaimSemanticSearch {
    ClaimSemanticSearch::new(
        ClaimSearch::new("", 64)
            .with_trust(BTreeSet::from([ClaimTrustStatus::Proposed]))
            .with_classifications(classifications.iter().copied().collect()),
        embedding([1.0, 0.0, 0.0]),
    )
}

fn unclassified() -> BTreeSet<MemoryClassification> {
    BTreeSet::from([MemoryClassification::Unclassified])
}

/// How many of the four embedding columns the claim table has.
async fn embedding_column_count(client: &Client) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pg_attribute \
             WHERE attrelid = 'rakka_agent_knowledge_claim'::regclass \
               AND attname = ANY($1) AND NOT attisdropped",
            &[&EMBEDDING_COLUMNS.to_vec()],
        )
        .await
        .expect("the catalog reads")
        .get(0)
}

async fn embedding_trigger_count(client: &Client) -> i64 {
    client
        .query_one(
            "SELECT count(*) FROM pg_trigger \
             WHERE tgrelid = 'rakka_agent_knowledge_claim'::regclass \
               AND tgname = $1 AND NOT tgisinternal",
            &[&EMBEDDING_TRIGGER_NAME],
        )
        .await
        .expect("the catalog reads")
        .get(0)
}

/// Whether the embedding check exists, and whether it is validated.
async fn embedding_check(client: &Client) -> Option<bool> {
    client
        .query_opt(
            "SELECT convalidated FROM pg_constraint \
             WHERE conrelid = 'rakka_agent_knowledge_claim'::regclass AND conname = $1",
            &[&EMBEDDING_CHECK_NAME],
        )
        .await
        .expect("the catalog reads")
        .map(|row| row.get(0))
}

/// Everything the semantic migration leaves in the catalog, to compare
/// before and after a second run.
async fn semantic_catalog(client: &Client) -> (Vec<(String, String)>, Option<bool>, i64) {
    let columns = client
        .query(
            "SELECT attname::TEXT, format_type(atttypid, atttypmod) FROM pg_attribute \
             WHERE attrelid = 'rakka_agent_knowledge_claim'::regclass \
               AND attname = ANY($1) AND NOT attisdropped ORDER BY attname",
            &[&EMBEDDING_COLUMNS.to_vec()],
        )
        .await
        .expect("the catalog reads")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    (
        columns,
        embedding_check(client).await,
        embedding_trigger_count(client).await,
    )
}

async fn claim_table_filenode(client: &Client) -> i64 {
    client
        .query_one(
            "SELECT relfilenode::BIGINT FROM pg_class \
             WHERE oid = 'rakka_agent_knowledge_claim'::regclass",
            &[],
        )
        .await
        .expect("the catalog reads")
        .get(0)
}

/// Whether one claim row holds anything in any of its four embedding columns.
async fn holds_embedding(raw: &Client, scope: &KnowledgeSpaceScope, claim_id: &ClaimId) -> bool {
    raw.query_one(
        "SELECT (embedding IS NOT NULL OR embedding_model IS NOT NULL \
                 OR embedding_dimensions IS NOT NULL OR embedding_version IS NOT NULL) AS held \
         FROM rakka_agent_knowledge_claim \
         WHERE tenant = $1 AND space = $2 AND claim_id = $3",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &claim_id.as_str(),
        ],
    )
    .await
    .expect("the row reads")
    .get("held")
}

/// Runs one raw assignment against one claim row.
async fn doctor(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    assignment: &str,
) -> Result<u64, tokio_postgres::Error> {
    raw.execute(
        &format!(
            "UPDATE rakka_agent_knowledge_claim SET {assignment} \
             WHERE tenant = $1 AND space = $2 AND claim_id = $3"
        ),
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &claim_id.as_str(),
        ],
    )
    .await
}

/// Writes a copy of `source`'s row under `raw_id` past the store, as a
/// later build with a wider id alphabet would: a claim id this build
/// refuses.
async fn plant_refused_claim_id(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    source: &ClaimId,
    raw_id: &str,
) {
    assert!(
        ClaimId::new(raw_id).is_err(),
        "{raw_id} is a claim id this build refuses"
    );
    let planted = raw
        .execute(
            "INSERT INTO rakka_agent_knowledge_claim \
                (tenant, space, claim_id, operation_id, subject, predicate, object_node, trust, \
                 transition_count, record, tombstoned, search_tokens, search_classification, \
                 search_confidence_bps, search_read_capabilities) \
             SELECT tenant, space, $3, operation_id || $3, subject, predicate, object_node, \
                    trust, transition_count, record, tombstoned, search_tokens, \
                    search_classification, search_confidence_bps, search_read_capabilities \
             FROM rakka_agent_knowledge_claim \
             WHERE tenant = $1 AND space = $2 AND claim_id = $4",
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &raw_id,
                &source.as_str(),
            ],
        )
        .await
        .expect("the row is planted");
    assert_eq!(planted, 1, "one copy of the source row is planted");
}

/// The claim ids of one space in the database's own order.
async fn claim_order(raw: &Client, scope: &KnowledgeSpaceScope) -> Vec<String> {
    raw.query(
        "SELECT claim_id FROM rakka_agent_knowledge_claim \
         WHERE tenant = $1 AND space = $2 ORDER BY claim_id",
        &[&scope.tenant().as_str(), &scope.space().as_str()],
    )
    .await
    .expect("the claim ids read")
    .iter()
    .map(|row| row.get::<_, String>("claim_id"))
    .collect()
}

const RAW_EMBEDDING: &str = "embedding = '[1,0,0]'::vector, embedding_model = 'proof-embedder', \
                             embedding_dimensions = 3, embedding_version = 1";

#[test]
fn postgres_the_base_migration_never_names_the_extension() {
    // A deployment that does not opt in must be able to run on a database
    // with no `vector` extension. The base migration therefore never names
    // the extension, its type, or the columns that hold one.
    for (name, sql) in [
        ("MIGRATION_SQL", MIGRATION_SQL),
        ("MIGRATION_VALIDATE_SQL", MIGRATION_VALIDATE_SQL),
    ] {
        let lowered = sql.to_lowercase();
        for forbidden in ["vector", "embedding"] {
            assert!(
                !lowered.contains(forbidden),
                "{name} names `{forbidden}`, which only the opt-in migration may"
            );
        }
    }
}

#[tokio::test]
async fn postgres_a_store_without_the_flag_adds_nothing_when_dsn_is_set() {
    let Some(dsn) = dsn() else { return };
    let schema = PrivateSchema::create(&dsn, "sem_plain").await;
    let store = schema.plain_store().await;
    store.migrate().await.expect("the base schema applies");
    let raw = schema.client().await;

    assert!(!store.serves_semantic_search());
    assert!(!store
        .capabilities()
        .supports(KnowledgeGraphCapability::SemanticSearch));
    assert_eq!(embedding_column_count(&raw).await, 0);
    assert_eq!(embedding_trigger_count(&raw).await, 0);
    assert_eq!(embedding_check(&raw).await, None);

    let scopes = ConformanceScopes::unique("pg-sem-plain");
    let claim = append(&store, &scopes.primary, "plain", "the car is here").await;
    assert_eq!(
        store
            .store_claim_embedding(
                &scopes.primary,
                &claim.claim_id,
                &embedding([1.0, 0.0, 0.0])
            )
            .await
            .expect_err("a store that did not opt in refuses an embedding")
            .code(),
        "claim-capability-unsupported"
    );
    schema.drop().await;
}

#[tokio::test]
async fn postgres_the_semantic_migration_fails_whole_without_the_extension_when_dsn_is_set() {
    let Some(dsn) = dsn() else { return };
    if extension_available(&dsn).await {
        eprintln!(
            "SKIPPED: postgres_the_semantic_migration_fails_whole_without_the_extension did not \
             run: this database can provide the `vector` extension, so the failure it proves \
             cannot be produced here."
        );
        return;
    }
    let schema = PrivateSchema::create(&dsn, "sem_absent").await;
    let raw = schema.client().await;

    let refused = schema
        .semantic_store()
        .await
        .migrate()
        .await
        .expect_err("opting in needs the extension");
    assert_eq!(refused.code(), "claim-backend-failed");
    // The base schema applied; of the opt-in migration, nothing did.
    assert_eq!(embedding_column_count(&raw).await, 0);
    assert_eq!(embedding_trigger_count(&raw).await, 0);

    // The failed migrator released the migration lock, and a store that does
    // not opt in serves the schema it left.
    let plain = schema.plain_store().await;
    plain
        .migrate()
        .await
        .expect("a store that does not opt in still migrates");
    let scopes = ConformanceScopes::unique("pg-sem-absent");
    append(&plain, &scopes.primary, "absent", "the car is here").await;
    schema.drop().await;
}

#[tokio::test]
async fn postgres_the_semantic_migration_rewrites_nothing_when_dsn_is_set() {
    let Some(dsn) = pgvector_dsn("postgres_the_semantic_migration_rewrites_nothing").await else {
        return;
    };
    let schema = PrivateSchema::create(&dsn, "sem_rewrite").await;
    let plain = schema.plain_store().await;
    plain.migrate().await.expect("the base schema applies");
    let raw = schema.client().await;
    let scopes = ConformanceScopes::unique("pg-sem-rewrite");
    let scope = &scopes.primary;
    let mut before = Vec::new();
    for index in 0..3 {
        before.push(append(&plain, scope, &format!("row-{index}"), "the car is here").await);
    }
    let filenode = claim_table_filenode(&raw).await;

    let semantic = schema.semantic_store().await;
    semantic
        .migrate()
        .await
        .expect("the semantic schema applies");
    assert_eq!(
        claim_table_filenode(&raw).await,
        filenode,
        "opting in must not rewrite the claim table"
    );
    for claim in &before {
        assert_eq!(
            semantic
                .get(scope, &claim.claim_id)
                .await
                .expect("the claim reads")
                .as_ref(),
            Some(claim)
        );
        assert!(!holds_embedding(&raw, scope, &claim.claim_id).await);
    }
    let catalog = semantic_catalog(&raw).await;
    assert_eq!(
        catalog,
        (
            vec![
                ("embedding".to_string(), "vector".to_string()),
                ("embedding_dimensions".to_string(), "integer".to_string()),
                ("embedding_model".to_string(), "text".to_string()),
                ("embedding_version".to_string(), "bigint".to_string()),
            ],
            Some(true),
            1
        ),
        "four nullable columns, a validated check and one trigger"
    );

    // A second run, and a run by a store that did not opt in, change nothing.
    semantic.migrate().await.expect("the migration repeats");
    plain.migrate().await.expect("the base migration repeats");
    assert_eq!(semantic_catalog(&raw).await, catalog);
    assert_eq!(claim_table_filenode(&raw).await, filenode);
    schema.drop().await;
}

#[tokio::test]
async fn postgres_an_earlier_build_still_works_on_the_migrated_table_when_dsn_is_set() {
    let Some(dsn) =
        pgvector_dsn("postgres_an_earlier_build_still_works_on_the_migrated_table").await
    else {
        return;
    };
    let (schema, raw, semantic) = PrivateSchema::migrated(&dsn, "sem_earlier").await;
    let earlier = schema.plain_store().await;
    let scopes = ConformanceScopes::unique("pg-sem-earlier");
    let scope = &scopes.primary;

    // Append, read, search by word, transition, tombstone and delete: every
    // statement of a build that names none of the new columns.
    let kept = append(&earlier, scope, "kept", "the car is here").await;
    let withdrawn = append(&earlier, scope, "withdrawn", "the boat is here").await;
    let removed = append(&earlier, scope, "removed", "the plane is here").await;
    assert_eq!(
        earlier
            .get(scope, &kept.claim_id)
            .await
            .expect("the claim reads"),
        Some(kept.clone())
    );
    let found = earlier
        .search(
            scope,
            &ClaimSearch::new("car", 8)
                .with_trust(BTreeSet::from([ClaimTrustStatus::Proposed]))
                .with_classifications(unclassified()),
        )
        .await
        .expect("the word search answers");
    assert_eq!(found.hits.len(), 1);
    earlier
        .transition(
            scope,
            &ClaimTrustTransitionRequest::new(
                kept.claim_id.clone(),
                ClaimOperationId::derive_transition(scope, &kept.claim_id, "verify")
                    .expect("the operation id derives"),
                ClaimTrustStatus::Verified,
                conformance_resolver(),
                NOW,
            ),
            &ClaimPromotionPolicy::ungated(),
            NOW,
        )
        .await
        .expect("the transition applies");
    earlier
        .tombstone(scope, &tombstone_request(scope, &withdrawn.claim_id, "t"))
        .await
        .expect("the tombstone applies");
    earlier
        .delete(
            scope,
            &ClaimDeleteRequest {
                claim_id: removed.claim_id.clone(),
                operation_id: ClaimOperationId::derive_delete(scope, &removed.claim_id, "d")
                    .expect("the operation id derives"),
                expected: None,
            },
        )
        .await
        .expect("the delete applies");

    // A claim the earlier build appended has no embedding, and is owed one.
    assert!(!holds_embedding(&raw, scope, &kept.claim_id).await);
    let owed = semantic
        .unembedded_claims(scope, &reference(1), &unclassified(), None, 64)
        .await
        .expect("the page answers");
    assert_eq!(
        owed.claims
            .iter()
            .map(|claim| &claim.claim_id)
            .collect::<Vec<_>>(),
        vec![&kept.claim_id]
    );
    schema.drop().await;
}

#[tokio::test]
async fn postgres_an_earlier_tombstone_clears_the_embedding_when_dsn_is_set() {
    let Some(dsn) = pgvector_dsn("postgres_an_earlier_tombstone_clears_the_embedding").await else {
        return;
    };
    let (schema, raw, semantic) = PrivateSchema::migrated(&dsn, "sem_tombstone").await;
    let earlier = schema.plain_store().await;
    let scopes = ConformanceScopes::unique("pg-sem-tombstone");
    let scope = &scopes.primary;
    let mut claims = Vec::new();
    for discriminator in ["by-earlier-store", "by-raw-update", "by-this-store"] {
        let claim = append(&semantic, scope, discriminator, "the car is here").await;
        assert!(semantic
            .store_claim_embedding(scope, &claim.claim_id, &embedding([1.0, 0.0, 0.0]))
            .await
            .expect("the embedding stores"));
        assert!(holds_embedding(&raw, scope, &claim.claim_id).await);
        claims.push(claim);
    }

    // The earlier build's tombstone statement names none of the columns.
    earlier
        .tombstone(scope, &tombstone_request(scope, &claims[0].claim_id, "t"))
        .await
        .expect("the tombstone applies");
    assert!(
        !holds_embedding(&raw, scope, &claims[0].claim_id).await,
        "an earlier build's tombstone clears the embedding through the trigger"
    );

    // The least an earlier statement could do: flip the column and nothing
    // else.
    doctor(&raw, scope, &claims[1].claim_id, "tombstoned = TRUE")
        .await
        .expect("the raw update applies");
    assert!(!holds_embedding(&raw, scope, &claims[1].claim_id).await);

    // This build's own tombstone.
    semantic
        .tombstone(scope, &tombstone_request(scope, &claims[2].claim_id, "t"))
        .await
        .expect("the tombstone applies");
    assert!(!holds_embedding(&raw, scope, &claims[2].claim_id).await);

    // Nothing can write an embedding back onto a tombstoned row, whatever
    // statement tries.
    for claim in &claims {
        doctor(&raw, scope, &claim.claim_id, RAW_EMBEDDING)
            .await
            .expect("the raw update is accepted and cleared");
        assert!(
            !holds_embedding(&raw, scope, &claim.claim_id).await,
            "a tombstoned row never holds an embedding"
        );
    }
    schema.drop().await;
}

#[tokio::test]
async fn postgres_a_tombstone_beats_a_late_embedding_when_dsn_is_set() {
    let Some(dsn) = pgvector_dsn("postgres_a_tombstone_beats_a_late_embedding").await else {
        return;
    };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "sem_late").await;
    let scopes = ConformanceScopes::unique("pg-sem-late");
    let scope = &scopes.primary;
    // The pass read this claim as live and owed an embedding...
    let claim = append(&store, scope, "late", "the car is here").await;
    let listed = store
        .unembedded_claims(scope, &reference(1), &unclassified(), None, 8)
        .await
        .expect("the page answers");
    assert_eq!(listed.claims.len(), 1);
    // ...and a tombstone committed before the pass wrote.
    store
        .tombstone(scope, &tombstone_request(scope, &claim.claim_id, "t"))
        .await
        .expect("the tombstone applies");

    // The write statement itself refuses the row.
    let params: [&(dyn ToSql + Sync); 7] = [
        &scope.tenant().as_str(),
        &scope.space().as_str(),
        &claim.claim_id.as_str(),
        &"[1,0,0]",
        &"proof-embedder",
        &3_i32,
        &1_i64,
    ];
    assert_eq!(
        raw.execute(CLAIM_EMBEDDING_WRITE_SQL, &params)
            .await
            .expect("the write statement runs"),
        0,
        "the embedding write changes no tombstoned row"
    );
    assert!(!store
        .store_claim_embedding(scope, &claim.claim_id, &embedding([1.0, 0.0, 0.0]))
        .await
        .expect("a withdrawn claim is an answer, not an error"));
    assert!(!holds_embedding(&raw, scope, &claim.claim_id).await);
    schema.drop().await;
}

#[tokio::test]
async fn postgres_a_drifted_filter_column_cannot_leak_a_claim_when_dsn_is_set() {
    let Some(dsn) = pgvector_dsn("postgres_a_drifted_filter_column_cannot_leak_a_claim").await
    else {
        return;
    };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "sem_drift").await;
    let scopes = ConformanceScopes::unique("pg-sem-drift");
    let scope = &scopes.primary;
    let sensitive = store
        .append(
            scope,
            &text_claim(
                scope,
                "sensitive",
                "the sensitive car",
                MemoryClassification::Sensitive,
            ),
        )
        .await
        .expect("the claim appends");
    assert!(store
        .store_claim_embedding(scope, &sensitive.claim_id, &embedding([1.0, 0.0, 0.0]))
        .await
        .expect("the embedding stores"));
    // An honest column: a search that admits only `Unclassified` finds
    // nothing.
    assert!(store
        .search_semantic(
            scope,
            &toward_first_axis(&[MemoryClassification::Unclassified])
        )
        .await
        .expect("the search answers")
        .hits
        .is_empty());

    // The column now claims the record is `Unclassified`.
    doctor(
        &raw,
        scope,
        &sensitive.claim_id,
        "search_classification = 'unclassified'",
    )
    .await
    .expect("the raw update applies");
    let refused = store
        .search_semantic(
            scope,
            &toward_first_axis(&[MemoryClassification::Unclassified]),
        )
        .await
        .expect_err("a row its record does not admit is drift, never a hit");
    assert_eq!(refused.code(), "claim-backend-failed");
    assert!(
        !refused.to_string().contains("sensitive car"),
        "the refusal carries no content: {refused}"
    );

    // The same drift must not hand the claim's text to an embedder either:
    // the page of claims owed an embedding lists by the record.
    doctor(
        &raw,
        scope,
        &sensitive.claim_id,
        "embedding = NULL, embedding_model = NULL, embedding_dimensions = NULL, \
         embedding_version = NULL",
    )
    .await
    .expect("the raw update applies");
    let page = store
        .unembedded_claims(scope, &reference(1), &unclassified(), None, 8)
        .await
        .expect("the page answers");
    assert!(
        page.claims.is_empty(),
        "a claim outside the classifications given is never listed, whatever its column says"
    );
    assert_eq!(page.undecodable, 1);
    schema.drop().await;
}

#[tokio::test]
async fn postgres_the_database_refuses_a_vector_of_the_wrong_length_when_dsn_is_set() {
    let Some(dsn) =
        pgvector_dsn("postgres_the_database_refuses_a_vector_of_the_wrong_length").await
    else {
        return;
    };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "sem_length").await;
    let scopes = ConformanceScopes::unique("pg-sem-length");
    let scope = &scopes.primary;
    let claim = append(&store, scope, "length", "the car is here").await;

    for malformed in [
        // Two values beside a dimension count of three.
        "embedding = '[1,0]'::vector, embedding_model = 'proof-embedder', \
         embedding_dimensions = 3, embedding_version = 1",
        // A vector with no reference beside it.
        "embedding = '[1,0,0]'::vector",
        // A reference with no vector.
        "embedding_model = 'proof-embedder'",
    ] {
        let refused = doctor(&raw, scope, &claim.claim_id, malformed)
            .await
            .expect_err("the check refuses the row");
        assert_eq!(
            refused.code(),
            Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION),
            "{malformed}: {refused}"
        );
    }
    assert!(!holds_embedding(&raw, scope, &claim.claim_id).await);
    schema.drop().await;
}

#[tokio::test]
async fn postgres_an_unindexed_claim_is_not_ranked_and_flags_the_search_when_dsn_is_set() {
    let Some(dsn) =
        pgvector_dsn("postgres_an_unindexed_claim_is_not_ranked_and_flags_the_search").await
    else {
        return;
    };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "sem_unindexed").await;
    let scopes = ConformanceScopes::unique("pg-sem-unindexed");
    let scope = &scopes.primary;
    let claim = append(&store, scope, "unindexed", "the car is here").await;
    // The row as a build from before the word index left it: no filter
    // columns. It even has an embedding, and still cannot be judged.
    doctor(
        &raw,
        scope,
        &claim.claim_id,
        "search_tokens = NULL, search_classification = NULL, search_confidence_bps = NULL, \
         search_read_capabilities = NULL",
    )
    .await
    .expect("the raw update applies");
    assert!(store
        .store_claim_embedding(scope, &claim.claim_id, &embedding([1.0, 0.0, 0.0]))
        .await
        .expect("the embedding stores"));

    let search = toward_first_axis(&[MemoryClassification::Unclassified]);
    let before = store
        .search_semantic(scope, &search)
        .await
        .expect("the search answers");
    assert!(
        before.hits.is_empty(),
        "a row whose filters cannot be judged is not a candidate"
    );
    assert!(before.unindexed, "and it makes the answer incomplete");
    assert!(
        store
            .unembedded_claims(scope, &reference(2), &unclassified(), None, 8)
            .await
            .expect("the page answers")
            .claims
            .is_empty(),
        "nor is it listed for an embedder: its classification cannot be judged"
    );

    // The word index's own pass is what makes the row judgeable.
    assert_eq!(
        store
            .index_claims(scope, None, 8)
            .await
            .expect("the pass answers")
            .indexed,
        1
    );
    let after = store
        .search_semantic(scope, &search)
        .await
        .expect("the search answers");
    assert_eq!(after.hits.len(), 1);
    assert!(!after.unindexed);
    assert_eq!(
        store
            .unembedded_claims(scope, &reference(2), &unclassified(), None, 8)
            .await
            .expect("the page answers")
            .claims
            .len(),
        1
    );

    // A row with no filter columns can still be judged on its trust, which
    // is a column every row has. A retracted one does not make a search that
    // admits no retracted claim incomplete; it would hold the flag up for
    // good, since a retracted claim is never embedded.
    let scope = &scopes.foreign;
    let retracted = append(&store, scope, "unindexed-retracted", "the car is here").await;
    store
        .transition(
            scope,
            &ClaimTrustTransitionRequest::new(
                retracted.claim_id.clone(),
                ClaimOperationId::derive_transition(scope, &retracted.claim_id, "retract")
                    .expect("the operation id derives"),
                ClaimTrustStatus::Retracted,
                conformance_resolver(),
                NOW,
            ),
            &ClaimPromotionPolicy::ungated(),
            NOW,
        )
        .await
        .expect("the retraction applies");
    doctor(
        &raw,
        scope,
        &retracted.claim_id,
        "search_tokens = NULL, search_classification = NULL, search_confidence_bps = NULL, \
         search_read_capabilities = NULL",
    )
    .await
    .expect("the raw update applies");
    assert!(
        !store
            .search_semantic(scope, &search)
            .await
            .expect("the search answers")
            .unindexed,
        "a retracted row with no filter columns is not a claim this search could have found"
    );
    let admitting_retracted = ClaimSemanticSearch::new(
        ClaimSearch::new("", 64)
            .with_trust(BTreeSet::from([ClaimTrustStatus::Retracted]))
            .with_classifications(unclassified()),
        embedding([1.0, 0.0, 0.0]),
    );
    assert!(
        store
            .search_semantic(scope, &admitting_retracted)
            .await
            .expect("the search answers")
            .unindexed,
        "a search that names the row's trust state cannot judge the rest of it"
    );
    schema.drop().await;
}

#[tokio::test]
async fn postgres_two_embedding_passes_at_once_embed_every_claim_when_dsn_is_set() {
    let Some(dsn) = pgvector_dsn("postgres_two_embedding_passes_at_once_embed_every_claim").await
    else {
        return;
    };
    let (schema, raw, first) = PrivateSchema::migrated(&dsn, "sem_concurrent").await;
    // A second node, and something embedding at append beside both passes.
    let second = schema.semantic_store().await;
    let appender = schema.semantic_store().await;
    let scopes = ConformanceScopes::unique("pg-sem-concurrent");
    let scope = &scopes.primary;
    let embedder = ConceptEmbedder::new(&[&["car", "automobile"]]);
    let mut claims = Vec::new();
    for index in 0..40 {
        claims.push(append(&first, scope, &format!("claim-{index}"), "the car is here").await);
    }

    async fn walk(
        store: &PostgresKnowledgeGraphStore,
        embedder: &ConceptEmbedder,
        scope: &KnowledgeSpaceScope,
    ) -> u64 {
        let mut embedded = 0;
        let mut after: Option<ClaimId> = None;
        loop {
            let page = embed_claims(store, embedder, &unclassified(), scope, after.as_ref(), 7)
                .await
                .expect("the pass answers");
            assert_eq!(page.failed, 0);
            embedded += page.embedded;
            match page.next {
                Some(next) => after = Some(next),
                None => return embedded,
            }
        }
    }
    let at_append = async {
        let vector = embedder
            .embed("the car is here")
            .await
            .expect("the text embeds");
        for claim in &claims {
            assert!(appender
                .store_claim_embedding(
                    scope,
                    &claim.claim_id,
                    &ClaimEmbedding {
                        reference: embedder.embedding_ref(),
                        vector: vector.clone(),
                    },
                )
                .await
                .expect("the embedding stores"));
        }
    };
    tokio::join!(
        walk(&first, &embedder, scope),
        walk(&second, &embedder, scope),
        at_append
    );

    let embedded: i64 = raw
        .query_one(
            "SELECT count(*) FROM rakka_agent_knowledge_claim \
             WHERE tenant = $1 AND space = $2 AND embedding IS NOT NULL \
               AND embedding_model = $3 AND embedding_dimensions = 2 AND embedding_version = 1",
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &ConceptEmbedder::MODEL,
            ],
        )
        .await
        .expect("the rows read")
        .get(0);
    assert_eq!(embedded, 40, "every claim holds exactly its one embedding");
    assert_eq!(
        walk(&first, &embedder, scope).await,
        0,
        "nothing is left to embed"
    );
    schema.drop().await;
}

#[tokio::test]
async fn postgres_the_embedding_pass_reads_past_a_claim_id_this_build_refuses_when_dsn_is_set() {
    // A row whose claim id this build refuses (one written past the store,
    // by a later build with a wider id alphabet) is counted and read past,
    // and does not count toward the limit. It used to make a full page whose
    // last row it was an error, and every retry the same error.
    let Some(dsn) =
        pgvector_dsn("postgres_the_embedding_pass_reads_past_a_claim_id_this_build_refuses").await
    else {
        return;
    };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "sem_refused_id").await;
    let scopes = ConformanceScopes::unique("pg-sem-refused-id");
    let scope = &scopes.primary;
    let embedder = ConceptEmbedder::new(&[&["car", "automobile"]]);
    let mut ids = Vec::new();
    for index in 0..3 {
        ids.push(
            append(&store, scope, &format!("claim-{index}"), "the car is here")
                .await
                .claim_id,
        );
    }
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let first_refused = format!("{}|x", ids[0].as_str());
    let last_refused = format!("{}|x", ids[2].as_str());
    plant_refused_claim_id(&raw, scope, &ids[0], &first_refused).await;
    plant_refused_claim_id(&raw, scope, &ids[2], &last_refused).await;
    assert_eq!(
        claim_order(&raw, scope).await,
        [
            ids[0].as_str(),
            first_refused.as_str(),
            ids[1].as_str(),
            ids[2].as_str(),
            last_refused.as_str(),
        ]
    );

    let mut pages = Vec::new();
    let mut after: Option<ClaimId> = None;
    for _ in 0..8 {
        let page = embed_claims(&store, &embedder, &unclassified(), scope, after.as_ref(), 1)
            .await
            .expect("a refused claim id never fails the pass");
        pages.push((
            page.embedded,
            page.failed,
            page.undecodable,
            page.next.clone(),
        ));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(
        pages,
        vec![
            (1, 0, 0, Some(ids[0].clone())),
            (1, 0, 1, Some(ids[1].clone())),
            (1, 0, 0, Some(ids[2].clone())),
            (0, 0, 1, None),
        ]
    );
    schema.drop().await;
}

#[tokio::test]
async fn postgres_a_claim_is_found_by_meaning_when_dsn_is_set() {
    let Some(dsn) = pgvector_dsn("postgres_a_claim_is_found_by_meaning").await else {
        return;
    };
    let (schema, _raw, store) = PrivateSchema::migrated(&dsn, "sem_meaning").await;
    let scopes = ConformanceScopes::unique("pg-sem-meaning");
    let scope = &scopes.primary;
    let embedder = ConceptEmbedder::new(&[&["car", "automobile"], &["boat", "ship"]]);
    let car = append(&store, scope, "car", "the car is at the depot").await;
    let boat = append(&store, scope, "boat", "the boat left at noon").await;

    let retriever = KnowledgeGraphCommunalRetriever::new(Arc::new(store.clone()))
        .expect("the PostgreSQL backend searches by word")
        .with_embedder(Arc::new(embedder.clone()));
    assert_eq!(retriever.ranking(), KnowledgeGraphCommunalRanking::Semantic);
    let query = CommunalRetrievalQuery {
        text: "where is the automobile".to_string(),
        limit: 8,
        min_confidence_bps: 0,
        classifications: unclassified(),
        trust: BTreeSet::from([AgentCommunalClaimTrust::Proposed]),
        reader: BTreeSet::new(),
    };

    // Before the pass: nothing is embedded, and the search says so.
    let before = retriever
        .retrieve(scope.tenant(), scope.space(), &query, NOW)
        .await
        .expect("the search answers");
    assert!(before.ranked.is_empty());
    assert!(before.index_incomplete);

    let page = embed_claims(&store, &embedder, &unclassified(), scope, None, 64)
        .await
        .expect("the pass answers");
    assert_eq!((page.embedded, page.failed, page.next), (2, 0, None));

    // The query shares no word with the claim it finds.
    let after = retriever
        .retrieve(scope.tenant(), scope.space(), &query, NOW)
        .await
        .expect("the search answers");
    assert!(!after.index_incomplete);
    assert_eq!(
        after
            .ranked
            .iter()
            .map(|ranked| (ranked.claim.clone(), ranked.relevance_bps))
            .collect::<Vec<_>>(),
        vec![
            (
                AgentCommunalClaimId::new(car.claim_id.as_str()).expect("the id is valid"),
                10_000
            ),
            (
                AgentCommunalClaimId::new(boat.claim_id.as_str()).expect("the id is valid"),
                5_000
            ),
        ]
    );
    // A word search of the same store finds nothing for the same query word.
    let by_words = KnowledgeGraphCommunalRetriever::new(Arc::new(store.clone()))
        .expect("the PostgreSQL backend searches by word")
        .retrieve(
            scope.tenant(),
            scope.space(),
            &CommunalRetrievalQuery {
                text: "automobile".to_string(),
                ..query
            },
            NOW,
        )
        .await
        .expect("the word search answers");
    assert!(by_words.ranked.is_empty());
    schema.drop().await;
}
