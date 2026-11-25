//! What only a live database can prove about the claim search index.
//!
//! Issue #69 keeps a keyword-search index as columns on the claim row. The
//! conformance suite proves the shared search semantics on this backend; this
//! file proves what needs real rows and a real *earlier binary*: that the
//! table migrates under one without a rewrite, that everything one still does
//! to a migrated table is safe, and — above all — that a claim's search words
//! never outlive the claim, whichever binary withdraws it.
//!
//! The earlier binary is represented by the statements it runs, frozen below
//! byte for byte from `86cf087` (the last commit before the index existed) and
//! bound exactly as that commit's store bound them. They are literals on
//! purpose: a proof that imported today's constants would only prove today's
//! build agrees with itself.
//!
//! Every test is gated on `RAKKA_POSTGRES_TEST_DSN` and passes silently
//! without it.

use std::collections::BTreeSet;

use rakka_agent_knowledge_graph::{ClaimSearch, ClaimSearchReport};
use rakka_agent_knowledge_graph_postgres::CLAIM_SEARCH_SQL;
use rakka_agent_knowledge_graph_postgres::{
    CLAIM_SEARCH_INDEX_PAGE_MAX_ROWS, CLAIM_SEARCH_INDEX_WRITE_SQL,
};

use rakka_agent::{AgentCapabilityId, AgentId, AgentTaskContent, MemoryClassification};
use rakka_agent_knowledge_graph::conformance::{conformance_resolver, ConformanceScopes};
use rakka_agent_knowledge_graph::{
    claim_search_tokens, Claim, ClaimAccess, ClaimDeleteRequest, ClaimId, ClaimNodeId, ClaimObject,
    ClaimOperationId, ClaimPredicate, ClaimPromotionPolicy, ClaimProvenance, ClaimTombstoneReason,
    ClaimTombstoneRequest, ClaimTransitionOutcome, ClaimTrustStatus, ClaimTrustTransition,
    ClaimTrustTransitionRequest, KnowledgeGraphStore, KnowledgeSpaceScope, CLAIM_TOMBSTONE_MARKER,
};
use rakka_agent_knowledge_graph_postgres::{
    PostgresKnowledgeGraphStore, SEARCH_TOKENS_INDEX_NAME, SEARCH_TRIGGER_NAME,
    TOMBSTONE_FENCE_TRANSITION_COUNT, UNINDEXED_INDEX_NAME,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, Row};

const MIGRATION_SQL_86CF087: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_agent_knowledge_claim (
    tenant TEXT NOT NULL,
    space TEXT NOT NULL,
    claim_id TEXT COLLATE "C" NOT NULL,
    operation_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_node TEXT,
    trust TEXT NOT NULL CHECK (trust IN ('proposed', 'verified', 'disputed', 'retracted')),
    transition_count INTEGER NOT NULL CHECK (transition_count >= 0),
    record BYTEA NOT NULL,
    tombstoned BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (tenant, space, claim_id)
);

CREATE INDEX IF NOT EXISTS rakka_agent_knowledge_claim_subject
    ON rakka_agent_knowledge_claim (tenant, space, subject);

CREATE INDEX IF NOT EXISTS rakka_agent_knowledge_claim_object
    ON rakka_agent_knowledge_claim (tenant, space, object_node)
    WHERE object_node IS NOT NULL;

CREATE TABLE IF NOT EXISTS rakka_agent_knowledge_claim_transition (
    tenant TEXT NOT NULL,
    space TEXT NOT NULL,
    claim_id TEXT COLLATE "C" NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    record BYTEA NOT NULL,
    PRIMARY KEY (tenant, space, claim_id, ordinal)
);

CREATE TABLE IF NOT EXISTS rakka_agent_knowledge_claim_op (
    tenant TEXT NOT NULL,
    space TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    result BYTEA NOT NULL,
    PRIMARY KEY (tenant, space, operation_id),
    CONSTRAINT rakka_agent_knowledge_claim_op_kind_check
        CHECK (kind IN ('append', 'transition', 'tombstone', 'delete', 'erased'))
);

DO $migrate$
DECLARE
    stale RECORD;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_attribute
        WHERE attrelid = 'rakka_agent_knowledge_claim'::regclass
          AND attname = 'tombstoned'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE rakka_agent_knowledge_claim
            ADD COLUMN tombstoned BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'rakka_agent_knowledge_claim_op'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%''erased''%'
    ) THEN
        FOR stale IN
            SELECT conname FROM pg_constraint
            WHERE conrelid = 'rakka_agent_knowledge_claim_op'::regclass
              AND contype = 'c'
              AND pg_get_constraintdef(oid) LIKE '%kind%'
        LOOP
            EXECUTE format(
                'ALTER TABLE rakka_agent_knowledge_claim_op DROP CONSTRAINT %I',
                stale.conname
            );
        END LOOP;
        ALTER TABLE rakka_agent_knowledge_claim_op
            ADD CONSTRAINT rakka_agent_knowledge_claim_op_kind_check
            CHECK (kind IN ('append', 'transition', 'tombstone', 'delete', 'erased'))
            NOT VALID;
    END IF;
END
$migrate$;
"#;

const APPEND_SQL_86CF087: &str = r#"
WITH existing_op AS (
    SELECT kind, result FROM rakka_agent_knowledge_claim_op
    WHERE tenant = $1 AND space = $2 AND operation_id = $3
), ins_claim AS (
    INSERT INTO rakka_agent_knowledge_claim
        (tenant, space, claim_id, operation_id, subject, predicate,
         object_node, trust, transition_count, record)
    SELECT $1, $2, $4, $3, $5, $6, $7, $8, $9, $10
    WHERE NOT EXISTS (SELECT 1 FROM existing_op)
    ON CONFLICT (tenant, space, claim_id) DO NOTHING
    RETURNING claim_id
), ins_op AS (
    INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result)
    SELECT $1, $2, $3, 'append', $10 FROM ins_claim
)
SELECT EXISTS (SELECT 1 FROM existing_op) AS replayed,
       (SELECT kind   FROM existing_op)   AS replay_kind,
       (SELECT result FROM existing_op)   AS replay_result,
       EXISTS (SELECT 1 FROM ins_claim)   AS applied
"#;

const TRANSITION_SQL_86CF087: &str = r#"
WITH existing_op AS (
    SELECT kind, result FROM rakka_agent_knowledge_claim_op
    WHERE tenant = $1 AND space = $2 AND operation_id = $3
), upd AS (
    UPDATE rakka_agent_knowledge_claim
    SET trust = $4, transition_count = $5, record = $6
    WHERE tenant = $1 AND space = $2 AND claim_id = $7
      AND transition_count = $8
      AND NOT EXISTS (SELECT 1 FROM existing_op)
    RETURNING claim_id
), ins_tr AS (
    INSERT INTO rakka_agent_knowledge_claim_transition (tenant, space, claim_id, ordinal, record)
    SELECT $1, $2, $7, $9, $10 FROM upd
), ins_op AS (
    INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result)
    SELECT $1, $2, $3, 'transition', $11 FROM upd
    ON CONFLICT (tenant, space, operation_id) DO NOTHING
)
SELECT EXISTS (SELECT 1 FROM existing_op) AS replayed,
       (SELECT kind   FROM existing_op)   AS replay_kind,
       (SELECT result FROM existing_op)   AS replay_result,
       EXISTS (SELECT 1 FROM upd)         AS applied
"#;

const TOMBSTONE_SQL_86CF087: &str = r#"
WITH existing_op AS (
    SELECT kind, result FROM rakka_agent_knowledge_claim_op
    WHERE tenant = $1 AND space = $2 AND operation_id = $3
), upd AS (
    UPDATE rakka_agent_knowledge_claim
    SET subject = $5, predicate = $5, object_node = NULL, tombstoned = TRUE,
        transition_count = $6, record = $7
    WHERE tenant = $1 AND space = $2 AND claim_id = $4
      AND transition_count = $8
      AND NOT EXISTS (SELECT 1 FROM existing_op)
    RETURNING claim_id
), erased AS (
    UPDATE rakka_agent_knowledge_claim_op
    SET kind = $10, result = ''::BYTEA
    WHERE tenant = $1 AND space = $2 AND operation_id = ANY($9)
      AND EXISTS (SELECT 1 FROM upd)
), ins_op AS (
    INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result)
    SELECT $1, $2, $3, $11, $7 FROM upd
)
SELECT EXISTS (SELECT 1 FROM upd) AS applied
"#;

const DELETE_SQL_86CF087: &str = r#"
WITH existing_op AS (
    SELECT kind, result FROM rakka_agent_knowledge_claim_op
    WHERE tenant = $1 AND space = $2 AND operation_id = $3
), del AS (
    DELETE FROM rakka_agent_knowledge_claim
    WHERE tenant = $1 AND space = $2 AND claim_id = $4
      AND transition_count = $5
      AND NOT EXISTS (SELECT 1 FROM existing_op)
    RETURNING claim_id
), del_tr AS (
    DELETE FROM rakka_agent_knowledge_claim_transition
    WHERE tenant = $1 AND space = $2 AND claim_id = $4
      AND EXISTS (SELECT 1 FROM del)
), erased AS (
    UPDATE rakka_agent_knowledge_claim_op
    SET kind = $7, result = ''::BYTEA
    WHERE tenant = $1 AND space = $2 AND operation_id = ANY($6)
      AND EXISTS (SELECT 1 FROM del)
), ins_op AS (
    INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result)
    SELECT $1, $2, $3, $8, ''::BYTEA FROM del
)
SELECT EXISTS (SELECT 1 FROM del) AS applied
"#;

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);

/// The four index columns, in the order the migration adds them.
const SEARCH_COLUMNS: [&str; 4] = [
    "search_tokens",
    "search_classification",
    "search_confidence_bps",
    "search_read_capabilities",
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

/// A raw client and a migrated store in the shared schema, for the proofs
/// that change nothing another test could see.
async fn shared() -> Option<(Client, PostgresKnowledgeGraphStore)> {
    let dsn = dsn()?;
    let store = PostgresKnowledgeGraphStore::new(connect(&dsn).await);
    store.migrate().await.expect("the schema applies");
    Some((connect(&dsn).await, store))
}

/// A client whose session resolves the tables in a private schema.
async fn client_in(dsn: &str, schema: &str) -> Client {
    let client = connect(dsn).await;
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .await
        .expect("the session targets the private schema");
    client
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

/// One private schema: the tables of a test live here and nowhere else, so a
/// test can run the earlier binary's DDL, drop an index or drop a trigger
/// without touching what any other test sees.
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
        client_in(&self.dsn, &self.name).await
    }

    /// A store in the schema, not yet migrated.
    async fn store(&self) -> PostgresKnowledgeGraphStore {
        PostgresKnowledgeGraphStore::new(self.client().await)
    }

    /// The schema with the tables exactly as `86cf087` created them.
    async fn earlier(dsn: &str, label: &str) -> (Self, Client) {
        let schema = Self::create(dsn, label).await;
        let raw = schema.client().await;
        raw.batch_execute(MIGRATION_SQL_86CF087)
            .await
            .expect("the earlier schema applies");
        (schema, raw)
    }

    /// The schema migrated by this build.
    async fn migrated(dsn: &str, label: &str) -> (Self, Client, PostgresKnowledgeGraphStore) {
        let schema = Self::create(dsn, label).await;
        let store = schema.store().await;
        store.migrate().await.expect("the schema applies");
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

/// A claim whose statement is the given subject, predicate and inline text.
fn text_claim(
    scope: &KnowledgeSpaceScope,
    discriminator: &str,
    subject: &str,
    predicate: &str,
    text: &str,
) -> Claim {
    Claim::new(
        scope,
        ClaimOperationId::derive_append(scope, discriminator).expect("the operation id derives"),
        ClaimNodeId::new(subject).expect("the node id is valid"),
        ClaimPredicate::new(predicate).expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::Value::String(text.to_string()))
                .expect("the value is inline-bounded"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
}

fn transition_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
    to: ClaimTrustStatus,
) -> ClaimTrustTransitionRequest {
    ClaimTrustTransitionRequest::new(
        claim_id.clone(),
        ClaimOperationId::derive_transition(scope, claim_id, discriminator)
            .expect("the operation id derives"),
        to,
        conformance_resolver(),
        NOW,
    )
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

/// Runs the `86cf087` append statement, bound as that commit's `append` bound
/// it. It names none of the index columns.
async fn earlier_append(raw: &Client, scope: &KnowledgeSpaceScope, claim: &Claim) -> Row {
    let record = serde_json::to_vec(&claim.to_record()).expect("the record serializes");
    let transition_count = 0_i32;
    let params: [&(dyn ToSql + Sync); 10] = [
        &scope.tenant().as_str(),
        &scope.space().as_str(),
        &claim.operation_id.as_str(),
        &claim.claim_id.as_str(),
        &claim.subject.as_str(),
        &claim.predicate.as_str(),
        &claim.object.node().map(ClaimNodeId::as_str),
        &claim.trust().as_label(),
        &transition_count,
        &record,
    ];
    raw.query_one(APPEND_SQL_86CF087, &params)
        .await
        .expect("the earlier append statement runs")
}

/// Runs the `86cf087` transition statement, bound as that commit's
/// `transition` bound it from a read of `read`, and answers the claim it
/// wrote with the statement's row.
async fn earlier_transition(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    read: &Claim,
    request: &ClaimTrustTransitionRequest,
) -> (Claim, Row) {
    let updated = read
        .apply_transition(request.to)
        .expect("the earlier binary found the transition legal");
    let transition = ClaimTrustTransition::new(
        read.claim_id.clone(),
        request.operation_id.clone(),
        updated.transition_count(),
        read.trust(),
        request.to,
        request.actor.clone(),
        request.occurred_at,
    )
    .expect("the transition is valid");
    let outcome = ClaimTransitionOutcome {
        claim: updated.clone(),
        transition: transition.clone(),
    };
    let fence = i32::try_from(read.transition_count()).expect("the count fits");
    let updated_count = i32::try_from(updated.transition_count()).expect("the count fits");
    let updated_record = serde_json::to_vec(&updated.to_record()).expect("the record serializes");
    let transition_record =
        serde_json::to_vec(&transition.to_record()).expect("the record serializes");
    let outcome_record = serde_json::to_vec(&outcome).expect("the outcome serializes");
    let params: [&(dyn ToSql + Sync); 11] = [
        &scope.tenant().as_str(),
        &scope.space().as_str(),
        &request.operation_id.as_str(),
        &updated.trust().as_label(),
        &updated_count,
        &updated_record,
        &request.claim_id.as_str(),
        &fence,
        &updated_count,
        &transition_record,
        &outcome_record,
    ];
    let row = raw
        .query_one(TRANSITION_SQL_86CF087, &params)
        .await
        .expect("the earlier transition statement runs");
    (updated, row)
}

/// Runs the `86cf087` tombstone statement, bound as that commit's
/// `tombstone_claim` bound it from a read of `read`: fenced on the transition
/// count that read saw, erasing the ledger payloads of `earlier_operations`.
/// It does not name `search_tokens`.
async fn earlier_tombstone(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    read: &Claim,
    request: &ClaimTombstoneRequest,
    earlier_operations: &[&ClaimOperationId],
) -> Row {
    let stub = read
        .apply_tombstone(request.to_tombstone())
        .expect("the earlier binary found the claim live");
    let stub_record = serde_json::to_vec(&stub.to_record()).expect("the record serializes");
    let fence = i32::try_from(read.transition_count()).expect("the count fits");
    let operations: Vec<String> = earlier_operations
        .iter()
        .map(|operation| operation.as_str().to_string())
        .collect();
    let params: [&(dyn ToSql + Sync); 11] = [
        &scope.tenant().as_str(),
        &scope.space().as_str(),
        &request.operation_id.as_str(),
        &request.claim_id.as_str(),
        &CLAIM_TOMBSTONE_MARKER,
        &TOMBSTONE_FENCE_TRANSITION_COUNT,
        &stub_record,
        &fence,
        &operations,
        &"erased",
        &"tombstone",
    ];
    raw.query_one(TOMBSTONE_SQL_86CF087, &params)
        .await
        .expect("the earlier tombstone statement runs")
}

/// Runs the `86cf087` delete statement, bound as that commit's `delete_claim`
/// bound it from a read of a live `read`.
async fn earlier_delete(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    read: &Claim,
    discriminator: &str,
    earlier_operations: &[&ClaimOperationId],
) -> Row {
    let operation_id = ClaimOperationId::derive_delete(scope, &read.claim_id, discriminator)
        .expect("the operation id derives");
    let fence = i32::try_from(read.transition_count()).expect("the count fits");
    let operations: Vec<String> = earlier_operations
        .iter()
        .map(|operation| operation.as_str().to_string())
        .collect();
    let params: [&(dyn ToSql + Sync); 8] = [
        &scope.tenant().as_str(),
        &scope.space().as_str(),
        &operation_id.as_str(),
        &read.claim_id.as_str(),
        &fence,
        &operations,
        &"erased",
        &"delete",
    ];
    raw.query_one(DELETE_SQL_86CF087, &params)
        .await
        .expect("the earlier delete statement runs")
}

/// The storage file of the claim table: a rewrite changes it.
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

/// The type of each index column the claim table has, by name.
async fn search_column_types(client: &Client) -> Vec<(String, String)> {
    client
        .query(
            "SELECT attname::TEXT, format_type(atttypid, atttypmod) FROM pg_attribute \
             WHERE attrelid = 'rakka_agent_knowledge_claim'::regclass \
               AND attname = ANY($1) AND NOT attisdropped \
             ORDER BY attnum",
            &[&SEARCH_COLUMNS.as_slice()],
        )
        .await
        .expect("the catalog reads")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Whether one index of the session's schema exists and is valid: `None` when
/// there is no such index.
async fn index_valid(client: &Client, index: &str) -> Option<bool> {
    client
        .query_opt(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass($1)",
            &[&index],
        )
        .await
        .expect("the catalog reads")
        .map(|row| row.get(0))
}

/// Everything the search migration creates, with each object's catalog
/// identity and the transaction that last wrote its catalog row. A repeat
/// migration that altered, replaced or re-created any of it changes this.
async fn search_catalog(client: &Client) -> Vec<String> {
    let mut entries = Vec::new();
    for row in client
        .query(
            "SELECT attname::TEXT, attnum::INT, atttypid::BIGINT, xmin::TEXT FROM pg_attribute \
             WHERE attrelid = 'rakka_agent_knowledge_claim'::regclass \
               AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
            &[],
        )
        .await
        .expect("the catalog reads")
    {
        entries.push(format!(
            "column {} {} {} {}",
            row.get::<_, String>(0),
            row.get::<_, i32>(1),
            row.get::<_, i64>(2),
            row.get::<_, String>(3)
        ));
    }
    for row in client
        .query(
            "SELECT t.tgname::TEXT, t.oid::BIGINT, t.xmin::TEXT, p.oid::BIGINT, p.xmin::TEXT \
             FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid \
             WHERE t.tgrelid = 'rakka_agent_knowledge_claim'::regclass AND NOT t.tgisinternal \
             ORDER BY t.tgname",
            &[],
        )
        .await
        .expect("the catalog reads")
    {
        entries.push(format!(
            "trigger {} {} {} function {} {}",
            row.get::<_, String>(0),
            row.get::<_, i64>(1),
            row.get::<_, String>(2),
            row.get::<_, i64>(3),
            row.get::<_, String>(4)
        ));
    }
    for row in client
        .query(
            "SELECT c.relname::TEXT, i.indexrelid::BIGINT, i.indisvalid, c.relfilenode::BIGINT \
             FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             WHERE i.indrelid = 'rakka_agent_knowledge_claim'::regclass ORDER BY c.relname",
            &[],
        )
        .await
        .expect("the catalog reads")
    {
        entries.push(format!(
            "index {} {} {} {}",
            row.get::<_, String>(0),
            row.get::<_, i64>(1),
            row.get::<_, bool>(2),
            row.get::<_, i64>(3)
        ));
    }
    entries
}

/// Whether the clearing trigger is on the session's claim table.
async fn search_trigger_exists(client: &Client) -> bool {
    client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_trigger \
             WHERE tgrelid = 'rakka_agent_knowledge_claim'::regclass \
               AND tgname = $1 AND NOT tgisinternal)",
            &[&SEARCH_TRIGGER_NAME],
        )
        .await
        .expect("the catalog reads")
        .get(0)
}

// ---------------------------------------------------------------------------
// The migration.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn postgres_the_search_migration_rewrites_nothing_when_dsn_is_set() {
    // The tables exactly as `86cf087` created them, holding rows that commit's
    // append wrote, gain the index in place: no rewrite of the claim table,
    // four nullable columns that are `NULL` on every row that predates them,
    // the clearing trigger, and both indexes built and valid.
    let Some(dsn) = dsn() else { return };
    let (schema, raw) = PrivateSchema::earlier(&dsn, "search_migration").await;

    let scope = ConformanceScopes::unique("pg-search-migration").primary;
    for index in 0..3 {
        let claim = text_claim(
            &scope,
            &format!("before-{index}"),
            "order-42",
            "status",
            "delayed at customs",
        );
        let row = earlier_append(&raw, &scope, &claim).await;
        assert!(row.get::<_, bool>("applied"), "the earlier append applies");
    }
    assert!(
        search_column_types(&raw).await.is_empty(),
        "the earlier schema has none of the index columns"
    );
    let filenode_before = claim_table_filenode(&raw).await;

    schema
        .store()
        .await
        .migrate()
        .await
        .expect("the schema migrates");

    assert_eq!(
        claim_table_filenode(&raw).await,
        filenode_before,
        "adding the index columns must not rewrite the claim table"
    );
    assert_eq!(
        search_column_types(&raw).await,
        vec![
            ("search_tokens".to_string(), "text[]".to_string()),
            ("search_classification".to_string(), "text".to_string()),
            ("search_confidence_bps".to_string(), "integer".to_string()),
            ("search_read_capabilities".to_string(), "text[]".to_string()),
        ]
    );
    let unindexed: i64 = raw
        .query_one(
            "SELECT count(*) FROM rakka_agent_knowledge_claim \
             WHERE tenant = $1 AND space = $2 \
               AND search_tokens IS NULL AND search_classification IS NULL \
               AND search_confidence_bps IS NULL AND search_read_capabilities IS NULL",
            &[&scope.tenant().as_str(), &scope.space().as_str()],
        )
        .await
        .expect("the rows read")
        .get(0);
    assert_eq!(
        unindexed, 3,
        "a row that predates the columns holds NULL in every one of them"
    );
    assert!(search_trigger_exists(&raw).await);
    assert_eq!(
        index_valid(&raw, SEARCH_TOKENS_INDEX_NAME).await,
        Some(true)
    );
    assert_eq!(index_valid(&raw, UNINDEXED_INDEX_NAME).await, Some(true));

    schema.drop().await;
}

#[tokio::test]
async fn postgres_a_fresh_schema_is_created_in_the_migrated_shape_when_dsn_is_set() {
    // A database that never held the earlier tables ends where an upgraded
    // one does.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, _store) = PrivateSchema::migrated(&dsn, "search_fresh").await;

    assert_eq!(search_column_types(&raw).await.len(), 4);
    assert!(search_trigger_exists(&raw).await);
    assert_eq!(
        index_valid(&raw, SEARCH_TOKENS_INDEX_NAME).await,
        Some(true)
    );
    assert_eq!(index_valid(&raw, UNINDEXED_INDEX_NAME).await, Some(true));

    schema.drop().await;
}

#[tokio::test]
async fn postgres_a_second_search_migration_alters_nothing_when_dsn_is_set() {
    // A repeat run writes nothing to the catalog: the same columns, the same
    // trigger and function rows, the same index files.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "search_repeat").await;

    let first = search_catalog(&raw).await;
    assert!(
        first.iter().any(|entry| entry.starts_with("trigger "))
            && first
                .iter()
                .filter(|entry| entry.starts_with("index "))
                .count()
                >= 4,
        "the catalog read sees the trigger and the indexes: {first:?}"
    );
    store.migrate().await.expect("the schema migrates again");
    store
        .migrate()
        .await
        .expect("the schema migrates a third time");
    assert_eq!(search_catalog(&raw).await, first);

    schema.drop().await;
}

#[tokio::test]
async fn postgres_an_invalid_search_index_is_rebuilt_when_dsn_is_set() {
    // A concurrent index build that is cancelled leaves an index the planner
    // will not use and `IF NOT EXISTS` will not replace. The migration drops
    // it and builds it again.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "search_invalid").await;
    let scope = ConformanceScopes::unique("pg-search-invalid").primary;

    for index in [SEARCH_TOKENS_INDEX_NAME, UNINDEXED_INDEX_NAME] {
        raw.batch_execute(&format!("DROP INDEX {index}"))
            .await
            .expect("the index is dropped");
    }

    // A transaction that has written to the claim table and not ended: a
    // concurrent build waits for it, and the timeout cancels the wait.
    let mut holder = schema.client().await;
    let open = holder.transaction().await.expect("the transaction opens");
    open.execute(
        "UPDATE rakka_agent_knowledge_claim SET trust = trust WHERE tenant = $1",
        &[&scope.tenant().as_str()],
    )
    .await
    .expect("the open transaction touches the table");

    let builder = schema.client().await;
    builder
        .batch_execute("SET statement_timeout = '300ms'")
        .await
        .expect("the timeout is set");
    for (index, definition) in [
        (
            SEARCH_TOKENS_INDEX_NAME,
            "USING GIN (search_tokens)".to_string(),
        ),
        (
            UNINDEXED_INDEX_NAME,
            "(tenant, space, claim_id) WHERE search_tokens IS NULL AND NOT tombstoned".to_string(),
        ),
    ] {
        builder
            .batch_execute(&format!(
                "CREATE INDEX CONCURRENTLY {index} ON rakka_agent_knowledge_claim {definition}"
            ))
            .await
            .expect_err("the build is cancelled while it waits for the open transaction");
        assert_eq!(
            index_valid(&raw, index).await,
            Some(false),
            "a cancelled concurrent build leaves an invalid index"
        );
    }

    open.rollback().await.expect("the transaction ends");
    store.migrate().await.expect("the schema migrates");
    assert_eq!(
        index_valid(&raw, SEARCH_TOKENS_INDEX_NAME).await,
        Some(true)
    );
    assert_eq!(index_valid(&raw, UNINDEXED_INDEX_NAME).await, Some(true));

    schema.drop().await;
}

#[tokio::test]
async fn postgres_concurrent_search_migrators_do_not_deadlock_when_dsn_is_set() {
    // Several nodes start at once and each migrates. A concurrent index build
    // waits for every transaction whose snapshot is older than its own, and a
    // migrator blocked inside `pg_advisory_lock` holds exactly such a
    // snapshot while it waits for the builder: a cycle PostgreSQL breaks by
    // failing one of them. A waiting migrator must therefore hold nothing.
    let Some(dsn) = dsn() else { return };
    let (schema, raw) = PrivateSchema::earlier(&dsn, "search_migrators").await;
    let scope = ConformanceScopes::unique("pg-search-migrators").primary;
    for index in 0..8 {
        let claim = text_claim(
            &scope,
            &format!("before-{index}"),
            "order-42",
            "status",
            "delayed at customs",
        );
        assert!(earlier_append(&raw, &scope, &claim)
            .await
            .get::<_, bool>("applied"));
    }

    let mut migrators = Vec::new();
    for _ in 0..4 {
        let store = schema.store().await;
        migrators.push(tokio::spawn(async move { store.migrate().await }));
    }
    for migrator in migrators {
        migrator
            .await
            .expect("the migrator runs")
            .expect("every concurrent migrator succeeds");
    }
    assert_eq!(search_column_types(&raw).await.len(), 4);
    assert!(search_trigger_exists(&raw).await);
    assert_eq!(
        index_valid(&raw, SEARCH_TOKENS_INDEX_NAME).await,
        Some(true)
    );
    assert_eq!(index_valid(&raw, UNINDEXED_INDEX_NAME).await, Some(true));

    schema.drop().await;
}

#[tokio::test]
async fn postgres_every_earlier_statement_still_works_after_the_search_migration_when_dsn_is_set() {
    // An earlier binary keeps serving against a migrated table. Each of the
    // four writes it makes still applies, and its own migration, which knows
    // nothing of the new columns, still runs.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, _store) = PrivateSchema::migrated(&dsn, "search_earlier").await;
    let scope = ConformanceScopes::unique("pg-search-earlier").primary;

    let claim = text_claim(&scope, "served", "order-42", "status", "delayed at customs");
    assert!(earlier_append(&raw, &scope, &claim)
        .await
        .get::<_, bool>("applied"));

    let dispute = transition_request(
        &scope,
        &claim.claim_id,
        "dispute",
        ClaimTrustStatus::Disputed,
    );
    let (disputed, row) = earlier_transition(&raw, &scope, &claim, &dispute).await;
    assert!(
        row.get::<_, bool>("applied"),
        "the earlier transition applies"
    );

    let tombstone = tombstone_request(&scope, &claim.claim_id, "withdraw");
    let row = earlier_tombstone(
        &raw,
        &scope,
        &disputed,
        &tombstone,
        &[&claim.operation_id, &dispute.operation_id],
    )
    .await;
    assert!(
        row.get::<_, bool>("applied"),
        "the earlier tombstone applies"
    );

    let removed = text_claim(&scope, "removed", "order-43", "status", "lost in transit");
    assert!(earlier_append(&raw, &scope, &removed)
        .await
        .get::<_, bool>("applied"));
    let row = earlier_delete(&raw, &scope, &removed, "remove", &[&removed.operation_id]).await;
    assert!(row.get::<_, bool>("applied"), "the earlier delete applies");

    let catalog = search_catalog(&raw).await;
    raw.batch_execute(MIGRATION_SQL_86CF087)
        .await
        .expect("the earlier binary's own migration still runs");
    assert_eq!(
        search_catalog(&raw).await,
        catalog,
        "the earlier migration changes nothing this one made"
    );

    schema.drop().await;
}

// ---------------------------------------------------------------------------
// The index is written with the claim and erased with the claim.
// ---------------------------------------------------------------------------

/// The four index columns of one claim row; `None` when the row is absent.
#[derive(Debug, PartialEq, Eq)]
struct IndexColumns {
    tokens: Option<Vec<String>>,
    classification: Option<String>,
    confidence_bps: Option<i32>,
    read_capabilities: Option<Vec<String>>,
}

async fn index_columns(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
) -> Option<IndexColumns> {
    raw.query_opt(
        "SELECT search_tokens, search_classification, search_confidence_bps, \
                search_read_capabilities \
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
    .map(|row| IndexColumns {
        tokens: row.get(0),
        classification: row.get(1),
        confidence_bps: row.get(2),
        read_capabilities: row.get(3),
    })
}

/// The claim's search words, in the order the column holds them.
fn sorted_tokens(claim: &Claim) -> Vec<String> {
    claim_search_tokens(claim).into_iter().collect()
}

fn capability(id: &str) -> AgentCapabilityId {
    AgentCapabilityId::new(id).expect("the capability id is valid")
}

/// A sensitive, guarded claim: every index column has something to hold.
fn guarded_claim(scope: &KnowledgeSpaceScope, discriminator: &str) -> Claim {
    Claim::new(
        scope,
        ClaimOperationId::derive_append(scope, discriminator).expect("the operation id derives"),
        ClaimNodeId::new("Order-42").expect("the node id is valid"),
        ClaimPredicate::new("shipping_status").expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!("Delayed at customs"))
                .expect("the value is inline-bounded"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        7_250,
        MemoryClassification::Sensitive,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
    .with_access(ClaimAccess {
        required_read_capabilities: BTreeSet::from([capability("cap-b"), capability("cap-a")]),
    })
    .expect("the access set is bounded")
}

#[tokio::test]
async fn postgres_append_writes_the_index_columns_when_dsn_is_set() {
    // The statement that inserts the claim writes its index, so a claim is
    // searchable as soon as it is stored: there is no second write to lose.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-append").primary;
    let claim = guarded_claim(&scope, "indexed");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");

    assert_eq!(
        index_columns(&raw, &scope, &claim.claim_id).await,
        Some(IndexColumns {
            tokens: Some(sorted_tokens(&claim)),
            classification: Some("sensitive".to_string()),
            confidence_bps: Some(7_250),
            read_capabilities: Some(vec!["cap-a".to_string(), "cap-b".to_string()]),
        })
    );
    assert_eq!(
        sorted_tokens(&claim),
        ["42", "at", "customs", "delayed", "order", "shipping", "status"]
    );

    // A claim that requires nothing holds an empty array, not NULL: `NULL`
    // would make "requires nothing" indistinguishable from "not indexed".
    let open = text_claim(&scope, "open", "order-43", "status", "delivered");
    store
        .append(&scope, &open)
        .await
        .expect("the claim appends");
    let columns = index_columns(&raw, &scope, &open.claim_id)
        .await
        .expect("the row exists");
    assert_eq!(columns.read_capabilities, Some(Vec::new()));
    assert_eq!(columns.classification.as_deref(), Some("unclassified"));

    // A replayed append changes nothing.
    store
        .append(&scope, &claim)
        .await
        .expect("the replay answers");
    assert_eq!(
        index_columns(&raw, &scope, &claim.claim_id)
            .await
            .and_then(|columns| columns.tokens),
        Some(sorted_tokens(&claim))
    );
}

#[tokio::test]
async fn postgres_an_artifact_object_is_indexed_by_its_subject_and_predicate_when_dsn_is_set() {
    // A value held by reference gives the index no words: nothing of the
    // reference itself (its id, its location, its checksum) is searchable.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-artifact").primary;
    let claim = Claim::new(
        &scope,
        ClaimOperationId::derive_append(&scope, "referenced").expect("the operation id derives"),
        ClaimNodeId::new("order-42").expect("the node id is valid"),
        ClaimPredicate::new("manifest").expect("the predicate is valid"),
        ClaimObject::Value(AgentTaskContent::artifact(ArtifactRef {
            artifact_id: "secretartifactid".to_string(),
            kind: ArtifactKind::File,
            uri: "s3://evidence/secreturi".to_string(),
            checksum: Some("sha256:secretchecksum".to_string()),
            content_type: Some("application/json".to_string()),
            byte_len: Some(64),
            retention_class: None,
            encryption: None,
            redaction: RedactionStatus::Unredacted,
            created_at: AgentTimestampMillis::new(1),
            metadata: AgentAttributes::default(),
        })),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");

    assert_eq!(
        index_columns(&raw, &scope, &claim.claim_id)
            .await
            .and_then(|columns| columns.tokens),
        Some(vec![
            "42".to_string(),
            "manifest".to_string(),
            "order".to_string()
        ])
    );
}

#[tokio::test]
async fn postgres_a_transition_leaves_the_index_columns_when_dsn_is_set() {
    // Nothing the index copies can change after the append, and trust is a
    // column of its own, so a transition writes none of the four.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-transition").primary;
    let claim = guarded_claim(&scope, "moved");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    let before = index_columns(&raw, &scope, &claim.claim_id).await;

    store
        .transition(
            &scope,
            &transition_request(
                &scope,
                &claim.claim_id,
                "dispute",
                ClaimTrustStatus::Disputed,
            ),
            &ClaimPromotionPolicy::ungated(),
            NOW,
        )
        .await
        .expect("the transition applies");

    assert!(before
        .as_ref()
        .is_some_and(|columns| columns.tokens.is_some()));
    assert_eq!(index_columns(&raw, &scope, &claim.claim_id).await, before);
}

#[tokio::test]
async fn postgres_tombstone_clears_the_search_words_when_dsn_is_set() {
    // The search words are derived from the statement a tombstone withdraws,
    // so the tombstone's one statement clears them.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-tombstone").primary;
    let claim = guarded_claim(&scope, "withdrawn");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    assert!(index_columns(&raw, &scope, &claim.claim_id)
        .await
        .is_some_and(|columns| columns.tokens.is_some()));

    store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &claim.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");

    let columns = index_columns(&raw, &scope, &claim.claim_id)
        .await
        .expect("the stub row is kept");
    assert_eq!(columns.tokens, None, "a stub row holds no search words");
}

/// Appends a guarded claim through the store and tombstones it with the
/// `86cf087` statement, which does not name `search_tokens`. Answers the
/// row's search words afterwards.
async fn words_after_an_earlier_tombstone(
    raw: &Client,
    store: &PostgresKnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
) -> Option<Vec<String>> {
    let claim = guarded_claim(scope, "withdrawn-earlier");
    store
        .append(scope, &claim)
        .await
        .expect("the claim appends");
    assert_eq!(
        index_columns(raw, scope, &claim.claim_id)
            .await
            .and_then(|columns| columns.tokens),
        Some(sorted_tokens(&claim)),
        "the claim is indexed before it is withdrawn"
    );
    let row = earlier_tombstone(
        raw,
        scope,
        &claim,
        &tombstone_request(scope, &claim.claim_id, "withdraw"),
        &[&claim.operation_id],
    )
    .await;
    assert!(
        row.get::<_, bool>("applied"),
        "the earlier tombstone applies"
    );
    index_columns(raw, scope, &claim.claim_id)
        .await
        .expect("the stub row is kept")
        .tokens
}

#[tokio::test]
async fn postgres_an_earlier_tombstone_statement_clears_the_search_words_when_dsn_is_set() {
    // A node on the earlier binary withdraws a claim this build indexed. Its
    // statement knows nothing of the index, and the words are still gone when
    // it commits: the database clears them.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "search_trigger").await;
    let scope = ConformanceScopes::unique("pg-search-trigger").primary;
    assert_eq!(
        words_after_an_earlier_tombstone(&raw, &store, &scope).await,
        None,
        "an earlier binary's tombstone must not leave a withdrawn statement's words"
    );
    schema.drop().await;

    // The same statement without the trigger leaves the words where they
    // were. That is what proves the trigger, and nothing else, clears them.
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "search_no_trigger").await;
    raw.batch_execute(&format!(
        "DROP TRIGGER {SEARCH_TRIGGER_NAME} ON rakka_agent_knowledge_claim"
    ))
    .await
    .expect("the trigger is dropped");
    let scope = ConformanceScopes::unique("pg-search-no-trigger").primary;
    let left = words_after_an_earlier_tombstone(&raw, &store, &scope).await;
    assert!(
        left.is_some_and(|words| words.contains(&"customs".to_string())),
        "without the trigger the earlier statement leaves the words in the row"
    );
    schema.drop().await;
}

#[tokio::test]
async fn postgres_delete_leaves_no_row_when_dsn_is_set() {
    // A delete removes the row, and the index with it.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-delete").primary;
    let claim = guarded_claim(&scope, "removed");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    assert!(index_columns(&raw, &scope, &claim.claim_id).await.is_some());

    store
        .delete(
            &scope,
            &ClaimDeleteRequest {
                claim_id: claim.claim_id.clone(),
                operation_id: ClaimOperationId::derive_delete(&scope, &claim.claim_id, "remove")
                    .expect("the operation id derives"),
                expected: None,
            },
        )
        .await
        .expect("the delete applies");

    assert_eq!(index_columns(&raw, &scope, &claim.claim_id).await, None);
}

// ---------------------------------------------------------------------------
// Search: the index nominates, the record decides.
// ---------------------------------------------------------------------------

/// A search admitting every trust state and classification, for a reader
/// holding `reader`.
fn admitting_all(text: &str, reader: &[&str]) -> ClaimSearch {
    ClaimSearch::new(text, 8)
        .with_trust(ClaimTrustStatus::ALL.into_iter().collect())
        .with_classifications(BTreeSet::from([
            MemoryClassification::Unclassified,
            MemoryClassification::Sensitive,
            MemoryClassification::Redacted,
        ]))
        .with_reader(reader.iter().map(|id| capability(id)).collect())
}

fn hit_ids(report: &ClaimSearchReport) -> Vec<ClaimId> {
    report
        .hits
        .iter()
        .map(|hit| hit.claim.claim_id.clone())
        .collect()
}

/// Rewrites one index column of one claim row, as corruption would.
async fn doctor(raw: &Client, scope: &KnowledgeSpaceScope, claim_id: &ClaimId, assignment: &str) {
    let changed = raw
        .execute(
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
        .expect("the row is doctored");
    assert_eq!(changed, 1, "exactly the addressed row is doctored");
}

#[tokio::test]
async fn postgres_a_claim_is_found_straight_after_its_append_when_dsn_is_set() {
    let Some((_raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-found").primary;
    let claim = guarded_claim(&scope, "found");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");

    let report = store
        .search(&scope, &admitting_all("customs delay", &["cap-a", "cap-b"]))
        .await
        .expect("the search answers");
    assert_eq!(hit_ids(&report), vec![claim.claim_id.clone()]);
    assert_eq!(report.hits[0].claim, claim, "a hit is the stored claim");
    assert_eq!(report.hits[0].relevance_bps, 5_000);
    assert!(!report.unindexed);
}

#[tokio::test]
async fn postgres_an_earlier_binarys_claim_is_reported_unindexed_when_dsn_is_set() {
    // A node on the earlier binary appends into a migrated table. Its row has
    // no index, so a search cannot find it, and says so rather than answering
    // as though the space held nothing more.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-unindexed").primary;
    let earlier = text_claim(
        &scope,
        "earlier",
        "order-42",
        "status",
        "delayed at customs",
    );
    assert!(earlier_append(&raw, &scope, &earlier)
        .await
        .get::<_, bool>("applied"));
    let indexed = text_claim(&scope, "indexed", "order-43", "status", "held at customs");
    store
        .append(&scope, &indexed)
        .await
        .expect("the claim appends");

    let report = store
        .search(&scope, &admitting_all("customs", &[]))
        .await
        .expect("the search answers");
    assert_eq!(
        hit_ids(&report),
        vec![indexed.claim_id.clone()],
        "the earlier binary's claim is not a hit"
    );
    assert!(
        report.unindexed,
        "the space holds a live claim the index does not cover"
    );

    // A stub is not a claim the index is missing: one that was withdrawn
    // before it was ever indexed does not keep the space reporting
    // `unindexed`, and neither does one withdrawn after.
    let row = earlier_tombstone(
        &raw,
        &scope,
        &earlier,
        &tombstone_request(&scope, &earlier.claim_id, "withdraw"),
        &[&earlier.operation_id],
    )
    .await;
    assert!(row.get::<_, bool>("applied"));
    store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &indexed.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");
    let report = store
        .search(&scope, &admitting_all("customs", &[]))
        .await
        .expect("the search answers");
    assert!(report.hits.is_empty());
    assert!(!report.unindexed, "a space holding only stubs is covered");
}

#[tokio::test]
async fn postgres_a_drifted_capability_column_cannot_leak_a_claim_when_dsn_is_set() {
    // The index says the claim requires nothing; the record says it requires
    // two capabilities. A reader holding none must not be handed the claim:
    // the search is refused as drift.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-drift-cap").primary;
    let claim = guarded_claim(&scope, "guarded");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    let nobody = admitting_all("customs", &[]);
    assert!(store
        .search(&scope, &nobody)
        .await
        .expect("the search answers")
        .hits
        .is_empty());

    doctor(
        &raw,
        &scope,
        &claim.claim_id,
        "search_read_capabilities = '{}'",
    )
    .await;

    let error = store
        .search(&scope, &nobody)
        .await
        .expect_err("a row the record does not admit is refused, never answered");
    assert_eq!(error.code(), "claim-backend-failed");
}

#[tokio::test]
async fn postgres_a_drifted_classification_column_cannot_leak_a_claim_when_dsn_is_set() {
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-drift-class").primary;
    let claim = guarded_claim(&scope, "sensitive");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    let unclassified_only = ClaimSearch::new("customs", 8)
        .with_trust(ClaimTrustStatus::ALL.into_iter().collect())
        .with_classifications(BTreeSet::from([MemoryClassification::Unclassified]))
        .with_reader(BTreeSet::from([capability("cap-a"), capability("cap-b")]));
    assert!(store
        .search(&scope, &unclassified_only)
        .await
        .expect("the search answers")
        .hits
        .is_empty());

    doctor(
        &raw,
        &scope,
        &claim.claim_id,
        "search_classification = 'unclassified'",
    )
    .await;

    let error = store
        .search(&scope, &unclassified_only)
        .await
        .expect_err("a sensitive claim is never answered to an unclassified-only search");
    assert_eq!(error.code(), "claim-backend-failed");
}

#[tokio::test]
async fn postgres_drifted_search_words_cannot_surface_a_claim_when_dsn_is_set() {
    // Words in the index that the record does not hold: the claim would be
    // found by a query it has nothing to do with. Refused as drift.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-drift-words").primary;
    let claim = text_claim(&scope, "words", "order-42", "status", "delayed");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    doctor(
        &raw,
        &scope,
        &claim.claim_id,
        "search_tokens = '{unrelatedword}'",
    )
    .await;

    let error = store
        .search(&scope, &admitting_all("unrelatedword", &[]))
        .await
        .expect_err("a claim that scores nothing against the query is not a hit");
    assert_eq!(error.code(), "claim-backend-failed");
}

#[tokio::test]
async fn postgres_a_query_without_words_is_a_valid_statement_when_dsn_is_set() {
    // The statement runs with an empty word array, and with every other set
    // empty too: no hits, no error.
    let Some((_raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-wordless").primary;
    let claim = text_claim(&scope, "present", "order-42", "status", "delayed");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");

    for search in [
        admitting_all("...", &[]),
        admitting_all("", &[]),
        ClaimSearch::new("...", 8),
        ClaimSearch::new("delayed", 8),
    ] {
        let report = store
            .search(&scope, &search)
            .await
            .expect("a search that can match nothing is not an error");
        assert!(report.hits.is_empty());
        assert!(!report.unindexed);
    }
}

#[tokio::test]
async fn postgres_a_row_with_no_words_is_indexed_and_never_a_hit_when_dsn_is_set() {
    // An empty word array means "indexed, and holds no words", which is not
    // the `NULL` that means "not indexed".
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-search-no-words").primary;
    let claim = text_claim(&scope, "wordless", "order-42", "status", "delayed");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    doctor(&raw, &scope, &claim.claim_id, "search_tokens = '{}'").await;

    for text in [
        "order",
        "42",
        "status",
        "delayed",
        "order 42 status delayed",
    ] {
        let report = store
            .search(&scope, &admitting_all(text, &[]))
            .await
            .expect("the search answers");
        assert!(
            report.hits.is_empty(),
            "a row with no words matches nothing"
        );
        assert!(
            !report.unindexed,
            "a row with an empty word array is indexed"
        );
    }
}

#[tokio::test]
async fn postgres_search_uses_the_word_index_when_dsn_is_set() {
    // The search's word predicate is one the GIN index can answer. In a scope
    // of five thousand rows where one holds the word, the planner reads the
    // word index rather than every row of the scope.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, _store) = PrivateSchema::migrated(&dsn, "search_plan").await;
    raw.batch_execute(
        "INSERT INTO rakka_agent_knowledge_claim \
            (tenant, space, claim_id, operation_id, subject, predicate, trust, \
             transition_count, record, search_tokens, search_classification, \
             search_confidence_bps, search_read_capabilities) \
         SELECT 'tenant-a', 'space-a', 'claim-' || g, 'op-' || g, 's', 'p', 'proposed', 0, \
                ''::BYTEA, ARRAY['common', 'word' || g], 'unclassified', 5000, '{}' \
         FROM generate_series(1, 5000) AS g; \
         ANALYZE rakka_agent_knowledge_claim; \
         SET enable_seqscan = off",
    )
    .await
    .expect("the rows are planted");

    let params: [&(dyn ToSql + Sync); 8] = [
        &"tenant-a",
        &"space-a",
        &vec!["word77".to_string()],
        &vec!["proposed".to_string()],
        &vec!["unclassified".to_string()],
        &0_i32,
        &Vec::<String>::new(),
        &8_i64,
    ];
    let plan: Vec<String> = raw
        .query(&format!("EXPLAIN {CLAIM_SEARCH_SQL}"), &params)
        .await
        .expect("the statement plans")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    assert!(
        plan.iter()
            .any(|line| line.contains(SEARCH_TOKENS_INDEX_NAME)),
        "the plan reads the word index: {plan:#?}"
    );
    // The same statement, run, finds the one row that holds the word.
    let rows = raw
        .query(CLAIM_SEARCH_SQL, &params)
        .await
        .expect("the statement runs");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>("claim_id"), "claim-77");

    schema.drop().await;
}

// ---------------------------------------------------------------------------
// The indexing pass.
// ---------------------------------------------------------------------------

/// Pages the indexing pass to its end and answers the totals.
async fn index_to_the_end(
    store: &PostgresKnowledgeGraphStore,
    scope: &KnowledgeSpaceScope,
    limit: usize,
) -> (u64, u64) {
    let (mut indexed, mut skipped) = (0, 0);
    let mut after: Option<ClaimId> = None;
    loop {
        let page = store
            .index_claims(scope, after.as_ref(), limit)
            .await
            .expect("the indexing pass answers");
        indexed += page.indexed;
        skipped += page.skipped;
        match page.next {
            Some(next) => after = Some(next),
            None => return (indexed, skipped),
        }
    }
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

/// How many live rows of one scope have no search words.
async fn unindexed_rows(raw: &Client, scope: &KnowledgeSpaceScope) -> i64 {
    raw.query_one(
        "SELECT count(*) FROM rakka_agent_knowledge_claim \
         WHERE tenant = $1 AND space = $2 AND search_tokens IS NULL AND NOT tombstoned",
        &[&scope.tenant().as_str(), &scope.space().as_str()],
    )
    .await
    .expect("the rows count")
    .get(0)
}

/// Appends `count` claims with the earlier binary's statement: live rows with
/// no index. Each holds the word `customs` and a word of its own.
async fn earlier_claims(raw: &Client, scope: &KnowledgeSpaceScope, count: usize) -> Vec<Claim> {
    let mut claims = Vec::with_capacity(count);
    for index in 0..count {
        let claim = text_claim(
            scope,
            &format!("earlier-{index}"),
            "order",
            "status",
            &format!("held at customs parcel{index}"),
        );
        assert!(earlier_append(raw, scope, &claim)
            .await
            .get::<_, bool>("applied"));
        claims.push(claim);
    }
    claims
}

#[tokio::test]
async fn postgres_the_indexing_pass_makes_an_earlier_claim_searchable_when_dsn_is_set() {
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-index-pass").primary;
    let claims = earlier_claims(&raw, &scope, 3).await;
    let search = admitting_all("customs", &[]);
    let before = store
        .search(&scope, &search)
        .await
        .expect("the search answers");
    assert!(before.hits.is_empty() && before.unindexed);

    // Two rows a page: the first page is full and hands back a cursor, the
    // second reads the one row left and ends the pass.
    let first = store
        .index_claims(&scope, None, 2)
        .await
        .expect("the indexing pass answers");
    assert_eq!((first.indexed, first.skipped), (2, 0));
    let cursor = first.next.expect("a full page hands back a cursor");
    let second = store
        .index_claims(&scope, Some(&cursor), 2)
        .await
        .expect("the indexing pass answers");
    assert_eq!((second.indexed, second.skipped, second.next), (1, 0, None));

    let after = store
        .search(&scope, &search)
        .await
        .expect("the search answers");
    let mut expected: Vec<ClaimId> = claims.iter().map(|claim| claim.claim_id.clone()).collect();
    expected.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    assert_eq!(
        hit_ids(&after),
        expected,
        "every earlier claim is now a hit"
    );
    assert!(!after.unindexed);
    // A row the pass indexed holds what the append would have written.
    for claim in &claims {
        assert_eq!(
            index_columns(&raw, &scope, &claim.claim_id).await,
            Some(IndexColumns {
                tokens: Some(sorted_tokens(claim)),
                classification: Some("unclassified".to_string()),
                confidence_bps: Some(5_000),
                read_capabilities: Some(Vec::new()),
            })
        );
    }
}

#[tokio::test]
async fn postgres_the_indexing_pass_is_idempotent_when_dsn_is_set() {
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-index-again").primary;
    earlier_claims(&raw, &scope, 3).await;
    assert_eq!(index_to_the_end(&store, &scope, 2).await, (3, 0));

    let again = store
        .index_claims(&scope, None, 2)
        .await
        .expect("the indexing pass answers");
    assert_eq!((again.indexed, again.skipped, again.next), (0, 0, None));

    // A space that never held an unindexed claim answers the same.
    let empty = ConformanceScopes::unique("pg-index-empty").primary;
    let nothing = store
        .index_claims(&empty, None, 2)
        .await
        .expect("the indexing pass answers");
    assert_eq!(
        (nothing.indexed, nothing.skipped, nothing.next),
        (0, 0, None)
    );
}

#[tokio::test]
async fn postgres_the_indexing_pass_skips_a_damaged_record_when_dsn_is_set() {
    // One record this build cannot decode does not stop the pass. It is
    // counted, the rows after it are indexed, and it stays unindexed, so its
    // space keeps saying so until the record is repaired or erased.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "search_damaged").await;
    let scope = ConformanceScopes::unique("pg-index-damaged").primary;
    let mut claims = earlier_claims(&raw, &scope, 4).await;
    claims.sort_by(|a, b| a.claim_id.as_str().cmp(b.claim_id.as_str()));
    // The first row in the pass's order, so everything else comes after it.
    doctor(&raw, &scope, &claims[0].claim_id, "record = 'not json'").await;

    assert_eq!(index_to_the_end(&store, &scope, 2).await, (3, 1));
    assert_eq!(unindexed_rows(&raw, &scope).await, 1);
    let report = store
        .search(&scope, &admitting_all("customs", &[]))
        .await
        .expect("the search answers");
    assert_eq!(
        hit_ids(&report),
        claims[1..]
            .iter()
            .map(|claim| claim.claim_id.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        report.unindexed,
        "a claim the pass could not index is still reported"
    );

    schema.drop().await;
}

#[tokio::test]
async fn postgres_the_indexing_pass_reads_past_a_claim_id_this_build_refuses_when_dsn_is_set() {
    // A row whose claim id this build refuses is counted in `skipped` and
    // read past, and does not count toward the limit. It used to make a full
    // page whose last row it was an error, and every retry the same error.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "search_refused_id").await;
    let scope = ConformanceScopes::unique("pg-index-refused-id").primary;
    let mut ids: Vec<ClaimId> = earlier_claims(&raw, &scope, 3)
        .await
        .into_iter()
        .map(|claim| claim.claim_id)
        .collect();
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let first_refused = format!("{}|x", ids[0].as_str());
    let last_refused = format!("{}|x", ids[2].as_str());
    plant_refused_claim_id(&raw, &scope, &ids[0], &first_refused).await;
    plant_refused_claim_id(&raw, &scope, &ids[2], &last_refused).await;
    assert_eq!(
        claim_order(&raw, &scope).await,
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
        let page = store
            .index_claims(&scope, after.as_ref(), 1)
            .await
            .expect("a refused claim id never fails the pass");
        pages.push((page.indexed, page.skipped, page.next.clone()));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(
        pages,
        vec![
            (1, 0, Some(ids[0].clone())),
            (1, 1, Some(ids[1].clone())),
            (1, 0, Some(ids[2].clone())),
            (0, 1, None),
        ]
    );

    // A limit of zero reads one row whose id the pass reads, and reads past
    // a refused one to reach it.
    let zero = ConformanceScopes::unique("pg-index-refused-zero").primary;
    let mut ids: Vec<ClaimId> = earlier_claims(&raw, &zero, 2)
        .await
        .into_iter()
        .map(|claim| claim.claim_id)
        .collect();
    ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
    let refused = format!("{}|x", ids[0].as_str());
    plant_refused_claim_id(&raw, &zero, &ids[0], &refused).await;
    let page = store
        .index_claims(&zero, Some(&ids[0]), 0)
        .await
        .expect("a refused claim id never fails the pass");
    assert_eq!(
        (page.indexed, page.skipped, page.next),
        (1, 1, Some(ids[1].clone()))
    );

    schema.drop().await;
}

#[tokio::test]
async fn postgres_the_indexing_pass_clamps_its_limit_when_dsn_is_set() {
    // Three hundred unindexed rows the pass can read and not decode: each
    // counts as read, which is what the limit bounds.
    let Some(dsn) = dsn() else { return };
    let (schema, raw, store) = PrivateSchema::migrated(&dsn, "search_clamp").await;
    let scope = ConformanceScopes::unique("pg-index-clamp").primary;
    raw.execute(
        "INSERT INTO rakka_agent_knowledge_claim \
            (tenant, space, claim_id, operation_id, subject, predicate, trust, \
             transition_count, record) \
         SELECT $1, $2, 'claim-' || lpad(g::TEXT, 4, '0'), 'op-' || g, 's', 'p', 'proposed', 0, \
                'not json'::BYTEA \
         FROM generate_series(1, 300) AS g",
        &[&scope.tenant().as_str(), &scope.space().as_str()],
    )
    .await
    .expect("the rows are planted");

    let one = store
        .index_claims(&scope, None, 0)
        .await
        .expect("the indexing pass answers");
    assert_eq!(
        one.indexed + one.skipped,
        1,
        "a limit of zero reads one row"
    );
    assert_eq!(
        one.next.as_ref().map(ClaimId::as_str),
        Some("claim-0001"),
        "the cursor is the last row read"
    );

    let most = store
        .index_claims(&scope, None, 10_000)
        .await
        .expect("the indexing pass answers");
    assert_eq!(CLAIM_SEARCH_INDEX_PAGE_MAX_ROWS, 256);
    assert_eq!(most.indexed + most.skipped, 256);
    assert_eq!(most.next.as_ref().map(ClaimId::as_str), Some("claim-0256"));

    let rest = store
        .index_claims(&scope, most.next.as_ref(), 10_000)
        .await
        .expect("the indexing pass answers");
    assert_eq!((rest.indexed + rest.skipped, rest.next), (44, None));

    schema.drop().await;
}

#[tokio::test]
async fn postgres_a_tombstone_beats_the_indexing_pass_when_dsn_is_set() {
    // The pass read a live row; a tombstone lands before the pass writes. The
    // write's own condition refuses the row, so the withdrawn statement's
    // words never reach it and the pass does not count it.
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-index-tombstone").primary;
    let claim = earlier_claims(&raw, &scope, 1).await.remove(0);
    let words = sorted_tokens(&claim);
    store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &claim.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");

    let written = raw
        .execute(
            CLAIM_SEARCH_INDEX_WRITE_SQL,
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &claim.claim_id.as_str(),
                &words,
                &"unclassified",
                &5_000_i32,
                &Vec::<String>::new(),
            ],
        )
        .await
        .expect("the pass's write statement runs");
    assert_eq!(written, 0, "the write refuses a row that is now a stub");
    assert_eq!(
        index_columns(&raw, &scope, &claim.claim_id)
            .await
            .expect("the stub row is kept")
            .tokens,
        None
    );

    // And a row the pass already indexed is not written twice.
    let scope = ConformanceScopes::unique("pg-index-once").primary;
    let live = earlier_claims(&raw, &scope, 1).await.remove(0);
    assert_eq!(index_to_the_end(&store, &scope, 8).await, (1, 0));
    let again = raw
        .execute(
            CLAIM_SEARCH_INDEX_WRITE_SQL,
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &live.claim_id.as_str(),
                &vec!["overwritten".to_string()],
                &"unclassified",
                &5_000_i32,
                &Vec::<String>::new(),
            ],
        )
        .await
        .expect("the pass's write statement runs");
    assert_eq!(again, 0, "the write refuses a row that is already indexed");
    assert_eq!(
        index_columns(&raw, &scope, &live.claim_id)
            .await
            .and_then(|columns| columns.tokens),
        Some(sorted_tokens(&live))
    );
}

#[tokio::test]
async fn postgres_two_indexing_passes_at_once_index_every_claim_once_when_dsn_is_set() {
    // Two nodes run the pass over one space at the same moment. Neither
    // errors, every claim ends indexed, and no claim is counted twice.
    let Some(dsn) = dsn() else { return };
    let Some((raw, _store)) = shared().await else {
        return;
    };
    let scope = ConformanceScopes::unique("pg-index-two").primary;
    earlier_claims(&raw, &scope, 40).await;

    let mut passes = Vec::new();
    for _ in 0..2 {
        let store = PostgresKnowledgeGraphStore::new(connect(&dsn).await);
        let scope = scope.clone();
        passes.push(tokio::spawn(async move {
            index_to_the_end(&store, &scope, 7).await
        }));
    }
    let mut indexed = 0;
    for pass in passes {
        let (pass_indexed, pass_skipped) = pass.await.expect("the pass runs");
        assert_eq!(pass_skipped, 0);
        indexed += pass_indexed;
    }
    assert_eq!(indexed, 40, "every claim is indexed by exactly one pass");
    assert_eq!(unindexed_rows(&raw, &scope).await, 0);
}

#[tokio::test]
async fn postgres_the_indexing_pass_stays_in_its_scope_when_dsn_is_set() {
    let Some((raw, store)) = shared().await else {
        return;
    };
    let scopes = ConformanceScopes::unique("pg-index-scope");
    let other_space = KnowledgeSpaceScope::new(
        scopes.primary.tenant().clone(),
        rakka_agent::KnowledgeSpaceId::new("space-other").expect("the space id is valid"),
    )
    .expect("the scope is valid");
    earlier_claims(&raw, &scopes.primary, 2).await;
    earlier_claims(&raw, &scopes.foreign, 2).await;
    earlier_claims(&raw, &other_space, 2).await;

    assert_eq!(index_to_the_end(&store, &scopes.primary, 8).await, (2, 0));

    assert_eq!(unindexed_rows(&raw, &scopes.primary).await, 0);
    assert_eq!(
        unindexed_rows(&raw, &scopes.foreign).await,
        2,
        "another tenant's claims are not indexed by this pass"
    );
    assert_eq!(
        unindexed_rows(&raw, &other_space).await,
        2,
        "another space's claims are not indexed by this pass"
    );
}
