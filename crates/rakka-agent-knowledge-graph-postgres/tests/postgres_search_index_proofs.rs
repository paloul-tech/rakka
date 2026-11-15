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

use rakka_agent::{AgentId, AgentTaskContent, MemoryClassification};
use rakka_agent_knowledge_graph::conformance::{conformance_resolver, ConformanceScopes};
use rakka_agent_knowledge_graph::{
    Claim, ClaimId, ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate, ClaimProvenance,
    ClaimTombstoneReason, ClaimTombstoneRequest, ClaimTransitionOutcome, ClaimTrustStatus,
    ClaimTrustTransition, ClaimTrustTransitionRequest, KnowledgeSpaceScope, CLAIM_TOMBSTONE_MARKER,
};
use rakka_agent_knowledge_graph_postgres::{
    PostgresKnowledgeGraphStore, SEARCH_TOKENS_INDEX_NAME, SEARCH_TRIGGER_NAME,
    TOMBSTONE_FENCE_TRANSITION_COUNT, UNINDEXED_INDEX_NAME,
};
use rakka_agent_workflow::AgentTimestampMillis;
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
