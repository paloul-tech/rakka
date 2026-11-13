//! What only a live database can prove about tombstone and delete.
//!
//! [Specification 13.1](../../../docs/plans/rakka-agent/spec.md): every memory
//! API provides retention, tombstone, and deletion semantics. The conformance
//! suite proves the shared semantics on this backend; this file proves what
//! needs real rows, real concurrency, and — above all — a real *earlier
//! binary*: that the schema migrates under one, that one still appends
//! afterwards, and that nothing one has in flight can write a withdrawn
//! statement back or restore a deleted claim.
//!
//! The earlier binary is represented by the three statements it runs, frozen
//! below byte for byte from `d12b0f8` (the last commit before tombstones
//! existed) and bound exactly as that commit's store bound them. They are
//! literals on purpose: a proof that imported today's constants would only
//! prove today's build agrees with itself.
//!
//! Every test is gated on `RAKKA_POSTGRES_TEST_DSN` and passes silently
//! without it.

use rakka_agent::{AgentId, AgentTaskContent, MemoryClassification};
use rakka_agent_knowledge_graph::conformance::{
    conformance_claim, conformance_resolver, ConformanceScopes,
};
use rakka_agent_knowledge_graph::{
    Claim, ClaimCursor, ClaimDeleteExpectation, ClaimDeleteRequest, ClaimFilter, ClaimId,
    ClaimNodeId, ClaimObject, ClaimOperationId, ClaimPredicate, ClaimPromotionPolicy,
    ClaimProvenance, ClaimTombstoneReason, ClaimTombstoneRequest, ClaimTransitionCursor,
    ClaimTransitionOutcome, ClaimTrustStatus, ClaimTrustTransition, ClaimTrustTransitionRequest,
    KnowledgeGraphStore, KnowledgeSpaceScope, CLAIM_TOMBSTONE_MARKER,
};
use rakka_agent_knowledge_graph_postgres::{
    PostgresKnowledgeGraphStore, MIGRATION_SQL, MIGRATION_VALIDATE_SQL,
    TOMBSTONE_FENCE_TRANSITION_COUNT,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, Row};

/// `MIGRATION_SQL` at `d12b0f8`, byte for byte.
const MIGRATION_SQL_D12B0F8: &str = r#"
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
    kind TEXT NOT NULL CHECK (kind IN ('append', 'transition')),
    result BYTEA NOT NULL,
    PRIMARY KEY (tenant, space, operation_id)
);
"#;

/// `APPEND_SQL` at `d12b0f8`, byte for byte.
const APPEND_SQL_D12B0F8: &str = r#"
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
    ON CONFLICT (tenant, space, operation_id) DO NOTHING
)
SELECT EXISTS (SELECT 1 FROM existing_op) AS replayed,
       (SELECT kind   FROM existing_op)   AS replay_kind,
       (SELECT result FROM existing_op)   AS replay_result,
       EXISTS (SELECT 1 FROM ins_claim)   AS applied
"#;

/// `TRANSITION_SQL` at `d12b0f8`, byte for byte.
const TRANSITION_SQL_D12B0F8: &str = r#"
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

/// Three strings no identifier, digest or marker contains, so finding one in a
/// row means the statement's own bytes are there.
const SUBJECT: &str = "subj-zebra-7431";
const PREDICATE: &str = "pred-quokka-9182";
const OBJECT: &str = "obj-narwhal-5527";
/// A fourth, carried by the claim's evidence reference.
const EVIDENCE: &str = "evid-ocelot-3310";
const STATEMENT_STRINGS: [&str; 4] = [SUBJECT, PREDICATE, OBJECT, EVIDENCE];

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(10);

/// How many times the tombstone/transition race is run.
const TOMBSTONE_RACE_RUNS: u32 = 48;

/// How many times the delete/transition race is run.
const DELETE_RACE_RUNS: u32 = 48;

/// How many times each same-operation race is run.
const SAME_OPERATION_RACE_RUNS: u32 = 64;

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

async fn client() -> Option<Client> {
    Some(connect(&dsn()?).await)
}

async fn store() -> Option<PostgresKnowledgeGraphStore> {
    let store = PostgresKnowledgeGraphStore::new(client().await?);
    store.migrate().await.expect("the schema applies");
    Some(store)
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

fn delete_request(
    scope: &KnowledgeSpaceScope,
    claim_id: &ClaimId,
    discriminator: &str,
    expected: Option<ClaimDeleteExpectation>,
) -> ClaimDeleteRequest {
    ClaimDeleteRequest {
        claim_id: claim_id.clone(),
        operation_id: ClaimOperationId::derive_delete(scope, claim_id, discriminator)
            .expect("the operation id derives"),
        expected,
    }
}

/// An attribute claim whose subject, predicate, value and evidence are the
/// four distinctive strings.
fn distinctive_claim(scope: &KnowledgeSpaceScope, discriminator: &str) -> Claim {
    Claim::new(
        scope,
        ClaimOperationId::derive_append(scope, discriminator).expect("the operation id derives"),
        ClaimNodeId::new(SUBJECT).expect("the node id is valid"),
        ClaimPredicate::new(PREDICATE).expect("the predicate is valid"),
        ClaimObject::Value(
            AgentTaskContent::inline(serde_json::json!({ "text": OBJECT }))
                .expect("the value is inline-bounded"),
        ),
        ClaimProvenance::for_agent(AgentId::new("scout").expect("the agent id is valid")),
        5_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the claim is valid")
    .with_evidence(vec![ArtifactRef {
        artifact_id: EVIDENCE.to_string(),
        kind: ArtifactKind::File,
        uri: format!("s3://evidence/{EVIDENCE}"),
        checksum: None,
        content_type: None,
        byte_len: None,
        retention_class: None,
        encryption: None,
        redaction: RedactionStatus::Unredacted,
        created_at: AgentTimestampMillis::new(1),
        metadata: AgentAttributes::default(),
    }])
    .expect("the evidence is within bounds")
}

/// Runs the `d12b0f8` append statement, bound as that commit's `append` bound
/// it. A live claim encodes to the same bytes in both builds.
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
    raw.query_one(APPEND_SQL_D12B0F8, &params)
        .await
        .expect("the earlier append statement runs")
}

/// Runs the `d12b0f8` transition statement, bound as that commit's
/// `transition` bound it from a read of `read`: the fence is the transition
/// count that read saw, and the record written is `read` with its statement
/// intact and the transition applied.
async fn earlier_transition(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    read: &Claim,
    request: &ClaimTrustTransitionRequest,
) -> Row {
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
    raw.query_one(TRANSITION_SQL_D12B0F8, &params)
        .await
        .expect("the earlier transition statement runs")
}

/// Every row one scope holds in the three tables, each flattened to its text
/// columns followed by its raw `BYTEA` payload. Comparable for byte identity,
/// and searchable for a statement's bytes wherever they could hide.
async fn scope_rows(raw: &Client, scope: &KnowledgeSpaceScope) -> Vec<Vec<u8>> {
    let key: [&(dyn ToSql + Sync); 2] = [&scope.tenant().as_str(), &scope.space().as_str()];
    let mut rows = Vec::new();
    for row in raw
        .query(
            "SELECT claim_id, operation_id, subject, predicate, \
                    COALESCE(object_node, '<null>') AS object_node, trust, \
                    transition_count, tombstoned, record \
             FROM rakka_agent_knowledge_claim \
             WHERE tenant = $1 AND space = $2 ORDER BY claim_id",
            &key,
        )
        .await
        .expect("the claim rows read")
    {
        let mut flat = format!(
            "claim|{}|{}|{}|{}|{}|{}|{}|{}|",
            row.get::<_, String>("claim_id"),
            row.get::<_, String>("operation_id"),
            row.get::<_, String>("subject"),
            row.get::<_, String>("predicate"),
            row.get::<_, String>("object_node"),
            row.get::<_, String>("trust"),
            row.get::<_, i32>("transition_count"),
            row.get::<_, bool>("tombstoned"),
        )
        .into_bytes();
        flat.extend(row.get::<_, Vec<u8>>("record"));
        rows.push(flat);
    }
    for row in raw
        .query(
            "SELECT claim_id, ordinal, record FROM rakka_agent_knowledge_claim_transition \
             WHERE tenant = $1 AND space = $2 ORDER BY claim_id, ordinal",
            &key,
        )
        .await
        .expect("the transition rows read")
    {
        let mut flat = format!(
            "transition|{}|{}|",
            row.get::<_, String>("claim_id"),
            row.get::<_, i32>("ordinal"),
        )
        .into_bytes();
        flat.extend(row.get::<_, Vec<u8>>("record"));
        rows.push(flat);
    }
    for row in raw
        .query(
            "SELECT operation_id, kind, result FROM rakka_agent_knowledge_claim_op \
             WHERE tenant = $1 AND space = $2 ORDER BY operation_id",
            &key,
        )
        .await
        .expect("the ledger rows read")
    {
        let mut flat = format!(
            "op|{}|{}|",
            row.get::<_, String>("operation_id"),
            row.get::<_, String>("kind"),
        )
        .into_bytes();
        flat.extend(row.get::<_, Vec<u8>>("result"));
        rows.push(flat);
    }
    rows
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

/// The statement strings still present anywhere in the scope's rows.
fn statement_strings_in(rows: &[Vec<u8>]) -> Vec<&'static str> {
    STATEMENT_STRINGS
        .into_iter()
        .filter(|needle| rows.iter().any(|row| contains(row, needle)))
        .collect()
}

/// The ledger row of one operation: its kind and its payload.
async fn ledger_row(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    operation_id: &ClaimOperationId,
) -> Option<(String, Vec<u8>)> {
    raw.query_opt(
        "SELECT kind, result FROM rakka_agent_knowledge_claim_op \
         WHERE tenant = $1 AND space = $2 AND operation_id = $3",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &operation_id.as_str(),
        ],
    )
    .await
    .expect("the ledger row reads")
    .map(|row| (row.get("kind"), row.get("result")))
}

/// The storage file of each of the three tables: a rewrite changes it.
async fn table_filenodes(client: &Client) -> Vec<(String, i64)> {
    client
        .query(
            "SELECT relname::TEXT, relfilenode::BIGINT FROM pg_class \
             WHERE oid IN ('rakka_agent_knowledge_claim'::regclass, \
                           'rakka_agent_knowledge_claim_transition'::regclass, \
                           'rakka_agent_knowledge_claim_op'::regclass) \
             ORDER BY relname",
            &[],
        )
        .await
        .expect("the catalog reads")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Every constraint of the claim and ledger tables, with its definition and
/// its catalog identity: a replaced constraint has a new oid.
async fn table_constraints(client: &Client) -> Vec<(String, String, String, i64)> {
    client
        .query(
            "SELECT conrelid::regclass::TEXT, conname::TEXT, pg_get_constraintdef(oid), \
                    oid::BIGINT \
             FROM pg_constraint \
             WHERE conrelid IN ('rakka_agent_knowledge_claim'::regclass, \
                                'rakka_agent_knowledge_claim_op'::regclass) \
             ORDER BY 1, 2",
            &[],
        )
        .await
        .expect("the catalog reads")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect()
}

/// Inserts one content-free ledger row of the given kind.
async fn probe_ledger_kind(
    raw: &Client,
    scope: &KnowledgeSpaceScope,
    kind: &str,
) -> Result<u64, tokio_postgres::Error> {
    raw.execute(
        "INSERT INTO rakka_agent_knowledge_claim_op (tenant, space, operation_id, kind, result) \
         VALUES ($1, $2, $3, $4, ''::BYTEA)",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &format!("probe-{kind}"),
            &kind,
        ],
    )
    .await
}

/// Rewrites one claim row's columns by hand, as corruption would.
async fn doctor(raw: &Client, scope: &KnowledgeSpaceScope, claim_id: &ClaimId, sql: &str) {
    let changed = raw
        .execute(
            sql,
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
async fn postgres_migrates_the_earlier_schema_with_rows_when_dsn_is_set() {
    // The tables exactly as `d12b0f8` created them, holding rows that commit's
    // statements wrote, migrate in place: every row reads back unchanged, the
    // new column and the wider ledger check are there, and a second run
    // rewrites nothing.
    let Some(dsn) = dsn() else { return };
    let setup = connect(&dsn).await;
    let schema = unique_schema("earlier_schema");
    setup
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("the private schema is created");

    let raw = client_in(&dsn, &schema).await;
    raw.batch_execute(MIGRATION_SQL_D12B0F8)
        .await
        .expect("the earlier schema applies");

    let scope = ConformanceScopes::unique("pg-earlier-schema").primary;
    let moved = conformance_claim(&scope, "moved", "node-a", "node-b");
    let untouched = distinctive_claim(&scope, "untouched");
    for claim in [&moved, &untouched] {
        let row = earlier_append(&raw, &scope, claim).await;
        assert!(row.get::<_, bool>("applied"), "the earlier append applies");
    }
    let dispute = transition_request(
        &scope,
        &moved.claim_id,
        "dispute",
        ClaimTrustStatus::Disputed,
    );
    let row = earlier_transition(&raw, &scope, &moved, &dispute).await;
    assert!(
        row.get::<_, bool>("applied"),
        "the earlier transition applies"
    );

    let before_filenodes = table_filenodes(&raw).await;

    let store = PostgresKnowledgeGraphStore::new(client_in(&dsn, &schema).await);
    store.migrate().await.expect("the earlier schema migrates");
    assert_eq!(
        kind_check_validated(&raw).await,
        Some(true),
        "a full migration leaves the wider check validated"
    );
    let migrated_filenodes = table_filenodes(&raw).await;
    let migrated_constraints = table_constraints(&raw).await;
    let migrated_rows = scope_rows(&raw, &scope).await;
    assert_eq!(
        migrated_filenodes, before_filenodes,
        "the migration rewrites no table: the new column has a constant default"
    );

    store
        .migrate()
        .await
        .expect("a second migration is a no-op");
    assert_eq!(table_filenodes(&raw).await, migrated_filenodes);
    assert_eq!(
        table_constraints(&raw).await,
        migrated_constraints,
        "a second migration replaces no constraint"
    );
    assert_eq!(scope_rows(&raw, &scope).await, migrated_rows);

    // Through the new store, the rows the earlier binary wrote are the claims
    // it wrote.
    let read = store
        .get(&scope, &moved.claim_id)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    assert_eq!(
        read,
        moved
            .apply_transition(ClaimTrustStatus::Disputed)
            .expect("the transition applies")
    );
    assert_eq!(
        store
            .get(&scope, &untouched.claim_id)
            .await
            .expect("the claim reads")
            .expect("the claim exists"),
        untouched
    );
    let history = store
        .transitions(&scope, &moved.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    assert_eq!(history.transitions.len(), 1);
    assert_eq!(history.transitions[0].operation_id, dispute.operation_id);
    let listed = store
        .query(&scope, &ClaimFilter::matching_all(), ClaimCursor::start())
        .await
        .expect("the query answers");
    assert_eq!(listed.claims.len(), 2);

    // The migrated schema takes new writes of every kind.
    let fresh = conformance_claim(&scope, "fresh", "node-a", "node-c");
    store
        .append(&scope, &fresh)
        .await
        .expect("a new append works on the migrated schema");
    let stub = store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &untouched.claim_id, "withdraw"),
        )
        .await
        .expect("a tombstone works on the migrated schema");
    assert!(stub.is_tombstoned());

    // The ledger's check now admits the erased kind, and still refuses a kind
    // nobody defined.
    probe_ledger_kind(&raw, &scope, "erased")
        .await
        .expect("the migrated ledger accepts an erased row");
    probe_ledger_kind(&raw, &scope, "delete")
        .await
        .expect("the migrated ledger accepts a delete row");
    assert!(
        probe_ledger_kind(&raw, &scope, "resurrect").await.is_err(),
        "the migrated ledger still refuses an undefined kind"
    );

    setup
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .expect("the private schema is dropped");
}

#[tokio::test]
async fn postgres_an_earlier_binary_still_appends_after_the_migration_when_dsn_is_set() {
    // The roll order is migrate first, then upgrade binaries: so the statements
    // of a binary that has not been upgraded yet must keep working against the
    // migrated schema. Its insert does not name the new column, which takes
    // its default.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scope = ConformanceScopes::unique("pg-earlier-append").primary;

    let claim = conformance_claim(&scope, "earlier", "node-a", "node-b");
    let row = earlier_append(&raw, &scope, &claim).await;
    assert!(!row.get::<_, bool>("replayed"));
    assert!(
        row.get::<_, bool>("applied"),
        "the earlier append statement applies against the migrated schema"
    );
    let read = store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the new build reads the row")
        .expect("the claim exists");
    assert_eq!(read, claim);
    assert!(!read.is_tombstoned(), "the row reads as a live claim");
    // Not vacuous: the schema this ran against is the migrated one, and the
    // column the earlier statement never named took its default.
    let row = raw
        .query_one(
            "SELECT tombstoned FROM rakka_agent_knowledge_claim \
             WHERE tenant = $1 AND space = $2 AND claim_id = $3",
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &claim.claim_id.as_str(),
            ],
        )
        .await
        .expect("the migrated column reads");
    assert!(!row.get::<_, bool>("tombstoned"));

    // Its transition statement keeps working on a live claim too.
    let dispute = transition_request(
        &scope,
        &claim.claim_id,
        "dispute",
        ClaimTrustStatus::Disputed,
    );
    let row = earlier_transition(&raw, &scope, &claim, &dispute).await;
    assert!(row.get::<_, bool>("applied"));
    let read = store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the new build reads the row")
        .expect("the claim exists");
    assert_eq!(read.trust(), ClaimTrustStatus::Disputed);
    // And the new build replays what the earlier one wrote.
    assert_eq!(
        store
            .append(&scope, &claim)
            .await
            .expect("the earlier append replays"),
        claim
    );
}

#[tokio::test]
async fn postgres_a_stub_row_holds_markers_and_the_fence_when_dsn_is_set() {
    // The denormalized columns of a stub row say what the record says — with
    // one stated exception: `transition_count` holds the fence value, which no
    // reader can hold as an expectation, while the record keeps the true count.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scope = ConformanceScopes::unique("pg-stub-row").primary;
    let claim = conformance_claim(&scope, "stub", "node-a", "node-b");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
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
        .expect("the dispute applies");
    let stub = store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &claim.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");

    let row = raw
        .query_one(
            "SELECT subject, predicate, object_node, tombstoned, transition_count, trust, record \
             FROM rakka_agent_knowledge_claim \
             WHERE tenant = $1 AND space = $2 AND claim_id = $3",
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &claim.claim_id.as_str(),
            ],
        )
        .await
        .expect("the stub row reads");
    assert_eq!(row.get::<_, String>("subject"), CLAIM_TOMBSTONE_MARKER);
    assert_eq!(row.get::<_, String>("predicate"), CLAIM_TOMBSTONE_MARKER);
    assert_eq!(row.get::<_, Option<String>>("object_node"), None);
    assert!(row.get::<_, bool>("tombstoned"));
    assert_eq!(
        row.get::<_, i32>("transition_count"),
        TOMBSTONE_FENCE_TRANSITION_COUNT
    );
    assert_eq!(TOMBSTONE_FENCE_TRANSITION_COUNT, i32::MAX);
    assert_eq!(
        row.get::<_, String>("trust"),
        "disputed",
        "a tombstone does not move the trust column"
    );
    let record: Claim =
        serde_json::from_slice(&row.get::<_, Vec<u8>>("record")).expect("the stub record decodes");
    assert_eq!(record, stub);
    assert_eq!(
        record.transition_count(),
        1,
        "the record keeps the true transition count"
    );

    // A `tombstoned` column that disagrees with the record is drift, refused on
    // every read rather than preferred either way.
    doctor(
        &raw,
        &scope,
        &claim.claim_id,
        "UPDATE rakka_agent_knowledge_claim SET tombstoned = FALSE \
         WHERE tenant = $1 AND space = $2 AND claim_id = $3",
    )
    .await;
    assert_eq!(
        store
            .get(&scope, &claim.claim_id)
            .await
            .expect_err("a stub record under a live column is drift")
            .code(),
        "claim-backend-failed"
    );
    assert_eq!(
        store
            .query(
                &scope,
                &ClaimFilter::matching_all().including_tombstoned(),
                ClaimCursor::start()
            )
            .await
            .expect_err("a query over the drifted row is refused")
            .code(),
        "claim-backend-failed"
    );
    doctor(
        &raw,
        &scope,
        &claim.claim_id,
        "UPDATE rakka_agent_knowledge_claim SET tombstoned = TRUE \
         WHERE tenant = $1 AND space = $2 AND claim_id = $3",
    )
    .await;
    assert_eq!(
        store
            .get(&scope, &claim.claim_id)
            .await
            .expect("the repaired row reads"),
        Some(stub)
    );

    // A stub row that lost its fence value is drift too: with the true count
    // back in the column, an earlier binary's transition could match it again.
    doctor(
        &raw,
        &scope,
        &claim.claim_id,
        "UPDATE rakka_agent_knowledge_claim SET transition_count = 1 \
         WHERE tenant = $1 AND space = $2 AND claim_id = $3",
    )
    .await;
    assert_eq!(
        store
            .transition(
                &scope,
                &transition_request(
                    &scope,
                    &claim.claim_id,
                    "retract",
                    ClaimTrustStatus::Retracted
                ),
                &ClaimPromotionPolicy::ungated(),
                NOW,
            )
            .await
            .expect_err("a stub row without its fence is drift")
            .code(),
        "claim-backend-failed"
    );
}

#[tokio::test]
async fn postgres_tombstone_leaves_no_statement_bytes_when_dsn_is_set() {
    // After a tombstone, the statement is in no column and no payload of any
    // row of the scope: not the claim row, not the transition rows, and not
    // the ledger rows that used to replay the full claim.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scope = ConformanceScopes::unique("pg-tombstone-bytes").primary;

    let claim = distinctive_claim(&scope, "withdrawn");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    let mut dispute = transition_request(
        &scope,
        &claim.claim_id,
        "dispute",
        ClaimTrustStatus::Disputed,
    );
    dispute.reason = Some("reviewed and challenged".to_string());
    store
        .transition(&scope, &dispute, &ClaimPromotionPolicy::ungated(), NOW)
        .await
        .expect("the dispute applies");

    // Not vacuous: before the tombstone every string is there — in the claim
    // row, and again in the two ledger rows that replay it.
    let before = scope_rows(&raw, &scope).await;
    assert_eq!(statement_strings_in(&before), STATEMENT_STRINGS);
    for needle in STATEMENT_STRINGS {
        let holders = before.iter().filter(|row| contains(row, needle)).count();
        assert!(
            holders >= 3,
            "{needle} is held by the claim row and both ledger rows, found {holders}"
        );
    }

    let stub = store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &claim.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");

    let after = scope_rows(&raw, &scope).await;
    assert_eq!(
        statement_strings_in(&after),
        Vec::<&str>::new(),
        "a tombstone leaves the statement's bytes nowhere in the scope"
    );
    assert_eq!(
        after.len(),
        before.len() + 1,
        "every row is kept, and the tombstone adds its own ledger row"
    );

    // The erased ledger rows keep their operation ids and hold nothing.
    for operation_id in [&claim.operation_id, &dispute.operation_id] {
        let (kind, result) = ledger_row(&raw, &scope, operation_id)
            .await
            .expect("an erased ledger row is kept");
        assert_eq!(kind, "erased");
        assert!(result.is_empty(), "an erased ledger row holds no payload");
    }
    let (kind, result) = ledger_row(
        &raw,
        &scope,
        &stub.tombstone().expect("a stub").operation_id,
    )
    .await
    .expect("the tombstone has its ledger row");
    assert_eq!(kind, "tombstone");
    let replayable: Claim = serde_json::from_slice(&result).expect("the stub replays");
    assert_eq!(replayable, stub);

    // The history is still there, and still lists the transition.
    let history = store
        .transitions(&scope, &claim.claim_id, ClaimTransitionCursor::start())
        .await
        .expect("the history lists");
    assert_eq!(history.transitions.len(), 1);
    assert_eq!(history.transitions[0].reason, dispute.reason);
}

#[tokio::test]
async fn postgres_an_earlier_transition_statement_cannot_write_over_a_stub_when_dsn_is_set() {
    // The hole the fence value closes. An earlier binary reads a live claim,
    // a tombstone commits, and the earlier binary then runs its transition
    // statement — fenced only on the transition count it read, which a
    // tombstone does not change. Were the stub row still holding that count,
    // the statement would match it and write the full statement back, with a
    // transition row and a ledger row that embeds the claim.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scope = ConformanceScopes::unique("pg-earlier-over-stub").primary;

    let claim = distinctive_claim(&scope, "withdrawn");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    // The earlier binary's read: the live claim, at transition count zero.
    let read_by_the_earlier_binary = store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    assert_eq!(read_by_the_earlier_binary.transition_count(), 0);

    store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &claim.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");
    let before = scope_rows(&raw, &scope).await;
    assert_eq!(statement_strings_in(&before), Vec::<&str>::new());

    let in_flight = transition_request(
        &scope,
        &claim.claim_id,
        "in-flight",
        ClaimTrustStatus::Disputed,
    );
    let row = earlier_transition(&raw, &scope, &read_by_the_earlier_binary, &in_flight).await;
    assert!(!row.get::<_, bool>("replayed"));
    assert!(
        !row.get::<_, bool>("applied"),
        "the earlier transition statement matches nothing: the stub row no longer holds the count it read"
    );

    let after = scope_rows(&raw, &scope).await;
    assert_eq!(
        after, before,
        "the claim row, the transition rows and the ledger rows are byte-identical"
    );
    assert_eq!(statement_strings_in(&after), Vec::<&str>::new());
    assert!(ledger_row(&raw, &scope, &in_flight.operation_id)
        .await
        .is_none());
}

#[tokio::test]
async fn postgres_concurrent_tombstone_and_transition_stay_coherent_when_dsn_is_set() {
    // Two connections race a tombstone and a transition on one claim. Every
    // run ends in one of the two serial outcomes, never a mixture: either the
    // transition landed first and the stub lists it with its ledger row
    // erased, or the tombstone landed first and the transition was refused.
    // Either way the statement is in no row.
    let Some(store_a) = store().await else { return };
    let store_b = store().await.expect("a second connection opens");
    let raw = client().await.expect("a raw connection opens");
    let policy = ClaimPromotionPolicy::ungated();
    let mut transition_first = 0_u32;
    let mut tombstone_first = 0_u32;

    for run in 0..TOMBSTONE_RACE_RUNS {
        let scope = ConformanceScopes::unique("pg-race-tombstone").primary;
        let claim = distinctive_claim(&scope, &format!("race-{run}"));
        store_a
            .append(&scope, &claim)
            .await
            .expect("the claim appends");
        let dispute = transition_request(
            &scope,
            &claim.claim_id,
            "dispute",
            ClaimTrustStatus::Disputed,
        );
        let tombstone = tombstone_request(&scope, &claim.claim_id, "withdraw");

        // A tombstone makes one more round trip than a transition before its
        // write, so started together the transition always wins. Holding the
        // transition back mixes the orders. The step is nominal: a runtime
        // timer is far coarser than 40 microseconds, so each non-zero delay
        // is really "up to about a millisecond, by timer phase". That is
        // enough to produce both orders; neither is asserted to occur.
        let head_start = std::time::Duration::from_micros(u64::from(run % 8) * 40);
        let (tombstoned, transitioned) =
            tokio::join!(store_a.tombstone(&scope, &tombstone), async {
                tokio::time::sleep(head_start).await;
                store_b.transition(&scope, &dispute, &policy, NOW).await
            },);
        let stub = tombstoned.expect("the tombstone always lands: it retries a lost race");
        let history = store_a
            .transitions(&scope, &claim.claim_id, ClaimTransitionCursor::start())
            .await
            .expect("the history lists");
        let stored = store_a
            .get(&scope, &claim.claim_id)
            .await
            .expect("the claim reads")
            .expect("the stub exists");
        assert_eq!(stored, stub);
        assert!(stored.is_tombstoned());

        match transitioned {
            Ok(outcome) => {
                transition_first += 1;
                assert_eq!(outcome.claim.trust(), ClaimTrustStatus::Disputed);
                assert_eq!(
                    history.transitions.len(),
                    1,
                    "the stub lists the transition"
                );
                assert_eq!(stored.trust(), ClaimTrustStatus::Disputed);
                assert_eq!(stored.transition_count(), 1);
                let (kind, result) = ledger_row(&raw, &scope, &dispute.operation_id)
                    .await
                    .expect("the transition's ledger row is kept");
                assert_eq!(
                    kind, "erased",
                    "the tombstone erased the transition it followed"
                );
                assert!(result.is_empty());
            }
            Err(error) => {
                tombstone_first += 1;
                assert_eq!(
                    error.code(),
                    "claim-tombstoned",
                    "a transition that lost to the tombstone is refused as such: {error}"
                );
                assert!(history.transitions.is_empty());
                assert_eq!(stored.trust(), ClaimTrustStatus::Proposed);
                assert!(ledger_row(&raw, &scope, &dispute.operation_id)
                    .await
                    .is_none());
            }
        }
        assert_eq!(
            statement_strings_in(&scope_rows(&raw, &scope).await),
            Vec::<&str>::new(),
            "run {run}: the statement survives in no row, whichever writer won"
        );
    }
    eprintln!(
        "tombstone/transition race: transition first {transition_first}, tombstone first {tombstone_first}"
    );
}

#[tokio::test]
async fn postgres_delete_leaves_no_statement_bytes_when_dsn_is_set() {
    // A delete removes the transition rows too, so — unlike a tombstone — it
    // also reaches a transition reason that quoted the statement. Afterwards
    // the scope's rows hold the statement nowhere, whether the claim was
    // deleted live or as a stub.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let policy = ClaimPromotionPolicy::ungated();

    for tombstone_first in [false, true] {
        let scope = ConformanceScopes::unique("pg-delete-bytes").primary;
        let claim = distinctive_claim(&scope, "erased");
        store
            .append(&scope, &claim)
            .await
            .expect("the claim appends");
        let mut dispute = transition_request(
            &scope,
            &claim.claim_id,
            "dispute",
            ClaimTrustStatus::Disputed,
        );
        dispute.reason = Some(format!("the reviewer quoted {SUBJECT} back"));
        store
            .transition(&scope, &dispute, &policy, NOW)
            .await
            .expect("the dispute applies");
        let mut spent = vec![claim.operation_id.clone(), dispute.operation_id.clone()];

        if tombstone_first {
            let tombstone = tombstone_request(&scope, &claim.claim_id, "withdraw");
            store
                .tombstone(&scope, &tombstone)
                .await
                .expect("the tombstone applies");
            spent.push(tombstone.operation_id);
            // The stated exposure of a tombstone: it keeps the transition
            // history whole, reason included. Only the delete below reaches it.
            assert_eq!(
                statement_strings_in(&scope_rows(&raw, &scope).await),
                vec![SUBJECT],
                "after a tombstone only the transition reason still quotes the statement"
            );
        } else {
            assert_eq!(
                statement_strings_in(&scope_rows(&raw, &scope).await),
                STATEMENT_STRINGS
            );
        }

        let delete = delete_request(&scope, &claim.claim_id, "erase", None);
        store
            .delete(&scope, &delete)
            .await
            .expect("the delete applies");

        let after = scope_rows(&raw, &scope).await;
        assert_eq!(
            statement_strings_in(&after),
            Vec::<&str>::new(),
            "a delete leaves the statement's bytes nowhere in the scope"
        );
        assert_eq!(
            after.len(),
            spent.len() + 1,
            "only ledger rows remain: one per spent operation, and the delete's own"
        );
        assert!(
            after.iter().all(|row| row.starts_with(b"op|")),
            "the claim row and every transition row are gone"
        );
        for operation_id in &spent {
            let (kind, result) = ledger_row(&raw, &scope, operation_id)
                .await
                .expect("an erased ledger row is kept");
            assert_eq!(kind, "erased");
            assert!(result.is_empty());
        }
        let (kind, result) = ledger_row(&raw, &scope, &delete.operation_id)
            .await
            .expect("the delete has its ledger row");
        assert_eq!(kind, "delete");
        assert!(result.is_empty(), "a delete's ledger row holds no payload");

        assert!(store
            .get(&scope, &claim.claim_id)
            .await
            .expect("the read answers")
            .is_none());
        assert!(store
            .transitions(&scope, &claim.claim_id, ClaimTransitionCursor::start())
            .await
            .expect("the history lists")
            .transitions
            .is_empty());
    }
}

#[tokio::test]
async fn postgres_an_earlier_binary_replaying_an_erased_append_cannot_restore_it_when_dsn_is_set() {
    // The anti-goal this whole design turns on: a replayed append must not
    // restore a deleted claim. An earlier binary retries the append it never
    // got an answer for, with the original record. Its statement consults the
    // ledger first and finds the row still there — erased, not removed — so
    // it inserts nothing.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scope = ConformanceScopes::unique("pg-earlier-replay").primary;

    let claim = distinctive_claim(&scope, "erased");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    store
        .delete(
            &scope,
            &delete_request(&scope, &claim.claim_id, "erase", None),
        )
        .await
        .expect("the delete applies");
    let before = scope_rows(&raw, &scope).await;

    let row = earlier_append(&raw, &scope, &claim).await;
    assert!(
        row.get::<_, bool>("replayed"),
        "the earlier statement finds the erased ledger row"
    );
    assert!(
        !row.get::<_, bool>("applied"),
        "the earlier statement inserts nothing"
    );
    // What `replayed_append` at `d12b0f8` does with this row: a kind other
    // than `append` is `claim-operation-conflict`, and the payload is read as
    // non-null bytes, so it neither panics nor decodes a claim.
    let kind: Option<String> = row.get("replay_kind");
    assert_eq!(kind.as_deref(), Some("erased"));
    assert_ne!(kind.as_deref(), Some("append"));
    let result: Option<Vec<u8>> = row.get("replay_result");
    assert_eq!(
        result,
        Some(Vec::new()),
        "the erased payload is empty bytes, never NULL"
    );

    assert_eq!(
        scope_rows(&raw, &scope).await,
        before,
        "the replay changed no row"
    );
    assert!(
        store
            .get(&scope, &claim.claim_id)
            .await
            .expect("the read answers")
            .is_none(),
        "the claim was not restored"
    );
    assert_eq!(
        statement_strings_in(&scope_rows(&raw, &scope).await),
        Vec::<&str>::new()
    );

    // The same holds over a stub: the replay cannot put the statement back
    // beside a tombstone either.
    let withdrawn = distinctive_claim(&scope, "withdrawn");
    store
        .append(&scope, &withdrawn)
        .await
        .expect("the claim appends");
    let stub = store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &withdrawn.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");
    let row = earlier_append(&raw, &scope, &withdrawn).await;
    assert!(row.get::<_, bool>("replayed"));
    assert!(!row.get::<_, bool>("applied"));
    assert_eq!(
        row.get::<_, Option<String>>("replay_kind").as_deref(),
        Some("erased")
    );
    assert_eq!(
        store
            .get(&scope, &withdrawn.claim_id)
            .await
            .expect("the read answers"),
        Some(stub),
        "the stub is untouched"
    );
    assert_eq!(
        statement_strings_in(&scope_rows(&raw, &scope).await),
        Vec::<&str>::new()
    );
}

#[tokio::test]
async fn postgres_an_earlier_transition_statement_against_a_deleted_claim_applies_nothing_when_dsn_is_set(
) {
    // An earlier binary read a live claim, a delete committed, and its
    // transition statement then runs. There is no row to match: it writes no
    // claim, no transition row and no ledger row, and its retry reads no
    // claim (`claim-not-found`).
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scope = ConformanceScopes::unique("pg-earlier-over-deleted").primary;

    let claim = distinctive_claim(&scope, "erased");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
    let read_by_the_earlier_binary = store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the claim reads")
        .expect("the claim exists");
    store
        .delete(
            &scope,
            &delete_request(&scope, &claim.claim_id, "erase", None),
        )
        .await
        .expect("the delete applies");
    let before = scope_rows(&raw, &scope).await;

    let in_flight = transition_request(
        &scope,
        &claim.claim_id,
        "in-flight",
        ClaimTrustStatus::Disputed,
    );
    let row = earlier_transition(&raw, &scope, &read_by_the_earlier_binary, &in_flight).await;
    assert!(!row.get::<_, bool>("replayed"));
    assert!(
        !row.get::<_, bool>("applied"),
        "the earlier transition statement matches no row"
    );
    assert_eq!(
        scope_rows(&raw, &scope).await,
        before,
        "no transition row and no ledger row was inserted"
    );
    assert!(ledger_row(&raw, &scope, &in_flight.operation_id)
        .await
        .is_none());
    assert_eq!(
        statement_strings_in(&scope_rows(&raw, &scope).await),
        Vec::<&str>::new()
    );
}

#[tokio::test]
async fn postgres_concurrent_delete_and_transition_stay_coherent_when_dsn_is_set() {
    // Two connections race a delete and a transition on one claim. Every run
    // ends in one of the two serial outcomes: the transition landed first and
    // the delete took it with the claim, its ledger row erased; or the delete
    // landed first and the transition found no claim. Never a mixture — no
    // orphaned transition row, no ledger row still holding the claim.
    let Some(store_a) = store().await else { return };
    let store_b = store().await.expect("a second connection opens");
    let raw = client().await.expect("a raw connection opens");
    let policy = ClaimPromotionPolicy::ungated();
    let mut transition_first = 0_u32;
    let mut delete_first = 0_u32;

    for run in 0..DELETE_RACE_RUNS {
        let scope = ConformanceScopes::unique("pg-race-delete").primary;
        let claim = distinctive_claim(&scope, &format!("race-{run}"));
        store_a
            .append(&scope, &claim)
            .await
            .expect("the claim appends");
        let dispute = transition_request(
            &scope,
            &claim.claim_id,
            "dispute",
            ClaimTrustStatus::Disputed,
        );
        let delete = delete_request(&scope, &claim.claim_id, "erase", None);

        // As in the tombstone race: the delete makes one more round trip
        // before its write, so the transition is held back by a nominal step
        // (coarsened by the runtime's timer) to mix the two orders.
        let head_start = std::time::Duration::from_micros(u64::from(run % 8) * 40);
        let (deleted, transitioned) = tokio::join!(store_a.delete(&scope, &delete), async {
            tokio::time::sleep(head_start).await;
            store_b.transition(&scope, &dispute, &policy, NOW).await
        },);
        deleted.expect("an unconditional delete always lands: it retries a lost race");

        assert!(
            store_a
                .get(&scope, &claim.claim_id)
                .await
                .expect("the read answers")
                .is_none(),
            "run {run}: the claim is gone"
        );
        let rows = scope_rows(&raw, &scope).await;
        assert!(
            rows.iter().all(|row| row.starts_with(b"op|")),
            "run {run}: no claim row and no transition row survives the delete"
        );
        assert_eq!(
            statement_strings_in(&rows),
            Vec::<&str>::new(),
            "run {run}: the statement survives in no row, whichever writer won"
        );
        let (kind, _) = ledger_row(&raw, &scope, &claim.operation_id)
            .await
            .expect("the append's ledger row is kept");
        assert_eq!(kind, "erased");

        match transitioned {
            Ok(_) => {
                transition_first += 1;
                let (kind, result) = ledger_row(&raw, &scope, &dispute.operation_id)
                    .await
                    .expect("the transition's ledger row is kept");
                assert_eq!(
                    kind, "erased",
                    "run {run}: the delete erased the transition it followed"
                );
                assert!(result.is_empty());
            }
            Err(error) => {
                delete_first += 1;
                assert_eq!(
                    error.code(),
                    "claim-not-found",
                    "run {run}: a transition that lost to the delete finds no claim: {error}"
                );
                assert!(ledger_row(&raw, &scope, &dispute.operation_id)
                    .await
                    .is_none());
            }
        }
    }
    eprintln!(
        "delete/transition race: transition first {transition_first}, delete first {delete_first}"
    );
}

#[tokio::test]
async fn postgres_a_stale_delete_writes_nothing_when_dsn_is_set() {
    // A delete whose expectation no longer holds is refused before any write:
    // every row of the scope is byte-identical, and no ledger row records the
    // attempt — which is why the same operation id can be tried again.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let scope = ConformanceScopes::unique("pg-stale-delete").primary;

    let claim = distinctive_claim(&scope, "kept");
    store
        .append(&scope, &claim)
        .await
        .expect("the claim appends");
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
        .expect("the dispute applies");
    let before = scope_rows(&raw, &scope).await;

    for expected in [
        // The count the caller read before the dispute landed.
        ClaimDeleteExpectation {
            transition_count: 0,
            tombstoned: false,
        },
        // The right count, but the caller read a stub.
        ClaimDeleteExpectation {
            transition_count: 1,
            tombstoned: true,
        },
    ] {
        let request = delete_request(&scope, &claim.claim_id, "erase", Some(expected));
        assert_eq!(
            store
                .delete(&scope, &request)
                .await
                .expect_err("a stale delete is refused")
                .code(),
            "claim-delete-stale"
        );
        assert_eq!(
            scope_rows(&raw, &scope).await,
            before,
            "a stale delete leaves every row byte-identical"
        );
        assert!(
            ledger_row(&raw, &scope, &request.operation_id)
                .await
                .is_none(),
            "a stale delete records nothing"
        );
    }

    // A tombstone landing after the caller's read makes a live expectation
    // stale, and the stub survives.
    let stub = store
        .tombstone(
            &scope,
            &tombstone_request(&scope, &claim.claim_id, "withdraw"),
        )
        .await
        .expect("the tombstone applies");
    let before = scope_rows(&raw, &scope).await;
    let request = delete_request(
        &scope,
        &claim.claim_id,
        "erase",
        Some(ClaimDeleteExpectation {
            transition_count: 1,
            tombstoned: false,
        }),
    );
    assert_eq!(
        store
            .delete(&scope, &request)
            .await
            .expect_err("a delete that read the live claim is stale against its stub")
            .code(),
        "claim-delete-stale"
    );
    assert_eq!(scope_rows(&raw, &scope).await, before);
    assert_eq!(
        store
            .get(&scope, &claim.claim_id)
            .await
            .expect("the read answers"),
        Some(stub)
    );
}

#[tokio::test]
async fn postgres_same_operation_races_converge_every_time_when_dsn_is_set() {
    // Two connections race the *same* operation id. Exactly one applies, and
    // the other must answer the winner's result — not a refusal decided
    // against the state the winner left behind. The window is the gap between
    // a store reading the ledger and reading the claim: a racer that commits
    // inside it leaves a ledger-less reader looking at a claim its own
    // operation already moved. One read of both closes it; the runs hold the
    // second racer back by a nominal step to land some of them in that window.
    let Some(store_a) = store().await else { return };
    let store_b = store().await.expect("a second connection opens");
    let policy = ClaimPromotionPolicy::ungated();

    for run in 0..SAME_OPERATION_RACE_RUNS {
        let head_start = std::time::Duration::from_micros(u64::from(run % 16) * 25);
        let scope = ConformanceScopes::unique("pg-race-same-op").primary;

        // The same transition: both racers answer the one applied outcome.
        let moved = conformance_claim(&scope, &format!("moved-{run}"), "node-a", "node-b");
        store_a
            .append(&scope, &moved)
            .await
            .expect("the claim appends");
        let dispute = transition_request(
            &scope,
            &moved.claim_id,
            "dispute",
            ClaimTrustStatus::Disputed,
        );
        let (first, second) =
            tokio::join!(store_a.transition(&scope, &dispute, &policy, NOW), async {
                tokio::time::sleep(head_start).await;
                store_b.transition(&scope, &dispute, &policy, NOW).await
            },);
        let first = first.unwrap_or_else(|error| panic!("run {run}: the first racer: {error}"));
        let second = second.unwrap_or_else(|error| panic!("run {run}: the second racer: {error}"));
        assert_eq!(first, second, "run {run}: one transition, one outcome");

        // The same tombstone: both racers answer the one stub.
        let withdrawn = conformance_claim(&scope, &format!("withdrawn-{run}"), "node-a", "node-c");
        store_a
            .append(&scope, &withdrawn)
            .await
            .expect("the claim appends");
        let tombstone = tombstone_request(&scope, &withdrawn.claim_id, "withdraw");
        let (first, second) = tokio::join!(store_a.tombstone(&scope, &tombstone), async {
            tokio::time::sleep(head_start).await;
            store_b.tombstone(&scope, &tombstone).await
        },);
        let first = first.unwrap_or_else(|error| panic!("run {run}: the first racer: {error}"));
        let second = second.unwrap_or_else(|error| panic!("run {run}: the second racer: {error}"));
        assert_eq!(first, second, "run {run}: one tombstone, one stub");

        // The same delete: both racers answer success.
        let erased = conformance_claim(&scope, &format!("erased-{run}"), "node-a", "node-d");
        store_a
            .append(&scope, &erased)
            .await
            .expect("the claim appends");
        let delete = delete_request(&scope, &erased.claim_id, "erase", None);
        let (first, second) = tokio::join!(store_a.delete(&scope, &delete), async {
            tokio::time::sleep(head_start).await;
            store_b.delete(&scope, &delete).await
        },);
        first.unwrap_or_else(|error| panic!("run {run}: the first racer: {error}"));
        second.unwrap_or_else(|error| panic!("run {run}: the second racer: {error}"));
    }
}

/// How long a proof waits for a store call on another connection to reach the
/// row lock it is meant to block on. If the call is slower than this the proof
/// still passes — it then sees the committed state instead of waiting on it —
/// so the pause can under-exercise the window but never fail it.
const BLOCKED_WRITER_PAUSE: std::time::Duration = std::time::Duration::from_millis(200);

/// Whether the ledger's kind check is validated, when the check exists.
async fn kind_check_validated(client: &Client) -> Option<bool> {
    client
        .query_opt(
            "SELECT convalidated FROM pg_constraint \
             WHERE conrelid = 'rakka_agent_knowledge_claim_op'::regclass \
               AND conname = 'rakka_agent_knowledge_claim_op_kind_check'",
            &[],
        )
        .await
        .expect("the catalog reads")
        .map(|row| row.get(0))
}

#[tokio::test]
async fn postgres_a_duplicate_append_cannot_restore_a_claim_deleted_while_it_waited_when_dsn_is_set(
) {
    // The narrowest way a deleted claim could come back. A duplicate of an
    // append that has not committed yet waits on the original's claim row. If
    // the original commits and a delete of that claim then commits before the
    // waiter looks again, the waiter finds no claim row and inserts one. Its
    // ledger insert then meets the erased row — and must fail the whole
    // statement there, not skip quietly and leave the claim restored.
    //
    // The interleaving is made exact by running the original append and the
    // delete's effect in one transaction: when it commits, the waiter sees
    // precisely "no claim row, an erased ledger row".
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let observer = client().await.expect("a second raw connection opens");
    let scope = ConformanceScopes::unique("pg-append-waits-on-delete").primary;
    let claim = distinctive_claim(&scope, "restored");

    raw.batch_execute("BEGIN")
        .await
        .expect("the transaction opens");
    let row = earlier_append(&raw, &scope, &claim).await;
    assert!(row.get::<_, bool>("applied"));

    // The duplicate: same operation, same claim, through the real store.
    let duplicate = {
        let store = store.clone();
        let scope = scope.clone();
        let claim = claim.clone();
        tokio::spawn(async move { store.append(&scope, &claim).await })
    };
    tokio::time::sleep(BLOCKED_WRITER_PAUSE).await;

    // The delete's effect: the claim row gone, its append erased.
    let key: [&(dyn ToSql + Sync); 3] = [
        &scope.tenant().as_str(),
        &scope.space().as_str(),
        &claim.claim_id.as_str(),
    ];
    raw.execute(
        "DELETE FROM rakka_agent_knowledge_claim \
         WHERE tenant = $1 AND space = $2 AND claim_id = $3",
        &key,
    )
    .await
    .expect("the claim row is deleted");
    raw.execute(
        "UPDATE rakka_agent_knowledge_claim_op SET kind = 'erased', result = ''::BYTEA \
         WHERE tenant = $1 AND space = $2 AND operation_id = $3",
        &[
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &claim.operation_id.as_str(),
        ],
    )
    .await
    .expect("the ledger row is erased");
    raw.batch_execute("COMMIT")
        .await
        .expect("the transaction commits");

    let answered = duplicate.await.expect("the task completes");
    assert_eq!(
        answered
            .expect_err("the duplicate append does not report a stored claim")
            .code(),
        "claim-operation-erased"
    );
    let rows = scope_rows(&observer, &scope).await;
    assert!(
        rows.iter().all(|row| row.starts_with(b"op|")),
        "the deleted claim was not restored"
    );
    assert_eq!(statement_strings_in(&rows), Vec::<&str>::new());
    assert!(store
        .get(&scope, &claim.claim_id)
        .await
        .expect("the read answers")
        .is_none());
}

#[tokio::test]
async fn postgres_an_operation_id_in_flight_for_another_claim_is_a_conflict_when_dsn_is_set() {
    // An erasure operation id belongs to one claim. The serial case is a
    // conformance clause; this is the concurrent one. A delete under id X is
    // in flight for some other claim — its ledger row written, not yet
    // committed — when a delete under the same X arrives for this claim. The
    // second must not apply and skip its ledger row: it waits, finds the id
    // spent, and answers a conflict with its claim untouched.
    let Some(store) = store().await else { return };
    let raw = client().await.expect("a raw connection opens");
    let observer = client().await.expect("a second raw connection opens");
    let scope = ConformanceScopes::unique("pg-id-in-flight").primary;

    for (kind, as_tombstone) in [("delete", false), ("delete", true)] {
        let claim = conformance_claim(
            &scope,
            &format!("kept-{as_tombstone}"),
            "node-a",
            &format!("node-{as_tombstone}"),
        );
        store
            .append(&scope, &claim)
            .await
            .expect("the claim appends");
        let before = scope_rows(&observer, &scope).await;
        let shared = ClaimOperationId::derive_delete(&scope, &claim.claim_id, "shared-request")
            .expect("the operation id derives");

        raw.batch_execute("BEGIN")
            .await
            .expect("the transaction opens");
        raw.execute(
            "INSERT INTO rakka_agent_knowledge_claim_op \
                 (tenant, space, operation_id, kind, result) \
             VALUES ($1, $2, $3, $4, ''::BYTEA)",
            &[
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &shared.as_str(),
                &kind,
            ],
        )
        .await
        .expect("the other claim's ledger row is written, uncommitted");

        let second = {
            let store = store.clone();
            let scope = scope.clone();
            let claim_id = claim.claim_id.clone();
            let shared = shared.clone();
            tokio::spawn(async move {
                if as_tombstone {
                    let mut request = tombstone_request(&scope, &claim_id, "unused");
                    request.operation_id = shared;
                    store.tombstone(&scope, &request).await.map(|_| ())
                } else {
                    let mut request = delete_request(&scope, &claim_id, "unused", None);
                    request.operation_id = shared;
                    store.delete(&scope, &request).await
                }
            })
        };
        tokio::time::sleep(BLOCKED_WRITER_PAUSE).await;
        raw.batch_execute("COMMIT")
            .await
            .expect("the transaction commits");

        let answered = second.await.expect("the task completes");
        assert_eq!(
            answered
                .expect_err("an operation id spent on another claim is refused")
                .code(),
            "claim-operation-conflict",
            "tombstone: {as_tombstone}"
        );
        let after = scope_rows(&observer, &scope).await;
        assert_eq!(
            after.len(),
            before.len() + 1,
            "only the other claim's ledger row was added"
        );
        assert_eq!(
            store
                .get(&scope, &claim.claim_id)
                .await
                .expect("the read answers"),
            Some(claim),
            "the claim the reused id named is exactly as it was"
        );
    }
}

#[tokio::test]
async fn postgres_the_migration_validates_its_wider_check_without_blocking_writers_when_dsn_is_set()
{
    // Widening the ledger's check is two steps on purpose. The first replaces
    // the check without scanning the table (`NOT VALID`), under a lock held
    // only for a catalog change. The second validates it in a statement of its
    // own, under a lock that does not conflict with a writer's — so the scan
    // of a large ledger never stalls the graph.
    let Some(dsn) = dsn() else { return };
    let setup = connect(&dsn).await;
    let schema = unique_schema("validate_step");
    setup
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("the private schema is created");
    let migrator = client_in(&dsn, &schema).await;
    let writer = client_in(&dsn, &schema).await;
    migrator
        .batch_execute(MIGRATION_SQL_D12B0F8)
        .await
        .expect("the earlier schema applies");
    let scope = ConformanceScopes::unique("pg-validate-step").primary;
    let claim = conformance_claim(&scope, "existing", "node-a", "node-b");
    assert!(earlier_append(&writer, &scope, &claim)
        .await
        .get::<_, bool>("applied"));

    // Step one: the check is replaced, and is not yet validated.
    migrator
        .batch_execute(MIGRATION_SQL)
        .await
        .expect("the first migration step applies");
    assert_eq!(
        kind_check_validated(&migrator).await,
        Some(false),
        "the first step adds the wider check without scanning the ledger"
    );
    // Rows are checked against it from this moment, validated or not.
    assert!(
        probe_ledger_kind(&writer, &scope, "resurrect")
            .await
            .is_err(),
        "an unvalidated check still refuses an undefined kind"
    );

    // A writer mid-transaction, holding the lock every ledger write holds.
    writer
        .batch_execute("BEGIN")
        .await
        .expect("the writer's transaction opens");
    probe_ledger_kind(&writer, &scope, "erased")
        .await
        .expect("the writer inserts under the wider check");

    // Step two validates while that writer is still open. A step that needed
    // an exclusive table lock would wait here until the timeout.
    migrator
        .batch_execute("SET lock_timeout = '3s'")
        .await
        .expect("the lock timeout is set");
    migrator
        .batch_execute(MIGRATION_VALIDATE_SQL)
        .await
        .expect("validation does not wait for a writer");
    writer
        .batch_execute("COMMIT")
        .await
        .expect("the writer commits");
    assert_eq!(kind_check_validated(&migrator).await, Some(true));

    // Both steps are no-ops from here.
    let constraints = table_constraints(&migrator).await;
    migrator
        .batch_execute(MIGRATION_SQL)
        .await
        .expect("the first step repeats");
    migrator
        .batch_execute(MIGRATION_VALIDATE_SQL)
        .await
        .expect("the second step repeats");
    assert_eq!(table_constraints(&migrator).await, constraints);

    setup
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .expect("the private schema is dropped");
}

#[tokio::test]
async fn postgres_concurrent_migrators_upgrade_the_earlier_schema_when_dsn_is_set() {
    // Several nodes of the new build start at once against a database an
    // earlier build created. Every one runs the upgrade; all must succeed, and
    // the schema must end migrated exactly once. Without the advisory lock the
    // losers would find the column and the check already there mid-statement.
    let Some(dsn) = dsn() else { return };
    let setup = connect(&dsn).await;
    let schema = unique_schema("upgrade_race");
    setup
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("the private schema is created");
    let raw = client_in(&dsn, &schema).await;
    raw.batch_execute(MIGRATION_SQL_D12B0F8)
        .await
        .expect("the earlier schema applies");
    let scope = ConformanceScopes::unique("pg-upgrade-race").primary;
    let claim = conformance_claim(&scope, "existing", "node-a", "node-b");
    assert!(earlier_append(&raw, &scope, &claim)
        .await
        .get::<_, bool>("applied"));

    let mut migrators = Vec::new();
    for _ in 0..6_u32 {
        let dsn = dsn.clone();
        let schema = schema.clone();
        migrators.push(tokio::spawn(async move {
            PostgresKnowledgeGraphStore::new(client_in(&dsn, &schema).await)
                .migrate()
                .await
        }));
    }
    let mut failures = Vec::new();
    for migrator in migrators {
        if let Err(error) = migrator.await.expect("the migrator task completes") {
            failures.push(error.to_string());
        }
    }

    let validated = kind_check_validated(&raw).await;
    let store = PostgresKnowledgeGraphStore::new(client_in(&dsn, &schema).await);
    let read = store.get(&scope, &claim.claim_id).await;
    let cleanup = setup
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await;
    assert!(
        failures.is_empty(),
        "concurrent migrators raced the upgrade: {failures:?}"
    );
    assert_eq!(validated, Some(true), "the wider check ends validated");
    assert_eq!(
        read.expect("the migrated schema reads"),
        Some(claim),
        "the earlier build's row reads through the upgraded schema"
    );
    cleanup.expect("the private schema is dropped");
}
