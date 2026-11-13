#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! PostgreSQL binding for the communal knowledge-graph store SPI.
//!
//! This crate is the second, structurally different
//! [`KnowledgeGraphStore`] implementation of implementation-plan slice 2.4:
//! relational tables instead of ordered in-process maps, proven by running the
//! [`rakka_agent_knowledge_graph::conformance`] suite unchanged (scenario 20).
//! It never changes claim identity, provenance, trust, idempotency, or
//! authorization semantics — it only persists them
//! ([specification 13.6](../../../docs/plans/rakka-agent/spec.md)).
//!
//! # Design
//!
//! - **The `record` BYTEA is authoritative.** Claims and transitions are
//!   stored as their canonical `serde_json` encodings and rebuilt through the
//!   domain crate's `restore` doors, so schema-window gating, statement-digest
//!   re-derivation, and trust-coherence checks run fail-closed on every load.
//!   The columns beside it (`subject`, `predicate`, `object_node`, `trust`,
//!   `transition_count`, `tombstoned`) are denormalized only for traversal
//!   predicates and the compare-and-set fence; a row whose columns drift from
//!   its own record is refused, never silently preferred.
//! - **Every write is one data-modifying-CTE statement** — one implicit
//!   transaction on the shared pipelined client, never `BEGIN`/`COMMIT` — so
//!   a claim mutation, its transition row, and its operation-ledger row commit
//!   or fail together. The ledger stores each operation's original result;
//!   a replay answers those bytes, never a re-derivation (scenario 16), and a
//!   decided promotion gate is not re-evaluated on replay.
//! - **Erasure rewrites the ledger in the same statement.** A tombstone
//!   replaces the claim row with a content-free stub and sets the ledger rows
//!   of the claim's append and of every earlier transition to an `erased`
//!   kind with an empty payload; a delete removes the claim row and its
//!   transition rows and erases the same ledger rows, plus a tombstone's. An
//!   erased row keeps its operation id, which is what stops a replayed append
//!   from restoring the claim: the statement finds the row before it would
//!   insert anything. Erased and deleted ledger rows are kept for good.
//! - **A stub row's `transition_count` column holds a fence value**
//!   ([`TOMBSTONE_FENCE_TRANSITION_COUNT`]), not the count, so a transition
//!   statement from a binary that predates tombstones cannot match a stub and
//!   write the statement back over it.
//! - **Queries are keyset scans with the shared admission rule.**
//!   [`rakka_agent_knowledge_graph::store::ClaimFilter`] deliberately exposes
//!   no field accessors, so its predicate cannot be pushed into SQL; the store
//!   scans one scope in ascending claim-id order (`COLLATE "C"`, exactly the
//!   reference implementation's string order) and applies `admits` in Rust.
//!   Nothing is lost — the cursor resumes by claim-id position, not offset —
//!   and the cost is linear in one scope's corpus, the same exactness-first
//!   trade the pgvector retriever documents.
//! - **Traversal is the reference breadth-first expansion over bounded
//!   per-node queries.** A recursive CTE cannot express the global spent-edge
//!   set, the deterministic frontier-order/claim-id-order edge sequence, or
//!   truncation at the exact edge the budget cuts; round trips are bounded by
//!   the crate caps (at most 256 frontier nodes across at most 4 levels).
//!
//! Scope isolation is structural: every statement carries the scope's
//! `(tenant, space)` key columns, so a wrong-scope read answers exactly the
//! empty shape an empty space answers and a wrong-scope write fails with the
//! same `claim-not-found` an absent claim produces (scenario 18).
//!
//! The crate does not open its own connection: the deploying application
//! supplies a [`tokio_postgres::Client`] — created with its own TLS and
//! credential choices — and this crate runs bounded SQL against it. Schema is
//! applied by [`PostgresKnowledgeGraphStore::migrate`], idempotently, under
//! the crate's own advisory lock.
//!
//! # Rolling this build out
//!
//! Run `migrate` from this build **first**: its reads select the `tombstoned`
//! column. The migration is safe under binaries from before tombstones — the
//! new column has a default their statements never name, and the ledger's
//! check only widens. Then upgrade every binary that reads the graph, and only
//! then call `tombstone`, `delete` or the retention sweep. A binary that was
//! missed fails closed: a stub answers it `schema-version-ahead`, and a replay
//! of an erased operation answers it `claim-operation-conflict`.
//!
//! Gated tests run only when `RAKKA_POSTGRES_TEST_DSN` is set, like the other
//! PostgreSQL adapter crates:
//!
//! ```sh
//! RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p rakka-agent-knowledge-graph-postgres
//! ```

use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use rakka_agent_knowledge_graph::claim::{
    Claim, ClaimId, ClaimNodeId, ClaimOperationId, ClaimRecord,
};
use rakka_agent_knowledge_graph::error::{ClaimError, ClaimFuture, ClaimResult};
use rakka_agent_knowledge_graph::promotion::{validate_promotion, ClaimPromotionPolicy};
use rakka_agent_knowledge_graph::scope::KnowledgeSpaceScope;
use rakka_agent_knowledge_graph::store::{
    ClaimCursor, ClaimFilter, ClaimPage, ClaimTransitionCursor, ClaimTransitionPage,
    ClaimTraversal, ClaimTraversalDirection, ClaimTraversalReport, KnowledgeGraphCapabilities,
    KnowledgeGraphStore,
};
use rakka_agent_knowledge_graph::tombstone::{ClaimDeleteRequest, ClaimTombstoneRequest};
use rakka_agent_knowledge_graph::transition::{
    ClaimTransitionOutcome, ClaimTrustTransition, ClaimTrustTransitionRecord,
    ClaimTrustTransitionRequest,
};
use rakka_agent_knowledge_graph::ClaimTrustStatus;
use rakka_agent_knowledge_graph::CLAIM_TOMBSTONE_MARKER;
use rakka_agent_workflow::AgentTimestampMillis;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};

mod erasure;

/// Stable backend name, reported for diagnostics and error detail.
pub const BACKEND_NAME: &str = "postgres";

/// The claim table.
pub const CLAIM_TABLE_NAME: &str = "rakka_agent_knowledge_claim";

/// The append-only trust-transition table.
pub const TRANSITION_TABLE_NAME: &str = "rakka_agent_knowledge_claim_transition";

/// The operation-ledger table.
pub const OPERATION_TABLE_NAME: &str = "rakka_agent_knowledge_claim_op";

/// Advisory-lock id serializing concurrent migrations of this crate's schema.
///
/// A bare `CREATE TABLE IF NOT EXISTS` can race two migrators against
/// PostgreSQL's system catalogs; taking a session advisory lock first makes
/// the migration safe to run concurrently. The id is this crate's own —
/// distinct from every other Rakka PostgreSQL adapter's lock — because a
/// shared id would needlessly serialize unrelated subsystems' migrations in a
/// shared database (the lesson `rakka-agent-postgres` records).
pub const MIGRATION_LOCK_ID: i64 = 982_451_927;

/// Largest number of compare-and-set attempts one [`transition`] call makes
/// before failing with `claim-backend-failed`.
///
/// Each lost race re-reads the claim and re-runs legality against the state
/// that beat it, so a bounded retry converges under contention exactly as the
/// in-memory store's lock does; exhaustion is an explicit refusal, never a
/// silently wrong answer.
///
/// [`transition`]: KnowledgeGraphStore::transition
pub const TRANSITION_CAS_MAX_ATTEMPTS: usize = 8;

/// The value a tombstoned stub's row holds in its `transition_count` column.
///
/// Not the claim's transition count — that stays in the authoritative record —
/// but a reserved value above every count a claim can reach
/// (`CLAIM_MAX_TRUST_TRANSITIONS` is 32), so **no reader can hold it as an
/// expectation**. It exists for binaries from before tombstones: their
/// transition statement fences only on the transition count they read, and a
/// tombstone does not change that count. Had the stub row kept it, a
/// transition such a binary had in flight when the tombstone committed would
/// still match the row and write the full statement back over the stub. With
/// the fence value in the column, that update matches nothing.
///
/// This is the one stated exception to "the denormalized columns agree with
/// the record", and the drift check knows it: a stub row holding any other
/// value is refused as drift.
pub const TOMBSTONE_FENCE_TRANSITION_COUNT: i32 = i32::MAX;

/// Rows one query-scan batch reads before admission filtering.
const QUERY_SCAN_BATCH_ROWS: i64 = 256;

/// Idempotent schema for the communal knowledge-graph tables.
///
/// `claim_id` collates as `"C"` so SQL ordering and range comparisons are
/// byte order — exactly the reference implementation's Rust string order —
/// whatever the database's default collation. `subject`, `predicate`,
/// `object_node`, and `trust` are denormalized from the record purely so the
/// bounded traversal can filter ahead of its per-node `LIMIT`;
/// `transition_count` is the compare-and-set fence. The `record` column is
/// the authoritative claim, re-validated fail-closed on every load. The
/// operation ledger is scope-wide — not claim-scoped — which is what makes an
/// operation id reused across kinds detectable (`claim-operation-conflict`).
/// Its `result` stays `NOT NULL` through erasure: an erased row and a delete's
/// own row hold empty bytes, never `NULL`, so a binary from before tombstones
/// that reads one decodes a value rather than panicking on a null.
///
/// Fresh tables are created in the current shape. Existing ones are upgraded
/// in place by the trailing block, which adds the `tombstoned` column and
/// widens the ledger's `kind` check — each only when it is not already there,
/// so a second run takes no table lock and rewrites nothing. Adding a column
/// with a constant default does not rewrite the table, and the wider check is
/// added `NOT VALID` and validated separately, so neither step holds an
/// exclusive lock across a scan. Both are safe under a binary from before
/// tombstones that is still appending: its insert does not name the new
/// column, and both kinds it writes are in the wider check.
pub const MIGRATION_SQL: &str = r#"
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
        ALTER TABLE rakka_agent_knowledge_claim_op
            VALIDATE CONSTRAINT rakka_agent_knowledge_claim_op_kind_check;
    END IF;
END
$migrate$;
"#;

/// One append: the ledger is consulted, the claim row and its ledger row are
/// written together, and the final row reports which door answered — all one
/// statement, so there is no crash window between the claim and its ledger.
const APPEND_SQL: &str = "
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
";

/// One transition write: the compare-and-set fence on `transition_count` is
/// what makes the claim update, the transition append, and the ledger row an
/// all-or-nothing outcome of exactly the state the caller read.
const TRANSITION_SQL: &str = "
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
";

const GET_SQL: &str = "
SELECT record, tombstoned FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2 AND claim_id = $3
";

/// What every fenced write starts from: the ledger row of its operation id
/// and the head of its claim, read by **one statement** and therefore under
/// one snapshot.
///
/// Read separately, the two leave a window: a racer running the same
/// operation can commit between them, and a reader that saw no ledger row
/// then sees a claim its own operation already moved — and refuses a replay
/// it should have answered (`claim-transition-illegal` for a transition whose
/// target the claim now holds, `claim-tombstoned` for a tombstone that
/// already landed, `claim-not-found` for a delete that already did). A write
/// commits its claim change and its ledger row together, so one snapshot sees
/// both or neither.
const WRITE_START_SQL: &str = "
SELECT op.kind, op.result, claim.record, claim.tombstoned, claim.transition_count
FROM (SELECT 1) AS one
LEFT JOIN rakka_agent_knowledge_claim_op AS op
       ON op.tenant = $1 AND op.space = $2 AND op.operation_id = $3
LEFT JOIN rakka_agent_knowledge_claim AS claim
       ON claim.tenant = $1 AND claim.space = $2 AND claim.claim_id = $4
";

const LEDGER_SQL: &str = "
SELECT kind, result FROM rakka_agent_knowledge_claim_op
WHERE tenant = $1 AND space = $2 AND operation_id = $3
";

const QUERY_SCAN_FIRST_SQL: &str = "
SELECT claim_id, record, tombstoned FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2
ORDER BY claim_id
LIMIT $3
";

const QUERY_SCAN_AFTER_SQL: &str = "
SELECT claim_id, record, tombstoned FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2 AND claim_id > $3
ORDER BY claim_id
LIMIT $4
";

const TRANSITIONS_SQL: &str = "
SELECT record FROM rakka_agent_knowledge_claim_transition
WHERE tenant = $1 AND space = $2 AND claim_id = $3 AND ordinal > $4
ORDER BY ordinal
LIMIT $5
";

/// The candidate edges one frontier node yields, every traversal predicate —
/// edge shape, trust, predicate set, spent exclusion, direction — ahead of
/// the `LIMIT`, so a budget's worth of candidates is a budget's worth of
/// followable edges, never a shorter answer post-filtered in Rust.
const TRAVERSAL_EDGE_SCAN_SQL: &str = "
SELECT claim_id, record, tombstoned FROM rakka_agent_knowledge_claim
WHERE tenant = $1 AND space = $2
  AND NOT tombstoned
  AND object_node IS NOT NULL
  AND trust = ANY($3)
  AND ($4::TEXT[] IS NULL OR predicate = ANY($4))
  AND NOT (claim_id = ANY($5))
  AND (CASE $6::TEXT
       WHEN 'outbound' THEN subject = $7
       WHEN 'inbound' THEN object_node = $7
       ELSE (subject = $7 OR object_node = $7)
       END)
ORDER BY claim_id
LIMIT $8
";

/// The ledger kind of an applied append.
const KIND_APPEND: &str = "append";
/// The ledger kind of an applied transition.
const KIND_TRANSITION: &str = "transition";
/// The ledger kind of an applied tombstone; its payload is the stub.
const KIND_TOMBSTONE: &str = "tombstone";
/// The ledger kind of an applied delete; its payload is empty.
const KIND_DELETE: &str = "delete";
/// The ledger kind of an operation whose payload a later tombstone or delete
/// of its claim erased; its payload is empty.
const KIND_ERASED: &str = "erased";

/// What the operation ledger answered for one operation id.
enum LedgerHit {
    /// An applied append, with the claim it stored.
    Append(Vec<u8>),
    /// An applied transition, with the outcome it produced.
    Transition(Vec<u8>),
    /// An applied tombstone, with the stub it stored.
    Tombstone(Vec<u8>),
    /// An applied delete.
    Deleted,
    /// An operation whose payload was erased with its claim.
    Erased,
}

/// What one fenced write found when it read its operation and its claim
/// together.
enum WriteStart {
    /// The operation id is already spent; the ledger answers.
    Spent(LedgerHit),
    /// The operation is new and the claim does not exist in the scope.
    Absent,
    /// The operation is new; the claim, and the fence its row holds.
    Present(Box<Claim>, i32),
}

impl LedgerHit {
    /// Reads one ledger row. An unrecognized kind is a failure, never a guess:
    /// a row this build cannot interpret must not be replayed as an append.
    fn from_row(kind: &str, result: Vec<u8>) -> ClaimResult<Self> {
        match kind {
            KIND_APPEND => Ok(Self::Append(result)),
            KIND_TRANSITION => Ok(Self::Transition(result)),
            KIND_TOMBSTONE => Ok(Self::Tombstone(result)),
            KIND_DELETE => Ok(Self::Deleted),
            KIND_ERASED => Ok(Self::Erased),
            other => Err(ClaimError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: format!("the ledger holds an operation of unknown kind {other:?}"),
            }),
        }
    }
}

/// The PostgreSQL [`KnowledgeGraphStore`].
#[derive(Clone)]
pub struct PostgresKnowledgeGraphStore {
    client: Arc<Client>,
}

impl PostgresKnowledgeGraphStore {
    /// Creates a store over an owned client.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::from_shared_client(Arc::new(client))
    }

    /// Creates a store that shares an already-`Arc`-wrapped client.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>) -> Self {
        Self { client }
    }

    /// Applies the idempotent schema under the crate's advisory lock.
    ///
    /// # Errors
    ///
    /// Returns [`ClaimError::Backend`] if the DDL cannot be applied.
    pub async fn migrate(&self) -> ClaimResult<()> {
        self.client
            .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
            .await
            .map_err(map_error)?;
        let applied = self.client.batch_execute(MIGRATION_SQL).await;
        let unlocked = self
            .client
            .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_ID])
            .await;
        applied.map_err(map_error)?;
        unlocked.map_err(map_error)?;
        Ok(())
    }

    /// Reads the ledger row of one operation id, when any.
    async fn ledger_hit(
        &self,
        scope: &KnowledgeSpaceScope,
        operation_id: &str,
    ) -> ClaimResult<Option<LedgerHit>> {
        let row = self
            .client
            .query_opt(
                LEDGER_SQL,
                &[
                    &scope.tenant().as_str(),
                    &scope.space().as_str(),
                    &operation_id,
                ],
            )
            .await
            .map_err(map_error)?;
        row.map(|row| {
            let kind: String = row.get("kind");
            LedgerHit::from_row(&kind, row.get("result"))
        })
        .transpose()
    }

    /// Reads, in one snapshot, the ledger row of one operation id and the
    /// head of one claim — what every fenced write decides from.
    ///
    /// A spent operation is answered from the ledger alone, without decoding
    /// the claim: a replay does not depend on the state the claim has since
    /// moved to. Otherwise the claim is decoded with the fence its row holds —
    /// the `transition_count` column: the claim's count for a live row,
    /// [`TOMBSTONE_FENCE_TRANSITION_COUNT`] for a stub. Any other pairing is
    /// drift, refused here so no write is ever fenced on it.
    async fn write_start(
        &self,
        scope: &KnowledgeSpaceScope,
        operation_id: &ClaimOperationId,
        claim_id: &ClaimId,
    ) -> ClaimResult<WriteStart> {
        let row = self
            .client
            .query_one(
                WRITE_START_SQL,
                &[
                    &scope.tenant().as_str(),
                    &scope.space().as_str(),
                    &operation_id.as_str(),
                    &claim_id.as_str(),
                ],
            )
            .await
            .map_err(map_error)?;
        if let Some(kind) = row.get::<_, Option<String>>("kind") {
            let result: Option<Vec<u8>> = row.get("result");
            return Ok(WriteStart::Spent(LedgerHit::from_row(
                &kind,
                result.unwrap_or_default(),
            )?));
        }
        let Some(record) = row.get::<_, Option<Vec<u8>>>("record") else {
            return Ok(WriteStart::Absent);
        };
        let tombstoned: Option<bool> = row.get("tombstoned");
        let fence: Option<i32> = row.get("transition_count");
        let (Some(tombstoned), Some(fence)) = (tombstoned, fence) else {
            return Err(ClaimError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: "a claim row was read without its fence columns".to_string(),
            });
        };
        let claim = decode_claim_columns(&record, tombstoned)?;
        let expected = if claim.is_tombstoned() {
            TOMBSTONE_FENCE_TRANSITION_COUNT
        } else {
            int_count(claim.transition_count())?
        };
        if fence != expected {
            return Err(drift_error(&claim.claim_id));
        }
        Ok(WriteStart::Present(Box::new(claim), fence))
    }

    /// The candidate edges one frontier node yields, in ascending claim-id
    /// order, re-verified against the authoritative record.
    async fn edge_candidates(
        &self,
        scope: &KnowledgeSpaceScope,
        traversal: &ClaimTraversal,
        node: &ClaimNodeId,
        spent: &BTreeSet<String>,
        limit: i64,
    ) -> ClaimResult<Vec<(Claim, ClaimNodeId)>> {
        let trust: Vec<String> = traversal
            .trust()
            .iter()
            .map(|status| status.as_label().to_string())
            .collect();
        let predicates: Option<Vec<String>> = traversal
            .predicates()
            .map(|set| set.iter().map(|p| p.as_str().to_string()).collect());
        let spent: Vec<String> = spent.iter().cloned().collect();
        let direction = match traversal.direction() {
            ClaimTraversalDirection::Outbound => "outbound",
            ClaimTraversalDirection::Inbound => "inbound",
            ClaimTraversalDirection::Both => "both",
        };
        let params: [&(dyn ToSql + Sync); 8] = [
            &scope.tenant().as_str(),
            &scope.space().as_str(),
            &trust,
            &predicates,
            &spent,
            &direction,
            &node.as_str(),
            &limit,
        ];
        let rows = self
            .client
            .query(TRAVERSAL_EDGE_SCAN_SQL, &params)
            .await
            .map_err(map_error)?;
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let claim = decode_claim_row(&row)?;
            // The SQL predicates mirror the reference `follows`/`neighbor`
            // closures exactly, so a candidate the record itself does not
            // admit means the denormalized columns drifted from the record —
            // corruption, refused rather than skipped (a skip would answer a
            // traversal short of what the corpus holds).
            let followable = claim.object.node().is_some()
                && traversal.trust().contains(&claim.trust())
                && traversal
                    .predicates()
                    .is_none_or(|predicates| predicates.contains(&claim.predicate));
            let reached = traversal_neighbor(&claim, node, traversal.direction());
            match (followable, reached) {
                (true, Some(reached)) => candidates.push((claim, reached)),
                _ => {
                    return Err(drift_error(&claim.claim_id));
                }
            }
        }
        Ok(candidates)
    }
}

impl KnowledgeGraphStore for PostgresKnowledgeGraphStore {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn capabilities(&self) -> KnowledgeGraphCapabilities {
        KnowledgeGraphCapabilities::core()
    }

    fn append<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim: &'a Claim,
    ) -> ClaimFuture<'a, Claim> {
        Box::pin(async move {
            // The same precedence as the reference implementation: bounds,
            // born-`Proposed`, derived identity — all before any I/O.
            claim.validate()?;
            // An append stores a live claim: a stub row is written only by the
            // tombstone statement, which sets its columns and its fence.
            if claim.is_tombstoned() {
                return Err(ClaimError::Tombstoned {
                    claim_id: claim.claim_id.clone(),
                });
            }
            if claim.trust() != ClaimTrustStatus::Proposed || claim.transition_count() != 0 {
                return Err(ClaimError::AppendNotProposed {
                    claim_id: claim.claim_id.clone(),
                });
            }
            let derived = ClaimId::derive_appended(scope, &claim.operation_id)?;
            if claim.claim_id != derived {
                return Err(ClaimError::AppendIdNotDerived {
                    claim_id: claim.claim_id.clone(),
                    derived,
                });
            }
            let record = encode(&claim.to_record())?;
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
            let row = self
                .client
                .query_one(APPEND_SQL, &params)
                .await
                .map_err(map_error)?;
            if row.get::<_, bool>("replayed") {
                return replayed_append(&row, &claim.operation_id);
            }
            if row.get::<_, bool>("applied") {
                return Ok(claim.clone());
            }
            // Neither door answered: a concurrent writer got between the
            // ledger read and the insert. Re-consult the ledger once — a
            // same-operation race replays its winner — then the claim row,
            // whose presence under a different operation is the id collision.
            match self.ledger_hit(scope, claim.operation_id.as_str()).await? {
                Some(LedgerHit::Append(result)) => decode_claim(&result),
                Some(LedgerHit::Erased) => Err(ClaimError::OperationErased {
                    operation_id: claim.operation_id.clone(),
                }),
                Some(LedgerHit::Transition(_) | LedgerHit::Tombstone(_) | LedgerHit::Deleted) => {
                    Err(ClaimError::OperationConflict {
                        operation_id: claim.operation_id.clone(),
                    })
                }
                None => {
                    let occupied = self
                        .client
                        .query_opt(
                            GET_SQL,
                            &[
                                &scope.tenant().as_str(),
                                &scope.space().as_str(),
                                &claim.claim_id.as_str(),
                            ],
                        )
                        .await
                        .map_err(map_error)?
                        .is_some();
                    if occupied {
                        Err(ClaimError::AlreadyExists {
                            claim_id: claim.claim_id.clone(),
                        })
                    } else {
                        Err(ClaimError::Backend {
                            backend: BACKEND_NAME.to_string(),
                            message: "the append raced a concurrent writer and neither door \
                                      answered; retry"
                                .to_string(),
                        })
                    }
                }
            }
        })
    }

    fn get<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
    ) -> ClaimFuture<'a, Option<Claim>> {
        Box::pin(async move {
            let row = self
                .client
                .query_opt(
                    GET_SQL,
                    &[
                        &scope.tenant().as_str(),
                        &scope.space().as_str(),
                        &claim_id.as_str(),
                    ],
                )
                .await
                .map_err(map_error)?;
            row.map(|row| decode_claim_row(&row)).transpose()
        })
    }

    fn query<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        filter: &'a ClaimFilter,
        cursor: ClaimCursor,
    ) -> ClaimFuture<'a, ClaimPage> {
        Box::pin(async move {
            let mut position: Option<String> = cursor.position().map(|id| id.as_str().to_string());
            let mut page: Vec<Claim> = Vec::new();
            let mut next: Option<ClaimCursor> = None;
            'scan: loop {
                let rows = match &position {
                    Some(after) => {
                        let params: [&(dyn ToSql + Sync); 4] = [
                            &scope.tenant().as_str(),
                            &scope.space().as_str(),
                            after,
                            &QUERY_SCAN_BATCH_ROWS,
                        ];
                        self.client.query(QUERY_SCAN_AFTER_SQL, &params).await
                    }
                    None => {
                        let params: [&(dyn ToSql + Sync); 3] = [
                            &scope.tenant().as_str(),
                            &scope.space().as_str(),
                            &QUERY_SCAN_BATCH_ROWS,
                        ];
                        self.client.query(QUERY_SCAN_FIRST_SQL, &params).await
                    }
                }
                .map_err(map_error)?;
                let exhausted = rows.len() < QUERY_SCAN_BATCH_ROWS as usize;
                for row in rows {
                    let id: String = row.get("claim_id");
                    position = Some(id);
                    let claim = decode_claim_row(&row)?;
                    if !filter.admits(&claim) {
                        continue;
                    }
                    if page.len() == cursor.limit() {
                        // The (limit+1)-th admitted claim proves more remain:
                        // the next cursor resumes after the last returned
                        // claim, at the same limit — the reference
                        // implementation's convention exactly.
                        let last = page.last().expect("a full page holds its limit");
                        next = Some(
                            ClaimCursor::after(last.claim_id.clone()).with_limit(cursor.limit()),
                        );
                        break 'scan;
                    }
                    page.push(claim);
                }
                if exhausted {
                    break;
                }
            }
            Ok(ClaimPage { claims: page, next })
        })
    }

    fn traverse<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        traversal: &'a ClaimTraversal,
    ) -> ClaimFuture<'a, ClaimTraversalReport> {
        Box::pin(async move {
            let mut report = ClaimTraversalReport {
                nodes: Vec::new(),
                edges: Vec::new(),
                truncated: false,
            };
            let mut visited: BTreeSet<ClaimNodeId> = BTreeSet::new();
            let mut spent: BTreeSet<String> = BTreeSet::new();
            let mut frontier: VecDeque<ClaimNodeId> = VecDeque::new();

            // The start node enters the report only when at least one
            // in-scope edge touches it, so an unknown and a foreign-scope
            // start are indistinguishable (scenario 18).
            let touched = !self
                .edge_candidates(scope, traversal, traversal.start(), &spent, 1)
                .await?
                .is_empty();
            if !touched {
                return Ok(report);
            }
            report.nodes.push(traversal.start().clone());
            visited.insert(traversal.start().clone());
            frontier.push_back(traversal.start().clone());

            for _ in 0..traversal.depth() {
                if frontier.is_empty() {
                    break;
                }
                let mut next_level: BTreeSet<ClaimNodeId> = BTreeSet::new();
                for node in std::mem::take(&mut frontier) {
                    // One more candidate than the remaining budget: the
                    // extra, if it exists, is exactly the edge whose arrival
                    // proves the cut.
                    let remaining = traversal.edge_budget() - report.edges.len();
                    let limit = i64::try_from(remaining)
                        .unwrap_or(i64::MAX)
                        .saturating_add(1);
                    let candidates = self
                        .edge_candidates(scope, traversal, &node, &spent, limit)
                        .await?;
                    for (claim, reached) in candidates {
                        if report.edges.len() == traversal.edge_budget() {
                            report.truncated = true;
                            return Ok(report);
                        }
                        spent.insert(claim.claim_id.as_str().to_string());
                        report.edges.push(claim);
                        if !visited.contains(&reached) {
                            next_level.insert(reached);
                        }
                    }
                }
                for reached in next_level {
                    if report.nodes.len() == traversal.node_budget() {
                        report.truncated = true;
                        return Ok(report);
                    }
                    report.nodes.push(reached.clone());
                    visited.insert(reached.clone());
                    frontier.push_back(reached);
                }
            }
            // Depth exhausted with reachable work left is a cut, not an end.
            for node in &frontier {
                if !self
                    .edge_candidates(scope, traversal, node, &spent, 1)
                    .await?
                    .is_empty()
                {
                    report.truncated = true;
                    break;
                }
            }
            Ok(report)
        })
    }

    fn transition<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTrustTransitionRequest,
        policy: &'a ClaimPromotionPolicy,
        now: AgentTimestampMillis,
    ) -> ClaimFuture<'a, ClaimTransitionOutcome> {
        Box::pin(async move {
            for _ in 0..TRANSITION_CAS_MAX_ATTEMPTS {
                // The ledger first, on every attempt, and in the same
                // snapshot as the claim: a replay answers the original outcome
                // without re-running legality or the gate — a decided
                // promotion is not re-litigated, even by a grant that has
                // since expired — and never against a claim the same
                // operation already moved.
                let (claim, fence) = match self
                    .write_start(scope, &request.operation_id, &request.claim_id)
                    .await?
                {
                    WriteStart::Spent(LedgerHit::Transition(result)) => {
                        return decode_outcome(&result);
                    }
                    WriteStart::Spent(LedgerHit::Erased) => {
                        return Err(ClaimError::OperationErased {
                            operation_id: request.operation_id.clone(),
                        });
                    }
                    WriteStart::Spent(
                        LedgerHit::Append(_) | LedgerHit::Tombstone(_) | LedgerHit::Deleted,
                    ) => {
                        return Err(ClaimError::OperationConflict {
                            operation_id: request.operation_id.clone(),
                        });
                    }
                    // Absent and out-of-scope are the same refusal
                    // (scenario 18).
                    WriteStart::Absent => {
                        return Err(ClaimError::NotFound {
                            claim_id: request.claim_id.clone(),
                        });
                    }
                    WriteStart::Present(claim, fence) => (*claim, fence),
                };

                // Refuses a stub (`claim-tombstoned`) before the table is
                // consulted: a stub's trust is frozen.
                let updated = claim.apply_transition(request.to)?;
                let gate = if request.to == ClaimTrustStatus::Verified {
                    validate_promotion(scope, &claim, policy, request.promotion.as_deref(), now)?
                } else {
                    None
                };

                let mut transition = ClaimTrustTransition::new(
                    claim.claim_id.clone(),
                    request.operation_id.clone(),
                    updated.transition_count(),
                    claim.trust(),
                    request.to,
                    request.actor.clone(),
                    request.occurred_at,
                )?;
                if let Some(provenance) = &request.provenance {
                    transition = transition.with_provenance(provenance.clone())?;
                }
                if let Some(reason) = &request.reason {
                    transition = transition.with_reason(reason.clone())?;
                }
                if !request.evidence.is_empty() {
                    transition = transition.with_evidence(request.evidence.clone())?;
                }
                if let Some(policy_ref) = &request.policy {
                    transition = transition.with_policy(policy_ref.clone());
                }
                if let Some(receipt) = gate {
                    transition = transition.with_gate(receipt);
                }
                let outcome = ClaimTransitionOutcome {
                    claim: updated.clone(),
                    transition: transition.clone(),
                };

                let updated_count = int_count(updated.transition_count())?;
                let updated_record = encode(&updated.to_record())?;
                let transition_record = encode(&transition.to_record())?;
                let outcome_record = encode(&outcome)?;
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
                let row = self
                    .client
                    .query_one(TRANSITION_SQL, &params)
                    .await
                    .map_err(map_error)?;
                if row.get::<_, bool>("replayed") {
                    return replayed_transition(&row, &request.operation_id);
                }
                if row.get::<_, bool>("applied") {
                    return Ok(outcome);
                }
                // The fence moved: a concurrent operation won the race. Loop —
                // the fresh read re-runs legality against the state that won,
                // which may now be a stub (`claim-tombstoned`) or no claim at
                // all (`claim-not-found`).
            }
            Err(ClaimError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: format!(
                    "the transition lost {TRANSITION_CAS_MAX_ATTEMPTS} compare-and-set races; \
                     retry"
                ),
            })
        })
    }

    fn transitions<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        claim_id: &'a ClaimId,
        cursor: ClaimTransitionCursor,
    ) -> ClaimFuture<'a, ClaimTransitionPage> {
        Box::pin(async move {
            let after = i32::try_from(cursor.position().unwrap_or(0)).unwrap_or(i32::MAX);
            let probe = i64::try_from(cursor.limit())
                .unwrap_or(i64::MAX)
                .saturating_add(1);
            let params: [&(dyn ToSql + Sync); 5] = [
                &scope.tenant().as_str(),
                &scope.space().as_str(),
                &claim_id.as_str(),
                &after,
                &probe,
            ];
            let rows = self
                .client
                .query(TRANSITIONS_SQL, &params)
                .await
                .map_err(map_error)?;
            let more = rows.len() > cursor.limit();
            let mut transitions = Vec::with_capacity(rows.len().min(cursor.limit()));
            for row in rows.iter().take(cursor.limit()) {
                let record: Vec<u8> = row.get("record");
                transitions.push(decode_transition(&record)?);
            }
            let next = if more {
                let last = transitions.last().expect("a full page holds its limit");
                Some(ClaimTransitionCursor::after_ordinal(last.ordinal).with_limit(cursor.limit()))
            } else {
                None
            };
            Ok(ClaimTransitionPage { transitions, next })
        })
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimTombstoneRequest,
    ) -> ClaimFuture<'a, Claim> {
        Box::pin(self.tombstone_claim(scope, request))
    }

    fn delete<'a>(
        &'a self,
        scope: &'a KnowledgeSpaceScope,
        request: &'a ClaimDeleteRequest,
    ) -> ClaimFuture<'a, ()> {
        Box::pin(self.delete_claim(scope, request))
    }
}

/// The neighbor an edge yields for a frontier node, per direction — the
/// reference implementation's rule verbatim.
fn traversal_neighbor(
    claim: &Claim,
    node: &ClaimNodeId,
    direction: ClaimTraversalDirection,
) -> Option<ClaimNodeId> {
    let object = claim.object.node()?;
    match direction {
        ClaimTraversalDirection::Outbound if &claim.subject == node => Some(object.clone()),
        ClaimTraversalDirection::Inbound if object == node => Some(claim.subject.clone()),
        ClaimTraversalDirection::Both if &claim.subject == node => Some(object.clone()),
        ClaimTraversalDirection::Both if object == node => Some(claim.subject.clone()),
        _ => None,
    }
}

/// Answers a replayed append from its ledger bytes, refusing a cross-kind hit.
fn replayed_append(row: &Row, operation_id: &ClaimOperationId) -> ClaimResult<Claim> {
    let kind: Option<String> = row.get("replay_kind");
    let result: Option<Vec<u8>> = row.get("replay_result");
    match (kind.as_deref(), result) {
        (Some(KIND_APPEND), Some(result)) => decode_claim(&result),
        // Found before anything was inserted: this is what stops a replayed
        // append from restoring a tombstoned or deleted claim.
        (Some(KIND_ERASED), _) => Err(ClaimError::OperationErased {
            operation_id: operation_id.clone(),
        }),
        (Some(_), _) => Err(ClaimError::OperationConflict {
            operation_id: operation_id.clone(),
        }),
        _ => Err(ClaimError::Backend {
            backend: BACKEND_NAME.to_string(),
            message: "the ledger reported a replay without its stored result".to_string(),
        }),
    }
}

/// Answers a replayed transition from its ledger bytes, refusing a cross-kind
/// hit.
fn replayed_transition(
    row: &Row,
    operation_id: &ClaimOperationId,
) -> ClaimResult<ClaimTransitionOutcome> {
    let kind: Option<String> = row.get("replay_kind");
    let result: Option<Vec<u8>> = row.get("replay_result");
    match (kind.as_deref(), result) {
        (Some(KIND_TRANSITION), Some(result)) => decode_outcome(&result),
        (Some(KIND_ERASED), _) => Err(ClaimError::OperationErased {
            operation_id: operation_id.clone(),
        }),
        (Some(_), _) => Err(ClaimError::OperationConflict {
            operation_id: operation_id.clone(),
        }),
        _ => Err(ClaimError::Backend {
            backend: BACKEND_NAME.to_string(),
            message: "the ledger reported a replay without its stored result".to_string(),
        }),
    }
}

/// A denormalized column disagreed with the authoritative record it was
/// derived from — corruption, refused rather than repaired or skipped.
fn drift_error(claim_id: &ClaimId) -> ClaimError {
    ClaimError::Backend {
        backend: BACKEND_NAME.to_string(),
        message: format!(
            "the denormalized columns of claim {claim_id} disagree with its authoritative record"
        ),
    }
}

/// A bounded transition count as the column type; the crate cap (32) keeps
/// this infallible in practice, and an out-of-range value fails closed.
fn int_count(count: u32) -> ClaimResult<i32> {
    i32::try_from(count).map_err(|_| ClaimError::Backend {
        backend: BACKEND_NAME.to_string(),
        message: format!("the transition count {count} exceeds the column range"),
    })
}

/// Encodes one record as its canonical `serde_json` bytes.
fn encode<T: serde::Serialize>(value: &T) -> ClaimResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| ClaimError::Encoding {
        message: format!("the record could not be encoded: {error}"),
    })
}

/// Decodes a claim through its record mirror and [`Claim::restore`], so
/// schema gating, digest re-derivation, and coherence checks keep their typed
/// refusal codes; only a JSON parse failure is a backend error.
fn decode_claim(bytes: &[u8]) -> ClaimResult<Claim> {
    let record: ClaimRecord =
        serde_json::from_slice(bytes).map_err(|error| ClaimError::Backend {
            backend: BACKEND_NAME.to_string(),
            message: format!("a stored claim record could not be parsed: {error}"),
        })?;
    Claim::restore(record)
}

/// Decodes one claim row, refusing a `tombstoned` column that disagrees with
/// the record it sits beside.
///
/// Every read statement selects the column, so a stub record under a live
/// column — or a live record under a stub column — is refused on every load
/// rather than preferred either way.
fn decode_claim_row(row: &Row) -> ClaimResult<Claim> {
    let record: Vec<u8> = row.get("record");
    decode_claim_columns(&record, row.get("tombstoned"))
}

/// Decodes one claim from its record and the `tombstoned` column beside it.
fn decode_claim_columns(record: &[u8], tombstoned: bool) -> ClaimResult<Claim> {
    let claim = decode_claim(record)?;
    if tombstoned != claim.is_tombstoned() {
        return Err(drift_error(&claim.claim_id));
    }
    Ok(claim)
}

/// Decodes a transition through its record mirror and
/// [`ClaimTrustTransition::restore`], like [`decode_claim`].
fn decode_transition(bytes: &[u8]) -> ClaimResult<ClaimTrustTransition> {
    let record: ClaimTrustTransitionRecord =
        serde_json::from_slice(bytes).map_err(|error| ClaimError::Backend {
            backend: BACKEND_NAME.to_string(),
            message: format!("a stored transition record could not be parsed: {error}"),
        })?;
    ClaimTrustTransition::restore(record)
}

/// Decodes a ledger outcome; its embedded claim and transition run their own
/// restore validation inside deserialization.
fn decode_outcome(bytes: &[u8]) -> ClaimResult<ClaimTransitionOutcome> {
    serde_json::from_slice(bytes).map_err(|error| ClaimError::Backend {
        backend: BACKEND_NAME.to_string(),
        message: format!("a stored transition outcome could not be parsed: {error}"),
    })
}

/// Maps a PostgreSQL error into the claim-domain backend error.
///
/// A `tokio_postgres` database error stringifies to a bare "db error", so the
/// server's own message is pulled from the underlying database error when
/// present, keeping a failure diagnosable.
fn map_error(error: tokio_postgres::Error) -> ClaimError {
    let message = error.as_db_error().map_or_else(
        || error.to_string(),
        |db_error| db_error.message().to_string(),
    );
    ClaimError::Backend {
        backend: BACKEND_NAME.to_string(),
        message,
    }
}
