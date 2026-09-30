#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! PostgreSQL adapters for agent memory: session, snapshots, private
//! long-term records, and pgvector retrieval.
//!
//! This crate is the PostgreSQL binding for the agent-domain memory contracts
//! of [`rakka_agent`]: [`SessionMemoryStore`] scoped `(TenantId, AgentId,
//! AgentRunId)`, [`ContextSnapshotStore`] scoped by an immutable snapshot
//! reference, the [`AgentPrivateMemoryStore`] of authoritative agent-private
//! records, and the [`PgvectorPrivateMemoryRetriever`] that ranks those
//! records by derived vectors ([specification 13.2, 13.3, 13.5,
//! 13.6](../../../docs/plans/rakka-agent/spec.md)). It never changes the
//! agent-domain identity, provenance, idempotency, or snapshot-reuse semantics —
//! it only persists them.
//!
//! The retrieval adapter has its own migration
//! ([`PgvectorPrivateMemoryRetriever::migrate`]) because it needs the
//! `vector` extension; the three stores and [`MIGRATION_SQL`] stay green on
//! databases without pgvector. See [`PgvectorPrivateMemoryRetriever`] for
//! the recall characteristics, the derive/rebuild/retention runbook, and the
//! extension prerequisite.
//!
//! Idempotency is enforced by uniqueness constraints, so a replay is harmless: a
//! session append deduplicates on `(tenant, agent, run, operation_id)`, and a
//! snapshot persist is first-writer-wins on `(tenant, agent, run, snapshot_id)`.
//! A different entry that claims a sequence already taken by another operation
//! fails closed rather than overwriting it.
//!
//! The crate does not open its own connection: the deploying application supplies
//! a [`tokio_postgres::Client`] — created with its own TLS and credential choices
//! — and this crate runs bounded SQL against it. Schema is applied by
//! [`PostgresSessionMemoryStore::migrate`] /
//! [`PostgresContextSnapshotStore::migrate`], both idempotent.
//!
//! Gated tests run only when `RAKKA_POSTGRES_TEST_DSN` is set, like
//! `rakka-persistence-postgres`:
//!
//! ```sh
//! RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
//!     cargo test -p rakka-agent-postgres
//! ```

pub mod retrieval;

pub use retrieval::{
    pgvector_available, IndexOutcome, PgvectorDistance, PgvectorPrivateMemoryRetriever,
    PgvectorRetrieverConfig, ReindexPage, EMBEDDING_TABLE_NAME, PGVECTOR_MAX_DIMENSIONS,
    PGVECTOR_RETRIEVER_NAME, PGVECTOR_RETRIEVER_VERSION, VECTOR_MIGRATION_SQL,
};

use std::sync::Arc;

use rakka_agent::{
    check_memory_schema, check_private_memory_schema, AgentContextSnapshotRef, AgentPrivateMemory,
    AgentPrivateMemoryId, AgentPrivateMemoryStore, AgentRevisionNumber, AgentRunScope,
    AgentSchemaPolicy, AgentScope, ContextSnapshotStore, MemoryContextSnapshot, MemoryError,
    MemoryFuture, MemorySequence, MemoryTombstone, PrivateMemoryCursor, PrivateMemoryDeleteRequest,
    PrivateMemoryExpectation, PrivateMemoryPage, PrivateMemoryTombstoneRequest,
    SessionMemoryCursor, SessionMemoryEntry, SessionMemoryPage, SessionMemoryStore,
    SessionPurgeOutcome, SessionRetentionPolicy,
};
use rakka_agent_workflow::AgentTimestampMillis;
use tokio_postgres::error::SqlState;
use tokio_postgres::Client;

/// Stable backend name, reported in telemetry.
pub const BACKEND_NAME: &str = "postgres";

/// The session-memory table.
pub const SESSION_TABLE_NAME: &str = "rakka_agent_session_memory";

/// The context-snapshot table.
pub const SNAPSHOT_TABLE_NAME: &str = "rakka_agent_context_snapshot";

/// The agent-private long-term memory table.
pub const PRIVATE_MEMORY_TABLE_NAME: &str = "rakka_agent_private_memory";

/// The private-memory operation ledger table.
pub const PRIVATE_MEMORY_OP_TABLE_NAME: &str = "rakka_agent_private_memory_op";

/// Advisory-lock id serializing concurrent migrations of this crate's schema.
///
/// A bare `CREATE TABLE IF NOT EXISTS` can race two migrators against
/// PostgreSQL's system catalogs; taking a session advisory lock first makes the
/// migration safe to run concurrently, as `rakka-a2a` does for its projection.
/// The id is this crate's own: the value this slice replaced (`982_451_653`)
/// collided with `rakka-sharding-postgres`, needlessly serializing the two
/// subsystems' migrations in a shared database.
pub const MIGRATION_LOCK_ID: i64 = 982_451_881;

/// Idempotent schema for the memory tables.
///
/// The session table's `(tenant, agent, run, sequence)` primary key orders a
/// run's session and rejects two operations claiming one sequence; its
/// `(tenant, agent, run, operation_id)` unique index makes an append replay
/// harmless. The snapshot table's primary key makes a snapshot immutable and its
/// re-persist a no-op.
///
/// The private-memory table denormalizes `operation_id`, `revision`,
/// `expires_at`, and `tombstoned` from the record purely so SQL can
/// compare-and-set and filter without decoding BYTEA; the `record` column is
/// the authoritative [`rakka_agent::AgentPrivateMemory`], and for a tombstoned
/// row it *is* the content-free stub, so decode-time schema gating and the
/// record's own validation re-check on every load. The operation ledger is
/// what makes a replay answer with its original result and a deletion final: a
/// `NULL` result marks a payload erased by a later delete or purge, and its
/// replay fails closed rather than resurrect deleted content.
pub const MIGRATION_SQL: &str = "
CREATE TABLE IF NOT EXISTS rakka_agent_session_memory (
    tenant TEXT NOT NULL,
    agent TEXT NOT NULL,
    run TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    operation_id TEXT NOT NULL,
    entry BYTEA NOT NULL,
    PRIMARY KEY (tenant, agent, run, sequence),
    UNIQUE (tenant, agent, run, operation_id)
);

CREATE TABLE IF NOT EXISTS rakka_agent_context_snapshot (
    tenant TEXT NOT NULL,
    agent TEXT NOT NULL,
    run TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    snapshot BYTEA NOT NULL,
    PRIMARY KEY (tenant, agent, run, snapshot_id)
);

CREATE TABLE IF NOT EXISTS rakka_agent_private_memory (
    tenant TEXT NOT NULL,
    agent TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    expires_at BIGINT,
    tombstoned BOOLEAN NOT NULL DEFAULT FALSE,
    record BYTEA NOT NULL,
    PRIMARY KEY (tenant, agent, memory_id)
);

CREATE INDEX IF NOT EXISTS rakka_agent_private_memory_expiry
    ON rakka_agent_private_memory (tenant, agent, expires_at)
    WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS rakka_agent_private_memory_op (
    tenant TEXT NOT NULL,
    agent TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    memory_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('upsert', 'tombstone', 'delete')),
    result BYTEA,
    PRIMARY KEY (tenant, agent, operation_id)
);
";

/// The PostgreSQL short-term session-memory store.
#[derive(Clone)]
pub struct PostgresSessionMemoryStore {
    client: Arc<Client>,
    policy: AgentSchemaPolicy,
}

impl PostgresSessionMemoryStore {
    /// Creates a store over an owned client.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::from_shared_client(Arc::new(client))
    }

    /// Creates a store that shares an already-`Arc`-wrapped client.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>) -> Self {
        Self {
            client,
            policy: AgentSchemaPolicy::default(),
        }
    }

    /// Uses an explicit schema-compatibility policy for fail-closed loads.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Applies the idempotent schema.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::Backend`] if the DDL cannot be applied.
    pub async fn migrate(&self) -> Result<(), MemoryError> {
        apply_migration(&self.client).await
    }
}

impl SessionMemoryStore for PostgresSessionMemoryStore {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        entry: &'a SessionMemoryEntry,
    ) -> MemoryFuture<'a, SessionMemoryEntry> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(entry).map_err(|error| MemoryError::Encoding {
                message: error.to_string(),
            })?;
            let sequence =
                i64::try_from(entry.sequence.get()).map_err(|_| MemoryError::Backend {
                    backend: BACKEND_NAME.to_string(),
                    message: "the session memory sequence exceeds the storable range".to_string(),
                })?;
            let tenant = scope.tenant().as_str();
            let agent = scope.agent().as_str();
            let run = scope.run().as_str();
            let operation = entry.operation_id.as_str();

            let inserted = self
                .client
                .query_opt(
                    "INSERT INTO rakka_agent_session_memory \
                     (tenant, agent, run, sequence, operation_id, entry) \
                     VALUES ($1, $2, $3, $4, $5, $6) \
                     ON CONFLICT (tenant, agent, run, operation_id) DO NOTHING \
                     RETURNING sequence",
                    &[&tenant, &agent, &run, &sequence, &operation, &bytes],
                )
                .await;

            match inserted {
                // A fresh insert: the entry is now stored exactly as given.
                Ok(Some(_)) => Ok(entry.clone()),
                // The operation id already exists: an append replay. Return the
                // entry stored under it, the original logical result.
                Ok(None) => {
                    let row = self
                        .client
                        .query_one(
                            "SELECT entry FROM rakka_agent_session_memory \
                             WHERE tenant = $1 AND agent = $2 AND run = $3 AND operation_id = $4",
                            &[&tenant, &agent, &run, &operation],
                        )
                        .await
                        .map_err(map_error)?;
                    self.decode_entry(row.get::<_, Vec<u8>>("entry"))
                }
                // A different operation claimed this sequence: fail closed.
                Err(error) if is_unique_violation(&error) => Err(MemoryError::SequenceConflict {
                    sequence: entry.sequence,
                }),
                Err(error) => Err(map_error(error)),
            }
        })
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        cursor: SessionMemoryCursor,
    ) -> MemoryFuture<'a, SessionMemoryPage> {
        Box::pin(async move {
            let tenant = scope.tenant().as_str();
            let agent = scope.agent().as_str();
            let run = scope.run().as_str();
            let after =
                i64::try_from(cursor.position().map_or(0, MemorySequence::get)).unwrap_or(i64::MAX);
            // One extra row tells us whether another page remains.
            let limit = i64::try_from(cursor.limit().saturating_add(1)).unwrap_or(i64::MAX);

            let rows = self
                .client
                .query(
                    "SELECT entry FROM rakka_agent_session_memory \
                     WHERE tenant = $1 AND agent = $2 AND run = $3 AND sequence > $4 \
                     ORDER BY sequence ASC LIMIT $5",
                    &[&tenant, &agent, &run, &after, &limit],
                )
                .await
                .map_err(map_error)?;

            let mut entries: Vec<SessionMemoryEntry> = Vec::with_capacity(rows.len());
            for row in rows {
                entries.push(self.decode_entry(row.get::<_, Vec<u8>>("entry"))?);
            }

            let next = (entries.len() > cursor.limit())
                .then(|| {
                    entries.pop();
                    entries.last().map(|entry| {
                        SessionMemoryCursor::after(entry.sequence).with_limit(cursor.limit())
                    })
                })
                .flatten();

            Ok(SessionMemoryPage { entries, next })
        })
    }

    fn purge_run<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        policy: &'a SessionRetentionPolicy,
        terminal_at: AgentTimestampMillis,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, SessionPurgeOutcome> {
        Box::pin(async move {
            if policy.legal_hold() {
                return Ok(SessionPurgeOutcome::Held);
            }
            if now < policy.purge_due_at(terminal_at) {
                return Ok(SessionPurgeOutcome::NotYetDue);
            }
            let deleted = self
                .client
                .execute(
                    "DELETE FROM rakka_agent_session_memory \
                     WHERE tenant = $1 AND agent = $2 AND run = $3",
                    &[
                        &scope.tenant().as_str(),
                        &scope.agent().as_str(),
                        &scope.run().as_str(),
                    ],
                )
                .await
                .map_err(map_error)?;
            Ok(SessionPurgeOutcome::Purged { entries: deleted })
        })
    }
}

impl PostgresSessionMemoryStore {
    /// Decodes a stored entry and fails closed on an unsupported schema version.
    fn decode_entry(&self, bytes: Vec<u8>) -> Result<SessionMemoryEntry, MemoryError> {
        let entry: SessionMemoryEntry =
            serde_json::from_slice(&bytes).map_err(|error| MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: format!("a stored session entry could not be decoded: {error}"),
            })?;
        check_memory_schema(&self.policy, &entry)?;
        Ok(entry)
    }
}

/// The PostgreSQL immutable context-snapshot store.
#[derive(Clone)]
pub struct PostgresContextSnapshotStore {
    client: Arc<Client>,
    policy: AgentSchemaPolicy,
}

impl PostgresContextSnapshotStore {
    /// Creates a store over an owned client.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::from_shared_client(Arc::new(client))
    }

    /// Creates a store that shares an already-`Arc`-wrapped client.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>) -> Self {
        Self {
            client,
            policy: AgentSchemaPolicy::default(),
        }
    }

    /// Uses an explicit schema-compatibility policy for fail-closed loads.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Applies the idempotent schema.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::Backend`] if the DDL cannot be applied.
    pub async fn migrate(&self) -> Result<(), MemoryError> {
        apply_migration(&self.client).await
    }

    /// Decodes a stored snapshot and fails closed on an unsupported schema
    /// version.
    fn decode_snapshot(&self, bytes: Vec<u8>) -> Result<MemoryContextSnapshot, MemoryError> {
        let snapshot: MemoryContextSnapshot =
            serde_json::from_slice(&bytes).map_err(|error| MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: format!("a stored snapshot could not be decoded: {error}"),
            })?;
        self.policy
            .check_record(&snapshot)
            .map_err(MemoryError::from)?;
        Ok(snapshot)
    }
}

impl ContextSnapshotStore for PostgresContextSnapshotStore {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn persist<'a>(
        &'a self,
        snapshot: &'a MemoryContextSnapshot,
    ) -> MemoryFuture<'a, MemoryContextSnapshot> {
        Box::pin(async move {
            let bytes = serde_json::to_vec(snapshot).map_err(|error| MemoryError::Encoding {
                message: error.to_string(),
            })?;
            let tenant = snapshot.scope.tenant().as_str();
            let agent = snapshot.scope.agent().as_str();
            let run = snapshot.scope.run().as_str();
            let snapshot_id = snapshot.reference.snapshot_id.as_str();

            // First writer wins: a snapshot is immutable, so a re-persist reads
            // the original back rather than replacing it.
            self.client
                .execute(
                    "INSERT INTO rakka_agent_context_snapshot \
                     (tenant, agent, run, snapshot_id, snapshot) \
                     VALUES ($1, $2, $3, $4, $5) \
                     ON CONFLICT (tenant, agent, run, snapshot_id) DO NOTHING",
                    &[&tenant, &agent, &run, &snapshot_id, &bytes],
                )
                .await
                .map_err(map_error)?;

            let row = self
                .client
                .query_one(
                    "SELECT snapshot FROM rakka_agent_context_snapshot \
                     WHERE tenant = $1 AND agent = $2 AND run = $3 AND snapshot_id = $4",
                    &[&tenant, &agent, &run, &snapshot_id],
                )
                .await
                .map_err(map_error)?;
            self.decode_snapshot(row.get::<_, Vec<u8>>("snapshot"))
        })
    }

    fn load<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        reference: &'a AgentContextSnapshotRef,
    ) -> MemoryFuture<'a, Option<MemoryContextSnapshot>> {
        Box::pin(async move {
            let tenant = scope.tenant().as_str();
            let agent = scope.agent().as_str();
            let run = scope.run().as_str();
            let snapshot_id = reference.snapshot_id.as_str();

            let row = self
                .client
                .query_opt(
                    "SELECT snapshot FROM rakka_agent_context_snapshot \
                     WHERE tenant = $1 AND agent = $2 AND run = $3 AND snapshot_id = $4",
                    &[&tenant, &agent, &run, &snapshot_id],
                )
                .await
                .map_err(map_error)?;

            match row {
                Some(row) => Ok(Some(
                    self.decode_snapshot(row.get::<_, Vec<u8>>("snapshot"))?,
                )),
                None => Ok(None),
            }
        })
    }

    fn purge_run<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        policy: &'a SessionRetentionPolicy,
        terminal_at: AgentTimestampMillis,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, SessionPurgeOutcome> {
        Box::pin(async move {
            if policy.legal_hold() {
                return Ok(SessionPurgeOutcome::Held);
            }
            if now < policy.purge_due_at(terminal_at) {
                return Ok(SessionPurgeOutcome::NotYetDue);
            }
            let deleted = self
                .client
                .execute(
                    "DELETE FROM rakka_agent_context_snapshot \
                     WHERE tenant = $1 AND agent = $2 AND run = $3",
                    &[
                        &scope.tenant().as_str(),
                        &scope.agent().as_str(),
                        &scope.run().as_str(),
                    ],
                )
                .await
                .map_err(map_error)?;
            Ok(SessionPurgeOutcome::Purged { entries: deleted })
        })
    }
}

/// The PostgreSQL agent-private long-term memory store
/// ([specification 13.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// It implements the exact write table the
/// [`AgentPrivateMemoryStore`] contract documents. Every write is **one
/// data-modifying-CTE statement**: a single statement is a single implicit
/// transaction, so the operation-ledger row and the memory-row mutation
/// commit or fail together — there is no crash window where an applied
/// operation lacks its ledger row — without holding raw `BEGIN`/`COMMIT`
/// state on the shared pipelined client. The `WHERE revision = $n` update is
/// the genuinely concurrent compare-and-set of scenario 15: of two racing
/// writers, exactly one row-locks and wins, and the loser surfaces
/// `memory-revision-conflict`.
#[derive(Clone)]
pub struct PostgresAgentPrivateMemoryStore {
    client: Arc<Client>,
    policy: AgentSchemaPolicy,
}

impl PostgresAgentPrivateMemoryStore {
    /// Creates a store over an owned client.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::from_shared_client(Arc::new(client))
    }

    /// Creates a store that shares an already-`Arc`-wrapped client.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>) -> Self {
        Self {
            client,
            policy: AgentSchemaPolicy::default(),
        }
    }

    /// Uses an explicit schema-compatibility policy for fail-closed loads.
    #[must_use]
    pub fn with_schema_policy(mut self, policy: AgentSchemaPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Applies the idempotent schema.
    ///
    /// # Errors
    ///
    /// Returns [`MemoryError::Backend`] if the DDL cannot be applied.
    pub async fn migrate(&self) -> Result<(), MemoryError> {
        apply_migration(&self.client).await
    }

    /// Decodes a stored memory and fails closed on an unsupported schema
    /// version.
    fn decode_memory(&self, bytes: &[u8]) -> Result<AgentPrivateMemory, MemoryError> {
        let memory: AgentPrivateMemory =
            serde_json::from_slice(bytes).map_err(|error| MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: format!("a stored private memory could not be decoded: {error}"),
            })?;
        check_private_memory_schema(&self.policy, &memory)?;
        Ok(memory)
    }

    /// Answers a replayed operation from its ledger row: the original result,
    /// or the fail-closed refusal when a later deletion erased its payload.
    fn replay_result(
        &self,
        result: Option<Vec<u8>>,
        operation_id: &rakka_agent::MemoryOperationId,
    ) -> Result<AgentPrivateMemory, MemoryError> {
        match result {
            Some(bytes) => self.decode_memory(&bytes),
            None => Err(MemoryError::OperationErased {
                operation_id: operation_id.clone(),
            }),
        }
    }

    /// Reads a replayed operation's ledger row, if one exists.
    async fn ledger_row(
        &self,
        tenant: &str,
        agent: &str,
        operation_id: &str,
    ) -> Result<Option<(String, Option<Vec<u8>>)>, MemoryError> {
        let row = self
            .client
            .query_opt(
                "SELECT kind, result FROM rakka_agent_private_memory_op \
                 WHERE tenant = $1 AND agent = $2 AND operation_id = $3",
                &[&tenant, &agent, &operation_id],
            )
            .await
            .map_err(map_error)?;
        Ok(row.map(|row| (row.get("kind"), row.get("result"))))
    }
}

/// The denormalized columns one write stamps beside the authoritative record.
fn denormalize(
    memory: &AgentPrivateMemory,
) -> Result<(i64, Option<i64>, bool, Vec<u8>), MemoryError> {
    let revision = i64::try_from(memory.revision.get()).map_err(|_| MemoryError::Backend {
        backend: BACKEND_NAME.to_string(),
        message: "the memory revision exceeds the storable range".to_string(),
    })?;
    let expires_at = match memory.retention.expires_at() {
        None => None,
        Some(at) => Some(
            i64::try_from(at.as_millis()).map_err(|_| MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: "the memory expiry exceeds the storable range".to_string(),
            })?,
        ),
    };
    let bytes = serde_json::to_vec(memory).map_err(|error| MemoryError::Encoding {
        message: error.to_string(),
    })?;
    Ok((revision, expires_at, memory.is_tombstoned(), bytes))
}

impl AgentPrivateMemoryStore for PostgresAgentPrivateMemoryStore {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn upsert<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory: &'a AgentPrivateMemory,
        expected: PrivateMemoryExpectation,
    ) -> MemoryFuture<'a, AgentPrivateMemory> {
        Box::pin(async move {
            let tenant = scope.tenant().as_str().to_string();
            let agent = scope.agent().as_str().to_string();
            let memory_id = memory.memory_id.as_str().to_string();
            let operation = memory.operation_id.as_str().to_string();

            // The store stamps the revision itself; the caller's field is
            // ignored, exactly as the contract documents.
            let mut stamped = memory.clone();
            stamped.revision = match expected {
                PrivateMemoryExpectation::Absent => AgentRevisionNumber::INITIAL,
                PrivateMemoryExpectation::Revision(revision) => revision.next(),
            };
            let (revision, expires_at, _tombstoned, bytes) = denormalize(&stamped)?;

            let row = match expected {
                PrivateMemoryExpectation::Absent => self
                    .client
                    .query_one(
                        "WITH existing_op AS (
                             SELECT result FROM rakka_agent_private_memory_op
                             WHERE tenant = $1 AND agent = $2 AND operation_id = $3
                         ), ins_row AS (
                             INSERT INTO rakka_agent_private_memory
                                 (tenant, agent, memory_id, operation_id, revision,
                                  expires_at, tombstoned, record)
                             SELECT $1, $2, $4, $3, $5, $6, FALSE, $7
                             WHERE NOT EXISTS (SELECT 1 FROM existing_op)
                             ON CONFLICT (tenant, agent, memory_id) DO NOTHING
                             RETURNING record
                         ), ins_op AS (
                             INSERT INTO rakka_agent_private_memory_op
                                 (tenant, agent, operation_id, memory_id, kind, result)
                             SELECT $1, $2, $3, $4, 'upsert', record FROM ins_row
                             ON CONFLICT (tenant, agent, operation_id) DO NOTHING
                         )
                         SELECT EXISTS (SELECT 1 FROM existing_op) AS replayed,
                                (SELECT result FROM existing_op)   AS replay_result,
                                (SELECT record FROM ins_row)       AS applied",
                        &[
                            &tenant,
                            &agent,
                            &operation,
                            &memory_id,
                            &revision,
                            &expires_at,
                            &bytes,
                        ],
                    )
                    .await
                    .map_err(map_error)?,
                PrivateMemoryExpectation::Revision(expected_revision) => {
                    let expected_raw = i64::try_from(expected_revision.get()).map_err(|_| {
                        MemoryError::Backend {
                            backend: BACKEND_NAME.to_string(),
                            message: "the expected revision exceeds the storable range".to_string(),
                        }
                    })?;
                    self.client
                        .query_one(
                            "WITH existing_op AS (
                                 SELECT result FROM rakka_agent_private_memory_op
                                 WHERE tenant = $1 AND agent = $2 AND operation_id = $3
                             ), upd AS (
                                 UPDATE rakka_agent_private_memory
                                 SET operation_id = $3, revision = $5, expires_at = $6,
                                     record = $7
                                 WHERE tenant = $1 AND agent = $2 AND memory_id = $4
                                   AND revision = $8 AND tombstoned = FALSE
                                   AND NOT EXISTS (SELECT 1 FROM existing_op)
                                 RETURNING record
                             ), ins_op AS (
                                 INSERT INTO rakka_agent_private_memory_op
                                     (tenant, agent, operation_id, memory_id, kind, result)
                                 SELECT $1, $2, $3, $4, 'upsert', record FROM upd
                                 ON CONFLICT (tenant, agent, operation_id) DO NOTHING
                             )
                             SELECT EXISTS (SELECT 1 FROM existing_op) AS replayed,
                                    (SELECT result FROM existing_op)   AS replay_result,
                                    (SELECT record FROM upd)           AS applied",
                            &[
                                &tenant,
                                &agent,
                                &operation,
                                &memory_id,
                                &revision,
                                &expires_at,
                                &bytes,
                                &expected_raw,
                            ],
                        )
                        .await
                        .map_err(map_error)?
                }
            };

            if row.get::<_, bool>("replayed") {
                return self.replay_result(row.get("replay_result"), &memory.operation_id);
            }
            if let Some(bytes) = row.get::<_, Option<Vec<u8>>>("applied") {
                return self.decode_memory(&bytes);
            }

            // Neither replayed nor applied: establish which refusal this is.
            // A same-operation race may have applied concurrently, so the
            // ledger is consulted again before the row decides.
            if let Some((_, result)) = self.ledger_row(&tenant, &agent, &operation).await? {
                return self.replay_result(result, &memory.operation_id);
            }
            let current = self
                .client
                .query_opt(
                    "SELECT revision, tombstoned FROM rakka_agent_private_memory \
                     WHERE tenant = $1 AND agent = $2 AND memory_id = $3",
                    &[&tenant, &agent, &memory_id],
                )
                .await
                .map_err(map_error)?;
            match (expected, current) {
                (PrivateMemoryExpectation::Absent, Some(_)) => Err(MemoryError::AlreadyExists {
                    memory_id: memory.memory_id.clone(),
                }),
                (PrivateMemoryExpectation::Revision(_), None) => Err(MemoryError::NotFound {
                    memory_id: memory.memory_id.clone(),
                }),
                (PrivateMemoryExpectation::Revision(_), Some(row))
                    if row.get::<_, bool>("tombstoned") =>
                {
                    Err(MemoryError::Tombstoned {
                        memory_id: memory.memory_id.clone(),
                    })
                }
                (PrivateMemoryExpectation::Revision(expected_revision), Some(row)) => {
                    let actual = u64::try_from(row.get::<_, i64>("revision")).unwrap_or(0);
                    Err(MemoryError::RevisionConflict {
                        memory_id: memory.memory_id.clone(),
                        expected: expected_revision,
                        actual: AgentRevisionNumber::new(actual),
                    })
                }
                (PrivateMemoryExpectation::Absent, None) => Err(MemoryError::Backend {
                    backend: BACKEND_NAME.to_string(),
                    message: "the create raced a concurrent deletion; retry the operation"
                        .to_string(),
                }),
            }
        })
    }

    fn get<'a>(
        &'a self,
        scope: &'a AgentScope,
        memory_id: &'a AgentPrivateMemoryId,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentPrivateMemory>> {
        Box::pin(async move {
            let now = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
            let row = self
                .client
                .query_opt(
                    "SELECT record FROM rakka_agent_private_memory \
                     WHERE tenant = $1 AND agent = $2 AND memory_id = $3 \
                       AND (expires_at IS NULL OR expires_at > $4)",
                    &[
                        &scope.tenant().as_str(),
                        &scope.agent().as_str(),
                        &memory_id.as_str(),
                        &now,
                    ],
                )
                .await
                .map_err(map_error)?;
            match row {
                Some(row) => Ok(Some(self.decode_memory(&row.get::<_, Vec<u8>>("record"))?)),
                None => Ok(None),
            }
        })
    }

    fn list<'a>(
        &'a self,
        scope: &'a AgentScope,
        cursor: PrivateMemoryCursor,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, PrivateMemoryPage> {
        Box::pin(async move {
            let now = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
            let after = cursor
                .position()
                .map_or(String::new(), |id| id.as_str().to_string());
            let include_tombstoned = cursor.tombstoned_included();
            // One extra row tells us whether another page remains.
            let limit = i64::try_from(cursor.limit().saturating_add(1)).unwrap_or(i64::MAX);

            let rows = self
                .client
                .query(
                    "SELECT record FROM rakka_agent_private_memory \
                     WHERE tenant = $1 AND agent = $2 AND memory_id > $3 \
                       AND (expires_at IS NULL OR expires_at > $4) \
                       AND (tombstoned = FALSE OR $5) \
                     ORDER BY memory_id ASC LIMIT $6",
                    &[
                        &scope.tenant().as_str(),
                        &scope.agent().as_str(),
                        &after,
                        &now,
                        &include_tombstoned,
                        &limit,
                    ],
                )
                .await
                .map_err(map_error)?;

            let mut memories: Vec<AgentPrivateMemory> = Vec::with_capacity(rows.len());
            for row in rows {
                memories.push(self.decode_memory(&row.get::<_, Vec<u8>>("record"))?);
            }
            let next = (memories.len() > cursor.limit())
                .then(|| {
                    memories.pop();
                    memories.last().map(|memory| {
                        let next = PrivateMemoryCursor::after(memory.memory_id.clone())
                            .with_limit(cursor.limit());
                        if include_tombstoned {
                            next.include_tombstoned()
                        } else {
                            next
                        }
                    })
                })
                .flatten();

            Ok(PrivateMemoryPage { memories, next })
        })
    }

    fn tombstone<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryTombstoneRequest,
    ) -> MemoryFuture<'a, AgentPrivateMemory> {
        Box::pin(async move {
            let tenant = scope.tenant().as_str().to_string();
            let agent = scope.agent().as_str().to_string();
            let memory_id = request.memory_id.as_str().to_string();
            let operation = request.operation_id.as_str().to_string();

            // Read-then-CAS, bounded: a concurrent update moves the revision
            // and the compare-and-set below misses, so the read is retried a
            // few times before failing over to the backend error.
            for _attempt in 0..4 {
                if let Some((_, result)) = self.ledger_row(&tenant, &agent, &operation).await? {
                    return self.replay_result(result, &request.operation_id);
                }
                let current = self
                    .client
                    .query_opt(
                        "SELECT record FROM rakka_agent_private_memory \
                         WHERE tenant = $1 AND agent = $2 AND memory_id = $3",
                        &[&tenant, &agent, &memory_id],
                    )
                    .await
                    .map_err(map_error)?;
                let Some(row) = current else {
                    return Err(MemoryError::NotFound {
                        memory_id: request.memory_id.clone(),
                    });
                };
                let current = self.decode_memory(&row.get::<_, Vec<u8>>("record"))?;
                if current.is_tombstoned() {
                    return Err(MemoryError::Tombstoned {
                        memory_id: request.memory_id.clone(),
                    });
                }

                let mut stub = current.clone();
                stub.content = AgentPrivateMemory::tombstone_content();
                stub.tombstone = Some(MemoryTombstone {
                    operation_id: request.operation_id.clone(),
                    reason: request.reason,
                    tombstoned_at: request.tombstoned_at,
                });
                stub.operation_id = request.operation_id.clone();
                stub.revision = current.revision.next();
                stub.updated_at = request.tombstoned_at;
                let (new_revision, expires_at, _tombstoned, bytes) = denormalize(&stub)?;
                let held_revision =
                    i64::try_from(current.revision.get()).map_err(|_| MemoryError::Backend {
                        backend: BACKEND_NAME.to_string(),
                        message: "the memory revision exceeds the storable range".to_string(),
                    })?;

                let row = self
                    .client
                    .query_one(
                        "WITH upd AS (
                             UPDATE rakka_agent_private_memory
                             SET operation_id = $3, revision = $5, expires_at = $6,
                                 tombstoned = TRUE, record = $7
                             WHERE tenant = $1 AND agent = $2 AND memory_id = $4
                               AND revision = $8 AND tombstoned = FALSE
                             RETURNING record
                         ), erase AS (
                             UPDATE rakka_agent_private_memory_op SET result = NULL
                             WHERE tenant = $1 AND agent = $2 AND memory_id = $4
                               AND operation_id <> $3
                               AND EXISTS (SELECT 1 FROM upd)
                         ), ins_op AS (
                             INSERT INTO rakka_agent_private_memory_op
                                 (tenant, agent, operation_id, memory_id, kind, result)
                             SELECT $1, $2, $3, $4, 'tombstone', record FROM upd
                             ON CONFLICT (tenant, agent, operation_id) DO NOTHING
                         )
                         SELECT (SELECT record FROM upd) AS applied",
                        &[
                            &tenant,
                            &agent,
                            &operation,
                            &memory_id,
                            &new_revision,
                            &expires_at,
                            &bytes,
                            &held_revision,
                        ],
                    )
                    .await
                    .map_err(map_error)?;
                if let Some(bytes) = row.get::<_, Option<Vec<u8>>>("applied") {
                    return self.decode_memory(&bytes);
                }
                // The compare-and-set missed; loop re-reads and re-decides.
            }
            Err(MemoryError::Backend {
                backend: BACKEND_NAME.to_string(),
                message: "the tombstone lost its compare-and-set repeatedly; retry".to_string(),
            })
        })
    }

    fn delete<'a>(
        &'a self,
        scope: &'a AgentScope,
        request: &'a PrivateMemoryDeleteRequest,
    ) -> MemoryFuture<'a, ()> {
        Box::pin(async move {
            let tenant = scope.tenant().as_str().to_string();
            let agent = scope.agent().as_str().to_string();
            let memory_id = request.memory_id.as_str().to_string();
            let operation = request.operation_id.as_str().to_string();

            let row = self
                .client
                .query_one(
                    "WITH existing_op AS (
                         SELECT kind FROM rakka_agent_private_memory_op
                         WHERE tenant = $1 AND agent = $2 AND operation_id = $3
                     ), del AS (
                         DELETE FROM rakka_agent_private_memory
                         WHERE tenant = $1 AND agent = $2 AND memory_id = $4
                           AND NOT EXISTS (SELECT 1 FROM existing_op)
                         RETURNING memory_id
                     ), erase AS (
                         UPDATE rakka_agent_private_memory_op SET result = NULL
                         WHERE tenant = $1 AND agent = $2 AND memory_id = $4
                           AND EXISTS (SELECT 1 FROM del)
                     ), ins_op AS (
                         INSERT INTO rakka_agent_private_memory_op
                             (tenant, agent, operation_id, memory_id, kind, result)
                         SELECT $1, $2, $3, $4, 'delete', NULL FROM del
                         ON CONFLICT (tenant, agent, operation_id) DO NOTHING
                     )
                     SELECT EXISTS (SELECT 1 FROM existing_op) AS replayed,
                            (SELECT kind FROM existing_op)     AS replay_kind,
                            EXISTS (SELECT 1 FROM del)         AS deleted",
                    &[&tenant, &agent, &operation, &memory_id],
                )
                .await
                .map_err(map_error)?;

            if row.get::<_, bool>("replayed") {
                return match row.get::<_, Option<String>>("replay_kind").as_deref() {
                    Some("delete") => Ok(()),
                    _ => Err(MemoryError::OperationConflict {
                        operation_id: request.operation_id.clone(),
                    }),
                };
            }
            if row.get::<_, bool>("deleted") {
                return Ok(());
            }
            Err(MemoryError::NotFound {
                memory_id: request.memory_id.clone(),
            })
        })
    }

    fn purge_expired<'a>(
        &'a self,
        scope: &'a AgentScope,
        now: AgentTimestampMillis,
        limit: usize,
    ) -> MemoryFuture<'a, u64> {
        Box::pin(async move {
            let now = i64::try_from(now.as_millis()).unwrap_or(i64::MAX);
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let row = self
                .client
                .query_one(
                    "WITH victims AS (
                         SELECT memory_id FROM rakka_agent_private_memory
                         WHERE tenant = $1 AND agent = $2
                           AND expires_at IS NOT NULL AND expires_at <= $3
                         ORDER BY memory_id LIMIT $4
                     ), erase AS (
                         UPDATE rakka_agent_private_memory_op SET result = NULL
                         WHERE tenant = $1 AND agent = $2
                           AND memory_id IN (SELECT memory_id FROM victims)
                     ), del AS (
                         DELETE FROM rakka_agent_private_memory
                         WHERE tenant = $1 AND agent = $2
                           AND memory_id IN (SELECT memory_id FROM victims)
                         RETURNING memory_id
                     )
                     SELECT count(*) AS purged FROM del",
                    &[
                        &scope.tenant().as_str(),
                        &scope.agent().as_str(),
                        &now,
                        &limit,
                    ],
                )
                .await
                .map_err(map_error)?;
            Ok(u64::try_from(row.get::<_, i64>("purged")).unwrap_or(0))
        })
    }
}

/// Applies the idempotent schema under an advisory lock, so concurrent
/// migrators do not race the system catalogs.
async fn apply_migration(client: &Client) -> Result<(), MemoryError> {
    apply_sql_under_migration_lock(client, MIGRATION_SQL).await
}

/// Applies one idempotent DDL batch under the crate's advisory lock.
///
/// One crate, one schema, one lock: the base migration and the vector
/// migration serialize against each other harmlessly. The batch is one
/// implicit transaction, so a failure applies nothing.
async fn apply_sql_under_migration_lock(client: &Client, sql: &str) -> Result<(), MemoryError> {
    client
        .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
        .await
        .map_err(map_error)?;
    let applied = client.batch_execute(sql).await;
    let unlocked = client
        .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_ID])
        .await;
    applied.map_err(map_error)?;
    unlocked.map_err(map_error)?;
    Ok(())
}

/// Whether a PostgreSQL error is a unique-constraint violation.
fn is_unique_violation(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|db_error| db_error.code() == &SqlState::UNIQUE_VIOLATION)
}

/// Maps a PostgreSQL error into the agent-domain memory error.
///
/// A `tokio_postgres` database error stringifies to a bare "db error", so the
/// server's own message is pulled from the [`tokio_postgres::error::DbError`]
/// when present, keeping a failure diagnosable.
fn map_error(error: tokio_postgres::Error) -> MemoryError {
    let message = error.as_db_error().map_or_else(
        || error.to_string(),
        |db_error| db_error.message().to_string(),
    );
    MemoryError::Backend {
        backend: BACKEND_NAME.to_string(),
        message,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rakka_agent::{
        assemble_session_context, AgentId, AgentRevisionNumber, AgentRunId, AgentTaskContent,
        MemoryClassification, MemoryEntryId, MemoryEntryRole, MemoryOperationId, MemorySequence,
        SessionMemoryCursor, SessionWindowPolicy, TenantId,
    };
    use rakka_agent_workflow::AgentTimestampMillis;
    use tokio_postgres::NoTls;

    use super::*;

    /// A unique suffix so concurrent test runs against one database never
    /// collide on a tenant.
    fn unique() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_nanos();
        format!("t{nanos}")
    }

    fn run_scope(tenant: &str, agent: &str, run: &str) -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new(tenant),
            AgentId::new(agent).expect("agent id"),
            AgentRunId::new(run).expect("run id"),
        )
        .expect("run scope")
    }

    fn entry(scope: &AgentRunScope, slot: &str, sequence: u64) -> SessionMemoryEntry {
        SessionMemoryEntry::new(
            MemoryEntryId::derive(scope, slot).expect("entry id"),
            MemoryOperationId::derive(scope, slot).expect("op id"),
            MemorySequence::new(sequence),
            MemoryEntryRole::Assistant,
            AgentTaskContent::inline(serde_json::json!({ "slot": slot })).expect("content"),
            1,
            None,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(sequence),
        )
        .expect("the entry is bounded")
    }

    async fn client() -> Option<Client> {
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
        Some(client)
    }

    #[tokio::test]
    async fn postgres_session_isolation_and_idempotent_append_when_dsn_is_set() {
        // Scenarios 14 and 16 against the PostgreSQL store.
        let Some(client) = client().await else {
            return;
        };
        let store = PostgresSessionMemoryStore::from_shared_client(Arc::new(client));
        store.migrate().await.expect("migrate");

        let tenant = unique();
        let run_a = run_scope(&tenant, "support", "run-a");
        let run_b = run_scope(&tenant, "support", "run-b");
        let other_agent = run_scope(&tenant, "billing", "run-a");

        let first = entry(&run_a, "assistant", 1);
        let a = store.append(&run_a, &first).await.expect("first append");
        // Scenario 16: a replay returns the original without a second entry.
        let b = store.append(&run_a, &first).await.expect("replay append");
        assert_eq!(a, b);
        store
            .append(&run_a, &entry(&run_a, "second", 2))
            .await
            .expect("second append");

        let page_a = store
            .read(&run_a, SessionMemoryCursor::start())
            .await
            .expect("read run a");
        assert_eq!(page_a.entries.len(), 2, "the replay added no entry");

        // Scenario 14: a sibling run and another agent see nothing.
        let page_b = store
            .read(&run_b, SessionMemoryCursor::start())
            .await
            .expect("read run b");
        assert!(page_b.entries.is_empty(), "a sibling run is isolated");
        let page_other = store
            .read(&other_agent, SessionMemoryCursor::start())
            .await
            .expect("read other agent");
        assert!(page_other.entries.is_empty(), "another agent is isolated");
    }

    #[tokio::test]
    async fn postgres_snapshot_is_immutable_when_dsn_is_set() {
        // Scenario 17 against the PostgreSQL store.
        let Some(client) = client().await else {
            return;
        };
        let shared = Arc::new(client);
        let session = PostgresSessionMemoryStore::from_shared_client(shared.clone());
        let snapshots = PostgresContextSnapshotStore::from_shared_client(shared);
        session.migrate().await.expect("migrate session");
        snapshots.migrate().await.expect("migrate snapshots");

        let tenant = unique();
        let scope = run_scope(&tenant, "support", "run-1");
        session
            .append(&scope, &entry(&scope, "assistant", 1))
            .await
            .expect("first turn recorded");

        let reference = AgentContextSnapshotRef::for_turn(&scope, 2).expect("ref");
        let window = SessionWindowPolicy::recent_window();
        let assembled = assemble_session_context(
            &session,
            &scope,
            &reference,
            2,
            &window,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(10),
        )
        .await
        .expect("assemble");
        assert_eq!(assembled.session.len(), 1);
        let stored = snapshots.persist(&assembled).await.expect("persist");

        // Newer memory arrives; a re-assembly sees it, but the immutable store
        // returns the original snapshot on re-persist and on load.
        session
            .append(&scope, &entry(&scope, "later", 2))
            .await
            .expect("newer memory");
        let reassembled = assemble_session_context(
            &session,
            &scope,
            &reference,
            2,
            &window,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(20),
        )
        .await
        .expect("reassemble");
        assert_eq!(
            reassembled.session.len(),
            2,
            "the re-assembly saw newer memory"
        );

        let re_persisted = snapshots.persist(&reassembled).await.expect("re-persist");
        assert_eq!(re_persisted, stored, "the re-persist returned the original");
        let loaded = snapshots
            .load(&scope, &reference)
            .await
            .expect("load")
            .expect("the snapshot exists");
        assert_eq!(loaded, stored);
        assert_eq!(
            loaded.session.len(),
            1,
            "the original snapshot is immutable"
        );
    }

    // =======================================================================
    // Agent-private long-term memory (slice 2.1).
    // =======================================================================

    use rakka_agent::{
        AgentPrivateMemoryKind, MemoryRetention, MemoryTombstoneReason, PrivateMemoryScope,
    };

    fn agent_scope(tenant: &str, agent: &str) -> PrivateMemoryScope {
        AgentScope::new(
            TenantId::new(tenant),
            AgentId::new(agent).expect("agent id"),
        )
        .expect("agent scope")
    }

    fn memory(scope: &AgentScope, slot: &str, at: u64) -> AgentPrivateMemory {
        AgentPrivateMemory::new(
            AgentPrivateMemoryId::new(format!("mem-{slot}")).expect("memory id"),
            MemoryOperationId::derive_for_agent(scope, format!("write-{slot}-{at}"))
                .expect("op id"),
            AgentPrivateMemoryKind::Semantic,
            AgentTaskContent::inline(serde_json::json!({ "slot": slot, "at": at }))
                .expect("content"),
            9_000,
            MemoryClassification::Unclassified,
            AgentTimestampMillis::new(at),
        )
        .expect("the memory is bounded")
    }

    #[tokio::test]
    async fn postgres_private_create_replay_and_cas_when_dsn_is_set() {
        // Scenario 16, private half, against the PostgreSQL store.
        let Some(client) = client().await else {
            return;
        };
        let store = PostgresAgentPrivateMemoryStore::from_shared_client(Arc::new(client));
        store.migrate().await.expect("migrate");
        let scope = agent_scope(&unique(), "support");
        let now = AgentTimestampMillis::new(1_000);

        let fact = memory(&scope, "fact", 10);
        let created = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("create");
        assert_eq!(created.revision, AgentRevisionNumber::INITIAL);

        // The replay answers the original result without a second write.
        let replayed = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("replayed create");
        assert_eq!(replayed, created);

        // A create over the existing memory under a new operation is refused.
        let duplicate = memory(&scope, "fact", 20);
        let refused = store
            .upsert(&scope, &duplicate, PrivateMemoryExpectation::Absent)
            .await
            .expect_err("the duplicate create is refused");
        assert_eq!(refused.code(), "memory-already-exists");

        // A compare-and-set update stamps the next revision; its replay
        // returns its own result even after later updates.
        let mut second = memory(&scope, "fact", 30);
        second.content = AgentTaskContent::inline(serde_json::json!({ "v": 2 })).expect("content");
        let updated = store
            .upsert(
                &scope,
                &second,
                PrivateMemoryExpectation::Revision(created.revision),
            )
            .await
            .expect("update");
        assert_eq!(updated.revision, created.revision.next());
        let replayed = store
            .upsert(
                &scope,
                &second,
                PrivateMemoryExpectation::Revision(created.revision),
            )
            .await
            .expect("replayed update");
        assert_eq!(replayed, updated);

        // A stale expectation is refused without overwriting.
        let stale = memory(&scope, "fact", 40);
        let refused = store
            .upsert(
                &scope,
                &stale,
                PrivateMemoryExpectation::Revision(created.revision),
            )
            .await
            .expect_err("the stale update is refused");
        assert_eq!(refused.code(), "memory-revision-conflict");
        let held = store
            .get(&scope, &fact.memory_id, now)
            .await
            .expect("get")
            .expect("the memory exists");
        assert_eq!(held, updated, "the stale writer wrote nothing");
    }

    #[tokio::test]
    async fn postgres_concurrent_cas_writers_admit_exactly_one_when_dsn_is_set() {
        // Scenario 15 against real SQL: two separate connections race the
        // same expected revision; the single-statement compare-and-set admits
        // exactly one.
        let Some(first) = client().await else {
            return;
        };
        let Some(second) = client().await else {
            return;
        };
        let left = PostgresAgentPrivateMemoryStore::from_shared_client(Arc::new(first));
        let right = PostgresAgentPrivateMemoryStore::from_shared_client(Arc::new(second));
        left.migrate().await.expect("migrate");
        let scope = agent_scope(&unique(), "support");

        let created = left
            .upsert(
                &scope,
                &memory(&scope, "fact", 10),
                PrivateMemoryExpectation::Absent,
            )
            .await
            .expect("create");
        let expectation = PrivateMemoryExpectation::Revision(created.revision);
        let left_write = memory(&scope, "fact", 20);
        let right_write = memory(&scope, "fact", 21);

        let (a, b) = tokio::join!(
            left.upsert(&scope, &left_write, expectation),
            right.upsert(&scope, &right_write, expectation),
        );
        let outcomes = [a, b];
        assert_eq!(
            outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
            1,
            "exactly one concurrent writer wins"
        );
        let refusal = outcomes
            .iter()
            .find_map(|outcome| outcome.as_ref().err())
            .expect("one writer is refused");
        assert_eq!(refusal.code(), "memory-revision-conflict");
    }

    #[tokio::test]
    async fn postgres_cross_scope_private_reads_reveal_nothing_when_dsn_is_set() {
        // Scenario 18, private half: cross-scope reads are byte-identical to
        // reading a memory that never existed.
        let Some(client) = client().await else {
            return;
        };
        let store = PostgresAgentPrivateMemoryStore::from_shared_client(Arc::new(client));
        store.migrate().await.expect("migrate");
        let tenant = unique();
        let owner = agent_scope(&tenant, "support");
        let sibling = agent_scope(&tenant, "billing");
        let foreign = agent_scope(&unique(), "support");
        let now = AgentTimestampMillis::new(1_000);

        let fact = memory(&owner, "fact", 10);
        store
            .upsert(&owner, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("create");

        for scope in [&sibling, &foreign] {
            assert!(store
                .get(scope, &fact.memory_id, now)
                .await
                .expect("get")
                .is_none());
            let page = store
                .list(scope, PrivateMemoryCursor::start(), now)
                .await
                .expect("list");
            assert!(page.memories.is_empty());
        }

        // The same memory id coexists independently in a sibling scope.
        let twin = memory(&sibling, "fact", 30);
        store
            .upsert(&sibling, &twin, PrivateMemoryExpectation::Absent)
            .await
            .expect("create the twin");
        let owner_view = store
            .get(&owner, &fact.memory_id, now)
            .await
            .expect("get")
            .expect("the owner's memory");
        assert_eq!(owner_view.created_at, AgentTimestampMillis::new(10));
    }

    #[tokio::test]
    async fn postgres_tombstone_and_delete_strip_content_and_replay_harmlessly_when_dsn_is_set() {
        let Some(client) = client().await else {
            return;
        };
        let store = PostgresAgentPrivateMemoryStore::from_shared_client(Arc::new(client));
        store.migrate().await.expect("migrate");
        let scope = agent_scope(&unique(), "support");
        let now = AgentTimestampMillis::new(1_000);

        let fact = memory(&scope, "fact", 10);
        let created = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect("create");

        // Tombstone: the stub keeps the digest, loses the content, and takes
        // the next revision; its replay is harmless; a second withdrawal and
        // any later update are refused.
        let request = PrivateMemoryTombstoneRequest {
            memory_id: created.memory_id.clone(),
            operation_id: MemoryOperationId::derive_for_agent(&scope, "tombstone-fact")
                .expect("op id"),
            reason: MemoryTombstoneReason::Retracted,
            tombstoned_at: AgentTimestampMillis::new(50),
        };
        let stub = store.tombstone(&scope, &request).await.expect("tombstone");
        assert!(stub.is_tombstoned());
        assert_eq!(stub.content, AgentPrivateMemory::tombstone_content());
        assert_eq!(stub.content_digest, created.content_digest);
        assert_eq!(stub.revision, created.revision.next());
        let replayed = store
            .tombstone(&scope, &request)
            .await
            .expect("replayed tombstone");
        assert_eq!(replayed, stub);
        let visible = store
            .get(&scope, &created.memory_id, now)
            .await
            .expect("get")
            .expect("the stub is visible in scope");
        assert!(visible.is_tombstoned());
        let page = store
            .list(&scope, PrivateMemoryCursor::start(), now)
            .await
            .expect("list");
        assert!(page.memories.is_empty(), "a list excludes tombstones");
        let audit = store
            .list(
                &scope,
                PrivateMemoryCursor::start().include_tombstoned(),
                now,
            )
            .await
            .expect("audit list");
        assert_eq!(audit.memories.len(), 1);

        // The tombstone erased the create's ledger payload: its replay fails
        // closed rather than resurrect the withdrawn content.
        let refused = store
            .upsert(&scope, &fact, PrivateMemoryExpectation::Absent)
            .await
            .expect_err("the erased create fails closed");
        assert_eq!(refused.code(), "memory-operation-erased");

        // Delete: the row is gone, the replay answers success, and reads
        // under this and any other scope are identical to absence.
        let delete = PrivateMemoryDeleteRequest {
            memory_id: created.memory_id.clone(),
            operation_id: MemoryOperationId::derive_for_agent(&scope, "delete-fact")
                .expect("op id"),
        };
        store.delete(&scope, &delete).await.expect("delete");
        store
            .delete(&scope, &delete)
            .await
            .expect("replayed delete");
        assert!(store
            .get(&scope, &created.memory_id, now)
            .await
            .expect("get")
            .is_none());
        let absent = PrivateMemoryDeleteRequest {
            memory_id: AgentPrivateMemoryId::new("mem-missing").expect("memory id"),
            operation_id: MemoryOperationId::derive_for_agent(&scope, "delete-missing")
                .expect("op id"),
        };
        let refused = store
            .delete(&scope, &absent)
            .await
            .expect_err("deleting an absent memory is refused");
        assert_eq!(refused.code(), "memory-not-found");
    }

    #[tokio::test]
    async fn postgres_purge_expired_is_bounded_when_dsn_is_set() {
        let Some(client) = client().await else {
            return;
        };
        let store = PostgresAgentPrivateMemoryStore::from_shared_client(Arc::new(client));
        store.migrate().await.expect("migrate");
        let scope = agent_scope(&unique(), "support");

        for slot in ["a", "b", "c"] {
            let expiring = memory(&scope, slot, 10).with_retention(MemoryRetention::ExpiresAt {
                at: AgentTimestampMillis::new(100),
            });
            store
                .upsert(&scope, &expiring, PrivateMemoryExpectation::Absent)
                .await
                .expect("create");
        }

        // Expiry hides the rows from the instant itself, sweep or no sweep.
        let before = store
            .list(
                &scope,
                PrivateMemoryCursor::start(),
                AgentTimestampMillis::new(99),
            )
            .await
            .expect("list");
        assert_eq!(before.memories.len(), 3);
        let after = store
            .list(
                &scope,
                PrivateMemoryCursor::start(),
                AgentTimestampMillis::new(100),
            )
            .await
            .expect("list");
        assert!(after.memories.is_empty());

        // The sweep honors its bound and converges to zero.
        let first = store
            .purge_expired(&scope, AgentTimestampMillis::new(100), 2)
            .await
            .expect("purge");
        assert_eq!(first, 2);
        let second = store
            .purge_expired(&scope, AgentTimestampMillis::new(100), 2)
            .await
            .expect("purge");
        assert_eq!(second, 1);
        let third = store
            .purge_expired(&scope, AgentTimestampMillis::new(100), 2)
            .await
            .expect("purge");
        assert_eq!(third, 0);
    }

    #[tokio::test]
    async fn postgres_session_purge_honors_legal_hold_when_dsn_is_set() {
        // Open decision 7: terminal-run session retention against real SQL.
        let Some(client) = client().await else {
            return;
        };
        let client = Arc::new(client);
        let session = PostgresSessionMemoryStore::from_shared_client(client.clone());
        let snapshots = PostgresContextSnapshotStore::from_shared_client(client);
        session.migrate().await.expect("migrate");
        let scope = run_scope(&unique(), "support", "run-1");
        session
            .append(&scope, &entry(&scope, "assistant", 1))
            .await
            .expect("append");
        let window = SessionWindowPolicy::recent_window();
        let reference = AgentContextSnapshotRef::for_turn(&scope, 1).expect("reference");
        let assembled = assemble_session_context(
            &session,
            &scope,
            &reference,
            1,
            &window,
            AgentRevisionNumber::INITIAL,
            AgentTimestampMillis::new(5),
        )
        .await
        .expect("assemble");
        snapshots.persist(&assembled).await.expect("persist");

        let terminal_at = AgentTimestampMillis::new(100);
        let held = SessionRetentionPolicy::bounded_default()
            .with_retain_for_millis(50)
            .with_legal_hold(true);
        let due = SessionRetentionPolicy::bounded_default().with_retain_for_millis(50);

        // Held and not-yet-due purges delete nothing, on both stores.
        assert_eq!(
            session
                .purge_run(&scope, &held, terminal_at, AgentTimestampMillis::new(1_000))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Held,
        );
        assert_eq!(
            snapshots
                .purge_run(&scope, &held, terminal_at, AgentTimestampMillis::new(1_000))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Held,
        );
        assert_eq!(
            session
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(149))
                .await
                .expect("purge"),
            SessionPurgeOutcome::NotYetDue,
        );
        let page = session
            .read(&scope, SessionMemoryCursor::start())
            .await
            .expect("read");
        assert_eq!(page.entries.len(), 1, "held rows survive");

        // A due purge deletes the run's rows on both stores; the replay
        // reports zero.
        assert_eq!(
            session
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(150))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Purged { entries: 1 },
        );
        assert_eq!(
            snapshots
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(150))
                .await
                .expect("purge"),
            SessionPurgeOutcome::Purged { entries: 1 },
        );
        assert_eq!(
            session
                .purge_run(&scope, &due, terminal_at, AgentTimestampMillis::new(151))
                .await
                .expect("replayed purge"),
            SessionPurgeOutcome::Purged { entries: 0 },
        );
        let page = session
            .read(&scope, SessionMemoryCursor::start())
            .await
            .expect("read");
        assert!(page.entries.is_empty());
        assert!(snapshots
            .load(&scope, &reference)
            .await
            .expect("load")
            .is_none());
    }
}
