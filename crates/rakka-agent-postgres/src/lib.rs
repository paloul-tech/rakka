#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! PostgreSQL adapters for agent short-term session memory and immutable context
//! snapshots.
//!
//! This crate is the PostgreSQL binding for the agent-domain memory contracts of
//! [`rakka_agent`]: [`SessionMemoryStore`] scoped `(TenantId, AgentId,
//! AgentRunId)` and [`ContextSnapshotStore`] scoped by an immutable snapshot
//! reference ([specification 13.2, 13.5, and the short-term clauses of
//! 13.6](../../../docs/plans/rakka-agent/spec.md)). It never changes the
//! agent-domain identity, provenance, idempotency, or snapshot-reuse semantics —
//! it only persists them.
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

use std::sync::Arc;

use rakka_agent::{
    check_memory_schema, AgentContextSnapshotRef, AgentRunScope, AgentSchemaPolicy,
    ContextSnapshotStore, MemoryContextSnapshot, MemoryError, MemoryFuture, MemorySequence,
    SessionMemoryCursor, SessionMemoryEntry, SessionMemoryPage, SessionMemoryStore,
};
use tokio_postgres::error::SqlState;
use tokio_postgres::Client;

/// Stable backend name, reported in telemetry.
pub const BACKEND_NAME: &str = "postgres";

/// The session-memory table.
pub const SESSION_TABLE_NAME: &str = "rakka_agent_session_memory";

/// The context-snapshot table.
pub const SNAPSHOT_TABLE_NAME: &str = "rakka_agent_context_snapshot";

/// Advisory-lock id serializing concurrent migrations of this crate's schema.
///
/// A bare `CREATE TABLE IF NOT EXISTS` can race two migrators against
/// PostgreSQL's system catalogs; taking a session advisory lock first makes the
/// migration safe to run concurrently, as `rakka-a2a` does for its projection.
pub const MIGRATION_LOCK_ID: i64 = 982_451_653;

/// Idempotent schema for the session-memory and context-snapshot tables.
///
/// The session table's `(tenant, agent, run, sequence)` primary key orders a
/// run's session and rejects two operations claiming one sequence; its
/// `(tenant, agent, run, operation_id)` unique index makes an append replay
/// harmless. The snapshot table's primary key makes a snapshot immutable and its
/// re-persist a no-op.
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
}

/// Applies the idempotent schema under an advisory lock, so concurrent
/// migrators do not race the system catalogs.
async fn apply_migration(client: &Client) -> Result<(), MemoryError> {
    client
        .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
        .await
        .map_err(map_error)?;
    let applied = client.batch_execute(MIGRATION_SQL).await;
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
}
