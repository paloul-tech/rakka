#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! PostgreSQL durable state plugin.

use std::error::Error;
use std::sync::Arc;

mod shutdown;

use rakka_core::Subsystem;
use rakka_persistence::{
    DurableError, DurableResult, DurableState, DurableStateStore, EventJournal, EventMetadata,
    EventRecord, PersistenceEvent, PersistenceId, Revision, SequenceNr, SnapshotMetadata,
    SnapshotRecord, SnapshotSelection, SnapshotStore, StateCodec, StateRecord, StoreFuture,
    TaggedEvent,
};
use tokio_postgres::{types::ToSql, Client, Row};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-persistence-postgres";

/// Subsystem associated with the PostgreSQL durable state plugin.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::PersistencePostgres
}

/// Backend name for PostgreSQL durable state telemetry.
pub const BACKEND_NAME: &str = "postgres";

/// PostgreSQL advisory lock id held while applying this crate's migration.
///
/// `CREATE TABLE IF NOT EXISTS` is *not* atomic against a concurrent creation
/// of the same table: both migrators see the table missing, both proceed, and
/// the loser fails against the system catalogs with a `pg_type` unique
/// violation rather than the no-op the `IF NOT EXISTS` reads like. Two nodes
/// starting at once against a fresh database is the ordinary case that hits it,
/// so the migration takes a session advisory lock first — the same guard
/// `rakka-a2a`, `rakka-sharding-postgres`, and `rakka-agent-postgres` already
/// apply to theirs.
///
/// The id is this crate's own, distinct from every sibling's, so the
/// subsystems' migrations do not serialize against each other in a shared
/// database.
pub const MIGRATION_LOCK_ID: i64 = 982_451_707;

/// Default durable state table name.
pub const TABLE_NAME: &str = "rakka_durable_state";

/// Default event journal table name.
pub const EVENT_JOURNAL_TABLE_NAME: &str = "rakka_events";

/// Default snapshot table name.
pub const SNAPSHOT_TABLE_NAME: &str = "rakka_snapshots";

/// SQL migration for the default durable state, journal, and snapshot tables.
pub const MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_durable_state (
    persistence_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    state BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rakka_events (
    persistence_id TEXT NOT NULL,
    sequence_nr BIGINT NOT NULL CHECK (sequence_nr > 0),
    event BYTEA NOT NULL,
    tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    slice INTEGER NULL,
    timestamp_millis BIGINT NOT NULL CHECK (timestamp_millis >= 0),
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (persistence_id, sequence_nr)
);

CREATE INDEX IF NOT EXISTS rakka_events_tag_idx
    ON rakka_events USING GIN (tags);

CREATE TABLE IF NOT EXISTS rakka_snapshots (
    persistence_id TEXT NOT NULL,
    sequence_nr BIGINT NOT NULL CHECK (sequence_nr >= 0),
    snapshot BYTEA NOT NULL,
    timestamp_millis BIGINT NOT NULL CHECK (timestamp_millis >= 0),
    PRIMARY KEY (persistence_id, sequence_nr)
);
"#;

/// PostgreSQL durable state store using a pluggable state codec.
pub struct PostgresDurableStateStore<C> {
    client: Arc<Client>,
    codec: C,
}

impl<C> PostgresDurableStateStore<C>
where
    C: Clone,
{
    /// Creates a PostgreSQL durable state store.
    #[must_use]
    pub fn new(client: Client, codec: C) -> Self {
        Self {
            client: Arc::new(client),
            codec,
        }
    }

    /// Creates a store that shares an already-`Arc`-wrapped client.
    ///
    /// Use this to back several typed stores with a single PostgreSQL
    /// connection instead of opening one connection per store.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>, codec: C) -> Self {
        Self { client, codec }
    }

    /// Applies the default table migration, safe to run concurrently
    /// ([`MIGRATION_LOCK_ID`]).
    pub async fn migrate(&self) -> DurableResult<()> {
        apply_migration_under_lock(&self.client).await
    }
}

impl<C> Clone for PostgresDurableStateStore<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<S, C> DurableStateStore<S> for PostgresDurableStateStore<C>
where
    S: DurableState,
    C: StateCodec<S>,
{
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        let client = self.client.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let row = client
                .query_opt(
                    "SELECT revision, state FROM rakka_durable_state WHERE persistence_id = $1",
                    &[&persistence_id.as_str()],
                )
                .await
                .map_err(map_postgres_error)?;

            row.map(|row| decode_row(&codec, row)).transpose()
        })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        let client = self.client.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let encoded = codec.encode(&state)?;
            let encoded = encoded.as_slice();
            let next_revision = expected_revision.next();
            let expected = revision_to_i64(expected_revision)?;
            let next = revision_to_i64(next_revision)?;
            let insert_params: &[&(dyn ToSql + Sync)] =
                &[&persistence_id.as_str(), &next, &encoded];
            let update_params: &[&(dyn ToSql + Sync)] =
                &[&persistence_id.as_str(), &next, &encoded, &expected];
            let row = if expected_revision == Revision::INITIAL {
                client
                    .query_opt(
                        r#"
INSERT INTO rakka_durable_state (persistence_id, revision, state)
VALUES ($1, $2::bigint, $3::bytea)
ON CONFLICT (persistence_id) DO NOTHING
RETURNING revision, state
"#,
                        insert_params,
                    )
                    .await
            } else {
                client
                    .query_opt(
                        r#"
UPDATE rakka_durable_state
SET revision = $2::bigint,
    state = $3::bytea,
    updated_at = now()
WHERE persistence_id = $1
  AND revision = $4::bigint
RETURNING revision, state
"#,
                        update_params,
                    )
                    .await
            }
            .map_err(map_postgres_error)?;

            match row {
                Some(row) => decode_row(&codec, row),
                None => {
                    let actual = load_actual_revision(&client, persistence_id).await?;
                    Err(DurableError::revision_conflict(
                        persistence_id.clone(),
                        expected_revision,
                        actual,
                    ))
                }
            }
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        let client = self.client.clone();
        Box::pin(async move {
            let expected = revision_to_i64(expected_revision)?;
            let deleted = client
                .execute(
                    "DELETE FROM rakka_durable_state WHERE persistence_id = $1 AND revision = $2",
                    &[&persistence_id.as_str(), &expected],
                )
                .await
                .map_err(map_postgres_error)?;

            if deleted == 1 {
                return Ok(Revision::INITIAL);
            }

            let actual = load_actual_revision(&client, persistence_id).await?;
            if expected_revision == Revision::INITIAL && actual == Revision::INITIAL {
                Ok(Revision::INITIAL)
            } else {
                Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ))
            }
        })
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        let client = self.client.clone();
        Box::pin(async move {
            let rows = client
                .query(
                    "SELECT persistence_id FROM rakka_durable_state ORDER BY persistence_id",
                    &[],
                )
                .await
                .map_err(map_postgres_error)?;
            Ok(rows
                .into_iter()
                .map(|row| PersistenceId::new(row.get::<_, String>("persistence_id")))
                .collect())
        })
    }
}

/// PostgreSQL event journal using a pluggable event codec.
pub struct PostgresEventJournal<C> {
    client: Arc<Client>,
    codec: C,
}

impl<C> PostgresEventJournal<C>
where
    C: Clone,
{
    /// Creates a PostgreSQL event journal.
    #[must_use]
    pub fn new(client: Client, codec: C) -> Self {
        Self {
            client: Arc::new(client),
            codec,
        }
    }

    /// Applies the default table migration, safe to run concurrently
    /// ([`MIGRATION_LOCK_ID`]).
    pub async fn migrate(&self) -> DurableResult<()> {
        apply_migration_under_lock(&self.client).await
    }
}

impl<C> Clone for PostgresEventJournal<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<E, C> EventJournal<E> for PostgresEventJournal<C>
where
    E: PersistenceEvent,
    C: StateCodec<E>,
{
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn append<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_sequence_nr: SequenceNr,
        events: Vec<TaggedEvent<E>>,
    ) -> StoreFuture<'a, Vec<EventRecord<E>>> {
        let client = self.client.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let actual = load_highest_sequence_nr(&client, persistence_id).await?;
            if actual != expected_sequence_nr {
                return Err(DurableError::sequence_conflict(
                    persistence_id.clone(),
                    expected_sequence_nr,
                    actual,
                ));
            }

            let mut sequence_nr = expected_sequence_nr;
            let mut appended = Vec::with_capacity(events.len());
            for tagged in events {
                sequence_nr = sequence_nr.next();
                let encoded = codec.encode(&tagged.event)?;
                let timestamp_millis = current_timestamp_millis_i64()?;
                let sequence = sequence_to_i64(sequence_nr)?;
                let tags = tagged.tags;
                let slice: Option<i32> = None;
                client
                    .execute(
                        r#"
INSERT INTO rakka_events
    (persistence_id, sequence_nr, event, tags, slice, timestamp_millis)
VALUES ($1, $2::bigint, $3::bytea, $4::text[], $5::integer, $6::bigint)
"#,
                        &[
                            &persistence_id.as_str(),
                            &sequence,
                            &encoded.as_slice(),
                            &tags,
                            &slice,
                            &timestamp_millis,
                        ],
                    )
                    .await
                    .map_err(map_postgres_error)?;
                let metadata = EventMetadata {
                    persistence_id: persistence_id.clone(),
                    sequence_nr,
                    timestamp_millis: u64::try_from(timestamp_millis).map_err(|_negative| {
                        DurableError::store(BACKEND_NAME, "event timestamp was negative")
                    })?,
                    tags,
                    slice: None,
                };
                appended.push(EventRecord::new(tagged.event, metadata));
            }
            Ok(appended)
        })
    }

    fn replay<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        from: SequenceNr,
        to: SequenceNr,
    ) -> StoreFuture<'a, Vec<EventRecord<E>>> {
        let client = self.client.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let from = sequence_to_i64(from)?;
            let to = sequence_bound_to_i64(to);
            let rows = client
                .query(
                    r#"
SELECT sequence_nr, event, tags, slice, timestamp_millis
FROM rakka_events
WHERE persistence_id = $1
  AND sequence_nr >= $2::bigint
  AND sequence_nr <= $3::bigint
  AND deleted = FALSE
ORDER BY sequence_nr ASC
"#,
                    &[&persistence_id.as_str(), &from, &to],
                )
                .await
                .map_err(map_postgres_error)?;
            rows.into_iter()
                .map(|row| decode_event_row(&codec, persistence_id.clone(), row))
                .collect()
        })
    }

    fn delete_to<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        to: SequenceNr,
    ) -> StoreFuture<'a, ()> {
        let client = self.client.clone();
        Box::pin(async move {
            let to = sequence_to_i64(to)?;
            client
                .execute(
                    r#"
UPDATE rakka_events
SET deleted = TRUE
WHERE persistence_id = $1
  AND sequence_nr <= $2::bigint
"#,
                    &[&persistence_id.as_str(), &to],
                )
                .await
                .map_err(map_postgres_error)?;
            Ok(())
        })
    }

    fn highest_sequence_nr<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, SequenceNr> {
        let client = self.client.clone();
        Box::pin(async move { load_highest_sequence_nr(&client, persistence_id).await })
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        let client = self.client.clone();
        Box::pin(async move {
            let rows = client
                .query(
                    "SELECT DISTINCT persistence_id FROM rakka_events ORDER BY persistence_id",
                    &[],
                )
                .await
                .map_err(map_postgres_error)?;
            Ok(rows
                .into_iter()
                .map(|row| PersistenceId::new(row.get::<_, String>("persistence_id")))
                .collect())
        })
    }

    fn events_by_tag<'a>(&'a self, tag: &'a str) -> StoreFuture<'a, Vec<EventRecord<E>>> {
        let client = self.client.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let rows = client
                .query(
                    r#"
SELECT persistence_id, sequence_nr, event, tags, slice, timestamp_millis
FROM rakka_events
WHERE $1 = ANY(tags)
  AND deleted = FALSE
ORDER BY timestamp_millis ASC, persistence_id ASC, sequence_nr ASC
"#,
                    &[&tag],
                )
                .await
                .map_err(map_postgres_error)?;
            rows.into_iter()
                .map(|row| {
                    let persistence_id = PersistenceId::new(row.get::<_, String>("persistence_id"));
                    decode_event_row(&codec, persistence_id, row)
                })
                .collect()
        })
    }
}

/// PostgreSQL snapshot store using a pluggable snapshot codec.
pub struct PostgresSnapshotStore<C> {
    client: Arc<Client>,
    codec: C,
}

impl<C> PostgresSnapshotStore<C>
where
    C: Clone,
{
    /// Creates a PostgreSQL snapshot store.
    #[must_use]
    pub fn new(client: Client, codec: C) -> Self {
        Self {
            client: Arc::new(client),
            codec,
        }
    }

    /// Applies the default table migration, safe to run concurrently
    /// ([`MIGRATION_LOCK_ID`]).
    pub async fn migrate(&self) -> DurableResult<()> {
        apply_migration_under_lock(&self.client).await
    }
}

impl<C> Clone for PostgresSnapshotStore<C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            codec: self.codec.clone(),
        }
    }
}

impl<S, C> SnapshotStore<S> for PostgresSnapshotStore<C>
where
    S: DurableState,
    C: StateCodec<S>,
{
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn save<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        sequence_nr: SequenceNr,
        snapshot: S,
    ) -> StoreFuture<'a, SnapshotRecord<S>> {
        let client = self.client.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let encoded = codec.encode(&snapshot)?;
            let sequence = sequence_to_i64(sequence_nr)?;
            let timestamp_millis = current_timestamp_millis_i64()?;
            client
                .execute(
                    r#"
INSERT INTO rakka_snapshots
    (persistence_id, sequence_nr, snapshot, timestamp_millis)
VALUES ($1, $2::bigint, $3::bytea, $4::bigint)
ON CONFLICT (persistence_id, sequence_nr)
DO UPDATE SET snapshot = EXCLUDED.snapshot,
              timestamp_millis = EXCLUDED.timestamp_millis
"#,
                    &[
                        &persistence_id.as_str(),
                        &sequence,
                        &encoded.as_slice(),
                        &timestamp_millis,
                    ],
                )
                .await
                .map_err(map_postgres_error)?;
            Ok(SnapshotRecord::new(
                snapshot,
                SnapshotMetadata::new(
                    persistence_id.clone(),
                    sequence_nr,
                    u64::try_from(timestamp_millis).map_err(|_negative| {
                        DurableError::store(BACKEND_NAME, "snapshot timestamp was negative")
                    })?,
                ),
            ))
        })
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, Option<SnapshotRecord<S>>> {
        let client = self.client.clone();
        let codec = self.codec.clone();
        Box::pin(async move {
            let row = query_snapshot_rows(&client, persistence_id, selection, Some(1))
                .await?
                .into_iter()
                .next();
            row.map(|row| decode_snapshot_row(&codec, persistence_id.clone(), row))
                .transpose()
        })
    }

    fn list<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, Vec<SnapshotMetadata>> {
        let client = self.client.clone();
        Box::pin(async move {
            let rows = query_snapshot_rows(&client, persistence_id, selection, None).await?;
            rows.into_iter()
                .map(|row| decode_snapshot_metadata(persistence_id.clone(), row))
                .collect()
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, usize> {
        let client = self.client.clone();
        Box::pin(async move {
            let min_sequence = sequence_bound_to_i64(selection.min_sequence_nr);
            let max_sequence = sequence_bound_to_i64(selection.max_sequence_nr);
            let min_timestamp = timestamp_bound_to_i64(selection.min_timestamp_millis);
            let max_timestamp = timestamp_bound_to_i64(selection.max_timestamp_millis);
            let removed = client
                .execute(
                    r#"
DELETE FROM rakka_snapshots
WHERE persistence_id = $1
  AND sequence_nr >= $2::bigint
  AND sequence_nr <= $3::bigint
  AND timestamp_millis >= $4::bigint
  AND timestamp_millis <= $5::bigint
"#,
                    &[
                        &persistence_id.as_str(),
                        &min_sequence,
                        &max_sequence,
                        &min_timestamp,
                        &max_timestamp,
                    ],
                )
                .await
                .map_err(map_postgres_error)?;
            usize::try_from(removed)
                .map_err(|_overflow| DurableError::store(BACKEND_NAME, "delete count overflow"))
        })
    }
}

/// Identity codec for byte-vector state.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesStateCodec;

impl StateCodec<Vec<u8>> for BytesStateCodec {
    fn encode(&self, state: &Vec<u8>) -> DurableResult<Vec<u8>> {
        Ok(state.clone())
    }

    fn decode(&self, bytes: &[u8]) -> DurableResult<Vec<u8>> {
        Ok(bytes.to_vec())
    }
}

fn decode_row<S, C>(codec: &C, row: Row) -> DurableResult<StateRecord<S>>
where
    S: DurableState,
    C: StateCodec<S>,
{
    let revision: i64 = row.get("revision");
    let state: Vec<u8> = row.get("state");
    Ok(StateRecord::new(
        codec.decode(&state)?,
        revision_from_i64(revision)?,
    ))
}

async fn load_actual_revision(
    client: &Client,
    persistence_id: &PersistenceId,
) -> DurableResult<Revision> {
    let revision = client
        .query_opt(
            "SELECT revision FROM rakka_durable_state WHERE persistence_id = $1",
            &[&persistence_id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?
        .map_or(Ok(Revision::INITIAL), |row| {
            let revision: i64 = row.get("revision");
            revision_from_i64(revision)
        })?;

    Ok(revision)
}

fn revision_to_i64(revision: Revision) -> DurableResult<i64> {
    i64::try_from(revision.get())
        .map_err(|_overflow| DurableError::store(BACKEND_NAME, "revision exceeds i64 range"))
}

fn revision_from_i64(revision: i64) -> DurableResult<Revision> {
    u64::try_from(revision)
        .map(Revision::new)
        .map_err(|_negative| DurableError::store(BACKEND_NAME, "stored revision was negative"))
}

fn sequence_to_i64(sequence_nr: SequenceNr) -> DurableResult<i64> {
    i64::try_from(sequence_nr.get())
        .map_err(|_overflow| DurableError::store(BACKEND_NAME, "sequence number exceeds i64 range"))
}

fn sequence_bound_to_i64(sequence_nr: SequenceNr) -> i64 {
    i64::try_from(sequence_nr.get()).unwrap_or(i64::MAX)
}

fn sequence_from_i64(sequence_nr: i64) -> DurableResult<SequenceNr> {
    u64::try_from(sequence_nr)
        .map(SequenceNr::new)
        .map_err(|_negative| {
            DurableError::store(BACKEND_NAME, "stored sequence number was negative")
        })
}

fn timestamp_bound_to_i64(timestamp_millis: u64) -> i64 {
    i64::try_from(timestamp_millis).unwrap_or(i64::MAX)
}

fn current_timestamp_millis_i64() -> DurableResult<i64> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis)
        .map_err(|_overflow| DurableError::store(BACKEND_NAME, "timestamp exceeds i64 range"))
}

async fn load_highest_sequence_nr(
    client: &Client,
    persistence_id: &PersistenceId,
) -> DurableResult<SequenceNr> {
    let sequence_nr = client
        .query_opt(
            "SELECT MAX(sequence_nr) AS sequence_nr FROM rakka_events WHERE persistence_id = $1",
            &[&persistence_id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?
        .and_then(|row| row.get::<_, Option<i64>>("sequence_nr"))
        .map_or(Ok(SequenceNr::INITIAL), sequence_from_i64)?;
    Ok(sequence_nr)
}

fn decode_event_row<E, C>(
    codec: &C,
    persistence_id: PersistenceId,
    row: Row,
) -> DurableResult<EventRecord<E>>
where
    E: PersistenceEvent,
    C: StateCodec<E>,
{
    let sequence_nr = sequence_from_i64(row.get("sequence_nr"))?;
    let event: Vec<u8> = row.get("event");
    let timestamp_millis: i64 = row.get("timestamp_millis");
    let tags: Vec<String> = row.get("tags");
    let slice: Option<i32> = row.get("slice");
    let slice = slice
        .map(u32::try_from)
        .transpose()
        .map_err(|_negative| DurableError::store(BACKEND_NAME, "stored slice was negative"))?;
    let timestamp_millis = u64::try_from(timestamp_millis)
        .map_err(|_negative| DurableError::store(BACKEND_NAME, "event timestamp was negative"))?;
    Ok(EventRecord::new(
        codec.decode(&event)?,
        EventMetadata {
            persistence_id,
            sequence_nr,
            timestamp_millis,
            tags,
            slice,
        },
    ))
}

async fn query_snapshot_rows(
    client: &Client,
    persistence_id: &PersistenceId,
    selection: SnapshotSelection,
    limit: Option<i64>,
) -> DurableResult<Vec<Row>> {
    let min_sequence = sequence_bound_to_i64(selection.min_sequence_nr);
    let max_sequence = sequence_bound_to_i64(selection.max_sequence_nr);
    let min_timestamp = timestamp_bound_to_i64(selection.min_timestamp_millis);
    let max_timestamp = timestamp_bound_to_i64(selection.max_timestamp_millis);
    let rows = if let Some(limit) = limit {
        client
            .query(
                r#"
SELECT sequence_nr, snapshot, timestamp_millis
FROM rakka_snapshots
WHERE persistence_id = $1
  AND sequence_nr >= $2::bigint
  AND sequence_nr <= $3::bigint
  AND timestamp_millis >= $4::bigint
  AND timestamp_millis <= $5::bigint
ORDER BY sequence_nr DESC, timestamp_millis DESC
LIMIT $6::bigint
"#,
                &[
                    &persistence_id.as_str(),
                    &min_sequence,
                    &max_sequence,
                    &min_timestamp,
                    &max_timestamp,
                    &limit,
                ],
            )
            .await
    } else {
        client
            .query(
                r#"
SELECT sequence_nr, snapshot, timestamp_millis
FROM rakka_snapshots
WHERE persistence_id = $1
  AND sequence_nr >= $2::bigint
  AND sequence_nr <= $3::bigint
  AND timestamp_millis >= $4::bigint
  AND timestamp_millis <= $5::bigint
ORDER BY sequence_nr DESC, timestamp_millis DESC
"#,
                &[
                    &persistence_id.as_str(),
                    &min_sequence,
                    &max_sequence,
                    &min_timestamp,
                    &max_timestamp,
                ],
            )
            .await
    }
    .map_err(map_postgres_error)?;
    Ok(rows)
}

fn decode_snapshot_metadata(
    persistence_id: PersistenceId,
    row: Row,
) -> DurableResult<SnapshotMetadata> {
    let sequence_nr = sequence_from_i64(row.get("sequence_nr"))?;
    let timestamp_millis: i64 = row.get("timestamp_millis");
    Ok(SnapshotMetadata::new(
        persistence_id,
        sequence_nr,
        u64::try_from(timestamp_millis).map_err(|_negative| {
            DurableError::store(BACKEND_NAME, "snapshot timestamp was negative")
        })?,
    ))
}

fn decode_snapshot_row<S, C>(
    codec: &C,
    persistence_id: PersistenceId,
    row: Row,
) -> DurableResult<SnapshotRecord<S>>
where
    S: DurableState,
    C: StateCodec<S>,
{
    let snapshot: Vec<u8> = row.get("snapshot");
    let metadata = decode_snapshot_metadata(persistence_id, row)?;
    Ok(SnapshotRecord::new(codec.decode(&snapshot)?, metadata))
}

/// Applies the idempotent schema under the crate's advisory lock, so
/// concurrent migrators do not race PostgreSQL's system catalogs
/// ([`MIGRATION_LOCK_ID`]).
///
/// The lock is released whether or not the batch applied, and the batch's error
/// is reported ahead of the unlock's: a failed migration is the more useful
/// diagnosis, and the lock is session-scoped, so a lost connection releases it
/// regardless. The batch is one implicit transaction, so a failure applies
/// nothing.
async fn apply_migration_under_lock(client: &Client) -> DurableResult<()> {
    client
        .execute("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
        .await
        .map_err(map_postgres_error)?;
    let applied = client.batch_execute(MIGRATION_SQL).await;
    let unlocked = client
        .execute("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_ID])
        .await;
    applied.map_err(map_postgres_error)?;
    unlocked.map_err(map_postgres_error)?;
    Ok(())
}

fn map_postgres_error(error: tokio_postgres::Error) -> DurableError {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    DurableError::store(BACKEND_NAME, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_core::{
        CoordinatedShutdown, CoordinatedShutdownReason, ShutdownPhase, ShutdownTaskStatus,
    };
    use rakka_persistence::register_persistence_shutdown_task;
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn postgres_concurrent_migrators_do_not_race_when_dsn_is_set() {
        // Two nodes starting at once against a fresh database both run the
        // migration, and `CREATE TABLE IF NOT EXISTS` is not atomic against a
        // concurrent creation: without the advisory lock the loser fails with a
        // `pg_type` unique violation instead of the no-op it reads like.
        //
        // The race only exists while the tables are absent, so this runs in a
        // private schema rather than dropping the shared ones out from under
        // the tests running beside it: an empty `search_path` schema is a fresh
        // namespace for the same unqualified DDL.
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let schema = format!(
            "rakka_migration_race_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let (setup, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres connection error: {error}");
            }
        });
        setup
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("the private schema is created");

        // Each migrator needs its own session: an advisory lock is re-entrant
        // within one session, so racing on a shared connection would prove
        // nothing.
        let mut migrators = Vec::new();
        for _ in 0..4u32 {
            let dsn = dsn.clone();
            let schema = schema.clone();
            migrators.push(tokio::spawn(async move {
                let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        eprintln!("postgres connection error: {error}");
                    }
                });
                client
                    .batch_execute(&format!("SET search_path TO {schema}"))
                    .await
                    .expect("the migrator targets the private schema");
                PostgresDurableStateStore::new(client, BytesStateCodec)
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

        let cleanup = setup
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await;
        assert!(
            failures.is_empty(),
            "concurrent migrators raced the system catalogs: {failures:?}"
        );
        cleanup.expect("the private schema is dropped");
    }

    #[tokio::test]
    async fn postgres_round_trip_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres connection error: {error}");
            }
        });

        let store = PostgresDurableStateStore::new(client, BytesStateCodec);
        store.migrate().await.unwrap();
        let id = PersistenceId::new(format!(
            "postgres-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let first = store
            .compare_and_set(&id, Revision::INITIAL, b"one".to_vec())
            .await
            .unwrap();
        assert_eq!(first.revision, Revision::new(1));
        assert_eq!(first.state, b"one".to_vec());

        let loaded = store.load(&id).await.unwrap().unwrap();
        assert_eq!(loaded, first);

        let second = store
            .compare_and_set(&id, first.revision, b"two".to_vec())
            .await
            .unwrap();
        assert_eq!(second.revision, Revision::new(2));

        let conflict = store
            .compare_and_set(&id, first.revision, b"stale".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(conflict, DurableError::RevisionConflict { .. }));

        store.delete(&id, second.revision).await.unwrap();
        assert_eq!(store.load(&id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn postgres_journal_and_snapshots_round_trip_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };

        let (journal_client, journal_connection) =
            tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = journal_connection.await {
                eprintln!("postgres journal connection error: {error}");
            }
        });
        let journal = PostgresEventJournal::new(journal_client, BytesStateCodec);
        journal.migrate().await.unwrap();
        let journal_id = unique_id("postgres-journal");

        let appended = journal
            .append(
                &journal_id,
                SequenceNr::INITIAL,
                vec![TaggedEvent::with_tags(b"one".to_vec(), ["tag-a"])],
            )
            .await
            .unwrap();
        assert_eq!(appended[0].metadata.sequence_nr, SequenceNr::FIRST);
        assert_eq!(
            journal
                .replay(&journal_id, SequenceNr::FIRST, SequenceNr::MAX)
                .await
                .unwrap()[0]
                .event,
            b"one".to_vec()
        );
        assert_eq!(journal.events_by_tag("tag-a").await.unwrap().len(), 1);
        journal
            .delete_to(&journal_id, SequenceNr::FIRST)
            .await
            .unwrap();
        assert!(journal
            .replay(&journal_id, SequenceNr::FIRST, SequenceNr::MAX)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            journal.highest_sequence_nr(&journal_id).await.unwrap(),
            SequenceNr::FIRST
        );

        let (snapshot_client, snapshot_connection) =
            tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = snapshot_connection.await {
                eprintln!("postgres snapshot connection error: {error}");
            }
        });
        let snapshots = PostgresSnapshotStore::new(snapshot_client, BytesStateCodec);
        snapshots.migrate().await.unwrap();
        let snapshot_id = unique_id("postgres-snapshot");

        snapshots
            .save(&snapshot_id, SequenceNr::new(2), b"snapshot".to_vec())
            .await
            .unwrap();
        let loaded = snapshots
            .load(&snapshot_id, SnapshotSelection::latest())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.snapshot, b"snapshot".to_vec());
        assert_eq!(loaded.metadata.sequence_nr, SequenceNr::new(2));
        assert_eq!(
            snapshots
                .delete(&snapshot_id, SnapshotSelection::latest())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn postgres_shutdown_task_checks_backend_readiness_when_dsn_is_set() {
        let dsn = match std::env::var("RAKKA_POSTGRES_TEST_DSN") {
            Ok(dsn) => dsn,
            Err(_) => return,
        };
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("postgres shutdown connection error: {error}");
            }
        });

        let shutdown = CoordinatedShutdown::new();
        let store = PostgresDurableStateStore::new(client, BytesStateCodec);
        let task = register_persistence_shutdown_task(&shutdown, "postgres-readiness", store)
            .expect("postgres persistence shutdown task should register");

        assert_eq!(task.phase(), &ShutdownPhase::flush_persistence());
        assert!(task.options().attributes().iter().any(|attribute| {
            attribute.key() == "operation" && attribute.value() == "postgres-readiness-check"
        }));
        assert!(task
            .options()
            .attributes()
            .iter()
            .any(|attribute| attribute.key() == "backend" && attribute.value() == BACKEND_NAME));

        let report = shutdown
            .run(CoordinatedShutdownReason::user_request())
            .await
            .expect("postgres readiness shutdown should complete");
        let status = report
            .phases()
            .iter()
            .find(|phase| phase.phase() == &ShutdownPhase::flush_persistence())
            .and_then(|phase| {
                phase
                    .tasks()
                    .iter()
                    .find(|task| task.task_name() == "postgres-readiness")
            })
            .map(|task| task.status());

        assert_eq!(status, Some(ShutdownTaskStatus::Completed));
    }

    fn unique_id(prefix: &str) -> PersistenceId {
        PersistenceId::new(format!(
            "{prefix}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
}
