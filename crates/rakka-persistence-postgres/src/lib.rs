#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! PostgreSQL durable state plugin.

use std::error::Error;
use std::sync::Arc;

use rakka_core::Subsystem;
use rakka_persistence::{
    DurableError, DurableResult, DurableState, DurableStateStore, PersistenceId, Revision,
    StateCodec, StateRecord, StoreFuture,
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

/// Default durable state table name.
pub const TABLE_NAME: &str = "rakka_durable_state";

/// SQL migration for the default durable state table.
pub const MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_durable_state (
    persistence_id TEXT PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    state BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
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

    /// Applies the default table migration.
    pub async fn migrate(&self) -> DurableResult<()> {
        self.client
            .batch_execute(MIGRATION_SQL)
            .await
            .map_err(map_postgres_error)?;
        Ok(())
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
    use tokio_postgres::NoTls;

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
}
