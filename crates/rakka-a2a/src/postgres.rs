//! PostgreSQL A2A read-model storage: task projections and task events.
//!
//! The store follows the workspace's PostgreSQL plugin conventions:
//! idempotent additive DDL embedded as a constant (offline-packageable, no
//! migration files), applied under a Postgres advisory lock so many pods can
//! run [`PostgresA2ATaskProjectionStore::migrate`] at startup concurrently.
//! Schema evolution stays additive-only within a release: during a rolling
//! update old pods must tolerate new columns and new pods must tolerate
//! new-optional columns being absent (N/N+1 downgrade safety).
//!
//! Per the crate's multi-tenant read-scoping rule, this store is **always
//! tenant-scoped**: every query carries a `(tenant, ...)` predicate and
//! unscoped (`tenant = None`) reads are refused with
//! [`TaskProjectionError::TenantRequired`]. A tenant mismatch is
//! indistinguishable from a missing task.

use std::sync::Arc;

use a2a::{ListTasksRequest, ListTasksResponse, TaskState};
use async_trait::async_trait;
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::Value;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, Row};

use crate::projection::{A2ATaskEventRetention, A2ATaskProjectionStore};
use crate::task::{
    adopted_snapshot, page_offset, page_size, parse_replay_cursor, A2ATaskEvent,
    A2ATaskEventPayload, A2ATaskProjection, TaskProjectionError, TaskProjectionResult,
};

/// Backend name for PostgreSQL A2A store telemetry.
pub const BACKEND_NAME: &str = "postgres";

/// PostgreSQL advisory lock id held while applying A2A migrations.
///
/// Deliberately distinct from the `rakka-sharding-postgres` coordinator
/// migration lock (`982_451_653`) so the two subsystems never serialize on
/// each other's schema application.
pub const MIGRATION_LOCK_ID: i64 = 982_451_777;

/// Idempotent DDL for the A2A read model.
///
/// Applied via [`PostgresA2ATaskProjectionStore::migrate`] under
/// [`MIGRATION_LOCK_ID`]. Changes must stay additive within a release
/// (new tables, new nullable/defaulted columns, new indexes) so N and N+1
/// pods can share the schema during rolling updates.
pub const MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_a2a_tasks (
    tenant TEXT NOT NULL,
    task_id TEXT NOT NULL,
    context_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    workflow_type TEXT NOT NULL DEFAULT '',
    definition_version TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    status_timestamp_millis BIGINT NOT NULL CHECK (status_timestamp_millis >= 0),
    projection_revision BIGINT NOT NULL CHECK (projection_revision >= 0),
    history JSONB NOT NULL DEFAULT '[]'::jsonb,
    artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, task_id)
);

CREATE INDEX IF NOT EXISTS rakka_a2a_tasks_status_idx
    ON rakka_a2a_tasks (tenant, status, status_timestamp_millis);

CREATE INDEX IF NOT EXISTS rakka_a2a_tasks_context_idx
    ON rakka_a2a_tasks (tenant, context_id, updated_at);

CREATE INDEX IF NOT EXISTS rakka_a2a_tasks_workflow_idx
    ON rakka_a2a_tasks (tenant, workflow_id, updated_at);

CREATE TABLE IF NOT EXISTS rakka_a2a_task_events (
    tenant TEXT NOT NULL,
    task_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_kind TEXT NOT NULL,
    occurred_at_millis BIGINT NOT NULL CHECK (occurred_at_millis >= 0),
    projected_state TEXT NOT NULL,
    redaction TEXT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (tenant, task_id, sequence)
);
"#;

/// Bound on optimistic append retries under concurrent writers.
const MAX_APPEND_ATTEMPTS: usize = 5;

/// Connects a shared PostgreSQL client suitable for backing this store (and
/// other Rakka stores) over one connection.
///
/// The connection driver task is spawned onto the current Tokio runtime;
/// connection errors are reported through subsequent query failures.
pub async fn connect_shared_postgres_client(dsn: &str) -> TaskProjectionResult<Arc<Client>> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls)
        .await
        .map_err(store_error)?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(error = %error, "rakka-a2a postgres connection closed");
        }
    });
    Ok(Arc::new(client))
}

/// Shared PostgreSQL projection store for the A2A read model.
///
/// Any node can serve `get_task`, `list_tasks`, and stream replay from this
/// store after owner movement, because every node reads the same durable
/// projection rows and event log.
#[derive(Clone)]
pub struct PostgresA2ATaskProjectionStore {
    client: Arc<Client>,
    retention: A2ATaskEventRetention,
}

impl std::fmt::Debug for PostgresA2ATaskProjectionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresA2ATaskProjectionStore")
            .field("retention", &self.retention)
            .finish_non_exhaustive()
    }
}

impl PostgresA2ATaskProjectionStore {
    /// Creates a store over a dedicated client.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::from_shared_client(Arc::new(client))
    }

    /// Creates a store that shares an already-`Arc`-wrapped client with
    /// other Rakka PostgreSQL stores.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>) -> Self {
        Self {
            client,
            retention: A2ATaskEventRetention::default(),
        }
    }

    /// Overrides the per-task event retention bound.
    #[must_use]
    pub fn with_retention(mut self, retention: A2ATaskEventRetention) -> Self {
        self.retention = retention;
        self
    }

    /// Applies the idempotent A2A schema under the crate's advisory lock.
    ///
    /// Safe to run from every node at startup; concurrent callers serialize
    /// on [`MIGRATION_LOCK_ID`].
    pub async fn migrate(&self) -> TaskProjectionResult<()> {
        self.client
            .query_one("SELECT pg_advisory_lock($1)", &[&MIGRATION_LOCK_ID])
            .await
            .map_err(store_error)?;
        let applied = self.client.batch_execute(MIGRATION_SQL).await;
        let unlocked = self
            .client
            .query_one("SELECT pg_advisory_unlock($1)", &[&MIGRATION_LOCK_ID])
            .await;
        applied.map_err(store_error)?;
        unlocked.map_err(store_error)?;
        Ok(())
    }

    /// Reads the projection row plus any durable events past its revision,
    /// folded into the current view. Folding self-heals the window between
    /// an event insert and its projection-row update.
    async fn folded_projection(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> TaskProjectionResult<Option<(A2ATaskProjection, u64)>> {
        let row = self
            .client
            .query_opt(
                "SELECT tenant, task_id, context_id, workflow_id, status, \
                 status_timestamp_millis, projection_revision, history, artifacts, metadata \
                 FROM rakka_a2a_tasks WHERE tenant = $1 AND task_id = $2",
                &[&tenant, &task_id],
            )
            .await
            .map_err(store_error)?;
        let base = row.map(|row| projection_from_row(&row)).transpose()?;
        let base_revision = base.as_ref().map_or(0, |p| p.projection_revision);

        let tail = self
            .client
            .query(
                "SELECT payload FROM rakka_a2a_task_events \
                 WHERE tenant = $1 AND task_id = $2 AND sequence > $3 ORDER BY sequence",
                &[&tenant, &task_id, &to_i64(base_revision)?],
            )
            .await
            .map_err(store_error)?;
        let tail = tail
            .into_iter()
            .map(|row| event_from_row(&row))
            .collect::<TaskProjectionResult<Vec<_>>>()?;
        let high_watermark = tail.last().map_or(base_revision, |event| event.sequence);

        let folded = fold_events(base, &tail);
        Ok(folded.map(|projection| (projection, high_watermark)))
    }

    /// Writes the projection row, keeping the highest revision on conflict.
    async fn converge_row(
        &self,
        projection: &A2ATaskProjection,
        allow_equal_revision: bool,
    ) -> TaskProjectionResult<()> {
        let guard = if allow_equal_revision {
            "rakka_a2a_tasks.projection_revision <= EXCLUDED.projection_revision"
        } else {
            "rakka_a2a_tasks.projection_revision < EXCLUDED.projection_revision"
        };
        let statement = format!(
            "INSERT INTO rakka_a2a_tasks (tenant, task_id, context_id, workflow_id, \
             workflow_type, definition_version, status, status_timestamp_millis, \
             projection_revision, history, artifacts, metadata) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
             ON CONFLICT (tenant, task_id) DO UPDATE SET \
             context_id = EXCLUDED.context_id, \
             workflow_id = EXCLUDED.workflow_id, \
             workflow_type = EXCLUDED.workflow_type, \
             definition_version = EXCLUDED.definition_version, \
             status = EXCLUDED.status, \
             status_timestamp_millis = EXCLUDED.status_timestamp_millis, \
             projection_revision = EXCLUDED.projection_revision, \
             history = EXCLUDED.history, \
             artifacts = EXCLUDED.artifacts, \
             metadata = EXCLUDED.metadata, \
             updated_at = now() \
             WHERE {guard}"
        );
        let workflow_type = metadata_text(&projection.metadata, crate::mapping::META_WORKFLOW_TYPE);
        let definition_version = metadata_text(
            &projection.metadata,
            crate::mapping::META_DEFINITION_VERSION,
        );
        let params: &[&(dyn ToSql + Sync)] = &[
            &projection.tenant,
            &projection.task_id,
            &projection.context_id,
            &projection.workflow_id,
            &workflow_type,
            &definition_version,
            &encode_state(&projection.status)?,
            &to_i64(projection.status_timestamp.as_millis())?,
            &to_i64(projection.projection_revision)?,
            &to_json(&projection.history)?,
            &to_json(&projection.artifacts)?,
            &to_json(&projection.metadata)?,
        ];
        self.client
            .execute(&statement, params)
            .await
            .map_err(store_error)?;
        Ok(())
    }

    /// Claims one event sequence; false means a concurrent writer won it.
    async fn insert_event(&self, event: &A2ATaskEvent) -> TaskProjectionResult<bool> {
        let params: &[&(dyn ToSql + Sync)] = &[
            &event.tenant,
            &event.task_id,
            &to_i64(event.sequence)?,
            &event.kind().as_label(),
            &to_i64(event.occurred_at.as_millis())?,
            &encode_state(&event.projected_state)?,
            &event.redaction.as_label(),
            &to_json(event)?,
            &to_json(&event.metadata)?,
        ];
        let inserted = self
            .client
            .execute(
                "INSERT INTO rakka_a2a_task_events (tenant, task_id, sequence, event_kind, \
                 occurred_at_millis, projected_state, redaction, payload, metadata) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                 ON CONFLICT (tenant, task_id, sequence) DO NOTHING",
                params,
            )
            .await
            .map_err(store_error)?;
        Ok(inserted == 1)
    }

    /// Compacts the event tail past the retention bound, always keeping the
    /// newest snapshot event so subscribers can re-bootstrap.
    async fn apply_retention(&self, tenant: &str, task_id: &str) -> TaskProjectionResult<()> {
        let limit = to_i64(self.retention.max_events_per_task as u64)?;
        self.client
            .execute(
                "DELETE FROM rakka_a2a_task_events \
                 WHERE tenant = $1 AND task_id = $2 \
                 AND sequence <= (SELECT COALESCE(MAX(sequence), 0) - $3 \
                                  FROM rakka_a2a_task_events \
                                  WHERE tenant = $1 AND task_id = $2) \
                 AND sequence <> COALESCE((SELECT MAX(sequence) \
                                           FROM rakka_a2a_task_events \
                                           WHERE tenant = $1 AND task_id = $2 \
                                           AND event_kind = 'snapshot'), 0)",
                &[&tenant, &task_id, &limit],
            )
            .await
            .map_err(store_error)?;
        Ok(())
    }

    async fn append(
        &self,
        event_for_sequence: impl Fn(
            u64,
            Option<&A2ATaskProjection>,
        ) -> TaskProjectionResult<A2ATaskEvent>,
        tenant: &str,
        task_id: &str,
    ) -> TaskProjectionResult<A2ATaskEvent> {
        for _ in 0..MAX_APPEND_ATTEMPTS {
            let folded = self.folded_projection(tenant, task_id).await?;
            let (current, high_watermark) = match folded {
                Some((projection, watermark)) => (Some(projection), watermark),
                None => (None, 0),
            };
            let next_sequence = high_watermark
                .max(current.as_ref().map_or(0, |p| p.projection_revision))
                .saturating_add(1);
            let mut event = event_for_sequence(next_sequence, current.as_ref())?;

            let new_projection = match current {
                Some(mut projection) => {
                    projection.apply_event(&event)?;
                    event.projected_state = projection.status.clone();
                    projection
                }
                None => {
                    let A2ATaskEventPayload::Snapshot(snapshot) = &event.payload else {
                        return Err(TaskProjectionError::TaskNotFound {
                            task_id: task_id.to_string(),
                        });
                    };
                    let adopted = adopted_snapshot(snapshot, &event);
                    event.projected_state = adopted.status.clone();
                    adopted
                }
            };

            if !self.insert_event(&event).await? {
                // A concurrent writer claimed this sequence; re-read and retry.
                continue;
            }
            self.converge_row(&new_projection, false).await?;
            self.apply_retention(tenant, task_id).await?;
            return Ok(event);
        }
        Err(TaskProjectionError::Store {
            backend: BACKEND_NAME,
            message: "concurrent append contention exceeded retry bound".to_string(),
        })
    }
}

#[async_trait]
impl A2ATaskProjectionStore for PostgresA2ATaskProjectionStore {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn supports_shared_replay(&self) -> bool {
        true
    }

    async fn upsert(&self, projection: A2ATaskProjection) -> TaskProjectionResult<()> {
        // Idempotent replace at the same revision; never regresses a newer row.
        self.converge_row(&projection, true).await
    }

    async fn projection(
        &self,
        tenant: Option<&str>,
        task_id: &str,
    ) -> TaskProjectionResult<A2ATaskProjection> {
        let Some(tenant) = tenant else {
            return Err(TaskProjectionError::TenantRequired);
        };
        let Some((projection, _)) = self.folded_projection(tenant, task_id).await? else {
            return Err(TaskProjectionError::TaskNotFound {
                task_id: task_id.to_string(),
            });
        };
        Ok(projection)
    }

    async fn append_event_payload(
        &self,
        tenant: &str,
        task_id: &str,
        context_id: &str,
        occurred_at: AgentTimestampMillis,
        payload: A2ATaskEventPayload,
    ) -> TaskProjectionResult<A2ATaskEvent> {
        self.append(
            |sequence, _| {
                Ok(A2ATaskEvent::new(
                    tenant,
                    task_id,
                    context_id,
                    sequence,
                    occurred_at,
                    payload.clone(),
                ))
            },
            tenant,
            task_id,
        )
        .await
    }

    async fn append_event(&self, event: A2ATaskEvent) -> TaskProjectionResult<A2ATaskEvent> {
        let tenant = event.tenant.clone();
        let task_id = event.task_id.clone();
        self.append(
            |sequence, current| {
                // Fixed-sequence appends must extend the current view exactly,
                // matching the in-memory store's ordering rule; snapshots may
                // bootstrap an unknown task at any sequence.
                if current.is_some() && event.sequence != sequence {
                    return Err(TaskProjectionError::EventOrder {
                        expected: sequence,
                        actual: event.sequence,
                    });
                }
                Ok(event.clone())
            },
            &tenant,
            &task_id,
        )
        .await
    }

    async fn list(&self, request: &ListTasksRequest) -> TaskProjectionResult<ListTasksResponse> {
        let Some(tenant) = request.tenant.as_deref() else {
            return Err(TaskProjectionError::TenantRequired);
        };
        let offset = page_offset(request.page_token.as_deref())?;
        let page_size = page_size(request.page_size);

        let mut conditions = vec!["tenant = $1".to_string()];
        let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(tenant.to_string())];
        if let Some(context_id) = request.context_id.as_deref() {
            params.push(Box::new(context_id.to_string()));
            conditions.push(format!("context_id = ${}", params.len()));
        }
        if let Some(status) = request.status.as_ref() {
            params.push(Box::new(encode_state(status)?));
            conditions.push(format!("status = ${}", params.len()));
        }
        if let Some(after) = request.status_timestamp_after {
            params.push(Box::new(after.timestamp_millis()));
            conditions.push(format!("status_timestamp_millis > ${}", params.len()));
        }
        params.push(Box::new(to_i64(offset as u64)?));
        let offset_param = params.len();
        params.push(Box::new(to_i64(page_size as u64)?));
        let limit_param = params.len();

        let statement = format!(
            "SELECT tenant, task_id, context_id, workflow_id, status, \
             status_timestamp_millis, projection_revision, history, artifacts, metadata, \
             COUNT(*) OVER () AS total_size \
             FROM rakka_a2a_tasks WHERE {} \
             ORDER BY tenant, task_id OFFSET ${offset_param} LIMIT ${limit_param}",
            conditions.join(" AND ")
        );
        let param_refs = params
            .iter()
            .map(|param| param.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let rows = self
            .client
            .query(&statement, &param_refs)
            .await
            .map_err(store_error)?;

        let total_size = rows
            .first()
            .map(|row| row.get::<_, i64>("total_size"))
            .unwrap_or(0);
        let total_size = if rows.is_empty() {
            // The page ran past the end; count the filtered set separately.
            let count_statement = format!(
                "SELECT COUNT(*) AS total_size FROM rakka_a2a_tasks WHERE {}",
                conditions.join(" AND ")
            );
            let count_refs = &param_refs[..param_refs.len() - 2];
            self.client
                .query_one(&count_statement, count_refs)
                .await
                .map_err(store_error)?
                .get::<_, i64>("total_size")
        } else {
            total_size
        };

        let tasks = rows
            .iter()
            .map(|row| {
                projection_from_row(row).map(|projection| {
                    projection.to_task(
                        request.history_length,
                        request.include_artifacts.unwrap_or(false),
                    )
                })
            })
            .collect::<TaskProjectionResult<Vec<_>>>()?;
        let next_offset = offset.saturating_add(tasks.len());
        let next_page_token = if (next_offset as i64) < total_size {
            next_offset.to_string()
        } else {
            String::new()
        };
        Ok(ListTasksResponse {
            tasks,
            next_page_token,
            page_size: i32::try_from(page_size).unwrap_or(i32::MAX),
            total_size: i32::try_from(total_size).unwrap_or(i32::MAX),
        })
    }

    async fn replay_events(
        &self,
        tenant: &str,
        task_id: &str,
        after_cursor: Option<&str>,
    ) -> TaskProjectionResult<Vec<A2ATaskEvent>> {
        let after_sequence = match after_cursor {
            None => 0,
            Some(cursor) => {
                let (cursor_task_id, sequence) = parse_replay_cursor(cursor)?;
                if cursor_task_id != task_id {
                    return Err(TaskProjectionError::InvalidReplayCursor {
                        cursor: cursor.to_string(),
                    });
                }
                sequence
            }
        };

        if after_sequence > 0 {
            // A cursor beyond everything durably recorded cannot prove
            // continuity; force a resync instead of silently returning an
            // empty tail.
            let known = self
                .client
                .query_one(
                    "SELECT GREATEST( \
                       COALESCE((SELECT MAX(sequence) FROM rakka_a2a_task_events \
                                 WHERE tenant = $1 AND task_id = $2), 0), \
                       COALESCE((SELECT projection_revision FROM rakka_a2a_tasks \
                                 WHERE tenant = $1 AND task_id = $2), 0)) AS known",
                    &[&tenant, &task_id],
                )
                .await
                .map_err(store_error)?
                .get::<_, i64>("known");
            if after_sequence > u64::try_from(known).unwrap_or(0) {
                return Err(TaskProjectionError::InvalidReplayCursor {
                    cursor: after_cursor.unwrap_or_default().to_string(),
                });
            }
        }

        let rows = self
            .client
            .query(
                "SELECT payload FROM rakka_a2a_task_events \
                 WHERE tenant = $1 AND task_id = $2 AND sequence > $3 ORDER BY sequence",
                &[&tenant, &task_id, &to_i64(after_sequence)?],
            )
            .await
            .map_err(store_error)?;
        let events = rows
            .iter()
            .map(event_from_row)
            .collect::<TaskProjectionResult<Vec<_>>>()?;

        if events.is_empty() {
            if after_sequence == 0 {
                return Ok(Vec::new());
            }
            // The projection is ahead of the retained log (retention removed
            // the tail); incremental replay cannot resume from this cursor.
            return Err(TaskProjectionError::ReplayWindowExpired {
                task_id: task_id.to_string(),
                earliest_sequence: 0,
            });
        }
        // Retention keeps snapshots out of order with the tail; replay from
        // before the retained window must resync, never skip silently.
        let mut expected = after_sequence.saturating_add(1);
        for event in &events {
            if event.sequence != expected {
                return Err(TaskProjectionError::ReplayWindowExpired {
                    task_id: task_id.to_string(),
                    earliest_sequence: event.sequence,
                });
            }
            expected = event.sequence.saturating_add(1);
        }
        Ok(events)
    }
}

/// Folds tail events onto a base projection, adopting snapshots to bridge
/// bootstrap and any non-contiguous stretch left by retention.
fn fold_events(
    base: Option<A2ATaskProjection>,
    tail: &[A2ATaskEvent],
) -> Option<A2ATaskProjection> {
    let mut current = base;
    for event in tail {
        match current.as_mut() {
            Some(projection) if event.sequence == projection.projection_revision + 1 => {
                if projection.apply_event(event).is_err() {
                    // Deterministic events cannot fail ordered apply; skip
                    // defensively rather than poison the read path.
                    continue;
                }
            }
            _ => {
                if let A2ATaskEventPayload::Snapshot(snapshot) = &event.payload {
                    current = Some(adopted_snapshot(snapshot, event));
                }
            }
        }
    }
    current
}

fn projection_from_row(row: &Row) -> TaskProjectionResult<A2ATaskProjection> {
    let status: String = row.get("status");
    let history: Value = row.get("history");
    let artifacts: Value = row.get("artifacts");
    let metadata: Value = row.get("metadata");
    Ok(A2ATaskProjection {
        task_id: row.get("task_id"),
        context_id: row.get("context_id"),
        tenant: row.get("tenant"),
        workflow_id: row.get("workflow_id"),
        status: decode_state(&status)?,
        status_timestamp: AgentTimestampMillis::new(
            u64::try_from(row.get::<_, i64>("status_timestamp_millis")).unwrap_or(0),
        ),
        history: from_json(history)?,
        artifacts: from_json(artifacts)?,
        metadata: from_json(metadata)?,
        projection_revision: u64::try_from(row.get::<_, i64>("projection_revision")).unwrap_or(0),
    })
}

fn event_from_row(row: &Row) -> TaskProjectionResult<A2ATaskEvent> {
    let payload: Value = row.get("payload");
    from_json(payload)
}

fn metadata_text(metadata: &std::collections::HashMap<String, Value>, key: &str) -> String {
    metadata
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn encode_state(state: &TaskState) -> TaskProjectionResult<String> {
    match serde_json::to_value(state).map_err(codec_error)? {
        Value::String(label) => Ok(label),
        other => Ok(other.to_string()),
    }
}

fn decode_state(label: &str) -> TaskProjectionResult<TaskState> {
    serde_json::from_value(Value::String(label.to_string())).map_err(codec_error)
}

fn to_json<T: serde::Serialize>(value: &T) -> TaskProjectionResult<Value> {
    serde_json::to_value(value).map_err(codec_error)
}

fn from_json<T: serde::de::DeserializeOwned>(value: Value) -> TaskProjectionResult<T> {
    serde_json::from_value(value).map_err(codec_error)
}

fn to_i64(value: u64) -> TaskProjectionResult<i64> {
    i64::try_from(value).map_err(|_| TaskProjectionError::Store {
        backend: BACKEND_NAME,
        message: format!("value {value} exceeds BIGINT range"),
    })
}

fn store_error(error: tokio_postgres::Error) -> TaskProjectionError {
    TaskProjectionError::Store {
        backend: BACKEND_NAME,
        message: error.to_string(),
    }
}

fn codec_error(error: serde_json::Error) -> TaskProjectionError {
    TaskProjectionError::Store {
        backend: BACKEND_NAME,
        message: format!("codec: {error}"),
    }
}

// The PostgreSQL event watcher (bounded interval polling per the crate's
// replay design) is provided by `postgres_watcher`; re-exported here so the
// postgres feature surfaces one module.
pub use crate::postgres_watcher::PostgresA2ATaskEventWatcher;
