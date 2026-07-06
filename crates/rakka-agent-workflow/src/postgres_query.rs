//! PostgreSQL operational query index for agent workflows.

use std::error::Error;
use std::sync::Arc;

use tokio_postgres::{types::ToSql, Client, Row};

use crate::{
    AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanFingerprint, AgentDispatchId,
    AgentDispatchIndexEntry, AgentDispatchQuery, AgentDispatchStatus, AgentDispatchTargetClass,
    AgentDispatcherWorkerId, AgentEffectId, AgentEffectKind, AgentGraphNodeStatus,
    AgentGraphRunProjection, AgentGraphWaitReason, AgentRunId, AgentRunIndexEntry,
    AgentRunQueryWaitingReason, AgentRunStatus, AgentRuntimeEventProjection, AgentStepId,
    AgentTenantId, AgentTimerId, AgentTimerIndexEntry, AgentTimerQuery, AgentTimerStatus,
    AgentTimestampMillis, AgentWorkflowId, AgentWorkflowQueryError, AgentWorkflowQueryFuture,
    AgentWorkflowQueryIndex, AgentWorkflowQueryResult, AgentWorkflowRunQuery,
    AgentWorkflowShardOwnership, HumanCheckpointId, WorkflowDefinitionVersion,
};

/// Default namespace for PostgreSQL workflow query projections.
pub const DEFAULT_AGENT_WORKFLOW_QUERY_NAMESPACE: &str = "default";

/// PostgreSQL advisory lock id used while applying workflow query migrations.
pub const AGENT_WORKFLOW_QUERY_MIGRATION_LOCK_ID: i64 = 982_451_659;

/// Default workflow run index table name.
pub const AGENT_WORKFLOW_RUN_INDEX_TABLE: &str = "rakka_agent_workflow_run_index";

/// Default workflow graph-node projection table name.
pub const AGENT_WORKFLOW_GRAPH_NODE_INDEX_TABLE: &str = "rakka_agent_workflow_graph_node_index";

/// Default workflow timer index table name.
pub const AGENT_WORKFLOW_TIMER_INDEX_TABLE: &str = "rakka_agent_workflow_timer_index";

/// Default workflow checkpoint index table name.
pub const AGENT_WORKFLOW_CHECKPOINT_INDEX_TABLE: &str = "rakka_agent_workflow_checkpoint_index";

/// Default workflow dispatcher index table name.
pub const AGENT_WORKFLOW_DISPATCH_INDEX_TABLE: &str = "rakka_agent_workflow_dispatch_index";

/// Default workflow audit index table name.
pub const AGENT_WORKFLOW_AUDIT_INDEX_TABLE: &str = "rakka_agent_workflow_audit_index";

/// Default runtime event projection index table name.
pub const AGENT_WORKFLOW_RUNTIME_EVENT_PROJECTION_TABLE: &str =
    "rakka_agent_workflow_runtime_event_projection";

/// SQL migration for PostgreSQL agent workflow query indexes.
pub const AGENT_WORKFLOW_QUERY_MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS rakka_agent_workflow_run_index (
    store_namespace TEXT NOT NULL,
    run_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    workflow_type TEXT NOT NULL,
    definition_version TEXT NOT NULL,
    tenant TEXT NULL,
    workflow_namespace TEXT NULL,
    status TEXT NOT NULL,
    waiting_reason TEXT NULL,
    current_step_id TEXT NULL,
    failed_step_id TEXT NULL,
    pending_human_checkpoint TEXT NULL,
    open_checkpoint_created_at_millis BIGINT NULL CHECK (open_checkpoint_created_at_millis >= 0),
    open_checkpoint_due_at_millis BIGINT NULL CHECK (open_checkpoint_due_at_millis >= 0),
    shard_entity_type TEXT NULL,
    shard_id TEXT NULL,
    shard_owner_node_id TEXT NULL,
    graph_plan_id TEXT NULL,
    graph_plan_fingerprint TEXT NULL,
    graph_scheduler_revision BIGINT NULL CHECK (graph_scheduler_revision >= 0),
    graph_last_event_sequence BIGINT NULL CHECK (graph_last_event_sequence >= 0),
    graph_terminal_status TEXT NULL,
    graph_projection_json TEXT NULL,
    created_at_millis BIGINT NOT NULL CHECK (created_at_millis >= 0),
    updated_at_millis BIGINT NOT NULL CHECK (updated_at_millis >= 0),
    completed_at_millis BIGINT NULL CHECK (completed_at_millis >= 0),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_namespace, run_id)
);

ALTER TABLE rakka_agent_workflow_run_index
    ADD COLUMN IF NOT EXISTS graph_plan_id TEXT NULL;
ALTER TABLE rakka_agent_workflow_run_index
    ADD COLUMN IF NOT EXISTS graph_plan_fingerprint TEXT NULL;
ALTER TABLE rakka_agent_workflow_run_index
    ADD COLUMN IF NOT EXISTS graph_scheduler_revision BIGINT NULL CHECK (graph_scheduler_revision >= 0);
ALTER TABLE rakka_agent_workflow_run_index
    ADD COLUMN IF NOT EXISTS graph_last_event_sequence BIGINT NULL CHECK (graph_last_event_sequence >= 0);
ALTER TABLE rakka_agent_workflow_run_index
    ADD COLUMN IF NOT EXISTS graph_terminal_status TEXT NULL;
ALTER TABLE rakka_agent_workflow_run_index
    ADD COLUMN IF NOT EXISTS graph_projection_json TEXT NULL;

CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_status_idx
    ON rakka_agent_workflow_run_index (store_namespace, status, updated_at_millis, run_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_waiting_idx
    ON rakka_agent_workflow_run_index (store_namespace, waiting_reason, updated_at_millis, run_id)
    WHERE waiting_reason IS NOT NULL;
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_workflow_idx
    ON rakka_agent_workflow_run_index
    (store_namespace, workflow_type, definition_version, updated_at_millis, run_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_tenant_idx
    ON rakka_agent_workflow_run_index
    (store_namespace, tenant, workflow_namespace, updated_at_millis, run_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_failed_step_idx
    ON rakka_agent_workflow_run_index (store_namespace, failed_step_id, updated_at_millis, run_id)
    WHERE failed_step_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_checkpoint_age_idx
    ON rakka_agent_workflow_run_index
    (store_namespace, open_checkpoint_created_at_millis, run_id)
    WHERE open_checkpoint_created_at_millis IS NOT NULL;
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_shard_owner_idx
    ON rakka_agent_workflow_run_index (store_namespace, shard_owner_node_id, shard_id, run_id)
    WHERE shard_owner_node_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_run_graph_plan_idx
    ON rakka_agent_workflow_run_index (store_namespace, graph_plan_fingerprint, updated_at_millis, run_id)
    WHERE graph_plan_fingerprint IS NOT NULL;

CREATE TABLE IF NOT EXISTS rakka_agent_workflow_graph_node_index (
    store_namespace TEXT NOT NULL,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    graph_plan_fingerprint TEXT NOT NULL,
    node_kind TEXT NOT NULL,
    node_status TEXT NOT NULL,
    wait_reason TEXT NULL,
    error_code TEXT NULL,
    updated_at_millis BIGINT NOT NULL CHECK (updated_at_millis >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_namespace, run_id, node_id)
);

CREATE INDEX IF NOT EXISTS rakka_agent_workflow_graph_node_plan_idx
    ON rakka_agent_workflow_graph_node_index
    (store_namespace, graph_plan_fingerprint, run_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_graph_node_status_kind_idx
    ON rakka_agent_workflow_graph_node_index
    (store_namespace, node_status, node_kind, run_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_graph_node_wait_idx
    ON rakka_agent_workflow_graph_node_index
    (store_namespace, wait_reason, run_id)
    WHERE wait_reason IS NOT NULL;
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_graph_node_error_idx
    ON rakka_agent_workflow_graph_node_index
    (store_namespace, error_code, run_id)
    WHERE error_code IS NOT NULL;

CREATE TABLE IF NOT EXISTS rakka_agent_workflow_timer_index (
    store_namespace TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    tenant TEXT NOT NULL,
    workflow_namespace TEXT NULL,
    due_at_millis BIGINT NOT NULL CHECK (due_at_millis >= 0),
    status TEXT NOT NULL,
    updated_at_millis BIGINT NOT NULL CHECK (updated_at_millis >= 0),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_namespace, timer_id)
);

CREATE INDEX IF NOT EXISTS rakka_agent_workflow_timer_due_idx
    ON rakka_agent_workflow_timer_index (store_namespace, status, due_at_millis, timer_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_timer_run_idx
    ON rakka_agent_workflow_timer_index (store_namespace, run_id, due_at_millis, timer_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_timer_tenant_idx
    ON rakka_agent_workflow_timer_index (store_namespace, tenant, workflow_namespace, due_at_millis);

CREATE TABLE IF NOT EXISTS rakka_agent_workflow_checkpoint_index (
    store_namespace TEXT NOT NULL,
    checkpoint_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    tenant TEXT NULL,
    workflow_namespace TEXT NULL,
    status TEXT NOT NULL,
    created_at_millis BIGINT NOT NULL CHECK (created_at_millis >= 0),
    due_at_millis BIGINT NULL CHECK (due_at_millis >= 0),
    resolved_at_millis BIGINT NULL CHECK (resolved_at_millis >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_namespace, checkpoint_id)
);

CREATE INDEX IF NOT EXISTS rakka_agent_workflow_checkpoint_run_idx
    ON rakka_agent_workflow_checkpoint_index (store_namespace, run_id, status, checkpoint_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_checkpoint_age_idx
    ON rakka_agent_workflow_checkpoint_index
    (store_namespace, status, created_at_millis, checkpoint_id);

CREATE TABLE IF NOT EXISTS rakka_agent_workflow_dispatch_index (
    store_namespace TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    workflow_id TEXT NULL,
    run_id TEXT NOT NULL,
    effect_id TEXT NOT NULL,
    effect_kind TEXT NOT NULL,
    target_class TEXT NOT NULL,
    graph_plan_fingerprint TEXT NULL,
    graph_node_id TEXT NULL,
    graph_node_kind TEXT NULL,
    graph_loop_instance_id TEXT NULL,
    due_at_millis BIGINT NOT NULL CHECK (due_at_millis >= 0),
    status TEXT NOT NULL,
    worker_id TEXT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    claimed_at_millis BIGINT NULL CHECK (claimed_at_millis >= 0),
    lease_expires_at_millis BIGINT NULL CHECK (lease_expires_at_millis >= 0),
    updated_at_millis BIGINT NOT NULL CHECK (updated_at_millis >= 0),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_namespace, dispatch_id)
);

ALTER TABLE rakka_agent_workflow_dispatch_index
    ADD COLUMN IF NOT EXISTS graph_plan_fingerprint TEXT NULL;
ALTER TABLE rakka_agent_workflow_dispatch_index
    ADD COLUMN IF NOT EXISTS graph_node_id TEXT NULL;
ALTER TABLE rakka_agent_workflow_dispatch_index
    ADD COLUMN IF NOT EXISTS graph_node_kind TEXT NULL;
ALTER TABLE rakka_agent_workflow_dispatch_index
    ADD COLUMN IF NOT EXISTS graph_loop_instance_id TEXT NULL;

CREATE INDEX IF NOT EXISTS rakka_agent_workflow_dispatch_due_idx
    ON rakka_agent_workflow_dispatch_index (store_namespace, status, due_at_millis, dispatch_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_dispatch_stuck_idx
    ON rakka_agent_workflow_dispatch_index
    (store_namespace, status, lease_expires_at_millis, dispatch_id)
    WHERE lease_expires_at_millis IS NOT NULL;
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_dispatch_run_idx
    ON rakka_agent_workflow_dispatch_index (store_namespace, run_id, status, dispatch_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_dispatch_target_idx
    ON rakka_agent_workflow_dispatch_index
    (store_namespace, target_class, status, due_at_millis, dispatch_id);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_dispatch_graph_node_idx
    ON rakka_agent_workflow_dispatch_index
    (store_namespace, graph_node_kind, status, due_at_millis, dispatch_id)
    WHERE graph_node_kind IS NOT NULL;
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_dispatch_graph_plan_idx
    ON rakka_agent_workflow_dispatch_index
    (store_namespace, graph_plan_fingerprint, status, due_at_millis, dispatch_id)
    WHERE graph_plan_fingerprint IS NOT NULL;

CREATE TABLE IF NOT EXISTS rakka_agent_workflow_audit_index (
    store_namespace TEXT NOT NULL,
    audit_event_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    tenant TEXT NULL,
    workflow_namespace TEXT NULL,
    audit_kind TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT NOT NULL,
    occurred_at_millis BIGINT NOT NULL CHECK (occurred_at_millis >= 0),
    redaction TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_namespace, audit_event_id)
);

CREATE INDEX IF NOT EXISTS rakka_agent_workflow_audit_run_idx
    ON rakka_agent_workflow_audit_index (store_namespace, run_id, occurred_at_millis);
CREATE INDEX IF NOT EXISTS rakka_agent_workflow_audit_correlation_idx
    ON rakka_agent_workflow_audit_index (store_namespace, correlation_id, occurred_at_millis);

CREATE TABLE IF NOT EXISTS rakka_agent_workflow_runtime_event_projection (
    store_namespace TEXT NOT NULL,
    run_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    definition_version TEXT NOT NULL,
    graph_plan_fingerprint TEXT NOT NULL,
    last_scheduler_revision BIGINT NOT NULL CHECK (last_scheduler_revision >= 0),
    last_event_sequence BIGINT NOT NULL CHECK (last_event_sequence >= 0),
    last_event_at_millis BIGINT NULL CHECK (last_event_at_millis >= 0),
    last_event_kind TEXT NULL,
    event_count BIGINT NOT NULL CHECK (event_count >= 0),
    node_event_count BIGINT NOT NULL CHECK (node_event_count >= 0),
    effect_event_count BIGINT NOT NULL CHECK (effect_event_count >= 0),
    timer_event_count BIGINT NOT NULL CHECK (timer_event_count >= 0),
    human_event_count BIGINT NOT NULL CHECK (human_event_count >= 0),
    terminal_event_kind TEXT NULL,
    projection_json TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_namespace, run_id)
);

CREATE INDEX IF NOT EXISTS rakka_agent_workflow_runtime_event_projection_plan_idx
    ON rakka_agent_workflow_runtime_event_projection
    (store_namespace, graph_plan_fingerprint, last_event_sequence, run_id);
"#;

/// Builder for [`PostgresAgentWorkflowQueryIndex`].
pub struct PostgresAgentWorkflowQueryIndexBuilder {
    client: Client,
    namespace: String,
}

impl PostgresAgentWorkflowQueryIndexBuilder {
    /// Sets the namespace used to isolate workflow query projections.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Builds a PostgreSQL workflow query index.
    #[must_use]
    pub fn build(self) -> PostgresAgentWorkflowQueryIndex {
        PostgresAgentWorkflowQueryIndex {
            client: Arc::new(self.client),
            namespace: self.namespace.into(),
        }
    }

    /// Applies the default migration and returns the built index.
    pub async fn migrate(self) -> AgentWorkflowQueryResult<PostgresAgentWorkflowQueryIndex> {
        let index = self.build();
        index.migrate().await?;
        Ok(index)
    }
}

/// PostgreSQL operational query index for agent workflows.
#[derive(Clone)]
pub struct PostgresAgentWorkflowQueryIndex {
    client: Arc<Client>,
    namespace: Arc<str>,
}

impl PostgresAgentWorkflowQueryIndex {
    /// Creates a PostgreSQL workflow query index in the default namespace.
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self::builder(client).build()
    }

    /// Creates a builder for a PostgreSQL workflow query index.
    #[must_use]
    pub fn builder(client: Client) -> PostgresAgentWorkflowQueryIndexBuilder {
        PostgresAgentWorkflowQueryIndexBuilder {
            client,
            namespace: DEFAULT_AGENT_WORKFLOW_QUERY_NAMESPACE.to_string(),
        }
    }

    /// Creates a PostgreSQL workflow query index in an explicit namespace.
    #[must_use]
    pub fn with_namespace(client: Client, namespace: impl Into<String>) -> Self {
        Self::builder(client).with_namespace(namespace).build()
    }

    /// Namespace used to isolate workflow query projections.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Applies the default workflow query index migration.
    pub async fn migrate(&self) -> AgentWorkflowQueryResult<()> {
        acquire_migration_lock(&self.client)
            .await
            .map_err(map_postgres_error)?;
        let migration_result = self
            .client
            .batch_execute(AGENT_WORKFLOW_QUERY_MIGRATION_SQL)
            .await;
        let unlock_result = release_migration_lock(&self.client).await;

        match (migration_result, unlock_result) {
            (Err(error), _) | (Ok(()), Err(error)) => Err(map_postgres_error(error)),
            (Ok(()), Ok(())) => Ok(()),
        }
    }

    /// Deletes all projections for this store namespace.
    pub async fn delete_namespace(&self) -> AgentWorkflowQueryResult<()> {
        for table in [
            AGENT_WORKFLOW_RUNTIME_EVENT_PROJECTION_TABLE,
            AGENT_WORKFLOW_AUDIT_INDEX_TABLE,
            AGENT_WORKFLOW_CHECKPOINT_INDEX_TABLE,
            AGENT_WORKFLOW_GRAPH_NODE_INDEX_TABLE,
            AGENT_WORKFLOW_DISPATCH_INDEX_TABLE,
            AGENT_WORKFLOW_TIMER_INDEX_TABLE,
            AGENT_WORKFLOW_RUN_INDEX_TABLE,
        ] {
            let sql = format!("DELETE FROM {table} WHERE store_namespace = $1");
            self.client
                .execute(&sql, &[&self.namespace.as_ref()])
                .await
                .map_err(map_postgres_error)?;
        }
        Ok(())
    }

    /// Inserts or replaces one run-level runtime event projection.
    ///
    /// The projection is fenced by `last_event_sequence`, so stale rebuilds
    /// cannot overwrite a newer event stream view.
    pub async fn upsert_runtime_event_projection(
        &self,
        projection: AgentRuntimeEventProjection,
    ) -> AgentWorkflowQueryResult<()> {
        upsert_runtime_event_projection(&self.client, self.namespace.as_ref(), projection).await
    }

    /// Returns one run-level runtime event projection, when indexed.
    pub async fn runtime_event_projection(
        &self,
        run_id: AgentRunId,
    ) -> AgentWorkflowQueryResult<Option<AgentRuntimeEventProjection>> {
        runtime_event_projection(&self.client, self.namespace.as_ref(), run_id).await
    }
}

impl std::fmt::Debug for PostgresAgentWorkflowQueryIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresAgentWorkflowQueryIndex")
            .field("namespace", &self.namespace())
            .finish_non_exhaustive()
    }
}

impl AgentWorkflowQueryIndex for PostgresAgentWorkflowQueryIndex {
    fn upsert_run<'a>(&'a mut self, entry: AgentRunIndexEntry) -> AgentWorkflowQueryFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move { upsert_run(&client, namespace.as_ref(), entry).await })
    }

    fn remove_run<'a>(&'a mut self, run_id: AgentRunId) -> AgentWorkflowQueryFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            client
                .execute(
                    "DELETE FROM rakka_agent_workflow_run_index WHERE store_namespace = $1 AND run_id = $2",
                    &[&namespace.as_ref(), &run_id.as_str()],
                )
                .await
                .map_err(map_postgres_error)?;
            client
                .execute(
                    "DELETE FROM rakka_agent_workflow_checkpoint_index WHERE store_namespace = $1 AND run_id = $2",
                    &[&namespace.as_ref(), &run_id.as_str()],
                )
                .await
                .map_err(map_postgres_error)?;
            client
                .execute(
                    "DELETE FROM rakka_agent_workflow_graph_node_index WHERE store_namespace = $1 AND run_id = $2",
                    &[&namespace.as_ref(), &run_id.as_str()],
                )
                .await
                .map_err(map_postgres_error)?;
            Ok(())
        })
    }

    fn upsert_timer<'a>(
        &'a mut self,
        entry: AgentTimerIndexEntry,
    ) -> AgentWorkflowQueryFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move { upsert_timer(&client, namespace.as_ref(), entry).await })
    }

    fn remove_timer<'a>(&'a mut self, timer_id: AgentTimerId) -> AgentWorkflowQueryFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            client
                .execute(
                    "DELETE FROM rakka_agent_workflow_timer_index WHERE store_namespace = $1 AND timer_id = $2",
                    &[&namespace.as_ref(), &timer_id.as_str()],
                )
                .await
                .map_err(map_postgres_error)?;
            Ok(())
        })
    }

    fn upsert_dispatch<'a>(
        &'a mut self,
        entry: AgentDispatchIndexEntry,
    ) -> AgentWorkflowQueryFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move { upsert_dispatch(&client, namespace.as_ref(), entry).await })
    }

    fn remove_dispatch<'a>(
        &'a mut self,
        dispatch_id: AgentDispatchId,
    ) -> AgentWorkflowQueryFuture<'a, ()> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move {
            client
                .execute(
                    "DELETE FROM rakka_agent_workflow_dispatch_index WHERE store_namespace = $1 AND dispatch_id = $2",
                    &[&namespace.as_ref(), &dispatch_id.as_str()],
                )
                .await
                .map_err(map_postgres_error)?;
            Ok(())
        })
    }

    fn query_runs<'a>(
        &'a self,
        query: AgentWorkflowRunQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentRunIndexEntry>> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move { query_runs(&client, namespace.as_ref(), query).await })
    }

    fn query_timers<'a>(
        &'a self,
        query: AgentTimerQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentTimerIndexEntry>> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move { query_timers(&client, namespace.as_ref(), query).await })
    }

    fn query_dispatches<'a>(
        &'a self,
        query: AgentDispatchQuery,
    ) -> AgentWorkflowQueryFuture<'a, Vec<AgentDispatchIndexEntry>> {
        let client = self.client.clone();
        let namespace = self.namespace.clone();
        Box::pin(async move { query_dispatches(&client, namespace.as_ref(), query).await })
    }
}

async fn upsert_run(
    client: &Client,
    namespace: &str,
    entry: AgentRunIndexEntry,
) -> AgentWorkflowQueryResult<()> {
    let tenant = entry
        .tenant
        .as_ref()
        .map(|tenant| tenant.as_str().to_string());
    let current_step_id = entry
        .current_step_id
        .as_ref()
        .map(|step_id| step_id.as_str().to_string());
    let failed_step_id = entry
        .failed_step_id
        .as_ref()
        .map(|step_id| step_id.as_str().to_string());
    let pending_checkpoint = entry
        .pending_human_checkpoint
        .as_ref()
        .map(|checkpoint_id| checkpoint_id.as_str().to_string());
    let ownership = entry.shard_ownership.clone();
    let shard_entity_type = ownership
        .as_ref()
        .map(|ownership| ownership.entity_type.clone());
    let shard_id = ownership
        .as_ref()
        .map(|ownership| ownership.shard_id.clone());
    let shard_owner = ownership
        .as_ref()
        .map(|ownership| ownership.owner_node_id.clone());
    let graph = entry.graph.clone();
    let graph_plan_id = graph
        .as_ref()
        .map(|graph| graph.plan_id.as_str().to_string());
    let graph_plan_fingerprint = graph
        .as_ref()
        .map(|graph| graph.plan_fingerprint.as_str().to_string());
    let graph_scheduler_revision = graph
        .as_ref()
        .map(|graph| u64_to_i64(graph.scheduler_revision, "graph scheduler revision"))
        .transpose()?;
    let graph_last_event_sequence = graph
        .as_ref()
        .map(|graph| u64_to_i64(graph.last_event_sequence, "graph last event sequence"))
        .transpose()?;
    let graph_terminal_status = graph.as_ref().and_then(|graph| {
        graph
            .terminal_status
            .map(|status| status.as_label().to_string())
    });
    let graph_projection_json = graph
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(map_json_error)?;
    let waiting_reason = entry.waiting_reason.map(waiting_reason_label);
    let open_checkpoint_created_at = optional_millis(entry.open_checkpoint_created_at)?;
    let open_checkpoint_due_at = optional_millis(entry.open_checkpoint_due_at)?;
    let created_at = millis_to_i64(entry.created_at)?;
    let updated_at = millis_to_i64(entry.updated_at)?;
    let completed_at = optional_millis(entry.completed_at)?;

    let row = client
        .query_opt(
            r#"
INSERT INTO rakka_agent_workflow_run_index (
    store_namespace,
    run_id,
    workflow_id,
    workflow_type,
    definition_version,
    tenant,
    workflow_namespace,
    status,
    waiting_reason,
    current_step_id,
    failed_step_id,
    pending_human_checkpoint,
    open_checkpoint_created_at_millis,
    open_checkpoint_due_at_millis,
    shard_entity_type,
    shard_id,
    shard_owner_node_id,
    graph_plan_id,
    graph_plan_fingerprint,
    graph_scheduler_revision,
    graph_last_event_sequence,
    graph_terminal_status,
    graph_projection_json,
    created_at_millis,
    updated_at_millis,
    completed_at_millis
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26
)
ON CONFLICT (store_namespace, run_id) DO UPDATE
SET workflow_id = EXCLUDED.workflow_id,
    workflow_type = EXCLUDED.workflow_type,
    definition_version = EXCLUDED.definition_version,
    tenant = EXCLUDED.tenant,
    workflow_namespace = EXCLUDED.workflow_namespace,
    status = EXCLUDED.status,
    waiting_reason = EXCLUDED.waiting_reason,
    current_step_id = EXCLUDED.current_step_id,
    failed_step_id = EXCLUDED.failed_step_id,
    pending_human_checkpoint = EXCLUDED.pending_human_checkpoint,
    open_checkpoint_created_at_millis = EXCLUDED.open_checkpoint_created_at_millis,
    open_checkpoint_due_at_millis = EXCLUDED.open_checkpoint_due_at_millis,
    shard_entity_type = EXCLUDED.shard_entity_type,
    shard_id = EXCLUDED.shard_id,
    shard_owner_node_id = EXCLUDED.shard_owner_node_id,
    graph_plan_id = EXCLUDED.graph_plan_id,
    graph_plan_fingerprint = EXCLUDED.graph_plan_fingerprint,
    graph_scheduler_revision = EXCLUDED.graph_scheduler_revision,
    graph_last_event_sequence = EXCLUDED.graph_last_event_sequence,
    graph_terminal_status = EXCLUDED.graph_terminal_status,
    graph_projection_json = EXCLUDED.graph_projection_json,
    created_at_millis = EXCLUDED.created_at_millis,
    updated_at_millis = EXCLUDED.updated_at_millis,
    completed_at_millis = EXCLUDED.completed_at_millis,
    revision = rakka_agent_workflow_run_index.revision + 1,
    updated_at = now()
WHERE rakka_agent_workflow_run_index.updated_at_millis <= EXCLUDED.updated_at_millis
RETURNING revision
"#,
            &[
                &namespace,
                &entry.run_id.as_str(),
                &entry.workflow_id.as_str(),
                &entry.workflow_type,
                &entry.definition_version.as_str(),
                &tenant,
                &entry.namespace,
                &entry.status.as_label(),
                &waiting_reason,
                &current_step_id,
                &failed_step_id,
                &pending_checkpoint,
                &open_checkpoint_created_at,
                &open_checkpoint_due_at,
                &shard_entity_type,
                &shard_id,
                &shard_owner,
                &graph_plan_id,
                &graph_plan_fingerprint,
                &graph_scheduler_revision,
                &graph_last_event_sequence,
                &graph_terminal_status,
                &graph_projection_json,
                &created_at,
                &updated_at,
                &completed_at,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if row.is_none() {
        return Err(stale_write(
            "run index update was older than the current projection",
        ));
    }

    upsert_open_checkpoint(client, namespace, &entry).await?;
    upsert_graph_nodes(client, namespace, &entry).await?;
    Ok(())
}

async fn upsert_graph_nodes(
    client: &Client,
    namespace: &str,
    entry: &AgentRunIndexEntry,
) -> AgentWorkflowQueryResult<()> {
    client
        .execute(
            r#"
DELETE FROM rakka_agent_workflow_graph_node_index
WHERE store_namespace = $1
  AND run_id = $2
"#,
            &[&namespace, &entry.run_id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;

    let Some(graph) = &entry.graph else {
        return Ok(());
    };

    let updated_at = millis_to_i64(entry.updated_at)?;
    for node in &graph.nodes {
        let wait_reason = node.wait_reason.map(graph_wait_reason_label);
        client
            .execute(
                r#"
INSERT INTO rakka_agent_workflow_graph_node_index (
    store_namespace,
    run_id,
    node_id,
    workflow_id,
    graph_plan_fingerprint,
    node_kind,
    node_status,
    wait_reason,
    error_code,
    updated_at_millis
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
)
"#,
                &[
                    &namespace,
                    &entry.run_id.as_str(),
                    &node.node_id.as_str(),
                    &entry.workflow_id.as_str(),
                    &graph.plan_fingerprint.as_str(),
                    &node.kind.as_label(),
                    &node.status.as_label(),
                    &wait_reason,
                    &node.error_code,
                    &updated_at,
                ],
            )
            .await
            .map_err(map_postgres_error)?;
    }
    Ok(())
}

async fn upsert_open_checkpoint(
    client: &Client,
    namespace: &str,
    entry: &AgentRunIndexEntry,
) -> AgentWorkflowQueryResult<()> {
    let Some(checkpoint_id) = &entry.pending_human_checkpoint else {
        delete_open_checkpoints(client, namespace, &entry.run_id, None).await?;
        return Ok(());
    };
    let Some(created_at) = entry.open_checkpoint_created_at else {
        delete_open_checkpoints(client, namespace, &entry.run_id, None).await?;
        return Ok(());
    };
    let tenant = entry
        .tenant
        .as_ref()
        .map(|tenant| tenant.as_str().to_string());
    let created_at = millis_to_i64(created_at)?;
    let due_at = optional_millis(entry.open_checkpoint_due_at)?;
    client
        .execute(
            r#"
INSERT INTO rakka_agent_workflow_checkpoint_index (
    store_namespace,
    checkpoint_id,
    workflow_id,
    run_id,
    tenant,
    workflow_namespace,
    status,
    created_at_millis,
    due_at_millis,
    resolved_at_millis
) VALUES (
    $1, $2, $3, $4, $5, $6, 'open', $7, $8, NULL
)
ON CONFLICT (store_namespace, checkpoint_id) DO UPDATE
SET workflow_id = EXCLUDED.workflow_id,
    run_id = EXCLUDED.run_id,
    tenant = EXCLUDED.tenant,
    workflow_namespace = EXCLUDED.workflow_namespace,
    status = EXCLUDED.status,
    created_at_millis = EXCLUDED.created_at_millis,
    due_at_millis = EXCLUDED.due_at_millis,
    resolved_at_millis = EXCLUDED.resolved_at_millis,
    updated_at = now()
"#,
            &[
                &namespace,
                &checkpoint_id.as_str(),
                &entry.workflow_id.as_str(),
                &entry.run_id.as_str(),
                &tenant,
                &entry.namespace,
                &created_at,
                &due_at,
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    delete_open_checkpoints(client, namespace, &entry.run_id, Some(checkpoint_id)).await?;
    Ok(())
}

async fn delete_open_checkpoints(
    client: &Client,
    namespace: &str,
    run_id: &AgentRunId,
    keep_checkpoint_id: Option<&HumanCheckpointId>,
) -> AgentWorkflowQueryResult<()> {
    match keep_checkpoint_id {
        Some(checkpoint_id) => {
            client
                .execute(
                    r#"
DELETE FROM rakka_agent_workflow_checkpoint_index
WHERE store_namespace = $1
  AND run_id = $2
  AND status = 'open'
  AND checkpoint_id <> $3
"#,
                    &[&namespace, &run_id.as_str(), &checkpoint_id.as_str()],
                )
                .await
        }
        None => {
            client
                .execute(
                    r#"
DELETE FROM rakka_agent_workflow_checkpoint_index
WHERE store_namespace = $1
  AND run_id = $2
  AND status = 'open'
"#,
                    &[&namespace, &run_id.as_str()],
                )
                .await
        }
    }
    .map_err(map_postgres_error)?;
    Ok(())
}

async fn upsert_timer(
    client: &Client,
    namespace: &str,
    entry: AgentTimerIndexEntry,
) -> AgentWorkflowQueryResult<()> {
    let due_at = millis_to_i64(entry.due_at)?;
    let updated_at = millis_to_i64(entry.updated_at)?;
    let row = client
        .query_opt(
            r#"
INSERT INTO rakka_agent_workflow_timer_index (
    store_namespace,
    timer_id,
    workflow_id,
    run_id,
    tenant,
    workflow_namespace,
    due_at_millis,
    status,
    updated_at_millis
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
ON CONFLICT (store_namespace, timer_id) DO UPDATE
SET workflow_id = EXCLUDED.workflow_id,
    run_id = EXCLUDED.run_id,
    tenant = EXCLUDED.tenant,
    workflow_namespace = EXCLUDED.workflow_namespace,
    due_at_millis = EXCLUDED.due_at_millis,
    status = EXCLUDED.status,
    updated_at_millis = EXCLUDED.updated_at_millis,
    revision = rakka_agent_workflow_timer_index.revision + 1,
    updated_at = now()
WHERE rakka_agent_workflow_timer_index.updated_at_millis <= EXCLUDED.updated_at_millis
RETURNING revision
"#,
            &[
                &namespace,
                &entry.timer_id.as_str(),
                &entry.workflow_id.as_str(),
                &entry.run_id.as_str(),
                &entry.tenant.as_str(),
                &entry.namespace,
                &due_at,
                &entry.status.as_label(),
                &updated_at,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if row.is_none() {
        return Err(stale_write(
            "timer index update was older than the current projection",
        ));
    }
    Ok(())
}

async fn upsert_dispatch(
    client: &Client,
    namespace: &str,
    entry: AgentDispatchIndexEntry,
) -> AgentWorkflowQueryResult<()> {
    let workflow_id = entry
        .workflow_id
        .as_ref()
        .map(|workflow_id| workflow_id.as_str().to_string());
    let graph_plan_fingerprint = entry
        .graph_plan_fingerprint
        .as_ref()
        .map(|fingerprint| fingerprint.as_str().to_string());
    let graph_node_id = entry
        .graph_node_id
        .as_ref()
        .map(|node_id| node_id.as_str().to_string());
    let graph_node_kind = entry
        .graph_node_kind
        .map(|kind| kind.as_label().to_string());
    let graph_loop_instance_id = entry.graph_loop_instance_id.clone();
    let worker_id = entry
        .worker_id
        .as_ref()
        .map(|worker_id| worker_id.as_str().to_string());
    let due_at = millis_to_i64(entry.due_at)?;
    let fencing_token = u64_to_i64(entry.fencing_token.unwrap_or(0), "dispatch fencing token")?;
    let claimed_at = optional_millis(entry.claimed_at)?;
    let lease_expires_at = optional_millis(entry.lease_expires_at)?;
    let updated_at = millis_to_i64(entry.updated_at)?;

    let row = client
        .query_opt(
            r#"
INSERT INTO rakka_agent_workflow_dispatch_index (
    store_namespace,
    dispatch_id,
    workflow_id,
    run_id,
    effect_id,
    effect_kind,
    target_class,
    graph_plan_fingerprint,
    graph_node_id,
    graph_node_kind,
    graph_loop_instance_id,
    due_at_millis,
    status,
    worker_id,
    fencing_token,
    claimed_at_millis,
    lease_expires_at_millis,
    updated_at_millis
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (store_namespace, dispatch_id) DO UPDATE
SET workflow_id = EXCLUDED.workflow_id,
    run_id = EXCLUDED.run_id,
    effect_id = EXCLUDED.effect_id,
    effect_kind = EXCLUDED.effect_kind,
    target_class = EXCLUDED.target_class,
    graph_plan_fingerprint = EXCLUDED.graph_plan_fingerprint,
    graph_node_id = EXCLUDED.graph_node_id,
    graph_node_kind = EXCLUDED.graph_node_kind,
    graph_loop_instance_id = EXCLUDED.graph_loop_instance_id,
    due_at_millis = EXCLUDED.due_at_millis,
    status = EXCLUDED.status,
    worker_id = EXCLUDED.worker_id,
    fencing_token = EXCLUDED.fencing_token,
    claimed_at_millis = EXCLUDED.claimed_at_millis,
    lease_expires_at_millis = EXCLUDED.lease_expires_at_millis,
    updated_at_millis = EXCLUDED.updated_at_millis,
    revision = rakka_agent_workflow_dispatch_index.revision + 1,
    updated_at = now()
WHERE rakka_agent_workflow_dispatch_index.fencing_token <= EXCLUDED.fencing_token
RETURNING revision
"#,
            &[
                &namespace,
                &entry.dispatch_id.as_str(),
                &workflow_id,
                &entry.run_id.as_str(),
                &entry.effect_id.as_str(),
                &entry.effect_kind.as_label(),
                &entry.target_class.as_label(),
                &graph_plan_fingerprint,
                &graph_node_id,
                &graph_node_kind,
                &graph_loop_instance_id,
                &due_at,
                &entry.status.as_label(),
                &worker_id,
                &fencing_token,
                &claimed_at,
                &lease_expires_at,
                &updated_at,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if row.is_none() {
        return Err(stale_write(
            "dispatch index update was fenced by a newer projection",
        ));
    }
    Ok(())
}

async fn upsert_runtime_event_projection(
    client: &Client,
    namespace: &str,
    projection: AgentRuntimeEventProjection,
) -> AgentWorkflowQueryResult<()> {
    let last_scheduler_revision = u64_to_i64(
        projection.last_scheduler_revision,
        "runtime event last scheduler revision",
    )?;
    let last_event_sequence = u64_to_i64(
        projection.last_event_sequence,
        "runtime event last event sequence",
    )?;
    let last_event_at = optional_millis(projection.last_event_at)?;
    let last_event_kind = projection
        .last_event_kind
        .map(|kind| kind.as_label().to_string());
    let event_count = u64_to_i64(projection.event_count, "runtime event count")?;
    let node_event_count = u64_to_i64(projection.node_event_count, "runtime node event count")?;
    let effect_event_count =
        u64_to_i64(projection.effect_event_count, "runtime effect event count")?;
    let timer_event_count = u64_to_i64(projection.timer_event_count, "runtime timer event count")?;
    let human_event_count = u64_to_i64(projection.human_event_count, "runtime human event count")?;
    let terminal_event_kind = projection
        .terminal_event_kind
        .map(|kind| kind.as_label().to_string());
    let projection_json = serde_json::to_string(&projection).map_err(map_json_error)?;

    let row = client
        .query_opt(
            r#"
INSERT INTO rakka_agent_workflow_runtime_event_projection (
    store_namespace,
    run_id,
    workflow_id,
    definition_version,
    graph_plan_fingerprint,
    last_scheduler_revision,
    last_event_sequence,
    last_event_at_millis,
    last_event_kind,
    event_count,
    node_event_count,
    effect_event_count,
    timer_event_count,
    human_event_count,
    terminal_event_kind,
    projection_json
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13, $14, $15, $16
)
ON CONFLICT (store_namespace, run_id) DO UPDATE
SET workflow_id = EXCLUDED.workflow_id,
    definition_version = EXCLUDED.definition_version,
    graph_plan_fingerprint = EXCLUDED.graph_plan_fingerprint,
    last_scheduler_revision = EXCLUDED.last_scheduler_revision,
    last_event_sequence = EXCLUDED.last_event_sequence,
    last_event_at_millis = EXCLUDED.last_event_at_millis,
    last_event_kind = EXCLUDED.last_event_kind,
    event_count = EXCLUDED.event_count,
    node_event_count = EXCLUDED.node_event_count,
    effect_event_count = EXCLUDED.effect_event_count,
    timer_event_count = EXCLUDED.timer_event_count,
    human_event_count = EXCLUDED.human_event_count,
    terminal_event_kind = EXCLUDED.terminal_event_kind,
    projection_json = EXCLUDED.projection_json,
    revision = rakka_agent_workflow_runtime_event_projection.revision + 1,
    updated_at = now()
WHERE rakka_agent_workflow_runtime_event_projection.last_event_sequence <= EXCLUDED.last_event_sequence
RETURNING revision
"#,
            &[
                &namespace,
                &projection.run_id.as_str(),
                &projection.workflow_id.as_str(),
                &projection.definition_version.as_str(),
                &projection.plan_fingerprint.as_str(),
                &last_scheduler_revision,
                &last_event_sequence,
                &last_event_at,
                &last_event_kind,
                &event_count,
                &node_event_count,
                &effect_event_count,
                &timer_event_count,
                &human_event_count,
                &terminal_event_kind,
                &projection_json,
            ],
        )
        .await
        .map_err(map_postgres_error)?;

    if row.is_none() {
        return Err(stale_write(
            "runtime event projection update was older than the current projection",
        ));
    }
    Ok(())
}

async fn runtime_event_projection(
    client: &Client,
    namespace: &str,
    run_id: AgentRunId,
) -> AgentWorkflowQueryResult<Option<AgentRuntimeEventProjection>> {
    let row = client
        .query_opt(
            r#"
SELECT projection_json
FROM rakka_agent_workflow_runtime_event_projection
WHERE store_namespace = $1
  AND run_id = $2
"#,
            &[&namespace, &run_id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;

    row.map(|row| decode_runtime_event_projection(row.get("projection_json")))
        .transpose()
}

async fn query_runs(
    client: &Client,
    namespace: &str,
    query: AgentWorkflowRunQuery,
) -> AgentWorkflowQueryResult<Vec<AgentRunIndexEntry>> {
    validate_limit(query.limit)?;
    let tenant = query
        .tenant
        .as_ref()
        .map(|tenant| tenant.as_str().to_string());
    let definition_version = query
        .definition_version
        .as_ref()
        .map(|version| version.as_str().to_string());
    let statuses = optional_status_labels(&query.statuses);
    let updated_from = optional_millis(query.updated_at_from)?;
    let updated_to = optional_millis(query.updated_at_to)?;
    let waiting_reasons = optional_waiting_reason_labels(&query.waiting_reasons);
    let checkpoint_created = optional_millis(query.checkpoint_created_at_or_before)?;
    let failed_step_id = query
        .failed_step_id
        .as_ref()
        .map(|step_id| step_id.as_str().to_string());
    let due_timer_at = optional_millis(query.due_timer_at_or_before)?;
    let stuck_dispatcher_at = optional_millis(query.stuck_dispatcher_at_or_before)?;
    let graph_plan_fingerprint = query
        .graph_plan_fingerprint
        .as_ref()
        .map(|fingerprint| fingerprint.as_str().to_string());
    let graph_node_statuses = optional_graph_node_status_labels(&query.graph_node_statuses);
    let graph_node_kinds = optional_graph_node_kind_labels(&query.graph_node_kinds);
    let graph_wait_reasons = optional_graph_wait_reason_labels(&query.graph_wait_reasons);
    let limit = limit_to_i64(query.limit)?;

    let params: &[&(dyn ToSql + Sync)] = &[
        &namespace,
        &tenant,
        &query.namespace,
        &query.workflow_type,
        &definition_version,
        &statuses,
        &updated_from,
        &updated_to,
        &waiting_reasons,
        &checkpoint_created,
        &failed_step_id,
        &due_timer_at,
        &stuck_dispatcher_at,
        &query.shard_owner_node_id,
        &query.shard_id,
        &graph_plan_fingerprint,
        &graph_node_statuses,
        &graph_node_kinds,
        &graph_wait_reasons,
        &query.graph_error_code,
        &limit,
    ];
    let rows = client
        .query(
            r#"
SELECT *
FROM rakka_agent_workflow_run_index AS run
WHERE run.store_namespace = $1
  AND ($2::text IS NULL OR run.tenant = $2)
  AND ($3::text IS NULL OR run.workflow_namespace = $3)
  AND ($4::text IS NULL OR run.workflow_type = $4)
  AND ($5::text IS NULL OR run.definition_version = $5)
  AND ($6::text[] IS NULL OR run.status = ANY($6))
  AND ($7::bigint IS NULL OR run.updated_at_millis >= $7)
  AND ($8::bigint IS NULL OR run.updated_at_millis <= $8)
  AND ($9::text[] IS NULL OR run.waiting_reason = ANY($9))
  AND ($10::bigint IS NULL OR run.open_checkpoint_created_at_millis <= $10)
  AND ($11::text IS NULL OR run.failed_step_id = $11)
  AND (
      $12::bigint IS NULL
      OR EXISTS (
          SELECT 1
          FROM rakka_agent_workflow_timer_index AS timer
          WHERE timer.store_namespace = run.store_namespace
            AND timer.run_id = run.run_id
            AND timer.status = 'pending'
            AND timer.due_at_millis <= $12
      )
  )
  AND (
      $13::bigint IS NULL
      OR EXISTS (
          SELECT 1
          FROM rakka_agent_workflow_dispatch_index AS dispatch
          WHERE dispatch.store_namespace = run.store_namespace
            AND dispatch.run_id = run.run_id
            AND dispatch.status = 'claimed'
            AND dispatch.lease_expires_at_millis <= $13
      )
  )
  AND ($14::text IS NULL OR run.shard_owner_node_id = $14)
  AND ($15::text IS NULL OR run.shard_id = $15)
  AND ($16::text IS NULL OR run.graph_plan_fingerprint = $16)
  AND (
      ($17::text[] IS NULL AND $18::text[] IS NULL AND $19::text[] IS NULL AND $20::text IS NULL)
      OR EXISTS (
          SELECT 1
          FROM rakka_agent_workflow_graph_node_index AS node
          WHERE node.store_namespace = run.store_namespace
            AND node.run_id = run.run_id
            AND ($17::text[] IS NULL OR node.node_status = ANY($17))
            AND ($18::text[] IS NULL OR node.node_kind = ANY($18))
            AND ($19::text[] IS NULL OR node.wait_reason = ANY($19))
            AND ($20::text IS NULL OR node.error_code = $20)
      )
  )
ORDER BY run.updated_at_millis, run.run_id
LIMIT $21::bigint
"#,
            params,
        )
        .await
        .map_err(map_postgres_error)?;
    rows.into_iter().map(decode_run_row).collect()
}

async fn query_timers(
    client: &Client,
    namespace: &str,
    query: AgentTimerQuery,
) -> AgentWorkflowQueryResult<Vec<AgentTimerIndexEntry>> {
    validate_limit(query.limit)?;
    let run_id = query
        .run_id
        .as_ref()
        .map(|run_id| run_id.as_str().to_string());
    let workflow_id = query
        .workflow_id
        .as_ref()
        .map(|workflow_id| workflow_id.as_str().to_string());
    let tenant = query
        .tenant
        .as_ref()
        .map(|tenant| tenant.as_str().to_string());
    let statuses = optional_timer_status_labels(&query.statuses);
    let due_at = optional_millis(query.due_at_or_before)?;
    let limit = limit_to_i64(query.limit)?;

    let rows = client
        .query(
            r#"
SELECT *
FROM rakka_agent_workflow_timer_index
WHERE store_namespace = $1
  AND ($2::text IS NULL OR run_id = $2)
  AND ($3::text IS NULL OR workflow_id = $3)
  AND ($4::text IS NULL OR tenant = $4)
  AND ($5::text IS NULL OR workflow_namespace = $5)
  AND ($6::text[] IS NULL OR status = ANY($6))
  AND ($7::bigint IS NULL OR due_at_millis <= $7)
ORDER BY due_at_millis, timer_id
LIMIT $8::bigint
"#,
            &[
                &namespace,
                &run_id,
                &workflow_id,
                &tenant,
                &query.namespace,
                &statuses,
                &due_at,
                &limit,
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    rows.into_iter().map(decode_timer_row).collect()
}

async fn query_dispatches(
    client: &Client,
    namespace: &str,
    query: AgentDispatchQuery,
) -> AgentWorkflowQueryResult<Vec<AgentDispatchIndexEntry>> {
    validate_limit(query.limit)?;
    let run_id = query
        .run_id
        .as_ref()
        .map(|run_id| run_id.as_str().to_string());
    let workflow_id = query
        .workflow_id
        .as_ref()
        .map(|workflow_id| workflow_id.as_str().to_string());
    let statuses = optional_dispatch_status_labels(&query.statuses);
    let target_class = query.target_class.map(target_class_label);
    let graph_plan_fingerprint = query
        .graph_plan_fingerprint
        .as_ref()
        .map(|fingerprint| fingerprint.as_str().to_string());
    let graph_node_id = query
        .graph_node_id
        .as_ref()
        .map(|node_id| node_id.as_str().to_string());
    let graph_node_kind = query
        .graph_node_kind
        .map(|kind| kind.as_label().to_string());
    let due_at = optional_millis(query.due_at_or_before)?;
    let stuck_at = optional_millis(query.stuck_at_or_before)?;
    let limit = limit_to_i64(query.limit)?;

    let rows = client
        .query(
            r#"
SELECT *
FROM rakka_agent_workflow_dispatch_index
WHERE store_namespace = $1
  AND ($2::text IS NULL OR run_id = $2)
  AND ($3::text IS NULL OR workflow_id = $3)
  AND ($4::text[] IS NULL OR status = ANY($4))
  AND ($5::text IS NULL OR target_class = $5)
  AND ($6::text IS NULL OR graph_plan_fingerprint = $6)
  AND ($7::text IS NULL OR graph_node_id = $7)
  AND ($8::text IS NULL OR graph_node_kind = $8)
  AND ($9::bigint IS NULL OR due_at_millis <= $9)
  AND ($10::bigint IS NULL OR (status = 'claimed' AND lease_expires_at_millis <= $10))
ORDER BY due_at_millis, dispatch_id
LIMIT $11::bigint
"#,
            &[
                &namespace,
                &run_id,
                &workflow_id,
                &statuses,
                &target_class,
                &graph_plan_fingerprint,
                &graph_node_id,
                &graph_node_kind,
                &due_at,
                &stuck_at,
                &limit,
            ],
        )
        .await
        .map_err(map_postgres_error)?;
    rows.into_iter().map(decode_dispatch_row).collect()
}

fn decode_run_row(row: Row) -> AgentWorkflowQueryResult<AgentRunIndexEntry> {
    let tenant = row
        .get::<_, Option<String>>("tenant")
        .map(AgentTenantId::new);
    let waiting_reason = row
        .get::<_, Option<String>>("waiting_reason")
        .map(|value| parse_waiting_reason(&value))
        .transpose()?;
    let shard_ownership = match (
        row.get::<_, Option<String>>("shard_entity_type"),
        row.get::<_, Option<String>>("shard_id"),
        row.get::<_, Option<String>>("shard_owner_node_id"),
    ) {
        (Some(entity_type), Some(shard_id), Some(owner_node_id)) => Some(
            AgentWorkflowShardOwnership::new(entity_type, shard_id, owner_node_id),
        ),
        _ => None,
    };
    Ok(AgentRunIndexEntry {
        run_id: AgentRunId::new(row.get::<_, String>("run_id")),
        workflow_id: AgentWorkflowId::new(row.get::<_, String>("workflow_id")),
        workflow_type: row.get("workflow_type"),
        definition_version: WorkflowDefinitionVersion::new(
            row.get::<_, String>("definition_version"),
        ),
        tenant,
        namespace: row.get("workflow_namespace"),
        status: parse_run_status(&row.get::<_, String>("status"))?,
        waiting_reason,
        current_step_id: row
            .get::<_, Option<String>>("current_step_id")
            .map(AgentStepId::new),
        failed_step_id: row
            .get::<_, Option<String>>("failed_step_id")
            .map(AgentStepId::new),
        pending_human_checkpoint: row
            .get::<_, Option<String>>("pending_human_checkpoint")
            .map(HumanCheckpointId::new),
        open_checkpoint_created_at: decode_optional_millis(
            row.get("open_checkpoint_created_at_millis"),
        )?,
        open_checkpoint_due_at: decode_optional_millis(row.get("open_checkpoint_due_at_millis"))?,
        shard_ownership,
        graph: decode_graph_projection(row.get("graph_projection_json"))?,
        created_at: decode_millis(row.get("created_at_millis"))?,
        updated_at: decode_millis(row.get("updated_at_millis"))?,
        completed_at: decode_optional_millis(row.get("completed_at_millis"))?,
    })
}

fn decode_timer_row(row: Row) -> AgentWorkflowQueryResult<AgentTimerIndexEntry> {
    Ok(AgentTimerIndexEntry {
        timer_id: AgentTimerId::new(row.get::<_, String>("timer_id")),
        workflow_id: AgentWorkflowId::new(row.get::<_, String>("workflow_id")),
        run_id: AgentRunId::new(row.get::<_, String>("run_id")),
        tenant: AgentTenantId::new(row.get::<_, String>("tenant")),
        namespace: row.get("workflow_namespace"),
        due_at: decode_millis(row.get("due_at_millis"))?,
        status: parse_timer_status(&row.get::<_, String>("status"))?,
        updated_at: decode_millis(row.get("updated_at_millis"))?,
    })
}

fn decode_dispatch_row(row: Row) -> AgentWorkflowQueryResult<AgentDispatchIndexEntry> {
    let fencing_token = row.get::<_, i64>("fencing_token");
    Ok(AgentDispatchIndexEntry {
        dispatch_id: AgentDispatchId::new(row.get::<_, String>("dispatch_id")),
        workflow_id: row
            .get::<_, Option<String>>("workflow_id")
            .map(AgentWorkflowId::new),
        run_id: AgentRunId::new(row.get::<_, String>("run_id")),
        effect_id: AgentEffectId::new(row.get::<_, String>("effect_id")),
        effect_kind: parse_effect_kind(&row.get::<_, String>("effect_kind"))?,
        target_class: parse_target_class(&row.get::<_, String>("target_class"))?,
        graph_plan_fingerprint: row
            .get::<_, Option<String>>("graph_plan_fingerprint")
            .map(AgentCompiledPlanFingerprint::new),
        graph_node_id: row
            .get::<_, Option<String>>("graph_node_id")
            .map(AgentCompiledNodeId::new),
        graph_node_kind: row
            .get::<_, Option<String>>("graph_node_kind")
            .map(|value| parse_compiled_node_kind(&value))
            .transpose()?,
        graph_loop_instance_id: row.get("graph_loop_instance_id"),
        due_at: decode_millis(row.get("due_at_millis"))?,
        status: parse_dispatch_status(&row.get::<_, String>("status"))?,
        worker_id: row
            .get::<_, Option<String>>("worker_id")
            .map(AgentDispatcherWorkerId::new),
        fencing_token: Some(i64_to_u64(fencing_token, "fencing_token")?),
        claimed_at: decode_optional_millis(row.get("claimed_at_millis"))?,
        lease_expires_at: decode_optional_millis(row.get("lease_expires_at_millis"))?,
        updated_at: decode_millis(row.get("updated_at_millis"))?,
    })
}

fn optional_status_labels(statuses: &[AgentRunStatus]) -> Option<Vec<String>> {
    (!statuses.is_empty()).then(|| {
        statuses
            .iter()
            .map(|status| status.as_label().to_string())
            .collect()
    })
}

fn optional_timer_status_labels(statuses: &[AgentTimerStatus]) -> Option<Vec<String>> {
    (!statuses.is_empty()).then(|| {
        statuses
            .iter()
            .map(|status| status.as_label().to_string())
            .collect()
    })
}

fn optional_dispatch_status_labels(statuses: &[AgentDispatchStatus]) -> Option<Vec<String>> {
    (!statuses.is_empty()).then(|| {
        statuses
            .iter()
            .map(|status| status.as_label().to_string())
            .collect()
    })
}

fn optional_graph_node_status_labels(statuses: &[AgentGraphNodeStatus]) -> Option<Vec<String>> {
    (!statuses.is_empty()).then(|| {
        statuses
            .iter()
            .map(|status| status.as_label().to_string())
            .collect()
    })
}

fn optional_graph_node_kind_labels(kinds: &[AgentCompiledNodeKind]) -> Option<Vec<String>> {
    (!kinds.is_empty()).then(|| {
        kinds
            .iter()
            .map(|kind| kind.as_label().to_string())
            .collect()
    })
}

fn optional_graph_wait_reason_labels(reasons: &[AgentGraphWaitReason]) -> Option<Vec<String>> {
    (!reasons.is_empty()).then(|| {
        reasons
            .iter()
            .map(|reason| graph_wait_reason_label(*reason))
            .collect()
    })
}

fn optional_waiting_reason_labels(reasons: &[AgentRunQueryWaitingReason]) -> Option<Vec<String>> {
    (!reasons.is_empty()).then(|| {
        reasons
            .iter()
            .map(|reason| waiting_reason_label(*reason))
            .collect()
    })
}

fn waiting_reason_label(reason: AgentRunQueryWaitingReason) -> String {
    reason.as_label().to_string()
}

fn graph_wait_reason_label(reason: AgentGraphWaitReason) -> String {
    reason.as_label().to_string()
}

fn target_class_label(target_class: AgentDispatchTargetClass) -> String {
    target_class.as_label().to_string()
}

fn parse_run_status(value: &str) -> AgentWorkflowQueryResult<AgentRunStatus> {
    match value {
        "accepted" => Ok(AgentRunStatus::Accepted),
        "running" => Ok(AgentRunStatus::Running),
        "waiting-for-timer" => Ok(AgentRunStatus::WaitingForTimer),
        "waiting-for-human" => Ok(AgentRunStatus::WaitingForHuman),
        "waiting-for-effect" => Ok(AgentRunStatus::WaitingForEffect),
        "cancelling" => Ok(AgentRunStatus::Cancelling),
        "completed" => Ok(AgentRunStatus::Completed),
        "failed" => Ok(AgentRunStatus::Failed),
        "compensating" => Ok(AgentRunStatus::Compensating),
        "cancelled" => Ok(AgentRunStatus::Cancelled),
        _ => Err(invalid_label("run.status", value)),
    }
}

fn parse_waiting_reason(value: &str) -> AgentWorkflowQueryResult<AgentRunQueryWaitingReason> {
    match value {
        "timer" => Ok(AgentRunQueryWaitingReason::Timer),
        "human" => Ok(AgentRunQueryWaitingReason::Human),
        "effect" => Ok(AgentRunQueryWaitingReason::Effect),
        _ => Err(invalid_label("run.waiting_reason", value)),
    }
}

fn parse_timer_status(value: &str) -> AgentWorkflowQueryResult<AgentTimerStatus> {
    match value {
        "pending" => Ok(AgentTimerStatus::Pending),
        "fired" => Ok(AgentTimerStatus::Fired),
        "cancelled" => Ok(AgentTimerStatus::Cancelled),
        _ => Err(invalid_label("timer.status", value)),
    }
}

fn parse_dispatch_status(value: &str) -> AgentWorkflowQueryResult<AgentDispatchStatus> {
    match value {
        "pending" => Ok(AgentDispatchStatus::Pending),
        "claimed" => Ok(AgentDispatchStatus::Claimed),
        "completed" => Ok(AgentDispatchStatus::Completed),
        "retry-scheduled" => Ok(AgentDispatchStatus::RetryScheduled),
        "exhausted" => Ok(AgentDispatchStatus::Exhausted),
        "cancelled" => Ok(AgentDispatchStatus::Cancelled),
        _ => Err(invalid_label("dispatch.status", value)),
    }
}

fn parse_target_class(value: &str) -> AgentWorkflowQueryResult<AgentDispatchTargetClass> {
    match value {
        "model" => Ok(AgentDispatchTargetClass::Model),
        "tool" => Ok(AgentDispatchTargetClass::Tool),
        "process" => Ok(AgentDispatchTargetClass::Process),
        "a2a-peer" => Ok(AgentDispatchTargetClass::A2aPeer),
        "http" => Ok(AgentDispatchTargetClass::Http),
        "grpc" => Ok(AgentDispatchTargetClass::Grpc),
        "webhook" => Ok(AgentDispatchTargetClass::Webhook),
        "notification" => Ok(AgentDispatchTargetClass::Notification),
        "push-notification" => Ok(AgentDispatchTargetClass::PushNotification),
        "human" => Ok(AgentDispatchTargetClass::Human),
        "stream" => Ok(AgentDispatchTargetClass::Stream),
        "artifact" => Ok(AgentDispatchTargetClass::Artifact),
        "child-workflow" => Ok(AgentDispatchTargetClass::ChildWorkflow),
        "audit" => Ok(AgentDispatchTargetClass::Audit),
        "other" => Ok(AgentDispatchTargetClass::Other),
        _ => Err(invalid_label("dispatch.target_class", value)),
    }
}

fn parse_effect_kind(value: &str) -> AgentWorkflowQueryResult<AgentEffectKind> {
    match value {
        "model-call" => Ok(AgentEffectKind::ModelCall),
        "tool-call" => Ok(AgentEffectKind::ToolCall),
        "process-call" => Ok(AgentEffectKind::ProcessCall),
        "http-call" => Ok(AgentEffectKind::HttpCall),
        "grpc-call" => Ok(AgentEffectKind::GrpcCall),
        "stream-publish" => Ok(AgentEffectKind::StreamPublish),
        "artifact-write" => Ok(AgentEffectKind::ArtifactWrite),
        "human-approval-request" => Ok(AgentEffectKind::HumanApprovalRequest),
        "notification" => Ok(AgentEffectKind::Notification),
        "child-workflow-command" => Ok(AgentEffectKind::ChildWorkflowCommand),
        "audit-event" => Ok(AgentEffectKind::AuditEvent),
        _ => Err(invalid_label("dispatch.effect_kind", value)),
    }
}

fn parse_compiled_node_kind(value: &str) -> AgentWorkflowQueryResult<AgentCompiledNodeKind> {
    AgentCompiledNodeKind::from_label(value)
        .ok_or_else(|| invalid_label("dispatch.graph_node_kind", value))
}

fn decode_graph_projection(
    value: Option<String>,
) -> AgentWorkflowQueryResult<Option<AgentGraphRunProjection>> {
    value
        .map(|value| serde_json::from_str(&value).map_err(map_json_error))
        .transpose()
}

fn decode_runtime_event_projection(
    value: String,
) -> AgentWorkflowQueryResult<AgentRuntimeEventProjection> {
    serde_json::from_str(&value).map_err(map_json_error)
}

fn validate_limit(limit: Option<usize>) -> AgentWorkflowQueryResult<()> {
    if matches!(limit, Some(0)) {
        return Err(AgentWorkflowQueryError::InvalidQuery {
            field: "limit",
            reason: "must be greater than zero",
        });
    }
    Ok(())
}

fn limit_to_i64(limit: Option<usize>) -> AgentWorkflowQueryResult<i64> {
    match limit {
        Some(limit) => {
            i64::try_from(limit).map_err(|_error| AgentWorkflowQueryError::InvalidQuery {
                field: "limit",
                reason: "must fit in a PostgreSQL bigint",
            })
        }
        None => Ok(i64::MAX),
    }
}

fn optional_millis(value: Option<AgentTimestampMillis>) -> AgentWorkflowQueryResult<Option<i64>> {
    value.map(millis_to_i64).transpose()
}

fn millis_to_i64(value: AgentTimestampMillis) -> AgentWorkflowQueryResult<i64> {
    u64_to_i64(value.as_millis(), "timestamp")
}

fn u64_to_i64(value: u64, field: &'static str) -> AgentWorkflowQueryResult<i64> {
    i64::try_from(value).map_err(|_error| AgentWorkflowQueryError::InvalidQuery {
        field,
        reason: "must fit in a PostgreSQL bigint",
    })
}

fn decode_millis(value: i64) -> AgentWorkflowQueryResult<AgentTimestampMillis> {
    i64_to_u64(value, "timestamp").map(AgentTimestampMillis::new)
}

fn decode_optional_millis(
    value: Option<i64>,
) -> AgentWorkflowQueryResult<Option<AgentTimestampMillis>> {
    value.map(decode_millis).transpose()
}

fn i64_to_u64(value: i64, field: &'static str) -> AgentWorkflowQueryResult<u64> {
    u64::try_from(value).map_err(|_error| AgentWorkflowQueryError::Store {
        message: format!("PostgreSQL field {field} was negative"),
    })
}

async fn acquire_migration_lock(client: &Client) -> Result<(), tokio_postgres::Error> {
    client
        .query_one(
            "SELECT pg_advisory_lock($1)",
            &[&AGENT_WORKFLOW_QUERY_MIGRATION_LOCK_ID],
        )
        .await?;
    Ok(())
}

async fn release_migration_lock(client: &Client) -> Result<(), tokio_postgres::Error> {
    client
        .query_one(
            "SELECT pg_advisory_unlock($1)",
            &[&AGENT_WORKFLOW_QUERY_MIGRATION_LOCK_ID],
        )
        .await?;
    Ok(())
}

fn stale_write(message: impl Into<String>) -> AgentWorkflowQueryError {
    AgentWorkflowQueryError::Store {
        message: message.into(),
    }
}

fn invalid_label(field: &'static str, value: &str) -> AgentWorkflowQueryError {
    AgentWorkflowQueryError::Store {
        message: format!("invalid {field} label in PostgreSQL index: {value}"),
    }
}

fn map_postgres_error(error: tokio_postgres::Error) -> AgentWorkflowQueryError {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    AgentWorkflowQueryError::Store { message }
}

fn map_json_error(error: serde_json::Error) -> AgentWorkflowQueryError {
    AgentWorkflowQueryError::Store {
        message: format!("invalid PostgreSQL projection JSON: {error}"),
    }
}
