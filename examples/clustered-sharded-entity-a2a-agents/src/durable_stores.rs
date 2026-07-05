//! Durable-store selection for the clustered A2A example.
//!
//! Tests use in-memory stores. Runtime defaults to file-backed stores under a
//! shared local directory so another process can lazily recover a run after
//! shard ownership moves. Production deployments should use PostgreSQL-backed
//! stores instead of this intentionally small example file store.

use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rakka::agent_workflow::substrate::WorkflowState;
use rakka::agent_workflow::AgentRunState;
use rakka::persistence::{
    DurableError, DurableResult, DurableState, DurableStateStore, InMemoryDurableStateStore,
    PersistenceId, Revision, StateRecord, StoreFuture,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::config::ExampleConfig;
use crate::support::{current_timestamp_millis, hex_encode};

/// Durable store for agent run state.
pub type RunStore = ExampleDurableStateStore<AgentRunState>;

/// Durable store for workflow inbox/outbox state.
pub type WorkflowStore = ExampleDurableStateStore<WorkflowState>;

/// Builds the runtime stores from environment-backed configuration.
#[must_use]
pub fn build_stores(config: &ExampleConfig) -> (RunStore, WorkflowStore) {
    (
        RunStore::file(config.state_dir.join("runs")),
        WorkflowStore::file(config.state_dir.join("workflow")),
    )
}

/// Builds isolated in-memory stores for unit tests.
#[must_use]
#[cfg(test)]
pub fn build_in_memory_stores() -> (RunStore, WorkflowStore) {
    (RunStore::memory(), WorkflowStore::memory())
}

/// Example durable store implementation.
#[derive(Debug)]
pub enum ExampleDurableStateStore<S>
where
    S: DurableState,
{
    /// Process-local in-memory store.
    Memory(InMemoryDurableStateStore<S>),
    /// File-backed store for local multi-node recovery.
    File(FileDurableStateStore<S>),
}

impl<S> ExampleDurableStateStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    /// Creates an in-memory store.
    #[must_use]
    #[cfg(test)]
    pub fn memory() -> Self {
        Self::Memory(InMemoryDurableStateStore::new())
    }

    /// Creates a file-backed store rooted at `root`.
    #[must_use]
    pub fn file(root: impl Into<PathBuf>) -> Self {
        Self::File(FileDurableStateStore::new(root))
    }
}

impl<S> Clone for ExampleDurableStateStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        match self {
            Self::Memory(store) => Self::Memory(store.clone()),
            Self::File(store) => Self::File(store.clone()),
        }
    }
}

impl<S> DurableStateStore<S> for ExampleDurableStateStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    fn backend_name(&self) -> &'static str {
        match self {
            Self::Memory(store) => store.backend_name(),
            Self::File(store) => store.backend_name(),
        }
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        match self {
            Self::Memory(store) => store.load(persistence_id),
            Self::File(store) => store.load(persistence_id),
        }
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        match self {
            Self::Memory(store) => store.compare_and_set(persistence_id, expected_revision, state),
            Self::File(store) => store.compare_and_set(persistence_id, expected_revision, state),
        }
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        match self {
            Self::Memory(store) => store.delete(persistence_id, expected_revision),
            Self::File(store) => store.delete(persistence_id, expected_revision),
        }
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        match self {
            Self::Memory(store) => store.persistence_ids(),
            Self::File(store) => store.persistence_ids(),
        }
    }
}

/// Small JSON file state store for local multi-process demos.
#[derive(Debug)]
pub struct FileDurableStateStore<S>
where
    S: DurableState,
{
    root: Arc<PathBuf>,
    _state: PhantomData<fn() -> S>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStateRecord<S> {
    persistence_id: PersistenceId,
    revision: u64,
    state: S,
}

impl<S> FileDurableStateStore<S>
where
    S: DurableState,
{
    /// Creates a file-backed store rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
            _state: PhantomData,
        }
    }

    fn record_path(&self, persistence_id: &PersistenceId) -> PathBuf {
        self.root
            .join(format!("{}.json", hex_encode(persistence_id.as_str())))
    }
}

impl<S> FileDurableStateStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    fn load_record(&self, persistence_id: &PersistenceId) -> DurableResult<Option<StateRecord<S>>> {
        let path = self.record_path(persistence_id);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(file_store_error(error)),
        };
        let stored: StoredStateRecord<S> = serde_json::from_slice(&bytes)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        if stored.persistence_id != *persistence_id {
            return Err(DurableError::codec(format!(
                "record path for {persistence_id} contained {}",
                stored.persistence_id
            )));
        }
        Ok(Some(StateRecord::new(
            stored.state,
            Revision::new(stored.revision),
        )))
    }

    fn write_record(
        &self,
        persistence_id: &PersistenceId,
        record: &StateRecord<S>,
    ) -> DurableResult<()> {
        std::fs::create_dir_all(self.root.as_ref()).map_err(file_store_error)?;
        let path = self.record_path(persistence_id);
        let temp = path.with_extension(format!("json.tmp.{}", current_timestamp_millis()));
        let stored = StoredStateRecord {
            persistence_id: persistence_id.clone(),
            revision: record.revision.get(),
            state: record.state.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        std::fs::write(&temp, bytes).map_err(file_store_error)?;
        std::fs::rename(&temp, &path).map_err(file_store_error)?;
        Ok(())
    }

    fn list_persistence_ids(&self) -> DurableResult<Vec<PersistenceId>> {
        let entries = match std::fs::read_dir(self.root.as_ref()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(file_store_error(error)),
        };
        let mut ids = Vec::new();
        for entry in entries {
            let path = entry.map_err(file_store_error)?.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let Some(id) = self.persistence_id_from_file(&path)? else {
                continue;
            };
            ids.push(id);
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    fn persistence_id_from_file(&self, path: &Path) -> DurableResult<Option<PersistenceId>> {
        let bytes = std::fs::read(path).map_err(file_store_error)?;
        let stored: StoredStateRecord<serde_json::Value> = serde_json::from_slice(&bytes)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        Ok(Some(stored.persistence_id))
    }
}

impl<S> Clone for FileDurableStateStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            _state: PhantomData,
        }
    }
}

impl<S> DurableStateStore<S> for FileDurableStateStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    fn backend_name(&self) -> &'static str {
        "example-file"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        Box::pin(async move { self.load_record(persistence_id) })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        Box::pin(async move {
            let actual = self
                .load_record(persistence_id)?
                .map_or(Revision::INITIAL, |record| record.revision);
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            let record = StateRecord::new(state, expected_revision.next());
            self.write_record(persistence_id, &record)?;
            Ok(record)
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        Box::pin(async move {
            let actual = self
                .load_record(persistence_id)?
                .map_or(Revision::INITIAL, |record| record.revision);
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            match std::fs::remove_file(self.record_path(persistence_id)) {
                Ok(()) => Ok(Revision::INITIAL),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Revision::INITIAL),
                Err(error) => Err(file_store_error(error)),
            }
        })
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        Box::pin(async move { self.list_persistence_ids() })
    }
}

fn file_store_error(error: impl ToString) -> DurableError {
    DurableError::store("example-file", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka::agent_workflow::{
        AgentRunId, AgentRunStatus, AgentStatePayload, AgentTimestampMillis, AgentWorkflowId,
        StateSchemaVersion, WorkflowDefinitionVersion,
    };

    #[tokio::test]
    async fn file_store_round_trips_state_and_ids() {
        let root = std::env::temp_dir().join(format!(
            "rakka-a2a-file-store-test-{}",
            current_timestamp_millis()
        ));
        let store = FileDurableStateStore::new(&root);
        let persistence_id = PersistenceId::new("agent-run:run-1");
        let state = AgentRunState {
            run_id: AgentRunId::new("run-1"),
            workflow_id: AgentWorkflowId::new("workflow"),
            tenant: None,
            definition_version: WorkflowDefinitionVersion::new("v1"),
            state_schema_version: StateSchemaVersion::new(1),
            graph_state: None,
            status: AgentRunStatus::Accepted,
            current_step_id: None,
            current_attempt: 0,
            inputs_ref: None,
            state_payload: AgentStatePayload::Empty,
            checkpoints: Vec::new(),
            pending_effects: Vec::new(),
            pending_human_checkpoint: None,
            cancellation: None,
            created_at: AgentTimestampMillis::new(1),
            updated_at: AgentTimestampMillis::new(1),
            completed_at: None,
        };

        let written = store
            .compare_and_set(&persistence_id, Revision::INITIAL, state.clone())
            .await
            .unwrap();
        assert_eq!(written.revision, Revision::new(1));
        assert_eq!(
            store.load(&persistence_id).await.unwrap().unwrap().state,
            state
        );
        assert_eq!(store.persistence_ids().await.unwrap(), vec![persistence_id]);

        let _ = std::fs::remove_dir_all(root);
    }
}
