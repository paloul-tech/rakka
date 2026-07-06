//! Durable-store selection for the clustered A2A example.
//!
//! Tests use in-memory stores. Runtime defaults to file-backed stores under a
//! shared local directory so another process can lazily recover a run after
//! shard ownership moves. Production deployments should use PostgreSQL-backed
//! stores instead of this intentionally small example file store.

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka::agent_workflow::substrate::WorkflowState;
use rakka::agent_workflow::AgentRunState;
#[cfg(feature = "postgres")]
use rakka::persistence::StateCodec;
use rakka::persistence::{
    DurableError, DurableResult, DurableState, DurableStateStore, InMemoryDurableStateStore,
    PersistenceId, Revision, StateRecord, StoreFuture,
};
#[cfg(feature = "postgres")]
use rakka_persistence_postgres::PostgresDurableStateStore;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::config::{ExampleConfig, PersistenceKind};
use crate::push_config::A2APushConfigState;
use crate::support::{example_error, hex_decode, hex_encode, ExampleResult};

/// Durable store for agent run state.
pub type RunStore = ExampleDurableStateStore<AgentRunState>;

/// Durable store for workflow inbox/outbox state.
pub type WorkflowStore = ExampleDurableStateStore<WorkflowState>;

/// Durable store for A2A push notification configs.
pub type PushConfigStore = ExampleDurableStateStore<A2APushConfigState>;

/// Builds the runtime stores from environment-backed configuration.
pub async fn build_stores(
    config: &ExampleConfig,
) -> ExampleResult<(RunStore, WorkflowStore, PushConfigStore)> {
    match config.persistence {
        PersistenceKind::File => Ok((
            RunStore::file(config.state_dir.join("runs")),
            WorkflowStore::file(config.state_dir.join("workflow")),
            PushConfigStore::file(config.state_dir.join("push-configs")),
        )),
        PersistenceKind::Postgres => build_postgres_stores(config).await,
    }
}

/// Builds isolated in-memory stores for unit tests.
#[must_use]
#[cfg(test)]
pub fn build_in_memory_stores() -> (RunStore, WorkflowStore, PushConfigStore) {
    (
        RunStore::memory(),
        WorkflowStore::memory(),
        PushConfigStore::memory(),
    )
}

/// Example durable store implementation.
pub enum ExampleDurableStateStore<S>
where
    S: DurableState,
{
    /// Process-local in-memory store.
    Memory(InMemoryDurableStateStore<S>),
    /// File-backed store for local multi-node recovery.
    File(FileDurableStateStore<S>),
    /// Shared PostgreSQL store for multi-pod recovery.
    #[cfg(feature = "postgres")]
    Postgres(PostgresDurableStateStore<JsonStateCodec<S>>),
}

impl<S> std::fmt::Debug for ExampleDurableStateStore<S>
where
    S: DurableState,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory(_) => f.write_str("ExampleDurableStateStore::Memory"),
            Self::File(_) => f.write_str("ExampleDurableStateStore::File"),
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => f.write_str("ExampleDurableStateStore::Postgres"),
        }
    }
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
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => Self::Postgres(store.clone()),
        }
    }
}

impl<S> DurableStateStore<S> for ExampleDurableStateStore<S>
where
    S: DurableState + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn backend_name(&self) -> &'static str {
        match self {
            Self::Memory(store) => store.backend_name(),
            Self::File(store) => store.backend_name(),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.backend_name(),
        }
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        match self {
            Self::Memory(store) => store.load(persistence_id),
            Self::File(store) => store.load(persistence_id),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.load(persistence_id),
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
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => {
                store.compare_and_set(persistence_id, expected_revision, state)
            }
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
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.delete(persistence_id, expected_revision),
        }
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        match self {
            Self::Memory(store) => store.persistence_ids(),
            Self::File(store) => store.persistence_ids(),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.persistence_ids(),
        }
    }
}

/// JSON `StateCodec` used by the PostgreSQL store.
#[cfg(feature = "postgres")]
pub struct JsonStateCodec<S> {
    _state: PhantomData<fn() -> S>,
}

#[cfg(feature = "postgres")]
impl<S> JsonStateCodec<S> {
    fn new() -> Self {
        Self {
            _state: PhantomData,
        }
    }
}

#[cfg(feature = "postgres")]
impl<S> Clone for JsonStateCodec<S> {
    fn clone(&self) -> Self {
        Self {
            _state: PhantomData,
        }
    }
}

#[cfg(feature = "postgres")]
impl<S> StateCodec<S> for JsonStateCodec<S>
where
    S: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn encode(&self, state: &S) -> DurableResult<Vec<u8>> {
        serde_json::to_vec(state).map_err(|error| DurableError::codec(error.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> DurableResult<S> {
        serde_json::from_slice(bytes).map_err(|error| DurableError::codec(error.to_string()))
    }
}

#[cfg(feature = "postgres")]
async fn build_postgres_stores(
    config: &ExampleConfig,
) -> ExampleResult<(RunStore, WorkflowStore, PushConfigStore)> {
    let dsn = config.postgres_dsn.as_deref().ok_or_else(|| {
        example_error("RAKKA_POSTGRES_DSN is required when RAKKA_PERSISTENCE=postgres")
    })?;
    let run = connect_postgres_store::<AgentRunState>(dsn).await?;
    let workflow = connect_postgres_store::<WorkflowState>(dsn).await?;
    let push = connect_postgres_store::<A2APushConfigState>(dsn).await?;
    Ok((
        RunStore::Postgres(run),
        WorkflowStore::Postgres(workflow),
        PushConfigStore::Postgres(push),
    ))
}

#[cfg(feature = "postgres")]
async fn connect_postgres_store<S>(
    dsn: &str,
) -> ExampleResult<PostgresDurableStateStore<JsonStateCodec<S>>>
where
    S: DurableState + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| example_error(format!("postgres connect failed: {error}")))?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {error}");
        }
    });
    let store = PostgresDurableStateStore::new(client, JsonStateCodec::<S>::new());
    store
        .migrate()
        .await
        .map_err(|error| example_error(format!("postgres migrate failed: {error}")))?;
    Ok(store)
}

#[cfg(not(feature = "postgres"))]
async fn build_postgres_stores(
    _config: &ExampleConfig,
) -> ExampleResult<(RunStore, WorkflowStore, PushConfigStore)> {
    Err(
        example_error("RAKKA_PERSISTENCE=postgres requires building with --features postgres")
            .into(),
    )
}

/// Small JSON file state store for local multi-process demos.
///
/// Each revision of a record is committed as its own immutable file named
/// `<hex(id)>.r<revision>.json`. A commit writes a unique temp file and then
/// `hard_link`s it to the revision name; the link is atomic and exclusive on
/// POSIX filesystems, so two processes racing the same compare-and-set can
/// never both win — the loser observes the existing revision file and reports
/// a revision conflict. `delete` has no such claim and is only safe while no
/// concurrent writer exists for the same record; this example never deletes
/// concurrently.
#[derive(Debug)]
pub struct FileDurableStateStore<S>
where
    S: DurableState,
{
    root: Arc<PathBuf>,
    _state: PhantomData<fn() -> S>,
}

/// Monotonic per-process suffix keeping temp file names collision-free.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

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

    fn revision_path(&self, persistence_id: &PersistenceId, revision: Revision) -> PathBuf {
        self.root.join(format!(
            "{}.r{:020}.json",
            hex_encode(persistence_id.as_str()),
            revision.get()
        ))
    }

    /// Parses `<hex(id)>.r<revision>.json` file names; temp files never match.
    fn parse_record_file_name(file_name: &str) -> Option<(String, Revision)> {
        let stem = file_name.strip_suffix(".json")?;
        let (hex, digits) = stem.rsplit_once(".r")?;
        let revision = digits.parse::<u64>().ok()?;
        Some((hex_decode(hex)?, Revision::new(revision)))
    }

    fn record_files(&self) -> DurableResult<Vec<(String, Revision, PathBuf)>> {
        let entries = match std::fs::read_dir(self.root.as_ref()) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(file_store_error(error)),
        };
        let mut records = Vec::new();
        for entry in entries {
            let path = entry.map_err(file_store_error)?.path();
            let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            let Some((id, revision)) = Self::parse_record_file_name(file_name) else {
                continue;
            };
            records.push((id, revision, path));
        }
        Ok(records)
    }

    /// Returns the newest committed revision file for `persistence_id`.
    fn current_revision_file(
        &self,
        persistence_id: &PersistenceId,
    ) -> DurableResult<Option<(Revision, PathBuf)>> {
        let mut newest: Option<(Revision, PathBuf)> = None;
        for (id, revision, path) in self.record_files()? {
            if id != persistence_id.as_str() {
                continue;
            }
            if newest
                .as_ref()
                .is_none_or(|(current, _)| revision > *current)
            {
                newest = Some((revision, path));
            }
        }
        Ok(newest)
    }

    fn current_revision(&self, persistence_id: &PersistenceId) -> DurableResult<Revision> {
        Ok(self
            .current_revision_file(persistence_id)?
            .map_or(Revision::INITIAL, |(revision, _)| revision))
    }
}

impl<S> FileDurableStateStore<S>
where
    S: DurableState + Serialize + DeserializeOwned,
{
    fn load_record(&self, persistence_id: &PersistenceId) -> DurableResult<Option<StateRecord<S>>> {
        let Some((_, path)) = self.current_revision_file(persistence_id)? else {
            return Ok(None);
        };
        let bytes = std::fs::read(&path).map_err(file_store_error)?;
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

    /// Commits `record` as the exclusive winner of its revision.
    ///
    /// The `hard_link` from the fully written temp file to the revision file
    /// is the atomic claim: if another process (or task) already committed
    /// this revision the link fails with `AlreadyExists` and the caller gets
    /// a revision conflict instead of silently overwriting durable state.
    fn commit_record(
        &self,
        persistence_id: &PersistenceId,
        record: &StateRecord<S>,
    ) -> DurableResult<()> {
        std::fs::create_dir_all(self.root.as_ref()).map_err(file_store_error)?;
        let path = self.revision_path(persistence_id, record.revision);
        let temp = path.with_extension(format!(
            "json.tmp-{}-{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let stored = StoredStateRecord {
            persistence_id: persistence_id.clone(),
            revision: record.revision.get(),
            state: record.state.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        let write_result = (|| {
            use std::io::Write as _;
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()
        })();
        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&temp);
            return Err(file_store_error(error));
        }
        let linked = std::fs::hard_link(&temp, &path);
        let _ = std::fs::remove_file(&temp);
        match linked {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let actual = self.current_revision(persistence_id)?;
                Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    Revision::new(record.revision.get().saturating_sub(1)),
                    actual,
                ))
            }
            Err(error) => Err(file_store_error(error)),
        }
    }

    /// Best-effort removal of revision files older than `keep`.
    fn prune_older_revisions(&self, persistence_id: &PersistenceId, keep: Revision) {
        let Ok(records) = self.record_files() else {
            return;
        };
        for (id, revision, path) in records {
            if id == persistence_id.as_str() && revision < keep {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    fn list_persistence_ids(&self) -> DurableResult<Vec<PersistenceId>> {
        let mut ids = self
            .record_files()?
            .into_iter()
            .map(|(id, _, _)| PersistenceId::new(id))
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        Ok(ids)
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
            let actual = self.current_revision(persistence_id)?;
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            let record = StateRecord::new(state, expected_revision.next());
            // The commit itself re-checks exclusivity: a concurrent writer
            // that also passed the revision check loses the hard-link claim
            // and surfaces as a revision conflict, never a lost update.
            self.commit_record(persistence_id, &record)?;
            self.prune_older_revisions(persistence_id, record.revision);
            Ok(record)
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        Box::pin(async move {
            let actual = self.current_revision(persistence_id)?;
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            for (id, _, path) in self.record_files()? {
                if id == persistence_id.as_str() {
                    match std::fs::remove_file(path) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(file_store_error(error)),
                    }
                }
            }
            Ok(Revision::INITIAL)
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
    use crate::support::current_timestamp_millis;
    use rakka::agent_workflow::{
        AgentRunId, AgentRunStatus, AgentStatePayload, AgentTimestampMillis, AgentWorkflowId,
        StateSchemaVersion, WorkflowDefinitionVersion,
    };

    fn sample_state(run_id: &str, attempt: u32) -> AgentRunState {
        AgentRunState {
            run_id: AgentRunId::new(run_id),
            workflow_id: AgentWorkflowId::new("workflow"),
            tenant: None,
            definition_version: WorkflowDefinitionVersion::new("v1"),
            state_schema_version: StateSchemaVersion::new(1),
            graph_state: None,
            status: AgentRunStatus::Accepted,
            current_step_id: None,
            current_attempt: attempt,
            inputs_ref: None,
            state_payload: AgentStatePayload::Empty,
            checkpoints: Vec::new(),
            pending_effects: Vec::new(),
            pending_human_checkpoint: None,
            cancellation: None,
            created_at: AgentTimestampMillis::new(1),
            updated_at: AgentTimestampMillis::new(1),
            completed_at: None,
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "rakka-a2a-file-store-{label}-{}-{}",
            std::process::id(),
            current_timestamp_millis()
        ))
    }

    #[tokio::test]
    async fn file_store_round_trips_state_and_ids() {
        let root = temp_root("round-trip");
        let store = FileDurableStateStore::new(&root);
        let persistence_id = PersistenceId::new("agent-run:run-1");
        let state = sample_state("run-1", 0);

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

    #[tokio::test]
    async fn compare_and_set_admits_exactly_one_writer_per_revision() {
        let root = temp_root("cas-race");
        // Two store handles over one directory stand in for two node
        // processes sharing RAKKA_STATE_DIR during shard ownership overlap.
        let store_a = FileDurableStateStore::new(&root);
        let store_b = FileDurableStateStore::new(&root);
        let persistence_id = PersistenceId::new("agent-run:run-race");

        store_a
            .compare_and_set(
                &persistence_id,
                Revision::INITIAL,
                sample_state("run-race", 0),
            )
            .await
            .unwrap();

        // Model the lost-update window directly: both writers already passed
        // the revision check at revision 1 and now race the commit of
        // revision 2. The hard-link claim admits exactly one.
        let winner = StateRecord::new(sample_state("run-race", 1), Revision::new(2));
        store_a.commit_record(&persistence_id, &winner).unwrap();
        let loser = StateRecord::new(sample_state("run-race", 9), Revision::new(2));
        let conflict = store_b.commit_record(&persistence_id, &loser).unwrap_err();
        assert!(matches!(conflict, DurableError::RevisionConflict { .. }));

        // The winner's acknowledged write survived intact.
        let current = store_b.load(&persistence_id).await.unwrap().unwrap();
        assert_eq!(current.revision, Revision::new(2));
        assert_eq!(current.state.current_attempt, 1);

        // The loser recovers by re-reading and retrying at the new revision.
        let retried = store_b
            .compare_and_set(
                &persistence_id,
                Revision::new(2),
                sample_state("run-race", 2),
            )
            .await
            .unwrap();
        assert_eq!(retried.revision, Revision::new(3));

        let _ = std::fs::remove_dir_all(root);
    }
}
