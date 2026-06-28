//! Durable-store selection: local file (default) or shared PostgreSQL.
//!
//! For a real multi-pod Kubernetes deployment the durable store must be shared
//! so a run recovers on a new owner pod after shard movement, scale-in, or a
//! crash. The file store is single-host only and is the default for local runs;
//! the PostgreSQL store (behind the `postgres` feature) is the production path.

#[cfg(feature = "postgres")]
use std::marker::PhantomData;

use rakka::agent_workflow::substrate::WorkflowState;
use rakka::agent_workflow::AgentRunState;
use rakka::persistence::{DurableStateStore, PersistenceId, Revision, StateRecord, StoreFuture};
use serde::de::DeserializeOwned;
use serde::Serialize;

#[cfg(feature = "postgres")]
use rakka::persistence::{DurableError, DurableResult, StateCodec};

use crate::config::{ExampleConfig, PersistenceKind};
use crate::store::FileDurableStateStore;
use crate::support::{example_error, ExampleResult};

#[cfg(feature = "postgres")]
use rakka_persistence_postgres::PostgresDurableStateStore;

/// JSON `StateCodec` used by the PostgreSQL store.
#[cfg(feature = "postgres")]
pub struct JsonStateCodec<T> {
    _state: PhantomData<fn() -> T>,
}

#[cfg(feature = "postgres")]
impl<T> JsonStateCodec<T> {
    fn new() -> Self {
        Self {
            _state: PhantomData,
        }
    }
}

#[cfg(feature = "postgres")]
impl<T> Clone for JsonStateCodec<T> {
    fn clone(&self) -> Self {
        Self {
            _state: PhantomData,
        }
    }
}

#[cfg(feature = "postgres")]
impl<T> StateCodec<T> for JsonStateCodec<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn encode(&self, state: &T) -> DurableResult<Vec<u8>> {
        serde_json::to_vec(state).map_err(|error| DurableError::codec(error.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> DurableResult<T> {
        serde_json::from_slice(bytes).map_err(|error| DurableError::codec(error.to_string()))
    }
}

/// Durable store selected at runtime.
pub enum AppStore<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Local file store.
    File(FileDurableStateStore<T>),
    /// Shared PostgreSQL store.
    #[cfg(feature = "postgres")]
    Postgres(PostgresDurableStateStore<JsonStateCodec<T>>),
}

impl<T> Clone for AppStore<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        match self {
            Self::File(store) => Self::File(store.clone()),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => Self::Postgres(store.clone()),
        }
    }
}

impl<T> DurableStateStore<T> for AppStore<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn backend_name(&self) -> &'static str {
        match self {
            Self::File(store) => store.backend_name(),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.backend_name(),
        }
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<T>>> {
        match self {
            Self::File(store) => store.load(persistence_id),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.load(persistence_id),
        }
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: T,
    ) -> StoreFuture<'a, StateRecord<T>> {
        match self {
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
            Self::File(store) => store.delete(persistence_id, expected_revision),
            #[cfg(feature = "postgres")]
            Self::Postgres(store) => store.delete(persistence_id, expected_revision),
        }
    }
}

/// Builds the run and workflow durable stores selected by configuration.
pub async fn build_stores(
    config: &ExampleConfig,
) -> ExampleResult<(AppStore<AgentRunState>, AppStore<WorkflowState>)> {
    match config.persistence {
        PersistenceKind::File => Ok((
            AppStore::File(FileDurableStateStore::new(
                config.run_state_dir.clone(),
                "example-file-run",
            )),
            AppStore::File(FileDurableStateStore::new(
                config.workflow_state_dir.clone(),
                "example-file-workflow",
            )),
        )),
        PersistenceKind::Postgres => build_postgres_stores(config).await,
    }
}

#[cfg(feature = "postgres")]
async fn build_postgres_stores(
    config: &ExampleConfig,
) -> ExampleResult<(AppStore<AgentRunState>, AppStore<WorkflowState>)> {
    let dsn = config.postgres_dsn.as_deref().ok_or_else(|| {
        example_error("RAKKA_POSTGRES_DSN is required when RAKKA_PERSISTENCE=postgres")
    })?;
    let run = connect_postgres_store::<AgentRunState>(dsn).await?;
    let workflow = connect_postgres_store::<WorkflowState>(dsn).await?;
    Ok((AppStore::Postgres(run), AppStore::Postgres(workflow)))
}

#[cfg(feature = "postgres")]
async fn connect_postgres_store<T>(
    dsn: &str,
) -> ExampleResult<PostgresDurableStateStore<JsonStateCodec<T>>>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| example_error(format!("postgres connect failed: {error}")))?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("postgres connection error: {error}");
        }
    });
    let store = PostgresDurableStateStore::new(client, JsonStateCodec::<T>::new());
    store
        .migrate()
        .await
        .map_err(|error| example_error(format!("postgres migrate failed: {error}")))?;
    Ok(store)
}

#[cfg(not(feature = "postgres"))]
async fn build_postgres_stores(
    _config: &ExampleConfig,
) -> ExampleResult<(AppStore<AgentRunState>, AppStore<WorkflowState>)> {
    Err(
        example_error("RAKKA_PERSISTENCE=postgres requires building with --features postgres")
            .into(),
    )
}
