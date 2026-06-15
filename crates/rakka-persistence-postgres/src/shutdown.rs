//! Coordinated shutdown support for PostgreSQL persistence stores.

use std::sync::Arc;

use rakka_core::{RakkaError, RakkaResult, Subsystem};
use rakka_persistence::shutdown::{PersistenceShutdown, PersistenceShutdownFuture};
use tokio_postgres::Client;

use crate::{PostgresDurableStateStore, PostgresEventJournal, PostgresSnapshotStore, BACKEND_NAME};

impl<C> PersistenceShutdown for PostgresDurableStateStore<C>
where
    C: Clone + Send + Sync + 'static,
{
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn shutdown_operation(&self) -> &'static str {
        "postgres-readiness-check"
    }

    fn flush<'a>(&'a self) -> PersistenceShutdownFuture<'a> {
        let client = self.client.clone();
        Box::pin(async move { check_postgres_backend(client).await })
    }
}

impl<C> PersistenceShutdown for PostgresEventJournal<C>
where
    C: Clone + Send + Sync + 'static,
{
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn shutdown_operation(&self) -> &'static str {
        "postgres-readiness-check"
    }

    fn flush<'a>(&'a self) -> PersistenceShutdownFuture<'a> {
        let client = self.client.clone();
        Box::pin(async move { check_postgres_backend(client).await })
    }
}

impl<C> PersistenceShutdown for PostgresSnapshotStore<C>
where
    C: Clone + Send + Sync + 'static,
{
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn shutdown_operation(&self) -> &'static str {
        "postgres-readiness-check"
    }

    fn flush<'a>(&'a self) -> PersistenceShutdownFuture<'a> {
        let client = self.client.clone();
        Box::pin(async move { check_postgres_backend(client).await })
    }
}

async fn check_postgres_backend(client: Arc<Client>) -> RakkaResult<()> {
    client.simple_query("SELECT 1").await.map_err(|error| {
        RakkaError::new(
            Subsystem::PersistencePostgres,
            "postgres-readiness-check",
            error.to_string(),
        )
    })?;
    Ok(())
}
