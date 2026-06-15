//! Coordinated shutdown helpers for persistence backends and query streams.

use std::future::Future;
use std::pin::Pin;

use rakka_core::{
    CoordinatedShutdown, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
};
use rakka_stream::StreamSource;

use crate::{
    DurableState, InMemoryDurableStateStore, InMemoryEventJournal, InMemorySnapshotStore,
    PersistenceEvent,
};

/// Future returned by persistence shutdown backends.
pub type PersistenceShutdownFuture<'a> = Pin<Box<dyn Future<Output = RakkaResult<()>> + Send + 'a>>;

/// Backend hook used by coordinated shutdown to make persistence cleanup visible.
///
/// Implementations should only perform work the backend can honestly support.
/// Write-through stores can return a no-op success; buffered stores can flush;
/// external stores can run a final readiness or close check.
pub trait PersistenceShutdown: Clone + Send + Sync + 'static {
    /// Stable backend name used in coordinated shutdown task attributes.
    fn backend_name(&self) -> &'static str;

    /// Optional persistence id or shard scope covered by this shutdown task.
    fn persistence_scope(&self) -> Option<String> {
        None
    }

    /// Stable operation label used in coordinated shutdown task attributes.
    fn shutdown_operation(&self) -> &'static str {
        "persistence-shutdown"
    }

    /// Flushes, checks, or closes the backend during coordinated shutdown.
    fn flush<'a>(&'a self) -> PersistenceShutdownFuture<'a>;
}

/// Registers a persistence backend shutdown task in the `flush-persistence` phase.
pub fn register_persistence_shutdown_task<B>(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    backend: B,
) -> RakkaResult<ShutdownTask>
where
    B: PersistenceShutdown,
{
    let backend_name = backend.backend_name();
    let operation = backend.shutdown_operation();
    let persistence_scope = backend.persistence_scope();
    shutdown.add_task_with_options(
        ShutdownPhase::flush_persistence(),
        task_name,
        persistence_shutdown_options(operation, backend_name, persistence_scope.as_deref())?,
        move |_context| {
            let backend = backend.clone();
            async move { backend.flush().await }
        },
    )
}

/// Registers a custom persistence flush/check task in the `flush-persistence` phase.
pub fn register_persistence_flush_task<F, Fut>(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    backend_name: &'static str,
    persistence_scope: Option<String>,
    flush: F,
) -> RakkaResult<ShutdownTask>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = RakkaResult<()>> + Send + 'static,
{
    shutdown.add_task_with_options(
        ShutdownPhase::flush_persistence(),
        task_name,
        persistence_shutdown_options(
            "persistence-flush",
            backend_name,
            persistence_scope.as_deref(),
        )?,
        move |_context| flush(),
    )
}

/// Registers cancellation for a persistence query stream before stores are flushed or closed.
pub fn register_persistence_query_cancel_task<T>(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    source: StreamSource<T>,
    reason: impl Into<String>,
) -> RakkaResult<ShutdownTask>
where
    T: Send + 'static,
{
    let reason = reason.into();
    shutdown.add_task_with_options(
        ShutdownPhase::drain_adapters(),
        task_name,
        persistence_shutdown_options("query-stream-cancel", "query-stream", None)?,
        move |_context| {
            let source = source.clone();
            let reason = reason.clone();
            async move {
                let _dropped = source.cancel(reason);
                Ok(())
            }
        },
    )
}

impl<S> PersistenceShutdown for InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn shutdown_operation(&self) -> &'static str {
        "noop-flush"
    }

    fn flush<'a>(&'a self) -> PersistenceShutdownFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

impl<E> PersistenceShutdown for InMemoryEventJournal<E>
where
    E: PersistenceEvent,
{
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn shutdown_operation(&self) -> &'static str {
        "noop-flush"
    }

    fn flush<'a>(&'a self) -> PersistenceShutdownFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

impl<S> PersistenceShutdown for InMemorySnapshotStore<S>
where
    S: DurableState,
{
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn shutdown_operation(&self) -> &'static str {
        "noop-flush"
    }

    fn flush<'a>(&'a self) -> PersistenceShutdownFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

fn persistence_shutdown_options(
    operation: &'static str,
    backend_name: &'static str,
    persistence_scope: Option<&str>,
) -> RakkaResult<ShutdownTaskOptions> {
    let options = ShutdownTaskOptions::default()
        .with_attribute("operation", operation)?
        .with_attribute("backend", backend_name)?;
    match persistence_scope {
        Some(scope) => options.with_attribute("persistence-scope", scope),
        None => Ok(options),
    }
}
