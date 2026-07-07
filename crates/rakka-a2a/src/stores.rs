//! Type-erased durable store handles.
//!
//! `DurableStateStore` is not object safe (it requires `Clone`), so the
//! service builder erases application-chosen store implementations behind
//! [`SharedDurableStateStore`]. The wrapper is `Clone` (shared `Arc`) and
//! implements `DurableStateStore` by delegation, so it plugs into
//! `AgentStepRunner`, `AgentRunActor`, and `AgentRunInbox` unchanged.

use std::sync::Arc;

use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::AgentRunState;
use rakka_persistence::{
    DurableState, DurableStateStore, PersistenceId, Revision, StateRecord, StoreFuture,
};

/// Object-safe mirror of `DurableStateStore` used for erasure.
trait DynDurableStateStore<S>: Send + Sync
where
    S: DurableState,
{
    fn backend_name(&self) -> &'static str;
    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>>;
    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>>;
    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision>;
    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>>;
}

impl<S, T> DynDurableStateStore<S> for T
where
    S: DurableState,
    T: DurableStateStore<S>,
{
    fn backend_name(&self) -> &'static str {
        DurableStateStore::backend_name(self)
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        DurableStateStore::load(self, persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        DurableStateStore::compare_and_set(self, persistence_id, expected_revision, state)
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        DurableStateStore::delete(self, persistence_id, expected_revision)
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        DurableStateStore::persistence_ids(self)
    }
}

/// A shareable, type-erased durable state store handle.
///
/// Every clone shares the same underlying store; the same handle must be
/// shared across the request handler and the sharded run host so both
/// observe one durable truth.
pub struct SharedDurableStateStore<S>
where
    S: DurableState,
{
    inner: Arc<dyn DynDurableStateStore<S>>,
}

impl<S> SharedDurableStateStore<S>
where
    S: DurableState,
{
    /// Erases an application-chosen durable store implementation.
    #[must_use]
    pub fn new(store: impl DurableStateStore<S>) -> Self {
        Self {
            inner: Arc::new(store),
        }
    }
}

impl<S> Clone for SharedDurableStateStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> std::fmt::Debug for SharedDurableStateStore<S>
where
    S: DurableState,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedDurableStateStore")
            .field("backend", &self.inner.backend_name())
            .finish_non_exhaustive()
    }
}

impl<S> DurableStateStore<S> for SharedDurableStateStore<S>
where
    S: DurableState,
{
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        self.inner
            .compare_and_set(persistence_id, expected_revision, state)
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        self.inner.delete(persistence_id, expected_revision)
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        self.inner.persistence_ids()
    }
}

/// Shared durable store for agent run state.
pub type A2ARunStateStore = SharedDurableStateStore<AgentRunState>;

/// Shared durable store for workflow inbox/outbox state.
pub type A2AWorkflowStateStore = SharedDurableStateStore<WorkflowState>;
