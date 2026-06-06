//! In-memory durable state store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{DurableError, DurableResult};
use crate::store::{
    DurableState, DurableStateStore, PersistenceId, Revision, StateRecord, StoreFuture,
};

/// In-memory durable state store for tests and local examples.
#[derive(Debug)]
pub struct InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    records: Arc<Mutex<HashMap<PersistenceId, StateRecord<S>>>>,
}

impl<S> InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    /// Creates an empty in-memory durable state store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the number of stored records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records
            .lock()
            .expect("in-memory durable state mutex poisoned")
            .len()
    }

    /// Returns true when no records are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<S> Clone for InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            records: self.records.clone(),
        }
    }
}

impl<S> Default for InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> DurableStateStore<S> for InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        let result = self
            .records
            .lock()
            .expect("in-memory durable state mutex poisoned")
            .get(persistence_id)
            .cloned();
        Box::pin(async move { Ok(result) })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        let result: DurableResult<StateRecord<S>> = {
            let mut records = self
                .records
                .lock()
                .expect("in-memory durable state mutex poisoned");
            let actual_revision = records
                .get(persistence_id)
                .map_or(Revision::INITIAL, |record| record.revision);

            if actual_revision != expected_revision {
                Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual_revision,
                ))
            } else {
                let record = StateRecord::new(state, expected_revision.next());
                records.insert(persistence_id.clone(), record.clone());
                Ok(record)
            }
        };

        Box::pin(async move { result })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        let result: DurableResult<Revision> = {
            let mut records = self
                .records
                .lock()
                .expect("in-memory durable state mutex poisoned");
            let actual_revision = records
                .get(persistence_id)
                .map_or(Revision::INITIAL, |record| record.revision);

            if actual_revision != expected_revision {
                Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual_revision,
                ))
            } else {
                records.remove(persistence_id);
                Ok(Revision::INITIAL)
            }
        };

        Box::pin(async move { result })
    }
}
