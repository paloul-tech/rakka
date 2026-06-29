//! Generic file-backed durable state store shared by run and workflow state.
//!
//! This intentionally tiny store keeps the example self-contained while still
//! being durable: it persists state to JSON files under a directory, with
//! compare-and-set revision checks. Pointing several local processes at the same
//! directory lets a run recover on a new owner after shard movement.
//!
//! It is not a production distributed CAS store. Real multi-host deployments
//! should use the PostgreSQL persistence plugin or another shared
//! [`rakka::persistence::DurableStateStore`].

use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use rakka::persistence::{
    DurableError, DurableResult, DurableStateStore, PersistenceId, Revision, StateRecord,
    StoreFuture,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::support::{current_timestamp_millis, hex_encode};

/// File-backed durable state store generic over any serializable state type.
#[derive(Debug)]
pub struct FileDurableStateStore<T> {
    root: Arc<PathBuf>,
    backend: &'static str,
    _state: PhantomData<fn() -> T>,
}

// Manual `Clone` so the store is cloneable regardless of whether `T: Clone`
// (the actor factory clones the store for every entity it spawns).
impl<T> Clone for FileDurableStateStore<T> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            backend: self.backend,
            _state: PhantomData,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredRecord<T> {
    revision: u64,
    state: T,
}

impl<T> FileDurableStateStore<T> {
    /// Creates a store rooted at `root` with a stable backend name.
    pub fn new(root: impl Into<PathBuf>, backend: &'static str) -> Self {
        Self {
            root: Arc::new(root.into()),
            backend,
            _state: PhantomData,
        }
    }

    fn record_path(&self, persistence_id: &PersistenceId) -> PathBuf {
        self.root
            .join(format!("{}.json", hex_encode(persistence_id.as_str())))
    }
}

impl<T> FileDurableStateStore<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn load_record(&self, persistence_id: &PersistenceId) -> DurableResult<Option<StateRecord<T>>> {
        let path = self.record_path(persistence_id);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(self.store_error(error)),
        };
        let stored: StoredRecord<T> = serde_json::from_slice(&bytes)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        Ok(Some(StateRecord::new(
            stored.state,
            Revision::new(stored.revision),
        )))
    }

    fn write_record(
        &self,
        persistence_id: &PersistenceId,
        record: &StateRecord<T>,
    ) -> DurableResult<()> {
        std::fs::create_dir_all(self.root.as_ref()).map_err(|error| self.store_error(error))?;
        let path = self.record_path(persistence_id);
        let temp = path.with_extension(format!("json.tmp.{}", current_timestamp_millis()));
        let stored = StoredRecord {
            revision: record.revision.get(),
            state: record.state.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        std::fs::write(&temp, bytes).map_err(|error| self.store_error(error))?;
        std::fs::rename(&temp, &path).map_err(|error| self.store_error(error))?;
        Ok(())
    }

    fn current_revision(&self, persistence_id: &PersistenceId) -> DurableResult<Revision> {
        Ok(self
            .load_record(persistence_id)?
            .map_or(Revision::INITIAL, |record| record.revision))
    }

    fn store_error(&self, error: impl ToString) -> DurableError {
        DurableError::store(self.backend, error.to_string())
    }
}

impl<T> DurableStateStore<T> for FileDurableStateStore<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn backend_name(&self) -> &'static str {
        self.backend
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<T>>> {
        Box::pin(async move { self.load_record(persistence_id) })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: T,
    ) -> StoreFuture<'a, StateRecord<T>> {
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
            let actual = self.current_revision(persistence_id)?;
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
                Err(error) => Err(self.store_error(error)),
            }
        })
    }
}
