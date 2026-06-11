//! In-memory durable state store.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::error::{DurableError, DurableResult};
use crate::store::{
    current_timestamp_millis, DurableState, DurableStateStore, EventJournal, EventRecord,
    PersistenceEvent, PersistenceId, Revision, SequenceNr, SnapshotMetadata, SnapshotRecord,
    SnapshotSelection, SnapshotStore, StateRecord, StoreFuture, TaggedEvent,
};

/// In-memory durable state store for tests and local examples.
#[derive(Debug)]
pub struct InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    records: Arc<Mutex<BTreeMap<PersistenceId, StateRecord<S>>>>,
}

impl<S> InMemoryDurableStateStore<S>
where
    S: DurableState,
{
    /// Creates an empty in-memory durable state store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(BTreeMap::new())),
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

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        let result = self
            .records
            .lock()
            .expect("in-memory durable state mutex poisoned")
            .keys()
            .cloned()
            .collect();
        Box::pin(async move { Ok(result) })
    }
}

/// In-memory event journal for tests and local examples.
#[derive(Debug)]
pub struct InMemoryEventJournal<E>
where
    E: PersistenceEvent,
{
    records: Arc<Mutex<BTreeMap<PersistenceId, JournalState<E>>>>,
}

impl<E> InMemoryEventJournal<E>
where
    E: PersistenceEvent,
{
    /// Creates an empty in-memory event journal.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Returns the number of stored, non-deleted event records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records
            .lock()
            .expect("in-memory event journal mutex poisoned")
            .values()
            .map(|state| state.events.len())
            .sum()
    }

    /// Returns true when no non-deleted events are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<E> Clone for InMemoryEventJournal<E>
where
    E: PersistenceEvent,
{
    fn clone(&self) -> Self {
        Self {
            records: self.records.clone(),
        }
    }
}

impl<E> Default for InMemoryEventJournal<E>
where
    E: PersistenceEvent,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> EventJournal<E> for InMemoryEventJournal<E>
where
    E: PersistenceEvent,
{
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn append<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_sequence_nr: SequenceNr,
        events: Vec<TaggedEvent<E>>,
    ) -> StoreFuture<'a, Vec<EventRecord<E>>> {
        let result: DurableResult<Vec<EventRecord<E>>> = {
            let mut records = self
                .records
                .lock()
                .expect("in-memory event journal mutex poisoned");
            let state = records.entry(persistence_id.clone()).or_default();
            let actual_sequence_nr = state.highest_sequence_nr;

            if actual_sequence_nr != expected_sequence_nr {
                Err(DurableError::sequence_conflict(
                    persistence_id.clone(),
                    expected_sequence_nr,
                    actual_sequence_nr,
                ))
            } else {
                let mut appended = Vec::with_capacity(events.len());
                for tagged in events {
                    state.highest_sequence_nr = state.highest_sequence_nr.next();
                    let metadata = crate::store::EventMetadata::new(
                        persistence_id.clone(),
                        state.highest_sequence_nr,
                        current_timestamp_millis(),
                    )
                    .with_tags(tagged.tags);
                    let record = EventRecord::new(tagged.event, metadata);
                    state.events.push(record.clone());
                    appended.push(record);
                }
                Ok(appended)
            }
        };

        Box::pin(async move { result })
    }

    fn replay<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        from: SequenceNr,
        to: SequenceNr,
    ) -> StoreFuture<'a, Vec<EventRecord<E>>> {
        let result = self
            .records
            .lock()
            .expect("in-memory event journal mutex poisoned")
            .get(persistence_id)
            .map_or_else(Vec::new, |state| {
                state
                    .events
                    .iter()
                    .filter(|record| {
                        record.metadata.sequence_nr >= from && record.metadata.sequence_nr <= to
                    })
                    .cloned()
                    .collect()
            });
        Box::pin(async move { Ok(result) })
    }

    fn delete_to<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        to: SequenceNr,
    ) -> StoreFuture<'a, ()> {
        {
            let mut records = self
                .records
                .lock()
                .expect("in-memory event journal mutex poisoned");
            if let Some(state) = records.get_mut(persistence_id) {
                state
                    .events
                    .retain(|record| record.metadata.sequence_nr > to);
            }
        }
        Box::pin(async move { Ok(()) })
    }

    fn highest_sequence_nr<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, SequenceNr> {
        let result = self
            .records
            .lock()
            .expect("in-memory event journal mutex poisoned")
            .get(persistence_id)
            .map_or(SequenceNr::INITIAL, |state| state.highest_sequence_nr);
        Box::pin(async move { Ok(result) })
    }

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        let result = self
            .records
            .lock()
            .expect("in-memory event journal mutex poisoned")
            .keys()
            .cloned()
            .collect();
        Box::pin(async move { Ok(result) })
    }

    fn events_by_tag<'a>(&'a self, tag: &'a str) -> StoreFuture<'a, Vec<EventRecord<E>>> {
        let result = self
            .records
            .lock()
            .expect("in-memory event journal mutex poisoned")
            .values()
            .flat_map(|state| state.events.iter())
            .filter(|record| {
                record
                    .metadata
                    .tags
                    .iter()
                    .any(|candidate| candidate == tag)
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(result) })
    }
}

/// In-memory snapshot store for tests and local examples.
#[derive(Debug)]
pub struct InMemorySnapshotStore<S>
where
    S: DurableState,
{
    records: Arc<Mutex<BTreeMap<PersistenceId, Vec<SnapshotRecord<S>>>>>,
}

impl<S> InMemorySnapshotStore<S>
where
    S: DurableState,
{
    /// Creates an empty in-memory snapshot store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Returns the number of stored snapshots.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records
            .lock()
            .expect("in-memory snapshot store mutex poisoned")
            .values()
            .map(Vec::len)
            .sum()
    }

    /// Returns true when no snapshots are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<S> Clone for InMemorySnapshotStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            records: self.records.clone(),
        }
    }
}

impl<S> Default for InMemorySnapshotStore<S>
where
    S: DurableState,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> SnapshotStore<S> for InMemorySnapshotStore<S>
where
    S: DurableState,
{
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn save<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        sequence_nr: SequenceNr,
        snapshot: S,
    ) -> StoreFuture<'a, SnapshotRecord<S>> {
        let record = SnapshotRecord::new(
            snapshot,
            SnapshotMetadata::new(
                persistence_id.clone(),
                sequence_nr,
                current_timestamp_millis(),
            ),
        );
        {
            let mut records = self
                .records
                .lock()
                .expect("in-memory snapshot store mutex poisoned");
            let snapshots = records.entry(persistence_id.clone()).or_default();
            snapshots.retain(|existing| existing.metadata.sequence_nr != sequence_nr);
            snapshots.push(record.clone());
            sort_snapshots_newest_first(snapshots);
        }
        Box::pin(async move { Ok(record) })
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, Option<SnapshotRecord<S>>> {
        let result = self
            .records
            .lock()
            .expect("in-memory snapshot store mutex poisoned")
            .get(persistence_id)
            .and_then(|snapshots| {
                snapshots
                    .iter()
                    .find(|snapshot| selection.matches(&snapshot.metadata))
                    .cloned()
            });
        Box::pin(async move { Ok(result) })
    }

    fn list<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, Vec<SnapshotMetadata>> {
        let result = self
            .records
            .lock()
            .expect("in-memory snapshot store mutex poisoned")
            .get(persistence_id)
            .map_or_else(Vec::new, |snapshots| {
                snapshots
                    .iter()
                    .filter(|snapshot| selection.matches(&snapshot.metadata))
                    .map(|snapshot| snapshot.metadata.clone())
                    .collect()
            });
        Box::pin(async move { Ok(result) })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, usize> {
        let removed = {
            let mut records = self
                .records
                .lock()
                .expect("in-memory snapshot store mutex poisoned");
            let Some(snapshots) = records.get_mut(persistence_id) else {
                return Box::pin(async move { Ok(0) });
            };
            let before = snapshots.len();
            snapshots.retain(|snapshot| !selection.matches(&snapshot.metadata));
            let removed = before - snapshots.len();
            if snapshots.is_empty() {
                records.remove(persistence_id);
            }
            removed
        };
        Box::pin(async move { Ok(removed) })
    }
}

#[derive(Debug, Clone)]
struct JournalState<E>
where
    E: PersistenceEvent,
{
    highest_sequence_nr: SequenceNr,
    events: Vec<EventRecord<E>>,
}

impl<E> Default for JournalState<E>
where
    E: PersistenceEvent,
{
    fn default() -> Self {
        Self {
            highest_sequence_nr: SequenceNr::INITIAL,
            events: Vec::new(),
        }
    }
}

fn sort_snapshots_newest_first<S>(snapshots: &mut [SnapshotRecord<S>])
where
    S: DurableState,
{
    snapshots.sort_by(|left, right| {
        right
            .metadata
            .sequence_nr
            .cmp(&left.metadata.sequence_nr)
            .then_with(|| {
                right
                    .metadata
                    .timestamp_millis
                    .cmp(&left.metadata.timestamp_millis)
            })
    });
}
