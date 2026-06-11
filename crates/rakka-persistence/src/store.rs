//! Typed persistence store abstractions.

use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{DurableError, DurableResult};

/// Separator used by [`PersistenceId::of`].
pub const PERSISTENCE_ID_SEPARATOR: &str = "|";

/// Boxed future returned by persistence stores.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = DurableResult<T>> + Send + 'a>>;

/// Marker trait for durable actor state.
pub trait DurableState: Clone + Send + Sync + 'static {}

impl<T> DurableState for T where T: Clone + Send + Sync + 'static {}

/// Marker trait for event-sourced persistence events.
pub trait PersistenceEvent: Clone + Send + Sync + 'static {}

impl<T> PersistenceEvent for T where T: Clone + Send + Sync + 'static {}

/// Stable durable identity for an actor or entity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PersistenceId(String);

impl PersistenceId {
    /// Creates a new persistence id.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates a persistence id from an Akka-style entity type and entity id.
    ///
    /// Both parts must be non-empty and may not contain
    /// [`PERSISTENCE_ID_SEPARATOR`].
    pub fn of(entity_type: impl AsRef<str>, entity_id: impl AsRef<str>) -> DurableResult<Self> {
        let entity_type = entity_type.as_ref();
        let entity_id = entity_id.as_ref();
        validate_persistence_id_part("entity_type", entity_type)?;
        validate_persistence_id_part("entity_id", entity_id)?;
        Ok(Self(format!(
            "{entity_type}{PERSISTENCE_ID_SEPARATOR}{entity_id}"
        )))
    }

    /// Returns the persistence id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the entity type and entity id if this id was created by
    /// [`PersistenceId::of`].
    #[must_use]
    pub fn entity_parts(&self) -> Option<(&str, &str)> {
        self.0.split_once(PERSISTENCE_ID_SEPARATOR)
    }
}

impl Display for PersistenceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Monotonic revision for durable state records.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Revision(u64);

impl Revision {
    /// Initial revision for a missing durable state record.
    pub const INITIAL: Self = Self(0);

    /// Creates a revision from a raw integer.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw revision value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next revision.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for Revision {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonic event sequence number for event-sourced records.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct SequenceNr(u64);

impl SequenceNr {
    /// Sequence number before the first persisted event.
    pub const INITIAL: Self = Self(0);

    /// Sequence number assigned to the first persisted event.
    pub const FIRST: Self = Self(1);

    /// Highest representable sequence number.
    pub const MAX: Self = Self(u64::MAX);

    /// Creates a sequence number from a raw integer.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw sequence number value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next sequence number.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Display for SequenceNr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// State record returned by a durable state store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRecord<S>
where
    S: DurableState,
{
    /// Latest durable state.
    pub state: S,
    /// Revision associated with the state.
    pub revision: Revision,
}

impl<S> StateRecord<S>
where
    S: DurableState,
{
    /// Creates a new state record.
    #[must_use]
    pub const fn new(state: S, revision: Revision) -> Self {
        Self { state, revision }
    }

    /// Creates an in-memory record for missing durable state.
    #[must_use]
    pub const fn missing(empty_state: S) -> Self {
        Self {
            state: empty_state,
            revision: Revision::INITIAL,
        }
    }
}

/// Event plus tags selected for one journal append.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaggedEvent<E>
where
    E: PersistenceEvent,
{
    /// Event payload.
    pub event: E,
    /// Query tags attached to this event.
    pub tags: Vec<String>,
}

impl<E> TaggedEvent<E>
where
    E: PersistenceEvent,
{
    /// Creates an untagged event.
    #[must_use]
    pub fn new(event: E) -> Self {
        Self {
            event,
            tags: Vec::new(),
        }
    }

    /// Creates an event with query tags.
    #[must_use]
    pub fn with_tags<I, T>(event: E, tags: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            event,
            tags: tags.into_iter().map(Into::into).collect(),
        }
    }
}

impl<E> From<E> for TaggedEvent<E>
where
    E: PersistenceEvent,
{
    fn from(event: E) -> Self {
        Self::new(event)
    }
}

/// Metadata assigned to a persisted event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Durable identity that owns the event.
    pub persistence_id: PersistenceId,
    /// Sequence number assigned by the journal.
    pub sequence_nr: SequenceNr,
    /// Wall-clock timestamp in Unix epoch milliseconds.
    pub timestamp_millis: u64,
    /// Query tags attached to the event.
    pub tags: Vec<String>,
    /// Optional slice identifier for sharded query fan-out.
    pub slice: Option<u32>,
}

impl EventMetadata {
    /// Creates event metadata.
    #[must_use]
    pub fn new(
        persistence_id: PersistenceId,
        sequence_nr: SequenceNr,
        timestamp_millis: u64,
    ) -> Self {
        Self {
            persistence_id,
            sequence_nr,
            timestamp_millis,
            tags: Vec::new(),
            slice: None,
        }
    }

    /// Returns metadata with query tags.
    #[must_use]
    pub fn with_tags<I, T>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.tags = tags.into_iter().map(Into::into).collect();
        self
    }

    /// Returns metadata with a slice identifier.
    #[must_use]
    pub const fn with_slice(mut self, slice: u32) -> Self {
        self.slice = Some(slice);
        self
    }
}

/// Event record returned by an event journal or persistence query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRecord<E>
where
    E: PersistenceEvent,
{
    /// Event payload.
    pub event: E,
    /// Event metadata.
    pub metadata: EventMetadata,
}

impl<E> EventRecord<E>
where
    E: PersistenceEvent,
{
    /// Creates a persisted event record.
    #[must_use]
    pub const fn new(event: E, metadata: EventMetadata) -> Self {
        Self { event, metadata }
    }
}

/// Snapshot metadata stored alongside a snapshot payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Durable identity that owns the snapshot.
    pub persistence_id: PersistenceId,
    /// Sequence number covered by the snapshot.
    pub sequence_nr: SequenceNr,
    /// Wall-clock timestamp in Unix epoch milliseconds.
    pub timestamp_millis: u64,
}

impl SnapshotMetadata {
    /// Creates snapshot metadata.
    #[must_use]
    pub const fn new(
        persistence_id: PersistenceId,
        sequence_nr: SequenceNr,
        timestamp_millis: u64,
    ) -> Self {
        Self {
            persistence_id,
            sequence_nr,
            timestamp_millis,
        }
    }
}

/// Snapshot record returned by a snapshot store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRecord<S>
where
    S: DurableState,
{
    /// Snapshot payload.
    pub snapshot: S,
    /// Snapshot metadata.
    pub metadata: SnapshotMetadata,
}

impl<S> SnapshotRecord<S>
where
    S: DurableState,
{
    /// Creates a snapshot record.
    #[must_use]
    pub const fn new(snapshot: S, metadata: SnapshotMetadata) -> Self {
        Self { snapshot, metadata }
    }
}

/// Selection criteria for loading, listing, or deleting snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotSelection {
    /// Minimum accepted snapshot sequence number.
    pub min_sequence_nr: SequenceNr,
    /// Maximum accepted snapshot sequence number.
    pub max_sequence_nr: SequenceNr,
    /// Minimum accepted snapshot timestamp in Unix epoch milliseconds.
    pub min_timestamp_millis: u64,
    /// Maximum accepted snapshot timestamp in Unix epoch milliseconds.
    pub max_timestamp_millis: u64,
}

impl SnapshotSelection {
    /// Selects all snapshots, with latest-load semantics decided by the store.
    pub const LATEST: Self = Self {
        min_sequence_nr: SequenceNr::INITIAL,
        max_sequence_nr: SequenceNr::MAX,
        min_timestamp_millis: 0,
        max_timestamp_millis: u64::MAX,
    };

    /// Selects all snapshots.
    #[must_use]
    pub const fn latest() -> Self {
        Self::LATEST
    }

    /// Selects snapshots up to and including `max_sequence_nr`.
    #[must_use]
    pub const fn up_to(max_sequence_nr: SequenceNr) -> Self {
        Self {
            max_sequence_nr,
            ..Self::LATEST
        }
    }

    /// Selects snapshots between the two inclusive sequence numbers.
    #[must_use]
    pub const fn between(min_sequence_nr: SequenceNr, max_sequence_nr: SequenceNr) -> Self {
        Self {
            min_sequence_nr,
            max_sequence_nr,
            min_timestamp_millis: 0,
            max_timestamp_millis: u64::MAX,
        }
    }

    /// Returns a copy with timestamp bounds in Unix epoch milliseconds.
    #[must_use]
    pub const fn with_timestamp_millis(
        mut self,
        min_timestamp_millis: u64,
        max_timestamp_millis: u64,
    ) -> Self {
        self.min_timestamp_millis = min_timestamp_millis;
        self.max_timestamp_millis = max_timestamp_millis;
        self
    }

    /// Returns true when the metadata matches this selection.
    #[must_use]
    pub const fn matches(self, metadata: &SnapshotMetadata) -> bool {
        metadata.sequence_nr.get() >= self.min_sequence_nr.get()
            && metadata.sequence_nr.get() <= self.max_sequence_nr.get()
            && metadata.timestamp_millis >= self.min_timestamp_millis
            && metadata.timestamp_millis <= self.max_timestamp_millis
    }
}

impl Default for SnapshotSelection {
    fn default() -> Self {
        Self::latest()
    }
}

/// Recovery configuration for an event-sourced behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryOptions {
    /// Snapshot selection used before event replay.
    pub snapshot_selection: SnapshotSelection,
    /// Inclusive first sequence number replayed after snapshot recovery.
    pub replay_from: SequenceNr,
    /// Inclusive highest sequence number replayed during recovery.
    pub replay_to: SequenceNr,
}

impl RecoveryOptions {
    /// Creates default recovery options.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            snapshot_selection: SnapshotSelection::LATEST,
            replay_from: SequenceNr::FIRST,
            replay_to: SequenceNr::MAX,
        }
    }

    /// Returns a copy with a snapshot selection.
    #[must_use]
    pub const fn with_snapshot_selection(mut self, selection: SnapshotSelection) -> Self {
        self.snapshot_selection = selection;
        self
    }

    /// Returns a copy with replay sequence bounds.
    #[must_use]
    pub const fn with_replay(mut self, replay_from: SequenceNr, replay_to: SequenceNr) -> Self {
        self.replay_from = replay_from;
        self.replay_to = replay_to;
        self
    }
}

impl Default for RecoveryOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Codec used by byte-oriented durable state stores.
pub trait StateCodec<S>: Clone + Send + Sync + 'static
where
    S: DurableState,
{
    /// Encodes state into bytes.
    fn encode(&self, state: &S) -> DurableResult<Vec<u8>>;

    /// Decodes state from bytes.
    fn decode(&self, bytes: &[u8]) -> DurableResult<S>;
}

/// Durable state store with optimistic revision fencing.
pub trait DurableStateStore<S>: Clone + Send + Sync + 'static
where
    S: DurableState,
{
    /// Stable backend name used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Loads the latest state record, if present.
    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>>;

    /// Writes a state record if the current revision matches `expected_revision`.
    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>>;

    /// Deletes a state record if the current revision matches `expected_revision`.
    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision>;

    /// Lists known durable-state persistence ids when the backend supports it.
    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        let backend = self.backend_name();
        Box::pin(async move {
            Err(DurableError::store(
                backend,
                "durable state persistence id queries are not supported by this backend",
            ))
        })
    }
}

/// Event journal for typed event-sourced persistence.
pub trait EventJournal<E>: Clone + Send + Sync + 'static
where
    E: PersistenceEvent,
{
    /// Stable backend name used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Appends events if the current highest sequence number matches `expected_sequence_nr`.
    fn append<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_sequence_nr: SequenceNr,
        events: Vec<TaggedEvent<E>>,
    ) -> StoreFuture<'a, Vec<EventRecord<E>>>;

    /// Replays events for one persistence id between inclusive sequence bounds.
    fn replay<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        from: SequenceNr,
        to: SequenceNr,
    ) -> StoreFuture<'a, Vec<EventRecord<E>>>;

    /// Deletes events up to and including `to`.
    fn delete_to<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        to: SequenceNr,
    ) -> StoreFuture<'a, ()>;

    /// Returns the highest sequence number assigned to one persistence id.
    fn highest_sequence_nr<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, SequenceNr>;

    /// Lists known event-sourced persistence ids.
    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>>;

    /// Queries events carrying a tag.
    fn events_by_tag<'a>(&'a self, tag: &'a str) -> StoreFuture<'a, Vec<EventRecord<E>>>;
}

/// Snapshot store for typed event-sourced persistence.
pub trait SnapshotStore<S>: Clone + Send + Sync + 'static
where
    S: DurableState,
{
    /// Stable backend name used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// Saves a snapshot for one sequence number.
    fn save<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        sequence_nr: SequenceNr,
        snapshot: S,
    ) -> StoreFuture<'a, SnapshotRecord<S>>;

    /// Loads the latest snapshot matching a selection.
    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, Option<SnapshotRecord<S>>>;

    /// Lists snapshot metadata matching a selection, newest first.
    fn list<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, Vec<SnapshotMetadata>>;

    /// Deletes snapshots matching a selection and returns the count removed.
    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        selection: SnapshotSelection,
    ) -> StoreFuture<'a, usize>;
}

pub(crate) fn current_timestamp_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn validate_persistence_id_part(name: &str, value: &str) -> DurableResult<()> {
    if value.is_empty() {
        return Err(DurableError::invalid_persistence_id(format!(
            "{name} must not be empty"
        )));
    }

    if value.contains(PERSISTENCE_ID_SEPARATOR) {
        return Err(DurableError::invalid_persistence_id(format!(
            "{name} must not contain persistence id separator {PERSISTENCE_ID_SEPARATOR:?}"
        )));
    }

    Ok(())
}
