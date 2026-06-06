//! Durable state store abstractions.

use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::error::DurableResult;

/// Boxed future returned by durable state stores.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = DurableResult<T>> + Send + 'a>>;

/// Marker trait for durable actor state.
pub trait DurableState: Clone + Send + Sync + 'static {}

impl<T> DurableState for T where T: Clone + Send + Sync + 'static {}

/// Stable durable identity for an actor or entity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PersistenceId(String);

impl PersistenceId {
    /// Creates a new persistence id.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the persistence id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
}
