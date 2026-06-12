//! Durable coordinator store abstractions.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::coordinator::ShardOwnershipSnapshot;
use crate::coordinator_lease::LeaseToken;
use crate::error::{ShardingError, ShardingResult};
use crate::identity::EntityType;

/// Boxed future returned by asynchronous coordinator store operations.
pub type CoordinatorStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = ShardingResult<T>> + Send + 'a>>;

/// Durable snapshot of one shard coordinator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedShardCoordinatorState {
    snapshot: ShardOwnershipSnapshot,
    allocation_strategy: String,
    updated_at_millis: u64,
}

impl PersistedShardCoordinatorState {
    /// Creates persisted coordinator state.
    #[must_use]
    pub fn new(
        snapshot: ShardOwnershipSnapshot,
        allocation_strategy: impl Into<String>,
        updated_at_millis: u64,
    ) -> Self {
        Self {
            snapshot,
            allocation_strategy: allocation_strategy.into(),
            updated_at_millis,
        }
    }

    /// Creates persisted coordinator state with the current wall-clock timestamp.
    #[must_use]
    pub fn now(snapshot: ShardOwnershipSnapshot, allocation_strategy: impl Into<String>) -> Self {
        Self::new(snapshot, allocation_strategy, current_timestamp_millis())
    }

    /// Persisted ownership snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &ShardOwnershipSnapshot {
        &self.snapshot
    }

    /// Allocation strategy name used when the snapshot was written.
    #[must_use]
    pub fn allocation_strategy(&self) -> &str {
        &self.allocation_strategy
    }

    /// Wall-clock update timestamp in milliseconds.
    #[must_use]
    pub const fn updated_at_millis(&self) -> u64 {
        self.updated_at_millis
    }
}

/// Durable coordinator backend.
pub trait ShardCoordinatorStore: Debug + Send + Sync + 'static {
    /// Stable backend name used for diagnostics.
    fn backend_name(&self) -> &'static str;

    /// Loads persisted coordinator state for an entity type.
    fn load(
        &self,
        entity_type: &EntityType,
    ) -> ShardingResult<Option<PersistedShardCoordinatorState>>;

    /// Persists coordinator state if the stored revision matches `expected_revision`.
    fn compare_and_set(
        &self,
        entity_type: &EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
    ) -> ShardingResult<PersistedShardCoordinatorState>;

    /// Persists coordinator state with an optional lease fencing token.
    fn compare_and_set_with_lease(
        &self,
        entity_type: &EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
        _lease_token: Option<&LeaseToken>,
    ) -> ShardingResult<PersistedShardCoordinatorState> {
        self.compare_and_set(entity_type, expected_revision, state)
    }

    /// Deletes coordinator state if the stored revision matches `expected_revision`.
    fn delete(&self, entity_type: &EntityType, expected_revision: u64) -> ShardingResult<()>;
}

/// Asynchronous durable coordinator backend.
///
/// Persistent stores should implement this trait so runtime threads do not
/// block while loading or fencing coordinator snapshots.
pub trait AsyncShardCoordinatorStore: Debug + Send + Sync + 'static {
    /// Stable backend name used for diagnostics.
    fn backend_name(&self) -> &'static str;

    /// Loads persisted coordinator state for an entity type.
    fn load<'a>(
        &'a self,
        entity_type: &'a EntityType,
    ) -> CoordinatorStoreFuture<'a, Option<PersistedShardCoordinatorState>>;

    /// Persists coordinator state if the stored revision matches `expected_revision`.
    fn compare_and_set<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState>;

    /// Persists coordinator state with an optional lease fencing token.
    fn compare_and_set_with_lease<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
        lease_token: Option<&'a LeaseToken>,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState> {
        Box::pin(async move {
            let _lease_token = lease_token;
            self.compare_and_set(entity_type, expected_revision, state)
                .await
        })
    }

    /// Deletes coordinator state if the stored revision matches `expected_revision`.
    fn delete<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
    ) -> CoordinatorStoreFuture<'a, ()>;
}

impl<T> AsyncShardCoordinatorStore for T
where
    T: ShardCoordinatorStore,
{
    fn backend_name(&self) -> &'static str {
        ShardCoordinatorStore::backend_name(self)
    }

    fn load<'a>(
        &'a self,
        entity_type: &'a EntityType,
    ) -> CoordinatorStoreFuture<'a, Option<PersistedShardCoordinatorState>> {
        Box::pin(async move { ShardCoordinatorStore::load(self, entity_type) })
    }

    fn compare_and_set<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState> {
        Box::pin(async move {
            ShardCoordinatorStore::compare_and_set(self, entity_type, expected_revision, state)
        })
    }

    fn compare_and_set_with_lease<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
        lease_token: Option<&'a LeaseToken>,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState> {
        Box::pin(async move {
            ShardCoordinatorStore::compare_and_set_with_lease(
                self,
                entity_type,
                expected_revision,
                state,
                lease_token,
            )
        })
    }

    fn delete<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
    ) -> CoordinatorStoreFuture<'a, ()> {
        Box::pin(async move { ShardCoordinatorStore::delete(self, entity_type, expected_revision) })
    }
}

/// In-memory durable coordinator store for tests and single-process deployments.
#[derive(Clone, Default)]
pub struct InMemoryShardCoordinatorStore {
    states: Arc<Mutex<BTreeMap<EntityType, PersistedShardCoordinatorState>>>,
}

impl InMemoryShardCoordinatorStore {
    /// Creates an empty in-memory coordinator store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of persisted coordinator states.
    #[must_use]
    pub fn len(&self) -> usize {
        self.states
            .lock()
            .expect("in-memory shard coordinator store mutex poisoned")
            .len()
    }

    /// Returns true when no coordinator states are persisted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Debug for InMemoryShardCoordinatorStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryShardCoordinatorStore")
            .field("states", &self.len())
            .finish_non_exhaustive()
    }
}

impl ShardCoordinatorStore for InMemoryShardCoordinatorStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn load(
        &self,
        entity_type: &EntityType,
    ) -> ShardingResult<Option<PersistedShardCoordinatorState>> {
        Ok(self
            .states
            .lock()
            .expect("in-memory shard coordinator store mutex poisoned")
            .get(entity_type)
            .cloned())
    }

    fn compare_and_set(
        &self,
        entity_type: &EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
    ) -> ShardingResult<PersistedShardCoordinatorState> {
        validate_state_entity_type(entity_type, &state)?;
        let mut states = self
            .states
            .lock()
            .expect("in-memory shard coordinator store mutex poisoned");
        let actual_revision = states
            .get(entity_type)
            .map_or(0, |stored| stored.snapshot().revision());

        if actual_revision != expected_revision {
            return Err(ShardingError::CoordinatorRevisionConflict {
                entity_type: entity_type.clone(),
                expected_revision,
                actual_revision,
            });
        }

        states.insert(entity_type.clone(), state.clone());
        Ok(state)
    }

    fn delete(&self, entity_type: &EntityType, expected_revision: u64) -> ShardingResult<()> {
        let mut states = self
            .states
            .lock()
            .expect("in-memory shard coordinator store mutex poisoned");
        let actual_revision = states
            .get(entity_type)
            .map_or(0, |stored| stored.snapshot().revision());

        if actual_revision != expected_revision {
            return Err(ShardingError::CoordinatorRevisionConflict {
                entity_type: entity_type.clone(),
                expected_revision,
                actual_revision,
            });
        }

        states.remove(entity_type);
        Ok(())
    }
}

fn validate_state_entity_type(
    entity_type: &EntityType,
    state: &PersistedShardCoordinatorState,
) -> ShardingResult<()> {
    if state.snapshot().entity_type() == entity_type {
        Ok(())
    } else {
        Err(ShardingError::PersistedCoordinatorSnapshotMismatch {
            expected_entity_type: entity_type.clone(),
            actual_entity_type: state.snapshot().entity_type().clone(),
            expected_shards: state.snapshot().number_of_shards(),
            actual_shards: state.snapshot().number_of_shards(),
        })
    }
}

fn current_timestamp_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must not be before Unix epoch")
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}
