//! Remembered entity storage and replay settings.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::ShardingResult;
use crate::identity::{EntityId, ShardKey};

/// Boxed future returned by asynchronous remembered entity store operations.
pub type RememberedStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = ShardingResult<T>> + Send + 'a>>;

/// Asynchronous durable store for remembered sharded entity ids.
pub trait RememberedEntityStore: Debug + Send + Sync + 'static {
    /// Stable backend name used for diagnostics.
    fn backend_name(&self) -> &'static str;

    /// Records that an entity was successfully activated in its shard.
    fn remember<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, ()>;

    /// Removes an entity id from the remembered set.
    ///
    /// Returns true when a remembered row or in-memory entry was removed.
    fn forget<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, bool>;

    /// Loads remembered entity ids for a shard in deterministic replay order.
    fn remembered_for_shard<'a>(
        &'a self,
        shard: &'a ShardKey,
    ) -> RememberedStoreFuture<'a, Vec<EntityId>>;
}

/// High-level settings that enable remembered entities for one entity type.
#[derive(Clone)]
pub struct RememberedEntities {
    start_batch_size: usize,
    start_batch_delay: Duration,
    store: Arc<dyn RememberedEntityStore>,
}

impl RememberedEntities {
    /// Enables remembered entities using an explicit in-memory store.
    #[must_use]
    pub fn enabled() -> Self {
        Self {
            start_batch_size: RememberedEntityReplaySettings::DEFAULT_START_BATCH_SIZE,
            start_batch_delay: Duration::ZERO,
            store: Arc::new(InMemoryRememberedEntityStore::new()),
        }
    }

    /// Sets the maximum number of remembered ids started before yielding.
    #[must_use]
    pub fn with_start_batch_size(mut self, start_batch_size: usize) -> Self {
        self.start_batch_size = start_batch_size.max(1);
        self
    }

    /// Sets the delay inserted between remembered replay batches.
    #[must_use]
    pub const fn with_start_batch_delay(mut self, start_batch_delay: Duration) -> Self {
        self.start_batch_delay = start_batch_delay;
        self
    }

    /// Sets the remembered entity store.
    #[must_use]
    pub fn with_store(mut self, store: impl RememberedEntityStore) -> Self {
        self.store = Arc::new(store);
        self
    }

    /// Sets a shared remembered entity store.
    #[must_use]
    pub fn with_store_ref(mut self, store: Arc<dyn RememberedEntityStore>) -> Self {
        self.store = store;
        self
    }

    /// Maximum remembered ids started before yielding.
    #[must_use]
    pub const fn start_batch_size(&self) -> usize {
        self.start_batch_size
    }

    /// Delay inserted between remembered replay batches.
    #[must_use]
    pub const fn start_batch_delay(&self) -> Duration {
        self.start_batch_delay
    }

    /// Replay settings derived from this configuration.
    #[must_use]
    pub const fn replay_settings(&self) -> RememberedEntityReplaySettings {
        RememberedEntityReplaySettings {
            start_batch_size: self.start_batch_size,
            start_batch_delay: self.start_batch_delay,
        }
    }

    /// Shared remembered entity store.
    #[must_use]
    pub fn store(&self) -> Arc<dyn RememberedEntityStore> {
        self.store.clone()
    }

    /// Remembered entity store backend name.
    #[must_use]
    pub fn store_backend(&self) -> &'static str {
        self.store.backend_name()
    }
}

impl Debug for RememberedEntities {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RememberedEntities")
            .field("start_batch_size", &self.start_batch_size)
            .field("start_batch_delay", &self.start_batch_delay)
            .field("store", &self.store_backend())
            .finish_non_exhaustive()
    }
}

/// Replay controls for remembered entity activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RememberedEntityReplaySettings {
    start_batch_size: usize,
    start_batch_delay: Duration,
}

impl RememberedEntityReplaySettings {
    /// Default remembered entity replay batch size.
    pub const DEFAULT_START_BATCH_SIZE: usize = 64;

    /// Creates replay settings.
    #[must_use]
    pub fn new(start_batch_size: usize, start_batch_delay: Duration) -> Self {
        Self {
            start_batch_size: start_batch_size.max(1),
            start_batch_delay,
        }
    }

    /// Maximum remembered ids started before yielding.
    #[must_use]
    pub const fn start_batch_size(&self) -> usize {
        self.start_batch_size
    }

    /// Delay inserted between remembered replay batches.
    #[must_use]
    pub const fn start_batch_delay(&self) -> Duration {
        self.start_batch_delay
    }
}

impl Default for RememberedEntityReplaySettings {
    fn default() -> Self {
        Self::new(Self::DEFAULT_START_BATCH_SIZE, Duration::ZERO)
    }
}

/// Summary of one remembered entity replay attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RememberedEntityReplay {
    shard: ShardKey,
    loaded: usize,
    activated: usize,
    failed: usize,
}

impl RememberedEntityReplay {
    /// Creates a remembered replay summary.
    #[must_use]
    pub const fn new(shard: ShardKey, loaded: usize, activated: usize, failed: usize) -> Self {
        Self {
            shard,
            loaded,
            activated,
            failed,
        }
    }

    /// Shard that was replayed.
    #[must_use]
    pub const fn shard(&self) -> &ShardKey {
        &self.shard
    }

    /// Number of ids loaded from the remembered store.
    #[must_use]
    pub const fn loaded(&self) -> usize {
        self.loaded
    }

    /// Number of ids successfully activated locally.
    #[must_use]
    pub const fn activated(&self) -> usize {
        self.activated
    }

    /// Number of ids that failed local activation.
    #[must_use]
    pub const fn failed(&self) -> usize {
        self.failed
    }
}

/// In-memory remembered entity store for tests and single-process examples.
#[derive(Clone, Default)]
pub struct InMemoryRememberedEntityStore {
    entities: Arc<Mutex<BTreeMap<ShardKey, BTreeSet<EntityId>>>>,
}

impl InMemoryRememberedEntityStore {
    /// Creates an empty in-memory remembered entity store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of remembered ids across every shard.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entities
            .lock()
            .expect("in-memory remembered entity store mutex poisoned")
            .values()
            .map(BTreeSet::len)
            .sum()
    }

    /// Returns true when no entity ids are remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of remembered ids for one shard.
    #[must_use]
    pub fn len_for_shard(&self, shard: &ShardKey) -> usize {
        self.entities
            .lock()
            .expect("in-memory remembered entity store mutex poisoned")
            .get(shard)
            .map_or(0, BTreeSet::len)
    }
}

impl Debug for InMemoryRememberedEntityStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryRememberedEntityStore")
            .field("entities", &self.len())
            .finish_non_exhaustive()
    }
}

impl RememberedEntityStore for InMemoryRememberedEntityStore {
    fn backend_name(&self) -> &'static str {
        "in-memory"
    }

    fn remember<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, ()> {
        Box::pin(async move {
            self.entities
                .lock()
                .expect("in-memory remembered entity store mutex poisoned")
                .entry(shard.clone())
                .or_default()
                .insert(entity_id.clone());
            Ok(())
        })
    }

    fn forget<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, bool> {
        Box::pin(async move {
            let mut entities = self
                .entities
                .lock()
                .expect("in-memory remembered entity store mutex poisoned");
            let removed = entities
                .get_mut(shard)
                .is_some_and(|ids| ids.remove(entity_id));
            if entities.get(shard).is_some_and(BTreeSet::is_empty) {
                entities.remove(shard);
            }
            Ok(removed)
        })
    }

    fn remembered_for_shard<'a>(
        &'a self,
        shard: &'a ShardKey,
    ) -> RememberedStoreFuture<'a, Vec<EntityId>> {
        Box::pin(async move {
            Ok(self
                .entities
                .lock()
                .expect("in-memory remembered entity store mutex poisoned")
                .get(shard)
                .map(|ids| ids.iter().cloned().collect())
                .unwrap_or_default())
        })
    }
}
