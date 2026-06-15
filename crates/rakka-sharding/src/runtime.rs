//! Cluster/sharding runtime facade for deterministic ownership refresh.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rakka_cluster::{
    ClusterError, ClusterMembership, DiscoveryProvider, DiscoverySnapshot, MembershipEvent,
    MembershipState, NodeId,
};
use rakka_core::Message;

use crate::{
    AsyncShardCoordinatorStore, EntityType, LeaseToken, PersistedShardCoordinatorState,
    ShardAllocationStrategy, ShardCoordinator, ShardCoordinatorLease, ShardCoordinatorStore,
    ShardDecision, ShardHandoff, ShardHandoffState, ShardMoveReason, ShardOwnershipSnapshot,
    ShardRebalancePlan, ShardRegion, ShardingConfig, ShardingError, ShardingResult,
};

/// Convenient result alias for cluster/sharding runtime operations.
pub type ClusterShardingResult<T> = Result<T, ClusterShardingError>;

/// Failure returned by the cluster/sharding runtime facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterShardingError {
    /// Membership or discovery operation failed.
    Cluster {
        /// Cluster failure.
        error: ClusterError,
    },
    /// Sharding operation failed.
    Sharding {
        /// Sharding failure.
        error: ShardingError,
    },
    /// Entity type was registered with a different shard count.
    EntityTypeConfigMismatch {
        /// Entity type.
        entity_type: EntityType,
        /// Existing number of shards.
        expected_shards: u32,
        /// Requested number of shards.
        actual_shards: u32,
    },
    /// Entity type has not been initialized in this sharding facade.
    EntityTypeNotRegistered {
        /// Entity type.
        entity_type: EntityType,
    },
    /// Entity type was initialized for a different message protocol.
    EntityTypeMessageMismatch {
        /// Entity type.
        entity_type: EntityType,
    },
    /// The facade runtime is currently held by an async operation.
    RuntimeBusy,
}

impl Display for ClusterShardingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cluster { error } => Display::fmt(error, f),
            Self::Sharding { error } => Display::fmt(error, f),
            Self::EntityTypeConfigMismatch {
                entity_type,
                expected_shards,
                actual_shards,
            } => write!(
                f,
                "entity type {entity_type} was registered with {actual_shards} shards; expected {expected_shards}"
            ),
            Self::EntityTypeNotRegistered { entity_type } => {
                write!(f, "entity type {entity_type} has not been initialized")
            }
            Self::EntityTypeMessageMismatch { entity_type } => write!(
                f,
                "entity type {entity_type} was initialized for a different message protocol"
            ),
            Self::RuntimeBusy => write!(
                f,
                "cluster sharding runtime is busy; retry or use the async sharding API"
            ),
        }
    }
}

impl Error for ClusterShardingError {}

impl From<ClusterError> for ClusterShardingError {
    fn from(error: ClusterError) -> Self {
        Self::Cluster { error }
    }
}

impl From<ShardingError> for ClusterShardingError {
    fn from(error: ShardingError) -> Self {
        Self::Sharding { error }
    }
}

/// Rebalance result for one entity type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityShardRebalance {
    entity_type: EntityType,
    plan: ShardRebalancePlan,
}

impl EntityShardRebalance {
    /// Creates an entity rebalance result.
    #[must_use]
    pub fn new(entity_type: EntityType, plan: ShardRebalancePlan) -> Self {
        Self { entity_type, plan }
    }

    /// Entity type reconciled by this result.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Applied rebalance plan.
    #[must_use]
    pub const fn plan(&self) -> &ShardRebalancePlan {
        &self.plan
    }
}

/// Result of one cluster/sharding runtime update.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterShardingUpdate {
    membership_events: Vec<MembershipEvent>,
    handoffs: Vec<ShardHandoff>,
    rebalances: Vec<EntityShardRebalance>,
}

impl ClusterShardingUpdate {
    /// Creates a runtime update from membership events and rebalance plans.
    #[must_use]
    pub fn new(
        membership_events: Vec<MembershipEvent>,
        handoffs: Vec<ShardHandoff>,
        rebalances: Vec<EntityShardRebalance>,
    ) -> Self {
        Self {
            membership_events,
            handoffs,
            rebalances,
        }
    }

    /// Membership events applied during this update.
    #[must_use]
    pub fn membership_events(&self) -> &[MembershipEvent] {
        &self.membership_events
    }

    /// Graceful shard handoff steps applied during this update.
    #[must_use]
    pub fn handoffs(&self) -> &[ShardHandoff] {
        &self.handoffs
    }

    /// Shard rebalances applied during this update.
    #[must_use]
    pub fn rebalances(&self) -> &[EntityShardRebalance] {
        &self.rebalances
    }

    /// Returns true when the update did not change membership or ownership.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.membership_events.is_empty() && self.handoffs.is_empty() && self.rebalances.is_empty()
    }
}

#[derive(Clone)]
enum CoordinatorStoreMode {
    None,
    Sync(Arc<dyn ShardCoordinatorStore>),
    Async(Arc<dyn AsyncShardCoordinatorStore>),
}

impl CoordinatorStoreMode {
    fn backend_name(&self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Sync(store) => Some(ShardCoordinatorStore::backend_name(store.as_ref())),
            Self::Async(store) => Some(AsyncShardCoordinatorStore::backend_name(store.as_ref())),
        }
    }

    const fn is_async_only(&self) -> bool {
        matches!(self, Self::Async(_))
    }

    fn load_sync(
        &self,
        entity_type: &EntityType,
    ) -> ClusterShardingResult<Option<PersistedShardCoordinatorState>> {
        match self {
            Self::None => Ok(None),
            Self::Sync(store) => Ok(ShardCoordinatorStore::load(store.as_ref(), entity_type)?),
            Self::Async(store) => Err(async_store_requires_async_api(
                AsyncShardCoordinatorStore::backend_name(store.as_ref()),
            )),
        }
    }

    async fn load_async(
        &self,
        entity_type: &EntityType,
    ) -> ClusterShardingResult<Option<PersistedShardCoordinatorState>> {
        match self {
            Self::None => Ok(None),
            Self::Sync(store) => Ok(ShardCoordinatorStore::load(store.as_ref(), entity_type)?),
            Self::Async(store) => {
                Ok(AsyncShardCoordinatorStore::load(store.as_ref(), entity_type).await?)
            }
        }
    }

    fn compare_and_set_sync(
        &self,
        entity_type: &EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
        lease_token: Option<&LeaseToken>,
    ) -> ClusterShardingResult<()> {
        match self {
            Self::None => Ok(()),
            Self::Sync(store) => {
                ShardCoordinatorStore::compare_and_set_with_lease(
                    store.as_ref(),
                    entity_type,
                    expected_revision,
                    state,
                    lease_token,
                )?;
                Ok(())
            }
            Self::Async(store) => Err(async_store_requires_async_api(
                AsyncShardCoordinatorStore::backend_name(store.as_ref()),
            )),
        }
    }

    async fn compare_and_set_async(
        &self,
        entity_type: &EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
        lease_token: Option<&LeaseToken>,
    ) -> ClusterShardingResult<()> {
        match self {
            Self::None => Ok(()),
            Self::Sync(store) => {
                ShardCoordinatorStore::compare_and_set_with_lease(
                    store.as_ref(),
                    entity_type,
                    expected_revision,
                    state,
                    lease_token,
                )?;
                Ok(())
            }
            Self::Async(store) => {
                AsyncShardCoordinatorStore::compare_and_set_with_lease(
                    store.as_ref(),
                    entity_type,
                    expected_revision,
                    state,
                    lease_token,
                )
                .await?;
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
enum CoordinatorLeaseMode {
    None,
    Async(Arc<dyn ShardCoordinatorLease>),
}

impl CoordinatorLeaseMode {
    fn lease_name(&self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Async(lease) => Some(ShardCoordinatorLease::lease_name(lease.as_ref())),
        }
    }

    const fn requires_async_api(&self) -> bool {
        matches!(self, Self::Async(_))
    }

    fn ensure_sync_api(&self) -> ClusterShardingResult<()> {
        match self {
            Self::None => Ok(()),
            Self::Async(lease) => Err(async_lease_requires_async_api(
                ShardCoordinatorLease::lease_name(lease.as_ref()),
            )),
        }
    }
}

/// Deterministic runtime facade that connects membership and shard ownership refresh.
pub struct ClusterShardingRuntime {
    membership: ClusterMembership,
    coordinators: BTreeMap<EntityType, ShardCoordinator>,
    regions: BTreeMap<EntityType, Vec<Arc<dyn RegisteredShardRegion>>>,
    coordinator_store: CoordinatorStoreMode,
    coordinator_lease: CoordinatorLeaseMode,
    lease_tokens: BTreeMap<EntityType, LeaseToken>,
}

impl ClusterShardingRuntime {
    /// Creates a runtime facade from a membership table.
    #[must_use]
    pub fn new(membership: ClusterMembership) -> Self {
        Self {
            membership,
            coordinators: BTreeMap::new(),
            regions: BTreeMap::new(),
            coordinator_store: CoordinatorStoreMode::None,
            coordinator_lease: CoordinatorLeaseMode::None,
            lease_tokens: BTreeMap::new(),
        }
    }

    /// Creates a runtime facade with a durable shard coordinator store.
    #[must_use]
    pub fn with_coordinator_store(
        membership: ClusterMembership,
        coordinator_store: impl ShardCoordinatorStore,
    ) -> Self {
        Self::with_coordinator_store_ref(membership, Arc::new(coordinator_store))
    }

    /// Creates a runtime facade with a shared durable shard coordinator store.
    #[must_use]
    pub fn with_coordinator_store_ref(
        membership: ClusterMembership,
        coordinator_store: Arc<dyn ShardCoordinatorStore>,
    ) -> Self {
        Self {
            membership,
            coordinators: BTreeMap::new(),
            regions: BTreeMap::new(),
            coordinator_store: CoordinatorStoreMode::Sync(coordinator_store),
            coordinator_lease: CoordinatorLeaseMode::None,
            lease_tokens: BTreeMap::new(),
        }
    }

    /// Creates a runtime facade with an async durable shard coordinator store.
    #[must_use]
    pub fn with_async_coordinator_store(
        membership: ClusterMembership,
        coordinator_store: impl AsyncShardCoordinatorStore,
    ) -> Self {
        Self::with_async_coordinator_store_ref(membership, Arc::new(coordinator_store))
    }

    /// Creates a runtime facade with a shared async durable shard coordinator store.
    #[must_use]
    pub fn with_async_coordinator_store_ref(
        membership: ClusterMembership,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
    ) -> Self {
        Self {
            membership,
            coordinators: BTreeMap::new(),
            regions: BTreeMap::new(),
            coordinator_store: CoordinatorStoreMode::Async(coordinator_store),
            coordinator_lease: CoordinatorLeaseMode::None,
            lease_tokens: BTreeMap::new(),
        }
    }

    /// Adds an async coordinator leadership lease backend to this runtime.
    #[must_use]
    pub fn with_coordinator_lease(self, coordinator_lease: impl ShardCoordinatorLease) -> Self {
        self.with_coordinator_lease_ref(Arc::new(coordinator_lease))
    }

    /// Adds a shared async coordinator leadership lease backend to this runtime.
    #[must_use]
    pub fn with_coordinator_lease_ref(
        mut self,
        coordinator_lease: Arc<dyn ShardCoordinatorLease>,
    ) -> Self {
        self.coordinator_lease = CoordinatorLeaseMode::Async(coordinator_lease);
        self
    }

    /// Returns the current membership table.
    #[must_use]
    pub const fn membership(&self) -> &ClusterMembership {
        &self.membership
    }

    /// Returns a coordinator by entity type.
    #[must_use]
    pub fn coordinator(&self, entity_type: &EntityType) -> Option<&ShardCoordinator> {
        self.coordinators.get(entity_type)
    }

    /// Durable coordinator store backend name, when configured.
    #[must_use]
    pub fn coordinator_store_backend(&self) -> Option<&'static str> {
        self.coordinator_store.backend_name()
    }

    /// Coordinator leadership lease backend name, when configured.
    #[must_use]
    pub fn coordinator_lease_backend(&self) -> Option<&'static str> {
        self.coordinator_lease.lease_name()
    }

    /// Returns true when the configured coordinator store requires async runtime APIs.
    #[must_use]
    pub const fn coordinator_store_requires_async_api(&self) -> bool {
        self.coordinator_store.is_async_only()
    }

    /// Returns true when the configured coordinator lease requires async runtime APIs.
    #[must_use]
    pub const fn coordinator_lease_requires_async_api(&self) -> bool {
        self.coordinator_lease.requires_async_api()
    }

    /// Returns the current lease token for an entity type, if this node holds one.
    #[must_use]
    pub fn coordinator_lease_token(&self, entity_type: &EntityType) -> Option<&LeaseToken> {
        self.lease_tokens.get(entity_type)
    }

    /// Registers a shard region for ownership refresh.
    pub fn register_region<M>(&mut self, region: ShardRegion<M>) -> ClusterShardingResult<()>
    where
        M: Message,
    {
        let entity_type = region.entity_type().clone();
        let config = region.config().clone();
        let allocation_strategy = region.allocation_strategy();
        self.ensure_coordinator(entity_type.clone(), config, allocation_strategy)?;
        let snapshot = self
            .coordinators
            .get(&entity_type)
            .expect("coordinator must exist after ensure_coordinator")
            .snapshot();
        region
            .refresh_ownership(&snapshot)
            .map_err(|error| ClusterShardingError::Sharding { error })?;
        self.regions
            .entry(entity_type)
            .or_default()
            .push(Arc::new(region));
        Ok(())
    }

    /// Registers a shard region for ownership refresh using async durable storage when configured.
    pub async fn register_region_async<M>(
        &mut self,
        region: ShardRegion<M>,
    ) -> ClusterShardingResult<()>
    where
        M: Message,
    {
        let entity_type = region.entity_type().clone();
        let config = region.config().clone();
        let allocation_strategy = region.allocation_strategy();
        self.ensure_coordinator_async(entity_type.clone(), config, allocation_strategy)
            .await?;
        let snapshot = self
            .coordinators
            .get(&entity_type)
            .expect("coordinator must exist after ensure_coordinator")
            .snapshot();
        region
            .refresh_ownership(&snapshot)
            .map_err(|error| ClusterShardingError::Sharding { error })?;
        self.regions
            .entry(entity_type)
            .or_default()
            .push(Arc::new(region));
        Ok(())
    }

    /// Applies a discovery snapshot, promotes joining members when possible, and refreshes regions.
    pub fn apply_discovery(
        &mut self,
        snapshot: DiscoverySnapshot,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        self.coordinator_lease.ensure_sync_api()?;
        let observed_at_millis = snapshot.observed_at_millis();
        let mut events = self.membership.record_discovery(snapshot)?;
        events.extend(self.promote_joining_members(observed_at_millis)?);
        self.reconcile_and_publish(events)
    }

    /// Applies a discovery snapshot and refreshes regions through async durable storage.
    pub async fn apply_discovery_async(
        &mut self,
        snapshot: DiscoverySnapshot,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let observed_at_millis = snapshot.observed_at_millis();
        let mut events = self.membership.record_discovery(snapshot)?;
        events.extend(self.promote_joining_members(observed_at_millis)?);
        self.reconcile_and_publish_async(events).await
    }

    /// Polls a discovery provider and applies the returned snapshot.
    pub fn poll_discovery(
        &mut self,
        provider: &impl DiscoveryProvider,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let snapshot = provider.discover(observed_at_millis)?;
        self.apply_discovery(snapshot)
    }

    /// Polls a discovery provider and applies the returned snapshot through async storage.
    pub async fn poll_discovery_async(
        &mut self,
        provider: &impl DiscoveryProvider,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let snapshot = provider.discover(observed_at_millis)?;
        self.apply_discovery_async(snapshot).await
    }

    /// Records a heartbeat and refreshes ownership if membership changed.
    pub fn heartbeat(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        self.coordinator_lease.ensure_sync_api()?;
        let events = self
            .membership
            .heartbeat(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish(events)
    }

    /// Records a heartbeat and refreshes ownership through async durable storage.
    pub async fn heartbeat_async(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self
            .membership
            .heartbeat(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish_async(events).await
    }

    /// Begins graceful leave and refreshes ownership.
    pub fn mark_leaving(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        self.coordinator_lease.ensure_sync_api()?;
        let events = self
            .membership
            .mark_leaving(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish(events)
    }

    /// Begins graceful leave and refreshes ownership through async durable storage.
    pub async fn mark_leaving_async(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self
            .membership
            .mark_leaving(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish_async(events).await
    }

    /// Marks a member down and refreshes ownership.
    pub fn mark_down(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        self.coordinator_lease.ensure_sync_api()?;
        let events = self
            .membership
            .mark_down(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish(events)
    }

    /// Marks a member down and refreshes ownership through async durable storage.
    pub async fn mark_down_async(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self
            .membership
            .mark_down(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish_async(events).await
    }

    /// Advances failure detection and refreshes ownership after unreachable/down events.
    pub fn tick(&mut self, now_millis: u64) -> ClusterShardingResult<ClusterShardingUpdate> {
        self.coordinator_lease.ensure_sync_api()?;
        let events = self.membership.tick(now_millis);
        self.reconcile_and_publish(events)
    }

    /// Advances failure detection and refreshes ownership through async durable storage.
    pub async fn tick_async(
        &mut self,
        now_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self.membership.tick(now_millis);
        self.reconcile_and_publish_async(events).await
    }

    /// Renews all currently held coordinator leadership leases.
    pub async fn renew_coordinator_leases_async(&mut self) -> ClusterShardingResult<()> {
        let CoordinatorLeaseMode::Async(lease) = self.coordinator_lease.clone() else {
            return Ok(());
        };
        let tokens = self.lease_tokens.values().cloned().collect::<Vec<_>>();
        for token in tokens {
            if let Err(error) = ShardCoordinatorLease::renew(lease.as_ref(), &token).await {
                self.lease_tokens.remove(token.entity_type());
                return Err(error.into());
            }
        }
        Ok(())
    }

    /// Releases all currently held coordinator leadership leases.
    pub async fn release_coordinator_leases_async(&mut self) -> ClusterShardingResult<()> {
        let CoordinatorLeaseMode::Async(lease) = self.coordinator_lease.clone() else {
            self.lease_tokens.clear();
            return Ok(());
        };
        let tokens = std::mem::take(&mut self.lease_tokens);
        for token in tokens.into_values() {
            ShardCoordinatorLease::release(lease.as_ref(), token).await?;
        }
        Ok(())
    }

    fn ensure_coordinator(
        &mut self,
        entity_type: EntityType,
        config: ShardingConfig,
        allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> ClusterShardingResult<()> {
        self.coordinator_lease.ensure_sync_api()?;
        if let Some(coordinator) = self.coordinators.get(&entity_type) {
            if coordinator.config().number_of_shards() != config.number_of_shards() {
                return Err(ClusterShardingError::EntityTypeConfigMismatch {
                    entity_type,
                    expected_shards: coordinator.config().number_of_shards(),
                    actual_shards: config.number_of_shards(),
                });
            }
            return Ok(());
        }

        let lease_token = self.ensure_coordinator_leadership_sync(&entity_type)?;
        let mut coordinator = match self.coordinator_store.load_sync(&entity_type)? {
            Some(state) => ShardCoordinator::from_snapshot_with_allocation_strategy_ref(
                entity_type.clone(),
                config,
                state.snapshot(),
                allocation_strategy,
            )?,
            None => ShardCoordinator::with_allocation_strategy_ref(
                entity_type.clone(),
                config,
                allocation_strategy,
            ),
        };

        let plan = coordinator.reconcile(&self.membership);
        if !plan.is_empty() {
            let snapshot = coordinator.snapshot();
            self.persist_coordinator_snapshot(
                &snapshot,
                coordinator.allocation_strategy_name(),
                plan.previous_revision(),
                lease_token.as_ref(),
            )?;
        }
        self.coordinators.insert(entity_type, coordinator);
        Ok(())
    }

    async fn ensure_coordinator_async(
        &mut self,
        entity_type: EntityType,
        config: ShardingConfig,
        allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> ClusterShardingResult<()> {
        if let Some(coordinator) = self.coordinators.get(&entity_type) {
            if coordinator.config().number_of_shards() != config.number_of_shards() {
                return Err(ClusterShardingError::EntityTypeConfigMismatch {
                    entity_type,
                    expected_shards: coordinator.config().number_of_shards(),
                    actual_shards: config.number_of_shards(),
                });
            }
            return Ok(());
        }

        let lease_token = self
            .ensure_coordinator_leadership_async(&entity_type)
            .await?;
        let store = self.coordinator_store.clone();
        let mut coordinator = match store.load_async(&entity_type).await? {
            Some(state) => ShardCoordinator::from_snapshot_with_allocation_strategy_ref(
                entity_type.clone(),
                config,
                state.snapshot(),
                allocation_strategy,
            )?,
            None => ShardCoordinator::with_allocation_strategy_ref(
                entity_type.clone(),
                config,
                allocation_strategy,
            ),
        };

        let plan = coordinator.reconcile(&self.membership);
        if !plan.is_empty() {
            let snapshot = coordinator.snapshot();
            self.persist_coordinator_snapshot_async(
                &snapshot,
                coordinator.allocation_strategy_name(),
                plan.previous_revision(),
                lease_token.as_ref(),
            )
            .await?;
        }
        self.coordinators.insert(entity_type, coordinator);
        Ok(())
    }

    fn promote_joining_members(
        &mut self,
        observed_at_millis: u64,
    ) -> Result<Vec<MembershipEvent>, ClusterError> {
        if !self.membership.has_min_contact_points() {
            return Ok(Vec::new());
        }

        let joining = self
            .membership
            .members()
            .filter(|member| member.state() == MembershipState::Joining)
            .map(|member| member.node().id().clone())
            .collect::<Vec<_>>();
        let mut events = Vec::new();
        for node_id in joining {
            if let Some(event) = self.membership.mark_up(&node_id, observed_at_millis)? {
                events.push(event);
            }
        }
        Ok(events)
    }

    fn reconcile_and_publish(
        &mut self,
        membership_events: Vec<MembershipEvent>,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let mut handoffs = Vec::new();
        let mut rebalances = Vec::new();
        let entity_types = self.coordinators.keys().cloned().collect::<Vec<_>>();

        for entity_type in entity_types {
            let lease_token = self.ensure_coordinator_leadership_sync(&entity_type)?;
            let (plan, snapshot, allocation_strategy_name) = {
                let coordinator = self
                    .coordinators
                    .get_mut(&entity_type)
                    .expect("coordinator key was collected from map");
                let plan = coordinator.reconcile(&self.membership);
                (
                    plan,
                    coordinator.snapshot(),
                    coordinator.allocation_strategy_name(),
                )
            };
            if plan.is_empty() {
                continue;
            }

            self.persist_coordinator_snapshot(
                &snapshot,
                allocation_strategy_name,
                plan.previous_revision(),
                lease_token.as_ref(),
            )?;
            handoffs.extend(self.apply_graceful_handoffs(&plan)?);
            self.publish_snapshot(&snapshot)?;
            rebalances.push(EntityShardRebalance::new(entity_type, plan));
        }

        Ok(ClusterShardingUpdate::new(
            membership_events,
            handoffs,
            rebalances,
        ))
    }

    async fn reconcile_and_publish_async(
        &mut self,
        membership_events: Vec<MembershipEvent>,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let mut handoffs = Vec::new();
        let mut rebalances = Vec::new();
        let entity_types = self.coordinators.keys().cloned().collect::<Vec<_>>();

        for entity_type in entity_types {
            let lease_token = self
                .ensure_coordinator_leadership_async(&entity_type)
                .await?;
            let (plan, snapshot, allocation_strategy_name) = {
                let coordinator = self
                    .coordinators
                    .get_mut(&entity_type)
                    .expect("coordinator key was collected from map");
                let plan = coordinator.reconcile(&self.membership);
                (
                    plan,
                    coordinator.snapshot(),
                    coordinator.allocation_strategy_name(),
                )
            };
            if plan.is_empty() {
                continue;
            }

            self.persist_coordinator_snapshot_async(
                &snapshot,
                allocation_strategy_name,
                plan.previous_revision(),
                lease_token.as_ref(),
            )
            .await?;
            handoffs.extend(self.apply_graceful_handoffs(&plan)?);
            self.publish_snapshot(&snapshot)?;
            rebalances.push(EntityShardRebalance::new(entity_type, plan));
        }

        Ok(ClusterShardingUpdate::new(
            membership_events,
            handoffs,
            rebalances,
        ))
    }

    fn persist_coordinator_snapshot(
        &self,
        snapshot: &ShardOwnershipSnapshot,
        allocation_strategy: &str,
        expected_revision: u64,
        lease_token: Option<&LeaseToken>,
    ) -> ClusterShardingResult<()> {
        self.coordinator_store.compare_and_set_sync(
            snapshot.entity_type(),
            expected_revision,
            PersistedShardCoordinatorState::now(snapshot.clone(), allocation_strategy),
            lease_token,
        )
    }

    async fn persist_coordinator_snapshot_async(
        &self,
        snapshot: &ShardOwnershipSnapshot,
        allocation_strategy: &str,
        expected_revision: u64,
        lease_token: Option<&LeaseToken>,
    ) -> ClusterShardingResult<()> {
        let store = self.coordinator_store.clone();
        store
            .compare_and_set_async(
                snapshot.entity_type(),
                expected_revision,
                PersistedShardCoordinatorState::now(snapshot.clone(), allocation_strategy),
                lease_token,
            )
            .await
    }

    fn ensure_coordinator_leadership_sync(
        &self,
        _entity_type: &EntityType,
    ) -> ClusterShardingResult<Option<LeaseToken>> {
        self.coordinator_lease.ensure_sync_api()?;
        Ok(None)
    }

    async fn ensure_coordinator_leadership_async(
        &mut self,
        entity_type: &EntityType,
    ) -> ClusterShardingResult<Option<LeaseToken>> {
        let CoordinatorLeaseMode::Async(lease) = self.coordinator_lease.clone() else {
            return Ok(None);
        };
        let holder = self.membership.local_node_id().clone();
        let existing = self.lease_tokens.get(entity_type).cloned();
        let token = match existing {
            Some(token)
                if token.holder_node() == &holder
                    && !token.is_expired_at(current_timestamp_millis()) =>
            {
                match ShardCoordinatorLease::renew(lease.as_ref(), &token).await {
                    Ok(()) => token,
                    Err(error) => {
                        self.lease_tokens.remove(entity_type);
                        return Err(error.into());
                    }
                }
            }
            _ => ShardCoordinatorLease::acquire(lease.as_ref(), entity_type, &holder).await?,
        };
        self.lease_tokens.insert(entity_type.clone(), token);
        Ok(self.lease_tokens.get(entity_type).cloned())
    }

    fn publish_snapshot(&self, snapshot: &ShardOwnershipSnapshot) -> ClusterShardingResult<()> {
        if let Some(regions) = self.regions.get(snapshot.entity_type()) {
            for region in regions {
                region.refresh_ownership(snapshot)?;
            }
        }
        Ok(())
    }

    fn apply_graceful_handoffs(
        &self,
        plan: &ShardRebalancePlan,
    ) -> ClusterShardingResult<Vec<ShardHandoff>> {
        let mut handoffs = Vec::new();

        for decision in plan.decisions() {
            let ShardDecision::Move {
                shard,
                from,
                to,
                reason: ShardMoveReason::GracefulLeave,
            } = decision
            else {
                continue;
            };

            let shard_id = shard.shard_id();
            let draining_stopped =
                self.begin_handoff_on_node(shard.entity_type(), from, shard_id)?;
            handoffs.push(ShardHandoff::new(
                shard.clone(),
                from.clone(),
                to.clone(),
                ShardMoveReason::GracefulLeave,
                ShardHandoffState::Draining,
                draining_stopped,
            ));

            let transferring_stopped =
                self.complete_handoff_on_node(shard.entity_type(), from, shard_id)?;
            handoffs.push(ShardHandoff::new(
                shard.clone(),
                from.clone(),
                to.clone(),
                ShardMoveReason::GracefulLeave,
                ShardHandoffState::Transferring,
                transferring_stopped,
            ));

            let acquired_stopped =
                self.acquire_handoff_on_node(shard.entity_type(), to, shard_id)?;
            handoffs.push(ShardHandoff::new(
                shard.clone(),
                from.clone(),
                to.clone(),
                ShardMoveReason::GracefulLeave,
                ShardHandoffState::Acquired,
                acquired_stopped,
            ));
        }

        Ok(handoffs)
    }

    fn begin_handoff_on_node(
        &self,
        entity_type: &EntityType,
        node_id: &NodeId,
        shard_id: crate::ShardId,
    ) -> ClusterShardingResult<usize> {
        self.apply_to_node_regions(entity_type, node_id, |region| {
            region.begin_shard_handoff(shard_id)
        })
    }

    fn complete_handoff_on_node(
        &self,
        entity_type: &EntityType,
        node_id: &NodeId,
        shard_id: crate::ShardId,
    ) -> ClusterShardingResult<usize> {
        self.apply_to_node_regions(entity_type, node_id, |region| {
            region.complete_shard_handoff(shard_id)
        })
    }

    fn acquire_handoff_on_node(
        &self,
        entity_type: &EntityType,
        node_id: &NodeId,
        shard_id: crate::ShardId,
    ) -> ClusterShardingResult<usize> {
        self.apply_to_node_regions(entity_type, node_id, |region| {
            region.acquire_shard(shard_id)
        })
    }

    fn apply_to_node_regions(
        &self,
        entity_type: &EntityType,
        node_id: &NodeId,
        mut apply: impl FnMut(&dyn RegisteredShardRegion) -> ShardingResult<usize>,
    ) -> ClusterShardingResult<usize> {
        let mut applied = 0;
        if let Some(regions) = self.regions.get(entity_type) {
            for region in regions {
                if region
                    .local_node_id()
                    .is_some_and(|local_node_id| local_node_id == node_id)
                {
                    applied += apply(region.as_ref())?;
                }
            }
        }
        Ok(applied)
    }
}

fn async_store_requires_async_api(backend: &str) -> ClusterShardingError {
    ShardingError::AsyncCoordinatorStoreRequiresAsyncApi {
        backend: backend.to_string(),
    }
    .into()
}

fn async_lease_requires_async_api(lease: &str) -> ClusterShardingError {
    ShardingError::AsyncCoordinatorLeaseRequiresAsyncApi {
        lease: lease.to_string(),
    }
    .into()
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

impl std::fmt::Debug for ClusterShardingRuntime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterShardingRuntime")
            .field("membership_revision", &self.membership.revision())
            .field("coordinator_count", &self.coordinators.len())
            .field("region_group_count", &self.regions.len())
            .field("coordinator_store", &self.coordinator_store_backend())
            .field("coordinator_lease", &self.coordinator_lease_backend())
            .field("held_lease_count", &self.lease_tokens.len())
            .finish_non_exhaustive()
    }
}

trait RegisteredShardRegion: Send + Sync {
    fn local_node_id(&self) -> Option<&NodeId>;

    fn refresh_ownership(&self, snapshot: &ShardOwnershipSnapshot) -> ShardingResult<()>;

    fn begin_shard_handoff(&self, shard_id: crate::ShardId) -> ShardingResult<usize>;

    fn complete_shard_handoff(&self, shard_id: crate::ShardId) -> ShardingResult<usize>;

    fn acquire_shard(&self, shard_id: crate::ShardId) -> ShardingResult<usize>;
}

impl<M> RegisteredShardRegion for ShardRegion<M>
where
    M: Message,
{
    fn local_node_id(&self) -> Option<&NodeId> {
        ShardRegion::local_node_id(self)
    }

    fn refresh_ownership(&self, snapshot: &ShardOwnershipSnapshot) -> ShardingResult<()> {
        ShardRegion::refresh_ownership(self, snapshot)
    }

    fn begin_shard_handoff(&self, shard_id: crate::ShardId) -> ShardingResult<usize> {
        ShardRegion::begin_shard_handoff(self, shard_id)
    }

    fn complete_shard_handoff(&self, shard_id: crate::ShardId) -> ShardingResult<usize> {
        ShardRegion::complete_shard_handoff(self, shard_id)
    }

    fn acquire_shard(&self, shard_id: crate::ShardId) -> ShardingResult<usize> {
        ShardRegion::acquire_shard(self, shard_id)
    }
}
