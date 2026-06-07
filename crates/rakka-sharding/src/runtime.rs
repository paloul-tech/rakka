//! Cluster/sharding runtime facade for deterministic ownership refresh.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_cluster::{
    ClusterError, ClusterMembership, DiscoveryProvider, DiscoverySnapshot, MembershipEvent,
    MembershipState, NodeId,
};
use rakka_core::Message;

use crate::{
    EntityType, ShardCoordinator, ShardDecision, ShardHandoff, ShardHandoffState, ShardMoveReason,
    ShardOwnershipSnapshot, ShardRebalancePlan, ShardRegion, ShardingConfig, ShardingError,
    ShardingResult,
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

/// Deterministic runtime facade that connects membership and shard ownership refresh.
pub struct ClusterShardingRuntime {
    membership: ClusterMembership,
    coordinators: BTreeMap<EntityType, ShardCoordinator>,
    regions: BTreeMap<EntityType, Vec<Arc<dyn RegisteredShardRegion>>>,
}

impl ClusterShardingRuntime {
    /// Creates a runtime facade from a membership table.
    #[must_use]
    pub fn new(membership: ClusterMembership) -> Self {
        Self {
            membership,
            coordinators: BTreeMap::new(),
            regions: BTreeMap::new(),
        }
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

    /// Registers a shard region for ownership refresh.
    pub fn register_region<M>(&mut self, region: ShardRegion<M>) -> ClusterShardingResult<()>
    where
        M: Message,
    {
        let entity_type = region.entity_type().clone();
        let config = region.config().clone();
        self.ensure_coordinator(entity_type.clone(), config)?;
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
        let observed_at_millis = snapshot.observed_at_millis();
        let mut events = self.membership.record_discovery(snapshot)?;
        events.extend(self.promote_joining_members(observed_at_millis)?);
        self.reconcile_and_publish(events)
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

    /// Records a heartbeat and refreshes ownership if membership changed.
    pub fn heartbeat(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self
            .membership
            .heartbeat(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish(events)
    }

    /// Begins graceful leave and refreshes ownership.
    pub fn mark_leaving(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self
            .membership
            .mark_leaving(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish(events)
    }

    /// Marks a member down and refreshes ownership.
    pub fn mark_down(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self
            .membership
            .mark_down(node_id, observed_at_millis)?
            .into_iter()
            .collect();
        self.reconcile_and_publish(events)
    }

    /// Advances failure detection and refreshes ownership after unreachable/down events.
    pub fn tick(&mut self, now_millis: u64) -> ClusterShardingResult<ClusterShardingUpdate> {
        let events = self.membership.tick(now_millis);
        self.reconcile_and_publish(events)
    }

    fn ensure_coordinator(
        &mut self,
        entity_type: EntityType,
        config: ShardingConfig,
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

        let mut coordinator = ShardCoordinator::new(entity_type.clone(), config);
        coordinator.reconcile(&self.membership);
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
            let plan = {
                let coordinator = self
                    .coordinators
                    .get_mut(&entity_type)
                    .expect("coordinator key was collected from map");
                coordinator.reconcile(&self.membership)
            };
            if plan.is_empty() {
                continue;
            }

            handoffs.extend(self.apply_graceful_handoffs(&plan)?);
            let snapshot = self
                .coordinators
                .get(&entity_type)
                .expect("coordinator key was collected from map")
                .snapshot();
            self.publish_snapshot(&snapshot)?;
            rebalances.push(EntityShardRebalance::new(entity_type, plan));
        }

        Ok(ClusterShardingUpdate::new(
            membership_events,
            handoffs,
            rebalances,
        ))
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

impl std::fmt::Debug for ClusterShardingRuntime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterShardingRuntime")
            .field("membership_revision", &self.membership.revision())
            .field("coordinator_count", &self.coordinators.len())
            .field("region_group_count", &self.regions.len())
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
