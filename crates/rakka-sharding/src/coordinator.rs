//! Shard ownership coordinator model.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rakka_cluster::{ClusterMembership, MembershipState, NodeId};
use rakka_core::{MetricsRecorder, METRIC_SHARD_OWNERSHIP_COUNT};
use serde::{Deserialize, Serialize};

use crate::allocation::{
    DeterministicModuloShardAllocationStrategy, ShardAllocationContext, ShardAllocationStrategy,
    ShardRebalanceContext,
};
use crate::error::{ShardingError, ShardingResult};
use crate::identity::{EntityId, EntityType, ShardId, ShardKey, ShardingConfig};

/// Ownership assignment for one shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAssignment {
    shard: ShardKey,
    owner: NodeId,
}

impl ShardAssignment {
    /// Creates a shard assignment.
    #[must_use]
    pub fn new(shard: ShardKey, owner: NodeId) -> Self {
        Self { shard, owner }
    }

    /// Shard key.
    #[must_use]
    pub fn shard(&self) -> &ShardKey {
        &self.shard
    }

    /// Owning node id.
    #[must_use]
    pub fn owner(&self) -> &NodeId {
        &self.owner
    }
}

/// Reason a shard ownership change was decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShardMoveReason {
    /// Shard had no previous owner.
    InitialAllocation,
    /// Shard moved to improve balance after membership changed.
    Rebalance,
    /// Previous owner is leaving gracefully and should hand off ownership.
    GracefulLeave,
    /// Previous owner is unavailable, down, removed, or absent.
    OwnerUnavailable,
    /// No routable members are available to own the shard.
    NoRoutableMembers,
}

/// Coordinator decision produced by reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardDecision {
    /// Assign an unowned shard.
    Assign {
        /// Shard key.
        shard: ShardKey,
        /// New owner.
        to: NodeId,
        /// Decision reason.
        reason: ShardMoveReason,
    },
    /// Move an owned shard from one node to another.
    Move {
        /// Shard key.
        shard: ShardKey,
        /// Previous owner.
        from: NodeId,
        /// New owner.
        to: NodeId,
        /// Decision reason.
        reason: ShardMoveReason,
    },
    /// Remove ownership when no routable member can accept the shard.
    Unassign {
        /// Shard key.
        shard: ShardKey,
        /// Previous owner.
        from: NodeId,
        /// Decision reason.
        reason: ShardMoveReason,
    },
}

impl ShardDecision {
    /// Shard affected by this decision.
    #[must_use]
    pub fn shard(&self) -> &ShardKey {
        match self {
            Self::Assign { shard, .. }
            | Self::Move { shard, .. }
            | Self::Unassign { shard, .. } => shard,
        }
    }

    /// Decision reason.
    #[must_use]
    pub const fn reason(&self) -> ShardMoveReason {
        match self {
            Self::Assign { reason, .. }
            | Self::Move { reason, .. }
            | Self::Unassign { reason, .. } => *reason,
        }
    }
}

/// Stable snapshot of shard ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardOwnershipSnapshot {
    revision: u64,
    entity_type: EntityType,
    number_of_shards: u32,
    assignments: Vec<ShardAssignment>,
}

impl ShardOwnershipSnapshot {
    /// Current coordinator revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Entity type this coordinator owns.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Number of shards in the entity type namespace.
    #[must_use]
    pub const fn number_of_shards(&self) -> u32 {
        self.number_of_shards
    }

    /// Shard assignments sorted by shard id.
    #[must_use]
    pub fn assignments(&self) -> &[ShardAssignment] {
        &self.assignments
    }

    /// Counts shards currently owned by `owner`.
    #[must_use]
    pub fn owned_shard_count(&self, owner: &NodeId) -> usize {
        self.assignments
            .iter()
            .filter(|assignment| assignment.owner() == owner)
            .count()
    }

    /// Counts owned shards grouped by node id.
    #[must_use]
    pub fn owner_counts(&self) -> Vec<ShardOwnerCount> {
        let mut counts = BTreeMap::<NodeId, usize>::new();
        for assignment in &self.assignments {
            *counts.entry(assignment.owner().clone()).or_default() += 1;
        }

        counts
            .into_iter()
            .map(|(owner, count)| ShardOwnerCount::new(owner, count))
            .collect()
    }

    /// Records shard ownership gauges by owner.
    pub fn record_metrics(&self, recorder: &dyn MetricsRecorder) -> Vec<ShardOwnerCount> {
        let counts = self.owner_counts();
        let entity_type = self.entity_type().to_string();
        let revision = self.revision().to_string();
        for owner_count in &counts {
            let owner = owner_count.owner().to_string();
            recorder.record_gauge(
                METRIC_SHARD_OWNERSHIP_COUNT,
                owner_count.count() as f64,
                &[
                    ("entity_type", entity_type.as_str()),
                    ("owner", owner.as_str()),
                    ("revision", revision.as_str()),
                ],
            );
        }
        counts
    }
}

/// Count of shards owned by one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardOwnerCount {
    owner: NodeId,
    count: usize,
}

impl ShardOwnerCount {
    /// Creates a shard owner count.
    #[must_use]
    pub const fn new(owner: NodeId, count: usize) -> Self {
        Self { owner, count }
    }

    /// Owning node id.
    #[must_use]
    pub const fn owner(&self) -> &NodeId {
        &self.owner
    }

    /// Number of shards owned by this node.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }
}

/// Result of reconciling shard ownership with cluster membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRebalancePlan {
    previous_revision: u64,
    new_revision: u64,
    decisions: Vec<ShardDecision>,
}

impl ShardRebalancePlan {
    /// Coordinator revision before reconciliation.
    #[must_use]
    pub const fn previous_revision(&self) -> u64 {
        self.previous_revision
    }

    /// Coordinator revision after reconciliation.
    #[must_use]
    pub const fn new_revision(&self) -> u64 {
        self.new_revision
    }

    /// Decisions applied during reconciliation.
    #[must_use]
    pub fn decisions(&self) -> &[ShardDecision] {
        &self.decisions
    }

    /// Returns true when no ownership changes were needed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.decisions.is_empty()
    }
}

/// Rakka-owned shard coordinator for one entity type.
#[derive(Debug, Clone)]
pub struct ShardCoordinator {
    entity_type: EntityType,
    config: ShardingConfig,
    assignments: BTreeMap<ShardId, NodeId>,
    revision: u64,
    allocation_strategy: Arc<dyn ShardAllocationStrategy>,
}

impl ShardCoordinator {
    /// Creates a coordinator for one entity type.
    #[must_use]
    pub fn new(entity_type: EntityType, config: ShardingConfig) -> Self {
        Self::with_allocation_strategy(
            entity_type,
            config,
            DeterministicModuloShardAllocationStrategy,
        )
    }

    /// Creates a coordinator for one entity type with a custom allocation strategy.
    #[must_use]
    pub fn with_allocation_strategy(
        entity_type: EntityType,
        config: ShardingConfig,
        allocation_strategy: impl ShardAllocationStrategy,
    ) -> Self {
        Self::with_allocation_strategy_ref(entity_type, config, Arc::new(allocation_strategy))
    }

    /// Creates a coordinator for one entity type with a shared allocation strategy.
    #[must_use]
    pub fn with_allocation_strategy_ref(
        entity_type: EntityType,
        config: ShardingConfig,
        allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Self {
        Self {
            entity_type,
            config,
            assignments: BTreeMap::new(),
            revision: 0,
            allocation_strategy,
        }
    }

    /// Restores a coordinator from a stable ownership snapshot.
    pub fn from_snapshot(
        entity_type: EntityType,
        config: ShardingConfig,
        snapshot: &ShardOwnershipSnapshot,
    ) -> ShardingResult<Self> {
        Self::from_snapshot_with_allocation_strategy(
            entity_type,
            config,
            snapshot,
            DeterministicModuloShardAllocationStrategy,
        )
    }

    /// Restores a coordinator from a stable ownership snapshot with a custom allocation strategy.
    pub fn from_snapshot_with_allocation_strategy(
        entity_type: EntityType,
        config: ShardingConfig,
        snapshot: &ShardOwnershipSnapshot,
        allocation_strategy: impl ShardAllocationStrategy,
    ) -> ShardingResult<Self> {
        Self::from_snapshot_with_allocation_strategy_ref(
            entity_type,
            config,
            snapshot,
            Arc::new(allocation_strategy),
        )
    }

    /// Restores a coordinator from a stable ownership snapshot with a shared allocation strategy.
    pub fn from_snapshot_with_allocation_strategy_ref(
        entity_type: EntityType,
        config: ShardingConfig,
        snapshot: &ShardOwnershipSnapshot,
        allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> ShardingResult<Self> {
        if snapshot.entity_type() != &entity_type
            || snapshot.number_of_shards() != config.number_of_shards()
        {
            return Err(ShardingError::PersistedCoordinatorSnapshotMismatch {
                expected_entity_type: entity_type,
                actual_entity_type: snapshot.entity_type().clone(),
                expected_shards: config.number_of_shards(),
                actual_shards: snapshot.number_of_shards(),
            });
        }

        let mut assignments = BTreeMap::new();
        for assignment in snapshot.assignments() {
            let shard = assignment.shard();
            if shard.entity_type() != &entity_type {
                return Err(ShardingError::PersistedCoordinatorSnapshotMismatch {
                    expected_entity_type: entity_type,
                    actual_entity_type: shard.entity_type().clone(),
                    expected_shards: config.number_of_shards(),
                    actual_shards: snapshot.number_of_shards(),
                });
            }

            let shard_id = shard.shard_id();
            if !config.contains_shard(shard_id) {
                return Err(ShardingError::UnknownShard {
                    shard_id,
                    number_of_shards: config.number_of_shards(),
                });
            }

            assignments.insert(shard_id, assignment.owner().clone());
        }

        Ok(Self {
            entity_type,
            config,
            assignments,
            revision: snapshot.revision(),
            allocation_strategy,
        })
    }

    /// Entity type coordinated by this instance.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Sharding configuration.
    #[must_use]
    pub const fn config(&self) -> &ShardingConfig {
        &self.config
    }

    /// Current coordinator revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Allocation strategy used by this coordinator.
    #[must_use]
    pub fn allocation_strategy(&self) -> Arc<dyn ShardAllocationStrategy> {
        self.allocation_strategy.clone()
    }

    /// Stable allocation strategy name used for diagnostics.
    #[must_use]
    pub fn allocation_strategy_name(&self) -> &'static str {
        self.allocation_strategy.strategy_name()
    }

    /// Computes the shard id for an entity id.
    #[must_use]
    pub fn shard_for_entity(&self, entity_id: &EntityId) -> ShardId {
        ShardId::for_entity(&self.entity_type, entity_id, &self.config)
    }

    /// Returns the owner for a shard.
    pub fn owner_for_shard(&self, shard_id: ShardId) -> ShardingResult<&NodeId> {
        self.ensure_known_shard(shard_id)?;
        self.assignments
            .get(&shard_id)
            .ok_or_else(|| ShardingError::NoShardOwner {
                entity_type: self.entity_type.clone(),
                shard_id,
            })
    }

    /// Returns the owner for an entity id.
    pub fn owner_for_entity(&self, entity_id: &EntityId) -> ShardingResult<&NodeId> {
        let shard_id = self.shard_for_entity(entity_id);
        self.assignments
            .get(&shard_id)
            .ok_or_else(|| ShardingError::NoEntityOwner {
                entity_type: self.entity_type.clone(),
                entity_id: entity_id.clone(),
                shard_id,
            })
    }

    /// Reconciles shard ownership against current cluster membership.
    pub fn reconcile(&mut self, membership: &ClusterMembership) -> ShardRebalancePlan {
        let previous_revision = self.revision;
        let routable_nodes = membership
            .routable_members()
            .into_iter()
            .map(|member| member.node().id().clone())
            .collect::<Vec<_>>();
        let mut decisions = Vec::new();

        if routable_nodes.is_empty() {
            for (shard_id, from) in std::mem::take(&mut self.assignments) {
                decisions.push(ShardDecision::Unassign {
                    shard: self.shard_key(shard_id),
                    from,
                    reason: ShardMoveReason::NoRoutableMembers,
                });
            }
            self.bump_revision_if_changed(&decisions);
            return ShardRebalancePlan {
                previous_revision,
                new_revision: self.revision,
                decisions,
            };
        }

        let mut assignments = self.assignments.clone();
        for shard_index in 0..self.config.number_of_shards() {
            let shard_id = ShardId::new(shard_index);
            let current_owner = assignments.get(&shard_id).cloned();
            let needs_allocation = match current_owner.as_ref() {
                Some(owner) => !is_routable_node(&routable_nodes, owner),
                None => true,
            };
            if !needs_allocation {
                continue;
            }

            let desired_owner = {
                let context = ShardAllocationContext::new(
                    &self.entity_type,
                    &self.config,
                    membership,
                    &routable_nodes,
                    &assignments,
                );
                self.allocation_strategy
                    .allocate_shard(&context, shard_id)
                    .filter(|owner| is_routable_node(&routable_nodes, owner))
            };

            match (current_owner, desired_owner) {
                (None, Some(to)) => {
                    assignments.insert(shard_id, to.clone());
                    decisions.push(ShardDecision::Assign {
                        shard: self.shard_key(shard_id),
                        to,
                        reason: ShardMoveReason::InitialAllocation,
                    });
                }
                (Some(from), Some(to)) if from != to => {
                    let reason = move_reason(membership, &from);
                    assignments.insert(shard_id, to.clone());
                    decisions.push(ShardDecision::Move {
                        shard: self.shard_key(shard_id),
                        from,
                        to,
                        reason,
                    });
                }
                (Some(from), None) => {
                    assignments.remove(&shard_id);
                    decisions.push(ShardDecision::Unassign {
                        shard: self.shard_key(shard_id),
                        from: from.clone(),
                        reason: move_reason(membership, &from),
                    });
                }
                (None, None) | (Some(_), Some(_)) => {}
            }
        }

        let reassignments = {
            let context = ShardRebalanceContext::new(
                &self.entity_type,
                &self.config,
                membership,
                &routable_nodes,
                &assignments,
            );
            self.allocation_strategy.rebalance(&context)
        };
        let mut reassigned_shards = BTreeSet::new();
        for reassignment in reassignments {
            let shard_id = reassignment.shard_id();
            if !self.config.contains_shard(shard_id) || !reassigned_shards.insert(shard_id) {
                continue;
            }
            let to = reassignment.to().clone();
            if !is_routable_node(&routable_nodes, &to) {
                continue;
            }
            match assignments.get(&shard_id).cloned() {
                None => {
                    assignments.insert(shard_id, to.clone());
                    decisions.push(ShardDecision::Assign {
                        shard: self.shard_key(shard_id),
                        to,
                        reason: ShardMoveReason::InitialAllocation,
                    });
                }
                Some(from) if from != to => {
                    let reason = move_reason(membership, &from);
                    assignments.insert(shard_id, to.clone());
                    decisions.push(ShardDecision::Move {
                        shard: self.shard_key(shard_id),
                        from,
                        to,
                        reason,
                    });
                }
                Some(_from) => {}
            }
        }

        self.assignments = assignments;
        self.bump_revision_if_changed(&decisions);
        ShardRebalancePlan {
            previous_revision,
            new_revision: self.revision,
            decisions,
        }
    }

    /// Returns a stable ownership snapshot.
    #[must_use]
    pub fn snapshot(&self) -> ShardOwnershipSnapshot {
        ShardOwnershipSnapshot {
            revision: self.revision,
            entity_type: self.entity_type.clone(),
            number_of_shards: self.config.number_of_shards(),
            assignments: self
                .assignments
                .iter()
                .map(|(shard_id, owner)| {
                    ShardAssignment::new(self.shard_key(*shard_id), owner.clone())
                })
                .collect(),
        }
    }

    fn ensure_known_shard(&self, shard_id: ShardId) -> ShardingResult<()> {
        if self.config.contains_shard(shard_id) {
            Ok(())
        } else {
            Err(ShardingError::UnknownShard {
                shard_id,
                number_of_shards: self.config.number_of_shards(),
            })
        }
    }

    fn shard_key(&self, shard_id: ShardId) -> ShardKey {
        ShardKey::new(self.entity_type.clone(), shard_id)
    }

    fn bump_revision_if_changed(&mut self, decisions: &[ShardDecision]) {
        if !decisions.is_empty() {
            self.revision += 1;
        }
    }
}

fn is_routable_node(routable_nodes: &[NodeId], node_id: &NodeId) -> bool {
    routable_nodes.contains(node_id)
}

fn move_reason(membership: &ClusterMembership, current_owner: &NodeId) -> ShardMoveReason {
    match membership
        .member(current_owner)
        .map(|member| member.state())
    {
        Some(MembershipState::Up) => ShardMoveReason::Rebalance,
        Some(MembershipState::Leaving) => ShardMoveReason::GracefulLeave,
        Some(
            MembershipState::Joining
            | MembershipState::Unreachable
            | MembershipState::Down
            | MembershipState::Removed,
        )
        | None => ShardMoveReason::OwnerUnavailable,
    }
}
