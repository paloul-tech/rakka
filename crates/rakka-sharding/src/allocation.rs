//! Shard allocation strategy APIs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use rakka_cluster::{ClusterMembership, NodeId};

use crate::coordinator::ShardOwnerCount;
use crate::identity::{EntityType, ShardId, ShardingConfig};

/// Strategy used by a shard coordinator to allocate and rebalance shards.
pub trait ShardAllocationStrategy: Debug + Send + Sync + 'static {
    /// Stable strategy name used for diagnostics.
    fn strategy_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Chooses an owner for an unowned or no-longer-routable shard.
    fn allocate_shard(
        &self,
        context: &ShardAllocationContext<'_>,
        shard_id: ShardId,
    ) -> Option<NodeId>;

    /// Returns owned shards that should move to improve balance.
    fn rebalance(&self, _context: &ShardRebalanceContext<'_>) -> Vec<ShardReassignment> {
        Vec::new()
    }
}

/// Read-only coordinator state passed to allocation decisions.
#[derive(Debug, Clone, Copy)]
pub struct ShardAllocationContext<'a> {
    entity_type: &'a EntityType,
    config: &'a ShardingConfig,
    membership: &'a ClusterMembership,
    routable_nodes: &'a [NodeId],
    assignments: &'a BTreeMap<ShardId, NodeId>,
}

impl<'a> ShardAllocationContext<'a> {
    pub(crate) const fn new(
        entity_type: &'a EntityType,
        config: &'a ShardingConfig,
        membership: &'a ClusterMembership,
        routable_nodes: &'a [NodeId],
        assignments: &'a BTreeMap<ShardId, NodeId>,
    ) -> Self {
        Self {
            entity_type,
            config,
            membership,
            routable_nodes,
            assignments,
        }
    }

    /// Entity type being coordinated.
    #[must_use]
    pub const fn entity_type(&self) -> &EntityType {
        self.entity_type
    }

    /// Sharding configuration for this entity type.
    #[must_use]
    pub const fn config(&self) -> &ShardingConfig {
        self.config
    }

    /// Current cluster membership table.
    #[must_use]
    pub const fn membership(&self) -> &ClusterMembership {
        self.membership
    }

    /// Active members eligible for shard ownership.
    #[must_use]
    pub const fn routable_nodes(&self) -> &[NodeId] {
        self.routable_nodes
    }

    /// Returns true when a node is eligible for shard ownership.
    #[must_use]
    pub fn is_routable_node(&self, node_id: &NodeId) -> bool {
        self.routable_nodes.contains(node_id)
    }

    /// Returns the current owner for a shard, when assigned.
    #[must_use]
    pub fn owner_for_shard(&self, shard_id: ShardId) -> Option<&NodeId> {
        self.assignments.get(&shard_id)
    }

    /// Iterates shard assignments sorted by shard id.
    pub fn assignments(&self) -> impl Iterator<Item = (ShardId, &NodeId)> + '_ {
        self.assignments
            .iter()
            .map(|(shard_id, owner)| (*shard_id, owner))
    }

    /// Counts shards currently owned by `owner`.
    #[must_use]
    pub fn owned_shard_count(&self, owner: &NodeId) -> usize {
        owned_shard_count(self.assignments, owner)
    }

    /// Counts owned shards grouped by node id.
    #[must_use]
    pub fn owner_counts(&self) -> Vec<ShardOwnerCount> {
        owner_counts(self.assignments)
    }
}

/// Read-only coordinator state passed to rebalance decisions.
#[derive(Debug, Clone, Copy)]
pub struct ShardRebalanceContext<'a> {
    entity_type: &'a EntityType,
    config: &'a ShardingConfig,
    membership: &'a ClusterMembership,
    routable_nodes: &'a [NodeId],
    assignments: &'a BTreeMap<ShardId, NodeId>,
}

impl<'a> ShardRebalanceContext<'a> {
    pub(crate) const fn new(
        entity_type: &'a EntityType,
        config: &'a ShardingConfig,
        membership: &'a ClusterMembership,
        routable_nodes: &'a [NodeId],
        assignments: &'a BTreeMap<ShardId, NodeId>,
    ) -> Self {
        Self {
            entity_type,
            config,
            membership,
            routable_nodes,
            assignments,
        }
    }

    /// Entity type being coordinated.
    #[must_use]
    pub const fn entity_type(&self) -> &EntityType {
        self.entity_type
    }

    /// Sharding configuration for this entity type.
    #[must_use]
    pub const fn config(&self) -> &ShardingConfig {
        self.config
    }

    /// Current cluster membership table.
    #[must_use]
    pub const fn membership(&self) -> &ClusterMembership {
        self.membership
    }

    /// Active members eligible for shard ownership.
    #[must_use]
    pub const fn routable_nodes(&self) -> &[NodeId] {
        self.routable_nodes
    }

    /// Returns true when a node is eligible for shard ownership.
    #[must_use]
    pub fn is_routable_node(&self, node_id: &NodeId) -> bool {
        self.routable_nodes.contains(node_id)
    }

    /// Returns the current owner for a shard, when assigned.
    #[must_use]
    pub fn owner_for_shard(&self, shard_id: ShardId) -> Option<&NodeId> {
        self.assignments.get(&shard_id)
    }

    /// Iterates shard assignments sorted by shard id.
    pub fn assignments(&self) -> impl Iterator<Item = (ShardId, &NodeId)> + '_ {
        self.assignments
            .iter()
            .map(|(shard_id, owner)| (*shard_id, owner))
    }

    /// Counts shards currently owned by `owner`.
    #[must_use]
    pub fn owned_shard_count(&self, owner: &NodeId) -> usize {
        owned_shard_count(self.assignments, owner)
    }

    /// Counts owned shards grouped by node id.
    #[must_use]
    pub fn owner_counts(&self) -> Vec<ShardOwnerCount> {
        owner_counts(self.assignments)
    }
}

/// A strategy-requested shard movement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardReassignment {
    shard_id: ShardId,
    to: NodeId,
}

impl ShardReassignment {
    /// Creates a shard reassignment.
    #[must_use]
    pub const fn new(shard_id: ShardId, to: NodeId) -> Self {
        Self { shard_id, to }
    }

    /// Shard to move.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Target owner.
    #[must_use]
    pub const fn to(&self) -> &NodeId {
        &self.to
    }
}

/// Deterministic modulo allocation strategy used by default.
///
/// Maps each shard to `sorted_routable_nodes[shard_index % len]`, so every node
/// computes identical ownership from the same membership without any shared
/// coordinator. `rebalance` returns the shards whose current owner differs from
/// that deterministic owner.
///
/// By default the rebalance is **unbounded** — every mis-placed shard moves in a
/// single pass. Large clusters can cap the moves per pass with
/// [`with_max_simultaneous_rebalance`](Self::with_max_simultaneous_rebalance) so a
/// scale event recovers over several reconcile passes instead of all at once
/// ("thundering" recovery). The cap is applied in ascending shard-id order, which
/// is identical on every node, so a capped rebalance stays deterministic across
/// per-node coordinators and still converges to the full deterministic placement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeterministicModuloShardAllocationStrategy {
    max_simultaneous_rebalance: usize,
}

impl DeterministicModuloShardAllocationStrategy {
    /// Creates a deterministic modulo strategy with unbounded rebalancing.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_simultaneous_rebalance: 0,
        }
    }

    /// Caps the number of shard moves returned by one rebalance pass.
    ///
    /// `0` (the default) restores unbounded rebalancing. A non-zero cap bounds how
    /// many shards move per reconcile so a large scale event recovers over several
    /// passes; convergence still reaches the full deterministic placement because
    /// each pass moves the next lowest-id mis-placed shards. Initial allocation of
    /// unowned shards and reassignment of orphaned shards (owner no longer
    /// routable) are never capped — only moves of healthy shards.
    #[must_use]
    pub const fn with_max_simultaneous_rebalance(
        mut self,
        max_simultaneous_rebalance: usize,
    ) -> Self {
        self.max_simultaneous_rebalance = max_simultaneous_rebalance;
        self
    }

    /// Maximum shard moves returned by one rebalance pass; `0` means unbounded.
    #[must_use]
    pub const fn max_simultaneous_rebalance(&self) -> usize {
        self.max_simultaneous_rebalance
    }
}

impl ShardAllocationStrategy for DeterministicModuloShardAllocationStrategy {
    fn allocate_shard(
        &self,
        context: &ShardAllocationContext<'_>,
        shard_id: ShardId,
    ) -> Option<NodeId> {
        desired_owner(context.routable_nodes(), shard_id).cloned()
    }

    fn rebalance(&self, context: &ShardRebalanceContext<'_>) -> Vec<ShardReassignment> {
        let moves = context.assignments().filter_map(|(shard_id, owner)| {
            let desired = desired_owner(context.routable_nodes(), shard_id)?;
            (owner != desired).then(|| ShardReassignment::new(shard_id, desired.clone()))
        });
        if self.max_simultaneous_rebalance == 0 {
            moves.collect()
        } else {
            moves.take(self.max_simultaneous_rebalance).collect()
        }
    }
}

/// Least-shard allocation strategy with bounded rebalance decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeastShardAllocationStrategy {
    rebalance_threshold: usize,
    max_simultaneous_rebalance: usize,
}

impl LeastShardAllocationStrategy {
    /// Creates a least-shard allocation strategy.
    #[must_use]
    pub const fn new(rebalance_threshold: usize, max_simultaneous_rebalance: usize) -> Self {
        Self {
            rebalance_threshold,
            max_simultaneous_rebalance,
        }
    }

    /// Allowed ownership-count difference before rebalance starts.
    #[must_use]
    pub const fn rebalance_threshold(&self) -> usize {
        self.rebalance_threshold
    }

    /// Maximum shard moves returned by one rebalance pass.
    #[must_use]
    pub const fn max_simultaneous_rebalance(&self) -> usize {
        self.max_simultaneous_rebalance
    }
}

impl Default for LeastShardAllocationStrategy {
    fn default() -> Self {
        Self::new(1, 10)
    }
}

impl ShardAllocationStrategy for LeastShardAllocationStrategy {
    fn allocate_shard(
        &self,
        context: &ShardAllocationContext<'_>,
        _shard_id: ShardId,
    ) -> Option<NodeId> {
        let counts = routable_owner_counts(context.routable_nodes(), context.assignments());
        counts
            .into_iter()
            .min_by(|(left_node, left_count), (right_node, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| left_node.cmp(right_node))
            })
            .map(|(node_id, _count)| node_id)
    }

    fn rebalance(&self, context: &ShardRebalanceContext<'_>) -> Vec<ShardReassignment> {
        if self.max_simultaneous_rebalance == 0 {
            return Vec::new();
        }

        let effective_threshold = self.rebalance_threshold.max(1);
        let mut assignments = context
            .assignments()
            .map(|(shard_id, owner)| (shard_id, owner.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut counts = routable_owner_counts(context.routable_nodes(), context.assignments());
        let mut moved = BTreeSet::new();
        let mut reassignments = Vec::new();

        while reassignments.len() < self.max_simultaneous_rebalance {
            let Some((from, max_count)) = max_owner(&counts) else {
                break;
            };
            let Some((to, min_count)) = min_owner(&counts) else {
                break;
            };
            if max_count.saturating_sub(min_count) <= effective_threshold {
                break;
            }

            let Some(shard_id) = assignments.iter().find_map(|(shard_id, owner)| {
                (owner == &from && !moved.contains(shard_id)).then_some(*shard_id)
            }) else {
                break;
            };

            assignments.insert(shard_id, to.clone());
            counts.insert(from.clone(), max_count - 1);
            counts.insert(to.clone(), min_count + 1);
            moved.insert(shard_id);
            reassignments.push(ShardReassignment::new(shard_id, to));
        }

        reassignments
    }
}

fn desired_owner(routable_nodes: &[NodeId], shard_id: ShardId) -> Option<&NodeId> {
    if routable_nodes.is_empty() {
        return None;
    }
    let index = shard_id.as_u32() as usize % routable_nodes.len();
    routable_nodes.get(index)
}

fn owned_shard_count(assignments: &BTreeMap<ShardId, NodeId>, owner: &NodeId) -> usize {
    assignments
        .values()
        .filter(|assigned_owner| *assigned_owner == owner)
        .count()
}

fn owner_counts(assignments: &BTreeMap<ShardId, NodeId>) -> Vec<ShardOwnerCount> {
    let mut counts = BTreeMap::<NodeId, usize>::new();
    for owner in assignments.values() {
        *counts.entry(owner.clone()).or_default() += 1;
    }

    counts
        .into_iter()
        .map(|(owner, count)| ShardOwnerCount::new(owner, count))
        .collect()
}

fn routable_owner_counts<'a>(
    routable_nodes: &[NodeId],
    assignments: impl Iterator<Item = (ShardId, &'a NodeId)>,
) -> BTreeMap<NodeId, usize> {
    let mut counts = routable_nodes
        .iter()
        .cloned()
        .map(|node_id| (node_id, 0))
        .collect::<BTreeMap<_, _>>();
    for (_shard_id, owner) in assignments {
        if let Some(count) = counts.get_mut(owner) {
            *count += 1;
        }
    }
    counts
}

fn min_owner(counts: &BTreeMap<NodeId, usize>) -> Option<(NodeId, usize)> {
    counts
        .iter()
        .min_by(|(left_node, left_count), (right_node, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| left_node.cmp(right_node))
        })
        .map(|(node_id, count)| (node_id.clone(), *count))
}

fn max_owner(counts: &BTreeMap<NodeId, usize>) -> Option<(NodeId, usize)> {
    counts
        .iter()
        .max_by(|(left_node, left_count), (right_node, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_node.cmp(left_node))
        })
        .map(|(node_id, count)| (node_id.clone(), *count))
}
