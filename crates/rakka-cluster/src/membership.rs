//! Cluster membership table and failure-detection state transitions.

use std::collections::BTreeMap;
use std::time::Duration;

use rakka_core::{MetricsRecorder, METRIC_CLUSTER_MEMBERS};
use serde::{Deserialize, Serialize};

use crate::discovery::DiscoverySnapshot;
use crate::error::{ClusterError, ClusterResult};
use crate::node::{ClusterNode, NodeId};

/// Lifecycle state for a Rakka cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MembershipState {
    /// Node has started but is not yet a cluster member.
    Joining,
    /// Node is an active cluster member.
    Up,
    /// Node is gracefully leaving the cluster.
    Leaving,
    /// Node is suspected unreachable.
    Unreachable,
    /// Node has been downed.
    Down,
    /// Node has been removed from membership.
    Removed,
}

impl MembershipState {
    /// Stable label used for metrics and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Joining => "joining",
            Self::Up => "up",
            Self::Leaving => "leaving",
            Self::Unreachable => "unreachable",
            Self::Down => "down",
            Self::Removed => "removed",
        }
    }
}

/// Cluster membership configuration.
#[derive(Debug, Clone)]
pub struct MembershipConfig {
    min_contact_points: usize,
    failure_timeout: Duration,
    down_after_unreachable: Duration,
}

impl MembershipConfig {
    /// Creates a membership configuration.
    #[must_use]
    pub const fn new(
        min_contact_points: usize,
        failure_timeout: Duration,
        down_after_unreachable: Duration,
    ) -> Self {
        Self {
            min_contact_points,
            failure_timeout,
            down_after_unreachable,
        }
    }

    /// Minimum discovered nodes required before bootstrap can proceed.
    #[must_use]
    pub const fn min_contact_points(&self) -> usize {
        self.min_contact_points
    }

    /// Duration after which a silent member is marked unreachable.
    #[must_use]
    pub const fn failure_timeout(&self) -> Duration {
        self.failure_timeout
    }

    /// Duration after unreachable detection before a silent member is downed.
    #[must_use]
    pub const fn down_after_unreachable(&self) -> Duration {
        self.down_after_unreachable
    }
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self::new(1, Duration::from_secs(10), Duration::from_secs(30))
    }
}

/// Membership record for one cluster node incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRecord {
    node: ClusterNode,
    state: MembershipState,
    last_seen_millis: u64,
    revision: u64,
}

impl MemberRecord {
    fn new(
        node: ClusterNode,
        state: MembershipState,
        last_seen_millis: u64,
        revision: u64,
    ) -> Self {
        Self {
            node,
            state,
            last_seen_millis,
            revision,
        }
    }

    /// Cluster node descriptor.
    #[must_use]
    pub fn node(&self) -> &ClusterNode {
        &self.node
    }

    /// Current membership state.
    #[must_use]
    pub const fn state(&self) -> MembershipState {
        self.state
    }

    /// Last heartbeat or discovery observation time in milliseconds.
    #[must_use]
    pub const fn last_seen_millis(&self) -> u64 {
        self.last_seen_millis
    }

    /// Membership revision that last changed this record.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }
}

/// Membership table event emitted by state transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipEvent {
    /// A node incarnation was first discovered.
    MemberDiscovered {
        /// Node id.
        node_id: NodeId,
    },
    /// A node became an active member.
    MemberUp {
        /// Node id.
        node_id: NodeId,
    },
    /// A previously unreachable node was observed again.
    MemberReachable {
        /// Node id.
        node_id: NodeId,
    },
    /// A node began graceful leave.
    MemberLeaving {
        /// Node id.
        node_id: NodeId,
    },
    /// A node missed heartbeats beyond the failure timeout.
    MemberUnreachable {
        /// Node id.
        node_id: NodeId,
    },
    /// A node was downed.
    MemberDown {
        /// Node id.
        node_id: NodeId,
    },
    /// A node was removed from membership.
    MemberRemoved {
        /// Node id.
        node_id: NodeId,
    },
}

impl MembershipEvent {
    /// Node id associated with this membership event.
    #[must_use]
    pub fn node_id(&self) -> &NodeId {
        match self {
            Self::MemberDiscovered { node_id }
            | Self::MemberUp { node_id }
            | Self::MemberReachable { node_id }
            | Self::MemberLeaving { node_id }
            | Self::MemberUnreachable { node_id }
            | Self::MemberDown { node_id }
            | Self::MemberRemoved { node_id } => node_id,
        }
    }
}

/// Stable snapshot of a membership table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipSnapshot {
    revision: u64,
    members: Vec<MemberRecord>,
}

impl MembershipSnapshot {
    /// Current membership table revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Members sorted by node id.
    #[must_use]
    pub fn members(&self) -> &[MemberRecord] {
        &self.members
    }
}

/// Count of members in one lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipStateCount {
    state: MembershipState,
    count: usize,
}

impl MembershipStateCount {
    /// Creates a membership state count.
    #[must_use]
    pub const fn new(state: MembershipState, count: usize) -> Self {
        Self { state, count }
    }

    /// Membership state.
    #[must_use]
    pub const fn state(&self) -> MembershipState {
        self.state
    }

    /// Number of members in this state.
    #[must_use]
    pub const fn count(&self) -> usize {
        self.count
    }
}

/// Serializable cluster-membership operational snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMembershipOperationalSnapshot {
    local_node_id: NodeId,
    revision: u64,
    total_members: usize,
    states: Vec<MembershipStateCount>,
}

impl ClusterMembershipOperationalSnapshot {
    /// Creates a cluster-membership operational snapshot.
    #[must_use]
    pub fn new(local_node_id: NodeId, revision: u64, states: Vec<MembershipStateCount>) -> Self {
        let total_members = states.iter().map(MembershipStateCount::count).sum();
        Self {
            local_node_id,
            revision,
            total_members,
            states,
        }
    }

    /// Local node id.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Membership table revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Total known members.
    #[must_use]
    pub const fn total_members(&self) -> usize {
        self.total_members
    }

    /// Counts by membership state.
    #[must_use]
    pub fn states(&self) -> &[MembershipStateCount] {
        &self.states
    }
}

/// In-memory cluster membership table.
#[derive(Debug, Clone)]
pub struct ClusterMembership {
    config: MembershipConfig,
    local_node_id: NodeId,
    members: BTreeMap<NodeId, MemberRecord>,
    revision: u64,
}

impl ClusterMembership {
    /// Creates a membership table containing the local node in `Joining` state.
    #[must_use]
    pub fn new(local_node: ClusterNode, config: MembershipConfig) -> Self {
        let local_node_id = local_node.id().clone();
        let mut members = BTreeMap::new();
        members.insert(
            local_node_id.clone(),
            MemberRecord::new(local_node, MembershipState::Joining, 0, 0),
        );

        Self {
            config,
            local_node_id,
            members,
            revision: 0,
        }
    }

    /// Returns membership configuration.
    #[must_use]
    pub const fn config(&self) -> &MembershipConfig {
        &self.config
    }

    /// Local node id.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Current membership table revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns a member record by node id.
    #[must_use]
    pub fn member(&self, node_id: &NodeId) -> Option<&MemberRecord> {
        self.members.get(node_id)
    }

    /// Iterates members sorted by node id.
    pub fn members(&self) -> impl Iterator<Item = &MemberRecord> {
        self.members.values()
    }

    /// Returns active members eligible for shard ownership and direct routing.
    #[must_use]
    pub fn routable_members(&self) -> Vec<&MemberRecord> {
        self.members
            .values()
            .filter(|record| record.state == MembershipState::Up)
            .collect()
    }

    /// Returns a compact operational snapshot for diagnostics.
    #[must_use]
    pub fn operational_snapshot(&self) -> ClusterMembershipOperationalSnapshot {
        let states = [
            MembershipState::Joining,
            MembershipState::Up,
            MembershipState::Leaving,
            MembershipState::Unreachable,
            MembershipState::Down,
            MembershipState::Removed,
        ]
        .into_iter()
        .filter_map(|state| {
            let count = self
                .members
                .values()
                .filter(|record| record.state == state)
                .count();
            (count > 0).then_some(MembershipStateCount::new(state, count))
        })
        .collect();

        ClusterMembershipOperationalSnapshot::new(self.local_node_id.clone(), self.revision, states)
    }

    /// Records cluster-member gauges grouped by membership state.
    pub fn record_metrics(
        &self,
        recorder: &dyn MetricsRecorder,
    ) -> ClusterMembershipOperationalSnapshot {
        let snapshot = self.operational_snapshot();
        let local_node = snapshot.local_node_id().to_string();
        let revision = snapshot.revision().to_string();
        for state in snapshot.states() {
            recorder.record_gauge(
                METRIC_CLUSTER_MEMBERS,
                state.count() as f64,
                &[
                    ("local_node", local_node.as_str()),
                    ("state", state.state().as_str()),
                    ("revision", revision.as_str()),
                ],
            );
        }
        snapshot
    }

    /// Returns true once enough non-removed contact points are known.
    #[must_use]
    pub fn has_min_contact_points(&self) -> bool {
        self.members
            .values()
            .filter(|record| {
                !matches!(
                    record.state,
                    MembershipState::Down | MembershipState::Removed
                )
            })
            .count()
            >= self.config.min_contact_points
    }

    /// Records a discovery snapshot and adds newly observed compatible nodes.
    pub fn record_discovery(
        &mut self,
        snapshot: DiscoverySnapshot,
    ) -> ClusterResult<Vec<MembershipEvent>> {
        let observed_at_millis = snapshot.observed_at_millis();
        let local_protocol = self.local_member()?.node.protocol();
        let mut events = Vec::new();

        for node in snapshot.into_nodes() {
            if !local_protocol.is_compatible_with(node.protocol()) {
                return Err(ClusterError::IncompatibleNode {
                    node_id: node.id().clone(),
                    local: local_protocol,
                    remote: node.protocol(),
                });
            }

            if node.id() == &self.local_node_id {
                self.update_local_descriptor(node, observed_at_millis)?;
                continue;
            }

            if let Some(record) = self.members.get_mut(node.id()) {
                record.node = node;
                if !matches!(
                    record.state,
                    MembershipState::Down | MembershipState::Removed
                ) {
                    record.last_seen_millis = observed_at_millis;
                    if record.state == MembershipState::Unreachable {
                        record.state = MembershipState::Up;
                        self.revision += 1;
                        record.revision = self.revision;
                        events.push(MembershipEvent::MemberReachable {
                            node_id: record.node.id().clone(),
                        });
                    }
                }
            } else {
                self.revision += 1;
                let node_id = node.id().clone();
                self.members.insert(
                    node_id.clone(),
                    MemberRecord::new(
                        node,
                        MembershipState::Joining,
                        observed_at_millis,
                        self.revision,
                    ),
                );
                events.push(MembershipEvent::MemberDiscovered { node_id });
            }
        }

        Ok(events)
    }

    /// Marks a node as an active member.
    pub fn mark_up(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<MembershipEvent>> {
        self.transition(
            node_id,
            observed_at_millis,
            MembershipState::Up,
            |from| {
                matches!(
                    from,
                    MembershipState::Joining | MembershipState::Unreachable
                )
            },
            |node_id| MembershipEvent::MemberUp { node_id },
        )
    }

    /// Observes a heartbeat for an existing member.
    pub fn heartbeat(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<MembershipEvent>> {
        let record = self
            .members
            .get_mut(node_id)
            .ok_or_else(|| ClusterError::UnknownNode {
                node_id: node_id.clone(),
            })?;

        if matches!(
            record.state,
            MembershipState::Down | MembershipState::Removed
        ) {
            return Err(ClusterError::InvalidTransition {
                node_id: node_id.clone(),
                from: record.state,
                to: MembershipState::Up,
            });
        }

        record.last_seen_millis = observed_at_millis;
        if record.state == MembershipState::Unreachable {
            record.state = MembershipState::Up;
            self.revision += 1;
            record.revision = self.revision;
            Ok(Some(MembershipEvent::MemberReachable {
                node_id: node_id.clone(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Begins graceful leave for a member.
    pub fn mark_leaving(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<MembershipEvent>> {
        self.transition(
            node_id,
            observed_at_millis,
            MembershipState::Leaving,
            |from| matches!(from, MembershipState::Joining | MembershipState::Up),
            |node_id| MembershipEvent::MemberLeaving { node_id },
        )
    }

    /// Marks a silent member unreachable.
    pub fn mark_unreachable(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<MembershipEvent>> {
        self.transition(
            node_id,
            observed_at_millis,
            MembershipState::Unreachable,
            |from| matches!(from, MembershipState::Joining | MembershipState::Up),
            |node_id| MembershipEvent::MemberUnreachable { node_id },
        )
    }

    /// Begins graceful leave for the local node.
    pub fn leave_local(
        &mut self,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<MembershipEvent>> {
        let node_id = self.local_node_id.clone();
        self.mark_leaving(&node_id, observed_at_millis)
    }

    /// Marks a member down.
    pub fn mark_down(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<MembershipEvent>> {
        self.transition(
            node_id,
            observed_at_millis,
            MembershipState::Down,
            |from| !matches!(from, MembershipState::Removed),
            |node_id| MembershipEvent::MemberDown { node_id },
        )
    }

    /// Removes a member from the table after graceful leave or downing.
    pub fn remove(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterResult<Option<MembershipEvent>> {
        self.transition(
            node_id,
            observed_at_millis,
            MembershipState::Removed,
            |from| matches!(from, MembershipState::Leaving | MembershipState::Down),
            |node_id| MembershipEvent::MemberRemoved { node_id },
        )
    }

    /// Advances failure detection, marking silent members unreachable or down.
    pub fn tick(&mut self, now_millis: u64) -> Vec<MembershipEvent> {
        let failure_timeout_millis = millis(self.config.failure_timeout);
        let down_after_millis = millis(self.config.down_after_unreachable);
        let down_threshold_millis = failure_timeout_millis.saturating_add(down_after_millis);
        let mut events = Vec::new();

        for (node_id, record) in &mut self.members {
            if node_id == &self.local_node_id {
                continue;
            }

            let elapsed_millis = now_millis.saturating_sub(record.last_seen_millis);
            match record.state {
                MembershipState::Joining | MembershipState::Up
                    if elapsed_millis >= failure_timeout_millis =>
                {
                    record.state = MembershipState::Unreachable;
                    self.revision += 1;
                    record.revision = self.revision;
                    events.push(MembershipEvent::MemberUnreachable {
                        node_id: node_id.clone(),
                    });
                }
                MembershipState::Unreachable if elapsed_millis >= down_threshold_millis => {
                    record.state = MembershipState::Down;
                    self.revision += 1;
                    record.revision = self.revision;
                    events.push(MembershipEvent::MemberDown {
                        node_id: node_id.clone(),
                    });
                }
                _ => {}
            }
        }

        events
    }

    /// Returns a stable membership snapshot.
    #[must_use]
    pub fn snapshot(&self) -> MembershipSnapshot {
        MembershipSnapshot {
            revision: self.revision,
            members: self.members.values().cloned().collect(),
        }
    }

    fn local_member(&self) -> ClusterResult<&MemberRecord> {
        self.members
            .get(&self.local_node_id)
            .ok_or_else(|| ClusterError::UnknownNode {
                node_id: self.local_node_id.clone(),
            })
    }

    fn update_local_descriptor(
        &mut self,
        node: ClusterNode,
        observed_at_millis: u64,
    ) -> ClusterResult<()> {
        let record =
            self.members
                .get_mut(&self.local_node_id)
                .ok_or_else(|| ClusterError::UnknownNode {
                    node_id: self.local_node_id.clone(),
                })?;
        record.node = node;
        record.last_seen_millis = observed_at_millis;
        Ok(())
    }

    fn transition(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
        to: MembershipState,
        is_allowed: impl FnOnce(MembershipState) -> bool,
        event: impl FnOnce(NodeId) -> MembershipEvent,
    ) -> ClusterResult<Option<MembershipEvent>> {
        let record = self
            .members
            .get_mut(node_id)
            .ok_or_else(|| ClusterError::UnknownNode {
                node_id: node_id.clone(),
            })?;
        let from = record.state;
        if from == to {
            record.last_seen_millis = observed_at_millis;
            return Ok(None);
        }

        if !is_allowed(from) {
            return Err(ClusterError::InvalidTransition {
                node_id: node_id.clone(),
                from,
                to,
            });
        }

        record.state = to;
        record.last_seen_millis = observed_at_millis;
        self.revision += 1;
        record.revision = self.revision;
        Ok(Some(event(node_id.clone())))
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
