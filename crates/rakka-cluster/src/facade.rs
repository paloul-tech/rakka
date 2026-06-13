//! Akka-style cluster extension facade.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rakka_core::ActorSystem;
use tokio::sync::broadcast;

use crate::{
    ClusterMembership, ClusterNode, ClusterResult, DiscoverySnapshot, MemberRecord,
    MembershipConfig, MembershipEvent, MembershipSnapshot, MembershipState, NodeAddress, NodeId,
};

const CLUSTER_EVENT_CAPACITY: usize = 1024;

/// Akka-style cluster extension facade.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterInner>,
}

struct ClusterInner {
    data: Mutex<ClusterData>,
    events: broadcast::Sender<ClusterEvent>,
    next_observed_at_millis: AtomicU64,
}

struct ClusterData {
    membership: ClusterMembership,
    history: Vec<ClusterEvent>,
}

impl Cluster {
    /// Creates a local cluster facade for an actor system.
    ///
    /// This creates a standalone cluster extension facade. Call
    /// [`ClusterManager::join_self`] or [`ClusterManager::join`] to move the
    /// local member from `Joining` to `Up`.
    #[must_use]
    pub fn get(system: &ActorSystem) -> Self {
        Self::for_local_node(local_node_for_system(system), MembershipConfig::default())
    }

    /// Creates a cluster facade for a configured local node.
    #[must_use]
    pub fn for_local_node(local_node: ClusterNode, config: MembershipConfig) -> Self {
        Self::from_membership(ClusterMembership::new(local_node, config))
    }

    /// Creates a cluster facade from an existing membership table.
    #[must_use]
    pub fn from_membership(membership: ClusterMembership) -> Self {
        let (events, _) = broadcast::channel(CLUSTER_EVENT_CAPACITY);
        Self {
            inner: Arc::new(ClusterInner {
                data: Mutex::new(ClusterData {
                    membership,
                    history: Vec::new(),
                }),
                events,
                next_observed_at_millis: AtomicU64::new(1),
            }),
        }
    }

    /// Returns a cluster manager facade for membership commands.
    #[must_use]
    pub fn manager(&self) -> ClusterManager {
        ClusterManager {
            cluster: self.clone(),
        }
    }

    /// Returns a subscription facade for cluster events.
    #[must_use]
    pub fn subscriptions(&self) -> ClusterSubscriptions {
        ClusterSubscriptions {
            cluster: self.clone(),
        }
    }

    /// Returns the current cluster state.
    #[must_use]
    pub fn state(&self) -> ClusterState {
        let data = self
            .inner
            .data
            .lock()
            .expect("cluster facade mutex poisoned");
        ClusterState::from_snapshot(
            data.membership.local_node_id().clone(),
            data.membership.snapshot(),
        )
    }

    /// Returns the current local member snapshot.
    pub fn self_member(&self) -> ClusterResult<SelfMember> {
        self.state()
            .self_member()
            .ok_or_else(|| crate::ClusterError::UnknownNode {
                node_id: self.local_node_id(),
            })
    }

    /// Returns the current local node id.
    #[must_use]
    pub fn local_node_id(&self) -> NodeId {
        let data = self
            .inner
            .data
            .lock()
            .expect("cluster facade mutex poisoned");
        data.membership.local_node_id().clone()
    }

    fn next_observed_at_millis(&self) -> u64 {
        self.inner
            .next_observed_at_millis
            .fetch_add(1, Ordering::Relaxed)
    }

    fn update(
        &self,
        mutate: impl FnOnce(&mut ClusterMembership, u64) -> ClusterResult<Vec<MembershipEvent>>,
    ) -> ClusterResult<ClusterUpdate> {
        let observed_at_millis = self.next_observed_at_millis();
        let (state, events) = {
            let mut data = self
                .inner
                .data
                .lock()
                .expect("cluster facade mutex poisoned");
            let membership_events = mutate(&mut data.membership, observed_at_millis)?;
            let events = cluster_events_for(&data.membership, membership_events)?;
            data.history.extend(events.clone());
            (
                ClusterState::from_snapshot(
                    data.membership.local_node_id().clone(),
                    data.membership.snapshot(),
                ),
                events,
            )
        };

        for event in &events {
            let _ = self.inner.events.send(event.clone());
        }

        Ok(ClusterUpdate::new(state, events))
    }
}

impl fmt::Debug for Cluster {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cluster")
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Cluster membership command facade.
#[derive(Debug, Clone)]
pub struct ClusterManager {
    cluster: Cluster,
}

impl ClusterManager {
    /// Joins the cluster as the local node.
    pub fn join_self(&self) -> ClusterResult<ClusterUpdate> {
        let local_node = self.cluster.self_member()?.node().clone();
        self.join(local_node)
    }

    /// Discovers and marks a node as an active cluster member.
    pub fn join(&self, node: ClusterNode) -> ClusterResult<ClusterUpdate> {
        self.cluster.update(|membership, observed_at_millis| {
            let mut events = membership.record_discovery(DiscoverySnapshot::new(
                "cluster-manager",
                observed_at_millis,
                [node.clone()],
            ))?;
            if let Some(event) = membership.mark_up(node.id(), observed_at_millis)? {
                events.push(event);
            }
            Ok(events)
        })
    }

    /// Discovers and marks seed nodes as active cluster members.
    pub fn join_seed_nodes(
        &self,
        nodes: impl IntoIterator<Item = ClusterNode>,
    ) -> ClusterResult<ClusterUpdate> {
        let nodes = nodes.into_iter().collect::<Vec<_>>();
        self.cluster.update(|membership, observed_at_millis| {
            let mut events = membership.record_discovery(DiscoverySnapshot::new(
                "cluster-seed-nodes",
                observed_at_millis,
                nodes.clone(),
            ))?;
            for node in &nodes {
                if let Some(event) = membership.mark_up(node.id(), observed_at_millis)? {
                    events.push(event);
                }
            }
            Ok(events)
        })
    }

    /// Begins graceful leave for a member.
    pub fn leave(&self, node_id: &NodeId) -> ClusterResult<ClusterUpdate> {
        self.cluster.update(|membership, observed_at_millis| {
            Ok(membership
                .mark_leaving(node_id, observed_at_millis)?
                .into_iter()
                .collect())
        })
    }

    /// Marks a member down.
    pub fn down(&self, node_id: &NodeId) -> ClusterResult<ClusterUpdate> {
        self.cluster.update(|membership, observed_at_millis| {
            Ok(membership
                .mark_down(node_id, observed_at_millis)?
                .into_iter()
                .collect())
        })
    }
}

/// Cluster event subscription facade.
#[derive(Debug, Clone)]
pub struct ClusterSubscriptions {
    cluster: Cluster,
}

impl ClusterSubscriptions {
    /// Subscribes to cluster events with a chosen initial replay mode.
    #[must_use]
    pub fn subscribe(&self, replay: ClusterSubscriptionReplay) -> ClusterSubscription {
        let data = self
            .cluster
            .inner
            .data
            .lock()
            .expect("cluster facade mutex poisoned");
        let pending = match replay {
            ClusterSubscriptionReplay::InitialState => {
                let state = ClusterState::from_snapshot(
                    data.membership.local_node_id().clone(),
                    data.membership.snapshot(),
                );
                VecDeque::from([ClusterEvent::CurrentState { state }])
            }
            ClusterSubscriptionReplay::InitialEvents => data.history.iter().cloned().collect(),
            ClusterSubscriptionReplay::LiveOnly => VecDeque::new(),
        };
        let receiver = self.cluster.inner.events.subscribe();
        drop(data);

        ClusterSubscription { pending, receiver }
    }
}

/// Initial event replay mode for cluster subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterSubscriptionReplay {
    /// Deliver one current-state snapshot before live events.
    InitialState,
    /// Replay membership events emitted before subscription, then live events.
    InitialEvents,
    /// Deliver only events emitted after subscription.
    LiveOnly,
}

/// Active cluster event subscription.
#[derive(Debug)]
pub struct ClusterSubscription {
    pending: VecDeque<ClusterEvent>,
    receiver: broadcast::Receiver<ClusterEvent>,
}

impl ClusterSubscription {
    /// Receives the next replayed or live cluster event.
    pub async fn recv(&mut self) -> Result<ClusterEvent, ClusterSubscriptionError> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(event);
        }

        self.receiver.recv().await.map_err(Into::into)
    }
}

/// Cluster subscription receive failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterSubscriptionError {
    /// The subscription sender was dropped.
    Closed,
    /// The receiver lagged behind the bounded event buffer.
    Lagged {
        /// Number of skipped events.
        skipped: u64,
    },
}

impl Display for ClusterSubscriptionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("cluster subscription closed"),
            Self::Lagged { skipped } => {
                write!(f, "cluster subscription lagged by {skipped} events")
            }
        }
    }
}

impl Error for ClusterSubscriptionError {}

impl From<broadcast::error::RecvError> for ClusterSubscriptionError {
    fn from(error: broadcast::error::RecvError) -> Self {
        match error {
            broadcast::error::RecvError::Closed => Self::Closed,
            broadcast::error::RecvError::Lagged(skipped) => Self::Lagged { skipped },
        }
    }
}

/// Stable cluster state snapshot returned by the facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    local_node_id: NodeId,
    revision: u64,
    members: Vec<MemberRecord>,
}

impl ClusterState {
    fn from_snapshot(local_node_id: NodeId, snapshot: MembershipSnapshot) -> Self {
        Self {
            local_node_id,
            revision: snapshot.revision(),
            members: snapshot.members().to_vec(),
        }
    }

    /// Local node id for this cluster facade.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Membership table revision.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Members sorted by node id.
    #[must_use]
    pub fn members(&self) -> &[MemberRecord] {
        &self.members
    }

    /// Returns a member by node id.
    #[must_use]
    pub fn member(&self, node_id: &NodeId) -> Option<&MemberRecord> {
        self.members
            .iter()
            .find(|member| member.node().id() == node_id)
    }

    /// Returns the local member snapshot.
    #[must_use]
    pub fn self_member(&self) -> Option<SelfMember> {
        self.member(&self.local_node_id)
            .cloned()
            .map(SelfMember::new)
    }
}

/// Snapshot of the local cluster member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfMember {
    record: MemberRecord,
}

impl SelfMember {
    fn new(record: MemberRecord) -> Self {
        Self { record }
    }

    /// Local cluster node descriptor.
    #[must_use]
    pub fn node(&self) -> &ClusterNode {
        self.record.node()
    }

    /// Local membership state.
    #[must_use]
    pub const fn state(&self) -> MembershipState {
        self.record.state()
    }

    /// Underlying member record.
    #[must_use]
    pub const fn record(&self) -> &MemberRecord {
        &self.record
    }
}

/// Result of a cluster manager command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterUpdate {
    state: ClusterState,
    events: Vec<ClusterEvent>,
}

impl ClusterUpdate {
    fn new(state: ClusterState, events: Vec<ClusterEvent>) -> Self {
        Self { state, events }
    }

    /// State after the command was applied.
    #[must_use]
    pub const fn state(&self) -> &ClusterState {
        &self.state
    }

    /// Events emitted by the command.
    #[must_use]
    pub fn events(&self) -> &[ClusterEvent] {
        &self.events
    }

    /// Returns true when the command changed no membership state.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// Cluster facade event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterEvent {
    /// Initial current-state replay event.
    CurrentState {
        /// Current cluster state.
        state: ClusterState,
    },
    /// A node incarnation was first discovered.
    MemberDiscovered {
        /// Member after the transition.
        member: MemberRecord,
    },
    /// A node became an active member.
    MemberUp {
        /// Member after the transition.
        member: MemberRecord,
    },
    /// A previously unreachable node was observed again.
    MemberReachable {
        /// Member after the transition.
        member: MemberRecord,
    },
    /// A node began graceful leave.
    MemberLeaving {
        /// Member after the transition.
        member: MemberRecord,
    },
    /// A node missed heartbeats beyond the failure timeout.
    MemberUnreachable {
        /// Member after the transition.
        member: MemberRecord,
    },
    /// A node was downed.
    MemberDown {
        /// Member after the transition.
        member: MemberRecord,
    },
    /// A node was removed from membership.
    MemberRemoved {
        /// Member after the transition.
        member: MemberRecord,
    },
}

impl ClusterEvent {
    /// Member carried by this event, if the event is member-specific.
    #[must_use]
    pub fn member(&self) -> Option<&MemberRecord> {
        match self {
            Self::CurrentState { .. } => None,
            Self::MemberDiscovered { member }
            | Self::MemberUp { member }
            | Self::MemberReachable { member }
            | Self::MemberLeaving { member }
            | Self::MemberUnreachable { member }
            | Self::MemberDown { member }
            | Self::MemberRemoved { member } => Some(member),
        }
    }

    /// Node id carried by this event, if the event is member-specific.
    #[must_use]
    pub fn node_id(&self) -> Option<&NodeId> {
        self.member().map(|member| member.node().id())
    }
}

fn cluster_events_for(
    membership: &ClusterMembership,
    events: Vec<MembershipEvent>,
) -> ClusterResult<Vec<ClusterEvent>> {
    events
        .into_iter()
        .map(|event| cluster_event_for(membership, event))
        .collect()
}

fn cluster_event_for(
    membership: &ClusterMembership,
    event: MembershipEvent,
) -> ClusterResult<ClusterEvent> {
    let member = membership.member(event.node_id()).cloned().ok_or_else(|| {
        crate::ClusterError::UnknownNode {
            node_id: event.node_id().clone(),
        }
    })?;
    Ok(match event {
        MembershipEvent::MemberDiscovered { .. } => ClusterEvent::MemberDiscovered { member },
        MembershipEvent::MemberUp { .. } => ClusterEvent::MemberUp { member },
        MembershipEvent::MemberReachable { .. } => ClusterEvent::MemberReachable { member },
        MembershipEvent::MemberLeaving { .. } => ClusterEvent::MemberLeaving { member },
        MembershipEvent::MemberUnreachable { .. } => ClusterEvent::MemberUnreachable { member },
        MembershipEvent::MemberDown { .. } => ClusterEvent::MemberDown { member },
        MembershipEvent::MemberRemoved { .. } => ClusterEvent::MemberRemoved { member },
    })
}

fn local_node_for_system(system: &ActorSystem) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(system.name(), "local"),
        NodeAddress::new("127.0.0.1", 0),
    )
}
