//! Akka-style cluster extension facade.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_core::ActorSystem;
use tokio::sync::broadcast;

use crate::{
    ClusterMembership, ClusterNode, ClusterResult, ClusteredReceptionistSettings,
    DiscoveryProvider, DiscoverySnapshot, MemberRecord, MembershipConfig, MembershipEvent,
    MembershipSnapshot, MembershipState, NodeAddress, NodeId,
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
        self.update_at(observed_at_millis, mutate)
    }

    fn update_at(
        &self,
        observed_at_millis: u64,
        mutate: impl FnOnce(&mut ClusterMembership, u64) -> ClusterResult<Vec<MembershipEvent>>,
    ) -> ClusterResult<ClusterUpdate> {
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

    /// Applies a discovery snapshot without automatically promoting members.
    pub fn apply_discovery(&self, snapshot: DiscoverySnapshot) -> ClusterResult<ClusterUpdate> {
        let observed_at_millis = snapshot.observed_at_millis();
        self.cluster
            .update_at(observed_at_millis, |membership, _observed_at_millis| {
                membership.record_discovery(snapshot)
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

/// Cluster extension runtime settings.
#[derive(Debug, Clone)]
pub struct ClusterSettings {
    local_node: ClusterNode,
    seed_nodes: Vec<ClusterNode>,
    membership_config: MembershipConfig,
    discovery_poll_interval: Duration,
    failure_tick_interval: Duration,
    clustered_receptionist: ClusteredReceptionistSettings,
}

impl ClusterSettings {
    /// Creates settings for one local cluster node.
    #[must_use]
    pub fn new(local_node: ClusterNode) -> Self {
        Self {
            local_node,
            seed_nodes: Vec::new(),
            membership_config: MembershipConfig::default(),
            discovery_poll_interval: Duration::from_secs(3),
            failure_tick_interval: Duration::from_secs(1),
            clustered_receptionist: ClusteredReceptionistSettings::default(),
        }
    }

    /// Local node descriptor.
    #[must_use]
    pub const fn local_node(&self) -> &ClusterNode {
        &self.local_node
    }

    /// Seed nodes joined by [`ClusterRuntime::join_seed_nodes`].
    #[must_use]
    pub fn seed_nodes(&self) -> &[ClusterNode] {
        &self.seed_nodes
    }

    /// Membership settings.
    #[must_use]
    pub const fn membership_config(&self) -> &MembershipConfig {
        &self.membership_config
    }

    /// Discovery polling interval for application-driven runtime loops.
    #[must_use]
    pub const fn discovery_poll_interval(&self) -> Duration {
        self.discovery_poll_interval
    }

    /// Failure-detection tick interval for application-driven runtime loops.
    #[must_use]
    pub const fn failure_tick_interval(&self) -> Duration {
        self.failure_tick_interval
    }

    /// Clustered receptionist propagation settings.
    #[must_use]
    pub const fn clustered_receptionist(&self) -> &ClusteredReceptionistSettings {
        &self.clustered_receptionist
    }

    /// Sets seed nodes.
    #[must_use]
    pub fn with_seed_nodes(mut self, seed_nodes: impl IntoIterator<Item = ClusterNode>) -> Self {
        self.seed_nodes = seed_nodes.into_iter().collect();
        self
    }

    /// Sets membership configuration.
    #[must_use]
    pub fn with_membership_config(mut self, membership_config: MembershipConfig) -> Self {
        self.membership_config = membership_config;
        self
    }

    /// Sets the minimum discovered contact points required before joining
    /// members are promoted to `Up`.
    #[must_use]
    pub fn with_min_contact_points(mut self, min_contact_points: usize) -> Self {
        self.membership_config = MembershipConfig::new(
            min_contact_points,
            self.membership_config.failure_timeout(),
            self.membership_config.down_after_unreachable(),
        );
        self
    }

    /// Sets the failure-detection timeout used by the default detector.
    #[must_use]
    pub fn with_failure_timeout(mut self, timeout: Duration) -> Self {
        self.membership_config = MembershipConfig::new(
            self.membership_config.min_contact_points(),
            timeout,
            self.membership_config.down_after_unreachable(),
        );
        self
    }

    /// Sets the down-after-unreachable timeout used by the default downing
    /// strategy.
    #[must_use]
    pub fn with_down_after_unreachable(mut self, timeout: Duration) -> Self {
        self.membership_config = MembershipConfig::new(
            self.membership_config.min_contact_points(),
            self.membership_config.failure_timeout(),
            timeout,
        );
        self
    }

    /// Sets discovery polling interval metadata.
    #[must_use]
    pub const fn with_discovery_poll_interval(mut self, interval: Duration) -> Self {
        self.discovery_poll_interval = interval;
        self
    }

    /// Sets failure tick interval metadata.
    #[must_use]
    pub const fn with_failure_tick_interval(mut self, interval: Duration) -> Self {
        self.failure_tick_interval = interval;
        self
    }

    /// Sets clustered receptionist propagation settings.
    #[must_use]
    pub fn with_clustered_receptionist(mut self, settings: ClusteredReceptionistSettings) -> Self {
        self.clustered_receptionist = settings;
        self
    }

    /// Creates a cluster facade from these settings.
    #[must_use]
    pub fn cluster(&self) -> Cluster {
        Cluster::for_local_node(self.local_node.clone(), self.membership_config.clone())
    }
}

/// Failure-detector hook used by [`ClusterRuntime`].
pub trait FailureDetector: fmt::Debug + Send + Sync + 'static {
    /// Returns members that should become unreachable at `now_millis`.
    fn unreachable_members(&self, membership: &ClusterMembership, now_millis: u64) -> Vec<NodeId>;
}

/// Timeout-based failure detector using [`MembershipConfig::failure_timeout`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeoutFailureDetector;

impl FailureDetector for TimeoutFailureDetector {
    fn unreachable_members(&self, membership: &ClusterMembership, now_millis: u64) -> Vec<NodeId> {
        let failure_timeout_millis = duration_millis(membership.config().failure_timeout());
        membership
            .members()
            .filter(|member| member.node().id() != membership.local_node_id())
            .filter(|member| {
                matches!(
                    member.state(),
                    MembershipState::Joining | MembershipState::Up
                )
            })
            .filter(|member| {
                now_millis.saturating_sub(member.last_seen_millis()) >= failure_timeout_millis
            })
            .map(|member| member.node().id().clone())
            .collect()
    }
}

/// Downing policy hook used by [`ClusterRuntime`].
pub trait DowningStrategy: fmt::Debug + Send + Sync + 'static {
    /// Returns unreachable members that should be downed at `now_millis`.
    fn down_members(&self, membership: &ClusterMembership, now_millis: u64) -> Vec<NodeId>;
}

/// Conservative timeout downing strategy using
/// [`MembershipConfig::down_after_unreachable`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeoutDowningStrategy;

impl DowningStrategy for TimeoutDowningStrategy {
    fn down_members(&self, membership: &ClusterMembership, now_millis: u64) -> Vec<NodeId> {
        let down_after_millis = duration_millis(membership.config().down_after_unreachable());
        membership
            .members()
            .filter(|member| member.node().id() != membership.local_node_id())
            .filter(|member| member.state() == MembershipState::Unreachable)
            .filter(|member| {
                now_millis.saturating_sub(member.last_seen_millis()) >= down_after_millis
            })
            .map(|member| member.node().id().clone())
            .collect()
    }
}

/// Downing strategy that never marks members down automatically.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoDowningStrategy;

impl DowningStrategy for NoDowningStrategy {
    fn down_members(&self, _membership: &ClusterMembership, _now_millis: u64) -> Vec<NodeId> {
        Vec::new()
    }
}

/// Cluster facade runtime that applies discovery and failure policies.
#[derive(Clone)]
pub struct ClusterRuntime {
    cluster: Cluster,
    settings: ClusterSettings,
    failure_detector: Arc<dyn FailureDetector>,
    downing_strategy: Arc<dyn DowningStrategy>,
}

impl ClusterRuntime {
    /// Creates a runtime from settings.
    #[must_use]
    pub fn from_settings(settings: ClusterSettings) -> Self {
        let cluster = settings.cluster();
        Self::new(cluster, settings)
    }

    /// Creates a runtime for an existing cluster facade.
    #[must_use]
    pub fn new(cluster: Cluster, settings: ClusterSettings) -> Self {
        Self {
            cluster,
            settings,
            failure_detector: Arc::new(TimeoutFailureDetector),
            downing_strategy: Arc::new(TimeoutDowningStrategy),
        }
    }

    /// Cluster facade driven by this runtime.
    #[must_use]
    pub const fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Runtime settings.
    #[must_use]
    pub const fn settings(&self) -> &ClusterSettings {
        &self.settings
    }

    /// Sets a failure detector.
    #[must_use]
    pub fn with_failure_detector(mut self, failure_detector: impl FailureDetector) -> Self {
        self.failure_detector = Arc::new(failure_detector);
        self
    }

    /// Sets a shared failure detector.
    #[must_use]
    pub fn with_failure_detector_ref(mut self, failure_detector: Arc<dyn FailureDetector>) -> Self {
        self.failure_detector = failure_detector;
        self
    }

    /// Sets a downing strategy.
    #[must_use]
    pub fn with_downing_strategy(mut self, downing_strategy: impl DowningStrategy) -> Self {
        self.downing_strategy = Arc::new(downing_strategy);
        self
    }

    /// Sets a shared downing strategy.
    #[must_use]
    pub fn with_downing_strategy_ref(mut self, downing_strategy: Arc<dyn DowningStrategy>) -> Self {
        self.downing_strategy = downing_strategy;
        self
    }

    /// Joins configured seed nodes.
    pub fn join_seed_nodes(&self) -> ClusterResult<ClusterUpdate> {
        self.cluster
            .manager()
            .join_seed_nodes(self.settings.seed_nodes.clone())
    }

    /// Polls a discovery provider once and promotes discovered joining members
    /// when the membership has enough contact points.
    pub fn poll_discovery(
        &self,
        provider: &(impl DiscoveryProvider + ?Sized),
        observed_at_millis: u64,
    ) -> ClusterResult<ClusterUpdate> {
        let snapshot = provider.discover(observed_at_millis)?;
        self.cluster
            .update_at(observed_at_millis, |membership, observed_at_millis| {
                let mut events = membership.record_discovery(snapshot)?;
                events.extend(promote_joining_members(membership, observed_at_millis)?);
                Ok(events)
            })
    }

    /// Advances failure detection and downing policies once.
    pub fn tick(&self, now_millis: u64) -> ClusterResult<ClusterUpdate> {
        let failure_detector = self.failure_detector.clone();
        let downing_strategy = self.downing_strategy.clone();
        self.cluster
            .update_at(now_millis, |membership, now_millis| {
                let mut events = Vec::new();
                for node_id in failure_detector.unreachable_members(membership, now_millis) {
                    if let Some(event) = membership.mark_unreachable(&node_id, now_millis)? {
                        events.push(event);
                    }
                }
                for node_id in downing_strategy.down_members(membership, now_millis) {
                    if let Some(event) = membership.mark_down(&node_id, now_millis)? {
                        events.push(event);
                    }
                }
                Ok(events)
            })
    }
}

impl fmt::Debug for ClusterRuntime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterRuntime")
            .field("cluster", &self.cluster)
            .field("settings", &self.settings)
            .field("failure_detector", &self.failure_detector)
            .field("downing_strategy", &self.downing_strategy)
            .finish_non_exhaustive()
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

fn promote_joining_members(
    membership: &mut ClusterMembership,
    observed_at_millis: u64,
) -> ClusterResult<Vec<MembershipEvent>> {
    if !membership.has_min_contact_points() {
        return Ok(Vec::new());
    }

    let joining = membership
        .members()
        .filter(|member| member.state() == MembershipState::Joining)
        .map(|member| member.node().id().clone())
        .collect::<Vec<_>>();
    let mut events = Vec::new();
    for node_id in joining {
        if let Some(event) = membership.mark_up(&node_id, observed_at_millis)? {
            events.push(event);
        }
    }
    Ok(events)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
