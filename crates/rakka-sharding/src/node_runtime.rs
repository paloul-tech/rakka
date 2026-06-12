//! Networked cluster node runtime that connects membership, TCP remoting, and sharding.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoveryProvider, DiscoverySnapshot, MemberRecord,
    MembershipConfig, MembershipState, NodeAddress, NodeId,
};
use rakka_core::{Message, MetricsRecorder, NoopMetricsRecorder, ReplyTo};
use rakka_remote::{
    RemoteEndpoint, RemoteEndpointError, RemoteEnvelopeHandler, RemoteRequestRegistry,
    RemoteTransportError, SerializationRegistry, TcpRemoteTransport, TcpRemoteTransportConfig,
    TcpRemoteTransportError, TcpRemoteTransportSnapshot,
};

use crate::{
    AsyncShardCoordinatorStore, ClusterShardingError, ClusterShardingRuntime,
    ClusterShardingUpdate, EntityRoute, RemoteEntityAskClient, RemoteEntityAskInbound,
    RemoteEntityInbound, RemoteEntityRoute, RemoteTransportEntityOutbound, ShardCoordinatorStore,
    ShardRegion,
};

/// Convenient result alias for networked cluster node runtime operations.
pub type ClusterNodeRuntimeResult<T> = Result<T, ClusterNodeRuntimeError>;

/// Failure returned by networked cluster node runtime operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterNodeRuntimeError {
    /// TCP transport setup or network loop operation failed.
    TcpTransport {
        /// TCP transport failure.
        error: TcpRemoteTransportError,
    },
    /// Remote endpoint handler registration failed.
    Endpoint {
        /// Remote endpoint failure.
        error: RemoteEndpointError,
    },
    /// Remote transport peer registration failed.
    Transport {
        /// Remote transport failure.
        error: RemoteTransportError,
    },
    /// Cluster/sharding membership or ownership update failed.
    Sharding {
        /// Cluster/sharding failure.
        error: ClusterShardingError,
    },
}

impl Display for ClusterNodeRuntimeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::TcpTransport { error } => Display::fmt(error, f),
            Self::Endpoint { error } => Display::fmt(error, f),
            Self::Transport { error } => Display::fmt(error, f),
            Self::Sharding { error } => Display::fmt(error, f),
        }
    }
}

impl Error for ClusterNodeRuntimeError {}

impl From<TcpRemoteTransportError> for ClusterNodeRuntimeError {
    fn from(error: TcpRemoteTransportError) -> Self {
        Self::TcpTransport { error }
    }
}

impl From<RemoteEndpointError> for ClusterNodeRuntimeError {
    fn from(error: RemoteEndpointError) -> Self {
        Self::Endpoint { error }
    }
}

impl From<RemoteTransportError> for ClusterNodeRuntimeError {
    fn from(error: RemoteTransportError) -> Self {
        Self::Transport { error }
    }
}

impl From<ClusterShardingError> for ClusterNodeRuntimeError {
    fn from(error: ClusterShardingError) -> Self {
        Self::Sharding { error }
    }
}

/// Builder for a networked cluster node runtime.
pub struct ClusterNodeRuntimeBuilder {
    local_node: ClusterNode,
    membership_config: MembershipConfig,
    transport_config: TcpRemoteTransportConfig,
    registry: SerializationRegistry,
    recorder: Arc<dyn MetricsRecorder>,
    advertise_bound_addr: bool,
    coordinator_store: Option<CoordinatorStoreBuilderMode>,
}

enum CoordinatorStoreBuilderMode {
    Sync(Arc<dyn ShardCoordinatorStore>),
    Async(Arc<dyn AsyncShardCoordinatorStore>),
}

impl CoordinatorStoreBuilderMode {
    fn backend_name(&self) -> &'static str {
        match self {
            Self::Sync(store) => ShardCoordinatorStore::backend_name(store.as_ref()),
            Self::Async(store) => AsyncShardCoordinatorStore::backend_name(store.as_ref()),
        }
    }
}

impl ClusterNodeRuntimeBuilder {
    /// Creates a builder for one local cluster node.
    #[must_use]
    pub fn new(local_node: ClusterNode) -> Self {
        Self {
            local_node,
            membership_config: MembershipConfig::default(),
            transport_config: TcpRemoteTransportConfig::default(),
            registry: SerializationRegistry::new(),
            recorder: Arc::new(NoopMetricsRecorder),
            advertise_bound_addr: false,
            coordinator_store: None,
        }
    }

    /// Sets membership behavior for the node runtime.
    #[must_use]
    pub fn with_membership_config(mut self, membership_config: MembershipConfig) -> Self {
        self.membership_config = membership_config;
        self
    }

    /// Sets the TCP transport configuration used to bind remoting.
    #[must_use]
    pub fn with_transport_config(mut self, transport_config: TcpRemoteTransportConfig) -> Self {
        self.transport_config = transport_config;
        self
    }

    /// Sets the serialization registry shared by remote entity handlers and ask replies.
    #[must_use]
    pub fn with_registry(mut self, registry: SerializationRegistry) -> Self {
        self.registry = registry;
        self
    }

    /// Sets the metrics recorder used by the TCP transport.
    #[must_use]
    pub fn with_metrics(mut self, recorder: Arc<dyn MetricsRecorder>) -> Self {
        self.recorder = recorder;
        self
    }

    /// Advertises the actual bound socket address after binding.
    ///
    /// This is useful for local tests and examples that bind to port `0`.
    /// Kubernetes deployments should usually advertise their stable pod DNS
    /// address instead.
    #[must_use]
    pub const fn advertise_bound_addr(mut self, advertise_bound_addr: bool) -> Self {
        self.advertise_bound_addr = advertise_bound_addr;
        self
    }

    /// Sets a durable store for shard coordinator ownership snapshots.
    #[must_use]
    pub fn with_shard_coordinator_store(
        mut self,
        coordinator_store: impl ShardCoordinatorStore,
    ) -> Self {
        self.coordinator_store = Some(CoordinatorStoreBuilderMode::Sync(Arc::new(
            coordinator_store,
        )));
        self
    }

    /// Sets a shared durable store for shard coordinator ownership snapshots.
    #[must_use]
    pub fn with_shard_coordinator_store_ref(
        mut self,
        coordinator_store: Arc<dyn ShardCoordinatorStore>,
    ) -> Self {
        self.coordinator_store = Some(CoordinatorStoreBuilderMode::Sync(coordinator_store));
        self
    }

    /// Sets an async durable store for shard coordinator ownership snapshots.
    #[must_use]
    pub fn with_async_shard_coordinator_store(
        mut self,
        coordinator_store: impl AsyncShardCoordinatorStore,
    ) -> Self {
        self.coordinator_store = Some(CoordinatorStoreBuilderMode::Async(Arc::new(
            coordinator_store,
        )));
        self
    }

    /// Sets a shared async durable store for shard coordinator ownership snapshots.
    #[must_use]
    pub fn with_async_shard_coordinator_store_ref(
        mut self,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
    ) -> Self {
        self.coordinator_store = Some(CoordinatorStoreBuilderMode::Async(coordinator_store));
        self
    }

    /// Binds TCP remoting and creates the cluster node runtime.
    pub async fn build(self) -> ClusterNodeRuntimeResult<ClusterNodeRuntime> {
        let endpoint = RemoteEndpoint::new(self.local_node.id().clone());
        let requests = RemoteRequestRegistry::new(self.registry.clone());
        endpoint.register_reply_handler(requests.clone());

        let transport = TcpRemoteTransport::bind_with_metrics(
            self.local_node.id().clone(),
            self.local_node.protocol(),
            endpoint.clone(),
            self.transport_config,
            self.recorder,
        )
        .await?;
        let local_node = advertised_node(
            self.local_node,
            transport.local_addr(),
            self.advertise_bound_addr,
        );
        let membership = ClusterMembership::new(local_node.clone(), self.membership_config);
        let sharding = match self.coordinator_store {
            Some(CoordinatorStoreBuilderMode::Sync(store)) => {
                ClusterShardingRuntime::with_coordinator_store_ref(membership, store)
            }
            Some(CoordinatorStoreBuilderMode::Async(store)) => {
                ClusterShardingRuntime::with_async_coordinator_store_ref(membership, store)
            }
            None => ClusterShardingRuntime::new(membership),
        };

        Ok(ClusterNodeRuntime {
            local_node,
            endpoint,
            requests,
            transport,
            sharding,
            registry: self.registry,
            registered_peers: BTreeSet::new(),
        })
    }
}

impl Debug for ClusterNodeRuntimeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterNodeRuntimeBuilder")
            .field("local_node", &self.local_node)
            .field("membership_config", &self.membership_config)
            .field("transport_config", &self.transport_config)
            .field("advertise_bound_addr", &self.advertise_bound_addr)
            .field(
                "coordinator_store",
                &self
                    .coordinator_store
                    .as_ref()
                    .map(CoordinatorStoreBuilderMode::backend_name),
            )
            .finish_non_exhaustive()
    }
}

/// Result of one networked cluster node runtime update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNodeRuntimeUpdate {
    sharding: ClusterShardingUpdate,
    registered_peers: usize,
}

impl ClusterNodeRuntimeUpdate {
    /// Creates a networked runtime update.
    #[must_use]
    pub const fn new(sharding: ClusterShardingUpdate, registered_peers: usize) -> Self {
        Self {
            sharding,
            registered_peers,
        }
    }

    /// Cluster/sharding update applied by the underlying runtime.
    #[must_use]
    pub const fn sharding(&self) -> &ClusterShardingUpdate {
        &self.sharding
    }

    /// Number of newly registered TCP remoting peers.
    #[must_use]
    pub const fn registered_peers(&self) -> usize {
        self.registered_peers
    }

    /// Returns true when neither membership/ownership nor peer registration changed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sharding.is_empty() && self.registered_peers == 0
    }
}

/// Runtime for one networked Rakka cluster node.
pub struct ClusterNodeRuntime {
    local_node: ClusterNode,
    endpoint: RemoteEndpoint,
    requests: RemoteRequestRegistry,
    transport: TcpRemoteTransport,
    sharding: ClusterShardingRuntime,
    registry: SerializationRegistry,
    registered_peers: BTreeSet<NodeId>,
}

impl ClusterNodeRuntime {
    /// Creates a builder for one local cluster node.
    #[must_use]
    pub fn builder(local_node: ClusterNode) -> ClusterNodeRuntimeBuilder {
        ClusterNodeRuntimeBuilder::new(local_node)
    }

    /// Local node descriptor advertised through membership and discovery.
    #[must_use]
    pub const fn local_node(&self) -> &ClusterNode {
        &self.local_node
    }

    /// Remote endpoint that dispatches inbound envelopes for this node.
    #[must_use]
    pub const fn endpoint(&self) -> &RemoteEndpoint {
        &self.endpoint
    }

    /// Remote request registry used for ask reply correlation.
    #[must_use]
    pub const fn requests(&self) -> &RemoteRequestRegistry {
        &self.requests
    }

    /// TCP transport used for remote envelopes.
    #[must_use]
    pub const fn transport(&self) -> &TcpRemoteTransport {
        &self.transport
    }

    /// Shared serialization registry.
    #[must_use]
    pub const fn registry(&self) -> &SerializationRegistry {
        &self.registry
    }

    /// Underlying cluster/sharding runtime.
    #[must_use]
    pub const fn sharding(&self) -> &ClusterShardingRuntime {
        &self.sharding
    }

    /// Mutable access to the underlying cluster/sharding runtime.
    #[must_use]
    pub const fn sharding_mut(&mut self) -> &mut ClusterShardingRuntime {
        &mut self.sharding
    }

    /// Number of remote peers registered with the TCP transport by this runtime.
    #[must_use]
    pub fn registered_peer_count(&self) -> usize {
        self.registered_peers.len()
    }

    /// Snapshot of the TCP remoting transport.
    #[must_use]
    pub fn transport_snapshot(&self) -> TcpRemoteTransportSnapshot {
        self.transport.snapshot()
    }

    /// Creates an outbound adapter backed by this runtime's TCP transport.
    #[must_use]
    pub fn outbound(&self) -> RemoteTransportEntityOutbound<TcpRemoteTransport> {
        RemoteTransportEntityOutbound::new(self.transport.clone())
    }

    /// Creates a remote-aware entity route backed by this runtime's TCP transport.
    #[must_use]
    pub fn remote_route<M, L>(
        &self,
        local_route: L,
    ) -> RemoteEntityRoute<M, L, RemoteTransportEntityOutbound<TcpRemoteTransport>>
    where
        M: Message + Sync,
        L: EntityRoute<M>,
    {
        RemoteEntityRoute::new(local_route, self.registry.clone(), self.outbound())
            .with_source(self.local_node.id().to_string())
    }

    /// Creates a remote ask client backed by this runtime's request registry and TCP transport.
    #[must_use]
    pub fn ask_client(&self) -> RemoteEntityAskClient<TcpRemoteTransport> {
        RemoteEntityAskClient::new(
            self.local_node.id().clone(),
            self.requests.clone(),
            self.transport.clone(),
        )
    }

    /// Registers a shard region for ownership refresh only.
    pub fn register_region<M>(&mut self, region: ShardRegion<M>) -> ClusterNodeRuntimeResult<()>
    where
        M: Message,
    {
        self.sharding.register_region(region)?;
        Ok(())
    }

    /// Registers a shard region through async durable coordinator storage when configured.
    pub async fn register_region_async<M>(
        &mut self,
        region: ShardRegion<M>,
    ) -> ClusterNodeRuntimeResult<()>
    where
        M: Message,
    {
        self.sharding.register_region_async(region).await?;
        Ok(())
    }

    /// Registers a shard region and a default inbound remote tell handler for its entity type.
    pub fn register_entity_region<M>(
        &mut self,
        region: ShardRegion<M>,
    ) -> ClusterNodeRuntimeResult<()>
    where
        M: Message + Sync,
    {
        let entity_type = region.entity_type().clone();
        self.sharding.register_region(region.clone())?;
        self.endpoint.register_entity_handler(
            entity_type.as_str(),
            RemoteEntityInbound::new(region, self.registry.clone()),
        )?;
        Ok(())
    }

    /// Registers a shard region and default inbound remote tell handler through async storage.
    pub async fn register_entity_region_async<M>(
        &mut self,
        region: ShardRegion<M>,
    ) -> ClusterNodeRuntimeResult<()>
    where
        M: Message + Sync,
    {
        let entity_type = region.entity_type().clone();
        self.sharding.register_region_async(region.clone()).await?;
        self.endpoint.register_entity_handler(
            entity_type.as_str(),
            RemoteEntityInbound::new(region, self.registry.clone()),
        )?;
        Ok(())
    }

    /// Registers a shard region and a custom inbound remote envelope handler for its entity type.
    pub fn register_entity_handler<M>(
        &mut self,
        region: ShardRegion<M>,
        handler: impl RemoteEnvelopeHandler,
    ) -> ClusterNodeRuntimeResult<()>
    where
        M: Message,
    {
        let entity_type = region.entity_type().clone();
        self.sharding.register_region(region)?;
        self.endpoint
            .register_entity_handler(entity_type.as_str(), handler)?;
        Ok(())
    }

    /// Registers a shard region and custom inbound remote handler through async storage.
    pub async fn register_entity_handler_async<M>(
        &mut self,
        region: ShardRegion<M>,
        handler: impl RemoteEnvelopeHandler,
    ) -> ClusterNodeRuntimeResult<()>
    where
        M: Message,
    {
        let entity_type = region.entity_type().clone();
        self.sharding.register_region_async(region).await?;
        self.endpoint
            .register_entity_handler(entity_type.as_str(), handler)?;
        Ok(())
    }

    /// Registers a shard region and an inbound remote ask handler for its entity type.
    pub fn register_entity_ask_region<Q, M, R, B>(
        &mut self,
        region: ShardRegion<M>,
        build: B,
    ) -> ClusterNodeRuntimeResult<()>
    where
        Q: Message + Sync,
        M: Message,
        R: Send + Sync + 'static,
        B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
    {
        let handler = RemoteEntityAskInbound::new(
            self.local_node.id().clone(),
            region.clone(),
            self.registry.clone(),
            self.transport.clone(),
            build,
        );
        self.register_entity_handler(region, handler)
    }

    /// Registers a shard region and inbound remote ask handler through async storage.
    pub async fn register_entity_ask_region_async<Q, M, R, B>(
        &mut self,
        region: ShardRegion<M>,
        build: B,
    ) -> ClusterNodeRuntimeResult<()>
    where
        Q: Message + Sync,
        M: Message,
        R: Send + Sync + 'static,
        B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
    {
        let handler = RemoteEntityAskInbound::new(
            self.local_node.id().clone(),
            region.clone(),
            self.registry.clone(),
            self.transport.clone(),
            build,
        );
        self.register_entity_handler_async(region, handler).await
    }

    /// Applies a discovery snapshot, refreshes sharding, and registers newly known TCP peers.
    pub fn apply_discovery(
        &mut self,
        snapshot: DiscoverySnapshot,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let update = self.sharding.apply_discovery(snapshot)?;
        let registered_peers = self.register_peers_from_membership()?;
        Ok(ClusterNodeRuntimeUpdate::new(update, registered_peers))
    }

    /// Applies a discovery snapshot through async durable storage and registers TCP peers.
    pub async fn apply_discovery_async(
        &mut self,
        snapshot: DiscoverySnapshot,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let update = self.sharding.apply_discovery_async(snapshot).await?;
        let registered_peers = self.register_peers_from_membership()?;
        Ok(ClusterNodeRuntimeUpdate::new(update, registered_peers))
    }

    /// Polls a discovery provider, applies the snapshot, and registers newly known TCP peers.
    pub fn poll_discovery(
        &mut self,
        provider: &impl DiscoveryProvider,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let snapshot = provider
            .discover(observed_at_millis)
            .map_err(ClusterShardingError::from)?;
        self.apply_discovery(snapshot)
    }

    /// Polls discovery, applies the snapshot through async storage, and registers peers.
    pub async fn poll_discovery_async(
        &mut self,
        provider: &impl DiscoveryProvider,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let snapshot = provider
            .discover(observed_at_millis)
            .map_err(ClusterShardingError::from)?;
        self.apply_discovery_async(snapshot).await
    }

    /// Records a heartbeat and refreshes ownership if membership changed.
    pub fn heartbeat(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self.sharding.heartbeat(node_id, observed_at_millis)?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    /// Records a heartbeat and refreshes ownership through async storage.
    pub async fn heartbeat_async(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self
            .sharding
            .heartbeat_async(node_id, observed_at_millis)
            .await?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    /// Begins graceful leave and refreshes ownership.
    pub fn mark_leaving(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self.sharding.mark_leaving(node_id, observed_at_millis)?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    /// Begins graceful leave and refreshes ownership through async storage.
    pub async fn mark_leaving_async(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self
            .sharding
            .mark_leaving_async(node_id, observed_at_millis)
            .await?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    /// Begins graceful leave for the local node and refreshes ownership.
    pub fn leave_local(
        &mut self,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let node_id = self.local_node.id().clone();
        self.mark_leaving(&node_id, observed_at_millis)
    }

    /// Begins graceful leave for the local node through async storage.
    pub async fn leave_local_async(
        &mut self,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let node_id = self.local_node.id().clone();
        self.mark_leaving_async(&node_id, observed_at_millis).await
    }

    /// Marks a member down and refreshes ownership.
    pub fn mark_down(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self.sharding.mark_down(node_id, observed_at_millis)?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    /// Marks a member down and refreshes ownership through async storage.
    pub async fn mark_down_async(
        &mut self,
        node_id: &NodeId,
        observed_at_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self
            .sharding
            .mark_down_async(node_id, observed_at_millis)
            .await?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    /// Advances failure detection and refreshes ownership after unreachable/down events.
    pub fn tick(&mut self, now_millis: u64) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self.sharding.tick(now_millis)?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    /// Advances failure detection and refreshes ownership through async storage.
    pub async fn tick_async(
        &mut self,
        now_millis: u64,
    ) -> ClusterNodeRuntimeResult<ClusterNodeRuntimeUpdate> {
        let sharding = self.sharding.tick_async(now_millis).await?;
        Ok(ClusterNodeRuntimeUpdate::new(sharding, 0))
    }

    fn register_peers_from_membership(&mut self) -> ClusterNodeRuntimeResult<usize> {
        let peers = self
            .sharding
            .membership()
            .snapshot()
            .members()
            .iter()
            .filter(|record| should_register_peer(record, self.local_node.id()))
            .map(|record| record.node().clone())
            .collect::<Vec<_>>();
        let mut registered = 0;

        for peer in peers {
            if self.registered_peers.contains(peer.id()) {
                continue;
            }
            match self.transport.register_peer(peer.clone()) {
                Ok(()) => {
                    self.registered_peers.insert(peer.id().clone());
                    registered += 1;
                }
                Err(RemoteTransportError::DuplicateNode { node_id }) => {
                    self.registered_peers.insert(node_id);
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(registered)
    }
}

impl Debug for ClusterNodeRuntime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterNodeRuntime")
            .field("local_node", &self.local_node)
            .field("registered_peer_count", &self.registered_peer_count())
            .field("transport", &self.transport)
            .field("sharding", &self.sharding)
            .finish_non_exhaustive()
    }
}

fn should_register_peer(record: &MemberRecord, local_node_id: &NodeId) -> bool {
    record.node().id() != local_node_id
        && matches!(
            record.state(),
            MembershipState::Joining
                | MembershipState::Up
                | MembershipState::Leaving
                | MembershipState::Unreachable
        )
}

fn advertised_node(
    local_node: ClusterNode,
    local_addr: SocketAddr,
    advertise_bound_addr: bool,
) -> ClusterNode {
    if !advertise_bound_addr && local_node.address().port() != 0 {
        return local_node;
    }

    let host = if local_node.address().host().is_empty() {
        socket_host(local_addr.ip())
    } else {
        local_node.address().host().to_string()
    };
    local_node.with_address(NodeAddress::new(host, local_addr.port()))
}

fn socket_host(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(address) if address == Ipv4Addr::UNSPECIFIED => Ipv4Addr::LOCALHOST.to_string(),
        IpAddr::V4(address) => address.to_string(),
        IpAddr::V6(address) => address.to_string(),
    }
}
