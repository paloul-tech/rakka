//! Akka-style high-level cluster sharding facade.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::{ClusterMembership, ClusterNode, MembershipConfig, NodeAddress, NodeId};
use rakka_core::{Actor, ActorOptions, ActorSystem, Message, ReplyTo};
use rakka_remote::{RemoteEnvelope, RemoteEnvelopeHandler, RemoteTransport};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    AsyncShardCoordinatorStore, ClusterNodeRuntime, ClusterNodeRuntimeResult, ClusterShardingError,
    ClusterShardingResult, ClusterShardingRuntime, EntityAskError, EntityId, EntityRef,
    EntityTellError, EntityType, LeastShardAllocationStrategy, LocalEntityContext,
    LocalEntityRoute, RememberedEntities, RemoteEntityAskClient, RemoteEntityAskError,
    RemoteEntityAskInbound, RemoteEntityInbound, ShardAllocationStrategy, ShardBufferConfig,
    ShardCoordinatorLease, ShardCoordinatorStore, ShardId, ShardKey, ShardRegion, ShardingConfig,
};

type RegionRegistry = Arc<Mutex<BTreeMap<EntityType, Box<dyn Any + Send + Sync>>>>;
type LocalControlRegistry = Arc<Mutex<BTreeMap<EntityType, Arc<dyn LocalEntityControl>>>>;
type StateRegistry = Arc<Mutex<BTreeMap<EntityType, EntityTypeRegistrationState>>>;
type RememberedRegistry = Arc<Mutex<BTreeMap<EntityType, RememberedEntities>>>;
type StopMessageFactory<M> = Arc<dyn Fn() -> M + Send + Sync>;
const DEFAULT_PASSIVATION_BUFFER_DURATION: Duration = Duration::from_millis(25);

/// Akka-style typed key for one sharded entity protocol.
#[derive(Debug, PartialEq, Eq)]
pub struct EntityTypeKey<M> {
    entity_type: EntityType,
    config: ShardingConfig,
    _message: PhantomData<fn(M)>,
}

impl<M> EntityTypeKey<M> {
    /// Creates a key with the default sharding configuration.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            entity_type: EntityType::new(name),
            config: ShardingConfig::default(),
            _message: PhantomData,
        }
    }

    /// Creates a key from an existing entity type and sharding configuration.
    #[must_use]
    pub fn with_config(mut self, config: ShardingConfig) -> Self {
        self.config = config;
        self
    }

    /// Creates a key with an explicit number of shards.
    pub fn with_number_of_shards(mut self, number_of_shards: u32) -> crate::ShardingResult<Self> {
        self.config = ShardingConfig::new(number_of_shards)?;
        Ok(self)
    }

    /// Entity type carried by this key.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Sharding configuration carried by this key.
    #[must_use]
    pub const fn config(&self) -> &ShardingConfig {
        &self.config
    }
}

impl<M> Clone for EntityTypeKey<M> {
    fn clone(&self) -> Self {
        Self {
            entity_type: self.entity_type.clone(),
            config: self.config.clone(),
            _message: PhantomData,
        }
    }
}

/// Context supplied to a sharded entity actor factory.
#[derive(Debug, PartialEq, Eq)]
pub struct EntityContext<M> {
    key: EntityTypeKey<M>,
    local: LocalEntityContext,
}

impl<M> EntityContext<M> {
    /// Creates an entity context from a typed key and local route context.
    #[must_use]
    pub fn new(key: EntityTypeKey<M>, local: LocalEntityContext) -> Self {
        Self { key, local }
    }

    /// Typed entity key.
    #[must_use]
    pub const fn entity_type_key(&self) -> &EntityTypeKey<M> {
        &self.key
    }

    /// Entity type.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        self.local.entity_type()
    }

    /// Entity id.
    #[must_use]
    pub fn entity_id(&self) -> &EntityId {
        self.local.entity_id()
    }

    /// Owning shard id.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.local.shard_id()
    }

    /// Local cluster node id.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        self.local.local_node_id()
    }

    /// Stable local actor name used for this entity incarnation.
    #[must_use]
    pub fn actor_name(&self) -> &str {
        self.local.actor_name()
    }

    /// Low-level local entity context.
    #[must_use]
    pub const fn local_context(&self) -> &LocalEntityContext {
        &self.local
    }

    /// Akka-style persistence id string for this entity.
    ///
    /// This matches the `rakka_persistence::PersistenceId::of(entity_type, entity_id)`
    /// convention without adding a dependency cycle from sharding to persistence.
    #[must_use]
    pub fn persistence_id(&self) -> String {
        format!(
            "{}|{}",
            self.entity_type().as_str(),
            self.entity_id().as_str()
        )
    }
}

impl<M> Clone for EntityContext<M> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            local: self.local.clone(),
        }
    }
}

/// Passivation command describing one entity to stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Passivate<M> {
    entity_id: EntityId,
    stop_message: Option<M>,
}

impl<M> Passivate<M> {
    /// Creates a passivation request without an entity stop message.
    #[must_use]
    pub fn new(entity_id: impl Into<String>) -> Self {
        Self {
            entity_id: EntityId::new(entity_id),
            stop_message: None,
        }
    }

    /// Creates a passivation request with an entity stop message.
    #[must_use]
    pub fn with_stop_message(entity_id: impl Into<String>, stop_message: M) -> Self {
        Self {
            entity_id: EntityId::new(entity_id),
            stop_message: Some(stop_message),
        }
    }

    /// Entity id to passivate.
    #[must_use]
    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    /// Optional stop message to deliver before stopping.
    #[must_use]
    pub const fn stop_message(&self) -> Option<&M> {
        self.stop_message.as_ref()
    }

    /// Consumes the command and returns its parts.
    #[must_use]
    pub fn into_parts(self) -> (EntityId, Option<M>) {
        (self.entity_id, self.stop_message)
    }
}

/// Entity factory and settings used by [`ClusterSharding::init`].
pub struct Entity<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
{
    key: EntityTypeKey<M>,
    factory: F,
    actor_options: ActorOptions,
    idle_passivation_timeout: Option<Duration>,
    stop_message_factory: Option<StopMessageFactory<M>>,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    remembered_entities: Option<RememberedEntities>,
}

impl<M, A, F> Entity<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
{
    /// Creates an entity definition from a type key and actor factory.
    #[must_use]
    pub fn of(key: EntityTypeKey<M>, factory: F) -> Self {
        Self {
            key,
            factory,
            actor_options: ActorOptions::default(),
            idle_passivation_timeout: None,
            stop_message_factory: None,
            buffer_config: Some(ShardBufferConfig::default()),
            passivation_buffer_duration: DEFAULT_PASSIVATION_BUFFER_DURATION,
            allocation_strategy: Arc::new(crate::DeterministicModuloShardAllocationStrategy::new()),
            remembered_entities: None,
        }
    }

    /// Sets options used for newly spawned local entity actors.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation after the given inactivity timeout.
    #[must_use]
    pub fn with_idle_passivation(mut self, timeout: Duration) -> Self {
        self.idle_passivation_timeout = Some(timeout);
        self
    }

    /// Configures bounded buffering for shard handoff, owner refresh, and passivation windows.
    #[must_use]
    pub fn with_buffering(mut self, config: ShardBufferConfig) -> Self {
        self.buffer_config = Some(config);
        self
    }

    /// Configures buffering with the given capacity and default overflow/TTL.
    #[must_use]
    pub fn with_handoff_buffer(mut self, capacity_per_shard: usize) -> Self {
        self.buffer_config =
            Some(ShardBufferConfig::default().with_capacity_per_shard(capacity_per_shard));
        self
    }

    /// Disables facade-level buffering for this entity type.
    #[must_use]
    pub fn without_buffering(mut self) -> Self {
        self.buffer_config = None;
        self
    }

    /// Sets how long explicit facade passivation should buffer incoming entity messages.
    #[must_use]
    pub const fn with_passivation_buffer_duration(mut self, duration: Duration) -> Self {
        self.passivation_buffer_duration = duration;
        self
    }

    /// Sets the shard allocation strategy for this entity type.
    #[must_use]
    pub fn with_allocation_strategy(
        mut self,
        allocation_strategy: impl ShardAllocationStrategy,
    ) -> Self {
        self.allocation_strategy = Arc::new(allocation_strategy);
        self
    }

    /// Sets a shared shard allocation strategy for this entity type.
    #[must_use]
    pub fn with_allocation_strategy_ref(
        mut self,
        allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Self {
        self.allocation_strategy = allocation_strategy;
        self
    }

    /// Uses the built-in least-shard allocation strategy for this entity type.
    #[must_use]
    pub fn with_least_shard_allocation(
        self,
        rebalance_threshold: usize,
        max_simultaneous_rebalance: usize,
    ) -> Self {
        self.with_allocation_strategy(LeastShardAllocationStrategy::new(
            rebalance_threshold,
            max_simultaneous_rebalance,
        ))
    }

    /// Enables remembered entities for this entity type.
    #[must_use]
    pub fn with_remembered_entities(mut self, remembered_entities: RememberedEntities) -> Self {
        self.remembered_entities = Some(remembered_entities);
        self
    }

    /// Configures a stop message delivered immediately before facade passivation stops an entity.
    #[must_use]
    pub fn with_stop_message(mut self, stop_message: M) -> Self
    where
        M: Clone,
    {
        let stop_message = Arc::new(Mutex::new(stop_message));
        self.stop_message_factory = Some(Arc::new(move || {
            stop_message
                .lock()
                .expect("entity stop message mutex poisoned")
                .clone()
        }));
        self
    }

    /// Configures a stop-message factory for explicit facade passivation.
    #[must_use]
    pub fn with_stop_message_factory(
        mut self,
        factory: impl Fn() -> M + Send + Sync + 'static,
    ) -> Self {
        self.stop_message_factory = Some(Arc::new(factory));
        self
    }

    /// Typed entity key.
    #[must_use]
    pub const fn key(&self) -> &EntityTypeKey<M> {
        &self.key
    }

    /// Configured idle passivation timeout.
    #[must_use]
    pub const fn idle_passivation_timeout(&self) -> Option<Duration> {
        self.idle_passivation_timeout
    }

    /// Returns true when a stop message has been configured.
    #[must_use]
    pub fn has_stop_message(&self) -> bool {
        self.stop_message_factory.is_some()
    }

    /// Configured shard buffering policy, when enabled.
    #[must_use]
    pub const fn buffer_config(&self) -> Option<&ShardBufferConfig> {
        self.buffer_config.as_ref()
    }

    /// Explicit passivation buffering window.
    #[must_use]
    pub const fn passivation_buffer_duration(&self) -> Duration {
        self.passivation_buffer_duration
    }

    /// Configured shard allocation strategy.
    #[must_use]
    pub fn allocation_strategy(&self) -> Arc<dyn ShardAllocationStrategy> {
        self.allocation_strategy.clone()
    }

    /// Stable shard allocation strategy name used for diagnostics.
    #[must_use]
    pub fn allocation_strategy_name(&self) -> &'static str {
        self.allocation_strategy.strategy_name()
    }

    /// Remembered entity settings, when enabled.
    #[must_use]
    pub const fn remembered_entities(&self) -> Option<&RememberedEntities> {
        self.remembered_entities.as_ref()
    }
}

impl<M, A, F> Debug for Entity<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entity")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("idle_passivation_timeout", &self.idle_passivation_timeout)
            .field("has_stop_message", &self.has_stop_message())
            .field("buffer_config", &self.buffer_config)
            .field(
                "passivation_buffer_duration",
                &self.passivation_buffer_duration,
            )
            .field("allocation_strategy", &self.allocation_strategy_name())
            .field("remembered_entities", &self.remembered_entities)
            .finish_non_exhaustive()
    }
}

/// High-level sharding extension facade.
#[derive(Clone)]
pub struct ClusterSharding {
    system: ActorSystem,
    local_node: ClusterNode,
    runtime: Arc<AsyncMutex<ClusterShardingRuntime>>,
    regions: RegionRegistry,
    controls: LocalControlRegistry,
    states: StateRegistry,
    remembered: RememberedRegistry,
}

impl ClusterSharding {
    /// Creates a local-only sharding facade for an actor system.
    #[must_use]
    pub fn get(system: &ActorSystem) -> Self {
        let local_node = local_node_for_system(system);
        Self::for_local_node(system, local_node, MembershipConfig::default())
            .expect("local cluster sharding facade should initialize")
    }

    /// Creates a local-only sharding facade with a durable shard coordinator store.
    #[must_use]
    pub fn get_with_coordinator_store(
        system: &ActorSystem,
        coordinator_store: impl ShardCoordinatorStore,
    ) -> Self {
        Self::get_with_coordinator_store_ref(system, Arc::new(coordinator_store))
    }

    /// Creates a local-only sharding facade with a shared durable shard coordinator store.
    #[must_use]
    pub fn get_with_coordinator_store_ref(
        system: &ActorSystem,
        coordinator_store: Arc<dyn ShardCoordinatorStore>,
    ) -> Self {
        let local_node = local_node_for_system(system);
        Self::for_local_node_with_coordinator_store_ref(
            system,
            local_node,
            MembershipConfig::default(),
            coordinator_store,
        )
        .expect("local cluster sharding facade should initialize")
    }

    /// Creates a local-only sharding facade with an async durable coordinator store.
    #[must_use]
    pub fn get_with_async_coordinator_store(
        system: &ActorSystem,
        coordinator_store: impl AsyncShardCoordinatorStore,
    ) -> Self {
        Self::get_with_async_coordinator_store_ref(system, Arc::new(coordinator_store))
    }

    /// Creates a local-only sharding facade with a shared async durable coordinator store.
    #[must_use]
    pub fn get_with_async_coordinator_store_ref(
        system: &ActorSystem,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
    ) -> Self {
        let local_node = local_node_for_system(system);
        Self::for_local_node_with_async_coordinator_store_ref(
            system,
            local_node,
            MembershipConfig::default(),
            coordinator_store,
        )
        .expect("local cluster sharding facade should initialize")
    }

    /// Creates a local-only sharding facade with async durable storage and a leadership lease.
    #[must_use]
    pub fn get_with_async_coordinator_store_and_lease(
        system: &ActorSystem,
        coordinator_store: impl AsyncShardCoordinatorStore,
        coordinator_lease: impl ShardCoordinatorLease,
    ) -> Self {
        Self::get_with_async_coordinator_store_and_lease_ref(
            system,
            Arc::new(coordinator_store),
            Arc::new(coordinator_lease),
        )
    }

    /// Creates a local-only sharding facade with shared async storage and leadership lease.
    #[must_use]
    pub fn get_with_async_coordinator_store_and_lease_ref(
        system: &ActorSystem,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
        coordinator_lease: Arc<dyn ShardCoordinatorLease>,
    ) -> Self {
        let local_node = local_node_for_system(system);
        Self::for_local_node_with_async_coordinator_store_and_lease_ref(
            system,
            local_node,
            MembershipConfig::default(),
            coordinator_store,
            coordinator_lease,
        )
        .expect("local cluster sharding facade should initialize")
    }

    /// Creates a sharding facade for an explicit local node descriptor.
    pub fn for_local_node(
        system: &ActorSystem,
        local_node: ClusterNode,
        config: MembershipConfig,
    ) -> ClusterShardingResult<Self> {
        let local_node_id = local_node.id().clone();
        let mut membership = ClusterMembership::new(local_node.clone(), config);
        membership.mark_up(&local_node_id, 0)?;
        Ok(Self::from_membership(system, local_node, membership))
    }

    /// Creates a sharding facade with a durable shard coordinator store.
    pub fn for_local_node_with_coordinator_store(
        system: &ActorSystem,
        local_node: ClusterNode,
        config: MembershipConfig,
        coordinator_store: impl ShardCoordinatorStore,
    ) -> ClusterShardingResult<Self> {
        Self::for_local_node_with_coordinator_store_ref(
            system,
            local_node,
            config,
            Arc::new(coordinator_store),
        )
    }

    /// Creates a sharding facade with a shared durable shard coordinator store.
    pub fn for_local_node_with_coordinator_store_ref(
        system: &ActorSystem,
        local_node: ClusterNode,
        config: MembershipConfig,
        coordinator_store: Arc<dyn ShardCoordinatorStore>,
    ) -> ClusterShardingResult<Self> {
        let local_node_id = local_node.id().clone();
        let mut membership = ClusterMembership::new(local_node.clone(), config);
        membership.mark_up(&local_node_id, 0)?;
        Ok(Self::from_membership_with_coordinator_store_ref(
            system,
            local_node,
            membership,
            coordinator_store,
        ))
    }

    /// Creates a sharding facade with an async durable shard coordinator store.
    pub fn for_local_node_with_async_coordinator_store(
        system: &ActorSystem,
        local_node: ClusterNode,
        config: MembershipConfig,
        coordinator_store: impl AsyncShardCoordinatorStore,
    ) -> ClusterShardingResult<Self> {
        Self::for_local_node_with_async_coordinator_store_ref(
            system,
            local_node,
            config,
            Arc::new(coordinator_store),
        )
    }

    /// Creates a sharding facade with a shared async durable shard coordinator store.
    pub fn for_local_node_with_async_coordinator_store_ref(
        system: &ActorSystem,
        local_node: ClusterNode,
        config: MembershipConfig,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
    ) -> ClusterShardingResult<Self> {
        let local_node_id = local_node.id().clone();
        let mut membership = ClusterMembership::new(local_node.clone(), config);
        membership.mark_up(&local_node_id, 0)?;
        Ok(Self::from_membership_with_async_coordinator_store_ref(
            system,
            local_node,
            membership,
            coordinator_store,
        ))
    }

    /// Creates a sharding facade with async durable storage and a leadership lease.
    pub fn for_local_node_with_async_coordinator_store_and_lease(
        system: &ActorSystem,
        local_node: ClusterNode,
        config: MembershipConfig,
        coordinator_store: impl AsyncShardCoordinatorStore,
        coordinator_lease: impl ShardCoordinatorLease,
    ) -> ClusterShardingResult<Self> {
        Self::for_local_node_with_async_coordinator_store_and_lease_ref(
            system,
            local_node,
            config,
            Arc::new(coordinator_store),
            Arc::new(coordinator_lease),
        )
    }

    /// Creates a sharding facade with shared async durable storage and leadership lease.
    pub fn for_local_node_with_async_coordinator_store_and_lease_ref(
        system: &ActorSystem,
        local_node: ClusterNode,
        config: MembershipConfig,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
        coordinator_lease: Arc<dyn ShardCoordinatorLease>,
    ) -> ClusterShardingResult<Self> {
        let local_node_id = local_node.id().clone();
        let mut membership = ClusterMembership::new(local_node.clone(), config);
        membership.mark_up(&local_node_id, 0)?;
        Ok(
            Self::from_membership_with_async_coordinator_store_and_lease_ref(
                system,
                local_node,
                membership,
                coordinator_store,
                coordinator_lease,
            ),
        )
    }

    /// Creates a facade companion for a networked cluster node runtime.
    ///
    /// Remote entity initialization methods on this facade register regions and
    /// endpoint handlers through the supplied [`ClusterNodeRuntime`]. The
    /// facade stores typed references and diagnostics for the same local node.
    pub fn for_node_runtime(
        system: &ActorSystem,
        runtime: &ClusterNodeRuntime,
    ) -> ClusterShardingResult<Self> {
        Self::for_local_node(
            system,
            runtime.local_node().clone(),
            MembershipConfig::default(),
        )
    }

    /// Creates a facade from an existing membership table.
    #[must_use]
    pub fn from_membership(
        system: &ActorSystem,
        local_node: ClusterNode,
        membership: ClusterMembership,
    ) -> Self {
        Self {
            system: system.clone(),
            local_node,
            runtime: Arc::new(AsyncMutex::new(ClusterShardingRuntime::new(membership))),
            regions: Arc::new(Mutex::new(BTreeMap::new())),
            controls: Arc::new(Mutex::new(BTreeMap::new())),
            states: Arc::new(Mutex::new(BTreeMap::new())),
            remembered: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Creates a facade from an existing membership table with a durable coordinator store.
    #[must_use]
    pub fn from_membership_with_coordinator_store(
        system: &ActorSystem,
        local_node: ClusterNode,
        membership: ClusterMembership,
        coordinator_store: impl ShardCoordinatorStore,
    ) -> Self {
        Self::from_membership_with_coordinator_store_ref(
            system,
            local_node,
            membership,
            Arc::new(coordinator_store),
        )
    }

    /// Creates a facade from an existing membership table with a shared durable coordinator store.
    #[must_use]
    pub fn from_membership_with_coordinator_store_ref(
        system: &ActorSystem,
        local_node: ClusterNode,
        membership: ClusterMembership,
        coordinator_store: Arc<dyn ShardCoordinatorStore>,
    ) -> Self {
        Self {
            system: system.clone(),
            local_node,
            runtime: Arc::new(AsyncMutex::new(
                ClusterShardingRuntime::with_coordinator_store_ref(membership, coordinator_store),
            )),
            regions: Arc::new(Mutex::new(BTreeMap::new())),
            controls: Arc::new(Mutex::new(BTreeMap::new())),
            states: Arc::new(Mutex::new(BTreeMap::new())),
            remembered: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Creates a facade from an existing membership table with an async coordinator store.
    #[must_use]
    pub fn from_membership_with_async_coordinator_store(
        system: &ActorSystem,
        local_node: ClusterNode,
        membership: ClusterMembership,
        coordinator_store: impl AsyncShardCoordinatorStore,
    ) -> Self {
        Self::from_membership_with_async_coordinator_store_ref(
            system,
            local_node,
            membership,
            Arc::new(coordinator_store),
        )
    }

    /// Creates a facade from an existing membership table with a shared async coordinator store.
    #[must_use]
    pub fn from_membership_with_async_coordinator_store_ref(
        system: &ActorSystem,
        local_node: ClusterNode,
        membership: ClusterMembership,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
    ) -> Self {
        Self {
            system: system.clone(),
            local_node,
            runtime: Arc::new(AsyncMutex::new(
                ClusterShardingRuntime::with_async_coordinator_store_ref(
                    membership,
                    coordinator_store,
                ),
            )),
            regions: Arc::new(Mutex::new(BTreeMap::new())),
            controls: Arc::new(Mutex::new(BTreeMap::new())),
            states: Arc::new(Mutex::new(BTreeMap::new())),
            remembered: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Creates a facade from membership with async storage and a leadership lease.
    #[must_use]
    pub fn from_membership_with_async_coordinator_store_and_lease(
        system: &ActorSystem,
        local_node: ClusterNode,
        membership: ClusterMembership,
        coordinator_store: impl AsyncShardCoordinatorStore,
        coordinator_lease: impl ShardCoordinatorLease,
    ) -> Self {
        Self::from_membership_with_async_coordinator_store_and_lease_ref(
            system,
            local_node,
            membership,
            Arc::new(coordinator_store),
            Arc::new(coordinator_lease),
        )
    }

    /// Creates a facade from membership with shared async storage and leadership lease.
    #[must_use]
    pub fn from_membership_with_async_coordinator_store_and_lease_ref(
        system: &ActorSystem,
        local_node: ClusterNode,
        membership: ClusterMembership,
        coordinator_store: Arc<dyn AsyncShardCoordinatorStore>,
        coordinator_lease: Arc<dyn ShardCoordinatorLease>,
    ) -> Self {
        Self {
            system: system.clone(),
            local_node,
            runtime: Arc::new(AsyncMutex::new(
                ClusterShardingRuntime::with_async_coordinator_store_ref(
                    membership,
                    coordinator_store,
                )
                .with_coordinator_lease_ref(coordinator_lease),
            )),
            regions: Arc::new(Mutex::new(BTreeMap::new())),
            controls: Arc::new(Mutex::new(BTreeMap::new())),
            states: Arc::new(Mutex::new(BTreeMap::new())),
            remembered: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Actor system backing this facade.
    #[must_use]
    pub const fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Local cluster node descriptor backing this facade.
    #[must_use]
    pub const fn local_node(&self) -> &ClusterNode {
        &self.local_node
    }

    /// Shared low-level sharding runtime.
    #[must_use]
    pub fn runtime(&self) -> Arc<AsyncMutex<ClusterShardingRuntime>> {
        self.runtime.clone()
    }

    /// Initializes a sharded entity type on this node.
    pub fn init<M, A, F>(
        &self,
        entity: Entity<M, A, F>,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
        A: Actor<Msg = M>,
        F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
    {
        let Entity {
            key,
            factory,
            actor_options,
            idle_passivation_timeout,
            stop_message_factory,
            buffer_config,
            passivation_buffer_duration,
            allocation_strategy,
            remembered_entities,
        } = entity;
        let key_for_factory = key.clone();
        let local_node_id = self.local_node.id().clone();
        let system = self.system.clone();
        let mut local_route = LocalEntityRoute::new(local_node_id, system, move |local_context| {
            factory(EntityContext::new(key_for_factory.clone(), local_context))
        })
        .with_actor_options(actor_options);

        if let Some(timeout) = idle_passivation_timeout {
            local_route = local_route.with_idle_passivation(timeout);
        }
        local_route = apply_remembered_activation(local_route, remembered_entities.clone());

        let control = Arc::new(LocalRouteControl::new(
            local_route.clone(),
            stop_message_factory.clone(),
        ));
        let region = apply_remembered_entities(
            apply_buffering(
                apply_allocation_strategy(
                    ShardRegion::new(
                        key.entity_type().clone(),
                        key.config().clone(),
                        local_route.clone(),
                    ),
                    allocation_strategy,
                ),
                buffer_config.clone(),
            ),
            remembered_entities.clone(),
        );
        self.register_typed_region(
            key.clone(),
            region.clone(),
            RegistrationMode::Local {
                control,
                idle_passivation_timeout,
                has_stop_message: stop_message_factory.is_some(),
                buffer_config,
                passivation_buffer_duration,
                remembered_entities,
            },
        )?;

        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Initializes a sharded entity type on this node through async coordinator storage.
    pub async fn init_async<M, A, F>(
        &self,
        entity: Entity<M, A, F>,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
        A: Actor<Msg = M>,
        F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
    {
        let Entity {
            key,
            factory,
            actor_options,
            idle_passivation_timeout,
            stop_message_factory,
            buffer_config,
            passivation_buffer_duration,
            allocation_strategy,
            remembered_entities,
        } = entity;
        let key_for_factory = key.clone();
        let local_node_id = self.local_node.id().clone();
        let system = self.system.clone();
        let mut local_route = LocalEntityRoute::new(local_node_id, system, move |local_context| {
            factory(EntityContext::new(key_for_factory.clone(), local_context))
        })
        .with_actor_options(actor_options);

        if let Some(timeout) = idle_passivation_timeout {
            local_route = local_route.with_idle_passivation(timeout);
        }
        local_route = apply_remembered_activation(local_route, remembered_entities.clone());

        let control = Arc::new(LocalRouteControl::new(
            local_route.clone(),
            stop_message_factory.clone(),
        ));
        let region = apply_remembered_entities(
            apply_buffering(
                apply_allocation_strategy(
                    ShardRegion::new(
                        key.entity_type().clone(),
                        key.config().clone(),
                        local_route.clone(),
                    ),
                    allocation_strategy,
                ),
                buffer_config.clone(),
            ),
            remembered_entities.clone(),
        );
        self.register_typed_region_async(
            key.clone(),
            region.clone(),
            RegistrationMode::Local {
                control,
                idle_passivation_timeout,
                has_stop_message: stop_message_factory.is_some(),
                buffer_config,
                passivation_buffer_duration,
                remembered_entities,
            },
        )
        .await?;

        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Initializes a remote-aware sharded entity type through a node runtime.
    ///
    /// This creates the local entity route, wraps it in a remote route, registers
    /// the shard region with the node runtime, and installs the default inbound
    /// remote tell handler for the entity type.
    pub fn init_remote<M, A, F>(
        &self,
        runtime: &mut ClusterNodeRuntime,
        entity: Entity<M, A, F>,
    ) -> ClusterNodeRuntimeResult<EntityTypeRegistration<M>>
    where
        M: Message + Sync,
        A: Actor<Msg = M>,
        F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
    {
        let Entity {
            key,
            factory,
            actor_options,
            idle_passivation_timeout,
            stop_message_factory,
            buffer_config,
            passivation_buffer_duration,
            allocation_strategy,
            remembered_entities,
        } = entity;
        let key_for_factory = key.clone();
        let local_node_id = runtime.local_node().id().clone();
        let system = self.system.clone();
        let mut local_route = LocalEntityRoute::new(local_node_id, system, move |local_context| {
            factory(EntityContext::new(key_for_factory.clone(), local_context))
        })
        .with_actor_options(actor_options);

        if let Some(timeout) = idle_passivation_timeout {
            local_route = local_route.with_idle_passivation(timeout);
        }
        local_route = apply_remembered_activation(local_route, remembered_entities.clone());

        let control = Arc::new(LocalRouteControl::new(
            local_route.clone(),
            stop_message_factory.clone(),
        ));
        let remote_route = runtime.remote_route(local_route.clone());
        let region = apply_remembered_entities(
            apply_buffering(
                apply_allocation_strategy(
                    ShardRegion::new(
                        key.entity_type().clone(),
                        key.config().clone(),
                        remote_route,
                    ),
                    allocation_strategy,
                ),
                buffer_config.clone(),
            ),
            remembered_entities.clone(),
        );
        runtime.register_entity_region(region.clone())?;
        self.store_typed_region(
            key.clone(),
            region.clone(),
            RegistrationMode::Local {
                control,
                idle_passivation_timeout,
                has_stop_message: stop_message_factory.is_some(),
                buffer_config,
                passivation_buffer_duration,
                remembered_entities,
            },
        );

        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Initializes a remote-aware sharded entity type through async coordinator storage.
    pub async fn init_remote_async<M, A, F>(
        &self,
        runtime: &mut ClusterNodeRuntime,
        entity: Entity<M, A, F>,
    ) -> ClusterNodeRuntimeResult<EntityTypeRegistration<M>>
    where
        M: Message + Sync,
        A: Actor<Msg = M>,
        F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
    {
        let Entity {
            key,
            factory,
            actor_options,
            idle_passivation_timeout,
            stop_message_factory,
            buffer_config,
            passivation_buffer_duration,
            allocation_strategy,
            remembered_entities,
        } = entity;
        let key_for_factory = key.clone();
        let local_node_id = runtime.local_node().id().clone();
        let system = self.system.clone();
        let mut local_route = LocalEntityRoute::new(local_node_id, system, move |local_context| {
            factory(EntityContext::new(key_for_factory.clone(), local_context))
        })
        .with_actor_options(actor_options);

        if let Some(timeout) = idle_passivation_timeout {
            local_route = local_route.with_idle_passivation(timeout);
        }
        local_route = apply_remembered_activation(local_route, remembered_entities.clone());

        let control = Arc::new(LocalRouteControl::new(
            local_route.clone(),
            stop_message_factory.clone(),
        ));
        let remote_route = runtime.remote_route(local_route.clone());
        let region = apply_remembered_entities(
            apply_buffering(
                apply_allocation_strategy(
                    ShardRegion::new(
                        key.entity_type().clone(),
                        key.config().clone(),
                        remote_route,
                    ),
                    allocation_strategy,
                ),
                buffer_config.clone(),
            ),
            remembered_entities.clone(),
        );
        runtime.register_entity_region_async(region.clone()).await?;
        self.store_typed_region(
            key.clone(),
            region.clone(),
            RegistrationMode::Local {
                control,
                idle_passivation_timeout,
                has_stop_message: stop_message_factory.is_some(),
                buffer_config,
                passivation_buffer_duration,
                remembered_entities,
            },
        );

        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Initializes a remote-aware entity type and installs remote ask handling.
    ///
    /// The endpoint handler accepts both tell envelopes and ask envelopes for
    /// the same entity type. Ask envelopes are identified by request metadata
    /// and converted into the entity command with `build`.
    pub fn init_remote_with_ask<Q, M, R, A, F, B>(
        &self,
        runtime: &mut ClusterNodeRuntime,
        entity: Entity<M, A, F>,
        build: B,
    ) -> ClusterNodeRuntimeResult<EntityTypeRegistration<M>>
    where
        Q: Message + Sync,
        M: Message + Sync,
        R: Send + Sync + 'static,
        A: Actor<Msg = M>,
        F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
        B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
    {
        let Entity {
            key,
            factory,
            actor_options,
            idle_passivation_timeout,
            stop_message_factory,
            buffer_config,
            passivation_buffer_duration,
            allocation_strategy,
            remembered_entities,
        } = entity;
        let key_for_factory = key.clone();
        let local_node_id = runtime.local_node().id().clone();
        let system = self.system.clone();
        let mut local_route = LocalEntityRoute::new(local_node_id, system, move |local_context| {
            factory(EntityContext::new(key_for_factory.clone(), local_context))
        })
        .with_actor_options(actor_options);

        if let Some(timeout) = idle_passivation_timeout {
            local_route = local_route.with_idle_passivation(timeout);
        }
        local_route = apply_remembered_activation(local_route, remembered_entities.clone());

        let control = Arc::new(LocalRouteControl::new(
            local_route.clone(),
            stop_message_factory.clone(),
        ));
        let remote_route = runtime.remote_route(local_route.clone());
        let region = apply_remembered_entities(
            apply_buffering(
                apply_allocation_strategy(
                    ShardRegion::new(
                        key.entity_type().clone(),
                        key.config().clone(),
                        remote_route,
                    ),
                    allocation_strategy,
                ),
                buffer_config.clone(),
            ),
            remembered_entities.clone(),
        );
        let tell_handler = RemoteEntityInbound::new(region.clone(), runtime.registry().clone());
        let ask_handler = RemoteEntityAskInbound::new(
            runtime.local_node().id().clone(),
            region.clone(),
            runtime.registry().clone(),
            runtime.transport().clone(),
            build,
        );
        runtime.register_entity_handler(region.clone(), move |envelope: RemoteEnvelope| {
            if envelope.request_id.is_some() {
                RemoteEnvelopeHandler::handle(&ask_handler, envelope)
            } else {
                RemoteEnvelopeHandler::handle(&tell_handler, envelope)
            }
        })?;
        self.store_typed_region(
            key.clone(),
            region.clone(),
            RegistrationMode::Local {
                control,
                idle_passivation_timeout,
                has_stop_message: stop_message_factory.is_some(),
                buffer_config,
                passivation_buffer_duration,
                remembered_entities,
            },
        );

        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Initializes a remote-aware entity type with ask handling through async storage.
    pub async fn init_remote_with_ask_async<Q, M, R, A, F, B>(
        &self,
        runtime: &mut ClusterNodeRuntime,
        entity: Entity<M, A, F>,
        build: B,
    ) -> ClusterNodeRuntimeResult<EntityTypeRegistration<M>>
    where
        Q: Message + Sync,
        M: Message + Sync,
        R: Send + Sync + 'static,
        A: Actor<Msg = M>,
        F: Fn(EntityContext<M>) -> A + Send + Sync + 'static,
        B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
    {
        let Entity {
            key,
            factory,
            actor_options,
            idle_passivation_timeout,
            stop_message_factory,
            buffer_config,
            passivation_buffer_duration,
            allocation_strategy,
            remembered_entities,
        } = entity;
        let key_for_factory = key.clone();
        let local_node_id = runtime.local_node().id().clone();
        let system = self.system.clone();
        let mut local_route = LocalEntityRoute::new(local_node_id, system, move |local_context| {
            factory(EntityContext::new(key_for_factory.clone(), local_context))
        })
        .with_actor_options(actor_options);

        if let Some(timeout) = idle_passivation_timeout {
            local_route = local_route.with_idle_passivation(timeout);
        }
        local_route = apply_remembered_activation(local_route, remembered_entities.clone());

        let control = Arc::new(LocalRouteControl::new(
            local_route.clone(),
            stop_message_factory.clone(),
        ));
        let remote_route = runtime.remote_route(local_route.clone());
        let region = apply_remembered_entities(
            apply_buffering(
                apply_allocation_strategy(
                    ShardRegion::new(
                        key.entity_type().clone(),
                        key.config().clone(),
                        remote_route,
                    ),
                    allocation_strategy,
                ),
                buffer_config.clone(),
            ),
            remembered_entities.clone(),
        );
        let tell_handler = RemoteEntityInbound::new(region.clone(), runtime.registry().clone());
        let ask_handler = RemoteEntityAskInbound::new(
            runtime.local_node().id().clone(),
            region.clone(),
            runtime.registry().clone(),
            runtime.transport().clone(),
            build,
        );
        runtime
            .register_entity_handler_async(region.clone(), move |envelope: RemoteEnvelope| {
                if envelope.request_id.is_some() {
                    RemoteEnvelopeHandler::handle(&ask_handler, envelope)
                } else {
                    RemoteEnvelopeHandler::handle(&tell_handler, envelope)
                }
            })
            .await?;
        self.store_typed_region(
            key.clone(),
            region.clone(),
            RegistrationMode::Local {
                control,
                idle_passivation_timeout,
                has_stop_message: stop_message_factory.is_some(),
                buffer_config,
                passivation_buffer_duration,
                remembered_entities,
            },
        );

        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Initializes a proxy-only entity type registration.
    pub fn init_proxy<M>(
        &self,
        key: EntityTypeKey<M>,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
    {
        self.init_proxy_with_allocation_strategy_ref(
            key,
            Arc::new(crate::DeterministicModuloShardAllocationStrategy::new()),
        )
    }

    /// Initializes a proxy-only entity type registration through async storage.
    pub async fn init_proxy_async<M>(
        &self,
        key: EntityTypeKey<M>,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
    {
        self.init_proxy_with_allocation_strategy_ref_async(
            key,
            Arc::new(crate::DeterministicModuloShardAllocationStrategy::new()),
        )
        .await
    }

    /// Initializes a proxy-only entity type registration with an allocation strategy.
    pub fn init_proxy_with_allocation_strategy<M>(
        &self,
        key: EntityTypeKey<M>,
        allocation_strategy: impl ShardAllocationStrategy,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
    {
        self.init_proxy_with_allocation_strategy_ref(key, Arc::new(allocation_strategy))
    }

    /// Initializes a proxy-only registration with an allocation strategy through async storage.
    pub async fn init_proxy_with_allocation_strategy_async<M>(
        &self,
        key: EntityTypeKey<M>,
        allocation_strategy: impl ShardAllocationStrategy,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
    {
        self.init_proxy_with_allocation_strategy_ref_async(key, Arc::new(allocation_strategy))
            .await
    }

    /// Initializes a proxy-only entity type registration with a shared allocation strategy.
    pub fn init_proxy_with_allocation_strategy_ref<M>(
        &self,
        key: EntityTypeKey<M>,
        allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
    {
        let entity_type = key.entity_type().clone();
        let region = apply_allocation_strategy(
            ShardRegion::new(
                entity_type.clone(),
                key.config().clone(),
                move |message: crate::RoutedEntityMessage<M>| {
                    let owner = message.owner().clone();
                    Err(EntityTellError::Delivery {
                        message: message.into_message(),
                        failure: crate::EntityDeliveryFailure::NotLocal { owner },
                    })
                },
            ),
            allocation_strategy,
        );
        self.register_typed_region(key.clone(), region.clone(), RegistrationMode::ProxyOnly)?;
        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Initializes a proxy-only registration with a shared allocation strategy through async storage.
    pub async fn init_proxy_with_allocation_strategy_ref_async<M>(
        &self,
        key: EntityTypeKey<M>,
        allocation_strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> ClusterShardingResult<EntityTypeRegistration<M>>
    where
        M: Message,
    {
        let entity_type = key.entity_type().clone();
        let region = apply_allocation_strategy(
            ShardRegion::new(
                entity_type.clone(),
                key.config().clone(),
                move |message: crate::RoutedEntityMessage<M>| {
                    let owner = message.owner().clone();
                    Err(EntityTellError::Delivery {
                        message: message.into_message(),
                        failure: crate::EntityDeliveryFailure::NotLocal { owner },
                    })
                },
            ),
            allocation_strategy,
        );
        self.register_typed_region_async(key.clone(), region.clone(), RegistrationMode::ProxyOnly)
            .await?;
        Ok(EntityTypeRegistration::new(key, region))
    }

    /// Returns a high-level entity reference carrying its shard region.
    pub fn entity_ref_for<M>(
        &self,
        key: &EntityTypeKey<M>,
        entity_id: impl Into<String>,
    ) -> ClusterShardingResult<ShardedEntityRef<M>>
    where
        M: Message,
    {
        let region = self.region_for(key)?;
        Ok(ShardedEntityRef::new(region.entity_ref(entity_id), region))
    }

    /// Returns the typed region for an initialized entity type.
    pub fn region_for<M>(&self, key: &EntityTypeKey<M>) -> ClusterShardingResult<ShardRegion<M>>
    where
        M: Message,
    {
        let regions = self
            .regions
            .lock()
            .expect("cluster sharding region registry mutex poisoned");
        let boxed = regions.get(key.entity_type()).ok_or_else(|| {
            ClusterShardingError::EntityTypeNotRegistered {
                entity_type: key.entity_type().clone(),
            }
        })?;
        boxed
            .downcast_ref::<ShardRegion<M>>()
            .cloned()
            .ok_or_else(|| ClusterShardingError::EntityTypeMessageMismatch {
                entity_type: key.entity_type().clone(),
            })
    }

    /// Passivates one local entity using the configured stop-message policy.
    pub fn passivate_entity<M>(
        &self,
        key: &EntityTypeKey<M>,
        entity_id: impl Into<String>,
    ) -> ClusterShardingResult<bool>
    where
        M: Message,
    {
        self.passivate_entity_id(key, &EntityId::new(entity_id))
    }

    /// Passivates one local entity using the configured stop-message policy.
    pub fn passivate_entity_id<M>(
        &self,
        key: &EntityTypeKey<M>,
        entity_id: &EntityId,
    ) -> ClusterShardingResult<bool>
    where
        M: Message,
    {
        let region = self.region_for(key)?;
        let passivation_buffer_duration = self
            .states
            .lock()
            .expect("cluster sharding state registry mutex poisoned")
            .get(key.entity_type())
            .map_or(Duration::ZERO, |state| state.passivation_buffer_duration);
        let should_buffer =
            region.buffer_config().is_some() && passivation_buffer_duration > Duration::ZERO;
        if should_buffer {
            region.begin_entity_passivation(entity_id.clone(), passivation_buffer_duration);
        }

        let control = self
            .controls
            .lock()
            .expect("cluster sharding control registry mutex poisoned")
            .get(key.entity_type())
            .cloned()
            .ok_or_else(|| ClusterShardingError::EntityTypeNotRegistered {
                entity_type: key.entity_type().clone(),
            })?;
        let passivated = control.passivate_entity(entity_id);

        if should_buffer {
            let region = region.clone();
            let entity_id = entity_id.clone();
            if passivated {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        tokio::time::sleep(passivation_buffer_duration).await;
                        region.end_entity_passivation(&entity_id);
                    });
                } else {
                    region.end_entity_passivation(&entity_id);
                }
            } else {
                region.end_entity_passivation(&entity_id);
            }
        }

        Ok(passivated)
    }

    /// Forgets one remembered entity id and passivates the local entity when active.
    pub async fn forget_entity<M>(
        &self,
        key: &EntityTypeKey<M>,
        entity_id: impl Into<String>,
    ) -> ClusterShardingResult<bool>
    where
        M: Message,
    {
        self.forget_entity_id(key, &EntityId::new(entity_id)).await
    }

    /// Forgets one remembered entity id and passivates the local entity when active.
    pub async fn forget_entity_id<M>(
        &self,
        key: &EntityTypeKey<M>,
        entity_id: &EntityId,
    ) -> ClusterShardingResult<bool>
    where
        M: Message,
    {
        let remembered_entities = self
            .remembered
            .lock()
            .expect("cluster sharding remembered registry mutex poisoned")
            .get(key.entity_type())
            .cloned();
        let Some(remembered_entities) = remembered_entities else {
            return Ok(false);
        };
        let shard = ShardKey::new(
            key.entity_type().clone(),
            ShardId::for_entity(key.entity_type(), entity_id, key.config()),
        );
        let removed = remembered_entities
            .store()
            .forget(&shard, entity_id)
            .await
            .map_err(ClusterShardingError::from)?;
        let passivated = self.passivate_entity_id(key, entity_id)?;
        Ok(removed || passivated)
    }

    /// Returns the facade state for all registered entity types.
    #[must_use]
    pub fn state(&self) -> ClusterShardingState {
        let states = self
            .states
            .lock()
            .expect("cluster sharding state registry mutex poisoned");
        let controls = self
            .controls
            .lock()
            .expect("cluster sharding control registry mutex poisoned");
        let entity_types = states
            .values()
            .cloned()
            .map(|state| {
                let entity_type = state.entity_type().clone();
                state.with_control(controls.get(&entity_type))
            })
            .collect();
        ClusterShardingState { entity_types }
    }

    /// Returns registration state for an entity type.
    #[must_use]
    pub fn registration_state<M>(
        &self,
        key: &EntityTypeKey<M>,
    ) -> Option<EntityTypeRegistrationState> {
        let states = self
            .states
            .lock()
            .expect("cluster sharding state registry mutex poisoned");
        let controls = self
            .controls
            .lock()
            .expect("cluster sharding control registry mutex poisoned");
        states
            .get(key.entity_type())
            .cloned()
            .map(|state| state.with_control(controls.get(key.entity_type())))
    }

    fn register_typed_region<M>(
        &self,
        key: EntityTypeKey<M>,
        region: ShardRegion<M>,
        mode: RegistrationMode,
    ) -> ClusterShardingResult<()>
    where
        M: Message,
    {
        self.runtime
            .try_lock()
            .map_err(|_error| ClusterShardingError::RuntimeBusy)?
            .register_region(region.clone())?;
        self.store_typed_region(key, region, mode);
        Ok(())
    }

    async fn register_typed_region_async<M>(
        &self,
        key: EntityTypeKey<M>,
        region: ShardRegion<M>,
        mode: RegistrationMode,
    ) -> ClusterShardingResult<()>
    where
        M: Message,
    {
        self.runtime
            .lock()
            .await
            .register_region_async(region.clone())
            .await?;
        self.store_typed_region(key, region, mode);
        Ok(())
    }

    fn store_typed_region<M>(
        &self,
        key: EntityTypeKey<M>,
        region: ShardRegion<M>,
        mode: RegistrationMode,
    ) where
        M: Message,
    {
        self.regions
            .lock()
            .expect("cluster sharding region registry mutex poisoned")
            .insert(key.entity_type().clone(), Box::new(region.clone()));

        let (
            proxy_only,
            idle_passivation_timeout,
            has_stop_message,
            buffer_config,
            passivation_buffer_duration,
            remembered_entities,
        ) = match mode {
            RegistrationMode::Local {
                control,
                idle_passivation_timeout,
                has_stop_message,
                buffer_config,
                passivation_buffer_duration,
                remembered_entities,
            } => {
                self.controls
                    .lock()
                    .expect("cluster sharding control registry mutex poisoned")
                    .insert(key.entity_type().clone(), control);
                if let Some(remembered_entities) = remembered_entities.clone() {
                    self.remembered
                        .lock()
                        .expect("cluster sharding remembered registry mutex poisoned")
                        .insert(key.entity_type().clone(), remembered_entities);
                } else {
                    self.remembered
                        .lock()
                        .expect("cluster sharding remembered registry mutex poisoned")
                        .remove(key.entity_type());
                }
                (
                    false,
                    idle_passivation_timeout,
                    has_stop_message,
                    buffer_config,
                    passivation_buffer_duration,
                    remembered_entities,
                )
            }
            RegistrationMode::ProxyOnly => {
                self.remembered
                    .lock()
                    .expect("cluster sharding remembered registry mutex poisoned")
                    .remove(key.entity_type());
                (true, None, false, None, Duration::ZERO, None)
            }
        };

        self.states
            .lock()
            .expect("cluster sharding state registry mutex poisoned")
            .insert(
                key.entity_type().clone(),
                EntityTypeRegistrationState {
                    entity_type: key.entity_type().clone(),
                    number_of_shards: key.config().number_of_shards(),
                    owner_revision: region.owner_revision(),
                    proxy_only,
                    local_entity_count: 0,
                    idle_passivation_timeout,
                    has_stop_message,
                    buffer_config,
                    passivation_buffer_duration,
                    buffered_message_count: region.buffered_message_count(),
                    allocation_strategy: region.allocation_strategy_name().to_string(),
                    remembered_entities_enabled: remembered_entities.is_some(),
                    remembered_start_batch_size: remembered_entities
                        .as_ref()
                        .map_or(0, RememberedEntities::start_batch_size),
                    remembered_start_batch_delay: remembered_entities
                        .as_ref()
                        .map_or(Duration::ZERO, RememberedEntities::start_batch_delay),
                    remembered_store_backend: remembered_entities
                        .as_ref()
                        .map(|remembered_entities| remembered_entities.store_backend().to_string()),
                },
            );
    }
}

impl Debug for ClusterSharding {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterSharding")
            .field("system", &self.system.name())
            .field("local_node", &self.local_node)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

/// Registration returned after initializing an entity type.
pub struct EntityTypeRegistration<M>
where
    M: Message,
{
    key: EntityTypeKey<M>,
    region: ShardRegion<M>,
}

impl<M> EntityTypeRegistration<M>
where
    M: Message,
{
    /// Creates a typed entity-type registration.
    #[must_use]
    pub fn new(key: EntityTypeKey<M>, region: ShardRegion<M>) -> Self {
        Self { key, region }
    }

    /// Entity type key.
    #[must_use]
    pub const fn key(&self) -> &EntityTypeKey<M> {
        &self.key
    }

    /// Underlying shard region.
    #[must_use]
    pub const fn region(&self) -> &ShardRegion<M> {
        &self.region
    }

    /// Returns a high-level entity reference for this registration.
    #[must_use]
    pub fn entity_ref_for(&self, entity_id: impl Into<String>) -> ShardedEntityRef<M> {
        ShardedEntityRef::new(self.region.entity_ref(entity_id), self.region.clone())
    }
}

impl<M> Clone for EntityTypeRegistration<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            region: self.region.clone(),
        }
    }
}

impl<M> Debug for EntityTypeRegistration<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityTypeRegistration")
            .field("entity_type", self.key.entity_type())
            .field("number_of_shards", &self.key.config().number_of_shards())
            .field("owner_revision", &self.region.owner_revision())
            .finish_non_exhaustive()
    }
}

/// High-level entity ref that carries the initialized shard region.
pub struct ShardedEntityRef<M>
where
    M: Message,
{
    entity_ref: EntityRef<M>,
    region: ShardRegion<M>,
}

impl<M> ShardedEntityRef<M>
where
    M: Message,
{
    /// Creates a high-level entity ref from a logical ref and shard region.
    #[must_use]
    pub fn new(entity_ref: EntityRef<M>, region: ShardRegion<M>) -> Self {
        Self { entity_ref, region }
    }

    /// Logical entity ref.
    #[must_use]
    pub const fn entity_ref(&self) -> &EntityRef<M> {
        &self.entity_ref
    }

    /// Region used to route this ref.
    #[must_use]
    pub const fn region(&self) -> &ShardRegion<M> {
        &self.region
    }

    /// Entity id.
    #[must_use]
    pub fn entity_id(&self) -> &EntityId {
        self.entity_ref.entity_id()
    }

    /// Entity type.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        self.entity_ref.entity_type()
    }

    /// Sends a message without waiting for a reply.
    pub fn tell(&self, message: M) -> Result<(), EntityTellError<M>> {
        self.entity_ref.tell(&self.region, message)
    }

    /// Sends a request message and waits for its reply.
    pub async fn ask<R>(
        &self,
        build: impl FnOnce(ReplyTo<R>) -> M,
        timeout: Duration,
    ) -> Result<R, EntityAskError>
    where
        R: Send + 'static,
    {
        self.entity_ref.ask(&self.region, build, timeout).await
    }

    /// Sends a request envelope through a remote ask client and waits for a reply.
    pub async fn remote_ask<Q, R, T>(
        &self,
        client: &RemoteEntityAskClient<T>,
        request: Q,
        timeout: Duration,
    ) -> Result<R, RemoteEntityAskError>
    where
        Q: Message + Sync,
        R: Send + Sync + 'static,
        T: RemoteTransport,
    {
        client
            .ask(&self.region, &self.entity_ref, request, timeout)
            .await
    }
}

impl<M> Clone for ShardedEntityRef<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            entity_ref: self.entity_ref.clone(),
            region: self.region.clone(),
        }
    }
}

impl<M> Debug for ShardedEntityRef<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedEntityRef")
            .field("entity_type", self.entity_type())
            .field("entity_id", self.entity_id())
            .finish_non_exhaustive()
    }
}

/// Snapshot of high-level sharding facade state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterShardingState {
    entity_types: Vec<EntityTypeRegistrationState>,
}

impl ClusterShardingState {
    /// Registered entity types.
    #[must_use]
    pub fn entity_types(&self) -> &[EntityTypeRegistrationState] {
        &self.entity_types
    }
}

/// Snapshot of one registered entity type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityTypeRegistrationState {
    entity_type: EntityType,
    number_of_shards: u32,
    owner_revision: u64,
    proxy_only: bool,
    local_entity_count: usize,
    idle_passivation_timeout: Option<Duration>,
    has_stop_message: bool,
    buffer_config: Option<ShardBufferConfig>,
    passivation_buffer_duration: Duration,
    buffered_message_count: usize,
    allocation_strategy: String,
    remembered_entities_enabled: bool,
    remembered_start_batch_size: usize,
    remembered_start_batch_delay: Duration,
    remembered_store_backend: Option<String>,
}

impl EntityTypeRegistrationState {
    /// Entity type.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Number of shards.
    #[must_use]
    pub const fn number_of_shards(&self) -> u32 {
        self.number_of_shards
    }

    /// Owner-cache revision observed at registration.
    #[must_use]
    pub const fn owner_revision(&self) -> u64 {
        self.owner_revision
    }

    /// Returns true for proxy-only registrations.
    #[must_use]
    pub const fn proxy_only(&self) -> bool {
        self.proxy_only
    }

    /// Number of currently cached local entity actors.
    #[must_use]
    pub const fn local_entity_count(&self) -> usize {
        self.local_entity_count
    }

    /// Configured idle passivation timeout.
    #[must_use]
    pub const fn idle_passivation_timeout(&self) -> Option<Duration> {
        self.idle_passivation_timeout
    }

    /// Returns true when a stop message was configured.
    #[must_use]
    pub const fn has_stop_message(&self) -> bool {
        self.has_stop_message
    }

    /// Configured shard buffering policy, when enabled.
    #[must_use]
    pub const fn buffer_config(&self) -> Option<&ShardBufferConfig> {
        self.buffer_config.as_ref()
    }

    /// Explicit passivation buffering window.
    #[must_use]
    pub const fn passivation_buffer_duration(&self) -> Duration {
        self.passivation_buffer_duration
    }

    /// Number of currently buffered messages observed by the facade.
    #[must_use]
    pub const fn buffered_message_count(&self) -> usize {
        self.buffered_message_count
    }

    /// Stable shard allocation strategy name used for this registration.
    #[must_use]
    pub fn allocation_strategy(&self) -> &str {
        &self.allocation_strategy
    }

    /// Returns true when remembered entities are enabled for this registration.
    #[must_use]
    pub const fn remembered_entities_enabled(&self) -> bool {
        self.remembered_entities_enabled
    }

    /// Remembered replay batch size.
    #[must_use]
    pub const fn remembered_start_batch_size(&self) -> usize {
        self.remembered_start_batch_size
    }

    /// Delay inserted between remembered replay batches.
    #[must_use]
    pub const fn remembered_start_batch_delay(&self) -> Duration {
        self.remembered_start_batch_delay
    }

    /// Remembered entity store backend name.
    #[must_use]
    pub fn remembered_store_backend(&self) -> Option<&str> {
        self.remembered_store_backend.as_deref()
    }

    fn with_control(mut self, control: Option<&Arc<dyn LocalEntityControl>>) -> Self {
        if let Some(control) = control {
            self.local_entity_count = control.entity_count();
        }
        self
    }
}

enum RegistrationMode {
    Local {
        control: Arc<dyn LocalEntityControl>,
        idle_passivation_timeout: Option<Duration>,
        has_stop_message: bool,
        buffer_config: Option<ShardBufferConfig>,
        passivation_buffer_duration: Duration,
        remembered_entities: Option<RememberedEntities>,
    },
    ProxyOnly,
}

trait LocalEntityControl: Send + Sync {
    fn entity_count(&self) -> usize;

    fn passivate_entity(&self, entity_id: &EntityId) -> bool;
}

struct LocalRouteControl<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    route: LocalEntityRoute<M, A, F>,
    stop_message_factory: Option<StopMessageFactory<M>>,
}

impl<M, A, F> LocalRouteControl<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    fn new(
        route: LocalEntityRoute<M, A, F>,
        stop_message_factory: Option<StopMessageFactory<M>>,
    ) -> Self {
        Self {
            route,
            stop_message_factory,
        }
    }
}

impl<M, A, F> LocalEntityControl for LocalRouteControl<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    fn entity_count(&self) -> usize {
        self.route.entity_count()
    }

    fn passivate_entity(&self, entity_id: &EntityId) -> bool {
        if let Some(actor_ref) = self.route.entity_actor(entity_id) {
            if let Some(stop_message_factory) = &self.stop_message_factory {
                let _ = actor_ref.tell(stop_message_factory());
            }
        }
        self.route.passivate_entity(entity_id)
    }
}

fn apply_buffering<M>(region: ShardRegion<M>, config: Option<ShardBufferConfig>) -> ShardRegion<M>
where
    M: Message,
{
    if let Some(config) = config {
        region.with_buffering(config)
    } else {
        region
    }
}

fn apply_remembered_entities<M>(
    region: ShardRegion<M>,
    remembered_entities: Option<RememberedEntities>,
) -> ShardRegion<M>
where
    M: Message,
{
    if let Some(remembered_entities) = remembered_entities {
        region.with_remembered_entities(remembered_entities)
    } else {
        region
    }
}

fn apply_allocation_strategy<M>(
    region: ShardRegion<M>,
    allocation_strategy: Arc<dyn ShardAllocationStrategy>,
) -> ShardRegion<M>
where
    M: Message,
{
    region.with_allocation_strategy_ref(allocation_strategy)
}

fn apply_remembered_activation<M, A, F>(
    route: LocalEntityRoute<M, A, F>,
    remembered_entities: Option<RememberedEntities>,
) -> LocalEntityRoute<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    let Some(remembered_entities) = remembered_entities else {
        return route;
    };
    let store = remembered_entities.store();
    route.with_activation_observer(move |context| {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let store = store.clone();
        let shard = ShardKey::new(context.entity_type().clone(), context.shard_id());
        let entity_id = context.entity_id().clone();
        handle.spawn(async move {
            let _remembered = store.remember(&shard, &entity_id).await;
        });
    })
}

fn local_node_for_system(system: &ActorSystem) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(system.name(), "local"),
        NodeAddress::new("127.0.0.1", 0),
    )
}
