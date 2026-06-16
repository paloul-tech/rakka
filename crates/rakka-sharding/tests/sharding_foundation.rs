//! Integration tests for sharding identity and coordinator foundations.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::{
    ClusterError, ClusterMembership, ClusterNode, ClusterProtocol, CompatibilityRange,
    DiscoverySnapshot, MembershipConfig, MembershipEvent, NodeAddress, NodeId, ProtocolVersion,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, CoordinatedShutdown,
    CoordinatedShutdownReason, InMemoryMetricsRecorder, ReplyTo, ShutdownOutcome, ShutdownPhase,
    ShutdownTaskStatus, METRIC_SHARD_OWNERSHIP_COUNT,
};
use rakka_remote::{
    InMemoryRemoteTransport, RemoteDestination, RemoteEndpoint, RemoteEnvelope,
    RemoteRequestRegistry, SerializationRegistry,
};
use rakka_sharding::{
    register_cluster_sharding_leave_task, AsyncShardCoordinatorStore, ClusterSharding,
    ClusterShardingError, ClusterShardingRuntime, ClusterShardingShutdownHandle,
    CoordinatorStoreFuture, Entity, EntityAskError, EntityContext, EntityDeliveryFailure, EntityId,
    EntityRef, EntityTellError, EntityType, EntityTypeKey, InMemoryRememberedEntityStore,
    InMemoryShardCoordinatorLease, InMemoryShardCoordinatorStore, LeastShardAllocationStrategy,
    LocalEntityContext, LocalEntityRoute, PersistedShardCoordinatorState, RememberedEntities,
    RememberedEntityStore, RemoteEntityAskClient, RemoteEntityAskError, RemoteEntityAskInbound,
    RemoteEntityInbound, RemoteEntityInboundError, RemoteEntityOutbound, RemoteEntityRoute,
    RemoteEntitySendFailure, RemoteTransportEntityOutbound, RoutedEntityMessage,
    ShardAllocationContext, ShardAllocationStrategy, ShardBufferConfig, ShardCoordinator,
    ShardCoordinatorLease, ShardCoordinatorStore, ShardDecision, ShardHandoffState, ShardId,
    ShardKey, ShardMoveReason, ShardOwnerCache, ShardRegion, ShardingConfig, ShardingError,
};

#[derive(Debug)]
enum CartCommand {
    Add(String),
    Get(ReplyTo<String>),
    Passivate,
}

struct CartEntity {
    context: LocalEntityContext,
    items: Vec<String>,
}

struct FacadeCartEntity {
    context: EntityContext<CartCommand>,
    items: Vec<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RemoteCartCommand {
    #[prost(string, tag = "1")]
    action: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RemoteCartGet {
    #[prost(string, tag = "1")]
    prefix: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RemoteCartReply {
    #[prost(string, tag = "1")]
    summary: String,
}

struct RemoteCartEntity;

struct RemoteAskCartEntity {
    context: LocalEntityContext,
}

struct SilentRemoteAskCartEntity;

struct NotifyingRemoteCartEntity {
    context: LocalEntityContext,
    delivered: tokio::sync::mpsc::UnboundedSender<(String, String)>,
}

enum RemoteAskCartCommand {
    Get {
        prefix: String,
        reply_to: ReplyTo<RemoteCartReply>,
    },
}

impl Actor for CartEntity {
    type Msg = CartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                CartCommand::Add(value) => self.items.push(value),
                CartCommand::Get(reply_to) => {
                    let value = format!("{}:{}", self.context.entity_id(), self.items.join(","));
                    let _ = reply_to.reply(value);
                }
                CartCommand::Passivate => return Ok(ActorAction::Stop),
            }
            Ok(ActorAction::Continue)
        })
    }
}

impl Actor for FacadeCartEntity {
    type Msg = CartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                CartCommand::Add(value) => self.items.push(value),
                CartCommand::Get(reply_to) => {
                    let value =
                        format!("{}:{}", self.context.persistence_id(), self.items.join(","));
                    let _ = reply_to.reply(value);
                }
                CartCommand::Passivate => return Ok(ActorAction::Stop),
            }
            Ok(ActorAction::Continue)
        })
    }
}

impl Actor for RemoteCartEntity {
    type Msg = RemoteCartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Continue) })
    }
}

impl Actor for RemoteAskCartEntity {
    type Msg = RemoteAskCartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entity_id = self.context.entity_id().as_str().to_string();
        actor_future(async move {
            match msg {
                RemoteAskCartCommand::Get { prefix, reply_to } => {
                    let _ = reply_to.reply(RemoteCartReply {
                        summary: format!("{entity_id}:{prefix}"),
                    });
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

impl Actor for SilentRemoteAskCartEntity {
    type Msg = RemoteAskCartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async { Ok(ActorAction::Continue) })
    }
}

impl Actor for NotifyingRemoteCartEntity {
    type Msg = RemoteCartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entity_id = self.context.entity_id().as_str().to_string();
        let action = msg.action;
        let delivered = self.delivered.clone();
        actor_future(async move {
            let _ = delivered.send((entity_id, action));
            Ok(ActorAction::Continue)
        })
    }
}

#[derive(Debug, Clone, Default)]
struct RecordingRemoteOutbound {
    sent: Arc<Mutex<Vec<(NodeId, RemoteEnvelope)>>>,
}

impl RecordingRemoteOutbound {
    fn sent(&self) -> Vec<(NodeId, RemoteEnvelope)> {
        self.sent.lock().expect("sent mutex poisoned").clone()
    }
}

impl RemoteEntityOutbound for RecordingRemoteOutbound {
    fn send(
        &self,
        owner: &NodeId,
        envelope: RemoteEnvelope,
    ) -> Result<(), RemoteEntitySendFailure> {
        self.sent
            .lock()
            .expect("sent mutex poisoned")
            .push((owner.clone(), envelope));
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct FailingRemoteOutbound;

impl RemoteEntityOutbound for FailingRemoteOutbound {
    fn send(
        &self,
        _owner: &NodeId,
        _envelope: RemoteEnvelope,
    ) -> Result<(), RemoteEntitySendFailure> {
        Err(RemoteEntitySendFailure::Rejected(
            "transport unavailable".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
struct LastRoutableAllocationStrategy;

impl ShardAllocationStrategy for LastRoutableAllocationStrategy {
    fn allocate_shard(
        &self,
        context: &ShardAllocationContext<'_>,
        _shard_id: ShardId,
    ) -> Option<NodeId> {
        context.routable_nodes().last().cloned()
    }
}

#[derive(Debug, Clone, Default)]
struct AsyncOnlyCoordinatorStore {
    inner: InMemoryShardCoordinatorStore,
}

impl AsyncOnlyCoordinatorStore {
    fn load_sync(&self, entity_type: &EntityType) -> Option<PersistedShardCoordinatorState> {
        ShardCoordinatorStore::load(&self.inner, entity_type).unwrap()
    }
}

impl AsyncShardCoordinatorStore for AsyncOnlyCoordinatorStore {
    fn backend_name(&self) -> &'static str {
        "async-only-test"
    }

    fn load<'a>(
        &'a self,
        entity_type: &'a EntityType,
    ) -> CoordinatorStoreFuture<'a, Option<PersistedShardCoordinatorState>> {
        Box::pin(async move { ShardCoordinatorStore::load(&self.inner, entity_type) })
    }

    fn compare_and_set<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState> {
        Box::pin(async move {
            ShardCoordinatorStore::compare_and_set(
                &self.inner,
                entity_type,
                expected_revision,
                state,
            )
        })
    }

    fn delete<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
    ) -> CoordinatorStoreFuture<'a, ()> {
        Box::pin(async move {
            ShardCoordinatorStore::delete(&self.inner, entity_type, expected_revision)
        })
    }
}

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
    )
}

fn membership_config() -> MembershipConfig {
    MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100))
}

fn membership_with_up_nodes(nodes: Vec<ClusterNode>) -> ClusterMembership {
    let local = nodes[0].clone();
    let mut membership = ClusterMembership::new(local, membership_config());

    membership
        .record_discovery(DiscoverySnapshot::new("test", 1, nodes))
        .unwrap();

    for member in membership
        .snapshot()
        .members()
        .iter()
        .map(|member| member.node().id().clone())
        .collect::<Vec<_>>()
    {
        membership.mark_up(&member, 2).unwrap();
    }

    membership
}

fn entity_owned_by(coordinator: &ShardCoordinator, logical_id: &str) -> EntityId {
    (0..1024)
        .map(|index| EntityId::new(format!("cart-{index}")))
        .find(|entity_id| {
            coordinator
                .owner_for_entity(entity_id)
                .is_ok_and(|owner| owner.logical_id() == logical_id)
        })
        .expect("expected at least one entity to map to requested owner")
}

fn remote_registry() -> SerializationRegistry {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<RemoteCartCommand>("rakka.test.RemoteCartCommand", 1)
        .unwrap();
    registry
        .register_protobuf::<RemoteCartGet>("rakka.test.RemoteCartGet", 1)
        .unwrap();
    registry
        .register_protobuf::<RemoteCartReply>("rakka.test.RemoteCartReply", 1)
        .unwrap();
    registry
}

fn remote_cart_envelope(
    registry: &SerializationRegistry,
    entity_type: &str,
    entity_id: &str,
    action: &str,
) -> RemoteEnvelope {
    let encoded = registry
        .encode(&RemoteCartCommand {
            action: action.to_string(),
        })
        .unwrap();
    RemoteEnvelope::new(
        RemoteDestination::Entity {
            entity_type: entity_type.to_string(),
            entity_id: entity_id.to_string(),
        },
        encoded,
    )
}

fn runtime_with_local(local: ClusterNode) -> ClusterShardingRuntime {
    ClusterShardingRuntime::new(ClusterMembership::new(local, membership_config()))
}

fn runtime_region(entity_type: EntityType, config: ShardingConfig) -> ShardRegion<CartCommand> {
    ShardRegion::new(
        entity_type,
        config,
        |_message: RoutedEntityMessage<CartCommand>| Ok(()),
    )
}

fn coordinator_owners_exclude(coordinator: &ShardCoordinator, node_id: &NodeId) -> bool {
    coordinator
        .snapshot()
        .assignments()
        .iter()
        .all(|assignment| assignment.owner() != node_id)
}

fn entity_ref_in_different_shard(
    entity_type: &EntityType,
    config: &ShardingConfig,
    avoided_shard: ShardId,
) -> EntityRef<CartCommand> {
    (0..1024)
        .map(|index| EntityRef::new(entity_type.clone(), EntityId::new(format!("cart-{index}"))))
        .find(|entity| entity.shard_id(config) != avoided_shard)
        .expect("expected at least one entity to map to another shard")
}

async fn wait_for_entity_count<A, F>(
    route: &LocalEntityRoute<CartCommand, A, F>,
    expected_count: usize,
) where
    A: Actor<Msg = CartCommand>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    for _attempt in 0..20 {
        if route.entity_count() == expected_count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(route.entity_count(), expected_count);
}

async fn wait_for_remembered_count(
    store: &InMemoryRememberedEntityStore,
    shard: &ShardKey,
    expected_count: usize,
) {
    for _attempt in 0..20 {
        if store.len_for_shard(shard) == expected_count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(store.len_for_shard(shard), expected_count);
}

async fn wait_for_entity_count_from_facade<M>(
    sharding: &ClusterSharding,
    key: &EntityTypeKey<M>,
    expected_count: usize,
) where
    M: rakka_core::Message,
{
    for _attempt in 0..20 {
        if sharding
            .registration_state(key)
            .is_some_and(|state| state.local_entity_count() == expected_count)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(
        sharding
            .registration_state(key)
            .map(|state| state.local_entity_count())
            .unwrap_or_default(),
        expected_count
    );
}

#[tokio::test]
async fn cluster_sharding_facade_initializes_and_routes_local_entity() {
    let system = ActorSystem::new("facade-local-test");
    let sharding = ClusterSharding::get(&system);
    let key = EntityTypeKey::<CartCommand>::new("FacadeCart")
        .with_number_of_shards(4)
        .unwrap();
    let registration = sharding
        .init(
            Entity::of(key.clone(), |context| FacadeCartEntity {
                context,
                items: Vec::new(),
            })
            .with_stop_message_factory(|| CartCommand::Passivate),
        )
        .unwrap();
    let cart = registration.entity_ref_for("cart-1");

    cart.tell(CartCommand::Add("apple".to_string())).unwrap();
    cart.tell(CartCommand::Add("banana".to_string())).unwrap();
    let value = cart
        .ask(CartCommand::Get, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(value, "FacadeCart|cart-1:apple,banana");
    let state = sharding.registration_state(&key).unwrap();
    assert_eq!(state.entity_type(), key.entity_type());
    assert_eq!(state.number_of_shards(), 4);
    assert_eq!(state.local_entity_count(), 1);
    assert!(state.has_stop_message());
    assert!(!state.proxy_only());
    assert_eq!(sharding.state().entity_types().len(), 1);

    assert!(sharding.passivate_entity(&key, "cart-1").unwrap());
    assert_eq!(
        sharding
            .registration_state(&key)
            .unwrap()
            .local_entity_count(),
        0
    );

    tokio::time::sleep(Duration::from_millis(20)).await;
    let cart = sharding.entity_ref_for(&key, "cart-1").unwrap();
    let value = cart
        .ask(CartCommand::Get, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(value, "FacadeCart|cart-1:");
    system.shutdown();
}

#[tokio::test]
async fn cluster_sharding_facade_buffers_messages_during_explicit_passivation() {
    let system = ActorSystem::new("facade-passivation-buffer-test");
    let sharding = ClusterSharding::get(&system);
    let key = EntityTypeKey::<CartCommand>::new("FacadeBufferedCart")
        .with_number_of_shards(4)
        .unwrap();
    let registration = sharding
        .init(
            Entity::of(key.clone(), |context| FacadeCartEntity {
                context,
                items: Vec::new(),
            })
            .with_stop_message_factory(|| CartCommand::Passivate)
            .with_passivation_buffer_duration(Duration::from_millis(50)),
        )
        .unwrap();
    let cart = registration.entity_ref_for("cart-1");

    cart.tell(CartCommand::Add("apple".to_string())).unwrap();
    let value = cart
        .ask(CartCommand::Get, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(value, "FacadeBufferedCart|cart-1:apple");

    assert!(sharding.passivate_entity(&key, "cart-1").unwrap());
    cart.tell(CartCommand::Add("after-passivate".to_string()))
        .unwrap();
    assert_eq!(registration.region().buffered_message_count(), 1);

    tokio::time::sleep(Duration::from_millis(80)).await;
    let value = cart
        .ask(CartCommand::Get, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(value, "FacadeBufferedCart|cart-1:after-passivate");
    assert_eq!(registration.region().buffered_message_count(), 0);
    system.shutdown();
}

#[tokio::test]
async fn in_memory_remembered_entity_store_remembers_forgets_and_lists_by_shard() {
    let store = InMemoryRememberedEntityStore::new();
    let cart_shard = ShardKey::new(EntityType::new("RememberedCart"), ShardId::new(1));
    let other_shard = ShardKey::new(EntityType::new("RememberedCart"), ShardId::new(2));
    let cart_a = EntityId::new("cart-a");
    let cart_b = EntityId::new("cart-b");

    store.remember(&cart_shard, &cart_b).await.unwrap();
    store.remember(&cart_shard, &cart_a).await.unwrap();
    store.remember(&cart_shard, &cart_a).await.unwrap();
    store.remember(&other_shard, &cart_a).await.unwrap();

    assert_eq!(store.len(), 3);
    assert_eq!(
        store.remembered_for_shard(&cart_shard).await.unwrap(),
        vec![cart_a.clone(), cart_b]
    );
    assert!(store.forget(&cart_shard, &cart_a).await.unwrap());
    assert!(!store.forget(&cart_shard, &cart_a).await.unwrap());
    assert_eq!(store.len_for_shard(&cart_shard), 1);
    assert_eq!(store.len_for_shard(&other_shard), 1);
}

#[tokio::test]
async fn remembered_entity_settings_propagate_to_facade_state() {
    let system = ActorSystem::new("remembered-settings-test");
    let sharding = ClusterSharding::get(&system);
    let store = InMemoryRememberedEntityStore::new();
    let key = EntityTypeKey::<CartCommand>::new("RememberedSettingsCart")
        .with_number_of_shards(4)
        .unwrap();
    let registration = sharding
        .init(
            Entity::of(key.clone(), |context| FacadeCartEntity {
                context,
                items: Vec::new(),
            })
            .with_remembered_entities(
                RememberedEntities::enabled()
                    .with_start_batch_size(3)
                    .with_start_batch_delay(Duration::from_millis(5))
                    .with_store(store),
            ),
        )
        .unwrap();

    let state = sharding.registration_state(&key).unwrap();
    assert!(state.remembered_entities_enabled());
    assert_eq!(state.remembered_start_batch_size(), 3);
    assert_eq!(
        state.remembered_start_batch_delay(),
        Duration::from_millis(5)
    );
    assert_eq!(state.remembered_store_backend(), Some("in-memory"));
    assert!(registration.region().remembered_entities().is_some());
    system.shutdown();
}

#[tokio::test]
async fn remembered_entities_record_activation_survive_passivation_and_forget_explicitly() {
    let system = ActorSystem::new("remembered-activation-test");
    let sharding = ClusterSharding::get(&system);
    let store = InMemoryRememberedEntityStore::new();
    let key = EntityTypeKey::<CartCommand>::new("RememberedActivationCart")
        .with_number_of_shards(4)
        .unwrap();
    let registration = sharding
        .init(
            Entity::of(key.clone(), |context| FacadeCartEntity {
                context,
                items: Vec::new(),
            })
            .with_stop_message_factory(|| CartCommand::Passivate)
            .with_remembered_entities(
                RememberedEntities::enabled().with_store_ref(Arc::new(store.clone())),
            ),
        )
        .unwrap();
    let entity_id = EntityId::new("cart-1");
    let shard = ShardKey::new(
        key.entity_type().clone(),
        ShardId::for_entity(key.entity_type(), &entity_id, key.config()),
    );
    let cart = registration.entity_ref_for(entity_id.as_str());

    cart.tell(CartCommand::Add("apple".to_string())).unwrap();
    wait_for_remembered_count(&store, &shard, 1).await;

    assert!(sharding.passivate_entity_id(&key, &entity_id).unwrap());
    wait_for_entity_count_from_facade(&sharding, &key, 0).await;
    assert_eq!(store.len_for_shard(&shard), 1);

    assert!(sharding.forget_entity_id(&key, &entity_id).await.unwrap());
    assert_eq!(store.len_for_shard(&shard), 0);
    system.shutdown();
}

#[tokio::test]
async fn remembered_entities_replay_on_local_registration() {
    let system = ActorSystem::new("remembered-registration-replay-test");
    let sharding = ClusterSharding::get(&system);
    let store = InMemoryRememberedEntityStore::new();
    let key = EntityTypeKey::<CartCommand>::new("RememberedReplayCart")
        .with_number_of_shards(4)
        .unwrap();
    let entity_id = EntityId::new("cart-1");
    let shard = ShardKey::new(
        key.entity_type().clone(),
        ShardId::for_entity(key.entity_type(), &entity_id, key.config()),
    );
    store.remember(&shard, &entity_id).await.unwrap();

    sharding
        .init(
            Entity::of(key.clone(), |context| FacadeCartEntity {
                context,
                items: Vec::new(),
            })
            .with_remembered_entities(
                RememberedEntities::enabled().with_store_ref(Arc::new(store.clone())),
            ),
        )
        .unwrap();

    wait_for_entity_count_from_facade(&sharding, &key, 1).await;
    system.shutdown();
}

#[test]
fn cluster_sharding_facade_passes_allocation_strategy_to_runtime() {
    let local = node("rakka-0", "uid-a");
    let remote = node("rakka-1", "uid-b");
    let membership = membership_with_up_nodes(vec![local.clone(), remote.clone()]);
    let system = ActorSystem::new("facade-allocation-strategy-test");
    let sharding = ClusterSharding::from_membership(&system, local, membership);
    let key = EntityTypeKey::<CartCommand>::new("FacadeStrategyCart")
        .with_number_of_shards(4)
        .unwrap();

    let registration = sharding
        .init(
            Entity::of(key.clone(), |context| FacadeCartEntity {
                context,
                items: Vec::new(),
            })
            .with_allocation_strategy(LastRoutableAllocationStrategy),
        )
        .unwrap();
    let state = sharding.registration_state(&key).unwrap();
    let runtime = sharding.runtime();
    let runtime = runtime.try_lock().expect("cluster sharding runtime busy");
    let coordinator = runtime.coordinator(key.entity_type()).unwrap();

    assert!(state
        .allocation_strategy()
        .contains("LastRoutableAllocationStrategy"));
    assert!(registration
        .region()
        .allocation_strategy_name()
        .contains("LastRoutableAllocationStrategy"));
    assert!(coordinator
        .snapshot()
        .assignments()
        .iter()
        .all(|assignment| assignment.owner() == remote.id()));
    system.shutdown();
}

#[test]
fn in_memory_coordinator_store_rejects_stale_revision() {
    let local = node("rakka-0", "uid-a");
    let membership = membership_with_up_nodes(vec![local]);
    let entity_type = EntityType::new("StoredCart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config);
    coordinator.reconcile(&membership);

    let store = InMemoryShardCoordinatorStore::new();
    let state = PersistedShardCoordinatorState::now(coordinator.snapshot(), "test");
    ShardCoordinatorStore::compare_and_set(&store, &entity_type, 0, state.clone()).unwrap();

    let error = ShardCoordinatorStore::compare_and_set(&store, &entity_type, 0, state).unwrap_err();

    assert!(matches!(
        error,
        ShardingError::CoordinatorRevisionConflict {
            entity_type,
            expected_revision: 0,
            actual_revision: 1,
        } if entity_type == EntityType::new("StoredCart")
    ));
}

#[tokio::test]
async fn in_memory_coordinator_lease_acquires_renews_and_releases() {
    let lease = InMemoryShardCoordinatorLease::new().with_lease_duration(Duration::from_millis(50));
    let entity_type = EntityType::new("LeaseCart");
    let holder = NodeId::new("rakka-0", "uid-a");

    let token = lease.acquire(&entity_type, &holder).await.unwrap();
    assert_eq!(token.namespace(), "default");
    assert_eq!(token.entity_type(), &entity_type);
    assert_eq!(token.holder_node(), &holder);
    assert_eq!(token.fencing_token(), 1);
    assert!(lease.token(&entity_type).is_some());

    lease.renew(&token).await.unwrap();
    lease.release(token).await.unwrap();
    assert!(lease.token(&entity_type).is_none());
}

#[tokio::test]
async fn in_memory_coordinator_lease_rejects_active_holder_and_fences_stale_token() {
    let lease = InMemoryShardCoordinatorLease::new().with_lease_duration(Duration::from_millis(5));
    let entity_type = EntityType::new("LeaseConflictCart");
    let holder_a = NodeId::new("rakka-0", "uid-a");
    let holder_b = NodeId::new("rakka-1", "uid-b");

    let token_a = lease.acquire(&entity_type, &holder_a).await.unwrap();
    let rejected = lease.acquire(&entity_type, &holder_b).await.unwrap_err();
    assert!(matches!(
        rejected,
        ShardingError::CoordinatorLeaseRejected {
            entity_type,
            holder_node,
            current_holder_node: Some(current_holder_node),
            ..
        } if *entity_type == EntityType::new("LeaseConflictCart")
            && *holder_node == holder_b
            && *current_holder_node == holder_a
    ));

    tokio::time::sleep(Duration::from_millis(10)).await;
    let token_b = lease.acquire(&entity_type, &holder_b).await.unwrap();
    assert_eq!(token_b.fencing_token(), 2);

    let lost = lease.renew(&token_a).await.unwrap_err();
    assert!(matches!(
        lost,
        ShardingError::CoordinatorLeaseLost {
            entity_type,
            holder_node,
            fencing_token: 1,
            actual_holder_node: Some(actual_holder_node),
            actual_fencing_token: Some(2),
            ..
        } if *entity_type == EntityType::new("LeaseConflictCart")
            && *holder_node == holder_a
            && *actual_holder_node == holder_b
    ));
}

#[test]
fn cluster_sharding_runtime_persists_coordinator_snapshot_on_registration() {
    let local = node("rakka-0", "uid-a");
    let membership = membership_with_up_nodes(vec![local]);
    let entity_type = EntityType::new("DurableRuntimeCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = InMemoryShardCoordinatorStore::new();
    let mut runtime = ClusterShardingRuntime::with_coordinator_store(membership, store.clone());
    let region = runtime_region(entity_type.clone(), config);

    runtime.register_region(region.clone()).unwrap();

    let persisted = ShardCoordinatorStore::load(&store, &entity_type)
        .unwrap()
        .unwrap();
    assert_eq!(runtime.coordinator_store_backend(), Some("in-memory"));
    assert_eq!(persisted.snapshot().revision(), 1);
    assert_eq!(persisted.snapshot().assignments().len(), 4);
    assert_eq!(region.owner_revision(), persisted.snapshot().revision());
}

#[tokio::test]
async fn async_runtime_with_lease_allows_only_current_holder_to_coordinate() {
    let local_a = node("rakka-0", "uid-a");
    let local_b = node("rakka-1", "uid-b");
    let entity_type = EntityType::new("LeasedRuntimeCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = AsyncOnlyCoordinatorStore::default();
    let lease = InMemoryShardCoordinatorLease::new().with_lease_duration(Duration::from_millis(50));
    let mut runtime_a = ClusterShardingRuntime::with_async_coordinator_store(
        membership_with_up_nodes(vec![local_a.clone()]),
        store.clone(),
    )
    .with_coordinator_lease(lease.clone());
    let mut runtime_b = ClusterShardingRuntime::with_async_coordinator_store(
        membership_with_up_nodes(vec![local_b]),
        store.clone(),
    )
    .with_coordinator_lease(lease);

    runtime_a
        .register_region_async(runtime_region(entity_type.clone(), config.clone()))
        .await
        .unwrap();
    assert_eq!(runtime_a.coordinator_lease_backend(), Some("in-memory"));
    assert!(runtime_a.coordinator_lease_requires_async_api());
    assert_eq!(
        runtime_a
            .coordinator_lease_token(&entity_type)
            .unwrap()
            .holder_node(),
        local_a.id()
    );

    let error = runtime_b
        .register_region_async(runtime_region(entity_type.clone(), config))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ClusterShardingError::Sharding {
            error: ShardingError::CoordinatorLeaseRejected { entity_type: rejected, .. }
        } if *rejected == entity_type
    ));

    assert_eq!(
        store.load_sync(&entity_type).unwrap().snapshot().revision(),
        1
    );
}

#[tokio::test]
async fn lost_lease_prevents_stale_runtime_from_publishing_ownership() {
    let local_a = node("rakka-0", "uid-a");
    let local_b = node("rakka-1", "uid-b");
    let remote = node("rakka-2", "uid-c");
    let entity_type = EntityType::new("LostLeaseCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = AsyncOnlyCoordinatorStore::default();
    let lease = InMemoryShardCoordinatorLease::new().with_lease_duration(Duration::from_millis(5));
    let region_a = runtime_region(entity_type.clone(), config.clone());
    let region_b = runtime_region(entity_type.clone(), config.clone());
    let mut runtime_a = ClusterShardingRuntime::with_async_coordinator_store(
        membership_with_up_nodes(vec![local_a, remote.clone()]),
        store.clone(),
    )
    .with_coordinator_lease(lease.clone());

    runtime_a
        .register_region_async(region_a.clone())
        .await
        .unwrap();
    let initial_revision = region_a.owner_revision();
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut runtime_b = ClusterShardingRuntime::with_async_coordinator_store(
        membership_with_up_nodes(vec![local_b, remote.clone()]),
        store.clone(),
    )
    .with_coordinator_lease(lease);
    runtime_b.register_region_async(region_b).await.unwrap();
    let revision_after_takeover = store.load_sync(&entity_type).unwrap().snapshot().revision();

    let error = runtime_a
        .mark_down_async(remote.id(), 3)
        .await
        .expect_err("stale runtime should not regain leadership while another holder is active");

    assert!(matches!(
        error,
        ClusterShardingError::Sharding {
            error: ShardingError::CoordinatorLeaseRejected { entity_type: rejected, .. }
        } if *rejected == entity_type
    ));
    assert_eq!(region_a.owner_revision(), initial_revision);
    assert_eq!(
        store.load_sync(&entity_type).unwrap().snapshot().revision(),
        revision_after_takeover
    );
}

#[test]
fn sync_runtime_rejects_async_only_coordinator_store() {
    let local = node("rakka-0", "uid-a");
    let membership = membership_with_up_nodes(vec![local]);
    let entity_type = EntityType::new("AsyncOnlySyncCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = AsyncOnlyCoordinatorStore::default();
    let mut runtime = ClusterShardingRuntime::with_async_coordinator_store(membership, store);

    let error = runtime
        .register_region(runtime_region(entity_type, config))
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterShardingError::Sharding {
            error: ShardingError::AsyncCoordinatorStoreRequiresAsyncApi { backend }
        } if backend == "async-only-test"
    ));
}

#[tokio::test]
async fn async_runtime_persists_coordinator_snapshot_on_registration() {
    let local = node("rakka-0", "uid-a");
    let membership = membership_with_up_nodes(vec![local]);
    let entity_type = EntityType::new("AsyncDurableRuntimeCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = AsyncOnlyCoordinatorStore::default();
    let mut runtime =
        ClusterShardingRuntime::with_async_coordinator_store(membership, store.clone());
    let region = runtime_region(entity_type.clone(), config);

    runtime.register_region_async(region.clone()).await.unwrap();

    let persisted = store.load_sync(&entity_type).unwrap();
    assert_eq!(runtime.coordinator_store_backend(), Some("async-only-test"));
    assert!(runtime.coordinator_store_requires_async_api());
    assert_eq!(persisted.snapshot().revision(), 1);
    assert_eq!(persisted.snapshot().assignments().len(), 4);
    assert_eq!(region.owner_revision(), persisted.snapshot().revision());
}

#[tokio::test]
async fn async_runtime_recovers_persisted_coordinator_snapshot() {
    let local = node("rakka-0", "uid-a");
    let remote = node("rakka-1", "uid-b");
    let membership = membership_with_up_nodes(vec![local, remote]);
    let entity_type = EntityType::new("AsyncRecoveredRuntimeCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = AsyncOnlyCoordinatorStore::default();

    let mut first_runtime =
        ClusterShardingRuntime::with_async_coordinator_store(membership.clone(), store.clone());
    first_runtime
        .register_region_async(runtime_region(entity_type.clone(), config.clone()))
        .await
        .unwrap();
    let first_state = store.load_sync(&entity_type).unwrap();

    let mut recovered_runtime =
        ClusterShardingRuntime::with_async_coordinator_store(membership, store.clone());
    let recovered_region = runtime_region(entity_type.clone(), config);
    recovered_runtime
        .register_region_async(recovered_region.clone())
        .await
        .unwrap();
    let recovered = recovered_runtime.coordinator(&entity_type).unwrap();

    assert_eq!(recovered.revision(), first_state.snapshot().revision());
    assert_eq!(
        recovered.snapshot().assignments(),
        first_state.snapshot().assignments()
    );
    assert_eq!(
        recovered_region.owner_revision(),
        first_state.snapshot().revision()
    );
}

#[tokio::test]
async fn async_runtime_persists_rebalance_after_membership_update() {
    let local = node("rakka-0", "uid-a");
    let remote = node("rakka-1", "uid-b");
    let remote_id = remote.id().clone();
    let membership = membership_with_up_nodes(vec![local, remote]);
    let entity_type = EntityType::new("AsyncRebalancedRuntimeCart");
    let config = ShardingConfig::new(8).unwrap();
    let store = AsyncOnlyCoordinatorStore::default();
    let mut runtime =
        ClusterShardingRuntime::with_async_coordinator_store(membership, store.clone());

    runtime
        .register_region_async(runtime_region(entity_type.clone(), config))
        .await
        .unwrap();
    let initial = store.load_sync(&entity_type).unwrap();

    let update = runtime.mark_down_async(&remote_id, 3).await.unwrap();
    let persisted = store.load_sync(&entity_type).unwrap();

    assert!(!update.rebalances().is_empty());
    assert_eq!(
        initial.snapshot().revision() + 1,
        persisted.snapshot().revision()
    );
    assert!(persisted
        .snapshot()
        .assignments()
        .iter()
        .all(|assignment| assignment.owner() != &remote_id));
}

#[test]
fn cluster_sharding_runtime_recovers_persisted_coordinator_snapshot() {
    let local = node("rakka-0", "uid-a");
    let remote = node("rakka-1", "uid-b");
    let membership = membership_with_up_nodes(vec![local, remote]);
    let entity_type = EntityType::new("RecoveredRuntimeCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = InMemoryShardCoordinatorStore::new();

    let mut first_runtime =
        ClusterShardingRuntime::with_coordinator_store(membership.clone(), store.clone());
    first_runtime
        .register_region(runtime_region(entity_type.clone(), config.clone()))
        .unwrap();
    let first_state = ShardCoordinatorStore::load(&store, &entity_type)
        .unwrap()
        .unwrap();

    let mut recovered_runtime =
        ClusterShardingRuntime::with_coordinator_store(membership, store.clone());
    let recovered_region = runtime_region(entity_type.clone(), config);
    recovered_runtime
        .register_region(recovered_region.clone())
        .unwrap();
    let recovered = recovered_runtime.coordinator(&entity_type).unwrap();

    assert_eq!(recovered.revision(), first_state.snapshot().revision());
    assert_eq!(
        recovered.snapshot().assignments(),
        first_state.snapshot().assignments()
    );
    assert_eq!(
        recovered_region.owner_revision(),
        first_state.snapshot().revision()
    );
}

#[test]
fn cluster_sharding_runtime_rejects_mismatched_persisted_coordinator_snapshot() {
    let local = node("rakka-0", "uid-a");
    let membership = membership_with_up_nodes(vec![local]);
    let entity_type = EntityType::new("MismatchedDurableCart");
    let store = InMemoryShardCoordinatorStore::new();
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config);
    coordinator.reconcile(&membership);
    ShardCoordinatorStore::compare_and_set(
        &store,
        &entity_type,
        0,
        PersistedShardCoordinatorState::now(coordinator.snapshot(), "test"),
    )
    .unwrap();

    let mut runtime = ClusterShardingRuntime::with_coordinator_store(membership, store);
    let error = runtime
        .register_region(runtime_region(
            entity_type.clone(),
            ShardingConfig::new(8).unwrap(),
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterShardingError::Sharding {
            error: ShardingError::PersistedCoordinatorSnapshotMismatch {
                expected_entity_type,
                actual_entity_type,
                expected_shards: 8,
                actual_shards: 4,
            }
        } if expected_entity_type == entity_type && actual_entity_type == entity_type
    ));
}

#[test]
fn cluster_sharding_facade_can_use_durable_coordinator_store() {
    let system = ActorSystem::new("facade-durable-coordinator-test");
    let store = InMemoryShardCoordinatorStore::new();
    let sharding = ClusterSharding::get_with_coordinator_store(&system, store.clone());
    let key = EntityTypeKey::<CartCommand>::new("FacadeDurableCart")
        .with_number_of_shards(4)
        .unwrap();

    sharding
        .init(Entity::of(key.clone(), |context| FacadeCartEntity {
            context,
            items: Vec::new(),
        }))
        .unwrap();

    let persisted = ShardCoordinatorStore::load(&store, key.entity_type())
        .unwrap()
        .unwrap();
    let runtime = sharding.runtime();
    let runtime = runtime.try_lock().expect("cluster sharding runtime busy");
    assert_eq!(runtime.coordinator_store_backend(), Some("in-memory"));
    assert_eq!(persisted.snapshot().entity_type(), key.entity_type());
    assert_eq!(persisted.snapshot().number_of_shards(), 4);
    system.shutdown();
}

#[test]
fn sync_facade_rejects_async_only_coordinator_store() {
    let system = ActorSystem::new("facade-sync-async-store-test");
    let store = AsyncOnlyCoordinatorStore::default();
    let sharding = ClusterSharding::get_with_async_coordinator_store(&system, store);
    let key = EntityTypeKey::<CartCommand>::new("FacadeSyncAsyncStoreCart")
        .with_number_of_shards(4)
        .unwrap();

    let error = sharding
        .init(Entity::of(key, |context| FacadeCartEntity {
            context,
            items: Vec::new(),
        }))
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterShardingError::Sharding {
            error: ShardingError::AsyncCoordinatorStoreRequiresAsyncApi { backend }
        } if backend == "async-only-test"
    ));
    system.shutdown();
}

#[tokio::test]
async fn async_facade_init_uses_async_only_coordinator_store() {
    let system = ActorSystem::new("facade-async-coordinator-test");
    let store = AsyncOnlyCoordinatorStore::default();
    let sharding = ClusterSharding::get_with_async_coordinator_store(&system, store.clone());
    let key = EntityTypeKey::<CartCommand>::new("FacadeAsyncDurableCart")
        .with_number_of_shards(4)
        .unwrap();

    sharding
        .init_async(Entity::of(key.clone(), |context| FacadeCartEntity {
            context,
            items: Vec::new(),
        }))
        .await
        .unwrap();

    let persisted = store.load_sync(key.entity_type()).unwrap();
    let runtime = sharding.runtime();
    let runtime = runtime.try_lock().expect("cluster sharding runtime busy");
    assert_eq!(runtime.coordinator_store_backend(), Some("async-only-test"));
    assert_eq!(persisted.snapshot().entity_type(), key.entity_type());
    assert_eq!(persisted.snapshot().number_of_shards(), 4);
    system.shutdown();
}

#[test]
fn cluster_sharding_facade_reports_proxy_and_message_mismatch_state() {
    let system = ActorSystem::new("facade-proxy-test");
    let sharding = ClusterSharding::get(&system);
    let key = EntityTypeKey::<CartCommand>::new("ProxyCart")
        .with_number_of_shards(4)
        .unwrap();
    let proxy = sharding.init_proxy(key.clone()).unwrap();

    let state = sharding.registration_state(&key).unwrap();
    assert!(state.proxy_only());
    assert_eq!(state.local_entity_count(), 0);
    let proxy_ref = proxy.entity_ref_for("cart-1");
    let error = proxy_ref
        .tell(CartCommand::Add("apple".to_string()))
        .unwrap_err();
    assert!(matches!(
        error,
        EntityTellError::Delivery {
            failure: EntityDeliveryFailure::NotLocal { .. },
            ..
        }
    ));

    let mismatched_key = EntityTypeKey::<RemoteCartCommand>::new("ProxyCart")
        .with_number_of_shards(4)
        .unwrap();
    let error = sharding.region_for(&mismatched_key).unwrap_err();
    assert!(matches!(
        error,
        ClusterShardingError::EntityTypeMessageMismatch { entity_type }
            if entity_type == EntityType::new("ProxyCart")
    ));
    system.shutdown();
}

#[test]
fn entity_refs_compute_stable_shard_keys() {
    let config = ShardingConfig::new(16).unwrap();
    let entity_type = EntityType::new("Cart");
    let first = EntityRef::<CartCommand>::new(entity_type.clone(), EntityId::new("cart-42"));
    let second = EntityRef::<CartCommand>::new(entity_type.clone(), EntityId::new("cart-42"));
    let other_type =
        EntityRef::<CartCommand>::new(EntityType::new("Order"), EntityId::new("cart-42"));

    assert_eq!(first.shard_id(&config), second.shard_id(&config));
    assert_eq!(first.shard_key(&config).entity_type(), &entity_type);
    assert!(first.shard_id(&config).as_u32() < config.number_of_shards());
    assert_ne!(first.shard_id(&config), other_type.shard_id(&config));
}

#[test]
fn shard_ownership_snapshot_records_owner_counts() {
    let local = node("rakka-0", "uid-a");
    let remote = node("rakka-1", "uid-b");
    let membership = membership_with_up_nodes(vec![local.clone(), remote.clone()]);
    let mut coordinator = ShardCoordinator::new(
        EntityType::new("Cart"),
        ShardingConfig::new(4).expect("valid sharding config"),
    );
    let plan = coordinator.reconcile(&membership);
    assert!(!plan.is_empty());

    let recorder = InMemoryMetricsRecorder::new();
    let snapshot = coordinator.snapshot();
    let counts = snapshot.record_metrics(&recorder);

    assert_eq!(snapshot.owned_shard_count(local.id()), 2);
    assert_eq!(snapshot.owned_shard_count(remote.id()), 2);
    assert_eq!(counts.len(), 2);
    assert!(recorder
        .snapshot()
        .observations_named(METRIC_SHARD_OWNERSHIP_COUNT)
        .iter()
        .any(
            |observation| observation.attribute("entity_type") == Some("Cart")
                && observation.value() == 2.0
        ));
}

#[test]
fn invalid_zero_shard_config_fails_closed() {
    assert_eq!(
        ShardingConfig::new(0).unwrap_err(),
        ShardingError::InvalidShardCount
    );
}

#[test]
fn coordinator_allocates_all_shards_to_routable_members() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(6).unwrap());

    let plan = coordinator.reconcile(&membership);
    let snapshot = coordinator.snapshot();

    assert_eq!(plan.previous_revision(), 0);
    assert_eq!(plan.new_revision(), 1);
    assert_eq!(plan.decisions().len(), 6);
    assert!(plan.decisions().iter().all(|decision| {
        matches!(
            decision,
            ShardDecision::Assign {
                reason: ShardMoveReason::InitialAllocation,
                ..
            }
        )
    }));
    assert_eq!(snapshot.assignments().len(), 6);
    assert_eq!(snapshot.assignments()[0].owner().logical_id(), "rakka-0");
    assert_eq!(snapshot.assignments()[1].owner().logical_id(), "rakka-1");
}

#[test]
fn coordinator_uses_custom_strategy_for_initial_allocation() {
    let membership = membership_with_up_nodes(vec![
        node("rakka-0", "uid-a"),
        node("rakka-1", "uid-b"),
        node("rakka-2", "uid-c"),
    ]);
    let mut coordinator = ShardCoordinator::with_allocation_strategy(
        EntityType::new("Cart"),
        ShardingConfig::new(6).unwrap(),
        LastRoutableAllocationStrategy,
    );

    let plan = coordinator.reconcile(&membership);
    let snapshot = coordinator.snapshot();

    assert_eq!(plan.decisions().len(), 6);
    assert!(plan.decisions().iter().all(|decision| {
        matches!(
            decision,
            ShardDecision::Assign {
                to,
                reason: ShardMoveReason::InitialAllocation,
                ..
            } if to.logical_id() == "rakka-2"
        )
    }));
    assert!(snapshot
        .assignments()
        .iter()
        .all(|assignment| assignment.owner().logical_id() == "rakka-2"));
}

#[test]
fn reconciliation_is_empty_when_membership_does_not_change() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(4).unwrap());

    coordinator.reconcile(&membership);
    let plan = coordinator.reconcile(&membership);

    assert!(plan.is_empty());
    assert_eq!(plan.previous_revision(), 1);
    assert_eq!(plan.new_revision(), 1);
}

#[test]
fn joining_member_rebalances_existing_ownership() {
    let two_nodes =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let three_nodes = membership_with_up_nodes(vec![
        node("rakka-0", "uid-a"),
        node("rakka-1", "uid-b"),
        node("rakka-2", "uid-c"),
    ]);
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(6).unwrap());

    coordinator.reconcile(&two_nodes);
    let plan = coordinator.reconcile(&three_nodes);

    assert_eq!(plan.previous_revision(), 1);
    assert_eq!(plan.new_revision(), 2);
    assert!(plan.decisions().iter().any(|decision| {
        matches!(
            decision,
            ShardDecision::Move {
                to,
                reason: ShardMoveReason::Rebalance,
                ..
            } if to.logical_id() == "rakka-2"
        )
    }));
    assert_eq!(
        coordinator
            .snapshot()
            .assignments()
            .iter()
            .filter(|assignment| assignment.owner().logical_id() == "rakka-2")
            .count(),
        2
    );
}

#[test]
fn least_shard_strategy_rebalances_existing_ownership_with_limit() {
    let one_node = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let three_nodes = membership_with_up_nodes(vec![
        node("rakka-0", "uid-a"),
        node("rakka-1", "uid-b"),
        node("rakka-2", "uid-c"),
    ]);
    let mut coordinator = ShardCoordinator::with_allocation_strategy(
        EntityType::new("Cart"),
        ShardingConfig::new(6).unwrap(),
        LeastShardAllocationStrategy::new(1, 2),
    );

    coordinator.reconcile(&one_node);
    let plan = coordinator.reconcile(&three_nodes);
    let snapshot = coordinator.snapshot();

    let rebalance_moves = plan
        .decisions()
        .iter()
        .filter(|decision| {
            matches!(
                decision,
                ShardDecision::Move {
                    from,
                    reason: ShardMoveReason::Rebalance,
                    ..
                } if from.logical_id() == "rakka-0"
            )
        })
        .count();

    assert_eq!(rebalance_moves, 2);
    assert_eq!(
        snapshot.owned_shard_count(&NodeId::new("rakka-0", "uid-a")),
        4
    );
    assert_eq!(
        snapshot.owned_shard_count(&NodeId::new("rakka-1", "uid-b")),
        1
    );
    assert_eq!(
        snapshot.owned_shard_count(&NodeId::new("rakka-2", "uid-c")),
        1
    );
}

#[test]
fn leaving_member_triggers_graceful_handoff_decisions() {
    let mut membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let leaving_id = NodeId::new("rakka-1", "uid-b");
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(4).unwrap());
    coordinator.reconcile(&membership);

    membership.mark_leaving(&leaving_id, 10).unwrap();
    let plan = coordinator.reconcile(&membership);

    assert_eq!(plan.new_revision(), 2);
    assert!(plan.decisions().iter().all(|decision| {
        matches!(
            decision,
            ShardDecision::Move {
                from,
                to,
                reason: ShardMoveReason::GracefulLeave,
                ..
            } if from == &leaving_id && to.logical_id() == "rakka-0"
        )
    }));
    assert!(coordinator
        .snapshot()
        .assignments()
        .iter()
        .all(|assignment| assignment.owner().logical_id() == "rakka-0"));
}

#[test]
fn down_member_triggers_failover_decisions() {
    let mut membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let down_id = NodeId::new("rakka-1", "uid-b");
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(4).unwrap());
    coordinator.reconcile(&membership);

    membership.mark_down(&down_id, 10).unwrap();
    let plan = coordinator.reconcile(&membership);

    assert!(plan.decisions().iter().all(|decision| {
        matches!(
            decision,
            ShardDecision::Move {
                from,
                to,
                reason: ShardMoveReason::OwnerUnavailable,
                ..
            } if from == &down_id && to.logical_id() == "rakka-0"
        )
    }));
}

#[test]
fn cluster_sharding_runtime_rebalances_and_refreshes_region_when_node_joins() {
    let local = node("rakka-0", "uid-a");
    let local_id = local.id().clone();
    let remote = node("rakka-1", "uid-b");
    let remote_id = remote.id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut runtime = runtime_with_local(local.clone());
    let region = runtime_region(entity_type.clone(), config);

    runtime.register_region(region.clone()).unwrap();
    assert_eq!(region.owner_revision(), 0);

    let local_update = runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local.clone()]))
        .unwrap();

    assert_eq!(
        local_update.membership_events(),
        &[MembershipEvent::MemberUp { node_id: local_id }]
    );
    assert_eq!(region.owner_revision(), 1);

    let join_update = runtime
        .apply_discovery(DiscoverySnapshot::new("test", 2, [local, remote]))
        .unwrap();

    assert!(join_update
        .membership_events()
        .contains(&MembershipEvent::MemberDiscovered {
            node_id: remote_id.clone(),
        }));
    assert!(join_update
        .membership_events()
        .contains(&MembershipEvent::MemberUp {
            node_id: remote_id.clone(),
        }));
    assert!(join_update
        .rebalances()
        .iter()
        .any(|rebalance| rebalance.entity_type() == &entity_type && !rebalance.plan().is_empty()));

    let coordinator = runtime.coordinator(&entity_type).unwrap();
    assert_eq!(region.owner_revision(), coordinator.revision());
    assert_eq!(
        coordinator.owner_for_shard(ShardId::new(1)).unwrap(),
        &remote_id
    );
}

#[tokio::test]
async fn ownership_refresh_passivates_entities_for_shards_lost_to_joining_node() {
    let local = node("rakka-0", "uid-a");
    let local_id = local.id().clone();
    let remote = node("rakka-1", "uid-b");
    let remote_id = remote.id().clone();
    let entity_type = EntityType::new("CartJoinPassivation");
    let config = ShardingConfig::new(4).unwrap();
    let system = ActorSystem::new("join-passivation-node-a-test");
    let route = LocalEntityRoute::new(local_id, system.clone(), |context: LocalEntityContext| {
        CartEntity {
            context,
            items: Vec::new(),
        }
    });
    let region = ShardRegion::new(entity_type.clone(), config.clone(), route.clone());
    let mut runtime = runtime_with_local(local.clone());
    let mut joined_coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    joined_coordinator.reconcile(&membership_with_up_nodes(vec![
        local.clone(),
        remote.clone(),
    ]));
    let entity_id = entity_owned_by(&joined_coordinator, remote_id.logical_id());
    let entity = EntityRef::<CartCommand>::new(entity_type.clone(), entity_id);
    let shard_id = entity.shard_id(&config);

    runtime.register_region(region.clone()).unwrap();
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local.clone()]))
        .unwrap();

    entity
        .tell(&region, CartCommand::Add("before-join".to_string()))
        .unwrap();
    assert_eq!(route.entity_count(), 1);

    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 2, [local, remote]))
        .unwrap();

    assert_eq!(route.entity_count(), 0);
    assert_eq!(
        route.shard_handoff_state(shard_id),
        ShardHandoffState::Transferring
    );
    assert_eq!(
        runtime
            .coordinator(&entity_type)
            .unwrap()
            .owner_for_shard(shard_id)
            .unwrap(),
        &remote_id
    );

    system.shutdown();
}

#[test]
fn cluster_sharding_runtime_moves_shards_from_leaving_node() {
    let local = node("rakka-0", "uid-a");
    let remote = node("rakka-1", "uid-b");
    let remote_id = remote.id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut runtime = runtime_with_local(local.clone());
    let region = runtime_region(entity_type.clone(), config);

    runtime.register_region(region.clone()).unwrap();
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local, remote]))
        .unwrap();

    let leave_update = runtime.mark_leaving(&remote_id, 3).unwrap();

    assert!(leave_update
        .membership_events()
        .contains(&MembershipEvent::MemberLeaving {
            node_id: remote_id.clone(),
        }));
    assert!(leave_update.rebalances().iter().any(|rebalance| {
        rebalance.entity_type() == &entity_type
            && rebalance.plan().decisions().iter().any(|decision| {
                matches!(
                    decision,
                    ShardDecision::Move {
                        from,
                        reason: ShardMoveReason::GracefulLeave,
                        ..
                    } if from == &remote_id
                )
            })
    }));

    let coordinator = runtime.coordinator(&entity_type).unwrap();
    assert!(coordinator_owners_exclude(coordinator, &remote_id));
    assert_eq!(region.owner_revision(), coordinator.revision());
}

#[tokio::test]
async fn cluster_sharding_runtime_runs_graceful_handoff_before_ownership_publish() {
    let local = node("rakka-0", "uid-a");
    let local_id = local.id().clone();
    let leaving = node("rakka-1", "uid-b");
    let leaving_id = leaving.id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let route_a = LocalEntityRoute::new(
        local_id.clone(),
        ActorSystem::new("handoff-node-a-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let route_b = LocalEntityRoute::new(
        leaving_id.clone(),
        ActorSystem::new("handoff-node-b-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region_a = ShardRegion::new(entity_type.clone(), config.clone(), route_a.clone());
    let region_b = ShardRegion::new(entity_type.clone(), config.clone(), route_b.clone());
    let mut runtime = runtime_with_local(local.clone());

    runtime.register_region(region_a.clone()).unwrap();
    runtime.register_region(region_b.clone()).unwrap();
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local, leaving]))
        .unwrap();

    let entity_id = entity_owned_by(runtime.coordinator(&entity_type).unwrap(), "rakka-1");
    let entity = EntityRef::<CartCommand>::new(entity_type.clone(), entity_id);
    let shard_key = entity.shard_key(&config);
    let shard_id = shard_key.shard_id();

    entity
        .tell(&region_b, CartCommand::Add("apple".to_string()))
        .unwrap();
    assert_eq!(route_b.entity_count(), 1);

    let leave_update = runtime.mark_leaving(&leaving_id, 2).unwrap();
    let states = leave_update
        .handoffs()
        .iter()
        .filter(|handoff| handoff.shard() == &shard_key)
        .map(|handoff| handoff.state())
        .collect::<Vec<_>>();

    assert_eq!(
        states,
        vec![
            ShardHandoffState::Draining,
            ShardHandoffState::Transferring,
            ShardHandoffState::Acquired,
        ]
    );
    assert_eq!(
        leave_update
            .handoffs()
            .iter()
            .find(|handoff| {
                handoff.shard() == &shard_key && handoff.state() == ShardHandoffState::Transferring
            })
            .unwrap()
            .stopped_entities(),
        1
    );
    assert_eq!(
        route_b.shard_handoff_state(shard_id),
        ShardHandoffState::Transferring
    );
    assert_eq!(route_b.entity_count(), 0);
    assert_eq!(
        route_a.shard_handoff_state(shard_id),
        ShardHandoffState::Acquired
    );

    let coordinator = runtime.coordinator(&entity_type).unwrap();
    assert_eq!(coordinator.owner_for_shard(shard_id).unwrap(), &local_id);
    assert_eq!(region_a.owner_revision(), coordinator.revision());

    entity
        .tell(&region_a, CartCommand::Add("banana".to_string()))
        .unwrap();
    assert_eq!(route_a.entity_count(), 1);
}

#[tokio::test]
async fn coordinated_shutdown_runs_cluster_sharding_local_leave_handoff() {
    let local = node("rakka-0", "uid-a");
    let local_id = local.id().clone();
    let remote = node("rakka-1", "uid-b");
    let entity_type = EntityType::new("CartShutdown");
    let config = ShardingConfig::new(4).unwrap();
    let route_a = LocalEntityRoute::new(
        local_id.clone(),
        ActorSystem::new("shutdown-handoff-node-a-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let route_b = LocalEntityRoute::new(
        remote.id().clone(),
        ActorSystem::new("shutdown-handoff-node-b-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region_a = ShardRegion::new(entity_type.clone(), config.clone(), route_a.clone());
    let region_b = ShardRegion::new(entity_type.clone(), config.clone(), route_b);
    let mut runtime = runtime_with_local(local.clone());

    runtime.register_region(region_a.clone()).unwrap();
    runtime.register_region(region_b).unwrap();
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local, remote]))
        .unwrap();

    let entity_id = entity_owned_by(runtime.coordinator(&entity_type).unwrap(), "rakka-0");
    let entity = EntityRef::<CartCommand>::new(entity_type.clone(), entity_id);
    let shard_key = entity.shard_key(&config);
    let shard_id = shard_key.shard_id();
    entity
        .tell(&region_a, CartCommand::Add("apple".to_string()))
        .unwrap();
    assert_eq!(route_a.entity_count(), 1);

    let handle = ClusterShardingShutdownHandle::new(runtime);
    let shutdown = CoordinatedShutdown::new();
    register_cluster_sharding_leave_task(&shutdown, "handoff-local-shards", handle.clone())
        .unwrap();

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();
    let update = handle.last_update().expect("shutdown update should record");

    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    assert_eq!(
        report
            .phases()
            .iter()
            .find(|phase| phase.phase() == &ShutdownPhase::handoff_shards())
            .and_then(|phase| phase.tasks().first())
            .map(|task| task.status()),
        Some(ShutdownTaskStatus::Completed)
    );
    assert!(update.handoffs().iter().any(|handoff| {
        handoff.shard() == &shard_key
            && handoff.state() == ShardHandoffState::Transferring
            && handoff.stopped_entities() == 1
    }));
    assert_eq!(
        route_a.shard_handoff_state(shard_id),
        ShardHandoffState::Transferring
    );
}

#[tokio::test]
async fn remembered_entities_replay_after_graceful_handoff_acquire() {
    let local = node("rakka-0", "uid-a");
    let local_id = local.id().clone();
    let leaving = node("rakka-1", "uid-b");
    let leaving_id = leaving.id().clone();
    let entity_type = EntityType::new("RememberedHandoffCart");
    let config = ShardingConfig::new(4).unwrap();
    let store = InMemoryRememberedEntityStore::new();
    let remembered = RememberedEntities::enabled().with_store_ref(Arc::new(store.clone()));
    let route_a = LocalEntityRoute::new(
        local_id.clone(),
        ActorSystem::new("remembered-handoff-node-a-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let route_b = LocalEntityRoute::new(
        leaving_id.clone(),
        ActorSystem::new("remembered-handoff-node-b-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region_a = ShardRegion::new(entity_type.clone(), config.clone(), route_a.clone())
        .with_remembered_entities(remembered.clone());
    let region_b = ShardRegion::new(entity_type.clone(), config.clone(), route_b.clone())
        .with_remembered_entities(remembered);
    let mut runtime = runtime_with_local(local.clone());

    runtime.register_region(region_a.clone()).unwrap();
    runtime.register_region(region_b.clone()).unwrap();
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local, leaving]))
        .unwrap();

    let entity_id = entity_owned_by(runtime.coordinator(&entity_type).unwrap(), "rakka-1");
    let entity = EntityRef::<CartCommand>::new(entity_type.clone(), entity_id.clone());
    let shard = entity.shard_key(&config);
    store.remember(&shard, &entity_id).await.unwrap();
    entity
        .tell(&region_b, CartCommand::Add("apple".to_string()))
        .unwrap();
    assert_eq!(route_b.entity_count(), 1);

    let _leave_update = runtime.mark_leaving(&leaving_id, 2).unwrap();

    assert_eq!(
        route_a.shard_handoff_state(shard.shard_id()),
        ShardHandoffState::Acquired
    );
    assert_eq!(route_b.entity_count(), 0);
    wait_for_entity_count(&route_a, 1).await;
}

#[tokio::test]
async fn shard_region_buffers_messages_during_local_handoff_until_acquire() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("BufferedCart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-handoff-buffer-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route.clone())
            .unwrap()
            .with_buffering(ShardBufferConfig::new(8, Duration::from_secs(1)));
    let entity = region.entity_ref("cart-42");
    let shard_id = entity.shard_id(region.config());

    assert_eq!(region.begin_shard_handoff(shard_id).unwrap(), 0);
    entity
        .tell(&region, CartCommand::Add("buffered".to_string()))
        .unwrap();

    assert_eq!(region.buffered_message_count_for_shard(shard_id), 1);
    assert_eq!(route.entity_count(), 0);

    assert_eq!(region.acquire_shard(shard_id).unwrap(), 0);
    let reply = entity
        .ask(&region, CartCommand::Get, Duration::from_millis(250))
        .await
        .unwrap();

    assert_eq!(reply, "cart-42:buffered");
    assert_eq!(region.buffered_message_count_for_shard(shard_id), 0);
    assert_eq!(route.entity_count(), 1);
}

#[test]
fn shard_region_reports_buffer_full_when_handoff_buffer_overflows() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("OverflowCart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-handoff-buffer-overflow-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route.clone())
            .unwrap()
            .with_buffering(ShardBufferConfig::new(1, Duration::from_secs(1)));
    let entity = region.entity_ref("cart-42");
    let shard_id = entity.shard_id(region.config());

    region.begin_shard_handoff(shard_id).unwrap();
    entity
        .tell(&region, CartCommand::Add("first".to_string()))
        .unwrap();
    let error = entity
        .tell(&region, CartCommand::Add("second".to_string()))
        .expect_err("second message should overflow the shard buffer");

    assert_eq!(region.buffered_message_count_for_shard(shard_id), 1);
    match error {
        EntityTellError::Delivery {
            message: CartCommand::Add(value),
            failure:
                EntityDeliveryFailure::ShardBufferFull {
                    shard_id: failed_shard_id,
                    capacity,
                },
        } => {
            assert_eq!(value, "second");
            assert_eq!(failed_shard_id, shard_id);
            assert_eq!(capacity, 1);
        }
        other => panic!("unexpected buffer overflow error: {other:?}"),
    }
}

#[test]
fn cluster_sharding_runtime_failover_refreshes_regions_after_unreachable_tick() {
    let local = node("rakka-0", "uid-a");
    let remote = node("rakka-1", "uid-b");
    let remote_id = remote.id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut runtime = runtime_with_local(local.clone());
    let region = runtime_region(entity_type.clone(), config);

    runtime.register_region(region.clone()).unwrap();
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local, remote]))
        .unwrap();

    let failover_update = runtime.tick(51).unwrap();

    assert_eq!(
        failover_update.membership_events(),
        &[MembershipEvent::MemberUnreachable {
            node_id: remote_id.clone(),
        }]
    );
    assert!(failover_update.rebalances().iter().any(|rebalance| {
        rebalance.entity_type() == &entity_type
            && rebalance.plan().decisions().iter().any(|decision| {
                matches!(
                    decision,
                    ShardDecision::Move {
                        from,
                        reason: ShardMoveReason::OwnerUnavailable,
                        ..
                    } if from == &remote_id
                )
            })
    }));

    let coordinator = runtime.coordinator(&entity_type).unwrap();
    assert!(coordinator_owners_exclude(coordinator, &remote_id));
    assert_eq!(region.owner_revision(), coordinator.revision());
}

#[test]
fn cluster_sharding_runtime_rejects_incompatible_nodes_before_ownership() {
    let local = node("rakka-0", "uid-a");
    let incompatible_protocol = ClusterProtocol::new(
        ProtocolVersion::new(2, 0),
        CompatibilityRange::new(ProtocolVersion::new(2, 0), ProtocolVersion::new(2, 0)),
    );
    let remote = node("rakka-1", "uid-b").with_protocol(incompatible_protocol);
    let remote_id = remote.id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut runtime = runtime_with_local(local.clone());
    let region = runtime_region(entity_type.clone(), config);

    runtime.register_region(region.clone()).unwrap();
    runtime
        .apply_discovery(DiscoverySnapshot::new("test", 1, [local.clone()]))
        .unwrap();

    let error = runtime
        .apply_discovery(DiscoverySnapshot::new("test", 2, vec![local, remote]))
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterShardingError::Cluster {
            error: ClusterError::IncompatibleNode {
                node_id,
                remote,
                ..
            },
        } if node_id == remote_id && remote == incompatible_protocol
    ));
    assert_eq!(runtime.membership().snapshot().members().len(), 1);

    let coordinator = runtime.coordinator(&entity_type).unwrap();
    assert!(coordinator_owners_exclude(coordinator, &remote_id));
    assert_eq!(region.owner_revision(), coordinator.revision());
}

#[test]
fn no_routable_members_unassigns_existing_shards() {
    let mut membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_id = membership.local_node_id().clone();
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(3).unwrap());
    coordinator.reconcile(&membership);

    membership.mark_down(&local_id, 10).unwrap();
    let plan = coordinator.reconcile(&membership);

    assert_eq!(plan.decisions().len(), 3);
    assert!(plan.decisions().iter().all(|decision| {
        matches!(
            decision,
            ShardDecision::Unassign {
                reason: ShardMoveReason::NoRoutableMembers,
                ..
            }
        )
    }));
    assert!(coordinator.snapshot().assignments().is_empty());
}

#[test]
fn owner_lookup_reports_missing_and_unknown_shards() {
    let coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(4).unwrap());

    assert!(matches!(
        coordinator.owner_for_shard(ShardId::new(2)).unwrap_err(),
        ShardingError::NoShardOwner { .. }
    ));
    assert!(matches!(
        coordinator.owner_for_shard(ShardId::new(4)).unwrap_err(),
        ShardingError::UnknownShard { .. }
    ));
}

#[test]
fn owner_cache_refreshes_from_coordinator_snapshots() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);

    let snapshot = coordinator.snapshot();
    let cache = ShardOwnerCache::from_snapshot(entity_type, config, &snapshot).unwrap();

    assert_eq!(cache.revision(), 1);
    assert_eq!(
        cache.owner_for_shard(ShardId::new(0)).unwrap().logical_id(),
        "rakka-0"
    );
    assert_eq!(
        cache.owner_for_shard(ShardId::new(1)).unwrap().logical_id(),
        "rakka-1"
    );
}

#[test]
fn owner_cache_rejects_mismatched_snapshots() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Order"), ShardingConfig::new(4).unwrap());
    coordinator.reconcile(&membership);

    let error = ShardOwnerCache::from_snapshot(
        EntityType::new("Cart"),
        ShardingConfig::new(4).unwrap(),
        &coordinator.snapshot(),
    )
    .unwrap_err();

    assert!(matches!(
        error,
        ShardingError::OwnershipSnapshotMismatch { .. }
    ));
}

#[test]
fn shard_region_tell_routes_resolved_messages() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let delivered_for_route = delivered.clone();
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        move |message: RoutedEntityMessage<CartCommand>| {
            let owner = message.owner().clone();
            let shard_id = message.shard_id();
            let entity_id = message.entity_id().clone();
            let payload = match message.into_message() {
                CartCommand::Add(value) => value,
                CartCommand::Get(_reply_to) => "unexpected-get".to_string(),
                CartCommand::Passivate => "unexpected-passivate".to_string(),
            };
            delivered_for_route
                .lock()
                .expect("delivered mutex poisoned")
                .push((owner, shard_id, entity_id, payload));
            Ok(())
        },
    )
    .unwrap();
    let entity = region.entity_ref("cart-42");
    let expected_shard = entity.shard_id(region.config());

    entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap();

    let delivered = delivered.lock().expect("delivered mutex poisoned");
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].1, expected_shard);
    assert_eq!(delivered[0].2, EntityId::new("cart-42"));
    assert_eq!(delivered[0].3, "apple");
}

#[tokio::test]
async fn shard_region_ask_routes_and_receives_replies() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        move |message: RoutedEntityMessage<CartCommand>| {
            match message.into_message() {
                CartCommand::Get(reply_to) => {
                    let _ = reply_to.reply("cart-is-ready".to_string());
                }
                CartCommand::Add(_value) => {}
                CartCommand::Passivate => {}
            }
            Ok(())
        },
    )
    .unwrap();
    let entity = region.entity_ref("cart-42");

    let reply = entity
        .ask(&region, CartCommand::Get, Duration::from_millis(100))
        .await
        .unwrap();

    assert_eq!(reply, "cart-is-ready");
}

#[test]
fn shard_region_returns_message_when_owner_is_missing() {
    let region = ShardRegion::new(
        EntityType::new("Cart"),
        ShardingConfig::new(4).unwrap(),
        |_message: RoutedEntityMessage<CartCommand>| Ok(()),
    );
    let entity = region.entity_ref("cart-42");

    let error = entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap_err();

    assert!(matches!(error, EntityTellError::NoRoute { .. }));
    assert!(matches!(
        error.into_message(),
        CartCommand::Add(value) if value == "apple"
    ));
}

#[test]
fn shard_region_rejects_entity_refs_from_other_entity_types() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let mut coordinator =
        ShardCoordinator::new(EntityType::new("Cart"), ShardingConfig::new(4).unwrap());
    coordinator.reconcile(&membership);
    let region = ShardRegion::from_snapshot(
        EntityType::new("Cart"),
        ShardingConfig::new(4).unwrap(),
        &coordinator.snapshot(),
        |_message: RoutedEntityMessage<CartCommand>| Ok(()),
    )
    .unwrap();
    let wrong_entity =
        EntityRef::<CartCommand>::new(EntityType::new("Order"), EntityId::new("order-1"));

    let error = wrong_entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap_err();

    assert!(matches!(
        error,
        EntityTellError::NoRoute {
            error: ShardingError::EntityTypeMismatch { .. },
            ..
        }
    ));
}

#[tokio::test]
async fn shard_region_ask_maps_delivery_failures() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        |message: RoutedEntityMessage<CartCommand>| {
            Err(EntityTellError::Delivery {
                message: message.into_message(),
                failure: EntityDeliveryFailure::MailboxFull,
            })
        },
    )
    .unwrap();
    let entity = region.entity_ref("cart-42");

    let error = entity
        .ask(&region, CartCommand::Get, Duration::from_millis(100))
        .await
        .unwrap_err();

    assert_eq!(error, EntityAskError::MailboxFull);
}

#[tokio::test]
async fn local_entity_route_spawns_and_reuses_entity_actors() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let created = Arc::new(Mutex::new(Vec::new()));
    let created_for_factory = created.clone();
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-sharding-test"),
        move |context: LocalEntityContext| {
            created_for_factory
                .lock()
                .expect("created mutex poisoned")
                .push(context.clone());
            CartEntity {
                context,
                items: Vec::new(),
            }
        },
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route.clone())
            .unwrap();
    let entity = region.entity_ref("cart-42");

    entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap();
    entity
        .tell(&region, CartCommand::Add("banana".to_string()))
        .unwrap();
    let reply = entity
        .ask(&region, CartCommand::Get, Duration::from_millis(250))
        .await
        .unwrap();

    assert_eq!(reply, "cart-42:apple,banana");
    assert_eq!(route.entity_count(), 1);
    assert!(route.entity_actor(entity.entity_id()).is_some());
    let created = created.lock().expect("created mutex poisoned");
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].entity_id(), &EntityId::new("cart-42"));
    assert_eq!(created[0].entity_type(), &EntityType::new("Cart"));
    assert!(created[0].actor_name().contains("cart-42"));
}

#[tokio::test]
async fn local_entity_route_recreates_entity_after_self_passivation() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let created = Arc::new(Mutex::new(Vec::new()));
    let created_for_factory = created.clone();
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-self-passivation-test"),
        move |context: LocalEntityContext| {
            created_for_factory
                .lock()
                .expect("created mutex poisoned")
                .push(context.clone());
            CartEntity {
                context,
                items: Vec::new(),
            }
        },
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route.clone())
            .unwrap();
    let entity = region.entity_ref("cart-42");

    entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap();
    let reply = entity
        .ask(&region, CartCommand::Get, Duration::from_millis(250))
        .await
        .unwrap();
    assert_eq!(reply, "cart-42:apple");

    entity.tell(&region, CartCommand::Passivate).unwrap();
    wait_for_entity_count(&route, 0).await;
    assert!(route.entity_actor(entity.entity_id()).is_none());

    entity
        .tell(&region, CartCommand::Add("banana".to_string()))
        .unwrap();
    let reply = entity
        .ask(&region, CartCommand::Get, Duration::from_millis(250))
        .await
        .unwrap();

    assert_eq!(reply, "cart-42:banana");
    assert_eq!(route.entity_count(), 1);
    let created = created.lock().expect("created mutex poisoned");
    assert_eq!(created.len(), 2);
    assert_eq!(created[0].entity_id(), created[1].entity_id());
    assert_eq!(created[0].shard_id(), created[1].shard_id());
}

#[tokio::test]
async fn local_entity_route_passivates_one_entity_without_affecting_other_shards() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-explicit-passivation-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region = ShardRegion::from_snapshot(
        entity_type.clone(),
        config.clone(),
        &coordinator.snapshot(),
        route.clone(),
    )
    .unwrap();
    let first = region.entity_ref("cart-42");
    let second =
        entity_ref_in_different_shard(&entity_type, &config, first.shard_id(region.config()));

    first
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap();
    second
        .tell(&region, CartCommand::Add("pear".to_string()))
        .unwrap();
    assert_eq!(route.entity_count(), 2);

    assert!(route.passivate_entity(first.entity_id()));
    assert_eq!(route.entity_count(), 1);
    let second_reply = second
        .ask(&region, CartCommand::Get, Duration::from_millis(250))
        .await
        .unwrap();
    assert_eq!(
        second_reply,
        format!("{}:pear", second.entity_id().as_str())
    );

    first
        .tell(&region, CartCommand::Add("banana".to_string()))
        .unwrap();
    let first_reply = first
        .ask(&region, CartCommand::Get, Duration::from_millis(250))
        .await
        .unwrap();

    assert_eq!(first_reply, "cart-42:banana");
    assert_eq!(route.entity_count(), 2);
}

#[tokio::test]
async fn local_entity_route_idle_timeout_passivates_and_recreates_entity() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-idle-passivation-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    )
    .with_idle_passivation(Duration::from_millis(20));
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route.clone())
            .unwrap();
    let entity = region.entity_ref("cart-42");

    entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap();
    assert_eq!(route.entity_count(), 1);

    wait_for_entity_count(&route, 0).await;

    entity
        .tell(&region, CartCommand::Add("banana".to_string()))
        .unwrap();
    let reply = entity
        .ask(&region, CartCommand::Get, Duration::from_millis(250))
        .await
        .unwrap();

    assert_eq!(reply, "cart-42:banana");
    assert_eq!(route.entity_count(), 1);
}

#[tokio::test]
async fn local_entity_route_rejects_new_delivery_while_shard_is_draining() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-shard-draining-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route.clone())
            .unwrap();
    let entity = region.entity_ref("cart-42");
    let shard_id = entity.shard_id(region.config());

    assert_eq!(route.mark_shard_draining(shard_id), 0);
    let error = entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap_err();

    assert!(matches!(
        &error,
        EntityTellError::Delivery {
            failure: EntityDeliveryFailure::ShardHandoff {
                shard_id: failed_shard,
                state: ShardHandoffState::Draining,
            },
            ..
        } if *failed_shard == shard_id
    ));
    assert!(matches!(
        error.into_message(),
        CartCommand::Add(value) if value == "apple"
    ));
    assert_eq!(route.entity_count(), 0);

    assert_eq!(route.mark_shard_acquired(shard_id), 0);
    entity
        .tell(&region, CartCommand::Add("banana".to_string()))
        .unwrap();
    assert_eq!(route.entity_count(), 1);
}

#[test]
fn local_entity_route_rejects_remote_owners_without_spawning() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let local_node_id = NodeId::new("rakka-0", "uid-a");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-sharding-remote-owner-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route.clone())
            .unwrap();
    let remote_entity =
        EntityRef::<CartCommand>::new(EntityType::new("Cart"), remote_entity_id.clone());

    let error = remote_entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap_err();

    assert!(matches!(
        &error,
        EntityTellError::Delivery {
            failure: EntityDeliveryFailure::NotLocal { owner },
            ..
        } if owner.logical_id() == "rakka-1"
    ));
    assert!(matches!(
        error.into_message(),
        CartCommand::Add(value) if value == "apple"
    ));
    assert_eq!(route.entity_count(), 0);
}

#[test]
fn local_entity_route_returns_message_when_actor_spawn_fails() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("local-sharding-spawn-fail-test"),
        |context: LocalEntityContext| CartEntity {
            context,
            items: Vec::new(),
        },
    )
    .with_actor_options(rakka_core::ActorOptions::default().with_mailbox_capacity(0));
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route).unwrap();
    let entity = region.entity_ref("cart-42");

    let error = entity
        .tell(&region, CartCommand::Add("apple".to_string()))
        .unwrap_err();

    assert!(matches!(
        &error,
        EntityTellError::Delivery {
            failure: EntityDeliveryFailure::SpawnFailed(_),
            ..
        }
    ));
    assert!(matches!(
        error.into_message(),
        CartCommand::Add(value) if value == "apple"
    ));
}

#[test]
fn remote_entity_route_sends_remote_envelope_when_local_route_reports_not_local() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let local_node_id = NodeId::new("rakka-0", "uid-a");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let local_route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("remote-aware-route-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let outbound = RecordingRemoteOutbound::default();
    let registry = remote_registry();
    let route = RemoteEntityRoute::new(local_route.clone(), registry.clone(), outbound.clone())
        .with_source("rakka-0#uid-a");
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route).unwrap();
    let remote_entity =
        EntityRef::<RemoteCartCommand>::new(EntityType::new("Cart"), remote_entity_id.clone());

    remote_entity
        .tell(
            &region,
            RemoteCartCommand {
                action: "add-apple".to_string(),
            },
        )
        .unwrap();

    assert_eq!(local_route.entity_count(), 0);
    let sent = outbound.sent();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0.logical_id(), "rakka-1");
    assert_eq!(sent[0].1.source.as_deref(), Some("rakka-0#uid-a"));
    assert_eq!(
        sent[0].1.destination,
        RemoteDestination::Entity {
            entity_type: "Cart".to_string(),
            entity_id: remote_entity_id.as_str().to_string(),
        }
    );
    let decoded: RemoteCartCommand = registry.decode_envelope(&sent[0].1).unwrap();
    assert_eq!(decoded.action, "add-apple");
}

#[tokio::test]
async fn remote_entity_route_keeps_local_ownership_on_local_route() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let local_route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("remote-aware-local-owner-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let outbound = RecordingRemoteOutbound::default();
    let route = RemoteEntityRoute::new(local_route.clone(), remote_registry(), outbound.clone());
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route).unwrap();
    let entity = region.entity_ref("cart-42");

    entity
        .tell(
            &region,
            RemoteCartCommand {
                action: "local-add".to_string(),
            },
        )
        .unwrap();

    assert_eq!(local_route.entity_count(), 1);
    assert!(outbound.sent().is_empty());
}

#[test]
fn remote_entity_route_returns_message_when_codec_is_missing() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let local_node_id = NodeId::new("rakka-0", "uid-a");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let local_route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("remote-aware-missing-codec-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let outbound = RecordingRemoteOutbound::default();
    let route = RemoteEntityRoute::new(local_route, SerializationRegistry::new(), outbound.clone());
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route).unwrap();
    let remote_entity =
        EntityRef::<RemoteCartCommand>::new(EntityType::new("Cart"), remote_entity_id);

    let error = remote_entity
        .tell(
            &region,
            RemoteCartCommand {
                action: "add-apple".to_string(),
            },
        )
        .unwrap_err();

    assert!(matches!(
        &error,
        EntityTellError::Delivery {
            failure: EntityDeliveryFailure::RemoteEncode(_),
            ..
        }
    ));
    assert!(matches!(
        error.into_message(),
        RemoteCartCommand { action } if action == "add-apple"
    ));
    assert!(outbound.sent().is_empty());
}

#[test]
fn remote_entity_route_returns_message_when_outbound_rejects() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let local_node_id = NodeId::new("rakka-0", "uid-a");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let local_route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("remote-aware-outbound-fail-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let route = RemoteEntityRoute::new(local_route, remote_registry(), FailingRemoteOutbound);
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), route).unwrap();
    let remote_entity =
        EntityRef::<RemoteCartCommand>::new(EntityType::new("Cart"), remote_entity_id);

    let error = remote_entity
        .tell(
            &region,
            RemoteCartCommand {
                action: "add-apple".to_string(),
            },
        )
        .unwrap_err();

    assert!(matches!(
        &error,
        EntityTellError::Delivery {
            failure: EntityDeliveryFailure::RemoteSend(_),
            ..
        }
    ));
    assert!(matches!(
        error.into_message(),
        RemoteCartCommand { action } if action == "add-apple"
    ));
}

#[tokio::test]
async fn in_memory_transport_routes_remote_entity_to_owning_node() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let node_a = NodeId::new("rakka-0", "uid-a");
    let node_b = NodeId::new("rakka-1", "uid-b");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let snapshot = coordinator.snapshot();
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let registry = remote_registry();
    let transport = InMemoryRemoteTransport::new();

    let local_route_a = LocalEntityRoute::new(
        node_a,
        ActorSystem::new("remote-transport-node-a-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let outbound = RemoteTransportEntityOutbound::new(transport.clone());
    let remote_route_a = RemoteEntityRoute::new(local_route_a.clone(), registry.clone(), outbound)
        .with_source("rakka-0#uid-a");
    let region_a = ShardRegion::from_snapshot(
        entity_type.clone(),
        config.clone(),
        &snapshot,
        remote_route_a,
    )
    .unwrap();

    let (delivered, mut received) = tokio::sync::mpsc::unbounded_channel();
    let local_route_b = LocalEntityRoute::new(
        node_b.clone(),
        ActorSystem::new("remote-transport-node-b-test"),
        move |context: LocalEntityContext| NotifyingRemoteCartEntity {
            context,
            delivered: delivered.clone(),
        },
    );
    let region_b = ShardRegion::from_snapshot(
        entity_type.clone(),
        config,
        &snapshot,
        local_route_b.clone(),
    )
    .unwrap();
    let endpoint_b = RemoteEndpoint::new(node_b);
    endpoint_b
        .register_entity_handler("Cart", RemoteEntityInbound::new(region_b, registry.clone()))
        .unwrap();
    transport.register_endpoint(endpoint_b).unwrap();
    let remote_entity = EntityRef::<RemoteCartCommand>::new(entity_type, remote_entity_id.clone());

    remote_entity
        .tell(
            &region_a,
            RemoteCartCommand {
                action: "add-apple".to_string(),
            },
        )
        .unwrap();

    assert_eq!(local_route_a.entity_count(), 0);
    assert_eq!(local_route_b.entity_count(), 1);
    let delivered = tokio::time::timeout(Duration::from_secs(1), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivered,
        (
            remote_entity_id.as_str().to_string(),
            "add-apple".to_string()
        )
    );
}

#[tokio::test]
async fn remote_entity_ask_routes_reply_back_to_requesting_node() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let node_a = NodeId::new("rakka-0", "uid-a");
    let node_b = NodeId::new("rakka-1", "uid-b");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let snapshot = coordinator.snapshot();
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let registry = remote_registry();
    let transport = InMemoryRemoteTransport::new();

    let local_route_a = LocalEntityRoute::new(
        node_a.clone(),
        ActorSystem::new("remote-ask-node-a-test"),
        |context: LocalEntityContext| RemoteAskCartEntity { context },
    );
    let region_a = ShardRegion::from_snapshot(
        entity_type.clone(),
        config.clone(),
        &snapshot,
        local_route_a.clone(),
    )
    .unwrap();
    let request_registry = RemoteRequestRegistry::new(registry.clone());
    let ask_client =
        RemoteEntityAskClient::new(node_a.clone(), request_registry.clone(), transport.clone());
    let endpoint_a = RemoteEndpoint::new(node_a);
    endpoint_a.register_reply_handler(request_registry.clone());
    transport.register_endpoint(endpoint_a).unwrap();

    let local_route_b = LocalEntityRoute::new(
        node_b.clone(),
        ActorSystem::new("remote-ask-node-b-test"),
        |context: LocalEntityContext| RemoteAskCartEntity { context },
    );
    let region_b = ShardRegion::from_snapshot(
        entity_type.clone(),
        config,
        &snapshot,
        local_route_b.clone(),
    )
    .unwrap();
    let endpoint_b = RemoteEndpoint::new(node_b);
    endpoint_b
        .register_entity_handler(
            "Cart",
            RemoteEntityAskInbound::new(
                NodeId::new("rakka-1", "uid-b"),
                region_b,
                registry.clone(),
                transport.clone(),
                |request: RemoteCartGet, reply_to| RemoteAskCartCommand::Get {
                    prefix: request.prefix,
                    reply_to,
                },
            ),
        )
        .unwrap();
    transport.register_endpoint(endpoint_b).unwrap();
    let remote_entity =
        EntityRef::<RemoteAskCartCommand>::new(entity_type, remote_entity_id.clone());

    let reply: RemoteCartReply = ask_client
        .ask(
            &region_a,
            &remote_entity,
            RemoteCartGet {
                prefix: "items".to_string(),
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();

    assert_eq!(
        reply.summary,
        format!("{}:items", remote_entity_id.as_str())
    );
    assert_eq!(local_route_a.entity_count(), 0);
    assert_eq!(local_route_b.entity_count(), 1);
    assert_eq!(request_registry.pending_count(), 0);
}

#[tokio::test]
async fn remote_entity_ask_timeout_removes_pending_request() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let node_a = NodeId::new("rakka-0", "uid-a");
    let node_b = NodeId::new("rakka-1", "uid-b");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let snapshot = coordinator.snapshot();
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let registry = remote_registry();
    let transport = InMemoryRemoteTransport::new();
    let local_route_a = LocalEntityRoute::new(
        node_a.clone(),
        ActorSystem::new("remote-ask-timeout-node-a-test"),
        |_context: LocalEntityContext| SilentRemoteAskCartEntity,
    );
    let region_a = ShardRegion::from_snapshot(
        entity_type.clone(),
        config.clone(),
        &snapshot,
        local_route_a.clone(),
    )
    .unwrap();
    let request_registry = RemoteRequestRegistry::new(registry.clone());
    let ask_client =
        RemoteEntityAskClient::new(node_a.clone(), request_registry.clone(), transport.clone());
    let endpoint_a = RemoteEndpoint::new(node_a);
    endpoint_a.register_reply_handler(request_registry.clone());
    transport.register_endpoint(endpoint_a).unwrap();

    let local_route_b = LocalEntityRoute::new(
        node_b.clone(),
        ActorSystem::new("remote-ask-timeout-node-b-test"),
        |_context: LocalEntityContext| SilentRemoteAskCartEntity,
    );
    let region_b =
        ShardRegion::from_snapshot(entity_type.clone(), config, &snapshot, local_route_b).unwrap();
    let endpoint_b = RemoteEndpoint::new(node_b.clone());
    endpoint_b
        .register_entity_handler(
            "Cart",
            RemoteEntityAskInbound::new(
                node_b,
                region_b,
                registry.clone(),
                transport.clone(),
                |request: RemoteCartGet, reply_to| RemoteAskCartCommand::Get {
                    prefix: request.prefix,
                    reply_to,
                },
            ),
        )
        .unwrap();
    transport.register_endpoint(endpoint_b).unwrap();
    let remote_entity =
        EntityRef::<RemoteAskCartCommand>::new(entity_type, remote_entity_id.clone());

    let error = ask_client
        .ask::<RemoteCartGet, RemoteAskCartCommand, RemoteCartReply>(
            &region_a,
            &remote_entity,
            RemoteCartGet {
                prefix: "items".to_string(),
            },
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RemoteEntityAskError::Reply {
            error: rakka_remote::RemoteRequestError::Timeout
        }
    ));
    assert_eq!(request_registry.pending_count(), 0);
    assert_eq!(local_route_a.entity_count(), 0);
}

#[tokio::test]
async fn remote_entity_inbound_decodes_and_delivers_to_local_entity_route() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let local_node_id = membership.local_node_id().clone();
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let (delivered, mut received) = tokio::sync::mpsc::unbounded_channel();
    let local_route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("remote-inbound-local-route-test"),
        move |context: LocalEntityContext| NotifyingRemoteCartEntity {
            context,
            delivered: delivered.clone(),
        },
    );
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        local_route.clone(),
    )
    .unwrap();
    let registry = remote_registry();
    let inbound = RemoteEntityInbound::new(region, registry.clone());
    let envelope = remote_cart_envelope(&registry, "Cart", "cart-42", "add-apple");

    inbound.handle(envelope).unwrap();

    assert_eq!(local_route.entity_count(), 1);
    let delivered = tokio::time::timeout(Duration::from_secs(1), received.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivered, ("cart-42".to_string(), "add-apple".to_string()));
}

#[test]
fn remote_entity_inbound_rejects_non_entity_destination() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let local_route = LocalEntityRoute::new(
        membership.local_node_id().clone(),
        ActorSystem::new("remote-inbound-destination-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), local_route)
            .unwrap();
    let registry = remote_registry();
    let encoded = registry
        .encode(&RemoteCartCommand {
            action: "add-apple".to_string(),
        })
        .unwrap();
    let envelope = RemoteEnvelope::new(
        RemoteDestination::Service {
            service_key: "cart-service".to_string(),
        },
        encoded,
    );
    let inbound = RemoteEntityInbound::new(region, registry);

    let error = inbound.handle(envelope).unwrap_err();

    assert!(matches!(
        error,
        RemoteEntityInboundError::UnexpectedDestination {
            destination: RemoteDestination::Service { service_key },
        } if service_key == "cart-service"
    ));
}

#[test]
fn remote_entity_inbound_rejects_wrong_entity_type_before_decode() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let local_route = LocalEntityRoute::new(
        membership.local_node_id().clone(),
        ActorSystem::new("remote-inbound-type-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let region =
        ShardRegion::from_snapshot(entity_type, config, &coordinator.snapshot(), local_route)
            .unwrap();
    let envelope = remote_cart_envelope(&remote_registry(), "Order", "cart-42", "add-apple");
    let inbound = RemoteEntityInbound::new(region, SerializationRegistry::new());

    let error = inbound.handle(envelope).unwrap_err();

    assert!(matches!(
        error,
        RemoteEntityInboundError::EntityTypeMismatch { expected, actual }
            if expected == EntityType::new("Cart") && actual == EntityType::new("Order")
    ));
}

#[test]
fn remote_entity_inbound_reports_decode_failure_without_delivery() {
    let membership = membership_with_up_nodes(vec![node("rakka-0", "uid-a")]);
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(4).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let local_route = LocalEntityRoute::new(
        membership.local_node_id().clone(),
        ActorSystem::new("remote-inbound-decode-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        local_route.clone(),
    )
    .unwrap();
    let registry = remote_registry();
    let mut envelope = remote_cart_envelope(&registry, "Cart", "cart-42", "add-apple");
    envelope.payload = vec![0xff];
    let inbound = RemoteEntityInbound::new(region, registry);

    let error = inbound.handle(envelope).unwrap_err();

    assert!(matches!(error, RemoteEntityInboundError::Decode { .. }));
    assert_eq!(local_route.entity_count(), 0);
}

#[test]
fn remote_entity_inbound_preserves_decoded_message_when_local_delivery_fails() {
    let membership =
        membership_with_up_nodes(vec![node("rakka-0", "uid-a"), node("rakka-1", "uid-b")]);
    let local_node_id = NodeId::new("rakka-0", "uid-a");
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).unwrap();
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), config.clone());
    coordinator.reconcile(&membership);
    let remote_entity_id = entity_owned_by(&coordinator, "rakka-1");
    let local_route = LocalEntityRoute::new(
        local_node_id,
        ActorSystem::new("remote-inbound-delivery-fail-test"),
        |_context: LocalEntityContext| RemoteCartEntity,
    );
    let region = ShardRegion::from_snapshot(
        entity_type,
        config,
        &coordinator.snapshot(),
        local_route.clone(),
    )
    .unwrap();
    let registry = remote_registry();
    let envelope = remote_cart_envelope(&registry, "Cart", remote_entity_id.as_str(), "add-apple");
    let inbound = RemoteEntityInbound::new(region, registry);

    let error = inbound.handle(envelope).unwrap_err();

    match error {
        RemoteEntityInboundError::Delivery {
            error:
                EntityTellError::Delivery {
                    message,
                    failure: EntityDeliveryFailure::NotLocal { owner },
                },
        } => {
            assert_eq!(owner.logical_id(), "rakka-1");
            assert_eq!(message.action, "add-apple");
        }
        _other => panic!("unexpected inbound delivery error"),
    }
    assert_eq!(local_route.entity_count(), 0);
}
