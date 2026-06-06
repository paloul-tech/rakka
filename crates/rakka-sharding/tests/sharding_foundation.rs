//! Integration tests for sharding identity and coordinator foundations.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, ReplyTo,
};
use rakka_remote::{
    InMemoryRemoteTransport, RemoteDestination, RemoteEndpoint, RemoteEnvelope,
    SerializationRegistry,
};
use rakka_sharding::{
    EntityAskError, EntityDeliveryFailure, EntityId, EntityRef, EntityTellError, EntityType,
    LocalEntityContext, LocalEntityRoute, RemoteEntityInbound, RemoteEntityInboundError,
    RemoteEntityOutbound, RemoteEntityRoute, RemoteEntitySendFailure,
    RemoteTransportEntityOutbound, RoutedEntityMessage, ShardCoordinator, ShardDecision, ShardId,
    ShardMoveReason, ShardOwnerCache, ShardRegion, ShardingConfig, ShardingError,
};

#[derive(Debug)]
enum CartCommand {
    Add(String),
    Get(ReplyTo<String>),
}

struct CartEntity {
    context: LocalEntityContext,
    items: Vec<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RemoteCartCommand {
    #[prost(string, tag = "1")]
    action: String,
}

struct RemoteCartEntity;

struct NotifyingRemoteCartEntity {
    context: LocalEntityContext,
    delivered: tokio::sync::mpsc::UnboundedSender<(String, String)>,
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

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), 2552),
    )
}

fn membership_with_up_nodes(nodes: Vec<ClusterNode>) -> ClusterMembership {
    let local = nodes[0].clone();
    let mut membership = ClusterMembership::new(
        local,
        MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100)),
    );

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
