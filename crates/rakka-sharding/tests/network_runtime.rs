//! Networked cluster node runtime integration tests.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use rakka_cluster::{ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, ReplyTo,
};
use rakka_remote::{
    SerializationRegistry, TcpRemoteConnectionLifecycle, TcpRemoteTransportConfig,
    TcpRemoteTransportError,
};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeBuilder, ClusterNodeRuntimeError, ClusterSharding,
    Entity, EntityContext, EntityDeliveryFailure, EntityId, EntityRef, EntityTellError, EntityType,
    EntityTypeKey, LocalEntityContext, LocalEntityRoute, RoutedEntityMessage, ShardHandoffState,
    ShardMoveReason, ShardRegion, ShardingConfig,
};

#[derive(Clone, PartialEq, prost::Message)]
struct CartCommand {
    #[prost(string, tag = "1")]
    action: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CartGet {
    #[prost(string, tag = "1")]
    prefix: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CartReply {
    #[prost(string, tag = "1")]
    summary: String,
}

enum CartAskCommand {
    Get {
        prefix: String,
        reply_to: ReplyTo<CartReply>,
    },
}

struct NotifyingCartEntity {
    context: LocalEntityContext,
    delivered: tokio::sync::mpsc::UnboundedSender<(String, String)>,
}

struct FacadeNotifyingCartEntity {
    context: EntityContext<CartCommand>,
    delivered: tokio::sync::mpsc::UnboundedSender<(String, String)>,
}

struct AskCartEntity {
    context: LocalEntityContext,
}

struct FacadeAskCartEntity {
    context: EntityContext<CartAskCommand>,
}

impl Actor for NotifyingCartEntity {
    type Msg = CartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entity_id = self.context.entity_id().as_str().to_string();
        let delivered = self.delivered.clone();
        actor_future(async move {
            let _ = delivered.send((entity_id, msg.action));
            Ok(ActorAction::Continue)
        })
    }
}

impl Actor for FacadeNotifyingCartEntity {
    type Msg = CartCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entity_id = self.context.entity_id().as_str().to_string();
        let delivered = self.delivered.clone();
        actor_future(async move {
            let _ = delivered.send((entity_id, msg.action));
            Ok(ActorAction::Continue)
        })
    }
}

impl Actor for AskCartEntity {
    type Msg = CartAskCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entity_id = self.context.entity_id().as_str().to_string();
        actor_future(async move {
            match msg {
                CartAskCommand::Get { prefix, reply_to } => {
                    let _ = reply_to.reply(CartReply {
                        summary: format!("{entity_id}:{prefix}"),
                    });
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

impl Actor for FacadeAskCartEntity {
    type Msg = CartAskCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let entity_id = self.context.entity_id().as_str().to_string();
        actor_future(async move {
            match msg {
                CartAskCommand::Get { prefix, reply_to } => {
                    let _ = reply_to.reply(CartReply {
                        summary: format!("{entity_id}:{prefix}"),
                    });
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::test]
async fn networked_runtime_routes_remote_tell_over_tcp() {
    let registry = cart_registry();
    let Some((mut node_a, mut node_b)) = build_runtime_pair(registry.clone()).await else {
        return;
    };
    let entity_type = EntityType::new("Cart");
    let config = ShardingConfig::new(8).expect("valid sharding config");
    let system_a = ActorSystem::new("networked-tell-node-a");
    let system_b = ActorSystem::new("networked-tell-node-b");
    let (delivered_a, _received_a) = tokio::sync::mpsc::unbounded_channel();
    let (delivered_b, mut received_b) = tokio::sync::mpsc::unbounded_channel();
    let local_route_a = LocalEntityRoute::new(
        node_a.local_node().id().clone(),
        system_a.clone(),
        move |context: LocalEntityContext| NotifyingCartEntity {
            context,
            delivered: delivered_a.clone(),
        },
    );
    let region_a = ShardRegion::new(
        entity_type.clone(),
        config.clone(),
        node_a.remote_route(local_route_a),
    );
    let local_route_b = LocalEntityRoute::new(
        node_b.local_node().id().clone(),
        system_b.clone(),
        move |context: LocalEntityContext| NotifyingCartEntity {
            context,
            delivered: delivered_b.clone(),
        },
    );
    let region_b = ShardRegion::new(entity_type.clone(), config, local_route_b);

    node_a
        .register_entity_region(region_a.clone())
        .expect("node a region should register");
    node_b
        .register_entity_region(region_b)
        .expect("node b region should register");
    let update_a = apply_pair_discovery(&mut node_a, &mut node_b);
    assert_eq!(update_a.registered_peers(), 1);
    assert_eq!(node_a.registered_peer_count(), 1);

    let entity_id = entity_owned_by(&node_a, &entity_type, node_b.local_node().id().logical_id());
    let entity = EntityRef::<CartCommand>::new(entity_type, entity_id.clone());
    entity
        .tell(
            &region_a,
            CartCommand {
                action: "add-apple".to_string(),
            },
        )
        .expect("remote tell should enqueue");

    let delivered = tokio::time::timeout(Duration::from_secs(1), received_b.recv())
        .await
        .expect("remote tell should arrive")
        .expect("delivery channel should remain open");
    assert_eq!(
        delivered,
        (entity_id.as_str().to_string(), "add-apple".to_string())
    );
    wait_for(|| node_b.transport_snapshot().inbound_envelopes() >= 1).await;

    system_a.shutdown();
    system_b.shutdown();
}

#[tokio::test]
async fn networked_runtime_routes_remote_ask_reply_over_tcp() {
    let registry = cart_registry();
    let Some((mut node_a, mut node_b)) = build_runtime_pair(registry).await else {
        return;
    };
    let entity_type = EntityType::new("CartAsk");
    let config = ShardingConfig::new(8).expect("valid sharding config");
    let region_a = ShardRegion::new(
        entity_type.clone(),
        config.clone(),
        |_message: RoutedEntityMessage<CartAskCommand>| Ok(()),
    );
    let system_b = ActorSystem::new("networked-ask-node-b");
    let route_b = LocalEntityRoute::new(
        node_b.local_node().id().clone(),
        system_b.clone(),
        |context: LocalEntityContext| AskCartEntity { context },
    );
    let region_b = ShardRegion::new(entity_type.clone(), config, route_b);

    node_a
        .register_region(region_a.clone())
        .expect("node a owner cache region should register");
    node_b
        .register_entity_ask_region::<CartGet, CartAskCommand, CartReply, _>(
            region_b,
            |request, reply_to| CartAskCommand::Get {
                prefix: request.prefix,
                reply_to,
            },
        )
        .expect("node b ask region should register");
    apply_pair_discovery(&mut node_a, &mut node_b);

    let entity_id = entity_owned_by(&node_a, &entity_type, node_b.local_node().id().logical_id());
    let entity = EntityRef::<CartAskCommand>::new(entity_type, entity_id.clone());
    let reply: CartReply = node_a
        .ask_client()
        .ask(
            &region_a,
            &entity,
            CartGet {
                prefix: "total".to_string(),
            },
            Duration::from_secs(1),
        )
        .await
        .expect("remote ask should receive reply");

    assert_eq!(reply.summary, format!("{}:total", entity_id.as_str()));
    assert_eq!(node_a.requests().pending_count(), 0);
    wait_for(|| node_a.transport_snapshot().inbound_envelopes() >= 1).await;

    system_b.shutdown();
}

#[tokio::test]
async fn networked_facade_routes_remote_tell_over_tcp() {
    let registry = cart_registry();
    let Some((mut node_a, mut node_b)) = build_runtime_pair(registry).await else {
        return;
    };
    let entity_type = EntityType::new("FacadeCart");
    let key = EntityTypeKey::<CartCommand>::new(entity_type.as_str())
        .with_number_of_shards(8)
        .expect("valid sharding config");
    let system_a = ActorSystem::new("networked-facade-tell-node-a");
    let system_b = ActorSystem::new("networked-facade-tell-node-b");
    let sharding_a =
        ClusterSharding::for_node_runtime(&system_a, &node_a).expect("facade should initialize");
    let sharding_b =
        ClusterSharding::for_node_runtime(&system_b, &node_b).expect("facade should initialize");
    let (delivered_a, _received_a) = tokio::sync::mpsc::unbounded_channel();
    let (delivered_b, mut received_b) = tokio::sync::mpsc::unbounded_channel();
    let registration_a = sharding_a
        .init_remote(
            &mut node_a,
            Entity::of(key.clone(), move |context: EntityContext<CartCommand>| {
                FacadeNotifyingCartEntity {
                    context,
                    delivered: delivered_a.clone(),
                }
            }),
        )
        .expect("node a facade region should register");
    let registration_b = sharding_b
        .init_remote(
            &mut node_b,
            Entity::of(key.clone(), move |context: EntityContext<CartCommand>| {
                FacadeNotifyingCartEntity {
                    context,
                    delivered: delivered_b.clone(),
                }
            }),
        )
        .expect("node b facade region should register");

    apply_pair_discovery(&mut node_a, &mut node_b);
    let entity_id = entity_owned_by(&node_a, &entity_type, node_b.local_node().id().logical_id());
    let entity = sharding_a
        .entity_ref_for(&key, entity_id.as_str())
        .expect("facade entity ref should resolve");

    entity
        .tell(CartCommand {
            action: "add-apple".to_string(),
        })
        .expect("facade remote tell should enqueue");

    let delivered = tokio::time::timeout(Duration::from_secs(1), received_b.recv())
        .await
        .expect("facade remote tell should arrive")
        .expect("delivery channel should remain open");
    assert_eq!(
        delivered,
        (entity_id.as_str().to_string(), "add-apple".to_string())
    );
    assert_eq!(
        sharding_a
            .registration_state(registration_a.key())
            .expect("node a state")
            .local_entity_count(),
        0
    );
    assert_eq!(
        sharding_b
            .registration_state(registration_b.key())
            .expect("node b state")
            .local_entity_count(),
        1
    );
    wait_for(|| node_b.transport_snapshot().inbound_envelopes() >= 1).await;

    system_a.shutdown();
    system_b.shutdown();
}

#[tokio::test]
async fn networked_facade_routes_remote_ask_reply_over_tcp() {
    let registry = cart_registry();
    let Some((mut node_a, mut node_b)) = build_runtime_pair(registry).await else {
        return;
    };
    let entity_type = EntityType::new("FacadeCartAsk");
    let key = EntityTypeKey::<CartAskCommand>::new(entity_type.as_str())
        .with_number_of_shards(8)
        .expect("valid sharding config");
    let system_a = ActorSystem::new("networked-facade-ask-node-a");
    let system_b = ActorSystem::new("networked-facade-ask-node-b");
    let sharding_a =
        ClusterSharding::for_node_runtime(&system_a, &node_a).expect("facade should initialize");
    let sharding_b =
        ClusterSharding::for_node_runtime(&system_b, &node_b).expect("facade should initialize");

    sharding_a
        .init_remote_with_ask(
            &mut node_a,
            Entity::of(key.clone(), |context: EntityContext<CartAskCommand>| {
                FacadeAskCartEntity { context }
            }),
            |request: CartGet, reply_to| CartAskCommand::Get {
                prefix: request.prefix,
                reply_to,
            },
        )
        .expect("node a facade ask region should register");
    sharding_b
        .init_remote_with_ask(
            &mut node_b,
            Entity::of(key.clone(), |context: EntityContext<CartAskCommand>| {
                FacadeAskCartEntity { context }
            }),
            |request: CartGet, reply_to| CartAskCommand::Get {
                prefix: request.prefix,
                reply_to,
            },
        )
        .expect("node b facade ask region should register");
    apply_pair_discovery(&mut node_a, &mut node_b);

    let entity_id = entity_owned_by(&node_a, &entity_type, node_b.local_node().id().logical_id());
    let entity = sharding_a
        .entity_ref_for(&key, entity_id.as_str())
        .expect("facade ask entity ref should resolve");
    let reply: CartReply = entity
        .remote_ask(
            &node_a.ask_client(),
            CartGet {
                prefix: "total".to_string(),
            },
            Duration::from_secs(1),
        )
        .await
        .expect("facade remote ask should receive reply");

    assert_eq!(reply.summary, format!("{}:total", entity_id.as_str()));
    assert_eq!(node_a.requests().pending_count(), 0);
    wait_for(|| node_a.transport_snapshot().inbound_envelopes() >= 1).await;

    system_a.shutdown();
    system_b.shutdown();
}

#[tokio::test]
async fn networked_facade_reports_missing_serializer_without_losing_message() {
    let Some((mut node_a, mut node_b)) = build_runtime_pair(SerializationRegistry::new()).await
    else {
        return;
    };
    let entity_type = EntityType::new("FacadeCartMissingCodec");
    let key = EntityTypeKey::<CartCommand>::new(entity_type.as_str())
        .with_number_of_shards(8)
        .expect("valid sharding config");
    let system_a = ActorSystem::new("networked-facade-missing-codec-node-a");
    let system_b = ActorSystem::new("networked-facade-missing-codec-node-b");
    let sharding_a =
        ClusterSharding::for_node_runtime(&system_a, &node_a).expect("facade should initialize");
    let sharding_b =
        ClusterSharding::for_node_runtime(&system_b, &node_b).expect("facade should initialize");
    let (delivered_a, _received_a) = tokio::sync::mpsc::unbounded_channel();
    let (delivered_b, _received_b) = tokio::sync::mpsc::unbounded_channel();

    sharding_a
        .init_remote(
            &mut node_a,
            Entity::of(key.clone(), move |context: EntityContext<CartCommand>| {
                FacadeNotifyingCartEntity {
                    context,
                    delivered: delivered_a.clone(),
                }
            }),
        )
        .expect("node a facade region should register");
    sharding_b
        .init_remote(
            &mut node_b,
            Entity::of(key.clone(), move |context: EntityContext<CartCommand>| {
                FacadeNotifyingCartEntity {
                    context,
                    delivered: delivered_b.clone(),
                }
            }),
        )
        .expect("node b facade region should register");
    apply_pair_discovery(&mut node_a, &mut node_b);

    let entity_id = entity_owned_by(&node_a, &entity_type, node_b.local_node().id().logical_id());
    let entity = sharding_a
        .entity_ref_for(&key, entity_id.as_str())
        .expect("facade entity ref should resolve");
    let error = entity
        .tell(CartCommand {
            action: "add-apple".to_string(),
        })
        .expect_err("missing codec should fail before remote send");

    match error {
        EntityTellError::Delivery {
            message,
            failure: EntityDeliveryFailure::RemoteEncode(_),
        } => assert_eq!(message.action, "add-apple"),
        _other => panic!("unexpected tell error"),
    }

    system_a.shutdown();
    system_b.shutdown();
}

#[tokio::test]
async fn networked_facade_refreshes_ownership_after_leaving_handoff() {
    let registry = cart_registry();
    let Some((mut node_a, mut node_b)) = build_runtime_pair(registry).await else {
        return;
    };
    let node_b_id = node_b.local_node().id().clone();
    let entity_type = EntityType::new("FacadeCartLeave");
    let key = EntityTypeKey::<CartCommand>::new(entity_type.as_str())
        .with_number_of_shards(8)
        .expect("valid sharding config");
    let system_a = ActorSystem::new("networked-facade-leave-node-a");
    let system_b = ActorSystem::new("networked-facade-leave-node-b");
    let sharding_a =
        ClusterSharding::for_node_runtime(&system_a, &node_a).expect("facade should initialize");
    let sharding_b =
        ClusterSharding::for_node_runtime(&system_b, &node_b).expect("facade should initialize");
    let (delivered_a, mut received_a) = tokio::sync::mpsc::unbounded_channel();
    let (delivered_b, mut received_b) = tokio::sync::mpsc::unbounded_channel();
    let registration_a = sharding_a
        .init_remote(
            &mut node_a,
            Entity::of(key.clone(), move |context: EntityContext<CartCommand>| {
                FacadeNotifyingCartEntity {
                    context,
                    delivered: delivered_a.clone(),
                }
            }),
        )
        .expect("node a facade region should register");
    let registration_b = sharding_b
        .init_remote(
            &mut node_b,
            Entity::of(key.clone(), move |context: EntityContext<CartCommand>| {
                FacadeNotifyingCartEntity {
                    context,
                    delivered: delivered_b.clone(),
                }
            }),
        )
        .expect("node b facade region should register");
    apply_pair_discovery(&mut node_a, &mut node_b);

    let entity_id = entity_owned_by(&node_a, &entity_type, node_b_id.logical_id());
    let entity = sharding_a
        .entity_ref_for(&key, entity_id.as_str())
        .expect("facade entity ref should resolve");
    let shard_id = entity
        .entity_ref()
        .shard_id(registration_a.region().config());
    entity
        .tell(CartCommand {
            action: "before-leave".to_string(),
        })
        .expect("initial facade remote tell should enqueue");
    let _delivered_b = tokio::time::timeout(Duration::from_secs(1), received_b.recv())
        .await
        .expect("initial remote tell should arrive");

    let update_b = node_b
        .mark_leaving(&node_b_id, 3)
        .expect("node b should begin graceful leave");
    let _update_a = node_a
        .mark_leaving(&node_b_id, 3)
        .expect("node a should observe node b leaving");
    assert!(update_b.sharding().handoffs().iter().any(|handoff| {
        handoff.shard().entity_type() == &entity_type
            && handoff.state() == ShardHandoffState::Transferring
            && handoff.reason() == ShardMoveReason::GracefulLeave
            && handoff.stopped_entities() == 1
    }));
    assert_eq!(
        registration_b.region().shard_handoff_state(shard_id),
        Some(ShardHandoffState::Transferring)
    );

    entity
        .tell(CartCommand {
            action: "after-leave".to_string(),
        })
        .expect("post-handoff facade tell should route locally");
    let delivered_a = tokio::time::timeout(Duration::from_secs(1), received_a.recv())
        .await
        .expect("post-handoff local tell should arrive")
        .expect("delivery channel should remain open");
    assert_eq!(
        delivered_a,
        (entity_id.as_str().to_string(), "after-leave".to_string())
    );

    system_a.shutdown();
    system_b.shutdown();
}

#[tokio::test]
async fn networked_runtime_refreshes_ownership_after_leaving_handoff() {
    let registry = cart_registry();
    let Some((mut node_a, mut node_b)) = build_runtime_pair(registry).await else {
        return;
    };
    let node_b_id = node_b.local_node().id().clone();
    let entity_type = EntityType::new("CartLeave");
    let config = ShardingConfig::new(8).expect("valid sharding config");
    let system_a = ActorSystem::new("networked-leave-node-a");
    let system_b = ActorSystem::new("networked-leave-node-b");
    let (delivered_a, mut received_a) = tokio::sync::mpsc::unbounded_channel();
    let (delivered_b, mut received_b) = tokio::sync::mpsc::unbounded_channel();
    let local_route_a = LocalEntityRoute::new(
        node_a.local_node().id().clone(),
        system_a.clone(),
        move |context: LocalEntityContext| NotifyingCartEntity {
            context,
            delivered: delivered_a.clone(),
        },
    );
    let region_a = ShardRegion::new(
        entity_type.clone(),
        config.clone(),
        node_a.remote_route(local_route_a),
    );
    let local_route_b = LocalEntityRoute::new(
        node_b.local_node().id().clone(),
        system_b.clone(),
        move |context: LocalEntityContext| NotifyingCartEntity {
            context,
            delivered: delivered_b.clone(),
        },
    );
    let region_b = ShardRegion::new(entity_type.clone(), config, local_route_b.clone());

    node_a
        .register_entity_region(region_a.clone())
        .expect("node a region should register");
    node_b
        .register_entity_region(region_b.clone())
        .expect("node b region should register");
    apply_pair_discovery(&mut node_a, &mut node_b);

    let entity_id = entity_owned_by(&node_a, &entity_type, node_b_id.logical_id());
    let entity = EntityRef::<CartCommand>::new(entity_type.clone(), entity_id.clone());
    let shard_id = entity.shard_id(region_a.config());
    entity
        .tell(
            &region_a,
            CartCommand {
                action: "before-leave".to_string(),
            },
        )
        .expect("initial remote tell should enqueue");
    let _delivered_b = tokio::time::timeout(Duration::from_secs(1), received_b.recv())
        .await
        .expect("initial remote tell should arrive");

    let update_b = node_b
        .mark_leaving(&node_b_id, 3)
        .expect("node b should begin graceful leave");
    let _update_a = node_a
        .mark_leaving(&node_b_id, 3)
        .expect("node a should observe node b leaving");
    assert!(update_b.sharding().handoffs().iter().any(|handoff| {
        handoff.shard().entity_type() == &entity_type
            && handoff.state() == ShardHandoffState::Transferring
            && handoff.reason() == ShardMoveReason::GracefulLeave
            && handoff.stopped_entities() == 1
    }));
    assert_eq!(
        region_b.shard_handoff_state(shard_id),
        Some(ShardHandoffState::Transferring)
    );
    assert_eq!(
        node_a
            .sharding()
            .coordinator(&entity_type)
            .expect("node a coordinator")
            .owner_for_shard(shard_id)
            .expect("shard owner")
            .logical_id(),
        node_a.local_node().id().logical_id()
    );

    entity
        .tell(
            &region_a,
            CartCommand {
                action: "after-leave".to_string(),
            },
        )
        .expect("post-handoff tell should route locally");
    let delivered_a = tokio::time::timeout(Duration::from_secs(1), received_a.recv())
        .await
        .expect("post-handoff local tell should arrive")
        .expect("delivery channel should remain open");
    assert_eq!(
        delivered_a,
        (entity_id.as_str().to_string(), "after-leave".to_string())
    );

    system_a.shutdown();
    system_b.shutdown();
}

#[tokio::test]
async fn networked_runtime_records_unreachable_remote_delivery_failure() {
    let registry = cart_registry();
    let Some(mut node_a) = build_runtime("rakka-0", "uid-a", registry).await else {
        return;
    };
    let Some(remote_port) = unused_port() else {
        return;
    };
    let remote = node("rakka-1", "uid-b", remote_port);
    let remote_id = remote.id().clone();
    let entity_type = EntityType::new("CartMissing");
    let config = ShardingConfig::new(8).expect("valid sharding config");
    let system_a = ActorSystem::new("networked-missing-node-a");
    let (delivered_a, _received_a) = tokio::sync::mpsc::unbounded_channel();
    let local_route_a = LocalEntityRoute::new(
        node_a.local_node().id().clone(),
        system_a.clone(),
        move |context: LocalEntityContext| NotifyingCartEntity {
            context,
            delivered: delivered_a.clone(),
        },
    );
    let region_a = ShardRegion::new(
        entity_type.clone(),
        config,
        node_a.remote_route(local_route_a),
    );
    node_a
        .register_entity_region(region_a.clone())
        .expect("node a region should register");
    node_a
        .apply_discovery(DiscoverySnapshot::new(
            "network-runtime-test",
            1,
            [node_a.local_node().clone(), remote.clone()],
        ))
        .expect("discovery should register missing remote peer");

    let entity_id = entity_owned_by(&node_a, &entity_type, remote_id.logical_id());
    let entity = EntityRef::<CartCommand>::new(entity_type, entity_id);
    entity
        .tell(
            &region_a,
            CartCommand {
                action: "lost".to_string(),
            },
        )
        .expect("at-most-once send should enqueue before async connect failure");

    wait_for(|| {
        node_a
            .transport()
            .peer_snapshot(&remote_id)
            .is_some_and(|snapshot| {
                snapshot.failures() > 0
                    && snapshot.lifecycle() == TcpRemoteConnectionLifecycle::Failed
            })
    })
    .await;

    system_a.shutdown();
}

async fn build_runtime_pair(
    registry: SerializationRegistry,
) -> Option<(ClusterNodeRuntime, ClusterNodeRuntime)> {
    let node_a = build_runtime("rakka-0", "uid-a", registry.clone()).await?;
    let node_b = build_runtime("rakka-1", "uid-b", registry).await?;
    Some((node_a, node_b))
}

async fn build_runtime(
    logical_id: &str,
    incarnation: &str,
    registry: SerializationRegistry,
) -> Option<ClusterNodeRuntime> {
    match ClusterNodeRuntimeBuilder::new(node(logical_id, incarnation, 0))
        .with_membership_config(membership_config())
        .with_transport_config(tcp_config())
        .with_registry(registry)
        .advertise_bound_addr(true)
        .build()
        .await
    {
        Ok(runtime) => Some(runtime),
        Err(error) if bind_denied(&error) => {
            eprintln!("skipping network runtime test; loopback bind denied: {error}");
            None
        }
        Err(error) => panic!("networked cluster node runtime should bind: {error:?}"),
    }
}

fn apply_pair_discovery(
    node_a: &mut ClusterNodeRuntime,
    node_b: &mut ClusterNodeRuntime,
) -> rakka_sharding::ClusterNodeRuntimeUpdate {
    let nodes = [node_a.local_node().clone(), node_b.local_node().clone()];
    let update_a = node_a
        .apply_discovery(DiscoverySnapshot::new(
            "network-runtime-test",
            1,
            nodes.clone(),
        ))
        .expect("node a discovery should apply");
    let update_b = node_b
        .apply_discovery(DiscoverySnapshot::new("network-runtime-test", 1, nodes))
        .expect("node b discovery should apply");
    assert_eq!(update_b.registered_peers(), 1);
    update_a
}

fn cart_registry() -> SerializationRegistry {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf::<CartCommand>("rakka.test.CartCommand", 1)
        .expect("cart command codec should register");
    registry
        .register_protobuf::<CartGet>("rakka.test.CartGet", 1)
        .expect("cart get codec should register");
    registry
        .register_protobuf::<CartReply>("rakka.test.CartReply", 1)
        .expect("cart reply codec should register");
    registry
}

fn tcp_config() -> TcpRemoteTransportConfig {
    TcpRemoteTransportConfig::new()
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .connect_timeout(Duration::from_millis(100))
        .reconnect_backoff(Duration::from_millis(10))
        .idle_timeout(Duration::from_secs(10))
}

fn membership_config() -> MembershipConfig {
    MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100))
}

fn node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
    .with_role("sharded-entity")
}

fn entity_owned_by(
    runtime: &ClusterNodeRuntime,
    entity_type: &EntityType,
    logical_id: &str,
) -> EntityId {
    let coordinator = runtime
        .sharding()
        .coordinator(entity_type)
        .expect("coordinator should exist");
    (0..4096)
        .map(|index| EntityId::new(format!("cart-{index}")))
        .find(|entity_id| {
            coordinator
                .owner_for_entity(entity_id)
                .is_ok_and(|owner| owner.logical_id() == logical_id)
        })
        .expect("expected at least one entity to map to requested owner")
}

fn unused_port() -> Option<u16> {
    let listener = match std::net::TcpListener::bind(("127.0.0.1", 0)) {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("skipping network runtime test; unused port probe denied: {error}");
            return None;
        }
        Err(error) => panic!("unused port probe should bind: {error}"),
    };
    Some(
        listener
            .local_addr()
            .expect("local address should exist")
            .port(),
    )
}

fn bind_denied(error: &ClusterNodeRuntimeError) -> bool {
    matches!(
        error,
        ClusterNodeRuntimeError::TcpTransport {
            error: TcpRemoteTransportError::Io { message }
        } if message.contains("Operation not permitted") || message.contains("Permission denied")
    )
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if condition() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for condition"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
