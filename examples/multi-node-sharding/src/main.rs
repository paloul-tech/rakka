#![forbid(unsafe_code)]

//! Minimal multi-node sharding example using the deterministic in-memory remote transport.

use std::error::Error;
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem};
use rakka_remote::{InMemoryRemoteTransport, RemoteEndpoint, SerializationRegistry};
use rakka_sharding::{
    EntityId, EntityRef, EntityType, LocalEntityContext, LocalEntityRoute, RemoteEntityInbound,
    RemoteEntityRoute, RemoteTransportEntityOutbound, ShardCoordinator, ShardingConfig,
};

#[derive(Clone, PartialEq, prost::Message)]
struct CartCommand {
    #[prost(string, tag = "1")]
    action: String,
}

struct CartEntity {
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
        let entity_id = self.context.entity_id().as_str().to_string();
        let action = msg.action;
        let delivered = self.delivered.clone();
        actor_future(async move {
            let _sent = delivered.send((entity_id, action));
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let node_a = example_node("rakka-0", "uid-a");
    let node_b = example_node("rakka-1", "uid-b");
    let membership = membership_with_up_nodes([node_a.clone(), node_b.clone()])?;

    let entity_type = EntityType::new("Cart");
    let sharding_config = ShardingConfig::new(8)?;
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), sharding_config.clone());
    let rebalance = coordinator.reconcile(&membership);
    let ownership = coordinator.snapshot();
    let remote_entity_id = entity_owned_by(&coordinator, node_b.id().logical_id())?;
    let remote_entity_owner = coordinator.owner_for_entity(&remote_entity_id)?.clone();

    let mut registry = SerializationRegistry::new();
    registry.register_protobuf::<CartCommand>("rakka.example.CartCommand", 1)?;

    let transport = InMemoryRemoteTransport::new();
    let node_a_system = ActorSystem::new("rakka-example-node-a");
    let node_b_system = ActorSystem::new("rakka-example-node-b");

    let (node_a_delivered, _node_a_received) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let local_route_a = LocalEntityRoute::new(
        node_a.id().clone(),
        node_a_system.clone(),
        move |context: LocalEntityContext| CartEntity {
            context,
            delivered: node_a_delivered.clone(),
        },
    );
    let outbound = RemoteTransportEntityOutbound::new(transport.clone());
    let remote_route_a = RemoteEntityRoute::new(local_route_a.clone(), registry.clone(), outbound)
        .with_source(node_a.id().to_string());
    let region_a = rakka_sharding::ShardRegion::from_snapshot(
        entity_type.clone(),
        sharding_config.clone(),
        &ownership,
        remote_route_a,
    )?;

    let (node_b_delivered, mut node_b_received) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let local_route_b = LocalEntityRoute::new(
        node_b.id().clone(),
        node_b_system.clone(),
        move |context: LocalEntityContext| CartEntity {
            context,
            delivered: node_b_delivered.clone(),
        },
    );
    let region_b = rakka_sharding::ShardRegion::from_snapshot(
        entity_type.clone(),
        sharding_config,
        &ownership,
        local_route_b.clone(),
    )?;

    let endpoint_b = RemoteEndpoint::new(node_b.id().clone());
    endpoint_b.register_entity_handler(
        entity_type.as_str(),
        RemoteEntityInbound::new(region_b, registry.clone()),
    )?;
    transport.register_endpoint(endpoint_b)?;

    let remote_entity = EntityRef::<CartCommand>::new(entity_type, remote_entity_id.clone());
    remote_entity
        .tell(
            &region_a,
            CartCommand {
                action: "add-apple".to_string(),
            },
        )
        .map_err(|error| example_error(format!("remote entity tell failed: {error:?}")))?;

    let delivered = tokio::time::timeout(Duration::from_secs(1), node_b_received.recv())
        .await?
        .ok_or_else(|| example_error("remote entity delivery channel closed"))?;

    println!(
        "Rakka multi-node sharding routed {} to {} on {}.",
        delivered.1, delivered.0, remote_entity_owner
    );
    println!(
        "Shard ownership revision {} allocated {} shards across {} up nodes.",
        rebalance.new_revision(),
        ownership.assignments().len(),
        membership.routable_members().len()
    );
    println!(
        "node-a local entity count: {}",
        local_route_a.entity_count()
    );
    println!(
        "node-b local entity count: {}",
        local_route_b.entity_count()
    );

    node_a_system.shutdown();
    node_b_system.shutdown();
    Ok(())
}

fn example_node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(
            format!("{logical_id}.rakka.default.svc.cluster.local"),
            2552,
        ),
    )
    .with_role("sharded-entity")
}

fn membership_with_up_nodes(
    nodes: impl IntoIterator<Item = ClusterNode>,
) -> Result<ClusterMembership, Box<dyn Error>> {
    let nodes = nodes.into_iter().collect::<Vec<_>>();
    let local = nodes
        .first()
        .cloned()
        .ok_or_else(|| example_error("example requires at least one node"))?;
    let mut membership = ClusterMembership::new(
        local,
        MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100)),
    );
    membership.record_discovery(DiscoverySnapshot::new("example", 1, nodes.clone()))?;
    for (offset, node) in nodes.iter().enumerate() {
        membership.mark_up(node.id(), 2 + u64::try_from(offset)?)?;
    }
    Ok(membership)
}

fn entity_owned_by(
    coordinator: &ShardCoordinator,
    logical_id: &str,
) -> Result<EntityId, Box<dyn Error>> {
    (0..4096)
        .map(|index| EntityId::new(format!("cart-{index}")))
        .find_map(|entity_id| {
            coordinator
                .owner_for_entity(&entity_id)
                .ok()
                .filter(|owner| owner.logical_id() == logical_id)
                .map(|_owner| entity_id)
        })
        .ok_or_else(|| {
            example_error(format!(
                "could not find an example entity owned by logical node {logical_id}"
            ))
            .into()
        })
}

fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
