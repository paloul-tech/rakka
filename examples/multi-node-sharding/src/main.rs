#![forbid(unsafe_code)]

//! Multi-node sharding examples using deterministic and TCP remote transports.

use std::env;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::Stdio;
use std::time::Duration;

use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem};
use rakka_remote::{InMemoryRemoteTransport, RemoteEndpoint, SerializationRegistry};
use rakka_sharding::node_runtime::{
    ClusterNodeRuntime, ClusterNodeRuntimeBuilder, ClusterNodeRuntimeUpdate,
};
use rakka_sharding::{
    ClusterSharding, Entity, EntityContext, EntityId, EntityRef, EntityType, EntityTypeKey,
    InMemoryRememberedEntityStore, LocalEntityContext, LocalEntityRoute, RememberedEntities,
    RemoteEntityInbound, RemoteEntityRoute, RemoteTransportEntityOutbound, ShardCoordinator,
    ShardRegion, ShardingConfig,
};
use tokio::process::Command;

#[derive(Clone, PartialEq, prost::Message)]
struct CartCommand {
    #[prost(string, tag = "1")]
    action: String,
}

struct CartEntity {
    context: LocalEntityContext,
    delivered: tokio::sync::mpsc::UnboundedSender<(String, String)>,
}

struct FacadeCartEntity {
    context: EntityContext<CartCommand>,
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

impl Actor for FacadeCartEntity {
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
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [] => run_in_memory_example().await,
        [flag] if flag == "--networked-loopback" => run_networked_loopback_example().await,
        [flag] if flag == "--networked-processes" => run_networked_process_driver().await,
        [flag, logical_id, incarnation, local_port, peer_port, role]
            if flag == "--networked-node" =>
        {
            run_networked_child_node(logical_id, incarnation, local_port, peer_port, role).await
        }
        _ => Err(example_error(usage()).into()),
    }
}

async fn run_in_memory_example() -> Result<(), Box<dyn Error>> {
    let node_a = dns_example_node("rakka-0", "uid-a");
    let node_b = dns_example_node("rakka-1", "uid-b");
    let membership = membership_with_up_nodes([node_a.clone(), node_b.clone()])?;
    let node_b_membership = membership_with_up_nodes([node_b.clone(), node_a.clone()])?;

    let entity_type = EntityType::new("Cart");
    let sharding_config = ShardingConfig::new(8)?;
    let entity_key = EntityTypeKey::<CartCommand>::new(entity_type.as_str())
        .with_config(sharding_config.clone());
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), sharding_config.clone());
    let rebalance = coordinator.reconcile(&membership);
    let ownership = coordinator.snapshot();
    let remote_entity_id = entity_owned_by(&coordinator, node_b.id().logical_id())?;
    let remote_entity_owner = coordinator.owner_for_entity(&remote_entity_id)?.clone();

    let registry = cart_registry()?;
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
    let region_a = ShardRegion::from_snapshot(
        entity_type.clone(),
        sharding_config.clone(),
        &ownership,
        remote_route_a,
    )?;

    let (node_b_delivered, mut node_b_received) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let remembered_entities = InMemoryRememberedEntityStore::new();
    let node_b_sharding =
        ClusterSharding::from_membership(&node_b_system, node_b.clone(), node_b_membership);
    let node_b_registration = node_b_sharding.init(
        Entity::of(entity_key, move |context: EntityContext<CartCommand>| {
            FacadeCartEntity {
                context,
                delivered: node_b_delivered.clone(),
            }
        })
        .with_remembered_entities(
            RememberedEntities::enabled().with_store(remembered_entities.clone()),
        ),
    )?;
    let region_b = node_b_registration.region().clone();

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
    wait_for(|| remembered_entities.len() == 1).await?;

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
        "node-b facade local entity count: {}",
        node_b_sharding
            .registration_state(node_b_registration.key())
            .map(|state| state.local_entity_count())
            .unwrap_or_default()
    );
    println!(
        "node-b remembered entity count: {}",
        remembered_entities.len()
    );

    node_a_system.shutdown();
    node_b_system.shutdown();
    Ok(())
}

async fn run_networked_loopback_example() -> Result<(), Box<dyn Error>> {
    let registry = cart_registry()?;
    let mut node_a = build_networked_runtime(
        loopback_example_node("rakka-0", "uid-a", 0),
        registry.clone(),
        true,
    )
    .await?;
    let mut node_b =
        build_networked_runtime(loopback_example_node("rakka-1", "uid-b", 0), registry, true)
            .await?;
    let entity_type = EntityType::new("Cart");
    let sharding_config = ShardingConfig::new(8)?;
    let entity_key =
        EntityTypeKey::<CartCommand>::new(entity_type.as_str()).with_config(sharding_config);
    let node_a_system = ActorSystem::new("rakka-example-networked-node-a");
    let node_b_system = ActorSystem::new("rakka-example-networked-node-b");
    let (node_a_delivered, _node_a_received) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let (node_b_delivered, mut node_b_received) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let sharding_a = ClusterSharding::for_node_runtime(&node_a_system, &node_a)?;
    let sharding_b = ClusterSharding::for_node_runtime(&node_b_system, &node_b)?;

    let registration_a = sharding_a.init_remote(
        &mut node_a,
        Entity::of(
            entity_key.clone(),
            move |context: EntityContext<CartCommand>| FacadeCartEntity {
                context,
                delivered: node_a_delivered.clone(),
            },
        ),
    )?;
    let registration_b = sharding_b.init_remote(
        &mut node_b,
        Entity::of(
            entity_key.clone(),
            move |context: EntityContext<CartCommand>| FacadeCartEntity {
                context,
                delivered: node_b_delivered.clone(),
            },
        ),
    )?;

    let update = apply_networked_discovery(&mut node_a, &mut node_b)?;
    let coordinator = node_a
        .sharding()
        .coordinator(&entity_type)
        .ok_or_else(|| example_error("missing coordinator after discovery"))?;
    let remote_entity_id = entity_owned_by(coordinator, node_b.local_node().id().logical_id())?;
    let remote_entity_owner = coordinator.owner_for_entity(&remote_entity_id)?.clone();
    let remote_entity = sharding_a.entity_ref_for(&entity_key, remote_entity_id.as_str())?;

    remote_entity
        .tell(CartCommand {
            action: "add-apple".to_string(),
        })
        .map_err(|error| example_error(format!("networked entity tell failed: {error:?}")))?;
    let delivered = tokio::time::timeout(Duration::from_secs(1), node_b_received.recv())
        .await?
        .ok_or_else(|| example_error("networked delivery channel closed"))?;
    wait_for(|| node_b.transport_snapshot().inbound_envelopes() >= 1).await?;

    println!(
        "Rakka networked sharding routed {} to {} on {} over TCP loopback.",
        delivered.1, delivered.0, remote_entity_owner
    );
    println!(
        "Registered TCP peers: node-a {}, node-b {}; membership events: {}.",
        node_a.registered_peer_count(),
        node_b.registered_peer_count(),
        update.sharding().membership_events().len()
    );
    println!(
        "node-a facade local entity count: {}",
        sharding_a
            .registration_state(registration_a.key())
            .map(|state| state.local_entity_count())
            .unwrap_or_default()
    );
    println!(
        "node-b facade local entity count: {}",
        sharding_b
            .registration_state(registration_b.key())
            .map(|state| state.local_entity_count())
            .unwrap_or_default()
    );

    node_a_system.shutdown();
    node_b_system.shutdown();
    Ok(())
}

async fn run_networked_process_driver() -> Result<(), Box<dyn Error>> {
    let node_a_port = unused_port()?;
    let node_b_port = unused_port()?;
    let executable = env::current_exe()?;
    let mut node_b = Command::new(&executable)
        .args([
            "--networked-node",
            "rakka-1",
            "uid-b",
            &node_b_port.to_string(),
            &node_a_port.to_string(),
            "receive",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let node_a_output = Command::new(&executable)
        .args([
            "--networked-node",
            "rakka-0",
            "uid-a",
            &node_a_port.to_string(),
            &node_b_port.to_string(),
            "send",
        ])
        .output()
        .await?;
    let node_b_status = match tokio::time::timeout(Duration::from_secs(5), node_b.wait()).await {
        Ok(status) => status?,
        Err(_elapsed) => {
            let _ = node_b.kill().await;
            return Err(
                example_error("node-b child process timed out waiting for delivery").into(),
            );
        }
    };

    ensure_success("node-a", &node_a_output)?;
    ensure_status("node-b", node_b_status)?;

    println!(
        "Rakka networked sharding launched two node processes on 127.0.0.1:{node_a_port} and 127.0.0.1:{node_b_port}."
    );
    print_child_output("node-a", &node_a_output.stdout)?;
    Ok(())
}

async fn run_networked_child_node(
    logical_id: &str,
    incarnation: &str,
    local_port: &str,
    peer_port: &str,
    role: &str,
) -> Result<(), Box<dyn Error>> {
    let local_port = local_port.parse::<u16>()?;
    let peer_port = peer_port.parse::<u16>()?;
    let peer_logical_id = if logical_id == "rakka-0" {
        "rakka-1"
    } else {
        "rakka-0"
    };
    let peer_incarnation = if incarnation == "uid-a" {
        "uid-b"
    } else {
        "uid-a"
    };
    let registry = cart_registry()?;
    let mut runtime = build_networked_runtime(
        loopback_example_node(logical_id, incarnation, local_port),
        registry,
        false,
    )
    .await?;
    let peer = loopback_example_node(peer_logical_id, peer_incarnation, peer_port);
    let entity_type = EntityType::new("Cart");
    let entity_key =
        EntityTypeKey::<CartCommand>::new(entity_type.as_str()).with_number_of_shards(8)?;
    let system = ActorSystem::new(format!("rakka-example-networked-{logical_id}"));
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime)?;
    let (delivered_tx, mut delivered_rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, String)>();
    let registration = sharding.init_remote(
        &mut runtime,
        Entity::of(
            entity_key.clone(),
            move |context: EntityContext<CartCommand>| FacadeCartEntity {
                context,
                delivered: delivered_tx.clone(),
            },
        ),
    )?;

    runtime.apply_discovery(DiscoverySnapshot::new(
        "networked-process-example",
        1,
        [runtime.local_node().clone(), peer.clone()],
    ))?;

    match role {
        "send" => {
            let coordinator = runtime
                .sharding()
                .coordinator(&entity_type)
                .ok_or_else(|| example_error("missing coordinator after discovery"))?;
            let remote_entity_id = entity_owned_by(coordinator, peer.id().logical_id())?;
            let entity = sharding.entity_ref_for(&entity_key, remote_entity_id.as_str())?;
            entity
                .tell(CartCommand {
                    action: "add-apple".to_string(),
                })
                .map_err(|error| {
                    example_error(format!("networked child tell failed: {error:?}"))
                })?;
            wait_for(|| {
                runtime
                    .transport()
                    .peer_snapshot(peer.id())
                    .is_some_and(|snapshot| snapshot.sent() >= 1)
            })
            .await?;
            println!(
                "{logical_id} sent add-apple to {} on {}.",
                remote_entity_id,
                peer.id()
            );
        }
        "receive" => {
            let delivered = tokio::time::timeout(Duration::from_secs(3), delivered_rx.recv())
                .await?
                .ok_or_else(|| example_error("networked child delivery channel closed"))?;
            println!("{logical_id} received {} for {}.", delivered.1, delivered.0);
            println!(
                "{logical_id} facade local entity count: {}.",
                sharding
                    .registration_state(registration.key())
                    .map(|state| state.local_entity_count())
                    .unwrap_or_default()
            );
        }
        _ => return Err(example_error(format!("unknown networked node role {role}")).into()),
    }

    system.shutdown();
    Ok(())
}

async fn build_networked_runtime(
    local_node: ClusterNode,
    registry: SerializationRegistry,
    advertise_bound_addr: bool,
) -> Result<ClusterNodeRuntime, Box<dyn Error>> {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), local_node.address().port());
    ClusterNodeRuntimeBuilder::new(local_node)
        .with_membership_config(membership_config())
        .with_transport_config(
            rakka_remote::TcpRemoteTransportConfig::new()
                .bind_addr(bind_addr)
                .connect_timeout(Duration::from_millis(250))
                .reconnect_backoff(Duration::from_millis(20))
                .idle_timeout(Duration::from_secs(10)),
        )
        .with_registry(registry)
        .advertise_bound_addr(advertise_bound_addr)
        .build()
        .await
        .map_err(|error| {
            example_error(format!("networked runtime failed to start: {error}")).into()
        })
}

fn apply_networked_discovery(
    node_a: &mut ClusterNodeRuntime,
    node_b: &mut ClusterNodeRuntime,
) -> Result<ClusterNodeRuntimeUpdate, Box<dyn Error>> {
    let nodes = [node_a.local_node().clone(), node_b.local_node().clone()];
    let update = node_a.apply_discovery(DiscoverySnapshot::new(
        "networked-loopback-example",
        1,
        nodes.clone(),
    ))?;
    node_b.apply_discovery(DiscoverySnapshot::new(
        "networked-loopback-example",
        1,
        nodes,
    ))?;
    Ok(update)
}

fn dns_example_node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(
            format!("{logical_id}.rakka.default.svc.cluster.local"),
            2552,
        ),
    )
    .with_role("sharded-entity")
}

fn loopback_example_node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
    .with_role("sharded-entity")
}

fn membership_config() -> MembershipConfig {
    MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100))
}

fn membership_with_up_nodes(
    nodes: impl IntoIterator<Item = ClusterNode>,
) -> Result<ClusterMembership, Box<dyn Error>> {
    let nodes = nodes.into_iter().collect::<Vec<_>>();
    let local = nodes
        .first()
        .cloned()
        .ok_or_else(|| example_error("example requires at least one node"))?;
    let mut membership = ClusterMembership::new(local, membership_config());
    membership.record_discovery(DiscoverySnapshot::new("example", 1, nodes.clone()))?;
    for (offset, node) in nodes.iter().enumerate() {
        membership.mark_up(node.id(), 2 + u64::try_from(offset)?)?;
    }
    Ok(membership)
}

fn cart_registry() -> Result<SerializationRegistry, Box<dyn Error>> {
    let mut registry = SerializationRegistry::new();
    registry.register_protobuf::<CartCommand>("rakka.example.CartCommand", 1)?;
    Ok(registry)
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

fn unused_port() -> Result<u16, Box<dyn Error>> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    Ok(listener.local_addr()?.port())
}

fn ensure_success(name: &str, output: &std::process::Output) -> Result<(), Box<dyn Error>> {
    if output.status.success() {
        return Ok(());
    }
    Err(example_error(format!(
        "{name} exited with {}; stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    ))
    .into())
}

fn ensure_status(name: &str, status: std::process::ExitStatus) -> Result<(), Box<dyn Error>> {
    if status.success() {
        return Ok(());
    }
    Err(example_error(format!("{name} exited with {status}")).into())
}

fn print_child_output(name: &str, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
    let output = std::str::from_utf8(bytes)?;
    for line in output.lines() {
        println!("{name}: {line}");
    }
    Ok(())
}

async fn wait_for(mut condition: impl FnMut() -> bool) -> Result<(), Box<dyn Error>> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if condition() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(example_error("timed out waiting for networked example condition").into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn usage() -> String {
    [
        "usage:",
        "  cargo run -p rakka-example-multi-node-sharding",
        "  cargo run -p rakka-example-multi-node-sharding -- --networked-loopback",
        "  cargo run -p rakka-example-multi-node-sharding -- --networked-processes",
    ]
    .join("\n")
}

fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
