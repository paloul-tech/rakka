#![forbid(unsafe_code)]

//! Clustered receptionist propagation example.

use std::env;
use std::error::Error;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use rakka::cluster::{
    ClusterNode, ClusterProtocol, ClusteredReceptionistSettings, MembershipConfig, NodeAddress,
    NodeId,
};
use rakka::prelude::*;
use rakka::remote::{
    PayloadCodec, RemoteClusteredReceptionist, RemoteEndpoint, RemoteError,
    RemoteReceptionistListing, RemoteReceptionistListingCodec, RemoteResult, SerializationRegistry,
    TcpRemoteTransport, TcpRemoteTransportConfig,
};
use tokio::sync::mpsc;

#[derive(Debug)]
enum ClusterWork {
    Handle { id: u64 },
}

struct RemoteVisibleWorker {
    node: &'static str,
    delivered: mpsc::UnboundedSender<String>,
}

impl Actor for RemoteVisibleWorker {
    type Msg = ClusterWork;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let node = self.node;
        let delivered = self.delivered.clone();
        actor_future(async move {
            match msg {
                ClusterWork::Handle { id } => {
                    let _ = delivered.send(format!("{node}:{id}"));
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if env::args().any(|arg| arg == "--tcp-loopback") {
        return run_tcp_loopback().await;
    }

    run_deterministic().await
}

async fn run_deterministic() -> Result<(), Box<dyn Error>> {
    let node_a = node("rakka-0", "uid-a", 25520);
    let node_b = node("rakka-1", "uid-b", 25521);
    let system_a = ActorSystem::new("clustered-receptionist-a");
    let system_b = ActorSystem::new("clustered-receptionist-b");
    let cluster_a = cluster_for(node_a.clone(), node_b.clone())?;
    let cluster_b = cluster_for(node_b.clone(), node_a.clone())?;
    let receptionist_a = ClusteredReceptionist::get(&system_a, cluster_a);
    let receptionist_b = ClusteredReceptionist::get(&system_b, cluster_b);
    let key = ServiceKey::<ClusterWork>::new("example.cluster.workers");
    let (delivered, mut received) = mpsc::unbounded_channel();

    let worker_a = system_a.spawn_actor(
        "node-a-worker",
        RemoteVisibleWorker {
            node: "rakka-0",
            delivered,
        },
    )?;
    let _registration = Receptionist::get(&system_a).register(&key, worker_a)?;

    let propagated = receptionist_a.propagate_to(&receptionist_b, &key, 1)?;
    let group = Routers::group(key)
        .with_round_robin()
        .spawn(&system_b, "cluster-worker-group")?;
    group.tell(ClusterWork::Handle { id: 7 })?;

    let delivered = tokio::time::timeout(Duration::from_secs(1), received.recv())
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "delivery channel closed"))?;
    println!(
        "Rakka clustered receptionist propagated {propagated} and routed {delivered} through {} remote routee.",
        group.routee_count()
    );

    system_a.terminate().await?;
    system_b.terminate().await?;
    Ok(())
}

async fn run_tcp_loopback() -> Result<(), Box<dyn Error>> {
    let system_a = ActorSystem::new("tcp-clustered-receptionist-a");
    let system_b = ActorSystem::new("tcp-clustered-receptionist-b");
    let node_a_id = NodeId::new("tcp-rakka-0", "uid-a");
    let node_b_id = NodeId::new("tcp-rakka-1", "uid-b");
    let endpoint_a = RemoteEndpoint::new(node_a_id.clone());
    let endpoint_b = RemoteEndpoint::new(node_b_id.clone());
    let transport_a = TcpRemoteTransport::bind(
        node_a_id.clone(),
        ClusterProtocol::default(),
        endpoint_a.clone(),
        tcp_config(),
    )
    .await?;
    let transport_b = TcpRemoteTransport::bind(
        node_b_id.clone(),
        ClusterProtocol::default(),
        endpoint_b.clone(),
        tcp_config(),
    )
    .await?;
    let node_a = node_with_id(node_a_id.clone(), transport_a.local_addr().port());
    let node_b = node_with_id(node_b_id.clone(), transport_b.local_addr().port());
    let cluster_a = cluster_for(node_a.clone(), node_b.clone())?;
    let cluster_b = cluster_for(node_b.clone(), node_a.clone())?;
    transport_a.register_peer(node_b.clone())?;
    transport_b.register_peer(node_a)?;

    let runtime_a = RemoteClusteredReceptionist::with_transport(
        system_a.clone(),
        cluster_a,
        endpoint_a,
        transport_a.clone(),
        remote_registry()?,
        ClusteredReceptionistSettings::default(),
    );
    let runtime_b = RemoteClusteredReceptionist::with_transport(
        system_b.clone(),
        cluster_b,
        endpoint_b,
        transport_b.clone(),
        remote_registry()?,
        ClusteredReceptionistSettings::default(),
    );
    runtime_a.register_actor_ref_handler::<ClusterWork>()?;
    runtime_b.register_actor_ref_handler::<ClusterWork>()?;

    let key = ServiceKey::<ClusterWork>::new("example.cluster.workers.tcp");
    runtime_b.register_receptionist_listing_handler::<ClusterWork>(&key)?;
    let (delivered, mut received) = mpsc::unbounded_channel();
    let worker_a = system_a.spawn_actor(
        "node-a-worker",
        RemoteVisibleWorker {
            node: "tcp-rakka-0",
            delivered,
        },
    )?;
    let _registration = Receptionist::get(&system_a).register(&key, worker_a)?;

    let published = runtime_a.publish_once_to(&node_b_id, &key, 1)?;
    wait_for(
        || runtime_b.proxy_snapshot().proxy_count() == 1,
        "remote service proxy",
    )
    .await?;

    let group = Routers::group(key)
        .with_round_robin()
        .spawn(&system_b, "tcp-cluster-worker-group")?;
    group.tell(ClusterWork::Handle { id: 42 })?;

    let delivered = tokio::time::timeout(Duration::from_secs(1), received.recv())
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "delivery channel closed"))?;
    println!(
        "Rakka TCP clustered receptionist published {published} and routed {delivered} through {} proxy routee.",
        runtime_b.proxy_snapshot().proxy_count()
    );

    system_a.terminate().await?;
    system_b.terminate().await?;
    Ok(())
}

fn cluster_for(local: ClusterNode, peer: ClusterNode) -> Result<Cluster, Box<dyn Error>> {
    let cluster = Cluster::for_local_node(local.clone(), MembershipConfig::default());
    cluster.manager().join_seed_nodes([local, peer])?;
    Ok(cluster)
}

fn node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    node_with_id(NodeId::new(logical_id, incarnation), port)
}

fn node_with_id(node_id: NodeId, port: u16) -> ClusterNode {
    ClusterNode::new(node_id, NodeAddress::new("127.0.0.1", port))
}

fn tcp_config() -> TcpRemoteTransportConfig {
    TcpRemoteTransportConfig::new()
        .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
        .connect_timeout(Duration::from_millis(500))
        .reconnect_backoff(Duration::from_millis(10))
        .idle_timeout(Duration::from_secs(10))
}

fn remote_registry() -> RemoteResult<SerializationRegistry> {
    let mut registry = SerializationRegistry::new();
    registry.register::<ClusterWork, _>(ClusterWorkCodec)?;
    registry.register::<RemoteReceptionistListing, _>(RemoteReceptionistListingCodec)?;
    Ok(registry)
}

async fn wait_for(mut condition: impl FnMut() -> bool, label: &str) -> io::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if condition() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for {label}"),
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ClusterWorkCodec;

impl PayloadCodec<ClusterWork> for ClusterWorkCodec {
    fn codec_id(&self) -> &str {
        "example-binary"
    }

    fn message_type_id(&self) -> &str {
        "example.ClusterWork"
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn encode(&self, message: &ClusterWork) -> RemoteResult<Vec<u8>> {
        match message {
            ClusterWork::Handle { id } => {
                let mut bytes = Vec::with_capacity(9);
                bytes.push(1);
                bytes.extend_from_slice(&id.to_be_bytes());
                Ok(bytes)
            }
        }
    }

    fn decode(&self, payload: &[u8]) -> RemoteResult<ClusterWork> {
        if payload.len() != 9 || payload[0] != 1 {
            return Err(RemoteError::Decode {
                codec_id: self.codec_id().to_string(),
                message: "expected ClusterWork::Handle frame".to_string(),
            });
        }
        let mut id = [0u8; 8];
        id.copy_from_slice(&payload[1..]);
        Ok(ClusterWork::Handle {
            id: u64::from_be_bytes(id),
        })
    }
}
