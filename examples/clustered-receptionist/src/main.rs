#![forbid(unsafe_code)]

//! Deterministic clustered receptionist propagation example.

use std::error::Error;
use std::io;
use std::time::Duration;

use rakka::cluster::{ClusterNode, MembershipConfig, NodeAddress, NodeId};
use rakka::prelude::*;
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

fn cluster_for(local: ClusterNode, peer: ClusterNode) -> Result<Cluster, Box<dyn Error>> {
    let cluster = Cluster::for_local_node(local.clone(), MembershipConfig::default());
    cluster.manager().join_seed_nodes([local, peer])?;
    Ok(cluster)
}

fn node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
}
