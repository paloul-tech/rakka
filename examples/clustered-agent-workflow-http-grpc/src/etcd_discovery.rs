//! Dynamic etcd-backed discovery for Kubernetes (autoscaling-friendly).
//!
//! Each node registers itself under a leased key (`<prefix><node-id>`) and keeps
//! the lease alive. Peers are learned by ranging the prefix each tick. A scaled-in
//! or crashed node's key disappears when its lease lapses (or is revoked on
//! graceful shutdown), so membership grows and shrinks at runtime with no
//! configured replica count. Snapshots are fed to the same
//! `apply_discovery` / `tick` path the file provider uses.

use std::sync::Arc;

use etcd_client::{Client, GetOptions, PutOptions};
use rakka::cluster::{ClusterNode, DiscoverySnapshot};
use rakka::sharding::ClusterNodeRuntime;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::config::ExampleConfig;
use crate::discovery::{set_membership, MembershipView};
use crate::support::{
    current_timestamp_millis, example_error, ExampleError, ExampleResult, DEFAULT_DISCOVERY_POLL,
};

const PROVIDER: &str = "etcd";

/// A live etcd registration for this node.
pub struct EtcdSession {
    client: Client,
    lease_id: i64,
    key: String,
    value: Vec<u8>,
    prefix: String,
    lease_ttl_seconds: i64,
}

/// Connects to etcd, registers this node under a fresh lease, and applies the
/// first membership snapshot before the node starts serving.
pub async fn connect_register(
    config: &ExampleConfig,
    local_node: &ClusterNode,
    runtime: &mut ClusterNodeRuntime,
    membership: &MembershipView,
) -> ExampleResult<EtcdSession> {
    let mut client = Client::connect(&config.etcd_endpoints, None)
        .await
        .map_err(etcd_error)?;
    let grant = client
        .lease_grant(config.etcd_lease_ttl_seconds, None)
        .await
        .map_err(etcd_error)?;
    let lease_id = grant.id();
    let key = member_key(&config.etcd_prefix, local_node);
    let value = serde_json::to_vec(local_node)?;
    client
        .put(
            key.clone(),
            value.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        )
        .await
        .map_err(etcd_error)?;

    let nodes = range_members(&mut client, &config.etcd_prefix).await?;
    apply(runtime, membership, nodes);

    Ok(EtcdSession {
        client,
        lease_id,
        key,
        value,
        prefix: config.etcd_prefix.clone(),
        lease_ttl_seconds: config.etcd_lease_ttl_seconds,
    })
}

/// Runs the etcd discovery loop until `shutdown` is signalled, renewing the lease
/// and applying membership each tick; revokes the lease on shutdown so peers drop
/// this node immediately.
pub async fn run_etcd_discovery(
    mut session: EtcdSession,
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    membership: MembershipView,
    shutdown: Arc<Notify>,
) {
    let mut interval = tokio::time::interval(DEFAULT_DISCOVERY_POLL);
    loop {
        tokio::select! {
            () = shutdown.notified() => {
                let _ = session.client.lease_revoke(session.lease_id).await;
                break;
            }
            _ = interval.tick() => {
                if let Err(error) = renew_lease(&mut session).await {
                    eprintln!("etcd lease renew failed: {error}");
                    continue;
                }
                let nodes = match range_members(&mut session.client, &session.prefix).await {
                    Ok(nodes) => nodes,
                    Err(error) => {
                        eprintln!("etcd range failed: {error}");
                        continue;
                    }
                };
                let mut runtime = runtime.lock().await;
                apply(&mut runtime, &membership, nodes);
            }
        }
    }
}

fn apply(runtime: &mut ClusterNodeRuntime, membership: &MembershipView, nodes: Vec<ClusterNode>) {
    let now = current_timestamp_millis();
    set_membership(membership, &nodes);
    if let Err(error) = runtime.apply_discovery(DiscoverySnapshot::new(PROVIDER, now, nodes)) {
        eprintln!("etcd discovery apply failed: {error}");
    }
    if let Err(error) = runtime.tick(now) {
        eprintln!("membership tick failed: {error}");
    }
}

async fn renew_lease(session: &mut EtcdSession) -> ExampleResult<()> {
    // Try a keepalive first; if the lease has lapsed, grant a new one and
    // re-register so this node stays in membership.
    if let Ok((mut keeper, _stream)) = session.client.lease_keep_alive(session.lease_id).await {
        if keeper.keep_alive().await.is_ok() {
            return Ok(());
        }
    }
    let grant = session
        .client
        .lease_grant(session.lease_ttl_seconds, None)
        .await
        .map_err(etcd_error)?;
    session.lease_id = grant.id();
    session
        .client
        .put(
            session.key.clone(),
            session.value.clone(),
            Some(PutOptions::new().with_lease(session.lease_id)),
        )
        .await
        .map_err(etcd_error)?;
    Ok(())
}

async fn range_members(client: &mut Client, prefix: &str) -> ExampleResult<Vec<ClusterNode>> {
    let response = client
        .get(
            prefix.as_bytes().to_vec(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(etcd_error)?;
    let nodes = response
        .kvs()
        .iter()
        .filter_map(|kv| serde_json::from_slice::<ClusterNode>(kv.value()).ok())
        .collect();
    Ok(nodes)
}

fn member_key(prefix: &str, local_node: &ClusterNode) -> String {
    format!("{prefix}{}", local_node.id())
}

fn etcd_error(error: etcd_client::Error) -> ExampleError {
    example_error(format!("etcd: {error}")).into()
}
