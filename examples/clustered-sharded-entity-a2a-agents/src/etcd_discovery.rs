//! Dynamic etcd-backed discovery plus peer-reachability self-fencing.
//!
//! File discovery is the default local mode. Etcd mode is the production-like
//! mode for testing dynamic membership: each node registers under an etcd lease,
//! refreshes membership, and can revoke its own lease when sustained remote ask
//! failures show it cannot reach peers.

use std::sync::Arc;

use rakka::cluster::{ClusterNode, DiscoveryProvider, SelfFenceConfig, SelfFenceDetector};
use rakka::discovery_etcd::{
    connect as etcd_connect, EtcdDiscovery, EtcdDiscoveryConfig, EtcdDiscoverySession,
};
use rakka::sharding::ClusterNodeRuntime;
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::config::ExampleConfig;
use crate::discovery::{set_membership, MembershipView};
use crate::reachability::PeerReachability;
use crate::support::{
    current_timestamp_millis, example_error, ExampleResult, DEFAULT_DISCOVERY_POLL,
};

/// A connected etcd discovery provider and its async session.
pub struct EtcdHandle {
    /// Cache-backed discovery provider read for membership and diagnostics.
    pub discovery: EtcdDiscovery,
    /// Async session that registers, renews the lease, and refreshes membership.
    pub session: EtcdDiscoverySession,
}

/// Connects to etcd, registers this node, and applies the first membership snapshot.
pub async fn connect(
    config: &ExampleConfig,
    local_node: &ClusterNode,
    runtime: &mut ClusterNodeRuntime,
    membership: &MembershipView,
) -> ExampleResult<EtcdHandle> {
    let etcd_config = EtcdDiscoveryConfig::new(config.etcd_endpoints.clone())
        .with_prefix(config.etcd_prefix.clone())
        .with_lease_ttl_seconds(config.etcd_lease_ttl_seconds);
    let (discovery, session) = etcd_connect(etcd_config, local_node)
        .await
        .map_err(|error| example_error(format!("etcd connect failed: {error}")))?;
    let snapshot = discovery
        .discover(current_timestamp_millis())
        .map_err(|error| example_error(format!("etcd discover failed: {error}")))?;
    set_membership(membership, snapshot.nodes());
    runtime
        .apply_discovery(snapshot)
        .map_err(|error| example_error(format!("discovery apply failed: {error}")))?;
    Ok(EtcdHandle { discovery, session })
}

/// Runs the etcd discovery and optional self-fence loop until shutdown.
pub async fn run_etcd_discovery(
    mut session: EtcdDiscoverySession,
    discovery: EtcdDiscovery,
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    membership: MembershipView,
    reachability: PeerReachability,
    self_fence: Option<SelfFenceConfig>,
    shutdown: Arc<Notify>,
) {
    let mut detector = self_fence.map(SelfFenceDetector::new);
    let mut interval = tokio::time::interval(DEFAULT_DISCOVERY_POLL);
    loop {
        tokio::select! {
            () = shutdown.notified() => {
                let _ = session.leave().await;
                break;
            }
            _ = interval.tick() => {
                if let Err(error) = session.refresh().await {
                    eprintln!("etcd refresh failed: {error}");
                    continue;
                }
                let now = current_timestamp_millis();
                let snapshot = match discovery.discover(now) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        eprintln!("etcd discover failed: {error}");
                        continue;
                    }
                };
                let member_count = snapshot.nodes().len();
                set_membership(&membership, snapshot.nodes());
                {
                    let mut runtime = runtime.lock().await;
                    if let Err(error) = runtime.apply_discovery(snapshot) {
                        eprintln!("discovery apply failed: {error}");
                    }
                    if let Err(error) = runtime.tick(now) {
                        eprintln!("membership tick failed: {error}");
                    }
                }
                if let Some(detector) = detector.as_mut() {
                    if let Some(reachable) = reachability.evaluate_and_reset(member_count) {
                        if detector.observe(now, reachable).is_fenced() {
                            eprintln!(
                                "self-fence: cannot reach peers for {:?}; revoking etcd lease",
                                detector.config().fence_after()
                            );
                            let _ = session.leave().await;
                            shutdown.notify_one();
                            break;
                        }
                    }
                }
            }
        }
    }
}
