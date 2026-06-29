//! Dynamic etcd-backed discovery (Kubernetes autoscaling-friendly) plus
//! peer-reachability self-fencing.
//!
//! Membership is driven by the supported `rakka-discovery-etcd` provider: each
//! node registers under an etcd lease (the strongly-consistent membership
//! arbiter) and learns peers by ranging the prefix. When self-fencing is enabled
//! and this node cannot reach peers (see [`crate::reachability`]), it revokes its
//! lease and shuts down so the arbiter drops it — turning a reachability fault
//! into a consistent membership change rather than a silently mis-routed node.
//! See `docs/rakka-cluster-coordination-strategy.md`.

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

/// Connects to etcd, registers this node, and applies the first membership
/// snapshot before the node starts serving.
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

/// Runs the etcd discovery + self-fence loop until `shutdown` is signalled.
///
/// Each tick renews the lease, refreshes membership, advances the runtime, and
/// (when `self_fence` is set) evaluates peer reachability. On sustained
/// unreachability the node fences itself: it revokes its lease and signals
/// `shutdown` so the process leaves the cluster cleanly. On a normal shutdown
/// signal it simply revokes the lease and exits.
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
                    // Only feed the detector when there is evidence; idle windows
                    // must not reset an in-progress unreachable streak.
                    if let Some(reachable) = reachability.evaluate_and_reset(member_count) {
                        if detector.observe(now, reachable).is_fenced() {
                            eprintln!(
                                "self-fence: cannot reach peers for {:?}; revoking lease and leaving cluster",
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
