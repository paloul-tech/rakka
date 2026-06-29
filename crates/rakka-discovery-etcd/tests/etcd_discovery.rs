//! Gated integration test against a live etcd.
//!
//! Skipped unless `RAKKA_ETCD_TEST_ENDPOINTS` (comma-separated) is set, e.g.
//! `RAKKA_ETCD_TEST_ENDPOINTS=http://127.0.0.1:2379 \`
//! `  cargo test -p rakka-discovery-etcd --test etcd_discovery -- --nocapture`.

use std::time::Duration;

use rakka_cluster::{ClusterNode, DiscoveryProvider, NodeAddress, NodeId};
use rakka_discovery_etcd::{connect, EtcdDiscoveryConfig};

fn endpoints() -> Option<Vec<String>> {
    let raw = std::env::var("RAKKA_ETCD_TEST_ENDPOINTS").ok()?;
    let endpoints: Vec<String> = raw
        .split(',')
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();
    (!endpoints.is_empty()).then_some(endpoints)
}

fn node(logical: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical, format!("uid-{logical}")),
        NodeAddress::new(format!("{logical}.rakka.svc"), 2552),
    )
}

#[tokio::test]
async fn registers_discovers_peers_and_leaves_against_live_etcd() {
    let Some(endpoints) = endpoints() else {
        eprintln!("skipping: set RAKKA_ETCD_TEST_ENDPOINTS to run this test");
        return;
    };
    // Unique prefix per run so prior/concurrent runs do not interfere.
    let prefix = format!("/rakka-test/{}/", std::process::id());

    let local = node("rakka-0");
    let (discovery, mut session) = connect(
        EtcdDiscoveryConfig::new(endpoints.clone())
            .with_prefix(prefix.clone())
            .with_lease_ttl_seconds(5),
        &local,
    )
    .await
    .expect("connect local");

    // The initial snapshot already contains this node.
    let snapshot = discovery.discover(1).expect("discover");
    assert!(
        snapshot.nodes().iter().any(|n| n.id() == local.id()),
        "self should be registered"
    );

    // A second node registers under the same prefix and is discovered on refresh.
    let other = node("rakka-1");
    let (_other_discovery, mut other_session) = connect(
        EtcdDiscoveryConfig::new(endpoints)
            .with_prefix(prefix)
            .with_lease_ttl_seconds(5),
        &other,
    )
    .await
    .expect("connect other");

    session.refresh().await.expect("refresh after peer joins");
    let members = discovery.cached_members();
    assert!(
        members.iter().any(|n| n.id() == other.id()),
        "peer should be discovered"
    );
    assert!(members.len() >= 2);

    // Leaving revokes the peer's lease; after it propagates the peer is gone.
    other_session.leave().await.expect("peer leaves");
    tokio::time::sleep(Duration::from_millis(300)).await;
    session.refresh().await.expect("refresh after peer leaves");
    assert!(
        !discovery
            .cached_members()
            .iter()
            .any(|n| n.id() == other.id()),
        "left peer should be removed from membership"
    );

    session.leave().await.expect("local leaves");
}
