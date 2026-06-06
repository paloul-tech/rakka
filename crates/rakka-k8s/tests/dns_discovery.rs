//! Integration tests for Kubernetes DNS discovery contact points.

use rakka_cluster::{ClusterProtocol, DiscoveryProvider, ProtocolVersion};
use rakka_k8s::{KubernetesDnsDiscovery, KubernetesDnsDiscoveryConfig, KubernetesPodIdentity};

#[test]
fn headless_service_config_builds_direct_pod_dns_hosts() {
    let config = KubernetesDnsDiscoveryConfig::new("default", "rakka-internal", 2552);

    assert_eq!(
        config.pod_host("rakka-0"),
        "rakka-0.rakka-internal.default.svc.cluster.local"
    );

    let short_config = KubernetesDnsDiscoveryConfig::new("default", "rakka-internal", 2552)
        .with_cluster_domain("");
    assert_eq!(
        short_config.pod_host("rakka-0"),
        "rakka-0.rakka-internal.default.svc"
    );
}

#[test]
fn kubernetes_dns_discovery_maps_pods_to_cluster_nodes() {
    let protocol = ClusterProtocol::v1();
    let discovery = KubernetesDnsDiscovery::new(
        KubernetesDnsDiscoveryConfig::new("default", "rakka-internal", 2552),
        [
            KubernetesPodIdentity::new("rakka-1", "uid-b"),
            KubernetesPodIdentity::new("rakka-0", "uid-a"),
        ],
    )
    .with_protocol(protocol);

    let snapshot = discovery.discover(42).unwrap();

    assert_eq!(snapshot.provider(), "kubernetes-dns");
    assert_eq!(snapshot.observed_at_millis(), 42);
    assert_eq!(snapshot.nodes().len(), 2);
    assert_eq!(snapshot.nodes()[0].id().logical_id(), "rakka-0");
    assert_eq!(snapshot.nodes()[0].id().incarnation(), "uid-a");
    assert_eq!(
        snapshot.nodes()[0].address().host(),
        "rakka-0.rakka-internal.default.svc.cluster.local"
    );
    assert_eq!(snapshot.nodes()[0].address().port(), 2552);
    assert_eq!(
        snapshot.nodes()[0].protocol().version(),
        ProtocolVersion::new(1, 0)
    );
}
