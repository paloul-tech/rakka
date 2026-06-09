//! Integration tests for cluster membership and discovery foundations.

use std::time::Duration;

use rakka_cluster::{
    ClusterError, ClusterMembership, ClusterNode, ClusterProtocol, CompatibilityRange,
    DiscoveryProvider, DiscoverySnapshot, LocalDiscovery, MembershipConfig, MembershipEvent,
    MembershipState, NodeAddress, NodeId, ProtocolVersion, StaticDiscovery,
};
use rakka_core::{InMemoryMetricsRecorder, METRIC_CLUSTER_MEMBERS};

fn node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), port),
    )
}

fn config(min_contact_points: usize) -> MembershipConfig {
    MembershipConfig::new(
        min_contact_points,
        Duration::from_millis(50),
        Duration::from_millis(100),
    )
}

#[test]
fn static_discovery_deduplicates_and_sorts_nodes() {
    let discovery = StaticDiscovery::new([
        node("rakka-1", "uid-b", 2552),
        node("rakka-0", "uid-a", 2552),
        node("rakka-1", "uid-b", 2553),
    ]);

    let snapshot = discovery.discover(10).unwrap();

    assert_eq!(snapshot.provider(), "static");
    assert_eq!(snapshot.observed_at_millis(), 10);
    assert_eq!(snapshot.nodes().len(), 2);
    assert_eq!(snapshot.nodes()[0].id().logical_id(), "rakka-0");
    assert_eq!(snapshot.nodes()[1].address().port(), 2553);
}

#[test]
fn membership_tracks_join_up_leave_and_remove() {
    let local = node("rakka-0", "uid-a", 2552);
    let remote = node("rakka-1", "uid-b", 2552);
    let remote_id = remote.id().clone();
    let mut membership = ClusterMembership::new(local, config(2));

    assert!(!membership.has_min_contact_points());

    let discovered = membership
        .record_discovery(DiscoverySnapshot::new("static", 10, [remote]))
        .unwrap();

    assert_eq!(
        discovered,
        vec![MembershipEvent::MemberDiscovered {
            node_id: remote_id.clone(),
        }]
    );
    assert!(membership.has_min_contact_points());

    let local_id = membership.local_node_id().clone();
    assert_eq!(
        membership.mark_up(&local_id, 11).unwrap(),
        Some(MembershipEvent::MemberUp { node_id: local_id })
    );
    assert_eq!(
        membership.mark_up(&remote_id, 12).unwrap(),
        Some(MembershipEvent::MemberUp {
            node_id: remote_id.clone(),
        })
    );
    assert_eq!(membership.routable_members().len(), 2);

    assert_eq!(
        membership.mark_leaving(&remote_id, 20).unwrap(),
        Some(MembershipEvent::MemberLeaving {
            node_id: remote_id.clone(),
        })
    );
    assert_eq!(membership.routable_members().len(), 1);
    assert_eq!(
        membership.remove(&remote_id, 30).unwrap(),
        Some(MembershipEvent::MemberRemoved {
            node_id: remote_id.clone(),
        })
    );
    assert_eq!(
        membership.member(&remote_id).unwrap().state(),
        MembershipState::Removed
    );
}

#[test]
fn membership_operational_snapshot_records_member_counts_by_state() {
    let local = node("rakka-0", "uid-a", 2552);
    let remote = node("rakka-1", "uid-b", 2552);
    let mut membership = ClusterMembership::new(local, config(1));
    membership
        .record_discovery(DiscoverySnapshot::new("static", 1, [remote]))
        .expect("remote discovery should be accepted");
    let local_id = membership.local_node_id().clone();
    membership.mark_up(&local_id, 2).expect("local up");

    let recorder = InMemoryMetricsRecorder::new();
    let snapshot = membership.record_metrics(&recorder);

    assert_eq!(snapshot.total_members(), 2);
    assert!(snapshot
        .states()
        .iter()
        .any(|state| state.state() == MembershipState::Up && state.count() == 1));
    assert!(snapshot
        .states()
        .iter()
        .any(|state| state.state() == MembershipState::Joining && state.count() == 1));
    assert!(recorder
        .snapshot()
        .observations_named(METRIC_CLUSTER_MEMBERS)
        .iter()
        .any(|observation| observation.attribute("state") == Some("up")));
}

#[test]
fn failure_detection_marks_unreachable_then_down() {
    let local = node("rakka-0", "uid-a", 2552);
    let remote = node("rakka-1", "uid-b", 2552);
    let remote_id = remote.id().clone();
    let mut membership = ClusterMembership::new(local, config(1));

    membership
        .record_discovery(DiscoverySnapshot::new("static", 10, [remote]))
        .unwrap();
    membership.mark_up(&remote_id, 10).unwrap();

    assert!(membership.tick(59).is_empty());
    assert_eq!(
        membership.tick(60),
        vec![MembershipEvent::MemberUnreachable {
            node_id: remote_id.clone(),
        }]
    );
    assert_eq!(
        membership.member(&remote_id).unwrap().state(),
        MembershipState::Unreachable
    );
    assert_eq!(
        membership.tick(160),
        vec![MembershipEvent::MemberDown {
            node_id: remote_id.clone(),
        }]
    );
    assert_eq!(
        membership.member(&remote_id).unwrap().state(),
        MembershipState::Down
    );
}

#[test]
fn heartbeat_recovers_an_unreachable_member() {
    let local = node("rakka-0", "uid-a", 2552);
    let remote = node("rakka-1", "uid-b", 2552);
    let remote_id = remote.id().clone();
    let mut membership = ClusterMembership::new(local, config(1));

    membership
        .record_discovery(DiscoverySnapshot::new("static", 10, [remote]))
        .unwrap();
    membership.mark_up(&remote_id, 10).unwrap();
    membership.tick(60);

    assert_eq!(
        membership.heartbeat(&remote_id, 70).unwrap(),
        Some(MembershipEvent::MemberReachable {
            node_id: remote_id.clone(),
        })
    );
    assert_eq!(
        membership.member(&remote_id).unwrap().state(),
        MembershipState::Up
    );
}

#[test]
fn incarnation_uid_distinguishes_restarted_pods() {
    let local = node("rakka-0", "uid-a", 2552);
    let old_remote = node("rakka-1", "uid-b", 2552);
    let restarted_remote = node("rakka-1", "uid-c", 2552);
    let mut membership = ClusterMembership::new(local, config(1));

    let events = membership
        .record_discovery(DiscoverySnapshot::new(
            "static",
            10,
            [old_remote, restarted_remote],
        ))
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(membership.snapshot().members().len(), 3);
    assert!(membership
        .snapshot()
        .members()
        .iter()
        .any(|member| member.node().id().incarnation() == "uid-b"));
    assert!(membership
        .snapshot()
        .members()
        .iter()
        .any(|member| member.node().id().incarnation() == "uid-c"));
}

#[test]
fn incompatible_node_protocol_is_rejected() {
    let local = node("rakka-0", "uid-a", 2552);
    let incompatible_protocol = ClusterProtocol::new(
        ProtocolVersion::new(2, 0),
        CompatibilityRange::new(ProtocolVersion::new(2, 0), ProtocolVersion::new(2, 0)),
    );
    let remote = node("rakka-1", "uid-b", 2552).with_protocol(incompatible_protocol);
    let remote_id = remote.id().clone();
    let mut membership = ClusterMembership::new(local, config(1));

    let error = membership
        .record_discovery(DiscoverySnapshot::new("static", 10, [remote]))
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterError::IncompatibleNode {
            node_id,
            remote,
            ..
        } if node_id == remote_id && remote == incompatible_protocol
    ));
    assert_eq!(membership.snapshot().members().len(), 1);
}

#[test]
fn n_to_n_plus_one_protocols_can_coexist_during_rolling_update() {
    let rolling_window = CompatibilityRange::n_to_n_plus_one(1, 0);
    let local_protocol = ClusterProtocol::n_to_n_plus_one(ProtocolVersion::new(1, 0), 0);
    let remote_protocol = ClusterProtocol::n_to_n_plus_one(ProtocolVersion::new(1, 1), 0);
    let local = node("rakka-0", "uid-a", 2552).with_protocol(local_protocol);
    let remote = node("rakka-1", "uid-b", 2552).with_protocol(remote_protocol);
    let remote_id = remote.id().clone();
    let mut membership = ClusterMembership::new(local, config(1));

    let events = membership
        .record_discovery(DiscoverySnapshot::new("static", 10, [remote]))
        .unwrap();

    assert_eq!(local_protocol.compatible_with(), rolling_window);
    assert_eq!(remote_protocol.compatible_with(), rolling_window);
    assert_eq!(
        events,
        vec![MembershipEvent::MemberDiscovered {
            node_id: remote_id.clone(),
        }]
    );
    assert_eq!(
        membership.mark_up(&remote_id, 11).unwrap(),
        Some(MembershipEvent::MemberUp { node_id: remote_id })
    );
    assert_eq!(membership.routable_members().len(), 1);
}

#[test]
fn exact_protocol_policy_rejects_minor_version_mismatch() {
    let local_protocol = ClusterProtocol::exact(ProtocolVersion::new(1, 0));
    let remote_protocol = ClusterProtocol::exact(ProtocolVersion::new(1, 1));
    let local = node("rakka-0", "uid-a", 2552).with_protocol(local_protocol);
    let remote = node("rakka-1", "uid-b", 2552).with_protocol(remote_protocol);
    let remote_id = remote.id().clone();
    let mut membership = ClusterMembership::new(local, config(1));

    let error = membership
        .record_discovery(DiscoverySnapshot::new("static", 10, [remote]))
        .unwrap_err();

    assert!(matches!(
        error,
        ClusterError::IncompatibleNode {
            node_id,
            local,
            remote,
        } if node_id == remote_id && local == local_protocol && remote == remote_protocol
    ));
    assert_eq!(membership.snapshot().members().len(), 1);
}

#[test]
fn local_discovery_registry_updates_snapshots() {
    let discovery = LocalDiscovery::new();
    let first = node("rakka-0", "uid-a", 2552);
    let second = node("rakka-1", "uid-b", 2552);
    let first_id = first.id().clone();

    discovery.register(first).unwrap();
    discovery.register(second).unwrap();
    assert_eq!(discovery.discover(10).unwrap().nodes().len(), 2);

    let removed = discovery.unregister(&first_id).unwrap().unwrap();

    assert_eq!(removed.id(), &first_id);
    let snapshot = discovery.discover(20).unwrap();
    assert_eq!(snapshot.nodes().len(), 1);
    assert_eq!(snapshot.nodes()[0].id().logical_id(), "rakka-1");
}
