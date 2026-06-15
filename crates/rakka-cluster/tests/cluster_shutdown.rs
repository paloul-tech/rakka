//! Cluster coordinated shutdown hook tests.

use rakka_cluster::{
    register_cluster_down_self_task, register_cluster_leave_task, Cluster, ClusterNode,
    MembershipConfig, MembershipState, NodeAddress, NodeId,
};
use rakka_core::{
    CoordinatedShutdown, CoordinatedShutdownReason, ShutdownOutcome, ShutdownPhase,
    ShutdownTaskStatus,
};

#[tokio::test]
async fn coordinated_shutdown_marks_local_cluster_member_leaving() {
    let cluster = Cluster::for_local_node(node("rakka-0", "uid-a"), MembershipConfig::default());
    cluster.manager().join_self().unwrap();
    let shutdown = CoordinatedShutdown::new();

    register_cluster_leave_task(&shutdown, "leave-cluster", cluster.clone()).unwrap();

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();

    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    assert_eq!(
        cluster.self_member().expect("self member").state(),
        MembershipState::Leaving
    );
    assert_eq!(
        report
            .phases()
            .iter()
            .find(|phase| phase.phase() == &ShutdownPhase::leave_cluster())
            .and_then(|phase| phase.tasks().first())
            .map(|task| task.status()),
        Some(ShutdownTaskStatus::Completed)
    );
}

#[tokio::test]
async fn coordinated_shutdown_can_explicitly_down_local_member() {
    let cluster = Cluster::for_local_node(node("rakka-0", "uid-a"), MembershipConfig::default());
    cluster.manager().join_self().unwrap();
    let shutdown = CoordinatedShutdown::new();

    register_cluster_down_self_task(&shutdown, "down-self", cluster.clone()).unwrap();

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();

    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    assert_eq!(
        cluster.self_member().expect("self member").state(),
        MembershipState::Down
    );
}

fn node(logical_id: &str, incarnation: &str) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", 25520),
    )
}
