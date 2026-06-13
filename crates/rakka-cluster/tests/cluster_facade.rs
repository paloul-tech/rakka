//! Cluster extension facade tests.

use std::time::Duration;

use rakka_cluster::{
    Cluster, ClusterError, ClusterEvent, ClusterNode, ClusterSubscriptionReplay, MembershipConfig,
    MembershipState, NodeAddress, NodeId,
};
use rakka_core::ActorSystem;

#[test]
fn cluster_get_exposes_local_joining_member() {
    let system = ActorSystem::new("cluster-facade-test");
    let cluster = Cluster::get(&system);
    let state = cluster.state();
    let self_member = cluster
        .self_member()
        .expect("local member should be present");

    assert_eq!(state.local_node_id().logical_id(), "cluster-facade-test");
    assert_eq!(state.revision(), 0);
    assert_eq!(state.members().len(), 1);
    assert_eq!(self_member.state(), MembershipState::Joining);
}

#[tokio::test]
async fn manager_join_leave_and_down_emit_events() {
    let node = node("rakka-0", "uid-a", 25520);
    let cluster = Cluster::for_local_node(node.clone(), MembershipConfig::default());
    let mut subscription = cluster
        .subscriptions()
        .subscribe(ClusterSubscriptionReplay::LiveOnly);

    let join = cluster
        .manager()
        .join_self()
        .expect("local node should join");
    assert_eq!(join.state().revision(), 1);
    assert!(matches!(join.events(), [ClusterEvent::MemberUp { .. }]));
    assert_eq!(
        join.state().self_member().expect("self member").state(),
        MembershipState::Up
    );
    assert!(matches!(
        subscription.recv().await.expect("live up event"),
        ClusterEvent::MemberUp { .. }
    ));

    let leave = cluster
        .manager()
        .leave(node.id())
        .expect("local node should leave");
    assert!(matches!(
        leave.events(),
        [ClusterEvent::MemberLeaving { .. }]
    ));

    let down = cluster
        .manager()
        .down(node.id())
        .expect("local node should down");
    assert!(matches!(down.events(), [ClusterEvent::MemberDown { .. }]));
    assert_eq!(
        down.state().self_member().expect("self member").state(),
        MembershipState::Down
    );
}

#[test]
fn manager_join_seed_nodes_discovers_and_marks_nodes_up() {
    let node_a = node("rakka-0", "uid-a", 25520);
    let node_b = node("rakka-1", "uid-b", 25521);
    let cluster = Cluster::for_local_node(
        node_a.clone(),
        MembershipConfig::new(2, Duration::from_secs(10), Duration::from_secs(30)),
    );

    let update = cluster
        .manager()
        .join_seed_nodes([node_a.clone(), node_b.clone()])
        .expect("seed nodes should join");

    assert_eq!(update.state().members().len(), 2);
    assert_eq!(
        update.state().member(node_a.id()).expect("node a").state(),
        MembershipState::Up
    );
    assert_eq!(
        update.state().member(node_b.id()).expect("node b").state(),
        MembershipState::Up
    );
    assert!(matches!(
        update.events(),
        [
            ClusterEvent::MemberDiscovered { .. },
            ClusterEvent::MemberUp { .. },
            ClusterEvent::MemberUp { .. }
        ]
    ));
}

#[tokio::test]
async fn subscription_replays_initial_state_then_live_events() {
    let node = node("rakka-0", "uid-a", 25520);
    let cluster = Cluster::for_local_node(node.clone(), MembershipConfig::default());
    let mut subscription = cluster
        .subscriptions()
        .subscribe(ClusterSubscriptionReplay::InitialState);

    let initial = subscription.recv().await.expect("initial state event");
    let ClusterEvent::CurrentState { state } = initial else {
        panic!("expected current state replay");
    };
    assert_eq!(state.revision(), 0);
    assert_eq!(
        state.self_member().expect("self member").state(),
        MembershipState::Joining
    );

    cluster
        .manager()
        .join_self()
        .expect("local node should join");
    assert!(matches!(
        subscription.recv().await.expect("live event"),
        ClusterEvent::MemberUp { .. }
    ));
}

#[tokio::test]
async fn subscription_replays_initial_events_without_current_state() {
    let node_a = node("rakka-0", "uid-a", 25520);
    let node_b = node("rakka-1", "uid-b", 25521);
    let cluster = Cluster::for_local_node(node_a.clone(), MembershipConfig::default());

    cluster.manager().join_self().expect("node a joins");
    cluster
        .manager()
        .join(node_b.clone())
        .expect("node b joins");

    let mut subscription = cluster
        .subscriptions()
        .subscribe(ClusterSubscriptionReplay::InitialEvents);
    assert!(matches!(
        subscription.recv().await.expect("first replayed event"),
        ClusterEvent::MemberUp { .. }
    ));
    assert!(matches!(
        subscription.recv().await.expect("second replayed event"),
        ClusterEvent::MemberDiscovered { .. }
    ));
    assert!(matches!(
        subscription.recv().await.expect("third replayed event"),
        ClusterEvent::MemberUp { .. }
    ));

    cluster.manager().leave(node_b.id()).expect("node b leaves");
    assert!(matches!(
        subscription.recv().await.expect("live leave event"),
        ClusterEvent::MemberLeaving { .. }
    ));
}

#[tokio::test]
async fn live_only_subscription_does_not_replay_history() {
    let node = node("rakka-0", "uid-a", 25520);
    let cluster = Cluster::for_local_node(node.clone(), MembershipConfig::default());
    cluster.manager().join_self().expect("node joins");

    let mut subscription = cluster
        .subscriptions()
        .subscribe(ClusterSubscriptionReplay::LiveOnly);
    let no_replay = tokio::time::timeout(Duration::from_millis(10), subscription.recv()).await;
    assert!(
        no_replay.is_err(),
        "live-only subscription replayed history"
    );

    cluster.manager().leave(node.id()).expect("node leaves");
    assert!(matches!(
        subscription.recv().await.expect("live leave event"),
        ClusterEvent::MemberLeaving { .. }
    ));
}

#[test]
fn invalid_transition_fails_closed() {
    let node = node("rakka-0", "uid-a", 25520);
    let cluster = Cluster::for_local_node(node.clone(), MembershipConfig::default());
    cluster
        .manager()
        .leave(node.id())
        .expect("joining can leave");

    let error = cluster
        .manager()
        .join_self()
        .expect_err("leaving node should not rejoin directly");
    assert!(matches!(
        error,
        ClusterError::InvalidTransition {
            from: MembershipState::Leaving,
            to: MembershipState::Up,
            ..
        }
    ));
}

fn node(logical_id: &str, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
}
