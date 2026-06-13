//! Clustered receptionist propagation tests.

use std::time::Duration;

use rakka_cluster::{
    Cluster, ClusterNode, ClusteredReceptionist, ClusteredReceptionistSettings, MembershipConfig,
    MembershipState, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, Receptionist,
    Routers, ServiceKey,
};
use tokio::sync::mpsc;

#[tokio::test]
async fn propagated_registration_appears_in_remote_listing() {
    let fixture = ClusteredReceptionistFixture::new("propagated-registration");
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, _received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 0, delivered);
    let _registration = fixture
        .receptionist_a
        .register(&key, worker.clone())
        .expect("worker should register on node a");

    assert!(fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 1)
        .expect("listing should propagate"));

    let remote_listing = fixture
        .receptionist_b
        .find(&key)
        .expect("remote listing should resolve");
    assert_eq!(remote_listing.len(), 1);
    assert_eq!(remote_listing.revision(), 1);
    assert!(remote_listing.contains(&worker));

    fixture.shutdown();
}

#[tokio::test]
async fn deregistration_propagates_empty_listing() {
    let fixture = ClusteredReceptionistFixture::new("deregistration");
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, _received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 0, delivered);
    let registration = fixture
        .receptionist_a
        .register(&key, worker)
        .expect("worker should register on node a");
    fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 1)
        .expect("initial listing should propagate");

    assert!(registration
        .deregister()
        .expect("worker should deregister on node a"));
    assert!(fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 2)
        .expect("empty listing should propagate"));

    assert!(fixture
        .receptionist_b
        .find(&key)
        .expect("remote listing should resolve")
        .is_empty());

    fixture.shutdown();
}

#[tokio::test]
async fn down_node_prunes_remote_routees() {
    let fixture = ClusteredReceptionistFixture::new("down-prunes");
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, _received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 0, delivered);
    let _registration = fixture
        .receptionist_a
        .register(&key, worker)
        .expect("worker should register on node a");
    fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 1)
        .expect("initial listing should propagate");

    fixture
        .cluster_b
        .manager()
        .down(&fixture.node_a.id().clone())
        .expect("node a should be marked down from node b");
    assert_eq!(fixture.clustered_b.prune_unreachable_members(), 1);

    assert!(fixture
        .receptionist_b
        .find(&key)
        .expect("remote listing should resolve")
        .is_empty());
    assert_eq!(
        fixture
            .cluster_b
            .state()
            .member(fixture.node_a.id())
            .expect("node a should be known")
            .state(),
        MembershipState::Down
    );

    fixture.shutdown();
}

#[tokio::test]
async fn stale_remote_listings_expire_by_ttl() {
    let fixture = ClusteredReceptionistFixture::with_b_settings(
        "ttl-expiry",
        ClusteredReceptionistSettings::default().with_remote_listing_ttl(Duration::from_millis(10)),
    );
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, _received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 0, delivered);
    let _registration = fixture
        .receptionist_a
        .register(&key, worker)
        .expect("worker should register on node a");
    fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 5)
        .expect("initial listing should propagate");

    assert_eq!(fixture.clustered_b.expire_stale_listings(14), 0);
    assert_eq!(fixture.clustered_b.expire_stale_listings(16), 1);
    assert!(fixture
        .receptionist_b
        .find(&key)
        .expect("remote listing should resolve")
        .is_empty());

    fixture.shutdown();
}

#[tokio::test]
async fn same_version_publication_refreshes_ttl() {
    let fixture = ClusteredReceptionistFixture::with_b_settings(
        "ttl-refresh",
        ClusteredReceptionistSettings::default().with_remote_listing_ttl(Duration::from_millis(10)),
    );
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, _received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 0, delivered);
    let _registration = fixture
        .receptionist_a
        .register(&key, worker)
        .expect("worker should register on node a");
    fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 5)
        .expect("initial listing should propagate");

    assert!(!fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 15)
        .expect("same-version listing should refresh timestamp without routee change"));
    assert_eq!(fixture.clustered_b.expire_stale_listings(24), 0);
    assert_eq!(fixture.clustered_b.expire_stale_listings(26), 1);

    fixture.shutdown();
}

#[tokio::test]
async fn stale_remote_version_does_not_overwrite_newer_listing() {
    let fixture = ClusteredReceptionistFixture::new("stale-version");
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, _received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 0, delivered);
    let registration = fixture
        .receptionist_a
        .register(&key, worker.clone())
        .expect("worker should register on node a");
    let stale = fixture
        .clustered_a
        .publish_local(&key, 1)
        .expect("local listing should publish")
        .expect("clustered receptionist is enabled");
    fixture
        .clustered_b
        .apply_remote(stale.clone())
        .expect("initial listing should apply");

    registration
        .deregister()
        .expect("worker should deregister on node a");
    fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 2)
        .expect("newer empty listing should apply");
    assert!(!fixture
        .clustered_b
        .apply_remote(stale)
        .expect("stale listing should be ignored"));

    assert!(fixture
        .receptionist_b
        .find(&key)
        .expect("remote listing should resolve")
        .is_empty());

    fixture.shutdown();
}

#[tokio::test]
async fn group_router_routes_to_propagated_listing() {
    let fixture = ClusteredReceptionistFixture::new("group-router");
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, mut received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 7, delivered);
    let _registration = fixture
        .receptionist_a
        .register(&key, worker)
        .expect("worker should register on node a");
    fixture
        .clustered_a
        .propagate_to(&fixture.clustered_b, &key, 1)
        .expect("listing should propagate");

    let router = Routers::group(key)
        .spawn(&fixture.system_b, "workers-group")
        .expect("group router should spawn");
    router
        .tell(WorkCommand::Record { sequence: 42 })
        .expect("router should route to propagated worker");

    assert_eq!(receive_records(&mut received, 1).await, vec![(42, 7)]);

    fixture.shutdown();
}

#[tokio::test]
async fn listing_size_limit_fails_closed() {
    let fixture = ClusteredReceptionistFixture::with_b_settings(
        "listing-limit",
        ClusteredReceptionistSettings::default().with_max_routees_per_listing(0),
    );
    let key = ServiceKey::<WorkCommand>::new("workers");
    let (delivered, _received) = mpsc::unbounded_channel();
    let worker = fixture.spawn_worker_on_a("worker-a", 0, delivered);
    let _registration = fixture
        .receptionist_a
        .register(&key, worker)
        .expect("worker should register on node a");
    let listing = fixture
        .clustered_a
        .publish_local(&key, 1)
        .expect("local listing should publish")
        .expect("clustered receptionist is enabled");

    let error = fixture
        .clustered_b
        .apply_remote(listing)
        .expect_err("oversized listing should fail closed");
    assert_eq!(error.code(), "receptionist-listing-too-large");

    fixture.shutdown();
}

struct ClusteredReceptionistFixture {
    system_a: ActorSystem,
    system_b: ActorSystem,
    node_a: ClusterNode,
    cluster_b: Cluster,
    receptionist_a: Receptionist,
    receptionist_b: Receptionist,
    clustered_a: ClusteredReceptionist,
    clustered_b: ClusteredReceptionist,
}

impl ClusteredReceptionistFixture {
    fn new(name: &str) -> Self {
        Self::with_b_settings(name, ClusteredReceptionistSettings::default())
    }

    fn with_b_settings(name: &str, b_settings: ClusteredReceptionistSettings) -> Self {
        let system_a = ActorSystem::new(format!("{name}-a"));
        let system_b = ActorSystem::new(format!("{name}-b"));
        let node_a = node(format!("{name}-a"), "uid-a", 25_520);
        let node_b = node(format!("{name}-b"), "uid-b", 25_521);
        let cluster_a = cluster_with_nodes(node_a.clone(), node_b.clone());
        let cluster_b = cluster_with_nodes(node_b, node_a.clone());
        let receptionist_a = Receptionist::get(&system_a);
        let receptionist_b = Receptionist::get(&system_b);
        let clustered_a = ClusteredReceptionist::new(
            cluster_a,
            receptionist_a.clone(),
            ClusteredReceptionistSettings::default(),
        );
        let clustered_b =
            ClusteredReceptionist::new(cluster_b.clone(), receptionist_b.clone(), b_settings);

        Self {
            system_a,
            system_b,
            node_a,
            cluster_b,
            receptionist_a,
            receptionist_b,
            clustered_a,
            clustered_b,
        }
    }

    fn spawn_worker_on_a(
        &self,
        name: &str,
        id: usize,
        delivered: mpsc::UnboundedSender<(usize, usize)>,
    ) -> rakka_core::ActorRef<WorkCommand> {
        self.system_a
            .spawn_actor(name, RecordingWorker { id, delivered })
            .expect("worker should spawn on node a")
    }

    fn shutdown(self) {
        self.system_a.shutdown();
        self.system_b.shutdown();
    }
}

fn cluster_with_nodes(local: ClusterNode, remote: ClusterNode) -> Cluster {
    let cluster = Cluster::for_local_node(
        local.clone(),
        MembershipConfig::new(2, Duration::from_secs(10), Duration::from_secs(30)),
    );
    cluster
        .manager()
        .join_seed_nodes([local, remote])
        .expect("cluster should know both nodes");
    cluster
}

fn node(logical_id: impl Into<String>, incarnation: &str, port: u16) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
}

#[derive(Debug)]
enum WorkCommand {
    Record { sequence: usize },
}

struct RecordingWorker {
    id: usize,
    delivered: mpsc::UnboundedSender<(usize, usize)>,
}

impl Actor for RecordingWorker {
    type Msg = WorkCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                WorkCommand::Record { sequence } => {
                    let _ = self.delivered.send((sequence, self.id));
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

async fn receive_records(
    received: &mut mpsc::UnboundedReceiver<(usize, usize)>,
    count: usize,
) -> Vec<(usize, usize)> {
    let mut observed = Vec::new();
    for _ in 0..count {
        observed.push(
            tokio::time::timeout(Duration::from_secs(1), received.recv())
                .await
                .expect("timed out waiting for worker record")
                .expect("worker record channel closed"),
        );
    }
    observed
}
