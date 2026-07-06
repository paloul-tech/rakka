//! Slice 3.6 owner movement and recovery tests.
//!
//! Each test drives one or two in-process cluster nodes over networked
//! loopback remoting with shared file-backed durable stores, covering:
//! acceptance on one node with read/cancel from another, owner shutdown with
//! recovery on the surviving owner, ownership movement with an in-flight
//! command retried idempotently, idle passivation with lazy recovery, and
//! duplicate message retry after owner movement.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use rakka::cluster::{ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId};
use rakka::prelude::{ActorSystem, ClusterSharding};
use rakka::remote::TcpRemoteTransportConfig;
use rakka::sharding::{ClusterNodeRuntime, EntityId, EntityType};
use tower::ServiceExt;

use crate::a2a_handler::{A2ARunRouter, HeaderObserver, RakkaA2ARequestHandler};
use crate::agent_card::build_agent_card;
use crate::codec::serialization_registry;
use crate::config::{DiscoveryProviderKind, ExampleConfig};
use crate::durable_stores::build_stores;
use crate::push_config::A2APushConfigStore;
use crate::reachability::PeerReachability;
use crate::server::{router, AppState};
use crate::sharded_run_entity::{a2a_run_entity_key, init_a2a_run_sharding, A2ARunHost};
use crate::support::{current_timestamp_millis, ENTITY_TYPE, RUN_ENTITY_IDLE_PASSIVATION};
use crate::task_projection::InMemoryA2ATaskProjectionStore;
use crate::workflow::demo_workflow;

/// One in-process cluster node: actor system, node runtime, and HTTP app.
struct TestNode {
    logical_id: String,
    system: ActorSystem,
    runtime: ClusterNodeRuntime,
    app: Router,
    handler: Arc<RakkaA2ARequestHandler>,
}

impl TestNode {
    async fn terminate(self) {
        self.system.terminate().await.expect("system terminates");
    }
}

fn test_config(logical_id: &str, state_dir: &Path) -> ExampleConfig {
    ExampleConfig {
        bind_host: "127.0.0.1".parse().expect("loopback address"),
        advertise_host: "127.0.0.1".to_string(),
        rakka_port: 0,
        http_port: 0,
        node_logical_id: logical_id.to_string(),
        node_incarnation: "test".to_string(),
        discovery_provider: DiscoveryProviderKind::File,
        discovery_dir: std::env::temp_dir(),
        etcd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
        etcd_prefix: crate::support::DEFAULT_ETCD_PREFIX.to_string(),
        etcd_lease_ttl_seconds: crate::support::DEFAULT_ETCD_LEASE_TTL_SECONDS,
        state_dir: state_dir.to_path_buf(),
        self_fence: false,
        self_fence_after: Duration::from_secs(15),
        self_fence_rejoin_after: Duration::from_secs(10),
        public_url: None,
    }
}

/// Boots one node sharing durable state under `state_dir`.
///
/// Returns `None` (skipping the test) when loopback binding is unavailable
/// in the sandbox, mirroring the other networked tests in this repository.
async fn build_node(
    logical_id: &str,
    state_dir: &Path,
    idle_passivation: Duration,
) -> Option<TestNode> {
    let config = test_config(logical_id, state_dir);
    let workflow = demo_workflow();
    let system = ActorSystem::new(format!("cluster-tests-{logical_id}"));
    let local_node = ClusterNode::new(
        NodeId::new(logical_id, "test"),
        NodeAddress::new("127.0.0.1", 0),
    );
    let runtime = ClusterNodeRuntime::builder(local_node)
        .with_membership_config(MembershipConfig::new(
            1,
            Duration::from_secs(10),
            Duration::from_secs(30),
        ))
        .with_transport_config(
            TcpRemoteTransportConfig::new().bind_addr("127.0.0.1:0".parse().expect("bind addr")),
        )
        .advertise_bound_addr(true)
        .with_registry(serialization_registry().expect("registry"))
        .build()
        .await;
    let Ok(mut runtime) = runtime else {
        eprintln!("skipping cluster test; loopback bind unavailable");
        system.terminate().await.expect("system terminates");
        return None;
    };
    let ask_client = runtime.ask_client();
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime).expect("sharding");
    let key = a2a_run_entity_key().expect("entity key");
    let (run_store, workflow_store, push_config_store) = build_stores(&config);
    let task_store = InMemoryA2ATaskProjectionStore::local();
    let push_configs = A2APushConfigStore::new(push_config_store);
    init_a2a_run_sharding(
        &system,
        &mut runtime,
        &sharding,
        key.clone(),
        A2ARunHost {
            workflow: workflow.clone(),
            run_store: run_store.clone(),
            workflow_store: workflow_store.clone(),
            task_store: task_store.clone(),
            push_configs: push_configs.clone(),
            idle_passivation,
        },
    )
    .expect("sharding init");
    let agent_card = build_agent_card(&config);
    let route_helper = A2ARunRouter::new(sharding, key, ask_client, PeerReachability::new());
    let handler = Arc::new(
        RakkaA2ARequestHandler::new(
            agent_card.clone(),
            workflow,
            task_store,
            run_store,
            workflow_store,
            push_configs,
            HeaderObserver::default(),
        )
        .with_router(route_helper),
    );
    let app = router(AppState {
        node_id: format!("{logical_id}#test"),
        membership: Arc::new(std::sync::Mutex::new(vec![format!("{logical_id}#test")])),
        agent_card,
        header_observer: HeaderObserver::default(),
        handler: handler.clone(),
    });
    Some(TestNode {
        logical_id: logical_id.to_string(),
        system,
        runtime,
        app,
        handler,
    })
}

/// Applies one membership snapshot listing `members` to every node.
fn apply_membership(nodes: &mut [&mut TestNode], members: Vec<ClusterNode>, version: u64) {
    for node in nodes {
        node.runtime
            .apply_discovery(DiscoverySnapshot::new(
                "cluster-tests",
                version,
                members.clone(),
            ))
            .expect("discovery applies");
    }
}

fn member_nodes(nodes: &[&mut TestNode]) -> Vec<ClusterNode> {
    nodes
        .iter()
        .map(|node| node.runtime.local_node().clone())
        .collect()
}

/// Resolves which logical node owns the shard for `task_id`.
fn owner_logical_id(node: &TestNode, task_id: &str) -> String {
    node.runtime
        .sharding()
        .coordinator(&EntityType::new(ENTITY_TYPE))
        .expect("coordinator")
        .owner_for_entity(&EntityId::new(task_id))
        .expect("owner")
        .logical_id()
        .to_string()
}

fn temp_state_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rakka-a2a-cluster-tests-{label}-{}-{}",
        std::process::id(),
        current_timestamp_millis()
    ))
}

fn send_body(
    message_id: &str,
    task_id: Option<&str>,
    return_immediately: bool,
) -> serde_json::Value {
    let mut message = serde_json::json!({
        "messageId": message_id,
        "role": "ROLE_USER",
        "parts": [{"text": "hello owner movement"}]
    });
    if let Some(task_id) = task_id {
        message["taskId"] = serde_json::Value::String(task_id.to_string());
    }
    serde_json::json!({
        "message": message,
        "configuration": { "returnImmediately": return_immediately },
        "tenant": "tenant-a"
    })
}

async fn http(
    app: &Router,
    method: &str,
    uri: &str,
    body: Option<&serde_json::Value>,
    tenant: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    if let Some(tenant) = tenant {
        request = request.header("x-rakka-tenant", tenant);
    }
    let request = request
        .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
        .expect("request builds");
    let response = app.clone().oneshot(request).await.expect("request runs");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let value = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, value)
}

async fn send_ok(app: &Router, body: &serde_json::Value) -> serde_json::Value {
    let (status, value) = http(
        app,
        "POST",
        "/a2a/message:send",
        Some(body),
        Some("tenant-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "send failed: {value}");
    value
}

/// Slice 3.6: task accepted through one public node is readable and
/// cancelable through the other, including an unscoped (no-tenant) read.
#[tokio::test]
async fn task_accepted_on_one_node_reads_and_cancels_from_another() {
    let state_dir = temp_state_dir("cross-node");
    let Some(mut node_a) = build_node("move-a", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        return;
    };
    let Some(mut node_b) = build_node("move-b", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let sent = send_ok(&node_a.app, &send_body("cross-node-message", None, true)).await;
    let task_id = sent["task"]["id"].as_str().expect("task id").to_string();
    assert_eq!(sent["task"]["status"]["state"], "TASK_STATE_SUBMITTED");

    // Exercise the remote leg deliberately: read and cancel through the node
    // that does not own the shard.
    let owner = owner_logical_id(&node_a, &task_id);
    let reader = if owner == node_a.logical_id {
        &node_b
    } else {
        &node_a
    };

    let (status, task) = http(
        &reader.app,
        "GET",
        &format!("/a2a/tasks/{task_id}"),
        None,
        Some("tenant-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "scoped read failed: {task}");
    assert_eq!(task["status"]["state"], "TASK_STATE_SUBMITTED");

    // An unscoped read (no tenant header) resolves the run's stored tenant
    // instead of coercing a default and failing (finding 4 regression).
    let (status, task) = http(
        &reader.app,
        "GET",
        &format!("/a2a/tasks/{task_id}"),
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "unscoped read failed: {task}");
    assert_eq!(task["id"], serde_json::Value::String(task_id.clone()));

    // Cross-tenant scoping still holds through the sharded owner.
    let (status, _) = http(
        &reader.app,
        "GET",
        &format!("/a2a/tasks/{task_id}"),
        None,
        Some("tenant-b"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, canceled) = http(
        &reader.app,
        "POST",
        &format!("/a2a/tasks/{task_id}:cancel"),
        None,
        Some("tenant-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {canceled}");
    assert_eq!(canceled["status"]["state"], "TASK_STATE_CANCELED");

    node_a.terminate().await;
    node_b.terminate().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

/// Slice 4.4: a stream opened on the public node that does NOT own the shard
/// still receives live updates and terminal completion, via the owner-routed
/// stream cursor bridge.
#[tokio::test]
async fn stream_on_non_owner_node_receives_live_terminal_event() {
    use a2a::{StreamResponse, SubscribeToTaskRequest, TaskState};
    use a2a_server::{RequestHandler, ServiceParams};
    use futures_util::StreamExt;
    use tokio::time::timeout;

    let state_dir = temp_state_dir("stream-cross-node");
    let Some(mut node_a) = build_node("stream-a", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        return;
    };
    let Some(mut node_b) = build_node("stream-b", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let sent = send_ok(&node_a.app, &send_body("stream-cross-message", None, true)).await;
    let task_id = sent["task"]["id"].as_str().expect("task id").to_string();

    // Subscribe deliberately through the node that does not own the shard.
    let owner = owner_logical_id(&node_a, &task_id);
    let (subscriber, canceller) = if owner == node_a.logical_id {
        (&node_b, &node_a)
    } else {
        (&node_a, &node_b)
    };
    let params =
        ServiceParams::from([("x-rakka-tenant".to_string(), vec!["tenant-a".to_string()])]);
    let mut stream = subscriber
        .handler
        .subscribe_to_task(
            &params,
            SubscribeToTaskRequest {
                id: task_id.clone(),
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("subscribe on non-owner node");

    let first = timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("first stream item within deadline")
        .expect("stream open")
        .expect("stream response");
    match first {
        StreamResponse::Task(task) => assert_eq!(task.status.state, TaskState::Submitted),
        other => panic!("expected initial task snapshot, got {other:?}"),
    }

    // Cancel through the other node; the owner emits the terminal event.
    let (status, canceled) = http(
        &canceller.app,
        "POST",
        &format!("/a2a/tasks/{task_id}:cancel"),
        None,
        Some("tenant-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {canceled}");

    // The non-owner stream must observe the canceled terminal state through
    // owner polling (heartbeats carry the pre-cancel status and are skipped).
    let mut saw_terminal = false;
    for _ in 0..20 {
        let Ok(item) = timeout(Duration::from_secs(20), stream.next()).await else {
            break;
        };
        let Some(item) = item else {
            break;
        };
        let response = item.expect("stream response");
        if let StreamResponse::StatusUpdate(update) = &response {
            if update.status.state == TaskState::Canceled {
                saw_terminal = true;
                break;
            }
        }
        if let StreamResponse::Task(task) = &response {
            if task.status.state == TaskState::Canceled {
                saw_terminal = true;
                break;
            }
        }
    }
    assert!(
        saw_terminal,
        "non-owner stream never observed the terminal state"
    );

    // Terminal state completes the stream.
    let end = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream end within deadline");
    assert!(end.is_none(), "stream must end after the terminal event");

    node_a.terminate().await;
    node_b.terminate().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

/// Slice 3.6: after the owner shuts down and membership updates, the
/// surviving node adopts the shard and lazily recovers durable run state.
#[tokio::test]
async fn owner_shutdown_recovers_run_on_new_owner() {
    let state_dir = temp_state_dir("owner-shutdown");
    let Some(mut node_a) = build_node("down-a", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        return;
    };
    let Some(mut node_b) = build_node("down-b", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let sent = send_ok(
        &node_a.app,
        &send_body("owner-shutdown-message", None, true),
    )
    .await;
    let task_id = sent["task"]["id"].as_str().expect("task id").to_string();

    let owner = owner_logical_id(&node_a, &task_id);
    let (owner_node, mut survivor) = if owner == node_a.logical_id {
        (node_a, node_b)
    } else {
        (node_b, node_a)
    };
    let owner_id = owner_node.runtime.local_node().id().clone();
    owner_node.terminate().await;
    // Downing is the membership update: discovery removal alone keeps the
    // peer in the up-set until failure detection expires it, so the survivor
    // downs the dead owner the way lease expiry / operator downing would.
    survivor
        .runtime
        .mark_down(&owner_id, current_timestamp_millis())
        .expect("downing applies");

    // The survivor now owns every shard; the run recovers lazily from the
    // shared durable stores on its first reference.
    let (status, task) = http(
        &survivor.app,
        "GET",
        &format!("/a2a/tasks/{task_id}"),
        None,
        Some("tenant-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "recovered read failed: {task}");
    assert_eq!(task["status"]["state"], "TASK_STATE_SUBMITTED");

    // The recovered owner remains the single writer: cancellation drives the
    // durable run to its terminal state.
    let (status, canceled) = http(
        &survivor.app,
        "POST",
        &format!("/a2a/tasks/{task_id}:cancel"),
        None,
        Some("tenant-a"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "cancel failed: {canceled}");
    assert_eq!(canceled["status"]["state"], "TASK_STATE_CANCELED");

    survivor.terminate().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

/// Slice 3.6: a command in flight while shard ownership moves either
/// completes or fails retryably, and the client retry is idempotent.
#[tokio::test]
async fn inflight_command_during_ownership_move_retries_idempotently() {
    let state_dir = temp_state_dir("inflight-move");
    let Some(mut node_a) = build_node("flight-a", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        return;
    };
    let Some(mut node_b) = build_node("flight-b", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let sent = send_ok(&node_a.app, &send_body("inflight-message-1", None, true)).await;
    let task_id = sent["task"]["id"].as_str().expect("task id").to_string();

    let owner = owner_logical_id(&node_a, &task_id);
    let (owner_node, mut survivor) = if owner == node_a.logical_id {
        (node_a, node_b)
    } else {
        (node_b, node_a)
    };

    // Race a continuation against the downing decision that moves the shard
    // to the survivor. The command may win, lose retryably, or land after
    // the move; every outcome must keep the durable run consistent.
    let continuation = send_body("inflight-message-2", Some(&task_id), true);
    let owner_id = owner_node.runtime.local_node().id().clone();
    let survivor_runtime = &mut survivor.runtime;
    let inflight = http(
        &survivor.app,
        "POST",
        "/a2a/message:send",
        Some(&continuation),
        Some("tenant-a"),
    );
    let (inflight_result, ()) = tokio::join!(inflight, async {
        survivor_runtime
            .mark_down(&owner_id, current_timestamp_millis())
            .expect("downing applies");
    });
    let (inflight_status, _) = inflight_result;
    assert!(
        inflight_status == StatusCode::OK || inflight_status.is_server_error(),
        "in-flight command must succeed or fail retryably, got {inflight_status}"
    );

    // The client retries the identical message until it lands; duplicate
    // acceptance must be idempotent across the ownership move.
    let mut retried = None;
    for _ in 0..10 {
        let (status, value) = http(
            &survivor.app,
            "POST",
            "/a2a/message:send",
            Some(&continuation),
            Some("tenant-a"),
        )
        .await;
        if status == StatusCode::OK {
            retried = Some(value);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let retried = retried.expect("retry should eventually succeed");
    assert_eq!(
        retried["task"]["id"],
        serde_json::Value::String(task_id.clone())
    );

    // Exactly one copy of the continuation exists: original + continuation.
    let history = retried["task"]["history"].as_array().expect("history");
    assert_eq!(history.len(), 2, "duplicate retry must not append twice");

    owner_node.terminate().await;
    survivor.terminate().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

/// Slice 3.6: an idle entity passivates and the next reference lazily
/// recovers it (also the regression test for the child respawn panic).
#[tokio::test]
async fn passivated_entity_recovers_lazily_on_next_reference() {
    let state_dir = temp_state_dir("passivation");
    let Some(mut node) = build_node("passiv-a", &state_dir, Duration::from_millis(200)).await
    else {
        return;
    };
    let member = node.runtime.local_node().clone();
    apply_membership(&mut [&mut node], vec![member], 1);

    let sent = send_ok(&node.app, &send_body("passivation-message", None, true)).await;
    let task_id = sent["task"]["id"].as_str().expect("task id").to_string();

    // Let the idle entity passivate, then reference it again: the entity
    // re-activates with a fresh child actor and recovers durable state.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let (status, task) = http(
        &node.app,
        "GET",
        &format!("/a2a/tasks/{task_id}"),
        None,
        Some("tenant-a"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "post-passivation read failed: {task}"
    );
    assert_eq!(task["status"]["state"], "TASK_STATE_SUBMITTED");

    // A second passivation cycle proves repeated re-activation stays safe
    // (deterministic child names used to collide with the stopping child).
    tokio::time::sleep(Duration::from_millis(700)).await;
    let (status, canceled) = http(
        &node.app,
        "POST",
        &format!("/a2a/tasks/{task_id}:cancel"),
        None,
        Some("tenant-a"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "post-passivation cancel failed: {canceled}"
    );
    assert_eq!(canceled["status"]["state"], "TASK_STATE_CANCELED");

    node.terminate().await;
    let _ = std::fs::remove_dir_all(state_dir);
}

/// Slice 3.6: a duplicate client retry after ownership moved to another
/// node deduplicates durably instead of double-accepting.
#[tokio::test]
async fn duplicate_send_retry_after_owner_movement_is_deduplicated() {
    let state_dir = temp_state_dir("dup-retry");
    let Some(mut node_a) = build_node("dup-a", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        return;
    };
    let Some(mut node_b) = build_node("dup-b", &state_dir, RUN_ENTITY_IDLE_PASSIVATION).await
    else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let original = send_body("dup-retry-message", None, true);
    let sent = send_ok(&node_a.app, &original).await;
    let task_id = sent["task"]["id"].as_str().expect("task id").to_string();
    assert_eq!(sent["task"]["status"]["state"], "TASK_STATE_SUBMITTED");

    let owner = owner_logical_id(&node_a, &task_id);
    let (owner_node, mut survivor) = if owner == node_a.logical_id {
        (node_a, node_b)
    } else {
        (node_b, node_a)
    };
    let owner_id = owner_node.runtime.local_node().id().clone();
    owner_node.terminate().await;
    survivor
        .runtime
        .mark_down(&owner_id, current_timestamp_millis())
        .expect("downing applies");

    // The client retries the identical message against the new owner. The
    // durable inbox recognises the duplicate (same command id and dedup key)
    // and still drives a run waiting in Accepted through its first
    // transition, mirroring the single-node duplicate-send contract.
    let retry = send_body("dup-retry-message", None, false);
    let retried = send_ok(&survivor.app, &retry).await;
    assert_eq!(retried["task"]["id"], serde_json::Value::String(task_id));
    assert_eq!(retried["task"]["status"]["state"], "TASK_STATE_WORKING");
    let history = retried["task"]["history"].as_array().expect("history");
    assert_eq!(history.len(), 1, "duplicate retry must not append twice");

    survivor.terminate().await;
    let _ = std::fs::remove_dir_all(state_dir);
}
