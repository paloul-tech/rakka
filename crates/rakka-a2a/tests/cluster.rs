//! Sharded owner host cluster tests.
//!
//! Two in-process cluster nodes over networked loopback remoting share the
//! same durable store handles. Each node runs a full `RakkaA2AService` with an
//! owner-routing `A2ARunRouter`, so owner-only paths (accept, cancel, query
//! snapshot, stream cursor) cross real remoting between nodes.
//!
//! Skipped automatically when loopback binding is unavailable in the sandbox,
//! mirroring the other networked tests in this workspace.

use std::sync::Arc;
use std::time::Duration;

use a2a::{CancelTaskRequest, GetTaskRequest, SendMessageRequest, SendMessageResponse, Task};
use a2a_server::{RequestHandler, ServiceParams};
use rakka_a2a::codec::register_a2a_run_codecs;
use rakka_a2a::host::{default_a2a_run_entity_key, init_a2a_run_sharding, A2ARunHost};
use rakka_a2a::mapping::A2AHeaderTenantResolver;
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_a2a::push::{A2APushConfigState, A2APushConfigStore, A2APushCredentialPolicy};
use rakka_a2a::router::A2ARunRouter;
use rakka_a2a::stores::{A2ARunStateStore, A2AWorkflowStateStore};
use rakka_a2a::testing::{fixture_agent_card, fixture_workflow};
use rakka_a2a::{RakkaA2AService, RakkaA2AServiceBuilder};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::AgentRunState;
use rakka_cluster::{ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId};
use rakka_core::ActorSystem;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_remote::{SerializationRegistry, TcpRemoteTransportConfig};
use rakka_sharding::{ClusterNodeRuntime, ClusterSharding, EntityId, EntityType};

/// Shared durable stores cloned into every node so both observe one truth.
#[derive(Clone)]
struct SharedStores {
    run: A2ARunStateStore,
    workflow: A2AWorkflowStateStore,
    task: InMemoryA2ATaskProjectionStore,
    push: InMemoryDurableStateStore<A2APushConfigState>,
}

impl SharedStores {
    fn new() -> Self {
        Self {
            run: A2ARunStateStore::new(InMemoryDurableStateStore::<AgentRunState>::new()),
            workflow: A2AWorkflowStateStore::new(InMemoryDurableStateStore::<WorkflowState>::new()),
            task: InMemoryA2ATaskProjectionStore::local(),
            push: InMemoryDurableStateStore::<A2APushConfigState>::new(),
        }
    }

    /// Shares the durable run/workflow/push core but gives this node its own
    /// process-local task projection store, so cross-node streaming must go
    /// through the owner-polling path rather than a shared event log.
    fn with_private_task_store(&self) -> Self {
        Self {
            run: self.run.clone(),
            workflow: self.workflow.clone(),
            task: InMemoryA2ATaskProjectionStore::local(),
            push: self.push.clone(),
        }
    }
}

struct TestNode {
    logical_id: String,
    system: ActorSystem,
    runtime: ClusterNodeRuntime,
    service: RakkaA2AService,
}

impl TestNode {
    async fn terminate(self) {
        self.system.terminate().await.expect("system terminates");
    }
}

fn registry() -> SerializationRegistry {
    let mut registry = SerializationRegistry::new();
    register_a2a_run_codecs(&mut registry).expect("register codecs");
    registry
}

async fn build_node(logical_id: &str, stores: SharedStores) -> Option<TestNode> {
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
        .with_registry(registry())
        .build()
        .await;
    let Ok(mut runtime) = runtime else {
        eprintln!("skipping cluster test; loopback bind unavailable");
        system.terminate().await.expect("system terminates");
        return None;
    };
    let ask_client = runtime.ask_client();
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime).expect("sharding");
    let key = default_a2a_run_entity_key().expect("entity key");
    let push_configs = A2APushConfigStore::new(stores.push.clone())
        .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence);
    let task_store: Arc<dyn rakka_a2a::projection::A2ATaskProjectionStore> =
        Arc::new(stores.task.clone());
    init_a2a_run_sharding(
        &system,
        &mut runtime,
        &sharding,
        key.clone(),
        A2ARunHost::new(
            fixture_workflow(),
            stores.run.clone(),
            stores.workflow.clone(),
            Arc::clone(&task_store),
            push_configs.clone(),
        ),
    )
    .expect("sharding init");
    let router = A2ARunRouter::new(sharding, key, ask_client, Duration::from_secs(3));
    let service = RakkaA2AServiceBuilder::new()
        .agent_card(fixture_agent_card())
        .single_workflow(fixture_workflow())
        .task_store_with_watcher(stores.task.clone())
        .run_store(stores.run.clone())
        .workflow_store(stores.workflow.clone())
        .push_config_store(push_configs)
        .tenant_resolver(A2AHeaderTenantResolver)
        .router(router)
        .build()
        .expect("service");
    Some(TestNode {
        logical_id: logical_id.to_string(),
        system,
        runtime,
        service,
    })
}

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

fn owner_logical_id(node: &TestNode, task_id: &str) -> String {
    node.runtime
        .sharding()
        .coordinator(&EntityType::new(rakka_a2a::host::DEFAULT_ENTITY_TYPE))
        .expect("coordinator")
        .owner_for_entity(&EntityId::new(task_id))
        .expect("owner")
        .logical_id()
        .to_string()
}

fn params(tenant: &str) -> ServiceParams {
    ServiceParams::from([("x-rakka-tenant".to_string(), vec![tenant.to_string()])])
}

fn send_request(message_id: &str, task_id: Option<&str>, immediate: bool) -> SendMessageRequest {
    let mut message = serde_json::json!({
        "messageId": message_id,
        "role": "ROLE_USER",
        "parts": [{"text": "hello owner"}]
    });
    if let Some(task_id) = task_id {
        message["taskId"] = serde_json::Value::String(task_id.to_string());
    }
    serde_json::from_value(serde_json::json!({
        "message": message,
        "configuration": { "returnImmediately": immediate },
        "tenant": "tenant-a"
    }))
    .expect("send request")
}

fn task_of(response: SendMessageResponse) -> Task {
    match response {
        SendMessageResponse::Task(task) => task,
        other => panic!("expected task response, got {other:?}"),
    }
}

async fn send(node: &TestNode, request: SendMessageRequest) -> Task {
    task_of(
        node.service
            .handler()
            .send_message(&params("tenant-a"), request)
            .await
            .expect("send"),
    )
}

#[tokio::test]
async fn task_accepted_on_one_node_reads_and_cancels_from_another() {
    let stores = SharedStores::new();
    let Some(mut node_a) = build_node("cross-a", stores.clone()).await else {
        return;
    };
    let Some(mut node_b) = build_node("cross-b", stores.clone()).await else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let task = send(&node_a, send_request("cross-message", None, true)).await;
    assert_eq!(task.status.state, a2a::TaskState::Submitted);

    // Read and cancel deliberately through the node that does not own the shard.
    let owner = owner_logical_id(&node_a, &task.id);
    let reader = if owner == node_a.logical_id {
        &node_b
    } else {
        &node_a
    };

    let read = reader
        .service
        .handler()
        .get_task(
            &params("tenant-a"),
            GetTaskRequest {
                id: task.id.clone(),
                history_length: None,
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("scoped read through non-owner");
    assert_eq!(read.status.state, a2a::TaskState::Submitted);

    // An unscoped read resolves the run's stored tenant through the owner.
    let unscoped = reader
        .service
        .handler()
        .get_task(
            &ServiceParams::new(),
            GetTaskRequest {
                id: task.id.clone(),
                history_length: None,
                tenant: None,
            },
        )
        .await
        .expect("unscoped read through non-owner");
    assert_eq!(unscoped.id, task.id);

    // Cross-tenant scoping still holds through the sharded owner.
    let cross = reader
        .service
        .handler()
        .get_task(
            &params("tenant-b"),
            GetTaskRequest {
                id: task.id.clone(),
                history_length: None,
                tenant: Some("tenant-b".to_string()),
            },
        )
        .await
        .expect_err("cross tenant read");
    assert!(
        cross.message.contains("task not found"),
        "unexpected: {cross:?}"
    );

    let canceled = reader
        .service
        .handler()
        .cancel_task(
            &params("tenant-a"),
            CancelTaskRequest {
                id: task.id.clone(),
                metadata: None,
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("cancel through non-owner");
    assert_eq!(canceled.status.state, a2a::TaskState::Canceled);

    node_a.terminate().await;
    node_b.terminate().await;
}

#[tokio::test]
async fn stream_on_non_owner_node_receives_live_terminal_event() {
    use a2a::{StreamResponse, SubscribeToTaskRequest, TaskState};
    use futures_util::StreamExt;
    use tokio::time::timeout;

    // Shared durable core, but each node keeps a private task projection
    // store so cross-node live updates must arrive through owner polling.
    let core = SharedStores::new();
    let Some(mut node_a) = build_node("stream-a", core.with_private_task_store()).await else {
        return;
    };
    let Some(mut node_b) = build_node("stream-b", core.with_private_task_store()).await else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let task = send(&node_a, send_request("stream-message", None, true)).await;

    // Subscribe deliberately through the node that does not own the shard.
    let owner = owner_logical_id(&node_a, &task.id);
    let (subscriber, canceller) = if owner == node_a.logical_id {
        (&node_b, &node_a)
    } else {
        (&node_a, &node_b)
    };
    let mut stream = subscriber
        .service
        .handler()
        .subscribe_to_task(
            &params("tenant-a"),
            SubscribeToTaskRequest {
                id: task.id.clone(),
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

    // Cancel through the owning node; the owner emits the terminal event.
    canceller
        .service
        .handler()
        .cancel_task(
            &params("tenant-a"),
            CancelTaskRequest {
                id: task.id.clone(),
                metadata: None,
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("cancel through owner");

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
        let terminal = match &response {
            StreamResponse::StatusUpdate(update) => update.status.state == TaskState::Canceled,
            StreamResponse::Task(task) => task.status.state == TaskState::Canceled,
            _ => false,
        };
        if terminal {
            saw_terminal = true;
            break;
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
}

#[tokio::test]
async fn owner_shutdown_recovers_run_on_new_owner() {
    let stores = SharedStores::new();
    let Some(mut node_a) = build_node("down-a", stores.clone()).await else {
        return;
    };
    let Some(mut node_b) = build_node("down-b", stores.clone()).await else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let task = send(&node_a, send_request("owner-shutdown", None, true)).await;

    let owner = owner_logical_id(&node_a, &task.id);
    let (owner_node, mut survivor) = if owner == node_a.logical_id {
        (node_a, node_b)
    } else {
        (node_b, node_a)
    };
    let owner_id = owner_node.runtime.local_node().id().clone();
    owner_node.terminate().await;
    survivor
        .runtime
        .mark_down(&owner_id, now_millis())
        .expect("downing applies");

    // The survivor now owns every shard; the run recovers lazily from shared
    // durable stores on its first reference.
    let read = survivor
        .service
        .handler()
        .get_task(
            &params("tenant-a"),
            GetTaskRequest {
                id: task.id.clone(),
                history_length: None,
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("recovered read");
    assert_eq!(read.status.state, a2a::TaskState::Submitted);

    let canceled = survivor
        .service
        .handler()
        .cancel_task(
            &params("tenant-a"),
            CancelTaskRequest {
                id: task.id.clone(),
                metadata: None,
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("cancel on recovered owner");
    assert_eq!(canceled.status.state, a2a::TaskState::Canceled);

    survivor.terminate().await;
}

#[tokio::test]
async fn duplicate_send_retry_after_owner_movement_is_deduplicated() {
    let stores = SharedStores::new();
    let Some(mut node_a) = build_node("dup-a", stores.clone()).await else {
        return;
    };
    let Some(mut node_b) = build_node("dup-b", stores.clone()).await else {
        node_a.terminate().await;
        return;
    };
    let members = member_nodes(&[&mut node_a, &mut node_b]);
    apply_membership(&mut [&mut node_a, &mut node_b], members, 1);

    let task = send(&node_a, send_request("dup-message", None, true)).await;
    assert_eq!(task.status.state, a2a::TaskState::Submitted);

    let owner = owner_logical_id(&node_a, &task.id);
    let (owner_node, mut survivor) = if owner == node_a.logical_id {
        (node_a, node_b)
    } else {
        (node_b, node_a)
    };
    let owner_id = owner_node.runtime.local_node().id().clone();
    owner_node.terminate().await;
    survivor
        .runtime
        .mark_down(&owner_id, now_millis())
        .expect("downing applies");

    // The client retries the identical message against the new owner. The
    // durable inbox recognises the duplicate and still drives a run waiting in
    // Accepted through its first transition.
    let retried = send(&survivor, send_request("dup-message", None, false)).await;
    assert_eq!(retried.id, task.id);
    assert_eq!(retried.status.state, a2a::TaskState::Working);
    assert_eq!(
        retried.history.expect("history").len(),
        1,
        "duplicate retry must not append twice"
    );

    survivor.terminate().await;
}

#[tokio::test]
async fn passivated_entity_recovers_lazily_on_next_reference() {
    let stores = SharedStores::new();
    // A short passivation window so the idle entity passivates between calls.
    let Some(mut node) =
        build_node_with_passivation("passiv-a", stores, Duration::from_millis(200)).await
    else {
        return;
    };
    let member = node.runtime.local_node().clone();
    apply_membership(&mut [&mut node], vec![member], 1);

    let task = send(&node, send_request("passiv-message", None, true)).await;

    // Let the idle entity passivate, then reference it again: it re-activates
    // with a fresh child actor and recovers durable state.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let read = node
        .service
        .handler()
        .get_task(
            &params("tenant-a"),
            GetTaskRequest {
                id: task.id.clone(),
                history_length: None,
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("post-passivation read");
    assert_eq!(read.status.state, a2a::TaskState::Submitted);

    // A second passivation cycle proves repeated re-activation stays safe.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let canceled = node
        .service
        .handler()
        .cancel_task(
            &params("tenant-a"),
            CancelTaskRequest {
                id: task.id.clone(),
                metadata: None,
                tenant: Some("tenant-a".to_string()),
            },
        )
        .await
        .expect("post-passivation cancel");
    assert_eq!(canceled.status.state, a2a::TaskState::Canceled);

    node.terminate().await;
}

async fn build_node_with_passivation(
    logical_id: &str,
    stores: SharedStores,
    idle_passivation: Duration,
) -> Option<TestNode> {
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
        .with_registry(registry())
        .build()
        .await;
    let Ok(mut runtime) = runtime else {
        eprintln!("skipping cluster test; loopback bind unavailable");
        system.terminate().await.expect("system terminates");
        return None;
    };
    let ask_client = runtime.ask_client();
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime).expect("sharding");
    let key = default_a2a_run_entity_key().expect("entity key");
    let push_configs = A2APushConfigStore::new(stores.push.clone())
        .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence);
    let task_store: Arc<dyn rakka_a2a::projection::A2ATaskProjectionStore> =
        Arc::new(stores.task.clone());
    init_a2a_run_sharding(
        &system,
        &mut runtime,
        &sharding,
        key.clone(),
        A2ARunHost::new(
            fixture_workflow(),
            stores.run.clone(),
            stores.workflow.clone(),
            Arc::clone(&task_store),
            push_configs.clone(),
        )
        .idle_passivation(idle_passivation),
    )
    .expect("sharding init");
    let router = A2ARunRouter::new(sharding, key, ask_client, Duration::from_secs(3));
    let service = RakkaA2AServiceBuilder::new()
        .agent_card(fixture_agent_card())
        .single_workflow(fixture_workflow())
        .task_store_with_watcher(stores.task.clone())
        .run_store(stores.run.clone())
        .workflow_store(stores.workflow.clone())
        .push_config_store(push_configs)
        .tenant_resolver(A2AHeaderTenantResolver)
        .router(router)
        .build()
        .expect("service");
    Some(TestNode {
        logical_id: logical_id.to_string(),
        system,
        runtime,
        service,
    })
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(0)
        })
}
