//! Process bootstrap and HTTP router composition.
//!
//! This is the example's product-composition layer: it wires the reusable
//! `rakka-a2a` service (durable request handler, sharded run host, owner
//! router, dynamic agent card, and route composition) to this example's demo
//! workflow, environment configuration, and file/etcd discovery.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use a2a_server::ServiceParams;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use rakka::cluster::{MembershipConfig, SelfFenceConfig};
use rakka::http::{serve_with_graceful_shutdown, HttpServerConfig};
use rakka::prelude::{ActorSystem, ClusterSharding};
use rakka::remote::TcpRemoteTransportConfig;
use rakka::sharding::ClusterNodeRuntime;
use rakka_a2a::agent_card::A2AAgentCardBuilder;
use rakka_a2a::catalog::A2AStaticWorkflowCatalog;
use rakka_a2a::codec::register_a2a_run_codecs;
use rakka_a2a::host::{default_a2a_run_entity_key, init_a2a_run_sharding, A2ARunHost};
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_a2a::push::{A2APushConfigStore, A2APushCredentialPolicy};
use rakka_a2a::router::A2ARunRouter;
use rakka_a2a::routes::a2a_routes;
use rakka_a2a::routing::A2ADrainGate;
use rakka_a2a::stores::{A2ARunStateStore, A2AWorkflowStateStore};
use rakka_a2a::RakkaA2AServiceBuilder;
use rakka_remote::SerializationRegistry;
use serde::Serialize;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;

use crate::config::{DiscoveryProviderKind, ExampleConfig, PersistenceKind};
use crate::discovery::{
    membership_snapshot, new_membership_view, run_file_discovery, seed_file_discovery,
    MembershipView,
};
use crate::durable_stores::build_stores;
use crate::etcd_discovery::{connect, run_etcd_discovery};
use crate::reachability::PeerReachability;
use crate::support::{
    current_timestamp_millis, ExampleResult, DEFAULT_CONNECT_TIMEOUT, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RECONNECT_BACKOFF, RUN_ASK_TIMEOUT, RUN_ENTITY_IDLE_PASSIVATION,
};
use crate::workflow::demo_workflow;

struct Booted {
    config: ExampleConfig,
    system: ActorSystem,
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    discovery_task: JoinHandle<()>,
    shutdown: Arc<Notify>,
    state: AppState,
}

/// Captures the service parameters of the most recent A2A request so the
/// example's `/debug/last-a2a-headers` route can prove header propagation.
#[derive(Clone, Default)]
pub(crate) struct HeaderObserver {
    last: Arc<Mutex<Option<ServiceParams>>>,
}

impl HeaderObserver {
    fn record(&self, params: &ServiceParams) {
        *self.last.lock().expect("header observer mutex") = Some(params.clone());
    }

    fn last(&self) -> Option<ServiceParams> {
        self.last.lock().expect("header observer mutex").clone()
    }
}

/// Shared HTTP route state.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) node_id: String,
    pub(crate) membership: MembershipView,
    pub(crate) header_observer: HeaderObserver,
    pub(crate) drain_gate: A2ADrainGate,
    pub(crate) service: rakka_a2a::RakkaA2AService,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    node_id: String,
    members: Vec<String>,
}

#[derive(Serialize)]
struct ReadinessResponse {
    ready: bool,
    reason: &'static str,
}

#[derive(Serialize)]
struct DrainResponse {
    draining: bool,
    reason: &'static str,
}

#[derive(Serialize)]
struct HeaderSnapshot {
    last_header_names: Vec<String>,
}

/// Boots one cluster node and serves the HTTP/A2A surface until shutdown.
pub async fn run() -> ExampleResult<()> {
    let booted = boot().await?;
    let http_addr = booted.config.http_bind_addr();
    print_banner(&booted);

    let stop = booted.shutdown.clone();
    serve_with_graceful_shutdown(
        router(booted.state.clone()),
        HttpServerConfig::new(http_addr),
        shutdown_signal(stop),
    )
    .await?;

    shutdown(booted).await
}

pub(crate) fn router(state: AppState) -> Router {
    let a2a = a2a_routes(&state.service);
    health_router(state).merge(a2a)
}

fn health_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
        .route("/drain", get(drain).post(drain))
        .route("/cluster", get(cluster))
        .route("/debug/last-a2a-headers", get(last_a2a_headers))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        node_id: state.node_id,
        members: membership_snapshot(&state.membership),
    })
}

async fn readiness(
    State(state): State<AppState>,
) -> (axum::http::StatusCode, Json<ReadinessResponse>) {
    let accepting = state.drain_gate.accepts_public_commands();
    let status = if accepting {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(ReadinessResponse {
            ready: accepting,
            reason: if accepting {
                "clustered-a2a-handler-ready"
            } else {
                "draining"
            },
        }),
    )
}

async fn drain(State(state): State<AppState>) -> Json<DrainResponse> {
    state.drain_gate.begin_drain();
    Json(DrainResponse {
        draining: true,
        reason: "kubernetes-drain",
    })
}

async fn cluster(State(state): State<AppState>) -> Json<Vec<String>> {
    Json(membership_snapshot(&state.membership))
}

async fn last_a2a_headers(State(state): State<AppState>) -> Json<HeaderSnapshot> {
    let mut names = state
        .header_observer
        .last()
        .unwrap_or_default()
        .into_keys()
        .collect::<Vec<_>>();
    names.sort();
    Json(HeaderSnapshot {
        last_header_names: names,
    })
}

fn serialization_registry() -> ExampleResult<SerializationRegistry> {
    let mut registry = SerializationRegistry::new();
    register_a2a_run_codecs(&mut registry)
        .map_err(|error| crate::support::example_error(format!("codec registration: {error}")))?;
    Ok(registry)
}

async fn boot() -> ExampleResult<Booted> {
    let config = ExampleConfig::from_env()?;
    let local_node = config.local_node();
    let workflow = demo_workflow();
    let system = ActorSystem::new(format!(
        "clustered-sharded-entity-a2a-{}",
        config.node_logical_id
    ));

    let mut runtime = ClusterNodeRuntime::builder(local_node.clone())
        .with_membership_config(MembershipConfig::new(
            1,
            Duration::from_secs(10),
            Duration::from_secs(30),
        ))
        .with_transport_config(
            TcpRemoteTransportConfig::new()
                .bind_addr(config.tcp_bind_addr())
                .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                .reconnect_backoff(DEFAULT_RECONNECT_BACKOFF)
                .idle_timeout(DEFAULT_IDLE_TIMEOUT),
        )
        .with_registry(serialization_registry()?)
        .build()
        .await?;

    let ask_client = runtime.ask_client();
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime)?;
    let key = default_a2a_run_entity_key()
        .map_err(|error| crate::support::example_error(format!("entity key: {error}")))?;
    let (run_store, workflow_store, push_config_store) = build_stores(&config).await?;
    let run_store = A2ARunStateStore::new(run_store);
    let workflow_store = A2AWorkflowStateStore::new(workflow_store);
    let task_store = InMemoryA2ATaskProjectionStore::local();
    // Local/demo deployment: no secret backend, so raw push credentials are
    // redacted and only their presence is recorded. A production deployment
    // supplies a credential binding resolver instead.
    let push_configs = A2APushConfigStore::new(push_config_store)
        .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence);
    let shared_task_store: Arc<dyn rakka_a2a::projection::A2ATaskProjectionStore> =
        Arc::new(task_store.clone());
    init_a2a_run_sharding(
        &system,
        &mut runtime,
        &sharding,
        key.clone(),
        A2ARunHost::new(
            workflow.clone(),
            run_store.clone(),
            workflow_store.clone(),
            Arc::clone(&shared_task_store),
            push_configs.clone(),
        )
        .idle_passivation(RUN_ENTITY_IDLE_PASSIVATION)
        .run_ask_timeout(RUN_ASK_TIMEOUT),
    )
    .map_err(|error| crate::support::example_error(format!("sharding init: {error}")))?;

    let membership = new_membership_view();
    let shutdown = Arc::new(Notify::new());
    let reachability = PeerReachability::new();
    let etcd_handle = match config.discovery_provider {
        DiscoveryProviderKind::File => {
            seed_file_discovery(&config, &local_node, &mut runtime, &membership)?;
            None
        }
        DiscoveryProviderKind::Etcd => {
            Some(connect(&config, &local_node, &mut runtime, &membership).await?)
        }
    };

    let runtime = Arc::new(AsyncMutex::new(runtime));
    let self_fence = config
        .self_fence
        .then(|| SelfFenceConfig::new(config.self_fence_after, config.self_fence_rejoin_after));
    let discovery_task = match etcd_handle {
        None => tokio::spawn(run_file_discovery(
            runtime.clone(),
            config.clone(),
            local_node.clone(),
            membership.clone(),
            shutdown.clone(),
        )),
        Some(handle) => tokio::spawn(run_etcd_discovery(
            handle.session,
            handle.discovery,
            runtime.clone(),
            membership.clone(),
            reachability.clone(),
            self_fence,
            shutdown.clone(),
        )),
    };

    let catalog = A2AStaticWorkflowCatalog::single(workflow.clone());
    let agent_card = build_agent_card(&config, &catalog);
    let header_observer = HeaderObserver::default();
    let router = A2ARunRouter::new(sharding, key, ask_client, RUN_ASK_TIMEOUT)
        .with_reachability_observer(Arc::new(reachability));
    let drain_gate = A2ADrainGate::new();
    let observer = header_observer.clone();
    let service = RakkaA2AServiceBuilder::new()
        .agent_card(agent_card)
        .workflow_catalog(catalog)
        .task_store_with_watcher(task_store)
        .run_store(run_store)
        .workflow_store(workflow_store)
        .push_config_store(push_configs)
        .drain_gate(drain_gate.clone())
        .router(router)
        .request_observer(move |params| observer.record(params))
        .build()
        .map_err(|error| crate::support::example_error(format!("service build: {error}")))?;
    service
        .recover_task_projections()
        .await
        .map_err(|error| crate::support::example_error(format!("projection recovery: {error}")))?;

    let state = AppState {
        node_id: local_node.id().to_string(),
        membership,
        header_observer,
        drain_gate,
        service,
    };

    Ok(Booted {
        config,
        system,
        runtime,
        discovery_task,
        shutdown,
        state,
    })
}

/// Builds the example's public agent card from the crate's dynamic builder.
pub(crate) fn build_agent_card(
    config: &ExampleConfig,
    catalog: &A2AStaticWorkflowCatalog,
) -> a2a::AgentCard {
    let base_url = config
        .public_url
        .clone()
        .unwrap_or_else(|| config.local_public_url());
    A2AAgentCardBuilder::new(
        "Rakka Clustered A2A Agent",
        "A Rakka A2A example with durable command acceptance, task projections, and clustered \
         sharded run hosting.",
    )
    .version(env!("CARGO_PKG_VERSION"))
    .public_base_url(base_url)
    .provider("Rakka", "https://github.com/rakka-rs/rakka")
    .build(catalog)
}

async fn shutdown(booted: Booted) -> ExampleResult<()> {
    booted.shutdown.notify_one();
    let _ = tokio::time::timeout(Duration::from_secs(3), booted.discovery_task).await;
    if let Ok(mut runtime) = booted.runtime.try_lock() {
        let _ = runtime.leave_local(current_timestamp_millis());
    }
    booted.system.terminate().await?;
    Ok(())
}

async fn shutdown_signal(stop: Arc<Notify>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
            () = stop.notified() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            () = stop.notified() => {}
        }
    }
}

fn print_banner(booted: &Booted) {
    let addr = booted.config.http_bind_addr();
    let discovery = match booted.config.discovery_provider {
        DiscoveryProviderKind::File => "file",
        DiscoveryProviderKind::Etcd => "etcd",
    };
    let persistence = match booted.config.persistence {
        PersistenceKind::File => "file",
        PersistenceKind::Postgres => "postgres",
    };
    println!(
        "Rakka A2A node {} | remoting {} | HTTP/A2A {}",
        booted.config.node_logical_id, booted.config.rakka_port, addr,
    );
    println!(
        "discovery: {discovery}; persistence: {persistence}; state dir: {}",
        booted.config.state_dir.display()
    );
    println!("agent card: http://{addr}/.well-known/agent-card.json");
    println!("REST base: http://{addr}/a2a");
    println!("JSON-RPC: http://{addr}/a2a/jsonrpc");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_stores::{build_in_memory_stores, RunStore};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    const PHASE6_DOC: &str = include_str!("../doc/phase-6-production-topology.md");
    const README: &str = include_str!("../README.md");
    const K8S_AGENT: &str = include_str!("../k8s/agent-a2a.yaml");
    const K8S_ETCD: &str = include_str!("../k8s/etcd.yaml");
    const K8S_POSTGRES: &str = include_str!("../k8s/postgres.yaml");

    /// Route state plus the shared stores tests inspect directly.
    struct TestContext {
        state: AppState,
        run_store: RunStore,
    }

    #[test]
    fn phase6_kubernetes_manifest_covers_public_private_and_persistence_paths() {
        for manifest in [K8S_AGENT, K8S_ETCD, K8S_POSTGRES] {
            for doc in manifest_documents(manifest) {
                assert!(doc.contains("apiVersion:"), "missing apiVersion: {doc}");
                assert!(doc.contains("kind:"), "missing kind: {doc}");
                assert!(doc.contains("metadata:"), "missing metadata: {doc}");
                assert!(doc.contains("  name:"), "missing metadata.name: {doc}");
            }
        }

        for expected in [
            "kind: Namespace",
            "kind: ServiceAccount",
            "name: rakka-a2a-agent-config",
            "RAKKA_DISCOVERY_PROVIDER: etcd",
            "RAKKA_ETCD_ENDPOINTS: http://rakka-a2a-etcd:2379",
            "RAKKA_PERSISTENCE: postgres",
            "RAKKA_A2A_PUBLIC_URL:",
            "secretKeyRef:",
            "name: RAKKA_POSTGRES_DSN",
            "kind: StatefulSet",
            "readinessProbe:",
            "path: /readyz",
            "livenessProbe:",
            "path: /healthz",
            "startupProbe:",
            "preStop:",
            "path: /drain",
            "kind: PodDisruptionBudget",
            "kind: HorizontalPodAutoscaler",
        ] {
            assert!(
                K8S_AGENT.contains(expected),
                "app manifest missing {expected}"
            );
        }

        let internal = manifest_document_named(K8S_AGENT, "Service", "rakka-a2a-internal");
        assert!(internal.contains("clusterIP: None"));
        assert!(internal.contains("publishNotReadyAddresses: true"));
        assert!(internal.contains("name: remoting"));

        let public = manifest_document_named(K8S_AGENT, "Service", "rakka-a2a-public");
        assert!(public.contains("type: LoadBalancer"));
        assert!(public.contains("name: http"));
        assert!(
            !public.contains("remoting"),
            "public A2A Service must not expose Rakka remoting"
        );

        assert!(K8S_ETCD.contains("kind: Deployment"));
        assert!(K8S_ETCD.contains("name: rakka-a2a-etcd"));
        assert!(K8S_POSTGRES.contains("kind: Secret"));
        assert!(K8S_POSTGRES.contains("dsn: host=rakka-a2a-postgres"));
    }

    #[test]
    fn phase6_docs_cover_exit_criteria_and_known_boundaries() {
        for expected in [
            "Public traffic enters through the load-balanced `rakka-a2a-public` Service",
            "Private Rakka remoting uses the headless `rakka-a2a-internal` Service",
            "etcd provides dynamic membership",
            "`RAKKA_PERSISTENCE=postgres`",
            "The agent card must point to that public URL",
            "`/drain` closes mutating public A2A ingress",
            "OpenTelemetry guidance",
            "Scale-out signals",
            "Failure Injection",
            "Production-Candidate Review",
            "shared PostgreSQL query/event table",
        ] {
            assert!(
                PHASE6_DOC.contains(expected),
                "Phase 6 doc missing {expected}"
            );
        }

        for expected in [
            "RAKKA_PERSISTENCE",
            "--features postgres",
            "examples/clustered-sharded-entity-a2a-agents/k8s/",
            "a2a-agent-draining",
            "RAKKA_A2A_PUBLIC_URL",
            "doc/phase-6-production-topology.md",
        ] {
            assert!(README.contains(expected), "README missing {expected}");
        }
    }

    #[tokio::test]
    async fn agent_card_and_health_routes_share_server() {
        let app = router(test_state().state);

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let card = app
            .oneshot(
                Request::builder()
                    .uri("/.well-known/agent-card.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(card.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rest_headers_reach_a2a_service_params() {
        let ctx = test_state();
        let observer = ctx.state.header_observer.clone();
        let app = router(ctx.state);
        let body = serde_json::json!({
            "message": {
                "messageId": "phase0-message",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            },
            "tenant": "tenant-a"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a/message:send")
                    .header("content-type", "application/json")
                    .header("x-rakka-phase", "0")
                    .header("x-rakka-tenant", "tenant-a")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            payload["task"]["status"]["state"],
            serde_json::Value::String("TASK_STATE_WORKING".to_string())
        );

        let params = observer.last().expect("handler should capture params");
        assert_eq!(params.get("x-rakka-phase"), Some(&vec!["0".to_string()]));
    }

    #[tokio::test]
    async fn rest_send_message_is_durable_and_deduplicated() {
        let app = router(test_state().state);
        let body = serde_json::json!({
            "message": {
                "messageId": "dedupe-message",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            },
            "configuration": { "returnImmediately": true },
            "tenant": "tenant-a"
        });

        let first = post_json(app.clone(), "/a2a/message:send", &body, "tenant-a").await;
        assert_eq!(first["task"]["status"]["state"], "TASK_STATE_SUBMITTED");
        let task_id = first["task"]["id"].as_str().expect("task id").to_string();

        let retry = post_json(app.clone(), "/a2a/message:send", &body, "tenant-a").await;
        assert_eq!(retry["task"]["id"], task_id);
        assert_eq!(
            retry["task"]["metadata"]["io.rakka.projection.revision"],
            first["task"]["metadata"]["io.rakka.projection.revision"]
        );
        assert_eq!(
            retry["task"]["history"].as_array().expect("history").len(),
            1
        );

        let list = app
            .oneshot(
                Request::builder()
                    .uri("/a2a/tasks")
                    .header("x-rakka-tenant", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(list.status(), StatusCode::OK);
        let bytes = list.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(payload["tasks"].as_array().expect("tasks").len(), 1);
    }

    #[tokio::test]
    async fn drain_closes_mutating_ingress_but_keeps_reads_available() {
        let app = router(test_state().state);
        let accepted = serde_json::json!({
            "message": {
                "messageId": "drain-before-message",
                "role": "ROLE_USER",
                "parts": [{"text": "accepted before drain"}]
            },
            "configuration": { "returnImmediately": true },
            "tenant": "tenant-a"
        });
        let first = post_json(app.clone(), "/a2a/message:send", &accepted, "tenant-a").await;
        let task_id = first["task"]["id"].as_str().expect("task id").to_string();

        let drain = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/drain")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(drain.status(), StatusCode::OK);

        let readiness = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);

        let rejected = serde_json::json!({
            "message": {
                "messageId": "drain-after-message",
                "role": "ROLE_USER",
                "parts": [{"text": "reject during drain"}]
            },
            "configuration": { "returnImmediately": true },
            "tenant": "tenant-a"
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a/message:send")
                    .header("content-type", "application/json")
                    .header("x-rakka-tenant", "tenant-a")
                    .body(Body::from(rejected.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            error.to_string().contains("a2a-agent-draining"),
            "drain rejection should carry a stable code: {error}"
        );

        let read = app
            .oneshot(
                Request::builder()
                    .uri(format!("/a2a/tasks/{task_id}"))
                    .header("x-rakka-tenant", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn rest_cancel_updates_durable_run_state() {
        use rakka_agent_workflow::{AgentRunId, AgentRunStatus, AgentStepRunner};

        let ctx = test_state();
        let run_store = ctx.run_store.clone();
        let app = router(ctx.state);
        let body = serde_json::json!({
            "message": {
                "messageId": "cancel-message",
                "role": "ROLE_USER",
                "parts": [{"text": "cancel me"}]
            },
            "configuration": { "returnImmediately": true },
            "tenant": "tenant-a"
        });
        let sent = post_json(app.clone(), "/a2a/message:send", &body, "tenant-a").await;
        let task_id = sent["task"]["id"].as_str().expect("task id").to_string();

        let cancel = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/a2a/tasks/{task_id}:cancel"))
                    .header("x-rakka-tenant", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::OK);
        let bytes = cancel.into_body().collect().await.unwrap().to_bytes();
        let canceled: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        // An accepted cancellation completes durably and reports the terminal
        // public state, not a Working-forever cancelling limbo.
        assert_eq!(canceled["status"]["state"], "TASK_STATE_CANCELED");

        // The durable run itself reached the terminal Cancelled state.
        let mut runner =
            AgentStepRunner::new(demo_workflow(), AgentRunId::new(task_id.clone()), run_store);
        let state = runner.recover().await.unwrap().expect("run state");
        assert_eq!(state.status, AgentRunStatus::Cancelled);
    }

    fn test_config() -> ExampleConfig {
        ExampleConfig {
            bind_host: "127.0.0.1".parse().expect("loopback address"),
            advertise_host: "127.0.0.1".to_string(),
            rakka_port: crate::support::DEFAULT_RAKKA_PORT,
            http_port: crate::support::DEFAULT_RAKKA_PORT.saturating_add(10_000),
            node_logical_id: "test-node".to_string(),
            node_incarnation: "test".to_string(),
            discovery_provider: DiscoveryProviderKind::File,
            discovery_dir: std::env::temp_dir(),
            etcd_endpoints: vec!["http://127.0.0.1:2379".to_string()],
            etcd_prefix: crate::support::DEFAULT_ETCD_PREFIX.to_string(),
            etcd_lease_ttl_seconds: crate::support::DEFAULT_ETCD_LEASE_TTL_SECONDS,
            persistence: PersistenceKind::File,
            postgres_dsn: None,
            state_dir: std::env::temp_dir(),
            self_fence: false,
            self_fence_after: Duration::from_secs(15),
            self_fence_rejoin_after: Duration::from_secs(10),
            public_url: None,
        }
    }

    fn test_state() -> TestContext {
        let config = test_config();
        let catalog = A2AStaticWorkflowCatalog::single(demo_workflow());
        let agent_card = build_agent_card(&config, &catalog);
        let task_store = InMemoryA2ATaskProjectionStore::local();
        let (run_store, workflow_store, push_config_store) = build_in_memory_stores();
        let push_configs = A2APushConfigStore::new(push_config_store)
            .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence);
        let header_observer = HeaderObserver::default();
        let drain_gate = A2ADrainGate::new();
        let observer = header_observer.clone();
        let service = RakkaA2AServiceBuilder::new()
            .agent_card(agent_card)
            .workflow_catalog(catalog)
            .task_store_with_watcher(task_store)
            .run_store(A2ARunStateStore::new(run_store.clone()))
            .workflow_store(A2AWorkflowStateStore::new(workflow_store))
            .push_config_store(push_configs)
            .drain_gate(drain_gate.clone())
            .request_observer(move |params| observer.record(params))
            .build()
            .expect("service");
        TestContext {
            state: AppState {
                node_id: "test-node#uid".to_string(),
                membership: Arc::new(std::sync::Mutex::new(vec!["test-node#uid".to_string()])),
                header_observer,
                drain_gate,
                service,
            },
            run_store,
        }
    }

    async fn post_json(
        app: Router,
        uri: &str,
        body: &serde_json::Value,
        tenant: &str,
    ) -> serde_json::Value {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .header("x-rakka-tenant", tenant)
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "response body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).unwrap()
    }

    fn manifest_documents(manifest: &'static str) -> Vec<String> {
        manifest
            .split("\n---")
            .map(|doc| {
                doc.lines()
                    .filter(|line| !line.trim_start().starts_with('#'))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .map(|doc| doc.trim().to_string())
            .filter(|doc| !doc.is_empty())
            .collect()
    }

    fn manifest_document_named(manifest: &'static str, kind: &str, name: &str) -> String {
        let kind = format!("kind: {kind}");
        let name = format!("  name: {name}");
        manifest_documents(manifest)
            .into_iter()
            .find(|doc| doc.contains(&kind) && doc.contains(&name))
            .unwrap_or_else(|| panic!("missing document {kind} {name}"))
    }
}
