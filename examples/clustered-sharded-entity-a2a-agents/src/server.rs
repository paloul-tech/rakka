//! Process bootstrap and HTTP router composition.

use std::sync::Arc;
use std::time::Duration;

use a2a_server::agent_card::agent_card_router;
use a2a_server::jsonrpc::jsonrpc_router;
use a2a_server::rest::rest_router;
use a2a_server::StaticAgentCard;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use rakka::cluster::MembershipConfig;
use rakka::http::{serve_with_graceful_shutdown, HttpServerConfig};
use rakka::prelude::{ActorSystem, ClusterSharding};
use rakka::remote::TcpRemoteTransportConfig;
use rakka::sharding::ClusterNodeRuntime;
use serde::Serialize;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;

use crate::a2a_handler::{HeaderObserver, Phase1A2AHandler};
use crate::agent_card::build_agent_card;
use crate::codec::serialization_registry;
use crate::config::ExampleConfig;
use crate::discovery::{
    membership_snapshot, new_membership_view, run_file_discovery, seed_file_discovery,
    MembershipView,
};
use crate::durable_stores::build_stores;
use crate::sharded_run_entity::init_demo_run_sharding;
use crate::support::{
    current_timestamp_millis, ExampleResult, DEFAULT_CONNECT_TIMEOUT, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RECONNECT_BACKOFF,
};
use crate::task_projection::InMemoryA2ATaskProjectionStore;
use crate::workflow::demo_workflow;

struct Booted {
    config: ExampleConfig,
    system: ActorSystem,
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    discovery_task: JoinHandle<()>,
    shutdown: Arc<Notify>,
    state: AppState,
}

/// Shared HTTP route state.
#[derive(Clone)]
struct AppState {
    node_id: String,
    membership: MembershipView,
    agent_card: a2a::AgentCard,
    workflow: rakka::agent_workflow::AgentWorkflow,
    task_store: InMemoryA2ATaskProjectionStore,
    header_observer: HeaderObserver,
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

fn router(state: AppState) -> Router {
    let agent_card = state.agent_card.clone();
    let handler = Arc::new(Phase1A2AHandler::new(
        agent_card.clone(),
        state.workflow.clone(),
        state.task_store.clone(),
        state.header_observer.clone(),
    ));

    health_router(state.clone())
        .merge(agent_card_router(Arc::new(StaticAgentCard::new(
            agent_card,
        ))))
        .nest("/a2a", rest_router(handler.clone()))
        .nest("/a2a/jsonrpc", jsonrpc_router(handler))
}

fn health_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(readiness))
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

async fn readiness() -> Json<ReadinessResponse> {
    Json(ReadinessResponse {
        ready: true,
        reason: "phase-1-runtime-booted",
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
        .with_registry(serialization_registry())
        .build()
        .await?;

    let sharding = ClusterSharding::for_node_runtime(&system, &runtime)?;
    let (run_store, workflow_store) = build_stores();
    init_demo_run_sharding(&sharding, workflow.clone(), run_store, workflow_store)?;

    let membership = new_membership_view();
    let shutdown = Arc::new(Notify::new());
    seed_file_discovery(&config, &local_node, &mut runtime, &membership)?;

    let runtime = Arc::new(AsyncMutex::new(runtime));
    let discovery_task = tokio::spawn(run_file_discovery(
        runtime.clone(),
        config.clone(),
        local_node.clone(),
        membership.clone(),
        shutdown.clone(),
    ));

    let state = AppState {
        node_id: local_node.id().to_string(),
        membership,
        agent_card: build_agent_card(&config),
        workflow,
        task_store: InMemoryA2ATaskProjectionStore::local(),
        header_observer: HeaderObserver::default(),
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
    println!(
        "Rakka A2A Phase 1 node {} | remoting {} | HTTP/A2A {}",
        booted.config.node_logical_id, booted.config.rakka_port, addr,
    );
    println!("agent card: http://{}/.well-known/agent-card.json", addr);
    println!("REST base: http://{addr}/a2a");
    println!("JSON-RPC: http://{addr}/a2a/jsonrpc");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn agent_card_and_health_routes_share_server() {
        let state = test_state();
        let app = router(state);

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
        let state = test_state();
        let observer = state.header_observer.clone();
        let app = router(state);
        let body = serde_json::json!({
            "message": {
                "messageId": "phase0-message",
                "role": "ROLE_USER",
                "parts": [{"text": "hello"}]
            }
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/a2a/message:send")
                    .header("content-type", "application/json")
                    .header("x-rakka-phase", "0")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let payload: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            payload["error"]["status"],
            serde_json::Value::String("FAILED_PRECONDITION".to_string())
        );
        assert!(payload["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("Phase 2")));

        let params = observer.last().expect("handler should capture params");
        assert_eq!(params.get("x-rakka-phase"), Some(&vec!["0".to_string()]));
    }

    #[tokio::test]
    async fn rest_reads_are_scoped_by_tenant_header() {
        use crate::task_projection::A2ATaskProjection;
        use rakka::agent_workflow::AgentTimestampMillis;

        let state = test_state();
        state.task_store.upsert(A2ATaskProjection::accepted(
            "task-tenant-a",
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(10),
            Vec::new(),
            0,
        ));
        let app = router(state);

        let cross_tenant = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/a2a/tasks/task-tenant-a")
                    .header("x-rakka-tenant", "tenant-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_tenant.status(), StatusCode::NOT_FOUND);

        let same_tenant = app
            .oneshot(
                Request::builder()
                    .uri("/a2a/tasks/task-tenant-a")
                    .header("x-rakka-tenant", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(same_tenant.status(), StatusCode::OK);
    }

    fn test_state() -> AppState {
        AppState {
            node_id: "test-node#uid".to_string(),
            membership: Arc::new(std::sync::Mutex::new(vec!["test-node#uid".to_string()])),
            agent_card: build_agent_card(&ExampleConfig {
                bind_host: "127.0.0.1".parse().expect("loopback address"),
                advertise_host: "127.0.0.1".to_string(),
                rakka_port: crate::support::DEFAULT_RAKKA_PORT,
                http_port: crate::support::DEFAULT_RAKKA_PORT.saturating_add(10_000),
                node_logical_id: "test-node".to_string(),
                node_incarnation: "test".to_string(),
                discovery_dir: std::env::temp_dir(),
                public_url: None,
            }),
            workflow: demo_workflow(),
            task_store: InMemoryA2ATaskProjectionStore::local(),
            header_observer: HeaderObserver::default(),
        }
    }
}
