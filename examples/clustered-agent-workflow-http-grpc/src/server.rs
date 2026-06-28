//! Process bootstrap: actor system, TCP remoting, sharded run hosting, the
//! selected discovery provider, and one public ingress (HTTP or gRPC).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rakka::cluster::MembershipConfig;
use rakka::http::{serve_with_graceful_shutdown, HttpServerConfig};
use rakka::prelude::{ActorSystem, ClusterSharding, EntityTypeKey};
use rakka::remote::{SerializationRegistry, TcpRemoteTransportConfig};
use rakka::sharding::ClusterNodeRuntime;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tonic::transport::Server;

use crate::codec::JsonPayloadCodec;
use crate::config::{DiscoveryProviderKind, ExampleConfig, PersistenceKind};
use crate::discovery::{new_membership_view, run_file_discovery, seed_file_discovery};
use crate::etcd_discovery::{connect_register, run_etcd_discovery};
use crate::generated::agent_api::agent_workflow_ingress_server::AgentWorkflowIngressServer;
use crate::grpc::AgentWorkflowGrpc;
use crate::http;
use crate::ingress::AppState;
use crate::model::{RunRequest, WorkflowRunView};
use crate::persistence;
use crate::run_entity::{init_run_sharding, RunHost};
use crate::support::{
    current_timestamp_millis, ExampleResult, DEFAULT_CONNECT_TIMEOUT, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RECONNECT_BACKOFF, ENTITY_TYPE, NUMBER_OF_SHARDS,
};
use crate::workflow::demo_workflow;

/// Everything one running node owns, independent of which ingress is serving.
struct Booted {
    config: ExampleConfig,
    system: ActorSystem,
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    discovery_task: JoinHandle<()>,
    shutdown: Arc<Notify>,
    state: AppState,
}

/// Boots one cluster node and serves the HTTP ingress until shutdown.
pub async fn run_http() -> ExampleResult<()> {
    let booted = boot().await?;
    let http_addr = booted.config.http_bind_addr();
    print_banner(&booted, "HTTP", http_addr);
    println!("Submit a compiled workflow with: POST http://{http_addr}/workflows");

    serve_with_graceful_shutdown(
        http::router(booted.state.clone()),
        HttpServerConfig::new(http_addr),
        shutdown_signal(),
    )
    .await?;

    shutdown(booted).await
}

/// Boots one cluster node and serves the gRPC ingress until shutdown.
pub async fn run_grpc() -> ExampleResult<()> {
    let booted = boot().await?;
    let grpc_addr = booted.config.grpc_bind_addr();
    print_banner(&booted, "gRPC", grpc_addr);
    println!(
        "Submit a compiled workflow with: grpc://{grpc_addr} AgentWorkflowIngress/SubmitWorkflow"
    );

    let service = AgentWorkflowIngressServer::new(AgentWorkflowGrpc::new(booted.state.clone()));
    Server::builder()
        .add_service(service)
        .serve_with_shutdown(grpc_addr, shutdown_signal())
        .await?;

    shutdown(booted).await
}

async fn boot() -> ExampleResult<Booted> {
    let config = ExampleConfig::from_env()?;
    let local_node = config.local_node();
    let workflow = demo_workflow();
    let system = ActorSystem::new(format!(
        "clustered-agent-workflow-{}",
        config.node_logical_id
    ));

    // Teach rakka-remote how to move the inter-node ask payloads between nodes.
    let mut registry = SerializationRegistry::new();
    registry.register::<RunRequest, _>(JsonPayloadCodec::<RunRequest>::new(
        "rakka.examples.agent_workflow.RunRequest",
    ))?;
    registry.register::<WorkflowRunView, _>(JsonPayloadCodec::<WorkflowRunView>::new(
        "rakka.examples.agent_workflow.WorkflowRunView",
    ))?;

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
        .with_registry(registry)
        .build()
        .await?;

    let ask_client = runtime.ask_client();
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime)?;
    let key = EntityTypeKey::new(ENTITY_TYPE).with_number_of_shards(NUMBER_OF_SHARDS)?;

    // Durable state lives in a file store (local dev) or shared PostgreSQL
    // (multi-pod recovery), selected by configuration.
    let (run_store, workflow_store) = persistence::build_stores(&config).await?;
    init_run_sharding(
        &system,
        &mut runtime,
        &sharding,
        key.clone(),
        RunHost {
            workflow: workflow.clone(),
            run_store,
            workflow_store,
            node_label: local_node.id().to_string(),
        },
    )?;

    // Seed the selected discovery provider (needs `&mut runtime`), then move the
    // runtime behind a shared lock and spawn the discovery loop.
    let membership = new_membership_view();
    let shutdown = Arc::new(Notify::new());
    let etcd_session = match config.discovery_provider {
        DiscoveryProviderKind::File => {
            seed_file_discovery(&config, &local_node, &mut runtime, &membership)?;
            None
        }
        DiscoveryProviderKind::Etcd => {
            Some(connect_register(&config, &local_node, &mut runtime, &membership).await?)
        }
    };

    let runtime = Arc::new(AsyncMutex::new(runtime));
    let discovery_task = match etcd_session {
        None => tokio::spawn(run_file_discovery(
            runtime.clone(),
            config.clone(),
            local_node.clone(),
            membership.clone(),
            shutdown.clone(),
        )),
        Some(session) => tokio::spawn(run_etcd_discovery(
            session,
            runtime.clone(),
            membership.clone(),
            shutdown.clone(),
        )),
    };

    let state = AppState {
        sharding,
        key,
        ask_client,
        workflow,
        node_label: local_node.id().to_string(),
        membership,
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
    // Signal the discovery loop to clean up (remove file record / revoke etcd
    // lease) and wait briefly for it to finish.
    booted.shutdown.notify_one();
    let _ = tokio::time::timeout(Duration::from_secs(3), booted.discovery_task).await;
    if let Ok(mut runtime) = booted.runtime.try_lock() {
        let _ = runtime.leave_local(current_timestamp_millis());
    }
    booted.system.terminate().await?;
    Ok(())
}

/// Resolves when the process receives SIGINT (Ctrl-C) or SIGTERM (Kubernetes).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn print_banner(booted: &Booted, ingress: &str, addr: SocketAddr) {
    let discovery = match booted.config.discovery_provider {
        DiscoveryProviderKind::File => "file",
        DiscoveryProviderKind::Etcd => "etcd",
    };
    let persistence = match booted.config.persistence {
        PersistenceKind::File => "file",
        PersistenceKind::Postgres => "postgres",
    };
    println!(
        "Rakka clustered agent-workflow node {} | remoting {} | {ingress} ingress {addr}",
        booted.config.node_logical_id, booted.config.rakka_port,
    );
    println!(
        "discovery: {discovery}; persistence: {persistence}; advertise {}:{}",
        booted.config.advertise_host, booted.config.rakka_port,
    );
}
