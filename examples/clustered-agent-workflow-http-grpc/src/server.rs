//! Process bootstrap: actor system, TCP remoting, sharded run hosting, and one
//! public ingress (HTTP or gRPC).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rakka::agent_workflow::substrate::WorkflowState;
use rakka::agent_workflow::AgentRunState;
use rakka::cluster::{ClusterNode, MembershipConfig};
use rakka::http::{serve_with_graceful_shutdown, HttpServerConfig};
use rakka::prelude::{ActorSystem, ClusterSharding, EntityTypeKey};
use rakka::remote::{SerializationRegistry, TcpRemoteTransportConfig};
use rakka::sharding::ClusterNodeRuntime;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tonic::transport::Server;

use crate::codec::JsonPayloadCodec;
use crate::config::ExampleConfig;
use crate::discovery::{discovery_loop, publish_and_apply_discovery, remove_discovery_record};
use crate::generated::agent_api::agent_workflow_ingress_server::AgentWorkflowIngressServer;
use crate::grpc::AgentWorkflowGrpc;
use crate::http;
use crate::ingress::AppState;
use crate::model::{RunRequest, WorkflowRunView};
use crate::run_entity::{init_run_sharding, RunHost};
use crate::store::FileDurableStateStore;
use crate::support::{
    current_timestamp_millis, ExampleResult, DEFAULT_CONNECT_TIMEOUT, DEFAULT_IDLE_TIMEOUT,
    DEFAULT_RECONNECT_BACKOFF, ENTITY_TYPE, NUMBER_OF_SHARDS,
};
use crate::workflow::demo_workflow;

/// Everything one running node owns, independent of which ingress is serving.
struct Booted {
    config: ExampleConfig,
    local_node: ClusterNode,
    system: ActorSystem,
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    discovery_task: JoinHandle<()>,
    state: AppState,
}

/// Boots one cluster node and serves the HTTP ingress until Ctrl-C.
pub async fn run_http() -> ExampleResult<()> {
    let booted = boot().await?;
    let http_addr = booted.config.http_bind_addr();
    print_banner(&booted, "HTTP", http_addr);
    println!("Submit a compiled workflow with: POST http://{http_addr}/workflows");

    serve_with_graceful_shutdown(
        http::router(booted.state.clone()),
        HttpServerConfig::new(http_addr),
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await?;

    shutdown(booted).await
}

/// Boots one cluster node and serves the gRPC ingress until Ctrl-C.
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
        .serve_with_shutdown(grpc_addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
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

    let run_store = FileDurableStateStore::<AgentRunState>::new(
        config.run_state_dir.clone(),
        "example-file-run",
    );
    let workflow_store = FileDurableStateStore::<WorkflowState>::new(
        config.workflow_state_dir.clone(),
        "example-file-workflow",
    );

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

    publish_and_apply_discovery(&config, &local_node, &mut runtime)?;
    let runtime = Arc::new(AsyncMutex::new(runtime));
    let discovery_task = tokio::spawn(discovery_loop(
        runtime.clone(),
        config.clone(),
        local_node.clone(),
    ));

    let state = AppState {
        sharding,
        key,
        ask_client,
        workflow,
        node_label: local_node.id().to_string(),
        discovery_dir: config.discovery_dir.clone(),
        local_node: local_node.clone(),
    };

    Ok(Booted {
        config,
        local_node,
        system,
        runtime,
        discovery_task,
        state,
    })
}

async fn shutdown(booted: Booted) -> ExampleResult<()> {
    booted.discovery_task.abort();
    let _ = remove_discovery_record(
        &booted.config.discovery_dir,
        booted.local_node.id().logical_id(),
    );
    if let Ok(mut runtime) = booted.runtime.try_lock() {
        let _ = runtime.leave_local(current_timestamp_millis());
    }
    booted.system.terminate().await?;
    Ok(())
}

fn print_banner(booted: &Booted, ingress: &str, addr: SocketAddr) {
    println!(
        "Rakka clustered agent-workflow node {} | remoting {} | {ingress} ingress {addr}",
        booted.local_node.id(),
        booted.config.tcp_bind_addr(),
    );
    println!(
        "Discovery dir: {}; run state: {}; workflow state: {}",
        booted.config.discovery_dir.display(),
        booted.config.run_state_dir.display(),
        booted.config.workflow_state_dir.display(),
    );
}
