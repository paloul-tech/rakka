#![forbid(unsafe_code)]

//! Generated gRPC and mirrored HTTP contract example.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{BufRead, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use bytes::Bytes;
use futures_util::stream;
use prost::Message as ProstMessage;
use rakka_cluster::{
    ClusterMembership, ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorRef, ActorSystem, ReplyTo,
};
use rakka_grpc::{
    bidi_streaming_service, client_streaming_service, server_streaming_service, unary_actor_ask,
    unary_entity_ask, unary_service, GrpcError, GrpcResponseStream, GrpcStreamConfig,
    GrpcUnaryConfig,
};
use rakka_http::{
    binary_service_route, json_actor_ask_route, json_entity_ask_route, json_service_route,
    HttpError, HttpRouteConfig,
};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_process::{
    spawn_stdio_actor, ExecutableAllowlist, LineJsonCodec, ProcessSpec, ProcessStdio, StdioCommand,
    StdioProtocolConfig,
};
use rakka_sharding::{
    EntityRef, EntityType, RoutedEntityMessage, ShardCoordinator, ShardRegion, ShardingConfig,
};
use rakka_stream::{bounded_channel, StreamError, StreamSendError};
use rakka_testkit::{assert_http_status, http_post_bytes, http_post_json};
use rakka_workflow::{
    DurableInbox, InboxAcceptance, InboxCommand, WorkflowError, WorkflowId, WorkflowState,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// Generated contract module from `proto/rakka/examples/contracts/v1/store.proto`.
#[allow(missing_docs, clippy::all)]
pub mod contract {
    tonic::include_proto!("rakka.examples.contracts.v1");
}

use contract::cart_live_service_client::CartLiveServiceClient;
use contract::cart_live_service_server::{CartLiveService, CartLiveServiceServer};
use contract::cart_service_client::CartServiceClient;
use contract::cart_service_server::{CartService, CartServiceServer};
use contract::catalog_service_client::CatalogServiceClient;
use contract::catalog_service_server::{CatalogService, CatalogServiceServer};
use contract::counter_service_client::CounterServiceClient;
use contract::counter_service_server::{CounterService, CounterServiceServer};
use contract::ingest_service_client::IngestServiceClient;
use contract::ingest_service_server::{IngestService, IngestServiceServer};
use contract::legacy_service_client::LegacyServiceClient;
use contract::legacy_service_server::{LegacyService, LegacyServiceServer};
use contract::workflow_service_client::WorkflowServiceClient;
use contract::workflow_service_server::{WorkflowService, WorkflowServiceServer};
use contract::{
    CartAck, CartItem, CatalogItem, CatalogRequest, CounterDelta, CounterValue, IngestSummary,
    LegacyReply, LegacyRequest, WorkflowAck, WorkflowCommand,
};

const LEGACY_CHILD_FLAG: &str = "--generated-contract-legacy-child";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

/// Error type used by the generated-contract example.
pub type ContractExampleError = Box<dyn Error + Send + Sync>;

/// Result alias used by the generated-contract example.
pub type ContractExampleResult<T> = Result<T, ContractExampleError>;

/// Options controlling the generated-contract demo runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoOptions {
    legacy_program: PathBuf,
    legacy_args: Vec<String>,
}

impl DemoOptions {
    /// Creates demo options that launch the current example binary as the legacy child.
    pub fn current_exe_child() -> ContractExampleResult<Self> {
        Ok(Self::new(std::env::current_exe()?).arg(LEGACY_CHILD_FLAG))
    }

    /// Creates demo options with an explicit legacy child executable.
    #[must_use]
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            legacy_program: program.into(),
            legacy_args: Vec::new(),
        }
    }

    /// Adds one legacy child argument.
    #[must_use]
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.legacy_args.push(arg.into());
        self
    }

    /// Legacy child executable path.
    #[must_use]
    pub fn legacy_program(&self) -> &Path {
        &self.legacy_program
    }

    /// Legacy child arguments.
    #[must_use]
    pub fn legacy_args(&self) -> &[String] {
        &self.legacy_args
    }
}

/// Summary produced by the generated-contract demo.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractDemoReport {
    /// Counter value returned through generated gRPC unary actor ask.
    pub grpc_counter_value: i64,
    /// SKU accepted through generated gRPC unary entity ask.
    pub grpc_cart_sku: String,
    /// Catalog SKUs returned through generated gRPC server streaming.
    pub grpc_catalog_items: Vec<String>,
    /// Number of uploaded cart items accepted through generated gRPC client streaming.
    pub grpc_ingested_count: u64,
    /// Number of acknowledgements returned through generated gRPC bidirectional streaming.
    pub grpc_bidi_ack_count: usize,
    /// Durable workflow revision returned through generated gRPC unary service.
    pub grpc_workflow_revision: u64,
    /// Legacy child process result returned through generated gRPC unary service.
    pub grpc_legacy_result: u64,
    /// Counter value returned through mirrored HTTP JSON actor route.
    pub http_counter_value: i64,
    /// SKU accepted through mirrored HTTP JSON entity route.
    pub http_cart_sku: String,
    /// Counter value returned through mirrored HTTP binary/protobuf route.
    pub http_binary_counter_value: i64,
    /// Durable workflow revision returned through mirrored HTTP JSON service route.
    pub http_workflow_revision: u64,
    /// Legacy child process result returned through mirrored HTTP JSON service route.
    pub http_legacy_result: u64,
    /// Entity events recorded by gRPC, HTTP, and stream adapters.
    pub cart_events: Vec<String>,
}

/// Runs the generated-contract demo using the current executable as its child process.
pub async fn run_generated_contract_demo() -> ContractExampleResult<ContractDemoReport> {
    run_generated_contract_demo_with_options(DemoOptions::current_exe_child()?).await
}

/// Runs the generated-contract demo with explicit legacy child process options.
pub async fn run_generated_contract_demo_with_options(
    options: DemoOptions,
) -> ContractExampleResult<ContractDemoReport> {
    let app = ContractApp::new(options).await?;
    let grpc = app.spawn_grpc_server().await?;
    let endpoint = grpc.endpoint.clone();

    let grpc_counter_value = call_counter_grpc(&endpoint).await?;
    let grpc_cart_sku = call_cart_grpc(&endpoint).await?;
    let grpc_catalog_items = call_catalog_grpc(&endpoint).await?;
    let grpc_ingested_count = call_ingest_grpc(&endpoint).await?;
    let grpc_bidi_ack_count = call_bidi_grpc(&endpoint).await?;
    let grpc_workflow_revision = call_workflow_grpc(&endpoint).await?;
    let grpc_legacy_result = call_legacy_grpc(&endpoint).await?;

    let http_router = app.http_router();
    let http_counter_value = call_counter_http(http_router.clone()).await?;
    let http_cart_sku = call_cart_http(http_router.clone()).await?;
    let http_binary_counter_value = call_counter_binary_http(http_router.clone()).await?;
    let http_workflow_revision = call_workflow_http(http_router.clone()).await?;
    let http_legacy_result = call_legacy_http(http_router).await?;

    grpc.abort();
    app.shutdown()?;

    Ok(ContractDemoReport {
        grpc_counter_value,
        grpc_cart_sku,
        grpc_catalog_items,
        grpc_ingested_count,
        grpc_bidi_ack_count,
        grpc_workflow_revision,
        grpc_legacy_result,
        http_counter_value,
        http_cart_sku,
        http_binary_counter_value,
        http_workflow_revision,
        http_legacy_result,
        cart_events: app.cart_events(),
    })
}

/// Runs the line-json legacy child protocol used by the demo process actor.
pub fn run_legacy_child() -> ContractExampleResult<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        let frame: serde_json::Value = serde_json::from_str(&line)?;
        let request_id = frame
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| example_error("line-json request is missing id"))?;
        let request: LegacyRequest = serde_json::from_value(
            frame
                .get("payload")
                .cloned()
                .ok_or_else(|| example_error("line-json request is missing payload"))?,
        )?;
        let response = serde_json::json!({
            "id": request_id,
            "payload": {
                "service": "generated-contract-legacy",
                "result": request.value + 1,
            },
        });
        writeln!(stdout, "{response}")?;
        stdout.flush()?;
    }
    Ok(())
}

struct ContractApp {
    system: ActorSystem,
    counter: ActorRef<CounterCommand>,
    cart_region: ShardRegion<CartCommand>,
    cart_entity: EntityRef<CartCommand>,
    cart_events: Arc<Mutex<Vec<String>>>,
    workflow_store: InMemoryDurableStateStore<WorkflowState>,
    legacy: ActorRef<StdioCommand<LegacyRequest, LegacyReply>>,
}

type CartRegionParts = (
    ShardRegion<CartCommand>,
    EntityRef<CartCommand>,
    Arc<Mutex<Vec<String>>>,
);

impl ContractApp {
    async fn new(options: DemoOptions) -> ContractExampleResult<Self> {
        let system = ActorSystem::new("generated-contract-example");
        let counter = system.spawn_actor("counter", CounterActor { value: 0 })?;
        let (cart_region, cart_entity, cart_events) = cart_region()?;
        let workflow_store = InMemoryDurableStateStore::<WorkflowState>::new();
        let legacy = spawn_legacy_actor(&system, &options)?;

        Ok(Self {
            system,
            counter,
            cart_region,
            cart_entity,
            cart_events,
            workflow_store,
            legacy,
        })
    }

    fn http_router(&self) -> Router {
        json_actor_ask_route(
            "/contract/counter/add",
            HttpRouteConfig::default(),
            self.counter.clone(),
            |request: CounterDelta, reply_to| CounterCommand::Add {
                amount: request.amount,
                reply_to,
            },
        )
        .merge(json_entity_ask_route(
            "/contract/cart/add",
            HttpRouteConfig::default(),
            self.cart_region.clone(),
            self.cart_entity.clone(),
            |request: CartItem, reply_to| CartCommand::Add {
                sku: request.sku,
                reply_to,
            },
        ))
        .merge(binary_service_route(
            "/contract/counter/add.bin",
            HttpRouteConfig::default(),
            {
                let counter = self.counter.clone();
                move |bytes| {
                    let counter = counter.clone();
                    async move { counter_binary_handler(counter, bytes).await }
                }
            },
        ))
        .merge(json_service_route(
            "/contract/workflow/submit",
            HttpRouteConfig::default(),
            {
                let store = self.workflow_store.clone();
                move |request| {
                    let store = store.clone();
                    async move {
                        submit_workflow(store, request)
                            .await
                            .map_err(|error| HttpError::service(error.to_string()))
                    }
                }
            },
        ))
        .merge(json_service_route(
            "/contract/legacy/increment",
            HttpRouteConfig::default(),
            {
                let legacy = self.legacy.clone();
                move |request| {
                    let legacy = legacy.clone();
                    async move {
                        call_legacy_actor(&legacy, request)
                            .await
                            .map_err(|error| HttpError::service(error.to_string()))
                    }
                }
            },
        ))
    }

    async fn spawn_grpc_server(&self) -> ContractExampleResult<GeneratedGrpcServer> {
        let listener =
            TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
        let address = listener.local_addr()?;
        let endpoint = format!("http://{address}");
        let incoming = TcpListenerStream::new(listener);
        let server = Server::builder()
            .add_service(CounterServiceServer::new(CounterGrpc {
                counter: self.counter.clone(),
            }))
            .add_service(CartServiceServer::new(CartGrpc {
                region: self.cart_region.clone(),
                entity: self.cart_entity.clone(),
            }))
            .add_service(CatalogServiceServer::new(CatalogGrpc))
            .add_service(IngestServiceServer::new(IngestGrpc {
                events: Arc::clone(&self.cart_events),
            }))
            .add_service(CartLiveServiceServer::new(CartLiveGrpc {
                region: self.cart_region.clone(),
                entity: self.cart_entity.clone(),
            }))
            .add_service(WorkflowServiceServer::new(WorkflowGrpc {
                store: self.workflow_store.clone(),
            }))
            .add_service(LegacyServiceServer::new(LegacyGrpc {
                legacy: self.legacy.clone(),
            }))
            .serve_with_incoming(incoming);
        let task = tokio::spawn(server);
        Ok(GeneratedGrpcServer { endpoint, task })
    }

    fn cart_events(&self) -> Vec<String> {
        self.cart_events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn shutdown(&self) -> ContractExampleResult<()> {
        self.legacy.stop()?;
        self.system.shutdown();
        Ok(())
    }
}

struct GeneratedGrpcServer {
    endpoint: String,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl GeneratedGrpcServer {
    fn abort(self) {
        self.task.abort();
    }
}

enum CounterCommand {
    Add {
        amount: i64,
        reply_to: ReplyTo<CounterValue>,
    },
}

struct CounterActor {
    value: i64,
}

impl Actor for CounterActor {
    type Msg = CounterCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        match msg {
            CounterCommand::Add { amount, reply_to } => {
                self.value += amount;
                let value = self.value;
                actor_future(async move {
                    let _sent = reply_to.reply(CounterValue { value });
                    Ok(ActorAction::Continue)
                })
            }
        }
    }
}

enum CartCommand {
    Add {
        sku: String,
        reply_to: ReplyTo<CartAck>,
    },
}

#[derive(Clone)]
struct CounterGrpc {
    counter: ActorRef<CounterCommand>,
}

#[tonic::async_trait]
impl CounterService for CounterGrpc {
    async fn add(&self, request: Request<CounterDelta>) -> Result<Response<CounterValue>, Status> {
        unary_actor_ask(
            request,
            GrpcUnaryConfig::default(),
            &self.counter,
            |request, reply_to| CounterCommand::Add {
                amount: request.amount,
                reply_to,
            },
        )
        .await
    }
}

#[derive(Clone)]
struct CartGrpc {
    region: ShardRegion<CartCommand>,
    entity: EntityRef<CartCommand>,
}

#[tonic::async_trait]
impl CartService for CartGrpc {
    async fn add_item(&self, request: Request<CartItem>) -> Result<Response<CartAck>, Status> {
        unary_entity_ask(
            request,
            GrpcUnaryConfig::default(),
            &self.region,
            &self.entity,
            |request, reply_to| CartCommand::Add {
                sku: request.sku,
                reply_to,
            },
        )
        .await
    }
}

#[derive(Clone)]
struct CatalogGrpc;

#[tonic::async_trait]
impl CatalogService for CatalogGrpc {
    type ListStream = GrpcResponseStream<CatalogItem>;

    async fn list(
        &self,
        request: Request<CatalogRequest>,
    ) -> Result<Response<Self::ListStream>, Status> {
        server_streaming_service(
            request,
            GrpcStreamConfig::default().buffer_capacity(8),
            |request: CatalogRequest| async move {
                let (sink, source) = bounded_channel(8).map_err(stream_status)?;
                for sku in ["book", "box", "pencil", "paper"]
                    .into_iter()
                    .filter(|sku| sku.starts_with(&request.prefix))
                {
                    sink.try_send(CatalogItem {
                        sku: sku.to_owned(),
                    })
                    .map_err(stream_send_status)?;
                }
                sink.drain().map_err(stream_status)?;
                Ok(source)
            },
        )
        .await
    }
}

#[derive(Clone)]
struct IngestGrpc {
    events: Arc<Mutex<Vec<String>>>,
}

#[tonic::async_trait]
impl IngestService for IngestGrpc {
    async fn upload(
        &self,
        request: Request<tonic::Streaming<CartItem>>,
    ) -> Result<Response<IngestSummary>, Status> {
        let events = Arc::clone(&self.events);
        client_streaming_service(
            request,
            GrpcStreamConfig::default().buffer_capacity(2),
            move |mut inbound| async move {
                let mut count = 0u64;
                while let Some(item) = inbound.source().next().await.map_err(stream_status)? {
                    events
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(format!("grpc-client-stream:{}", item.sku));
                    count = count.saturating_add(1);
                }
                inbound.join().await?;
                Ok(IngestSummary { accepted: count })
            },
        )
        .await
    }
}

#[derive(Clone)]
struct CartLiveGrpc {
    region: ShardRegion<CartCommand>,
    entity: EntityRef<CartCommand>,
}

#[tonic::async_trait]
impl CartLiveService for CartLiveGrpc {
    type WatchStream = GrpcResponseStream<CartAck>;

    async fn watch(
        &self,
        request: Request<tonic::Streaming<CartItem>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let region = self.region.clone();
        let entity = self.entity.clone();
        bidi_streaming_service(
            request,
            GrpcStreamConfig::default().buffer_capacity(2),
            move |inbound, outbound| async move {
                while let Some(item) = inbound.next().await.map_err(stream_status)? {
                    let ack = ask_cart(&region, &entity, item.sku).await?;
                    outbound.send(ack).await.map_err(stream_send_status)?;
                }
                Ok(())
            },
        )
    }
}

#[derive(Clone)]
struct WorkflowGrpc {
    store: InMemoryDurableStateStore<WorkflowState>,
}

#[tonic::async_trait]
impl WorkflowService for WorkflowGrpc {
    async fn submit(
        &self,
        request: Request<WorkflowCommand>,
    ) -> Result<Response<WorkflowAck>, Status> {
        unary_service(request, GrpcUnaryConfig::default(), {
            let store = self.store.clone();
            move |request| async move {
                submit_workflow(store, request)
                    .await
                    .map_err(|error| GrpcError::service(error.to_string()).into_status())
            }
        })
        .await
    }
}

#[derive(Clone)]
struct LegacyGrpc {
    legacy: ActorRef<StdioCommand<LegacyRequest, LegacyReply>>,
}

#[tonic::async_trait]
impl LegacyService for LegacyGrpc {
    async fn increment(
        &self,
        request: Request<LegacyRequest>,
    ) -> Result<Response<LegacyReply>, Status> {
        unary_service(request, GrpcUnaryConfig::default(), {
            let legacy = self.legacy.clone();
            move |request| async move {
                call_legacy_actor(&legacy, request)
                    .await
                    .map_err(|error| GrpcError::service(error.to_string()).into_status())
            }
        })
        .await
    }
}

async fn call_counter_grpc(endpoint: &str) -> ContractExampleResult<i64> {
    let mut client = CounterServiceClient::connect(endpoint.to_owned()).await?;
    let response = client
        .add(Request::new(CounterDelta { amount: 7 }))
        .await?
        .into_inner();
    Ok(response.value)
}

async fn call_cart_grpc(endpoint: &str) -> ContractExampleResult<String> {
    let mut client = CartServiceClient::connect(endpoint.to_owned()).await?;
    let response = client
        .add_item(Request::new(CartItem {
            cart_id: "cart-1".to_owned(),
            sku: "book".to_owned(),
        }))
        .await?
        .into_inner();
    Ok(response.sku)
}

async fn call_catalog_grpc(endpoint: &str) -> ContractExampleResult<Vec<String>> {
    let mut client = CatalogServiceClient::connect(endpoint.to_owned()).await?;
    let mut stream = client
        .list(Request::new(CatalogRequest {
            prefix: "b".to_owned(),
        }))
        .await?
        .into_inner();
    let mut items = Vec::new();
    while let Some(item) = stream.message().await? {
        items.push(item.sku);
    }
    Ok(items)
}

async fn call_ingest_grpc(endpoint: &str) -> ContractExampleResult<u64> {
    let mut client = IngestServiceClient::connect(endpoint.to_owned()).await?;
    let response = client
        .upload(Request::new(stream::iter([
            CartItem {
                cart_id: "cart-1".to_owned(),
                sku: "paper".to_owned(),
            },
            CartItem {
                cart_id: "cart-1".to_owned(),
                sku: "folder".to_owned(),
            },
        ])))
        .await?
        .into_inner();
    Ok(response.accepted)
}

async fn call_bidi_grpc(endpoint: &str) -> ContractExampleResult<usize> {
    let mut client = CartLiveServiceClient::connect(endpoint.to_owned()).await?;
    let mut stream = client
        .watch(Request::new(stream::iter([
            CartItem {
                cart_id: "cart-1".to_owned(),
                sku: "eraser".to_owned(),
            },
            CartItem {
                cart_id: "cart-1".to_owned(),
                sku: "ruler".to_owned(),
            },
        ])))
        .await?
        .into_inner();
    let mut count = 0usize;
    while let Some(_ack) = stream.message().await? {
        count = count.saturating_add(1);
    }
    Ok(count)
}

async fn call_workflow_grpc(endpoint: &str) -> ContractExampleResult<u64> {
    let mut client = WorkflowServiceClient::connect(endpoint.to_owned()).await?;
    let response = client
        .submit(Request::new(WorkflowCommand {
            workflow_id: "checkout".to_owned(),
            command_id: "grpc-command-1".to_owned(),
        }))
        .await?
        .into_inner();
    Ok(response.revision)
}

async fn call_legacy_grpc(endpoint: &str) -> ContractExampleResult<u64> {
    let mut client = LegacyServiceClient::connect(endpoint.to_owned()).await?;
    let response = client
        .increment(Request::new(LegacyRequest {
            command: "increment".to_owned(),
            value: 41,
        }))
        .await?
        .into_inner();
    Ok(response.result)
}

async fn call_counter_http(router: Router) -> ContractExampleResult<i64> {
    let response =
        http_post_json(router, "/contract/counter/add", &CounterDelta { amount: 5 }).await;
    assert_http_status(&response, axum::http::StatusCode::OK);
    Ok(response.json::<CounterValue>().value)
}

async fn call_cart_http(router: Router) -> ContractExampleResult<String> {
    let response = http_post_json(
        router,
        "/contract/cart/add",
        &CartItem {
            cart_id: "cart-1".to_owned(),
            sku: "pencil".to_owned(),
        },
    )
    .await;
    assert_http_status(&response, axum::http::StatusCode::OK);
    Ok(response.json::<CartAck>().sku)
}

async fn call_counter_binary_http(router: Router) -> ContractExampleResult<i64> {
    let mut body = Vec::new();
    CounterDelta { amount: 11 }.encode(&mut body)?;
    let response = http_post_bytes(
        router,
        "/contract/counter/add.bin",
        body,
        "application/octet-stream",
    )
    .await;
    assert_http_status(&response, axum::http::StatusCode::OK);
    let reply = CounterValue::decode(response.body().clone())?;
    Ok(reply.value)
}

async fn call_workflow_http(router: Router) -> ContractExampleResult<u64> {
    let response = http_post_json(
        router,
        "/contract/workflow/submit",
        &WorkflowCommand {
            workflow_id: "checkout".to_owned(),
            command_id: "http-command-1".to_owned(),
        },
    )
    .await;
    assert_http_status(&response, axum::http::StatusCode::OK);
    Ok(response.json::<WorkflowAck>().revision)
}

async fn call_legacy_http(router: Router) -> ContractExampleResult<u64> {
    let response = http_post_json(
        router,
        "/contract/legacy/increment",
        &LegacyRequest {
            command: "increment".to_owned(),
            value: 99,
        },
    )
    .await;
    assert_http_status(&response, axum::http::StatusCode::OK);
    Ok(response.json::<LegacyReply>().result)
}

async fn counter_binary_handler(
    counter: ActorRef<CounterCommand>,
    bytes: Bytes,
) -> Result<Bytes, HttpError> {
    let request =
        CounterDelta::decode(bytes).map_err(|error| HttpError::service(error.to_string()))?;
    let response = counter
        .ask(
            |reply_to| CounterCommand::Add {
                amount: request.amount,
                reply_to,
            },
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(|error| HttpError::service(error.to_string()))?;
    let mut encoded = Vec::new();
    response
        .encode(&mut encoded)
        .map_err(|error| HttpError::service(error.to_string()))?;
    Ok(Bytes::from(encoded))
}

// `tonic::Status` is the protocol's error type the service handler answers
// with, so the helper's error is not ours to box.
#[allow(clippy::result_large_err)]
async fn ask_cart(
    region: &ShardRegion<CartCommand>,
    entity: &EntityRef<CartCommand>,
    sku: String,
) -> Result<CartAck, Status> {
    region
        .ask(
            entity,
            |reply_to| CartCommand::Add { sku, reply_to },
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))
}

async fn submit_workflow(
    store: InMemoryDurableStateStore<WorkflowState>,
    command: WorkflowCommand,
) -> Result<WorkflowAck, WorkflowError> {
    let workflow_id = WorkflowId::new(command.workflow_id.clone());
    let mut inbox = DurableInbox::new(workflow_id.clone(), store);
    inbox.recover().await?;
    let acceptance = inbox
        .accept(
            InboxCommand::new(
                command.command_id.clone(),
                "rakka.examples.contracts.v1.WorkflowCommand",
                command.command_id.as_bytes().to_vec(),
            )
            .deduplication_key(command.command_id.clone()),
        )
        .await?;
    let duplicate = matches!(acceptance, InboxAcceptance::Duplicate { .. });
    Ok(WorkflowAck {
        workflow_id: workflow_id.as_str().to_owned(),
        message_id: acceptance.entry().message_id().as_str().to_owned(),
        revision: acceptance.revision().get(),
        duplicate,
    })
}

async fn call_legacy_actor(
    legacy: &ActorRef<StdioCommand<LegacyRequest, LegacyReply>>,
    request: LegacyRequest,
) -> Result<LegacyReply, String> {
    legacy
        .ask(
            |reply_to| StdioCommand::Request { request, reply_to },
            DEFAULT_TIMEOUT,
        )
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
}

fn cart_region() -> ContractExampleResult<CartRegionParts> {
    let node = example_node();
    let membership = membership_with_up_node(node.clone())?;
    let entity_type = EntityType::new("GeneratedContractCart");
    let sharding = ShardingConfig::new(8)?;
    let mut coordinator = ShardCoordinator::new(entity_type.clone(), sharding.clone());
    coordinator.reconcile(&membership);
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_route = Arc::clone(&events);
    let region = ShardRegion::from_snapshot(
        entity_type,
        sharding,
        &coordinator.snapshot(),
        move |routed: RoutedEntityMessage<CartCommand>| {
            let entity_id = routed.entity_id().as_str().to_owned();
            match routed.into_message() {
                CartCommand::Add { sku, reply_to } => {
                    events_for_route
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(format!("cart:{sku}"));
                    let _sent = reply_to.reply(CartAck {
                        accepted: true,
                        cart_id: entity_id,
                        sku,
                    });
                }
            }
            Ok(())
        },
    )?;
    let entity = region.entity_ref("cart-1");
    Ok((region, entity, events))
}

fn example_node() -> ClusterNode {
    ClusterNode::new(
        NodeId::new("generated-contract-node", "uid-a"),
        NodeAddress::new(
            "generated-contract-node.rakka.default.svc.cluster.local",
            2552,
        ),
    )
    .with_role("generated-contracts")
}

fn membership_with_up_node(node: ClusterNode) -> ContractExampleResult<ClusterMembership> {
    let mut membership = ClusterMembership::new(
        node.clone(),
        MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100)),
    );
    membership.record_discovery(DiscoverySnapshot::new(
        "generated-contracts",
        1,
        [node.clone()],
    ))?;
    membership.mark_up(node.id(), 2)?;
    Ok(membership)
}

fn spawn_legacy_actor(
    system: &ActorSystem,
    options: &DemoOptions,
) -> ContractExampleResult<ActorRef<StdioCommand<LegacyRequest, LegacyReply>>> {
    let allowlist = ExecutableAllowlist::from_exact_paths([options.legacy_program().to_path_buf()]);
    let mut spec = ProcessSpec::new(options.legacy_program().to_path_buf())
        .stdin(ProcessStdio::Piped)
        .stdout(ProcessStdio::Piped)
        .stderr(ProcessStdio::Piped)
        .shutdown_timeout(Duration::from_secs(1));
    for arg in options.legacy_args() {
        spec = spec.arg(arg);
    }

    Ok(spawn_stdio_actor(
        system,
        "generated-contract-legacy",
        spec,
        allowlist,
        LineJsonCodec::<LegacyRequest, LegacyReply>::new(),
        StdioProtocolConfig::new().default_request_timeout(DEFAULT_TIMEOUT),
    )?)
}

fn stream_status(error: StreamError) -> Status {
    Status::internal(error.to_string())
}

fn stream_send_status<T>(error: StreamSendError<T>) -> Status {
    Status::internal(error.to_string())
}

fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

impl Display for ContractDemoReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "generated contracts: grpc counter {}, grpc cart {}, catalog {} item(s), uploaded {}, bidi {}, workflow rev {}, legacy {}; http counter {}, http cart {}, binary counter {}, workflow rev {}, legacy {}",
            self.grpc_counter_value,
            self.grpc_cart_sku,
            self.grpc_catalog_items.len(),
            self.grpc_ingested_count,
            self.grpc_bidi_ack_count,
            self.grpc_workflow_revision,
            self.grpc_legacy_result,
            self.http_counter_value,
            self.http_cart_sku,
            self.http_binary_counter_value,
            self.http_workflow_revision,
            self.http_legacy_result
        )
    }
}
