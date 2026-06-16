#![forbid(unsafe_code)]

//! Clustered, sharded, persistent counter exposed through REST/JSON.

use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path as AxumPath, State};
use axum::routing::{get, post};
use axum::Json;
use rakka::cluster::{ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId};
use rakka::http::{serve_with_graceful_shutdown, HttpError, HttpRouteConfig, HttpServerConfig};
use rakka::persistence::{DurableError, DurableResult, StateRecord, StoreFuture};
use rakka::prelude::*;
use rakka::remote::{
    PayloadCodec, RemoteError, RemoteRequestError, RemoteResult, SerializationRegistry,
    TcpRemoteTransportConfig,
};
use rakka::sharding::{
    ClusterNodeRuntime, EntityAskError, RemoteEntityAskClient, RemoteEntityAskError,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;

type ExampleError = Box<dyn Error + Send + Sync>;
type ExampleResult<T> = Result<T, ExampleError>;

const ENTITY_TYPE: &str = "Counter";
const DEFAULT_RAKKA_TCP_PORT: u16 = 25520;
const DEFAULT_DISCOVERY_POLL: Duration = Duration::from_millis(750);
const DEFAULT_DISCOVERY_TTL: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(25);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CounterState {
    value: i64,
    initialized: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum CounterAction {
    Initiate,
    Get,
    Increase,
    Decrease,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CounterOperation {
    name: String,
    amount: i64,
    action: CounterAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CounterValue {
    name: String,
    value: i64,
    revision: u64,
    initialized: bool,
    created: bool,
    owner_node: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct InitiateCounterRequest {
    #[serde(default)]
    initial_value: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct ChangeCounterRequest {
    #[serde(default = "default_change_amount")]
    amount: i64,
}

enum CounterCommand {
    Apply {
        operation: CounterOperation,
        reply_to: ReplyTo<CounterValue>,
    },
}

struct CounterDurableActor {
    persistence_id: PersistenceId,
    owner_node: String,
}

impl DurableActor for CounterDurableActor {
    type Command = CounterCommand;
    type State = CounterState;

    fn persistence_id(&self) -> PersistenceId {
        self.persistence_id.clone()
    }

    fn empty_state(&self) -> Self::State {
        CounterState {
            value: 0,
            initialized: false,
        }
    }

    fn handle_command<'a>(
        &'a mut self,
        ctx: &'a mut DurableActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> DurableActorFuture<'a, Self::State> {
        match command {
            CounterCommand::Apply {
                operation,
                reply_to,
            } => {
                let created = !state.initialized;
                let current_revision = ctx.revision();
                let owner_node = self.owner_node.clone();
                let name = operation.name;
                let amount = operation.amount;

                match operation.action {
                    CounterAction::Initiate if state.initialized => {
                        let reply = ctx.reply_after_commit(
                            reply_to,
                            CounterValue {
                                name,
                                value: state.value,
                                revision: current_revision.get(),
                                initialized: true,
                                created: false,
                                owner_node,
                            },
                        );
                        durable_actor_future(
                            async move { Ok(DurableEffect::none().then_run(reply)) },
                        )
                    }
                    CounterAction::Initiate => {
                        let next = CounterState {
                            value: amount,
                            initialized: true,
                        };
                        persist_counter_reply(ctx, reply_to, name, next, created, owner_node)
                    }
                    CounterAction::Increase => {
                        let next = CounterState {
                            value: state.value.saturating_add(amount),
                            initialized: true,
                        };
                        persist_counter_reply(ctx, reply_to, name, next, created, owner_node)
                    }
                    CounterAction::Decrease => {
                        let next = CounterState {
                            value: state.value.saturating_sub(amount),
                            initialized: true,
                        };
                        persist_counter_reply(ctx, reply_to, name, next, created, owner_node)
                    }
                    CounterAction::Get => {
                        let reply = ctx.reply_after_commit(
                            reply_to,
                            CounterValue {
                                name,
                                value: state.value,
                                revision: current_revision.get(),
                                initialized: state.initialized,
                                created: false,
                                owner_node,
                            },
                        );
                        durable_actor_future(
                            async move { Ok(DurableEffect::none().then_run(reply)) },
                        )
                    }
                }
            }
        }
    }
}

fn persist_counter_reply<'a>(
    ctx: &DurableActorContext<'a, CounterCommand>,
    reply_to: ReplyTo<CounterValue>,
    name: String,
    next: CounterState,
    created: bool,
    owner_node: String,
) -> DurableActorFuture<'a, CounterState> {
    let revision = ctx.revision().next().get();
    let reply_value = CounterValue {
        name,
        value: next.value,
        revision,
        initialized: next.initialized,
        created,
        owner_node,
    };
    let reply = ctx.reply_after_commit(reply_to, reply_value);
    durable_actor_future(async move { Ok(DurableEffect::persist(next).then_run(reply)) })
}

struct CounterEntity<Store>
where
    Store: DurableStateStore<CounterState>,
{
    child: ActorRef<CounterCommand>,
    _store: PhantomData<Store>,
}

impl<Store> CounterEntity<Store>
where
    Store: DurableStateStore<CounterState>,
{
    fn new(
        system: ActorSystem,
        context: EntityContext<CounterCommand>,
        store: Store,
        owner_node: String,
    ) -> Self {
        let persistence_id =
            PersistenceId::of(context.entity_type().as_str(), context.entity_id().as_str())
                .expect("counter entity ids are validated before routing");
        let actor_name = format!("{}-durable", context.actor_name());
        let actor_persistence_id = persistence_id.clone();
        let actor_owner_node = owner_node.clone();
        let child = spawn_durable_actor_factory(
            &system,
            actor_name,
            move || CounterDurableActor {
                persistence_id: actor_persistence_id.clone(),
                owner_node: actor_owner_node.clone(),
            },
            store,
        )
        .expect("counter durable actor should spawn");

        Self {
            child,
            _store: PhantomData,
        }
    }
}

impl<Store> Actor for CounterEntity<Store>
where
    Store: DurableStateStore<CounterState>,
{
    type Msg = CounterCommand;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        actor_future(async move {
            child.tell(msg).map_err(|_error| {
                RakkaError::core(
                    "counter-forward-failed",
                    "counter durable child mailbox was unavailable",
                )
            })?;
            Ok(ActorAction::Continue)
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a TerminationReason,
    ) -> ActorFuture<'a> {
        let child = self.child.clone();
        actor_future(async move {
            let _ = child.stop();
            Ok(ActorAction::Continue)
        })
    }
}

#[derive(Debug, Clone)]
struct FileCounterStateStore {
    root: Arc<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredCounterState {
    revision: u64,
    value: i64,
    initialized: bool,
}

impl FileCounterStateStore {
    fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
        }
    }

    fn record_path(&self, persistence_id: &PersistenceId) -> PathBuf {
        self.root
            .join(format!("{}.json", hex_encode(persistence_id.as_str())))
    }

    fn load_record(
        &self,
        persistence_id: &PersistenceId,
    ) -> DurableResult<Option<StateRecord<CounterState>>> {
        let path = self.record_path(persistence_id);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(file_store_error(error)),
        };
        let stored: StoredCounterState = serde_json::from_slice(&bytes)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        Ok(Some(StateRecord::new(
            CounterState {
                value: stored.value,
                initialized: stored.initialized,
            },
            Revision::new(stored.revision),
        )))
    }

    fn write_record(
        &self,
        persistence_id: &PersistenceId,
        record: &StateRecord<CounterState>,
    ) -> DurableResult<()> {
        std::fs::create_dir_all(self.root.as_ref()).map_err(file_store_error)?;
        let path = self.record_path(persistence_id);
        let temp = path.with_extension(format!("json.tmp.{}", current_timestamp_millis()));
        let stored = StoredCounterState {
            revision: record.revision.get(),
            value: record.state.value,
            initialized: record.state.initialized,
        };
        let bytes = serde_json::to_vec_pretty(&stored)
            .map_err(|error| DurableError::codec(error.to_string()))?;
        std::fs::write(&temp, bytes).map_err(file_store_error)?;
        std::fs::rename(&temp, &path).map_err(file_store_error)?;
        Ok(())
    }
}

impl DurableStateStore<CounterState> for FileCounterStateStore {
    fn backend_name(&self) -> &'static str {
        "example-file"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<CounterState>>> {
        Box::pin(async move { self.load_record(persistence_id) })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: CounterState,
    ) -> StoreFuture<'a, StateRecord<CounterState>> {
        Box::pin(async move {
            let actual = self
                .load_record(persistence_id)?
                .map_or(Revision::INITIAL, |record| record.revision);
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            let record = StateRecord::new(state, expected_revision.next());
            self.write_record(persistence_id, &record)?;
            Ok(record)
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        Box::pin(async move {
            let actual = self
                .load_record(persistence_id)?
                .map_or(Revision::INITIAL, |record| record.revision);
            if actual != expected_revision {
                return Err(DurableError::revision_conflict(
                    persistence_id.clone(),
                    expected_revision,
                    actual,
                ));
            }

            match std::fs::remove_file(self.record_path(persistence_id)) {
                Ok(()) => Ok(Revision::INITIAL),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Revision::INITIAL),
                Err(error) => Err(file_store_error(error)),
            }
        })
    }
}

fn file_store_error(error: impl ToString) -> DurableError {
    DurableError::store("example-file", error.to_string())
}

#[derive(Debug, Clone)]
struct JsonPayloadCodec<T> {
    message_type_id: &'static str,
    _marker: PhantomData<fn() -> T>,
}

impl<T> JsonPayloadCodec<T> {
    fn new(message_type_id: &'static str) -> Self {
        Self {
            message_type_id,
            _marker: PhantomData,
        }
    }
}

impl<T> PayloadCodec<T> for JsonPayloadCodec<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn codec_id(&self) -> &str {
        "example-json"
    }

    fn message_type_id(&self) -> &str {
        self.message_type_id
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn encode(&self, message: &T) -> RemoteResult<Vec<u8>> {
        serde_json::to_vec(message).map_err(|error| RemoteError::Encode {
            codec_id: self.codec_id().to_string(),
            message: error.to_string(),
        })
    }

    fn decode(&self, payload: &[u8]) -> RemoteResult<T> {
        serde_json::from_slice(payload).map_err(|error| RemoteError::Decode {
            codec_id: self.codec_id().to_string(),
            message: error.to_string(),
        })
    }
}

#[derive(Debug, Clone)]
struct ExampleConfig {
    bind_host: IpAddr,
    advertise_host: String,
    tcp_port: u16,
    http_port: u16,
    node_logical_id: String,
    node_incarnation: String,
    discovery_dir: PathBuf,
    counter_store_dir: PathBuf,
}

impl ExampleConfig {
    fn from_env() -> ExampleResult<Self> {
        let tcp_port = env_u16("RAKKA_TCP_PORT", DEFAULT_RAKKA_TCP_PORT)?;
        let http_port = env::var("RAKKA_HTTP_PORT")
            .ok()
            .map(|value| parse_u16("RAKKA_HTTP_PORT", &value))
            .transpose()?
            .unwrap_or_else(|| tcp_port.saturating_add(10_000));
        let bind_host = env::var("RAKKA_BIND_HOST")
            .unwrap_or_else(|_| Ipv4Addr::LOCALHOST.to_string())
            .parse::<IpAddr>()?;
        let advertise_host =
            env::var("RAKKA_ADVERTISE_HOST").unwrap_or_else(|_| bind_host.to_string());
        let node_logical_id = env::var("RAKKA_NODE_LOGICAL_ID")
            .unwrap_or_else(|_| format!("counter-node-{tcp_port}"));
        let node_incarnation =
            env::var("RAKKA_NODE_INCARNATION").unwrap_or_else(|_| format!("uid-{tcp_port}"));
        let base_dir = env::temp_dir().join("rakka-clustered-counter-http");
        let discovery_dir = env::var_os("RAKKA_DISCOVERY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("discovery"));
        let counter_store_dir = env::var_os("RAKKA_COUNTER_STORE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("counter-state"));

        Ok(Self {
            bind_host,
            advertise_host,
            tcp_port,
            http_port,
            node_logical_id,
            node_incarnation,
            discovery_dir,
            counter_store_dir,
        })
    }

    fn local_node(&self) -> ClusterNode {
        ClusterNode::new(
            NodeId::new(self.node_logical_id.clone(), self.node_incarnation.clone()),
            NodeAddress::new(self.advertise_host.clone(), self.tcp_port),
        )
        .with_role("counter")
    }

    fn tcp_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.tcp_port)
    }

    fn http_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.http_port)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryRecord {
    node: ClusterNode,
    updated_at_millis: u64,
}

#[derive(Clone)]
struct CounterHttp {
    sharding: ClusterSharding,
    key: EntityTypeKey<CounterCommand>,
    ask_client: RemoteEntityAskClient<rakka::remote::TcpRemoteTransport>,
}

impl CounterHttp {
    async fn apply(
        &self,
        operation: CounterOperation,
        timeout: Duration,
    ) -> Result<CounterValue, HttpError> {
        let entity = self
            .sharding
            .entity_ref_for(&self.key, operation.name.clone())
            .map_err(|error| HttpError::service(error.to_string()))?;
        let (owner, _shard_id) = entity
            .region()
            .resolve(entity.entity_ref())
            .map_err(|error| HttpError::EntityNoRoute {
                message: error.to_string(),
            })?;
        let is_local = entity
            .region()
            .local_node_id()
            .is_some_and(|local_node_id| local_node_id == &owner);

        if is_local {
            entity
                .ask(
                    |reply_to| CounterCommand::Apply {
                        operation,
                        reply_to,
                    },
                    timeout,
                )
                .await
                .map_err(entity_ask_http_error)
        } else {
            entity
                .remote_ask(&self.ask_client, operation, timeout)
                .await
                .map_err(remote_ask_http_error)
        }
    }
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.first().map(String::as_str) {
        Some("client") => run_client(&args[1..]).await,
        Some("serve") | None => run_server().await,
        _ => Err(example_error(usage()).into()),
    }
}

async fn run_server() -> ExampleResult<()> {
    let config = ExampleConfig::from_env()?;
    let local_node = config.local_node();
    let system = ActorSystem::new(format!("clustered-counter-http-{}", config.node_logical_id));
    let mut registry = SerializationRegistry::new();
    registry.register::<CounterOperation, _>(JsonPayloadCodec::<CounterOperation>::new(
        "rakka.examples.clustered_counter_http.CounterOperation",
    ))?;
    registry.register::<CounterValue, _>(JsonPayloadCodec::<CounterValue>::new(
        "rakka.examples.clustered_counter_http.CounterValue",
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
    let key = EntityTypeKey::<CounterCommand>::new(ENTITY_TYPE).with_number_of_shards(32)?;
    let store = FileCounterStateStore::new(config.counter_store_dir.clone());
    let node_id = local_node.id().to_string();
    sharding.init_remote_with_ask(
        &mut runtime,
        Entity::of(key.clone(), {
            let system = system.clone();
            let store = store.clone();
            let node_id = node_id.clone();
            move |context: EntityContext<CounterCommand>| {
                CounterEntity::new(system.clone(), context, store.clone(), node_id.clone())
            }
        }),
        |operation: CounterOperation, reply_to| CounterCommand::Apply {
            operation,
            reply_to,
        },
    )?;

    publish_and_apply_discovery(&config, &local_node, &mut runtime)?;
    let runtime = Arc::new(AsyncMutex::new(runtime));
    let discovery_task = tokio::spawn(discovery_loop(
        runtime.clone(),
        config.clone(),
        local_node.clone(),
    ));
    let app = CounterHttp {
        sharding,
        key,
        ask_client,
    };
    let http_addr = config.http_bind_addr();
    let router = counter_router(app);

    println!(
        "Rakka clustered counter HTTP node {} listening: remoting {} / HTTP {}",
        local_node.id(),
        config.tcp_bind_addr(),
        http_addr
    );
    println!(
        "Discovery dir: {}; counter state dir: {}",
        config.discovery_dir.display(),
        config.counter_store_dir.display()
    );

    serve_with_graceful_shutdown(router, HttpServerConfig::new(http_addr), async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;

    discovery_task.abort();
    let _ = remove_discovery_record(&config.discovery_dir, local_node.id().logical_id());
    if let Ok(mut runtime) = runtime.try_lock() {
        let _ = runtime.leave_local(current_timestamp_millis());
    }
    system.terminate().await?;
    Ok(())
}

fn counter_router(app: CounterHttp) -> rakka::http::Router {
    rakka::http::Router::new()
        .route("/counters/:name", get(get_counter))
        .route("/counters/:name/initiate", post(initiate_counter))
        .route("/counters/:name/increase", post(increase_counter))
        .route("/counters/:name/decrease", post(decrease_counter))
        .with_state(app)
}

async fn get_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: 0,
                action: CounterAction::Get,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

async fn initiate_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<InitiateCounterRequest>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: request.initial_value,
                action: CounterAction::Initiate,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

async fn increase_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<ChangeCounterRequest>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    validate_non_negative_amount(request.amount)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: request.amount,
                action: CounterAction::Increase,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

async fn decrease_counter(
    State(app): State<CounterHttp>,
    AxumPath(name): AxumPath<String>,
    Json(request): Json<ChangeCounterRequest>,
) -> Result<Json<CounterValue>, HttpError> {
    validate_counter_name(&name)?;
    validate_non_negative_amount(request.amount)?;
    let value = app
        .apply(
            CounterOperation {
                name,
                amount: request.amount,
                action: CounterAction::Decrease,
            },
            HttpRouteConfig::default().request_timeout_value(),
        )
        .await?;
    Ok(Json(value))
}

async fn discovery_loop(
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    config: ExampleConfig,
    local_node: ClusterNode,
) {
    let mut interval = tokio::time::interval(DEFAULT_DISCOVERY_POLL);
    loop {
        interval.tick().await;
        if let Err(error) = publish_discovery_record(&config.discovery_dir, &local_node) {
            eprintln!("discovery publish failed: {error}");
            continue;
        }

        let nodes = match read_discovery_nodes(&config.discovery_dir, &local_node) {
            Ok(nodes) => nodes,
            Err(error) => {
                eprintln!("discovery read failed: {error}");
                continue;
            }
        };
        let now = current_timestamp_millis();
        let snapshot = DiscoverySnapshot::new("example-file-discovery", now, nodes);
        let mut runtime = runtime.lock().await;
        if let Err(error) = runtime.apply_discovery(snapshot) {
            eprintln!("discovery apply failed: {error}");
        }
        if let Err(error) = runtime.tick(now) {
            eprintln!("membership tick failed: {error}");
        }
    }
}

fn publish_and_apply_discovery(
    config: &ExampleConfig,
    local_node: &ClusterNode,
    runtime: &mut ClusterNodeRuntime,
) -> ExampleResult<()> {
    publish_discovery_record(&config.discovery_dir, local_node)?;
    let nodes = read_discovery_nodes(&config.discovery_dir, local_node)?;
    runtime.apply_discovery(DiscoverySnapshot::new(
        "example-file-discovery",
        current_timestamp_millis(),
        nodes,
    ))?;
    Ok(())
}

fn publish_discovery_record(dir: &Path, node: &ClusterNode) -> ExampleResult<()> {
    std::fs::create_dir_all(dir)?;
    let path = discovery_record_path(dir, node.id().logical_id());
    let temp = path.with_extension(format!("json.tmp.{}", current_timestamp_millis()));
    let record = DiscoveryRecord {
        node: node.clone(),
        updated_at_millis: current_timestamp_millis(),
    };
    let bytes = serde_json::to_vec_pretty(&record)?;
    std::fs::write(&temp, bytes)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

fn read_discovery_nodes(dir: &Path, local_node: &ClusterNode) -> ExampleResult<Vec<ClusterNode>> {
    let now = current_timestamp_millis();
    let ttl = millis(DEFAULT_DISCOVERY_TTL);
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(vec![local_node.clone()]);
        }
        Err(error) => return Err(error.into()),
    };
    let mut nodes = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        let record: DiscoveryRecord = serde_json::from_slice(&bytes)?;
        if now.saturating_sub(record.updated_at_millis) <= ttl
            || record.node.id() == local_node.id()
        {
            nodes.push(record.node);
        }
    }
    if !nodes.iter().any(|node| node.id() == local_node.id()) {
        nodes.push(local_node.clone());
    }
    Ok(nodes)
}

fn remove_discovery_record(dir: &Path, logical_id: &str) -> ExampleResult<()> {
    match std::fs::remove_file(discovery_record_path(dir, logical_id)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn discovery_record_path(dir: &Path, logical_id: &str) -> PathBuf {
    dir.join(format!("{}.json", hex_encode(logical_id)))
}

async fn run_client(args: &[String]) -> ExampleResult<()> {
    let endpoint = env::var("RAKKA_HTTP_ENDPOINT").unwrap_or_else(|_| {
        let port = env_u16(
            "RAKKA_HTTP_PORT",
            DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000),
        )
        .unwrap_or(DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000));
        format!("http://127.0.0.1:{port}")
    });
    let value = match args {
        [operation, name] if operation == "initiate" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/initiate"),
                &InitiateCounterRequest { initial_value: 0 },
            )
            .await?
        }
        [operation, name, value] if operation == "initiate" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/initiate"),
                &InitiateCounterRequest {
                    initial_value: value.parse()?,
                },
            )
            .await?
        }
        [operation, name] if operation == "get" => {
            get_counter_json(&endpoint, &format!("/counters/{name}")).await?
        }
        [operation, name] if operation == "increase" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/increase"),
                &ChangeCounterRequest {
                    amount: default_change_amount(),
                },
            )
            .await?
        }
        [operation, name, amount] if operation == "increase" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/increase"),
                &ChangeCounterRequest {
                    amount: amount.parse()?,
                },
            )
            .await?
        }
        [operation, name] if operation == "decrease" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/decrease"),
                &ChangeCounterRequest {
                    amount: default_change_amount(),
                },
            )
            .await?
        }
        [operation, name, amount] if operation == "decrease" => {
            post_counter_json(
                &endpoint,
                &format!("/counters/{name}/decrease"),
                &ChangeCounterRequest {
                    amount: amount.parse()?,
                },
            )
            .await?
        }
        _ => return Err(example_error(usage()).into()),
    };

    println!(
        "{}={} revision={} initialized={} created={} owner={}",
        value.name, value.value, value.revision, value.initialized, value.created, value.owner_node
    );
    Ok(())
}

async fn get_counter_json(endpoint: &str, path: &str) -> ExampleResult<CounterValue> {
    let (host, port) = parse_http_endpoint(endpoint)?;
    let mut stream = TcpStream::connect((host.as_str(), port)).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let (status, body) = parse_http_response(&response)?;
    if !(200..300).contains(&status) {
        return Err(example_error(format!(
            "HTTP request failed with {status}: {}",
            String::from_utf8_lossy(&body)
        ))
        .into());
    }
    Ok(serde_json::from_slice(&body)?)
}

async fn post_counter_json<T>(
    endpoint: &str,
    path: &str,
    payload: &T,
) -> ExampleResult<CounterValue>
where
    T: Serialize,
{
    let (host, port) = parse_http_endpoint(endpoint)?;
    let body = serde_json::to_vec(payload)?;
    let mut stream = TcpStream::connect((host.as_str(), port)).await?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nAccept: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let (status, body) = parse_http_response(&response)?;
    if !(200..300).contains(&status) {
        return Err(example_error(format!(
            "HTTP request failed with {status}: {}",
            String::from_utf8_lossy(&body)
        ))
        .into());
    }
    Ok(serde_json::from_slice(&body)?)
}

fn parse_http_endpoint(endpoint: &str) -> ExampleResult<(String, u16)> {
    let authority = endpoint
        .strip_prefix("http://")
        .ok_or_else(|| example_error("RAKKA_HTTP_ENDPOINT must start with http://"))?;
    let authority = authority.trim_end_matches('/');
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| example_error("RAKKA_HTTP_ENDPOINT must include host:port"))?;
    Ok((host.to_string(), port.parse()?))
}

fn parse_http_response(response: &[u8]) -> ExampleResult<(u16, Vec<u8>)> {
    let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
        return Err(example_error("HTTP response did not include headers").into());
    };
    let header_bytes = &response[..header_end];
    let body = response[header_end + 4..].to_vec();
    let headers = std::str::from_utf8(header_bytes)?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| example_error("HTTP response status line was invalid"))?
        .parse::<u16>()?;

    if headers
        .lines()
        .any(|line| line.eq_ignore_ascii_case("transfer-encoding: chunked"))
    {
        return Ok((status, decode_chunked_body(&body)?));
    }

    Ok((status, body))
}

fn decode_chunked_body(body: &[u8]) -> ExampleResult<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut offset = 0usize;
    loop {
        let Some(line_end) = body[offset..]
            .windows(2)
            .position(|window| window == b"\r\n")
            .map(|position| offset + position)
        else {
            return Err(example_error("chunked response was truncated").into());
        };
        let size_text = std::str::from_utf8(&body[offset..line_end])?;
        let size = usize::from_str_radix(size_text.trim(), 16)?;
        offset = line_end + 2;
        if size == 0 {
            break;
        }
        let chunk_end = offset
            .checked_add(size)
            .ok_or_else(|| example_error("chunked response size overflow"))?;
        if body.len() < chunk_end + 2 {
            return Err(example_error("chunked response body was truncated").into());
        }
        decoded.extend_from_slice(&body[offset..chunk_end]);
        offset = chunk_end + 2;
    }
    Ok(decoded)
}

fn validate_counter_name(name: &str) -> Result<(), HttpError> {
    if name.is_empty() {
        return Err(HttpError::JsonDecode {
            message: "counter name must not be empty".to_string(),
        });
    }
    if name.contains('|') || name.contains('/') {
        return Err(HttpError::JsonDecode {
            message: "counter name must not contain '|' or '/'".to_string(),
        });
    }
    Ok(())
}

fn validate_non_negative_amount(amount: i64) -> Result<(), HttpError> {
    if amount < 0 {
        return Err(HttpError::JsonDecode {
            message: "amount must be non-negative".to_string(),
        });
    }
    Ok(())
}

fn entity_ask_http_error(error: EntityAskError) -> HttpError {
    match error {
        EntityAskError::NoRoute(error) => HttpError::EntityNoRoute {
            message: error.to_string(),
        },
        EntityAskError::MailboxFull => HttpError::EntityMailboxFull,
        EntityAskError::MailboxClosed => HttpError::EntityMailboxClosed,
        EntityAskError::NotLocal { owner } => HttpError::EntityNotLocal {
            owner: owner.to_string(),
        },
        EntityAskError::SpawnFailed(message) => HttpError::EntitySpawnFailed { message },
        EntityAskError::RemoteEncode(message) => HttpError::EntityRemoteEncode { message },
        EntityAskError::RemoteSend(message) => HttpError::EntityRemoteSend { message },
        EntityAskError::ShardHandoff { shard_id, state } => HttpError::EntityShardHandoff {
            shard_id: shard_id.to_string(),
            state: state.to_string(),
        },
        EntityAskError::ShardBufferFull { shard_id, capacity } => {
            HttpError::EntityShardBufferFull {
                shard_id: shard_id.to_string(),
                capacity,
            }
        }
        EntityAskError::Rejected(message) => HttpError::EntityRejected { message },
        EntityAskError::Timeout => HttpError::EntityTimeout,
        EntityAskError::ReplyDropped => HttpError::EntityReplyDropped,
    }
}

fn remote_ask_http_error(error: RemoteEntityAskError) -> HttpError {
    match error {
        RemoteEntityAskError::NoRoute { error } => HttpError::EntityNoRoute {
            message: error.to_string(),
        },
        RemoteEntityAskError::Encode { error } => HttpError::EntityRemoteEncode {
            message: error.to_string(),
        },
        RemoteEntityAskError::Register { error } => HttpError::service(error.to_string()),
        RemoteEntityAskError::Send { message } => HttpError::EntityRemoteSend { message },
        RemoteEntityAskError::Reply { error } => match error {
            RemoteRequestError::Timeout => HttpError::EntityTimeout,
            RemoteRequestError::ReplyDropped => HttpError::EntityReplyDropped,
            RemoteRequestError::Decode { error } => HttpError::EntityRemoteEncode {
                message: error.to_string(),
            },
            other => HttpError::service(other.to_string()),
        },
    }
}

fn env_u16(name: &str, default: u16) -> ExampleResult<u16> {
    env::var(name)
        .ok()
        .map(|value| parse_u16(name, &value))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_u16(name: &str, value: &str) -> ExampleResult<u16> {
    value
        .parse::<u16>()
        .map_err(|error| example_error(format!("{name} must be a TCP port: {error}")).into())
}

fn default_change_amount() -> i64 {
    1
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn hex_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn usage() -> String {
    [
        "usage:",
        "  cargo run -p rakka-example-clustered-counter-http",
        "  cargo run -p rakka-example-clustered-counter-http -- serve",
        "  cargo run -p rakka-example-clustered-counter-http -- client initiate <name> [initial]",
        "  cargo run -p rakka-example-clustered-counter-http -- client get <name>",
        "  cargo run -p rakka-example-clustered-counter-http -- client increase <name> [amount]",
        "  cargo run -p rakka-example-clustered-counter-http -- client decrease <name> [amount]",
    ]
    .join("\n")
}

fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
