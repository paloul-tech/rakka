#![forbid(unsafe_code)]

//! Clustered, sharded, persistent counter exposed through generated gRPC.

use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rakka::cluster::{ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId};
use rakka::grpc::{effective_request_timeout, validation_status, GrpcError, GrpcUnaryConfig};
use rakka::persistence::{DurableError, DurableResult, StateRecord, StoreFuture};
use rakka::prelude::*;
use rakka::remote::{RemoteRequestError, SerializationRegistry, TcpRemoteTransportConfig};
use rakka::sharding::{ClusterNodeRuntime, RemoteEntityAskClient, RemoteEntityAskError};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[allow(missing_docs, clippy::all)]
pub mod counter_api {
    tonic::include_proto!("rakka.examples.clustered_counter.v1");
}

use counter_api::counter_service_client::CounterServiceClient;
use counter_api::counter_service_server::{CounterService, CounterServiceServer};
use counter_api::{
    ChangeCounterRequest, CounterAction, CounterOperation, CounterValue, GetCounterRequest,
    InitiateCounterRequest,
};

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
                let action =
                    CounterAction::try_from(operation.action).unwrap_or(CounterAction::Unspecified);
                let created = !state.initialized;
                let current_revision = ctx.revision();
                let owner_node = self.owner_node.clone();
                let name = operation.name;
                let amount = operation.amount;

                match action {
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
                    CounterAction::Get | CounterAction::Unspecified => {
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
    _store: std::marker::PhantomData<Store>,
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
            _store: std::marker::PhantomData,
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

    fn persistence_ids<'a>(&'a self) -> StoreFuture<'a, Vec<PersistenceId>> {
        Box::pin(async move {
            let entries = match std::fs::read_dir(self.root.as_ref()) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(file_store_error(error)),
            };
            let mut ids = Vec::new();
            for entry in entries {
                let entry = entry.map_err(file_store_error)?;
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) != Some("json") {
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                    continue;
                };
                if let Some(value) = hex_decode(stem) {
                    ids.push(PersistenceId::new(value));
                }
            }
            ids.sort();
            Ok(ids)
        })
    }
}

fn file_store_error(error: impl ToString) -> DurableError {
    DurableError::store("example-file", error.to_string())
}

#[derive(Debug, Clone)]
struct ExampleConfig {
    bind_host: IpAddr,
    advertise_host: String,
    tcp_port: u16,
    grpc_port: u16,
    node_logical_id: String,
    node_incarnation: String,
    discovery_dir: PathBuf,
    counter_store_dir: PathBuf,
}

impl ExampleConfig {
    fn from_env() -> ExampleResult<Self> {
        let tcp_port = env_u16("RAKKA_TCP_PORT", DEFAULT_RAKKA_TCP_PORT)?;
        let grpc_port = env::var("RAKKA_GRPC_PORT")
            .ok()
            .map(|value| parse_u16("RAKKA_GRPC_PORT", &value))
            .transpose()?
            .unwrap_or_else(|| tcp_port.saturating_add(10_000));
        let bind_host = env::var("RAKKA_BIND_HOST")
            .unwrap_or_else(|_| Ipv4Addr::LOCALHOST.to_string())
            .parse::<IpAddr>()?;
        let advertise_host =
            env::var("RAKKA_ADVERTISE_HOST").unwrap_or_else(|_| bind_host.to_string());
        let node_logical_id = env::var("RAKKA_NODE_LOGICAL_ID")
            .unwrap_or_else(|_| format!("counter-node-{tcp_port}"));
        let node_incarnation = env::var("RAKKA_NODE_INCARNATION")
            .unwrap_or_else(|_| default_node_incarnation(tcp_port));
        let base_dir = env::temp_dir().join("rakka-clustered-counter-grpc");
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
            grpc_port,
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

    fn grpc_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.grpc_port)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryRecord {
    node: ClusterNode,
    updated_at_millis: u64,
}

#[derive(Clone)]
struct CounterGrpc {
    sharding: ClusterSharding,
    key: EntityTypeKey<CounterCommand>,
    ask_client: RemoteEntityAskClient<rakka::remote::TcpRemoteTransport>,
}

#[tonic::async_trait]
impl CounterService for CounterGrpc {
    async fn initiate(
        &self,
        request: Request<InitiateCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: request.initial_value,
                action: CounterAction::Initiate as i32,
            },
            timeout,
        )
        .await
    }

    async fn get(
        &self,
        request: Request<GetCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: 0,
                action: CounterAction::Get as i32,
            },
            timeout,
        )
        .await
    }

    async fn increase(
        &self,
        request: Request<ChangeCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        validate_non_negative_amount(request.amount).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: request.amount,
                action: CounterAction::Increase as i32,
            },
            timeout,
        )
        .await
    }

    async fn decrease(
        &self,
        request: Request<ChangeCounterRequest>,
    ) -> Result<Response<CounterValue>, Status> {
        let timeout = effective_request_timeout(&request, GrpcUnaryConfig::default());
        let request = request.into_inner();
        validate_counter_name(&request.name).map_err(validation_status)?;
        validate_non_negative_amount(request.amount).map_err(validation_status)?;
        self.apply(
            CounterOperation {
                name: request.name,
                amount: request.amount,
                action: CounterAction::Decrease as i32,
            },
            timeout,
        )
        .await
    }
}

impl CounterGrpc {
    async fn apply(
        &self,
        operation: CounterOperation,
        timeout: Duration,
    ) -> Result<Response<CounterValue>, Status> {
        let entity = self
            .sharding
            .entity_ref_for(&self.key, operation.name.clone())
            .map_err(|error| GrpcError::service(error.to_string()).into_status())?;
        let (owner, _shard_id) = entity
            .region()
            .resolve(entity.entity_ref())
            .map_err(|error| {
                GrpcError::EntityNoRoute {
                    message: error.to_string(),
                }
                .into_status()
            })?;
        let is_local = entity
            .region()
            .local_node_id()
            .is_some_and(|local_node_id| local_node_id == &owner);
        let value = if is_local {
            entity
                .ask(
                    |reply_to| CounterCommand::Apply {
                        operation,
                        reply_to,
                    },
                    timeout,
                )
                .await
                .map_err(|error| GrpcError::from_entity_ask(error).into_status())?
        } else {
            entity
                .remote_ask(&self.ask_client, operation, timeout)
                .await
                .map_err(remote_ask_status)?
        };
        Ok(Response::new(value))
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
    let system = ActorSystem::new(format!("clustered-counter-{}", config.node_logical_id));
    let mut registry = SerializationRegistry::new();
    registry.register_protobuf::<CounterOperation>(
        "rakka.examples.clustered_counter.v1.CounterOperation",
        1,
    )?;
    registry
        .register_protobuf::<CounterValue>("rakka.examples.clustered_counter.v1.CounterValue", 1)?;

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
    let grpc = CounterGrpc {
        sharding,
        key,
        ask_client,
    };
    let grpc_addr = config.grpc_bind_addr();

    println!(
        "Rakka clustered counter node {} listening: remoting {} / gRPC {}",
        local_node.id(),
        config.tcp_bind_addr(),
        grpc_addr
    );
    println!(
        "Discovery dir: {}; counter state dir: {}",
        config.discovery_dir.display(),
        config.counter_store_dir.display()
    );

    Server::builder()
        .add_service(CounterServiceServer::new(grpc))
        .serve_with_shutdown(grpc_addr, async {
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
    let endpoint = env::var("RAKKA_GRPC_ENDPOINT").unwrap_or_else(|_| {
        let port = env_u16(
            "RAKKA_GRPC_PORT",
            DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000),
        )
        .unwrap_or(DEFAULT_RAKKA_TCP_PORT.saturating_add(10_000));
        format!("http://127.0.0.1:{port}")
    });
    let mut client = CounterServiceClient::connect(endpoint).await?;
    let value = match args {
        [operation, name] if operation == "initiate" => client
            .initiate(Request::new(InitiateCounterRequest {
                name: name.clone(),
                initial_value: 0,
            }))
            .await?
            .into_inner(),
        [operation, name, value] if operation == "initiate" => client
            .initiate(Request::new(InitiateCounterRequest {
                name: name.clone(),
                initial_value: value.parse()?,
            }))
            .await?
            .into_inner(),
        [operation, name] if operation == "get" => client
            .get(Request::new(GetCounterRequest { name: name.clone() }))
            .await?
            .into_inner(),
        [operation, name] if operation == "increase" => client
            .increase(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: 1,
            }))
            .await?
            .into_inner(),
        [operation, name, amount] if operation == "increase" => client
            .increase(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: amount.parse()?,
            }))
            .await?
            .into_inner(),
        [operation, name] if operation == "decrease" => client
            .decrease(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: 1,
            }))
            .await?
            .into_inner(),
        [operation, name, amount] if operation == "decrease" => client
            .decrease(Request::new(ChangeCounterRequest {
                name: name.clone(),
                amount: amount.parse()?,
            }))
            .await?
            .into_inner(),
        _ => return Err(example_error(usage()).into()),
    };

    println!(
        "{}={} revision={} initialized={} created={} owner={}",
        value.name, value.value, value.revision, value.initialized, value.created, value.owner_node
    );
    Ok(())
}

fn validate_counter_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("counter name must not be empty");
    }
    if name.contains('|') {
        return Err("counter name must not contain '|'");
    }
    Ok(())
}

fn validate_non_negative_amount(amount: i64) -> Result<(), &'static str> {
    if amount < 0 {
        return Err("amount must be non-negative");
    }
    Ok(())
}

fn remote_ask_status(error: RemoteEntityAskError) -> Status {
    match error {
        RemoteEntityAskError::NoRoute { error } => GrpcError::EntityNoRoute {
            message: error.to_string(),
        },
        RemoteEntityAskError::Encode { error } => GrpcError::EntityRemoteEncode {
            message: error.to_string(),
        },
        RemoteEntityAskError::Register { error } => GrpcError::Service {
            message: error.to_string(),
        },
        RemoteEntityAskError::Send { message } => GrpcError::EntityRemoteSend { message },
        RemoteEntityAskError::Reply { error } => match error {
            RemoteRequestError::Timeout => GrpcError::EntityTimeout,
            RemoteRequestError::ReplyDropped => GrpcError::EntityReplyDropped,
            RemoteRequestError::Decode { error } => GrpcError::EntityRemoteEncode {
                message: error.to_string(),
            },
            other => GrpcError::Service {
                message: other.to_string(),
            },
        },
    }
    .into_status()
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

fn default_node_incarnation(tcp_port: u16) -> String {
    format!(
        "uid-{tcp_port}-{}-{}",
        current_timestamp_millis(),
        std::process::id()
    )
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

fn hex_decode(value: &str) -> Option<String> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let text = std::str::from_utf8(chunk).ok()?;
        let byte = u8::from_str_radix(text, 16).ok()?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).ok()
}

fn usage() -> String {
    [
        "usage:",
        "  cargo run -p rakka-example-clustered-counter-grpc",
        "  cargo run -p rakka-example-clustered-counter-grpc -- serve",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client initiate <name> [initial]",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client get <name>",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client increase <name> [amount]",
        "  cargo run -p rakka-example-clustered-counter-grpc -- client decrease <name> [amount]",
    ]
    .join("\n")
}

fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
