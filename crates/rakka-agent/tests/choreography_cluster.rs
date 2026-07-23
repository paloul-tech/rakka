//! Inter-entity choreography across shard owners.
//!
//! Specification: section 9.8; scenario 60 of section 18. A cross-entity command
//! traverses durable outbox/inbox acceptance whether the two entities share a
//! node or not, and it stays correct after they move apart.
//!
//! The entities here are sharded [`ChoreographyProbe`] participants driven
//! through [`rakka_agent::ShardedExchangeRoute`], which resolves the target's
//! shard owner and then either asks the local entity or asks the owning node over
//! `rakka-remote`. Both paths reach the same durable
//! [`rakka_agent::AgentExchangeHost::accept`], so the durable record an exchange
//! leaves behind is the assertion that colocation changed nothing.
//!
//! The exchanges run on the default per-node deterministic-modulo shard
//! coordinator with symmetric hosting; the fenced-lease coordinator collapses an
//! entity type onto its single lease holder and cannot host these entities.
//!
//! Skipped automatically when loopback binding is unavailable in the sandbox,
//! mirroring the other networked tests in this workspace.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent::testkit::{ChoreographyProbe, ChoreographyProbeState};
use rakka_agent::{
    drive_pending_exchanges, register_agent_exchange_codecs, AgentEntityAddress, AgentEntityClass,
    AgentExchangeEnvelope, AgentExchangeHost, AgentExchangeKind, AgentExchangeReply,
    AgentExchangeRouter, AgentId, AgentOperationId, AgentOperationKind, AgentRunId, AgentRunScope,
    AgentTaskId, AgentTaskScope, ShardedExchangeRoute, TenantId,
};
use rakka_agent_workflow::AgentTimestampMillis;
use rakka_cluster::{ClusterNode, DiscoverySnapshot, MembershipConfig, NodeAddress, NodeId};
use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem, ReplyTo,
};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_remote::{
    SerializationRegistry, TcpRemoteTransport, TcpRemoteTransportConfig, TcpRemoteTransportError,
};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeBuilder, ClusterNodeRuntimeError, ClusterSharding,
    Entity, EntityContext, EntityId, EntityTypeKey, EntityTypeRegistration, RemoteEntityAskClient,
};

type Store = InMemoryDurableStateStore<ChoreographyProbeState>;
type ProbeHost = AgentExchangeHost<ChoreographyProbe, Store>;

const TENANT: &str = "acme";
const TASK_ENTITY_TYPE: &str = "RakkaAgentTaskProbe";
const RUN_ENTITY_TYPE: &str = "RakkaAgentRunProbe";
const ASK_TIMEOUT: Duration = Duration::from_secs(2);

/// Everything a probe entity can be asked, including the exchange itself.
///
/// The exchange arm is what [`ShardedExchangeRoute`] builds, locally from a
/// mailbox ask and remotely from an [`AgentExchangeEnvelope`] that crossed the
/// wire. The rest is test control, and only ever asked of a local owner.
enum ProbeEntityMessage {
    /// The envelope is boxed, as standing constraint 5 requires of every large
    /// entity-protocol payload: an entity's mailbox moves this value, and a real
    /// task or run entity carries other commands in the same enum.
    Exchange {
        envelope: Box<AgentExchangeEnvelope>,
        reply_to: ReplyTo<AgentExchangeReply>,
    },
    Initiate {
        envelope: Box<AgentExchangeEnvelope>,
        reply_to: ReplyTo<ProbeControlReply>,
    },
    Drive {
        reply_to: ReplyTo<ProbeControlReply>,
    },
    Describe {
        reply_to: ReplyTo<ProbeControlReply>,
    },
}

#[derive(Debug)]
enum ProbeControlReply {
    Driven {
        settled: usize,
        failed: usize,
        outstanding: usize,
    },
    Snapshot(Box<ChoreographyProbeState>),
    Rejected {
        code: String,
    },
}

impl ProbeControlReply {
    fn driven(self) -> (usize, usize, usize) {
        match self {
            Self::Driven {
                settled,
                failed,
                outstanding,
            } => (settled, failed, outstanding),
            Self::Rejected { code } => panic!("the entity rejected the command: {code}"),
            other => panic!("expected a drive report, got {other:?}"),
        }
    }

    fn snapshot(self) -> ChoreographyProbeState {
        match self {
            Self::Snapshot(state) => *state,
            Self::Rejected { code } => panic!("the entity rejected the command: {code}"),
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }
}

/// A sharded choreography participant.
///
/// It owns exactly one durable host, so it is the single writer of its own
/// record, and it drives the exchanges it owes itself. That is the shape the
/// task and run entities of slices 1.4 and 1.5 take.
struct ProbeEntity {
    host: Option<ProbeHost>,
    router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
    address: Result<AgentEntityAddress, String>,
    store: Store,
}

impl ProbeEntity {
    fn new(
        class: AgentEntityClass,
        entity_id: &EntityId,
        store: Store,
        router: AgentExchangeRouter,
        clock: Arc<AtomicU64>,
    ) -> Self {
        let address =
            AgentEntityAddress::from_entity_id(class, entity_id).map_err(|error| error.to_string());
        Self {
            host: None,
            router,
            clock,
            address,
            store,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// Recovery is lazy and idempotent, which is exactly what an entity
    /// re-materialized on a new shard owner must do before it transitions.
    async fn host(&mut self) -> Result<&mut ProbeHost, String> {
        let address = self.address.clone()?;
        if self.host.is_none() {
            let mut host = AgentExchangeHost::new(address, ChoreographyProbe, self.store.clone());
            let now = self.now();
            host.recover(now).await.map_err(|error| error.to_string())?;
            self.host = Some(host);
        }
        Ok(self.host.as_mut().expect("the host was just recovered"))
    }
}

impl Actor for ProbeEntity {
    type Msg = ProbeEntityMessage;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                ProbeEntityMessage::Exchange { envelope, reply_to } => {
                    let now = self.now();
                    let host = match self.host().await {
                        Ok(host) => host,
                        Err(_) => return Ok(ActorAction::Continue),
                    };
                    if let Ok(reply) = host.accept(&envelope, now).await {
                        let _dropped = reply_to.reply(reply);
                    }
                }
                ProbeEntityMessage::Initiate { envelope, reply_to } => {
                    let now = self.now();
                    let reply = match self.initiate(*envelope, now).await {
                        Ok(reply) => reply,
                        Err(code) => ProbeControlReply::Rejected { code },
                    };
                    let _dropped = reply_to.reply(reply);
                }
                ProbeEntityMessage::Drive { reply_to } => {
                    let reply = match self.drive().await {
                        Ok(reply) => reply,
                        Err(code) => ProbeControlReply::Rejected { code },
                    };
                    let _dropped = reply_to.reply(reply);
                }
                ProbeEntityMessage::Describe { reply_to } => {
                    let host = match self.host().await {
                        Ok(host) => host,
                        Err(code) => {
                            let _dropped = reply_to.reply(ProbeControlReply::Rejected { code });
                            return Ok(ActorAction::Continue);
                        }
                    };
                    let state = host.state().expect("recovered").clone();
                    let _dropped = reply_to.reply(ProbeControlReply::Snapshot(Box::new(state)));
                }
            }
            Ok(ActorAction::Continue)
        })
    }
}

impl ProbeEntity {
    async fn initiate(
        &mut self,
        envelope: AgentExchangeEnvelope,
        now: AgentTimestampMillis,
    ) -> Result<ProbeControlReply, String> {
        {
            let host = self.host().await?;
            host.initiate(now, move |_state| Ok(vec![envelope]))
                .await
                .map_err(|error| error.code().to_string())?;
        }
        self.drive().await
    }

    /// The entity drives its own outstanding exchanges. On recovery it would do
    /// exactly the same thing, from exactly the same durable list.
    async fn drive(&mut self) -> Result<ProbeControlReply, String> {
        let now = self.now();
        let router = self.router.clone();
        let host = self.host().await?;
        let report = drive_pending_exchanges(host, &router, now)
            .await
            .map_err(|error| error.code().to_string())?;
        Ok(ProbeControlReply::Driven {
            settled: report.settled,
            failed: report.failed,
            outstanding: host.outstanding().map_err(|e| e.code().to_string())?.len(),
        })
    }
}

/// One booted node hosting both probe entity types.
struct Node {
    runtime: ClusterNodeRuntime,
    system: ActorSystem,
    task: EntityTypeRegistration<ProbeEntityMessage>,
    run: EntityTypeRegistration<ProbeEntityMessage>,
}

impl Node {
    async fn boot(
        name: &str,
        logical_id: &str,
        incarnation: &str,
        store: Store,
        clock: Arc<AtomicU64>,
    ) -> Option<Self> {
        let mut runtime = build_runtime(logical_id, incarnation).await?;
        let system = ActorSystem::new(name);
        let sharding = ClusterSharding::for_node_runtime(&system, &runtime)
            .expect("the sharding facade should initialize");

        let task_key = task_key();
        let run_key = run_key();
        let ask_client = runtime.ask_client();

        // Both entity classes route through the same substrate. Nothing here
        // knows or cares whether a target is local.
        let router = AgentExchangeRouter::new()
            .with_route(
                AgentEntityClass::Task,
                Arc::new(exchange_route(
                    &sharding,
                    task_key.clone(),
                    ask_client.clone(),
                )),
            )
            .with_route(
                AgentEntityClass::Run,
                Arc::new(exchange_route(&sharding, run_key.clone(), ask_client)),
            );

        let task = init_probe_entity(
            &sharding,
            &mut runtime,
            AgentEntityClass::Task,
            task_key,
            store.clone(),
            router.clone(),
            clock.clone(),
        );
        let run = init_probe_entity(
            &sharding,
            &mut runtime,
            AgentEntityClass::Run,
            run_key,
            store,
            router,
            clock,
        );

        Some(Self {
            runtime,
            system,
            task,
            run,
        })
    }

    fn owns(
        &self,
        registration: &EntityTypeRegistration<ProbeEntityMessage>,
        address: &AgentEntityAddress,
    ) -> bool {
        let entity = registration.entity_ref_for(address.entity_id().as_str());
        let Ok((owner, _shard)) = entity.region().resolve(entity.entity_ref()) else {
            return false;
        };
        entity
            .region()
            .local_node_id()
            .is_some_and(|local| local == &owner)
    }

    async fn ask(
        &self,
        registration: &EntityTypeRegistration<ProbeEntityMessage>,
        address: &AgentEntityAddress,
        build: impl FnOnce(ReplyTo<ProbeControlReply>) -> ProbeEntityMessage,
    ) -> ProbeControlReply {
        registration
            .entity_ref_for(address.entity_id().as_str())
            .ask(build, ASK_TIMEOUT)
            .await
            .expect("the probe entity should reply")
    }
}

fn exchange_route(
    sharding: &ClusterSharding,
    key: EntityTypeKey<ProbeEntityMessage>,
    ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
) -> ShardedExchangeRoute<ProbeEntityMessage, TcpRemoteTransport> {
    ShardedExchangeRoute::new(
        sharding.clone(),
        key,
        ask_client,
        ASK_TIMEOUT,
        |envelope, reply_to| ProbeEntityMessage::Exchange {
            envelope: Box::new(envelope),
            reply_to,
        },
    )
}

fn init_probe_entity(
    sharding: &ClusterSharding,
    runtime: &mut ClusterNodeRuntime,
    class: AgentEntityClass,
    key: EntityTypeKey<ProbeEntityMessage>,
    store: Store,
    router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
) -> EntityTypeRegistration<ProbeEntityMessage> {
    let entity = Entity::of(key, move |context: EntityContext<ProbeEntityMessage>| {
        ProbeEntity::new(
            class,
            context.entity_id(),
            store.clone(),
            router.clone(),
            clock.clone(),
        )
    });

    // The envelope is the remote request type of every participant entity, and
    // the reply is the remote reply type, so an exchange crosses `rakka-remote`
    // with no per-entity protocol of its own.
    sharding
        .init_remote_with_ask(
            runtime,
            entity,
            |envelope: AgentExchangeEnvelope, reply_to: ReplyTo<AgentExchangeReply>| {
                ProbeEntityMessage::Exchange {
                    envelope: Box::new(envelope),
                    reply_to,
                }
            },
        )
        .expect("the probe entity type should register")
}

fn task_key() -> EntityTypeKey<ProbeEntityMessage> {
    EntityTypeKey::new(TASK_ENTITY_TYPE)
        .with_number_of_shards(16)
        .expect("entity type key should be valid")
}

fn run_key() -> EntityTypeKey<ProbeEntityMessage> {
    EntityTypeKey::new(RUN_ENTITY_TYPE)
        .with_number_of_shards(16)
        .expect("entity type key should be valid")
}

fn task_address(id: &str) -> AgentEntityAddress {
    AgentEntityAddress::Task(
        AgentTaskScope::new(
            TenantId::new(TENANT),
            AgentTaskId::new(id).expect("task id should be valid"),
        )
        .expect("task scope should be valid"),
    )
}

fn run_address(id: &str) -> AgentEntityAddress {
    AgentEntityAddress::Run(
        AgentRunScope::new(
            TenantId::new(TENANT),
            AgentId::new("support-agent").expect("agent id should be valid"),
            AgentRunId::new(id).expect("run id should be valid"),
        )
        .expect("run scope should be valid"),
    )
}

fn operation(label: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, label])
        .expect("operation id should be derivable")
}

fn envelope(
    kind: AgentExchangeKind,
    label: &str,
    initiator: AgentEntityAddress,
    target: AgentEntityAddress,
) -> AgentExchangeEnvelope {
    ChoreographyProbe::envelope(
        kind,
        operation(label),
        initiator,
        target,
        AgentTimestampMillis::new(1),
    )
    .expect("the envelope should be valid")
}

/// Reads a participant's durable record without waking its entity.
async fn durable_state(store: &Store, address: &AgentEntityAddress) -> ChoreographyProbeState {
    let mut host = AgentExchangeHost::new(address.clone(), ChoreographyProbe, store.clone());
    host.recover(AgentTimestampMillis::new(0))
        .await
        .expect("the durable state should recover")
        .clone()
}

#[tokio::test]
async fn colocated_entities_still_traverse_the_durable_outbox_and_inbox() {
    let store = Store::new();
    let clock = Arc::new(AtomicU64::new(1));
    let Some(mut node) = Node::boot(
        "choreography-colocated",
        "rakka-0",
        "uid-a",
        store.clone(),
        clock,
    )
    .await
    else {
        return;
    };
    node.runtime
        .apply_discovery(DiscoverySnapshot::new(
            "choreography-test",
            1,
            [node.runtime.local_node().clone()],
        ))
        .expect("single-node discovery should apply");

    let run = run_address("run-1");
    let task = task_address("ticket-1");
    assert!(node.owns(&node.run, &run), "the run is local");
    assert!(node.owns(&node.task, &task), "the task is local, too");

    // Colocated, and it changes nothing: the run entity records the exchange in
    // its own durable journal, the courier delivers it, and the task entity
    // durably accepts it before replying.
    let reply = node
        .ask(&node.run, &run, |reply_to| ProbeEntityMessage::Initiate {
            envelope: Box::new(envelope(
                AgentExchangeKind::Creation,
                "creation",
                run.clone(),
                task.clone(),
            )),
            reply_to,
        })
        .await;
    assert_eq!(reply.driven(), (1, 0, 0));

    let task_state = durable_state(&store, &task).await;
    let run_state = durable_state(&store, &run).await;

    assert_eq!(
        task_state.applied_count(AgentExchangeKind::Creation),
        1,
        "the receiver durably accepted the exchange"
    );
    assert!(task_state.is_created());
    assert_eq!(
        task_state.journal().applied_count(),
        1,
        "the acceptance is in the receiver's durable inbox, not an in-memory shortcut"
    );
    assert_eq!(
        run_state.settled_count(AgentExchangeKind::Creation),
        1,
        "the initiator settled the reply exactly once"
    );
    assert_eq!(run_state.journal().outstanding_count(), 0);
    assert_eq!(run_state.journal().settled_count(), 1);

    // No envelope crossed the wire, because nothing had to. The durable records
    // above are identical to the cross-node case all the same.
    assert_eq!(node.runtime.transport_snapshot().inbound_envelopes(), 0);

    node.system.shutdown();
}

#[tokio::test]
async fn an_exchange_converges_across_nodes_and_survives_the_entities_moving() {
    let store = Store::new();
    let clock = Arc::new(AtomicU64::new(1));
    let Some(mut node_a) = Node::boot(
        "choreography-split-a",
        "rakka-0",
        "uid-a",
        store.clone(),
        clock.clone(),
    )
    .await
    else {
        return;
    };
    let Some(mut node_b) = Node::boot(
        "choreography-split-b",
        "rakka-1",
        "uid-b",
        store.clone(),
        clock,
    )
    .await
    else {
        node_a.system.shutdown();
        return;
    };
    let node_b_id = node_b.runtime.local_node().id().clone();
    apply_pair_discovery(&mut node_a.runtime, &mut node_b.runtime);

    // Find a run that node B owns and a task that node A owns: the two ends of
    // the exchange are then genuinely on different nodes.
    let Some(run) = (0..256)
        .map(|index| run_address(&format!("run-{index}")))
        .find(|address| node_b.owns(&node_b.run, address) && !node_a.owns(&node_a.run, address))
    else {
        panic!("expected some run to be owned by node b");
    };
    let Some(task) = (0..256)
        .map(|index| task_address(&format!("ticket-{index}")))
        .find(|address| node_a.owns(&node_a.task, address) && !node_b.owns(&node_b.task, address))
    else {
        panic!("expected some task to be owned by node a");
    };

    // The run entity on node B initiates, and its courier has to reach node A.
    let reply = node_b
        .ask(&node_b.run, &run, |reply_to| ProbeEntityMessage::Initiate {
            envelope: Box::new(envelope(
                AgentExchangeKind::Creation,
                "creation",
                run.clone(),
                task.clone(),
            )),
            reply_to,
        })
        .await;
    assert_eq!(reply.driven(), (1, 0, 0));

    let task_state = durable_state(&store, &task).await;
    let run_state = durable_state(&store, &run).await;
    assert_eq!(task_state.applied_count(AgentExchangeKind::Creation), 1);
    assert_eq!(task_state.journal().applied_count(), 1);
    assert_eq!(run_state.settled_count(AgentExchangeKind::Creation), 1);
    assert_eq!(run_state.journal().outstanding_count(), 0);

    // And it really did cross the wire, rather than finding a local shortcut:
    // node A received the exchange request, and node B received its reply.
    wait_for(|| node_a.runtime.transport_snapshot().inbound_envelopes() >= 1).await;
    wait_for(|| node_b.runtime.transport_snapshot().inbound_envelopes() >= 1).await;

    // Re-driving the same operation id across the wire converges: the receiver
    // deduplicates, and the initiator finds nothing left to owe.
    let reply = node_b
        .ask(&node_b.run, &run, |reply_to| ProbeEntityMessage::Drive {
            reply_to,
        })
        .await;
    assert_eq!(reply.driven(), (0, 0, 0));
    assert_eq!(
        durable_state(&store, &task)
            .await
            .applied_count(AgentExchangeKind::Creation),
        1,
        "the re-drive must not produce a second transition"
    );

    // Now the entities move. Node B leaves gracefully, so node A takes over its
    // shards and the run entity is re-materialized there — from durable state
    // alone, on a node that has never seen it.
    node_b
        .runtime
        .mark_leaving(&node_b_id, 3)
        .expect("node b should begin a graceful leave");
    node_a
        .runtime
        .mark_leaving(&node_b_id, 3)
        .expect("node a should observe node b leaving");
    assert!(node_a.owns(&node_a.run, &run), "node a now owns the run");

    // The same saga continues on the moved entity, and converges the same way.
    let reply = node_a
        .ask(&node_a.run, &run, |reply_to| ProbeEntityMessage::Initiate {
            envelope: Box::new(envelope(
                AgentExchangeKind::Assignment,
                "assignment",
                run.clone(),
                task.clone(),
            )),
            reply_to,
        })
        .await;
    assert_eq!(reply.driven(), (1, 0, 0));

    let moved_run = node_a
        .ask(&node_a.run, &run, |reply_to| ProbeEntityMessage::Describe {
            reply_to,
        })
        .await
        .snapshot();
    assert_eq!(
        moved_run.settled_count(AgentExchangeKind::Creation),
        1,
        "the moved entity recovered what it had already settled"
    );
    assert_eq!(moved_run.settled_count(AgentExchangeKind::Assignment), 1);

    let task_state = durable_state(&store, &task).await;
    assert_eq!(task_state.applied_count(AgentExchangeKind::Creation), 1);
    assert_eq!(task_state.applied_count(AgentExchangeKind::Assignment), 1);
    assert_eq!(task_state.assignment_generation(), 1);

    node_a.system.shutdown();
    node_b.system.shutdown();
}

fn apply_pair_discovery(node_a: &mut ClusterNodeRuntime, node_b: &mut ClusterNodeRuntime) {
    let nodes = [node_a.local_node().clone(), node_b.local_node().clone()];
    node_a
        .apply_discovery(DiscoverySnapshot::new(
            "choreography-test",
            1,
            nodes.clone(),
        ))
        .expect("node a discovery should apply");
    node_b
        .apply_discovery(DiscoverySnapshot::new("choreography-test", 1, nodes))
        .expect("node b discovery should apply");
}

async fn build_runtime(logical_id: &str, incarnation: &str) -> Option<ClusterNodeRuntime> {
    let mut registry = SerializationRegistry::new();
    register_agent_exchange_codecs(&mut registry).expect("the exchange codecs should register");

    let node = ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", 0),
    )
    .with_role("agent-choreography");

    match ClusterNodeRuntimeBuilder::new(node)
        .with_membership_config(MembershipConfig::new(
            1,
            Duration::from_millis(50),
            Duration::from_millis(100),
        ))
        .with_transport_config(
            TcpRemoteTransportConfig::new()
                .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                .connect_timeout(Duration::from_millis(200))
                .reconnect_backoff(Duration::from_millis(10))
                .idle_timeout(Duration::from_secs(10)),
        )
        .with_registry(registry)
        .advertise_bound_addr(true)
        .build()
        .await
    {
        Ok(runtime) => Some(runtime),
        Err(error) if bind_denied(&error) => {
            eprintln!("skipping choreography cluster test; loopback bind denied: {error}");
            None
        }
        Err(error) => panic!("the cluster node runtime should bind: {error:?}"),
    }
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if condition() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the exchange to cross the wire"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn bind_denied(error: &ClusterNodeRuntimeError) -> bool {
    matches!(
        error,
        ClusterNodeRuntimeError::TcpTransport {
            error: TcpRemoteTransportError::Io { message }
        } if message.contains("Operation not permitted") || message.contains("Permission denied")
    )
}
