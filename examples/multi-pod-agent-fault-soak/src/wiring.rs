//! One pod: a real `ActorSystem`, real TCP remoting, and all five sharded
//! agent entity classes registered through their *remote* registrations.
//!
//! This is the first consumer in the repository of
//! [`rakka_agent::init_agent_entity_remote_sharding`] and its four siblings,
//! and of the production
//! [`rakka_agent::ShardedExchangeRoute`] carrying a real agent entity's
//! exchanges. Every acceptance example so far has used the testkit's
//! `LocalShardedExchangeRoute`, whose own documentation says it is "the local
//! arm ... without the `rakka-remote` ask client the other arm needs". A
//! harness that never took the other arm could not claim the entities recover
//! on a different pod.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent::testkit::SharedAtomicWorkflowClock;
use rakka_agent::{
    agent_conversation_entity_type_key, agent_run_entity_type_key, agent_task_entity_type_key,
    agent_team_entity_type_key, init_agent_conversation_entity_remote_sharding,
    init_agent_entity_remote_sharding, init_agent_run_entity_remote_sharding,
    init_agent_task_entity_remote_sharding, init_agent_team_entity_remote_sharding,
    register_agent_exchange_codecs, AgentConversationEntityMessage,
    AgentConversationEntityShardingSettings, AgentConversationState, AgentEntityClass,
    AgentEntityCommand, AgentEntityMessage, AgentEntityRegistration, AgentEntityReply,
    AgentEntityShardingSettings, AgentEntityState, AgentExchangeRouter, AgentRunEntityMessage,
    AgentRunEntityRegistration, AgentRunEntityShardingSettings, AgentRunState,
    AgentTaskEntityMessage, AgentTaskEntityRegistration, AgentTaskEntityShardingSettings,
    AgentTaskState, AgentTeamEntityMessage, AgentTeamEntityShardingSettings, AgentTeamState,
    InMemoryAgentConversationHistoryStore, InMemoryAgentTaskHistoryStore,
    InMemoryAgentTeamHistoryStore, ShardedExchangeRoute, WorkflowAgentRunEffectSink,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::AgentTimestampMillis;
use rakka_cluster::{ClusterNode, MembershipConfig, NodeAddress, NodeId};
use rakka_core::{ActorSystem, Message, ReplyTo};
use rakka_remote::{SerializationRegistry, TcpRemoteTransport, TcpRemoteTransportConfig};
use rakka_sharding::{
    ClusterNodeRuntime, ClusterNodeRuntimeBuilder, ClusterSharding, EntityTypeKey,
    EntityTypeRegistration, RemoteEntityAskClient,
};

use crate::codec::JsonPayloadCodec;
use crate::stores::{PodCrash, PodCrashStore, SharedFileStore, CRASHED};

/// The cluster role every pod in this harness joins under.
pub const ROLE: &str = "agent-multi-pod";

/// How long an exchange waits on a remote owner before the initiator treats
/// the delivery as failed. A delivery failure is never evidence the receiver
/// did not apply the exchange, which is why the fault matrix asserts on the
/// durable record rather than on this timing out.
pub const ASK_TIMEOUT: Duration = Duration::from_secs(2);

/// Every durable store one pod reads and writes.
///
/// The five entity-state stores and the workflow outbox live in the shared
/// directory, because those are what a recovering pod must find. The three
/// history sinks are deliberately per-pod: history is bounded observability
/// behind an authorized cursor, never the correctness source, and this harness
/// asserts convergence only from durable entity state.
#[derive(Debug, Clone)]
pub struct PodStores {
    /// The agent entity's durable store, armable to kill this pod.
    pub agents: PodCrashStore<AgentEntityState>,
    /// The task entity's durable store, armable to kill this pod.
    pub tasks: PodCrashStore<AgentTaskState>,
    /// The run entity's durable store, armable to kill this pod.
    pub runs: PodCrashStore<AgentRunState>,
    /// The team board's durable store.
    pub teams: SharedFileStore<AgentTeamState>,
    /// The conversation's durable store.
    pub conversations: SharedFileStore<AgentConversationState>,
    /// The durable workflow outbox every effect ticket lands in.
    pub workflow: SharedFileStore<WorkflowState>,
    /// Per-pod history sinks; see the struct documentation.
    pub task_history: InMemoryAgentTaskHistoryStore,
    /// Per-pod team history sink.
    pub team_history: InMemoryAgentTeamHistoryStore,
    /// Per-pod conversation history sink.
    pub conversation_history: InMemoryAgentConversationHistoryStore,
}

impl PodStores {
    /// Opens every store against the shared directory.
    ///
    /// `crash` arms this pod — and only this pod — to die at the `nth` write
    /// of the named class. A pod given no arming is an ordinary survivor.
    #[must_use]
    pub fn open(root: &Path, crash: Option<(CrashTarget, usize, PodCrash)>) -> Self {
        let arm = |target: CrashTarget| {
            crash.and_then(|(armed, nth, point)| (armed == target).then_some((nth, point)))
        };
        let agents = PodCrashStore::new(SharedFileStore::new(root.join("agents")));
        let tasks = PodCrashStore::new(SharedFileStore::new(root.join("tasks")));
        let runs = PodCrashStore::new(SharedFileStore::new(root.join("runs")));
        Self {
            agents: match arm(CrashTarget::Agents) {
                Some((nth, point)) => agents.armed_at(nth, point, root.join(CRASHED)),
                None => agents,
            },
            tasks: match arm(CrashTarget::Tasks) {
                Some((nth, point)) => tasks.armed_at(nth, point, root.join(CRASHED)),
                None => tasks,
            },
            runs: match arm(CrashTarget::Runs) {
                Some((nth, point)) => runs.armed_at(nth, point, root.join(CRASHED)),
                None => runs,
            },
            teams: SharedFileStore::new(root.join("teams")),
            conversations: SharedFileStore::new(root.join("conversations")),
            workflow: SharedFileStore::new(root.join("workflow")),
            task_history: InMemoryAgentTaskHistoryStore::new(),
            team_history: InMemoryAgentTeamHistoryStore::new(),
            conversation_history: InMemoryAgentConversationHistoryStore::new(),
        }
    }
}

/// Which durable class a pod is armed to die inside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashTarget {
    /// The agent entity's store.
    Agents,
    /// The task entity's store.
    Tasks,
    /// The run entity's store.
    Runs,
}

impl CrashTarget {
    /// Parses the driver's `--crash-store` argument.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "agents" => Some(Self::Agents),
            "tasks" => Some(Self::Tasks),
            "runs" => Some(Self::Runs),
            _ => None,
        }
    }

    /// Stable kebab-case label, as the driver passes it.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Agents => "agents",
            Self::Tasks => "tasks",
            Self::Runs => "runs",
        }
    }
}

/// One booted pod, holding everything its lifetime owns.
pub struct Pod {
    /// The node runtime: membership, transport, and the remote ask client.
    pub runtime: ClusterNodeRuntime,
    /// This pod's actor system. Dropping it stops every hosted entity.
    pub system: ActorSystem,
    /// The sharding facade the five entity classes registered through.
    pub sharding: ClusterSharding,
    /// The router every cross-entity exchange resolves its owner through.
    pub router: AgentExchangeRouter,
    /// The durable stores, shared with every other pod.
    pub stores: PodStores,
    /// The shared logical clock, advanced by this pod's own transitions.
    pub clock: Arc<AtomicU64>,
    /// The shared directory every pod reads and writes.
    pub root: std::path::PathBuf,
    /// The agent entity's registration, used to resolve shard ownership and to
    /// command the entity through its shard.
    pub agents: AgentEntityRegistration,
    /// The task entity's registration, used to resolve shard ownership.
    pub tasks: AgentTaskEntityRegistration,
    /// The run entity's registration, used to resolve shard ownership.
    pub runs: AgentRunEntityRegistration,
    /// The remote ask client the agent entity's command surface travels.
    pub ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
}

impl Pod {
    /// Whether this pod currently owns the shard hosting `entity_id` of the
    /// task class.
    #[must_use]
    pub fn owns_task(&self, entity_id: &str) -> bool {
        owns(&self.tasks, entity_id)
    }

    /// Whether this pod currently owns the shard hosting `entity_id` of the
    /// run class.
    #[must_use]
    pub fn owns_run(&self, entity_id: &str) -> bool {
        owns(&self.runs, entity_id)
    }

    /// Commands the agent entity through its shard, wherever it is hosted.
    ///
    /// Returns whether the command crossed the wire. This is the only path in
    /// the harness that reaches `init_agent_entity_remote_sharding`'s remote
    /// arm: the other four classes are addressed by exchange envelope through
    /// [`AgentExchangeRouter`], while the agent class takes a serializable
    /// [`AgentEntityCommand`] paired with a node-local reply channel on the
    /// owner. Without it the registration is made and never exercised, and the
    /// payload codecs it requires can be absent without anything noticing.
    ///
    /// The local and remote arms are the same decision `ShardedExchangeRoute`
    /// makes for the other four classes: resolve the owner, then ask the local
    /// entity or ask the owning node.
    pub async fn command_agent_entity(
        &self,
        entity_id: &str,
        command: AgentEntityCommand,
    ) -> Result<bool, String> {
        let entity = self.agents.entity_ref_for(entity_id);
        let (owner, _shard) = entity
            .region()
            .resolve(entity.entity_ref())
            .map_err(|error| format!("resolving the agent entity's owner: {error}"))?;
        let is_local = entity
            .region()
            .local_node_id()
            .is_some_and(|local| local == &owner);

        if is_local {
            entity
                .ask(
                    move |reply_to| AgentEntityMessage { command, reply_to },
                    ASK_TIMEOUT,
                )
                .await
                .map(|_reply: AgentEntityReply| false)
                .map_err(|error| format!("asking the local agent entity: {error}"))
        } else {
            self.ask_client
                .ask::<AgentEntityCommand, AgentEntityMessage, AgentEntityReply>(
                    entity.region(),
                    entity.entity_ref(),
                    command,
                    ASK_TIMEOUT,
                )
                .await
                .map(|_reply| true)
                .map_err(|error| format!("asking the agent entity on {owner:?}: {error}"))
        }
    }
}

/// Resolves an entity's owner and compares it to this node.
///
/// A pod that does not own the shard leaves the entity alone rather than
/// becoming a second writer to its record — which is what production does, and
/// what makes the exchanges below actually cross the wire.
fn owns<M: Message>(registration: &EntityTypeRegistration<M>, entity_id: &str) -> bool {
    let entity = registration.entity_ref_for(entity_id);
    let Ok((owner, _shard)) = entity.region().resolve(entity.entity_ref()) else {
        return false;
    };
    entity
        .region()
        .local_node_id()
        .is_some_and(|local| local == &owner)
}

/// Boots one pod and registers all five entity classes for remote hosting.
///
/// Every failure keeps its message. This used to funnel eight independent
/// fallible steps — codec registration, the runtime build, `ClusterSharding`,
/// and all five entity registrations — through `.ok()?` into one `None` the
/// driver reported as "loopback binding is unavailable", so a duplicate entity
/// key, a codec collision, or a renamed entity type made the whole gated
/// harness exit 0 having proved nothing, with the real error already discarded.
/// Whether the sandbox permits loopback binding is now settled once by the
/// driver before any world runs, which is the only thing that may skip.
pub async fn boot_pod(
    logical_id: &str,
    incarnation: &str,
    port: u16,
    root: &Path,
    crash: Option<(CrashTarget, usize, PodCrash)>,
) -> Result<Pod, String> {
    let mut registry = SerializationRegistry::new();
    register_agent_exchange_codecs(&mut registry)
        .map_err(|error| format!("registering the agent exchange codecs: {error}"))?;

    // `init_agent_entity_remote_sharding`'s documented precondition. The other
    // four classes are addressed by exchange envelope, whose codecs the call
    // above registers; the agent class takes a command, and without these two
    // its remote arm cannot encode one.
    registry
        .register::<AgentEntityCommand, _>(JsonPayloadCodec::<AgentEntityCommand>::new(
            "rakka.examples.multi_pod_agent_fault_soak.AgentEntityCommand",
        ))
        .map_err(|error| format!("registering the agent entity command codec: {error}"))?;
    registry
        .register::<AgentEntityReply, _>(JsonPayloadCodec::<AgentEntityReply>::new(
            "rakka.examples.multi_pod_agent_fault_soak.AgentEntityReply",
        ))
        .map_err(|error| format!("registering the agent entity reply codec: {error}"))?;

    let node = ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new("127.0.0.1", port),
    )
    .with_role(ROLE);

    let mut runtime = ClusterNodeRuntimeBuilder::new(node)
        .with_membership_config(MembershipConfig::new(
            1,
            Duration::from_millis(50),
            Duration::from_millis(100),
        ))
        .with_transport_config(
            TcpRemoteTransportConfig::new()
                .bind_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
                .connect_timeout(Duration::from_millis(500))
                .reconnect_backoff(Duration::from_millis(10))
                .idle_timeout(Duration::from_secs(30)),
        )
        .with_registry(registry)
        .build()
        .await
        .map_err(|error| format!("building the cluster node runtime on port {port}: {error}"))?;

    let system = ActorSystem::new(format!("rakka-multi-pod-{logical_id}"));
    let sharding = ClusterSharding::for_node_runtime(&system, &runtime)
        .map_err(|error| format!("creating cluster sharding: {error}"))?;
    let stores = PodStores::open(root, crash);
    let counter = Arc::new(AtomicU64::new(1));
    let clock = SharedAtomicWorkflowClock::new(counter.clone());

    // The same clock the drive loop stamps its transitions from. Left at
    // `default()`, each entity actor stamps wall-clock milliseconds while
    // `flow::now` stamps this counter — two writers putting values ~1.75e12
    // apart into the same `updated_at`, so which one a record carries depends
    // on who wrote last. Nothing reads elapsed time in this fixture today (no
    // goal deadline, wake policy, escrow, or checkpoint), which is the only
    // reason it is latent rather than wrong; every sibling acceptance example
    // aligns the two, and so does this one now.
    let entity_clock: Arc<dyn Fn() -> AgentTimestampMillis + Send + Sync> = {
        let counter = counter.clone();
        Arc::new(move || AgentTimestampMillis::new(counter.fetch_add(1, Ordering::SeqCst)))
    };
    let ask_client = runtime.ask_client();
    let command_ask_client = runtime.ask_client();

    // Every class routes through the same production route. Nothing below
    // knows or cares whether a target is local.
    // The agent entity takes commands, not exchange envelopes, so it has no
    // route here; `AgentEntityClass::Agent` is an address class for the
    // journal's own bookkeeping rather than an exchange destination.
    let router = AgentExchangeRouter::new()
        .with_route(
            AgentEntityClass::Task,
            Arc::new(route(
                &sharding,
                agent_task_entity_type_key(),
                ask_client.clone(),
                |envelope, reply_to| AgentTaskEntityMessage::Exchange {
                    envelope: Box::new(envelope),
                    reply_to,
                },
            )),
        )
        .with_route(
            AgentEntityClass::Run,
            Arc::new(route(
                &sharding,
                agent_run_entity_type_key(),
                ask_client.clone(),
                |envelope, reply_to| AgentRunEntityMessage::Exchange {
                    envelope: Box::new(envelope),
                    reply_to,
                },
            )),
        )
        .with_route(
            AgentEntityClass::Team,
            Arc::new(route(
                &sharding,
                agent_team_entity_type_key(),
                ask_client.clone(),
                |envelope, reply_to| AgentTeamEntityMessage::Exchange {
                    envelope: Box::new(envelope),
                    reply_to,
                },
            )),
        )
        .with_route(
            AgentEntityClass::Conversation,
            Arc::new(route(
                &sharding,
                agent_conversation_entity_type_key(),
                ask_client,
                |envelope, reply_to| AgentConversationEntityMessage::Exchange {
                    envelope: Box::new(envelope),
                    reply_to,
                },
            )),
        );

    let agents = init_agent_entity_remote_sharding(
        &sharding,
        &mut runtime,
        stores.agents.clone(),
        AgentEntityShardingSettings::default(),
    )
    .map_err(|error| format!("registering the agent entity for remote sharding: {error}"))?;
    let tasks = init_agent_task_entity_remote_sharding(
        &sharding,
        &mut runtime,
        stores.tasks.clone(),
        stores.agents.clone(),
        stores.task_history.clone(),
        router.clone(),
        AgentTaskEntityShardingSettings::default().with_clock(entity_clock.clone()),
    )
    .map_err(|error| format!("registering the task entity for remote sharding: {error}"))?;
    let runs = init_agent_run_entity_remote_sharding(
        &sharding,
        &mut runtime,
        stores.runs.clone(),
        WorkflowAgentRunEffectSink::new(stores.workflow.clone(), clock.clone()),
        router.clone(),
        AgentRunEntityShardingSettings::default().with_clock(entity_clock.clone()),
    )
    .map_err(|error| format!("registering the run entity for remote sharding: {error}"))?;
    init_agent_team_entity_remote_sharding(
        &sharding,
        &mut runtime,
        stores.teams.clone(),
        stores.team_history.clone(),
        router.clone(),
        AgentTeamEntityShardingSettings::default().with_clock(entity_clock.clone()),
    )
    .map_err(|error| format!("registering the team entity for remote sharding: {error}"))?;
    init_agent_conversation_entity_remote_sharding(
        &sharding,
        &mut runtime,
        stores.conversations.clone(),
        stores.agents.clone(),
        stores.conversation_history.clone(),
        router.clone(),
        AgentConversationEntityShardingSettings::default().with_clock(entity_clock),
    )
    .map_err(|error| format!("registering the conversation entity for remote sharding: {error}"))?;

    Ok(Pod {
        runtime,
        system,
        sharding,
        router,
        stores,
        clock: counter,
        root: root.to_path_buf(),
        agents,
        tasks,
        runs,
        ask_client: command_ask_client,
    })
}

fn route<M>(
    sharding: &ClusterSharding,
    key: EntityTypeKey<M>,
    ask_client: RemoteEntityAskClient<TcpRemoteTransport>,
    build: impl Fn(rakka_agent::AgentExchangeEnvelope, ReplyTo<rakka_agent::AgentExchangeReply>) -> M
        + Send
        + Sync
        + 'static,
) -> ShardedExchangeRoute<M, TcpRemoteTransport>
where
    M: Message,
{
    ShardedExchangeRoute::new(sharding.clone(), key, ask_client, ASK_TIMEOUT, build)
}
