//! The coordination world: all five real sharded entity types on one node,
//! the in-process `rakka-a2a` service core every coordination command travels
//! through, and the production effect dispatcher.
//!
//! Two stores are crash-armable — the task store and the conversation store —
//! because those are the two the walk kills an owner inside. The rest are
//! plain: arming a store the walk never crashes would only obscure which
//! window a failure came from.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_a2a::agents::{
    A2AAgentClock, A2AAgentHandoffSendExecutor, A2AAgentTarget, A2AStaticAgentCatalog,
    RakkaAgentA2AService,
};
use rakka_a2a::auth::AllowAllAuthorizer;
use rakka_a2a::mapping::A2AHeaderTenantResolver;
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    CrashingStateStore, DeferredExchangeRouter, DeterministicModelAdapter,
    InProcessRunResultDelivery, KillSwitchProbe, LocalShardedExchangeRoute, RecordingToolExecutor,
    ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    agent_conversation_entity_type_key, agent_entity_type_key, agent_run_entity_type_key,
    agent_task_entity_type_key, agent_team_entity_type_key,
    init_agent_conversation_entity_sharding, init_agent_entity_sharding,
    init_agent_run_entity_sharding, init_agent_task_entity_sharding,
    init_agent_team_entity_sharding, AgentCapabilityId, AgentConversationEntityMessage,
    AgentConversationEntityRegistration, AgentConversationEntityShardingSettings,
    AgentConversationState, AgentCoordinationCapabilityKind, AgentDelegationTarget,
    AgentEffectSpec, AgentEntityAuthority, AgentEntityClass, AgentEntityRegistration,
    AgentEntityShardingSettings, AgentEntityState, AgentExchangeRouter, AgentRevisionNumber,
    AgentRunDelegationConfig, AgentRunEntityMessage, AgentRunEntityRegistration,
    AgentRunEntityShardingSettings, AgentRunMemory, AgentRunState, AgentSchemaId, AgentSchemaRef,
    AgentTaskEntityMessage, AgentTaskEntityRegistration, AgentTaskEntityShardingSettings,
    AgentTaskState, AgentTeamEntityMessage, AgentTeamEntityRegistration,
    AgentTeamEntityShardingSettings, AgentTeamState, AgentToolAuthority, AgentToolBinding,
    AgentToolDeclaration, AgentToolDescriptor, AgentToolId, AgentToolKind, AgentToolRegistry,
    InMemoryAgentConversationHistoryStore, InMemoryAgentTaskHistoryStore,
    InMemoryAgentTeamHistoryStore, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    StaticAgentDelegationCatalog, WorkflowAgentRunEffectSink,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId,
    AgentTimestampMillis,
};
use rakka_core::{ActorSystem, InMemoryMetricsRecorder};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_sharding::ClusterSharding;

/// The tenant of the whole walk.
pub const TENANT: &str = "acme";
/// The team the board belongs to.
pub const TEAM: &str = "support-pod";
/// The board task every act carries — one `AgentTaskId`, start to finish.
pub const TASK: &str = "ticket-4711";
/// The team member that wins the claim and later hands the task off.
pub const TRIAGER: &str = "triage-agent";
/// The team member whose definition never granted `Team`.
pub const UNCAPABLE: &str = "intern-agent";
/// The handoff target: the agent that finishes the task.
pub const SPECIALIST: &str = "billing-agent";
/// The moderator of the review conversation.
pub const MODERATOR: &str = "review-moderator";
/// The human-owned upstream the specialist blocks on.
pub const APPROVAL_TASK: &str = "refund-approval";
/// The moderated conversation the specialist's review runs in.
pub const CONVERSATION: &str = "refund-review";
/// The typed task definition every agent here accepts.
pub const TASK_DEFINITION: &str = "resolve-ticket";
/// The specialist's skill, the handoff target catalog's key.
pub const SKILL: &str = "billing";
/// The model-visible handoff tool: the verb the loop intercepts into a
/// transfer.
pub const HANDOFF_TOOL: &str = "transfer";
/// The config's delegation verb. This walk never delegates — M4's example
/// owns that milestone — but `AgentRunDelegationConfig` carries one tool id
/// for the delegation verb and the handoff policy carries its own, and the
/// two may not collide. Declaring it and never scripting it is the honest
/// shape: the capability set below grants `Handoff` and nothing else, so the
/// verb is unreachable even if a model asked for it.
pub const DELEGATE_TOOL: &str = "delegate";
/// The consequential, non-idempotent external tool: checkpoint-gated.
pub const REFUND_TOOL: &str = "issue-refund";
/// The dispatcher fleet's lease.
pub const LEASE_MS: u64 = 60_000;
/// Every sharded ask's timeout.
pub const ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// The concrete A2A service over this world's stores. The task and
/// conversation stores are the crash-armable ones — the two the walk kills an
/// owner inside — and the service holds the very same handles, so a kill
/// reaches the wire path too.
pub type Service = RakkaAgentA2AService<
    CrashingStateStore<AgentTaskState>,
    InMemoryDurableStateStore<AgentEntityState>,
    InMemoryAgentTaskHistoryStore,
    InMemoryDurableStateStore<AgentRunState>,
    InMemoryDurableStateStore<AgentTeamState>,
    InMemoryAgentTeamHistoryStore,
    CrashingStateStore<AgentConversationState>,
    InMemoryAgentConversationHistoryStore,
>;

/// The production dispatch pipeline over this world's stores.
pub type Pipeline = rakka_agent::AgentRunEffectDispatcher<
    InMemoryDurableStateStore<WorkflowState>,
    InMemoryDurableStateStore<AgentDispatcherFleetState>,
    InMemoryDurableStateStore<AgentRunState>,
    SharedAtomicWorkflowClock,
>;

/// A deterministic service clock over the shared tick counter.
struct TickClock(Arc<AtomicU64>);

impl A2AAgentClock for TickClock {
    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

/// A schema reference at its initial revision.
#[must_use]
pub fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("the schema id is valid"),
        AgentRevisionNumber::INITIAL,
    )
}

/// The registry binding the one consequential tool: non-idempotent and
/// approval-required, so the walk's checkpoint bullet is a property of the
/// deployment rather than of the test.
#[must_use]
pub fn tool_registry() -> AgentToolRegistry {
    let descriptor = AgentToolDescriptor::new(
        AgentToolId::new(REFUND_TOOL).expect("the tool id is valid"),
        AgentToolKind::Function,
        "Issues one refund against the billing system.",
        schema("refund-input"),
        schema("refund-output"),
    )
    .expect("the descriptor is valid");
    let declaration = AgentToolDeclaration::new(rakka_agent::AgentEffectSafetyClass::NonIdempotent);
    // Checkpoint-required by declaration: the effect gate is a property of
    // this deployment's registry, which is what makes the M5 bullet's
    // "exact effect approval still uses a bound checkpoint" clause a fact
    // about the wiring rather than about the walk.
    let spec = AgentEffectSpec::non_idempotent().with_checkpoint_required();
    AgentToolRegistry::new()
        .register(
            AgentToolBinding::new(descriptor, declaration, spec.max_attempts)
                .with_checkpoint_required(),
        )
        .expect("the tool registers")
}

/// The handoff wiring: the specialist is the one resolvable target, reached
/// by skill, and the `Handoff` capability is declared at construction — a
/// deployment cannot wire the tool while forgetting the capability that
/// authorizes it.
#[must_use]
pub fn handoff_config() -> AgentRunDelegationConfig {
    let catalog = StaticAgentDelegationCatalog::new().with_target(
        AgentCapabilityId::new(SKILL).expect("the skill id is valid"),
        AgentDelegationTarget::new(
            rakka_agent::AgentId::new(SPECIALIST).expect("the agent id is valid"),
            rakka_agent::AgentTaskDefinitionId::new(TASK_DEFINITION)
                .expect("the definition id is valid"),
        ),
    );
    AgentRunDelegationConfig::new(
        AgentToolId::new(DELEGATE_TOOL).expect("the tool id is valid"),
        Arc::new(catalog),
        [AgentCoordinationCapabilityKind::Handoff]
            .into_iter()
            .collect(),
    )
    .expect("the handoff config is valid")
    .with_handoff(rakka_agent::AgentHandoffPolicy::new(
        AgentToolId::new(HANDOFF_TOOL).expect("the tool id is valid"),
        AgentRevisionNumber::INITIAL,
    ))
    .expect("the handoff policy is authorized by the declared capability")
}

/// The five-entity deployment the walk runs over.
pub struct World {
    /// The actor system hosting the sharded entities.
    pub system: ActorSystem,
    /// The single-node sharding facade.
    pub sharding: ClusterSharding,
    /// Durable task records; crash-armable for the handoff-loss bullet.
    pub tasks: CrashingStateStore<AgentTaskState>,
    /// Durable agent records — the envelope source both new doors read.
    pub agents: InMemoryDurableStateStore<AgentEntityState>,
    /// Durable run records.
    pub runs: InMemoryDurableStateStore<AgentRunState>,
    /// Durable team records: the shared board.
    pub teams: InMemoryDurableStateStore<AgentTeamState>,
    /// Durable conversation records; crash-armable for the moderation-loss
    /// bullet.
    pub conversations: CrashingStateStore<AgentConversationState>,
    /// Append-only task history.
    pub history: InMemoryAgentTaskHistoryStore,
    /// Append-only team history — one of the three replayed logs.
    pub team_history: InMemoryAgentTeamHistoryStore,
    /// Append-only conversation history — one of the three replayed logs.
    pub conversation_history: InMemoryAgentConversationHistoryStore,
    /// The workflow outbox the runs' effects ticket through.
    pub workflow_store: InMemoryDurableStateStore<WorkflowState>,
    /// The dispatcher fleet's lease records.
    pub fleet_store: InMemoryDurableStateStore<AgentDispatcherFleetState>,
    /// The workflow clock, advanced deliberately to expire leases.
    pub wf_clock: SharedAtomicWorkflowClock,
    /// The shared tick counter behind every timestamp.
    pub clock: Arc<AtomicU64>,
    /// The recording tool executor — the billing system.
    pub tools: RecordingToolExecutor,
    /// The dispatcher kill switch.
    pub probe: KillSwitchProbe,
    /// The deployment's tool registry.
    pub registry: AgentToolRegistry,
    /// The bounded metrics recorder wired into the sharded runs.
    pub metrics: Arc<InMemoryMetricsRecorder>,
    /// The session-memory backend — the private memory a handoff must not
    /// carry across.
    pub session: Arc<InMemorySessionMemoryStore>,
    /// The immutable context-snapshot store.
    pub snapshots: Arc<InMemoryContextSnapshotStore>,
    /// The exchange router: every cross-entity exchange goes through the
    /// sharded entities' own durable accept path.
    pub router: AgentExchangeRouter,
    /// The in-process A2A service core every coordination command travels.
    pub service: Arc<Service>,
    /// The agent entity type registration.
    pub agent_registration: AgentEntityRegistration,
    /// The task entity type registration.
    pub task_registration: AgentTaskEntityRegistration,
    /// The run entity type registration.
    pub run_registration: AgentRunEntityRegistration,
    /// The team entity type registration.
    pub team_registration: AgentTeamEntityRegistration,
    /// The conversation entity type registration.
    pub conversation_registration: AgentConversationEntityRegistration,
}

impl World {
    /// Builds the world: stores, all five sharded entity types, the router,
    /// and the service.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn new() -> Self {
        let system = ActorSystem::new("CoordinationCapabilityAcceptance");
        let sharding = ClusterSharding::get(&system);
        let tasks = CrashingStateStore::<AgentTaskState>::new();
        let agents = InMemoryDurableStateStore::<AgentEntityState>::new();
        let runs = InMemoryDurableStateStore::<AgentRunState>::new();
        let teams = InMemoryDurableStateStore::<AgentTeamState>::new();
        let conversations = CrashingStateStore::<AgentConversationState>::new();
        // Deliberately bounded: the milestone's replay bullet has two arms,
        // and the retention-gap arm is only demonstrable against a log that
        // really did drop its oldest entries. The bound is generous enough
        // that the walk's own paging still reads a long contiguous prefix.
        let history = InMemoryAgentTaskHistoryStore::new().with_retention(16);
        let team_history = InMemoryAgentTeamHistoryStore::new();
        let conversation_history = InMemoryAgentConversationHistoryStore::new();
        let workflow_store = InMemoryDurableStateStore::<WorkflowState>::new();
        let fleet_store = InMemoryDurableStateStore::<AgentDispatcherFleetState>::new();
        let clock = Arc::new(AtomicU64::new(1));
        let wf_clock = SharedAtomicWorkflowClock::new(clock.clone());
        let tools = RecordingToolExecutor::new().with_result(
            REFUND_TOOL,
            rakka_agent::AgentTaskContent::inline(serde_json::json!({ "refunded": true }))
                .expect("the tool result is inline-bounded"),
        );
        let registry = tool_registry();
        let metrics = Arc::new(InMemoryMetricsRecorder::new());
        let session = Arc::new(InMemorySessionMemoryStore::new());
        let snapshots = Arc::new(InMemoryContextSnapshotStore::new());

        let policies = registry
            .effect_policies()
            .expect("the registry projects valid policies");
        let sink = WorkflowAgentRunEffectSink::new(workflow_store.clone(), wf_clock.clone());

        let deferred = DeferredExchangeRouter::new();
        let entity_clock = {
            let clock = clock.clone();
            Arc::new(move || AgentTimestampMillis::new(clock.fetch_add(1, Ordering::SeqCst)))
        };

        let agent_registration = init_agent_entity_sharding(
            &sharding,
            agents.clone(),
            AgentEntityShardingSettings::new(agent_entity_type_key()),
        )
        .expect("agent entity sharding initializes");
        let task_registration = init_agent_task_entity_sharding(
            &sharding,
            tasks.clone(),
            agents.clone(),
            history.clone(),
            deferred.as_router(),
            AgentTaskEntityShardingSettings::new(agent_task_entity_type_key())
                .with_clock(entity_clock.clone()),
        )
        .expect("task entity sharding initializes");
        // The sharded factory is the production driver of every run, so the
        // handoff interception is wired here, on the one path every driver
        // shares.
        let run_registration = init_agent_run_entity_sharding(
            &sharding,
            runs.clone(),
            sink,
            deferred.as_router(),
            AgentRunEntityShardingSettings::new(agent_run_entity_type_key())
                .with_clock(entity_clock.clone())
                .with_effect_policies(policies)
                .with_metrics(metrics.clone())
                .with_memory(AgentRunMemory::new(session.clone(), snapshots.clone()))
                .with_delegation(handoff_config()),
        )
        .expect("run entity sharding initializes");
        let team_registration = init_agent_team_entity_sharding(
            &sharding,
            teams.clone(),
            team_history.clone(),
            deferred.as_router(),
            AgentTeamEntityShardingSettings::new(agent_team_entity_type_key())
                .with_clock(entity_clock.clone()),
        )
        .expect("team entity sharding initializes");
        // The conversation entity reads the agents' durable records to decide
        // whether a speaker's definition admits moderated participation. It is
        // a store read, never an ask, so it stays correct while every agent is
        // passivated.
        let conversation_registration = init_agent_conversation_entity_sharding(
            &sharding,
            conversations.clone(),
            agents.clone(),
            conversation_history.clone(),
            deferred.as_router(),
            AgentConversationEntityShardingSettings::new(agent_conversation_entity_type_key())
                .with_clock(entity_clock),
        )
        .expect("conversation entity sharding initializes");

        let router = AgentExchangeRouter::new()
            .with_route(
                AgentEntityClass::Task,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    task_registration.key().clone(),
                    ASK_TIMEOUT,
                    |envelope, reply_to| AgentTaskEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            )
            .with_route(
                AgentEntityClass::Run,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    run_registration.key().clone(),
                    ASK_TIMEOUT,
                    |envelope, reply_to| AgentRunEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            )
            .with_route(
                AgentEntityClass::Team,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    team_registration.key().clone(),
                    ASK_TIMEOUT,
                    |envelope, reply_to| AgentTeamEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            )
            .with_route(
                AgentEntityClass::Conversation,
                Arc::new(LocalShardedExchangeRoute::new(
                    sharding.clone(),
                    conversation_registration.key().clone(),
                    ASK_TIMEOUT,
                    |envelope, reply_to| AgentConversationEntityMessage::Exchange {
                        envelope: Box::new(envelope),
                        reply_to,
                    },
                )),
            );
        deferred.install(router.clone());

        let service = Arc::new(
            Service::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                runs.clone(),
                teams.clone(),
                team_history.clone(),
                conversations.clone(),
                conversation_history.clone(),
                router.clone(),
                Arc::new(
                    A2AStaticAgentCatalog::new().with_target(A2AAgentTarget::new(
                        rakka_agent::AgentId::new(SPECIALIST).expect("the agent id is valid"),
                        crate::flow::task_definition(),
                    )),
                ),
                Arc::new(InMemoryA2ATaskProjectionStore::local()),
                Arc::new(A2AHeaderTenantResolver),
                Arc::new(AllowAllAuthorizer),
            )
            .with_clock(Arc::new(TickClock(clock.clone())))
            .with_default_tenant(TENANT),
        );

        Self {
            system,
            sharding,
            tasks,
            agents,
            runs,
            teams,
            conversations,
            history,
            team_history,
            conversation_history,
            workflow_store,
            fleet_store,
            wf_clock,
            clock,
            tools,
            probe: KillSwitchProbe::new(),
            registry,
            metrics,
            session,
            snapshots,
            router,
            service,
            agent_registration,
            task_registration,
            run_registration,
            team_registration,
            conversation_registration,
        }
    }

    /// A fresh production dispatch worker over the shared durable stores,
    /// scripted with `adapter` — building one anew is exactly what recovery
    /// after a dispatcher death looks like, and each agent's runs pump under
    /// that agent's own scripted model.
    ///
    /// The handoff send executor is the real `rakka-a2a` one: the transfer
    /// traverses the durable outbox and the A2A boundary even though source
    /// and target are colocated, which is what specification 8.9 requires.
    #[must_use]
    pub fn pipeline(&self, adapter: DeterministicModelAdapter) -> Pipeline {
        rakka_agent::AgentRunEffectDispatcher::new(
            AgentDispatcherWorkerId::new("worker-1"),
            self.workflow_store.clone(),
            self.fleet_store.clone(),
            self.runs.clone(),
            self.wf_clock.clone(),
            Arc::new(adapter),
            Arc::new(self.tools.clone()),
            Arc::new(AgentEntityAuthority::new(
                self.agents.clone(),
                AgentToolAuthority::new(self.registry.clone()),
            )),
            Arc::new(
                InProcessRunResultDelivery::new(
                    self.runs.clone(),
                    WorkflowAgentRunEffectSink::new(
                        self.workflow_store.clone(),
                        self.wf_clock.clone(),
                    ),
                    self.router.clone(),
                    self.clock.clone(),
                )
                .with_effect_policies(
                    self.registry
                        .effect_policies()
                        .expect("the registry projects valid policies"),
                )
                .with_delegation(handoff_config()),
            ),
        )
        .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
        .with_probe(Arc::new(self.probe.clone()))
        .with_reconciler(Arc::new(ScriptedReconciler::new()))
        .with_a2a_handoff_executor(Arc::new(A2AAgentHandoffSendExecutor::new(
            self.service.clone(),
        )))
    }

    /// The next deterministic tick.
    pub fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// How many entities of every registered type are currently resident.
    ///
    /// All five types, because "the team remains logically active while every
    /// member passivates" is only demonstrated if nothing at all is holding a
    /// runtime resource — not the board, not the members, not the task.
    #[must_use]
    pub fn resident(&self) -> usize {
        [
            self.sharding
                .registration_state(self.agent_registration.key()),
            self.sharding
                .registration_state(self.task_registration.key()),
            self.sharding
                .registration_state(self.run_registration.key()),
            self.sharding
                .registration_state(self.team_registration.key()),
            self.sharding
                .registration_state(self.conversation_registration.key()),
        ]
        .into_iter()
        .map(|state| state.expect("the registration exists").local_entity_count())
        .sum()
    }

    /// Advances the shared clock past the fleet lease.
    pub fn expire_lease(&self) {
        self.wf_clock.advance(LEASE_MS + 1);
    }
}

impl Default for World {
    fn default() -> Self {
        Self::new()
    }
}
