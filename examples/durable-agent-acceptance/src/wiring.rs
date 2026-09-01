//! The sharded world: all three real entity types on one node, the
//! in-process A2A service core, and the production effect dispatcher.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_a2a::agents::{
    A2AAgentClock, A2AAgentTarget, A2AStaticAgentCatalog, RakkaAgentA2AService,
};
use rakka_a2a::auth::AllowAllAuthorizer;
use rakka_a2a::mapping::A2AHeaderTenantResolver;
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    CrashingStateStore, DeferredExchangeRouter, DeterministicModelAdapter,
    InProcessRunResultDelivery, KillSwitchProbe, LocalShardedExchangeRoute, RecordingToolExecutor,
    ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::AgentRevisionNumber;
use rakka_agent::{
    agent_entity_type_key, agent_run_entity_type_key, agent_task_entity_type_key,
    init_agent_entity_sharding, init_agent_run_entity_sharding, init_agent_task_entity_sharding,
    AgentDecisionEvent, AgentDecisionEventPage, AgentDecisionEventSink, AgentDecisionWriteStatus,
    AgentEffectSpec, AgentEntityAuthority, AgentEntityClass, AgentEntityRegistration,
    AgentEntityShardingSettings, AgentEntityState, AgentExchangeRouter, AgentObservabilityError,
    AgentObservabilityFuture, AgentRunEntityMessage, AgentRunEntityRegistration,
    AgentRunEntityShardingSettings, AgentRunMemory, AgentRunScope, AgentRunState, AgentSchemaId,
    AgentSchemaRef, AgentTaskEntityMessage, AgentTaskEntityRegistration,
    AgentTaskEntityShardingSettings, AgentTaskState, AgentToolAuthority, AgentToolBinding,
    AgentToolDeclaration, AgentToolDescriptor, AgentToolId, AgentToolKind, AgentToolRegistry,
    InMemoryAgentTaskHistoryStore, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    WorkflowAgentRunEffectSink,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId,
    AgentTimestampMillis,
};
use rakka_core::{ActorSystem, InMemoryMetricsRecorder};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_sharding::ClusterSharding;

/// The single tool of the walk: non-idempotent, checkpoint-required.
pub const TOOL: &str = "charge-card";
/// The dispatcher fleet's lease, expired deliberately after a worker death.
pub const LEASE_MS: u64 = 60_000;
/// Every sharded ask's timeout.
pub const ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// The concrete A2A service over this world's stores.
pub type Service = RakkaAgentA2AService<
    InMemoryDurableStateStore<AgentTaskState>,
    InMemoryDurableStateStore<AgentEntityState>,
    InMemoryAgentTaskHistoryStore,
    CrashingStateStore<AgentRunState>,
    InMemoryDurableStateStore<rakka_agent::AgentTeamState>,
    rakka_agent::InMemoryAgentTeamHistoryStore,
    InMemoryDurableStateStore<rakka_agent::AgentConversationState>,
    rakka_agent::InMemoryAgentConversationHistoryStore,
>;

/// The production dispatch pipeline over this world's stores.
pub type Pipeline = rakka_agent::AgentRunEffectDispatcher<
    InMemoryDurableStateStore<WorkflowState>,
    InMemoryDurableStateStore<AgentDispatcherFleetState>,
    CrashingStateStore<AgentRunState>,
    SharedAtomicWorkflowClock,
>;

/// A decision sink that is always down — the unavailable telemetry backend
/// of the statement's last bullet.
#[derive(Debug)]
pub struct UnavailableDecisionSink;

impl AgentDecisionEventSink for UnavailableDecisionSink {
    fn backend_name(&self) -> &'static str {
        "unavailable"
    }

    fn append<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _event: &'a AgentDecisionEvent,
    ) -> AgentObservabilityFuture<'a, AgentDecisionWriteStatus> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }

    fn read<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _after: u64,
        _limit: usize,
    ) -> AgentObservabilityFuture<'a, AgentDecisionEventPage> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }
}

/// A deterministic service clock over the shared tick counter.
struct TickClock(Arc<AtomicU64>);

impl A2AAgentClock for TickClock {
    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

/// A schema reference at its initial revision.
pub fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("the schema id is valid"),
        AgentRevisionNumber::INITIAL,
    )
}

/// The registry binding the walk's one tool: non-idempotent, one attempt,
/// gated behind an effect-bound approval checkpoint.
pub fn tool_registry() -> AgentToolRegistry {
    let descriptor = AgentToolDescriptor::new(
        AgentToolId::new(TOOL).expect("the tool id is valid"),
        AgentToolKind::Function,
        "Charges the customer's card.",
        schema("charge-input"),
        schema("charge-output"),
    )
    .expect("the descriptor is valid");
    let declaration = AgentToolDeclaration::new(rakka_agent::AgentEffectSafetyClass::NonIdempotent);
    let spec = AgentEffectSpec::non_idempotent();
    AgentToolRegistry::new()
        .register(
            AgentToolBinding::new(descriptor, declaration, spec.max_attempts)
                .with_checkpoint_required(),
        )
        .expect("the tool registers")
}

/// The whole in-process world the acceptance walk drives.
pub struct World {
    /// The actor system hosting the sharded entities.
    pub system: ActorSystem,
    /// The single-node sharding facade.
    pub sharding: ClusterSharding,
    /// Durable task records.
    pub tasks: InMemoryDurableStateStore<AgentTaskState>,
    /// Durable agent records.
    pub agents: InMemoryDurableStateStore<AgentEntityState>,
    /// Durable run records; crash-armable for the owner-loss bullet.
    pub runs: CrashingStateStore<AgentRunState>,
    /// Append-only task history.
    pub history: InMemoryAgentTaskHistoryStore,
    /// The workflow outbox the run's effects ticket through.
    pub workflow_store: InMemoryDurableStateStore<WorkflowState>,
    /// The dispatcher fleet's lease records.
    pub fleet_store: InMemoryDurableStateStore<AgentDispatcherFleetState>,
    /// The workflow clock, advanced deliberately to expire leases.
    pub wf_clock: SharedAtomicWorkflowClock,
    /// The shared tick counter behind every timestamp.
    pub clock: Arc<AtomicU64>,
    /// The deterministic model adapter the dispatcher invokes.
    pub adapter: DeterministicModelAdapter,
    /// The recording tool executor — the external system.
    pub tools: RecordingToolExecutor,
    /// The dispatcher kill switch.
    pub probe: KillSwitchProbe,
    /// The deployment's tool registry.
    pub registry: AgentToolRegistry,
    /// The bounded metrics recorder wired into the sharded runs.
    pub metrics: Arc<InMemoryMetricsRecorder>,
    /// The session-memory backend wired into the sharded runs.
    pub session: Arc<InMemorySessionMemoryStore>,
    /// The immutable context-snapshot store wired into the sharded runs.
    pub snapshots: Arc<InMemoryContextSnapshotStore>,
    /// The exchange router: every cross-entity exchange goes through the
    /// sharded entities' own durable accept path.
    pub router: AgentExchangeRouter,
    /// The in-process A2A service core.
    pub service: Arc<Service>,
    /// The agent entity type registration.
    pub agent_registration: AgentEntityRegistration,
    /// The task entity type registration.
    pub task_registration: AgentTaskEntityRegistration,
    /// The run entity type registration.
    pub run_registration: AgentRunEntityRegistration,
}

impl World {
    /// Builds the world: stores, sharded entity types, router, service.
    ///
    /// `adapter` scripts the model; the walk passes one scripted for its
    /// two-turn flow.
    pub fn new(adapter: DeterministicModelAdapter, catalog_target: A2AAgentTarget) -> Self {
        let system = ActorSystem::new("DurableAgentAcceptance");
        let sharding = ClusterSharding::get(&system);
        let tasks = InMemoryDurableStateStore::<AgentTaskState>::new();
        let agents = InMemoryDurableStateStore::<AgentEntityState>::new();
        let runs = CrashingStateStore::<AgentRunState>::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let workflow_store = InMemoryDurableStateStore::<WorkflowState>::new();
        let fleet_store = InMemoryDurableStateStore::<AgentDispatcherFleetState>::new();
        let clock = Arc::new(AtomicU64::new(1));
        let wf_clock = SharedAtomicWorkflowClock::new(clock.clone());
        let tools = RecordingToolExecutor::new().with_result(
            TOOL,
            rakka_agent::AgentTaskContent::inline(serde_json::json!({ "charged": true }))
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
        let run_registration = init_agent_run_entity_sharding(
            &sharding,
            runs.clone(),
            sink,
            deferred.as_router(),
            AgentRunEntityShardingSettings::new(agent_run_entity_type_key())
                .with_clock(entity_clock)
                .with_effect_policies(policies)
                .with_metrics(metrics.clone())
                .with_decision_events(Arc::new(UnavailableDecisionSink))
                .with_memory(AgentRunMemory::new(session.clone(), snapshots.clone())),
        )
        .expect("run entity sharding initializes");

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
            );
        deferred.install(router.clone());

        let service = Arc::new(
            Service::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                runs.clone(),
                InMemoryDurableStateStore::default(),
                rakka_agent::InMemoryAgentTeamHistoryStore::new(),
                InMemoryDurableStateStore::<rakka_agent::AgentConversationState>::default(),
                rakka_agent::InMemoryAgentConversationHistoryStore::new(),
                router.clone(),
                Arc::new(A2AStaticAgentCatalog::single(catalog_target)),
                Arc::new(InMemoryA2ATaskProjectionStore::local()),
                Arc::new(A2AHeaderTenantResolver),
                Arc::new(AllowAllAuthorizer),
            )
            .with_clock(Arc::new(TickClock(clock.clone())))
            .with_default_tenant("acme"),
        );

        Self {
            system,
            sharding,
            tasks,
            agents,
            runs,
            history,
            workflow_store,
            fleet_store,
            wf_clock,
            clock,
            adapter,
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
        }
    }

    /// A fresh production dispatch worker over the shared durable stores —
    /// building one anew is exactly what recovery after a dispatcher death
    /// looks like.
    pub fn pipeline(&self) -> Pipeline {
        rakka_agent::AgentRunEffectDispatcher::new(
            AgentDispatcherWorkerId::new("worker-1"),
            self.workflow_store.clone(),
            self.fleet_store.clone(),
            self.runs.clone(),
            self.wf_clock.clone(),
            Arc::new(self.adapter.clone()),
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
                .with_metrics(self.metrics.clone()),
            ),
        )
        .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
        .with_probe(Arc::new(self.probe.clone()))
        .with_reconciler(Arc::new(ScriptedReconciler::new()))
    }

    /// Advances the shared clock past the fleet lease.
    pub fn expire_lease(&self) {
        self.wf_clock.advance(LEASE_MS + 1);
    }
}
