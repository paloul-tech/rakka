//! The sharded world the export walk drives: all three real entity types on
//! one node, the in-process A2A service core, and the production effect
//! dispatcher — with one segment sink threaded through every driver of a run.
//!
//! **Four wirings, not one.** A run is advanced by the entity a caller drives,
//! by the dispatch pipeline, by `InProcessRunResultDelivery` (delivering a
//! durable result commits loop transitions, which is where `decide` and
//! `checkpoint-open` segments close), and — for the ingress `SERVER` span —
//! by the A2A service. A sink wired to fewer than four sees a partial trace,
//! and the walk fails rather than reporting a smaller number: dropping the
//! delivery's sink alone loses every segment that driver closes — the
//! effect-result folds and the reconciliation-side checkpoint segments — and
//! the 39-span, 12-name transcript no longer matches. The metrics recorder follows the same rule and was
//! missing from that driver entirely — see `InProcessRunResultDelivery`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_a2a::agents::{
    A2AAgentClock, A2AAgentTarget, A2AStaticAgentCatalog, RakkaAgentA2AService,
};
use rakka_a2a::auth::AllowAllAuthorizer;
use rakka_a2a::mapping::A2AHeaderTenantResolver;
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::otel::AgentGenAiSpanExporter;
use rakka_agent::testkit::{
    DeferredExchangeRouter, DeterministicModelAdapter, InProcessRunResultDelivery, KillSwitchProbe,
    LocalShardedExchangeRoute, RecordingToolExecutor, ScriptedReconciler,
    SharedAtomicWorkflowClock,
};
use rakka_agent::AgentRevisionNumber;
use rakka_agent::{
    agent_entity_type_key, agent_run_entity_type_key, agent_task_entity_type_key,
    init_agent_entity_sharding, init_agent_run_entity_sharding, init_agent_task_entity_sharding,
    AgentEffectSpec, AgentEntityAuthority, AgentEntityClass, AgentEntityRegistration,
    AgentEntityShardingSettings, AgentEntityState, AgentExchangeRouter, AgentRunEntityMessage,
    AgentRunEntityRegistration, AgentRunEntityShardingSettings, AgentRunMemory, AgentRunState,
    AgentSchemaId, AgentSchemaRef, AgentTaskEntityMessage, AgentTaskEntityRegistration,
    AgentTaskEntityShardingSettings, AgentTaskState, AgentToolAuthority, AgentToolBinding,
    AgentToolDeclaration, AgentToolDescriptor, AgentToolId, AgentToolKind, AgentToolRegistry,
    InMemoryAgentDecisionEventSink, InMemoryAgentTaskHistoryStore, InMemoryContextSnapshotStore,
    InMemorySessionMemoryStore, WorkflowAgentRunEffectSink,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId,
    AgentTimestampMillis,
};
use rakka_core::{ActorSystem, InMemoryMetricsRecorder};
use rakka_persistence::InMemoryDurableStateStore;

use crate::sdk::{ExemplarReservoir, ExemplarSegmentSink};
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
    InMemoryDurableStateStore<AgentRunState>,
    InMemoryDurableStateStore<rakka_agent::AgentTeamState>,
    rakka_agent::InMemoryAgentTeamHistoryStore,
    InMemoryDurableStateStore<rakka_agent::AgentConversationState>,
    rakka_agent::InMemoryAgentConversationHistoryStore,
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
    /// Durable run records.
    pub runs: InMemoryDurableStateStore<AgentRunState>,
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
    /// The bounded ring every closed segment maps into.
    pub spans: Arc<AgentGenAiSpanExporter>,
    /// The application-owned exemplar reservoir, fed by the same segments.
    pub exemplars: Arc<ExemplarReservoir>,
    /// The one sink all four drivers of a run share.
    pub segments: Arc<ExemplarSegmentSink>,
    /// The durable decision sink, so `decide` spans carry their events.
    pub decisions: Arc<InMemoryAgentDecisionEventSink>,
}

impl World {
    /// Builds the world: stores, sharded entity types, router, service.
    ///
    /// `adapter` scripts the model; the walk passes one scripted for its
    /// two-turn flow.
    pub fn new(adapter: DeterministicModelAdapter, catalog_target: A2AAgentTarget) -> Self {
        let system = ActorSystem::new("AgentOtlpExportAcceptance");
        let sharding = ClusterSharding::get(&system);
        let tasks = InMemoryDurableStateStore::<AgentTaskState>::new();
        let agents = InMemoryDurableStateStore::<AgentEntityState>::new();
        let runs = InMemoryDurableStateStore::<AgentRunState>::new();
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
        let decisions = Arc::new(InMemoryAgentDecisionEventSink::new());
        // The exporter publishes its own queue depth and loss into the same
        // recorder the run entity records into, so the walk's loss assertions
        // read one snapshot rather than two surfaces.
        let spans = Arc::new(AgentGenAiSpanExporter::new().with_metrics(metrics.clone()));
        let exemplars = Arc::new(ExemplarReservoir::new());
        let segments = Arc::new(ExemplarSegmentSink::new(spans.clone(), exemplars.clone()));

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
                .with_clock(entity_clock.clone())
                // The task entity is the sole recording site for the wake,
                // epoch, human-result, dependency, stagnation, goal-status and
                // goal-lifecycle counters. Without this they are recorded into
                // the default `NoopMetricsRecorder` and cannot reach the
                // snapshot, the bridge export, or the receiver — in the one
                // walk built to prove the metric surface exports.
                .with_metrics(metrics.clone()),
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
                .with_segments(segments.clone())
                .with_decision_events(decisions.clone())
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
            .with_segments(segments.clone())
            .with_metrics(metrics.clone())
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
            spans,
            exemplars,
            segments,
            decisions,
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
                .with_segments(self.segments.clone())
                .with_metrics(self.metrics.clone())
                .with_decision_events(self.decisions.clone()),
            ),
        )
        .with_segments(self.segments.clone())
        .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
        .with_probe(Arc::new(self.probe.clone()))
        .with_reconciler(Arc::new(ScriptedReconciler::new()))
    }

    /// Advances the shared clock past the fleet lease.
    pub fn expire_lease(&self) {
        self.wf_clock.advance(LEASE_MS + 1);
    }
}
