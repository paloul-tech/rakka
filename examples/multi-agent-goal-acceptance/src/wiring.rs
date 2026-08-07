//! The sharded world: all three real entity types for three agents on one
//! node, the in-process A2A service core the delegation sends travel, the
//! communal knowledge graph, the compiled workflow child's durable inbox,
//! and the production effect dispatcher.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_a2a::agents::{
    A2AAgentClock, A2AAgentDelegationSendExecutor, A2AAgentTarget, A2AStaticAgentCatalog,
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
    agent_entity_type_key, agent_run_entity_type_key, agent_task_entity_type_key,
    init_agent_entity_sharding, init_agent_run_entity_sharding, init_agent_task_entity_sharding,
    workflow_start_command, AgentCapabilityId, AgentDispatchError, AgentDispatchFuture,
    AgentEffectSpec, AgentEntityAuthority, AgentEntityClass, AgentEntityRegistration,
    AgentEntityShardingSettings, AgentEntityState, AgentExchangeRouter,
    AgentGoalEvaluationExecutor, AgentGoalEvaluationFinding, AgentGoalEvaluationOutcome,
    AgentGoalEvaluationRequest, AgentRevisionNumber, AgentRunDelegationConfig, AgentRunEffect,
    AgentRunEntityMessage, AgentRunEntityRegistration, AgentRunEntityShardingSettings,
    AgentRunMemory, AgentRunScope, AgentRunState, AgentRunWorkflowConfig, AgentSchemaId,
    AgentSchemaRef, AgentTaskDefinitionId, AgentTaskEntityMessage, AgentTaskEntityRegistration,
    AgentTaskEntityShardingSettings, AgentTaskState, AgentToolAuthority, AgentToolBinding,
    AgentToolDeclaration, AgentToolDescriptor, AgentToolId, AgentToolKind, AgentToolRegistry,
    AgentWorkflowInvocationRecord, AgentWorkflowStartExecutor, AgentWorkflowStartFinding,
    AgentWorkflowToolDescriptor, InMemoryAgentTaskHistoryStore, InMemoryContextSnapshotStore,
    InMemorySessionMemoryStore, KnowledgeSpaceId, StaticAgentDelegationCatalog,
    WorkflowAgentRunEffectSink,
};
use rakka_agent_knowledge_graph::{InMemoryKnowledgeGraphStore, KnowledgeGraphClaimAppendExecutor};
use rakka_agent_workflow::substrate::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId,
    AgentEphemeralCredential, AgentRunInbox, AgentTimestampMillis, AgentWorkflowId,
    AgentWorkflowRegistry, WorkflowDefinitionVersion,
};
use rakka_core::{ActorSystem, InMemoryMetricsRecorder};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_sharding::ClusterSharding;

/// The tenant of the whole walk.
pub const TENANT: &str = "acme";
/// The coordinating agent.
pub const COORDINATOR: &str = "mission-coordinator";
/// The first specialist.
pub const TRANSLATOR: &str = "translator";
/// The second specialist.
pub const SUMMARIZER: &str = "summarizer";
/// The first specialist skill.
pub const SKILL_TRANSLATION: &str = "translation";
/// The second specialist skill.
pub const SKILL_SUMMARIZATION: &str = "summarization";
/// The coordination tool the coordinator's runs declare.
pub const DELEGATE_TOOL: &str = "delegate";
/// The await verb closing the fan-out group.
pub const AWAIT_TOOL: &str = "await_children";
/// The workflow tool of the walk.
pub const WORKFLOW_TOOL: &str = "refund-flow";
/// The workflow type its descriptor pins.
pub const WORKFLOW_TYPE: &str = "refund";
/// The workflow definition version its descriptor pins.
pub const WORKFLOW_VERSION: &str = "v1";
/// The communal knowledge space the goal grants.
pub const SPACE: &str = "mission-findings";
/// The summarizer's non-idempotent external tool.
pub const TOOL: &str = "submit-payment";
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
>;

/// The production dispatch pipeline over this world's stores.
pub type Pipeline = rakka_agent::AgentRunEffectDispatcher<
    InMemoryDurableStateStore<WorkflowState>,
    InMemoryDurableStateStore<AgentDispatcherFleetState>,
    CrashingStateStore<AgentRunState>,
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

/// The registry binding the summarizer's one external tool: non-idempotent,
/// so a worker death after its invocation is an ambiguity only a
/// reconciliation decision resolves.
pub fn tool_registry() -> AgentToolRegistry {
    let descriptor = AgentToolDescriptor::new(
        AgentToolId::new(TOOL).expect("the tool id is valid"),
        AgentToolKind::Function,
        "Submits the mission's payment.",
        schema("payment-input"),
        schema("payment-output"),
    )
    .expect("the descriptor is valid");
    let declaration = AgentToolDeclaration::new(rakka_agent::AgentEffectSafetyClass::NonIdempotent);
    let spec = AgentEffectSpec::non_idempotent();
    AgentToolRegistry::new()
        .register(AgentToolBinding::new(
            descriptor,
            declaration,
            spec.max_attempts,
        ))
        .expect("the tool registers")
}

/// The delegation wiring every hosted run serves: the declared coordination
/// tool, the static catalog resolving both specialist skills, the await
/// verb, and the shared knowledge space explicitly delegated to the
/// translator.
pub fn delegation_config() -> AgentRunDelegationConfig {
    AgentRunDelegationConfig::new(
        AgentToolId::new(DELEGATE_TOOL).expect("the tool id is valid"),
        Arc::new(
            StaticAgentDelegationCatalog::new()
                .with_target(
                    AgentCapabilityId::new(SKILL_TRANSLATION).expect("the skill id is valid"),
                    rakka_agent::AgentDelegationTarget::new(
                        rakka_agent::AgentId::new(TRANSLATOR).expect("the agent id is valid"),
                        AgentTaskDefinitionId::new("translate-document")
                            .expect("the definition id is valid"),
                    )
                    .with_knowledge_space(
                        KnowledgeSpaceId::new(SPACE).expect("the space id is valid"),
                    ),
                )
                .with_target(
                    AgentCapabilityId::new(SKILL_SUMMARIZATION).expect("the skill id is valid"),
                    rakka_agent::AgentDelegationTarget::new(
                        rakka_agent::AgentId::new(SUMMARIZER).expect("the agent id is valid"),
                        AgentTaskDefinitionId::new("summarize-document")
                            .expect("the definition id is valid"),
                    ),
                ),
        ),
        std::collections::BTreeSet::from([
            rakka_agent::AgentCoordinationCapabilityKind::Delegation,
        ]),
    )
    .expect("the delegation configuration declares the capability")
    .with_fan_in_tool(AgentToolId::new(AWAIT_TOOL).expect("the tool id is valid"))
}

/// The versioned descriptor under which the compiled refund workflow appears
/// in the coordinator's toolset.
pub fn workflow_tool_descriptor() -> AgentWorkflowToolDescriptor {
    AgentWorkflowToolDescriptor::new(
        rakka_agent::AgentWorkflowToolId::new(WORKFLOW_TOOL).expect("the tool id is valid"),
        WORKFLOW_TYPE,
        WorkflowDefinitionVersion::new(WORKFLOW_VERSION),
        "Runs the compiled refund workflow.",
        schema("refund-input"),
        schema("refund-output"),
    )
    .expect("the workflow-tool descriptor is valid")
    .with_capability(AgentCapabilityId::new("issue-refunds").expect("the capability id is valid"))
    .expect("the descriptor accepts the capability")
}

/// The workflow-tool wiring every hosted run serves.
pub fn workflow_config() -> AgentRunWorkflowConfig {
    AgentRunWorkflowConfig::new()
        .with_descriptor(workflow_tool_descriptor())
        .expect("the workflow configuration accepts the descriptor")
}

/// The registered compiled refund workflow the start executor validates
/// pinned coordinates against.
fn refund_workflow() -> rakka_agent_workflow::AgentWorkflow {
    rakka_agent_workflow::AgentWorkflow {
        workflow_id: AgentWorkflowId::new("wf-refund"),
        workflow_type: WORKFLOW_TYPE.to_string(),
        definition_version: WorkflowDefinitionVersion::new(WORKFLOW_VERSION),
        state_schema_version: rakka_agent_workflow::StateSchemaVersion::new(1),
        display_name: Some("Compiled refund workflow".to_string()),
        status_labels: vec![
            rakka_agent_workflow::AgentRunStatus::Accepted
                .as_label()
                .to_string(),
            rakka_agent_workflow::AgentRunStatus::Completed
                .as_label()
                .to_string(),
        ],
        command_types: vec![rakka_agent_workflow::AgentCommandKind::StartRun
            .type_name()
            .to_string()],
        steps: vec![rakka_agent_workflow::AgentStep {
            step_id: rakka_agent_workflow::AgentStepId::new("refund"),
            kind: rakka_agent_workflow::AgentStepKind::Planner,
            display_name: Some("Issue the refund deterministically".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: Default::default(),
        }],
        payload_types: Vec::new(),
        retry_policy_ref: None,
        approval_policy_ref: None,
        timeout_policy_ref: None,
        observability_labels: Default::default(),
    }
}

/// The registry-validated, inbox-backed workflow start executor: the
/// application-owed bridge from the parent's `WorkflowStart` effect to the
/// child workflow run's durable inbox.
///
/// The record's pinned workflow type and definition version must resolve in
/// the registry — an unknown pin is a definitive refusal — and the accepted
/// command is the derived `StartRun` built verbatim from the record, so the
/// child's inbox deduplication is what makes a replayed start an adoption.
pub struct RegistryInboxStartExecutor {
    registry: AgentWorkflowRegistry,
    store: InMemoryDurableStateStore<WorkflowState>,
    clock: AtomicU64,
    seen: Mutex<Vec<String>>,
}

impl RegistryInboxStartExecutor {
    fn new(store: InMemoryDurableStateStore<WorkflowState>) -> Self {
        let mut registry = AgentWorkflowRegistry::new();
        registry
            .register(refund_workflow())
            .expect("the refund workflow registers");
        Self {
            registry,
            store,
            clock: AtomicU64::new(1),
            seen: Mutex::new(Vec::new()),
        }
    }

    /// Every invocation id the executor was ever driven with.
    pub fn seen(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("the sighting log is not poisoned")
            .clone()
    }
}

impl AgentWorkflowStartExecutor for RegistryInboxStartExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        invocation: &'a AgentWorkflowInvocationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentWorkflowStartFinding> {
        self.seen
            .lock()
            .expect("the sighting log is not poisoned")
            .push(invocation.invocation.as_str().to_string());
        Box::pin(async move {
            let Some(workflow) = self
                .registry
                .get(&invocation.workflow_type, &invocation.definition_version)
            else {
                return Ok(AgentWorkflowStartFinding::Refused {
                    code: "workflow-registry-unknown".to_string(),
                    message: format!(
                        "no compiled workflow serves {} {}",
                        invocation.workflow_type, invocation.definition_version
                    ),
                });
            };
            let command = workflow_start_command(
                invocation,
                workflow.workflow_id.clone(),
                None,
                AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst)),
            )
            .map_err(|error| AgentDispatchError::Invocation {
                code: "workflow-start-command-invalid",
                message: error.to_string(),
            })?;
            let mut inbox = AgentRunInbox::with_clock(
                invocation.child_run.clone(),
                self.store.clone(),
                ManualWorkflowClock::new(WorkflowTimestamp::from_millis(
                    self.clock.fetch_add(1, Ordering::SeqCst),
                )),
            );
            inbox
                .recover()
                .await
                .map_err(|error| AgentDispatchError::Invocation {
                    code: "workflow-start-inbox-unavailable",
                    message: error.to_string(),
                })?;
            let acceptance = inbox.accept_command(command).await.map_err(|error| {
                AgentDispatchError::Invocation {
                    code: "workflow-start-inbox-refused",
                    message: error.to_string(),
                }
            })?;
            Ok(if acceptance.is_accepted() {
                AgentWorkflowStartFinding::Started
            } else {
                AgentWorkflowStartFinding::Adopted
            })
        })
    }
}

/// The configured completion evaluator: refuses unless every required
/// evidence class is present, and judges `Satisfied` from the durable
/// evidence otherwise. Application-owned, deterministic, and the only door
/// through which this goal may become `Satisfied`.
pub struct AcceptanceEvaluator;

impl AgentGoalEvaluationExecutor for AcceptanceEvaluator {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        evaluation: &'a AgentGoalEvaluationRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
        _now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentGoalEvaluationFinding> {
        Box::pin(async move {
            let presented: std::collections::BTreeSet<&str> = evaluation
                .evidence
                .iter()
                .map(|item| item.class.as_str())
                .collect();
            if !presented.contains("artifact") {
                return Ok(AgentGoalEvaluationFinding::Refused {
                    code: "evidence-missing".to_string(),
                    message: "the required artifact evidence class is absent".to_string(),
                });
            }
            Ok(AgentGoalEvaluationFinding::Evaluated {
                outcome: AgentGoalEvaluationOutcome::Satisfied,
                reason_code: "criteria-verified".to_string(),
                evidence: evaluation.evidence.clone(),
                evaluated_by: None,
            })
        })
    }
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
    /// Durable run records; crash-armable for the root-loss bullet.
    pub runs: CrashingStateStore<AgentRunState>,
    /// Append-only task history.
    pub history: InMemoryAgentTaskHistoryStore,
    /// The workflow outbox the runs' effects ticket through.
    pub workflow_store: InMemoryDurableStateStore<WorkflowState>,
    /// The dispatcher fleet's lease records.
    pub fleet_store: InMemoryDurableStateStore<AgentDispatcherFleetState>,
    /// The child workflow run's own durable inbox store — the identity and
    /// deduplication surface the workflow bullets rest on.
    pub child_inbox_store: InMemoryDurableStateStore<WorkflowState>,
    /// The communal knowledge graph.
    pub graph: Arc<InMemoryKnowledgeGraphStore>,
    /// The workflow clock, advanced deliberately to expire leases.
    pub wf_clock: SharedAtomicWorkflowClock,
    /// The shared tick counter behind every timestamp.
    pub clock: Arc<AtomicU64>,
    /// The recording tool executor — the summarizer's external system.
    pub tools: RecordingToolExecutor,
    /// The dispatcher kill switch.
    pub probe: KillSwitchProbe,
    /// The deployment's tool registry.
    pub registry: AgentToolRegistry,
    /// The bounded metrics recorder wired into the sharded runs.
    pub metrics: Arc<InMemoryMetricsRecorder>,
    /// The session-memory backend wired into the sharded runs — the private
    /// memory that must never leak into communal surfaces.
    pub session: Arc<InMemorySessionMemoryStore>,
    /// The immutable context-snapshot store wired into the sharded runs.
    pub snapshots: Arc<InMemoryContextSnapshotStore>,
    /// The exchange router: every cross-entity exchange goes through the
    /// sharded entities' own durable accept path.
    pub router: AgentExchangeRouter,
    /// The in-process A2A service core the delegation sends travel.
    pub service: Arc<Service>,
    /// The registry-validated workflow start executor.
    pub workflow_starts: Arc<RegistryInboxStartExecutor>,
    /// How many times the compiled refund step executed.
    pub refund_step_executions: Arc<AtomicU64>,
    /// Which inbox message ids the refund step already ran for — the
    /// application-side once-guard keyed on the durable command identity.
    pub refund_steps_done: Arc<Mutex<std::collections::BTreeSet<String>>>,
    /// The agent entity type registration.
    pub agent_registration: AgentEntityRegistration,
    /// The task entity type registration.
    pub task_registration: AgentTaskEntityRegistration,
    /// The run entity type registration.
    pub run_registration: AgentRunEntityRegistration,
}

impl World {
    /// Builds the world: stores, sharded entity types, router, service,
    /// graph, and the workflow child's inbox.
    #[must_use]
    pub fn new() -> Self {
        let system = ActorSystem::new("MultiAgentGoalAcceptance");
        let sharding = ClusterSharding::get(&system);
        let tasks = InMemoryDurableStateStore::<AgentTaskState>::new();
        let agents = InMemoryDurableStateStore::<AgentEntityState>::new();
        let runs = CrashingStateStore::<AgentRunState>::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let workflow_store = InMemoryDurableStateStore::<WorkflowState>::new();
        let fleet_store = InMemoryDurableStateStore::<AgentDispatcherFleetState>::new();
        let child_inbox_store = InMemoryDurableStateStore::<WorkflowState>::new();
        let graph = Arc::new(InMemoryKnowledgeGraphStore::new());
        let clock = Arc::new(AtomicU64::new(1));
        let wf_clock = SharedAtomicWorkflowClock::new(clock.clone());
        let tools = RecordingToolExecutor::new().with_result(
            TOOL,
            rakka_agent::AgentTaskContent::inline(serde_json::json!({ "paid": true }))
                .expect("the tool result is inline-bounded"),
        );
        let registry = tool_registry();
        let metrics = Arc::new(InMemoryMetricsRecorder::new());
        let session = Arc::new(InMemorySessionMemoryStore::new());
        let snapshots = Arc::new(InMemoryContextSnapshotStore::new());
        let workflow_starts = Arc::new(RegistryInboxStartExecutor::new(child_inbox_store.clone()));

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
        // The sharded factory is the production driver of every run —
        // coordinator and specialists alike — so the delegation and
        // workflow-tool interception is wired here, on the one path every
        // driver shares.
        let run_registration = init_agent_run_entity_sharding(
            &sharding,
            runs.clone(),
            sink,
            deferred.as_router(),
            AgentRunEntityShardingSettings::new(agent_run_entity_type_key())
                .with_clock(entity_clock)
                .with_effect_policies(policies)
                .with_metrics(metrics.clone())
                .with_memory(AgentRunMemory::new(session.clone(), snapshots.clone()))
                .with_delegation(delegation_config())
                .with_workflow_tools(workflow_config()),
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
                router.clone(),
                Arc::new(
                    A2AStaticAgentCatalog::new()
                        .with_target(A2AAgentTarget::new(
                            rakka_agent::AgentId::new(TRANSLATOR).expect("the agent id is valid"),
                            crate::flow::translate_definition(),
                        ))
                        .with_target(A2AAgentTarget::new(
                            rakka_agent::AgentId::new(SUMMARIZER).expect("the agent id is valid"),
                            crate::flow::summarize_definition(),
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
            history,
            workflow_store,
            fleet_store,
            child_inbox_store,
            graph,
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
            workflow_starts,
            refund_step_executions: Arc::new(AtomicU64::new(0)),
            refund_steps_done: Arc::new(Mutex::new(std::collections::BTreeSet::new())),
            agent_registration,
            task_registration,
            run_registration,
        }
    }

    /// A fresh production dispatch worker over the shared durable stores,
    /// scripted with `adapter` — building one anew is exactly what recovery
    /// after a dispatcher death looks like, and each agent's runs pump under
    /// that agent's own scripted model.
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
                .with_delegation(delegation_config())
                .with_workflow_tools(workflow_config()),
            ),
        )
        .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
        .with_probe(Arc::new(self.probe.clone()))
        .with_reconciler(Arc::new(ScriptedReconciler::new()))
        .with_a2a_send_executor(Arc::new(A2AAgentDelegationSendExecutor::new(
            self.service.clone(),
        )))
        .with_workflow_start_executor(self.workflow_starts.clone())
        .with_goal_evaluation_executor(Arc::new(AcceptanceEvaluator))
        .with_claim_append_executor(Arc::new(KnowledgeGraphClaimAppendExecutor::new(
            self.graph.clone(),
        )))
    }

    /// Runs the compiled refund step for one accepted `StartRun`, exactly
    /// once per durable command identity: the once-guard is keyed on the
    /// inbox entry's message id, so a replayed — adopted — start can never
    /// re-execute the step.
    pub async fn run_refund_step(&self, child_run: &rakka_agent_workflow::AgentRunId) -> usize {
        let mut inbox = AgentRunInbox::with_clock(
            child_run.clone(),
            self.child_inbox_store.clone(),
            ManualWorkflowClock::new(WorkflowTimestamp::from_millis(
                self.clock.fetch_add(1, Ordering::SeqCst),
            )),
        );
        inbox.recover().await.expect("the child inbox recovers");
        let pending = inbox
            .inner()
            .recoverable_inbox()
            .expect("the inbox enumerates");
        for entry in &pending {
            let message_id = entry.message_id().to_string();
            let mut done = self
                .refund_steps_done
                .lock()
                .expect("the step guard is not poisoned");
            if done.insert(message_id) {
                self.refund_step_executions.fetch_add(1, Ordering::SeqCst);
            }
        }
        pending.len()
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
