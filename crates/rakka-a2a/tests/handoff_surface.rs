//! Durable handoff across the A2A collaboration surface, end to end.
//!
//! Scenario 38 of specification 18, over real entities: a source run's model
//! turn requests a transfer through the declared handoff tool, the loop
//! persists the handoff record with its send effect, the in-process executor
//! carries it — with the versioned handoff cluster, naming the *same*
//! `message.task_id` — through [`RakkaAgentA2AService`] exactly as an
//! external A2A caller would, and the same task gains a new accepted
//! generation under the target agent while the source terminalizes
//! `HandedOff`. The wire-level half proves the ingress: a replayed handoff
//! send converges on the recorded transfer, a forged source claim fails
//! closed against durable task state, and a half-formed handoff cluster is
//! refused rather than half-understood (specification 8.9 and 14.4).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest};
use serde_json::{json, Value};

use rakka_a2a::agents::{
    A2AAgentHandoffSendExecutor, A2AAgentTarget, A2AStaticAgentCatalog, RakkaAgentA2AError,
    RakkaAgentA2AService, AGENT_COLLABORATION_EXTENSION_URI, AGENT_COLLABORATION_SCHEMA_VERSION,
    META_AGENT_ID, META_COLLABORATION, META_TASK_DEFINITION,
};
use rakka_a2a::auth::{
    A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer, A2AOperation,
    AllowAllAuthorizer,
};
use rakka_a2a::mapping::{A2AHeaderTenantResolver, META_DEDUPLICATION_KEY};
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    run_entity, CrashPoint, CrashingStateStore, DeferredExchangeRouter, DeterministicModelAdapter,
    InProcessRunEntityTransport, InProcessTaskEntityTransport, ScriptedDispatcher,
};
use rakka_agent::InMemoryAgentTeamHistoryStore;
use rakka_agent::{
    handoff_id_for, run_id_for_assignment, AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor,
    AgentAssignmentGeneration, AgentAssignmentStatus, AgentAuthorityEnvelope, AgentCapabilityId,
    AgentCoordinationCapabilityKind, AgentDefinition, AgentDefinitionId, AgentDelegationTarget,
    AgentDispatchError, AgentEffectPolicies, AgentEntityClass, AgentEntityCommand,
    AgentEntityState, AgentEntityStore, AgentExchangeRouter, AgentHandoffPolicy,
    AgentHandoffRecord, AgentId, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunDelegationConfig, AgentRunEffect,
    AgentRunEffectRequest, AgentRunScope, AgentRunState, AgentRunStatus, AgentSchemaId,
    AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent, AgentTaskDefinition,
    AgentTaskDefinitionId, AgentTaskEntityStore, AgentTaskHandoffStatus, AgentTaskId,
    AgentTaskLimits, AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope,
    AgentTaskState, AgentToolCallId, AgentToolCallRequest, AgentToolId, InMemoryAgentRunEffectSink,
    InMemoryAgentTaskHistoryStore, StaticAgentDelegationCatalog, TenantId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentEffectId, AgentTimestampMillis};
use rakka_persistence::InMemoryDurableStateStore;

type TaskStore = CrashingStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = CrashingStateStore<AgentRunState>;
type Service = RakkaAgentA2AService<
    TaskStore,
    AgentStore,
    InMemoryAgentTaskHistoryStore,
    RunStore,
    TeamStore,
    InMemoryAgentTeamHistoryStore,
>;
type TeamStore = InMemoryDurableStateStore<rakka_agent::AgentTeamState>;

const TENANT: &str = "acme";
const SOURCE: &str = "support-agent";
const TARGET: &str = "billing-agent";
const TASK_DEFINITION: &str = "resolve-ticket";
const HANDOFF_SKILL: &str = "billing";
const HANDOFF_TOOL: &str = "transfer";
const TASK: &str = "ticket-1";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn source() -> AgentId {
    AgentId::new(SOURCE).expect("agent id should be valid")
}

fn target() -> AgentId {
    AgentId::new(TARGET).expect("agent id should be valid")
}

fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn definition() -> AgentTaskDefinition {
    let mut per_run = rakka_agent::AgentBudgetAllocation::unbounded();
    per_run.set(rakka_agent::AgentBudgetDimension::LoopIterations, Some(3));
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("definition id should be valid"),
        "One typed support ticket.",
        schema("input"),
        schema("result"),
    )
    .expect("task definition should be valid")
    .with_limits(AgentTaskLimits::new().with_max_result_rejections(2))
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(rakka_agent::AgentBudgetCeilings {
        max_loop_iterations: Some(12),
        ..rakka_agent::AgentBudgetCeilings::unbounded()
    })
    .with_run_allocation(per_run)
}

fn handoff_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Transferring the ticket to billing.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                AgentToolId::new(HANDOFF_TOOL).expect("tool id should be valid"),
                json!({ "skill": HANDOFF_SKILL, "reason": "needs billing authority" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

struct TestClock(Arc<AtomicU64>);

impl rakka_a2a::agents::A2AAgentClock for TestClock {
    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct Fixture {
    tasks: TaskStore,
    agents: AgentStore,
    runs: RunStore,
    history: InMemoryAgentTaskHistoryStore,
    effects: InMemoryAgentRunEffectSink,
    router: AgentExchangeRouter,
    dispatcher: ScriptedDispatcher,
    handoff: AgentRunDelegationConfig,
    clock: Arc<AtomicU64>,
    service: Arc<Service>,
}

impl Fixture {
    fn new(adapter: DeterministicModelAdapter) -> Self {
        Self::with_authorizer(adapter, Arc::new(AllowAllAuthorizer))
    }

    fn with_authorizer(
        adapter: DeterministicModelAdapter,
        authorizer: Arc<dyn A2AAuthorizer>,
    ) -> Self {
        let tasks = TaskStore::new();
        let agents = AgentStore::new();
        let runs = RunStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let effects = InMemoryAgentRunEffectSink::new();
        let clock = Arc::new(AtomicU64::new(1));

        let deferred = DeferredExchangeRouter::new();
        let task_transport = InProcessTaskEntityTransport::new(
            tasks.clone(),
            agents.clone(),
            history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let run_transport = InProcessRunEntityTransport::new(
            runs.clone(),
            effects.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Task, Arc::new(task_transport))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport.clone()));
        deferred.install(router.clone());

        let catalog = A2AStaticAgentCatalog::new()
            .with_target(A2AAgentTarget::new(source(), definition()))
            .with_target(A2AAgentTarget::new(target(), definition()));
        let service = Arc::new(
            Service::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                runs.clone(),
                TeamStore::default(),
                InMemoryAgentTeamHistoryStore::new(),
                router.clone(),
                Arc::new(catalog),
                Arc::new(InMemoryA2ATaskProjectionStore::local()),
                Arc::new(A2AHeaderTenantResolver),
                authorizer,
            )
            .with_clock(Arc::new(TestClock(clock.clone())))
            .with_default_tenant(TENANT),
        );

        // The outbound half: the model requests a transfer skill; the
        // application-owned catalog resolves the target — which must serve
        // the same task definition — and the executor carries the send back
        // through the same service core an external caller uses.
        let handoff = AgentRunDelegationConfig::new(
            AgentToolId::new("delegate").expect("tool id should be valid"),
            Arc::new(
                StaticAgentDelegationCatalog::new().with_target(
                    AgentCapabilityId::new(HANDOFF_SKILL).expect("capability id should be valid"),
                    AgentDelegationTarget::new(
                        target(),
                        AgentTaskDefinitionId::new(TASK_DEFINITION)
                            .expect("definition id should be valid"),
                    ),
                ),
            ),
            std::collections::BTreeSet::from([
                AgentCoordinationCapabilityKind::Delegation,
                AgentCoordinationCapabilityKind::Handoff,
            ]),
        )
        .expect("the delegation configuration declares the capability")
        .with_handoff(AgentHandoffPolicy::new(
            AgentToolId::new(HANDOFF_TOOL).expect("tool id should be valid"),
            AgentRevisionNumber::INITIAL,
        ))
        .expect("the handoff configuration declares the capability");
        run_transport.install_delegation(handoff.clone());

        let dispatcher = ScriptedDispatcher::with_adapter(adapter)
            .with_a2a_handoff_executor(Arc::new(A2AAgentHandoffSendExecutor::new(service.clone())));

        Self {
            tasks,
            agents,
            runs,
            history,
            effects,
            router,
            dispatcher,
            handoff,
            clock,
            service,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    async fn instantiate(&self, agent: &AgentId) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope
            .task_definitions
            .insert(AgentTaskDefinitionId::new(TASK_DEFINITION).expect("definition id"));
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
            "One collaborating agent.",
            envelope,
        )
        .expect("the agent definition should be valid");
        let scope = AgentScope::new(tenant(), agent.clone()).expect("agent scope");
        let mut store = AgentEntityStore::new(scope.clone(), self.agents.clone());
        store.recover().await.expect("the agent should recover");
        store
            .apply(AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &scope,
                    "1",
                )
                .expect("operation id should be derivable"),
                definition: Box::new(definition),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(AgentRevisionProvenance {
                    principal: rakka_agent_workflow::PrincipalRef {
                        principal_type: "user".to_string(),
                        principal_id: "operator-7".to_string(),
                        display_name: None,
                    },
                    accepted_at: AgentTimestampMillis::new(1),
                    causation_id: rakka_agent_workflow::AgentCausationId::new("cause-1"),
                    audit_ref: rakka_agent_workflow::AgentAuditEventId::new("audit-1"),
                }),
            })
            .await
            .expect("the agent should instantiate");
    }

    fn task_scope(&self) -> AgentTaskScope {
        AgentTaskScope::new(
            tenant(),
            AgentTaskId::new(TASK).expect("task id should be valid"),
        )
        .expect("task scope should be valid")
    }

    fn run_scope(&self, agent: &AgentId, generation: u64) -> AgentRunScope {
        let run = run_id_for_assignment(
            self.task_scope().task(),
            AgentAssignmentGeneration::new(generation),
        )
        .expect("the run id should be derivable");
        AgentRunScope::new(tenant(), agent.clone(), run).expect("run scope should be valid")
    }

    /// Creates the task, assigned to the source agent.
    async fn create_task(&self) {
        let scope = self.task_scope();
        let mut task = AgentTaskEntityStore::new(
            scope.clone(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        task.apply(
            rakka_agent::AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, TASK, "1"],
                )
                .expect("operation id should be derivable"),
                creation: Box::new(rakka_agent::AgentTaskCreation {
                    definition: definition(),
                    input: AgentTaskContent::inline(json!({ "ticket": 1 }))
                        .expect("the input is inline-bounded"),
                    assignee: Some(source()),
                    team: None,
                    goal: None,
                    goal_mode: Default::default(),
                    goal_spec: None,
                    parent: None,
                    dependencies: Vec::new(),
                    escrow: None,
                    wake: None,
                    delegation: None,
                    telemetry: Default::default(),
                }),
            },
            &self.router,
            self.now(),
        )
        .await
        .expect("the task should create");
    }

    /// Drives the task and one agent's run for the given generation, fault
    /// tolerantly: the service's own courier may write to the same run
    /// between a driven entity's read and its write, and the loser of that
    /// race recovers from durable state on the next round — exactly as a
    /// sharded owner would.
    async fn pump(&self, agent: &AgentId, generation: u64) {
        for _round in 0..64 {
            let now = self.now();
            let mut task = AgentTaskEntityStore::new(
                self.task_scope(),
                self.tasks.clone(),
                self.agents.clone(),
                self.history.clone(),
            );
            if task.recover(now).await.is_ok() {
                let _ = task.settle_side_effects(&self.router, now).await;
            }

            let now = self.now();
            let mut run = run_entity(
                &self.run_scope(agent, generation),
                &self.runs,
                &self.effects,
            )
            .with_delegation(self.handoff.clone());
            if run.recover(now).await.is_err() {
                continue;
            }
            let progress = run.settle_side_effects(&self.router, now).await;
            let answered = self
                .dispatcher
                .drive(&mut run, &self.router, self.now())
                .await;

            let terminal = run
                .state()
                .ok()
                .and_then(rakka_agent::AgentRunState::status)
                .is_some_and(AgentRunStatus::is_terminal);
            let quiet = matches!(
                (&progress, &answered),
                (Ok(progress), Ok(0))
                    if progress.transitions == 0
                        && progress.effects_dispatched == 0
                        && progress.settled == 0
            );
            if terminal || quiet {
                return;
            }
        }
        panic!("the handoff surface did not converge");
    }
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

/// A handoff send crafted at the wire level, the way a remote Rakka peer's
/// executor would emit it.
fn handoff_message(handoff: &str, source_generation: u64) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "handoff": handoff })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = handoff.to_string();
    message.task_id = Some(TASK.to_string());
    message.extensions = Some(vec![AGENT_COLLABORATION_EXTENSION_URI.to_string()]);
    message.metadata = Some(
        [
            (
                META_DEDUPLICATION_KEY.to_string(),
                Value::String(handoff.to_string()),
            ),
            (META_AGENT_ID.to_string(), Value::String(TARGET.to_string())),
            (
                META_TASK_DEFINITION.to_string(),
                Value::String(TASK_DEFINITION.to_string()),
            ),
            (
                META_COLLABORATION.to_string(),
                json!({
                    "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
                    "handoff": handoff,
                    "source-agent": SOURCE,
                    "source-run": format!("{TASK}-gen-{source_generation}"),
                    "source-generation": source_generation,
                    "target-agent": TARGET,
                    "target-task-definition": TASK_DEFINITION,
                    "reason": "needs billing authority",
                    "policy-revision": 1,
                }),
            ),
        ]
        .into_iter()
        .collect(),
    );
    message
}

fn send_request(message: Message) -> SendMessageRequest {
    SendMessageRequest {
        message,
        configuration: None,
        metadata: None,
        tenant: Some(TENANT.to_string()),
    }
}

/// Scenario 38 end to end: the transfer preserves the `AgentTaskId`, the
/// fenced source terminalizes `HandedOff` only after the target's durable
/// acceptance, exactly one target run exists — and the target completes the
/// very same task.
#[tokio::test]
async fn a_handoff_transfers_the_same_task_across_the_a2a_surface() {
    let fixture = Fixture::new(
        DeterministicModelAdapter::new()
            .with_turn(handoff_turn())
            .with_turn(proposing_turn()),
    );
    fixture.instantiate(&source()).await;
    fixture.instantiate(&target()).await;
    fixture.create_task().await;

    // Bootstrap the public projection before the transfer — the shape of
    // every A2A-created task, whose projection exists from creation. The
    // executor's post-send identity check must find the fresh handoff echo
    // on this *existing* projection: only the metadata half of the
    // projection sync keeps it truthful after bootstrap.
    fixture
        .service
        .get_task(&params(), Some(TENANT), TASK, None)
        .await
        .expect("the projection bootstraps");

    // The source's world: the model turn commits the transfer, the executor
    // carries it through the service, and the courier resolves the source.
    fixture.pump(&source(), 1).await;
    fixture.pump(&source(), 1).await;

    let mut run = run_entity(
        &fixture.run_scope(&source(), 1),
        &fixture.runs,
        &fixture.effects,
    );
    run.recover(fixture.now())
        .await
        .expect("the source recovers");
    let state = run.state().expect("state");
    assert_eq!(state.status(), Some(AgentRunStatus::HandedOff));
    drop(run);

    // The same task, one generation later, accepted under the target.
    let mut task = AgentTaskEntityStore::new(
        fixture.task_scope(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    task.recover(fixture.now())
        .await
        .expect("the task recovers");
    let snapshot = task
        .snapshot()
        .expect("the snapshot reads")
        .expect("the task exists");
    assert_eq!(snapshot.scope.task().as_str(), TASK);
    let assignment = snapshot.assignment.as_ref().expect("the target owns it");
    assert_eq!(assignment.agent, target());
    assert_eq!(assignment.generation, AgentAssignmentGeneration::new(2));
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    let provenance = snapshot.handoff.as_deref().expect("the provenance rides");
    assert_eq!(provenance.status, AgentTaskHandoffStatus::Accepted);
    drop(task);

    // The public task is never terminal mid-handoff, and its projection
    // echoes the transfer for the sender's identity check.
    let public = fixture
        .service
        .get_task(&params(), Some(TENANT), TASK, None)
        .await
        .expect("the public task reads");
    assert_eq!(public.id, TASK);
    let echoed = public
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(META_COLLABORATION))
        .and_then(|echo| echo.get("handoff"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    assert_eq!(echoed, Some(provenance.handoff.as_str().to_string()));

    // The target completes the very same task.
    fixture.pump(&target(), 2).await;
    let mut task = AgentTaskEntityStore::new(
        fixture.task_scope(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    task.recover(fixture.now())
        .await
        .expect("the task recovers");
    let snapshot = task
        .snapshot()
        .expect("the snapshot reads")
        .expect("the task exists");
    assert_eq!(snapshot.status, rakka_agent::AgentTaskStatus::Completed);
    assert!(snapshot.accepted_result.is_some());
}

/// The wire half: a replayed handoff send converges on the recorded
/// transfer, a forged source claim fails closed against durable state, and a
/// half-formed cluster is refused rather than half-understood.
#[tokio::test]
async fn wire_level_handoff_sends_converge_and_fail_closed() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture.instantiate(&source()).await;
    fixture.instantiate(&target()).await;
    fixture.create_task().await;
    // Drive the source's acceptance — the handoff door requires the claimed
    // source to be the current *accepted* assignment — without dispatching
    // any of the run's own work: the task's settle passes deliver the
    // assignment and settle the run's durable acceptance.
    for _ in 0..4 {
        let now = fixture.now();
        let mut task = AgentTaskEntityStore::new(
            fixture.task_scope(),
            fixture.tasks.clone(),
            fixture.agents.clone(),
            fixture.history.clone(),
        );
        task.recover(now).await.expect("the task recovers");
        let _ = task
            .settle_side_effects(&fixture.router, fixture.now())
            .await;
    }

    let handoff = handoff_id_for(&fixture.run_scope(&source(), 1), 7, 0)
        .expect("the handoff id derives")
        .into_string();

    // A forged source claim — a generation the task never accepted — fails
    // closed against durable task state, whatever the metadata says.
    let forged = fixture
        .service
        .send_message(&params(), &send_request(handoff_message(&handoff, 7)))
        .await;
    assert!(
        matches!(
            &forged,
            Err(RakkaAgentA2AError::Task(error))
                if error.code() == "handoff-source-not-current"
        ),
        "a forged source fails closed, got {forged:?}"
    );

    // A cluster that does not deduplicate under its own handoff id is
    // refused before anything durable: the id doubles verbatim as the
    // deduplication key, and any other binding could alias one transfer
    // onto another's recorded operation.
    let mut mismatched = handoff_message(&handoff, 1);
    if let Some(metadata) = mismatched.metadata.as_mut() {
        metadata.insert(
            META_DEDUPLICATION_KEY.to_string(),
            Value::String("other-key".to_string()),
        );
    }
    let refused = fixture
        .service
        .send_message(&params(), &send_request(mismatched))
        .await;
    assert!(
        matches!(
            &refused,
            Err(RakkaAgentA2AError::Refused { code, .. }) if code == "handoff-identity-mismatch"
        ),
        "a foreign deduplication key fails closed, got {refused:?}"
    );

    // A cluster naming a target this surface does not serve is refused by
    // the same catalog gate the creation path passes — before the
    // state-mutating command could spend one of the task's bounded handoffs.
    let mut foreign = handoff_message("handoff-foreign", 1);
    if let Some(metadata) = foreign.metadata.as_mut() {
        if let Some(Value::Object(cluster)) = metadata.get_mut(META_COLLABORATION) {
            cluster.insert(
                "target-agent".to_string(),
                Value::String("internal-ops".to_string()),
            );
        }
    }
    let refused = fixture
        .service
        .send_message(&params(), &send_request(foreign))
        .await;
    assert!(
        matches!(&refused, Err(RakkaAgentA2AError::UnknownAgent { .. })),
        "an unserved target fails closed, got {refused:?}"
    );

    // A cluster whose context projection exceeds the sender-side structural
    // bounds is re-validated at the transition and refused: the wire's claim
    // never bloats the task's bounded materialized state.
    let mut oversized = handoff_message("handoff-oversized", 1);
    if let Some(metadata) = oversized.metadata.as_mut() {
        if let Some(Value::Object(cluster)) = metadata.get_mut(META_COLLABORATION) {
            cluster.insert(
                "context".to_string(),
                json!((0..100).map(|i| format!("ref-{i}")).collect::<Vec<_>>()),
            );
        }
    }
    let refused = fixture
        .service
        .send_message(&params(), &send_request(oversized))
        .await;
    assert!(
        matches!(
            &refused,
            Err(RakkaAgentA2AError::Task(error)) if error.code() == "handoff-context-invalid"
        ),
        "an oversized context projection fails closed, got {refused:?}"
    );

    // The genuine transfer records once; a replay converges on it.
    let first = fixture
        .service
        .send_message(&params(), &send_request(handoff_message(&handoff, 1)))
        .await
        .expect("the transfer records");
    assert_eq!(first.id, TASK);
    let replay = fixture
        .service
        .send_message(&params(), &send_request(handoff_message(&handoff, 1)))
        .await
        .expect("the replay converges");
    assert_eq!(replay.id, TASK);
    let mut task = AgentTaskEntityStore::new(
        fixture.task_scope(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    task.recover(fixture.now())
        .await
        .expect("the task recovers");
    let snapshot = task
        .snapshot()
        .expect("the snapshot reads")
        .expect("the task exists");
    assert_eq!(snapshot.handoffs, 1, "one transfer, however often replayed");
    assert_eq!(
        snapshot.assignment_generation,
        AgentAssignmentGeneration::new(2)
    );

    // A cluster with a field this build does not serve fails closed — the
    // deny-unknown-fields posture that keeps half-understood collaboration
    // metadata from silently severing a transfer.
    let mut widened = handoff_message("handoff-widened", 1);
    if let Some(metadata) = widened.metadata.as_mut() {
        if let Some(Value::Object(cluster)) = metadata.get_mut(META_COLLABORATION) {
            cluster.insert("escrow-grant".to_string(), json!({ "tokens": 1_000_000 }));
        }
    }
    let refused = fixture
        .service
        .send_message(&params(), &send_request(widened))
        .await;
    assert!(
        matches!(&refused, Err(RakkaAgentA2AError::Unsupported { .. })),
        "an unserved cluster field fails closed, got {refused:?}"
    );

    // A handoff cluster without the task it transfers is refused: a handoff
    // continues a task, never creates one.
    let mut unaddressed = handoff_message("handoff-unaddressed", 1);
    unaddressed.task_id = None;
    let refused = fixture
        .service
        .send_message(&params(), &send_request(unaddressed))
        .await;
    assert!(
        matches!(&refused, Err(RakkaAgentA2AError::Unsupported { .. })),
        "a task-less handoff fails closed, got {refused:?}"
    );
}

/// The transfer is its own operation class at the authorization boundary: a
/// deployment authorizer that permits ordinary sends can still deny
/// `RecordHandoff`, and the check carries the cluster's claimed source and
/// target so the authorizer can bind the caller to the source run it claims
/// to speak for.
#[tokio::test]
async fn a_handoff_authorizes_as_its_own_operation_class() {
    struct DenyTransfers;

    #[async_trait::async_trait]
    impl A2AAuthorizer for DenyTransfers {
        async fn authorize(
            &self,
            request: &A2AAuthorizationRequest<'_>,
        ) -> A2AAuthorizationDecision {
            match request.operation {
                A2AOperation::RecordHandoff => {
                    let claim = request.handoff.expect("the transfer claim rides the check");
                    assert_eq!(claim.source_agent, SOURCE);
                    assert_eq!(claim.target_agent, TARGET);
                    assert_eq!(claim.source_generation, 1);
                    A2AAuthorizationDecision::Deny
                }
                _ => A2AAuthorizationDecision::Allow,
            }
        }
    }

    let fixture =
        Fixture::with_authorizer(DeterministicModelAdapter::new(), Arc::new(DenyTransfers));
    fixture.instantiate(&source()).await;
    fixture.instantiate(&target()).await;
    fixture.create_task().await;
    for _ in 0..4 {
        let now = fixture.now();
        let mut task = AgentTaskEntityStore::new(
            fixture.task_scope(),
            fixture.tasks.clone(),
            fixture.agents.clone(),
            fixture.history.clone(),
        );
        task.recover(now).await.expect("the task recovers");
        let _ = task
            .settle_side_effects(&fixture.router, fixture.now())
            .await;
    }

    let handoff = handoff_id_for(&fixture.run_scope(&source(), 1), 7, 0)
        .expect("the handoff id derives")
        .into_string();
    let denied = fixture
        .service
        .send_message(&params(), &send_request(handoff_message(&handoff, 1)))
        .await;
    assert!(
        matches!(&denied, Err(RakkaAgentA2AError::Unauthorized)),
        "a denied transfer fails closed, got {denied:?}"
    );

    // Nothing durable happened: the task still carries no transfer and its
    // handoff budget is unspent.
    let mut task = AgentTaskEntityStore::new(
        fixture.task_scope(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    task.recover(fixture.now())
        .await
        .expect("the task recovers");
    let snapshot = task
        .snapshot()
        .expect("the snapshot reads")
        .expect("the task exists");
    assert!(snapshot.handoff.is_none());
    assert_eq!(snapshot.handoffs, 0);
}

/// An ambiguous send failure with no recorded transfer is not a definitive
/// refusal: the write that failed may still land after the probe reads, so
/// the attempt stays retryable and the deduplicated re-send converges on the
/// recorded transfer — instead of resuming the source beside it.
#[tokio::test]
async fn an_ambiguous_send_stays_retryable_until_the_transfer_records() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture.instantiate(&source()).await;
    fixture.instantiate(&target()).await;
    fixture.create_task().await;
    // Drive the source's acceptance, exactly as the wire test does.
    for _ in 0..4 {
        let now = fixture.now();
        let mut task = AgentTaskEntityStore::new(
            fixture.task_scope(),
            fixture.tasks.clone(),
            fixture.agents.clone(),
            fixture.history.clone(),
        );
        task.recover(now).await.expect("the task recovers");
        let _ = task
            .settle_side_effects(&fixture.router, fixture.now())
            .await;
    }

    let scope = fixture.run_scope(&source(), 1);
    let handoff = handoff_id_for(&scope, 7, 0).expect("the handoff id derives");
    let record = AgentHandoffRecord {
        handoff: handoff.clone(),
        goal: None,
        task: AgentTaskId::new(TASK).expect("the task id is valid"),
        source_run: scope.clone(),
        source_generation: AgentAssignmentGeneration::new(1),
        requested_skill: AgentCapabilityId::new(HANDOFF_SKILL).expect("the skill id is valid"),
        resolved: AgentDelegationTarget::new(
            target(),
            AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id is valid"),
        ),
        reason: "needs billing authority".to_string(),
        policy_revision: AgentRevisionNumber::INITIAL,
        definition_revision: AgentRevisionNumber::INITIAL,
        settings_revision: AgentRevisionNumber::INITIAL,
        context: Vec::new(),
        a2a_message_id: handoff.as_str().to_string(),
        deduplication_key: handoff.as_str().to_string(),
        turn: 7,
        slot: 0,
        effect: AgentEffectId::new("effect-1"),
        call_id: AgentToolCallId::new("call-1").expect("the call id is valid"),
        telemetry: Default::default(),
        created_at: AgentTimestampMillis::new(1),
    };
    // The intent is a throwaway for direct executor re-invocation: the
    // executor reads only the record, exactly as its contract states. The
    // request variant is loop-constructible only in production; the test
    // re-encodes the record through serde, the path a durable replay takes.
    let request: AgentRunEffectRequest =
        serde_json::from_value(json!({ "a2a-handoff": { "handoff": record } }))
            .expect("the request round-trips");
    let spec = AgentEffectPolicies::default().spec_for(&request).clone();
    let intent = AgentRunEffect::new(
        &scope,
        7,
        0,
        request,
        &spec,
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the intent builds");
    let executor = A2AAgentHandoffSendExecutor::new(fixture.service.clone());

    // Lose the RecordHandoff write before it commits: the send fails
    // ambiguously and the probe finds no recorded transfer — which cannot
    // prove the failed write will never land, so the attempt must stay
    // retryable rather than refusing definitively.
    fixture.tasks.reset_writes();
    fixture.tasks.crash_at(1, CrashPoint::BeforeWrite);
    let finding = executor.execute(&scope, &intent, &record, None).await;
    fixture.tasks.assert_crash_fired(1, CrashPoint::BeforeWrite);
    fixture.tasks.survive();
    assert!(
        matches!(
            &finding,
            Err(AgentDispatchError::Invocation { code, .. }) if *code == "handoff-unrecorded"
        ),
        "an unproven absence stays retryable, got {finding:?}"
    );
    let mut task = AgentTaskEntityStore::new(
        fixture.task_scope(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    task.recover(fixture.now())
        .await
        .expect("the task recovers");
    let snapshot = task
        .snapshot()
        .expect("the snapshot reads")
        .expect("the task exists");
    assert!(snapshot.handoff.is_none(), "nothing was recorded");
    drop(task);

    // The retry re-drives the same deduplicated send and records the
    // transfer: the executor converges instead of stranding the source.
    let finding = executor
        .execute(&scope, &intent, &record, None)
        .await
        .expect("the re-driven send records the transfer");
    assert!(
        matches!(finding, AgentA2aHandoffFinding::Recorded { .. }),
        "the re-driven send converges, got {finding:?}"
    );
}
