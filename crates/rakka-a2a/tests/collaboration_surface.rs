//! Durable delegation across the A2A collaboration surface, end to end.
//!
//! Scenarios 28 and 39 of specification 18, over real entities: a
//! coordinator run's model turn requests a skill through the declared
//! coordination tool, the loop persists the delegation record with its send
//! effect, the in-process executor carries it — with the versioned
//! collaboration metadata — through [`RakkaAgentA2AService`] exactly as an
//! external A2A caller would, and the specialist's child task and run are
//! durably created exactly once while the parent task's identity and
//! ownership stay untouched. The fail-closed version matrix and the
//! plain-client compatibility of specification 14.4 are proven over the same
//! wiring.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest};
use serde_json::{json, Value};

use rakka_a2a::agents::{
    A2AAgentDelegationSendExecutor, A2AAgentTarget, A2AStaticAgentCatalog, RakkaAgentA2AError,
    RakkaAgentA2AService, AGENT_COLLABORATION_EXTENSION_PREFIX, AGENT_COLLABORATION_EXTENSION_URI,
    AGENT_COLLABORATION_SCHEMA_VERSION, META_AGENT_ID, META_COLLABORATION, META_TASK_DEFINITION,
};
use rakka_a2a::auth::AllowAllAuthorizer;
use rakka_a2a::mapping::{A2AHeaderTenantResolver, META_DEDUPLICATION_KEY};
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    run_entity, CrashingStateStore, DeferredExchangeRouter, DeterministicModelAdapter,
    InProcessRunEntityTransport, InProcessTaskEntityTransport, ScriptedDispatcher,
};
use rakka_agent::{
    delegation_id_for, effect_id_for, run_id_for_assignment, AgentA2aSendExecutor,
    AgentA2aSendFinding, AgentAssignmentGeneration, AgentAuthorityEnvelope, AgentCapabilityId,
    AgentCoordinationCapabilityKind, AgentDefinition, AgentDefinitionId, AgentDelegationRecord,
    AgentDelegationStatus, AgentDelegationTarget, AgentEffectSpec, AgentEntityClass,
    AgentEntityCommand, AgentEntityState, AgentEntityStore, AgentExchangeRouter, AgentId,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRevisionNumber,
    AgentRevisionProvenance, AgentRunDelegationConfig, AgentRunEffect, AgentRunEffectRequest,
    AgentRunScope, AgentRunState, AgentRunStatus, AgentSchemaId, AgentSchemaRef, AgentScope,
    AgentSettings, AgentTaskContent, AgentTaskDefinition, AgentTaskDefinitionId,
    AgentTaskEntityStore, AgentTaskId, AgentTaskLimits, AgentTaskResultCheck, AgentTaskResultRule,
    AgentTaskRuleId, AgentTaskScope, AgentTaskState, AgentTaskStatus, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore,
    StaticAgentDelegationCatalog, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis};
use rakka_persistence::{DurableStateStore, InMemoryDurableStateStore};

type TaskStore = CrashingStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = CrashingStateStore<AgentRunState>;
type Service = RakkaAgentA2AService<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore, RunStore>;

const TENANT: &str = "acme";
const COORDINATOR: &str = "coordinator";
const SPECIALIST: &str = "translator";
const COORDINATOR_DEFINITION: &str = "coordinate-goal";
const SPECIALIST_DEFINITION: &str = "translate-document";
const SKILL: &str = "translation";
const DELEGATION_TOOL: &str = "delegate";
const PARENT_TASK: &str = "goal-root";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn definition(id: &str, pointer: &str) -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(id).expect("definition id should be valid"),
        "One typed unit of collaborative work.",
        schema("input"),
        schema("result"),
    )
    .expect("task definition should be valid")
    .with_limits(AgentTaskLimits::new().with_max_result_rejections(2))
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: pointer.to_string(),
        },
    ))
    .with_budgets(rakka_agent::AgentBudgetCeilings {
        max_loop_iterations: Some(4),
        ..rakka_agent::AgentBudgetCeilings::unbounded()
    })
}

fn delegating_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Delegating the translation.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                AgentToolId::new(DELEGATION_TOOL).expect("tool id should be valid"),
                json!({ "skill": SKILL, "input": { "text": "hello" } }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(pointer_key: &str, answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(json!({ pointer_key: answer }))
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
    delegation: AgentRunDelegationConfig,
    clock: Arc<AtomicU64>,
    service: Arc<Service>,
}

impl Fixture {
    fn new(adapter: DeterministicModelAdapter) -> Self {
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

        // Both surfaces of the ingress catalog: the coordinator serves plain
        // creates, the specialist serves the delegated ones.
        let catalog = A2AStaticAgentCatalog::new()
            .with_target(A2AAgentTarget::new(
                coordinator(),
                definition(COORDINATOR_DEFINITION, "/answer"),
            ))
            .with_target(A2AAgentTarget::new(
                specialist(),
                definition(SPECIALIST_DEFINITION, "/translation"),
            ));
        let service = Arc::new(
            Service::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                runs.clone(),
                router.clone(),
                Arc::new(catalog),
                Arc::new(InMemoryA2ATaskProjectionStore::local()),
                Arc::new(A2AHeaderTenantResolver),
                Arc::new(AllowAllAuthorizer),
            )
            .with_clock(Arc::new(TestClock(clock.clone())))
            .with_default_tenant(TENANT),
        );

        // The outbound half: the model requests a skill; the application-owned
        // catalog resolves the specialist; the executor carries the send back
        // through the same service core an external caller uses.
        let delegation = AgentRunDelegationConfig::new(
            AgentToolId::new(DELEGATION_TOOL).expect("tool id should be valid"),
            Arc::new(
                StaticAgentDelegationCatalog::new().with_target(
                    AgentCapabilityId::new(SKILL).expect("capability id should be valid"),
                    AgentDelegationTarget::new(
                        specialist(),
                        AgentTaskDefinitionId::new(SPECIALIST_DEFINITION)
                            .expect("definition id should be valid"),
                    ),
                ),
            ),
            std::collections::BTreeSet::from([AgentCoordinationCapabilityKind::Delegation]),
        )
        .expect("the delegation configuration declares the capability");
        run_transport.install_delegation(delegation.clone());

        let dispatcher = ScriptedDispatcher::with_adapter(adapter).with_a2a_send_executor(
            Arc::new(A2AAgentDelegationSendExecutor::new(service.clone())),
        );

        Self {
            tasks,
            agents,
            runs,
            history,
            effects,
            router,
            dispatcher,
            delegation,
            clock,
            service,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    async fn instantiate(&self, agent: &AgentId, definition_id: &str, task_definition: &str) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope
            .task_definitions
            .insert(AgentTaskDefinitionId::new(task_definition).expect("definition id"));
        let definition = AgentDefinition::new(
            AgentDefinitionId::new(definition_id).expect("definition id should be valid"),
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

    fn task_scope(&self, task_id: &str) -> AgentTaskScope {
        AgentTaskScope::new(
            tenant(),
            AgentTaskId::new(task_id).expect("task id should be valid"),
        )
        .expect("task scope should be valid")
    }

    fn run_scope(&self, agent: &AgentId, task_id: &str) -> AgentRunScope {
        let run = run_id_for_assignment(
            self.task_scope(task_id).task(),
            AgentAssignmentGeneration::new(1),
        )
        .expect("the run id should be derivable");
        AgentRunScope::new(tenant(), agent.clone(), run).expect("run scope should be valid")
    }

    /// Drives one task and its first-generation run under `agent` until the
    /// run is terminal or nothing moves.
    async fn pump(&self, agent: &AgentId, task_id: &str) {
        for _round in 0..64 {
            let now = self.now();
            let mut task = AgentTaskEntityStore::new(
                self.task_scope(task_id),
                self.tasks.clone(),
                self.agents.clone(),
                self.history.clone(),
            );
            task.recover(now).await.expect("the task should recover");
            task.settle_side_effects(&self.router, now)
                .await
                .expect("the task should settle");

            let now = self.now();
            let mut run = run_entity(&self.run_scope(agent, task_id), &self.runs, &self.effects)
                .with_delegation(self.delegation.clone());
            run.recover(now).await.expect("the run should recover");
            let progress = run
                .settle_side_effects(&self.router, now)
                .await
                .expect("the run should settle");
            let answered = self
                .dispatcher
                .drive(&mut run, &self.router, self.now())
                .await
                .expect("the dispatcher should drive");

            let terminal = run
                .state()
                .ok()
                .and_then(rakka_agent::AgentRunState::status)
                .is_some_and(AgentRunStatus::is_terminal);
            if terminal
                || (progress.transitions == 0
                    && progress.effects_dispatched == 0
                    && progress.settled == 0
                    && answered == 0)
            {
                return;
            }
        }
        panic!("the collaboration surface did not converge");
    }

    /// Creates the coordinator's root task directly through the task entity,
    /// the way an application front end would institute it.
    async fn create_parent(&self) {
        let scope = self.task_scope(PARENT_TASK);
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
                    [TENANT, PARENT_TASK, "1"],
                )
                .expect("operation id should be derivable"),
                creation: Box::new(rakka_agent::AgentTaskCreation {
                    definition: definition(COORDINATOR_DEFINITION, "/answer"),
                    input: AgentTaskContent::inline(json!({ "goal": "translate everything" }))
                        .expect("the input is inline-bounded"),
                    assignee: Some(coordinator()),
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
        .expect("the parent task should create");
    }
}

fn coordinator() -> AgentId {
    AgentId::new(COORDINATOR).expect("agent id should be valid")
}

fn specialist() -> AgentId {
    AgentId::new(SPECIALIST).expect("agent id should be valid")
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

/// A collaboration send crafted at the wire level, the way a remote Rakka
/// peer's executor would emit it.
fn collaboration_message(delegation: &str, dedup: &str) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "text": "hello" })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = format!("{delegation}-message");
    message.extensions = Some(vec![AGENT_COLLABORATION_EXTENSION_URI.to_string()]);
    message.metadata = Some(
        [
            (
                META_DEDUPLICATION_KEY.to_string(),
                Value::String(dedup.to_string()),
            ),
            (
                META_AGENT_ID.to_string(),
                Value::String(SPECIALIST.to_string()),
            ),
            (
                META_TASK_DEFINITION.to_string(),
                Value::String(SPECIALIST_DEFINITION.to_string()),
            ),
            (
                META_COLLABORATION.to_string(),
                json!({
                    "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
                    "delegation": delegation,
                    "parent-task": PARENT_TASK,
                    "parent-run": format!("{TENANT}/{COORDINATOR}/{PARENT_TASK}-gen-1"),
                    "depth": 1,
                    "requested-skill": SKILL,
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

/// A wire-valid delegation id for hand-crafted sends.
fn wire_delegation_id(fixture: &Fixture, slot: usize) -> String {
    delegation_id_for(&fixture.run_scope(&coordinator(), PARENT_TASK), 9, slot)
        .expect("the delegation id derives")
        .into_string()
}

/// Scenario 39: delegation creates exactly one child task and run while the
/// parent task's identity and ownership remain unchanged — driven from the
/// coordinator's model turn all the way through the A2A surface to the
/// specialist's completed child.
#[tokio::test]
async fn a_delegation_creates_exactly_one_child_across_the_a2a_surface() {
    let fixture = Fixture::new(
        DeterministicModelAdapter::new()
            .with_turn(delegating_turn())
            .with_turn(proposing_turn("answer", "delegated and resolved"))
            .with_turn(proposing_turn("translation", "bonjour")),
    );
    fixture
        .instantiate(&coordinator(), "coordinator-v1", COORDINATOR_DEFINITION)
        .await;
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;
    fixture.create_parent().await;

    // The coordinator delegates, receives the send's confirmation as its
    // tool result, and completes its own task.
    fixture.pump(&coordinator(), PARENT_TASK).await;

    // The parent run holds exactly one settled cell naming the child.
    let parent_run_scope = fixture.run_scope(&coordinator(), PARENT_TASK);
    let mut parent_run = run_entity(&parent_run_scope, &fixture.runs, &fixture.effects);
    parent_run
        .recover(fixture.now())
        .await
        .expect("the parent run should recover");
    let state = parent_run.state().expect("the parent run state reads");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    let loop_state = state.loop_state().expect("the parent loop state exists");
    assert_eq!(loop_state.delegation_count(), 1);
    let cell = loop_state
        .delegations()
        .values()
        .next()
        .expect("the cell exists");
    let AgentDelegationStatus::ChildCreated { child_task, .. } = &cell.status else {
        panic!(
            "the delegation should settle child-created, not {:?}",
            cell.status
        );
    };
    let child_task = child_task.clone();
    let delegation_id = cell.record.delegation.clone();
    drop(parent_run);

    // The child exists once, under the specialist, carrying the delegation's
    // provenance; its run completes when pumped.
    fixture.pump(&specialist(), child_task.as_str()).await;
    let mut child = AgentTaskEntityStore::new(
        fixture.task_scope(child_task.as_str()),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    child
        .recover(fixture.now())
        .await
        .expect("the child task should recover");
    let snapshot = child
        .snapshot()
        .expect("the child snapshot reads")
        .expect("the child task exists");
    assert_eq!(snapshot.status, AgentTaskStatus::Completed);
    assert_eq!(snapshot.assignment_generation.get(), 1);
    let provenance = snapshot.delegation.as_deref().expect("provenance recorded");
    assert_eq!(provenance.delegation, delegation_id);
    assert_eq!(provenance.parent_task.as_str(), PARENT_TASK);
    assert_eq!(provenance.parent_run, parent_run_scope);
    assert_eq!(provenance.depth, 1);
    assert_eq!(provenance.requested_skill.as_str(), SKILL);
    assert_eq!(
        snapshot.parent.as_ref().map(AgentTaskId::as_str),
        Some(PARENT_TASK)
    );

    // The public projection echoes the delegation.
    let task = fixture
        .service
        .get_task(&params(), Some(TENANT), child_task.as_str(), None)
        .await
        .expect("the child task projects");
    let echo = task
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(META_COLLABORATION))
        .expect("the projection echoes the collaboration");
    assert_eq!(
        echo.get("delegation").and_then(Value::as_str),
        Some(delegation_id.as_str())
    );

    // Scenario 39's ownership half: the parent task is untouched by the
    // delegation — same generation, its own accepted result, no provenance.
    let mut parent = AgentTaskEntityStore::new(
        fixture.task_scope(PARENT_TASK),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    parent
        .recover(fixture.now())
        .await
        .expect("the parent task should recover");
    let parent_snapshot = parent
        .snapshot()
        .expect("the parent snapshot reads")
        .expect("the parent task exists");
    assert_eq!(parent_snapshot.status, AgentTaskStatus::Completed);
    assert_eq!(parent_snapshot.assignment_generation.get(), 1);
    assert!(parent_snapshot.delegation.is_none());
    assert!(parent_snapshot.accepted_result.is_some());

    // The specialist's run belongs to the child's own scope.
    let child_run_scope = fixture.run_scope(&specialist(), child_task.as_str());
    let mut child_run = run_entity(&child_run_scope, &fixture.runs, &fixture.effects);
    child_run
        .recover(fixture.now())
        .await
        .expect("the child run should recover");
    assert_eq!(
        child_run
            .state()
            .expect("the child run state reads")
            .status(),
        Some(AgentRunStatus::Completed)
    );
}

/// Scenario 28, the A2A half: replaying a delegation send creates exactly
/// one logical child, and a conflicting sender is answered with the original
/// delegation's child — an explicit, detectable conflict, never a second
/// child.
#[tokio::test]
async fn replayed_delegation_sends_converge_on_one_child_or_an_explicit_conflict() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;

    let delegation = wire_delegation_id(&fixture, 0);
    let first = fixture
        .service
        .send_message(
            &params(),
            &send_request(collaboration_message(&delegation, &delegation)),
        )
        .await
        .expect("the first send creates the child");
    let replay = fixture
        .service
        .send_message(
            &params(),
            &send_request(collaboration_message(&delegation, &delegation)),
        )
        .await
        .expect("the replay returns the same child");
    assert_eq!(first.id, replay.id);

    // A different delegation under the SAME deduplication key reaches the
    // same durable operation and is answered with the original child — whose
    // echo names the original delegation, which is exactly what lets the
    // conflicting sender detect that this child is not its own.
    let conflicting = wire_delegation_id(&fixture, 1);
    let answered = fixture
        .service
        .send_message(
            &params(),
            &send_request(collaboration_message(&conflicting, &delegation)),
        )
        .await
        .expect("the conflicting send is answered from the journal");
    assert_eq!(answered.id, first.id);
    let echo = answered
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(META_COLLABORATION))
        .and_then(|echo| echo.get("delegation"))
        .and_then(Value::as_str)
        .expect("the echo names the owning delegation");
    assert_eq!(echo, delegation);
    assert_ne!(echo, conflicting);

    // A distinct delegation under its own key is a distinct logical child.
    let sibling = wire_delegation_id(&fixture, 2);
    let second = fixture
        .service
        .send_message(
            &params(),
            &send_request(collaboration_message(&sibling, &sibling)),
        )
        .await
        .expect("the sibling delegation creates its own child");
    assert_ne!(second.id, first.id);
}

/// Scenario 28's aged-out edge: the child's deduplication window is bounded
/// ([`rakka_agent::AGENT_TASK_OPERATION_LOG_CAPACITY`] operations), so a
/// parent that recovers slowly can replay its send after the create
/// operation left the window and be answered `task-already-created` — about
/// its own child. The executor disambiguates against the child's
/// collaboration echo before declaring anything: the aged-out replay
/// converges on the original child exactly as an in-window replay would,
/// and only a child the delegation's identity does not own is the explicit
/// conflict of specification 6.6.
#[tokio::test]
async fn an_aged_out_replay_converges_on_its_own_child_instead_of_conflicting() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;

    let parent_run = fixture.run_scope(&coordinator(), PARENT_TASK);
    let original_key = delegation_id_for(&parent_run, 9, 0)
        .expect("the delegation id derives")
        .into_string();
    // The record exactly as the parent's interception persists it, keyed by
    // the derived `(turn, slot)` coordinate; the foreign record below shares
    // the deduplication key but not the delegation identity.
    let record_for = |slot: usize| {
        let delegation =
            delegation_id_for(&parent_run, 9, slot).expect("the delegation id derives");
        AgentDelegationRecord {
            a2a_message_id: delegation.as_str().to_string(),
            deduplication_key: original_key.clone(),
            delegation,
            goal: None,
            parent_task: AgentTaskId::new(PARENT_TASK).expect("task id should be valid"),
            parent_run: parent_run.clone(),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 1,
            requested_skill: AgentCapabilityId::new(SKILL).expect("capability id should be valid"),
            resolved: AgentDelegationTarget::new(
                specialist(),
                AgentTaskDefinitionId::new(SPECIALIST_DEFINITION)
                    .expect("definition id should be valid"),
            ),
            turn: 9,
            slot,
            effect: effect_id_for(&parent_run, 9, slot).expect("the effect id derives"),
            call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
            input: AgentTaskContent::inline(json!({ "text": "hello" }))
                .expect("the input is inline-bounded"),
            result_schema: None,
            budget: None,
            granted_descendants: None,
            deadline: None,
            definition_revision: AgentRevisionNumber::new(1),
            settings_revision: AgentRevisionNumber::new(1),
            telemetry: AgentTelemetryContext::default(),
            created_at: AgentTimestampMillis::new(1),
        }
    };
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let intent_for = |record: &AgentDelegationRecord| {
        AgentRunEffect::new(
            &parent_run,
            record.turn,
            record.slot,
            AgentRunEffectRequest::A2aSend {
                delegation: Box::new(record.clone()),
            },
            &spec,
            AgentRevisionNumber::new(1),
            AgentTimestampMillis::new(1),
        )
        .expect("the intent builds")
    };
    let executor = A2AAgentDelegationSendExecutor::new(fixture.service.clone());

    let record = record_for(0);
    let first = executor
        .execute(&parent_run, &intent_for(&record), &record, None)
        .await
        .expect("the first send executes");
    let AgentA2aSendFinding::Sent { child_task, .. } = first else {
        panic!("the first send creates the child, got {first:?}");
    };

    // Age the create operation out of the child's deduplication window: the
    // durable shape after `AGENT_TASK_OPERATION_LOG_CAPACITY` later
    // operations is exactly this state — the task record intact, the log
    // without the create — produced here directly so the test does not have
    // to manufacture sixty-four unrelated transitions.
    let child_scope =
        AgentTaskScope::new(tenant(), child_task.clone()).expect("the child scope is valid");
    let persistence_id = child_scope.persistence_id();
    let held = fixture
        .tasks
        .load(&persistence_id)
        .await
        .expect("the child state loads")
        .expect("the child exists");
    let mut encoded = serde_json::to_value(&held.state).expect("the state encodes");
    let log = encoded
        .get_mut("applied_operations")
        .expect("the operation log field exists");
    assert!(
        !log.as_array().expect("an array").is_empty(),
        "the create operation is in the window before eviction"
    );
    *log = json!([]);
    let evicted: AgentTaskState = serde_json::from_value(encoded).expect("the state decodes");
    fixture
        .tasks
        .compare_and_set(&persistence_id, held.revision, evicted)
        .await
        .expect("the evicted state stores");

    // The aged-out replay is told `task-already-created`; the echo proves the
    // child is this delegation's own, and the replay converges instead of
    // winding the parent down over a false conflict.
    let replay = executor
        .execute(&parent_run, &intent_for(&record), &record, None)
        .await
        .expect("the aged-out replay executes");
    match replay {
        AgentA2aSendFinding::Sent {
            child_task: replayed,
            ..
        } => assert_eq!(replayed, child_task),
        other => panic!("the aged-out replay converges on its own child, got {other:?}"),
    }

    // A different delegation under the same key meets the same refusal — and
    // the echo, naming the original delegation, makes it the genuine
    // conflict.
    let foreign = record_for(1);
    let answer = executor
        .execute(&parent_run, &intent_for(&foreign), &foreign, None)
        .await
        .expect("the foreign send executes");
    match answer {
        AgentA2aSendFinding::Conflict { code, .. } => {
            assert_eq!(code, "delegation-child-conflict");
        }
        other => {
            panic!("a child this delegation does not own is an explicit conflict, got {other:?}")
        }
    }
}

/// The fail-closed version matrix of specification 14.4: every half-formed
/// engagement of the collaboration extension refuses the send whole.
#[tokio::test]
async fn half_formed_collaboration_engagements_fail_closed() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;
    let delegation = wire_delegation_id(&fixture, 3);

    // An unserved extension version.
    let mut unserved = collaboration_message(&delegation, &delegation);
    unserved.extensions = Some(vec![format!("{AGENT_COLLABORATION_EXTENSION_PREFIX}v999")]);
    // The v1 declaration without the metadata object.
    let mut undeclared = collaboration_message(&delegation, &delegation);
    if let Some(metadata) = undeclared.metadata.as_mut() {
        metadata.remove(META_COLLABORATION);
    }
    // The metadata object without the declaration.
    let mut untagged = collaboration_message(&delegation, &delegation);
    untagged.extensions = None;
    // A foreign schema number inside a served envelope.
    let mut foreign = collaboration_message(&delegation, &delegation);
    if let Some(metadata) = foreign.metadata.as_mut() {
        if let Some(Value::Object(envelope)) = metadata.get_mut(META_COLLABORATION) {
            envelope.insert("schema".to_string(), json!(999));
        }
    }
    // An envelope that does not parse under schema version 1.
    let mut malformed = collaboration_message(&delegation, &delegation);
    if let Some(metadata) = malformed.metadata.as_mut() {
        metadata.insert(META_COLLABORATION.to_string(), json!({ "schema": 1 }));
    }

    for (label, message) in [
        ("unserved version", unserved),
        ("declaration without metadata", undeclared),
        ("metadata without declaration", untagged),
        ("foreign schema", foreign),
        ("malformed envelope", malformed),
    ] {
        let error = fixture
            .service
            .send_message(&params(), &send_request(message))
            .await
            .expect_err(label);
        assert!(
            matches!(error, RakkaAgentA2AError::Unsupported { .. }),
            "{label} should fail closed as unsupported, not {error}"
        );
    }
}

/// Forged parent bindings fail closed at the creation door: a depth that
/// does not agree with the presented lineage, and a parent run in a foreign
/// tenant, are both refused before anything durable records them — the
/// enforcement slices ceiling against these fields, so they are validated
/// where they enter, never trusted because a peer asserted them.
#[tokio::test]
async fn forged_parent_bindings_fail_closed_at_ingress() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;

    // A claimed depth with no chain behind it.
    let deep = wire_delegation_id(&fixture, 4);
    let mut incoherent = collaboration_message(&deep, &deep);
    if let Some(metadata) = incoherent.metadata.as_mut() {
        if let Some(Value::Object(envelope)) = metadata.get_mut(META_COLLABORATION) {
            envelope.insert("depth".to_string(), json!(4_000_000));
        }
    }

    // A parent run claimed in a tenant the child is not created in.
    let foreign = wire_delegation_id(&fixture, 5);
    let mut cross_tenant = collaboration_message(&foreign, &foreign);
    if let Some(metadata) = cross_tenant.metadata.as_mut() {
        if let Some(Value::Object(envelope)) = metadata.get_mut(META_COLLABORATION) {
            envelope.insert(
                "parent-run".to_string(),
                json!(format!("evil/{COORDINATOR}/{PARENT_TASK}-gen-1")),
            );
        }
    }

    for (label, message) in [
        ("incoherent depth", incoherent),
        ("cross-tenant parent run", cross_tenant),
    ] {
        let error = fixture
            .service
            .send_message(&params(), &send_request(message))
            .await
            .expect_err(label);
        match &error {
            RakkaAgentA2AError::Task(inner) => assert_eq!(
                inner.code(),
                "task-delegation-provenance-invalid",
                "{label} should refuse at the provenance door"
            ),
            other => panic!("{label} should refuse as a task error, got {other}"),
        }
    }
}

/// The unknown-optional compatibility of specification 14.4: an ordinary A2A
/// client that never engages the extension is untouched, and its child
/// carries no delegation provenance.
#[tokio::test]
async fn a_plain_client_send_is_untouched_by_the_collaboration_surface() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;

    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "text": "hello" })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = "plain-1".to_string();
    // An unrelated extension URI and unrelated metadata are ignored, exactly
    // as any unknown optional metadata is.
    message.extensions = Some(vec!["urn:example:unrelated:v1".to_string()]);
    message.metadata = Some(
        [
            (
                META_AGENT_ID.to_string(),
                Value::String(SPECIALIST.to_string()),
            ),
            ("com.example.custom".to_string(), json!({ "free": "form" })),
        ]
        .into_iter()
        .collect(),
    );

    let task = fixture
        .service
        .send_message(&params(), &send_request(message))
        .await
        .expect("the plain send creates an ordinary task");
    assert!(task
        .metadata
        .as_ref()
        .is_none_or(|metadata| !metadata.contains_key(META_COLLABORATION)));

    let mut child = AgentTaskEntityStore::new(
        fixture.task_scope(&task.id),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    child
        .recover(fixture.now())
        .await
        .expect("the task should recover");
    let snapshot = child
        .snapshot()
        .expect("the snapshot reads")
        .expect("the task exists");
    assert!(snapshot.delegation.is_none());
}

/// The envelope carries logical credential-binding references at most, and
/// exactly those references — never resolved material — reach the child's
/// durable provenance.
#[tokio::test]
async fn credential_binding_references_survive_as_references_only() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;

    let delegation = wire_delegation_id(&fixture, 4);
    let mut message = collaboration_message(&delegation, &delegation);
    if let Some(metadata) = message.metadata.as_mut() {
        if let Some(Value::Object(envelope)) = metadata.get_mut(META_COLLABORATION) {
            envelope.insert(
                "credential-bindings".to_string(),
                json!(["translation-api-binding"]),
            );
            envelope.insert("capability-scopes".to_string(), json!(["translate"]));
        }
    }
    let task = fixture
        .service
        .send_message(&params(), &send_request(message))
        .await
        .expect("the send creates the child");

    let mut child = AgentTaskEntityStore::new(
        fixture.task_scope(&task.id),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    child
        .recover(fixture.now())
        .await
        .expect("the task should recover");
    let snapshot = child
        .snapshot()
        .expect("the snapshot reads")
        .expect("the task exists");
    let provenance = snapshot.delegation.as_deref().expect("provenance recorded");
    assert_eq!(
        provenance
            .credential_bindings
            .iter()
            .map(|binding| binding.as_str().to_string())
            .collect::<Vec<_>>(),
        vec!["translation-api-binding".to_string()]
    );
    assert_eq!(provenance.capability_scopes.len(), 1);
}

/// The ancestor-agent chain rides the v1 envelope, parallel to the lineage:
/// present, it validates into typed provenance; empty, it is omitted from
/// the wire entirely — which is what keeps a root-level send parseable by a
/// strict receiver that predates the field.
#[tokio::test]
async fn ancestors_ride_the_wire_and_omit_when_empty() {
    use rakka_a2a::agents::{AgentCollaborationBudget, AgentCollaborationMetadata};

    let chained = AgentCollaborationMetadata {
        schema: AGENT_COLLABORATION_SCHEMA_VERSION,
        delegation: format!("delegation-{}", "c".repeat(64)),
        parent_task: PARENT_TASK.to_string(),
        parent_run: format!("{TENANT}/{COORDINATOR}/{PARENT_TASK}-gen-1"),
        goal: None,
        lineage: vec![format!("delegation-{}", "d".repeat(64))],
        ancestors: vec!["root-coordinator".to_string()],
        depth: 2,
        requested_skill: SKILL.to_string(),
        capability_scopes: Vec::new(),
        credential_bindings: Vec::new(),
        budget: Some(AgentCollaborationBudget {
            max_descendants: Some(0),
            ..Default::default()
        }),
        deadline: None,
        result_schema: None,
    };
    let wire = chained.to_value();
    assert_eq!(wire["ancestors"], json!(["root-coordinator"]));
    assert_eq!(wire["budget"]["max-descendants"], json!(0));
    let provenance = chained
        .to_provenance()
        .expect("the chained envelope validates");
    assert_eq!(provenance.ancestors.len(), 1);
    assert_eq!(provenance.ancestors[0].as_str(), "root-coordinator");
    assert_eq!(
        provenance
            .budget
            .expect("the narrowed budget arrives as the child's cap")
            .max_descendants,
        Some(0)
    );

    let rootward = AgentCollaborationMetadata {
        lineage: Vec::new(),
        ancestors: Vec::new(),
        depth: 1,
        ..chained
    };
    let wire = rootward.to_value();
    assert!(
        wire.get("ancestors").is_none(),
        "an empty ancestry is omitted, so a v1-strict receiver still parses \
         a root-level send"
    );

    // The decode side of the same omission: wire JSON with no `ancestors`
    // key — a pre-4.4 sender — parses into an empty chain and validates
    // into provenance with an empty chain.
    let decoded: AgentCollaborationMetadata =
        serde_json::from_value(wire).expect("a wire envelope without the key decodes");
    assert!(decoded.ancestors.is_empty());
    let provenance = decoded
        .to_provenance()
        .expect("the rootward envelope validates");
    assert!(provenance.ancestors.is_empty());
}

/// An ancestry that disagrees with the presented lineage fails closed at the
/// creation door: the ceilings and the cycle check read these fields as
/// validated inputs, so a gap a peer could hide an ancestor in never becomes
/// durable provenance.
#[tokio::test]
async fn a_forged_ancestry_fails_closed_at_ingress() {
    let fixture = Fixture::new(DeterministicModelAdapter::new());
    fixture
        .instantiate(&specialist(), "translator-v1", SPECIALIST_DEFINITION)
        .await;

    let delegation = wire_delegation_id(&fixture, 6);
    let lineage_entry = wire_delegation_id(&fixture, 7);
    let mut forged = collaboration_message(&delegation, &delegation);
    if let Some(metadata) = forged.metadata.as_mut() {
        if let Some(Value::Object(envelope)) = metadata.get_mut(META_COLLABORATION) {
            envelope.insert(
                "lineage".to_string(),
                json!([lineage_entry, wire_delegation_id(&fixture, 8)]),
            );
            envelope.insert("depth".to_string(), json!(3));
            // Two lineage entries, one claimed agent.
            envelope.insert("ancestors".to_string(), json!(["root-coordinator"]));
        }
    }
    let error = fixture
        .service
        .send_message(&params(), &send_request(forged))
        .await
        .expect_err("the forged ancestry refuses");
    match &error {
        RakkaAgentA2AError::Task(inner) => assert_eq!(
            inner.code(),
            "task-delegation-provenance-invalid",
            "the ancestry refuses at the provenance door"
        ),
        other => panic!("the forged ancestry should refuse as a task error, got {other}"),
    }
}
