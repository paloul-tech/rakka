//! The typed agent A2A surface, end to end over real entities.
//!
//! Scenario 1 of specification 18: duplicate A2A task message acceptance
//! does not create two tasks, initial runs, or turns. The fixture wires the
//! real task, agent, and run entity facades over in-memory durable stores,
//! the in-process exchange transports, and the deterministic model adapter,
//! then drives every command through [`RakkaAgentA2AService`] exactly as an
//! external A2A caller would. The management extension and the typed client
//! facade are proven over the same wiring.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest, TaskState};
use serde_json::{json, Value};

use rakka_a2a::agents::{
    management_request_message, parse_management_response, A2AAgentClientTransport, A2AAgentTarget,
    A2AStaticAgentCatalog, AgentManagementCommand, AgentManagementRequest, AgentManagementResponse,
    RakkaAgentA2AError, RakkaAgentA2AService, AGENT_MANAGEMENT_EXTENSION_PREFIX,
    AGENT_MANAGEMENT_SCHEMA_VERSION,
};
use rakka_a2a::auth::AllowAllAuthorizer;
use rakka_a2a::mapping::{A2AHeaderTenantResolver, META_PRINCIPAL_REF};
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    sweep_crash_points, CrashingStateStore, DeferredExchangeRouter, InProcessRunEntityTransport,
    InProcessTaskEntityTransport, ScriptedDispatcher,
};
use rakka_agent::InMemoryAgentTeamHistoryStore;
use rakka_agent::{
    run_id_for_assignment, AgentAssignmentGeneration, AgentAuthorityEnvelope,
    AgentClientManagementCommand, AgentClientManagementResponse, AgentClientTaskRequest,
    AgentClientTaskState, AgentDefinition, AgentDefinitionId, AgentEntityClass, AgentEntityCommand,
    AgentEntityState, AgentEntityStore, AgentExchangeRouter, AgentId, AgentModelTurn,
    AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentRevisionProvenance,
    AgentRunEntityStore, AgentRunScope, AgentRunState, AgentRunStatus, AgentSchemaId,
    AgentSchemaRef, AgentScope, AgentSettings, AgentSettingsChange, AgentTaskContent,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityStore, AgentTaskHistoryCursor,
    AgentTaskHistoryKind, AgentTaskHistoryStore, AgentTaskId, AgentTaskLimits,
    AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskState,
    AgentTaskStatus, InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore, RakkaAgentClient,
    TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
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
    ConversationStore,
    rakka_agent::InMemoryAgentConversationHistoryStore,
>;
type TeamStore = InMemoryDurableStateStore<rakka_agent::AgentTeamState>;
type ConversationStore = InMemoryDurableStateStore<rakka_agent::AgentConversationState>;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK_DEFINITION: &str = "resolve-ticket";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("agent id should be valid")
}

fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("agent scope should be valid")
}

fn task_definition_id() -> AgentTaskDefinitionId {
    AgentTaskDefinitionId::new(TASK_DEFINITION).expect("task definition id should be valid")
}

fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_limits(AgentTaskLimits::new().with_max_result_rejections(2))
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(rakka_agent::AgentBudgetCeilings {
        max_loop_iterations: Some(4),
        ..rakka_agent::AgentBudgetCeilings::unbounded()
    })
}

fn valid_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
        AgentTaskContent::inline(json!({ "answer": answer }))
            .expect("the proposal is inline-bounded"),
    )
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "operator-7".to_string(),
        display_name: None,
    }
}

/// Deterministic service clock over the fixture's shared tick counter.
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
    clock: Arc<AtomicU64>,
    service: Arc<Service>,
}

impl Fixture {
    fn new(dispatcher: ScriptedDispatcher) -> Self {
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
            .with_route(AgentEntityClass::Run, Arc::new(run_transport));
        deferred.install(router.clone());

        let catalog =
            A2AStaticAgentCatalog::single(A2AAgentTarget::new(agent_id(), task_definition()));
        let service = Arc::new(
            Service::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                runs.clone(),
                TeamStore::default(),
                InMemoryAgentTeamHistoryStore::new(),
                ConversationStore::default(),
                rakka_agent::InMemoryAgentConversationHistoryStore::new(),
                router.clone(),
                Arc::new(catalog),
                Arc::new(InMemoryA2ATaskProjectionStore::local()),
                Arc::new(A2AHeaderTenantResolver),
                Arc::new(AllowAllAuthorizer),
            )
            .with_clock(Arc::new(TestClock(clock.clone())))
            .with_default_tenant(TENANT),
        );

        Self {
            tasks,
            agents,
            runs,
            history,
            effects,
            router,
            dispatcher,
            clock,
            service,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    async fn instantiate_agent(&self) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.task_definitions.insert(task_definition_id());
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
            "Resolves customer support tickets end to end.",
            envelope,
        )
        .expect("the agent definition should be valid");

        let mut agent = AgentEntityStore::new(agent_scope(), self.agents.clone());
        agent.recover().await.expect("the agent should recover");
        agent
            .apply(AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &agent_scope(),
                    "1",
                )
                .expect("operation id should be derivable"),
                definition: Box::new(definition),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(AgentRevisionProvenance {
                    principal: principal(),
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

    fn run_scope(&self, task_id: &str) -> AgentRunScope {
        let run = run_id_for_assignment(
            self.task_scope(task_id).task(),
            AgentAssignmentGeneration::new(1),
        )
        .expect("the run id should be derivable");
        AgentRunScope::new(tenant(), agent_id(), run).expect("run scope should be valid")
    }

    fn task(
        &self,
        task_id: &str,
    ) -> AgentTaskEntityStore<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore> {
        AgentTaskEntityStore::new(
            self.task_scope(task_id),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        )
    }

    fn run(&self, task_id: &str) -> AgentRunEntityStore<RunStore, InMemoryAgentRunEffectSink> {
        rakka_agent::testkit::run_entity(&self.run_scope(task_id), &self.runs, &self.effects)
    }

    /// Drives everything the entities owe until the run is terminal or
    /// nothing moves — the same durable-state-only pump the entity tests use.
    async fn pump(&self, task_id: &str) {
        for _round in 0..64 {
            let now = self.now();
            let mut task = self.task(task_id);
            task.recover(now).await.expect("the task should recover");
            task.settle_side_effects(&self.router, now)
                .await
                .expect("the task should settle");

            let now = self.now();
            let mut run = self.run(task_id);
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
        panic!("the agents surface did not converge");
    }

    /// [`Self::pump`], but surfacing the first error instead of panicking —
    /// what a sweep needs, because an armed crash point kills an entity's
    /// owner mid-drive and the injected loss is the point, not a failure.
    async fn try_pump(&self, task_id: &str) -> Result<(), String> {
        for _round in 0..64 {
            let now = self.now();
            let mut task = self.task(task_id);
            task.recover(now)
                .await
                .map_err(|error| error.code().to_string())?;
            task.settle_side_effects(&self.router, now)
                .await
                .map_err(|error| error.code().to_string())?;

            let now = self.now();
            let mut run = self.run(task_id);
            run.recover(now)
                .await
                .map_err(|error| error.code().to_string())?;
            let progress = run
                .settle_side_effects(&self.router, now)
                .await
                .map_err(|error| error.code().to_string())?;
            let answered = self
                .dispatcher
                .drive(&mut run, &self.router, self.now())
                .await
                .map_err(|error| error.code().to_string())?;

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
                return Ok(());
            }
        }
        Err("the agents surface did not converge".to_string())
    }

    async fn history_count(&self, task_id: &str, kind: AgentTaskHistoryKind) -> usize {
        let mut count = 0;
        let mut cursor = AgentTaskHistoryCursor::default();
        loop {
            let page = self
                .history
                .read(&self.task_scope(task_id), cursor)
                .await
                .expect("history should read");
            count += page
                .entries
                .iter()
                .filter(|entry| entry.kind == kind)
                .count();
            match page.next {
                Some(next) => cursor = next,
                None => return count,
            }
        }
    }
}

fn send_request(message: &Message) -> SendMessageRequest {
    SendMessageRequest {
        message: message.clone(),
        configuration: None,
        metadata: None,
        tenant: Some(TENANT.to_string()),
    }
}

fn task_message(message_id: &str) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "ticket": 1 })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = message_id.to_string();
    message
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

/// Scenario 1: duplicate A2A task message acceptance does not create two
/// tasks, initial runs, or turns.
#[tokio::test]
async fn duplicate_sends_create_one_task_one_run_one_turn() {
    let fixture = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
    fixture.instantiate_agent().await;

    let message = task_message("msg-1");
    let first = fixture
        .service
        .send_message(&params(), &send_request(&message))
        .await
        .expect("the first send should be accepted");
    let duplicate = fixture
        .service
        .send_message(&params(), &send_request(&message))
        .await
        .expect("the duplicate send should be accepted");
    assert_eq!(first.id, duplicate.id, "one durable task identity");

    // One task, one assignment, one run — before any model work.
    let task_id = first.id.clone();
    {
        let mut task = fixture.task(&task_id);
        let now = fixture.now();
        task.recover(now).await.expect("the task should recover");
        let snapshot = task
            .snapshot()
            .expect("the snapshot should read")
            .expect("the task exists");
        assert_eq!(snapshot.assignment_generation.get(), 1);
    }

    // Drive the loop to completion: exactly one scripted model turn.
    fixture.pump(&task_id).await;

    // A third duplicate after completion still converges on the same task.
    let late = fixture
        .service
        .send_message(&params(), &send_request(&message))
        .await
        .expect("the late duplicate should be accepted");
    assert_eq!(late.id, task_id);

    let mut task = fixture.task(&task_id);
    let now = fixture.now();
    task.recover(now).await.expect("the task should recover");
    let snapshot = task
        .snapshot()
        .expect("the snapshot should read")
        .expect("the task exists");
    assert_eq!(snapshot.status, AgentTaskStatus::Completed);
    assert_eq!(snapshot.assignment_generation.get(), 1);

    let mut run = fixture.run(&task_id);
    let now = fixture.now();
    run.recover(now).await.expect("the run should recover");
    let run_snapshot = run
        .snapshot()
        .expect("the snapshot should read")
        .expect("the run exists");
    assert_eq!(run_snapshot.status, AgentRunStatus::Completed);
    assert_eq!(run_snapshot.turn, 1, "exactly one model turn");

    // One proposal, one acceptance — the durable history agrees.
    assert_eq!(
        fixture
            .history_count(&task_id, AgentTaskHistoryKind::ResultProposed)
            .await,
        1
    );
    assert_eq!(
        fixture
            .history_count(&task_id, AgentTaskHistoryKind::ResultAccepted)
            .await,
        1
    );

    // The public view reports the terminal state and replayable events lead
    // to it monotonically.
    let task = fixture
        .service
        .get_task(&params(), Some(TENANT), &task_id, None)
        .await
        .expect("the task should read");
    assert_eq!(task.status.state, TaskState::Completed);

    let events = fixture
        .service
        .replay_task_events(&params(), Some(TENANT), &task_id, None)
        .await
        .expect("events should replay");
    assert!(!events.is_empty());
    let sequences: Vec<u64> = events.iter().map(|event| event.sequence).collect();
    let mut sorted = sequences.clone();
    sorted.sort_unstable();
    assert_eq!(sequences, sorted, "events replay in order");
    assert_eq!(
        events.last().map(|event| event.projected_state.clone()),
        Some(TaskState::Completed)
    );
}

/// The management extension applies settings through the durable inbox with
/// revision fencing, deduplication, and fail-closed versioning.
#[tokio::test]
async fn management_extension_updates_settings_through_the_durable_inbox() {
    let fixture = Fixture::new(ScriptedDispatcher::new());
    fixture.instantiate_agent().await;

    let request = AgentManagementRequest {
        schema: AGENT_MANAGEMENT_SCHEMA_VERSION,
        command: AgentManagementCommand::UpdateSettings {
            agent: AGENT.to_string(),
            expected_revision: AgentRevisionNumber::INITIAL,
            changes: vec![AgentSettingsChange::RetrievalLimit(16)],
        },
    };
    let mut message = management_request_message(&request);
    message.message_id = "manage-1".to_string();
    let send = SendMessageRequest {
        message: message.clone(),
        configuration: None,
        metadata: Some(std::collections::HashMap::from([(
            META_PRINCIPAL_REF.to_string(),
            Value::String("user:operator-7".to_string()),
        )])),
        tenant: Some(TENANT.to_string()),
    };

    // Applied: the settings revision advances.
    let response = fixture
        .service
        .manage_agent(&params(), &send)
        .await
        .expect("the update should be served");
    let parsed = parse_management_response(&response).expect("the response should parse");
    let AgentManagementResponse::Applied { outcome } = &parsed else {
        panic!("expected an applied outcome, got {parsed:?}");
    };
    assert_eq!(outcome.settings_revision, AgentRevisionNumber::new(2));

    // A retry of the same message is a duplicate with the original outcome.
    let retry = fixture
        .service
        .manage_agent(&params(), &send)
        .await
        .expect("the retry should be served");
    let parsed = parse_management_response(&retry).expect("the response should parse");
    let AgentManagementResponse::Duplicate { outcome } = &parsed else {
        panic!("expected a duplicate outcome, got {parsed:?}");
    };
    assert_eq!(outcome.settings_revision, AgentRevisionNumber::new(2));

    // A stale expected revision answers with the conflict, not an error.
    let mut stale = send.clone();
    stale.message.message_id = "manage-2".to_string();
    let response = fixture
        .service
        .manage_agent(&params(), &stale)
        .await
        .expect("the stale update should be served");
    let parsed = parse_management_response(&response).expect("the response should parse");
    let AgentManagementResponse::Refused { code, .. } = &parsed else {
        panic!("expected a refusal, got {parsed:?}");
    };
    assert_eq!(code, "stale-settings-revision");

    // Describe reports the fenced revisions.
    let describe = AgentManagementRequest {
        schema: AGENT_MANAGEMENT_SCHEMA_VERSION,
        command: AgentManagementCommand::Describe {
            agent: AGENT.to_string(),
        },
    };
    let mut message = management_request_message(&describe);
    message.message_id = "manage-3".to_string();
    let response = fixture
        .service
        .manage_agent(&params(), &send_request(&message))
        .await
        .expect("describe should be served");
    let parsed = parse_management_response(&response).expect("the response should parse");
    let AgentManagementResponse::Described { description } = &parsed else {
        panic!("expected a description, got {parsed:?}");
    };
    assert_eq!(description.settings_revision, AgentRevisionNumber::new(2));

    // An unauthenticated write fails closed.
    let mut anonymous = send.clone();
    anonymous.metadata = None;
    anonymous.message.message_id = "manage-4".to_string();
    assert!(matches!(
        fixture.service.manage_agent(&params(), &anonymous).await,
        Err(RakkaAgentA2AError::Mapping(_))
    ));

    // An unknown management version fails closed.
    let mut future_version = send.clone();
    future_version.message.message_id = "manage-5".to_string();
    future_version.message.extensions =
        Some(vec![format!("{AGENT_MANAGEMENT_EXTENSION_PREFIX}v999")]);
    assert!(matches!(
        fixture
            .service
            .manage_agent(&params(), &future_version)
            .await,
        Err(RakkaAgentA2AError::Unsupported { .. })
    ));
}

/// Distinct lifecycle verbs that share one deduplication discriminator must
/// not alias each other's durable operation: a `Resume` reusing a prior
/// `Suspend`'s message id (hence discriminator) applies on its own — it is
/// never mistaken for a duplicate of the suspend. This is a regression guard
/// for the per-verb operation-id kinds; a single shared `LifecycleCommand`
/// kind made the resume collapse onto the suspend's cached outcome.
#[tokio::test]
async fn distinct_lifecycle_verbs_do_not_alias_on_a_shared_discriminator() {
    let fixture = Fixture::new(ScriptedDispatcher::new());
    fixture.instantiate_agent().await;

    let authenticated = || {
        Some(std::collections::HashMap::from([(
            META_PRINCIPAL_REF.to_string(),
            Value::String("user:operator-7".to_string()),
        )]))
    };
    // Both commands carry the same message id, so they derive the same
    // deduplication discriminator; only the per-verb operation-id kind keeps
    // them apart.
    let shared_message_id = "lifecycle-1";

    let suspend = AgentManagementRequest {
        schema: AGENT_MANAGEMENT_SCHEMA_VERSION,
        command: AgentManagementCommand::Suspend {
            agent: AGENT.to_string(),
            expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
        },
    };
    let mut suspend_message = management_request_message(&suspend);
    suspend_message.message_id = shared_message_id.to_string();
    let suspend_send = SendMessageRequest {
        message: suspend_message,
        configuration: None,
        metadata: authenticated(),
        tenant: Some(TENANT.to_string()),
    };
    let response = fixture
        .service
        .manage_agent(&params(), &suspend_send)
        .await
        .expect("the suspend should be served");
    let AgentManagementResponse::Applied { outcome } =
        parse_management_response(&response).expect("the response should parse")
    else {
        panic!("expected the suspend to apply");
    };
    assert_eq!(outcome.lifecycle_revision, AgentRevisionNumber::new(2));
    let after_suspend = outcome.lifecycle_revision;

    let resume = AgentManagementRequest {
        schema: AGENT_MANAGEMENT_SCHEMA_VERSION,
        command: AgentManagementCommand::Resume {
            agent: AGENT.to_string(),
            expected_lifecycle_revision: after_suspend,
        },
    };
    let mut resume_message = management_request_message(&resume);
    resume_message.message_id = shared_message_id.to_string();
    let resume_send = SendMessageRequest {
        message: resume_message,
        configuration: None,
        metadata: authenticated(),
        tenant: Some(TENANT.to_string()),
    };
    let response = fixture
        .service
        .manage_agent(&params(), &resume_send)
        .await
        .expect("the resume should be served");
    // Before the per-verb kinds this returned `Duplicate` carrying the
    // suspend's revision; the resume must instead apply its own transition.
    let parsed = parse_management_response(&response).expect("the response should parse");
    let AgentManagementResponse::Applied { outcome } = &parsed else {
        panic!("expected the resume to apply, got {parsed:?}");
    };
    assert_eq!(outcome.lifecycle_revision, AgentRevisionNumber::new(3));
}

/// The typed client facade drives the same durable path: create, read,
/// events, and management all converge with the direct A2A surface.
#[tokio::test]
async fn typed_client_converges_with_the_a2a_surface() {
    let fixture = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
    fixture.instantiate_agent().await;

    let transport = A2AAgentClientTransport::new(fixture.service.clone())
        .with_tenant(TENANT)
        .with_principal(principal());
    let client = RakkaAgentClient::new(transport);

    let view = client
        .create_task(AgentClientTaskRequest {
            input: json!({ "ticket": 1 }),
            deduplication_key: Some("order-42".to_string()),
            ..AgentClientTaskRequest::default()
        })
        .await
        .expect("the client create should be accepted");
    assert!(!view.state.is_terminal());

    // The same deduplication key converges on the same task.
    let duplicate = client
        .create_task(AgentClientTaskRequest {
            input: json!({ "ticket": 1 }),
            deduplication_key: Some("order-42".to_string()),
            ..AgentClientTaskRequest::default()
        })
        .await
        .expect("the duplicate create should be accepted");
    assert_eq!(view.task, duplicate.task);

    let task_id = view.task.as_str().to_string();
    fixture.pump(&task_id).await;

    let completed = client
        .task(&task_id)
        .await
        .expect("the task should read")
        .expect("the task exists");
    assert_eq!(completed.state, AgentClientTaskState::Completed);

    let events = client
        .task_events(&task_id, None)
        .await
        .expect("events should replay");
    assert!(!events.is_empty());
    // Cursoring past the last event yields nothing further.
    let cursor = events.last().map(|event| event.cursor.clone());
    let tail = client
        .task_events(&task_id, cursor.as_deref())
        .await
        .expect("the tail should replay");
    assert!(tail.is_empty());

    // An unknown task reads as absent, not as an error.
    assert!(client
        .task("task-0000000000000000")
        .await
        .expect("the read should succeed")
        .is_none());

    // Management through the client: describe, then a fenced update.
    let described = client
        .manage(
            AgentClientManagementCommand::Describe {
                agent: AGENT.to_string(),
            },
            None,
        )
        .await
        .expect("describe should be served");
    let AgentClientManagementResponse::Described(status) = described else {
        panic!("expected a description");
    };
    let updated = client
        .manage(
            AgentClientManagementCommand::UpdateSettings {
                agent: AGENT.to_string(),
                expected_revision: status.settings_revision,
                changes: vec![AgentSettingsChange::RetrievalLimit(8)],
            },
            None,
        )
        .await
        .expect("the update should be served");
    assert!(matches!(updated, AgentClientManagementResponse::Applied(_)));
}

/// A cancellation request alone never projects `CANCELED`; the terminal
/// state follows only the authoritative task condition.
#[tokio::test]
async fn cancellation_projects_the_authoritative_condition() {
    let fixture = Fixture::new(ScriptedDispatcher::new());
    fixture.instantiate_agent().await;

    let message = task_message("msg-cancel");
    let task = fixture
        .service
        .send_message(&params(), &send_request(&message))
        .await
        .expect("the send should be accepted");
    let task_id = task.id.clone();

    let cancel = a2a::CancelTaskRequest {
        id: task_id.clone(),
        metadata: None,
        tenant: Some(TENANT.to_string()),
    };
    let view = fixture
        .service
        .cancel_task(&params(), &cancel)
        .await
        .expect("the cancel should be accepted");
    // Whatever the propagation state, the projection never invents a
    // terminal or unspecified state ahead of the authoritative condition.
    assert_ne!(view.status.state, TaskState::Unspecified);

    fixture.pump(&task_id).await;

    let final_view = fixture
        .service
        .get_task(&params(), Some(TENANT), &task_id, None)
        .await
        .expect("the task should read");
    let mut task = fixture.task(&task_id);
    let now = fixture.now();
    task.recover(now).await.expect("the task should recover");
    let snapshot = task
        .snapshot()
        .expect("the snapshot should read")
        .expect("the task exists");
    if snapshot.status.is_terminal() {
        assert_eq!(final_view.status.state, TaskState::Canceled);
    } else {
        assert!(!final_view.status.state.is_terminal());
    }
}

/// Scenario 1 under the owner-kill sweep: kill the run's owner, then the
/// task's owner, at every durable write of the A2A accept -> assign -> run ->
/// complete flow, on both sides of the compare-and-set — then let the ingress
/// redeliver the same `message/send`. Every crash converges on one durable
/// task identity, one assignment generation, one run, and one turn. The task
/// id is derived from the deduplicated send, so the reference flow's id names
/// the task in every iteration.
#[tokio::test]
async fn duplicate_sends_survive_any_owner_loss_with_one_task_one_run_one_turn() {
    let build = || Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));

    let reference = build();
    reference.instantiate_agent().await;
    let message = task_message("msg-1");
    let accepted = reference
        .service
        .send_message(&params(), &send_request(&message))
        .await
        .expect("the reference send is accepted");
    let task_id = accepted.id.clone();
    reference.pump(&task_id).await;
    let run_writes = reference.runs.writes();
    let task_writes = reference.tasks.writes();
    assert!(
        run_writes >= 5 && task_writes >= 3,
        "the surface flow should write both stores, saw run {run_writes} / task {task_writes}"
    );

    let sweep = |armed: &'static str, writes: usize| {
        let task_id = task_id.clone();
        async move {
            sweep_crash_points(writes, |nth, point| {
                let task_id = task_id.clone();
                async move {
                    let fixture = build();
                    fixture.instantiate_agent().await;
                    match armed {
                        "run" => fixture.runs.crash_at(nth, point),
                        _ => fixture.tasks.crash_at(nth, point),
                    }

                    let message = task_message("msg-1");
                    let _crashed = fixture
                        .service
                        .send_message(&params(), &send_request(&message))
                        .await;
                    let _crashed = fixture.try_pump(&task_id).await;

                    match armed {
                        "run" => fixture.runs.assert_crash_fired(nth, point),
                        _ => fixture.tasks.assert_crash_fired(nth, point),
                    }

                    // A new owner activates; the ingress redelivers the send.
                    fixture.runs.survive();
                    fixture.tasks.survive();
                    let redelivered = fixture
                        .service
                        .send_message(&params(), &send_request(&message))
                        .await
                        .unwrap_or_else(|error| {
                            panic!(
                                "{armed} crash {point:?} at write {nth}: the redelivered \
                                 send was refused: {error:?}"
                            )
                        });
                    assert_eq!(
                        redelivered.id, task_id,
                        "{armed} crash {point:?} at write {nth} minted a second task"
                    );
                    fixture.try_pump(&task_id).await.unwrap_or_else(|error| {
                        panic!("{armed} crash {point:?} at write {nth} did not converge: {error}")
                    });

                    let mut task = fixture.task(&task_id);
                    let now = fixture.now();
                    task.recover(now).await.expect("the task recovers");
                    let snapshot = task
                        .snapshot()
                        .expect("the snapshot reads")
                        .expect("the task exists");
                    assert_eq!(
                        snapshot.status,
                        AgentTaskStatus::Completed,
                        "{armed} crash {point:?} at write {nth} should still complete"
                    );
                    assert_eq!(
                        snapshot.assignment_generation.get(),
                        1,
                        "{armed} crash {point:?} at write {nth} minted a second assignment"
                    );

                    let mut run = fixture.run(&task_id);
                    run.recover(fixture.now()).await.expect("the run recovers");
                    let run_snapshot = run
                        .snapshot()
                        .expect("the snapshot reads")
                        .expect("the run exists");
                    assert_eq!(run_snapshot.status, AgentRunStatus::Completed);
                    assert_eq!(
                        run_snapshot.turn, 1,
                        "{armed} crash {point:?} at write {nth} took a second turn"
                    );

                    for (kind, label) in [
                        (AgentTaskHistoryKind::Created, "task"),
                        (AgentTaskHistoryKind::AssignmentDecided, "assignment"),
                        (AgentTaskHistoryKind::ResultAccepted, "completion"),
                    ] {
                        assert_eq!(
                            fixture.history_count(&task_id, kind).await,
                            1,
                            "{armed} crash {point:?} at write {nth} duplicated a {label}"
                        );
                    }
                }
            })
            .await;
        }
    };

    sweep("run", run_writes).await;
    sweep("task", task_writes).await;
}
