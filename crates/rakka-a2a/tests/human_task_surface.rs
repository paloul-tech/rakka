//! Authenticated typed-result delivery to human-owned tasks over the A2A
//! agents surface (specification 8.12 and 14.3, scenario 41's wire half):
//! a `message/send` naming an existing `task_id` with the
//! `io.rakka.agent.result` binding completes a human-owned task through the
//! same deduplicated validation path — replacing the refusal slice 1.12
//! parked — while unauthenticated, half-formed, or misaddressed submissions
//! fail closed with their exact codes and nothing durable moves.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest, TaskState};
use async_trait::async_trait;
use serde_json::{json, Value};

use rakka_a2a::agents::{
    A2AAgentClientTransport, A2AAgentTarget, A2AStaticAgentCatalog, RakkaAgentA2AError,
    RakkaAgentA2AService, META_AGENT_ID, META_AGENT_RESULT, META_TASK_DEFINITION,
};
use rakka_a2a::auth::{
    A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer, A2AOperation,
    AllowAllAuthorizer,
};
use rakka_a2a::mapping::{A2AHeaderTenantResolver, A2AMappingError, META_DEDUPLICATION_KEY};
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    sweep_crash_points, CrashingStateStore, DeferredExchangeRouter, InProcessRunEntityTransport,
    InProcessTaskEntityTransport,
};
use rakka_agent::{
    load_agent_task_state, AgentClientTaskRequest, AgentClientTaskResultRequest,
    AgentClientTaskState, AgentEntityClass, AgentEntityState, AgentExchangeRouter, AgentId,
    AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentRunState, AgentSchemaId,
    AgentSchemaPolicy, AgentSchemaRef, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition,
    AgentTaskDefinitionId, AgentTaskEntityCommand, AgentTaskEntityStore, AgentTaskHistoryCursor,
    AgentTaskHistoryKind, AgentTaskHistoryStore, AgentTaskId, AgentTaskLimits, AgentTaskOwnership,
    AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskState,
    AgentTaskStatus, InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore,
    InMemoryAgentTeamHistoryStore, RakkaAgentClient, TenantId,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use rakka_persistence::InMemoryDurableStateStore;

type TaskStore = CrashingStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = InMemoryDurableStateStore<AgentRunState>;
type TeamStore = InMemoryDurableStateStore<rakka_agent::AgentTeamState>;
type ConversationStore = InMemoryDurableStateStore<rakka_agent::AgentConversationState>;
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

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK_DEFINITION: &str = "review-order";
const HUMAN_TASK: &str = "order-review-1";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn definition(ownership: AgentTaskOwnership) -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("definition id should be valid"),
        "One human-reviewed order.",
        schema("order-input"),
        schema("order-result"),
    )
    .expect("task definition should be valid")
    .with_ownership(ownership)
    .with_limits(AgentTaskLimits::new().with_max_result_rejections(2))
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
}

fn human_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(HUMAN_TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid")
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "human".to_string(),
        principal_id: "alice".to_string(),
        display_name: None,
    }
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
    history: InMemoryAgentTaskHistoryStore,
    router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
    service: Arc<Service>,
}

impl Fixture {
    fn new() -> Self {
        Self::with_authorizer(Arc::new(AllowAllAuthorizer))
    }

    fn with_authorizer(authorizer: Arc<dyn A2AAuthorizer>) -> Self {
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

        let catalog = A2AStaticAgentCatalog::new().with_target(A2AAgentTarget::new(
            AgentId::new(AGENT).expect("agent id should be valid"),
            definition(AgentTaskOwnership::Agent),
        ));
        let service = Arc::new(
            Service::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                runs,
                TeamStore::default(),
                InMemoryAgentTeamHistoryStore::new(),
                ConversationStore::default(),
                rakka_agent::InMemoryAgentConversationHistoryStore::new(),
                router.clone(),
                Arc::new(catalog),
                Arc::new(InMemoryA2ATaskProjectionStore::local()),
                Arc::new(A2AHeaderTenantResolver),
                authorizer,
            )
            .with_clock(Arc::new(TestClock(clock.clone())))
            .with_default_tenant(TENANT),
        );

        Self {
            tasks,
            agents,
            history,
            router,
            clock,
            service,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// Creates the human-owned task through the entity directly — the wire
    /// tests stay independent of how the application provisions human work.
    async fn create_human_task(&self) {
        let mut task = AgentTaskEntityStore::new(
            human_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        task.apply(
            AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, HUMAN_TASK, "1"],
                )
                .expect("operation id should be derivable"),
                creation: Box::new(AgentTaskCreation {
                    definition: definition(AgentTaskOwnership::Human),
                    input: AgentTaskContent::inline(json!({ "order": 42 }))
                        .expect("the input is inline-bounded"),
                    assignee: None,
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
        .expect("the human task should create");
    }

    async fn snapshot(&self) -> rakka_agent::AgentTaskSnapshot {
        load_agent_task_state(&self.tasks, &human_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the state loads")
            .expect("the task exists")
            .snapshot()
            .expect("the task snapshots")
    }

    async fn history_count(&self, kind: AgentTaskHistoryKind) -> usize {
        self.history
            .read(
                &human_scope(),
                AgentTaskHistoryCursor::start().with_limit(64),
            )
            .await
            .expect("the history reads")
            .entries
            .iter()
            .filter(|entry| entry.kind == kind)
            .count()
    }
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

fn result_binding() -> Value {
    json!({
        "definition": TASK_DEFINITION,
        "definition-version": 1,
        "result-schema": "order-result",
        "result-schema-version": 1,
    })
}

/// A submission message naming the human task, carrying the binding, the
/// principal, and the given content under the given deduplication key.
fn submission_message(dedup: &str, answer: Value) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(answer),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = format!("{dedup}-message");
    message.task_id = Some(HUMAN_TASK.to_string());
    message.metadata = Some(
        [
            (
                META_DEDUPLICATION_KEY.to_string(),
                Value::String(dedup.to_string()),
            ),
            (META_AGENT_RESULT.to_string(), result_binding()),
            (
                "io.rakka.principal.ref".to_string(),
                Value::String("human:alice".to_string()),
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

/// The wire happy path: an authenticated submission completes the human
/// task, a duplicate resend converges, and the projection turns terminal.
#[tokio::test]
async fn an_authenticated_submission_completes_the_human_task() {
    let fixture = Fixture::new();
    fixture.create_human_task().await;

    // Before the submission the public view waits on input.
    let waiting = fixture
        .service
        .get_task(&params(), None, HUMAN_TASK, None)
        .await
        .expect("the view reads");
    assert_eq!(waiting.status.state, TaskState::InputRequired);
    assert_eq!(
        waiting
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("io.rakka.agent.wait-reason")),
        Some(&Value::String("input".to_string()))
    );

    let task = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message(
                "submit-1",
                json!({ "answer": "approved" }),
            )),
        )
        .await
        .expect("the submission completes");
    assert_eq!(task.status.state, TaskState::Completed);

    // A duplicate resend converges on the same terminal view without a
    // second decision.
    let replay = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message(
                "submit-1",
                json!({ "answer": "approved" }),
            )),
        )
        .await
        .expect("the replay converges");
    assert_eq!(replay.status.state, TaskState::Completed);

    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot.status, AgentTaskStatus::Completed);
    let accepted = snapshot
        .accepted_result
        .as_deref()
        .expect("the result stands");
    assert_eq!(accepted.principal.as_deref(), Some("human:alice"));
    assert_eq!(
        fixture
            .history_count(AgentTaskHistoryKind::ResultAccepted)
            .await,
        1,
        "one acceptance, ever"
    );
}

/// The fail-closed matrix: every half-formed or misaddressed submission is
/// refused with its exact code, and nothing durable moves.
#[tokio::test]
async fn wire_level_result_submissions_fail_closed() {
    let fixture = Fixture::new();
    fixture.create_human_task().await;
    let before = fixture.snapshot().await;

    // Unauthenticated: no principal rides the send.
    let mut anonymous = submission_message("anon", json!({ "answer": "x" }));
    if let Some(metadata) = anonymous.metadata.as_mut() {
        metadata.remove("io.rakka.principal.ref");
    }
    match fixture
        .service
        .send_message(&params(), &send_request(anonymous))
        .await
    {
        Err(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField { field })) => {
            assert_eq!(field, "io.rakka.principal.ref");
        }
        other => panic!("an unauthenticated submission must fail closed: {other:?}"),
    }

    // No binding: there is no plain-input path to fall back to.
    let mut unbound = submission_message("unbound", json!({ "answer": "x" }));
    if let Some(metadata) = unbound.metadata.as_mut() {
        metadata.remove(META_AGENT_RESULT);
    }
    match fixture
        .service
        .send_message(&params(), &send_request(unbound))
        .await
    {
        Err(RakkaAgentA2AError::Mapping(A2AMappingError::MissingField { field })) => {
            assert_eq!(field, "io.rakka.agent.result");
        }
        other => panic!("a bindingless continuation must fail closed: {other:?}"),
    }

    // A binding with a field this build does not serve parses nothing.
    let mut unknown_field = submission_message("unknown", json!({ "answer": "x" }));
    if let Some(metadata) = unknown_field.metadata.as_mut() {
        metadata.insert(
            META_AGENT_RESULT.to_string(),
            json!({
                "definition": TASK_DEFINITION,
                "definition-version": 1,
                "result-schema": "order-result",
                "result-schema-version": 1,
                "escrow-grant": 100,
            }),
        );
    }
    assert!(matches!(
        fixture
            .service
            .send_message(&params(), &send_request(unknown_field))
            .await,
        Err(RakkaAgentA2AError::Mapping(
            A2AMappingError::InvalidMetadata { .. }
        ))
    ));

    // Binary parts keep failing closed.
    let mut binary = submission_message("binary", json!({}));
    binary.parts = vec![Part {
        content: PartContent::Raw(vec![1, 2, 3]),
        filename: None,
        media_type: Some("application/octet-stream".to_string()),
        metadata: None,
    }];
    assert!(matches!(
        fixture
            .service
            .send_message(&params(), &send_request(binary))
            .await,
        Err(RakkaAgentA2AError::Unsupported { .. })
    ));

    // An unknown task refuses without committing.
    let mut unknown_task = submission_message("nowhere", json!({ "answer": "x" }));
    unknown_task.task_id = Some("no-such-task".to_string());
    match fixture
        .service
        .send_message(&params(), &send_request(unknown_task))
        .await
    {
        Err(RakkaAgentA2AError::Refused { code, .. }) => {
            assert_eq!(code, "task-not-created");
        }
        other => panic!("an unknown task must refuse: {other:?}"),
    }

    // A declared contract on a task-creating send is a half-formed
    // engagement.
    let mut creating = submission_message("creating", json!({ "answer": "x" }));
    creating.task_id = None;
    if let Some(metadata) = creating.metadata.as_mut() {
        metadata.insert(META_AGENT_ID.to_string(), Value::String(AGENT.to_string()));
        metadata.insert(
            META_TASK_DEFINITION.to_string(),
            Value::String(TASK_DEFINITION.to_string()),
        );
    }
    match fixture
        .service
        .send_message(&params(), &send_request(creating))
        .await
    {
        Err(RakkaAgentA2AError::Refused { code, .. }) => {
            assert_eq!(code, "result-submission-requires-task");
        }
        other => panic!("a creating submission must refuse: {other:?}"),
    }

    // Nothing above committed anything durable.
    let after = fixture.snapshot().await;
    assert_eq!(after.status, before.status);
    assert_eq!(after.rejection_count, before.rejection_count);
    assert!(after.accepted_result.is_none());
    assert_eq!(
        fixture
            .history_count(AgentTaskHistoryKind::ResultProposed)
            .await,
        0
    );
}

/// An agent-owned target answers the entity's stable ownership refusal: the
/// wire stays ownership-agnostic.
#[tokio::test]
async fn an_agent_owned_target_refuses_the_submission() {
    let fixture = Fixture::new();
    // Create an agent-owned task through the wire's own creation path.
    let mut creating = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "order": 7 })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    creating.message_id = "agent-task-message".to_string();
    creating.metadata = Some(
        [
            (
                META_DEDUPLICATION_KEY.to_string(),
                Value::String("agent-task".to_string()),
            ),
            (META_AGENT_ID.to_string(), Value::String(AGENT.to_string())),
            (
                META_TASK_DEFINITION.to_string(),
                Value::String(TASK_DEFINITION.to_string()),
            ),
        ]
        .into_iter()
        .collect(),
    );
    let created = fixture
        .service
        .send_message(&params(), &send_request(creating))
        .await
        .expect("the agent task creates");

    let mut submission = submission_message("wrong-door", json!({ "answer": "x" }));
    submission.task_id = Some(created.id.clone());
    match fixture
        .service
        .send_message(&params(), &send_request(submission))
        .await
    {
        Err(RakkaAgentA2AError::Refused { code, .. }) => {
            assert_eq!(code, "task-not-human-owned");
        }
        other => panic!("an agent-owned target must refuse: {other:?}"),
    }
}

/// The submission authorizes under its own operation class, with the task,
/// the principal, and the claimed contract bound into the check.
#[tokio::test]
async fn a_result_submission_authorizes_under_its_own_operation_class() {
    struct DenySubmissions;

    #[async_trait]
    impl A2AAuthorizer for DenySubmissions {
        async fn authorize(
            &self,
            request: &A2AAuthorizationRequest<'_>,
        ) -> A2AAuthorizationDecision {
            if request.operation == A2AOperation::SubmitTaskResult {
                assert_eq!(request.task_id, Some(HUMAN_TASK));
                let principal = request.principal.expect("the principal rides the check");
                assert_eq!(principal.principal_id, "alice");
                let claim = request.task_result.expect("the claim rides the check");
                assert_eq!(claim.definition, Some(TASK_DEFINITION));
                assert_eq!(claim.definition_version, Some(1));
                assert_eq!(claim.result_schema, Some("order-result"));
                assert_eq!(claim.result_schema_version, Some(1));
                return A2AAuthorizationDecision::Deny;
            }
            A2AAuthorizationDecision::Allow
        }
    }

    let fixture = Fixture::with_authorizer(Arc::new(DenySubmissions));
    fixture.create_human_task().await;

    match fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message("denied", json!({ "answer": "x" }))),
        )
        .await
    {
        Err(RakkaAgentA2AError::Unauthorized) => {}
        other => panic!("the denial must surface: {other:?}"),
    }

    // Nothing durable changed, and an ordinary send still passes.
    let snapshot = fixture.snapshot().await;
    assert_eq!(snapshot.status, AgentTaskStatus::WaitingForInput);
    assert!(snapshot.accepted_result.is_none());
}

/// A committed validation rejection answers `Ok` with the task view carrying
/// the rejection echo — never an error that claims nothing happened — and a
/// corrected resubmission under a new key completes the task.
#[tokio::test]
async fn a_rejected_submission_records_the_decision_and_stays_open() {
    let fixture = Fixture::new();
    fixture.create_human_task().await;

    let rejected = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message("bad-1", json!({ "answer": "" }))),
        )
        .await
        .expect("a committed rejection answers the task view");
    assert_eq!(rejected.status.state, TaskState::InputRequired);
    let metadata = rejected
        .metadata
        .as_ref()
        .expect("the view carries metadata");
    assert_eq!(
        metadata.get("io.rakka.agent.rejections"),
        Some(&Value::Number(1.into()))
    );
    let echo = metadata
        .get("io.rakka.agent.last-rejection")
        .and_then(Value::as_object)
        .expect("the rejection echo rides the view");
    assert_eq!(
        echo.get("reason").and_then(Value::as_str),
        Some("empty-string-field")
    );
    assert_eq!(
        echo.get("rule").and_then(Value::as_str),
        Some("answer-present")
    );

    // Replaying the same key returns the original decision; the budget is
    // untouched.
    let replay = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message("bad-1", json!({ "answer": "" }))),
        )
        .await
        .expect("the replay converges");
    assert_eq!(replay.status.state, TaskState::InputRequired);
    assert_eq!(fixture.snapshot().await.rejection_count, 1);

    // The corrected resubmission is a new decision under a new key.
    let corrected = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message(
                "good-1",
                json!({ "answer": "approved" }),
            )),
        )
        .await
        .expect("the correction completes");
    assert_eq!(corrected.status.state, TaskState::Completed);

    // The rejected key replayed after completion still answers without a
    // second decision.
    let stale = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message("bad-1", json!({ "answer": "" }))),
        )
        .await
        .expect("the stale replay converges");
    assert_ne!(stale.status.state, TaskState::Unspecified);
    assert_eq!(fixture.snapshot().await.rejection_count, 1);
}

/// Exhausting the rejection budget fails the task and the public view turns
/// terminal `FAILED`.
#[tokio::test]
async fn an_exhausted_rejection_budget_fails_the_task() {
    let fixture = Fixture::new();
    fixture.create_human_task().await;

    let first = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message("bad-1", json!({ "answer": "" }))),
        )
        .await
        .expect("the first rejection commits");
    assert_eq!(first.status.state, TaskState::InputRequired);
    let second = fixture
        .service
        .send_message(
            &params(),
            &send_request(submission_message("bad-2", json!({ "answer": "" }))),
        )
        .await
        .expect("the exhausting rejection commits");
    assert_eq!(second.status.state, TaskState::Failed);
    assert_eq!(fixture.snapshot().await.status, AgentTaskStatus::Failed);
}

/// Owner loss at every durable task-store write of the submission converges
/// on one accepted result.
#[tokio::test]
async fn duplicate_submissions_survive_any_owner_loss_with_one_completion() {
    async fn world() -> Fixture {
        let fixture = Fixture::new();
        fixture.create_human_task().await;
        fixture.tasks.reset_writes();
        fixture
    }

    let reference = world().await;
    let _ = reference
        .service
        .send_message(
            &params(),
            &send_request(submission_message("submit-1", json!({ "answer": "ok" }))),
        )
        .await;
    let writes = reference.tasks.writes();
    assert!(writes >= 1, "the submission writes the task store");

    sweep_crash_points(writes, |point, window| async move {
        let fixture = world().await;
        fixture.tasks.crash_at(point, window);
        let _ = fixture
            .service
            .send_message(
                &params(),
                &send_request(submission_message("submit-1", json!({ "answer": "ok" }))),
            )
            .await;
        fixture.tasks.assert_crash_fired(point, window);
        fixture.tasks.survive();

        // The redelivered submission converges: one completion, one
        // acceptance row, the terminal public view. A duplicate reply skips
        // the settle phase, so the owed history is flushed by an explicit
        // pass — the recovery sweep a production owner runs.
        let task = fixture
            .service
            .send_message(
                &params(),
                &send_request(submission_message("submit-1", json!({ "answer": "ok" }))),
            )
            .await
            .expect("the redelivery converges");
        assert_eq!(task.status.state, TaskState::Completed);
        let mut store = AgentTaskEntityStore::new(
            human_scope(),
            fixture.tasks.clone(),
            fixture.agents.clone(),
            fixture.history.clone(),
        );
        store
            .recover(fixture.now())
            .await
            .expect("the task recovers");
        let _ = store
            .settle_side_effects(&fixture.router, fixture.now())
            .await;
        assert_eq!(
            fixture
                .history_count(AgentTaskHistoryKind::ResultAccepted)
                .await,
            1
        );
    })
    .await;
}

/// The typed client submits through the same durable path, sees the
/// rejection echo on `Ok`, and gets the ownership refusal as `Refused`.
#[tokio::test]
async fn the_typed_client_submits_through_the_same_durable_path() {
    let fixture = Fixture::new();
    fixture.create_human_task().await;

    let transport = A2AAgentClientTransport::new(fixture.service.clone())
        .with_tenant(TENANT)
        .with_principal(principal());
    let client = RakkaAgentClient::new(transport);

    // A committed rejection is Ok: the nonterminal view carries the echo.
    let rejected = client
        .submit_task_result(AgentClientTaskResultRequest {
            task: HUMAN_TASK.to_string(),
            result: json!({ "answer": "" }),
            definition: TASK_DEFINITION.to_string(),
            definition_version: 1,
            result_schema: "order-result".to_string(),
            result_schema_version: 1,
            deduplication_key: Some("client-bad".to_string()),
            ..AgentClientTaskResultRequest::default()
        })
        .await
        .expect("a committed rejection answers Ok");
    assert_eq!(rejected.state, AgentClientTaskState::InputRequired);
    assert!(rejected
        .metadata
        .contains_key("io.rakka.agent.last-rejection"));

    // The corrected resubmission completes; a client retry converges.
    let request = AgentClientTaskResultRequest {
        task: HUMAN_TASK.to_string(),
        result: json!({ "answer": "approved" }),
        definition: TASK_DEFINITION.to_string(),
        definition_version: 1,
        result_schema: "order-result".to_string(),
        result_schema_version: 1,
        deduplication_key: Some("client-good".to_string()),
        ..AgentClientTaskResultRequest::default()
    };
    let completed = client
        .submit_task_result(AgentClientTaskResultRequest {
            result: request.result.clone(),
            deduplication_key: request.deduplication_key.clone(),
            ..request.clone()
        })
        .await
        .expect("the submission completes");
    assert_eq!(completed.state, AgentClientTaskState::Completed);
    let replay = client
        .submit_task_result(request)
        .await
        .expect("the replay converges");
    assert_eq!(replay.state, AgentClientTaskState::Completed);

    // An agent-owned target is a non-committing refusal.
    let created = client
        .create_task(AgentClientTaskRequest {
            input: json!({ "order": 7 }),
            agent: Some(AGENT.to_string()),
            task_definition: Some(TASK_DEFINITION.to_string()),
            deduplication_key: Some("agent-task".to_string()),
            ..AgentClientTaskRequest::default()
        })
        .await
        .expect("the agent task creates");
    match client
        .submit_task_result(AgentClientTaskResultRequest {
            task: created.task.as_str().to_string(),
            result: json!({ "answer": "x" }),
            definition: TASK_DEFINITION.to_string(),
            definition_version: 1,
            result_schema: "order-result".to_string(),
            result_schema_version: 1,
            deduplication_key: Some("client-wrong-door".to_string()),
            ..AgentClientTaskResultRequest::default()
        })
        .await
    {
        Err(rakka_agent::AgentClientError::Refused { code, .. }) => {
            assert_eq!(code, "task-not-human-owned");
        }
        other => panic!("the ownership refusal must surface: {other:?}"),
    }
}
