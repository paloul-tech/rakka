//! The `A2aIngress` and `A2aEgress` guardrail boundaries over a real service.
//!
//! Ingress evaluates inside `RakkaAgentA2AService`, at the authorized leaf
//! every public content entry reaches, so an in-process caller is covered
//! whether it enters through `send` or `send_message`; egress evaluates in
//! the two in-process send executors before the service sees the message.
//! Specification 16 (spec section 6.3 of the Phase 7 design).

#![cfg(feature = "agents")]

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest, SendMessageResponse};
use async_trait::async_trait;
use serde_json::{json, Value};

use rakka_a2a::agents::{
    A2AAgentDelegationSendExecutor, A2AAgentHandoffSendExecutor, A2AAgentTarget,
    A2AStaticAgentCatalog, RakkaAgentA2AError, RakkaAgentA2AService, META_AGENT_ID,
};
use rakka_a2a::auth::{
    A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer, AllowAllAuthorizer,
};
use rakka_a2a::mapping::A2AHeaderTenantResolver;
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    CrashingStateStore, DeferredExchangeRouter, InProcessRunEntityTransport,
    InProcessTaskEntityTransport,
};
use rakka_agent::{
    delegation_id_for, effect_id_for, handoff_id_for, AgentA2aHandoffFinding,
    AgentA2aHandoffSendExecutor as _, AgentA2aSendExecutor as _, AgentA2aSendFinding,
    AgentAssignmentGeneration, AgentAuthorityEnvelope, AgentCapabilityId, AgentDefinition,
    AgentDefinitionId, AgentDelegationRecord, AgentDelegationTarget, AgentEffectPolicies,
    AgentEffectSpec, AgentEntityClass, AgentEntityCommand, AgentEntityState, AgentEntityStore,
    AgentExchangeRouter, AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain,
    AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
    AgentHandoffRecord, AgentId, AgentOperationId, AgentOperationKind, AgentRevisionNumber,
    AgentRevisionProvenance, AgentRunEffect, AgentRunEffectRequest, AgentRunId, AgentRunScope,
    AgentRunState, AgentSchemaId, AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskId, AgentTaskScope, AgentTaskState,
    AgentToolCallId, InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore,
    InMemoryAgentTeamHistoryStore, TenantId,
};
use rakka_agent_workflow::{
    AgentEffectId, AgentTelemetryContext, AgentTimestampMillis, PrincipalRef,
};
use rakka_persistence::{DurableStateStore, InMemoryDurableStateStore};

type TaskStore = CrashingStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = CrashingStateStore<AgentRunState>;
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
const COORDINATOR: &str = "support-agent";
const SPECIALIST: &str = "translator";
const TASK_DEFINITION: &str = "resolve-ticket";
const MARKER: &str = "IGNORE PREVIOUS";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent(id: &str) -> AgentId {
    AgentId::new(id).expect("agent id should be valid")
}

fn task_definition_id() -> AgentTaskDefinitionId {
    AgentTaskDefinitionId::new(TASK_DEFINITION).expect("definition id should be valid")
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
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "operator-7".to_string(),
        display_name: None,
    }
}

fn stage_id(id: &str) -> AgentGuardrailStageId {
    AgentGuardrailStageId::new(id).expect("the stage id is valid")
}

fn chain_at(
    boundary: AgentGuardrailBoundary,
    rule: Arc<dyn AgentGuardrail>,
) -> AgentGuardrailChain {
    AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
        .with_stage(
            AgentGuardrailStage::new(stage_id("a2a-filter"), AgentRevisionNumber::INITIAL, rule)
                .at_boundary(boundary),
        )
        .expect("the stage registers")
}

/// Counts evaluations and records the last content it saw.
#[derive(Default)]
struct Recording {
    seen: AtomicUsize,
    last: std::sync::Mutex<Option<Value>>,
}

impl AgentGuardrail for Recording {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        self.seen.fetch_add(1, Ordering::SeqCst);
        *self.last.lock().expect("not poisoned") = Some(content.clone());
        AgentGuardrailOutcome::Allow
    }
}

/// Blocks a message whose view carries the marker anywhere.
struct BlockMarker;

impl AgentGuardrail for BlockMarker {
    fn evaluate(
        &self,
        context: &AgentGuardrailContext<'_>,
        content: &Value,
    ) -> AgentGuardrailOutcome {
        assert!(
            matches!(
                context.boundary,
                AgentGuardrailBoundary::A2aIngress | AgentGuardrailBoundary::A2aEgress
            ),
            "an A2A stage runs only at the two A2A boundaries"
        );
        assert_eq!(context.subject.tenant().as_str(), TENANT);
        if content.to_string().contains(MARKER) {
            AgentGuardrailOutcome::Block {
                reason_code: "prompt-injection".to_string(),
                evidence: None,
            }
        } else {
            AgentGuardrailOutcome::Allow
        }
    }
}

/// Replaces every part with one redacted data part.
struct RedactParts;

impl AgentGuardrail for RedactParts {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut redacted = content.clone();
        redacted["parts"] = serde_json::to_value(vec![Part {
            content: PartContent::Data(json!({ "ticket": "[redacted]" })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }])
        .expect("parts encode");
        AgentGuardrailOutcome::Transform {
            content: redacted,
            reason_code: "parts-redacted".to_string(),
        }
    }
}

struct DenyAll;

#[async_trait]
impl A2AAuthorizer for DenyAll {
    async fn authorize(&self, _: &A2AAuthorizationRequest<'_>) -> A2AAuthorizationDecision {
        A2AAuthorizationDecision::Deny
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
    service: Arc<Service>,
}

impl Fixture {
    fn new(ingress: Option<AgentGuardrailChain>, authorizer: Arc<dyn A2AAuthorizer>) -> Self {
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

        let catalog = A2AStaticAgentCatalog::new()
            .with_target(A2AAgentTarget::new(agent(COORDINATOR), task_definition()))
            .with_target(A2AAgentTarget::new(agent(SPECIALIST), task_definition()));
        let mut service = Service::new(
            tasks.clone(),
            agents.clone(),
            history,
            runs,
            TeamStore::default(),
            InMemoryAgentTeamHistoryStore::new(),
            ConversationStore::default(),
            rakka_agent::InMemoryAgentConversationHistoryStore::new(),
            router,
            Arc::new(catalog),
            Arc::new(InMemoryA2ATaskProjectionStore::local()),
            Arc::new(A2AHeaderTenantResolver),
            authorizer,
        )
        .with_clock(Arc::new(TestClock(clock)))
        .with_default_tenant(TENANT);
        if let Some(chain) = ingress {
            service = service.with_ingress_guardrails(Arc::new(chain));
        }
        Self {
            tasks,
            agents,
            service: Arc::new(service),
        }
    }

    async fn instantiate(&self, id: &str) {
        let scope = AgentScope::new(tenant(), agent(id)).expect("agent scope should be valid");
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.task_definitions.insert(task_definition_id());
        let definition = AgentDefinition::new(
            AgentDefinitionId::new(format!("{id}-v1")).expect("definition id should be valid"),
            "Resolves customer support tickets end to end.",
            envelope,
        )
        .expect("the agent definition should be valid");
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
                    principal: principal(),
                    accepted_at: AgentTimestampMillis::new(1),
                    causation_id: rakka_agent_workflow::AgentCausationId::new("cause-1"),
                    audit_ref: rakka_agent_workflow::AgentAuditEventId::new("audit-1"),
                }),
            })
            .await
            .expect("the agent should instantiate");
    }

    /// Whether a durable task record exists under the given id.
    async fn task_exists(&self, task: &AgentTaskId) -> bool {
        let scope = AgentTaskScope::new(tenant(), task.clone()).expect("task scope");
        self.tasks
            .load(&scope.persistence_id())
            .await
            .expect("the task state loads")
            .is_some()
    }

    /// The durable task record, JSON-encoded, for content assertions.
    async fn task_state_json(&self, task_id: &str) -> String {
        let scope = AgentTaskScope::new(tenant(), AgentTaskId::new(task_id).expect("task id"))
            .expect("task scope");
        let held = self
            .tasks
            .load(&scope.persistence_id())
            .await
            .expect("the task state loads")
            .expect("the task exists");
        serde_json::to_string(&held.state).expect("the state encodes")
    }
}

fn task_message(message_id: &str, ticket: &str) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "ticket": ticket })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = message_id.to_string();
    message
}

fn send_request(message: &Message) -> SendMessageRequest {
    // The catalog serves two agents, so a creation send must name the one it
    // addresses; the specialist is the delegation/handoff target the egress
    // proofs send *to*.
    SendMessageRequest {
        message: message.clone(),
        configuration: None,
        metadata: Some(std::collections::HashMap::from([(
            META_AGENT_ID.to_string(),
            Value::String(COORDINATOR.to_string()),
        )])),
        tenant: Some(TENANT.to_string()),
    }
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

/// The task id a send *would* create, derived the way the service derives it:
/// the same normalization over the same inputs, so an assertion that no task
/// exists names the identity the blocked send would have used.
fn derived_task_id(message: &Message) -> AgentTaskId {
    let request = send_request(message);
    // The service merges request- and message-level metadata before
    // normalizing; these messages carry none of their own, so the merge is
    // the request map verbatim.
    assert!(
        request.message.metadata.is_none(),
        "the merged metadata is the request's alone"
    );
    rakka_a2a::agents::ingress::normalize_agent_send(
        &A2AHeaderTenantResolver,
        Some(TENANT),
        &params(),
        request.tenant.as_deref(),
        &request.message,
        &request.metadata.clone().unwrap_or_default(),
    )
    .expect("the send normalizes")
    .task
}

fn task_id_of(response: &SendMessageResponse) -> String {
    match response {
        SendMessageResponse::Task(task) => task.id.clone(),
        other => panic!("a new task was expected, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Ingress
// ---------------------------------------------------------------------------

/// A blocked message is refused before any task exists: the refusal carries
/// `guardrail-blocked`, a re-send of the same message is refused again (no
/// task was created under it), and a clean message still creates a task.
#[tokio::test]
async fn an_ingress_block_refuses_the_send_and_creates_nothing() {
    let fixture = Fixture::new(
        Some(chain_at(
            AgentGuardrailBoundary::A2aIngress,
            Arc::new(BlockMarker),
        )),
        Arc::new(AllowAllAuthorizer),
    );
    fixture.instantiate(COORDINATOR).await;

    let poisoned = task_message("m-1", MARKER);
    let poisoned_task = derived_task_id(&poisoned);
    for _ in 0..2 {
        let error = fixture
            .service
            .send(&params(), &send_request(&poisoned))
            .await
            .expect_err("the marker is blocked");
        assert!(
            matches!(&error, RakkaAgentA2AError::Refused { code, .. } if code == "guardrail-blocked"),
            "got {error:?}"
        );
    }
    // The second refusal alone proves nothing — the block precedes dedup, so
    // it would answer the same way over a task the first send had created.
    // What the claim needs is the store: nothing exists under the identity
    // the send would have used.
    assert!(
        !fixture.task_exists(&poisoned_task).await,
        "a blocked send creates no task under {poisoned_task}"
    );

    let clean_message = task_message("m-2", "hello");
    let clean_task = derived_task_id(&clean_message);
    let clean = fixture
        .service
        .send(&params(), &send_request(&clean_message))
        .await
        .expect("a clean message creates a task");
    // The derivation is the service's own: an admitted send lands under the
    // id it produces, so the absence asserted above is an absence at the
    // right address and not at an invented one.
    assert_eq!(task_id_of(&clean), clean_task.as_str());
    assert!(
        fixture.task_exists(&clean_task).await,
        "an admitted send does create the task"
    );
}

/// A transformed message is what the task records: the durable task state
/// holds the redacted input and never the original.
#[tokio::test]
async fn an_ingress_transform_is_what_the_task_records() {
    let fixture = Fixture::new(
        Some(chain_at(
            AgentGuardrailBoundary::A2aIngress,
            Arc::new(RedactParts),
        )),
        Arc::new(AllowAllAuthorizer),
    );
    fixture.instantiate(COORDINATOR).await;

    let response = fixture
        .service
        .send(&params(), &send_request(&task_message("m-1", MARKER)))
        .await
        .expect("the transformed message is admitted");
    let state = fixture.task_state_json(&task_id_of(&response)).await;
    assert!(state.contains("[redacted]"), "{state}");
    assert!(!state.contains(MARKER), "{state}");
}

/// `send` and `send_message` each evaluate the ingress stage exactly once per
/// request, and `send` does not evaluate a second time in the leaf it routes to.
#[tokio::test]
async fn send_and_send_message_each_evaluate_ingress_exactly_once() {
    let recording = Arc::new(Recording::default());
    let fixture = Fixture::new(
        Some(chain_at(
            AgentGuardrailBoundary::A2aIngress,
            recording.clone(),
        )),
        Arc::new(AllowAllAuthorizer),
    );
    fixture.instantiate(COORDINATOR).await;

    fixture
        .service
        .send(&params(), &send_request(&task_message("m-1", "one")))
        .await
        .expect("send admits");
    assert_eq!(recording.seen.load(Ordering::SeqCst), 1);
    let last = recording
        .last
        .lock()
        .expect("not poisoned")
        .clone()
        .expect("content seen");
    assert_eq!(last["kind"], "a2a-ingress");
    assert!(
        last.to_string().contains("one"),
        "the view carries the parts: {last}"
    );

    fixture
        .service
        .send_message(&params(), &send_request(&task_message("m-2", "two")))
        .await
        .expect("send_message admits");
    assert_eq!(recording.seen.load(Ordering::SeqCst), 2);
}

/// A caller the authorizer denies never reaches a stage.
#[tokio::test]
async fn a_denied_caller_never_reaches_an_ingress_stage() {
    let recording = Arc::new(Recording::default());
    let fixture = Fixture::new(
        Some(chain_at(
            AgentGuardrailBoundary::A2aIngress,
            recording.clone(),
        )),
        Arc::new(DenyAll),
    );
    fixture.instantiate(COORDINATOR).await;

    let error = fixture
        .service
        .send(&params(), &send_request(&task_message("m-1", MARKER)))
        .await
        .expect_err("denied");
    assert!(
        matches!(error, RakkaAgentA2AError::Unauthorized),
        "got {error:?}"
    );
    assert_eq!(recording.seen.load(Ordering::SeqCst), 0);
}

/// A service with no chain declares none and admits as before.
#[tokio::test]
async fn a_service_without_a_chain_declares_none_and_admits_as_before() {
    let fixture = Fixture::new(None, Arc::new(AllowAllAuthorizer));
    fixture.instantiate(COORDINATOR).await;
    assert!(fixture.service.ingress_guardrail_declaration().is_none());
    fixture
        .service
        .send(&params(), &send_request(&task_message("m-1", MARKER)))
        .await
        .expect("nothing evaluates, nothing refuses");
}

// ---------------------------------------------------------------------------
// Egress, and ingress as an in-process executor sees it
// ---------------------------------------------------------------------------

fn parent_run() -> AgentRunScope {
    AgentRunScope::new(
        tenant(),
        agent(COORDINATOR),
        AgentRunId::new("parent-run-1").expect("run id should be valid"),
    )
    .expect("run scope should be valid")
}

fn delegation_record(input: Value) -> AgentDelegationRecord {
    let parent_run = parent_run();
    let delegation = delegation_id_for(&parent_run, 9, 0).expect("the delegation id derives");
    AgentDelegationRecord {
        environments: Default::default(),
        knowledge_spaces: Default::default(),
        a2a_message_id: delegation.as_str().to_string(),
        deduplication_key: delegation.as_str().to_string(),
        delegation,
        goal: None,
        parent_task: AgentTaskId::new("parent-task").expect("task id should be valid"),
        parent_run: parent_run.clone(),
        lineage: Vec::new(),
        ancestors: Vec::new(),
        depth: 1,
        requested_skill: AgentCapabilityId::new("translate")
            .expect("capability id should be valid"),
        resolved: AgentDelegationTarget::new(agent(SPECIALIST), task_definition_id()),
        turn: 9,
        slot: 0,
        effect: effect_id_for(&parent_run, 9, 0).expect("the effect id derives"),
        call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
        input: AgentTaskContent::inline(input).expect("the input is inline-bounded"),
        result_schema: None,
        budget: None,
        granted_descendants: None,
        deadline: None,
        definition_revision: AgentRevisionNumber::new(1),
        settings_revision: AgentRevisionNumber::new(1),
        telemetry: AgentTelemetryContext::default(),
        created_at: AgentTimestampMillis::new(1),
    }
}

fn delegation_intent(record: &AgentDelegationRecord) -> AgentRunEffect {
    AgentRunEffect::new(
        &parent_run(),
        record.turn,
        record.slot,
        AgentRunEffectRequest::A2aSend {
            delegation: Box::new(record.clone()),
        },
        &AgentEffectSpec::idempotent(3).expect("the spec is valid"),
        AgentRevisionNumber::new(1),
        AgentTimestampMillis::new(1),
    )
    .expect("the intent builds")
}

fn handoff_record(reason: &str) -> AgentHandoffRecord {
    let scope = parent_run();
    let handoff = handoff_id_for(&scope, 7, 0).expect("the handoff id derives");
    AgentHandoffRecord {
        handoff: handoff.clone(),
        goal: None,
        task: AgentTaskId::new("parent-task").expect("task id should be valid"),
        source_run: scope,
        source_generation: AgentAssignmentGeneration::new(1),
        requested_skill: AgentCapabilityId::new("translate")
            .expect("capability id should be valid"),
        resolved: AgentDelegationTarget::new(agent(SPECIALIST), task_definition_id()),
        reason: reason.to_string(),
        policy_revision: AgentRevisionNumber::INITIAL,
        definition_revision: AgentRevisionNumber::INITIAL,
        settings_revision: AgentRevisionNumber::INITIAL,
        context: Vec::new(),
        a2a_message_id: handoff.as_str().to_string(),
        deduplication_key: handoff.as_str().to_string(),
        turn: 7,
        slot: 0,
        effect: AgentEffectId::new("effect-1"),
        call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
        telemetry: Default::default(),
        created_at: AgentTimestampMillis::new(1),
    }
}

fn handoff_intent(record: &AgentHandoffRecord) -> AgentRunEffect {
    let request: AgentRunEffectRequest =
        serde_json::from_value(json!({ "a2a-handoff": { "handoff": record } }))
            .expect("the request round-trips");
    let spec = AgentEffectPolicies::default().spec_for(&request).clone();
    AgentRunEffect::new(
        &parent_run(),
        7,
        0,
        request,
        &spec,
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the intent builds")
}

/// The host's case: no handler is mounted, the delegation executor delivers
/// in-process through `send_message`, and the service's ingress chain still
/// refuses — as a determinate `Refused` finding under `guardrail-blocked`.
#[tokio::test]
async fn an_ingress_block_reaches_an_in_process_executor_as_a_refused_finding() {
    let fixture = Fixture::new(
        Some(chain_at(
            AgentGuardrailBoundary::A2aIngress,
            Arc::new(BlockMarker),
        )),
        Arc::new(AllowAllAuthorizer),
    );
    fixture.instantiate(COORDINATOR).await;
    fixture.instantiate(SPECIALIST).await;
    let executor = A2AAgentDelegationSendExecutor::new(fixture.service.clone());

    let record = delegation_record(json!({ "text": MARKER }));
    let finding = executor
        .execute(&parent_run(), &delegation_intent(&record), &record, None)
        .await
        .expect("a block is a finding, not a transport error");
    assert!(
        matches!(&finding, AgentA2aSendFinding::Refused { code, .. } if code == "guardrail-blocked"),
        "got {finding:?}"
    );
}

/// An egress block refuses the delegation before the service sees the message.
#[tokio::test]
async fn an_egress_block_refuses_the_delegation_send_before_the_service_sees_it() {
    let recording = Arc::new(Recording::default());
    let fixture = Fixture::new(
        Some(chain_at(
            AgentGuardrailBoundary::A2aIngress,
            recording.clone(),
        )),
        Arc::new(AllowAllAuthorizer),
    );
    fixture.instantiate(COORDINATOR).await;
    fixture.instantiate(SPECIALIST).await;
    let executor = A2AAgentDelegationSendExecutor::new(fixture.service.clone())
        .with_egress_guardrails(Arc::new(chain_at(
            AgentGuardrailBoundary::A2aEgress,
            Arc::new(BlockMarker),
        )));

    let record = delegation_record(json!({ "text": MARKER }));
    let finding = executor
        .execute(&parent_run(), &delegation_intent(&record), &record, None)
        .await
        .expect("a block is a finding");
    assert!(
        matches!(&finding, AgentA2aSendFinding::Refused { code, .. } if code == "guardrail-blocked"),
        "got {finding:?}"
    );
    assert_eq!(
        recording.seen.load(Ordering::SeqCst),
        0,
        "the service never saw the message"
    );
}

/// An egress transform is what the service receives: the child task's durable
/// state holds the redacted input and never the original.
#[tokio::test]
async fn an_egress_transform_is_what_the_service_receives() {
    let fixture = Fixture::new(None, Arc::new(AllowAllAuthorizer));
    fixture.instantiate(COORDINATOR).await;
    fixture.instantiate(SPECIALIST).await;
    let executor = A2AAgentDelegationSendExecutor::new(fixture.service.clone())
        .with_egress_guardrails(Arc::new(chain_at(
            AgentGuardrailBoundary::A2aEgress,
            Arc::new(RedactParts),
        )));

    let record = delegation_record(json!({ "text": MARKER }));
    let finding = executor
        .execute(&parent_run(), &delegation_intent(&record), &record, None)
        .await
        .expect("the transformed send executes");
    let AgentA2aSendFinding::Sent { child_task, .. } = finding else {
        panic!("the send creates the child, got {finding:?}");
    };
    let state = fixture.task_state_json(child_task.as_str()).await;
    assert!(state.contains("[redacted]"), "{state}");
    assert!(!state.contains(MARKER), "{state}");
}

/// The handoff executor evaluates the cluster's free text too: a reason
/// carrying the marker is blocked before any send.
#[tokio::test]
async fn an_egress_block_refuses_the_handoff_send() {
    let recording = Arc::new(Recording::default());
    let fixture = Fixture::new(
        Some(chain_at(
            AgentGuardrailBoundary::A2aIngress,
            recording.clone(),
        )),
        Arc::new(AllowAllAuthorizer),
    );
    fixture.instantiate(COORDINATOR).await;
    fixture.instantiate(SPECIALIST).await;
    let executor =
        A2AAgentHandoffSendExecutor::new(fixture.service.clone()).with_egress_guardrails(Arc::new(
            chain_at(AgentGuardrailBoundary::A2aEgress, Arc::new(BlockMarker)),
        ));

    let record = handoff_record(MARKER);
    let finding = executor
        .execute(&parent_run(), &handoff_intent(&record), &record, None)
        .await
        .expect("a block is a finding");
    assert!(
        matches!(&finding, AgentA2aHandoffFinding::Refused { code, .. } if code == "guardrail-blocked"),
        "got {finding:?}"
    );
    assert_eq!(recording.seen.load(Ordering::SeqCst), 0);
}
