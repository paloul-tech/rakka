//! The conversation cluster on the agents A2A surface
//! ([specification 8.11 and 14.4](../../docs/plans/rakka-agent/spec.md),
//! scenario 43's wire half): turn-protocol commands ride the collaboration
//! extension as durable deduplicated commands over the real service core, a
//! retried send converges on one recorded turn, half-formed engagements
//! fail closed, domain refusals answer structured rejections to rebase on,
//! and the command authorizes under its own operation class — never an
//! undifferentiated send.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest};
use async_trait::async_trait;
use rakka_a2a::agents::{
    A2AAgentTarget, A2AStaticAgentCatalog, RakkaAgentA2AError, RakkaAgentA2AService,
    AGENT_COLLABORATION_EXTENSION_URI, AGENT_COLLABORATION_SCHEMA_VERSION, META_COLLABORATION,
};
use rakka_a2a::auth::{
    A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer, A2AOperation,
    AllowAllAuthorizer,
};
use rakka_a2a::mapping::{A2AHeaderTenantResolver, META_DEDUPLICATION_KEY, META_PRINCIPAL_REF};
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{
    DeferredExchangeRouter, InProcessConversationEntityTransport, InProcessRunEntityTransport,
    InProcessTaskEntityTransport, InProcessTeamEntityTransport,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentConversationCompletionRule, AgentConversationCreation,
    AgentConversationEntityCommand, AgentConversationEntityStore, AgentConversationId,
    AgentConversationMode, AgentConversationScope, AgentConversationStatus, AgentDefinition,
    AgentDefinitionId, AgentEntityClass, AgentEntityCommand, AgentEntityState, AgentEntityStore,
    AgentExchangeRouter, AgentId, AgentModerationPolicy, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRunState, AgentScope, AgentSettings, AgentTaskDefinition,
    AgentTaskDefinitionId, AgentTaskId, AgentTaskState, AgentTeamState, InMemoryAgentRunEffectSink,
    InMemoryAgentTaskHistoryStore, TenantId,
};
use rakka_agent_workflow::AgentTimestampMillis;
use rakka_persistence::InMemoryDurableStateStore;
use serde_json::{json, Value};

type TaskStore = InMemoryDurableStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = InMemoryDurableStateStore<AgentRunState>;
type TeamStore = InMemoryDurableStateStore<AgentTeamState>;
type ConversationStore = InMemoryDurableStateStore<rakka_agent::AgentConversationState>;
type Service = RakkaAgentA2AService<
    TaskStore,
    AgentStore,
    InMemoryAgentTaskHistoryStore,
    RunStore,
    TeamStore,
    rakka_agent::InMemoryAgentTeamHistoryStore,
    ConversationStore,
    rakka_agent::InMemoryAgentConversationHistoryStore,
>;

const TENANT: &str = "acme";
const CONVERSATION: &str = "design-review";
const MODERATOR: &str = "moderator";
const MEMBER_A: &str = "worker-a";
const MEMBER_B: &str = "worker-b";
const TASK_DEFINITION: &str = "resolve-ticket";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent(name: &str) -> AgentId {
    AgentId::new(name).expect("the agent id is valid")
}

fn conversation_scope() -> AgentConversationScope {
    AgentConversationScope::new(
        tenant(),
        AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
    )
    .expect("the conversation scope is valid")
}

fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id is valid"),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("the task definition is valid")
}

fn schema(name: &str) -> rakka_agent::AgentSchemaRef {
    rakka_agent::AgentSchemaRef::new(
        rakka_agent::AgentSchemaId::new(name).expect("the schema id is valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn provenance(at: u64) -> rakka_agent::AgentRevisionProvenance {
    rakka_agent::AgentRevisionProvenance {
        principal: rakka_agent_workflow::PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "wiring".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: rakka_agent_workflow::AgentCausationId::new(format!("cause-{at}")),
        audit_ref: rakka_agent_workflow::AgentAuditEventId::new(format!("audit-{at}")),
    }
}

struct TestClock(Arc<AtomicU64>);

impl rakka_a2a::agents::A2AAgentClock for TestClock {
    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

struct Fixture {
    metrics: Arc<rakka_core::InMemoryMetricsRecorder>,
    agents: AgentStore,
    conversations: ConversationStore,
    conversation_history: rakka_agent::InMemoryAgentConversationHistoryStore,
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
        let teams = TeamStore::new();
        let conversations = ConversationStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let team_history = rakka_agent::InMemoryAgentTeamHistoryStore::new();
        let conversation_history = rakka_agent::InMemoryAgentConversationHistoryStore::new();
        let effects = InMemoryAgentRunEffectSink::new();
        let clock = Arc::new(AtomicU64::new(1));
        let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());

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
            effects,
            deferred.as_router(),
            clock.clone(),
        );
        let team_transport = InProcessTeamEntityTransport::new(
            teams.clone(),
            team_history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let conversation_transport = InProcessConversationEntityTransport::new(
            conversations.clone(),
            agents.clone(),
            conversation_history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Task, Arc::new(task_transport))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport))
            .with_route(AgentEntityClass::Team, Arc::new(team_transport))
            .with_route(
                AgentEntityClass::Conversation,
                Arc::new(conversation_transport),
            );
        deferred.install(router.clone());

        let catalog = A2AStaticAgentCatalog::new()
            .with_target(A2AAgentTarget::new(agent(MEMBER_A), task_definition()));
        let service = Arc::new(
            Service::new(
                tasks,
                agents.clone(),
                history,
                runs,
                teams,
                team_history,
                conversations.clone(),
                conversation_history.clone(),
                router.clone(),
                Arc::new(catalog),
                Arc::new(InMemoryA2ATaskProjectionStore::local()),
                Arc::new(A2AHeaderTenantResolver),
                authorizer,
            )
            .with_clock(Arc::new(TestClock(clock.clone())))
            .with_default_tenant(TENANT)
            .with_metrics(metrics.clone()),
        );

        Self {
            metrics,
            agents,
            conversations,
            conversation_history,
            router,
            clock,
            service,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    async fn instantiate(&self, agent_id: &AgentId) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope
            .task_definitions
            .insert(AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id"));
        // The turn door reads this: a roster admits a speaker to one
        // conversation, its definition admits it to moderated work at all.
        envelope
            .coordination_capabilities
            .insert(rakka_agent::AgentCoordinationCapabilityKind::Moderation);
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
            "One moderated participant.",
            envelope,
        )
        .expect("the agent definition is valid");
        let scope = AgentScope::new(tenant(), agent_id.clone()).expect("the agent scope is valid");
        let mut store = AgentEntityStore::new(scope.clone(), self.agents.clone());
        store.recover().await.expect("the agent recovers");
        store
            .apply(AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &scope,
                    "1",
                )
                .expect("the operation id derives"),
                definition: Box::new(definition),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(provenance(1)),
            })
            .await
            .expect("the agent instantiates");
    }

    /// The moderated world the wire submits into: the conversation with its
    /// roster and budgets — trusted application wiring, exactly as
    /// production creates it. The wire carries no create operation.
    async fn conversation_world(&self) {
        for participant in [MODERATOR, MEMBER_A, MEMBER_B] {
            self.instantiate(&agent(participant)).await;
        }
        let mut store = AgentConversationEntityStore::new(
            conversation_scope(),
            self.conversations.clone(),
            self.agents.clone(),
            self.conversation_history.clone(),
        );
        let now = self.now();
        store.recover(now).await.expect("the conversation recovers");
        store
            .apply(
                AgentConversationEntityCommand::Create {
                    operation_id: rakka_agent::conversation_create_operation_id(
                        &tenant(),
                        &AgentConversationId::new(CONVERSATION)
                            .expect("the conversation id is valid"),
                    )
                    .expect("the operation id derives"),
                    creation: Box::new(AgentConversationCreation {
                        moderator: agent(MODERATOR),
                        participants: vec![agent(MEMBER_A), agent(MEMBER_B)],
                        mode: AgentConversationMode::RoundRobin,
                        completion: AgentConversationCompletionRule::ModeratorDecides,
                        policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
                        task: AgentTaskId::new("moderated-task").expect("the task id is valid"),
                        tokens: Some(500),
                        max_wall_clock_millis: None,
                        transcript_ref: None,
                    }),
                },
                &self.router,
                self.now(),
            )
            .await
            .expect("the conversation creates");
    }

    /// The same world, moderator-directed: the mode whose turns carry the
    /// wire's `designate` / `close-round` spellings.
    async fn moderator_directed_world(&self) {
        for participant in [MODERATOR, MEMBER_A, MEMBER_B] {
            self.instantiate(&agent(participant)).await;
        }
        let mut store = AgentConversationEntityStore::new(
            conversation_scope(),
            self.conversations.clone(),
            self.agents.clone(),
            self.conversation_history.clone(),
        );
        let now = self.now();
        store.recover(now).await.expect("the conversation recovers");
        store
            .apply(
                AgentConversationEntityCommand::Create {
                    operation_id: rakka_agent::conversation_create_operation_id(
                        &tenant(),
                        &AgentConversationId::new(CONVERSATION)
                            .expect("the conversation id is valid"),
                    )
                    .expect("the operation id derives"),
                    creation: Box::new(AgentConversationCreation {
                        moderator: agent(MODERATOR),
                        participants: vec![agent(MEMBER_A), agent(MEMBER_B)],
                        mode: AgentConversationMode::ModeratorDirected,
                        completion: AgentConversationCompletionRule::ModeratorDecides,
                        policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
                        task: AgentTaskId::new("moderated-task").expect("the task id is valid"),
                        tokens: Some(500),
                        max_wall_clock_millis: None,
                        transcript_ref: None,
                    }),
                },
                &self.router,
                self.now(),
            )
            .await
            .expect("the conversation creates");
    }

    async fn conversation_snapshot(&self) -> rakka_agent::AgentConversationSnapshot {
        let mut store = AgentConversationEntityStore::new(
            conversation_scope(),
            self.conversations.clone(),
            self.agents.clone(),
            self.conversation_history.clone(),
        );
        let now = self.now();
        store.recover(now).await.expect("the conversation recovers");
        store
            .snapshot()
            .expect("the conversation state reads")
            .expect("the conversation exists")
    }
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

/// A conversation command crafted at the wire level: the collaboration
/// extension's fourth cluster shape, discriminated by its `conversation`
/// field.
fn conversation_message(message_id: &str, cluster: Value) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "conversation": CONVERSATION })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = message_id.to_string();
    message.extensions = Some(vec![AGENT_COLLABORATION_EXTENSION_URI.to_string()]);
    message.metadata = Some(
        [
            (
                META_DEDUPLICATION_KEY.to_string(),
                Value::String(message_id.to_string()),
            ),
            (META_COLLABORATION.to_string(), cluster),
        ]
        .into_iter()
        .collect(),
    );
    message
}

fn submit_cluster(participant: &str, round: u64, turn: u32, body: &str, tokens: u64) -> Value {
    json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "conversation": CONVERSATION,
        "operation": "submit-turn",
        "participant": participant,
        "round": round,
        "turn": turn,
        "body": body,
        "tokens-consumed": tokens,
    })
}

/// A moderator turn carrying one of the wire's two direction spellings.
fn directed_cluster(body: &str, designate: Option<&str>, close_round: Option<bool>) -> Value {
    let mut cluster = submit_cluster(MODERATOR, 0, 0, body, 0);
    let object = cluster.as_object_mut().expect("the cluster is an object");
    if let Some(designated) = designate {
        object.insert("designate".to_string(), json!(designated));
    }
    if let Some(close) = close_round {
        object.insert("close-round".to_string(), json!(close));
    }
    cluster.clone()
}

fn end_cluster(participant: &str, expected_round: u64, reason: &str) -> Value {
    json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "conversation": CONVERSATION,
        "operation": "end",
        "participant": participant,
        "expected-round": expected_round,
        "reason": reason,
    })
}

fn send_request(message: Message) -> SendMessageRequest {
    SendMessageRequest {
        message,
        configuration: None,
        metadata: None,
        tenant: Some(TENANT.to_string()),
    }
}

/// Decodes the immediate conversation response message into the reply's
/// externally tagged JSON shape.
fn response_payload(response: &a2a::SendMessageResponse) -> Value {
    let a2a::SendMessageResponse::Message(message) = response else {
        panic!("a conversation command answers with a message, got a task");
    };
    let Some(Part {
        content: PartContent::Data(payload),
        ..
    }) = message.parts.first()
    else {
        panic!("the conversation response carries a data part");
    };
    payload.clone()
}

/// Scenario 43's wire half: a turn submitted over the A2A surface records
/// once, a domain refusal answers a structured rejection to rebase on, and
/// a retried send — whatever its message id — converges on the recorded
/// turn.
#[tokio::test]
async fn a_wire_turn_round_trip_converges_on_one_recorded_turn() {
    let fixture = Fixture::new();
    fixture.conversation_world().await;

    let first = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "turn-1",
                submit_cluster(MEMBER_A, 0, 0, "the proposal", 25),
            )),
        )
        .await
        .expect("the turn is served");
    let payload = response_payload(&first);
    assert!(
        payload.get("Applied").is_some(),
        "the turn applies: {payload}"
    );

    // Out of turn is a domain refusal: a structured rejection, not an
    // error, so the caller can rebase on the current protocol state.
    let out_of_turn = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "turn-2-wrong",
                submit_cluster(MEMBER_A, 0, 1, "speaking again", 0),
            )),
        )
        .await
        .expect("the out-of-turn submit is served as a structured refusal");
    let payload = response_payload(&out_of_turn);
    let rejected = payload
        .get("Rejected")
        .expect("the wrong speaker gets a structured refusal");
    assert_eq!(
        rejected.get("code").and_then(Value::as_str),
        Some("conversation-not-your-turn")
    );

    // The retried send converges on the recorded turn even under a fresh
    // message id: the operation id derives from the turn's own logical
    // coordinates and content, never the wire discriminator.
    let replay = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "turn-1-retry",
                submit_cluster(MEMBER_A, 0, 0, "the proposal", 25),
            )),
        )
        .await
        .expect("the replay is served");
    let payload = response_payload(&replay);
    assert!(
        payload.get("Duplicate").is_some(),
        "the replay answers from the deduplicated inbox: {payload}"
    );

    let snapshot = fixture.conversation_snapshot().await;
    assert_eq!(snapshot.turns.len(), 1, "one turn recorded, ever");
    assert_eq!(
        snapshot.budgets.consumed.tokens, 25,
        "the replayed usage charged once"
    );
    assert_eq!(snapshot.turn_in_round, 1);
}

/// The wire fail-closed matrix: every half-formed engagement of the
/// conversation cluster refuses before anything durable happens.
#[tokio::test]
async fn wire_level_conversation_sends_fail_closed() {
    let fixture = Fixture::new();
    fixture.conversation_world().await;

    // The reserved key without the extension declaration.
    let mut undeclared = conversation_message(
        "undeclared",
        submit_cluster(MEMBER_A, 0, 0, "an opening", 0),
    );
    undeclared.extensions = None;
    let error = fixture
        .service
        .send(&params(), &send_request(undeclared))
        .await
        .expect_err("the undeclared engagement fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // A conversation command must not name message.task_id.
    let mut names_task = conversation_message(
        "names-task",
        submit_cluster(MEMBER_A, 0, 0, "an opening", 0),
    );
    names_task.task_id = Some("moderated-task".to_string());
    let error = fixture
        .service
        .send(&params(), &send_request(names_task))
        .await
        .expect_err("a conversation send naming a task fails closed");
    let RakkaAgentA2AError::Refused { code, .. } = error else {
        panic!("expected a refusal, got {error:?}");
    };
    assert_eq!(code, "conversation-send-names-task");

    // An unknown cluster field fails the parse whole.
    let mut unknown = submit_cluster(MEMBER_A, 0, 0, "an opening", 0);
    unknown["escrow-grant"] = json!(5);
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message("unknown-field", unknown)),
        )
        .await
        .expect_err("an unknown field fails the parse whole");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // Two discriminators fail the send whole rather than parse as either.
    let mut two_shapes = submit_cluster(MEMBER_A, 0, 0, "an opening", 0);
    two_shapes["team"] = json!("support-team");
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message("two-shapes", two_shapes)),
        )
        .await
        .expect_err("a two-discriminator payload fails whole");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // The wire cannot mint a conversation: creation is not a verb.
    let mut create = submit_cluster(MEMBER_A, 0, 0, "an opening", 0);
    create["operation"] = json!("create");
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message("mint", create)),
        )
        .await
        .expect_err("an unknown verb fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // A submit without its claimed speaker is missing a required field,
    // named by its wire key verbatim.
    let mut speakerless = submit_cluster(MEMBER_A, 0, 0, "an opening", 0);
    speakerless
        .as_object_mut()
        .expect("the cluster is an object")
        .remove("participant");
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message("speakerless", speakerless)),
        )
        .await
        .expect_err("a speakerless submit fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Mapping(_)));

    // An early end records who accepted it: unauthenticated fails closed…
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "end-anon",
                end_cluster(MODERATOR, 0, "premature"),
            )),
        )
        .await
        .expect_err("an unauthenticated end fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Mapping(_)));

    // …an end naming no agent fails closed the same way, before anything
    // durable happens: the end's claim is as required as a turn's speaker.
    let mut agentless = end_cluster(MODERATOR, 0, "premature");
    agentless
        .as_object_mut()
        .expect("the cluster is an object")
        .remove("participant");
    let mut agentless = conversation_message("end-agentless", agentless);
    agentless
        .metadata
        .as_mut()
        .expect("the message carries metadata")
        .insert(META_PRINCIPAL_REF.to_string(), json!("user:operator-7"));
    let error = fixture
        .service
        .send(&params(), &send_request(agentless))
        .await
        .expect_err("an end naming no agent fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Mapping(_)));

    // …and an authenticated end claiming a roster participant rather than
    // the moderator is a domain refusal the caller rebases on, not a
    // terminalized conversation.
    let mut impostor = conversation_message("end-impostor", end_cluster("alpha", 0, "i am done"));
    impostor
        .metadata
        .as_mut()
        .expect("the message carries metadata")
        .insert(META_PRINCIPAL_REF.to_string(), json!("user:operator-7"));
    let response = fixture
        .service
        .send(&params(), &send_request(impostor))
        .await
        .expect("the refusal is served as a decision");
    let payload = response_payload(&response);
    assert_eq!(
        payload
            .get("Rejected")
            .and_then(|rejected| rejected.get("code"))
            .and_then(Value::as_str),
        Some("conversation-end-not-moderator"),
        "a non-moderator end refuses: {payload}"
    );

    // Nothing above touched the protocol.
    let snapshot = fixture.conversation_snapshot().await;
    assert!(snapshot.turns.is_empty(), "nothing durable happened");
    assert_eq!(snapshot.status, AgentConversationStatus::Active);

    // …and the same end with an authenticated principal applies.
    let mut authenticated =
        conversation_message("end-auth", end_cluster(MODERATOR, 0, "consensus"));
    authenticated
        .metadata
        .as_mut()
        .expect("the message carries metadata")
        .insert(META_PRINCIPAL_REF.to_string(), json!("user:operator-7"));
    let response = fixture
        .service
        .send(&params(), &send_request(authenticated))
        .await
        .expect("the authenticated end is served");
    let payload = response_payload(&response);
    assert!(
        payload.get("Applied").is_some(),
        "the authenticated end applies: {payload}"
    );
    let snapshot = fixture.conversation_snapshot().await;
    assert_eq!(snapshot.status, AgentConversationStatus::Ended);
}

/// The command is its own operation class at the authorization boundary,
/// with the cluster's claims bound into the request — and a denial leaves
/// nothing durable behind.
#[tokio::test]
async fn a_conversation_command_authorizes_under_its_own_operation_class() {
    struct DenyConversationCommands;

    #[async_trait]
    impl A2AAuthorizer for DenyConversationCommands {
        async fn authorize(
            &self,
            request: &A2AAuthorizationRequest<'_>,
        ) -> A2AAuthorizationDecision {
            match request.operation {
                A2AOperation::ConversationCommand => {
                    let claim = request
                        .conversation
                        .as_ref()
                        .expect("the claimed command rides the request");
                    assert_eq!(claim.conversation, CONVERSATION);
                    assert_eq!(claim.operation, "submit-turn");
                    assert_eq!(claim.participant, Some(MEMBER_A));
                    assert_eq!(claim.round, Some(0));
                    assert_eq!(claim.turn, Some(0));
                    A2AAuthorizationDecision::Deny
                }
                _ => A2AAuthorizationDecision::Allow,
            }
        }
    }

    let fixture = Fixture::with_authorizer(Arc::new(DenyConversationCommands));
    fixture.conversation_world().await;

    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "denied-turn",
                submit_cluster(MEMBER_A, 0, 0, "an opening", 0),
            )),
        )
        .await
        .expect_err("the denied command fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unauthorized));

    let snapshot = fixture.conversation_snapshot().await;
    assert!(snapshot.turns.is_empty(), "the denial left nothing durable");
    assert_eq!(snapshot.turn_in_round, 0);
}

/// An ordinary A2A client that never engages the collaboration extension is
/// untouched by the conversation surface: its send creates a plain task.
#[tokio::test]
async fn a_plain_client_send_is_untouched_by_the_conversation_surface() {
    let fixture = Fixture::new();
    fixture.conversation_world().await;
    fixture.instantiate(&agent(MEMBER_A)).await;

    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "ticket": 7 })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = "plain-send".to_string();
    let response = fixture
        .service
        .send(&params(), &send_request(message))
        .await
        .expect("the plain send is served");
    assert!(
        matches!(response, a2a::SendMessageResponse::Task(_)),
        "an ordinary send creates a task"
    );
}

/// The direction is part of the turn's identity, so the wire's two spellings
/// must reach the entity intact: a mapping that dropped or swapped one would
/// put a turn's operation id out of step with the decision it names.
#[tokio::test]
async fn the_wire_direction_spellings_map_and_bind_the_turn_identity() {
    let fixture = Fixture::new();
    fixture.moderator_directed_world().await;

    // A payload carrying both spellings is a half-formed engagement refused
    // whole, before anything durable happens.
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "both-directions",
                directed_cluster("next", Some(MEMBER_A), Some(true)),
            )),
        )
        .await
        .expect_err("a turn that both designates and closes the round fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));
    assert!(
        fixture.conversation_snapshot().await.turns.is_empty(),
        "nothing durable happened"
    );

    // `designate` reaches the entity as the durable owner fact.
    let response = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "designate",
                directed_cluster("next", Some(MEMBER_A), None),
            )),
        )
        .await
        .expect("the designating turn is served");
    let payload = response_payload(&response);
    assert!(
        payload.get("Applied").is_some(),
        "the designating turn applies: {payload}"
    );
    let snapshot = fixture.conversation_snapshot().await;
    assert_eq!(snapshot.designated, Some(agent(MEMBER_A)));
    assert_eq!(snapshot.current_speaker, Some(agent(MEMBER_A)));

    // The same words with the *other* spelling are a different decision, so
    // they derive a different operation id and the ledger refuses them —
    // rather than the wire absorbing them as a duplicate of the designation.
    let response = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "close-instead",
                directed_cluster("next", None, Some(true)),
            )),
        )
        .await
        .expect("the refusal is served as a decision");
    let payload = response_payload(&response);
    assert_eq!(
        payload
            .get("Rejected")
            .and_then(|rejected| rejected.get("code"))
            .and_then(Value::as_str),
        Some("conversation-turn-content-mismatch"),
        "the regenerated direction refuses loudly: {payload}"
    );

    // The designation the protocol recorded still stands.
    let snapshot = fixture.conversation_snapshot().await;
    assert_eq!(snapshot.designated, Some(agent(MEMBER_A)));
    assert_eq!(snapshot.round, 0, "the refused close-round did not advance");

    // And the identical redelivery under a fresh message id still converges.
    let response = fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "designate-again",
                directed_cluster("next", Some(MEMBER_A), None),
            )),
        )
        .await
        .expect("the redelivery is served");
    let payload = response_payload(&response);
    assert!(
        payload.get("Duplicate").is_some(),
        "the redelivery converges: {payload}"
    );
    assert_eq!(fixture.conversation_snapshot().await.turns.len(), 1);
}

/// The wire is the turn protocol's only production carrier, and it builds its
/// own entity store rather than routing through the sharded entity — so the
/// moderation counter has to be wired here or it stays at zero for every
/// deployment that actually serves turns.
#[tokio::test]
async fn the_wire_records_the_moderation_counter() {
    let fixture = Fixture::new();
    fixture.conversation_world().await;

    fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "counted-turn",
                submit_cluster(MEMBER_A, 0, 0, "opening", 3),
            )),
        )
        .await
        .expect("the turn is served");

    // A refusal counts too, under its own outcome label.
    fixture
        .service
        .send(
            &params(),
            &send_request(conversation_message(
                "counted-refusal",
                submit_cluster(MEMBER_A, 0, 1, "out of turn", 0),
            )),
        )
        .await
        .expect("the refusal is served as a decision");

    let snapshot = fixture.metrics.snapshot();
    let observed: Vec<Vec<(String, String)>> = snapshot
        .observations_named(rakka_agent::METRIC_AGENT_MODERATION_TURNS)
        .into_iter()
        .map(|observation| {
            observation
                .attributes()
                .iter()
                .map(|attribute| (attribute.key().to_string(), attribute.value().to_string()))
                .collect()
        })
        .collect();
    assert!(
        observed.contains(&vec![
            ("operation".to_string(), "turn".to_string()),
            ("outcome".to_string(), "applied".to_string()),
        ]),
        "the applied turn counted: {observed:?}"
    );
    assert!(
        observed.contains(&vec![
            ("operation".to_string(), "turn".to_string()),
            ("outcome".to_string(), "refused".to_string()),
        ]),
        "the refused turn counted: {observed:?}"
    );
}

/// The terminated conversation is observable from its governing task's
/// public projection: the terminal notice records the task-side provenance
/// cell, and the metadata-synced path echoes it under
/// `io.rakka.collaboration` on the read path — which heals a projection
/// written before the conversation ended.
#[tokio::test]
async fn a_terminated_conversation_echoes_on_its_governing_tasks_projection() {
    let fixture = Fixture::new();
    fixture.instantiate(&agent(MEMBER_A)).await;

    // The governing task is created through the public surface, so the
    // projection this test reads is the one production serves — and it is
    // projected *before* the conversation ends, which is exactly the stale
    // record the read-path heal exists for.
    let mut plain = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "ticket": 1 })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    plain.message_id = "governing-task".to_string();
    let response = fixture
        .service
        .send(&params(), &send_request(plain))
        .await
        .expect("the task send is served");
    let a2a::SendMessageResponse::Task(created) = response else {
        panic!("a plain send answers a task");
    };
    let task_id = created.id.clone();

    // The conversation names that task — trusted application wiring — and
    // the moderator ends it over the wire.
    let mut store = AgentConversationEntityStore::new(
        conversation_scope(),
        fixture.conversations.clone(),
        fixture.agents.clone(),
        fixture.conversation_history.clone(),
    );
    let now = fixture.now();
    store.recover(now).await.expect("the conversation recovers");
    store
        .apply(
            AgentConversationEntityCommand::Create {
                operation_id: rakka_agent::conversation_create_operation_id(
                    &tenant(),
                    &AgentConversationId::new(CONVERSATION).expect("the conversation id is valid"),
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentConversationCreation {
                    moderator: agent(MODERATOR),
                    participants: vec![agent(MEMBER_A), agent(MEMBER_B)],
                    mode: AgentConversationMode::RoundRobin,
                    completion: AgentConversationCompletionRule::ModeratorDecides,
                    policy: AgentModerationPolicy::new(AgentRevisionNumber::INITIAL),
                    task: rakka_agent::AgentTaskId::new(&task_id).expect("the task id is valid"),
                    tokens: Some(500),
                    max_wall_clock_millis: None,
                    transcript_ref: None,
                }),
            },
            &fixture.router,
            fixture.now(),
        )
        .await
        .expect("the conversation creates");

    let mut end = conversation_message("end-governed", end_cluster(MODERATOR, 0, "consensus"));
    end.metadata
        .as_mut()
        .expect("the message carries metadata")
        .insert(META_PRINCIPAL_REF.to_string(), json!("user:operator-7"));
    let response = fixture
        .service
        .send(&params(), &send_request(end))
        .await
        .expect("the end is served");
    assert!(
        response_payload(&response).get("Applied").is_some(),
        "the end applies"
    );

    // The read path serves — and heals — the echo beside the task's status.
    let task = fixture
        .service
        .get_task(&params(), Some(TENANT), &task_id, None)
        .await
        .expect("the task reads");
    let collaboration = task
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(META_COLLABORATION))
        .expect("the collaboration echo rides the projection");
    // `conversation-id`, not the bare `conversation`: that name is the
    // inbound cluster discriminator, and the echo must never trip it.
    assert_eq!(
        collaboration.get("conversation-id").and_then(Value::as_str),
        Some(CONVERSATION)
    );
    assert!(
        collaboration.get("conversation").is_none(),
        "the echo keeps clear of the inbound discriminator"
    );
    assert_eq!(
        collaboration
            .get("conversation-status")
            .and_then(Value::as_str),
        Some("ended")
    );
    assert_eq!(
        collaboration
            .get("conversation-reason")
            .and_then(Value::as_str),
        Some("moderator-ended")
    );
    assert_eq!(
        collaboration
            .get("conversation-rounds")
            .and_then(Value::as_u64),
        Some(0),
        "the coordinates ride the echo"
    );
}
