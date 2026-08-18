//! The team cluster on the agents A2A surface
//! ([specification 8.10 and 14.4](../../docs/plans/rakka-agent/spec.md),
//! scenario 42's wire half): board commands ride the collaboration
//! extension as durable deduplicated commands over the real service core,
//! stale commands answer structured refusals to rebase on, half-formed
//! engagements fail closed, and the command authorizes under its own
//! operation class — never an undifferentiated send.

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
    DeferredExchangeRouter, InProcessRunEntityTransport, InProcessTaskEntityTransport,
    InProcessTeamEntityTransport,
};
use rakka_agent::{
    AgentAssignmentGeneration, AgentAssignmentStatus, AgentAuthorityEnvelope, AgentDefinition,
    AgentDefinitionId, AgentEntityClass, AgentEntityCommand, AgentEntityState, AgentEntityStore,
    AgentExchangeRouter, AgentGoalId, AgentId, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunState, AgentScope, AgentSettings,
    AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId,
    AgentTaskEntityCommand, AgentTaskEntityStore, AgentTaskId, AgentTaskScope, AgentTaskState,
    AgentTeamBoardEntryStatus, AgentTeamCreation, AgentTeamEntityCommand, AgentTeamEntityStore,
    AgentTeamId, AgentTeamPolicy, AgentTeamScope, AgentTeamState, InMemoryAgentRunEffectSink,
    InMemoryAgentTaskHistoryStore, InMemoryAgentTeamHistoryStore, TenantId,
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
    InMemoryAgentTeamHistoryStore,
    ConversationStore,
    rakka_agent::InMemoryAgentConversationHistoryStore,
>;

const TENANT: &str = "acme";
const TEAM: &str = "support-team";
const MEMBER_A: &str = "worker-a";
const MEMBER_B: &str = "worker-b";
const TASK: &str = "board-ticket-1";
const TASK_DEFINITION: &str = "resolve-ticket";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn member(name: &str) -> AgentId {
    AgentId::new(name).expect("the member id is valid")
}

fn team_scope() -> AgentTeamScope {
    AgentTeamScope::new(
        tenant(),
        AgentTeamId::new(TEAM).expect("the team id is valid"),
    )
    .expect("the team scope is valid")
}

fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(TASK).expect("the task id is valid"),
    )
    .expect("the task scope is valid")
}

fn schema(name: &str) -> rakka_agent::AgentSchemaRef {
    rakka_agent::AgentSchemaRef::new(
        rakka_agent::AgentSchemaId::new(name).expect("the schema id is valid"),
        AgentRevisionNumber::INITIAL,
    )
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

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
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
    tasks: TaskStore,
    agents: AgentStore,
    history: InMemoryAgentTaskHistoryStore,
    teams: TeamStore,
    team_history: InMemoryAgentTeamHistoryStore,
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
        let history = InMemoryAgentTaskHistoryStore::new();
        let team_history = InMemoryAgentTeamHistoryStore::new();
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
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Task, Arc::new(task_transport))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport))
            .with_route(AgentEntityClass::Team, Arc::new(team_transport));
        deferred.install(router.clone());

        let catalog = A2AStaticAgentCatalog::new()
            .with_target(A2AAgentTarget::new(member(MEMBER_A), task_definition()));
        let service = Arc::new(
            Service::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                runs,
                teams.clone(),
                team_history.clone(),
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
            teams,
            team_history,
            router,
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
            .insert(AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id"));
        // The claim's assignment door reads this: board membership is trusted
        // wiring, and the envelope is the authority a claim spends.
        envelope
            .coordination_capabilities
            .insert(rakka_agent::AgentCoordinationCapabilityKind::Team);
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
            "One collaborating team member.",
            envelope,
        )
        .expect("the agent definition is valid");
        let scope = AgentScope::new(tenant(), agent.clone()).expect("the agent scope is valid");
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

    /// The board world the wire claims against: member agents, the team with
    /// its posted entry, and the board task entity — trusted application
    /// wiring, exactly as production creates them.
    async fn board_world(&self) {
        self.instantiate(&member(MEMBER_A)).await;
        self.instantiate(&member(MEMBER_B)).await;

        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        task.recover(now).await.expect("the task recovers");
        task.apply(
            AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, TASK, "1"],
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentTaskCreation {
                    definition: task_definition(),
                    input: AgentTaskContent::inline(json!({ "ticket": 1 }))
                        .expect("the input is inline-bounded"),
                    assignee: None,
                    team: Some(AgentTeamId::new(TEAM).expect("the team id is valid")),
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
        .expect("the board task creates");

        let mut members = std::collections::BTreeMap::new();
        members.insert(member(MEMBER_A), std::collections::BTreeSet::new());
        members.insert(member(MEMBER_B), std::collections::BTreeSet::new());
        let mut team =
            AgentTeamEntityStore::new(team_scope(), self.teams.clone(), self.team_history.clone());
        let now = self.now();
        team.recover(now).await.expect("the team recovers");
        team.apply(
            AgentTeamEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TeamOperation,
                    [TENANT, TEAM, "create"],
                )
                .expect("the operation id derives"),
                creation: Box::new(AgentTeamCreation {
                    leader: member(MEMBER_A),
                    root_goal: AgentGoalId::new("quarterly-support").expect("the goal id"),
                    policy: AgentTeamPolicy::new(AgentRevisionNumber::INITIAL),
                    members,
                }),
            },
            &self.router,
            self.now(),
        )
        .await
        .expect("the team creates");
        team.apply(
            AgentTeamEntityCommand::PostTask {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TeamOperation,
                    [TENANT, TEAM, "post"],
                )
                .expect("the operation id derives"),
                task: task_scope().task().clone(),
                posted_by: member(MEMBER_A),
            },
            &self.router,
            self.now(),
        )
        .await
        .expect("the post applies");
    }

    /// The courier duty between wire calls: settle passes over the team and
    /// task entities until the round trip quiesces.
    async fn settle_rounds(&self) {
        for _round in 0..4 {
            let mut team = AgentTeamEntityStore::new(
                team_scope(),
                self.teams.clone(),
                self.team_history.clone(),
            );
            let now = self.now();
            if team.recover(now).await.is_ok() {
                let _ = team.settle_side_effects(&self.router, self.now()).await;
            }
            let mut task = AgentTaskEntityStore::new(
                task_scope(),
                self.tasks.clone(),
                self.agents.clone(),
                self.history.clone(),
            );
            let now = self.now();
            if task.recover(now).await.is_ok() {
                let _ = task.settle_side_effects(&self.router, self.now()).await;
            }
        }
    }

    async fn team_snapshot(&self) -> rakka_agent::AgentTeamSnapshot {
        let mut team =
            AgentTeamEntityStore::new(team_scope(), self.teams.clone(), self.team_history.clone());
        let now = self.now();
        team.recover(now).await.expect("the team recovers");
        team.snapshot()
            .expect("the team state reads")
            .expect("the team exists")
    }

    async fn task_snapshot(&self) -> rakka_agent::AgentTaskSnapshot {
        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        task.recover(now).await.expect("the task recovers");
        task.snapshot()
            .expect("the task state reads")
            .expect("the task exists")
    }
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
}

/// A team command crafted at the wire level: the collaboration extension's
/// third cluster shape, discriminated by its `team` field.
fn team_message(message_id: &str, cluster: Value) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "team": TEAM })),
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

fn claim_cluster(member: &str, expected_epoch: u64) -> Value {
    json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "team": TEAM,
        "operation": "claim",
        "task": TASK,
        "member": member,
        "expected-epoch": expected_epoch,
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

/// Decodes the immediate team response message into the reply's externally
/// tagged JSON shape.
fn response_payload(response: &a2a::SendMessageResponse) -> Value {
    let a2a::SendMessageResponse::Message(message) = response else {
        panic!("a team command answers with a message, got a task");
    };
    let Some(Part {
        content: PartContent::Data(payload),
        ..
    }) = message.parts.first()
    else {
        panic!("the team response carries a data part");
    };
    payload.clone()
}

/// Scenario 42's wire half: two members claim the same open entry over the
/// A2A surface; the board's compare-and-set admits one, the loser gets a
/// structured stale refusal to rebase on, and the task ends with exactly
/// one accepted generation.
#[tokio::test]
async fn concurrent_wire_claims_admit_one_owner() {
    let fixture = Fixture::new();
    fixture.board_world().await;

    let first = fixture
        .service
        .send(
            &params(),
            &send_request(team_message("claim-a", claim_cluster(MEMBER_A, 0))),
        )
        .await
        .expect("the first claim is served");
    let payload = response_payload(&first);
    assert!(
        payload.get("Applied").is_some(),
        "the first claim applies: {payload}"
    );

    let second = fixture
        .service
        .send(
            &params(),
            &send_request(team_message("claim-b", claim_cluster(MEMBER_B, 0))),
        )
        .await
        .expect("the concurrent claim is served as a structured refusal");
    let payload = response_payload(&second);
    let rejected = payload
        .get("Rejected")
        .expect("the loser gets a structured refusal to rebase on");
    assert_eq!(
        rejected.get("code").and_then(Value::as_str),
        Some("team-claim-stale-epoch")
    );

    fixture.settle_rounds().await;

    let task = fixture.task_snapshot().await;
    assert_eq!(
        task.assignment_generation,
        AgentAssignmentGeneration::new(1),
        "one claim, one generation, one owner"
    );
    let assignment = task.assignment.expect("the assignment stands");
    assert_eq!(assignment.agent, member(MEMBER_A));
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);

    let team = fixture.team_snapshot().await;
    let entry = team
        .board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .expect("the board holds the task");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Active);
    assert_eq!(
        entry.claim.as_ref().expect("the echo stands").member,
        member(MEMBER_A)
    );

    // The replayed wire claim answers the original outcome from the
    // deduplicated inbox — one board decision however often it is sent.
    let replay = fixture
        .service
        .send(
            &params(),
            &send_request(team_message("claim-a", claim_cluster(MEMBER_A, 0))),
        )
        .await
        .expect("the replay is served");
    let payload = response_payload(&replay);
    assert!(
        payload.get("Duplicate").is_some(),
        "the replay answers from the operation log: {payload}"
    );
}

/// The wire fail-closed matrix: every half-formed engagement of the team
/// cluster refuses before anything durable happens.
#[tokio::test]
async fn wire_level_team_sends_fail_closed() {
    let fixture = Fixture::new();
    fixture.board_world().await;

    // The reserved key without the extension declaration.
    let mut undeclared = team_message("undeclared", claim_cluster(MEMBER_A, 0));
    undeclared.extensions = None;
    let error = fixture
        .service
        .send(&params(), &send_request(undeclared))
        .await
        .expect_err("the undeclared engagement fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // A team command must not name message.task_id.
    let mut names_task = team_message("names-task", claim_cluster(MEMBER_A, 0));
    names_task.task_id = Some(TASK.to_string());
    let error = fixture
        .service
        .send(&params(), &send_request(names_task))
        .await
        .expect_err("a team send naming a task fails closed");
    let RakkaAgentA2AError::Refused { code, .. } = error else {
        panic!("expected a refusal, got {error:?}");
    };
    assert_eq!(code, "team-send-names-task");

    // An unknown cluster field fails the parse whole.
    let mut unknown = claim_cluster(MEMBER_A, 0);
    unknown["escrow-grant"] = json!(5);
    let error = fixture
        .service
        .send(&params(), &send_request(team_message("unknown", unknown)))
        .await
        .expect_err("an unknown field fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // A payload carrying both discriminators parses as neither.
    let mut both = claim_cluster(MEMBER_A, 0);
    both["handoff"] = json!("handoff-1");
    let error = fixture
        .service
        .send(&params(), &send_request(team_message("both", both)))
        .await
        .expect_err("a two-discriminator payload fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // An unknown verb is outside the bounded vocabulary.
    let mut verb = claim_cluster(MEMBER_A, 0);
    verb["operation"] = json!("disband");
    let error = fixture
        .service
        .send(&params(), &send_request(team_message("verb", verb)))
        .await
        .expect_err("the wire cannot disband a team");
    assert!(matches!(error, RakkaAgentA2AError::Unsupported { .. }));

    // A claim without its epoch expectation is missing a required field.
    let mut epochless = claim_cluster(MEMBER_A, 0);
    epochless
        .as_object_mut()
        .expect("the cluster is an object")
        .remove("expected-epoch");
    let error = fixture
        .service
        .send(
            &params(),
            &send_request(team_message("epochless", epochless)),
        )
        .await
        .expect_err("a claim without an epoch expectation fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Mapping(_)));

    // A membership change without an authenticated principal fails closed.
    let join = json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "team": TEAM,
        "operation": "join",
        "member": "newcomer",
        "expected-lifecycle-revision": 1,
    });
    let error = fixture
        .service
        .send(&params(), &send_request(team_message("join", join)))
        .await
        .expect_err("an unauthenticated membership change fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Mapping(_)));

    // With the principal bound, the same change applies.
    let join = json!({
        "schema": AGENT_COLLABORATION_SCHEMA_VERSION,
        "team": TEAM,
        "operation": "join",
        "member": "newcomer",
        "expected-lifecycle-revision": 1,
    });
    let mut message = team_message("join-authed", join);
    message
        .metadata
        .as_mut()
        .expect("the metadata exists")
        .insert(META_PRINCIPAL_REF.to_string(), json!("user:operator-7"));
    let response = fixture
        .service
        .send(&params(), &send_request(message))
        .await
        .expect("the authenticated join is served");
    let payload = response_payload(&response);
    assert!(
        payload.get("Applied").is_some(),
        "the join applies: {payload}"
    );

    // Nothing above touched the board.
    let team = fixture.team_snapshot().await;
    let entry = team
        .board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .expect("the board holds the task");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Open);
}

/// The command authorizes under its own operation class with the claimed
/// verb bound in — a deployment authorizer that permits ordinary sends can
/// still refuse board commands.
#[tokio::test]
async fn a_team_command_authorizes_under_its_own_operation_class() {
    struct DenyTeamCommands;

    #[async_trait]
    impl A2AAuthorizer for DenyTeamCommands {
        async fn authorize(
            &self,
            request: &A2AAuthorizationRequest<'_>,
        ) -> A2AAuthorizationDecision {
            match request.operation {
                A2AOperation::TeamCommand => {
                    let claim = request
                        .team
                        .as_ref()
                        .expect("the team claim rides the check");
                    assert_eq!(claim.team, TEAM);
                    assert_eq!(claim.operation, "claim");
                    assert_eq!(claim.member, Some(MEMBER_A));
                    A2AAuthorizationDecision::Deny
                }
                _ => A2AAuthorizationDecision::Allow,
            }
        }
    }

    let fixture = Fixture::with_authorizer(Arc::new(DenyTeamCommands));
    fixture.board_world().await;

    let error = fixture
        .service
        .send(
            &params(),
            &send_request(team_message("claim-denied", claim_cluster(MEMBER_A, 0))),
        )
        .await
        .expect_err("the denied command never reaches the entity");
    assert!(matches!(error, RakkaAgentA2AError::Unauthorized));

    let team = fixture.team_snapshot().await;
    let entry = team
        .board
        .iter()
        .find(|entry| &entry.task == task_scope().task())
        .expect("the board holds the task");
    assert_eq!(entry.status, AgentTeamBoardEntryStatus::Open);
    assert_eq!(entry.claim_epoch, 0, "nothing durable happened");
}

/// A plain client send — no extension, no metadata key — is untouched by
/// the team surface: it creates its typed task exactly as before.
#[tokio::test]
async fn a_plain_client_send_is_untouched_by_the_team_surface() {
    let fixture = Fixture::new();
    fixture.instantiate(&member(MEMBER_A)).await;

    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(json!({ "ticket": 7 })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = "plain-1".to_string();
    let response = fixture
        .service
        .send(&params(), &send_request(message))
        .await
        .expect("the plain send is served");
    assert!(
        matches!(response, a2a::SendMessageResponse::Task(_)),
        "a plain send stays a typed task creation"
    );
}
