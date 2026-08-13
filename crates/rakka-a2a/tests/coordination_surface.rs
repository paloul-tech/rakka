//! The scoped coordination-event replay and the goal view on the agents A2A
//! surface ([specification 17.13 and 17.18](../../docs/plans/rakka-agent/spec.md),
//! scenario 45's wire half).
//!
//! Both are *reads*, and each is its own operation class at the authorization
//! boundary — a deployment can grant an operator the coordination history of an
//! entity without granting it to every task caller, and can gate a whole goal
//! tree separately from the one task a caller was permitted. Two properties
//! carry the surface: a cursor never crosses a tenant, and a denial is
//! indistinguishable from absence.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use rakka_a2a::agents::{
    A2AAgentTarget, A2AStaticAgentCatalog, RakkaAgentA2AError, RakkaAgentA2AService,
};
use rakka_a2a::auth::{
    A2AAuthorizationDecision, A2AAuthorizationRequest, A2AAuthorizer, A2AOperation,
    AllowAllAuthorizer,
};
use rakka_a2a::mapping::A2AHeaderTenantResolver;
use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
use rakka_agent::testkit::{DeferredExchangeRouter, InProcessTaskEntityTransport};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentCoordinationCursor, AgentCoordinationReplay, AgentDefinition,
    AgentDefinitionId, AgentEntityAddress, AgentEntityClass, AgentEntityCommand, AgentEntityState,
    AgentEntityStore, AgentExchangeRouter, AgentId, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunState, AgentScope, AgentSettings,
    AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId,
    AgentTaskEntityCommand, AgentTaskEntityStore, AgentTaskId, AgentTaskScope, AgentTaskState,
    AgentTeamState, InMemoryAgentTaskHistoryStore, InMemoryAgentTeamHistoryStore, TenantId,
};
use rakka_agent_workflow::AgentTimestampMillis;
use rakka_persistence::InMemoryDurableStateStore;
use serde_json::json;

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
const OTHER_TENANT: &str = "globex";
const AGENT: &str = "support-agent";
const TASK: &str = "ticket-1";
const TASK_DEFINITION: &str = "resolve-ticket";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("the agent id is valid")
}

fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(tenant(), AgentTaskId::new(TASK).expect("the task id"))
        .expect("the task scope is valid")
}

fn task_address() -> AgentEntityAddress {
    AgentEntityAddress::Task(task_scope())
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

fn owner() -> rakka_agent_workflow::PrincipalRef {
    rakka_agent_workflow::PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "goal-owner".to_string(),
        display_name: None,
    }
}

fn goal_spec() -> rakka_agent::AgentGoalSpec {
    rakka_agent::AgentGoalSpec {
        owner: owner(),
        objective: rakka_agent::AgentGoalObjective {
            artifact: None,
            summary: "resolve the ticket".to_string(),
        },
        criteria: rakka_agent::AgentGoalCriteria {
            source: rakka_agent::AgentGoalCriteriaSource::Policy(
                rakka_agent::AgentPolicyRef::new("ticket-resolved").expect("the policy ref"),
            ),
            revision: AgentRevisionNumber::INITIAL,
            digest: None,
        },
        priority: None,
        deadline: None,
        cancellation: None,
        allocation: rakka_agent::AgentBudgetAllocation::unbounded(),
        limits: rakka_agent::AgentBudgetLimits::unbounded(),
        delegation: None,
        fan_in: None,
        exhaustion: Default::default(),
        allowed_skills: Default::default(),
        allowed_tools: Default::default(),
        allowed_workflows: Default::default(),
        knowledge_spaces: Default::default(),
        environments: Default::default(),
        evaluator: None,
        required_evidence: Default::default(),
        escalation: None,
        terminal_decision: None,
        stagnation: None,
        stagnation_policy: Default::default(),
        settings_revision: None,
        policy_revision: None,
    }
}

fn params() -> a2a_server::ServiceParams {
    a2a_server::ServiceParams::new()
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
        let clock = Arc::new(AtomicU64::new(1));

        let deferred = DeferredExchangeRouter::new();
        let task_transport = InProcessTaskEntityTransport::new(
            tasks.clone(),
            agents.clone(),
            history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let router =
            AgentExchangeRouter::new().with_route(AgentEntityClass::Task, Arc::new(task_transport));
        deferred.install(router.clone());

        let catalog = A2AStaticAgentCatalog::new()
            .with_target(A2AAgentTarget::new(agent_id(), task_definition()));
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

    /// A task that has recorded real coordination history: created coordinating
    /// a goal the owner can read back, then assigned to the agent the catalog
    /// hosts.
    async fn coordinated_world(&self) {
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope
            .task_definitions
            .insert(AgentTaskDefinitionId::new(TASK_DEFINITION).expect("the definition id"));
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
            "One hosted agent.",
            envelope,
        )
        .expect("the agent definition is valid");
        let scope = AgentScope::new(tenant(), agent_id()).expect("the agent scope is valid");
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
                    assignee: Some(agent_id()),
                    team: None,
                    goal: None,
                    goal_mode: Default::default(),
                    goal_spec: Some(Box::new(rakka_agent::AgentGoalSpecDraft {
                        spec: goal_spec(),
                        provenance: provenance(2),
                        activate_on_creation: true,
                    })),
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
        .expect("the task creates");
        let now = self.now();
        task.settle_side_effects(&self.router, now)
            .await
            .expect("the owed history flushes");
    }
}

/// The wire half of scenario 45: a scoped cursor pages one entity's durable
/// coordination log through the real service core, resuming exactly where it
/// left off.
#[tokio::test]
async fn a_scoped_cursor_pages_one_entitys_history_over_the_service() {
    let fixture = Fixture::new();
    fixture.coordinated_world().await;
    let scope = task_address().key();

    let replay = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), &scope, None, 1)
        .await
        .expect("the scope replays");
    let AgentCoordinationReplay::Page(first) = replay else {
        panic!("an untrimmed log answers a page");
    };
    assert_eq!(first.events.len(), 1, "the page honors the limit");
    assert_eq!(first.complete_through, 1);
    assert!(first.has_more, "the world recorded more than one event");
    let cursor = first.next_cursor.expect("more history offers a cursor");

    let replay = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), &scope, Some(&cursor), 16)
        .await
        .expect("the cursor resumes");
    let AgentCoordinationReplay::Page(rest) = replay else {
        panic!("resuming is not an expired window");
    };
    let sequences: Vec<u64> = rest.events.iter().map(|event| event.sequence).collect();
    assert_eq!(
        sequences,
        (2..=(1 + sequences.len() as u64)).collect::<Vec<_>>(),
        "the tail resumes contiguously with no gap and no repeat"
    );
    assert!(
        rest.events
            .iter()
            .any(|event| event.kind.as_label() == "task/assignment-decided"),
        "the coordination vocabulary reaches the wire: {:?}",
        rest.events
            .iter()
            .map(|event| event.kind.as_label())
            .collect::<Vec<_>>()
    );
}

/// A cursor is not a capability. It carries its own tenant, so an
/// authenticated caller cannot page another tenant's coordination history by
/// spelling one — and the refusal reads exactly like a scope that is not there,
/// never like one that is.
#[tokio::test]
async fn a_scope_from_another_tenant_is_refused() {
    let fixture = Fixture::new();
    fixture.coordinated_world().await;

    let foreign = AgentEntityAddress::Task(
        AgentTaskScope::new(
            TenantId::new(OTHER_TENANT),
            AgentTaskId::new(TASK).expect("the task id"),
        )
        .expect("the scope"),
    )
    .key();
    let error = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), &foreign, None, 8)
        .await
        .expect_err("a scope outside the authenticated tenant is refused");
    assert!(
        matches!(error, RakkaAgentA2AError::Unauthorized),
        "another tenant's scope is not this caller's to read: {error}"
    );

    // A cursor naming another entity inside the caller's own tenant is refused
    // too — the scope fence is not only a tenant fence.
    let scope = task_address().key();
    let other = AgentCoordinationCursor::new(
        AgentEntityAddress::Task(
            AgentTaskScope::new(tenant(), AgentTaskId::new("another-ticket").expect("id"))
                .expect("the scope"),
        ),
        1,
    )
    .encode();
    let error = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), &scope, Some(&other), 8)
        .await
        .expect_err("a cursor naming another entity is refused");
    assert_eq!(error.code(), "coordination-cursor-scope-mismatch");

    // And a malformed scope fails closed rather than being guessed at.
    let error = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), "not-a-scope", None, 8)
        .await
        .expect_err("a scope that is not one is refused");
    assert_eq!(error.code(), "coordination-cursor-malformed");
}

/// The replay is its own operation class, with the addressed scope bound into
/// the check before any log is read — and a denial leaves the caller with
/// nothing.
#[tokio::test]
async fn a_coordination_read_authorizes_under_its_own_operation_class() {
    struct DenyCoordinationReads;

    #[async_trait]
    impl A2AAuthorizer for DenyCoordinationReads {
        async fn authorize(
            &self,
            request: &A2AAuthorizationRequest<'_>,
        ) -> A2AAuthorizationDecision {
            match request.operation {
                A2AOperation::CoordinationEventRead => {
                    let claim = request
                        .coordination
                        .as_ref()
                        .expect("the addressed scope rides the request");
                    assert_eq!(claim.scope_class, "task");
                    assert_eq!(claim.scope, task_address().key());
                    assert_eq!(request.tenant, Some(TENANT));
                    A2AAuthorizationDecision::Deny
                }
                _ => A2AAuthorizationDecision::Allow,
            }
        }
    }

    let fixture = Fixture::with_authorizer(Arc::new(DenyCoordinationReads));
    fixture.coordinated_world().await;

    let error = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), &task_address().key(), None, 8)
        .await
        .expect_err("the denied read fails closed");
    assert!(matches!(error, RakkaAgentA2AError::Unauthorized));

    // The same read under the permissive authorizer succeeds, so the denial
    // above was the operation class and not a broken world.
    let permissive = Fixture::new();
    permissive.coordinated_world().await;
    permissive
        .service
        .replay_coordination_events(&params(), Some(TENANT), &task_address().key(), None, 8)
        .await
        .expect("the same read is served when the class is permitted");
}

/// The goal view's deny-is-absent contract, carried to the wire: a denied
/// caller, a caller with no principal, and a goal that does not exist all
/// answer the same `None`. Anything else would hand an unauthorized caller the
/// existence oracle the query layer closed.
#[tokio::test]
async fn a_denied_goal_view_is_indistinguishable_from_an_absent_one() {
    struct DenyGoalViews;

    #[async_trait]
    impl A2AAuthorizer for DenyGoalViews {
        async fn authorize(
            &self,
            request: &A2AAuthorizationRequest<'_>,
        ) -> A2AAuthorizationDecision {
            match request.operation {
                A2AOperation::GoalViewRead => {
                    let claim = request
                        .goal_view
                        .as_ref()
                        .expect("the addressed goal rides the request");
                    assert_eq!(claim.goal, TASK);
                    A2AAuthorizationDecision::Deny
                }
                _ => A2AAuthorizationDecision::Allow,
            }
        }
    }

    // A non-owner: the deny-is-absent contract must hold for a real principal
    // reading a goal that genuinely exists.
    let principal = rakka_agent_workflow::PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "operator".to_string(),
        display_name: None,
    };

    // Denied.
    let denied = Fixture::with_authorizer(Arc::new(DenyGoalViews));
    denied.coordinated_world().await;
    let denied_answer = denied
        .service
        .agent_goal_view(&params(), Some(TENANT), TASK, Some(&principal), None)
        .await
        .expect("a denial is not a failure");

    // Permitted and the goal is real — but this caller does not own it, so the
    // owner fence answers absent. Without the positive read below, every arm of
    // this test would be `None` for the same trivial reason and the equality
    // would prove nothing.
    let permitted = Fixture::new();
    permitted.coordinated_world().await;
    let owner_answer = permitted
        .service
        .agent_goal_view(&params(), Some(TENANT), TASK, Some(&owner()), None)
        .await
        .expect("the owner's read succeeds");
    let view = owner_answer.expect("the owner sees the goal that exists");
    assert_eq!(view.root_task.as_str(), TASK);
    assert!(
        !view.tasks.is_empty(),
        "the wire assembled a real view, not an empty shell"
    );

    // The caller-supplied budget is clamped and honored, so a cheap question
    // does not cost an exhaustive traversal.
    let bounded = permitted
        .service
        .agent_goal_view(&params(), Some(TENANT), TASK, Some(&owner()), Some(1))
        .await
        .expect("the bounded read succeeds")
        .expect("the owner sees it");
    assert_eq!(bounded.tasks.len(), 1);

    let absent_answer = permitted
        .service
        .agent_goal_view(&params(), Some(TENANT), TASK, Some(&principal), None)
        .await
        .expect("an absent goal is not a failure");

    // Permitted, but nobody is authenticated.
    let unauthenticated_answer = permitted
        .service
        .agent_goal_view(&params(), Some(TENANT), TASK, None, None)
        .await
        .expect("a principal-less read is not a failure");

    // And a goal that was never created at all.
    let unknown_answer = permitted
        .service
        .agent_goal_view(
            &params(),
            Some(TENANT),
            "no-such-goal",
            Some(&principal),
            None,
        )
        .await
        .expect("an unknown goal is not a failure");

    assert!(denied_answer.is_none());
    assert_eq!(denied_answer.is_none(), absent_answer.is_none());
    assert_eq!(denied_answer.is_none(), unauthenticated_answer.is_none());
    assert_eq!(denied_answer.is_none(), unknown_answer.is_none());
}

/// A run scope is refused explicitly when no decision-event sink is wired, and
/// the agent scope is refused because it keeps no sequenced log at all. Neither
/// is answered with an empty page, which would claim nothing had happened.
#[tokio::test]
async fn an_unserved_scope_is_refused_by_name() {
    let fixture = Fixture::new();
    fixture.coordinated_world().await;

    let run = AgentEntityAddress::Run(
        rakka_agent::AgentRunScope::new(
            tenant(),
            agent_id(),
            rakka_agent::AgentRunId::new("ticket-1-gen-1").expect("the run id"),
        )
        .expect("the run scope"),
    )
    .key();
    let error = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), &run, None, 8)
        .await
        .expect_err("an unwired run scope is refused");
    assert_eq!(error.code(), "coordination-run-events-unavailable");

    let agent =
        AgentEntityAddress::Agent(AgentScope::new(tenant(), agent_id()).expect("the agent scope"))
            .key();
    let error = fixture
        .service
        .replay_coordination_events(&params(), Some(TENANT), &agent, None, 8)
        .await
        .expect_err("the agent scope keeps no replayable log");
    assert_eq!(error.code(), "coordination-scope-not-replayable");
}
