//! The typed-task entity: creation, dependencies, and the assignment exchange.
//!
//! Specification: sections 6.4, 9.1, 9.2, and 9.8; scenario 37 of section 18.
//! Replaying typed task creation, dependency, and assignment commands must yield
//! one `AgentTaskId`, one dependency edge, and one current assignment — and the
//! canonical creation → assignment → run-acceptance flow of specification 9.8
//! must converge on one run per generation, however often it is re-driven.
//!
//! The run entity does not exist until slice 1.5, so its half of the assignment
//! exchange is the `RunAcceptanceProbe` of `rakka_agent::testkit`. It receives
//! the entity's real `AgentRunAssignment` command and answers with a real
//! `AgentRunAcceptance`; the exchange it settles is the one slice 1.5 inherits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rakka_agent::testkit::{
    sweep_crash_points, CrashingStateStore, ExchangeFault, InProcessExchangeTransport,
    RunAcceptanceProbe, RunAcceptanceProbeState,
};
use rakka_agent::{
    init_agent_task_entity_sharding, load_agent_task_state, passivate_agent_task_entity,
    registered_agent_task_entity_ref, AgentAssignmentStatus, AgentAuthorityEnvelope,
    AgentBudgetAllocation, AgentBudgetDimension, AgentContinuousGoalSpec, AgentDefinition,
    AgentDefinitionId, AgentDependencyFailurePolicy, AgentEntityAddress, AgentEntityClass,
    AgentEntityCommand, AgentEntityState, AgentEntityStore, AgentExchangeEnvelope,
    AgentExchangeKind, AgentExchangePayload, AgentExchangeRouter, AgentGoalId, AgentGoalMode,
    AgentId, AgentOperationId, AgentOperationKind, AgentPolicyRef, AgentRevisionNumber,
    AgentRevisionProvenance, AgentRunId, AgentRunScope, AgentSchemaId, AgentSchemaPolicy,
    AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskDependencyDeclaration,
    AgentTaskDependencyOutcome, AgentTaskEntityCommand, AgentTaskEntityMessage,
    AgentTaskEntityReply, AgentTaskEntityShardingSettings, AgentTaskEntityStore,
    AgentTaskHistoryCursor, AgentTaskHistoryEntry, AgentTaskHistoryKind, AgentTaskId,
    AgentTaskOutcome, AgentTaskOwnership, AgentTaskScope, AgentTaskSnapshot, AgentTaskState,
    AgentTaskStatus, AgentWakePolicy, AgentWakePolicyRevision, AgentWakeTriggerKind,
    InMemoryAgentTaskHistoryStore, ScheduleRevision, TenantId,
    AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE, AGENT_TASK_CREATION_PAYLOAD_TYPE,
    CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentCorrelationId, AgentTimestampMillis, PrincipalRef,
};
use rakka_core::ActorSystem;
use rakka_persistence::{DurableStateStore, InMemoryDurableStateStore};
use rakka_sharding::{ClusterSharding, EntityTypeKey};

type TaskStore = CrashingStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = InMemoryDurableStateStore<RunAcceptanceProbeState>;
type TaskEntity = AgentTaskEntityStore<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore>;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK: &str = "ticket-1";
const TASK_DEFINITION: &str = "resolve-ticket";
const ASK_TIMEOUT: Duration = Duration::from_secs(2);

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("agent id should be valid")
}

fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("agent scope should be valid")
}

fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid")
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
}

fn operation(kind: AgentOperationKind, discriminator: &str) -> AgentOperationId {
    AgentOperationId::new(kind, [TENANT, TASK, discriminator])
        .expect("operation id should be derivable")
}

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "ingress".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn creation(dependencies: Vec<AgentTaskDependencyDeclaration>) -> AgentTaskCreation {
    AgentTaskCreation {
        definition: task_definition(),
        input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
            .expect("the input is inline-bounded"),
        assignee: Some(agent_id()),
        goal: None,
        goal_mode: Default::default(),
        parent: None,
        dependencies,
        escrow: None,
        wake: None,
        telemetry: Default::default(),
    }
}

fn create_command(dependencies: Vec<AgentTaskDependencyDeclaration>) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::Create {
        operation_id: operation(AgentOperationKind::TaskCreation, "1"),
        creation: Box::new(creation(dependencies)),
    }
}

fn dependency(id: &str) -> AgentTaskDependencyDeclaration {
    AgentTaskDependencyDeclaration::new(AgentTaskId::new(id).expect("task id should be valid"))
}

fn applied(reply: AgentTaskEntityReply) -> AgentTaskOutcome {
    match reply {
        AgentTaskEntityReply::Applied { outcome } => outcome,
        other => panic!("expected an applied transition, got {other:?}"),
    }
}

fn duplicate(reply: AgentTaskEntityReply) -> AgentTaskOutcome {
    match reply {
        AgentTaskEntityReply::Duplicate { outcome } => outcome,
        other => panic!("expected a deduplicated replay, got {other:?}"),
    }
}

fn snapshot(reply: AgentTaskEntityReply) -> AgentTaskSnapshot {
    match reply {
        AgentTaskEntityReply::Snapshot(Some(snapshot)) => *snapshot,
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

fn rejection_code(reply: AgentTaskEntityReply) -> String {
    match reply {
        AgentTaskEntityReply::Rejected { code, .. } => code,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

/// One durable store per entity class, one clock, and the router that carries an
/// exchange from the task to the run.
///
/// Task entities are created on demand and thrown away, because that is what a
/// sharded entity does: it is materialized on its owner, transitions, and
/// passivates. Nothing but the stores survives between them.
struct Fixture {
    tasks: TaskStore,
    agents: AgentStore,
    runs: RunStore,
    history: InMemoryAgentTaskHistoryStore,
    router: AgentExchangeRouter,
    run_transport: InProcessExchangeTransport<RunAcceptanceProbe, RunStore>,
    clock: Arc<AtomicU64>,
}

impl Fixture {
    fn new(run: RunAcceptanceProbe) -> Self {
        let tasks = TaskStore::new();
        let agents = AgentStore::new();
        let runs = RunStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let clock = Arc::new(AtomicU64::new(1));

        // The task reaches the run through the same durable substrate it would use
        // across a cluster; only the transport differs.
        let run_transport = InProcessExchangeTransport::new(run, runs.clone(), clock.clone());
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Run, Arc::new(run_transport.clone()));

        Self {
            tasks,
            agents,
            runs,
            history,
            router,
            run_transport,
            clock,
        }
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    /// Instantiates the agent the assignment decision will read.
    ///
    /// Its authority envelope declares the task definition, because an agent that
    /// declares none is authorized for none: the admission check fails closed.
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
                provenance: Box::new(provenance(1)),
            })
            .await
            .expect("the agent should instantiate");
    }

    /// Materializes the task entity from durable state alone.
    async fn task(&self) -> TaskEntity {
        let mut entity = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        entity
            .recover(self.now())
            .await
            .expect("the task entity should recover");
        entity
    }

    async fn apply(&self, command: AgentTaskEntityCommand) -> AgentTaskEntityReply {
        let mut entity = self.task().await;
        let now = self.now();
        match entity.apply(command, &self.router, now).await {
            Ok(reply) => reply,
            Err(error) => AgentTaskEntityReply::Rejected {
                code: error.code().to_string(),
                message: error.to_string(),
            },
        }
    }

    /// [`Self::apply`], but surfacing every error — recovery's included — so a
    /// sweep can drive a command into an armed crash point without panicking.
    async fn try_apply(
        &self,
        command: AgentTaskEntityCommand,
    ) -> Result<AgentTaskEntityReply, String> {
        let mut entity = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        entity
            .recover(self.now())
            .await
            .map_err(|error| error.code().to_string())?;
        entity
            .apply(command, &self.router, self.now())
            .await
            .map_err(|error| error.code().to_string())
    }

    /// Drives whatever the task owes: the assignment decision, the history, and
    /// the exchanges. This is what recovery does, and what a timer would do.
    async fn settle(&self) {
        self.try_settle()
            .await
            .expect("the task should settle what it owes");
    }

    /// [`Self::settle`], but surfacing the first error instead of panicking —
    /// what a sweep needs, because an armed crash point kills the owner
    /// mid-settle and the injected loss is the point, not a failure.
    async fn try_settle(&self) -> Result<(), String> {
        let mut entity = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        entity
            .recover(self.now())
            .await
            .map_err(|error| error.code().to_string())?;
        entity
            .settle_side_effects(&self.router, self.now())
            .await
            .map_err(|error| error.code().to_string())?;
        Ok(())
    }

    async fn snapshot(&self) -> AgentTaskSnapshot {
        snapshot(self.apply(AgentTaskEntityCommand::Describe).await)
    }

    async fn run_state(&self, generation: u64) -> RunAcceptanceProbeState {
        let run = rakka_agent::run_id_for_assignment(
            task_scope().task(),
            rakka_agent::AgentAssignmentGeneration::new(generation),
        )
        .expect("the run id should be derivable");
        let scope = rakka_agent::AgentRunScope::new(tenant(), agent_id(), run)
            .expect("the run scope should be valid");

        let mut host = rakka_agent::AgentExchangeHost::new(
            rakka_agent::AgentEntityAddress::Run(scope),
            RunAcceptanceProbe::accepting(),
            self.runs.clone(),
        );
        host.recover(self.now())
            .await
            .expect("the run should recover")
            .clone()
    }

    async fn history_kinds(&self) -> Vec<AgentTaskHistoryKind> {
        self.history_entries()
            .await
            .iter()
            .map(|entry| entry.kind)
            .collect()
    }

    async fn history_entries(&self) -> Vec<AgentTaskHistoryEntry> {
        let mut entries = Vec::new();
        let mut cursor = Some(AgentTaskHistoryCursor::start());
        while let Some(position) = cursor {
            let page =
                rakka_agent::AgentTaskHistoryStore::read(&self.history, &task_scope(), position)
                    .await
                    .expect("the history should read");
            entries.extend(page.entries.iter().cloned());
            cursor = page.next;
        }
        entries
    }
}

#[tokio::test]
async fn replaying_creation_dependency_and_assignment_yields_one_task_one_edge_one_assignment() {
    // Scenario 37.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    // Creation, with one dependency. A task that depends on another is not
    // eligible, so no assignment decision may happen yet.
    let outcome = applied(fx.apply(create_command(vec![dependency("upstream")])).await);
    assert_eq!(outcome.status, AgentTaskStatus::Blocked);
    assert!(!outcome.dependencies_satisfied);
    assert_eq!(outcome.assignment_generation.get(), 0);

    // The ingress redelivers the creation. It is deduplicated on its operation
    // id, and the original outcome comes back rather than a second task.
    let replay = duplicate(fx.apply(create_command(vec![dependency("upstream")])).await);
    assert_eq!(replay, outcome);

    // The same dependency is declared again, under a *different* operation id, so
    // the deduplication log cannot absorb it. The edge itself is what must be
    // idempotent.
    for discriminator in ["1", "2"] {
        applied(
            fx.apply(AgentTaskEntityCommand::DeclareDependency {
                operation_id: operation(AgentOperationKind::Command, discriminator),
                declaration: Box::new(dependency("upstream")),
            })
            .await,
        );
    }

    let blocked = fx.snapshot().await;
    assert_eq!(blocked.dependencies.len(), 1, "one dependency edge");
    assert_eq!(blocked.status, AgentTaskStatus::Blocked);
    assert!(blocked.assignment.is_none());

    // The dependency completes. The task becomes eligible, the entity reads the
    // agent's durable admission state, and the assignment decision follows.
    let resolve = AgentTaskEntityCommand::RecordDependencyOutcome {
        operation_id: operation(AgentOperationKind::Command, "resolve"),
        dependency: AgentTaskId::new("upstream").expect("task id should be valid"),
        outcome: AgentTaskDependencyOutcome::Completed,
    };
    applied(fx.apply(resolve.clone()).await);

    let assigned = fx.snapshot().await;
    assert_eq!(assigned.status, AgentTaskStatus::InProgress);
    let assignment = assigned.assignment.as_ref().expect("the task is assigned");
    assert_eq!(assignment.generation.get(), 1);
    assert_eq!(assignment.status, AgentAssignmentStatus::Accepted);
    assert_eq!(assignment.agent, agent_id());

    // Everything is re-driven: the dependency outcome replays, and the entity
    // settles again from durable state. Neither may produce a second assignment,
    // a second generation, or a second run.
    let replayed = duplicate(fx.apply(resolve).await);
    assert_eq!(replayed.assignment_generation.get(), 1);
    fx.settle().await;
    fx.settle().await;

    let converged = fx.snapshot().await;
    assert_eq!(converged.assignment_generation.get(), 1);
    assert_eq!(
        converged.assignment.as_ref().map(|a| a.run.clone()),
        Some(assignment.run.clone()),
        "the run is derived from the task and the generation, so a replay resolves to the same one"
    );

    let run = fx.run_state(1).await;
    assert_eq!(
        run.accepted_generations(),
        &[1],
        "the run durably accepted exactly one assignment"
    );

    let kinds = fx.history_kinds().await;
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == AgentTaskHistoryKind::Created)
            .count(),
        1,
        "one task"
    );
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == AgentTaskHistoryKind::DependencyDeclared)
            .count(),
        1,
        "one dependency edge"
    );
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == AgentTaskHistoryKind::AssignmentDecided)
            .count(),
        1,
        "one assignment decision"
    );
}

#[tokio::test]
async fn a_task_with_no_dependencies_is_assigned_and_accepted_in_one_pass() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let outcome = applied(fx.apply(create_command(Vec::new())).await);
    assert_eq!(
        outcome.status,
        AgentTaskStatus::Assigned,
        "the creation transition decides the assignment and owes the run-creation exchange in the \
         same pass"
    );

    let snapshot = fx.snapshot().await;
    assert_eq!(snapshot.status, AgentTaskStatus::InProgress);
    assert_eq!(
        snapshot
            .assignment
            .as_ref()
            .map(|assignment| assignment.status),
        Some(AgentAssignmentStatus::Accepted)
    );
}

#[tokio::test]
async fn an_assignment_is_refused_when_the_agent_cannot_be_admitted() {
    // The agent is never instantiated, so it has no durable admission state. The
    // decision fails closed, and the task stays logically available: it is still
    // assignable once the agent exists.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());

    let outcome = applied(fx.apply(create_command(Vec::new())).await);
    assert_eq!(outcome.status, AgentTaskStatus::Created);
    assert_eq!(outcome.assignment_generation.get(), 0);

    let refused = fx.snapshot().await;
    let refusal = refused
        .last_refusal
        .as_ref()
        .expect("a refusal is recorded");
    assert_eq!(
        refusal.reason,
        rakka_agent::AgentAssignmentRefusalReason::AgentNotInstantiated
    );
    assert!(refused.assignment.is_none());

    // The agent appears. The very next settlement assigns the task, with no new
    // creation command and no lost work.
    fx.instantiate_agent().await;
    fx.settle().await;

    let assigned = fx.snapshot().await;
    assert_eq!(assigned.status, AgentTaskStatus::InProgress);
    assert_eq!(assigned.assignment_generation.get(), 1);
    assert!(assigned.last_refusal.is_none());
}

#[tokio::test]
async fn an_agent_that_does_not_declare_the_task_definition_is_refused() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());

    // An agent whose envelope declares no task definition is authorized for none.
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
        "Resolves customer support tickets end to end.",
        AgentAuthorityEnvelope::empty(),
    )
    .expect("the agent definition should be valid");
    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
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
            provenance: Box::new(provenance(1)),
        })
        .await
        .expect("the agent should instantiate");

    applied(fx.apply(create_command(Vec::new())).await);

    let refused = fx.snapshot().await;
    assert_eq!(
        refused.last_refusal.as_ref().map(|refusal| refusal.reason),
        Some(rakka_agent::AgentAssignmentRefusalReason::TaskDefinitionNotPermitted)
    );
    assert_eq!(refused.status, AgentTaskStatus::Created);
}

#[tokio::test]
async fn a_standing_refusal_is_recorded_once_however_often_the_task_settles() {
    // The agent is never instantiated, so every decision refuses for the same
    // reason. The command that created the task decides twice (once inside the
    // transition, once on its settle pass) and each later sweep decides again —
    // but an unchanged refusal is not a new fact, so exactly one row lands in
    // the append-only history.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    applied(fx.apply(create_command(Vec::new())).await);
    for _ in 0..3 {
        fx.settle().await;
    }

    let kinds = fx.history_kinds().await;
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == AgentTaskHistoryKind::AssignmentRefused)
            .count(),
        1,
        "an unchanged refusal must not grow the history: {kinds:?}"
    );
    assert!(fx.snapshot().await.last_refusal.is_some());

    // The dedup fences repetition, not progress: once the agent exists, the
    // next sweep decides the assignment.
    fx.instantiate_agent().await;
    fx.settle().await;
    assert_eq!(fx.snapshot().await.status, AgentTaskStatus::InProgress);
}

#[tokio::test]
async fn conflicting_duplicate_dependency_declarations_fail_creation_closed() {
    // Declaring the same edge twice within one creation follows the same rule
    // as redeclaring it after creation: a repeat is idempotent, and a repeat
    // under a different failure policy is refused rather than last-wins.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let code = rejection_code(
        fx.apply(create_command(vec![
            dependency("upstream"),
            dependency("upstream").with_policy(AgentDependencyFailurePolicy::ContinueWithEvidence),
        ]))
        .await,
    );
    assert_eq!(code, "task-dependency-conflict");

    // The same repetition under one policy creates one task with one edge.
    let outcome = applied(
        fx.apply(create_command(vec![
            dependency("upstream"),
            dependency("upstream"),
        ]))
        .await,
    );
    assert_eq!(outcome.status, AgentTaskStatus::Blocked);
    assert_eq!(fx.snapshot().await.dependencies.len(), 1);
    let kinds = fx.history_kinds().await;
    assert_eq!(
        kinds
            .iter()
            .filter(|kind| **kind == AgentTaskHistoryKind::DependencyDeclared)
            .count(),
        1
    );
}

#[tokio::test]
async fn a_settlement_is_stamped_when_it_commits_not_when_its_envelope_was_created() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    // The run durably accepts the assignment, and the reply is lost: the
    // exchange stays outstanding past the command that decided it.
    fx.run_transport.inject(ExchangeFault::LoseReply);
    applied(fx.apply(create_command(Vec::new())).await);

    let offered = fx.snapshot().await;
    assert_eq!(
        offered
            .assignment
            .as_ref()
            .map(|assignment| assignment.status),
        Some(AgentAssignmentStatus::Offered)
    );
    let decided_at = offered.updated_at;

    // A later sweep re-drives the exchange and settles the acceptance. The
    // settlement is a new transition committing now, on this owner — it is not
    // back-dated to the envelope's creation, and `updated_at` never regresses.
    fx.settle().await;
    let accepted = fx.snapshot().await;
    assert_eq!(accepted.status, AgentTaskStatus::InProgress);
    assert!(
        accepted.updated_at > decided_at,
        "the settlement must be stamped at commit time: decided at {decided_at:?}, settled at {:?}",
        accepted.updated_at
    );

    // The settlement's history row is recorded by the drive, after the pass's
    // flush; one more pass carries it to the sink.
    fx.settle().await;
    let entries = fx.history_entries().await;
    let decided = entries
        .iter()
        .find(|entry| entry.kind == AgentTaskHistoryKind::AssignmentDecided)
        .expect("the decision is recorded");
    let settled = entries
        .iter()
        .find(|entry| entry.kind == AgentTaskHistoryKind::AssignmentAccepted)
        .expect("the acceptance is recorded");
    assert!(
        settled.at > decided.at,
        "history must be ordered in time as well as in sequence"
    );
}

#[tokio::test]
async fn an_accepted_creation_exchange_reply_is_typed_as_a_creation_outcome() {
    // The decision payload type names exactly one wire shape. A delegated
    // creation's accepted reply carries an `AgentTaskOutcome`, so it travels
    // under its own type rather than masquerading as an `AgentTaskDecision`.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let operation_id = operation(AgentOperationKind::TaskCreation, "delegated");
    let delegator = AgentRunScope::new(
        tenant(),
        agent_id(),
        AgentRunId::new("delegating-run").expect("run id should be valid"),
    )
    .expect("run scope should be valid");
    let envelope = AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::Creation,
        AgentEntityAddress::Run(delegator),
        AgentEntityAddress::Task(task_scope()),
        AgentExchangePayload::encode(AGENT_TASK_CREATION_PAYLOAD_TYPE, &creation(Vec::new()))
            .expect("the creation payload encodes"),
        AgentCorrelationId::new(operation_id.as_str()),
        fx.now(),
    )
    .expect("the creation envelope is well formed");

    let mut entity = fx.task().await;
    let reply = entity
        .accept(&envelope, &fx.router, fx.now())
        .await
        .expect("the creation exchange is accepted");
    let result = reply.result();
    assert!(result.is_accepted());
    assert_eq!(
        result.payload().payload_type(),
        AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE
    );
    let outcome: AgentTaskOutcome = result
        .payload()
        .decode(AGENT_TASK_CREATION_OUTCOME_PAYLOAD_TYPE)
        .expect("the outcome decodes under its own payload type");
    assert_eq!(outcome.status, AgentTaskStatus::Created);
}

#[tokio::test]
async fn a_run_that_refuses_its_assignment_retires_the_generation_and_leaves_the_task_assignable() {
    let fx = Fixture::new(RunAcceptanceProbe::refusing());
    fx.instantiate_agent().await;

    applied(fx.apply(create_command(Vec::new())).await);

    let released = fx.snapshot().await;
    assert!(
        released.assignment.is_none(),
        "the refused assignment is retired rather than left offered"
    );
    assert_eq!(released.status, AgentTaskStatus::Created);
    assert_eq!(
        released.last_refusal.as_ref().map(|refusal| refusal.reason),
        Some(rakka_agent::AgentAssignmentRefusalReason::RunRefusedAssignment)
    );

    // The default limit tolerates three assignments. The fourth cannot be made,
    // and the task fails closed rather than looping forever.
    for _ in 0..4 {
        fx.settle().await;
    }
    let exhausted = fx.snapshot().await;
    assert_eq!(exhausted.status, AgentTaskStatus::Failed);
    assert_eq!(
        exhausted
            .terminal_reason
            .as_ref()
            .map(rakka_agent::AgentTaskTerminalReason::code),
        Some("assignments-exhausted")
    );
}

#[tokio::test]
async fn a_dependency_declared_during_an_outstanding_assignment_blocks_the_refused_task() {
    let fx = Fixture::new(RunAcceptanceProbe::refusing());
    fx.instantiate_agent().await;

    // The assignment exchange is lost in flight, so the offer stays
    // outstanding and the task is still `Assigned` when the next command lands.
    fx.run_transport.inject(ExchangeFault::LoseEnvelope);
    applied(fx.apply(create_command(Vec::new())).await);
    let offered = fx.snapshot().await;
    assert_eq!(offered.status, AgentTaskStatus::Assigned);
    assert!(offered.assignment.is_some());

    // The edge lands while the assignment is outstanding; the same command's
    // settle pass re-drives the exchange, and the run refuses it.
    applied(
        fx.apply(AgentTaskEntityCommand::DeclareDependency {
            operation_id: operation(AgentOperationKind::Command, "declare-late"),
            declaration: Box::new(dependency("upstream")),
        })
        .await,
    );

    // The retired task is not assignable until the dependency resolves, and
    // its public status must say so rather than reporting `Created`.
    let released = fx.snapshot().await;
    assert!(released.assignment.is_none());
    assert_eq!(released.status, AgentTaskStatus::Blocked);
    assert!(!released.dependencies_satisfied);

    // A settle sweep over the blocked task must not consume another
    // generation.
    fx.settle().await;
    let settled = fx.snapshot().await;
    assert_eq!(settled.status, AgentTaskStatus::Blocked);
    assert_eq!(settled.assignment_generation.get(), 1);
}

#[tokio::test]
async fn a_failed_dependency_cancels_its_dependents_by_default() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    applied(fx.apply(create_command(vec![dependency("upstream")])).await);

    applied(
        fx.apply(AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: operation(AgentOperationKind::Command, "resolve"),
            dependency: AgentTaskId::new("upstream").expect("task id should be valid"),
            outcome: AgentTaskDependencyOutcome::Failed,
        })
        .await,
    );

    let cancelled = fx.snapshot().await;
    assert_eq!(cancelled.status, AgentTaskStatus::Cancelled);
    assert_eq!(
        cancelled
            .terminal_reason
            .as_ref()
            .map(rakka_agent::AgentTaskTerminalReason::code),
        Some("dependency-not-satisfied")
    );
    assert!(
        cancelled.assignment.is_none(),
        "a terminal task fences its run"
    );
}

#[tokio::test]
async fn a_continue_with_evidence_dependency_does_not_cancel_its_dependent() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let declaration = dependency("upstream")
        .with_policy(rakka_agent::AgentDependencyFailurePolicy::ContinueWithEvidence);
    applied(fx.apply(create_command(vec![declaration])).await);

    applied(
        fx.apply(AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: operation(AgentOperationKind::Command, "resolve"),
            dependency: AgentTaskId::new("upstream").expect("task id should be valid"),
            outcome: AgentTaskDependencyOutcome::Failed,
        })
        .await,
    );

    let snapshot = fx.snapshot().await;
    assert_eq!(snapshot.status, AgentTaskStatus::InProgress);
    assert!(snapshot.dependencies_satisfied);
}

#[tokio::test]
async fn a_dependency_cycle_and_a_self_dependency_fail_closed() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let code = rejection_code(fx.apply(create_command(vec![dependency(TASK)])).await);
    assert_eq!(
        code, "task-dependency-cycle",
        "a task cannot depend on itself"
    );

    applied(fx.apply(create_command(Vec::new())).await);

    // The dependency's own declared ancestry already contains this task, so the
    // edge would close a cycle.
    let cyclic = dependency("downstream").with_ancestors(vec![
        AgentTaskId::new(TASK).expect("task id should be valid")
    ]);
    let code = rejection_code(
        fx.apply(AgentTaskEntityCommand::DeclareDependency {
            operation_id: operation(AgentOperationKind::Command, "cyclic"),
            declaration: Box::new(cyclic),
        })
        .await,
    );
    assert_eq!(code, "task-dependency-cycle");
}

#[tokio::test]
async fn a_human_owned_task_is_never_assigned_to_an_agent() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let mut creation = creation(Vec::new());
    creation.definition = task_definition().with_ownership(AgentTaskOwnership::Human);
    creation.assignee = None;

    applied(
        fx.apply(AgentTaskEntityCommand::Create {
            operation_id: operation(AgentOperationKind::TaskCreation, "1"),
            creation: Box::new(creation),
        })
        .await,
    );

    let snapshot = fx.snapshot().await;
    assert_eq!(snapshot.status, AgentTaskStatus::WaitingForInput);
    assert!(snapshot.assignment.is_none());
    assert_eq!(snapshot.assignment_generation.get(), 0);
}

#[tokio::test]
async fn a_cancelled_task_accepts_no_further_transition() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;
    applied(fx.apply(create_command(Vec::new())).await);

    applied(
        fx.apply(AgentTaskEntityCommand::Cancel {
            operation_id: operation(AgentOperationKind::Cancellation, "1"),
            reason: "the customer withdrew the ticket".to_string(),
        })
        .await,
    );

    let cancelled = fx.snapshot().await;
    assert_eq!(cancelled.status, AgentTaskStatus::Cancelled);

    let code = rejection_code(
        fx.apply(AgentTaskEntityCommand::DeclareDependency {
            operation_id: operation(AgentOperationKind::Command, "late"),
            declaration: Box::new(dependency("upstream")),
        })
        .await,
    );
    assert_eq!(code, "task-terminal");
}

#[tokio::test]
async fn a_task_persists_passivates_and_recovers_on_another_owner() {
    // Specification 15: the entity keeps nothing in memory that its durable state
    // does not hold, so it can be dropped after any message and re-materialized.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let system = ActorSystem::new("task-recovery");
    let sharding = ClusterSharding::get(&system);
    let key = EntityTypeKey::new("RakkaAgentTaskRecovery")
        .with_number_of_shards(4)
        .expect("entity type key should be valid");
    let settings = AgentTaskEntityShardingSettings::new(key)
        .with_idle_passivation(Duration::from_secs(60))
        .with_clock({
            let clock = fx.clock.clone();
            Arc::new(move || AgentTimestampMillis::new(clock.fetch_add(1, Ordering::SeqCst)))
        });

    let registration = init_agent_task_entity_sharding(
        &sharding,
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
        fx.router.clone(),
        settings,
    )
    .expect("task entity sharding should initialize");
    let entity = registered_agent_task_entity_ref(&registration, &task_scope());

    let reply = entity
        .ask(
            |reply_to| AgentTaskEntityMessage::Command {
                command: Box::new(create_command(Vec::new())),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the task entity should reply");
    assert_eq!(applied(reply).status, AgentTaskStatus::Assigned);

    assert!(
        passivate_agent_task_entity(&sharding, registration.key(), &task_scope())
            .expect("passivation should be requested"),
        "the entity was resident and is now asked to stop"
    );

    // Nothing but durable state crosses the passivation. The re-materialized
    // entity answers from the record alone.
    let recovered = snapshot(
        entity
            .ask(
                |reply_to| AgentTaskEntityMessage::Command {
                    command: Box::new(AgentTaskEntityCommand::Describe),
                    reply_to,
                },
                ASK_TIMEOUT,
            )
            .await
            .expect("the recovered task entity should reply"),
    );
    assert_eq!(recovered.status, AgentTaskStatus::InProgress);
    assert_eq!(recovered.assignment_generation.get(), 1);

    // And the same record is readable without waking the entity at all, which is
    // the authoritative point query an operator uses while a task is passivated.
    let durable = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the durable read should succeed")
        .expect("the task exists");
    assert_eq!(durable.status(), Some(AgentTaskStatus::InProgress));
}

/// One full drive of the scenario-37 command sequence, exactly as the ingress
/// would redeliver it: creation, the doubly-declared dependency edge, the
/// dependency outcome, and two settle passes. Every command carries the same
/// operation id on every drive, so a re-drive is a redelivery, never new work.
/// The first error — an armed crash point included — surfaces as `Err`.
async fn drive_dependency_flow(fx: &Fixture) -> Result<(), String> {
    let ok = |reply: AgentTaskEntityReply| match reply {
        AgentTaskEntityReply::Rejected { code, message } => Err(format!("{code}: {message}")),
        _ => Ok(()),
    };
    ok(fx
        .try_apply(create_command(vec![dependency("upstream")]))
        .await?)?;
    for discriminator in ["1", "2"] {
        ok(fx
            .try_apply(AgentTaskEntityCommand::DeclareDependency {
                operation_id: operation(AgentOperationKind::Command, discriminator),
                declaration: Box::new(dependency("upstream")),
            })
            .await?)?;
    }
    ok(fx
        .try_apply(AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: operation(AgentOperationKind::Command, "resolve"),
            dependency: AgentTaskId::new("upstream").expect("task id should be valid"),
            outcome: AgentTaskDependencyOutcome::Completed,
        })
        .await?)?;
    fx.try_settle().await?;
    fx.try_settle().await
}

#[tokio::test]
async fn the_dependency_and_assignment_flow_survives_any_owner_loss() {
    // Scenario 37 under the owner-kill sweep: kill the task's owner at every
    // durable write of creation -> dependency -> resolution -> assignment, on
    // both sides of the compare-and-set, then let the ingress redeliver the
    // whole command sequence. Every crash converges on one task record, one
    // dependency edge, one assignment decision, and one durably accepted run.
    // The task store is the only store this flow's crash windows live in; the
    // run half is the acceptance probe over its own plain store.
    let reference = Fixture::new(RunAcceptanceProbe::accepting());
    reference.instantiate_agent().await;
    drive_dependency_flow(&reference)
        .await
        .expect("the reference flow completes");
    let writes = reference.tasks.writes();
    assert!(
        writes >= 3,
        "the dependency flow should make several durable writes, saw {writes}"
    );

    sweep_crash_points(writes, |nth, point| async move {
        let fx = Fixture::new(RunAcceptanceProbe::accepting());
        fx.instantiate_agent().await;

        fx.tasks.crash_at(nth, point);
        let _crashed = drive_dependency_flow(&fx).await;

        // A new owner activates; the ingress redelivers everything.
        fx.tasks.assert_crash_fired(nth, point);
        fx.tasks.survive();
        drive_dependency_flow(&fx).await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let converged = fx.snapshot().await;
        assert_eq!(
            converged.status,
            AgentTaskStatus::InProgress,
            "crash {point:?} at write {nth} should still assign the task"
        );
        assert_eq!(
            converged.assignment_generation.get(),
            1,
            "crash {point:?} at write {nth} minted a second assignment generation"
        );
        assert_eq!(
            converged.dependencies.len(),
            1,
            "crash {point:?} at write {nth} duplicated the dependency edge"
        );

        let run = fx.run_state(1).await;
        assert_eq!(
            run.accepted_generations(),
            &[1],
            "crash {point:?} at write {nth} made the run accept twice"
        );

        let kinds = fx.history_kinds().await;
        for (kind, label) in [
            (AgentTaskHistoryKind::Created, "task"),
            (AgentTaskHistoryKind::DependencyDeclared, "dependency edge"),
            (
                AgentTaskHistoryKind::AssignmentDecided,
                "assignment decision",
            ),
        ] {
            assert_eq!(
                kinds.iter().filter(|entry| **entry == kind).count(),
                1,
                "crash {point:?} at write {nth} recorded more than one {label}"
            );
        }
    })
    .await;
}

/// A continuous goal mode for the root-control-task tests: a durable-timer
/// wake with a bounded epoch, exactly what the slice 3.2 controller drives.
fn continuous_mode() -> AgentGoalMode {
    let mut epoch_budget = AgentBudgetAllocation::unbounded();
    epoch_budget.set(AgentBudgetDimension::ModelCalls, Some(8));
    let policy = AgentWakePolicy::new(
        [AgentWakeTriggerKind::DurableTimer],
        epoch_budget,
        Some(60_000),
    )
    .expect("the wake policy is valid");
    AgentGoalMode::Continuous(Box::new(AgentContinuousGoalSpec {
        schedule_revision: ScheduleRevision::INITIAL,
        wake_policy: AgentWakePolicyRevision::initial(policy, provenance(1))
            .expect("the initial wake-policy revision is accepted"),
        health_condition: AgentPolicyRef::new("nightly-health").expect("the policy ref is valid"),
        epoch: None,
    }))
}

#[tokio::test]
async fn a_continuous_task_must_bind_its_goal() {
    // A continuous root control task exists to admit epochs for a goal;
    // without the binding there is nothing for the wake controller to fence,
    // budget, or retire against, so the creation is refused closed.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let mut untethered = creation(Vec::new());
    untethered.goal_mode = continuous_mode();
    let code = rejection_code(
        fx.apply(AgentTaskEntityCommand::Create {
            operation_id: operation(AgentOperationKind::TaskCreation, "1"),
            creation: Box::new(untethered),
        })
        .await,
    );
    assert_eq!(code, "task-continuous-without-goal");
}

#[tokio::test]
async fn a_continuous_root_task_round_trips_its_mode() {
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let mut rooted = creation(Vec::new());
    rooted.goal = Some(AgentGoalId::new(TASK).expect("the goal id is valid"));
    rooted.goal_mode = continuous_mode();
    let expected = rooted.goal_mode.clone();
    applied(
        fx.apply(AgentTaskEntityCommand::Create {
            operation_id: operation(AgentOperationKind::TaskCreation, "1"),
            creation: Box::new(rooted),
        })
        .await,
    );

    let state = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the task state loads")
        .expect("the task state exists");
    let task = state.task().expect("the task is created");
    assert!(task.goal_mode.is_continuous());
    assert_eq!(task.goal_mode, expected);
}

#[tokio::test]
async fn a_wake_policy_revision_from_a_newer_binary_fails_closed_on_load() {
    // The wake policy carries its own schema version so it can evolve
    // independently of the task state's, which means the load gate must check
    // it independently too: a task record whose embedded revision was written
    // by a newer binary is unreadable even when the task state itself is not.
    let fx = Fixture::new(RunAcceptanceProbe::accepting());
    fx.instantiate_agent().await;

    let mut rooted = creation(Vec::new());
    rooted.goal = Some(AgentGoalId::new(TASK).expect("the goal id is valid"));
    rooted.goal_mode = continuous_mode();
    applied(
        fx.apply(AgentTaskEntityCommand::Create {
            operation_id: operation(AgentOperationKind::TaskCreation, "1"),
            creation: Box::new(rooted),
        })
        .await,
    );

    let persistence_id = task_scope().persistence_id();
    let record = fx
        .tasks
        .load(&persistence_id)
        .await
        .expect("the task record loads")
        .expect("the task record exists");
    let mut value = serde_json::to_value(&record.state).expect("the state serializes");
    let stored = &mut value["task"]["goal_mode"]["continuous"]["wake_policy"]["schema_version"];
    assert_eq!(
        *stored,
        serde_json::json!(CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION.get()),
        "the doctored path must reach the embedded revision's schema version"
    );
    *stored = serde_json::json!(CURRENT_AGENT_WAKE_POLICY_SCHEMA_VERSION.get() + 1);
    let doctored: AgentTaskState =
        serde_json::from_value(value).expect("the doctored state deserializes");
    fx.tasks
        .compare_and_set(&persistence_id, record.revision, doctored)
        .await
        .expect("the doctored state persists");

    let error = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect_err("a wake policy from a newer binary must fail closed");
    assert_eq!(error.code(), "schema-version-ahead");
}

#[test]
fn a_record_persisted_before_the_goal_mode_field_loads_as_finite() {
    let mut value = serde_json::to_value(creation(Vec::new())).expect("the creation serializes");
    value
        .as_object_mut()
        .expect("a creation is an object")
        .remove("goal_mode");
    let loaded: AgentTaskCreation =
        serde_json::from_value(value).expect("a record without the field loads");
    assert_eq!(loaded.goal_mode, AgentGoalMode::Finite);
}
