//! The shared task-and-run fixture the integration tests drive entities with.
//!
//! One durable store per entity class, one clock, and the router that carries
//! the assignment from the task to the run and the proposal back again.
//! Entities are created on demand and thrown away, because that is what a
//! sharded entity does: it is materialized on its owner, transitions, and
//! passivates. Nothing but the stores survives between calls — so every call
//! is already a restart.
//!
//! The fixture is generic over the model adapter its [`ScriptedDispatcher`]
//! answers with, and its run store is a [`CrashingStateStore`], which behaves
//! as a plain in-memory store until a test arms a [`CrashPoint`]
//! (`fx.runs.crash_at(..)`) — one fixture serves the happy path, the adapter
//! matrix, and the crash matrix alike.

// Each integration-test binary compiles this module independently and uses a
// different subset of it; what one binary leaves unused is not dead code.
#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    run_entity, CrashingStateStore, DeferredExchangeRouter, InProcessRunEntityTransport,
    InProcessTaskEntityTransport, ScriptedDispatcher,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentBudgetCeilings, AgentDefinition, AgentDefinitionId,
    AgentEffectPolicies, AgentEntityClass, AgentEntityCommand, AgentEntityState, AgentEntityStore,
    AgentExchangeRouter, AgentId, AgentModelAdapter, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunEffectSink, AgentRunEntityStore,
    AgentRunScope, AgentRunSnapshot, AgentRunState, AgentRunStatus, AgentSchemaId, AgentSchemaRef,
    AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition,
    AgentTaskDefinitionId, AgentTaskEntityCommand, AgentTaskEntityStore, AgentTaskResultCheck,
    AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskSnapshot, AgentTaskState,
    InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore, TenantId,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};
use rakka_persistence::InMemoryDurableStateStore;

/// Durable store for the task entity class.
pub type TaskStore = InMemoryDurableStateStore<AgentTaskState>;
/// Durable store for the agent entity class.
pub type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
/// Durable store for the run entity class; a pass-through until a crash point
/// is armed.
pub type RunStore = CrashingStateStore<AgentRunState>;

pub const TENANT: &str = "acme";
pub const AGENT: &str = "support-agent";
pub const TASK: &str = "ticket-1";
pub const TASK_DEFINITION: &str = "resolve-ticket";

pub fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

pub fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("agent id should be valid")
}

pub fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("agent scope should be valid")
}

pub fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new(TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid")
}

pub fn run_scope() -> AgentRunScope {
    let run = rakka_agent::run_id_for_assignment(
        task_scope().task(),
        rakka_agent::AgentAssignmentGeneration::new(1),
    )
    .expect("the run id should be derivable");
    AgentRunScope::new(tenant(), agent_id(), run).expect("run scope should be valid")
}

pub fn task_definition_id() -> AgentTaskDefinitionId {
    AgentTaskDefinitionId::new(TASK_DEFINITION).expect("task definition id should be valid")
}

pub fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

/// The task requires a non-empty answer, and permits at most three autonomous
/// iterations. Both are deterministic facts the run must satisfy; neither is
/// something the model gets to decide.
pub fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("rule id should be valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_budgets(AgentBudgetCeilings {
        max_loop_iterations: Some(3),
        ..AgentBudgetCeilings::unbounded()
    })
}

pub fn provenance(at: u64) -> AgentRevisionProvenance {
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

/// The task-and-run fixture, generic over the model adapter the dispatcher
/// answers model calls with and the durable sink the run's effects flush to.
pub struct Fixture<
    A: AgentModelAdapter = rakka_agent::testkit::DeterministicModelAdapter,
    S: AgentRunEffectSink = InMemoryAgentRunEffectSink,
> {
    pub tasks: TaskStore,
    pub agents: AgentStore,
    pub runs: RunStore,
    pub history: InMemoryAgentTaskHistoryStore,
    pub effects: S,
    pub policies: AgentEffectPolicies,
    pub router: AgentExchangeRouter,
    pub task_transport:
        InProcessTaskEntityTransport<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore>,
    pub dispatcher: ScriptedDispatcher<A>,
    pub clock: Arc<AtomicU64>,
}

impl<A: AgentModelAdapter> Fixture<A> {
    pub fn new(dispatcher: ScriptedDispatcher<A>) -> Self {
        Self::with_sink(
            dispatcher,
            InMemoryAgentRunEffectSink::new(),
            AgentEffectPolicies::default(),
            Arc::new(AtomicU64::new(1)),
        )
    }
}

impl<A: AgentModelAdapter, S: AgentRunEffectSink> Fixture<A, S> {
    /// Builds the fixture over an explicit effect sink, effect policies, and a
    /// shared clock counter — what the dispatch-pipeline tests need.
    pub fn with_sink(
        dispatcher: ScriptedDispatcher<A>,
        effects: S,
        policies: AgentEffectPolicies,
        clock: Arc<AtomicU64>,
    ) -> Self {
        let tasks = TaskStore::new();
        let agents = AgentStore::new();
        let runs = RunStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();

        // The task and the run exchange with each other, so each transport needs
        // the router the other lives in. The deferred router is that late binding
        // and nothing more; the durable path is unchanged.
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
        )
        .with_effect_policies(policies.clone());
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Task, Arc::new(task_transport.clone()))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport));
        deferred.install(router.clone());

        Self {
            tasks,
            agents,
            runs,
            history,
            effects,
            policies,
            router,
            task_transport,
            dispatcher,
            clock,
        }
    }

    pub fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn instantiate_agent(&self) {
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

    /// Creates the task. Its assignment decision commits with the creation, and
    /// the run-creation exchange it owes is driven to the run entity.
    pub async fn create_task(&self) {
        self.create_task_with(task_definition()).await;
    }

    /// Creates the task under an explicit definition, for tests that need their
    /// own budget ceilings.
    pub async fn create_task_with(&self, definition: AgentTaskDefinition) {
        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        let _reply = task
            .apply(
                AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, TASK, "1"],
                    )
                    .expect("operation id should be derivable"),
                    creation: Box::new(AgentTaskCreation {
                        definition,
                        input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                            .expect("the input is inline-bounded"),
                        assignee: Some(agent_id()),
                        goal: None,
                        parent: None,
                        dependencies: Vec::new(),
                    }),
                },
                &self.router,
                now,
            )
            .await;
    }

    pub fn run(&self) -> AgentRunEntityStore<RunStore, S> {
        run_entity(&run_scope(), &self.runs, &self.effects)
            .with_effect_policies(self.policies.clone())
    }

    /// Drives everything the task and the run owe until nothing moves.
    ///
    /// This is what a recovery sweep does, and what the entity does to itself
    /// after every transition. It reads only durable state, so calling it after a
    /// crash is the same operation as calling it after a success.
    ///
    /// It returns the first error it hits, which is how an injected crash
    /// surfaces.
    pub async fn pump(&self) -> Result<(), String> {
        for _round in 0..64 {
            let now = self.now();
            let mut task = AgentTaskEntityStore::new(
                task_scope(),
                self.tasks.clone(),
                self.agents.clone(),
                self.history.clone(),
            );
            task.recover(now)
                .await
                .map_err(|error| error.code().to_string())?;
            task.settle_side_effects(&self.router, now)
                .await
                .map_err(|error| error.code().to_string())?;

            let now = self.now();
            let mut run = self.run();
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
                .and_then(|state| state.status())
                .is_some_and(AgentRunStatus::is_terminal);
            if terminal {
                return Ok(());
            }
            if progress.transitions == 0
                && progress.effects_dispatched == 0
                && progress.settled == 0
                && progress.failed == 0
                && answered == 0
            {
                return Ok(());
            }
        }
        Err("the loop did not quiesce".to_string())
    }

    pub async fn run_snapshot(&self) -> Option<AgentRunSnapshot> {
        let mut run = self.run();
        let now = self.now();
        run.recover(now).await.expect("the run should recover");
        run.snapshot().expect("the snapshot should read")
    }

    pub async fn task_snapshot(&self) -> AgentTaskSnapshot {
        let mut task = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        task.snapshot()
            .expect("the snapshot should read")
            .expect("the task exists")
    }
}

impl<A: AgentModelAdapter> Fixture<A, InMemoryAgentRunEffectSink> {
    pub fn dispatched_effects(&self) -> usize {
        self.effects.len(&run_scope())
    }
}
