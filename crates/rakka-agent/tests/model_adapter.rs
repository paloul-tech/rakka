//! The model adapter, end to end over the durable effect path.
//!
//! Specification: sections 10.1, 10.2, and 10.4; the durable execution rule of
//! 9.5. One scripted model turn must run end to end through the *same* durable
//! effect path a production provider uses — the run persists a model effect and
//! passivates, a dispatcher invokes the adapter and returns the turn as a durable
//! result command, and the run resumes from durable state alone
//! ([specification 10.4](../../../docs/plans/rakka-agent/spec.md): the test
//! adapter must not make tests pass by invoking the loop directly around
//! persistence).
//!
//! [`drive_one_turn`] is the shared body. It is exercised twice: by the
//! deterministic [`DeterministicModelAdapter`], which needs no `rig` feature, and
//! — under the `rig` feature — by the Rig-backed [`RigModelAdapter`] over a
//! scripted stub provider. Both must converge on the same completed run, because
//! the adapter is the only thing that differs; the durable path is identical.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    run_entity, DeferredExchangeRouter, DeterministicModelAdapter, InProcessRunEntityTransport,
    InProcessTaskEntityTransport, ScriptedDispatcher,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentBudgetCeilings, AgentDefinition, AgentDefinitionId,
    AgentEntityClass, AgentEntityCommand, AgentEntityStore, AgentExchangeRouter, AgentId,
    AgentLoopPhase, AgentModelAdapter, AgentModelTurn, AgentModelUsage, AgentOperationId,
    AgentOperationKind, AgentRevisionNumber, AgentRevisionProvenance, AgentRunScope,
    AgentRunSnapshot, AgentRunState, AgentRunStatus, AgentRunTerminalReason, AgentSchemaId,
    AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityCommand, AgentTaskEntityStore,
    AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskSnapshot,
    AgentTaskState, AgentTaskStatus, InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore,
    TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};
use rakka_persistence::InMemoryDurableStateStore;

type TaskStore = InMemoryDurableStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<rakka_agent::AgentEntityState>;
type RunStore = InMemoryDurableStateStore<AgentRunState>;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK: &str = "ticket-1";
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

fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        rakka_agent::AgentTaskId::new(TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid")
}

fn run_scope() -> AgentRunScope {
    let run = rakka_agent::run_id_for_assignment(
        task_scope().task(),
        rakka_agent::AgentAssignmentGeneration::new(1),
    )
    .expect("the run id should be derivable");
    AgentRunScope::new(tenant(), agent_id(), run).expect("run scope should be valid")
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

/// The turn the deterministic adapter scripts: it proposes the resolved answer,
/// with the same usage the scripted Rig provider reports.
fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 0,
        })
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

/// One durable store per entity class, one clock, and the router that carries the
/// assignment from the task to the run and the proposal back again — generic over
/// the model adapter the dispatcher answers with.
struct Fixture<A: AgentModelAdapter> {
    tasks: TaskStore,
    agents: AgentStore,
    runs: RunStore,
    history: InMemoryAgentTaskHistoryStore,
    effects: InMemoryAgentRunEffectSink,
    router: AgentExchangeRouter,
    dispatcher: ScriptedDispatcher<A>,
    clock: Arc<AtomicU64>,
}

impl<A: AgentModelAdapter> Fixture<A> {
    fn new(dispatcher: ScriptedDispatcher<A>) -> Self {
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

        Self {
            tasks,
            agents,
            runs,
            history,
            effects,
            router,
            dispatcher,
            clock,
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
                provenance: Box::new(provenance(1)),
            })
            .await
            .expect("the agent should instantiate");
    }

    async fn create_task(&self) {
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
                        definition: task_definition(),
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

    fn run(&self) -> rakka_agent::AgentRunEntityStore<RunStore, InMemoryAgentRunEffectSink> {
        run_entity(&run_scope(), &self.runs, &self.effects)
    }

    /// Drives everything the task and the run owe until nothing moves. It reads
    /// only durable state and re-materializes each entity every round, so it is a
    /// faithful recovery sweep, not a shortcut around persistence.
    async fn pump(&self) -> Result<(), String> {
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

    async fn run_snapshot(&self) -> Option<AgentRunSnapshot> {
        let mut run = self.run();
        let now = self.now();
        run.recover(now).await.expect("the run should recover");
        run.snapshot().expect("the snapshot should read")
    }

    async fn task_snapshot(&self) -> AgentTaskSnapshot {
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

/// The shared body: one scripted turn drives a run to completion through the
/// durable effect path, and completes the public task only through the task
/// entity's decision — never by the run mutating its own state.
async fn drive_one_turn<A: AgentModelAdapter>(dispatcher: ScriptedDispatcher<A>) {
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    fx.create_task().await;

    // The task assigned the run and the run durably accepted, all before a
    // single model call.
    let accepted = fx.run_snapshot().await.expect("the run accepted");
    assert_eq!(accepted.generation.get(), 1);

    fx.pump().await.expect("the loop should run to completion");

    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.phase, AgentLoopPhase::Complete);
    assert_eq!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::ResultAccepted)
    );

    // The turn came back over the durable effect path: one model call, one
    // durable effect, one turn.
    assert_eq!(fx.dispatcher.model_calls(), 1);
    assert_eq!(fx.effects.len(&run_scope()), 1);
    assert_eq!(run.turn, 1);

    // The run's own state records the consequence; the *task* is what made the
    // public task terminal ([specification 9.5]).
    let accepted_result = run.accepted_result.expect("the task accepted a result");
    assert_eq!(
        accepted_result.content.inline_value(),
        Some(&serde_json::json!({ "answer": "resolved" }))
    );

    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);

    // The run charged what the turn billed, in its own ledger.
    assert_eq!(run.budget.tokens(), 15);
    assert_eq!(run.budget.model_calls(), 1);
}

#[tokio::test]
async fn the_deterministic_adapter_drives_one_turn_end_to_end() {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(proposing_turn()),
    );
    drive_one_turn(dispatcher).await;

    // The turn was produced by the adapter, over the effect path, exactly once.
}

#[cfg(feature = "rig")]
#[tokio::test]
async fn the_rig_adapter_drives_the_same_turn_against_a_stub_provider() {
    use rakka_agent::rig::{RigModelAdapter, ScriptedCompletionModel};

    let provider = ScriptedCompletionModel::new()
        .returning_text("I have an answer.")
        .returning_result(serde_json::json!({ "answer": "resolved" }))
        .with_usage(10, 5);
    let dispatcher = ScriptedDispatcher::with_adapter(RigModelAdapter::new(provider));
    drive_one_turn(dispatcher).await;
}
