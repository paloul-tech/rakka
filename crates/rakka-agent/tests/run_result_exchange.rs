//! The result proposal and decision exchange between a run and its task.
//!
//! Specification: sections 9.5 and 9.8; scenario 59 of section 18. Losing the run
//! or the task at *any* point in the result exchange — including after the task
//! has recorded its validation decision but before the run learns of it — must
//! converge on recovery without a second validation, a duplicate completion, or a
//! lost rejection.
//!
//! The asymmetry is the whole design. The task's persisted decision is the source
//! of truth for the validation outcome; the run's persisted state is the source of
//! truth for the run's consequence of it. Neither side ever reads the other's
//! state to find out what happened: the run re-drives its proposal under the same
//! derived id, and the task answers a replayed id from its journal rather than
//! validating again.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    run_entity, sweep_crash_points, CrashingStateStore, DeferredExchangeRouter, ExchangeFault,
    InProcessRunEntityTransport, InProcessTaskEntityTransport, ScriptedDispatcher,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId, AgentEntityClass,
    AgentEntityCommand, AgentEntityState, AgentEntityStore, AgentExchangeRouter, AgentId,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRevisionNumber,
    AgentRevisionProvenance, AgentRunEntityStore, AgentRunScope, AgentRunSnapshot, AgentRunState,
    AgentRunStatus, AgentRunTerminalReason, AgentSchemaId, AgentSchemaRef, AgentScope,
    AgentSettings, AgentTaskContent, AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId,
    AgentTaskEntityCommand, AgentTaskEntityStore, AgentTaskHistoryCursor, AgentTaskHistoryKind,
    AgentTaskHistoryStore, AgentTaskLimits, AgentTaskResultCheck, AgentTaskResultRule,
    AgentTaskRuleId, AgentTaskScope, AgentTaskSnapshot, AgentTaskState, AgentTaskStatus,
    InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore, TenantId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};
use rakka_persistence::InMemoryDurableStateStore;

type TaskStore = CrashingStateStore<AgentTaskState>;
type RunStore = CrashingStateStore<AgentRunState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK: &str = "ticket-1";
const TASK_DEFINITION: &str = "resolve-ticket";
const MAX_REJECTIONS: u32 = 2;

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

/// A non-empty answer is required, two rejections are tolerated, and the run may
/// take at most four autonomous iterations.
fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_limits(AgentTaskLimits::new().with_max_result_rejections(MAX_REJECTIONS))
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

/// A turn that proposes a result the deterministic rule accepts.
fn valid_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
        AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
            .expect("the proposal is inline-bounded"),
    )
}

/// A turn that proposes a result the deterministic rule refuses: the answer is
/// empty, and `NonEmptyString` says no.
fn invalid_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
        AgentTaskContent::inline(serde_json::json!({ "answer": "" }))
            .expect("the proposal is inline-bounded"),
    )
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

struct Fixture {
    tasks: TaskStore,
    agents: AgentStore,
    runs: RunStore,
    history: InMemoryAgentTaskHistoryStore,
    effects: InMemoryAgentRunEffectSink,
    router: AgentExchangeRouter,
    task_transport:
        InProcessTaskEntityTransport<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore>,
    dispatcher: ScriptedDispatcher,
    clock: Arc<AtomicU64>,
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
            .with_route(AgentEntityClass::Task, Arc::new(task_transport.clone()))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport));
        deferred.install(router.clone());

        Self {
            tasks,
            agents,
            runs,
            history,
            effects,
            router,
            task_transport,
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

    fn task(&self) -> AgentTaskEntityStore<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore> {
        AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        )
    }

    fn run(&self) -> AgentRunEntityStore<RunStore, InMemoryAgentRunEffectSink> {
        run_entity(&run_scope(), &self.runs, &self.effects)
    }

    async fn create_task(&self) {
        let mut task = self.task();
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
                now,
            )
            .await;
    }

    /// Drives everything both entities owe until nothing moves. Reads only durable
    /// state, so calling it after a fault is the same operation as calling it
    /// after a success.
    async fn pump(&self) -> Result<(), String> {
        for _round in 0..64 {
            let now = self.now();
            let mut task = self.task();
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
                && answered == 0
            {
                return Ok(());
            }
        }
        Err("the exchange did not converge".to_string())
    }

    async fn run_snapshot(&self) -> AgentRunSnapshot {
        let mut run = self.run();
        let now = self.now();
        run.recover(now).await.expect("the run should recover");
        run.snapshot()
            .expect("the snapshot should read")
            .expect("the run exists")
    }

    async fn task_snapshot(&self) -> AgentTaskSnapshot {
        let mut task = self.task();
        let now = self.now();
        task.recover(now).await.expect("the task should recover");
        task.snapshot()
            .expect("the snapshot should read")
            .expect("the task exists")
    }

    /// How many entries of one kind the task's append-only history holds.
    ///
    /// This is what makes "one validation" checkable rather than assertable: a
    /// second validation would leave a second row, whatever the bounded state
    /// happened to end up saying.
    async fn history_count(&self, kind: AgentTaskHistoryKind) -> usize {
        let mut count = 0;
        let mut cursor = Some(AgentTaskHistoryCursor::start());
        while let Some(position) = cursor {
            let page = AgentTaskHistoryStore::read(&self.history, &task_scope(), position)
                .await
                .expect("the history should read");
            count += page
                .entries
                .iter()
                .filter(|entry| entry.kind == kind)
                .count();
            cursor = page.next;
        }
        count
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn losing_the_task_after_it_records_its_decision_converges_on_one_completion() {
    // Scenario 59, the hardest window: the task validated the proposal, durably
    // recorded that it accepted it, and *then* the reply was lost. The run has no
    // way to tell this apart from an envelope that never arrived — and it must not
    // try to. It re-drives the same proposal id, and the task answers from its
    // journal rather than validating a second time.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    fx.task_transport.inject(ExchangeFault::LoseReply);
    fx.pump().await.expect("the exchange converges");

    let run = fx.run_snapshot().await;
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::ResultAccepted)
    );

    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Completed);
    assert_eq!(
        task.rejection_count, 0,
        "the accepted proposal must never be re-validated into a rejection"
    );

    // The append-only history is the witness: one proposal was validated, and one
    // result was accepted, however many times the exchange was re-driven.
    assert_eq!(
        fx.history_count(AgentTaskHistoryKind::ResultProposed).await,
        1,
        "the task validated exactly one proposal"
    );
    assert_eq!(
        fx.history_count(AgentTaskHistoryKind::ResultAccepted).await,
        1,
        "the task accepted exactly one result"
    );
}

#[tokio::test]
async fn a_lost_rejection_is_recovered_not_dropped() {
    // The other half of scenario 59: the task *rejected* the proposal, and the
    // reply was lost. A rejection is a durable decision, not a failure — so the
    // re-drive must return the original rejection, and the run must spend the
    // iteration it cost rather than silently getting the proposal accepted.
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(invalid_turn())
            .with_turn(valid_turn("resolved")),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;

    fx.task_transport.inject(ExchangeFault::LoseReply);
    fx.pump().await.expect("the exchange converges");

    let task = fx.task_snapshot().await;
    assert_eq!(
        task.rejection_count, 1,
        "the lost rejection was recovered, and counted exactly once"
    );
    assert_eq!(task.status, AgentTaskStatus::Completed);

    assert_eq!(
        fx.history_count(AgentTaskHistoryKind::ResultRejected).await,
        1,
        "one rejection decision, however often the exchange was re-driven"
    );
    assert_eq!(
        fx.history_count(AgentTaskHistoryKind::ResultAccepted).await,
        1,
    );

    // The run took the feedback and iterated, exactly once.
    let run = fx.run_snapshot().await;
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(run.turn, 2, "one rejected turn, then one accepted turn");
    assert_eq!(fx.dispatcher.model_calls(), 2);
}

#[tokio::test]
async fn a_duplicate_proposal_delivery_returns_the_original_decision() {
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    fx.task_transport.inject(ExchangeFault::DeliverTwice);
    fx.pump().await.expect("the exchange converges");

    assert_eq!(
        fx.history_count(AgentTaskHistoryKind::ResultProposed).await,
        1,
        "a duplicate delivery is deduplicated, not validated twice"
    );
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::Completed);
    assert_eq!(fx.run_snapshot().await.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn a_lost_proposal_envelope_is_re_driven_under_the_same_id() {
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
    fx.instantiate_agent().await;
    fx.create_task().await;

    // The first two deliveries never reach the task. The run cannot tell that
    // apart from a lost reply, and must not try: it keeps the proposal outstanding
    // and re-drives the same id.
    fx.task_transport.inject(ExchangeFault::LoseEnvelope);
    fx.task_transport.inject(ExchangeFault::LoseEnvelope);
    fx.pump().await.expect("the exchange converges");

    let run = fx.run_snapshot().await;
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        fx.history_count(AgentTaskHistoryKind::ResultProposed).await,
        1,
        "one proposal reached the task, under the id the run first minted"
    );
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::Completed);
}

#[tokio::test]
async fn losing_the_run_at_any_write_of_the_result_exchange_converges() {
    // Scenario 59, the run's side: kill the run's owner at every durable write it
    // makes, on both sides of the compare-and-set.
    let reference = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.runs.writes();
    assert!(writes >= 5);

    sweep_crash_points(writes, |nth, point| async move {
        let fx = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
        fx.instantiate_agent().await;

        fx.runs.crash_at(nth, point);
        fx.create_task().await;
        let _crashed = fx.pump().await;

        fx.runs.assert_crash_fired(nth, point);
        fx.runs.survive();
        fx.pump().await.unwrap_or_else(|error| {
            panic!("run crash {point:?} at write {nth} did not converge: {error}")
        });

        assert_eq!(
            fx.run_snapshot().await.status,
            AgentRunStatus::Completed,
            "run crash {point:?} at write {nth}"
        );
        assert_eq!(
            fx.task_snapshot().await.status,
            AgentTaskStatus::Completed,
            "run crash {point:?} at write {nth}"
        );
        assert_eq!(
            fx.history_count(AgentTaskHistoryKind::ResultProposed).await,
            1,
            "run crash {point:?} at write {nth} caused a second validation"
        );
        assert_eq!(
            fx.history_count(AgentTaskHistoryKind::ResultAccepted).await,
            1,
            "run crash {point:?} at write {nth} caused a duplicate completion"
        );
    })
    .await;
}

#[tokio::test]
async fn losing_the_task_at_any_write_of_the_result_exchange_converges() {
    // Scenario 59, the task's side: kill the *task's* owner at every durable write
    // it makes. The window that matters is the one where the task committed its
    // validation decision and then died before the reply left — after which it
    // must return that same decision, and never validate again.
    let reference = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
    reference.instantiate_agent().await;
    reference.tasks.reset_writes();
    reference.create_task().await;
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.tasks.writes();
    assert!(writes >= 3);

    sweep_crash_points(writes, |nth, point| async move {
        let fx = Fixture::new(ScriptedDispatcher::new().with_turn(valid_turn("resolved")));
        fx.instantiate_agent().await;

        fx.tasks.crash_at(nth, point);
        fx.create_task().await;
        let _crashed = fx.pump().await;

        fx.tasks.assert_crash_fired(nth, point);
        fx.tasks.survive();
        // The ingress re-delivers its command, because the ingress is what owns
        // the creation exchange: a task whose owner died before the creation
        // committed does not exist, and nothing inside the runtime can invent
        // it. The command is deduplicated on the operation id the ingress
        // minted, so a task that *did* commit is not created a second time
        // ([specification 9.8](../../../docs/plans/rakka-agent/spec.md)).
        fx.create_task().await;
        fx.pump().await.unwrap_or_else(|error| {
            panic!("task crash {point:?} at write {nth} did not converge: {error}")
        });

        let task = fx.task_snapshot().await;
        assert_eq!(
            task.status,
            AgentTaskStatus::Completed,
            "task crash {point:?} at write {nth}"
        );
        assert_eq!(
            task.rejection_count, 0,
            "task crash {point:?} at write {nth} re-validated an accepted proposal"
        );
        assert_eq!(
            fx.run_snapshot().await.status,
            AgentRunStatus::Completed,
            "task crash {point:?} at write {nth}"
        );
        assert_eq!(
            fx.history_count(AgentTaskHistoryKind::ResultAccepted).await,
            1,
            "task crash {point:?} at write {nth} caused a duplicate completion"
        );
    })
    .await;
}

#[tokio::test]
async fn an_exhausted_rejection_budget_fails_the_run_without_accepting_the_proposal() {
    // The task tolerates two rejections. The model never proposes anything valid,
    // so the task fails — and the run fails with it, rather than quietly having a
    // refused result treated as accepted ([specification 9.2]).
    let fx = Fixture::new(
        ScriptedDispatcher::new()
            .with_turn(invalid_turn())
            .with_turn(invalid_turn())
            .with_turn(invalid_turn()),
    );
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the exchange converges");

    let task = fx.task_snapshot().await;
    assert_eq!(task.status, AgentTaskStatus::Failed);
    assert_eq!(task.rejection_count, MAX_REJECTIONS);
    assert!(task.accepted_result.is_none());

    let run = fx.run_snapshot().await;
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert_eq!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::ResultRejectionsExhausted)
    );
    assert_eq!(
        fx.history_count(AgentTaskHistoryKind::ResultRejected).await,
        usize::try_from(MAX_REJECTIONS).expect("the count fits"),
    );
}
