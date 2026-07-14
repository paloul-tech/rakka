//! Typed result proposals, deterministic validation, and the rejection budget.
//!
//! Specification: sections 9.1, 9.2, and 9.8; scenario 40 of section 18. A
//! malformed or rule-rejected result must never complete the task, must persist
//! exactly one rejection decision however often it is redelivered, and must
//! consume only the bounded additional iterations the definition allows.
//!
//! The proposal travels the durable substrate, initiated by the run and decided
//! by the task, exactly as specification 9.8 requires: the task's persisted
//! decision is the source of truth for the validation outcome, and it is what
//! comes home as the exchange's reply.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{
    InProcessExchangeTransport, InProcessTaskEntityTransport, RunAcceptanceProbe,
    RunAcceptanceProbeState,
};
use rakka_agent::{
    drive_pending_exchanges, AgentAssignmentGeneration, AgentAuthorityEnvelope, AgentDefinition,
    AgentDefinitionId, AgentEntityAddress, AgentEntityClass, AgentEntityCommand, AgentEntityState,
    AgentEntityStore, AgentExchangeEnvelope, AgentExchangeHost, AgentExchangeKind,
    AgentExchangePayload, AgentExchangeRouter, AgentId, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunScope, AgentSchemaId, AgentSchemaRef,
    AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation, AgentTaskDecision,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityCommand, AgentTaskEntityReply,
    AgentTaskEntityStore, AgentTaskHistoryCursor, AgentTaskHistoryKind, AgentTaskHistoryStore,
    AgentTaskId, AgentTaskLimits, AgentTaskResultCheck, AgentTaskResultProposal,
    AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskSnapshot, AgentTaskState,
    AgentTaskStatus, InMemoryAgentTaskHistoryStore, TenantId, TypedTask,
    AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentCorrelationId, AgentTimestampMillis, PrincipalRef,
};
use rakka_persistence::InMemoryDurableStateStore;
use serde::{Deserialize, Serialize};

type TaskStore = InMemoryDurableStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = InMemoryDurableStateStore<RunAcceptanceProbeState>;
type RunHost = AgentExchangeHost<RunAcceptanceProbe, RunStore>;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK: &str = "ticket-1";
const TASK_DEFINITION: &str = "resolve-ticket";
const MAX_REJECTIONS: u32 = 2;

/// The application's Rust type for this task's result, bound to the definition's
/// schema reference by [`TypedTask`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TicketResult {
    answer: String,
    confidence: i64,
}

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("agent id should be valid")
}

fn task_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid")
}

fn run_scope() -> AgentRunScope {
    let run =
        rakka_agent::run_id_for_assignment(task_scope().task(), AgentAssignmentGeneration::new(1))
            .expect("the run id should be derivable");
    AgentRunScope::new(tenant(), agent_id(), run).expect("run scope should be valid")
}

fn schema(id: &str) -> AgentSchemaRef {
    AgentSchemaRef::new(
        AgentSchemaId::new(id).expect("schema id should be valid"),
        AgentRevisionNumber::INITIAL,
    )
}

fn rule(id: &str, check: AgentTaskResultCheck) -> AgentTaskResultRule {
    AgentTaskResultRule::new(
        AgentTaskRuleId::new(id).expect("rule id should be valid"),
        check,
    )
}

/// The task requires a non-empty answer and a confidence between 0 and 100.
/// Both rules are pure functions of the proposed value; nothing here can call a
/// model or load an artifact.
fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("task definition id should be valid"),
        "Resolve one customer support ticket.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_limits(AgentTaskLimits::new().with_max_result_rejections(MAX_REJECTIONS))
    .with_result_rule(rule(
        "answer-present",
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
    .with_result_rule(rule(
        "confidence-in-range",
        AgentTaskResultCheck::IntegerRange {
            pointer: "/confidence".to_string(),
            minimum: Some(0),
            maximum: Some(100),
        },
    ))
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

/// A running task: created, assigned, and durably accepted by its run.
struct Fixture {
    tasks: TaskStore,
    agents: AgentStore,
    runs: RunStore,
    history: InMemoryAgentTaskHistoryStore,
    task_router: AgentExchangeRouter,
    run_router: AgentExchangeRouter,
    clock: Arc<AtomicU64>,
}

impl Fixture {
    async fn running() -> Self {
        let tasks = TaskStore::new();
        let agents = AgentStore::new();
        let runs = RunStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let clock = Arc::new(AtomicU64::new(1));

        // The task sends its assignment to the run; the run sends its proposal to
        // the task. Both directions are the same durable substrate.
        let task_router = AgentExchangeRouter::new().with_route(
            AgentEntityClass::Run,
            Arc::new(InProcessExchangeTransport::new(
                RunAcceptanceProbe::accepting(),
                runs.clone(),
                clock.clone(),
            )),
        );
        let run_router = AgentExchangeRouter::new().with_route(
            AgentEntityClass::Task,
            Arc::new(InProcessTaskEntityTransport::new(
                tasks.clone(),
                agents.clone(),
                history.clone(),
                task_router.clone(),
                clock.clone(),
            )),
        );

        let fx = Self {
            tasks,
            agents,
            runs,
            history,
            task_router,
            run_router,
            clock,
        };
        fx.instantiate_agent().await;
        fx.create_task().await;
        fx
    }

    fn now(&self) -> AgentTimestampMillis {
        AgentTimestampMillis::new(self.clock.fetch_add(1, Ordering::SeqCst))
    }

    async fn instantiate_agent(&self) {
        let scope = AgentScope::new(tenant(), agent_id()).expect("agent scope should be valid");
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.task_definitions.insert(
            AgentTaskDefinitionId::new(TASK_DEFINITION)
                .expect("task definition id should be valid"),
        );
        let definition = AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
            "Resolves customer support tickets end to end.",
            envelope,
        )
        .expect("the agent definition should be valid");

        let mut agent = AgentEntityStore::new(scope.clone(), self.agents.clone());
        agent.recover().await.expect("the agent should recover");
        agent
            .apply(AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &scope,
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
        let creation = AgentTaskCreation {
            definition: task_definition(),
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(agent_id()),
            goal: None,
            parent: None,
            dependencies: Vec::new(),
            created_at: AgentTimestampMillis::new(1),
        };

        let mut entity = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        entity
            .recover(now)
            .await
            .expect("the task entity should recover");
        entity
            .apply(
                AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, TASK, "1"],
                    )
                    .expect("operation id should be derivable"),
                    creation: Box::new(creation),
                },
                &self.task_router,
                now,
            )
            .await
            .expect("the task should be created");

        assert_eq!(
            self.snapshot().await.status,
            AgentTaskStatus::InProgress,
            "the task is assigned and its run has durably accepted"
        );
    }

    /// The run entity of slice 1.5, standing in: it initiates the proposal from
    /// its own durable state and settles the task's decision.
    async fn run_host(&self) -> RunHost {
        let mut host = AgentExchangeHost::new(
            AgentEntityAddress::Run(run_scope()),
            RunAcceptanceProbe::accepting(),
            self.runs.clone(),
        );
        host.recover(self.now())
            .await
            .expect("the run should recover");
        host
    }

    /// Proposes a typed result and returns the task's durable decision.
    async fn propose(&self, proposal_id: &str, content: serde_json::Value) -> AgentTaskDecision {
        self.propose_with(proposal_id, |proposal| {
            proposal.content =
                AgentTaskContent::inline(content.clone()).expect("the result is inline-bounded");
        })
        .await
    }

    /// Proposes a result the caller may corrupt first, which is how the malformed
    /// cases are driven.
    async fn propose_with<F>(&self, proposal_id: &str, corrupt: F) -> AgentTaskDecision
    where
        F: FnOnce(&mut AgentTaskResultProposal),
    {
        let operation_id = AgentOperationId::new(
            AgentOperationKind::ResultProposal,
            [TENANT, TASK, proposal_id],
        )
        .expect("operation id should be derivable");

        let definition = task_definition();
        let mut proposal = AgentTaskResultProposal {
            proposal_id: operation_id.clone(),
            agent: agent_id(),
            run: run_scope().run().clone(),
            generation: AgentAssignmentGeneration::new(1),
            definition_id: definition.definition_id.clone(),
            definition_version: definition.version,
            result_schema: definition.result_schema.clone(),
            content: AgentTaskContent::inline(serde_json::json!({}))
                .expect("the result is inline-bounded"),
            evidence: Vec::new(),
            causation_id: AgentCausationId::new(format!("cause-{proposal_id}")),
            proposed_at: self.now(),
        };
        corrupt(&mut proposal);

        let now = self.now();
        let envelope = AgentExchangeEnvelope::new(
            operation_id.clone(),
            AgentExchangeKind::ResultProposal,
            AgentEntityAddress::Run(run_scope()),
            AgentEntityAddress::Task(task_scope()),
            AgentExchangePayload::encode(AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE, &proposal)
                .expect("the proposal payload is bounded"),
            AgentCorrelationId::new(operation_id.as_str()),
            now,
        )
        .expect("the proposal envelope should be valid");

        let mut run = self.run_host().await;
        run.initiate(now, move |_state| Ok(vec![envelope]))
            .await
            .expect("the run should persist its proposal before sending it");

        let now = self.now();
        drive_pending_exchanges(&mut run, &self.run_router, now)
            .await
            .expect("the courier should run");

        self.decision(&operation_id).await
    }

    /// Reads the decision the run settled, from the run's own durable state.
    async fn decision(&self, operation_id: &AgentOperationId) -> AgentTaskDecision {
        let run = self.run_host().await;
        let result = run
            .state()
            .expect("the run is recovered")
            .journal()
            .settled_result(operation_id, AgentExchangeKind::ResultProposal)
            .expect("the exchange kind matches")
            .expect("the run settled the proposal")
            .clone();
        result
            .payload()
            .decode(rakka_agent::AGENT_TASK_DECISION_PAYLOAD_TYPE)
            .expect("the decision decodes")
    }

    async fn snapshot(&self) -> AgentTaskSnapshot {
        let mut entity = AgentTaskEntityStore::new(
            task_scope(),
            self.tasks.clone(),
            self.agents.clone(),
            self.history.clone(),
        );
        let now = self.now();
        entity
            .recover(now)
            .await
            .expect("the task entity should recover");
        match entity
            .apply(AgentTaskEntityCommand::Describe, &self.task_router, now)
            .await
            .expect("describe should succeed")
        {
            AgentTaskEntityReply::Snapshot(Some(snapshot)) => *snapshot,
            other => panic!("expected a snapshot, got {other:?}"),
        }
    }

    async fn history_kinds(&self) -> Vec<AgentTaskHistoryKind> {
        let mut kinds = Vec::new();
        let mut cursor = Some(AgentTaskHistoryCursor::start());
        while let Some(position) = cursor {
            let page = AgentTaskHistoryStore::read(&self.history, &task_scope(), position)
                .await
                .expect("the history should read");
            kinds.extend(page.entries.iter().map(|entry| entry.kind));
            cursor = page.next;
        }
        kinds
    }

    fn rejected(decision: &AgentTaskDecision) -> (&rakka_agent::AgentTaskRejection, u32) {
        match decision {
            AgentTaskDecision::Rejected {
                rejection,
                remaining_iterations,
                ..
            } => (rejection, *remaining_iterations),
            other => panic!("expected a rejection, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_rule_rejected_result_never_completes_the_task_and_persists_one_rejection() {
    // Scenario 40.
    let fx = Fixture::running().await;

    // The confidence is out of range. A deterministic rule refuses it.
    let decision = fx
        .propose(
            "1",
            serde_json::json!({ "answer": "restart the router", "confidence": 900 }),
        )
        .await;

    let (rejection, remaining) = Fixture::rejected(&decision);
    assert_eq!(rejection.cause.reason, "integer-out-of-range");
    assert_eq!(
        rejection.cause.rule_id.as_ref().map(ToString::to_string),
        Some("confidence-in-range".to_string()),
        "the rejection names the exact rule that refused the result"
    );
    assert_eq!(rejection.rejection_count, 1);
    assert_eq!(remaining, MAX_REJECTIONS - 1);

    let snapshot = fx.snapshot().await;
    assert_eq!(
        snapshot.status,
        AgentTaskStatus::InProgress,
        "a refused result never completes the task"
    );
    assert_eq!(snapshot.rejection_count, 1);
    assert!(snapshot.accepted_result.is_none());

    // The proposal is redelivered — the run crashed before it settled the reply,
    // and re-drives the same operation id. The task returns its original decision
    // without validating again, so exactly one rejection decision exists.
    let replayed = fx
        .propose(
            "1",
            serde_json::json!({ "answer": "restart the router", "confidence": 900 }),
        )
        .await;
    assert_eq!(
        replayed, decision,
        "the original decision comes back unchanged"
    );
    assert_eq!(fx.snapshot().await.rejection_count, 1);
    assert_eq!(
        fx.history_kinds()
            .await
            .iter()
            .filter(|kind| **kind == AgentTaskHistoryKind::ResultRejected)
            .count(),
        1,
        "one rejection decision is persisted, however often the proposal arrives"
    );
}

#[tokio::test]
async fn exhausting_the_rejection_budget_fails_the_task_rather_than_accepting_the_result() {
    // Scenario 40: only bounded additional iterations are consumed.
    let fx = Fixture::running().await;

    let first = fx
        .propose("1", serde_json::json!({ "answer": "", "confidence": 50 }))
        .await;
    let (_, remaining) = Fixture::rejected(&first);
    assert_eq!(remaining, 1);
    assert_eq!(fx.snapshot().await.status, AgentTaskStatus::InProgress);

    let second = fx
        .propose("2", serde_json::json!({ "answer": "", "confidence": 50 }))
        .await;
    let (rejection, remaining) = Fixture::rejected(&second);
    assert_eq!(rejection.rejection_count, MAX_REJECTIONS);
    assert_eq!(remaining, 0);

    let failed = fx.snapshot().await;
    assert_eq!(
        failed.status,
        AgentTaskStatus::Failed,
        "the task fails rather than silently accepting the result it just refused"
    );
    assert_eq!(
        failed
            .terminal_reason
            .as_ref()
            .map(rakka_agent::AgentTaskTerminalReason::code),
        Some("result-rejections-exhausted")
    );
    assert!(failed.accepted_result.is_none());

    // A further proposal is refused outright, and cannot resurrect the task.
    let late = fx
        .propose(
            "3",
            serde_json::json!({ "answer": "restart the router", "confidence": 90 }),
        )
        .await;
    assert!(matches!(late, AgentTaskDecision::Refused { .. }));
    assert_eq!(fx.snapshot().await.status, AgentTaskStatus::Failed);
}

#[tokio::test]
async fn a_result_proposed_under_a_mismatched_definition_revision_fails_closed() {
    let fx = Fixture::running().await;

    let decision = fx
        .propose_with("1", |proposal| {
            proposal.content = AgentTaskContent::inline(
                serde_json::json!({ "answer": "restart the router", "confidence": 90 }),
            )
            .expect("the result is inline-bounded");
            // The run validated against a definition revision the task does not
            // run. Interpreting the result anyway would mean validating it under
            // rules it was never checked against.
            proposal.definition_version = AgentRevisionNumber::new(7);
        })
        .await;

    let (rejection, _) = Fixture::rejected(&decision);
    assert_eq!(rejection.cause.reason, "definition-version-mismatch");
    assert_eq!(fx.snapshot().await.status, AgentTaskStatus::InProgress);
    assert!(fx.snapshot().await.accepted_result.is_none());
}

#[tokio::test]
async fn a_proposal_from_a_superseded_generation_is_fenced_and_costs_the_live_run_nothing() {
    let fx = Fixture::running().await;

    let decision = fx
        .propose_with("1", |proposal| {
            proposal.content = AgentTaskContent::inline(
                serde_json::json!({ "answer": "restart the router", "confidence": 90 }),
            )
            .expect("the result is inline-bounded");
            proposal.generation = AgentAssignmentGeneration::new(99);
        })
        .await;

    match decision {
        AgentTaskDecision::Refused { code, status } => {
            assert_eq!(code, "stale-assignment-generation");
            assert_eq!(status, AgentTaskStatus::InProgress);
        }
        other => panic!("expected a fenced refusal, got {other:?}"),
    }

    let snapshot = fx.snapshot().await;
    assert_eq!(
        snapshot.rejection_count, 0,
        "a superseded run's proposal must not consume the live run's rejection budget"
    );
    assert_eq!(snapshot.status, AgentTaskStatus::InProgress);
}

#[tokio::test]
async fn an_accepted_result_completes_the_task_and_decodes_to_its_rust_type() {
    let fx = Fixture::running().await;
    let typed: TypedTask<serde_json::Value, TicketResult> = TypedTask::new(task_definition());

    let result = TicketResult {
        answer: "restart the router".to_string(),
        confidence: 90,
    };
    let content = typed
        .result(&result)
        .expect("the typed result encodes within the inline bound");

    let decision = fx
        .propose_with("1", |proposal| proposal.content = content.clone())
        .await;
    assert!(matches!(decision, AgentTaskDecision::Accepted { .. }));

    let completed = fx.snapshot().await;
    assert_eq!(completed.status, AgentTaskStatus::Completed);
    assert_eq!(
        completed
            .terminal_reason
            .as_ref()
            .map(rakka_agent::AgentTaskTerminalReason::code),
        Some("result-accepted")
    );

    let accepted = completed
        .accepted_result
        .as_ref()
        .expect("the task holds its accepted typed result");
    assert_eq!(
        typed
            .decode_accepted(accepted)
            .expect("the accepted result decodes under the definition it was accepted with"),
        result
    );

    // The task is terminal, so its run is fenced: nothing may complete it twice.
    let late = fx
        .propose(
            "2",
            serde_json::json!({ "answer": "other", "confidence": 10 }),
        )
        .await;
    assert!(matches!(late, AgentTaskDecision::Refused { .. }));
    assert_eq!(fx.snapshot().await.status, AgentTaskStatus::Completed);
}
