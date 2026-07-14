//! Bounded task state, and history behind authorized cursors.
//!
//! Specification: section 9.6; scenario 55 of section 18. The materialized state
//! a task transitions on must stay inside its configured limits however long the
//! task runs, while the history and content it accumulates remain available only
//! through bounded cursors and immutable artifact references.
//!
//! The two halves are tested together, because they are one claim: the state
//! stays bounded *because* the history left it, and the history is not lost
//! *because* it is durable somewhere else.

use std::collections::BTreeSet;
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
    AgentRevisionNumber, AgentRevisionProvenance, AgentRunScope, AgentSchemaId, AgentSchemaPolicy,
    AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent, AgentTaskCreation,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskDependencyDeclaration,
    AgentTaskDependencyOutcome, AgentTaskEntityCommand, AgentTaskEntityStore,
    AgentTaskHistoryCursor, AgentTaskHistoryEntry, AgentTaskHistoryKind, AgentTaskHistoryStore,
    AgentTaskId, AgentTaskLimits, AgentTaskResultCheck, AgentTaskResultProposal,
    AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentTaskState, AgentTaskStatus,
    InMemoryAgentTaskHistoryStore, TenantId, AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH,
    AGENT_TASK_HISTORY_MAX_PAGE_SIZE, AGENT_TASK_INLINE_CONTENT_MAX_BYTES,
    AGENT_TASK_MATERIALIZED_MAX_BYTES, AGENT_TASK_MAX_DEPENDENCIES,
    AGENT_TASK_MAX_HISTORY_PER_TRANSITION, AGENT_TASK_PENDING_HISTORY_CAPACITY,
    AGENT_TASK_RESULT_PROPOSAL_PAYLOAD_TYPE, AGENT_TASK_RULE_ONE_OF_MAX_VALUES,
    AGENT_TASK_RULE_POINTER_MAX_LENGTH, AGENT_TASK_RULE_VALUE_MAX_LENGTH,
    AGENT_TASK_STATE_GROWTH_RESERVE_BYTES,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentAuditEventId, AgentCausationId, AgentCorrelationId, AgentTimestampMillis,
    ArtifactKind, ArtifactRef, PrincipalRef, RedactionStatus,
};
use rakka_persistence::InMemoryDurableStateStore;

type TaskStore = InMemoryDurableStateStore<AgentTaskState>;
type AgentStore = InMemoryDurableStateStore<AgentEntityState>;
type RunStore = InMemoryDurableStateStore<RunAcceptanceProbeState>;

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const TASK: &str = "ticket-1";
const TASK_DEFINITION: &str = "resolve-ticket";

/// The task tolerates many rejections, so the run can churn against it long
/// enough for an unbounded state to show itself.
const MAX_REJECTIONS: u32 = 12;

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

fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new(TASK_DEFINITION).expect("task definition id should be valid"),
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

/// An immutable reference to application-owned content: a digest, a size, and a
/// location, and no bytes.
fn artifact(id: &str, bytes: u64) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_string(),
        kind: ArtifactKind::File,
        uri: format!("s3://tickets/{id}"),
        checksum: Some(format!("sha256:{id}")),
        content_type: Some("application/json".to_string()),
        byte_len: Some(bytes),
        retention_class: Some("standard".to_string()),
        encryption: None,
        redaction: RedactionStatus::Unredacted,
        created_at: AgentTimestampMillis::new(1),
        metadata: AgentAttributes::default(),
    }
}

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
    fn new() -> Self {
        let tasks = TaskStore::new();
        let agents = AgentStore::new();
        let runs = RunStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let clock = Arc::new(AtomicU64::new(1));

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

        Self {
            tasks,
            agents,
            runs,
            history,
            task_router,
            run_router,
            clock,
        }
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

    async fn entity(
        &self,
    ) -> AgentTaskEntityStore<TaskStore, AgentStore, InMemoryAgentTaskHistoryStore> {
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

    async fn create(&self, input: AgentTaskContent) -> Result<(), String> {
        let creation = AgentTaskCreation {
            definition: task_definition(),
            input,
            assignee: Some(agent_id()),
            goal: None,
            parent: None,
            dependencies: Vec::new(),
        };

        let mut entity = self.entity().await;
        let now = self.now();
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
            .map(|_| ())
            .map_err(|error| error.code().to_string())
    }

    /// Churns the task: one rejected proposal, which is the transition that
    /// accumulates the most history.
    async fn propose_rejected(&self, proposal_id: &str) {
        let operation_id = AgentOperationId::new(
            AgentOperationKind::ResultProposal,
            [TENANT, TASK, proposal_id],
        )
        .expect("operation id should be derivable");

        let definition = task_definition();
        let proposal = AgentTaskResultProposal {
            proposal_id: operation_id.clone(),
            agent: agent_id(),
            run: run_scope().run().clone(),
            generation: AgentAssignmentGeneration::new(1),
            definition_id: definition.definition_id.clone(),
            definition_version: definition.version,
            result_schema: definition.result_schema.clone(),
            // The rule requires a non-empty answer, so this is always refused.
            content: AgentTaskContent::inline(serde_json::json!({ "answer": "" }))
                .expect("the result is inline-bounded"),
            evidence: vec![artifact(&format!("evidence-{proposal_id}"), 4096)],
            causation_id: AgentCausationId::new(format!("cause-{proposal_id}")),
            proposed_at: self.now(),
        };

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

        let mut run = AgentExchangeHost::new(
            AgentEntityAddress::Run(run_scope()),
            RunAcceptanceProbe::accepting(),
            self.runs.clone(),
        );
        run.recover(self.now())
            .await
            .expect("the run should recover");
        run.initiate(now, move |_state| Ok(vec![envelope]))
            .await
            .expect("the run should persist its proposal before sending it");

        let now = self.now();
        drive_pending_exchanges(&mut run, &self.run_router, now)
            .await
            .expect("the courier should run");
    }

    async fn durable_state(&self) -> AgentTaskState {
        rakka_agent::load_agent_task_state(
            &self.tasks,
            &task_scope(),
            &AgentSchemaPolicy::default(),
        )
        .await
        .expect("the durable read should succeed")
        .expect("the task exists")
    }

    /// Reads the whole history through bounded cursor pages, the only way older
    /// history is reachable at all.
    async fn read_history(&self, page_size: usize) -> (Vec<AgentTaskHistoryEntry>, usize) {
        let mut entries = Vec::new();
        let mut pages = 0;
        let mut cursor = Some(AgentTaskHistoryCursor::start().with_limit(page_size));
        while let Some(position) = cursor {
            let page = AgentTaskHistoryStore::read(&self.history, &task_scope(), position)
                .await
                .expect("the history should read");
            assert!(
                page.entries.len() <= position.limit(),
                "a page never exceeds the cursor's clamped limit"
            );
            pages += 1;
            entries.extend(page.entries);
            cursor = page.next;
        }
        (entries, pages)
    }
}

#[tokio::test]
async fn materialized_state_stays_bounded_while_history_grows() {
    // Scenario 55.
    let fx = Fixture::new();
    fx.instantiate_agent().await;
    fx.create(
        AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
            .expect("the input is inline-bounded"),
    )
    .await
    .expect("the task should be created");

    // Churn the task through many rejections. Every one of them is a decision the
    // task must persist — and none of them may stay in the record it transitions
    // on.
    for index in 0..MAX_REJECTIONS {
        fx.propose_rejected(&index.to_string()).await;
    }

    let state = fx.durable_state().await;
    let task = state.task().expect("the task exists");

    assert_eq!(task.status, AgentTaskStatus::Failed, "the budget is spent");
    assert_eq!(task.rejection_count, MAX_REJECTIONS);
    assert!(
        task.materialized_size_bytes() <= AGENT_TASK_MATERIALIZED_MAX_BYTES,
        "the materialized record is {} bytes, past its {AGENT_TASK_MATERIALIZED_MAX_BYTES} byte limit",
        task.materialized_size_bytes()
    );

    // The record keeps the *current* facts, not the sequence that produced them:
    // one rejection, not twelve.
    let rejection = task
        .last_rejection
        .as_ref()
        .expect("the most recent rejection stays materialized");
    assert_eq!(rejection.rejection_count, MAX_REJECTIONS);
    assert!(
        state.pending_history().is_empty(),
        "the entity flushed everything it owed its sink"
    );

    // Every rejection is nonetheless durable — in the history, reachable only
    // through the cursor.
    let (entries, pages) = fx.read_history(4).await;
    let rejections = entries
        .iter()
        .filter(|entry| entry.kind == AgentTaskHistoryKind::ResultRejected)
        .count();
    assert_eq!(
        rejections, MAX_REJECTIONS as usize,
        "every rejection decision is preserved in history"
    );
    assert!(pages > 1, "the history is paged, not returned in one lump");

    // The proposals themselves never entered the task's state: history records
    // the fingerprint of what was proposed, and the content stays where the
    // application put it.
    let proposed = entries
        .iter()
        .find(|entry| entry.kind == AgentTaskHistoryKind::ResultProposed)
        .expect("proposals are recorded");
    assert!(proposed.digest.is_some());

    // Sequences are monotonic and gapless, so a cursor can never skip a decision.
    for (index, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence.get(), index as u64 + 1);
    }
}

#[tokio::test]
async fn the_widest_transition_never_loses_a_history_entry() {
    // A creation that declares the maximum number of dependencies records more
    // rows in one transition than a small outbox would hold. Not one of them may
    // be dropped: an owed history entry that disappears is audit history lost.
    let fx = Fixture::new();
    fx.instantiate_agent().await;

    let dependencies: Vec<_> = (0..AGENT_TASK_MAX_DEPENDENCIES)
        .map(|index| {
            AgentTaskDependencyDeclaration::new(
                AgentTaskId::new(format!("upstream-{index}")).expect("task id should be valid"),
            )
        })
        .collect();

    let creation = AgentTaskCreation {
        definition: task_definition(),
        input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
            .expect("the input is inline-bounded"),
        assignee: Some(agent_id()),
        goal: None,
        parent: None,
        dependencies,
    };

    let mut entity = fx.entity().await;
    let now = fx.now();
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
            &fx.task_router,
            now,
        )
        .await
        .expect("the task should be created");

    let state = fx.durable_state().await;
    assert_eq!(
        state.task().expect("the task exists").dependencies.len(),
        AGENT_TASK_MAX_DEPENDENCIES
    );
    assert!(
        state.pending_history().is_empty(),
        "everything the transition owed reached the sink"
    );

    let (entries, _) = fx.read_history(AGENT_TASK_MAX_DEPENDENCIES).await;
    assert_eq!(
        entries.len(),
        AGENT_TASK_MAX_DEPENDENCIES + 1,
        "the task's own row plus one per dependency, none of them dropped"
    );
    assert_eq!(entries[0].kind, AgentTaskHistoryKind::Created);
    for (index, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence.get(), index as u64 + 1, "no gaps");
    }
    // The outbox must be able to hold the widest transition the task can run, or
    // an entry would have nowhere to go. It is a compile-time invariant, not a
    // runtime hope.
    const {
        assert!(AGENT_TASK_MAX_HISTORY_PER_TRANSITION <= AGENT_TASK_PENDING_HISTORY_CAPACITY);
    }
}

#[tokio::test]
async fn a_history_cursor_cannot_ask_for_an_unbounded_page() {
    let fx = Fixture::new();
    fx.instantiate_agent().await;
    fx.create(
        AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
            .expect("the input is inline-bounded"),
    )
    .await
    .expect("the task should be created");

    let cursor = AgentTaskHistoryCursor::start().with_limit(usize::MAX);
    assert_eq!(
        cursor.limit(),
        AGENT_TASK_HISTORY_MAX_PAGE_SIZE,
        "a page size is clamped, so no reader can ask a store for an unbounded page"
    );

    let page = AgentTaskHistoryStore::read(&fx.history, &task_scope(), cursor)
        .await
        .expect("the history should read");
    assert!(page.entries.len() <= AGENT_TASK_HISTORY_MAX_PAGE_SIZE);
}

#[tokio::test]
async fn another_tenants_cursor_reveals_nothing() {
    let fx = Fixture::new();
    fx.instantiate_agent().await;
    fx.create(
        AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
            .expect("the input is inline-bounded"),
    )
    .await
    .expect("the task should be created");

    // The same task id under a different tenant is a different scope, and it has
    // no history — not a denied read that betrays the task's existence.
    let other = AgentTaskScope::new(
        TenantId::new("other-tenant"),
        AgentTaskId::new(TASK).expect("task id should be valid"),
    )
    .expect("task scope should be valid");

    let page = AgentTaskHistoryStore::read(&fx.history, &other, AgentTaskHistoryCursor::start())
        .await
        .expect("the history should read");
    assert!(page.entries.is_empty());
    assert!(!page.has_more());
    assert!(fx.history.is_empty(&other));
}

#[tokio::test]
async fn oversized_content_must_arrive_behind_an_artifact_reference() {
    let fx = Fixture::new();
    fx.instantiate_agent().await;

    // Content that would not fit in the task's bounded state is refused at
    // admission, rather than discovered when the record has already grown.
    let oversized = serde_json::json!({
        "transcript": "x".repeat(AGENT_TASK_INLINE_CONTENT_MAX_BYTES + 1),
    });
    let error = AgentTaskContent::inline(oversized.clone())
        .expect_err("oversized inline content is refused");
    assert_eq!(error.code(), "task-content-too-large");

    // The same content behind an immutable artifact reference is accepted: the
    // task holds the reference, and the bytes stay in application-owned storage.
    fx.create(AgentTaskContent::artifact(artifact(
        "ticket-transcript",
        (AGENT_TASK_INLINE_CONTENT_MAX_BYTES + 1) as u64,
    )))
    .await
    .expect("artifact-backed input is accepted");

    let state = fx.durable_state().await;
    let task = state.task().expect("the task exists");
    assert!(task.input.artifact_ref().is_some());
    assert!(task.materialized_size_bytes() <= AGENT_TASK_MATERIALIZED_MAX_BYTES);
    assert_eq!(task.status, AgentTaskStatus::InProgress);
}

#[tokio::test]
async fn a_result_held_behind_an_artifact_cannot_satisfy_a_rule_that_must_inspect_it() {
    // A rule that needs an inspectable value fails closed against artifact-backed
    // content: the task must never claim a result satisfied a rule it could not
    // evaluate.
    let fx = Fixture::new();
    fx.instantiate_agent().await;
    fx.create(
        AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
            .expect("the input is inline-bounded"),
    )
    .await
    .expect("the task should be created");

    let operation_id = AgentOperationId::new(
        AgentOperationKind::ResultProposal,
        [TENANT, TASK, "artifact"],
    )
    .expect("operation id should be derivable");
    let definition = task_definition();
    let proposal = AgentTaskResultProposal {
        proposal_id: operation_id.clone(),
        agent: agent_id(),
        run: run_scope().run().clone(),
        generation: AgentAssignmentGeneration::new(1),
        definition_id: definition.definition_id.clone(),
        definition_version: definition.version,
        result_schema: definition.result_schema.clone(),
        content: AgentTaskContent::artifact(artifact("ticket-result", 64)),
        evidence: Vec::new(),
        causation_id: AgentCausationId::new("cause-artifact"),
        proposed_at: fx.now(),
    };

    let now = fx.now();
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

    let mut run = AgentExchangeHost::new(
        AgentEntityAddress::Run(run_scope()),
        RunAcceptanceProbe::accepting(),
        fx.runs.clone(),
    );
    run.recover(fx.now()).await.expect("the run should recover");
    run.initiate(now, move |_state| Ok(vec![envelope]))
        .await
        .expect("the run should persist its proposal");
    drive_pending_exchanges(&mut run, &fx.run_router, fx.now())
        .await
        .expect("the courier should run");

    let state = fx.durable_state().await;
    let task = state.task().expect("the task exists");
    assert_eq!(task.status, AgentTaskStatus::InProgress);
    assert_eq!(task.rejection_count, 1);
    assert!(task.accepted_result.is_none());
}

#[tokio::test]
async fn an_agent_owned_task_id_reserves_room_for_its_derived_run_ids() {
    // Every assignment derives a run id as `{task}-gen-{generation}`, and the
    // derived id must be a valid identity for every generation the task can
    // reach. An id the widest suffix would push past the identity bound is
    // refused at creation, where the caller can still choose another — never
    // discovered at decision time, where the task would be created and
    // permanently unassignable.
    let fx = Fixture::new();

    async fn create_with_id(fx: &Fixture, id: &str) -> Result<(), String> {
        let scope = AgentTaskScope::new(
            tenant(),
            AgentTaskId::new(id).expect("the id is a valid identity on its own"),
        )
        .expect("the task scope should be valid");
        let mut entity = AgentTaskEntityStore::new(
            scope,
            fx.tasks.clone(),
            fx.agents.clone(),
            fx.history.clone(),
        );
        entity
            .recover(fx.now())
            .await
            .expect("the task entity should recover");
        entity
            .apply(
                AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, id, "1"],
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
                &fx.task_router,
                fx.now(),
            )
            .await
            .map(|_| ())
            .map_err(|error| error.code().to_string())
    }

    let over_bound = "t".repeat(AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH + 1);
    assert_eq!(
        create_with_id(&fx, &over_bound).await,
        Err("task-id-too-long".to_string())
    );

    // At the bound the task is admitted, and even the widest possible
    // generation still derives a valid run id.
    let at_bound = "t".repeat(AGENT_TASK_ASSIGNABLE_ID_MAX_LENGTH);
    create_with_id(&fx, &at_bound)
        .await
        .expect("a task id at the bound is admitted");
    rakka_agent::run_id_for_assignment(
        &AgentTaskId::new(&at_bound).expect("the id is a valid identity"),
        AgentAssignmentGeneration::new(u64::MAX),
    )
    .expect("the widest derivable run id stays a valid identity");
}

#[test]
fn a_task_definition_bounds_its_rule_content() {
    // A rule travels inside the definition, and the definition is part of the
    // materialized record: an unbounded pointer or value set would let a
    // definition grow durable state without limit.
    let invalid = |definition: AgentTaskDefinition| {
        assert_eq!(
            definition.validate().map_err(|error| error.code()),
            Err("invalid-task-definition")
        );
    };

    invalid(task_definition().with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("wide-pointer").expect("rule id should be valid"),
        AgentTaskResultCheck::Required {
            pointer: format!("/{}", "p".repeat(AGENT_TASK_RULE_POINTER_MAX_LENGTH)),
        },
    )));

    invalid(
        task_definition().with_result_rule(AgentTaskResultRule::new(
            AgentTaskRuleId::new("wide-set").expect("rule id should be valid"),
            AgentTaskResultCheck::OneOf {
                pointer: "/status".to_string(),
                values: (0..=AGENT_TASK_RULE_ONE_OF_MAX_VALUES)
                    .map(|value| format!("value-{value}"))
                    .collect(),
            },
        )),
    );

    invalid(
        task_definition().with_result_rule(AgentTaskResultRule::new(
            AgentTaskRuleId::new("wide-value").expect("rule id should be valid"),
            AgentTaskResultCheck::OneOf {
                pointer: "/status".to_string(),
                values: [format!("v{}", "x".repeat(AGENT_TASK_RULE_VALUE_MAX_LENGTH))]
                    .into_iter()
                    .collect(),
            },
        )),
    );
}

/// Pads the definition with bounded one-of rules, roughly 250 bytes per value,
/// so a sweep over `values` walks the materialized record across the admission
/// bound in bounded steps.
fn padded_definition(values: usize) -> AgentTaskDefinition {
    let mut definition = task_definition();
    let mut added = 0;
    let mut rule = 0;
    while added < values {
        let take = (values - added).min(AGENT_TASK_RULE_ONE_OF_MAX_VALUES);
        let set: BTreeSet<String> = (0..take)
            .map(|value| format!("{rule:02}-{value:02}-{}", "x".repeat(240)))
            .collect();
        definition = definition.with_result_rule(AgentTaskResultRule::new(
            AgentTaskRuleId::new(format!("pad-{rule}")).expect("rule id should be valid"),
            AgentTaskResultCheck::OneOf {
                pointer: "/status".to_string(),
                values: set,
            },
        ));
        added += take;
        rule += 1;
    }
    definition
}

#[tokio::test]
async fn an_admitted_task_reserves_growth_headroom_for_its_own_lifecycle() {
    // The identity bound closes "admitted, then unassignable" for run ids; the
    // growth reserve closes it for the record itself. A creation whose record
    // sits inside the reserve window is refused at admission, where the caller
    // can still slim the definition — never admitted and then refused its own
    // assignment, rejection, or terminal reason for want of room.
    let fx = Fixture::new();
    fx.instantiate_agent().await;

    let mut admitted = 0usize;
    let mut refused = 0usize;
    for step in 0..28usize {
        let id = format!("headroom-{step}");
        let scope = AgentTaskScope::new(
            tenant(),
            AgentTaskId::new(&id).expect("the task id should be valid"),
        )
        .expect("the task scope should be valid");
        let upstream =
            AgentTaskId::new(format!("upstream-{step}")).expect("the task id should be valid");

        let mut entity = AgentTaskEntityStore::new(
            scope.clone(),
            fx.tasks.clone(),
            fx.agents.clone(),
            fx.history.clone(),
        );
        entity
            .recover(fx.now())
            .await
            .expect("the task entity should recover");
        let created = entity
            .apply(
                AgentTaskEntityCommand::Create {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::TaskCreation,
                        [TENANT, &id, "1"],
                    )
                    .expect("operation id should be derivable"),
                    creation: Box::new(AgentTaskCreation {
                        definition: padded_definition(80 + step * 5),
                        input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                            .expect("the input is inline-bounded"),
                        assignee: Some(agent_id()),
                        goal: None,
                        parent: None,
                        // The dependency keeps the creation from deciding its
                        // own assignment, which is exactly the window where an
                        // unreserved record used to be admitted and later found
                        // too large to assign.
                        dependencies: vec![AgentTaskDependencyDeclaration::new(upstream.clone())],
                    }),
                },
                &fx.task_router,
                fx.now(),
            )
            .await;

        match created {
            Err(error) => {
                assert_eq!(error.code(), "task-state-too-large");
                refused += 1;
            }
            Ok(_) => {
                admitted += 1;
                let state = rakka_agent::load_agent_task_state(
                    &fx.tasks,
                    &scope,
                    &AgentSchemaPolicy::default(),
                )
                .await
                .expect("the durable read should succeed")
                .expect("the task exists");
                let size = state
                    .task()
                    .expect("the task is created")
                    .materialized_size_bytes();
                assert!(
                    size + AGENT_TASK_STATE_GROWTH_RESERVE_BYTES
                        <= AGENT_TASK_MATERIALIZED_MAX_BYTES,
                    "an admitted record leaves the whole growth reserve, got {size} bytes"
                );

                // The admitted task must survive its own lifecycle: resolving
                // the dependency decides the assignment in the same
                // transition, and that decision can never be refused for the
                // size of a record admission accepted.
                entity
                    .apply(
                        AgentTaskEntityCommand::RecordDependencyOutcome {
                            operation_id: AgentOperationId::new(
                                AgentOperationKind::Command,
                                [TENANT, &id, "resolve"],
                            )
                            .expect("operation id should be derivable"),
                            dependency: upstream,
                            outcome: AgentTaskDependencyOutcome::Completed,
                        },
                        &fx.task_router,
                        fx.now(),
                    )
                    .await
                    .expect("an admitted task is never too large to assign");
                let state = rakka_agent::load_agent_task_state(
                    &fx.tasks,
                    &scope,
                    &AgentSchemaPolicy::default(),
                )
                .await
                .expect("the durable read should succeed")
                .expect("the task exists");
                assert_eq!(
                    state.task().expect("the task is created").status,
                    AgentTaskStatus::InProgress
                );
            }
        }
    }

    assert!(admitted > 0, "the sweep starts below the admission bound");
    assert!(refused > 0, "the sweep crosses the admission bound");
}
