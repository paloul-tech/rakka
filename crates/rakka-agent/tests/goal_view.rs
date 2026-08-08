//! The authorized goal view
//! ([specification 17.18](../../docs/plans/rakka-agent/spec.md)): one goal's
//! tasks, runs, delegation graph, workflow links, evaluations, evidence,
//! budgets, and cancellation state, assembled from durable state alone.
//!
//! The view is slice 4.7's projection debt paid: the delegation, fan-in,
//! workflow-invocation, and goal-evaluation cells of slices 4.2-4.6 surface
//! through [`rakka_agent::AgentRunCollaborationView`] — on the run-scoped
//! operational snapshot and on the goal view's run nodes alike — and the
//! goal-wide assembly traverses the delegation graph by its durable edges.
//! Every test reads through the store-level assembly functions, never a
//! resident entity: the view must answer identically while everything is
//! passivated, which the fixture's rebuild-per-call discipline makes the
//! default rather than an arrangement.

mod common;

use std::sync::Arc;

use common::{
    delegation_config_with_fan_in, delegation_tool_id, fan_in_tool_id, goal_evaluation_request,
    goal_spec_draft, goal_spec_with_fan_out, goal_spec_with_workflow, goal_task_creation_command,
    run_scope, task_definition, task_scope, tenant, wake_policy, workflow_config, Fixture, SKILL,
    SKILL_2, TENANT,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    agent_goal_view_omission_code, agent_operational_snapshot, assemble_agent_goal_view,
    assemble_agent_goal_view_bounded, authorized_agent_goal_view, delegation_result_operation_id,
    evaluation_operation_id, AgentA2aSendExecutor, AgentA2aSendFinding, AgentCancellationProgress,
    AgentDelegationRecord, AgentDelegationReport, AgentDelegationStatus, AgentDispatchFuture,
    AgentEntityAddress, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentFanInPolicy, AgentGoalClaimFuture, AgentGoalClaimRef, AgentGoalClaimSource,
    AgentGoalClaimSourceError, AgentGoalEvaluationExecutor, AgentGoalEvaluationFinding,
    AgentGoalEvaluationOutcome, AgentGoalEvaluationRequest, AgentGoalId, AgentGoalStatus,
    AgentGoalView, AgentLoopPhase, AgentModelTurn, AgentRecordKind, AgentRunEffect,
    AgentRunEffectKind, AgentRunEffectRequest, AgentRunEntityCommand, AgentRunScope,
    AgentRunStatus, AgentSchemaCompatibility, AgentSchemaPolicy, AgentTaskContent, AgentTaskId,
    AgentTaskScope, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest,
    AgentWorkflowInvocationRecord, AgentWorkflowInvocationStatus, AgentWorkflowStartExecutor,
    AgentWorkflowStartFinding, TenantId, AGENT_DELEGATION_RESULT_PAYLOAD_TYPE,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentCorrelationId, AgentEphemeralCredential, AgentTimestampMillis, PrincipalRef,
    StateSchemaVersion,
};
use serde_json::json;

/// The goal the fixture root coordinates: derived from the root task's own
/// value, the resolved open-decision-14 default the view's resolution rests
/// on.
fn goal() -> AgentGoalId {
    AgentGoalId::for_root_task(task_scope().task())
}

fn owner() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "goal-owner".to_string(),
        display_name: None,
    }
}

/// A send executor that names each child after the skill it serves; an
/// optional skill fails definitively instead.
struct SkillSendExecutor {
    fail_skill: Option<&'static str>,
}

impl SkillSendExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self { fail_skill: None })
    }

    fn failing(skill: &'static str) -> Arc<Self> {
        Arc::new(Self {
            fail_skill: Some(skill),
        })
    }
}

fn child_task_for(skill: &str) -> AgentTaskId {
    AgentTaskId::new(format!("child-{skill}")).expect("task id should be valid")
}

impl AgentA2aSendExecutor for SkillSendExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        delegation: &'a AgentDelegationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aSendFinding> {
        let skill = delegation.requested_skill.as_str().to_string();
        let refused = self.fail_skill == Some(skill.as_str());
        Box::pin(async move {
            if refused {
                return Ok(AgentA2aSendFinding::Refused {
                    code: "peer-unavailable".to_string(),
                    message: "the specialist surface refused the send".to_string(),
                });
            }
            Ok(AgentA2aSendFinding::Sent {
                child_task: child_task_for(&skill),
                child_run: None,
                peer_status: "submitted".to_string(),
            })
        })
    }
}

/// A workflow-start executor that reports the derived child durably started.
struct StartedWorkflowExecutor;

impl AgentWorkflowStartExecutor for StartedWorkflowExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _invocation: &'a AgentWorkflowInvocationRecord,
        _credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentWorkflowStartFinding> {
        Box::pin(async move { Ok(AgentWorkflowStartFinding::Started) })
    }
}

/// A scripted application-owned evaluator echoing the request's evidence.
struct ScriptedEvaluator;

impl AgentGoalEvaluationExecutor for ScriptedEvaluator {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        evaluation: &'a AgentGoalEvaluationRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
        _now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentGoalEvaluationFinding> {
        Box::pin(async move {
            Ok(AgentGoalEvaluationFinding::Evaluated {
                outcome: AgentGoalEvaluationOutcome::Satisfied,
                reason_code: "scripted".to_string(),
                evidence: evaluation.evidence.clone(),
                evaluated_by: None,
            })
        })
    }
}

fn delegate_call(id: &str, skill: &str, text: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(id).expect("call id should be valid"),
        delegation_tool_id(),
        json!({ "skill": skill, "input": { "text": text } }),
    )
    .expect("the tool call is bounded")
}

fn await_call() -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("await-1").expect("call id should be valid"),
        fan_in_tool_id(),
        json!({}),
    )
    .expect("the tool call is bounded")
}

/// Two delegations, one workflow invocation, and the await: the full
/// fan-out shape of one coordinating turn.
fn tree_turn(input_text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Fanning out to both specialists and the refund workflow.")
        .with_tool_call(delegate_call("delegate-1", SKILL, input_text))
        .with_tool_call(delegate_call("delegate-2", SKILL_2, input_text))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("invoke-1").expect("call id should be valid"),
                rakka_agent::AgentToolId::new(common::WORKFLOW_TOOL)
                    .expect("tool id should be valid"),
                json!({ "order": "o-1" }),
            )
            .expect("the tool call is bounded"),
        )
        .with_tool_call(await_call())
}

/// Two delegations and the await, no workflow member.
fn fan_out_turn(input_text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Fanning out to both specialists and awaiting them.")
        .with_tool_call(delegate_call("delegate-1", SKILL, input_text))
        .with_tool_call(delegate_call("delegate-2", SKILL_2, input_text))
        .with_tool_call(await_call())
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Synthesizing the children's evidence.")
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

/// The full-tree fixture: both specialists resolvable, the workflow tool
/// wired, sends and starts scripted to succeed.
fn tree_fixture(input_text: &str) -> Fixture {
    Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(tree_turn(input_text))
                .with_turn(proposing_turn("synthesized")),
        )
        .with_a2a_send_executor(SkillSendExecutor::new())
        .with_workflow_start_executor(Arc::new(StartedWorkflowExecutor)),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_workflow_tools(workflow_config())
}

async fn create_root(fixture: &Fixture, spec: rakka_agent::AgentGoalSpec) {
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(spec, true),
        ))
        .await
        .expect("the goal task should create");
}

/// The committed `(delegation, child task)` pairs, in deterministic order.
async fn committed_children(
    fixture: &Fixture,
) -> Vec<(rakka_agent::AgentDelegationId, AgentTaskId)> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    loop_state
        .delegations()
        .iter()
        .filter_map(|(id, cell)| match &cell.status {
            AgentDelegationStatus::ChildCreated { child_task, .. } => {
                Some((id.clone(), child_task.clone()))
            }
            _ => None,
        })
        .collect()
}

/// Creates one real delegated child task, goal-bound and carrying the
/// provenance the traversing edge verifies. `bind_goal`/`with_provenance`
/// weaken the linkage for the fail-closed tests.
async fn create_child(
    fixture: &Fixture,
    delegation: &rakka_agent::AgentDelegationId,
    child_task: &AgentTaskId,
    skill: &str,
    bind_goal: bool,
    with_provenance: bool,
) {
    let scope =
        AgentTaskScope::new(tenant(), child_task.clone()).expect("the child scope is valid");
    let provenance_record = rakka_agent::AgentTaskDelegationProvenance {
        environments: Default::default(),
        knowledge_spaces: Default::default(),
        delegation: delegation.clone(),
        parent_task: task_scope().task().clone(),
        parent_run: run_scope(),
        lineage: Vec::new(),
        ancestors: Vec::new(),
        depth: 1,
        requested_skill: rakka_agent::AgentCapabilityId::new(skill)
            .expect("capability id should be valid"),
        capability_scopes: Default::default(),
        credential_bindings: Vec::new(),
        result_schema: None,
        budget: None,
        deadline: None,
    };
    fixture
        .apply_task_command_at(
            &scope,
            rakka_agent::AgentTaskEntityCommand::Create {
                operation_id: rakka_agent::AgentOperationId::new(
                    rakka_agent::AgentOperationKind::TaskCreation,
                    [TENANT, child_task.as_str(), "1"],
                )
                .expect("the operation id derives"),
                creation: Box::new(rakka_agent::AgentTaskCreation {
                    // Human-owned: the subject is the view's join, not the
                    // child's own loop.
                    definition: task_definition()
                        .with_ownership(rakka_agent::AgentTaskOwnership::Human),
                    input: AgentTaskContent::inline(json!({ "text": "delegated" }))
                        .expect("the input is inline-bounded"),
                    assignee: None,
                    goal: bind_goal.then(goal),
                    goal_mode: Default::default(),
                    goal_spec: None,
                    parent: Some(task_scope().task().clone()),
                    dependencies: Vec::new(),
                    escrow: None,
                    wake: None,
                    delegation: with_provenance.then(|| Box::new(provenance_record)),
                    telemetry: Default::default(),
                }),
            },
        )
        .await
        .expect("the child task creates");
}

/// One child's terminal report, as the child's owed exchange carries it.
fn child_result_envelope(
    fixture: &Fixture,
    delegation: &rakka_agent::AgentDelegationId,
    child_task: &AgentTaskId,
    status: AgentTaskStatus,
) -> AgentExchangeEnvelope {
    let tenant = TenantId::new(TENANT);
    let operation_id =
        delegation_result_operation_id(&tenant, delegation).expect("the operation id derives");
    let report = AgentDelegationReport {
        delegation: delegation.clone(),
        child_task: child_task.clone(),
        child_run: None,
        status,
        terminal_reason: (status != AgentTaskStatus::Completed)
            .then(|| "cancellation-requested".to_string()),
        result_digest: (status == AgentTaskStatus::Completed).then(|| {
            AgentTaskContent::inline(json!({ "answer": "done" }))
                .expect("the content is inline-bounded")
                .digest()
        }),
        descendants_created: 0,
    };
    let payload = AgentExchangePayload::encode(AGENT_DELEGATION_RESULT_PAYLOAD_TYPE, &report)
        .expect("the report encodes");
    let child_scope =
        AgentTaskScope::new(tenant, child_task.clone()).expect("the child scope is valid");
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::DelegationResult,
        AgentEntityAddress::Task(child_scope),
        AgentEntityAddress::Run(run_scope()),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid")
}

async fn deliver(fixture: &Fixture, envelope: &AgentExchangeEnvelope) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    assert!(reply.result().is_accepted());
}

/// Assembles the view from the durable stores alone, at a fixed observation
/// time so equality comparisons are exact.
async fn assemble(fixture: &Fixture) -> Option<AgentGoalView> {
    assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &AgentSchemaPolicy::default(),
        None,
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the assembly succeeds")
}

/// Drives the full tree to its parked fan-out: two created children (real
/// records), one started workflow invocation, a closed three-member group.
async fn drive_tree(fixture: &Fixture) -> Vec<(rakka_agent::AgentDelegationId, AgentTaskId)> {
    create_root(fixture, {
        let mut spec = goal_spec_with_workflow(Some(AgentFanInPolicy::All));
        spec.evaluator = None;
        spec
    })
    .await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(fixture).await;
    assert_eq!(children.len(), 2);
    for (index, (delegation, child_task)) in children.iter().enumerate() {
        let skill = if index == 0 { SKILL } else { SKILL_2 };
        create_child(fixture, delegation, child_task, skill, true, true).await;
    }
    children
}

/// The full delegated tree assembles: root and both specialist tasks, the
/// coordinating run, both delegation edges, the workflow link, the closed
/// fan-in group, the goal contract, and the budget rollup — from durable
/// state alone, with per-node revisions and no omissions.
#[tokio::test]
async fn the_goal_view_assembles_a_delegated_tree_with_workflow_links() {
    let fixture = tree_fixture("hello");
    drive_tree(&fixture).await;

    let view = assemble(&fixture).await.expect("the goal resolves");

    assert_eq!(view.goal, goal());
    assert_eq!(view.root_task, task_scope().task().clone());
    assert!(!view.truncated);
    assert!(view.omissions.is_empty(), "omissions: {:?}", view.omissions);
    // Root + two children, and each task+run read is counted.
    assert_eq!(view.tasks.len(), 3);
    assert_eq!(view.runs.len(), 1, "the children hold no assignment yet");
    assert_eq!(view.records_read, 4);

    let root = &view.tasks[0];
    assert!(root.is_root);
    assert_eq!(root.depth, 0);
    assert_eq!(root.status, AgentTaskStatus::InProgress);
    assert_eq!(root.cancellation, AgentCancellationProgress::NotRequested);
    let assignment = root.assignment.as_ref().expect("the root is assigned");
    assert_eq!(assignment.run, run_scope().run().clone());
    for child in &view.tasks[1..] {
        assert!(!child.is_root);
        assert!(!child.is_epoch);
        assert_eq!(child.depth, 1);
        assert_eq!(child.parent.as_ref(), Some(task_scope().task()));
        assert!(child.created_by_delegation.is_some());
    }

    let run = &view.runs[0];
    assert_eq!(run.status, AgentRunStatus::Running);
    assert_eq!(run.phase, AgentLoopPhase::AwaitingChildren);
    assert_eq!(run.outstanding_effects, 0);
    let collaboration = &run.collaboration;
    assert_eq!(collaboration.delegations.len(), 2);
    for edge in &collaboration.delegations {
        assert!(matches!(
            edge.status,
            AgentDelegationStatus::ChildCreated { .. }
        ));
        assert_eq!(edge.depth, 1);
        assert_eq!(edge.parent_task, task_scope().task().clone());
    }
    assert_eq!(collaboration.workflow_invocations.len(), 1);
    let invocation = &collaboration.workflow_invocations[0];
    assert_eq!(
        invocation.status,
        AgentWorkflowInvocationStatus::Started { adopted: false }
    );
    assert_eq!(invocation.workflow_type, common::WORKFLOW_TYPE);
    assert_eq!(
        invocation.child_run.as_str(),
        invocation.invocation.as_str()
    );
    let fan_in = collaboration.fan_in.as_ref().expect("the group exists");
    assert!(fan_in.closed);
    assert_eq!(fan_in.members.len(), 3);
    assert!(fan_in.resolution.is_none());
    // Both created children have not reported; the workflow child neither.
    assert_eq!(fan_in.unreported.len(), 3);

    assert_eq!(view.contract.status, AgentGoalStatus::Active);
    assert!(view.contract.terminal.is_none());
    assert_eq!(view.cancellation, AgentCancellationProgress::NotRequested);
    assert_eq!(view.budget.root_outstanding_children, 1);
    assert!(view.claims.is_empty());
    assert!(!view.claims_available, "no claim source is wired");
}

/// Assembling twice from the same durable records answers byte-identically:
/// the view is pure over the stores, needs no resident entity, and a
/// restart — the fixture rebuilds every entity per call — changes nothing.
#[tokio::test]
async fn the_view_answers_identically_after_restart_without_activation() {
    let fixture = tree_fixture("hello");
    drive_tree(&fixture).await;

    let first = assemble(&fixture).await.expect("the goal resolves");
    let second = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(first, second);
    assert_eq!(
        serde_json::to_string(&first).expect("the view serializes"),
        serde_json::to_string(&second).expect("the view serializes"),
    );
}

/// The run-scoped operational snapshot surfaces the same collaboration
/// cells — the slice 4.3-4.6 snapshot debt, paid by one shared derivation.
#[tokio::test]
async fn the_operational_snapshot_surfaces_collaboration_cells() {
    let fixture = tree_fixture("hello");
    drive_tree(&fixture).await;

    let snapshot = agent_operational_snapshot(
        &fixture.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the query succeeds")
    .expect("the run exists");

    assert_eq!(snapshot.collaboration.delegations.len(), 2);
    assert_eq!(snapshot.collaboration.workflow_invocations.len(), 1);
    assert!(snapshot.collaboration.fan_in.is_some());
    assert!(snapshot.collaboration.evaluation.is_none());

    // The view's run node carries the identical derivation.
    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(view.runs[0].collaboration, snapshot.collaboration);
}

/// Planted content never rides the view: the objective summary, the task
/// input, the delegated input, and the proposal/result content are all
/// redacted or referenced by digest only.
#[tokio::test]
async fn content_never_leaks_into_the_view() {
    const SENTINELS: [&str; 3] = [
        "SENTINEL-OBJECTIVE-SUMMARY",
        "SENTINEL-DELEGATION-INPUT",
        "SENTINEL-PROPOSAL-ANSWER",
    ];
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(fan_out_turn("SENTINEL-DELEGATION-INPUT"))
                .with_turn(proposing_turn("SENTINEL-PROPOSAL-ANSWER")),
        )
        .with_a2a_send_executor(SkillSendExecutor::new()),
    )
    .with_delegation(delegation_config_with_fan_in());

    let mut spec = goal_spec_with_fan_out(Some(AgentFanInPolicy::All), None);
    spec.objective.summary = "SENTINEL-OBJECTIVE-SUMMARY".to_string();
    create_root(&fixture, spec).await;
    fixture.pump().await.expect("the loop should converge");

    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);
    for (index, (delegation, child_task)) in children.iter().enumerate() {
        let skill = if index == 0 { SKILL } else { SKILL_2 };
        create_child(&fixture, delegation, child_task, skill, true, true).await;
        let envelope =
            child_result_envelope(&fixture, delegation, child_task, AgentTaskStatus::Completed);
        deliver(&fixture, &envelope).await;
    }
    fixture.pump().await.expect("the loop should converge");

    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(view.contract.status, AgentGoalStatus::Active);
    assert!(view.tasks[0].has_accepted_result, "the root completed");

    let serialized = serde_json::to_string(&view).expect("the view serializes");
    for sentinel in SENTINELS {
        assert!(
            !serialized.contains(sentinel),
            "the view leaked {sentinel}: {serialized}"
        );
    }
}

/// A non-owner principal reads exactly what a missing goal answers — and so
/// do an absent goal id, a child task id presented as a goal id, and any
/// other task that coordinates no goal record: `Ok(None)`, never an
/// existence oracle.
#[tokio::test]
async fn a_non_owner_reads_exactly_what_a_missing_goal_answers() {
    let fixture = tree_fixture("hello");
    let children = drive_tree(&fixture).await;
    let policy = AgentSchemaPolicy::default();
    let observed = AgentTimestampMillis::new(999_000);

    // The owner reads the view.
    let owned = authorized_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &owner(),
        &policy,
        None,
        observed,
    )
    .await
    .expect("the assembly succeeds");
    assert!(owned.is_some());

    // A non-owner reads what a missing goal answers.
    let intruder = PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "someone-else".to_string(),
        display_name: None,
    };
    let denied = authorized_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &intruder,
        &policy,
        None,
        observed,
    )
    .await
    .expect("the assembly succeeds");

    let absent_goal = AgentGoalId::new("no-such-goal").expect("the goal id is valid");
    let absent = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &absent_goal,
        &policy,
        None,
        observed,
    )
    .await
    .expect("the assembly succeeds");

    // A child task exists under this id, but it coordinates no goal record:
    // presenting it as a goal id answers exactly like the absent goal.
    let child_as_goal = AgentGoalId::new(children[0].1.as_str()).expect("the goal id is valid");
    let probed = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &child_as_goal,
        &policy,
        None,
        observed,
    )
    .await
    .expect("the assembly succeeds");

    assert_eq!(denied, None);
    assert_eq!(absent, None);
    assert_eq!(probed, None);

    // The fence precedes the schema gate: a root record the caller's policy
    // cannot read answers a non-owner with the same `Ok(None)` an absent
    // goal does — a schema error would be a distinguishable answer, and so
    // an existence oracle — while the owner still sees the honest failure.
    let rejecting_tasks = AgentSchemaPolicy::n_plus_one().with_compatibility(
        AgentRecordKind::TaskState,
        AgentSchemaCompatibility::new(StateSchemaVersion::new(99), StateSchemaVersion::new(99)),
    );
    let denied_unreadable = authorized_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &intruder,
        &rejecting_tasks,
        None,
        observed,
    )
    .await
    .expect("the denial answers rather than erring");
    assert_eq!(denied_unreadable, None);

    let owner_unreadable = authorized_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &owner(),
        &rejecting_tasks,
        None,
        observed,
    )
    .await;
    assert!(
        owner_unreadable.is_err(),
        "the owner sees the schema failure, not a vanished goal"
    );
}

/// A tighter node budget truncates with the explicit marker: the root
/// assembles, every unvisited child becomes a `node-budget-exhausted`
/// omission, and `truncated` says so.
#[tokio::test]
async fn the_node_budget_truncates_with_an_explicit_marker() {
    let fixture = tree_fixture("hello");
    let children = drive_tree(&fixture).await;

    let view = assemble_agent_goal_view_bounded(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &AgentSchemaPolicy::default(),
        None,
        1,
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the assembly succeeds")
    .expect("the goal resolves");

    assert!(view.truncated);
    assert_eq!(view.tasks.len(), 1, "only the root fits the budget");
    assert!(view.tasks[0].is_root);
    assert_eq!(view.omissions.len(), children.len());
    for omission in &view.omissions {
        assert_eq!(
            omission.code,
            agent_goal_view_omission_code::NODE_BUDGET_EXHAUSTED
        );
    }
}

/// Children that cannot be joined honestly are omissions beside their
/// edges, never failures: a missing child record, and a child whose goal
/// binding does not name this goal, both leave the edge visible and the
/// view whole.
#[tokio::test]
async fn partial_children_surface_as_edges_and_omissions_never_failures() {
    let fixture = tree_fixture("hello");
    create_root(&fixture, {
        let mut spec = goal_spec_with_workflow(Some(AgentFanInPolicy::All));
        spec.evaluator = None;
        spec
    })
    .await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);

    // The first child exists but is bound to no goal; the second was never
    // created at all.
    create_child(&fixture, &children[0].0, &children[0].1, SKILL, false, true).await;

    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(view.tasks.len(), 1, "only the root joins");
    assert_eq!(view.runs.len(), 1);
    assert_eq!(view.runs[0].collaboration.delegations.len(), 2);
    let codes: Vec<(AgentTaskId, String)> = view
        .omissions
        .iter()
        .map(|omission| (omission.task.clone(), omission.code.clone()))
        .collect();
    assert!(codes.contains(&(
        children[0].1.clone(),
        agent_goal_view_omission_code::FOREIGN_GOAL.to_string()
    )));
    assert!(codes.contains(&(
        children[1].1.clone(),
        agent_goal_view_omission_code::RECORD_MISSING.to_string()
    )));

    // A child carrying no provenance for the traversing edge fails closed
    // the same way: an unlinked-provenance omission, never a joined forgery.
    create_child(
        &fixture,
        &children[1].0,
        &children[1].1,
        SKILL_2,
        true,
        false,
    )
    .await;
    let view = assemble(&fixture).await.expect("the goal resolves");
    assert!(view.omissions.iter().any(|omission| {
        omission.task == children[1].1
            && omission.code == agent_goal_view_omission_code::UNLINKED_PROVENANCE
    }));
}

/// A definitively failed send is an edge without a node and without an
/// omission: the cell records what happened, and there is no child to join.
#[tokio::test]
async fn a_failed_send_is_an_edge_without_a_node_or_omission() {
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(fan_out_turn("hello"))
                .with_turn(proposing_turn("synthesized")),
        )
        .with_a2a_send_executor(SkillSendExecutor::failing(SKILL_2)),
    )
    .with_delegation(delegation_config_with_fan_in());
    create_root(
        &fixture,
        goal_spec_with_fan_out(Some(AgentFanInPolicy::All), None),
    )
    .await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 1, "one send failed definitively");
    create_child(&fixture, &children[0].0, &children[0].1, SKILL, true, true).await;

    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(view.tasks.len(), 2, "the root and the one created child");
    let edges = &view.runs[0].collaboration.delegations;
    assert_eq!(edges.len(), 2);
    assert!(edges
        .iter()
        .any(|edge| matches!(&edge.status, AgentDelegationStatus::Failed { code } if code == "peer-unavailable")));
    assert!(
        view.omissions.is_empty(),
        "a failed send left no child to omit"
    );
}

/// An unreadable root record fails the whole call closed — the root is the
/// authoritative anchor — while an unreadable run record marks its task
/// node's `run_omission`, leaving the durable half of the view intact.
#[tokio::test]
async fn schema_failures_fail_closed_at_the_root_and_omit_at_a_run() {
    let fixture = tree_fixture("hello");
    drive_tree(&fixture).await;

    // Task records unreadable: the root itself fails the call.
    let rejecting_tasks = AgentSchemaPolicy::n_plus_one().with_compatibility(
        AgentRecordKind::TaskState,
        AgentSchemaCompatibility::new(StateSchemaVersion::new(99), StateSchemaVersion::new(99)),
    );
    let result = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &rejecting_tasks,
        None,
        AgentTimestampMillis::new(999_000),
    )
    .await;
    assert!(result.is_err(), "the root record failing fails closed");

    // Run records unreadable: the run marks its assembled task node — under
    // the run-specific code, never the task-record one — and the task list
    // stays whole, with no omission naming a task the view did assemble.
    let rejecting_runs = AgentSchemaPolicy::n_plus_one().with_compatibility(
        AgentRecordKind::RunState,
        AgentSchemaCompatibility::new(StateSchemaVersion::new(99), StateSchemaVersion::new(99)),
    );
    let view = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &rejecting_runs,
        None,
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the assembly succeeds")
    .expect("the goal resolves");
    assert_eq!(view.tasks.len(), 1, "no run means no discovered children");
    assert!(view.runs.is_empty());
    assert_eq!(
        view.tasks[0].run_omission.as_deref(),
        Some(agent_goal_view_omission_code::RUN_SCHEMA_UNSUPPORTED)
    );
    assert!(
        view.omissions.is_empty(),
        "an assembled task never appears in the omissions: {:?}",
        view.omissions
    );
}

/// A standing assignment refusal suppresses run re-derivation: deciding a
/// generation clears `last_refusal`, so one on record proves the highest
/// generation never produced an accepted run — the node joins with no run,
/// no `run_omission`, and no anomaly read into a healthy refusal.
#[tokio::test]
async fn a_refused_generation_re_derives_no_run_and_raises_no_anomaly() {
    use rakka_persistence::DurableStateStore;

    let fixture = tree_fixture("hello");
    drive_tree(&fixture).await;

    let before = assemble(&fixture).await.expect("the goal resolves");
    let run_node = &before.runs[0];
    assert_eq!(run_node.task, *task_scope().task());
    let run_id = run_node.scope.persistence_id();
    let run_revision = run_node.revision;

    // Surgery: rewrite the root as a task whose highest generation was
    // refused — `last_refusal` standing, assignment cleared, generation
    // consumed — and drop the run record a refusal never produces.
    let scope = task_scope();
    let record = fixture
        .tasks
        .load(&scope.persistence_id())
        .await
        .expect("the record loads")
        .expect("the root exists");
    let refusal = rakka_agent::AgentAssignmentRefusal {
        agent: common::agent_id(),
        reason: rakka_agent::AgentAssignmentRefusalReason::RunRefusedAssignment,
        detail: "run-generation-conflict".to_string(),
        refused_at: AgentTimestampMillis::new(998_000),
    };
    let mut value = serde_json::to_value(&record.state).expect("the state serializes");
    value["task"]["assignment"] = serde_json::Value::Null;
    value["task"]["last_refusal"] = serde_json::to_value(&refusal).expect("the refusal serializes");
    let mutated: rakka_agent::AgentTaskState =
        serde_json::from_value(value).expect("the state deserializes");
    fixture
        .tasks
        .compare_and_set(&scope.persistence_id(), record.revision, mutated)
        .await
        .expect("the surgery commits");
    fixture
        .runs
        .delete(&run_id, run_revision)
        .await
        .expect("the run record deletes");

    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(
        view.tasks.len(),
        1,
        "without a run to read edges from, the subtree is unknown, not omitted"
    );
    assert_eq!(
        view.tasks[0].run_omission, None,
        "a refusal is not an anomaly"
    );
    assert!(
        view.runs.is_empty(),
        "no run joins for a refused generation"
    );
    assert!(view.omissions.is_empty(), "omissions: {:?}", view.omissions);
}

/// A requested cancellation is visible end to end: the root task defers
/// nonterminally, the view's goal-level rollup reads `Propagating` while
/// the assignment stands, and nothing claims the children stopped.
#[tokio::test]
async fn a_requested_cancellation_reads_propagating_at_the_root() {
    let fixture = tree_fixture("hello");
    drive_tree(&fixture).await;

    fixture
        .apply_task_command(rakka_agent::AgentTaskEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, common::TASK, "1"],
            )
            .expect("the operation id derives"),
            reason: "operator-requested".to_string(),
        })
        .await
        .expect("the cancellation request applies");

    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(view.cancellation, AgentCancellationProgress::Propagating);
    assert_eq!(
        view.tasks[0].cancellation,
        AgentCancellationProgress::Propagating
    );
    assert!(
        view.tasks[0].status != AgentTaskStatus::Cancelled,
        "the request defers; nothing claims the children stopped"
    );
    assert_eq!(view.contract.status, AgentGoalStatus::Cancelled);
}

/// A continuous root's admitted epochs join the view as `is_epoch` nodes:
/// reached through the wake controller's durable status view, never through
/// a delegation edge.
#[tokio::test]
async fn a_continuous_roots_epochs_join_the_view() {
    use rakka_agent::{
        AgentWakeBinding, AgentWakeOccurrence, AgentWakeTimerEntry, AgentWakeTimerStore,
        AgentWakeTriggerKind, ScheduleRevision,
    };

    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(proposing_turn("epoch-report")),
    ));
    fixture.instantiate_agent().await;
    // The continuous root institutes its goal record with the goal identity
    // derived from its own value, so the view's resolution convention holds.
    fixture
        .apply_task_command(rakka_agent::AgentTaskEntityCommand::Create {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::TaskCreation,
                [TENANT, common::TASK, "1"],
            )
            .expect("operation id should be derivable"),
            creation: Box::new(rakka_agent::AgentTaskCreation {
                definition: task_definition()
                    .with_ownership(rakka_agent::AgentTaskOwnership::Human),
                input: AgentTaskContent::inline(json!({ "goal": 1 }))
                    .expect("the input is inline-bounded"),
                assignee: None,
                goal: None,
                goal_mode: common::continuous_goal_mode(wake_policy()),
                goal_spec: Some(Box::new(goal_spec_draft(common::goal_spec(), true))),
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: None,
                telemetry: Default::default(),
            }),
        })
        .await
        .expect("the continuous root creates");

    // One durable occurrence bound to the derived goal, delivered by the
    // scanner, admitted as an epoch.
    let binding = AgentWakeBinding::new(
        tenant(),
        goal(),
        ScheduleRevision::INITIAL,
        AgentWakeOccurrence::Scheduled {
            due_at: AgentTimestampMillis::new(5),
        },
        AgentWakeTriggerKind::DurableTimer,
        AgentTimestampMillis::new(5),
        rakka_agent::AgentRevisionNumber::INITIAL,
    )
    .expect("the wake binding is valid");
    let entry = AgentWakeTimerEntry::new(
        binding.clone(),
        task_scope().task().clone(),
        AgentTimestampMillis::new(5),
    );
    AgentWakeTimerStore::new(fixture.wakes.clone())
        .schedule_occurrence(entry)
        .await
        .expect("the occurrence parks");
    // Advance the shared clock past the occurrence's due time before the
    // scanner's pass reads it.
    while fixture.now().as_millis() < 6 {}
    let scan = fixture
        .wake_scanner()
        .scan_due()
        .await
        .expect("the scan runs");
    assert_eq!(scan.outcomes.len(), 1);

    // Settle the owed creation and assignment exchanges without finishing
    // the epoch: the wake controller's status view carries an epoch ref only
    // while its occurrence is active.
    let (epoch_scope, epoch_run) = common::epoch_scopes_for(binding.wake_id());
    for _ in 0..4 {
        fixture
            .settle_task_at(&task_scope())
            .await
            .expect("the root settles");
        fixture
            .settle_task_at(&epoch_scope)
            .await
            .expect("the epoch settles");
    }

    let view = assemble(&fixture).await.expect("the goal resolves");
    let epoch_nodes: Vec<_> = view.tasks.iter().filter(|node| node.is_epoch).collect();
    assert_eq!(epoch_nodes.len(), 1, "tasks: {:?}", view.tasks);
    let epoch = epoch_nodes[0];
    assert_eq!(epoch.scope, epoch_scope);
    assert!(!epoch.is_root);
    assert_eq!(epoch.depth, 0, "epochs carry no delegation depth");
    assert_eq!(epoch.parent.as_ref(), Some(task_scope().task()));
    assert!(epoch.created_by_delegation.is_none());

    // Driven to completion, the occurrence releases and the epoch ref leaves
    // the controller's status view — a completed epoch is history, reachable
    // through the task projection and the history store, not this walk.
    fixture
        .pump_epoch(&epoch_scope, &epoch_run)
        .await
        .expect("the epoch converges");
    let view = assemble(&fixture).await.expect("the goal resolves");
    assert!(
        view.tasks.iter().all(|node| !node.is_epoch),
        "a released occurrence no longer joins its epoch"
    );
}

/// The evaluation cell and the terminal decision surface whole: the run
/// node carries the evaluation view — outcome, stable reason code, classed
/// evidence — and the contract carries the terminal decision with the
/// attested evaluation reference it rests on.
#[tokio::test]
async fn an_evaluation_and_its_evidence_surface_in_the_view() {
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new().with_turn_for(
                1,
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                    .with_text("investigating the goal"),
            ),
        )
        .with_goal_evaluation_executor(Arc::new(ScriptedEvaluator)),
    );
    create_root(&fixture, common::goal_spec_with_evaluator()).await;

    // Commit the evaluation effect, answer it as the dispatcher would, and
    // let the applying transition's settle pass courier the exchange to the
    // decision door.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::EvaluateGoal {
            operation_id: evaluation_operation_id(&run_scope(), "evaluate-1")
                .expect("the operation id derives"),
            evaluation: Box::new(goal_evaluation_request(
                rakka_agent::AgentRevisionNumber::INITIAL,
            )),
        },
        &fixture.router,
        fixture.now(),
    )
    .await
    .expect("the evaluation commits");

    let effect = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("the run recovers");
        run.state()
            .expect("the state reads")
            .loop_state()
            .expect("the loop state exists")
            .effects()
            .iter()
            .find(|effect| {
                effect.kind() == AgentRunEffectKind::GoalEvaluationCall && effect.is_outstanding()
            })
            .cloned()
            .expect("an evaluation effect is outstanding")
    };
    let AgentRunEffectRequest::Evaluation { evaluation } = &effect.request else {
        panic!("the effect carries an evaluation request");
    };
    let grant = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("the run recovers");
        run.state()
            .expect("the state reads")
            .loop_state()
            .expect("the loop state exists")
            .grant_for(&effect)
            .cloned()
    };
    let outcome = fixture
        .dispatcher
        .evaluation_outcome(
            &run_scope(),
            &effect,
            evaluation,
            grant.as_ref(),
            fixture.now(),
        )
        .await;
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: 1,
            fence: 0,
            outcome: Box::new(outcome),
        },
        &fixture.router,
        fixture.now(),
    )
    .await
    .expect("the evaluation result applies");

    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(view.contract.status, AgentGoalStatus::Satisfied);
    let terminal = view.contract.terminal.as_ref().expect("the goal decided");
    let evaluation_ref = terminal
        .evaluation
        .as_ref()
        .expect("a criteria decision carries its evaluation");
    assert!(evaluation_ref.digest.is_some(), "the reference is attested");
    assert_eq!(
        evaluation_ref
            .evidence_items
            .iter()
            .map(|item| item.class.as_str())
            .collect::<Vec<_>>(),
        vec!["artifact"],
    );

    let evaluation_view = view.runs[0]
        .collaboration
        .evaluation
        .as_ref()
        .expect("the run holds its evaluation cell");
    assert_eq!(
        evaluation_view.outcome,
        AgentGoalEvaluationOutcome::Satisfied
    );
    assert_eq!(evaluation_view.reason_code, "scripted");
    assert!(evaluation_view.reported, "the exchange settled");
    assert!(evaluation_view.refusal.is_none());
    assert_eq!(evaluation_view.evidence.len(), 1);
}

/// A wired claim source joins shared-knowledge references; a failing one
/// degrades the projection half honestly — `claims_available` goes `false`
/// with the failure's stable code on the view, a capped list marks itself
/// `claims_truncated`, and the durable half of the view is untouched.
#[tokio::test]
async fn claims_join_when_a_source_answers_and_degrade_when_it_fails() {
    struct StubSource {
        fail: bool,
        refs: Vec<AgentGoalClaimRef>,
    }

    impl AgentGoalClaimSource for StubSource {
        fn backend_name(&self) -> &'static str {
            "stub"
        }

        fn claims_for_goal<'a>(
            &'a self,
            _tenant: &'a rakka_agent::TenantId,
            _goal: &'a AgentGoalId,
            limit: usize,
        ) -> AgentGoalClaimFuture<'a> {
            Box::pin(async move {
                if self.fail {
                    return Err(AgentGoalClaimSourceError::new(
                        "store-unavailable",
                        "the graph store is down",
                    ));
                }
                let mut refs = self.refs.clone();
                refs.truncate(limit);
                Ok(refs)
            })
        }
    }

    let fixture = tree_fixture("hello");
    drive_tree(&fixture).await;

    let reference = AgentGoalClaimRef::new(
        rakka_agent::AgentCommunalClaimId::new("claim-abc").expect("the claim id is valid"),
        rakka_agent::KnowledgeSpaceId::new("mission-findings").expect("the space id is valid"),
        common::agent_id(),
    )
    .with_task(task_scope().task().clone());
    let answering = StubSource {
        fail: false,
        refs: vec![reference.clone()],
    };
    let view = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &AgentSchemaPolicy::default(),
        Some(&answering),
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the assembly succeeds")
    .expect("the goal resolves");
    assert!(view.claims_available);
    assert_eq!(view.claims, vec![reference.clone()]);
    assert!(
        !view.claims_truncated,
        "the source held nothing beyond the cap"
    );
    assert_eq!(view.claims_error_code, None);

    // A source holding more than the cap: the view carries exactly the cap
    // and says the list was cut — a capped list must never read as complete.
    let overflowing = StubSource {
        fail: false,
        refs: vec![reference; rakka_agent::AGENT_GOAL_VIEW_MAX_CLAIMS + 1],
    };
    let truncated = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &AgentSchemaPolicy::default(),
        Some(&overflowing),
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the assembly succeeds")
    .expect("the goal resolves");
    assert!(truncated.claims_available);
    assert!(truncated.claims_truncated);
    assert_eq!(
        truncated.claims.len(),
        rakka_agent::AGENT_GOAL_VIEW_MAX_CLAIMS
    );

    let failing = StubSource {
        fail: true,
        refs: Vec::new(),
    };
    let degraded = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &AgentSchemaPolicy::default(),
        Some(&failing),
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the assembly succeeds")
    .expect("the goal resolves");
    assert!(!degraded.claims_available);
    assert!(degraded.claims.is_empty());
    assert_eq!(
        degraded.claims_error_code.as_deref(),
        Some("store-unavailable"),
        "the failure's stable code rides the view"
    );
    assert_eq!(
        degraded.tasks.len(),
        view.tasks.len(),
        "the durable half is intact"
    );

    // No source wired at all: degraded the same way, but with no error code —
    // an unwired join is distinguishable from a failing one.
    let unwired = assemble_agent_goal_view(
        &fixture.tasks,
        &fixture.runs,
        &tenant(),
        &goal(),
        &AgentSchemaPolicy::default(),
        None,
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the assembly succeeds")
    .expect("the goal resolves");
    assert!(!unwired.claims_available);
    assert_eq!(unwired.claims_error_code, None);
}

// A note for the reader: schema-unsupported *task* children cannot be
// distinguished from an unreadable root under one policy — every task record
// shares a schema window — so the gate's coverage rides the run-record
// variant above (under its own `run-schema-unsupported` code), and the
// cross-version half belongs to the compatibility matrix.

/// The slice 5.1 lockstep: a mid-transfer handoff joins the run node's
/// collaboration view and the task node's handoff provenance in the same
/// assembly — and the pending transfer resolves the *source* run, whose pair
/// the task record itself no longer names.
#[tokio::test]
async fn a_pending_handoff_joins_the_view_and_resolves_the_source_run() {
    use rakka_agent::{AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor, AgentHandoffRecord};

    /// Claims the transfer was recorded without touching the task, holding
    /// the cell at `Sent` — the mid-transfer window the view must survive.
    struct ClaimingHandoffExecutor;
    impl AgentA2aHandoffSendExecutor for ClaimingHandoffExecutor {
        fn execute<'a>(
            &'a self,
            _scope: &'a AgentRunScope,
            _intent: &'a AgentRunEffect,
            _handoff: &'a AgentHandoffRecord,
            _credential: Option<&'a AgentEphemeralCredential>,
        ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
            Box::pin(async move {
                Ok(AgentA2aHandoffFinding::Recorded {
                    target_generation: None,
                    peer_status: "working".to_string(),
                })
            })
        }
    }

    let fixture = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn(
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("Transferring to billing.")
                .with_tool_call(
                    AgentToolCallRequest::new(
                        AgentToolCallId::new("call-1").expect("call id"),
                        common::handoff_tool_id(),
                        json!({ "skill": common::HANDOFF_SKILL, "reason": "needs billing" }),
                    )
                    .expect("the tool call is bounded"),
                ),
        ),
    ))
    .with_delegation(common::handoff_config());
    let _ = fixture
        .dispatcher
        .clone()
        .with_a2a_handoff_executor(Arc::new(ClaimingHandoffExecutor));
    fixture.instantiate_agent().await;
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(common::goal_spec_with_handoff(), true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the loop should converge");

    let view = assemble(&fixture).await.expect("the goal resolves");
    assert_eq!(view.tasks.len(), 1);
    assert_eq!(view.runs.len(), 1, "the source run joins the view");

    // The run node carries the handoff cell — the same shape the run-scoped
    // operational snapshot carries, so the two surfaces cannot disagree.
    let run_node = &view.runs[0];
    assert_eq!(
        run_node.scope,
        run_scope(),
        "the pending transfer resolves the source"
    );
    let cell = run_node
        .collaboration
        .handoff
        .as_ref()
        .expect("the handoff joins the collaboration view");
    assert_eq!(cell.status, "sent");
    assert_eq!(cell.target.as_str(), common::HANDOFF_TARGET);
    assert_eq!(cell.context_refs, 0);
    let snapshot = agent_operational_snapshot(
        &fixture.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(999_000),
    )
    .await
    .expect("the snapshot reads")
    .expect("the run exists");
    assert_eq!(
        view.runs[0].collaboration, snapshot.collaboration,
        "the goal view and the operational snapshot derive one collaboration shape"
    );
    assert!(snapshot.collaboration.handoff.is_some());
}
