//! Actor-backed agent run runtime tests.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentCompiledNodeKind,
    AgentCompiledNodeTarget, AgentCompiledPlanEdge, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentCompiledPlanNode, AgentCompiledPlanPort, AgentCompiledPortDirection,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffect, AgentEffectId,
    AgentEffectKind, AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget,
    AgentGraphEffectScheduleRequest, AgentGraphNodeStatus, AgentGraphRunProjection,
    AgentGraphRuntimeEffectOutcome, AgentGraphRuntimeTransition, AgentGraphWaitReason,
    AgentIdempotencyKey, AgentInboxDuplicateReason, AgentPayloadDescriptor, AgentRunActor,
    AgentRunActorCommand, AgentRunActorSnapshot, AgentRunId, AgentRunState, AgentRunStatus,
    AgentRunTransition, AgentRunTransitionKind, AgentStatePayload, AgentStep, AgentStepId,
    AgentStepKind, AgentStepSuccess, AgentTenantId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId, InlineState, StateSchemaVersion, WorkflowDefinitionVersion,
    CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};
use rakka_core::{ActorRef, ActorSystem, InMemoryMetricsRecorder, Message};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type TestActor = AgentRunActor<RunStore, WorkflowStore, ManualWorkflowClock>;
type TestActorRef = ActorRef<AgentRunActorCommand>;

const ASK_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct RuntimeFixture {
    workflow: AgentWorkflow,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    clock: ManualWorkflowClock,
    metrics: Arc<InMemoryMetricsRecorder>,
}

impl RuntimeFixture {
    fn new(workflow: AgentWorkflow) -> Self {
        Self {
            workflow,
            run_store: RunStore::new(),
            workflow_store: WorkflowStore::new(),
            clock: ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
            metrics: Arc::new(InMemoryMetricsRecorder::new()),
        }
    }
}

#[tokio::test]
async fn actor_restart_recovers_run_and_inbox_without_duplicate_command() {
    let system = ActorSystem::new("agent-runtime-restart");
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-runtime-restart");

    let first = spawn_actor(&system, "runtime-a", &fixture, run_id.clone());

    let accepted = first
        .ask(
            |reply_to| AgentRunActorCommand::AcceptCommand {
                command: start_command(&fixture.workflow, &run_id),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("accept ask should reply")
        .expect("start command should be accepted");
    assert!(accepted.is_accepted());

    let started = start(
        &first,
        accepted_run_state(&fixture.workflow, &run_id, AgentStepId::new("plan")),
    )
    .await;
    assert_eq!(started.kind, AgentRunTransitionKind::Start);
    assert_eq!(started.next_status, AgentRunStatus::Accepted);

    first.stop().expect("first actor should stop");
    wait_until_terminated(&first).await;

    let restarted = spawn_actor(&system, "runtime-b", &fixture, run_id.clone());

    let recovered = snapshot(&restarted).await;
    assert_eq!(
        recovered
            .run_state
            .as_ref()
            .expect("run state should recover")
            .status,
        AgentRunStatus::Accepted
    );
    assert_eq!(recovered.recoverable_command_count, 1);

    let duplicate = restarted
        .ask(
            |reply_to| AgentRunActorCommand::AcceptCommand {
                command: start_command(&fixture.workflow, &run_id),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("duplicate accept ask should reply")
        .expect("duplicate start command should return durable result");
    assert!(duplicate.is_duplicate());
    assert_eq!(
        duplicate.duplicate_reason(),
        Some(AgentInboxDuplicateReason::MessageId)
    );

    let began = begin_step(&restarted, AgentTimestampMillis::new(200)).await;
    assert_eq!(began.next_status, AgentRunStatus::Running);
    assert_eq!(began.state.current_attempt, 1);

    let completed = succeed_step(
        &restarted,
        AgentStepSuccess::complete(inline_payload("runtime-complete")),
        AgentTimestampMillis::new(300),
    )
    .await;
    assert_eq!(completed.kind, AgentRunTransitionKind::Complete);
    assert_eq!(completed.next_status, AgentRunStatus::Completed);

    let final_snapshot = snapshot(&restarted).await;
    assert_eq!(
        final_snapshot
            .run_state
            .expect("completed run state should remain durable")
            .status,
        AgentRunStatus::Completed
    );

    system.terminate().await.expect("system should terminate");
}

#[tokio::test]
async fn actor_recovers_due_effects_after_process_local_restart() {
    let system = ActorSystem::new("agent-runtime-outbox-restart");
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-runtime-outbox");

    let first = spawn_actor(&system, "runtime-outbox-a", &fixture, run_id.clone());

    let scheduled = first
        .ask(
            |reply_to| AgentRunActorCommand::ScheduleEffect {
                effect: effect("effect-runtime-1", AgentTimestampMillis::new(500)),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("schedule effect ask should reply")
        .expect("effect should schedule");
    assert!(scheduled.is_scheduled());

    first.stop().expect("first actor should stop");
    wait_until_terminated(&first).await;

    let restarted = spawn_actor(&system, "runtime-outbox-b", &fixture, run_id);

    let before_due = restarted
        .ask(
            |reply_to| AgentRunActorCommand::DueEffects { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("due effects ask should reply")
        .expect("due effects should decode");
    assert!(before_due.is_empty());

    fixture.clock.set(WorkflowTimestamp::from_millis(500));
    let due = restarted
        .ask(
            |reply_to| AgentRunActorCommand::DueEffects { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("due effects ask should reply after clock advance")
        .expect("due effects should decode after restart");
    assert_eq!(due.len(), 1);
    assert_eq!(
        due[0].effect.effect_id,
        AgentEffectId::new("effect-runtime-1")
    );

    let recovered = snapshot(&restarted).await;
    assert_eq!(recovered.due_effect_count, 1);

    system.terminate().await.expect("system should terminate");
}

#[tokio::test]
async fn local_actor_graph_run_completes_and_snapshots_node_counts() {
    let system = ActorSystem::new("agent-runtime-graph-local");
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-runtime-graph-local");
    let plan = Arc::new(local_graph_plan(&fixture.workflow));
    let actor = spawn_actor(&system, "runtime-graph-local", &fixture, run_id.clone());

    let started = start_graph(
        &actor,
        graph_accepted_run_state(&fixture.workflow, &run_id),
        plan.clone(),
        AgentTimestampMillis::new(100),
    )
    .await;
    assert_eq!(started.kind, AgentRunTransitionKind::Start);
    assert!(started.state.graph_state.is_some());

    let ready = mark_graph_ready(&actor, plan.clone(), AgentTimestampMillis::new(110)).await;
    assert_eq!(
        ready.run_transition.kind,
        AgentRunTransitionKind::GraphUpdated
    );
    assert_eq!(
        ready.graph_transition.runnable_node_ids,
        vec![node_id("input")]
    );

    let ready_snapshot = snapshot(&actor).await;
    let graph = ready_snapshot
        .graph
        .as_ref()
        .expect("actor snapshot should include graph summary");
    assert_eq!(graph.node_count, 2);
    assert_eq!(graph.runnable_node_count, 1);

    start_graph_node(
        &actor,
        plan.clone(),
        "input",
        AgentTimestampMillis::new(120),
    )
    .await;
    complete_graph_node(
        &actor,
        plan.clone(),
        "input",
        AgentTimestampMillis::new(130),
    )
    .await;
    mark_graph_ready(&actor, plan.clone(), AgentTimestampMillis::new(140)).await;
    start_graph_node(
        &actor,
        plan.clone(),
        "terminal",
        AgentTimestampMillis::new(150),
    )
    .await;
    let completed =
        complete_graph_node(&actor, plan, "terminal", AgentTimestampMillis::new(160)).await;

    assert_eq!(
        completed.run_transition.next_status,
        AgentRunStatus::Completed
    );
    let final_snapshot = snapshot(&actor).await;
    assert_eq!(
        final_snapshot
            .run_state
            .as_ref()
            .expect("run should remain durable")
            .status,
        AgentRunStatus::Completed
    );
    assert_eq!(
        final_snapshot
            .graph
            .as_ref()
            .expect("completed graph summary should remain visible")
            .terminal_node_count,
        1
    );

    system.terminate().await.expect("system should terminate");
}

#[tokio::test]
async fn actor_restart_after_graph_node_runnable_recovers_graph_state() {
    let system = ActorSystem::new("agent-runtime-graph-runnable-restart");
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-runtime-graph-runnable");
    let plan = Arc::new(local_graph_plan(&fixture.workflow));
    let first = spawn_actor(
        &system,
        "runtime-graph-runnable-a",
        &fixture,
        run_id.clone(),
    );

    start_graph(
        &first,
        graph_accepted_run_state(&fixture.workflow, &run_id),
        plan.clone(),
        AgentTimestampMillis::new(100),
    )
    .await;
    mark_graph_ready(&first, plan, AgentTimestampMillis::new(110)).await;

    first.stop().expect("first actor should stop");
    wait_until_terminated(&first).await;

    let restarted = spawn_actor(
        &system,
        "runtime-graph-runnable-b",
        &fixture,
        run_id.clone(),
    );
    let recovered = snapshot(&restarted).await;
    let graph = recovered
        .graph
        .as_ref()
        .expect("graph summary should recover");

    assert_eq!(graph.runnable_node_count, 1);
    assert_eq!(node_status(graph, "input"), AgentGraphNodeStatus::Runnable);
    assert_eq!(
        recovered
            .run_state
            .expect("run state should recover")
            .status,
        AgentRunStatus::Running
    );

    system.terminate().await.expect("system should terminate");
}

#[tokio::test]
async fn actor_restart_after_graph_effect_scheduled_recovers_graph_and_due_effect() {
    let system = ActorSystem::new("agent-runtime-graph-effect-restart");
    let fixture = RuntimeFixture::new(workflow());
    let run_id = AgentRunId::new("run-runtime-graph-effect");
    let plan = Arc::new(effect_graph_plan(&fixture.workflow));
    let first = spawn_actor(&system, "runtime-graph-effect-a", &fixture, run_id.clone());

    start_graph(
        &first,
        graph_accepted_run_state(&fixture.workflow, &run_id),
        plan.clone(),
        AgentTimestampMillis::new(100),
    )
    .await;
    mark_graph_ready(&first, plan.clone(), AgentTimestampMillis::new(110)).await;
    start_graph_node(
        &first,
        plan.clone(),
        "input",
        AgentTimestampMillis::new(120),
    )
    .await;
    complete_graph_node(
        &first,
        plan.clone(),
        "input",
        AgentTimestampMillis::new(130),
    )
    .await;
    mark_graph_ready(&first, plan.clone(), AgentTimestampMillis::new(140)).await;
    start_graph_node(
        &first,
        plan.clone(),
        "effect",
        AgentTimestampMillis::new(150),
    )
    .await;

    let scheduled =
        schedule_graph_effect(&first, plan, effect_request(run_id.clone(), "effect", 160)).await;
    assert!(scheduled.effect_outcome.acceptance.is_scheduled());
    assert_eq!(
        scheduled
            .effect_outcome
            .transition
            .state
            .node_states
            .get(&node_id("effect"))
            .expect("effect node state should exist")
            .wait_reason,
        Some(AgentGraphWaitReason::Effect)
    );

    first.stop().expect("first actor should stop");
    wait_until_terminated(&first).await;

    let restarted = spawn_actor(&system, "runtime-graph-effect-b", &fixture, run_id.clone());
    let due = restarted
        .ask(
            |reply_to| AgentRunActorCommand::DueEffects { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("due effects ask should reply")
        .expect("due effects should decode");
    assert_eq!(due.len(), 1);
    assert_eq!(
        due[0].effect.effect_id,
        AgentEffectId::new(
            "graph-effect:run=run-runtime-graph-effect;plan=sha256:runtime-effect-graph;node=effect;loop=root;kind=tool-call;target_class=tool"
        )
    );

    let recovered = snapshot(&restarted).await;
    assert_eq!(recovered.due_effect_count, 1);
    let graph = recovered
        .graph
        .as_ref()
        .expect("graph summary should recover");
    assert_eq!(graph.waiting_node_count, 1);
    assert_eq!(node_status(graph, "effect"), AgentGraphNodeStatus::Waiting);

    system.terminate().await.expect("system should terminate");
}

fn spawn_actor(
    system: &ActorSystem,
    name: &str,
    fixture: &RuntimeFixture,
    run_id: AgentRunId,
) -> TestActorRef {
    system
        .spawn(
            name,
            TestActor::with_clock_and_metrics(
                fixture.workflow.clone(),
                run_id,
                fixture.run_store.clone(),
                fixture.workflow_store.clone(),
                fixture.clock.clone(),
                fixture.metrics.clone(),
            ),
        )
        .expect("agent run actor should spawn")
}

async fn snapshot(actor: &TestActorRef) -> AgentRunActorSnapshot {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::Snapshot { reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("snapshot ask should reply")
        .expect("snapshot should succeed")
}

async fn start(actor: &TestActorRef, initial_state: AgentRunState) -> AgentRunTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::Start {
                initial_state,
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("start ask should reply")
        .expect("start should succeed")
}

async fn start_graph(
    actor: &TestActorRef,
    initial_state: AgentRunState,
    plan: Arc<AgentCompiledExecutionPlan>,
    now: AgentTimestampMillis,
) -> AgentRunTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::StartGraph {
                initial_state,
                plan,
                now,
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("start graph ask should reply")
        .expect("start graph should succeed")
}

async fn mark_graph_ready(
    actor: &TestActorRef,
    plan: Arc<AgentCompiledExecutionPlan>,
    now: AgentTimestampMillis,
) -> AgentGraphRuntimeTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::MarkGraphReady {
                plan,
                now,
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("mark graph ready ask should reply")
        .expect("mark graph ready should succeed")
}

async fn start_graph_node(
    actor: &TestActorRef,
    plan: Arc<AgentCompiledExecutionPlan>,
    node_id_value: &str,
    now: AgentTimestampMillis,
) -> AgentGraphRuntimeTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::StartGraphNode {
                plan,
                node_id: node_id(node_id_value),
                now,
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("start graph node ask should reply")
        .expect("start graph node should succeed")
}

async fn complete_graph_node(
    actor: &TestActorRef,
    plan: Arc<AgentCompiledExecutionPlan>,
    node_id_value: &str,
    now: AgentTimestampMillis,
) -> AgentGraphRuntimeTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::CompleteGraphNode {
                plan,
                node_id: node_id(node_id_value),
                now,
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("complete graph node ask should reply")
        .expect("complete graph node should succeed")
}

async fn schedule_graph_effect(
    actor: &TestActorRef,
    plan: Arc<AgentCompiledExecutionPlan>,
    request: AgentGraphEffectScheduleRequest,
) -> AgentGraphRuntimeEffectOutcome {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::ScheduleGraphNodeEffect {
                plan,
                request,
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("schedule graph effect ask should reply")
        .expect("schedule graph effect should succeed")
}

async fn begin_step(actor: &TestActorRef, now: AgentTimestampMillis) -> AgentRunTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::BeginStep { now, reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("begin step ask should reply")
        .expect("begin step should succeed")
}

async fn succeed_step(
    actor: &TestActorRef,
    success: AgentStepSuccess,
    now: AgentTimestampMillis,
) -> AgentRunTransition {
    actor
        .ask(
            |reply_to| AgentRunActorCommand::SucceedStep {
                success,
                now,
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("succeed step ask should reply")
        .expect("succeed step should persist")
}

async fn wait_until_terminated<M>(actor: &ActorRef<M>)
where
    M: Message,
{
    for _ in 0..100 {
        if actor.is_terminated() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-runtime"),
        workflow_type: "runtime".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Runtime workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec![AgentCommandKind::StartRun.type_name().to_string()],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("runtime.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::from([(
            "workflow_type".to_string(),
            "runtime".to_string(),
        )]),
    }
}

fn accepted_run_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    first_step_id: AgentStepId,
) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-runtime")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(first_step_id),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(100),
        completed_at: None,
    }
}

fn graph_accepted_run_state(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-runtime")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: None,
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(100),
        completed_at: None,
    }
}

fn local_graph_plan(workflow: &AgentWorkflow) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "payload"),
    );
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Input, "payload"),
        );

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-runtime-local-graph"),
        workflow.workflow_id.clone(),
        workflow.workflow_type.clone(),
        workflow.definition_version.clone(),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:runtime-local-graph"),
    )
    .entry_node("input")
    .node(input)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-terminal",
        "input",
        "payload",
        "terminal",
        "payload",
    ))
}

fn effect_graph_plan(workflow: &AgentWorkflow) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    );
    let effect = AgentCompiledPlanNode::new("effect", AgentCompiledNodeKind::ToolCall)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "effect-result",
        ))
        .target(
            AgentCompiledNodeTarget::new("tool", "runtime-tool")
                .address("tool://runtime-tool")
                .attribute("target_class", "tool"),
        );
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Input,
            "effect-result",
        ));

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-runtime-effect-graph"),
        workflow.workflow_id.clone(),
        workflow.workflow_type.clone(),
        workflow.definition_version.clone(),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:runtime-effect-graph"),
    )
    .entry_node("input")
    .node(input)
    .node(effect)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-effect",
        "input",
        "payload",
        "effect",
        "payload",
    ))
    .edge(AgentCompiledPlanEdge::new(
        "edge-effect-terminal",
        "effect",
        "result",
        "terminal",
        "result",
    ))
}

fn effect_request(
    run_id: AgentRunId,
    node_id_value: &str,
    created_at_millis: u64,
) -> AgentGraphEffectScheduleRequest {
    AgentGraphEffectScheduleRequest::new(
        run_id,
        node_id_value,
        AgentTimestampMillis::new(created_at_millis),
        AgentCausationId::new("cause:runtime-graph"),
        AgentCorrelationId::new("corr:runtime-graph"),
    )
    .due_at(AgentTimestampMillis::new(100))
    .timeout_ms(1_000)
    .expected_result_type("runtime.effect.result")
}

fn node_id(value: &str) -> AgentCompiledNodeId {
    AgentCompiledNodeId::new(value)
}

fn node_status(graph: &AgentGraphRunProjection, node_id_value: &str) -> AgentGraphNodeStatus {
    graph
        .nodes
        .iter()
        .find(|node| node.node_id == node_id(node_id_value))
        .expect("graph node projection should exist")
        .status
}

fn start_command(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            workflow.workflow_id.clone(),
            run_id.clone(),
            AgentCommandId::new(format!("command-start-{}", run_id.as_str())),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new(format!("start:{}", run_id.as_str())),
                AgentCausationId::new("runtime-ingress"),
                AgentCorrelationId::new(format!("corr-{}", run_id.as_str())),
            ),
            AgentTenantId::new("tenant-runtime"),
            AgentTimestampMillis::new(100),
        )
        .expect("start metadata should be valid"),
    )
    .expect("start command should be valid")
}

fn effect(effect_id: &str, due_at: AgentTimestampMillis) -> AgentEffect {
    AgentEffectSchedule::new(
        AgentEffectKind::ToolCall,
        AgentEffectTarget {
            target_type: "tool".to_string(),
            name: "runtime-tool".to_string(),
            address: None,
            attributes: BTreeMap::new(),
        },
        AgentEffectMetadata::new(
            AgentEffectId::new(effect_id),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new(format!("effect:{effect_id}")),
                AgentCausationId::new(format!("cause:{effect_id}")),
                AgentCorrelationId::new(format!("corr:{effect_id}")),
            ),
            AgentIdempotencyKey::new(format!("idempotency:{effect_id}")),
            AgentTimestampMillis::new(100),
        )
        .expect("effect metadata should be valid")
        .due_at(due_at),
    )
    .expect("effect schedule should be valid")
    .into_effect()
    .expect("effect should validate")
}

fn inline_payload(value: &str) -> AgentStatePayload {
    let bytes = value.as_bytes().to_vec();
    AgentStatePayload::Inline(InlineState {
        content_type: "text/plain".to_string(),
        size_bytes: bytes.len() as u64,
        bytes,
    })
}
