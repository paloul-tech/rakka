//! Compiled graph scheduler core tests.

use rakka_agent_workflow::{
    AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanEdge,
    AgentCompiledPlanFingerprint, AgentCompiledPlanId, AgentCompiledPlanNode,
    AgentCompiledPlanPort, AgentCompiledPortDirection, AgentGraphNodeState, AgentGraphNodeStatus,
    AgentGraphRunState, AgentGraphScheduler, AgentGraphWaitReason, AgentTimestampMillis,
    AgentWorkflowId, WorkflowDefinitionVersion, CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};

#[test]
fn scheduler_runs_linear_graph_in_deterministic_order() {
    let scheduler = AgentGraphScheduler::new();
    let plan = linear_plan();
    let mut state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");

    let ready = scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("ready set should compute");
    assert_eq!(node_ids(&ready), vec!["input"]);

    let transition = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(110))
        .expect("input should become runnable");
    assert_eq!(node_ids(&transition.changed_node_ids), vec!["input"]);
    assert_eq!(node_ids(&transition.runnable_node_ids), vec!["input"]);
    state = transition.state;

    state = scheduler
        .start_node(&plan, state, "input", ts(120))
        .expect("input should start")
        .state;
    state = scheduler
        .complete_node(&plan, state, "input", ts(130))
        .expect("input should complete")
        .state;

    let transition = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(140))
        .expect("transform should become runnable");
    assert_eq!(node_ids(&transition.runnable_node_ids), vec!["transform"]);
    state = transition.state;

    state = scheduler
        .start_node(&plan, state, "transform", ts(150))
        .expect("transform should start")
        .state;
    state = scheduler
        .complete_node(&plan, state, "transform", ts(160))
        .expect("transform should complete")
        .state;

    let transition = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(170))
        .expect("terminal should become runnable");
    assert_eq!(node_ids(&transition.runnable_node_ids), vec!["terminal"]);
    state = transition.state;

    state = scheduler
        .start_node(&plan, state, "terminal", ts(180))
        .expect("terminal should start")
        .state;
    state = scheduler
        .complete_node(&plan, state, "terminal", ts(190))
        .expect("terminal should complete graph")
        .state;

    assert_eq!(
        node_state(&state, "terminal").status,
        AgentGraphNodeStatus::Terminal
    );
    assert!(state.terminal_status.is_some());
    assert_eq!(state.scheduler_revision, 9);
}

#[test]
fn scheduler_recomputes_ready_set_after_command_acceptance_crash() {
    let scheduler = AgentGraphScheduler::new();
    let plan = linear_plan();
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");

    let first = scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("ready set should compute");
    let recovered = scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("ready set should recompute after recovery");

    assert_eq!(node_ids(&first), vec!["input"]);
    assert_eq!(first, recovered);
    assert_eq!(state.scheduler_revision, 0);
}

#[test]
fn scheduler_recovers_same_runnable_nodes_after_runnable_persisted() {
    let scheduler = AgentGraphScheduler::new();
    let plan = linear_plan();
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let persisted = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(110))
        .expect("input should become runnable")
        .state;

    assert_eq!(
        node_ids(&scheduler.runnable_nodes(&persisted)),
        vec!["input"]
    );
    let recovered_transition = scheduler
        .mark_ready_nodes_runnable(&plan, persisted.clone(), ts(120))
        .expect("marking ready again should be idempotent");
    assert!(recovered_transition.changed_node_ids.is_empty());
    assert_eq!(
        node_ids(&recovered_transition.runnable_node_ids),
        vec!["input"]
    );
    assert_eq!(recovered_transition.state.scheduler_revision, 1);
}

#[test]
fn scheduler_transitions_expose_stable_error_codes() {
    let scheduler = AgentGraphScheduler::new();
    let plan = linear_plan();
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");

    let error = scheduler
        .start_node(&plan, state, "transform", ts(110))
        .expect_err("pending transform cannot start");

    assert_eq!(error.code(), "invalid-graph-node-transition");

    let mismatched_state = AgentGraphRunState::new(
        AgentCompiledPlanId::new("plan-other"),
        plan.plan_fingerprint.clone(),
    );
    let error = scheduler
        .start_node(&plan, mismatched_state, "input", ts(120))
        .expect_err("state from another plan should fail");

    assert_eq!(error.code(), "graph-plan-state-mismatch");
}

#[test]
fn scheduler_can_move_running_node_to_waiting() {
    let scheduler = AgentGraphScheduler::new();
    let plan = linear_plan();
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(110))
        .expect("input should become runnable")
        .state;
    let state = scheduler
        .start_node(&plan, state, "input", ts(120))
        .expect("input should start")
        .state;
    let state = scheduler
        .wait_node(&plan, state, "input", AgentGraphWaitReason::Signal, ts(130))
        .expect("running input can wait")
        .state;

    assert_eq!(
        node_state(&state, "input").status,
        AgentGraphNodeStatus::Waiting
    );
    assert_eq!(
        node_state(&state, "input").wait_reason,
        Some(AgentGraphWaitReason::Signal)
    );
}

fn linear_plan() -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    );
    let transform = AgentCompiledPlanNode::new("transform", AgentCompiledNodeKind::Transform)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "result",
        ));
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("result", AgentCompiledPortDirection::Input, "result"),
        );

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-linear-v1"),
        AgentWorkflowId::new("workflow-linear"),
        "linear",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:linear"),
    )
    .entry_node("input")
    .node(terminal)
    .node(transform)
    .node(input)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-transform",
        "input",
        "payload",
        "transform",
        "payload",
    ))
    .edge(AgentCompiledPlanEdge::new(
        "edge-transform-terminal",
        "transform",
        "result",
        "terminal",
        "result",
    ))
}

fn node_state<'a>(state: &'a AgentGraphRunState, node_id: &str) -> &'a AgentGraphNodeState {
    state
        .node_states
        .get(&AgentCompiledNodeId::new(node_id))
        .expect("node state should exist")
}

fn node_ids(node_ids: &[AgentCompiledNodeId]) -> Vec<&str> {
    node_ids.iter().map(|node_id| node_id.as_str()).collect()
}

const fn ts(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}
