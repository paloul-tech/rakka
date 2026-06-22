//! Compiled graph scheduler core tests.

use rakka_agent_workflow::{
    AgentCompiledEdgeId, AgentCompiledEdgeMergeBehavior, AgentCompiledExecutionPlan,
    AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanEdge,
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

#[test]
fn scheduler_fan_out_marks_independent_downstream_nodes_runnable() {
    let scheduler = AgentGraphScheduler::new();
    let plan = fan_out_join_plan(AgentCompiledEdgeMergeBehavior::WaitForAll);
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let state = run_node(&scheduler, &plan, state, "input", ts(110));

    let transition = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(150))
        .expect("fan-out nodes should become runnable");

    assert_eq!(
        node_ids(&transition.changed_node_ids),
        vec!["left", "right"]
    );
    assert_eq!(
        node_ids(&transition.runnable_node_ids),
        vec!["left", "right"]
    );
}

#[test]
fn scheduler_fan_in_waits_for_all_required_upstream_nodes() {
    let scheduler = AgentGraphScheduler::new();
    let plan = fan_out_join_plan(AgentCompiledEdgeMergeBehavior::WaitForAll);
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let state = run_node(&scheduler, &plan, state, "input", ts(110));
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(150))
        .expect("fan-out nodes should become runnable")
        .state;
    let state = run_node(&scheduler, &plan, state, "left", ts(160));

    assert!(
        scheduler
            .compute_ready_nodes(&plan, &state)
            .expect("ready set should compute")
            .is_empty(),
        "join should wait for right node"
    );

    let state = run_node(&scheduler, &plan, state, "right", ts(200));
    let ready = scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("ready set should compute");

    assert_eq!(node_ids(&ready), vec!["join"]);
}

#[test]
fn scheduler_wait_for_any_join_runs_after_first_completed_upstream() {
    let scheduler = AgentGraphScheduler::new();
    let plan = fan_out_join_plan(AgentCompiledEdgeMergeBehavior::WaitForAny);
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let state = run_node(&scheduler, &plan, state, "input", ts(110));
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(150))
        .expect("fan-out nodes should become runnable")
        .state;
    let state = run_node(&scheduler, &plan, state, "left", ts(160));

    let ready = scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("ready set should compute");

    assert_eq!(node_ids(&ready), vec!["join"]);
}

#[test]
fn scheduler_branch_selection_skips_unselected_path_and_unblocks_join() {
    let scheduler = AgentGraphScheduler::new();
    let plan = branch_join_plan();
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let state = run_node(&scheduler, &plan, state, "input", ts(110));
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(150))
        .expect("branch should become runnable")
        .state;
    let state = scheduler
        .start_node(&plan, state, "branch", ts(160))
        .expect("branch should start")
        .state;
    let transition = scheduler
        .complete_branch_node(
            &plan,
            state,
            "branch",
            vec![AgentCompiledEdgeId::new("edge-branch-left")],
            ts(170),
        )
        .expect("branch selection should persist and propagate skips");
    let state = transition.state;

    assert_eq!(
        state
            .selected_branch_paths
            .get(&AgentCompiledNodeId::new("branch"))
            .cloned(),
        Some(vec![AgentCompiledEdgeId::new("edge-branch-left")])
    );
    assert_eq!(
        node_state(&state, "right").status,
        AgentGraphNodeStatus::Skipped
    );
    assert!(state
        .skipped_nodes
        .contains(&AgentCompiledNodeId::new("right")));

    let ready = scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("ready set should compute");
    assert_eq!(node_ids(&ready), vec!["left"]);

    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(180))
        .expect("selected path should become runnable")
        .state;
    let state = run_node(&scheduler, &plan, state, "left", ts(190));
    let ready = scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("ready set should compute");

    assert_eq!(node_ids(&ready), vec!["join"]);
}

#[test]
fn scheduler_rejects_branch_completion_without_selected_outgoing_edge() {
    let scheduler = AgentGraphScheduler::new();
    let plan = branch_join_plan();
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let state = run_node(&scheduler, &plan, state, "input", ts(110));
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(150))
        .expect("branch should become runnable")
        .state;
    let state = scheduler
        .start_node(&plan, state, "branch", ts(160))
        .expect("branch should start")
        .state;

    let error = scheduler
        .complete_branch_node(
            &plan,
            state,
            "branch",
            Vec::<AgentCompiledEdgeId>::new(),
            ts(170),
        )
        .expect_err("empty branch selection should fail");

    assert_eq!(error.code(), "invalid-branch-selection");
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

fn fan_out_join_plan(merge_behavior: AgentCompiledEdgeMergeBehavior) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    );
    let left = AgentCompiledPlanNode::new("left", AgentCompiledNodeKind::Transform)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "left-result",
        ));
    let right = AgentCompiledPlanNode::new("right", AgentCompiledNodeKind::Transform)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "right-result",
        ));
    let join = AgentCompiledPlanNode::new("join", AgentCompiledNodeKind::Join)
        .input_port(AgentCompiledPlanPort::new(
            "left",
            AgentCompiledPortDirection::Input,
            "left-result",
        ))
        .input_port(AgentCompiledPlanPort::new(
            "right",
            AgentCompiledPortDirection::Input,
            "right-result",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "joined",
            AgentCompiledPortDirection::Output,
            "joined",
        ));
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("result", AgentCompiledPortDirection::Input, "joined"),
        );

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-fan-out-join-v1"),
        AgentWorkflowId::new("workflow-fan-out-join"),
        "fan-out-join",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new(format!(
            "sha256:fan-out-join-{}",
            merge_behavior.as_label()
        )),
    )
    .entry_node("input")
    .node(input)
    .node(left)
    .node(right)
    .node(join)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-left",
        "input",
        "payload",
        "left",
        "payload",
    ))
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-right",
        "input",
        "payload",
        "right",
        "payload",
    ))
    .edge(
        AgentCompiledPlanEdge::new("edge-left-join", "left", "result", "join", "left")
            .merge_behavior(merge_behavior),
    )
    .edge(
        AgentCompiledPlanEdge::new("edge-right-join", "right", "result", "join", "right")
            .merge_behavior(merge_behavior),
    )
    .edge(AgentCompiledPlanEdge::new(
        "edge-join-terminal",
        "join",
        "joined",
        "terminal",
        "result",
    ))
}

fn branch_join_plan() -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    );
    let branch = AgentCompiledPlanNode::new("branch", AgentCompiledNodeKind::Branch)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "left",
            AgentCompiledPortDirection::Output,
            "branch-left",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "right",
            AgentCompiledPortDirection::Output,
            "branch-right",
        ));
    let left = AgentCompiledPlanNode::new("left", AgentCompiledNodeKind::Transform)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "branch-left",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "left-result",
        ));
    let right = AgentCompiledPlanNode::new("right", AgentCompiledNodeKind::Transform)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "branch-right",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "right-result",
        ));
    let join = AgentCompiledPlanNode::new("join", AgentCompiledNodeKind::Join)
        .input_port(AgentCompiledPlanPort::new(
            "left",
            AgentCompiledPortDirection::Input,
            "left-result",
        ))
        .input_port(AgentCompiledPlanPort::new(
            "right",
            AgentCompiledPortDirection::Input,
            "right-result",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "joined",
            AgentCompiledPortDirection::Output,
            "joined",
        ));
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("result", AgentCompiledPortDirection::Input, "joined"),
        );

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-branch-join-v1"),
        AgentWorkflowId::new("workflow-branch-join"),
        "branch-join",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new("sha256:branch-join"),
    )
    .entry_node("input")
    .node(input)
    .node(branch)
    .node(left)
    .node(right)
    .node(join)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-branch",
        "input",
        "payload",
        "branch",
        "payload",
    ))
    .edge(AgentCompiledPlanEdge::new(
        "edge-branch-left",
        "branch",
        "left",
        "left",
        "payload",
    ))
    .edge(AgentCompiledPlanEdge::new(
        "edge-branch-right",
        "branch",
        "right",
        "right",
        "payload",
    ))
    .edge(
        AgentCompiledPlanEdge::new("edge-left-join", "left", "result", "join", "left")
            .merge_behavior(AgentCompiledEdgeMergeBehavior::WaitForAll),
    )
    .edge(
        AgentCompiledPlanEdge::new("edge-right-join", "right", "result", "join", "right")
            .merge_behavior(AgentCompiledEdgeMergeBehavior::WaitForAll),
    )
    .edge(AgentCompiledPlanEdge::new(
        "edge-join-terminal",
        "join",
        "joined",
        "terminal",
        "result",
    ))
}

fn run_node(
    scheduler: &AgentGraphScheduler,
    plan: &AgentCompiledExecutionPlan,
    mut state: AgentGraphRunState,
    node_id: &str,
    at: AgentTimestampMillis,
) -> AgentGraphRunState {
    if node_state(&state, node_id).status == AgentGraphNodeStatus::Pending {
        state = scheduler
            .mark_ready_nodes_runnable(plan, state, at)
            .expect("node should become runnable")
            .state;
    }
    let started_at = AgentTimestampMillis::new(at.as_millis() + 1);
    let completed_at = AgentTimestampMillis::new(at.as_millis() + 2);
    let state = scheduler
        .start_node(plan, state, node_id, started_at)
        .expect("node should start")
        .state;
    scheduler
        .complete_node(plan, state, node_id, completed_at)
        .expect("node should complete")
        .state
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
