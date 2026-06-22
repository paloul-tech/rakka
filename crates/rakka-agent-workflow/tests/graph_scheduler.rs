//! Compiled graph scheduler core tests.

use rakka_agent_workflow::{
    AgentCompiledEdgeId, AgentCompiledEdgeMergeBehavior, AgentCompiledExecutionPlan,
    AgentCompiledIteratorPolicy, AgentCompiledNodeId, AgentCompiledNodeKind, AgentCompiledPlanEdge,
    AgentCompiledPlanFingerprint, AgentCompiledPlanId, AgentCompiledPlanNode,
    AgentCompiledPlanPort, AgentCompiledPortDirection, AgentGraphNodeState, AgentGraphNodeStatus,
    AgentGraphRunState, AgentGraphScheduler, AgentGraphTerminalStatus, AgentGraphWaitReason,
    AgentTimestampMillis, AgentWorkflowId, WorkflowDefinitionVersion,
    CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
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

#[test]
fn scheduler_completes_zero_iteration_iterator() {
    let scheduler = AgentGraphScheduler::new();
    let plan = iterator_plan(3);
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let state = run_node(&scheduler, &plan, state, "input", ts(110));
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(150))
        .expect("iterator should become runnable")
        .state;
    let state = scheduler
        .start_node(&plan, state, "iterate", ts(160))
        .expect("iterator should start")
        .state;
    let state = scheduler
        .complete_iterator_node(&plan, state, "iterate", ts(170))
        .expect("iterator can complete with zero actual iterations")
        .state;

    assert!(state.loop_instances.is_empty());
    assert_eq!(
        node_state(&state, "iterate").status,
        AgentGraphNodeStatus::Completed
    );
    assert_eq!(
        node_ids(
            &scheduler
                .compute_ready_nodes(&plan, &state)
                .expect("ready set should compute")
        ),
        vec!["terminal"]
    );
}

#[test]
fn scheduler_runs_multiple_bounded_iterator_iterations() {
    let scheduler = AgentGraphScheduler::new();
    let plan = iterator_plan(3);
    let state = start_iterator(&scheduler, &plan);
    let state = scheduler
        .start_iterator_iteration(&plan, state, "iterate", ts(170))
        .expect("iteration 0 should start")
        .state;
    assert_eq!(
        scheduler.current_iterator_iteration_index(&state, "iterate"),
        Some(0)
    );
    let state = scheduler
        .complete_iterator_iteration(&plan, state, "iterate", 0, ts(180))
        .expect("iteration 0 should complete")
        .state;
    let state = scheduler
        .start_iterator_iteration(&plan, state, "iterate", ts(190))
        .expect("iteration 1 should start")
        .state;
    assert_eq!(
        scheduler.current_iterator_iteration_index(&state, "iterate"),
        Some(1)
    );
    let state = scheduler
        .complete_iterator_iteration(&plan, state, "iterate", 1, ts(200))
        .expect("iteration 1 should complete")
        .state;
    let state = scheduler
        .complete_iterator_node(&plan, state, "iterate", ts(210))
        .expect("iterator should complete after iterations")
        .state;

    let iterations: Vec<_> = state
        .loop_instances
        .iter()
        .map(|instance| {
            (
                instance.node_id.as_str(),
                instance.iteration_index,
                instance.status,
            )
        })
        .collect();
    assert_eq!(
        iterations,
        vec![
            ("iterate", 0, AgentGraphNodeStatus::Completed),
            ("iterate", 1, AgentGraphNodeStatus::Completed),
        ]
    );
    assert_eq!(
        node_state(&state, "iterate").status,
        AgentGraphNodeStatus::Completed
    );
}

#[test]
fn scheduler_fails_iterator_when_iteration_bound_is_exceeded() {
    let scheduler = AgentGraphScheduler::new();
    let plan = iterator_plan(2);
    let state = start_iterator(&scheduler, &plan);
    let state = scheduler
        .start_iterator_iteration(&plan, state, "iterate", ts(170))
        .expect("iteration 0 should start")
        .state;
    let state = scheduler
        .complete_iterator_iteration(&plan, state, "iterate", 0, ts(180))
        .expect("iteration 0 should complete")
        .state;
    let state = scheduler
        .start_iterator_iteration(&plan, state, "iterate", ts(190))
        .expect("iteration 1 should start")
        .state;
    let state = scheduler
        .complete_iterator_iteration(&plan, state, "iterate", 1, ts(200))
        .expect("iteration 1 should complete")
        .state;
    let state = scheduler
        .start_iterator_iteration(&plan, state, "iterate", ts(210))
        .expect("exceeding the bound should produce a durable failure")
        .state;

    assert_eq!(
        node_state(&state, "iterate").status,
        AgentGraphNodeStatus::Failed
    );
    assert_eq!(
        node_state(&state, "iterate").error_code.as_deref(),
        Some("iterator-bound-exceeded")
    );
    assert_eq!(
        state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
    assert_eq!(state.loop_instances.len(), 2);
}

#[test]
fn scheduler_recovers_active_iterator_iteration_after_crash() {
    let scheduler = AgentGraphScheduler::new();
    let plan = iterator_plan(2);
    let state = start_iterator(&scheduler, &plan);
    let persisted = scheduler
        .start_iterator_iteration(&plan, state, "iterate", ts(170))
        .expect("iteration 0 should start")
        .state;

    let recovered = persisted.clone();
    assert_eq!(
        scheduler.current_iterator_iteration_index(&recovered, "iterate"),
        Some(0)
    );
    assert_eq!(recovered.loop_instances[0].node_id.as_str(), "iterate");
    assert_eq!(recovered.loop_instances[0].iteration_index, 0);
    assert_eq!(
        recovered.loop_instances[0].status,
        AgentGraphNodeStatus::Running
    );

    let error = scheduler
        .start_iterator_iteration(&plan, recovered.clone(), "iterate", ts(180))
        .expect_err("recovery should resume the active iteration");
    assert_eq!(error.code(), "invalid-iterator-transition");

    let state = scheduler
        .complete_iterator_iteration(&plan, recovered, "iterate", 0, ts(190))
        .expect("recovered iteration should complete")
        .state;
    assert_eq!(
        scheduler.current_iterator_iteration_index(&state, "iterate"),
        None
    );
}

#[test]
fn scheduler_cancels_graph_before_start() {
    let scheduler = AgentGraphScheduler::new();
    let plan = linear_plan();
    let state = scheduler
        .initialize_state(&plan, ts(100))
        .expect("graph state should initialize");
    let transition = scheduler
        .cancel_graph_run(&plan, state, ts(110))
        .expect("graph should cancel before start");
    let cancelled = transition.state;

    assert_eq!(
        node_ids(&transition.changed_node_ids),
        vec!["input", "terminal", "transform"]
    );
    assert_eq!(
        cancelled.terminal_status,
        Some(AgentGraphTerminalStatus::Cancelled)
    );
    assert!(cancelled
        .node_states
        .values()
        .all(|node| node.status == AgentGraphNodeStatus::Cancelled));
    assert!(scheduler.runnable_nodes(&cancelled).is_empty());
    assert!(scheduler
        .compute_ready_nodes(&plan, &cancelled)
        .expect("terminal graph ready set should compute")
        .is_empty());

    let cancelled_revision = cancelled.scheduler_revision;
    let idempotent = scheduler
        .cancel_graph_run(&plan, cancelled, ts(120))
        .expect("repeated cancellation should be idempotent");
    assert!(idempotent.changed_node_ids.is_empty());
    assert_eq!(idempotent.state.scheduler_revision, cancelled_revision);
}

#[test]
fn scheduler_cancels_graph_while_node_is_running() {
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
        .cancel_graph_run(&plan, state, ts(130))
        .expect("graph should cancel while running")
        .state;

    assert_eq!(
        node_state(&state, "input").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        node_state(&state, "transform").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        node_state(&state, "terminal").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        state.terminal_status,
        Some(AgentGraphTerminalStatus::Cancelled)
    );
    assert!(scheduler.runnable_nodes(&state).is_empty());
}

#[test]
fn scheduler_cancels_graph_while_waiting_for_effect() {
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
        .wait_node(&plan, state, "input", AgentGraphWaitReason::Effect, ts(130))
        .expect("input should wait for effect")
        .state;
    let state = scheduler
        .cancel_graph_run(&plan, state, ts(140))
        .expect("graph should cancel while waiting")
        .state;

    assert_eq!(
        node_state(&state, "input").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(node_state(&state, "input").wait_reason, None);
    assert_eq!(
        state.terminal_status,
        Some(AgentGraphTerminalStatus::Cancelled)
    );
}

#[test]
fn scheduler_terminal_failure_stops_downstream_scheduling() {
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
    let state = scheduler
        .start_node(&plan, state, "left", ts(160))
        .expect("left should start")
        .state;
    let state = scheduler
        .fail_node(&plan, state, "left", "left-failed", ts(170))
        .expect("left failure should fail graph")
        .state;

    assert_eq!(
        node_state(&state, "left").status,
        AgentGraphNodeStatus::Failed
    );
    assert_eq!(
        node_state(&state, "right").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        node_state(&state, "join").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        node_state(&state, "terminal").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
    assert!(scheduler.runnable_nodes(&state).is_empty());
    assert!(scheduler
        .compute_ready_nodes(&plan, &state)
        .expect("terminal graph ready set should compute")
        .is_empty());
}

#[test]
fn scheduler_terminal_success_cancels_unresolved_parallel_work() {
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
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(200))
        .expect("join should become runnable")
        .state;
    let state = run_node(&scheduler, &plan, state, "join", ts(210));
    let state = scheduler
        .mark_ready_nodes_runnable(&plan, state, ts(250))
        .expect("terminal should become runnable")
        .state;
    let state = run_node(&scheduler, &plan, state, "terminal", ts(260));

    assert_eq!(
        node_state(&state, "terminal").status,
        AgentGraphNodeStatus::Terminal
    );
    assert_eq!(
        node_state(&state, "right").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        state.terminal_status,
        Some(AgentGraphTerminalStatus::Completed)
    );
    assert!(scheduler.runnable_nodes(&state).is_empty());
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

fn iterator_plan(max_iterations: u32) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "items"),
    );
    let iterator = AgentCompiledPlanNode::new("iterate", AgentCompiledNodeKind::Iterator)
        .input_port(AgentCompiledPlanPort::new(
            "items",
            AgentCompiledPortDirection::Input,
            "items",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "item",
            AgentCompiledPortDirection::Output,
            "item",
        ))
        .iterator_policy(AgentCompiledIteratorPolicy::new(max_iterations));
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("result", AgentCompiledPortDirection::Input, "item"),
        );

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new(format!("plan-iterator-{max_iterations}-v1")),
        AgentWorkflowId::new("workflow-iterator"),
        "iterator",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new(format!("sha256:iterator-{max_iterations}")),
    )
    .entry_node("input")
    .node(input)
    .node(iterator)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-iterator",
        "input",
        "payload",
        "iterate",
        "items",
    ))
    .edge(AgentCompiledPlanEdge::new(
        "edge-iterator-terminal",
        "iterate",
        "item",
        "terminal",
        "result",
    ))
}

fn start_iterator(
    scheduler: &AgentGraphScheduler,
    plan: &AgentCompiledExecutionPlan,
) -> AgentGraphRunState {
    let state = scheduler
        .initialize_state(plan, ts(100))
        .expect("graph state should initialize");
    let state = run_node(scheduler, plan, state, "input", ts(110));
    let state = scheduler
        .mark_ready_nodes_runnable(plan, state, ts(150))
        .expect("iterator should become runnable")
        .state;
    scheduler
        .start_node(plan, state, "iterate", ts(160))
        .expect("iterator should start")
        .state
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
