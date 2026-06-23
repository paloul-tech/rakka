//! Compiled execution plan contract tests.

use rakka_agent_workflow::{
    validate_compiled_execution_plan, AgentCompiledEdgeMergeBehavior, AgentCompiledExecutionPlan,
    AgentCompiledIteratorPolicy, AgentCompiledNodeKind, AgentCompiledNodeKindCatalog,
    AgentCompiledNodeTarget, AgentCompiledPlanCompatibility, AgentCompiledPlanEdge,
    AgentCompiledPlanFingerprint, AgentCompiledPlanId, AgentCompiledPlanNode,
    AgentCompiledPlanPort, AgentCompiledPlanSchemaVersion, AgentCompiledPortDirection,
    AgentCredentialBindingRef, AgentWorkflowId, WorkflowDefinitionVersion,
};

#[test]
fn compiled_execution_plan_round_trips() {
    let plan = sample_plan();
    let json = serde_json::to_string(&plan).expect("compiled plan should serialize");
    let decoded: AgentCompiledExecutionPlan =
        serde_json::from_str(&json).expect("compiled plan should deserialize");

    assert_eq!(decoded, plan);
    assert_eq!(decoded.nodes.len(), 4);
    assert_eq!(decoded.edges.len(), 3);
    assert_eq!(decoded.entry_node_ids[0].as_str(), "input");
    assert_eq!(
        decoded.compatibility.required_capabilities,
        vec!["compiled-graph-v1"]
    );
}

#[test]
fn compiled_execution_plan_validates() {
    validate_compiled_execution_plan(&sample_plan()).expect("sample plan should validate");
}

#[test]
fn compiled_node_kinds_use_stable_wire_names() {
    let node = AgentCompiledPlanNode::new("call-model", AgentCompiledNodeKind::ModelCall);
    let value = serde_json::to_value(&node).expect("node should serialize");

    assert_eq!(value["kind"], "model-call");
    assert_eq!(
        AgentCompiledNodeKind::HumanCheckpoint.as_label(),
        "human-checkpoint"
    );
    assert_eq!(AgentCompiledNodeKind::TimerWait.as_label(), "timer-wait");
}

#[test]
fn runtime_catalog_lists_product_neutral_node_kinds() {
    let catalog = AgentCompiledNodeKindCatalog::current();

    assert_eq!(catalog.node_kinds.len(), AgentCompiledNodeKind::all().len());
    assert!(
        catalog
            .descriptor(AgentCompiledNodeKind::ModelCall)
            .expect("model call descriptor should exist")
            .requires_target
    );
    assert!(
        catalog
            .descriptor(AgentCompiledNodeKind::ToolCall)
            .expect("tool call descriptor should exist")
            .supports_credential_binding
    );
    assert!(catalog.node_kinds.iter().all(|descriptor| {
        !descriptor.label.contains("slack") && !descriptor.label.contains("openai")
    }));
}

#[test]
fn credential_binding_ref_round_trips_without_secret_material() {
    let node = AgentCompiledPlanNode::new("send-slack-message", AgentCompiledNodeKind::ToolCall)
        .target(AgentCompiledNodeTarget::new(
            "tool",
            "slack.chat.postMessage",
        ))
        .credential_binding_ref(AgentCredentialBindingRef::new("cred_binding_123"));

    let json = serde_json::to_string(&node).expect("node should serialize");
    let decoded: AgentCompiledPlanNode =
        serde_json::from_str(&json).expect("node should deserialize");

    assert!(json.contains("cred_binding_123"));
    assert!(!json.contains("xoxb-"));
    assert_eq!(decoded, node);
}

#[test]
fn validation_rejects_duplicate_node_ids() {
    let mut plan = sample_plan();
    plan.nodes.push(plan.nodes[0].clone());

    assert_validation_code(plan, "duplicate_node_id");
}

#[test]
fn validation_rejects_missing_edge_nodes_and_ports() {
    let mut missing_node = sample_plan();
    missing_node.edges[0].target_node_id = "missing-node".into();
    assert_validation_code(missing_node, "unknown_edge_node");

    let mut missing_port = sample_plan();
    missing_port.edges[0].target_port_id = "missing-port".into();
    assert_validation_code(missing_port, "unknown_edge_port");
}

#[test]
fn validation_rejects_edge_direction_mismatch() {
    let mut plan = sample_plan();
    plan.edges[0].source_node_id = "call-model".into();
    plan.edges[0].source_port_id = "prompt".into();

    assert_validation_code(plan, "port_direction_mismatch");
}

#[test]
fn validation_rejects_forbidden_cycles() {
    let mut plan = sample_plan();
    plan.edges.push(AgentCompiledPlanEdge::new(
        "edge-cycle",
        "join-results",
        "summary",
        "call-model",
        "prompt",
    ));

    assert_validation_code(plan, "cycle_detected");
}

#[test]
fn validation_handles_deeply_nested_acyclic_plans() {
    // Cycle detection is iterative, so a long acyclic chain validates in
    // heap-bounded space rather than recursing once per node (which a deep
    // enough plan could overflow). This also exercises the iterative DFS push/pop
    // across many levels.
    const DEPTH: usize = 2_000;

    let mut plan = AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-deep-chain"),
        AgentWorkflowId::new("workflow-deep-chain"),
        "deep-chain",
        WorkflowDefinitionVersion::new("v1"),
        AgentCompiledPlanSchemaVersion::new(1),
        AgentCompiledPlanFingerprint::new("sha256:deep-chain"),
    )
    .entry_node("node-0")
    .node(
        AgentCompiledPlanNode::new("node-0", AgentCompiledNodeKind::Input).output_port(
            AgentCompiledPlanPort::new("out", AgentCompiledPortDirection::Output, "payload"),
        ),
    );

    for index in 1..DEPTH - 1 {
        plan = plan.node(
            AgentCompiledPlanNode::new(format!("node-{index}"), AgentCompiledNodeKind::Transform)
                .input_port(AgentCompiledPlanPort::new(
                    "in",
                    AgentCompiledPortDirection::Input,
                    "payload",
                ))
                .output_port(AgentCompiledPlanPort::new(
                    "out",
                    AgentCompiledPortDirection::Output,
                    "payload",
                )),
        );
    }

    plan = plan.node(
        AgentCompiledPlanNode::new(
            format!("node-{}", DEPTH - 1),
            AgentCompiledNodeKind::Terminal,
        )
        .input_port(AgentCompiledPlanPort::new(
            "in",
            AgentCompiledPortDirection::Input,
            "payload",
        )),
    );

    for index in 0..DEPTH - 1 {
        plan = plan.edge(AgentCompiledPlanEdge::new(
            format!("edge-{index}"),
            format!("node-{index}"),
            "out",
            format!("node-{}", index + 1),
            "in",
        ));
    }

    validate_compiled_execution_plan(&plan).expect("deep acyclic chain should validate");
}

#[test]
fn validation_rejects_invalid_iterator_bounds() {
    let mut plan = iterator_plan();
    assert_validation_code(plan.clone(), "invalid_iterator_policy");

    plan.nodes[1].iterator_policy = Some(AgentCompiledIteratorPolicy::new(0));
    assert_validation_code(plan, "invalid_iterator_policy");
}

#[test]
fn validation_rejects_unreachable_terminal() {
    let mut plan = sample_plan();
    plan.edges.pop();

    assert_validation_code(plan, "missing_reachable_terminal");
}

#[test]
fn validation_rejects_invalid_branch_and_join_declarations() {
    assert_validation_code(
        branch_with_one_connected_path(),
        "invalid_branch_declaration",
    );

    let mut join_plan = sample_plan();
    join_plan.edges[1].merge_behavior = None;
    assert_validation_code(join_plan, "invalid_join_declaration");
}

#[test]
fn validation_rejects_secret_like_fields_and_hot_credential_labels() {
    let mut secret_label = sample_plan();
    secret_label
        .observability_labels
        .insert("api_key".to_string(), "sk-not-a-real-key".to_string());
    assert_validation_code(secret_label, "unsafe_attribute");

    let mut credential_label = sample_plan();
    credential_label.nodes[1]
        .observability_labels
        .insert("detail".to_string(), "cred_binding_openai".to_string());
    assert_validation_code(credential_label, "unsafe_attribute");
}

#[test]
fn validation_accepts_logical_credential_binding_refs() {
    let mut plan = sample_plan();
    plan.nodes[1].credential_binding_ref = Some(AgentCredentialBindingRef::new("binding/openai"));

    validate_compiled_execution_plan(&plan)
        .expect("logical credential binding ref should validate");
}

fn sample_plan() -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Output,
            "research.input",
        ),
    );
    let model = AgentCompiledPlanNode::new("call-model", AgentCompiledNodeKind::ModelCall)
        .input_port(AgentCompiledPlanPort::new(
            "prompt",
            AgentCompiledPortDirection::Input,
            "research.prompt",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "completion",
            AgentCompiledPortDirection::Output,
            "research.completion",
        ))
        .target(
            AgentCompiledNodeTarget::new("model", "openai.responses")
                .attribute("target_class", "model"),
        )
        .credential_binding_ref(AgentCredentialBindingRef::new("cred_binding_openai"))
        .observability_label("node_kind", "model-call");
    let join = AgentCompiledPlanNode::new("join-results", AgentCompiledNodeKind::Join)
        .input_port(AgentCompiledPlanPort::new(
            "completion",
            AgentCompiledPortDirection::Input,
            "research.completion",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "summary",
            AgentCompiledPortDirection::Output,
            "research.summary",
        ));
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Input,
            "research.summary",
        ));

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-research-v1"),
        AgentWorkflowId::new("workflow-research"),
        "research",
        WorkflowDefinitionVersion::new("v1"),
        AgentCompiledPlanSchemaVersion::new(1),
        AgentCompiledPlanFingerprint::new("sha256:compiled-plan"),
    )
    .entry_node("input")
    .node(input)
    .node(model)
    .node(join)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-model",
        "input",
        "payload",
        "call-model",
        "prompt",
    ))
    .edge(
        AgentCompiledPlanEdge::new(
            "edge-model-join",
            "call-model",
            "completion",
            "join-results",
            "completion",
        )
        .merge_behavior(AgentCompiledEdgeMergeBehavior::WaitForAll),
    )
    .edge(AgentCompiledPlanEdge::new(
        "edge-join-terminal",
        "join-results",
        "summary",
        "terminal",
        "result",
    ))
    .observability_label("workflow_type", "research")
    .with_compatibility(
        AgentCompiledPlanCompatibility::new()
            .min_runtime_version("0.1.0")
            .required_capability("compiled-graph-v1"),
    )
}

fn iterator_plan() -> AgentCompiledExecutionPlan {
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
        ));
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("result", AgentCompiledPortDirection::Input, "item"),
        );

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-iterator-v1"),
        AgentWorkflowId::new("workflow-iterator"),
        "iterator",
        WorkflowDefinitionVersion::new("v1"),
        AgentCompiledPlanSchemaVersion::new(1),
        AgentCompiledPlanFingerprint::new("sha256:iterator-plan"),
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
    .with_compatibility(
        AgentCompiledPlanCompatibility::new()
            .min_runtime_version("0.1.0")
            .required_capability("compiled-graph-v1"),
    )
}

fn branch_with_one_connected_path() -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "payload"),
    );
    let branch = AgentCompiledPlanNode::new("branch", AgentCompiledNodeKind::Branch)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "payload",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "yes",
            AgentCompiledPortDirection::Output,
            "payload",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "no",
            AgentCompiledPortDirection::Output,
            "payload",
        ));
    let terminal =
        AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal).input_port(
            AgentCompiledPlanPort::new("result", AgentCompiledPortDirection::Input, "payload"),
        );

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new("plan-branch-v1"),
        AgentWorkflowId::new("workflow-branch"),
        "branch",
        WorkflowDefinitionVersion::new("v1"),
        AgentCompiledPlanSchemaVersion::new(1),
        AgentCompiledPlanFingerprint::new("sha256:branch-plan"),
    )
    .entry_node("input")
    .node(input)
    .node(branch)
    .node(terminal)
    .edge(AgentCompiledPlanEdge::new(
        "edge-input-branch",
        "input",
        "payload",
        "branch",
        "payload",
    ))
    .edge(AgentCompiledPlanEdge::new(
        "edge-branch-terminal",
        "branch",
        "yes",
        "terminal",
        "result",
    ))
    .with_compatibility(
        AgentCompiledPlanCompatibility::new()
            .min_runtime_version("0.1.0")
            .required_capability("compiled-graph-v1"),
    )
}

fn assert_validation_code(plan: AgentCompiledExecutionPlan, expected_code: &str) {
    let error = validate_compiled_execution_plan(&plan).expect_err("plan should be invalid");
    assert_eq!(error.code(), expected_code, "{error}");
}

trait PlanTestExt {
    fn with_compatibility(self, compatibility: AgentCompiledPlanCompatibility) -> Self;
}

impl PlanTestExt for AgentCompiledExecutionPlan {
    fn with_compatibility(mut self, compatibility: AgentCompiledPlanCompatibility) -> Self {
        self.compatibility = compatibility;
        self
    }
}
