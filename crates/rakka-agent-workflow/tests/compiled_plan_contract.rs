//! Compiled execution plan contract tests.

use rakka_agent_workflow::{
    AgentCompiledEdgeMergeBehavior, AgentCompiledExecutionPlan, AgentCompiledNodeKind,
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

trait PlanTestExt {
    fn with_compatibility(self, compatibility: AgentCompiledPlanCompatibility) -> Self;
}

impl PlanTestExt for AgentCompiledExecutionPlan {
    fn with_compatibility(mut self, compatibility: AgentCompiledPlanCompatibility) -> Self {
        self.compatibility = compatibility;
        self
    }
}
