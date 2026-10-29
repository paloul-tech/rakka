//! Durable graph state contract tests.

use rakka_agent_workflow::{
    is_bounded_agent_metric_attribute, is_forbidden_agent_metric_attribute, validate_inline_state,
    AgentCommandKind, AgentCompiledEdgeId, AgentCompiledNodeId, AgentCompiledNodeKind,
    AgentCompiledPlanFingerprint, AgentCompiledPlanId, AgentCompiledPortId, AgentEffectId,
    AgentGraphBlockedReason, AgentGraphLoopInstanceState, AgentGraphNodeState,
    AgentGraphNodeStatus, AgentGraphReconciliationDecision, AgentGraphRunState,
    AgentGraphTerminalStatus, AgentGraphWaitReason, AgentTimestampMillis, ArtifactEncryptionRef,
    ArtifactKind, ArtifactRef, HumanCheckpointId, InlineState, RedactionStatus,
    CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION, DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES,
};
use serde_json::json;

#[test]
fn graph_run_state_round_trips() {
    let state = sample_graph_state();
    let json = serde_json::to_string(&state).expect("graph state should serialize");
    let decoded: AgentGraphRunState =
        serde_json::from_str(&json).expect("graph state should deserialize");

    assert_eq!(decoded, state);
    assert_eq!(
        decoded.graph_schema_version,
        CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION
    );
    assert_eq!(decoded.node_states.len(), 3);
    assert_eq!(decoded.scheduler_revision, 7);
    assert_eq!(decoded.last_event_sequence, 42);
}

#[test]
fn graph_node_statuses_have_stable_wire_names_and_labels() {
    let value =
        serde_json::to_value(AgentGraphNodeStatus::Runnable).expect("status should serialize");
    assert_eq!(value, json!("runnable"));
    assert_eq!(AgentGraphNodeStatus::Pending.as_label(), "pending");
    assert_eq!(AgentGraphNodeStatus::Running.as_label(), "running");
    assert_eq!(AgentGraphNodeStatus::Waiting.as_label(), "waiting");
    assert_eq!(AgentGraphNodeStatus::Completed.as_label(), "completed");
    assert_eq!(AgentGraphNodeStatus::Skipped.as_label(), "skipped");
    assert_eq!(AgentGraphNodeStatus::Failed.as_label(), "failed");
    assert_eq!(AgentGraphNodeStatus::Cancelled.as_label(), "cancelled");
    assert_eq!(AgentGraphNodeStatus::Terminal.as_label(), "terminal");
    assert_eq!(AgentGraphTerminalStatus::Completed.as_label(), "completed");
    assert_eq!(
        AgentGraphWaitReason::ChildWorkflow.as_label(),
        "child-workflow"
    );
    assert_eq!(
        serde_json::to_value(AgentGraphWaitReason::Reconciliation)
            .expect("wait reason should serialize"),
        json!("reconciliation")
    );
    assert_eq!(
        AgentGraphWaitReason::Reconciliation.as_label(),
        "reconciliation"
    );
}

#[test]
fn graph_state_uses_artifact_refs_for_large_values() {
    let state = sample_graph_state();
    let value = serde_json::to_value(&state).expect("graph state should serialize");

    assert_eq!(
        value["node_states"]["model"]["input_refs"]["prompt"]["artifact_id"],
        json!("artifact:prompt")
    );
    assert_eq!(
        value["output_refs"]["result"]["artifact_id"],
        json!("artifact:graph-output")
    );
    assert!(
        value.get("bytes").is_none(),
        "graph state should not expose inline payload bytes at the top level"
    );
}

#[test]
fn bounded_inline_state_policy_remains_enforced() {
    let oversized = InlineState {
        content_type: "application/json".to_string(),
        bytes: vec![b'x'; DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES as usize + 1],
        size_bytes: DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES + 1,
    };

    validate_inline_state(&oversized).expect_err("oversized inline state should be rejected");
}

#[test]
fn graph_state_ids_remain_out_of_hot_metric_labels() {
    assert!(is_forbidden_agent_metric_attribute("run_id"));
    assert!(is_forbidden_agent_metric_attribute("node_id"));
    assert!(is_forbidden_agent_metric_attribute("effect_id"));
    assert!(is_bounded_agent_metric_attribute("workflow_type"));
    assert!(is_bounded_agent_metric_attribute("status"));
}

fn sample_graph_state() -> AgentGraphRunState {
    let input_node = AgentGraphNodeState::new(
        AgentCompiledNodeId::new("input"),
        AgentCompiledNodeKind::Input,
        AgentTimestampMillis::new(100),
    )
    .status(AgentGraphNodeStatus::Completed)
    .dependencies_ready(true)
    .output_ref(
        AgentCompiledPortId::new("payload"),
        artifact("artifact:input", ArtifactKind::Input),
    );
    let model_node = AgentGraphNodeState::new(
        AgentCompiledNodeId::new("model"),
        AgentCompiledNodeKind::ModelCall,
        AgentTimestampMillis::new(110),
    )
    .status(AgentGraphNodeStatus::Waiting)
    .dependencies_ready(true)
    .input_ref(
        AgentCompiledPortId::new("prompt"),
        artifact("artifact:prompt", ArtifactKind::Prompt),
    )
    .scheduled_effect_id(AgentEffectId::new("effect-model"))
    .wait_reason(AgentGraphWaitReason::Effect);
    let terminal_node = AgentGraphNodeState::new(
        AgentCompiledNodeId::new("terminal"),
        AgentCompiledNodeKind::Terminal,
        AgentTimestampMillis::new(120),
    )
    .status(AgentGraphNodeStatus::Pending);

    AgentGraphRunState::new(
        AgentCompiledPlanId::new("plan-research-v1"),
        AgentCompiledPlanFingerprint::new("sha256:research-v1"),
    )
    .node_state(input_node)
    .node_state(model_node)
    .node_state(terminal_node)
    .selected_branch_path(
        AgentCompiledNodeId::new("branch"),
        vec![AgentCompiledEdgeId::new("edge-branch-yes")],
    )
    .loop_instance(
        AgentGraphLoopInstanceState::new(
            AgentCompiledNodeId::new("iterate"),
            0,
            AgentTimestampMillis::new(130),
        )
        .status(AgentGraphNodeStatus::Completed)
        .item_ref(artifact("artifact:item-0", ArtifactKind::Input))
        .output_ref(
            AgentCompiledPortId::new("item-output"),
            artifact("artifact:item-output-0", ArtifactKind::ToolOutput),
        ),
    )
    .output_ref(
        AgentCompiledPortId::new("result"),
        artifact("artifact:graph-output", ArtifactKind::Completion),
    )
    .blocked_reason(AgentGraphBlockedReason::new("waiting-effect").detail("model"))
    .terminal_status(AgentGraphTerminalStatus::Completed)
    .with_revision(7, 42)
    .with_human_checkpoint("model", HumanCheckpointId::new("checkpoint-model"))
}

fn artifact(artifact_id: &str, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind,
        uri: format!("object://bucket/{artifact_id}"),
        checksum: Some("sha256:abc".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("standard".to_string()),
        encryption: Some(
            ArtifactEncryptionRef::new("AES256-GCM", "kms://agent-workflow/test-key")
                .context("tenant", "tenant-a"),
        ),
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(123),
        metadata: Default::default(),
    }
}

trait GraphStateTestExt {
    fn with_revision(self, scheduler_revision: u64, last_event_sequence: u64) -> Self;

    fn with_human_checkpoint(
        self,
        node_id: &str,
        checkpoint_id: HumanCheckpointId,
    ) -> AgentGraphRunState;
}

impl GraphStateTestExt for AgentGraphRunState {
    fn with_revision(mut self, scheduler_revision: u64, last_event_sequence: u64) -> Self {
        self.scheduler_revision = scheduler_revision;
        self.last_event_sequence = last_event_sequence;
        self
    }

    fn with_human_checkpoint(
        mut self,
        node_id: &str,
        checkpoint_id: HumanCheckpointId,
    ) -> AgentGraphRunState {
        self.node_states
            .get_mut(&AgentCompiledNodeId::new(node_id))
            .expect("node should exist")
            .checkpoint_ids
            .push(checkpoint_id);
        self
    }
}

#[test]
fn reconciliation_decisions_have_stable_wire_names() {
    let not_executed = AgentGraphReconciliationDecision::NotExecuted;
    let abandon = AgentGraphReconciliationDecision::Abandon {
        error_code: "x".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&not_executed).expect("decision should serialize"),
        json!({"kind": "not-executed"})
    );
    assert_eq!(
        serde_json::to_value(&abandon).expect("decision should serialize"),
        json!({"kind": "abandon", "error_code": "x"})
    );
    assert_eq!(not_executed.as_label(), "not-executed");
    assert_eq!(abandon.as_label(), "abandon");

    let effect_id = AgentEffectId::new("graph-effect:x");
    let kind = AgentCommandKind::GraphReconciliationDecided {
        effect_id: effect_id.clone(),
        decision: abandon,
    };
    let value = serde_json::to_value(&kind).expect("command kind should serialize");
    assert_eq!(value["type"], json!("graph-reconciliation-decided"));
    assert_eq!(
        value["effect_id"],
        serde_json::to_value(&effect_id).expect("effect id should serialize")
    );
    assert_eq!(
        value["decision"],
        json!({"kind": "abandon", "error_code": "x"})
    );
    let decoded: AgentCommandKind =
        serde_json::from_value(value).expect("command kind should deserialize");
    assert_eq!(decoded, kind);
    assert_eq!(kind.type_name(), "GraphReconciliationDecided");
    assert_eq!(kind.message_type(), "agent.graph-reconciliation-decided");
}

#[test]
fn node_state_without_an_effect_generation_decodes_with_zero() {
    let node = AgentGraphNodeState::new(
        AgentCompiledNodeId::new("charge"),
        AgentCompiledNodeKind::ToolCall,
        AgentTimestampMillis::new(120),
    );
    assert_eq!(node.effect_generation, 0);
    let mut value = serde_json::to_value(&node).expect("node state should serialize");
    assert_eq!(value["effect_generation"], json!(0));
    value
        .as_object_mut()
        .expect("node state should be an object")
        .remove("effect_generation");

    let decoded: AgentGraphNodeState =
        serde_json::from_value(value).expect("a version-one node state should decode");
    assert_eq!(decoded.effect_generation, 0);
    assert_eq!(decoded, node);
}
