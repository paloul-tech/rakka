//! Compiled graph effect bridge tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    AgentCausationId, AgentCompiledExecutionPlan, AgentCompiledNodeKind, AgentCompiledNodeTarget,
    AgentCompiledPlanEdge, AgentCompiledPlanFingerprint, AgentCompiledPlanId,
    AgentCompiledPlanNode, AgentCompiledPlanPort, AgentCompiledPortDirection, AgentCorrelationId,
    AgentCredentialBindingRef, AgentEffectKind, AgentGraphEffectBridge,
    AgentGraphEffectScheduleRequest, AgentGraphNodeState, AgentGraphNodeStatus, AgentGraphRunState,
    AgentGraphScheduler, AgentGraphWaitReason, AgentRunId, AgentRunInbox, AgentTimestampMillis,
    AgentWorkflowId, ArtifactEncryptionRef, ArtifactKind, ArtifactRef, RedactionStatus,
    WorkflowDefinitionVersion, CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type TestStore = InMemoryDurableStateStore<WorkflowState>;
type TestInbox = AgentRunInbox<TestStore, ManualWorkflowClock>;

#[tokio::test]
async fn model_and_tool_nodes_schedule_durable_effects() {
    let bridge = AgentGraphEffectBridge::new();

    let model = schedule_kind(
        &bridge,
        AgentCompiledNodeKind::ModelCall,
        "model",
        "openai.responses",
        "llm",
        "run-graph-model-effect",
    )
    .await;
    assert_eq!(model.effect.kind, AgentEffectKind::ModelCall);
    assert_eq!(
        model.effect.payload_ref,
        Some(artifact("artifact:effect-payload", ArtifactKind::Prompt))
    );
    assert_eq!(model.effect.result_ref, None);
    assert_eq!(
        model
            .effect
            .target
            .attributes
            .get("credential_binding_ref")
            .map(String::as_str),
        Some("credential:effect-node")
    );

    let tool = schedule_kind(
        &bridge,
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "slack.chat.postMessage",
        "messaging",
        "run-graph-tool-effect",
    )
    .await;
    assert_eq!(tool.effect.kind, AgentEffectKind::ToolCall);
    assert!(tool.acceptance.is_scheduled());
    assert_eq!(
        node_state(&tool.transition.state, "effect").status,
        AgentGraphNodeStatus::Waiting
    );
    assert_eq!(
        node_state(&tool.transition.state, "effect").wait_reason,
        Some(AgentGraphWaitReason::Effect)
    );
    assert_eq!(
        node_state(&tool.transition.state, "effect").scheduled_effect_ids,
        vec![tool.effect.effect_id.clone()]
    );
}

#[tokio::test]
async fn duplicate_effect_node_scheduling_returns_existing_outbox_entry() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "research.search",
        "research",
    );
    let running_state = running_effect_state(&plan);
    let run_id = AgentRunId::new("run-graph-effect-duplicate");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");

    let request = effect_request(run_id, "effect", 200);
    let first = bridge
        .schedule_node_effect(&plan, running_state.clone(), request.clone(), &mut inbox)
        .await
        .expect("first scheduling should persist");
    assert!(first.acceptance.is_scheduled());

    let duplicate = bridge
        .schedule_node_effect(&plan, running_state, request, &mut inbox)
        .await
        .expect("duplicate scheduling should recover existing durable work");
    assert!(duplicate.acceptance.is_duplicate());
    assert_eq!(
        duplicate.acceptance.entry().message_id(),
        first.acceptance.entry().message_id()
    );
    assert_eq!(duplicate.effect.effect_id, first.effect.effect_id);
    assert_eq!(
        duplicate.effect.deduplication_key,
        first.effect.deduplication_key
    );
}

#[tokio::test]
async fn crash_after_effect_scheduling_recovers_due_effect() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ModelCall,
        "model",
        "anthropic.messages",
        "llm",
    );
    let running_state = running_effect_state(&plan);
    let run_id = AgentRunId::new("run-graph-effect-recovery");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");

    assert!(inbox
        .due_effects()
        .expect("empty outbox should be queryable")
        .is_empty());

    let outcome = bridge
        .schedule_node_effect(
            &plan,
            running_state,
            effect_request(run_id.clone(), "effect", 200),
            &mut inbox,
        )
        .await
        .expect("effect should schedule");
    assert!(outcome.acceptance.is_scheduled());

    let mut recovered = agent_inbox(run_id, store, clock);
    recovered
        .recover()
        .await
        .expect("fresh inbox should recover persisted outbox");
    let due = recovered
        .due_effects()
        .expect("recovered due effects should be queryable");

    assert_eq!(due.len(), 1);
    assert_eq!(due[0].effect, outcome.effect);
}

#[tokio::test]
async fn idempotency_key_is_stable_across_recovery_duplicate() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "github.issues.create",
        "issue-tracker",
    );
    let running_state = running_effect_state(&plan);
    let run_id = AgentRunId::new("run-graph-effect-idempotency");
    let request = effect_request(run_id.clone(), "effect", 200).loop_instance_id("iterate:0");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");

    let first = bridge
        .schedule_node_effect(&plan, running_state.clone(), request.clone(), &mut inbox)
        .await
        .expect("first scheduling should persist");

    let mut recovered = agent_inbox(run_id, store, clock);
    recovered
        .recover()
        .await
        .expect("fresh inbox should recover persisted outbox");
    let duplicate = bridge
        .schedule_node_effect(&plan, running_state, request, &mut recovered)
        .await
        .expect("recovered duplicate should return existing outbox entry");

    assert!(duplicate.acceptance.is_duplicate());
    assert_eq!(duplicate.effect.effect_id, first.effect.effect_id);
    assert_eq!(
        duplicate.effect.idempotency_key,
        first.effect.idempotency_key
    );
    assert!(duplicate
        .effect
        .idempotency_key
        .as_str()
        .contains("loop=iterate:0"));
}

async fn schedule_kind(
    bridge: &AgentGraphEffectBridge,
    kind: AgentCompiledNodeKind,
    target_type: &str,
    target_name: &str,
    target_class: &str,
    run_id: &str,
) -> rakka_agent_workflow::AgentGraphEffectScheduleOutcome {
    let plan = effect_plan(kind, target_type, target_name, target_class);
    let state = running_effect_state(&plan);
    let run_id = AgentRunId::new(run_id);
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");

    bridge
        .schedule_node_effect(
            &plan,
            state,
            effect_request(run_id, "effect", 200),
            &mut inbox,
        )
        .await
        .expect("effect should schedule")
}

fn running_effect_state(plan: &AgentCompiledExecutionPlan) -> AgentGraphRunState {
    let scheduler = AgentGraphScheduler::new();
    let state = scheduler
        .initialize_state(plan, ts(100))
        .expect("graph state should initialize");
    let state = scheduler
        .mark_ready_nodes_runnable(plan, state, ts(110))
        .expect("input should become runnable")
        .state;
    let state = scheduler
        .start_node(plan, state, "input", ts(120))
        .expect("input should start")
        .state;
    let state = scheduler
        .complete_node(plan, state, "input", ts(130))
        .expect("input should complete")
        .state;
    let state = scheduler
        .mark_ready_nodes_runnable(plan, state, ts(140))
        .expect("effect should become runnable")
        .state;
    scheduler
        .start_node(plan, state, "effect", ts(150))
        .expect("effect should start")
        .state
}

fn effect_plan(
    kind: AgentCompiledNodeKind,
    target_type: &str,
    target_name: &str,
    target_class: &str,
) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    );
    let effect = AgentCompiledPlanNode::new("effect", kind)
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
            AgentCompiledNodeTarget::new(target_type, target_name)
                .address(format!("{target_type}://{target_name}"))
                .attribute("target_class", target_class),
        )
        .credential_binding_ref(AgentCredentialBindingRef::new("credential:effect-node"));
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Input,
            "effect-result",
        ));

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new(format!("plan-effect-{}-v1", kind.as_label())),
        AgentWorkflowId::new("workflow-effect"),
        "effect-graph",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new(format!(
            "sha256:effect-{}-{target_type}-{target_name}",
            kind.as_label()
        )),
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
    node_id: &str,
    created_at_millis: u64,
) -> AgentGraphEffectScheduleRequest {
    AgentGraphEffectScheduleRequest::new(
        run_id,
        node_id,
        AgentTimestampMillis::new(created_at_millis),
        AgentCausationId::new("cause:start-run"),
        AgentCorrelationId::new("correlation:workflow"),
    )
    .payload_ref(artifact("artifact:effect-payload", ArtifactKind::Prompt))
    .timeout_ms(2_000)
    .expected_result_type("effect.result")
}

fn agent_inbox(run_id: AgentRunId, store: TestStore, clock: ManualWorkflowClock) -> TestInbox {
    AgentRunInbox::with_clock(run_id, store, clock)
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
        metadata: BTreeMap::new(),
    }
}

fn node_state<'a>(state: &'a AgentGraphRunState, node_id: &str) -> &'a AgentGraphNodeState {
    state
        .node_states
        .get(&rakka_agent_workflow::AgentCompiledNodeId::new(node_id))
        .expect("node state should exist")
}

const fn ts(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}
