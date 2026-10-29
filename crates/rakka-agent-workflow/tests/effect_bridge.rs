//! Compiled graph effect bridge tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandMetadata,
    AgentCompiledExecutionPlan, AgentCompiledNodeId, AgentCompiledNodeKind,
    AgentCompiledNodeTarget, AgentCompiledPlanEdge, AgentCompiledPlanFingerprint,
    AgentCompiledPlanId, AgentCompiledPlanNode, AgentCompiledPlanPort, AgentCompiledPortDirection,
    AgentCompiledPortId, AgentCorrelationId, AgentCredentialBindingRef, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentEffectId, AgentEffectKind, AgentEffectTarget,
    AgentGraphEffectBridge, AgentGraphEffectBridgeError, AgentGraphEffectFailureDisposition,
    AgentGraphEffectScheduleRequest, AgentGraphHumanCheckpointScheduleRequest, AgentGraphNodeState,
    AgentGraphNodeStatus, AgentGraphReconciliationDecision, AgentGraphRunState,
    AgentGraphScheduler, AgentGraphSchedulerError, AgentGraphStateSchemaVersion,
    AgentGraphTerminalStatus, AgentGraphTimerScheduleRequest, AgentGraphWaitReason, AgentRunId,
    AgentRunInbox, AgentTenantId, AgentTimerStore, AgentTimerStoreState, AgentTimestampMillis,
    AgentWorkflowId, ArtifactEncryptionRef, ArtifactKind, ArtifactRef, HumanCheckpoint,
    HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, PrincipalRef, RedactionStatus,
    WorkflowDefinitionVersion, CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
    CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION,
};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    ManualWorkflowClock, OutboxMessageId, OutboxStatus, WorkflowState, WorkflowTimestamp,
};
use serde_json::json;

type TestStore = InMemoryDurableStateStore<WorkflowState>;
type TestInbox = AgentRunInbox<TestStore, ManualWorkflowClock>;
type TestTimerStore = InMemoryDurableStateStore<AgentTimerStoreState>;

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

#[tokio::test]
async fn duplicate_completion_callback_does_not_advance_graph_twice() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "slack.chat.postMessage",
        "messaging",
    );
    let run_id = AgentRunId::new("run-graph-effect-complete-duplicate");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");

    let scheduled = bridge
        .schedule_node_effect(
            &plan,
            running_effect_state(&plan),
            effect_request(run_id.clone(), "effect", 200),
            &mut inbox,
        )
        .await
        .expect("effect should schedule");
    let command = bridge
        .effect_completed_command(
            command_metadata(&plan, run_id, "cmd-effect-completed-1", 300),
            scheduled.effect.effect_id.clone(),
            Some(artifact("artifact:effect-result", ArtifactKind::ToolOutput)),
        )
        .expect("completion command should build");

    let first = bridge
        .accept_and_apply_effect_completed(
            &plan,
            scheduled.transition.state,
            command.clone(),
            &mut inbox,
        )
        .await
        .expect("completion should apply");
    assert!(first.acceptance.is_accepted());
    assert_eq!(
        node_state(&first.transition.state, "effect").status,
        AgentGraphNodeStatus::Completed
    );
    assert_eq!(
        node_state(&first.transition.state, "effect")
            .output_refs
            .get(&AgentCompiledPortId::new("result")),
        Some(&artifact(
            "artifact:effect-result",
            ArtifactKind::ToolOutput
        ))
    );
    let completed_revision = first.transition.state.scheduler_revision;

    let duplicate = bridge
        .accept_and_apply_effect_completed(&plan, first.transition.state, command, &mut inbox)
        .await
        .expect("duplicate completion should be idempotent");
    assert!(duplicate.acceptance.is_duplicate());
    assert!(duplicate.transition.changed_node_ids.is_empty());
    assert_eq!(
        duplicate.transition.state.scheduler_revision,
        completed_revision
    );
}

#[tokio::test]
async fn retryable_failure_keeps_effect_node_waiting() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "github.issues.create",
        "issue-tracker",
    );
    let run_id = AgentRunId::new("run-graph-effect-retryable-failure");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");
    let scheduled = bridge
        .schedule_node_effect(
            &plan,
            running_effect_state(&plan),
            effect_request(run_id.clone(), "effect", 200),
            &mut inbox,
        )
        .await
        .expect("effect should schedule");
    let command = bridge
        .effect_failed_command(
            command_metadata(&plan, run_id, "cmd-effect-failed-retry", 300),
            scheduled.effect.effect_id,
            "rate-limited",
            AgentGraphEffectFailureDisposition::RetryScheduled,
        )
        .expect("failure command should build");

    let outcome = bridge
        .accept_and_apply_effect_failed(
            &plan,
            scheduled.transition.state,
            command.clone(),
            &mut inbox,
        )
        .await
        .expect("retryable failure should apply");
    let effect_node = node_state(&outcome.transition.state, "effect");
    assert_eq!(effect_node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(effect_node.wait_reason, Some(AgentGraphWaitReason::Effect));
    assert_eq!(effect_node.error_code.as_deref(), Some("rate-limited"));
    assert_eq!(outcome.transition.state.terminal_status, None);

    let retry_revision = outcome.transition.state.scheduler_revision;
    let duplicate = bridge
        .accept_and_apply_effect_failed(&plan, outcome.transition.state, command, &mut inbox)
        .await
        .expect("duplicate retryable failure should be idempotent");
    assert!(duplicate.acceptance.is_duplicate());
    assert!(duplicate.transition.changed_node_ids.is_empty());
    assert_eq!(
        duplicate.transition.state.scheduler_revision,
        retry_revision
    );
}

#[tokio::test]
async fn exhausted_failure_marks_node_and_graph_failed() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "payments.charge",
        "payments",
    );
    let run_id = AgentRunId::new("run-graph-effect-exhausted");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");
    let scheduled = bridge
        .schedule_node_effect(
            &plan,
            running_effect_state(&plan),
            effect_request(run_id.clone(), "effect", 200),
            &mut inbox,
        )
        .await
        .expect("effect should schedule");
    let command = bridge
        .effect_failed_command(
            command_metadata(&plan, run_id, "cmd-effect-failed-exhausted", 300),
            scheduled.effect.effect_id,
            "retry-budget-exhausted",
            AgentGraphEffectFailureDisposition::Exhausted,
        )
        .expect("failure command should build");

    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, scheduled.transition.state, command, &mut inbox)
        .await
        .expect("exhausted failure should fail graph");

    assert_eq!(
        node_state(&outcome.transition.state, "effect").status,
        AgentGraphNodeStatus::Failed
    );
    assert_eq!(
        node_state(&outcome.transition.state, "effect")
            .error_code
            .as_deref(),
        Some("retry-budget-exhausted")
    );
    assert_eq!(
        node_state(&outcome.transition.state, "terminal").status,
        AgentGraphNodeStatus::Cancelled
    );
    assert_eq!(
        outcome.transition.state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
}

#[tokio::test]
async fn crash_after_completion_command_acceptance_recovers_graph_transition() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ModelCall,
        "model",
        "openai.responses",
        "llm",
    );
    let run_id = AgentRunId::new("run-graph-effect-completion-recovery");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");
    let scheduled = bridge
        .schedule_node_effect(
            &plan,
            running_effect_state(&plan),
            effect_request(run_id.clone(), "effect", 200),
            &mut inbox,
        )
        .await
        .expect("effect should schedule");
    let waiting_state = scheduled.transition.state;
    let command = bridge
        .effect_completed_command(
            command_metadata(&plan, run_id.clone(), "cmd-effect-completed-recover", 300),
            scheduled.effect.effect_id,
            Some(artifact(
                "artifact:model-completion",
                ArtifactKind::Completion,
            )),
        )
        .expect("completion command should build");

    let accepted = inbox
        .accept_command(command.clone())
        .await
        .expect("completion command should persist");
    assert!(accepted.is_accepted());

    let mut recovered = agent_inbox(run_id, store, clock);
    recovered
        .recover()
        .await
        .expect("fresh inbox should recover accepted command");
    let outcome = bridge
        .accept_and_apply_effect_completed(&plan, waiting_state, command, &mut recovered)
        .await
        .expect("duplicate accepted command should still apply graph transition");

    assert!(outcome.acceptance.is_duplicate());
    assert_eq!(
        node_state(&outcome.transition.state, "effect").status,
        AgentGraphNodeStatus::Completed
    );
    assert_eq!(
        node_state(&outcome.transition.state, "effect")
            .output_refs
            .get(&AgentCompiledPortId::new("result")),
        Some(&artifact(
            "artifact:model-completion",
            ArtifactKind::Completion
        ))
    );
}

#[tokio::test]
async fn timer_node_resumes_once_after_timer_command_redelivery() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = wait_plan(AgentCompiledNodeKind::TimerWait);
    let run_id = AgentRunId::new("run-graph-timer-redelivery");
    let timer_store = TestTimerStore::new();
    let mut timers = AgentTimerStore::new(timer_store.clone());
    timers
        .recover(ts(100))
        .await
        .expect("timers should recover");

    let scheduled = bridge
        .schedule_node_timer(
            &plan,
            running_effect_state(&plan),
            timer_request(run_id.clone(), "effect", 500, 200),
            &mut timers,
        )
        .await
        .expect("timer node should schedule");
    assert!(!scheduled.duplicate);
    assert_eq!(
        node_state(&scheduled.transition.state, "effect").status,
        AgentGraphNodeStatus::Waiting
    );
    assert_eq!(
        node_state(&scheduled.transition.state, "effect").wait_reason,
        Some(AgentGraphWaitReason::Timer)
    );
    assert_eq!(
        node_state(&scheduled.transition.state, "effect").timer_ids,
        vec![scheduled.timer.timer_id.clone()]
    );

    let mut recovered_timers = AgentTimerStore::new(timer_store);
    recovered_timers
        .recover(ts(500))
        .await
        .expect("recovered timers should load");
    let duplicate_schedule = bridge
        .schedule_node_timer(
            &plan,
            scheduled.transition.state.clone(),
            timer_request(run_id.clone(), "effect", 500, 200),
            &mut recovered_timers,
        )
        .await
        .expect("recovered timer scheduling should be idempotent");
    assert!(duplicate_schedule.duplicate);
    assert_eq!(duplicate_schedule.timer.timer_id, scheduled.timer.timer_id);

    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(500));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");
    let command = bridge
        .timer_fired_command(
            command_metadata(&plan, run_id, "cmd-timer-fired", 500),
            scheduled.timer.timer_id,
        )
        .expect("timer command should build");

    let first = bridge
        .accept_and_apply_timer_fired(
            &plan,
            scheduled.transition.state,
            command.clone(),
            &mut inbox,
        )
        .await
        .expect("timer fired should complete node");
    assert!(first.acceptance.is_accepted());
    assert_eq!(
        node_state(&first.transition.state, "effect").status,
        AgentGraphNodeStatus::Completed
    );

    let completed_revision = first.transition.state.scheduler_revision;
    let duplicate = bridge
        .accept_and_apply_timer_fired(&plan, first.transition.state, command, &mut inbox)
        .await
        .expect("duplicate timer fired command should be idempotent");
    assert!(duplicate.acceptance.is_duplicate());
    assert!(duplicate.transition.changed_node_ids.is_empty());
    assert_eq!(
        duplicate.transition.state.scheduler_revision,
        completed_revision
    );
}

#[tokio::test]
async fn human_decision_resumes_waiting_graph_node() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = wait_plan(AgentCompiledNodeKind::HumanCheckpoint);
    let run_id = AgentRunId::new("run-graph-human-decision");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");

    let checkpoint = checkpoint("checkpoint-graph-review", 600);
    let opened = bridge
        .open_node_human_checkpoint(
            &plan,
            running_effect_state(&plan),
            human_checkpoint_request(run_id.clone(), "effect", checkpoint.clone(), 200),
            &mut inbox,
        )
        .await
        .expect("human checkpoint should open");

    assert!(opened.acceptance.is_scheduled());
    assert_eq!(opened.checkpoint, checkpoint);
    assert_eq!(
        opened.approval_effect.kind,
        AgentEffectKind::HumanApprovalRequest
    );
    assert_eq!(
        node_state(&opened.transition.state, "effect").wait_reason,
        Some(AgentGraphWaitReason::Human)
    );
    assert_eq!(
        node_state(&opened.transition.state, "effect").checkpoint_ids,
        vec![HumanCheckpointId::new("checkpoint-graph-review")]
    );

    let command = bridge
        .human_decision_submitted_command(
            command_metadata(&plan, run_id, "cmd-human-decision", 700).principal(PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "reviewer-1".to_string(),
                display_name: Some("Reviewer One".to_string()),
            }),
            HumanCheckpointId::new("checkpoint-graph-review"),
            "approve",
            HumanCheckpointStatus::Approved,
            Some(artifact("artifact:human-decision", ArtifactKind::State)),
        )
        .expect("human decision command should build");

    let outcome = bridge
        .accept_and_apply_human_decision(&plan, opened.transition.state, command, &mut inbox)
        .await
        .expect("human decision should complete node");
    assert!(outcome.acceptance.is_accepted());
    assert_eq!(
        node_state(&outcome.transition.state, "effect").status,
        AgentGraphNodeStatus::Completed
    );
    assert_eq!(
        node_state(&outcome.transition.state, "effect")
            .output_refs
            .get(&AgentCompiledPortId::new("result")),
        Some(&artifact("artifact:human-decision", ArtifactKind::State))
    );
}

#[tokio::test]
async fn child_workflow_command_uses_stable_deduplication_metadata() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ChildWorkflowCommand,
        "workflow",
        "invoice.approval",
        "child-workflow",
    );
    let running_state = running_effect_state(&plan);
    let run_id = AgentRunId::new("run-graph-child-workflow");
    let request = effect_request(run_id.clone(), "effect", 200);
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");

    let first = bridge
        .schedule_node_effect(&plan, running_state.clone(), request.clone(), &mut inbox)
        .await
        .expect("child workflow command should schedule");
    assert!(first.acceptance.is_scheduled());
    assert_eq!(first.effect.kind, AgentEffectKind::ChildWorkflowCommand);
    assert_eq!(
        node_state(&first.transition.state, "effect").wait_reason,
        Some(AgentGraphWaitReason::ChildWorkflow)
    );

    let duplicate = bridge
        .schedule_node_effect(&plan, running_state, request, &mut inbox)
        .await
        .expect("child workflow command scheduling should be idempotent");
    assert!(duplicate.acceptance.is_duplicate());
    assert_eq!(duplicate.effect.effect_id, first.effect.effect_id);
    assert_eq!(
        duplicate.effect.deduplication_key,
        first.effect.deduplication_key
    );
    assert_eq!(
        duplicate.effect.idempotency_key,
        first.effect.idempotency_key
    );

    let command = bridge
        .effect_completed_command(
            command_metadata(&plan, run_id, "cmd-child-completed", 300),
            first.effect.effect_id,
            Some(artifact("artifact:child-result", ArtifactKind::State)),
        )
        .expect("child completion command should build");
    let completed = bridge
        .accept_and_apply_effect_completed(&plan, first.transition.state, command, &mut inbox)
        .await
        .expect("child workflow completion should apply");
    assert_eq!(
        node_state(&completed.transition.state, "effect").status,
        AgentGraphNodeStatus::Completed
    );
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

fn wait_plan(kind: AgentCompiledNodeKind) -> AgentCompiledExecutionPlan {
    let input = AgentCompiledPlanNode::new("input", AgentCompiledNodeKind::Input).output_port(
        AgentCompiledPlanPort::new("payload", AgentCompiledPortDirection::Output, "input"),
    );
    let wait = AgentCompiledPlanNode::new("effect", kind)
        .input_port(AgentCompiledPlanPort::new(
            "payload",
            AgentCompiledPortDirection::Input,
            "input",
        ))
        .output_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Output,
            "effect-result",
        ));
    let terminal = AgentCompiledPlanNode::new("terminal", AgentCompiledNodeKind::Terminal)
        .input_port(AgentCompiledPlanPort::new(
            "result",
            AgentCompiledPortDirection::Input,
            "effect-result",
        ));

    AgentCompiledExecutionPlan::new(
        AgentCompiledPlanId::new(format!("plan-wait-{}-v1", kind.as_label())),
        AgentWorkflowId::new("workflow-effect"),
        "wait-graph",
        WorkflowDefinitionVersion::new("v1"),
        CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION,
        AgentCompiledPlanFingerprint::new(format!("sha256:wait-{}", kind.as_label())),
    )
    .entry_node("input")
    .node(input)
    .node(wait)
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

fn timer_request(
    run_id: AgentRunId,
    node_id: &str,
    due_at_millis: u64,
    created_at_millis: u64,
) -> AgentGraphTimerScheduleRequest {
    AgentGraphTimerScheduleRequest::new(
        run_id,
        AgentTenantId::new("tenant-a"),
        node_id,
        AgentTimestampMillis::new(due_at_millis),
        AgentTimestampMillis::new(created_at_millis),
        AgentCausationId::new("cause:start-run"),
        AgentCorrelationId::new("correlation:workflow"),
    )
}

fn human_checkpoint_request(
    run_id: AgentRunId,
    node_id: &str,
    checkpoint: HumanCheckpoint,
    created_at_millis: u64,
) -> AgentGraphHumanCheckpointScheduleRequest {
    AgentGraphHumanCheckpointScheduleRequest::new(
        run_id,
        node_id,
        checkpoint,
        AgentEffectTarget {
            target_type: "human".to_string(),
            name: "approval-ui".to_string(),
            address: Some("https://approvals.local/queue".to_string()),
            attributes: BTreeMap::from([(
                "target_class".to_string(),
                "human-approval".to_string(),
            )]),
        },
        AgentTimestampMillis::new(created_at_millis),
        AgentCausationId::new("cause:start-run"),
        AgentCorrelationId::new("correlation:workflow"),
    )
    .payload_ref(artifact(
        "artifact:human-approval-payload",
        ArtifactKind::State,
    ))
    .timeout_ms(3_000)
}

fn checkpoint(checkpoint_id: &str, due_at_millis: u64) -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new(checkpoint_id),
        status: HumanCheckpointStatus::Open,
        summary: "Review generated workflow action".to_string(),
        available_decisions: vec![
            HumanDecisionOption {
                value: "approve".to_string(),
                label: "Approve".to_string(),
                requires_comment: false,
            },
            HumanDecisionOption {
                value: "reject".to_string(),
                label: "Reject".to_string(),
                requires_comment: false,
            },
        ],
        required_roles: vec!["reviewer".to_string()],
        due_at: Some(AgentTimestampMillis::new(due_at_millis)),
        escalation_target: Some("workflow-ops".to_string()),
        context_artifacts: Vec::new(),
        created_by: Some(PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "workflow-runtime".to_string(),
            display_name: Some("Workflow Runtime".to_string()),
        }),
        resolved_by: None,
        created_at: AgentTimestampMillis::new(123),
        resolved_at: None,
        audit_event_ids: Vec::new(),
    }
}

fn command_metadata(
    plan: &AgentCompiledExecutionPlan,
    run_id: AgentRunId,
    command_id: &str,
    received_at_millis: u64,
) -> AgentCommandMetadata {
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("dedupe-{command_id}")),
        AgentCausationId::new(format!("cause-{command_id}")),
        AgentCorrelationId::new("correlation:workflow"),
    );
    AgentCommandMetadata::new(
        plan.workflow_id.clone(),
        run_id,
        AgentCommandId::new(command_id),
        durability,
        AgentTenantId::new("tenant-a"),
        AgentTimestampMillis::new(received_at_millis),
    )
    .expect("command metadata should validate")
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

#[tokio::test]
async fn bridge_refuses_a_graph_state_from_a_newer_schema() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "research.search",
        "research",
    );
    let mut running_state = running_effect_state(&plan);
    running_state.graph_schema_version = AgentGraphStateSchemaVersion::new(3);
    let run_id = AgentRunId::new("run-graph-newer-schema");
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), store, clock);
    inbox.recover().await.expect("inbox should recover");

    let error = bridge
        .schedule_node_effect(
            &plan,
            running_state,
            effect_request(run_id, "effect", 200),
            &mut inbox,
        )
        .await
        .expect_err("a newer schema version should be refused");
    assert!(
        matches!(
            &error,
            AgentGraphEffectBridgeError::Scheduler {
                error: AgentGraphSchedulerError::UnsupportedSchemaVersion { found, supported }
            } if *found == AgentGraphStateSchemaVersion::new(3)
                && *supported == CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION
        ),
        "{error:?}"
    );
    assert_eq!(error.code(), "graph-state-schema-unsupported");
    assert!(inbox
        .due_effects()
        .expect("due effects should read")
        .is_empty());
}

// ---- the reconciliation park (issue #110) ----

async fn recovered_inbox(run_id: &AgentRunId) -> TestInbox {
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let mut inbox = agent_inbox(run_id.clone(), TestStore::new(), clock);
    inbox.recover().await.expect("inbox should recover");
    inbox
}

async fn schedule_effect(
    bridge: &AgentGraphEffectBridge,
    plan: &AgentCompiledExecutionPlan,
    run_id: &AgentRunId,
    inbox: &mut TestInbox,
) -> (AgentEffectId, AgentGraphRunState) {
    let scheduled = bridge
        .schedule_node_effect(
            plan,
            running_effect_state(plan),
            effect_request(run_id.clone(), "effect", 200),
            inbox,
        )
        .await
        .expect("effect should schedule");
    assert!(scheduled.acceptance.is_scheduled());
    (scheduled.effect.effect_id, scheduled.transition.state)
}

fn message_id(effect_id: &AgentEffectId) -> OutboxMessageId {
    OutboxMessageId::new(effect_id.as_str())
}

fn entry_status(inbox: &TestInbox, effect_id: &AgentEffectId) -> OutboxStatus {
    inbox
        .inner()
        .state()
        .expect("inbox should be recovered")
        .outbox_entry(&message_id(effect_id))
        .expect("outbox entry should exist")
        .status()
}

async fn exhaust(inbox: &mut TestInbox, effect_id: &AgentEffectId) {
    inbox
        .inner_mut()
        .record_outbox_exhausted(&message_id(effect_id), "effect-outcome-unknown")
        .await
        .expect("entry should exhaust");
    assert_eq!(entry_status(inbox, effect_id), OutboxStatus::Exhausted);
}

fn reconcile_command(
    bridge: &AgentGraphEffectBridge,
    plan: &AgentCompiledExecutionPlan,
    run_id: &AgentRunId,
    command_id: &str,
    effect_id: &AgentEffectId,
    cause: &str,
) -> AgentCommand {
    bridge
        .effect_failed_command(
            command_metadata(plan, run_id.clone(), command_id, 300),
            effect_id.clone(),
            cause,
            AgentGraphEffectFailureDisposition::Reconcile,
        )
        .expect("reconcile command should build")
}

/// Schedules the `effect` node, exhausts its entry, and parks it with the
/// cause `effect-outcome-unknown`.
async fn park(
    bridge: &AgentGraphEffectBridge,
    plan: &AgentCompiledExecutionPlan,
    run_id: &AgentRunId,
    inbox: &mut TestInbox,
) -> (AgentEffectId, AgentGraphRunState) {
    let (effect_id, state) = schedule_effect(bridge, plan, run_id, inbox).await;
    exhaust(inbox, &effect_id).await;
    let command = reconcile_command(
        bridge,
        plan,
        run_id,
        "cmd-reconcile-1",
        &effect_id,
        "effect-outcome-unknown",
    );
    let outcome = bridge
        .accept_and_apply_effect_failed(plan, state, command, inbox)
        .await
        .expect("reconcile should park the node");
    assert_eq!(
        node_state(&outcome.transition.state, "effect").wait_reason,
        Some(AgentGraphWaitReason::Reconciliation)
    );
    (effect_id, outcome.transition.state)
}

fn assert_not_terminal(
    error: &AgentGraphEffectBridgeError,
    effect_id: &AgentEffectId,
    expected: OutboxStatus,
) {
    assert!(
        matches!(
            error,
            AgentGraphEffectBridgeError::EffectNotTerminal { effect_id: id, status }
                if id == effect_id && *status == expected
        ),
        "{error:?}"
    );
    assert_eq!(error.code(), "graph-effect-not-terminal");
}

fn payments_plan() -> AgentCompiledExecutionPlan {
    effect_plan(
        AgentCompiledNodeKind::ToolCall,
        "tool",
        "payments.charge",
        "payments",
    )
}

#[tokio::test]
async fn reconcile_disposition_parks_the_node_and_leaves_the_run_running() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-reconcile-park");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = schedule_effect(&bridge, &plan, &run_id, &mut inbox).await;
    let scheduled_revision = state.scheduler_revision;
    exhaust(&mut inbox, &effect_id).await;
    let command = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-1",
        &effect_id,
        "effect-outcome-unknown",
    );

    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
        .await
        .expect("reconcile should park the node");

    assert!(outcome.acceptance.is_accepted());
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(node.wait_reason, Some(AgentGraphWaitReason::Reconciliation));
    assert_eq!(node.error_code.as_deref(), Some("effect-outcome-unknown"));
    assert!(node.scheduled_effect_ids.contains(&effect_id));
    assert_eq!(
        outcome.transition.changed_node_ids,
        vec![AgentCompiledNodeId::new("effect")]
    );
    assert_eq!(
        outcome.transition.state.scheduler_revision,
        scheduled_revision + 1
    );
    assert_eq!(outcome.transition.state.blocked_reason, None);
    assert_eq!(outcome.transition.state.terminal_status, None);
    assert_eq!(
        node_state(&outcome.transition.state, "terminal").status,
        AgentGraphNodeStatus::Pending
    );
    assert!(outcome.transition.runnable_node_ids.is_empty());
    assert_eq!(entry_status(&inbox, &effect_id), OutboxStatus::Exhausted);
    assert!(inbox
        .due_effects()
        .expect("due effects should read")
        .is_empty());

    assert_eq!(
        AgentGraphEffectFailureDisposition::Reconcile.as_label(),
        "reconcile"
    );
    assert_eq!(
        "reconcile"
            .parse::<AgentGraphEffectFailureDisposition>()
            .expect("label should parse"),
        AgentGraphEffectFailureDisposition::Reconcile
    );
}

#[tokio::test]
async fn reconcile_is_refused_while_the_entry_is_live() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-reconcile-live");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = schedule_effect(&bridge, &plan, &run_id, &mut inbox).await;
    let command = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-live",
        &effect_id,
        "effect-outcome-unknown",
    );

    assert_eq!(entry_status(&inbox, &effect_id), OutboxStatus::Pending);
    let error = bridge
        .accept_and_apply_effect_failed(&plan, state.clone(), command.clone(), &mut inbox)
        .await
        .expect_err("a pending entry should refuse the park");
    assert_not_terminal(&error, &effect_id, OutboxStatus::Pending);

    inbox
        .inner_mut()
        .mark_outbox_dispatching(&message_id(&effect_id))
        .await
        .expect("entry should mark dispatching");
    assert_eq!(entry_status(&inbox, &effect_id), OutboxStatus::Dispatching);
    let error = bridge
        .accept_and_apply_effect_failed(&plan, state.clone(), command.clone(), &mut inbox)
        .await
        .expect_err("a dispatching entry should refuse the park");
    assert_not_terminal(&error, &effect_id, OutboxStatus::Dispatching);

    inbox
        .inner_mut()
        .record_outbox_failure(&message_id(&effect_id), "rate-limited", false)
        .await
        .expect("entry should record a retryable failure");
    assert_eq!(entry_status(&inbox, &effect_id), OutboxStatus::Failed);
    let error = bridge
        .accept_and_apply_effect_failed(&plan, state.clone(), command.clone(), &mut inbox)
        .await
        .expect_err("a failed entry with budget left should refuse the park");
    assert_not_terminal(&error, &effect_id, OutboxStatus::Failed);

    // The refusals recorded nothing: the same command id is accepted, not a
    // duplicate, once the entry is exhausted.
    exhaust(&mut inbox, &effect_id).await;
    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
        .await
        .expect("reconcile should park once the entry is exhausted");
    assert!(outcome.acceptance.is_accepted());
    assert_eq!(
        node_state(&outcome.transition.state, "effect").wait_reason,
        Some(AgentGraphWaitReason::Reconciliation)
    );

    // Dispatched, on a fresh run.
    let run_id = AgentRunId::new("run-graph-reconcile-dispatched");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = schedule_effect(&bridge, &plan, &run_id, &mut inbox).await;
    inbox
        .inner_mut()
        .mark_outbox_dispatching(&message_id(&effect_id))
        .await
        .expect("entry should mark dispatching");
    inbox
        .inner_mut()
        .record_outbox_success(&message_id(&effect_id))
        .await
        .expect("entry should record success");
    assert_eq!(entry_status(&inbox, &effect_id), OutboxStatus::Dispatched);
    let command = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-dispatched",
        &effect_id,
        "effect-outcome-unknown",
    );
    let error = bridge
        .accept_and_apply_effect_failed(&plan, state.clone(), command, &mut inbox)
        .await
        .expect_err("a dispatched entry should refuse the park");
    assert_not_terminal(&error, &effect_id, OutboxStatus::Dispatched);
    assert_eq!(
        node_state(&state, "effect").wait_reason,
        Some(AgentGraphWaitReason::Effect)
    );
}

#[tokio::test]
async fn reconcile_parks_over_a_cancelled_entry() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-reconcile-cancelled");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = schedule_effect(&bridge, &plan, &run_id, &mut inbox).await;
    let cancelled = inbox
        .inner_mut()
        .record_outbox_cancelled(&message_id(&effect_id), "operator-cancelled")
        .await
        .expect("entry should cancel");
    assert!(cancelled.is_some());
    assert_eq!(entry_status(&inbox, &effect_id), OutboxStatus::Cancelled);
    let command = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-cancelled",
        &effect_id,
        "effect-outcome-unknown",
    );

    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
        .await
        .expect("reconcile should park over a cancelled entry");

    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(node.wait_reason, Some(AgentGraphWaitReason::Reconciliation));
    assert_eq!(entry_status(&inbox, &effect_id), OutboxStatus::Cancelled);
}

#[tokio::test]
async fn reconcile_twice_is_a_no_op_and_a_new_cause_replaces() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-reconcile-twice");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let parked_revision = state.scheduler_revision;

    let same_cause = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-2",
        &effect_id,
        "effect-outcome-unknown",
    );
    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, state, same_cause, &mut inbox)
        .await
        .expect("a second reconcile with the same cause should apply");
    assert!(outcome.acceptance.is_accepted());
    assert!(outcome.transition.changed_node_ids.is_empty());
    assert_eq!(outcome.transition.state.scheduler_revision, parked_revision);

    let new_cause = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-3",
        &effect_id,
        "effect-reply-lost",
    );
    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, outcome.transition.state, new_cause, &mut inbox)
        .await
        .expect("a reconcile with a new cause should apply");
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(node.wait_reason, Some(AgentGraphWaitReason::Reconciliation));
    assert_eq!(node.error_code.as_deref(), Some("effect-reply-lost"));
    assert_eq!(
        outcome.transition.changed_node_ids,
        vec![AgentCompiledNodeId::new("effect")]
    );
    assert_eq!(
        outcome.transition.state.scheduler_revision,
        parked_revision + 1
    );
}

#[tokio::test]
async fn parked_node_completes_on_a_late_effect_completed() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-parked-completes");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let command = bridge
        .effect_completed_command(
            command_metadata(&plan, run_id, "cmd-late-completed", 400),
            effect_id,
            Some(artifact("artifact:late-result", ArtifactKind::ToolOutput)),
        )
        .expect("completion command should build");

    let outcome = bridge
        .accept_and_apply_effect_completed(&plan, state, command, &mut inbox)
        .await
        .expect("a late completion should resolve the park");

    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Completed);
    assert_eq!(node.wait_reason, None);
    assert_eq!(node.error_code, None);
    assert_eq!(
        node.output_refs.get(&AgentCompiledPortId::new("result")),
        Some(&artifact("artifact:late-result", ArtifactKind::ToolOutput))
    );
    assert_eq!(outcome.transition.state.terminal_status, None);
    let ready = AgentGraphScheduler::new()
        .mark_ready_nodes_runnable(&plan, outcome.transition.state, ts(500))
        .expect("terminal should become runnable");
    assert_eq!(
        ready.runnable_node_ids,
        vec![AgentCompiledNodeId::new("terminal")]
    );
}

#[tokio::test]
async fn parked_node_fails_on_a_definitive_effect_failed() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    for (disposition, suffix) in [
        (AgentGraphEffectFailureDisposition::Exhausted, "exhausted"),
        (AgentGraphEffectFailureDisposition::Terminal, "terminal"),
    ] {
        let run_id = AgentRunId::new(format!("run-graph-parked-fails-{suffix}"));
        let mut inbox = recovered_inbox(&run_id).await;
        let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
        let command = bridge
            .effect_failed_command(
                command_metadata(&plan, run_id, "cmd-definitive-failure", 400),
                effect_id,
                "retry-budget-exhausted",
                disposition,
            )
            .expect("failure command should build");

        let outcome = bridge
            .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
            .await
            .expect("a definitive failure should resolve the park");

        let node = node_state(&outcome.transition.state, "effect");
        assert_eq!(node.status, AgentGraphNodeStatus::Failed, "{suffix}");
        assert_eq!(node.wait_reason, None);
        assert_eq!(node.error_code.as_deref(), Some("retry-budget-exhausted"));
        assert_eq!(
            outcome.transition.state.terminal_status,
            Some(AgentGraphTerminalStatus::Failed)
        );
        assert_eq!(
            node_state(&outcome.transition.state, "terminal").status,
            AgentGraphNodeStatus::Cancelled
        );
    }
}

#[tokio::test]
async fn parked_node_ignores_a_retry_scheduled_report() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-parked-retry-report");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let parked_revision = state.scheduler_revision;
    let command = bridge
        .effect_failed_command(
            command_metadata(&plan, run_id, "cmd-stale-retry", 400),
            effect_id,
            "rate-limited",
            AgentGraphEffectFailureDisposition::RetryScheduled,
        )
        .expect("failure command should build");

    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
        .await
        .expect("a stale retry report should be a no-op");

    assert!(outcome.transition.changed_node_ids.is_empty());
    assert_eq!(outcome.transition.state.scheduler_revision, parked_revision);
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(node.wait_reason, Some(AgentGraphWaitReason::Reconciliation));
    assert_eq!(node.error_code.as_deref(), Some("effect-outcome-unknown"));
}

#[tokio::test]
async fn reconcile_after_an_exhausted_report_is_a_no_op_on_the_failed_node() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-reconcile-after-exhausted");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = schedule_effect(&bridge, &plan, &run_id, &mut inbox).await;
    exhaust(&mut inbox, &effect_id).await;
    let exhausted = bridge
        .effect_failed_command(
            command_metadata(&plan, run_id.clone(), "cmd-exhausted-first", 300),
            effect_id.clone(),
            "retry-budget-exhausted",
            AgentGraphEffectFailureDisposition::Exhausted,
        )
        .expect("failure command should build");
    let failed = bridge
        .accept_and_apply_effect_failed(&plan, state, exhausted, &mut inbox)
        .await
        .expect("exhausted should fail the run");
    assert_eq!(
        failed.transition.state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
    let failed_revision = failed.transition.state.scheduler_revision;

    let command = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-late",
        &effect_id,
        "effect-outcome-unknown",
    );
    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, failed.transition.state, command, &mut inbox)
        .await
        .expect("a reconcile after a definitive failure should be a no-op");

    assert!(outcome.transition.changed_node_ids.is_empty());
    assert_eq!(outcome.transition.state.scheduler_revision, failed_revision);
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Failed);
    assert_eq!(node.error_code.as_deref(), Some("retry-budget-exhausted"));
    assert_eq!(
        outcome.transition.state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
}

#[tokio::test]
async fn reconcile_parks_a_child_workflow_node_too() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = effect_plan(
        AgentCompiledNodeKind::ChildWorkflowCommand,
        "workflow",
        "child",
        "child-workflow",
    );
    let run_id = AgentRunId::new("run-graph-reconcile-child");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = schedule_effect(&bridge, &plan, &run_id, &mut inbox).await;
    assert_eq!(
        node_state(&state, "effect").wait_reason,
        Some(AgentGraphWaitReason::ChildWorkflow)
    );
    exhaust(&mut inbox, &effect_id).await;
    let command = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-child",
        &effect_id,
        "child-outcome-unknown",
    );

    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, state, command, &mut inbox)
        .await
        .expect("reconcile should park a child workflow node");

    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Waiting);
    assert_eq!(node.wait_reason, Some(AgentGraphWaitReason::Reconciliation));
    assert_eq!(node.error_code.as_deref(), Some("child-outcome-unknown"));
    assert_eq!(outcome.transition.state.terminal_status, None);
}

#[tokio::test]
async fn a_version_one_state_parks_and_is_written_at_version_two() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-reconcile-version-one");
    let mut inbox = recovered_inbox(&run_id).await;

    let mut value =
        serde_json::to_value(running_effect_state(&plan)).expect("graph state should serialize");
    value["graph_schema_version"] = json!(1);
    for node in value["node_states"]
        .as_object_mut()
        .expect("node states should be an object")
        .values_mut()
    {
        node.as_object_mut()
            .expect("node state should be an object")
            .remove("effect_generation");
    }
    let state: AgentGraphRunState =
        serde_json::from_value(value).expect("a version-one state should decode");
    assert_eq!(
        state.graph_schema_version,
        AgentGraphStateSchemaVersion::new(1)
    );

    let scheduled = bridge
        .schedule_node_effect(
            &plan,
            state,
            effect_request(run_id.clone(), "effect", 200),
            &mut inbox,
        )
        .await
        .expect("effect should schedule");
    assert_eq!(
        scheduled.transition.state.graph_schema_version,
        AgentGraphStateSchemaVersion::new(1),
        "scheduling writes no version-two value"
    );
    let effect_id = scheduled.effect.effect_id;
    exhaust(&mut inbox, &effect_id).await;
    let command = reconcile_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-reconcile-v1",
        &effect_id,
        "effect-outcome-unknown",
    );

    let outcome = bridge
        .accept_and_apply_effect_failed(&plan, scheduled.transition.state, command, &mut inbox)
        .await
        .expect("reconcile should park a version-one state");

    assert_eq!(
        outcome.transition.state.graph_schema_version,
        CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION
    );
    assert_eq!(
        node_state(&outcome.transition.state, "effect").wait_reason,
        Some(AgentGraphWaitReason::Reconciliation)
    );
    let json =
        serde_json::to_string(&outcome.transition.state).expect("parked state should serialize");
    let decoded: AgentGraphRunState =
        serde_json::from_str(&json).expect("parked state should deserialize");
    assert_eq!(decoded, outcome.transition.state);
}

// ---- the reconciliation decisions (issue #110) ----

fn decision_command(
    bridge: &AgentGraphEffectBridge,
    plan: &AgentCompiledExecutionPlan,
    run_id: &AgentRunId,
    command_id: &str,
    effect_id: &AgentEffectId,
    decision: AgentGraphReconciliationDecision,
) -> AgentCommand {
    bridge
        .reconciliation_decision_command(
            command_metadata(plan, run_id.clone(), command_id, 400),
            effect_id.clone(),
            decision,
        )
        .expect("decision command should build")
}

#[tokio::test]
async fn not_executed_decision_resumes_the_node_running_with_the_next_generation() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-decision-not-executed");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let parked = node_state(&state, "effect").clone();
    let parked_revision = state.scheduler_revision;
    let command = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-1",
        &effect_id,
        AgentGraphReconciliationDecision::NotExecuted,
    );

    let outcome = bridge
        .accept_and_apply_reconciliation_decision(&plan, state, command, &mut inbox)
        .await
        .expect("not-executed should resume the node");

    assert!(outcome.acceptance.is_accepted());
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Running);
    assert_eq!(node.wait_reason, None);
    assert_eq!(node.error_code, None);
    assert_eq!(node.effect_generation, 1);
    assert_eq!(
        (node.attempt, node.started_at),
        (parked.attempt, parked.started_at)
    );
    assert_eq!(node.scheduled_effect_ids, vec![effect_id]);
    assert_eq!(
        outcome.transition.changed_node_ids,
        vec![AgentCompiledNodeId::new("effect")]
    );
    assert_eq!(
        outcome.transition.state.scheduler_revision,
        parked_revision + 1
    );
    assert_eq!(
        outcome.transition.state.graph_schema_version,
        CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION
    );
    assert_eq!(outcome.transition.state.terminal_status, None);
}

#[tokio::test]
async fn abandon_decision_fails_the_node_and_the_run() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-decision-abandon");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let command = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-abandon",
        &effect_id,
        AgentGraphReconciliationDecision::Abandon {
            error_code: "reconciliation-abandoned".to_string(),
        },
    );

    let outcome = bridge
        .accept_and_apply_reconciliation_decision(&plan, state, command, &mut inbox)
        .await
        .expect("abandon should fail the node");

    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Failed);
    assert_eq!(node.wait_reason, None);
    assert_eq!(node.error_code.as_deref(), Some("reconciliation-abandoned"));
    assert_eq!(node.effect_generation, 0);
    assert_eq!(
        outcome.transition.state.terminal_status,
        Some(AgentGraphTerminalStatus::Failed)
    );
    assert_eq!(
        node_state(&outcome.transition.state, "terminal").status,
        AgentGraphNodeStatus::Cancelled
    );
}

#[tokio::test]
async fn an_abandon_without_a_code_is_refused_before_acceptance() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-decision-blank-code");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let blank = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-blank",
        &effect_id,
        AgentGraphReconciliationDecision::Abandon {
            error_code: "  ".to_string(),
        },
    );

    let error = bridge
        .accept_and_apply_reconciliation_decision(&plan, state.clone(), blank, &mut inbox)
        .await
        .expect_err("a blank abandon code should be refused");
    assert!(
        matches!(
            &error,
            AgentGraphEffectBridgeError::Scheduler {
                error: AgentGraphSchedulerError::EmptyCauseCode { node_id }
            } if node_id == &AgentCompiledNodeId::new("effect")
        ),
        "{error:?}"
    );
    assert_eq!(error.code(), "empty-reconciliation-cause");

    // The refusal recorded nothing: the same command id carries a valid code
    // and is accepted, not a duplicate.
    let valid = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-blank",
        &effect_id,
        AgentGraphReconciliationDecision::Abandon {
            error_code: "reconciliation-abandoned".to_string(),
        },
    );
    let outcome = bridge
        .accept_and_apply_reconciliation_decision(&plan, state, valid, &mut inbox)
        .await
        .expect("a valid abandon should apply");
    assert!(outcome.acceptance.is_accepted());
    assert_eq!(
        node_state(&outcome.transition.state, "effect").status,
        AgentGraphNodeStatus::Failed
    );
}

#[tokio::test]
async fn a_decision_on_an_unparked_node_is_refused() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-decision-unparked");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = schedule_effect(&bridge, &plan, &run_id, &mut inbox).await;
    let command = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-unparked",
        &effect_id,
        AgentGraphReconciliationDecision::NotExecuted,
    );

    let error = bridge
        .accept_and_apply_reconciliation_decision(&plan, state, command, &mut inbox)
        .await
        .expect_err("a decision on an unparked node should be refused");
    assert!(
        matches!(
            &error,
            AgentGraphEffectBridgeError::InvalidNodeStatus { node_id, status }
                if node_id == &AgentCompiledNodeId::new("effect")
                    && status == &AgentGraphNodeStatus::Waiting
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_replayed_decision_after_the_node_resolved_is_a_no_op() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-decision-replayed");
    let mut inbox = recovered_inbox(&run_id).await;
    let (effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let abandon = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-abandon",
        &effect_id,
        AgentGraphReconciliationDecision::Abandon {
            error_code: "reconciliation-abandoned".to_string(),
        },
    );
    let failed = bridge
        .accept_and_apply_reconciliation_decision(&plan, state, abandon, &mut inbox)
        .await
        .expect("abandon should apply");
    let failed_revision = failed.transition.state.scheduler_revision;

    let replay = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-late",
        &effect_id,
        AgentGraphReconciliationDecision::NotExecuted,
    );
    let outcome = bridge
        .accept_and_apply_reconciliation_decision(
            &plan,
            failed.transition.state,
            replay,
            &mut inbox,
        )
        .await
        .expect("a late decision on a resolved node should be a no-op");

    assert!(outcome.acceptance.is_accepted());
    assert!(outcome.transition.changed_node_ids.is_empty());
    assert_eq!(outcome.transition.state.scheduler_revision, failed_revision);
    let node = node_state(&outcome.transition.state, "effect");
    assert_eq!(node.status, AgentGraphNodeStatus::Failed);
    assert_eq!(node.effect_generation, 0);
}

#[tokio::test]
async fn a_decision_for_an_unknown_effect_is_refused() {
    let bridge = AgentGraphEffectBridge::new();
    let plan = payments_plan();
    let run_id = AgentRunId::new("run-graph-decision-unknown");
    let mut inbox = recovered_inbox(&run_id).await;
    let (_effect_id, state) = park(&bridge, &plan, &run_id, &mut inbox).await;
    let unknown = AgentEffectId::new("graph-effect:nope");
    let command = decision_command(
        &bridge,
        &plan,
        &run_id,
        "cmd-decide-unknown",
        &unknown,
        AgentGraphReconciliationDecision::NotExecuted,
    );

    let error = bridge
        .accept_and_apply_reconciliation_decision(&plan, state, command, &mut inbox)
        .await
        .expect_err("a decision for an unknown effect should be refused");
    assert!(
        matches!(
            &error,
            AgentGraphEffectBridgeError::UnknownEffect { effect_id } if effect_id == &unknown
        ),
        "{error:?}"
    );
}
