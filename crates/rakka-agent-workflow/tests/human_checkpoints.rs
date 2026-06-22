//! Durable human checkpoint facade tests.

use std::collections::BTreeMap;
use std::sync::Arc;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommandId, AgentCommandMetadata, AgentCorrelationId,
    AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffectId, AgentEffectKind,
    AgentEffectTarget, AgentHumanApprovalRequest, AgentHumanCheckpointRuntime,
    AgentHumanDecisionSubmission, AgentIdempotencyKey, AgentInboxDuplicateReason,
    AgentRunActorSnapshot, AgentRunId, AgentRunInbox, AgentRunState, AgentRunStatus,
    AgentRunTransitionKind, AgentStatePayload, AgentStep, AgentStepId, AgentStepKind,
    AgentStepRunner, AgentTenantId, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId,
    AgentWorkflowSnapshotRegistry, HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus,
    HumanDecisionOption, PrincipalRef, StateSchemaVersion, WorkflowDefinitionVersion,
    METRIC_AGENT_HUMAN_WAIT_LATENCY_MS,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, OutboxStatus, WorkflowState, WorkflowTimestamp};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;

#[tokio::test]
async fn checkpoint_opens_schedules_approval_and_decision_resumes_run() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-human-approval");
    start_running_run(&workflow, &run_store, &run_id).await;

    let checkpoint = checkpoint("checkpoint-approval", 500);
    let mut runtime = runtime(
        workflow.clone(),
        run_id.clone(),
        run_store.clone(),
        workflow_store.clone(),
        clock.clone(),
        metrics.clone(),
    );
    let opening = runtime
        .open_checkpoint(approval_request(&checkpoint, "effect-human-approval"))
        .await
        .expect("checkpoint should open");

    assert_eq!(
        opening.transition.kind,
        AgentRunTransitionKind::WaitForHuman
    );
    assert_eq!(
        opening.transition.next_status,
        AgentRunStatus::WaitingForHuman
    );
    assert!(opening.outbox_acceptance.is_scheduled());
    assert_eq!(
        opening.approval_effect.kind,
        AgentEffectKind::HumanApprovalRequest
    );

    let mut inbox =
        AgentRunInbox::with_clock(run_id.clone(), workflow_store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");
    let due = inbox.due_effects().expect("approval effect should be due");
    assert_eq!(due.len(), 1);
    assert_eq!(
        due[0].effect.effect_id,
        AgentEffectId::new("effect-human-approval")
    );
    assert_eq!(due[0].entry.status(), OutboxStatus::Pending);

    let waiting = recover_run(&workflow, &run_store, &run_id).await;
    assert_eq!(waiting.status, AgentRunStatus::WaitingForHuman);
    assert_eq!(
        waiting.pending_human_checkpoint,
        Some(HumanCheckpointId::new("checkpoint-approval"))
    );
    assert_eq!(waiting.checkpoints[0].status, HumanCheckpointStatus::Open);

    clock.set(WorkflowTimestamp::from_millis(700));
    let submission = decision_submission(
        &workflow,
        &run_id,
        "decision-approval",
        "checkpoint-approval",
        "approve",
        HumanCheckpointStatus::Approved,
        700,
    );
    let decision = runtime
        .submit_decision(submission.clone())
        .await
        .expect("decision should be accepted and resume run");
    assert!(decision.inbox_acceptance.is_accepted());
    let transition = decision
        .transition
        .as_ref()
        .expect("accepted decision should resume");
    assert_eq!(transition.kind, AgentRunTransitionKind::Resume);
    assert_eq!(
        transition.previous_status,
        Some(AgentRunStatus::WaitingForHuman)
    );
    assert_eq!(transition.next_status, AgentRunStatus::Running);
    assert_eq!(
        decision
            .checkpoint
            .as_ref()
            .expect("resolved checkpoint should be returned")
            .status,
        HumanCheckpointStatus::Approved
    );

    let duplicate = runtime
        .submit_decision(submission)
        .await
        .expect("duplicate decision should be deduplicated");
    assert!(duplicate.inbox_acceptance.is_duplicate());
    assert_eq!(
        duplicate.inbox_acceptance.duplicate_reason(),
        Some(AgentInboxDuplicateReason::MessageId)
    );
    assert!(duplicate.transition.is_none());

    let resumed = recover_run(&workflow, &run_store, &run_id).await;
    assert_eq!(resumed.status, AgentRunStatus::Running);
    assert_eq!(resumed.pending_human_checkpoint, None);
    assert_eq!(
        resumed.checkpoints[0].status,
        HumanCheckpointStatus::Approved
    );

    assert_eq!(
        metrics
            .snapshot()
            .observations_named(METRIC_AGENT_HUMAN_WAIT_LATENCY_MS)
            .len(),
        1
    );
}

#[tokio::test]
async fn overdue_checkpoint_can_escalate_and_still_accept_decision() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-human-escalation");
    start_running_run(&workflow, &run_store, &run_id).await;

    let checkpoint = checkpoint("checkpoint-escalate", 150);
    let mut runtime = runtime(
        workflow.clone(),
        run_id.clone(),
        run_store.clone(),
        workflow_store,
        clock.clone(),
        metrics,
    );
    runtime
        .open_checkpoint(approval_request(&checkpoint, "effect-human-escalate"))
        .await
        .expect("checkpoint should open");

    clock.set(WorkflowTimestamp::from_millis(200));
    let overdue = runtime
        .overdue_checkpoints(ts(200))
        .await
        .expect("overdue checkpoint should be discoverable");
    assert_eq!(overdue.len(), 1);
    assert_eq!(overdue[0].checkpoint_id, checkpoint.checkpoint_id);

    let escalated = runtime
        .escalate_checkpoint(&HumanCheckpointId::new("checkpoint-escalate"))
        .await
        .expect("checkpoint should escalate");
    assert_eq!(
        escalated.next_status,
        AgentRunStatus::WaitingForHuman,
        "escalation should not occupy or resume the run"
    );
    assert_eq!(
        escalated.state.checkpoints[0].status,
        HumanCheckpointStatus::Escalated
    );

    let submission = decision_submission(
        &workflow,
        &run_id,
        "decision-escalated",
        "checkpoint-escalate",
        "reject",
        HumanCheckpointStatus::Rejected,
        250,
    );
    let decision = runtime
        .submit_decision(submission)
        .await
        .expect("escalated checkpoint should still accept a decision");
    assert_eq!(
        decision
            .checkpoint
            .expect("checkpoint should resolve")
            .status,
        HumanCheckpointStatus::Rejected
    );
}

#[tokio::test]
async fn human_checkpoint_snapshot_reports_waiting_runs() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-human-snapshot");
    start_running_run(&workflow, &run_store, &run_id).await;

    let checkpoint = checkpoint("checkpoint-snapshot", 500);
    let mut runtime = runtime(
        workflow.clone(),
        run_id.clone(),
        run_store.clone(),
        workflow_store,
        clock,
        metrics,
    );
    let opening = runtime
        .open_checkpoint(approval_request(&checkpoint, "effect-human-snapshot"))
        .await
        .expect("checkpoint should open");

    let registry = AgentWorkflowSnapshotRegistry::new();
    registry.record_run_actor_snapshot(&AgentRunActorSnapshot {
        run_id: run_id.clone(),
        run_state: Some(opening.transition.state),
        recoverable_command_count: 0,
        due_effect_count: 1,
    });

    let snapshot = registry.human_checkpoint_snapshot();
    assert_eq!(snapshot.observed_run_count(), 1);
    assert_eq!(snapshot.waiting_run_count(), 1);
    assert_eq!(snapshot.open_checkpoint_count(), 1);
    assert_eq!(snapshot.due_checkpoint_count(), 1);
    assert_eq!(snapshot.sampled_runs().len(), 1);
    assert_eq!(
        snapshot.sampled_runs()[0].pending_checkpoint_id(),
        Some("checkpoint-snapshot")
    );
}

async fn start_running_run(workflow: &AgentWorkflow, store: &RunStore, run_id: &AgentRunId) {
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());
    runner.recover().await.expect("runner should recover");
    runner
        .start(accepted_run_state(workflow, run_id))
        .await
        .expect("run should start");
    runner
        .begin_step(ts(110))
        .await
        .expect("run should begin running");
}

async fn recover_run(
    workflow: &AgentWorkflow,
    store: &RunStore,
    run_id: &AgentRunId,
) -> AgentRunState {
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());
    runner
        .recover()
        .await
        .expect("runner should recover")
        .cloned()
        .expect("run state should exist")
}

fn runtime(
    workflow: AgentWorkflow,
    run_id: AgentRunId,
    run_store: RunStore,
    workflow_store: WorkflowStore,
    clock: ManualWorkflowClock,
    metrics: Arc<InMemoryMetricsRecorder>,
) -> AgentHumanCheckpointRuntime<RunStore, WorkflowStore, ManualWorkflowClock> {
    AgentHumanCheckpointRuntime::with_clock_and_metrics(
        workflow,
        run_id,
        run_store,
        workflow_store,
        clock,
        metrics,
    )
}

fn approval_request(checkpoint: &HumanCheckpoint, effect_id: &str) -> AgentHumanApprovalRequest {
    AgentHumanApprovalRequest::new(
        checkpoint.clone(),
        AgentEffectId::new(effect_id),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("approval:{}", checkpoint.checkpoint_id.as_str())),
            AgentCausationId::new(format!("cause:{}", checkpoint.checkpoint_id.as_str())),
            AgentCorrelationId::new(format!("corr:{}", checkpoint.checkpoint_id.as_str())),
        ),
        AgentIdempotencyKey::new(format!("human:{}", checkpoint.checkpoint_id.as_str())),
        AgentEffectTarget {
            target_type: "human".to_string(),
            name: "approval-ui".to_string(),
            address: Some("https://approvals.local/queue".to_string()),
            attributes: BTreeMap::new(),
        },
    )
    .expect("approval request should validate")
}

fn decision_submission(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    command_id: &str,
    checkpoint_id: &str,
    decision: &str,
    resolved_status: HumanCheckpointStatus,
    received_at: u64,
) -> AgentHumanDecisionSubmission {
    let metadata = AgentCommandMetadata::new(
        workflow.workflow_id.clone(),
        run_id.clone(),
        AgentCommandId::new(command_id),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("dedupe-{command_id}")),
            AgentCausationId::new(format!("cause-{command_id}")),
            AgentCorrelationId::new(format!("corr-{command_id}")),
        ),
        AgentTenantId::new("tenant-human"),
        ts(received_at),
    )
    .expect("command metadata should validate")
    .principal(PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "reviewer-1".to_string(),
        display_name: Some("Reviewer One".to_string()),
    });
    AgentHumanDecisionSubmission::new(
        metadata,
        HumanCheckpointId::new(checkpoint_id),
        decision,
        resolved_status,
    )
}

fn checkpoint(checkpoint_id: &str, due_at: u64) -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new(checkpoint_id),
        status: HumanCheckpointStatus::Open,
        summary: "Review proposed plan".to_string(),
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
        due_at: Some(ts(due_at)),
        escalation_target: Some("team-lead".to_string()),
        context_artifacts: Vec::new(),
        created_by: Some(PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "planner".to_string(),
            display_name: Some("Planner".to_string()),
        }),
        resolved_by: None,
        created_at: ts(100),
        resolved_at: None,
        audit_event_ids: Vec::new(),
    }
}

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-human"),
        workflow_type: "human-review".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Human review workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::WaitingForHuman.as_label().to_string(),
        ],
        command_types: vec![
            rakka_agent_workflow::AgentCommandKind::StartRun
                .type_name()
                .to_string(),
            rakka_agent_workflow::AgentCommandKind::HumanDecisionSubmitted {
                checkpoint_id: HumanCheckpointId::new("checkpoint"),
                decision: "approve".to_string(),
            }
            .type_name()
            .to_string(),
        ],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("review"),
            kind: AgentStepKind::HumanCheckpoint,
            display_name: Some("Review".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: BTreeMap::new(),
        }],
        payload_types: Vec::new(),
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::new(),
    }
}

fn accepted_run_state(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-human")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(AgentStepId::new("review")),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: ts(100),
        updated_at: ts(100),
        completed_at: None,
    }
}

const fn ts(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}
