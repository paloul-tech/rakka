//! Failure-injection coverage for Phase 7 production hardening.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use rakka_agent_workflow::{
    timer_fired_command, AgentAdapterOutcome, AgentAdapterReceipt, AgentCausationId, AgentCommand,
    AgentCommandId, AgentCommandKind, AgentCommandMetadata, AgentCorrelationId,
    AgentDeduplicationKey, AgentDispatcherError, AgentDispatcherFleetSettings,
    AgentDispatcherFleetState, AgentDispatcherWorker, AgentDispatcherWorkerId,
    AgentDurabilityMetadata, AgentEffect, AgentEffectDispatchFuture, AgentEffectDispatcher,
    AgentEffectId, AgentEffectKind, AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget,
    AgentHumanApprovalRequest, AgentHumanCheckpointRuntime, AgentHumanDecisionSubmission,
    AgentIdempotencyKey, AgentInboxDuplicateReason, AgentRunId, AgentRunInbox, AgentRunState,
    AgentRunStatus, AgentRunTransitionKind, AgentRunWaitReason, AgentStatePayload, AgentStep,
    AgentStepId, AgentStepKind, AgentStepRunner, AgentTenantId, AgentTimerEntry, AgentTimerId,
    AgentTimerPolicy, AgentTimerScanner, AgentTimerScannerSettings, AgentTimerStatus,
    AgentTimerStore, AgentTimerStoreState, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId,
    HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, PrincipalRef,
    RedactionStatus, StateSchemaVersion, WorkflowDefinitionVersion,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    ManualWorkflowClock, OutboxDispatchResult, OutboxMessageId, OutboxStatus, WorkflowState,
    WorkflowTimestamp,
};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type FleetStore = InMemoryDurableStateStore<AgentDispatcherFleetState>;
type TimerStoreBackend = InMemoryDurableStateStore<AgentTimerStoreState>;
type TestWorker = AgentDispatcherWorker<FleetStore, WorkflowStore, ManualWorkflowClock>;

#[tokio::test]
async fn crash_after_inbox_acceptance_and_effect_scheduling_recovers_durable_work() {
    let workflow = workflow();
    let workflow_store = WorkflowStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let run_id = AgentRunId::new("run-crash-after-durable-boundaries");

    let mut ingress =
        AgentRunInbox::with_clock(run_id.clone(), workflow_store.clone(), clock.clone());
    ingress.recover().await.expect("inbox should recover");
    let accepted = ingress
        .accept_command(start_command(&workflow, &run_id))
        .await
        .expect("start command should persist before acceptance");
    assert!(accepted.is_accepted());
    drop(ingress);

    let mut recovered =
        AgentRunInbox::with_clock(run_id.clone(), workflow_store.clone(), clock.clone());
    recovered
        .recover()
        .await
        .expect("recovered inbox should load accepted work");
    let duplicate = recovered
        .accept_command(start_command(&workflow, &run_id))
        .await
        .expect("replayed start command should deduplicate");
    assert!(duplicate.is_duplicate());
    assert_eq!(
        duplicate.duplicate_reason(),
        Some(AgentInboxDuplicateReason::MessageId)
    );

    recovered
        .schedule_effect(effect(
            "effect-after-schedule-crash",
            AgentEffectKind::ToolCall,
            "tool",
            "analysis-tool",
            100,
        ))
        .await
        .expect("effect should persist before scheduling returns");
    drop(recovered);

    let mut restarted = AgentRunInbox::with_clock(run_id, workflow_store, clock);
    restarted
        .recover()
        .await
        .expect("restarted inbox should recover scheduled effect");
    let due = restarted
        .due_effects()
        .expect("due outbox effects should decode after restart");
    assert_eq!(due.len(), 1);
    assert_eq!(
        due[0].effect.effect_id,
        AgentEffectId::new("effect-after-schedule-crash")
    );
    assert_eq!(due[0].entry.status(), OutboxStatus::Pending);
}

#[tokio::test]
async fn crash_after_external_result_before_success_persistence_redelivers_effect() {
    let workflow_store = WorkflowStore::new();
    let fleet_store = FleetStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-external-result-before-persist");
    let effect_id = "effect-external-result-before-persist";

    schedule_effect(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect(
            effect_id,
            AgentEffectKind::HttpCall,
            "http",
            "callback",
            100,
        ),
    )
    .await;

    let settings = AgentDispatcherFleetSettings::new(8, 10);
    let mut first = worker(
        "dispatcher-before-crash",
        fleet_store.clone(),
        workflow_store.clone(),
        clock.clone(),
        metrics.clone(),
        settings.clone(),
    );
    first.recover().await.expect("first worker should recover");
    first
        .refresh_run(run_id.clone(), None)
        .await
        .expect("first worker should index due effect");
    let first_claim = first
        .claim_due()
        .await
        .expect("first worker should claim due effect")
        .claims
        .pop()
        .expect("one claim should be issued");

    let mut expiring = ExpiringRecordingDispatcher {
        clock: clock.clone(),
        advance_ms: 20,
        seen: Vec::new(),
    };
    let error = first
        .dispatch_claim(first_claim, &mut expiring)
        .await
        .expect_err("lease loss after side effect result should fence success persistence");
    assert!(matches!(error, AgentDispatcherError::ClaimFenced { .. }));
    assert_eq!(expiring.seen, vec![AgentEffectId::new(effect_id)]);
    assert_outbox_status(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect_id,
        OutboxStatus::Dispatching,
    )
    .await;
    drop(first);

    let mut recovered = worker(
        "dispatcher-after-crash",
        fleet_store,
        workflow_store.clone(),
        clock.clone(),
        metrics,
        settings,
    );
    recovered
        .recover()
        .await
        .expect("recovered worker should load fleet state");
    recovered
        .refresh_run(run_id.clone(), None)
        .await
        .expect("dispatching effect should be rediscovered");
    let recovered_claim = recovered
        .claim_due()
        .await
        .expect("expired claim should be recoverable")
        .claims
        .pop()
        .expect("recovered claim should be issued");
    let mut dispatcher = RecordingDispatcher::new([OutboxDispatchResult::Success]);
    recovered
        .dispatch_claim(recovered_claim, &mut dispatcher)
        .await
        .expect("recovered claim should persist success");
    assert_eq!(dispatcher.seen, vec![AgentEffectId::new(effect_id)]);
    assert_outbox_status(
        &workflow_store,
        &clock,
        run_id,
        effect_id,
        OutboxStatus::Dispatched,
    )
    .await;
}

#[tokio::test]
async fn human_decision_after_checkpoint_runtime_restart_resumes_once() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-human-checkpoint-crash");
    start_running_run(&workflow, &run_store, &run_id).await;

    let checkpoint = checkpoint("checkpoint-crash-review", 500);
    let mut before_crash = human_runtime(
        workflow.clone(),
        run_id.clone(),
        run_store.clone(),
        workflow_store.clone(),
        clock.clone(),
        metrics.clone(),
    );
    let opening = before_crash
        .open_checkpoint(approval_request(&checkpoint, "effect-human-crash-review"))
        .await
        .expect("checkpoint should persist before returning");
    assert_eq!(
        opening.transition.next_status,
        AgentRunStatus::WaitingForHuman
    );
    drop(before_crash);

    clock.set(WorkflowTimestamp::from_millis(700));
    let mut restarted = human_runtime(
        workflow.clone(),
        run_id.clone(),
        run_store.clone(),
        workflow_store,
        clock,
        metrics,
    );
    let submission = decision_submission(
        &workflow,
        &run_id,
        "decision-after-runtime-restart",
        "checkpoint-crash-review",
        "approve",
        HumanCheckpointStatus::Approved,
        700,
    );
    let decision = restarted
        .submit_decision(submission.clone())
        .await
        .expect("human decision should resume after runtime restart");
    assert!(decision.inbox_acceptance.is_accepted());
    assert_eq!(
        decision
            .transition
            .as_ref()
            .expect("decision should resume waiting run")
            .kind,
        AgentRunTransitionKind::Resume
    );

    let duplicate = restarted
        .submit_decision(submission)
        .await
        .expect("replayed decision should deduplicate");
    assert!(duplicate.inbox_acceptance.is_duplicate());
    assert!(duplicate.transition.is_none());

    let resumed = recover_run(&workflow, &run_store, &run_id).await;
    assert_eq!(resumed.status, AgentRunStatus::Running);
    assert_eq!(resumed.pending_human_checkpoint, None);
    assert_eq!(
        resumed.checkpoints[0].status,
        HumanCheckpointStatus::Approved
    );
}

#[tokio::test]
async fn timer_firing_after_scanner_restart_does_not_resume_twice() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let timer_store = TimerStoreBackend::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let run_id = AgentRunId::new("run-timer-fire-crash");
    let timer_id = AgentTimerId::new("timer-fire-crash");

    put_run_in_timer_wait(&workflow, &run_store, &run_id).await;
    let mut timers = AgentTimerStore::new(timer_store.clone());
    timers
        .recover(ts(100))
        .await
        .expect("timer store should recover");
    timers
        .schedule_timer(timer(&workflow, &run_id, &timer_id, 500))
        .await
        .expect("timer should schedule");

    clock.set(WorkflowTimestamp::from_millis(500));
    let mut first_scanner = AgentTimerScanner::with_clock(
        workflow.clone(),
        timers,
        workflow_store.clone(),
        run_store.clone(),
        clock.clone(),
        AgentTimerScannerSettings::new(8),
    );
    let first_scan = first_scanner
        .scan_due()
        .await
        .expect("first scan should fire timer");
    assert_eq!(first_scan.fired.len(), 1);
    assert_eq!(
        first_scan.fired[0]
            .transition
            .as_ref()
            .expect("timer should resume run")
            .kind,
        AgentRunTransitionKind::Resume
    );
    let fired_timer = first_scan.fired[0].timer.clone();
    drop(first_scanner);

    let restarted_timers = AgentTimerStore::new(timer_store);
    let mut restarted_scanner = AgentTimerScanner::with_clock(
        workflow.clone(),
        restarted_timers,
        workflow_store.clone(),
        run_store.clone(),
        clock.clone(),
        AgentTimerScannerSettings::new(8),
    );
    let second_scan = restarted_scanner
        .scan_due()
        .await
        .expect("restarted scanner should load fired timer state");
    assert_eq!(second_scan.due_timer_count, 0);
    assert!(second_scan.fired.is_empty());

    let mut inbox = AgentRunInbox::with_clock(run_id.clone(), workflow_store, clock);
    inbox.recover().await.expect("inbox should recover");
    let duplicate = inbox
        .accept_command(timer_fired_command(&fired_timer, ts(500)).expect("timer command"))
        .await
        .expect("replayed timer command should be durable");
    assert!(duplicate.is_duplicate());

    let recovered = recover_run(&workflow, &run_store, &run_id).await;
    assert_eq!(recovered.status, AgentRunStatus::Running);
    assert_eq!(
        fired_timer.status,
        AgentTimerStatus::Fired,
        "fired timer state should be durable before duplicate delivery"
    );
}

#[test]
fn model_provider_timeout_maps_to_retryable_durable_outbox_timeout() {
    let outcome = AgentAdapterOutcome::timed_out(
        AgentAdapterReceipt::new(
            "receipt-timeout",
            "model-provider",
            "primary-chat",
            AgentIdempotencyKey::new("idem-timeout"),
            ts(1_000),
        )
        .redaction(RedactionStatus::ReferenceOnly),
        2_500,
        None,
    );

    assert_eq!(
        outcome.to_outbox_dispatch_result(),
        OutboxDispatchResult::timeout("adapter-timeout:2500")
    );
}

fn worker(
    worker_id: &str,
    fleet_store: FleetStore,
    workflow_store: WorkflowStore,
    clock: ManualWorkflowClock,
    metrics: Arc<InMemoryMetricsRecorder>,
    settings: AgentDispatcherFleetSettings,
) -> TestWorker {
    AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new(worker_id),
        fleet_store,
        workflow_store,
        settings,
        clock,
        metrics,
    )
}

async fn schedule_effect(
    store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    run_id: AgentRunId,
    effect: AgentEffect,
) {
    let mut inbox = AgentRunInbox::with_clock(run_id, store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");
    inbox
        .schedule_effect(effect)
        .await
        .expect("effect should schedule");
}

async fn assert_outbox_status(
    store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    run_id: AgentRunId,
    effect_id: &str,
    expected: OutboxStatus,
) {
    let mut inbox = AgentRunInbox::with_clock(run_id, store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");
    let message_id = OutboxMessageId::new(effect_id);
    let entry = inbox
        .inner()
        .state()
        .expect("workflow state should be recovered")
        .outbox_entry(&message_id)
        .expect("outbox entry should exist");
    assert_eq!(entry.status(), expected);
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

async fn put_run_in_timer_wait(workflow: &AgentWorkflow, store: &RunStore, run_id: &AgentRunId) {
    start_running_run(workflow, store, run_id).await;
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());
    runner.recover().await.expect("runner should recover");
    runner
        .wait(AgentRunWaitReason::Timer, ts(250))
        .await
        .expect("run should wait for timer");
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

fn human_runtime(
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

fn start_command(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            workflow.workflow_id.clone(),
            run_id.clone(),
            AgentCommandId::new(format!("command-start-{}", run_id.as_str())),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new(format!("start:{}", run_id.as_str())),
                AgentCausationId::new("phase7-ingress"),
                AgentCorrelationId::new(format!("corr-{}", run_id.as_str())),
            ),
            AgentTenantId::new("tenant-phase7"),
            ts(100),
        )
        .expect("start metadata should validate"),
    )
    .expect("start command should validate")
}

fn effect(
    effect_id: &str,
    kind: AgentEffectKind,
    target_type: &str,
    target_name: &str,
    due_at: u64,
) -> AgentEffect {
    let metadata = AgentEffectMetadata::new(
        AgentEffectId::new(effect_id),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("dedupe-{effect_id}")),
            AgentCausationId::new(format!("cause-{effect_id}")),
            AgentCorrelationId::new(format!("correlation-{effect_id}")),
        ),
        AgentIdempotencyKey::new(format!("idempotency-{effect_id}")),
        ts(100),
    )
    .expect("effect metadata should validate")
    .due_at(ts(due_at));

    AgentEffectSchedule::new(kind, target(target_type, target_name), metadata)
        .expect("effect schedule should validate")
        .expected_result_type("dispatch.result")
        .expect("expected result type should validate")
        .into_effect()
        .expect("effect should validate")
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
        target("human", "approval-ui"),
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
        AgentTenantId::new("tenant-phase7"),
        ts(received_at),
    )
    .expect("decision metadata should validate")
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
        summary: "Review failure-injection checkpoint".to_string(),
        available_decisions: vec![HumanDecisionOption {
            value: "approve".to_string(),
            label: "Approve".to_string(),
            requires_comment: false,
        }],
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

fn timer(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    timer_id: &AgentTimerId,
    due_at: u64,
) -> AgentTimerEntry {
    AgentTimerEntry::new(
        timer_id.clone(),
        workflow.workflow_id.clone(),
        run_id.clone(),
        AgentTenantId::new("tenant-phase7"),
        ts(due_at),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("timer:{}", timer_id.as_str())),
            AgentCausationId::new(format!("cause:{}", timer_id.as_str())),
            AgentCorrelationId::new(format!("corr:{}", timer_id.as_str())),
        ),
        ts(100),
    )
    .expect("timer entry should validate")
    .policy(
        AgentTimerPolicy::new()
            .policy_name("step-timeout")
            .max_lateness_ms(1_000),
    )
    .expect("timer policy should validate")
}

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-phase7-failure-injection"),
        workflow_type: "phase7-failure-injection".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Phase 7 failure injection workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::WaitingForHuman.as_label().to_string(),
            AgentRunStatus::WaitingForTimer.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec![
            AgentCommandKind::StartRun.type_name().to_string(),
            AgentCommandKind::HumanDecisionSubmitted {
                checkpoint_id: HumanCheckpointId::new("checkpoint"),
                decision: "approve".to_string(),
            }
            .type_name()
            .to_string(),
            AgentCommandKind::TimerFired {
                timer_id: "timer".to_string(),
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
        tenant: Some(AgentTenantId::new("tenant-phase7")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
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

fn target(target_type: &str, target_name: &str) -> AgentEffectTarget {
    AgentEffectTarget {
        target_type: target_type.to_string(),
        name: target_name.to_string(),
        address: Some(format!("{target_type}://{target_name}")),
        attributes: BTreeMap::new(),
    }
}

struct RecordingDispatcher {
    results: VecDeque<OutboxDispatchResult>,
    seen: Vec<AgentEffectId>,
}

impl RecordingDispatcher {
    fn new(results: impl IntoIterator<Item = OutboxDispatchResult>) -> Self {
        Self {
            results: results.into_iter().collect(),
            seen: Vec::new(),
        }
    }
}

impl AgentEffectDispatcher for RecordingDispatcher {
    fn dispatch<'a>(
        &'a mut self,
        job: &'a rakka_agent_workflow::AgentDispatchJob,
    ) -> AgentEffectDispatchFuture<'a> {
        self.seen.push(job.effect.effect_id.clone());
        let result = self
            .results
            .pop_front()
            .unwrap_or(OutboxDispatchResult::Success);
        Box::pin(async move { result })
    }
}

struct ExpiringRecordingDispatcher {
    clock: ManualWorkflowClock,
    advance_ms: u64,
    seen: Vec<AgentEffectId>,
}

impl AgentEffectDispatcher for ExpiringRecordingDispatcher {
    fn dispatch<'a>(
        &'a mut self,
        job: &'a rakka_agent_workflow::AgentDispatchJob,
    ) -> AgentEffectDispatchFuture<'a> {
        self.seen.push(job.effect.effect_id.clone());
        self.clock.advance_millis(self.advance_ms);
        Box::pin(async { OutboxDispatchResult::Success })
    }
}

const fn ts(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}
