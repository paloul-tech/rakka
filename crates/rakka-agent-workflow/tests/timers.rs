//! Durable timer model tests.

use std::collections::BTreeMap;
use std::sync::Arc;

use rakka_agent_workflow::{
    timer_fired_command, AgentCausationId, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentInboxDuplicateReason, AgentRunId, AgentRunState, AgentRunStatus,
    AgentRunTransitionKind, AgentRunWaitReason, AgentStatePayload, AgentStep, AgentStepId,
    AgentStepKind, AgentStepRunner, AgentTenantId, AgentTimerEntry, AgentTimerId, AgentTimerPolicy,
    AgentTimerScanner, AgentTimerScannerSettings, AgentTimerStatus, AgentTimerStore,
    AgentTimerStoreState, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId, StateSchemaVersion,
    WorkflowDefinitionVersion,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type TimerStoreBackend = InMemoryDurableStateStore<AgentTimerStoreState>;

#[tokio::test]
async fn due_timer_recovers_after_restart_injects_inbox_and_resumes_waiting_run() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let timer_store = TimerStoreBackend::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let run_id = AgentRunId::new("run-timer-restart");
    let timer_id = AgentTimerId::new("timer-restart");

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
    let restarted_timers = AgentTimerStore::new(timer_store.clone());
    let mut scanner = AgentTimerScanner::with_clock(
        workflow.clone(),
        restarted_timers,
        workflow_store.clone(),
        run_store.clone(),
        clock.clone(),
        AgentTimerScannerSettings::new(8),
    );

    let scan = scanner
        .scan_due()
        .await
        .expect("due timer scan should fire");
    assert_eq!(scan.due_timer_count, 1);
    assert!(!scan.backpressure_limited);
    assert_eq!(scan.fired.len(), 1);
    let fired = &scan.fired[0];
    assert_eq!(fired.timer.status, AgentTimerStatus::Fired);
    assert_eq!(fired.late_by_ms, 0);
    assert!(fired.inbox_acceptance.is_accepted());
    let transition = fired
        .transition
        .as_ref()
        .expect("waiting run should resume");
    assert_eq!(transition.kind, AgentRunTransitionKind::Resume);
    assert_eq!(
        transition.previous_status,
        Some(AgentRunStatus::WaitingForTimer)
    );
    assert_eq!(transition.next_status, AgentRunStatus::Running);

    let recovered = recover_run(&workflow, &run_store, &run_id).await;
    assert_eq!(recovered.status, AgentRunStatus::Running);

    let mut recovered_timers = AgentTimerStore::new(timer_store);
    let state = recovered_timers
        .recover(ts(500))
        .await
        .expect("timer store should recover after firing");
    assert_eq!(
        state.timer(&timer_id).expect("timer should persist").status,
        AgentTimerStatus::Fired
    );
}

#[tokio::test]
async fn duplicate_timer_delivery_is_deduplicated_by_inbox_key() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let timer_store = TimerStoreBackend::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(500));
    let run_id = AgentRunId::new("run-timer-duplicate");
    let timer_id = AgentTimerId::new("timer-duplicate");

    put_run_in_timer_wait(&workflow, &run_store, &run_id).await;
    let mut timers = AgentTimerStore::new(timer_store);
    timers
        .recover(ts(100))
        .await
        .expect("timer store should recover");
    timers
        .schedule_timer(timer(&workflow, &run_id, &timer_id, 500))
        .await
        .expect("timer should schedule");

    let mut scanner = AgentTimerScanner::with_clock(
        workflow.clone(),
        timers,
        workflow_store.clone(),
        run_store,
        clock.clone(),
        AgentTimerScannerSettings::new(8),
    );
    let scan = scanner
        .scan_due()
        .await
        .expect("due timer scan should fire");
    let fired_timer = scan.fired[0].timer.clone();

    let mut inbox = rakka_agent_workflow::AgentRunInbox::with_clock(run_id, workflow_store, clock);
    inbox.recover().await.expect("inbox should recover");
    let duplicate = inbox
        .accept_command(
            timer_fired_command(&fired_timer, ts(500)).expect("timer command should build"),
        )
        .await
        .expect("duplicate timer command should be durable");
    assert!(duplicate.is_duplicate());
    assert_eq!(
        duplicate.duplicate_reason(),
        Some(AgentInboxDuplicateReason::MessageId)
    );
}

#[tokio::test]
async fn scanner_bounds_due_work_and_reports_late_firing() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let timer_store = TimerStoreBackend::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(900));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let first_run = AgentRunId::new("run-timer-late-a");
    let second_run = AgentRunId::new("run-timer-late-b");

    put_run_in_timer_wait(&workflow, &run_store, &first_run).await;
    put_run_in_timer_wait(&workflow, &run_store, &second_run).await;

    let mut timers = AgentTimerStore::new(timer_store);
    timers
        .recover(ts(100))
        .await
        .expect("timer store should recover");
    timers
        .schedule_timer(timer(
            &workflow,
            &first_run,
            &AgentTimerId::new("timer-late-a"),
            500,
        ))
        .await
        .expect("first timer should schedule");
    timers
        .schedule_timer(timer(
            &workflow,
            &second_run,
            &AgentTimerId::new("timer-late-b"),
            600,
        ))
        .await
        .expect("second timer should schedule");

    let mut scanner = AgentTimerScanner::with_clock_and_metrics(
        workflow,
        timers,
        workflow_store,
        run_store,
        clock,
        AgentTimerScannerSettings::new(1),
        metrics.clone(),
    );
    let scan = scanner
        .scan_due()
        .await
        .expect("bounded scan should fire one timer");
    assert_eq!(scan.due_timer_count, 2);
    assert_eq!(scan.fired.len(), 1);
    assert!(scan.backpressure_limited);
    assert_eq!(scan.fired[0].late_by_ms, 400);
    assert_eq!(
        scan.fired[0].timer.timer_id,
        AgentTimerId::new("timer-late-a")
    );
}

async fn put_run_in_timer_wait(workflow: &AgentWorkflow, store: &RunStore, run_id: &AgentRunId) {
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());
    runner.recover().await.expect("runner should recover");
    runner
        .start(accepted_run_state(
            workflow,
            run_id,
            AgentStepId::new("plan"),
        ))
        .await
        .expect("run should start");
    runner
        .begin_step(ts(200))
        .await
        .expect("run should begin step");
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
    runner.recover().await.expect("runner should recover");
    runner
        .state()
        .expect("runner state should be readable")
        .cloned()
        .expect("run state should exist")
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
        AgentTenantId::new("tenant-timer"),
        ts(due_at),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("timer:{}", timer_id.as_str())),
            AgentCausationId::new(format!("cause:{}", timer_id.as_str())),
            AgentCorrelationId::new(format!("corr:{}", timer_id.as_str())),
        ),
        ts(100),
    )
    .expect("timer entry should be valid")
    .policy(
        AgentTimerPolicy::new()
            .policy_name("step-timeout")
            .max_lateness_ms(1_000),
    )
    .expect("timer policy should be valid")
}

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-timer"),
        workflow_type: "timer".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Timer workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::WaitingForTimer.as_label().to_string(),
        ],
        command_types: vec![
            rakka_agent_workflow::AgentCommandKind::StartRun
                .type_name()
                .to_string(),
            rakka_agent_workflow::AgentCommandKind::TimerFired {
                timer_id: "timer".to_string(),
            }
            .type_name()
            .to_string(),
        ],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan".to_string()),
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

fn accepted_run_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    first_step_id: AgentStepId,
) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-timer")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(first_step_id),
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
