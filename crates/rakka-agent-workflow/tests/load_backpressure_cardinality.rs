//! Load, back-pressure, and metric-cardinality coverage for agent workflows.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rakka_agent_workflow::{
    validate_agent_metric_attributes, AgentCausationId, AgentCommandKind, AgentCorrelationId,
    AgentDeduplicationKey, AgentDispatchConcurrencyLimits, AgentDispatchTargetClass,
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorker,
    AgentDispatcherWorkerId, AgentDurabilityMetadata, AgentEffect, AgentEffectId, AgentEffectKind,
    AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget, AgentIdempotencyKey, AgentRunId,
    AgentRunInbox, AgentRunIndexEntry, AgentRunState, AgentRunStatus, AgentRunTransitionKind,
    AgentRunWaitReason, AgentStatePayload, AgentStep, AgentStepId, AgentStepKind, AgentStepRunner,
    AgentTenantId, AgentTimerEntry, AgentTimerId, AgentTimerIndexEntry, AgentTimerPolicy,
    AgentTimerQuery, AgentTimerScanner, AgentTimerScannerSettings, AgentTimerStatus,
    AgentTimerStore, AgentTimerStoreState, AgentTimestampMillis, AgentWorkflow, AgentWorkflowId,
    AgentWorkflowQueryIndex, AgentWorkflowRunQuery, HumanCheckpoint, HumanCheckpointId,
    HumanCheckpointStatus, HumanDecisionOption, InMemoryAgentWorkflowQueryIndex, PrincipalRef,
    StateSchemaVersion, WorkflowDefinitionVersion, FORBIDDEN_HOT_METRIC_FIELDS,
    METRIC_AGENT_DISPATCHER_BACKLOG, METRIC_AGENT_DISPATCHER_FLEET,
    METRIC_AGENT_DISPATCHER_IN_FLIGHT, METRIC_AGENT_INBOX_COMMANDS, METRIC_AGENT_TIMERS,
    METRIC_AGENT_TIMERS_LATE_BY_MS,
};
use rakka_core::{InMemoryMetricsRecorder, MetricKind, MetricsSnapshot};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};

type RunStore = InMemoryDurableStateStore<AgentRunState>;
type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type FleetStore = InMemoryDurableStateStore<AgentDispatcherFleetState>;
type TimerStoreBackend = InMemoryDurableStateStore<AgentTimerStoreState>;
type TestWorker = AgentDispatcherWorker<FleetStore, WorkflowStore, ManualWorkflowClock>;

const DISPATCH_RUN_COUNT: usize = 64;
const DISPATCH_TARGET_LIMIT: usize = 5;
const TIMER_RUN_COUNT: usize = 36;
const TIMER_BATCH_SIZE: usize = 7;
const QUERY_RUN_COUNT: usize = 128;

#[tokio::test]
async fn dispatcher_load_claims_bounded_work_and_keeps_metric_series_bounded() {
    let workflow_store = WorkflowStore::new();
    let fleet_store = FleetStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(1_000));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());

    for index in 0..DISPATCH_RUN_COUNT {
        let run_id = AgentRunId::new(format!("run-load-dispatch-{index:03}"));
        schedule_effect(
            &workflow_store,
            &clock,
            run_id,
            effect(
                format!("effect-load-dispatch-{index:03}"),
                AgentEffectKind::ModelCall,
                "model",
                "shared-chat-model",
                1_000,
            ),
        )
        .await;
    }

    let limits = AgentDispatchConcurrencyLimits::new(DISPATCH_RUN_COUNT).target_limit(
        AgentDispatchTargetClass::Model,
        "shared-chat-model",
        DISPATCH_TARGET_LIMIT,
    );
    let settings = AgentDispatcherFleetSettings::new(16, 30_000).concurrency_limits(limits);
    let mut worker = worker(
        "dispatcher-load",
        fleet_store,
        workflow_store,
        clock,
        metrics.clone(),
        settings,
    );
    worker.recover().await.expect("fleet should recover");
    for index in 0..DISPATCH_RUN_COUNT {
        worker
            .refresh_run(
                AgentRunId::new(format!("run-load-dispatch-{index:03}")),
                None,
            )
            .await
            .expect("due effect should be indexed");
    }

    let batch = worker
        .claim_due()
        .await
        .expect("claim pass should apply target limit");
    assert_eq!(batch.due_dispatch_count, DISPATCH_RUN_COUNT);
    assert_eq!(batch.claims.len(), DISPATCH_TARGET_LIMIT);
    assert_eq!(
        batch.concurrency_limited,
        DISPATCH_RUN_COUNT - DISPATCH_TARGET_LIMIT
    );
    assert!(batch.backpressure_limited);

    let snapshot = worker.fleet().snapshot(3);
    assert_eq!(snapshot.observed_dispatch_count, DISPATCH_RUN_COUNT);
    assert_eq!(snapshot.in_flight_count, DISPATCH_TARGET_LIMIT);
    assert_eq!(
        snapshot.due_dispatch_count,
        DISPATCH_RUN_COUNT - DISPATCH_TARGET_LIMIT
    );
    assert_eq!(snapshot.sampled_entries.len(), 3);
    let encoded = serde_json::to_vec(&snapshot).expect("snapshot should serialize");
    assert!(
        encoded.len() < 4_096,
        "bounded dispatcher snapshot should stay compact under load: {} bytes",
        encoded.len()
    );

    let snapshot = metrics.snapshot();
    assert_all_metric_attributes_are_bounded(&snapshot);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_DISPATCHER_FLEET) <= 2);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_DISPATCHER_IN_FLIGHT) <= 1);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_DISPATCHER_BACKLOG) <= 1);
}

#[tokio::test]
async fn timer_backlog_load_uses_batch_limit_and_bounded_metrics() {
    let workflow = workflow();
    let run_store = RunStore::new();
    let workflow_store = WorkflowStore::new();
    let timer_store = TimerStoreBackend::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(1_000));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());

    for index in 0..TIMER_RUN_COUNT {
        let run_id = AgentRunId::new(format!("run-load-timer-{index:03}"));
        put_run_in_timer_wait(&workflow, &run_store, &run_id).await;
    }

    let mut timers = AgentTimerStore::new(timer_store);
    timers
        .recover(ts(100))
        .await
        .expect("timer store should recover");
    for index in 0..TIMER_RUN_COUNT {
        let run_id = AgentRunId::new(format!("run-load-timer-{index:03}"));
        timers
            .schedule_timer(timer(
                &workflow,
                &run_id,
                &AgentTimerId::new(format!("timer-load-{index:03}")),
                500 + index as u64,
            ))
            .await
            .expect("timer should schedule");
    }

    let mut scanner = AgentTimerScanner::with_clock_and_metrics(
        workflow,
        timers,
        workflow_store,
        run_store,
        clock,
        AgentTimerScannerSettings::new(TIMER_BATCH_SIZE),
        metrics.clone(),
    );
    let scan = scanner.scan_due().await.expect("bounded scan should run");
    assert_eq!(scan.due_timer_count, TIMER_RUN_COUNT);
    assert_eq!(scan.max_batch_size, TIMER_BATCH_SIZE);
    assert_eq!(scan.fired.len(), TIMER_BATCH_SIZE);
    assert!(scan.backpressure_limited);
    assert!(scan
        .fired
        .iter()
        .all(|firing| firing.inbox_acceptance.is_accepted()));
    assert!(scan.fired.iter().all(|firing| {
        firing
            .transition
            .as_ref()
            .is_some_and(|transition| transition.kind == AgentRunTransitionKind::Resume)
    }));

    let remaining = scanner
        .timers_mut()
        .due_timer_count(ts(1_000))
        .await
        .expect("remaining due timers should count");
    assert_eq!(remaining, TIMER_RUN_COUNT - TIMER_BATCH_SIZE);

    let snapshot = metrics.snapshot();
    assert_all_metric_attributes_are_bounded(&snapshot);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_TIMERS) <= 1);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_TIMERS_LATE_BY_MS) <= 1);
    assert!(metric_series_count(&snapshot, METRIC_AGENT_INBOX_COMMANDS) <= 1);
}

#[tokio::test]
async fn query_views_stay_bounded_under_large_run_counts() {
    let mut index = InMemoryAgentWorkflowQueryIndex::new();

    for run_number in 0..QUERY_RUN_COUNT {
        let status = match run_number % 4 {
            0 => AgentRunStatus::Running,
            1 => AgentRunStatus::WaitingForTimer,
            2 => AgentRunStatus::WaitingForHuman,
            _ => AgentRunStatus::Failed,
        };
        let checkpoint = (status == AgentRunStatus::WaitingForHuman).then(|| {
            checkpoint(
                format!("checkpoint-load-{run_number:03}"),
                run_number as u64,
            )
        });
        let run = run_state(
            format!("run-query-load-{run_number:03}"),
            status,
            100 + run_number as u64,
            checkpoint,
        );
        index
            .upsert_run(AgentRunIndexEntry::from_run_state(&run, "load-test").namespace("prod"))
            .await
            .expect("run should index");

        if status == AgentRunStatus::WaitingForTimer {
            index
                .upsert_timer(
                    AgentTimerIndexEntry::from_timer_entry(&timer(
                        &workflow(),
                        &run.run_id,
                        &AgentTimerId::new(format!("timer-query-load-{run_number:03}")),
                        600 + run_number as u64,
                    ))
                    .namespace("prod"),
                )
                .await
                .expect("timer should index");
        }
    }

    let waiting = index
        .query_runs(AgentWorkflowRunQuery::new().waiting().limit(13))
        .await
        .expect("waiting query should run");
    assert_eq!(waiting.len(), 13);
    assert!(waiting.iter().all(|entry| matches!(
        entry.status,
        AgentRunStatus::WaitingForTimer | AgentRunStatus::WaitingForHuman
    )));

    let due_timer_runs = index
        .query_runs(
            AgentWorkflowRunQuery::new()
                .due_timer_at_or_before(ts(700))
                .limit(11),
        )
        .await
        .expect("due timer run query should run");
    assert_eq!(due_timer_runs.len(), 11);
    assert!(due_timer_runs
        .iter()
        .all(|entry| entry.status == AgentRunStatus::WaitingForTimer));

    let due_timers = index
        .query_timers(
            AgentTimerQuery::new()
                .status(AgentTimerStatus::Pending)
                .due_at_or_before(ts(700))
                .limit(17),
        )
        .await
        .expect("timer query should run");
    assert_eq!(due_timers.len(), 17);
    assert!(due_timers
        .windows(2)
        .all(|window| window[0].due_at <= window[1].due_at));

    let too_wide = index
        .query_runs(AgentWorkflowRunQuery::new().limit(0))
        .await
        .expect_err("zero limit should be rejected");
    assert_eq!(too_wide.code(), "invalid-workflow-query");
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

async fn put_run_in_timer_wait(workflow: &AgentWorkflow, store: &RunStore, run_id: &AgentRunId) {
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());
    runner.recover().await.expect("runner should recover");
    runner
        .start(accepted_run_state(
            workflow,
            run_id,
            AgentStepId::new("load-review"),
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

fn effect(
    effect_id: impl Into<String>,
    kind: AgentEffectKind,
    target_type: &str,
    target_name: &str,
    due_at: u64,
) -> AgentEffect {
    let effect_id = effect_id.into();
    let metadata = AgentEffectMetadata::new(
        AgentEffectId::new(effect_id.as_str()),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("dedupe:{effect_id}")),
            AgentCausationId::new(format!("cause:{effect_id}")),
            AgentCorrelationId::new("corr:load"),
        ),
        AgentIdempotencyKey::new(format!("idempotency:{effect_id}")),
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
        AgentTenantId::new("tenant-load"),
        ts(due_at),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(format!("timer:{}", timer_id.as_str())),
            AgentCausationId::new("cause:timer-load"),
            AgentCorrelationId::new("corr:timer-load"),
        ),
        ts(100),
    )
    .expect("timer entry should validate")
    .policy(
        AgentTimerPolicy::new()
            .policy_name("load-timeout")
            .max_lateness_ms(5_000),
    )
    .expect("timer policy should validate")
}

fn run_state(
    run_id: impl Into<String>,
    status: AgentRunStatus,
    updated_at: u64,
    checkpoint: Option<HumanCheckpoint>,
) -> AgentRunState {
    let run_id = run_id.into();
    AgentRunState {
        run_id: AgentRunId::new(run_id),
        workflow_id: workflow().workflow_id,
        tenant: Some(AgentTenantId::new("tenant-load")),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        status,
        current_step_id: Some(AgentStepId::new("load-review")),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: checkpoint.into_iter().collect(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: (status == AgentRunStatus::WaitingForHuman)
            .then(|| HumanCheckpointId::new("checkpoint-load")),
        cancellation: None,
        created_at: ts(100),
        updated_at: ts(updated_at),
        completed_at: matches!(
            status,
            AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
        )
        .then(|| ts(updated_at)),
    }
}

fn checkpoint(checkpoint_id: impl Into<String>, created_at: u64) -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new(checkpoint_id.into()),
        status: HumanCheckpointStatus::Open,
        summary: "Review load-test checkpoint".to_string(),
        available_decisions: vec![HumanDecisionOption {
            value: "approve".to_string(),
            label: "Approve".to_string(),
            requires_comment: false,
        }],
        required_roles: vec!["reviewer".to_string()],
        due_at: Some(ts(created_at + 1_000)),
        escalation_target: Some("workflow-ops".to_string()),
        context_artifacts: Vec::new(),
        created_by: Some(PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "load-harness".to_string(),
            display_name: Some("Load Harness".to_string()),
        }),
        resolved_by: None,
        created_at: ts(created_at),
        resolved_at: None,
        audit_event_ids: Vec::new(),
    }
}

fn accepted_run_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    step_id: AgentStepId,
) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-load")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(step_id),
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

fn workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-load-backpressure"),
        workflow_type: "load-backpressure".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Load Backpressure Workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::WaitingForTimer.as_label().to_string(),
            AgentRunStatus::WaitingForHuman.as_label().to_string(),
            AgentRunStatus::Failed.as_label().to_string(),
        ],
        command_types: vec![
            AgentCommandKind::StartRun.type_name().to_string(),
            AgentCommandKind::TimerFired {
                timer_id: "timer".to_string(),
            }
            .type_name()
            .to_string(),
        ],
        steps: vec![AgentStep {
            step_id: AgentStepId::new("load-review"),
            kind: AgentStepKind::Planner,
            display_name: Some("Load Review".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(5_000),
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

fn target(target_type: &str, target_name: &str) -> AgentEffectTarget {
    AgentEffectTarget {
        target_type: target_type.to_string(),
        name: target_name.to_string(),
        address: Some(format!("{target_type}://{target_name}")),
        attributes: BTreeMap::new(),
    }
}

fn metric_series_count(snapshot: &MetricsSnapshot, name: &str) -> usize {
    snapshot
        .observations_named(name)
        .into_iter()
        .map(|observation| {
            let mut attributes: Vec<_> = observation
                .attributes()
                .iter()
                .map(|attribute| (attribute.key().to_string(), attribute.value().to_string()))
                .collect();
            attributes.sort();
            (observation.kind(), attributes)
        })
        .collect::<BTreeSet<(MetricKind, Vec<(String, String)>)>>()
        .len()
}

fn assert_all_metric_attributes_are_bounded(snapshot: &MetricsSnapshot) {
    let forbidden_runtime_ids = [
        "run_id",
        "effect_id",
        "timer_id",
        "command_id",
        "worker_id",
        "idempotency_key",
        "correlation_id",
        "causation_id",
    ];
    for observation in snapshot.observations() {
        let attributes: Vec<_> = observation
            .attributes()
            .iter()
            .map(|attribute| (attribute.key(), attribute.value()))
            .collect();
        validate_agent_metric_attributes(&attributes).unwrap_or_else(|error| {
            panic!(
                "metric {} has unbounded attributes {:?}: {error}",
                observation.name(),
                observation.attributes()
            )
        });
        for attribute in observation.attributes() {
            assert!(
                !forbidden_runtime_ids.contains(&attribute.key())
                    && !FORBIDDEN_HOT_METRIC_FIELDS.contains(&attribute.key()),
                "metric {} leaked high-cardinality attribute {}={}",
                observation.name(),
                attribute.key(),
                attribute.value()
            );
        }
    }
}

const fn ts(value: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(value)
}
