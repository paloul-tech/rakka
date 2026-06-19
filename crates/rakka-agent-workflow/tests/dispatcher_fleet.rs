//! Durable dispatcher fleet tests for agent workflow outbox effects.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use rakka_agent_workflow::{
    AgentCausationId, AgentCorrelationId, AgentDeduplicationKey, AgentDispatchConcurrencyLimits,
    AgentDispatchStatus, AgentDispatchTargetClass, AgentDispatcherError,
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorker,
    AgentDispatcherWorkerId, AgentDurabilityMetadata, AgentEffect, AgentEffectDispatchFuture,
    AgentEffectDispatcher, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
    AgentEffectSchedule, AgentEffectTarget, AgentIdempotencyKey, AgentRunId, AgentRunInbox,
    AgentTimestampMillis,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    ManualWorkflowClock, OutboxDispatchResult, OutboxMessageId, OutboxStatus, WorkflowState,
    WorkflowTimestamp,
};

type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type FleetStore = InMemoryDurableStateStore<AgentDispatcherFleetState>;
type TestWorker = AgentDispatcherWorker<FleetStore, WorkflowStore, ManualWorkflowClock>;

#[tokio::test]
async fn multiple_workers_do_not_claim_same_effect_concurrently() {
    let workflow_store = WorkflowStore::new();
    let fleet_store = FleetStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-dispatch-claim");
    let effect_id = "effect-dispatch-claim";

    schedule_effect(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect(
            effect_id,
            AgentEffectKind::ToolCall,
            "tool",
            "research-tool",
            100,
        ),
    )
    .await;

    let settings = AgentDispatcherFleetSettings::new(8, 1_000);
    let mut worker_a = worker(
        "dispatcher-a",
        fleet_store.clone(),
        workflow_store.clone(),
        clock.clone(),
        metrics.clone(),
        settings.clone(),
    );
    let mut worker_b = worker(
        "dispatcher-b",
        fleet_store.clone(),
        workflow_store.clone(),
        clock.clone(),
        metrics.clone(),
        settings,
    );

    worker_a
        .recover()
        .await
        .expect("worker A should recover fleet");
    worker_a
        .refresh_run(run_id.clone(), None)
        .await
        .expect("worker A should index due effect");
    let first_batch = worker_a
        .claim_due()
        .await
        .expect("worker A should claim due effect");
    assert_eq!(first_batch.due_dispatch_count, 1);
    assert_eq!(first_batch.claims.len(), 1);

    worker_b
        .recover()
        .await
        .expect("worker B should recover claimed fleet state");
    let second_batch = worker_b
        .claim_due()
        .await
        .expect("worker B should not claim active lease");
    assert_eq!(second_batch.due_dispatch_count, 0);
    assert!(second_batch.claims.is_empty());

    let mut dispatcher = RecordingDispatcher::new([OutboxDispatchResult::Success]);
    let completion = worker_a
        .dispatch_claim(first_batch.claims[0].clone(), &mut dispatcher)
        .await
        .expect("claimed effect should dispatch successfully");
    assert_eq!(completion.entry.status, AgentDispatchStatus::Completed);
    assert_eq!(dispatcher.seen.len(), 1);
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
async fn expired_claim_after_dispatching_is_recoverable_by_another_worker() {
    let workflow_store = WorkflowStore::new();
    let fleet_store = FleetStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-dispatch-crash");
    let effect_id = "effect-dispatch-crash";

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
    let mut worker_a = worker(
        "dispatcher-crash-a",
        fleet_store.clone(),
        workflow_store.clone(),
        clock.clone(),
        metrics.clone(),
        settings.clone(),
    );
    worker_a.recover().await.expect("fleet should recover");
    worker_a
        .refresh_run(run_id.clone(), None)
        .await
        .expect("due effect should index");
    let first_claim = worker_a
        .claim_due()
        .await
        .expect("worker should claim")
        .claims
        .pop()
        .expect("one claim should be issued");

    let mut expiring_dispatcher = ExpiringDispatcher {
        clock: clock.clone(),
        advance_ms: 20,
    };
    let error = worker_a
        .dispatch_claim(first_claim.clone(), &mut expiring_dispatcher)
        .await
        .expect_err("lease expiration should fence result persistence");
    assert!(matches!(error, AgentDispatcherError::ClaimFenced { .. }));
    assert_outbox_status(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect_id,
        OutboxStatus::Dispatching,
    )
    .await;

    let mut worker_b = worker(
        "dispatcher-crash-b",
        fleet_store,
        workflow_store.clone(),
        clock.clone(),
        metrics,
        settings,
    );
    worker_b
        .recover()
        .await
        .expect("second worker should recover fleet");
    worker_b
        .refresh_run(run_id.clone(), None)
        .await
        .expect("dispatching outbox entry should be rediscovered");
    let second_claim = worker_b
        .claim_due()
        .await
        .expect("expired claim should be claimable")
        .claims
        .pop()
        .expect("one recovered claim should be issued");
    assert!(second_claim.fencing_token > first_claim.fencing_token);

    let mut dispatcher = RecordingDispatcher::new([OutboxDispatchResult::Success]);
    let completion = worker_b
        .dispatch_claim(second_claim, &mut dispatcher)
        .await
        .expect("recovered claim should dispatch");
    assert_eq!(completion.entry.status, AgentDispatchStatus::Completed);
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
async fn target_concurrency_limits_bound_claims() {
    let workflow_store = WorkflowStore::new();
    let fleet_store = FleetStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-dispatch-limits");

    schedule_effect(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect(
            "effect-model-a",
            AgentEffectKind::ModelCall,
            "model",
            "chat-model",
            100,
        ),
    )
    .await;
    schedule_effect(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect(
            "effect-model-b",
            AgentEffectKind::ModelCall,
            "model",
            "chat-model",
            100,
        ),
    )
    .await;

    let limits = AgentDispatchConcurrencyLimits::new(8).target_limit(
        AgentDispatchTargetClass::Model,
        "chat-model",
        1,
    );
    let settings = AgentDispatcherFleetSettings::new(8, 1_000).concurrency_limits(limits);
    let mut worker = worker(
        "dispatcher-limits",
        fleet_store,
        workflow_store,
        clock,
        metrics,
        settings,
    );
    worker.recover().await.expect("fleet should recover");
    worker
        .refresh_run(run_id, None)
        .await
        .expect("due effects should index");

    let batch = worker
        .claim_due()
        .await
        .expect("claim pass should honor limits");
    assert_eq!(batch.due_dispatch_count, 2);
    assert_eq!(batch.claims.len(), 1);
    assert_eq!(batch.concurrency_limited, 1);
    assert!(batch.backpressure_limited);

    let snapshot = worker.fleet().snapshot(8);
    assert_eq!(snapshot.in_flight_count, 1);
    assert_eq!(snapshot.due_dispatch_count, 1);
}

#[tokio::test]
async fn failed_dispatch_records_retry_after_before_reclaim() {
    let workflow_store = WorkflowStore::new();
    let fleet_store = FleetStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-dispatch-retry");
    let effect_id = "effect-dispatch-retry";

    schedule_effect(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect(effect_id, AgentEffectKind::GrpcCall, "grpc", "billing", 100),
    )
    .await;

    let settings = AgentDispatcherFleetSettings::new(8, 5_000);
    let mut worker = worker(
        "dispatcher-retry",
        fleet_store,
        workflow_store.clone(),
        clock.clone(),
        metrics,
        settings,
    );
    worker.recover().await.expect("fleet should recover");
    worker
        .refresh_run(run_id.clone(), None)
        .await
        .expect("effect should index");
    let claim = worker
        .claim_due()
        .await
        .expect("claim should succeed")
        .claims
        .pop()
        .expect("one claim should be issued");

    let mut dispatcher = RecordingDispatcher::new([OutboxDispatchResult::failure("rate-limit")]);
    let completion = worker
        .dispatch_claim(claim, &mut dispatcher)
        .await
        .expect("failure should be recorded durably");
    assert_eq!(completion.entry.status, AgentDispatchStatus::RetryScheduled);
    assert_eq!(completion.entry.due_at, AgentTimestampMillis::new(1_100));
    assert_outbox_status(
        &workflow_store,
        &clock,
        run_id.clone(),
        effect_id,
        OutboxStatus::Failed,
    )
    .await;

    assert!(worker
        .claim_due()
        .await
        .expect("retry should not be due yet")
        .claims
        .is_empty());

    clock.set(WorkflowTimestamp::from_millis(1_100));
    let retry_batch = worker.claim_due().await.expect("retry should become due");
    assert_eq!(retry_batch.claims.len(), 1);
    assert_eq!(
        retry_batch.claims[0].effect_id,
        AgentEffectId::new(effect_id)
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

fn effect(
    effect_id: &str,
    kind: AgentEffectKind,
    target_type: &str,
    target_name: &str,
    due_at: u64,
) -> AgentEffect {
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("dedupe-{effect_id}")),
        AgentCausationId::new(format!("cause-{effect_id}")),
        AgentCorrelationId::new(format!("correlation-{effect_id}")),
    );
    let metadata = AgentEffectMetadata::new(
        AgentEffectId::new(effect_id),
        durability,
        AgentIdempotencyKey::new(format!("idempotency-{effect_id}")),
        AgentTimestampMillis::new(100),
    )
    .expect("effect metadata should be valid")
    .due_at(AgentTimestampMillis::new(due_at));

    AgentEffectSchedule::new(kind, target(target_type, target_name), metadata)
        .expect("effect schedule should be valid")
        .expected_result_type("dispatch.result")
        .expect("expected result type should be valid")
        .into_effect()
        .expect("effect should validate")
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

struct ExpiringDispatcher {
    clock: ManualWorkflowClock,
    advance_ms: u64,
}

impl AgentEffectDispatcher for ExpiringDispatcher {
    fn dispatch<'a>(
        &'a mut self,
        _job: &'a rakka_agent_workflow::AgentDispatchJob,
    ) -> AgentEffectDispatchFuture<'a> {
        self.clock.advance_millis(self.advance_ms);
        Box::pin(async { OutboxDispatchResult::Success })
    }
}
