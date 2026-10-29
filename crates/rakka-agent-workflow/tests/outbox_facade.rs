//! Durable outbox facade tests for agent effects.

use std::collections::BTreeMap;
use std::sync::Arc;

use rakka_agent_workflow::{
    agent_effect_to_outbox_command, AgentCausationId, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
    AgentEffectSchedule, AgentEffectStatus, AgentEffectTarget, AgentIdempotencyKey,
    AgentOutboxDuplicateReason, AgentRunId, AgentRunInbox, AgentSpanLink, AgentTelemetryContext,
    AgentTimestampMillis, METRIC_AGENT_OUTBOX_EFFECTS,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    ManualWorkflowClock, OutboxStatus, OutboxTarget, WorkflowState, WorkflowTimestamp,
};

type TestStore = InMemoryDurableStateStore<WorkflowState>;
type TestInbox = AgentRunInbox<TestStore, ManualWorkflowClock>;

#[tokio::test]
async fn schedule_effect_persists_full_agent_effect_payload_and_recovers_due_work() {
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-effect-persisted");
    let mut inbox = agent_inbox(
        run_id.clone(),
        store.clone(),
        clock.clone(),
        metrics.clone(),
    );
    inbox.recover().await.expect("inbox should recover");

    assert!(inbox
        .due_effects()
        .expect("empty recovered outbox should be queryable")
        .is_empty());

    let effect = effect("effect-model-1", "dedupe-model-1", None);
    let scheduled = inbox
        .schedule_effect(effect.clone())
        .await
        .expect("effect should schedule durably");

    assert!(scheduled.is_scheduled());
    assert_eq!(scheduled.revision(), rakka_persistence::Revision::new(1));
    assert_eq!(scheduled.entry().message_id().as_str(), "effect-model-1");
    assert_eq!(scheduled.entry().message_type(), effect.message_type());
    assert_eq!(scheduled.entry().status(), OutboxStatus::Pending);
    assert_eq!(
        scheduled.entry().target(),
        &OutboxTarget::application("research-tool")
    );

    let persisted: AgentEffect = serde_json::from_slice(scheduled.entry().payload())
        .expect("outbox payload should contain serialized effect");
    assert_eq!(persisted, effect);
    assert_eq!(
        persisted.causation_id,
        AgentCausationId::new("cause-effect-model-1")
    );
    assert_eq!(
        persisted.correlation_id,
        AgentCorrelationId::new("correlation-effect-model-1")
    );
    assert_eq!(
        persisted.telemetry_context.trace_parent.as_deref(),
        Some("00-00000000000000000000000000000001-0000000000000001-01")
    );

    let due = inbox
        .due_effects()
        .expect("scheduled effect should be discoverable as due");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].effect, effect);
    assert_eq!(due[0].entry, *scheduled.entry());

    let mut recovered = agent_inbox(run_id, store, clock, metrics.clone());
    recovered
        .recover()
        .await
        .expect("fresh inbox should recover persisted outbox");
    let recovered_due = recovered
        .due_effects()
        .expect("fresh inbox should discover persisted due effect");
    assert_eq!(recovered_due.len(), 1);
    assert_eq!(recovered_due[0].effect, effect);

    assert_eq!(
        metrics
            .snapshot()
            .observations_named(METRIC_AGENT_OUTBOX_EFFECTS)
            .len(),
        1
    );
}

#[tokio::test]
async fn duplicate_effects_return_existing_durable_entry() {
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = agent_inbox(
        AgentRunId::new("run-effect-duplicates"),
        store,
        clock,
        metrics,
    );
    inbox.recover().await.expect("inbox should recover");

    let first = effect("effect-duplicate-1", "dedupe-duplicate", None);
    let scheduled = inbox
        .schedule_effect(first.clone())
        .await
        .expect("first effect should schedule");
    assert!(scheduled.is_scheduled());

    let duplicate_by_message_id = inbox
        .schedule_effect(first)
        .await
        .expect("same effect should be duplicate");
    assert!(duplicate_by_message_id.is_duplicate());
    assert_eq!(
        duplicate_by_message_id.duplicate_reason(),
        Some(AgentOutboxDuplicateReason::MessageId)
    );
    assert_eq!(
        duplicate_by_message_id.entry().message_id().as_str(),
        "effect-duplicate-1"
    );

    let duplicate_by_key = effect("effect-duplicate-2", "dedupe-duplicate", None);
    let duplicate_by_deduplication_key = inbox
        .schedule_effect(duplicate_by_key)
        .await
        .expect("shared deduplication key should be duplicate");
    assert!(duplicate_by_deduplication_key.is_duplicate());
    assert_eq!(
        duplicate_by_deduplication_key.duplicate_reason(),
        Some(AgentOutboxDuplicateReason::DeduplicationKey)
    );
    assert_eq!(
        duplicate_by_deduplication_key.entry().message_id().as_str(),
        "effect-duplicate-1"
    );
}

#[tokio::test]
async fn due_effect_discovery_respects_effect_due_at_after_recovery() {
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-effect-due-at");
    let mut inbox = agent_inbox(
        run_id.clone(),
        store.clone(),
        clock.clone(),
        metrics.clone(),
    );
    inbox.recover().await.expect("inbox should recover");

    let delayed = effect(
        "effect-delayed-1",
        "dedupe-delayed-1",
        Some(AgentTimestampMillis::new(500)),
    );
    let scheduled = inbox
        .schedule_effect(delayed.clone())
        .await
        .expect("delayed effect should schedule");
    assert_eq!(
        scheduled.entry().scheduled_at(),
        WorkflowTimestamp::from_millis(500)
    );
    assert!(inbox
        .due_effects()
        .expect("outbox should be queryable before due time")
        .is_empty());

    clock.set(WorkflowTimestamp::from_millis(500));
    let due = inbox
        .due_effects()
        .expect("delayed effect should become due");
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].effect, delayed);

    let mut recovered = agent_inbox(run_id, store, clock, metrics);
    recovered
        .recover()
        .await
        .expect("fresh inbox should recover delayed effect");
    let recovered_due = recovered
        .due_effects()
        .expect("recovered delayed effect should remain due");
    assert_eq!(recovered_due.len(), 1);
    assert_eq!(
        recovered_due[0].entry.message_id().as_str(),
        "effect-delayed-1"
    );
}

#[tokio::test]
async fn invalid_effect_is_rejected_before_persistence() {
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = agent_inbox(AgentRunId::new("run-effect-invalid"), store, clock, metrics);
    inbox.recover().await.expect("inbox should recover");

    let mut invalid = effect("effect-invalid-1", "dedupe-invalid-1", None);
    invalid.status = AgentEffectStatus::Completed;

    let error = inbox
        .schedule_effect(invalid)
        .await
        .expect_err("non-scheduled effect should be rejected");
    assert_eq!(error.code(), "rejected-effect");
    assert!(inbox
        .due_effects()
        .expect("rejected effect should not create outbox work")
        .is_empty());
}

fn agent_inbox(
    run_id: AgentRunId,
    store: TestStore,
    clock: ManualWorkflowClock,
    metrics: Arc<InMemoryMetricsRecorder>,
) -> TestInbox {
    AgentRunInbox::with_clock_and_metrics(run_id, store, clock, metrics)
}

fn effect(
    effect_id: &str,
    deduplication_key: &str,
    due_at: Option<AgentTimestampMillis>,
) -> AgentEffect {
    let telemetry_context = AgentTelemetryContext {
        trace_parent: Some("00-00000000000000000000000000000001-0000000000000001-01".to_string()),
        trace_state: Some("vendor=value".to_string()),
        baggage: BTreeMap::from([("tenant_tier".to_string(), "test".to_string())]),
        span_links: vec![AgentSpanLink {
            trace_id: "00000000000000000000000000000002".to_string(),
            span_id: "0000000000000002".to_string(),
            trace_state: None,
            attributes: BTreeMap::from([("link_kind".to_string(), "retry".to_string())]),
        }],
    };
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(deduplication_key),
        AgentCausationId::new(format!("cause-{effect_id}")),
        AgentCorrelationId::new(format!("correlation-{effect_id}")),
    )
    .telemetry_context(telemetry_context);
    let mut metadata = AgentEffectMetadata::new(
        AgentEffectId::new(effect_id),
        durability,
        AgentIdempotencyKey::new(format!("idempotency-{effect_id}")),
        AgentTimestampMillis::new(100),
    )
    .expect("effect metadata should be valid")
    .timeout_ms(2_000);
    if let Some(due_at) = due_at {
        metadata = metadata.due_at(due_at);
    }

    AgentEffectSchedule::new(AgentEffectKind::ToolCall, target(), metadata)
        .expect("effect schedule should be valid")
        .expected_result_type("tool.result")
        .expect("expected result type should be valid")
        .into_effect()
        .expect("effect should validate")
}

fn target() -> AgentEffectTarget {
    AgentEffectTarget {
        target_type: "tool".to_string(),
        name: "research-tool".to_string(),
        address: Some("tool://research".to_string()),
        attributes: BTreeMap::from([("tool_kind".to_string(), "research".to_string())]),
    }
}

#[test]
fn outbox_command_reads_the_attempt_bound_leniently() {
    let bound = |value: Option<&str>| {
        let mut effect = effect("effect-bounded-1", "dedupe-bounded-1", None);
        if let Some(value) = value {
            effect
                .target
                .attributes
                .insert("agent_effect_max_attempts".to_string(), value.to_string());
        }
        agent_effect_to_outbox_command(&effect)
            .expect("effect should convert")
            .retry_policy_value()
    };

    let default = rakka_workflow::RetryPolicy::default();
    assert_eq!(bound(None).max_attempts(), default.max_attempts());
    assert_eq!(bound(None).max_attempts(), 3);
    assert_eq!(bound(Some("many")).max_attempts(), 3);
    assert_eq!(bound(Some("0")).max_attempts(), 3);
    let four = bound(Some("4"));
    assert_eq!(four.max_attempts(), 4);
    assert_eq!(
        four.initial_backoff_millis(),
        default.initial_backoff_millis()
    );
    assert_eq!(four.max_backoff_millis(), default.max_backoff_millis());
}

#[tokio::test]
async fn outbox_effects_selects_before_decoding_and_skips_an_entry_that_does_not_decode() {
    let store = TestStore::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = agent_inbox(AgentRunId::new("run-outbox-read"), store, clock, metrics);
    inbox.recover().await.expect("inbox should recover");
    let effect = effect("effect-decodes-1", "dedupe-decodes-1", None);
    inbox
        .schedule_effect(effect.clone())
        .await
        .expect("effect should schedule");
    inbox
        .inner_mut()
        .schedule_outbox(rakka_workflow::OutboxCommand::new(
            "effect-undecodable-1",
            OutboxTarget::application("tool"),
            "agent.effect",
            b"{not an effect".to_vec(),
        ))
        .await
        .expect("raw entry should schedule");
    inbox
        .inner_mut()
        .record_outbox_success(&rakka_workflow::OutboxMessageId::new(
            "effect-undecodable-1",
        ))
        .await
        .expect("raw entry should conclude");

    let all = inbox
        .outbox_effects(|_entry| true)
        .expect("the read should not fail on an undecodable entry");
    assert_eq!(all.effects.len(), 1);
    assert_eq!(all.effects[0].effect, effect);
    assert_eq!(all.effects[0].entry.status(), OutboxStatus::Pending);
    assert_eq!(all.undecodable, 1);

    let only_decodable = inbox
        .outbox_effects(|entry| entry.message_id().as_str() == "effect-decodes-1")
        .expect("the read should succeed");
    assert_eq!(only_decodable.effects.len(), 1);
    assert_eq!(
        only_decodable.undecodable, 0,
        "an entry the selector drops is never decoded"
    );

    let none = inbox
        .outbox_effects(|_entry| false)
        .expect("the read should succeed");
    assert!(none.effects.is_empty());
    assert_eq!(none.undecodable, 0);
}
