//! Trigger source metadata contract tests.

use std::sync::Arc;

use rakka_agent_workflow::{
    trigger_cancel_run_command, trigger_human_decision_command, trigger_retry_run_command,
    trigger_start_run_command, trigger_submit_signal_command, validate_agent_metric_attributes,
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentInboxDuplicateReason,
    AgentRunId, AgentRunInbox, AgentTenantId, AgentTimestampMillis, AgentTriggerCommandBuilder,
    AgentTriggerSource, AgentTriggerSourceKind, AgentWorkflowId, ArtifactKind, ArtifactRef,
    HumanCheckpointId, RedactionStatus, AGENT_METRIC_ATTR_DEPLOYMENT_CHANNEL,
    AGENT_METRIC_ATTR_DETAIL, AGENT_METRIC_ATTR_TENANT_TIER, AGENT_METRIC_ATTR_TRIGGER_KIND,
    AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE, AGENT_TRIGGER_KIND_ATTRIBUTE,
    AGENT_TRIGGER_TENANT_TIER_ATTRIBUTE,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowState, WorkflowTimestamp};
use serde_json::json;

type TestStore = InMemoryDurableStateStore<WorkflowState>;

#[test]
fn trigger_source_round_trips_with_stable_kind_labels() {
    let source = AgentTriggerSource::webhook()
        .deployment_channel("prod")
        .expect("deployment channel should be accepted")
        .tenant_tier("enterprise")
        .expect("tenant tier should be accepted");

    let value = serde_json::to_value(&source).expect("trigger source should serialize");
    assert_eq!(value["kind"], json!("webhook"));
    assert_eq!(
        value["labels"][AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE],
        json!("prod")
    );

    let decoded: AgentTriggerSource =
        serde_json::from_value(value).expect("trigger source should deserialize");
    assert_eq!(decoded, source);
    assert_eq!(AgentTriggerSourceKind::OnDemand.as_label(), "on-demand");
    assert_eq!(
        AgentTriggerSourceKind::from_label("external-callback"),
        Some(AgentTriggerSourceKind::ExternalCallback)
    );
}

#[test]
fn trigger_source_attaches_to_agent_command_attributes() {
    let source = AgentTriggerSource::schedule()
        .deployment_channel("stable")
        .expect("deployment channel should be accepted")
        .tenant_tier("team")
        .expect("tenant tier should be accepted");
    let command = AgentCommand::new(AgentCommandKind::StartRun, default_command_metadata())
        .expect("command should construct");

    let command = source
        .attach_to_command(command)
        .expect("trigger source should attach");

    assert_eq!(
        command.attributes.get(AGENT_TRIGGER_KIND_ATTRIBUTE),
        Some(&"schedule".to_string())
    );
    assert_eq!(
        command
            .attributes
            .get(AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE),
        Some(&"stable".to_string())
    );
    assert_eq!(
        command.attributes.get(AGENT_TRIGGER_TENANT_TIER_ATTRIBUTE),
        Some(&"team".to_string())
    );
    command
        .validate()
        .expect("command should remain valid after trigger attachment");
}

#[test]
fn trigger_source_rejects_unbounded_or_sensitive_metadata() {
    let user_id_error = AgentTriggerSource::api()
        .label("user_id", "user-123")
        .expect_err("raw user ids must not become hot labels");
    assert_eq!(user_id_error.code(), "invalid-trigger-label");

    let url_error = AgentTriggerSource::webhook()
        .label(AGENT_METRIC_ATTR_DETAIL, "https://example.test/webhook")
        .expect_err("raw webhook urls must not become hot labels");
    assert_eq!(url_error.code(), "invalid-trigger-label");

    let token_error = AgentTriggerSource::external_callback()
        .label(AGENT_METRIC_ATTR_DETAIL, "Bearer secret-token")
        .expect_err("tokens must not become hot labels");
    assert_eq!(token_error.code(), "invalid-trigger-label");

    let body_error = AgentTriggerSource::webhook()
        .label("request_body", "{\"event\":\"push\"}")
        .expect_err("raw request bodies must not become hot labels");
    assert_eq!(body_error.code(), "invalid-trigger-label");
}

#[test]
fn bounded_trigger_labels_are_metric_safe() {
    let source = AgentTriggerSource::on_demand()
        .deployment_channel("preview")
        .expect("deployment channel should be accepted")
        .tenant_tier("free")
        .expect("tenant tier should be accepted");
    source.validate().expect("source should validate");

    validate_agent_metric_attributes(&[
        (AGENT_METRIC_ATTR_TRIGGER_KIND, source.kind.as_label()),
        (AGENT_METRIC_ATTR_DEPLOYMENT_CHANNEL, "preview"),
        (AGENT_METRIC_ATTR_TENANT_TIER, "free"),
    ])
    .expect("trigger labels should be accepted by hot metric validation");
}

#[test]
fn trigger_source_rejects_conflicting_command_attributes() {
    let command = AgentCommand::new(AgentCommandKind::StartRun, default_command_metadata())
        .expect("command should construct")
        .attribute(AGENT_TRIGGER_KIND_ATTRIBUTE, "api")
        .expect("command attribute should attach");

    let error = AgentTriggerSource::schedule()
        .attach_to_command(command)
        .expect_err("trigger kind conflict should be rejected");

    assert_eq!(error.code(), "conflicting-command-attribute");
}

#[test]
fn api_start_command_builder_normalizes_to_durable_command() {
    let command = trigger_start_run_command(
        command_metadata("command-api-start", "trigger:api:run-1"),
        AgentTriggerSource::api()
            .deployment_channel("prod")
            .expect("deployment channel should be accepted"),
        Some(artifact("artifact:api-input", ArtifactKind::Input)),
    )
    .expect("api start command should build");

    assert_eq!(command.kind, AgentCommandKind::StartRun);
    assert_eq!(command.metadata.command_id.as_str(), "command-api-start");
    assert_eq!(
        command.attributes.get(AGENT_TRIGGER_KIND_ATTRIBUTE),
        Some(&"api".to_string())
    );
    assert_eq!(
        command.payload_ref.as_ref().map(|artifact| artifact.kind),
        Some(ArtifactKind::Input)
    );
    command.validate().expect("command should validate");
}

#[test]
fn webhook_signal_command_builder_supports_payload_ref() {
    let command = trigger_submit_signal_command(
        command_metadata("command-webhook-signal", "trigger:webhook:push-1"),
        AgentTriggerSource::webhook()
            .deployment_channel("prod")
            .expect("deployment channel should be accepted"),
        "github.push",
        Some(artifact("artifact:webhook-event", ArtifactKind::Input)),
    )
    .expect("webhook signal command should build");

    match command.kind {
        AgentCommandKind::SubmitSignal { signal_type } => {
            assert_eq!(signal_type, "github.push");
        }
        other => panic!("expected SubmitSignal command, got {other:?}"),
    }
    assert_eq!(
        command.attributes.get(AGENT_TRIGGER_KIND_ATTRIBUTE),
        Some(&"webhook".to_string())
    );
    assert_eq!(
        command.payload_ref.as_ref().map(|artifact| artifact.kind),
        Some(ArtifactKind::Input)
    );
}

#[test]
fn schedule_and_on_demand_start_commands_normalize_trigger_kind() {
    let schedule = trigger_start_run_command(
        command_metadata(
            "command-schedule-start",
            "trigger:schedule:2026-06-22T10:00Z",
        ),
        AgentTriggerSource::schedule(),
        None,
    )
    .expect("schedule start command should build");
    let on_demand = trigger_start_run_command(
        command_metadata("command-on-demand-start", "trigger:on-demand:run-1"),
        AgentTriggerSource::on_demand(),
        None,
    )
    .expect("on-demand start command should build");

    assert_eq!(
        schedule.attributes.get(AGENT_TRIGGER_KIND_ATTRIBUTE),
        Some(&"schedule".to_string())
    );
    assert_eq!(
        on_demand.attributes.get(AGENT_TRIGGER_KIND_ATTRIBUTE),
        Some(&"on-demand".to_string())
    );
}

#[test]
fn human_cancel_and_retry_command_builders_create_expected_kinds() {
    let human = trigger_human_decision_command(
        command_metadata("command-human", "trigger:human:checkpoint-1"),
        AgentTriggerSource::human_decision(),
        HumanCheckpointId::new("checkpoint-1"),
        "approve",
        Some(artifact("artifact:human-decision", ArtifactKind::Other)),
    )
    .expect("human decision command should build");
    match human.kind {
        AgentCommandKind::HumanDecisionSubmitted {
            checkpoint_id,
            decision,
        } => {
            assert_eq!(checkpoint_id.as_str(), "checkpoint-1");
            assert_eq!(decision, "approve");
        }
        other => panic!("expected HumanDecisionSubmitted command, got {other:?}"),
    }

    let cancel = trigger_cancel_run_command(
        command_metadata("command-cancel", "trigger:api:cancel-1"),
        AgentTriggerSource::api(),
        Some(artifact("artifact:cancel-reason", ArtifactKind::Other)),
    )
    .expect("cancel command should build");
    assert_eq!(cancel.kind, AgentCommandKind::CancelRun);
    assert!(cancel.payload_ref.is_some());

    let retry = trigger_retry_run_command(
        command_metadata("command-retry", "trigger:api:retry-1"),
        AgentTriggerSource::api(),
        None,
    )
    .expect("retry command should build");
    assert_eq!(retry.kind, AgentCommandKind::RetryRun);
}

#[test]
fn trigger_command_builder_rejects_invalid_command_shape() {
    let error = AgentTriggerCommandBuilder::submit_signal(
        command_metadata("command-invalid-signal", "trigger:webhook:invalid-signal"),
        AgentTriggerSource::webhook(),
        "",
    )
    .build()
    .expect_err("blank signal type should fail command validation");

    assert_eq!(error.code(), "invalid-trigger-command");
}

#[tokio::test]
async fn trigger_start_command_accepts_through_inbox_and_deduplicates_by_key() {
    let store = TestStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = test_inbox(store.clone(), metrics);
    inbox.recover().await.expect("inbox should recover");

    let first = trigger_start_run_command(
        command_metadata("command-trigger-1", "trigger:api:dedup-1"),
        AgentTriggerSource::api(),
        None,
    )
    .expect("first trigger command should build");
    let duplicate_by_key = trigger_start_run_command(
        command_metadata("command-trigger-2", "trigger:api:dedup-1"),
        AgentTriggerSource::api(),
        None,
    )
    .expect("duplicate trigger command should build");

    assert_eq!(store.len(), 0, "building a command must not persist it");
    let accepted = inbox
        .accept_command(first)
        .await
        .expect("first trigger command should persist");
    let duplicate = inbox
        .accept_command(duplicate_by_key)
        .await
        .expect("duplicate trigger command should be reported");

    assert!(accepted.is_accepted());
    assert!(duplicate.is_duplicate());
    assert_eq!(
        duplicate.duplicate_reason(),
        Some(AgentInboxDuplicateReason::DeduplicationKey)
    );
    assert_eq!(store.len(), 1);
}

fn default_command_metadata() -> AgentCommandMetadata {
    command_metadata("command-1", "command:run-1:start")
}

fn command_metadata(command_id: &str, deduplication_key: &str) -> AgentCommandMetadata {
    AgentCommandMetadata::new(
        AgentWorkflowId::new("workflow-1"),
        AgentRunId::new("run-1"),
        AgentCommandId::new(command_id),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(deduplication_key),
            AgentCausationId::new("trigger:schedule:2026-06-22T10:00:00Z"),
            AgentCorrelationId::new("corr-1"),
        ),
        AgentTenantId::new("tenant-a"),
        AgentTimestampMillis::new(100),
    )
    .expect("metadata should be valid")
}

fn artifact(artifact_id: &str, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind,
        uri: format!("artifact://{artifact_id}"),
        checksum: None,
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("short".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(100),
        metadata: Default::default(),
    }
}

fn test_inbox(
    store: TestStore,
    metrics: Arc<InMemoryMetricsRecorder>,
) -> AgentRunInbox<TestStore, ManualWorkflowClock> {
    AgentRunInbox::with_clock_and_metrics(
        AgentRunId::new("run-1"),
        store,
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
        metrics,
    )
}
