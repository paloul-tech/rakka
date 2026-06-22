//! Trigger source metadata contract tests.

use rakka_agent_workflow::{
    validate_agent_metric_attributes, AgentCausationId, AgentCommand, AgentCommandId,
    AgentCommandKind, AgentCommandMetadata, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentRunId, AgentTenantId, AgentTimestampMillis, AgentTriggerSource,
    AgentTriggerSourceKind, AgentWorkflowId, AGENT_METRIC_ATTR_DEPLOYMENT_CHANNEL,
    AGENT_METRIC_ATTR_DETAIL, AGENT_METRIC_ATTR_TENANT_TIER, AGENT_METRIC_ATTR_TRIGGER_KIND,
    AGENT_TRIGGER_DEPLOYMENT_CHANNEL_ATTRIBUTE, AGENT_TRIGGER_KIND_ATTRIBUTE,
    AGENT_TRIGGER_TENANT_TIER_ATTRIBUTE,
};
use serde_json::json;

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
    let command = AgentCommand::new(AgentCommandKind::StartRun, command_metadata())
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
    let command = AgentCommand::new(AgentCommandKind::StartRun, command_metadata())
        .expect("command should construct")
        .attribute(AGENT_TRIGGER_KIND_ATTRIBUTE, "api")
        .expect("command attribute should attach");

    let error = AgentTriggerSource::schedule()
        .attach_to_command(command)
        .expect_err("trigger kind conflict should be rejected");

    assert_eq!(error.code(), "conflicting-command-attribute");
}

fn command_metadata() -> AgentCommandMetadata {
    AgentCommandMetadata::new(
        AgentWorkflowId::new("workflow-1"),
        AgentRunId::new("run-1"),
        AgentCommandId::new("command-1"),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new("command:run-1:start"),
            AgentCausationId::new("trigger:schedule:2026-06-22T10:00:00Z"),
            AgentCorrelationId::new("corr-1"),
        ),
        AgentTenantId::new("tenant-a"),
        AgentTimestampMillis::new(100),
    )
    .expect("metadata should be valid")
}
