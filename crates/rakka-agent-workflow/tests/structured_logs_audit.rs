//! Structured log and durable audit tests.

use std::collections::BTreeMap;

use serde_json::json;

use rakka_agent_workflow::{
    agent_audit_event_kind_label, agent_audit_log_event_name, agent_log_event_from_audit_event,
    validate_agent_audit_event, validate_agent_log_event, AgentAttributes, AgentAuditEvent,
    AgentAuditEventId, AgentAuditEventKind, AgentAuditQuery, AgentAuditSink, AgentAuditWriteStatus,
    AgentCausationId, AgentCommandId, AgentCorrelationId, AgentEffectId, AgentLogEvent,
    AgentLogSeverity, AgentRedactionPolicy, AgentRunId, AgentStepId, AgentTelemetryContext,
    AgentTenantId, AgentTimestampMillis, AgentWorkflowId, ArtifactKind, ArtifactRef,
    HumanCheckpointId, InMemoryAgentAuditSink, PrincipalRef, RedactionStatus,
    WorkflowDefinitionVersion, AGENT_LOG_ATTR_AUDIT_EVENT_ID, AGENT_LOG_ATTR_CORRELATION_ID,
    AGENT_LOG_ATTR_REDACTION, AGENT_LOG_INSTRUMENTATION_SCOPE,
};

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const SPAN_ID: &str = "00f067aa0ba902b7";

#[test]
fn log_event_from_audit_uses_otel_fields_and_trace_correlation() {
    let audit_event = audit_event("audit-1", AgentAuditEventKind::CheckpointCreated);

    let log_event = agent_log_event_from_audit_event(&audit_event, AgentTimestampMillis::new(250))
        .expect("audit event should convert to a structured log");

    assert_eq!(
        log_event.event_name,
        agent_audit_log_event_name(AgentAuditEventKind::CheckpointCreated)
    );
    assert_eq!(log_event.timestamp, audit_event.occurred_at);
    assert_eq!(log_event.observed_timestamp, AgentTimestampMillis::new(250));
    assert_eq!(log_event.trace_id.as_deref(), Some(TRACE_ID));
    assert_eq!(log_event.span_id.as_deref(), Some(SPAN_ID));
    assert_eq!(log_event.trace_flags.as_deref(), Some("01"));
    assert_eq!(log_event.severity_text, "INFO");
    assert_eq!(
        log_event.severity_number,
        AgentLogSeverity::Info.severity_number()
    );
    assert_eq!(
        log_event.instrumentation_scope.name,
        AGENT_LOG_INSTRUMENTATION_SCOPE
    );
    assert_eq!(
        log_event
            .attributes
            .get(AGENT_LOG_ATTR_AUDIT_EVENT_ID)
            .map(String::as_str),
        Some("audit-1")
    );
    assert_eq!(
        log_event
            .attributes
            .get(AGENT_LOG_ATTR_CORRELATION_ID)
            .map(String::as_str),
        Some("corr-1")
    );
    assert_eq!(
        log_event
            .attributes
            .get(AGENT_LOG_ATTR_REDACTION)
            .map(String::as_str),
        Some("reference-only")
    );
    assert_eq!(log_event.artifact_refs.len(), 1);

    validate_agent_log_event(&log_event, AgentRedactionPolicy::new())
        .expect("converted log event should validate");
}

#[tokio::test]
async fn audit_sink_records_deduplicates_and_queries_independently_from_logs() {
    let mut sink = InMemoryAgentAuditSink::new();
    let run_created = audit_event("audit-run", AgentAuditEventKind::RunCreated);
    let tool_requested = audit_event("audit-tool", AgentAuditEventKind::ToolRequested);

    let acceptance = sink
        .record_audit_event(run_created.clone())
        .await
        .expect("audit event should record");
    assert_eq!(acceptance.status, AgentAuditWriteStatus::Recorded);

    let duplicate = sink
        .record_audit_event(run_created.clone())
        .await
        .expect("duplicate should be acknowledged");
    assert_eq!(duplicate.status, AgentAuditWriteStatus::Duplicate);

    sink.record_audit_event(tool_requested.clone())
        .await
        .expect("second audit event should record");

    let stored = sink
        .get_audit_event(AgentAuditEventId::new("audit-run"))
        .await
        .expect("get should succeed")
        .expect("event should exist");
    assert_eq!(stored.kind, AgentAuditEventKind::RunCreated);

    let by_run = sink
        .query_audit_events(AgentAuditQuery::new().run_id(AgentRunId::new("run-1")))
        .await
        .expect("query by run should succeed");
    assert_eq!(by_run.len(), 2);

    let by_kind = sink
        .query_audit_events(
            AgentAuditQuery::new()
                .correlation_id(AgentCorrelationId::new("corr-1"))
                .kind(AgentAuditEventKind::ToolRequested),
        )
        .await
        .expect("query by kind should succeed");
    assert_eq!(by_kind, vec![tool_requested]);

    assert_eq!(sink.events().len(), 2);
}

#[test]
fn redaction_policy_rejects_unredacted_log_bodies_and_missing_audit_evidence() {
    let log_event = AgentLogEvent::new(
        "rakka.agent_workflow.prompt.accepted",
        AgentLogSeverity::Info,
        AgentTimestampMillis::new(100),
        AgentTimestampMillis::new(100),
    )
    .body(json!({ "prompt": "raw user prompt" }))
    .redaction(RedactionStatus::Unredacted);

    let error = validate_agent_log_event(&log_event, AgentRedactionPolicy::new())
        .expect_err("unredacted log bodies should be blocked by default");
    assert_eq!(error.code(), "audit-redaction-policy");

    validate_agent_log_event(
        &log_event,
        AgentRedactionPolicy::new().allow_unredacted_log_body(true),
    )
    .expect("explicit policy should allow unredacted bodies");

    let mut audit_event = audit_event("audit-redacted", AgentAuditEventKind::ModelRequested);
    audit_event.artifact_refs.clear();
    audit_event.content_hashes.clear();

    let error = validate_agent_audit_event(&audit_event, AgentRedactionPolicy::new())
        .expect_err("reference-only audit events need evidence");
    assert_eq!(error.code(), "audit-redaction-policy");
}

#[test]
fn log_validation_requires_consistent_trace_fields_and_bounded_body() {
    let mut log_event = AgentLogEvent::new(
        "rakka.agent_workflow.run.failed",
        AgentLogSeverity::Error,
        AgentTimestampMillis::new(100),
        AgentTimestampMillis::new(101),
    );
    log_event.span_id = Some(SPAN_ID.to_string());

    let error = validate_agent_log_event(&log_event, AgentRedactionPolicy::new())
        .expect_err("span id without trace id should be invalid");
    assert_eq!(error.code(), "invalid-log-event");

    let large_body = "x".repeat(32);
    let bounded = AgentLogEvent::new(
        "rakka.agent_workflow.run.failed",
        AgentLogSeverity::Error,
        AgentTimestampMillis::new(100),
        AgentTimestampMillis::new(101),
    )
    .body(json!({ "message": large_body }))
    .redaction(RedactionStatus::Redacted);

    let error =
        validate_agent_log_event(&bounded, AgentRedactionPolicy::new().max_log_body_bytes(8))
            .expect_err("oversized log bodies should be rejected");
    assert_eq!(error.code(), "audit-redaction-policy");
}

#[test]
fn audit_kind_labels_are_stable_for_log_event_names() {
    assert_eq!(
        agent_audit_event_kind_label(AgentAuditEventKind::HumanDecisionMade),
        "human-decision-made"
    );
    assert_eq!(
        agent_audit_log_event_name(AgentAuditEventKind::RetentionDeletion),
        "rakka.agent_workflow.audit.retention-deletion"
    );
}

fn audit_event(audit_event_id: &str, kind: AgentAuditEventKind) -> AgentAuditEvent {
    AgentAuditEvent {
        audit_event_id: AgentAuditEventId::new(audit_event_id),
        kind,
        workflow_id: AgentWorkflowId::new("workflow-1"),
        run_id: AgentRunId::new("run-1"),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        tenant: Some(AgentTenantId::new("tenant-a")),
        step_id: Some(AgentStepId::new("step-review")),
        effect_id: Some(AgentEffectId::new("effect-review")),
        checkpoint_id: Some(HumanCheckpointId::new("checkpoint-review")),
        command_id: Some(AgentCommandId::new("command-review")),
        causation_id: AgentCausationId::new("cause-1"),
        correlation_id: AgentCorrelationId::new("corr-1"),
        actor_principal: Some(principal()),
        artifact_refs: vec![artifact("artifact-prompt", ArtifactKind::Prompt)],
        content_hashes: BTreeMap::from([("prompt".to_string(), "sha256:abc".to_string())]),
        redaction: RedactionStatus::ReferenceOnly,
        telemetry_context: telemetry_context(),
        occurred_at: AgentTimestampMillis::new(200),
        attributes: BTreeMap::from([("workflow_type".to_string(), "research".to_string())]),
    }
}

fn telemetry_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(TRACE_PARENT.to_string()),
        trace_state: Some("vendor=value".to_string()),
        baggage: AgentAttributes::new(),
        span_links: Vec::new(),
    }
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "reviewer-1".to_string(),
        display_name: Some("Reviewer".to_string()),
    }
}

fn artifact(artifact_id: &str, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind,
        uri: format!("object://agent-audit/{artifact_id}"),
        checksum: Some("sha256:abc".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("audit".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(190),
        metadata: BTreeMap::new(),
    }
}
