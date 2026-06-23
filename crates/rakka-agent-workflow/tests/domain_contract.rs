//! Agent workflow domain contract tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentCausationId, AgentCommandId,
    AgentCorrelationId, AgentDeduplicationKey, AgentEffect, AgentEffectId, AgentEffectKind,
    AgentEffectStatus, AgentEffectTarget, AgentIdempotencyKey, AgentPayloadDescriptor, AgentRunId,
    AgentRunState, AgentRunStatus, AgentSpanLink, AgentStatePayload, AgentStep, AgentStepId,
    AgentStepKind, AgentTelemetryContext, AgentTenantId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId, ArtifactEncryptionRef, ArtifactKind, ArtifactRef, HumanCheckpoint,
    HumanCheckpointId, HumanCheckpointStatus, HumanDecisionOption, PrincipalRef, RedactionStatus,
    StateSchemaVersion, WorkflowDefinitionVersion, BOUNDED_METRIC_FIELDS,
    FORBIDDEN_HOT_METRIC_FIELDS, TRACE_LOG_AUDIT_ID_FIELDS,
};

#[test]
fn persisted_domain_contracts_round_trip() {
    let workflow = sample_workflow();
    round_trip(&workflow);

    let run = sample_run_state();
    round_trip(&run);

    let step = sample_step();
    round_trip(&step);

    let effect = sample_effect();
    round_trip(&effect);

    let checkpoint = sample_checkpoint();
    round_trip(&checkpoint);

    let artifact = sample_artifact("artifact:standalone", ArtifactKind::File);
    round_trip(&artifact);

    let telemetry = sample_telemetry();
    round_trip(&telemetry);

    let audit = sample_audit_event();
    round_trip(&audit);
}

#[test]
fn identifier_types_round_trip_as_strings() {
    let run_id = AgentRunId::new("run-1");
    let json = serde_json::to_string(&run_id).expect("serialize run id");
    assert_eq!(json, "\"run-1\"");

    let decoded: AgentRunId = serde_json::from_str(&json).expect("deserialize run id");
    assert_eq!(decoded.as_str(), "run-1");
    assert_eq!(decoded.to_string(), "run-1");
}

#[test]
fn high_cardinality_policy_separates_metrics_from_trace_log_audit_ids() {
    assert!(FORBIDDEN_HOT_METRIC_FIELDS.contains(&"run_id"));
    assert!(FORBIDDEN_HOT_METRIC_FIELDS.contains(&"deduplication_key"));
    assert!(FORBIDDEN_HOT_METRIC_FIELDS.contains(&"idempotency_key"));
    assert!(FORBIDDEN_HOT_METRIC_FIELDS.contains(&"prompt_text"));
    assert!(TRACE_LOG_AUDIT_ID_FIELDS.contains(&"run_id"));
    assert!(TRACE_LOG_AUDIT_ID_FIELDS.contains(&"effect_id"));
    assert!(TRACE_LOG_AUDIT_ID_FIELDS.contains(&"deduplication_key"));
    assert!(BOUNDED_METRIC_FIELDS.contains(&"workflow_type"));
    assert!(BOUNDED_METRIC_FIELDS.contains(&"status"));
    assert!(BOUNDED_METRIC_FIELDS.contains(&"trigger_kind"));
    assert!(BOUNDED_METRIC_FIELDS.contains(&"deployment_channel"));
}

fn round_trip<T>(value: &T)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let json = serde_json::to_string(value).expect("serialize contract");
    let decoded = serde_json::from_str(&json).expect("deserialize contract");
    assert_eq!(*value, decoded);
}

fn sample_workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("research"),
        workflow_type: "research".to_string(),
        definition_version: WorkflowDefinitionVersion::new("2026-06-18"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Research".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::WaitingForHuman.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec!["StartRun".to_string(), "HumanDecisionSubmitted".to_string()],
        steps: vec![sample_step()],
        payload_types: vec![
            AgentPayloadDescriptor::new("research.input").content_type("application/json")
        ],
        retry_policy_ref: Some(sample_artifact(
            "artifact:retry-policy",
            ArtifactKind::Other,
        )),
        timeout_policy_ref: None,
        approval_policy_ref: Some(sample_artifact(
            "artifact:approval-policy",
            ArtifactKind::Other,
        )),
        observability_labels: attributes([
            ("tenant_tier", "internal"),
            ("workflow_type", "research"),
        ]),
    }
}

fn sample_run_state() -> AgentRunState {
    AgentRunState {
        run_id: AgentRunId::new("run-1"),
        workflow_id: AgentWorkflowId::new("research"),
        tenant: Some(AgentTenantId::new("tenant-a")),
        definition_version: WorkflowDefinitionVersion::new("2026-06-18"),
        state_schema_version: StateSchemaVersion::new(1),
        graph_state: None,
        status: AgentRunStatus::WaitingForHuman,
        current_step_id: Some(AgentStepId::new("approval")),
        current_attempt: 1,
        inputs_ref: Some(sample_artifact("artifact:input", ArtifactKind::Input)),
        state_payload: AgentStatePayload::Artifact(sample_artifact(
            "artifact:state",
            ArtifactKind::State,
        )),
        checkpoints: vec![sample_checkpoint()],
        pending_effects: vec![sample_effect()],
        pending_human_checkpoint: Some(HumanCheckpointId::new("checkpoint-1")),
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(200),
        completed_at: None,
    }
}

fn sample_step() -> AgentStep {
    AgentStep {
        step_id: AgentStepId::new("approval"),
        kind: AgentStepKind::HumanCheckpoint,
        display_name: Some("Approve plan".to_string()),
        next_step_ids: vec![AgentStepId::new("final")],
        timeout_ms: Some(30_000),
        config_ref: None,
        observability_labels: attributes([("step_kind", "human-checkpoint")]),
    }
}

fn sample_effect() -> AgentEffect {
    AgentEffect {
        effect_id: AgentEffectId::new("effect-1"),
        deduplication_key: AgentDeduplicationKey::new("effect:effect-1"),
        kind: AgentEffectKind::HumanApprovalRequest,
        target: AgentEffectTarget {
            target_type: "notification".to_string(),
            name: "review-ui".to_string(),
            address: Some("approval-queue".to_string()),
            attributes: attributes([("effect_kind", "human-approval-request")]),
        },
        status: AgentEffectStatus::Scheduled,
        payload_ref: Some(sample_artifact(
            "artifact:approval-request",
            ArtifactKind::Other,
        )),
        result_ref: None,
        timeout_ms: Some(60_000),
        idempotency_key: AgentIdempotencyKey::new("approval-request-1"),
        expected_result_type: Some("HumanDecision".to_string()),
        causation_id: AgentCausationId::new("step-approval"),
        correlation_id: AgentCorrelationId::new("corr-1"),
        telemetry_context: sample_telemetry(),
        attempt: 0,
        created_at: AgentTimestampMillis::new(150),
        due_at: Some(AgentTimestampMillis::new(160)),
        last_error_code: None,
    }
}

fn sample_checkpoint() -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new("checkpoint-1"),
        status: HumanCheckpointStatus::Open,
        summary: "Approve the generated research plan".to_string(),
        available_decisions: vec![
            HumanDecisionOption {
                value: "approve".to_string(),
                label: "Approve".to_string(),
                requires_comment: false,
            },
            HumanDecisionOption {
                value: "reject".to_string(),
                label: "Reject".to_string(),
                requires_comment: true,
            },
        ],
        required_roles: vec!["reviewer".to_string()],
        due_at: Some(AgentTimestampMillis::new(500)),
        escalation_target: Some("research-lead".to_string()),
        context_artifacts: vec![sample_artifact("artifact:plan", ArtifactKind::Prompt)],
        created_by: Some(sample_principal()),
        resolved_by: None,
        created_at: AgentTimestampMillis::new(200),
        resolved_at: None,
        audit_event_ids: vec![AgentAuditEventId::new("audit-1")],
    }
}

fn sample_audit_event() -> AgentAuditEvent {
    AgentAuditEvent {
        audit_event_id: AgentAuditEventId::new("audit-1"),
        kind: AgentAuditEventKind::CheckpointCreated,
        workflow_id: AgentWorkflowId::new("research"),
        run_id: AgentRunId::new("run-1"),
        definition_version: WorkflowDefinitionVersion::new("2026-06-18"),
        tenant: Some(AgentTenantId::new("tenant-a")),
        step_id: Some(AgentStepId::new("approval")),
        effect_id: Some(AgentEffectId::new("effect-1")),
        checkpoint_id: Some(HumanCheckpointId::new("checkpoint-1")),
        command_id: Some(AgentCommandId::new("command-1")),
        causation_id: AgentCausationId::new("step-approval"),
        correlation_id: AgentCorrelationId::new("corr-1"),
        actor_principal: Some(sample_principal()),
        artifact_refs: vec![sample_artifact("artifact:plan", ArtifactKind::Prompt)],
        content_hashes: attributes([("plan", "sha256:abc")]),
        redaction: RedactionStatus::ReferenceOnly,
        telemetry_context: sample_telemetry(),
        occurred_at: AgentTimestampMillis::new(210),
        attributes: attributes([("event", "checkpoint-created")]),
    }
}

fn sample_artifact(artifact_id: &str, kind: ArtifactKind) -> ArtifactRef {
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
        metadata: attributes([("classification", "internal")]),
    }
}

fn sample_telemetry() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
        trace_state: Some("vendor=value".to_string()),
        baggage: attributes([("tenant_tier", "internal")]),
        span_links: vec![AgentSpanLink {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            trace_state: None,
            attributes: attributes([("resume", "human-decision")]),
        }],
    }
}

fn sample_principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "user-1".to_string(),
        display_name: Some("Reviewer".to_string()),
    }
}

fn attributes<const N: usize>(items: [(&str, &str); N]) -> BTreeMap<String, String> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}
