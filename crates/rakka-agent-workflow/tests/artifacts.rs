//! Artifact reference policy tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    agent_audit_artifact_refs, agent_run_artifact_refs, validate_artifact_ref,
    validate_run_state_artifact_policy, AgentArtifactPolicy, AgentAuditEvent, AgentAuditEventId,
    AgentAuditEventKind, AgentCausationId, AgentCommandId, AgentCorrelationId,
    AgentDeduplicationKey, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectStatus,
    AgentEffectTarget, AgentIdempotencyKey, AgentRunId, AgentRunState, AgentRunStatus,
    AgentStatePayload, AgentStepId, AgentTelemetryContext, AgentTenantId, AgentTimestampMillis,
    AgentWorkflowId, ArtifactEncryptionRef, ArtifactKind, ArtifactRef, HumanCheckpoint,
    HumanCheckpointId, HumanCheckpointStatus, InlineState, PrincipalRef, RedactionStatus,
    StateSchemaVersion, WorkflowDefinitionVersion, DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES,
};

#[test]
fn artifact_reference_policy_validates_required_metadata_and_encryption() {
    let reference = artifact("artifact:prompt", ArtifactKind::Prompt).encrypted();

    validate_artifact_ref(&reference).expect("encrypted artifact reference should be valid");
    assert_eq!(reference.kind.as_label(), "prompt");
    assert_eq!(reference.redaction.as_label(), "reference-only");

    let json = serde_json::to_string(&reference).expect("artifact should serialize");
    let decoded: ArtifactRef = serde_json::from_str(&json).expect("artifact should deserialize");
    assert_eq!(decoded, reference);

    let mut missing_checksum = reference.clone();
    missing_checksum.checksum = None;
    let error = validate_artifact_ref(&missing_checksum).expect_err("checksum is required");
    assert_eq!(error.code(), "invalid-artifact-reference");

    let mut invalid_encryption = reference;
    invalid_encryption.encryption = Some(ArtifactEncryptionRef::new("", ""));
    let error = validate_artifact_ref(&invalid_encryption)
        .expect_err("encryption metadata should be bounded");
    assert_eq!(error.code(), "invalid-artifact-reference");
}

#[test]
fn run_state_policy_rejects_large_inline_state_by_default() {
    let mut run_state = run_state_with_references();
    let size_bytes = DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES + 1;
    run_state.state_payload = AgentStatePayload::Inline(InlineState {
        content_type: "application/json".to_string(),
        bytes: vec![b'x'; size_bytes as usize],
        size_bytes,
    });

    let error = validate_run_state_artifact_policy(&run_state, &AgentArtifactPolicy::default())
        .expect_err("large inline state should be rejected");
    assert_eq!(error.code(), "inline-state-too-large");

    run_state.state_payload =
        AgentStatePayload::Artifact(artifact("artifact:state", ArtifactKind::State));
    validate_run_state_artifact_policy(&run_state, &AgentArtifactPolicy::default())
        .expect("state artifact should satisfy hot-state policy");
}

#[test]
fn run_state_artifact_refs_collect_payloads_results_state_and_checkpoint_context() {
    let run_state = run_state_with_references();

    validate_run_state_artifact_policy(&run_state, &AgentArtifactPolicy::default())
        .expect("run state references should be valid");

    let artifact_ids = agent_run_artifact_refs(&run_state)
        .into_iter()
        .map(|reference| reference.artifact_id.as_str())
        .collect::<Vec<_>>();

    assert!(artifact_ids.contains(&"artifact:input"));
    assert!(artifact_ids.contains(&"artifact:state"));
    assert!(artifact_ids.contains(&"artifact:prompt"));
    assert!(artifact_ids.contains(&"artifact:completion"));
    assert!(artifact_ids.contains(&"artifact:checkpoint-context"));
}

#[test]
fn audit_artifact_refs_are_available_for_correlation() {
    let audit_event = audit_event();
    let references = agent_audit_artifact_refs(&audit_event);

    assert_eq!(references.len(), 2);
    assert_eq!(references[0].artifact_id, "artifact:prompt");
    assert_eq!(references[1].artifact_id, "artifact:completion");
}

#[test]
fn custom_policy_can_allow_larger_inline_state_when_explicit() {
    let size_bytes = DEFAULT_AGENT_INLINE_STATE_LIMIT_BYTES + 1;
    let run_state = AgentRunState {
        state_payload: AgentStatePayload::Inline(InlineState {
            content_type: "application/json".to_string(),
            bytes: vec![b'x'; size_bytes as usize],
            size_bytes,
        }),
        ..run_state_with_references()
    };
    let policy = AgentArtifactPolicy::default().inline_state_limit_bytes(size_bytes);

    validate_run_state_artifact_policy(&run_state, &policy)
        .expect("larger inline state requires an explicit policy");
}

trait EncryptableArtifact {
    fn encrypted(self) -> Self;
}

impl EncryptableArtifact for ArtifactRef {
    fn encrypted(mut self) -> Self {
        self.encryption = Some(
            ArtifactEncryptionRef::new("AES256-GCM", "kms://agent-workflow/test-key")
                .context("tenant", "tenant-a"),
        );
        self
    }
}

fn run_state_with_references() -> AgentRunState {
    AgentRunState {
        run_id: AgentRunId::new("run-artifacts"),
        workflow_id: AgentWorkflowId::new("workflow-artifacts"),
        tenant: Some(AgentTenantId::new("tenant-a")),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        graph_state: None,
        status: AgentRunStatus::WaitingForEffect,
        current_step_id: Some(AgentStepId::new("model-step")),
        current_attempt: 1,
        inputs_ref: Some(artifact("artifact:input", ArtifactKind::Input)),
        state_payload: AgentStatePayload::Artifact(artifact("artifact:state", ArtifactKind::State)),
        checkpoints: vec![checkpoint()],
        pending_effects: vec![model_effect()],
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(150),
        completed_at: None,
    }
}

fn model_effect() -> AgentEffect {
    AgentEffect {
        effect_id: AgentEffectId::new("effect-model"),
        deduplication_key: AgentDeduplicationKey::new("effect:effect-model"),
        kind: AgentEffectKind::ModelCall,
        target: AgentEffectTarget {
            target_type: "model".to_string(),
            name: "test-model".to_string(),
            address: Some("model://test-model".to_string()),
            attributes: BTreeMap::new(),
        },
        status: AgentEffectStatus::Completed,
        payload_ref: Some(artifact("artifact:prompt", ArtifactKind::Prompt)),
        result_ref: Some(artifact("artifact:completion", ArtifactKind::Completion)),
        timeout_ms: Some(5_000),
        idempotency_key: AgentIdempotencyKey::new("model-idempotency"),
        expected_result_type: Some("completion".to_string()),
        causation_id: AgentCausationId::new("cause-model"),
        correlation_id: AgentCorrelationId::new("corr-model"),
        telemetry_context: telemetry(),
        attempt: 1,
        created_at: AgentTimestampMillis::new(110),
        due_at: Some(AgentTimestampMillis::new(120)),
        last_error_code: None,
    }
}

fn checkpoint() -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new("checkpoint-artifacts"),
        status: HumanCheckpointStatus::Open,
        summary: "Review artifact-backed context".to_string(),
        available_decisions: Vec::new(),
        required_roles: vec!["reviewer".to_string()],
        due_at: None,
        escalation_target: None,
        context_artifacts: vec![artifact(
            "artifact:checkpoint-context",
            ArtifactKind::Prompt,
        )],
        created_by: Some(principal()),
        resolved_by: None,
        created_at: AgentTimestampMillis::new(130),
        resolved_at: None,
        audit_event_ids: Vec::new(),
    }
}

fn audit_event() -> AgentAuditEvent {
    AgentAuditEvent {
        audit_event_id: AgentAuditEventId::new("audit-artifacts"),
        kind: AgentAuditEventKind::ArtifactWritten,
        workflow_id: AgentWorkflowId::new("workflow-artifacts"),
        run_id: AgentRunId::new("run-artifacts"),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        tenant: Some(AgentTenantId::new("tenant-a")),
        step_id: Some(AgentStepId::new("model-step")),
        effect_id: Some(AgentEffectId::new("effect-model")),
        checkpoint_id: None,
        command_id: Some(AgentCommandId::new("command-artifacts")),
        causation_id: AgentCausationId::new("cause-model"),
        correlation_id: AgentCorrelationId::new("corr-model"),
        actor_principal: Some(principal()),
        artifact_refs: vec![
            artifact("artifact:prompt", ArtifactKind::Prompt),
            artifact("artifact:completion", ArtifactKind::Completion),
        ],
        content_hashes: BTreeMap::from([("prompt".to_string(), "sha256:abc".to_string())]),
        redaction: RedactionStatus::ReferenceOnly,
        telemetry_context: telemetry(),
        occurred_at: AgentTimestampMillis::new(160),
        attributes: BTreeMap::new(),
    }
}

fn artifact(artifact_id: &str, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind,
        uri: format!("object://agent-workflow/{artifact_id}"),
        checksum: Some("sha256:abc".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("standard".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(90),
        metadata: BTreeMap::from([("classification".to_string(), "internal".to_string())]),
    }
}

fn telemetry() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
        trace_state: None,
        baggage: BTreeMap::from([("tenant_tier".to_string(), "internal".to_string())]),
        span_links: Vec::new(),
    }
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "service".to_string(),
        principal_id: "artifact-policy-test".to_string(),
        display_name: None,
    }
}
