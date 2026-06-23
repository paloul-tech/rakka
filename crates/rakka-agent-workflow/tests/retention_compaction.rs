//! Agent workflow retention and compaction tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    compact_agent_audit_events, compact_agent_run_state, AgentAuditEvent, AgentAuditEventId,
    AgentAuditEventKind, AgentCausationId, AgentCorrelationId, AgentDeduplicationKey, AgentEffect,
    AgentEffectId, AgentEffectKind, AgentEffectStatus, AgentEffectTarget, AgentIdempotencyKey,
    AgentRetentionArchiveKind, AgentRetentionArchiveReason, AgentRetentionPolicy, AgentRunId,
    AgentRunState, AgentRunStatus, AgentStatePayload, AgentStepId, AgentTelemetryContext,
    AgentTenantId, AgentTimestampMillis, AgentWorkflowId, ArtifactKind, ArtifactRef,
    HumanCheckpoint, HumanCheckpointId, HumanCheckpointStatus, InlineState, RedactionStatus,
    StateSchemaVersion, WorkflowDefinitionVersion,
};

#[test]
fn run_state_compaction_trims_terminal_history_and_returns_archive_records() {
    let policy = AgentRetentionPolicy::new()
        .completed_checkpoint_retention_ms(1_000)
        .completed_effect_retention_ms(1_000)
        .audit_event_retention_ms(500)
        .artifact_reference_retention_ms(500)
        .prompt_artifact_retention_ms(400)
        .completion_artifact_retention_ms(400)
        .inline_state_retention_ms(100)
        .max_terminal_checkpoints(1)
        .max_terminal_effects(1);

    let compaction = compact_agent_run_state(run_state(), policy, ts(3_000));
    let compacted = compaction.run_state;
    let report = compaction.report;

    assert_eq!(compacted.inputs_ref, None);
    assert_eq!(compacted.state_payload, AgentStatePayload::Empty);
    assert_eq!(compacted.checkpoints.len(), 2);
    assert!(compacted
        .checkpoints
        .iter()
        .any(|checkpoint| checkpoint.status == HumanCheckpointStatus::Open));
    let retained_checkpoint = compacted
        .checkpoints
        .iter()
        .find(|checkpoint| checkpoint.checkpoint_id.as_str() == "checkpoint-newest")
        .expect("newest terminal checkpoint should be retained");
    assert!(retained_checkpoint.context_artifacts.is_empty());
    assert_eq!(retained_checkpoint.audit_event_ids.len(), 1);

    assert_eq!(compacted.pending_effects.len(), 2);
    assert!(compacted
        .pending_effects
        .iter()
        .any(|effect| effect.status == AgentEffectStatus::Scheduled));
    let retained_effect = compacted
        .pending_effects
        .iter()
        .find(|effect| effect.effect_id.as_str() == "effect-newest")
        .expect("newest terminal effect should be retained");
    assert_eq!(retained_effect.payload_ref, None);
    assert_eq!(retained_effect.result_ref, None);

    assert_eq!(report.removed_checkpoints, 2);
    assert_eq!(report.removed_effects, 2);
    assert_eq!(report.cleared_inline_state_bytes, 11);
    assert!(report.removed_artifact_refs >= 7);
    assert!(report.removed_audit_event_ids >= 2);
    assert!(report.archive_records.iter().any(|record| {
        record.kind == AgentRetentionArchiveKind::HumanCheckpoint
            && record.reason == AgentRetentionArchiveReason::RetentionWindowExpired
            && record.entity_id.as_deref() == Some("checkpoint-old")
    }));
    assert!(report.archive_records.iter().any(|record| {
        record.kind == AgentRetentionArchiveKind::HumanCheckpoint
            && record.reason == AgentRetentionArchiveReason::HistoryLimitExceeded
            && record.entity_id.as_deref() == Some("checkpoint-over-limit")
    }));
    assert!(report.archive_records.iter().any(|record| {
        record.kind == AgentRetentionArchiveKind::InlineRunState
            && record.reason == AgentRetentionArchiveReason::InlineStateWindowExpired
    }));
    assert!(report.archive_records.iter().any(|record| {
        record.kind == AgentRetentionArchiveKind::EffectArtifactRef
            && record.entity_id.as_deref() == Some("effect-newest")
    }));
}

#[test]
fn disabled_retention_policy_preserves_run_state() {
    let run = run_state();

    let compaction =
        compact_agent_run_state(run.clone(), AgentRetentionPolicy::disabled(), ts(100_000));

    assert_eq!(compaction.run_state, run);
    assert!(compaction.report.is_empty());
}

#[test]
fn audit_event_compaction_removes_expired_events_with_archive_handoff() {
    let old = audit_event(
        "audit-old",
        100,
        Some(artifact("audit-artifact", ArtifactKind::Log, 100)),
    );
    let recent = audit_event("audit-recent", 2_900, None);

    let compaction = compact_agent_audit_events(
        vec![old, recent],
        AgentRetentionPolicy::new().audit_event_retention_ms(1_000),
        ts(3_000),
    );

    assert_eq!(compaction.audit_events.len(), 1);
    assert_eq!(
        compaction.audit_events[0].audit_event_id.as_str(),
        "audit-recent"
    );
    assert_eq!(compaction.report.removed_audit_events, 1);
    let record = compaction
        .report
        .archive_records
        .iter()
        .find(|record| record.kind == AgentRetentionArchiveKind::AuditEvent)
        .expect("expired audit event should produce archive record");
    assert_eq!(record.entity_id.as_deref(), Some("audit-old"));
    assert_eq!(record.audit_event_ids.len(), 1);
    assert_eq!(record.artifact_refs.len(), 1);
}

fn run_state() -> AgentRunState {
    AgentRunState {
        run_id: run_id(),
        workflow_id: workflow_id(),
        tenant: Some(AgentTenantId::new("tenant-retention")),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        graph_state: None,
        status: AgentRunStatus::Completed,
        current_step_id: Some(AgentStepId::new("step-final")),
        current_attempt: 2,
        inputs_ref: Some(artifact("input-old", ArtifactKind::Input, 100)),
        state_payload: AgentStatePayload::Inline(InlineState {
            content_type: "application/json".to_string(),
            bytes: b"{\"done\":true}".to_vec(),
            size_bytes: 11,
        }),
        checkpoints: vec![
            checkpoint(
                "checkpoint-open",
                HumanCheckpointStatus::Open,
                100,
                None,
                true,
            ),
            checkpoint(
                "checkpoint-old",
                HumanCheckpointStatus::Approved,
                100,
                Some(500),
                true,
            ),
            checkpoint(
                "checkpoint-over-limit",
                HumanCheckpointStatus::Rejected,
                2_000,
                Some(2_600),
                false,
            ),
            checkpoint(
                "checkpoint-newest",
                HumanCheckpointStatus::Approved,
                2_100,
                Some(2_700),
                true,
            ),
        ],
        pending_effects: vec![
            effect(
                "effect-active",
                AgentEffectStatus::Scheduled,
                100,
                Some(100),
            ),
            effect("effect-old", AgentEffectStatus::Completed, 100, Some(500)),
            effect(
                "effect-over-limit",
                AgentEffectStatus::Completed,
                2_000,
                Some(2_600),
            ),
            effect(
                "effect-newest",
                AgentEffectStatus::Completed,
                2_100,
                Some(2_700),
            ),
        ],
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: ts(10),
        updated_at: ts(2_800),
        completed_at: Some(ts(2_800)),
    }
}

fn checkpoint(
    checkpoint_id: &str,
    status: HumanCheckpointStatus,
    created_at: u64,
    resolved_at: Option<u64>,
    with_artifact: bool,
) -> HumanCheckpoint {
    HumanCheckpoint {
        checkpoint_id: HumanCheckpointId::new(checkpoint_id),
        status,
        summary: "Retention review".to_string(),
        available_decisions: Vec::new(),
        required_roles: vec!["reviewer".to_string()],
        due_at: None,
        escalation_target: None,
        context_artifacts: if with_artifact {
            vec![artifact(
                format!("{checkpoint_id}-artifact"),
                ArtifactKind::File,
                created_at,
            )]
        } else {
            Vec::new()
        },
        created_by: None,
        resolved_by: None,
        created_at: ts(created_at),
        resolved_at: resolved_at.map(ts),
        audit_event_ids: vec![AgentAuditEventId::new(format!("{checkpoint_id}-audit"))],
    }
}

fn effect(
    effect_id: &str,
    status: AgentEffectStatus,
    created_at: u64,
    due_at: Option<u64>,
) -> AgentEffect {
    AgentEffect {
        effect_id: AgentEffectId::new(effect_id),
        deduplication_key: AgentDeduplicationKey::new(format!("dedup:{effect_id}")),
        kind: AgentEffectKind::ModelCall,
        target: AgentEffectTarget {
            target_type: "model".to_string(),
            name: "retention-model".to_string(),
            address: None,
            attributes: BTreeMap::new(),
        },
        status,
        payload_ref: Some(artifact(
            format!("{effect_id}-prompt"),
            ArtifactKind::Prompt,
            created_at,
        )),
        result_ref: Some(artifact(
            format!("{effect_id}-completion"),
            ArtifactKind::Completion,
            created_at,
        )),
        timeout_ms: Some(30_000),
        idempotency_key: AgentIdempotencyKey::new(format!("idem:{effect_id}")),
        expected_result_type: Some("alloc::string::String".to_string()),
        causation_id: AgentCausationId::new(format!("cause:{effect_id}")),
        correlation_id: AgentCorrelationId::new("corr:retention"),
        telemetry_context: AgentTelemetryContext::default(),
        attempt: 1,
        created_at: ts(created_at),
        due_at: due_at.map(ts),
        last_error_code: None,
    }
}

fn audit_event(
    audit_event_id: &str,
    occurred_at: u64,
    artifact_ref: Option<ArtifactRef>,
) -> AgentAuditEvent {
    AgentAuditEvent {
        audit_event_id: AgentAuditEventId::new(audit_event_id),
        kind: AgentAuditEventKind::RunCompleted,
        workflow_id: workflow_id(),
        run_id: run_id(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        tenant: Some(AgentTenantId::new("tenant-retention")),
        step_id: None,
        effect_id: None,
        checkpoint_id: None,
        command_id: None,
        causation_id: AgentCausationId::new(format!("cause:{audit_event_id}")),
        correlation_id: AgentCorrelationId::new("corr:retention"),
        actor_principal: None,
        artifact_refs: artifact_ref.into_iter().collect(),
        content_hashes: BTreeMap::new(),
        redaction: RedactionStatus::ReferenceOnly,
        telemetry_context: AgentTelemetryContext::default(),
        occurred_at: ts(occurred_at),
        attributes: BTreeMap::new(),
    }
}

fn artifact(artifact_id: impl Into<String>, kind: ArtifactKind, created_at: u64) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.into(),
        kind,
        uri: "memory://retention".to_string(),
        checksum: None,
        content_type: Some("application/octet-stream".to_string()),
        byte_len: Some(10),
        retention_class: Some("test".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: ts(created_at),
        metadata: BTreeMap::new(),
    }
}

fn run_id() -> AgentRunId {
    AgentRunId::new("run-retention")
}

fn workflow_id() -> AgentWorkflowId {
    AgentWorkflowId::new("workflow-retention")
}

const fn ts(value: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(value)
}
