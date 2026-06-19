//! Command and effect facade tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffectId,
    AgentEffectKind, AgentEffectMetadata, AgentEffectSchedule, AgentEffectStatus,
    AgentEffectTarget, AgentFacadeError, AgentIdempotencyKey, AgentRunId, AgentSpanLink,
    AgentTelemetryContext, AgentTenantId, AgentTimestampMillis, AgentWorkflowId, ArtifactKind,
    ArtifactRef, HumanCheckpointId, PrincipalRef, RedactionStatus,
};

#[test]
fn valid_command_makes_durability_metadata_explicit() {
    let command = AgentCommand::new(
        AgentCommandKind::StartRun,
        command_metadata()
            .telemetry_context(trace_context())
            .principal(principal()),
    )
    .expect("valid command should construct")
    .payload_ref(artifact("artifact:input", ArtifactKind::Input))
    .attribute("ingress", "http")
    .expect("bounded attribute should be accepted");

    assert_eq!(command.type_name(), "StartRun");
    assert_eq!(command.message_type(), "agent.start-run");
    assert_eq!(command.metadata.workflow_id.as_str(), "workflow-1");
    assert_eq!(command.metadata.run_id.as_str(), "run-1");
    assert_eq!(command.metadata.command_id.as_str(), "command-1");
    assert_eq!(
        command.metadata.deduplication_key.as_str(),
        "command:run-1:start"
    );
    assert_eq!(command.metadata.tenant.as_str(), "tenant-a");
    assert_eq!(
        command.metadata.telemetry_context.trace_parent.as_deref(),
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
    );
    assert_eq!(
        command.attributes.get("ingress").map(String::as_str),
        Some("http")
    );
    command.validate().expect("command should remain valid");
}

#[test]
fn command_metadata_rejects_missing_required_ids() {
    let error = AgentCommandMetadata::new(
        AgentWorkflowId::new("workflow-1"),
        AgentRunId::new("run-1"),
        AgentCommandId::new("command-1"),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new(" "),
            AgentCausationId::new("cause-1"),
            AgentCorrelationId::new("corr-1"),
        ),
        AgentTenantId::new("tenant-a"),
        AgentTimestampMillis::new(100),
    )
    .expect_err("blank deduplication key should fail");

    assert_eq!(
        error,
        AgentFacadeError::InvalidCommandMetadata {
            field: "deduplication_key",
            reason: "required field must be non-empty",
        }
    );
}

#[test]
fn command_kind_validation_rejects_missing_variant_data() {
    let error = AgentCommand::new(
        AgentCommandKind::EffectFailed {
            effect_id: AgentEffectId::new("effect-1"),
            error_code: String::new(),
        },
        command_metadata(),
    )
    .expect_err("blank error code should fail");

    assert_eq!(
        error,
        AgentFacadeError::InvalidCommand {
            command_type: "EffectFailed",
            field: "error_code",
            reason: "required field must be non-empty",
        }
    );
}

#[test]
fn first_class_command_kinds_have_stable_message_types() {
    let kinds = vec![
        AgentCommandKind::StartRun,
        AgentCommandKind::SubmitSignal {
            signal_type: "progress".to_string(),
        },
        AgentCommandKind::ContinueRun,
        AgentCommandKind::EffectCompleted {
            effect_id: AgentEffectId::new("effect-1"),
        },
        AgentCommandKind::EffectFailed {
            effect_id: AgentEffectId::new("effect-1"),
            error_code: "timeout".to_string(),
        },
        AgentCommandKind::HumanDecisionSubmitted {
            checkpoint_id: HumanCheckpointId::new("checkpoint-1"),
            decision: "approve".to_string(),
        },
        AgentCommandKind::TimerFired {
            timer_id: "timer-1".to_string(),
        },
        AgentCommandKind::CancelRun,
        AgentCommandKind::RetryRun,
        AgentCommandKind::ForgetRun,
    ];

    for kind in kinds {
        assert!(!kind.type_name().is_empty());
        assert!(kind.message_type().starts_with("agent."));
    }
}

#[test]
fn effect_schedule_produces_scheduled_effect_with_idempotency_metadata() {
    let schedule = AgentEffectSchedule::new(
        AgentEffectKind::HttpCall,
        target("http", "research-api"),
        effect_metadata()
            .telemetry_context(trace_context())
            .due_at(AgentTimestampMillis::new(250))
            .timeout_ms(5_000),
    )
    .expect("valid effect schedule should construct")
    .payload_ref(artifact("artifact:request", ArtifactKind::Other))
    .expected_result_type("HttpResponse")
    .expect("expected result type should be accepted");

    assert_eq!(schedule.message_type(), "agent.effect.http-call");

    let effect = schedule
        .into_effect()
        .expect("validated schedule should convert to effect");

    assert_eq!(effect.effect_id.as_str(), "effect-1");
    assert_eq!(effect.deduplication_key.as_str(), "effect:run-1:call-1");
    assert_eq!(effect.idempotency_key.as_str(), "idempotency:call-1");
    assert_eq!(effect.kind, AgentEffectKind::HttpCall);
    assert_eq!(effect.status, AgentEffectStatus::Scheduled);
    assert_eq!(effect.attempt, 0);
    assert_eq!(effect.timeout_ms, Some(5_000));
    assert_eq!(effect.due_at, Some(AgentTimestampMillis::new(250)));
    assert_eq!(effect.expected_result_type.as_deref(), Some("HttpResponse"));
    assert_eq!(effect.message_type(), "agent.effect.http-call");
}

#[test]
fn effect_schedule_rejects_missing_ids_and_targets() {
    let metadata_error = AgentEffectMetadata::new(
        AgentEffectId::new("effect-1"),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new("effect:run-1:call-1"),
            AgentCausationId::new("step-1"),
            AgentCorrelationId::new("corr-1"),
        ),
        AgentIdempotencyKey::new(""),
        AgentTimestampMillis::new(200),
    )
    .expect_err("blank idempotency key should fail");

    assert_eq!(
        metadata_error,
        AgentFacadeError::InvalidEffectMetadata {
            field: "idempotency_key",
            reason: "required field must be non-empty",
        }
    );

    let target_error = AgentEffectSchedule::new(
        AgentEffectKind::ToolCall,
        target("tool", " "),
        effect_metadata(),
    )
    .expect_err("blank target name should fail");

    assert_eq!(
        target_error,
        AgentFacadeError::InvalidEffect {
            effect_kind: AgentEffectKind::ToolCall,
            field: "target.name",
            reason: "required field must be non-empty",
        }
    );
}

fn command_metadata() -> AgentCommandMetadata {
    AgentCommandMetadata::new(
        AgentWorkflowId::new("workflow-1"),
        AgentRunId::new("run-1"),
        AgentCommandId::new("command-1"),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new("command:run-1:start"),
            AgentCausationId::new("ingress-1"),
            AgentCorrelationId::new("corr-1"),
        ),
        AgentTenantId::new("tenant-a"),
        AgentTimestampMillis::new(100),
    )
    .expect("sample command metadata should be valid")
}

fn effect_metadata() -> AgentEffectMetadata {
    AgentEffectMetadata::new(
        AgentEffectId::new("effect-1"),
        AgentDurabilityMetadata::new(
            AgentDeduplicationKey::new("effect:run-1:call-1"),
            AgentCausationId::new("step-1"),
            AgentCorrelationId::new("corr-1"),
        ),
        AgentIdempotencyKey::new("idempotency:call-1"),
        AgentTimestampMillis::new(200),
    )
    .expect("sample effect metadata should be valid")
}

fn target(target_type: &str, name: &str) -> AgentEffectTarget {
    AgentEffectTarget {
        target_type: target_type.to_string(),
        name: name.to_string(),
        address: Some("https://service.internal".to_string()),
        attributes: BTreeMap::from([("effect_kind".to_string(), "http-call".to_string())]),
    }
}

fn artifact(artifact_id: &str, kind: ArtifactKind) -> ArtifactRef {
    ArtifactRef {
        artifact_id: artifact_id.to_string(),
        kind,
        uri: format!("object://bucket/{artifact_id}"),
        checksum: Some("sha256:abc".to_string()),
        content_type: Some("application/json".to_string()),
        byte_len: Some(128),
        retention_class: Some("standard".to_string()),
        encryption: None,
        redaction: RedactionStatus::ReferenceOnly,
        created_at: AgentTimestampMillis::new(123),
        metadata: BTreeMap::new(),
    }
}

fn trace_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
        trace_state: Some("vendor=value".to_string()),
        baggage: BTreeMap::from([("tenant_tier".to_string(), "internal".to_string())]),
        span_links: vec![AgentSpanLink {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            span_id: "00f067aa0ba902b7".to_string(),
            trace_state: None,
            attributes: BTreeMap::new(),
        }],
    }
}

fn principal() -> PrincipalRef {
    PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "user-1".to_string(),
        display_name: Some("Reviewer".to_string()),
    }
}
