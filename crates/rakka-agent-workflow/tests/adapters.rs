//! Model and tool adapter contract tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    AgentAdapterFailureClass, AgentAdapterOutcome, AgentAdapterReceipt, AgentAdapterUsage,
    AgentCausationId, AgentCorrelationId, AgentDeduplicationKey, AgentEffect, AgentEffectId,
    AgentEffectKind, AgentEffectStatus, AgentEffectTarget, AgentIdempotencyKey, AgentModelRequest,
    AgentTelemetryContext, AgentTimestampMillis, AgentToolRequest, ArtifactKind, ArtifactRef,
    RedactionStatus,
};
use rakka_workflow::OutboxDispatchResult;

#[test]
fn model_request_preserves_effect_metadata_and_prompt_artifact() {
    let prompt_ref = artifact(
        "prompt-1",
        ArtifactKind::Prompt,
        RedactionStatus::ReferenceOnly,
    );
    let effect = effect(
        "model-effect",
        AgentEffectKind::ModelCall,
        Some(prompt_ref.clone()),
        target(
            "model",
            "fallback-model",
            [("model", "gpt-test"), ("temperature", "0.2")],
        ),
    );

    let request = AgentModelRequest::from_effect(effect.clone()).expect("model request");

    assert_eq!(request.effect, effect);
    assert_eq!(request.prompt_ref, Some(prompt_ref));
    assert_eq!(request.model_name.as_deref(), Some("gpt-test"));
    assert_eq!(
        request.parameters.get("temperature").map(String::as_str),
        Some("0.2")
    );
    assert_eq!(request.metadata.effect_kind, AgentEffectKind::ModelCall);
    assert_eq!(request.metadata.timeout_ms, Some(5_000));
    assert_eq!(request.metadata.attempt, 2);
    assert_eq!(
        request.metadata.idempotency_key,
        AgentIdempotencyKey::new("idem-model-effect")
    );
    assert_eq!(
        request.metadata.causation_id,
        AgentCausationId::new("cause-model-effect")
    );
    assert_eq!(
        request.metadata.correlation_id,
        AgentCorrelationId::new("corr-model-effect")
    );
    assert_eq!(
        request.metadata.telemetry_context.trace_parent.as_deref(),
        Some("00-00000000000000000000000000000001-0000000000000001-01")
    );
    assert_eq!(request.metadata.redaction, RedactionStatus::ReferenceOnly);
}

#[test]
fn tool_request_accepts_tool_and_process_effects() {
    let input_ref = artifact("tool-input", ArtifactKind::Input, RedactionStatus::Redacted);
    let tool_effect = effect(
        "tool-effect",
        AgentEffectKind::ToolCall,
        Some(input_ref.clone()),
        target("tool", "calculator", [("schema", "calculator.v1")]),
    );

    let tool_request = AgentToolRequest::from_effect(tool_effect).expect("tool request");
    assert_eq!(tool_request.input_ref, Some(input_ref));
    assert_eq!(tool_request.tool_name, "calculator");
    assert_eq!(
        tool_request.parameters.get("schema").map(String::as_str),
        Some("calculator.v1")
    );
    assert_eq!(tool_request.metadata.effect_kind, AgentEffectKind::ToolCall);
    assert_eq!(tool_request.metadata.redaction, RedactionStatus::Redacted);

    let process_effect = effect(
        "process-effect",
        AgentEffectKind::ProcessCall,
        None,
        target("process", "local-file-watch", []),
    );
    let process_request = AgentToolRequest::from_effect(process_effect).expect("process request");
    assert_eq!(process_request.tool_name, "local-file-watch");
    assert_eq!(
        process_request.metadata.effect_kind,
        AgentEffectKind::ProcessCall
    );
}

#[test]
fn request_builders_reject_incompatible_effect_kinds() {
    let model_error = AgentModelRequest::from_effect(effect(
        "not-model",
        AgentEffectKind::ToolCall,
        None,
        target("tool", "calculator", []),
    ))
    .expect_err("tool effect must not become model request");
    assert_eq!(model_error.code(), "invalid-effect-kind");

    let tool_error = AgentToolRequest::from_effect(effect(
        "not-tool",
        AgentEffectKind::ArtifactWrite,
        None,
        target("artifact", "artifact-store", []),
    ))
    .expect_err("artifact effect must not become tool request");
    assert_eq!(tool_error.code(), "invalid-effect-kind");
}

#[test]
fn adapter_outcomes_map_to_durable_outbox_dispatch_results() {
    let result_ref = artifact(
        "completion-1",
        ArtifactKind::Completion,
        RedactionStatus::Redacted,
    );
    let usage = AgentAdapterUsage::new()
        .input_tokens(100)
        .output_tokens(50)
        .total_tokens(150)
        .cost_microunits(42)
        .attribute("currency", "usd");
    let completed = AgentAdapterOutcome::completed(
        receipt("receipt-completed"),
        Some(result_ref.clone()),
        usage.clone(),
    );

    assert!(completed.is_completed());
    assert_eq!(
        completed.to_outbox_dispatch_result(),
        OutboxDispatchResult::Success
    );
    match completed {
        AgentAdapterOutcome::Completed {
            result_ref: Some(actual_ref),
            usage: actual_usage,
            ..
        } => {
            assert_eq!(actual_ref, result_ref);
            assert_eq!(actual_usage, usage);
        }
        other => panic!("unexpected completed outcome: {other:?}"),
    }

    let retryable = AgentAdapterOutcome::failed(
        receipt("receipt-retryable"),
        AgentAdapterFailureClass::Retryable,
        "rate-limited",
    );
    assert_eq!(
        retryable.to_outbox_dispatch_result(),
        OutboxDispatchResult::failure("retryable:rate-limited")
    );

    let error_ref = artifact(
        "tool-error",
        ArtifactKind::Log,
        RedactionStatus::ReferenceOnly,
    );
    let permanent = AgentAdapterOutcome::Failed {
        receipt: receipt("receipt-permanent"),
        classification: AgentAdapterFailureClass::Permanent,
        error_code: "invalid-arguments".to_string(),
        retry_after: None,
        error_ref: Some(error_ref.clone()),
    };
    assert_eq!(
        permanent.to_outbox_dispatch_result(),
        OutboxDispatchResult::failure("permanent:invalid-arguments")
    );
    match permanent {
        AgentAdapterOutcome::Failed {
            error_ref: Some(actual_ref),
            ..
        } => assert_eq!(actual_ref, error_ref),
        other => panic!("unexpected permanent outcome: {other:?}"),
    }

    let partial_ref = artifact(
        "partial-tool-output",
        ArtifactKind::ToolOutput,
        RedactionStatus::Redacted,
    );
    let timed_out = AgentAdapterOutcome::timed_out(
        receipt("receipt-timeout"),
        2_500,
        Some(partial_ref.clone()),
    );
    assert_eq!(
        timed_out.to_outbox_dispatch_result(),
        OutboxDispatchResult::timeout("adapter-timeout:2500")
    );
    match timed_out {
        AgentAdapterOutcome::TimedOut {
            partial_result_ref: Some(actual_ref),
            timeout_ms,
            ..
        } => {
            assert_eq!(actual_ref, partial_ref);
            assert_eq!(timeout_ms, 2_500);
        }
        other => panic!("unexpected timeout outcome: {other:?}"),
    }
}

#[test]
fn adapter_outcomes_are_persistable_contracts() {
    let outcome = AgentAdapterOutcome::completed(
        receipt("receipt-persisted"),
        Some(artifact(
            "completion-persisted",
            ArtifactKind::Completion,
            RedactionStatus::ReferenceOnly,
        )),
        AgentAdapterUsage::new().total_tokens(7),
    );

    let json = serde_json::to_string(&outcome).expect("serialize adapter outcome");
    let decoded: AgentAdapterOutcome =
        serde_json::from_str(&json).expect("deserialize adapter outcome");
    assert_eq!(decoded, outcome);
}

#[cfg(feature = "process-tools")]
#[test]
fn process_file_watch_tool_adapter_satisfies_tool_adapter_trait() {
    use rakka_agent_workflow::{AgentToolAdapter, ProcessFileWatchToolAdapter};
    use rakka_process::{ExecutableAllowlist, FileWatchCompletion, FileWatchConfig, ProcessSpec};

    fn assert_tool_adapter<T: AgentToolAdapter>(_adapter: &mut T) {}

    let mut adapter = ProcessFileWatchToolAdapter::new(
        "process-file-watch",
        ProcessSpec::new("/bin/echo"),
        ExecutableAllowlist::from_exact_paths(["/bin/echo"]),
        FileWatchConfig::new(
            "/tmp/rakka-agent-workflow-process-adapter-test",
            FileWatchCompletion::file_exists("done"),
        ),
    )
    .result_ref(artifact(
        "process-result",
        ArtifactKind::ToolOutput,
        RedactionStatus::ReferenceOnly,
    ))
    .redaction(RedactionStatus::ReferenceOnly);

    assert_tool_adapter(&mut adapter);
}

fn effect(
    id: &str,
    kind: AgentEffectKind,
    payload_ref: Option<ArtifactRef>,
    target: AgentEffectTarget,
) -> AgentEffect {
    AgentEffect {
        effect_id: AgentEffectId::new(id),
        deduplication_key: AgentDeduplicationKey::new(format!("dedupe-{id}")),
        kind,
        target,
        status: AgentEffectStatus::Scheduled,
        payload_ref,
        result_ref: None,
        timeout_ms: Some(5_000),
        idempotency_key: AgentIdempotencyKey::new(format!("idem-{id}")),
        expected_result_type: Some("adapter-test-result".to_string()),
        causation_id: AgentCausationId::new(format!("cause-{id}")),
        correlation_id: AgentCorrelationId::new(format!("corr-{id}")),
        telemetry_context: AgentTelemetryContext {
            trace_parent: Some(
                "00-00000000000000000000000000000001-0000000000000001-01".to_string(),
            ),
            trace_state: Some("rakka=test".to_string()),
            baggage: attrs([("tenant_tier", "internal")]),
            span_links: Vec::new(),
        },
        attempt: 2,
        created_at: AgentTimestampMillis::new(100),
        due_at: Some(AgentTimestampMillis::new(125)),
        last_error_code: None,
    }
}

fn target(
    target_type: &str,
    name: &str,
    attributes: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> AgentEffectTarget {
    AgentEffectTarget {
        target_type: target_type.to_string(),
        name: name.to_string(),
        address: Some(format!("rakka://{target_type}/{name}")),
        attributes: attrs(attributes),
    }
}

fn artifact(id: &str, kind: ArtifactKind, redaction: RedactionStatus) -> ArtifactRef {
    ArtifactRef {
        artifact_id: id.to_string(),
        kind,
        uri: format!("memory://adapter-tests/{id}"),
        checksum: Some(format!("len:{}", id.len())),
        content_type: Some("application/json".to_string()),
        byte_len: Some(id.len() as u64),
        retention_class: Some("test".to_string()),
        redaction,
        created_at: AgentTimestampMillis::new(90),
        metadata: attrs([("purpose", "adapter-contract")]),
    }
}

fn receipt(id: &str) -> AgentAdapterReceipt {
    AgentAdapterReceipt::new(
        id,
        "test-provider",
        "test-target",
        AgentIdempotencyKey::new(format!("idem-{id}")),
        AgentTimestampMillis::new(200),
    )
    .external_request_id(format!("external-{id}"))
    .latency_ms(25)
    .redaction(RedactionStatus::ReferenceOnly)
    .attribute("region", "local")
}

fn attrs(
    pairs: impl IntoIterator<Item = (&'static str, &'static str)>,
) -> BTreeMap<String, String> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}
