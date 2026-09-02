//! Trace context propagation tests.

use rakka_agent_workflow::{
    agent_child_telemetry_context, agent_durable_resume_telemetry_context,
    extract_agent_trace_context, human_decision_command, inject_agent_trace_context,
    parse_agent_trace_context, validate_agent_telemetry_context, AgentAttributes, AgentCausationId,
    AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata, AgentCorrelationId,
    AgentDeduplicationKey, AgentDurabilityMetadata, AgentEffectId, AgentEffectKind,
    AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget, AgentHumanDecisionSubmission,
    AgentIdempotencyKey, AgentRunId, AgentTelemetryContext, AgentTenantId, AgentTimerEntry,
    AgentTimestampMillis, AgentTraceContext, AgentWorkflowId, HumanCheckpointId,
    HumanCheckpointStatus, TRACEPARENT_HEADER, TRACESTATE_HEADER,
};

const ROOT_TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const ROOT_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const ROOT_SPAN_ID: &str = "00f067aa0ba902b7";
const TRACE_STATE: &str = "vendor=value";
const STEP_SPAN_ID: &str = "1111111111111111";
const TIMER_SPAN_ID: &str = "2222222222222222";
const CALLBACK_SPAN_ID: &str = "3333333333333333";
const HUMAN_SPAN_ID: &str = "4444444444444444";
const RECOVERY_SPAN_ID: &str = "5555555555555555";

#[test]
fn trace_context_round_trips_through_case_insensitive_carriers() {
    let trace = parse_agent_trace_context(ROOT_TRACE_PARENT, Some(TRACE_STATE))
        .expect("traceparent should parse");
    assert_eq!(trace.version, "00");
    assert_eq!(trace.trace_id, ROOT_TRACE_ID);
    assert_eq!(trace.span_id, ROOT_SPAN_ID);
    assert_eq!(trace.trace_parent(), ROOT_TRACE_PARENT);
    assert!(trace.is_sampled());

    let mut telemetry = trace.to_telemetry_context();
    telemetry.baggage = attributes(&[("tenant_tier", "gold")]);

    let mut carrier = attributes(&[("TraceParent", "stale"), ("unrelated", "kept")]);
    inject_agent_trace_context(&telemetry, &mut carrier).expect("context should inject");

    assert_eq!(
        carrier.get(TRACEPARENT_HEADER).map(String::as_str),
        Some(ROOT_TRACE_PARENT)
    );
    assert_eq!(
        carrier.get(TRACESTATE_HEADER).map(String::as_str),
        Some(TRACE_STATE)
    );
    assert_eq!(carrier.get("TraceParent"), None);
    assert_eq!(carrier.get("unrelated").map(String::as_str), Some("kept"));

    let mixed_case_carrier = attributes(&[
        ("TraceParent", ROOT_TRACE_PARENT),
        ("TraceState", TRACE_STATE),
    ]);
    let extracted = extract_agent_trace_context(&mixed_case_carrier)
        .expect("context should extract")
        .expect("traceparent should be present");
    assert_eq!(extracted.trace_parent.as_deref(), Some(ROOT_TRACE_PARENT));
    assert_eq!(extracted.trace_state.as_deref(), Some(TRACE_STATE));
}

#[test]
fn invalid_trace_context_is_rejected_without_mutating_extraction_carrier() {
    let mut carrier = attributes(&[(
        TRACEPARENT_HEADER,
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
    )]);
    let before = carrier.clone();

    let error =
        extract_agent_trace_context(&carrier).expect_err("all-zero trace ids should be invalid");
    assert_eq!(error.code(), "invalid-traceparent");
    assert_eq!(carrier, before);

    let error = parse_agent_trace_context(
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        None,
    )
    .expect_err("uppercase trace ids should be invalid");
    assert_eq!(error.code(), "invalid-traceparent");

    let context = AgentTelemetryContext {
        trace_parent: None,
        trace_state: Some(TRACE_STATE.to_string()),
        baggage: AgentAttributes::new(),
        span_links: Vec::new(),
    };
    let error = validate_agent_telemetry_context(&context)
        .expect_err("tracestate without traceparent should be invalid");
    assert_eq!(error.code(), "missing-traceparent");

    carrier.insert(TRACESTATE_HEADER.to_string(), "Vendor=value".to_string());
    let error = extract_agent_trace_context(&carrier)
        .expect_err("uppercase tracestate keys should be invalid");
    assert_eq!(error.code(), "invalid-tracestate");
}

#[test]
fn synchronous_child_context_uses_parent_span_without_linking() {
    let root = root_context();

    let child = agent_child_telemetry_context(&root, STEP_SPAN_ID)
        .expect("child context should be created");
    let child_trace = AgentTraceContext::from_trace_parent(
        child.trace_parent.as_deref().expect("child traceparent"),
        child.trace_state.as_deref(),
    )
    .expect("child traceparent should parse");

    assert_eq!(child_trace.trace_id, ROOT_TRACE_ID);
    assert_eq!(child_trace.span_id, STEP_SPAN_ID);
    assert_eq!(child.trace_state.as_deref(), Some(TRACE_STATE));
    assert_eq!(
        child.baggage.get("tenant_tier").map(String::as_str),
        Some("gold")
    );
    assert!(child.span_links.is_empty());
}

#[test]
fn durable_resume_context_links_back_to_the_parked_span() {
    let step_context = agent_child_telemetry_context(&root_context(), STEP_SPAN_ID)
        .expect("step context should be created");
    let resumed = agent_durable_resume_telemetry_context(
        &step_context,
        TIMER_SPAN_ID,
        attributes(&[("resume_kind", "timer-fired")]),
    )
    .expect("resume context should be created");

    let resumed_trace = AgentTraceContext::from_trace_parent(
        resumed.trace_parent.as_deref().expect("resume traceparent"),
        resumed.trace_state.as_deref(),
    )
    .expect("resume traceparent should parse");
    assert_eq!(resumed_trace.trace_id, ROOT_TRACE_ID);
    assert_eq!(resumed_trace.span_id, TIMER_SPAN_ID);

    assert_eq!(resumed.span_links.len(), 1);
    let link = &resumed.span_links[0];
    assert_eq!(link.trace_id, ROOT_TRACE_ID);
    assert_eq!(link.span_id, STEP_SPAN_ID);
    assert_eq!(link.trace_state.as_deref(), Some(TRACE_STATE));
    assert_eq!(
        link.attributes.get("resume_kind").map(String::as_str),
        Some("timer-fired")
    );

    validate_agent_telemetry_context(&resumed).expect("resume context should validate");
}

#[test]
fn durable_contracts_preserve_trace_context_for_workflow_boundaries() {
    let step_context = agent_child_telemetry_context(&root_context(), STEP_SPAN_ID)
        .expect("step context should be created");
    let public_ingress = AgentCommand::new(
        AgentCommandKind::StartRun,
        command_metadata("cmd-start", step_context.clone()),
    )
    .expect("public ingress command should build");
    assert_eq!(public_ingress.metadata.telemetry_context, step_context);

    let effect = AgentEffectSchedule::new(
        AgentEffectKind::ToolCall,
        AgentEffectTarget {
            target_type: "tool".to_string(),
            name: "search".to_string(),
            address: Some("stdio://search".to_string()),
            attributes: AgentAttributes::new(),
        },
        effect_metadata("effect-tool", step_context.clone()),
    )
    .expect("effect schedule should build")
    .into_effect()
    .expect("effect should persist");
    assert_eq!(effect.telemetry_context, step_context);

    let callback_context = agent_durable_resume_telemetry_context(
        &step_context,
        CALLBACK_SPAN_ID,
        attributes(&[("resume_kind", "effect-callback")]),
    )
    .expect("callback context should be created");
    let callback = AgentCommand::new(
        AgentCommandKind::EffectCompleted {
            effect_id: AgentEffectId::new("effect-tool"),
        },
        command_metadata("cmd-callback", callback_context.clone()),
    )
    .expect("callback command should build");
    assert_eq!(callback.metadata.telemetry_context, callback_context);

    let timer_context = agent_durable_resume_telemetry_context(
        &step_context,
        TIMER_SPAN_ID,
        attributes(&[("resume_kind", "timer-fired")]),
    )
    .expect("timer context should be created");
    let timer = AgentTimerEntry::new(
        "timer-review".into(),
        workflow_id(),
        run_id(),
        tenant_id(),
        AgentTimestampMillis::new(2_000),
        durability("timer-review", timer_context.clone()),
        AgentTimestampMillis::new(1_000),
    )
    .expect("timer should build");
    assert_eq!(timer.telemetry_context, timer_context);

    let human_context = agent_durable_resume_telemetry_context(
        &step_context,
        HUMAN_SPAN_ID,
        attributes(&[("resume_kind", "human-decision")]),
    )
    .expect("human context should be created");
    let human_submission = AgentHumanDecisionSubmission::new(
        command_metadata("cmd-human", human_context.clone()),
        HumanCheckpointId::new("checkpoint-review"),
        "approve",
        HumanCheckpointStatus::Approved,
    );
    let human_command =
        human_decision_command(&human_submission).expect("human command should build");
    assert_eq!(human_command.metadata.telemetry_context, human_context);

    let recovery_context = agent_durable_resume_telemetry_context(
        &step_context,
        RECOVERY_SPAN_ID,
        attributes(&[("resume_kind", "recovery")]),
    )
    .expect("recovery context should be created");
    let recovery = AgentCommand::new(
        AgentCommandKind::RetryRun,
        command_metadata("cmd-recovery", recovery_context.clone()),
    )
    .expect("recovery command should build");
    assert_eq!(recovery.metadata.telemetry_context, recovery_context);
}

#[test]
fn trace_context_serializes_with_span_links_for_recovery() {
    let recovered = agent_durable_resume_telemetry_context(
        &root_context(),
        RECOVERY_SPAN_ID,
        attributes(&[("resume_kind", "recovery")]),
    )
    .expect("recovery context should be created");

    let encoded = serde_json::to_string(&recovered).expect("context should serialize");
    let decoded: AgentTelemetryContext =
        serde_json::from_str(&encoded).expect("context should deserialize");

    assert_eq!(decoded, recovered);
    assert_eq!(
        decoded.span_links[0]
            .attributes
            .get("resume_kind")
            .map(String::as_str),
        Some("recovery")
    );
}

fn root_context() -> AgentTelemetryContext {
    let mut context = parse_agent_trace_context(ROOT_TRACE_PARENT, Some(TRACE_STATE))
        .expect("root trace context should parse")
        .to_telemetry_context();
    context.baggage = attributes(&[("tenant_tier", "gold")]);
    context
}

fn attributes(pairs: &[(&str, &str)]) -> AgentAttributes {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn command_metadata(
    command_id: &str,
    telemetry_context: AgentTelemetryContext,
) -> AgentCommandMetadata {
    AgentCommandMetadata::new(
        workflow_id(),
        run_id(),
        AgentCommandId::new(command_id),
        durability(command_id, telemetry_context),
        tenant_id(),
        AgentTimestampMillis::new(1_000),
    )
    .expect("command metadata should build")
}

fn effect_metadata(
    effect_id: &str,
    telemetry_context: AgentTelemetryContext,
) -> AgentEffectMetadata {
    AgentEffectMetadata::new(
        AgentEffectId::new(effect_id),
        durability(effect_id, telemetry_context),
        AgentIdempotencyKey::new(format!("{effect_id}-idempotency")),
        AgentTimestampMillis::new(1_000),
    )
    .expect("effect metadata should build")
}

fn durability(id: &str, telemetry_context: AgentTelemetryContext) -> AgentDurabilityMetadata {
    AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("{id}-dedupe")),
        AgentCausationId::new(format!("{id}-cause")),
        AgentCorrelationId::new("correlation-trace-context"),
    )
    .telemetry_context(telemetry_context)
}

fn workflow_id() -> AgentWorkflowId {
    AgentWorkflowId::new("workflow-trace-context")
}

fn run_id() -> AgentRunId {
    AgentRunId::new("run-trace-context")
}

fn tenant_id() -> AgentTenantId {
    AgentTenantId::new("tenant-trace-context")
}

/// A caller's link attributes are bounded where the context is built, not
/// discovered at export.
///
/// The returned context is *persisted*, and every span later built under it
/// copies these links onto its export record — where one over-long or
/// multi-line attribute makes the record fail validation for the whole life of
/// the run. A caller cannot be expected to know the export bound, so the
/// boundary that accepts its attributes applies it.
#[test]
fn a_resume_context_bounds_the_link_attributes_it_is_handed() {
    let step_context = agent_child_telemetry_context(
        &AgentTelemetryContext {
            trace_parent: Some(ROOT_TRACE_PARENT.to_string()),
            ..AgentTelemetryContext::default()
        },
        STEP_SPAN_ID,
    )
    .expect("the child context is created");

    let resumed = agent_durable_resume_telemetry_context(
        &step_context,
        TIMER_SPAN_ID,
        AgentAttributes::from([
            ("resume_kind".to_string(), "timer-fired".to_string()),
            ("multiline".to_string(), "two\nlines".to_string()),
            (
                "oversized".to_string(),
                "x".repeat(rakka_agent_workflow::AGENT_EXPORT_ATTRIBUTE_VALUE_MAX_BYTES + 1),
            ),
        ]),
    )
    .expect("the resume context is created");

    let link = &resumed.span_links[0];
    assert_eq!(
        link.attributes.get("resume_kind").map(String::as_str),
        Some("timer-fired"),
        "what can be exported is kept"
    );
    assert!(
        !link.attributes.contains_key("multiline") && !link.attributes.contains_key("oversized"),
        "and what cannot is dropped here rather than at export: {:?}",
        link.attributes
    );

    // The whole point: a span built under this context exports.
    rakka_agent_workflow::AgentOtelSpanExport::from_telemetry_context(
        "rakka.agent.run.resume",
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
        &resumed,
    )
    .expect("the span builds")
    .validate()
    .expect("a persisted link must never make a record unexportable");
}
