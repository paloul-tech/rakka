//! Agent workflow observability testkit assertions.

#![cfg(feature = "testkit")]

use rakka_agent_workflow::{
    agent_audit_log_event_name, agent_durable_resume_telemetry_context,
    agent_log_event_from_audit_event, record_agent_counter,
    testkit::{
        assert_agent_audit_correlation, assert_agent_log_fields,
        assert_agent_metric_attributes_bounded, assert_agent_metric_registered,
        assert_agent_otlp_bridge_export, assert_agent_resource_attributes,
        assert_agent_span_attributes, assert_agent_span_has_link, expect_agent_metric_observation,
        MinimalAgentFixture,
    },
    AgentAttributes, AgentAuditEventKind, AgentLogSeverity, AgentOtelResource, AgentOtelSpanExport,
    AgentOtlpBridgeExport, AgentOtlpExporterConfig, AgentTimestampMillis,
    AGENT_LOG_ATTR_AUDIT_EVENT_ID, AGENT_LOG_ATTR_AUDIT_KIND, AGENT_LOG_ATTR_CAUSATION_ID,
    AGENT_LOG_ATTR_CORRELATION_ID, AGENT_LOG_ATTR_RUN_ID, AGENT_METRIC_ATTR_OUTCOME,
    AGENT_METRIC_ATTR_STATUS, AGENT_METRIC_ATTR_TRANSITION, AGENT_METRIC_ATTR_WORKFLOW_TYPE,
    METRIC_AGENT_RUN_TRANSITIONS, OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME,
    OTEL_RESOURCE_K8S_NAMESPACE_NAME, OTEL_RESOURCE_K8S_POD_NAME, OTEL_RESOURCE_RAKKA_NODE_ID,
    OTEL_RESOURCE_SERVICE_NAME,
};
use rakka_core::{InMemoryMetricsRecorder, MetricKind};

const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const ROOT_SPAN_ID: &str = "00f067aa0ba902b7";
const RESUME_SPAN_ID: &str = "7c7b7a7978777675";
const ROOT_TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[test]
fn observability_testkit_asserts_representative_workflow_execution() {
    let mut fixture = MinimalAgentFixture::new();
    let workflow = fixture.sample_workflow();
    let run = fixture.sample_run_state(&workflow);
    let mut audit_event = fixture.record_sample_audit_event(&workflow, &run);
    audit_event.telemetry_context = telemetry_context();

    let metrics = InMemoryMetricsRecorder::new();
    record_agent_counter(
        &metrics,
        METRIC_AGENT_RUN_TRANSITIONS,
        1,
        &[
            (AGENT_METRIC_ATTR_WORKFLOW_TYPE, "test-workflow"),
            (AGENT_METRIC_ATTR_TRANSITION, "run-created"),
            (AGENT_METRIC_ATTR_STATUS, "accepted"),
            (AGENT_METRIC_ATTR_OUTCOME, "success"),
        ],
    )
    .expect("representative metric should record");
    let snapshot = metrics.snapshot();

    assert_agent_metric_registered(METRIC_AGENT_RUN_TRANSITIONS, MetricKind::Counter);
    let observation = expect_agent_metric_observation(
        &snapshot,
        METRIC_AGENT_RUN_TRANSITIONS,
        MetricKind::Counter,
        &[
            (AGENT_METRIC_ATTR_WORKFLOW_TYPE, "test-workflow"),
            (AGENT_METRIC_ATTR_TRANSITION, "run-created"),
            (AGENT_METRIC_ATTR_STATUS, "accepted"),
            (AGENT_METRIC_ATTR_OUTCOME, "success"),
        ],
    );
    assert_agent_metric_attributes_bounded(observation);

    let span = AgentOtelSpanExport::from_telemetry_context(
        "agent.workflow.human-checkpoint.resume",
        AgentTimestampMillis::new(100),
        AgentTimestampMillis::new(130),
        &audit_event.telemetry_context,
    )
    .expect("span should build from telemetry context");
    // The context supplies the trace identity and the links, and no
    // attributes: baggage is a propagation context rather than a span
    // attribute set, and baggage from an external caller is untrusted
    // (specification 17.15). This probe used to rely on the copy, which is
    // precisely why the copy had to be removed with a test that names it.
    assert!(
        span.attributes.is_empty(),
        "a context must contribute no span attributes: {:?}",
        span.attributes
    );
    assert!(
        !audit_event.telemetry_context.baggage.is_empty(),
        "the fixture must carry baggage, or the assertion above proves nothing"
    );

    // What an emitter decides to export, it sets.
    let span = span
        .attribute("workflow_type", "test-workflow")
        .attribute("tenant_tier", "gold")
        .attribute("step.kind", "human-checkpoint")
        .attribute("checkpoint.status", "open");
    assert_agent_span_attributes(
        &span,
        "agent.workflow.human-checkpoint.resume",
        &[
            ("workflow_type", "test-workflow"),
            ("tenant_tier", "gold"),
            ("step.kind", "human-checkpoint"),
            ("checkpoint.status", "open"),
        ],
    );
    assert_agent_span_has_link(
        &span,
        TRACE_ID,
        ROOT_SPAN_ID,
        &[
            ("resume.reason", "human-checkpoint"),
            ("resume.boundary", "human"),
        ],
    );

    let log_event = agent_log_event_from_audit_event(&audit_event, AgentTimestampMillis::new(131))
        .expect("audit event should convert to structured log");
    assert_agent_log_fields(
        &log_event,
        &agent_audit_log_event_name(AgentAuditEventKind::RunCreated),
        AgentLogSeverity::Info,
        Some(TRACE_ID),
        Some(RESUME_SPAN_ID),
        &[
            (
                AGENT_LOG_ATTR_AUDIT_EVENT_ID,
                audit_event.audit_event_id.as_str(),
            ),
            (AGENT_LOG_ATTR_AUDIT_KIND, "run-created"),
            (AGENT_LOG_ATTR_RUN_ID, run.run_id.as_str()),
            (
                AGENT_LOG_ATTR_CAUSATION_ID,
                audit_event.causation_id.as_str(),
            ),
            (
                AGENT_LOG_ATTR_CORRELATION_ID,
                audit_event.correlation_id.as_str(),
            ),
        ],
    );
    assert_agent_audit_correlation(
        &audit_event,
        audit_event.causation_id.as_str(),
        audit_event.correlation_id.as_str(),
        Some(TRACE_ID),
    );

    let resource = resource();
    assert_agent_resource_attributes(
        &resource,
        &[
            (OTEL_RESOURCE_SERVICE_NAME, "rakka-agent-workflow"),
            (OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME, "test"),
            (OTEL_RESOURCE_K8S_NAMESPACE_NAME, "rakka-system"),
            (OTEL_RESOURCE_K8S_POD_NAME, "rakka-agent-0"),
            (OTEL_RESOURCE_RAKKA_NODE_ID, "node-a"),
        ],
    );

    let export = AgentOtlpBridgeExport::from_signals(
        AgentOtlpExporterConfig::grpc("http://collector:4317"),
        resource,
        &snapshot,
        vec![span],
        vec![log_event],
    )
    .expect("bridge export should build from representative telemetry");
    assert_agent_otlp_bridge_export(
        &export,
        &[METRIC_AGENT_RUN_TRANSITIONS],
        &[
            (OTEL_RESOURCE_SERVICE_NAME, "rakka-agent-workflow"),
            (OTEL_RESOURCE_K8S_NAMESPACE_NAME, "rakka-system"),
            (OTEL_RESOURCE_RAKKA_NODE_ID, "node-a"),
        ],
    );
}

fn telemetry_context() -> rakka_agent_workflow::AgentTelemetryContext {
    let root = rakka_agent_workflow::AgentTelemetryContext {
        trace_parent: Some(ROOT_TRACE_PARENT.to_string()),
        trace_state: Some("vendor=value".to_string()),
        baggage: AgentAttributes::from([
            ("workflow_type".to_string(), "test-workflow".to_string()),
            ("tenant_tier".to_string(), "gold".to_string()),
        ]),
        span_links: Vec::new(),
    };

    agent_durable_resume_telemetry_context(
        &root,
        RESUME_SPAN_ID,
        AgentAttributes::from([
            ("resume.reason".to_string(), "human-checkpoint".to_string()),
            ("resume.boundary".to_string(), "human".to_string()),
        ]),
    )
    .expect("durable resume context should link to the parked span")
}

fn resource() -> AgentOtelResource {
    AgentOtelResource::new("rakka-agent-workflow")
        .deployment_environment("test")
        .k8s_namespace_name("rakka-system")
        .k8s_pod_name("rakka-agent-0")
        .rakka_node_id("node-a")
}
