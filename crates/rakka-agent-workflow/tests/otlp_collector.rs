//! OTLP bridge and Collector integration tests.

use rakka_agent_workflow::{
    record_agent_counter, AgentAttributes, AgentLogEvent, AgentLogSeverity, AgentOtelResource,
    AgentOtelSpanExport, AgentOtlpBridgeExport, AgentOtlpBridgeReceiver, AgentOtlpExporterConfig,
    AgentOtlpProtocol, AgentOtlpSignal, AgentTelemetryContext, AgentTimestampMillis,
    InMemoryAgentOtlpReceiver, METRIC_AGENT_RUN_TRANSITIONS, OTEL_EXPORTER_OTLP_ENDPOINT,
    OTEL_EXPORTER_OTLP_HEADERS, OTEL_EXPORTER_OTLP_LOGS_ENDPOINT, OTEL_EXPORTER_OTLP_PROTOCOL,
    OTEL_RESOURCE_CONTAINER_NAME, OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME,
    OTEL_RESOURCE_K8S_DEPLOYMENT_NAME, OTEL_RESOURCE_K8S_NAMESPACE_NAME,
    OTEL_RESOURCE_K8S_NODE_NAME, OTEL_RESOURCE_K8S_POD_NAME, OTEL_RESOURCE_K8S_POD_UID,
    OTEL_RESOURCE_RAKKA_NODE_ID, OTEL_RESOURCE_SERVICE_INSTANCE_ID, OTEL_RESOURCE_SERVICE_NAME,
    OTEL_RESOURCE_SERVICE_NAMESPACE, OTEL_RESOURCE_SERVICE_VERSION,
};
use rakka_core::InMemoryMetricsRecorder;

const TRACE_PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

#[test]
fn resource_helpers_emit_service_kubernetes_and_rakka_attributes() {
    let resource = resource();

    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_SERVICE_NAME)
            .map(String::as_str),
        Some("rakka-agent-workflow")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_SERVICE_NAMESPACE)
            .map(String::as_str),
        Some("rakka")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME)
            .map(String::as_str),
        Some("test")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_K8S_POD_NAME)
            .map(String::as_str),
        Some("rakka-agent-0")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_K8S_POD_UID)
            .map(String::as_str),
        Some("pod-uid-1")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_K8S_NODE_NAME)
            .map(String::as_str),
        Some("node-1")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_K8S_DEPLOYMENT_NAME)
            .map(String::as_str),
        Some("rakka-agent")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_CONTAINER_NAME)
            .map(String::as_str),
        Some("rakka")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_SERVICE_VERSION)
            .map(String::as_str),
        Some("0.1.0")
    );
    assert_eq!(
        resource
            .attributes
            .get(OTEL_RESOURCE_RAKKA_NODE_ID)
            .map(String::as_str),
        Some("node-a")
    );

    let metric_attributes = resource.to_metric_attributes();
    assert!(metric_attributes
        .iter()
        .any(|attribute| attribute.key() == OTEL_RESOURCE_SERVICE_INSTANCE_ID));
    resource.validate().expect("resource should validate");
}

#[test]
fn exporter_config_accepts_otel_env_style_values() {
    let env = AgentAttributes::from([
        (
            OTEL_EXPORTER_OTLP_ENDPOINT.to_string(),
            "http://collector:4318".to_string(),
        ),
        (
            OTEL_EXPORTER_OTLP_PROTOCOL.to_string(),
            "http/protobuf".to_string(),
        ),
        (
            OTEL_EXPORTER_OTLP_LOGS_ENDPOINT.to_string(),
            "http://collector:4318/v1/logs".to_string(),
        ),
        (
            OTEL_EXPORTER_OTLP_HEADERS.to_string(),
            "authorization=Bearer test,tenant=rakka".to_string(),
        ),
    ]);

    let config =
        AgentOtlpExporterConfig::from_env_map(&env).expect("env map should parse as OTLP config");

    assert_eq!(config.protocol, AgentOtlpProtocol::HttpProtobuf);
    assert_eq!(
        config.endpoint_for_signal(AgentOtlpSignal::Metrics),
        "http://collector:4318"
    );
    assert_eq!(
        config.endpoint_for_signal(AgentOtlpSignal::Logs),
        "http://collector:4318/v1/logs"
    );
    assert_eq!(
        config.headers.get("authorization").map(String::as_str),
        Some("Bearer test")
    );
}

#[tokio::test]
async fn bridge_export_routes_metrics_spans_and_logs_to_deterministic_receiver() {
    let metrics = InMemoryMetricsRecorder::new();
    record_agent_counter(
        &metrics,
        METRIC_AGENT_RUN_TRANSITIONS,
        1,
        &[("workflow_type", "research"), ("transition", "start")],
    )
    .expect("bounded metric labels should record");

    let telemetry_context = telemetry_context();
    let span = AgentOtelSpanExport::from_telemetry_context(
        "agent.workflow.step",
        AgentTimestampMillis::new(100),
        AgentTimestampMillis::new(125),
        &telemetry_context,
    )
    .expect("span should build")
    .attribute("step.kind", "tool");
    let log = AgentLogEvent::new(
        "rakka.agent_workflow.run.started",
        AgentLogSeverity::Info,
        AgentTimestampMillis::new(126),
        AgentTimestampMillis::new(126),
    )
    .telemetry_context(&telemetry_context)
    .expect("log should get trace correlation");

    let export = AgentOtlpBridgeExport::from_signals(
        AgentOtlpExporterConfig::grpc("http://collector:4317"),
        resource(),
        &metrics.snapshot(),
        vec![span],
        vec![log],
    )
    .expect("bridge export should build");
    let mut receiver = InMemoryAgentOtlpReceiver::new();
    receiver
        .export_bridge(export)
        .await
        .expect("deterministic receiver should accept export");

    let captured = &receiver.exports()[0];
    assert_eq!(
        captured.resource.attributes[OTEL_RESOURCE_SERVICE_NAME],
        "rakka-agent-workflow"
    );
    assert!(captured
        .metrics
        .metrics()
        .iter()
        .any(|metric| metric.name() == METRIC_AGENT_RUN_TRANSITIONS));
    assert_eq!(
        captured.spans[0].trace_id,
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );
    assert_eq!(
        captured.logs[0].trace_id.as_deref(),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    assert!(captured
        .metrics
        .resource_attributes()
        .iter()
        .any(|attribute| attribute.key() == OTEL_RESOURCE_K8S_NAMESPACE_NAME));
}

/// The optional instrumentation scope a batch carries is validated on the
/// receive path, the same way its spans, exporter, and resource are — a
/// blank-named or unversioned scope cannot ride in unchecked
/// ([specification 17.17]).
#[tokio::test]
async fn a_batch_with_a_blank_scope_is_rejected_on_receive() {
    use rakka_agent_workflow::AgentOtelInstrumentationScope;

    let base = AgentOtlpBridgeExport::from_signals(
        AgentOtlpExporterConfig::grpc("http://collector:4317"),
        resource(),
        &InMemoryMetricsRecorder::new().snapshot(),
        Vec::new(),
        Vec::new(),
    )
    .expect("the bridge export builds");

    // A well-formed scope validates and is accepted.
    let valid = base.clone().with_scope(AgentOtelInstrumentationScope {
        name: "rakka.agent".to_string(),
        version: "0.1.0".to_string(),
        schema_url: Some("https://opentelemetry.io/schemas/1.36.0".to_string()),
    });
    valid.validate().expect("a well-formed scope validates");
    InMemoryAgentOtlpReceiver::new()
        .export_bridge(valid)
        .await
        .expect("the receiver accepts a well-formed scope");

    // A blank scope name fails validation directly and on the receive path.
    let blank = base.with_scope(AgentOtelInstrumentationScope {
        name: String::new(),
        version: "0.1.0".to_string(),
        schema_url: None,
    });
    blank
        .validate()
        .expect_err("a blank scope name must fail validation");
    let mut receiver = InMemoryAgentOtlpReceiver::new();
    receiver
        .export_bridge(blank)
        .await
        .expect_err("the receiver rejects a batch carrying a blank scope");
    assert!(
        receiver.exports().is_empty(),
        "nothing was accepted from the rejected batch"
    );
}

#[test]
fn collector_config_example_defines_three_otlp_pipelines() {
    let config = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/plans/agentic-workflow/otel-collector-local.yaml"
    ));

    assert!(config.contains("receivers:"));
    assert!(config.contains("otlp:"));
    assert!(config.contains("endpoint: 0.0.0.0:4317"));
    assert!(config.contains("endpoint: 0.0.0.0:4318"));
    assert!(config.contains("traces:"));
    assert!(config.contains("metrics:"));
    assert!(config.contains("logs:"));
    assert!(config.contains("exporters: [debug]"));
}

#[test]
fn kubernetes_collector_topology_defines_agent_gateway_and_resource_enrichment() {
    let topology = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/plans/agentic-workflow/kubernetes-otel-collector-topology.yaml"
    ));

    for expected in [
        "kind: DaemonSet",
        "name: rakka-otel-agent",
        "kind: Deployment",
        "name: rakka-otel-gateway",
        "kind: Service",
        "name: rakka-otel-collector",
        "rakka-otel-agent-config",
        "rakka-otel-gateway-config",
        "kubeletstats:",
        "hostmetrics:",
        "k8sattributes:",
        "k8s.namespace.name",
        "k8s.pod.name",
        "k8s.pod.uid",
        "k8s.node.name",
        "k8s.deployment.name",
        "container.name",
        "service.namespace",
        "deployment.environment.name",
        "otlp/gateway:",
        "otlp/primary:",
        "transform/redact:",
        "probabilistic_sampler:",
        "sending_queue:",
        "retry_on_failure:",
    ] {
        assert!(topology.contains(expected), "topology missing {expected}");
    }
}

fn resource() -> AgentOtelResource {
    AgentOtelResource::new("rakka-agent-workflow")
        .service_namespace("rakka")
        .service_version("0.1.0")
        .service_instance_id("rakka-agent-0")
        .deployment_environment("test")
        .k8s_namespace_name("rakka-system")
        .k8s_pod_name("rakka-agent-0")
        .k8s_pod_uid("pod-uid-1")
        .k8s_node_name("node-1")
        .k8s_deployment_name("rakka-agent")
        .container_name("rakka")
        .rakka_node_id("node-a")
}

fn telemetry_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(TRACE_PARENT.to_string()),
        trace_state: Some("vendor=value".to_string()),
        baggage: AgentAttributes::from([("workflow_type".to_string(), "research".to_string())]),
        span_links: Vec::new(),
    }
}

/// The span-kind/status/events/scope extension is additive: a bridge record
/// serialized before the fields existed decodes with the defaults, and the
/// fields round-trip once set. This is what lets the agent-domain GenAI
/// adapter compose over the bridge without a breaking bridge revision.
#[test]
fn a_pre_extension_span_record_decodes_with_default_kind_status_and_events() {
    use rakka_agent_workflow::{AgentOtelSpanEvent, AgentOtelSpanKind, AgentOtelSpanStatus};

    let telemetry = AgentTelemetryContext {
        trace_parent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        ..AgentTelemetryContext::default()
    };
    let span = AgentOtelSpanExport::from_telemetry_context(
        "rakka.agent.effect.dispatch",
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
        &telemetry,
    )
    .expect("the span builds");

    let mut raw = serde_json::to_value(&span).expect("the span serializes");
    let object = raw.as_object_mut().expect("the span is an object");
    assert!(object.remove("kind").is_some());
    assert!(object.remove("status").is_some());
    assert!(object.remove("events").is_some());
    let decoded: AgentOtelSpanExport =
        serde_json::from_value(raw).expect("a pre-extension record decodes");
    assert_eq!(decoded.kind, AgentOtelSpanKind::Internal);
    assert_eq!(decoded.status, AgentOtelSpanStatus::Unset);
    assert!(decoded.events.is_empty());
    assert_eq!(
        decoded, span,
        "the defaults are the pre-extension semantics"
    );

    let stamped = span
        .kind(AgentOtelSpanKind::Consumer)
        .status(AgentOtelSpanStatus::Ok)
        .event(AgentOtelSpanEvent {
            name: "rakka.agent.decide".to_string(),
            time: AgentTimestampMillis::new(2),
            attributes: AgentAttributes::new(),
        });
    stamped.validate().expect("the extended record validates");
    let round_tripped: AgentOtelSpanExport =
        serde_json::from_value(serde_json::to_value(&stamped).expect("the record serializes"))
            .expect("the record round-trips");
    assert_eq!(round_tripped, stamped);
}

/// A span record built from a durable context carries the trace identity and
/// the links, and **no attributes at all**.
///
/// It used to copy the context's baggage verbatim into span attributes, past
/// a `validate` that inspected attributes not at all. Baggage is a
/// propagation context rather than a span attribute set, and baggage received
/// from an external caller is untrusted (specification 17.15), so a caller
/// that wants an attribute now sets it explicitly — which is what makes an
/// exported set exactly what its emitter decided to export.
#[test]
fn a_span_built_from_a_context_copies_no_baggage_into_its_attributes() {
    let context = telemetry_context();
    assert!(
        !context.baggage.is_empty(),
        "the fixture must carry baggage, or this proves nothing"
    );

    let span = AgentOtelSpanExport::from_telemetry_context(
        "agent.workflow.step",
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
        &context,
    )
    .expect("the span builds");

    assert!(
        span.attributes.is_empty(),
        "baggage reached the span attributes: {:?}",
        span.attributes
    );
    // The trace identity and the links are what a context does supply.
    assert_eq!(span.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(span.links.len(), context.span_links.len());
    span.validate().expect("the record is valid");

    // An explicitly-set attribute is kept, so the change removes a copy and
    // not the ability to carry attributes.
    let span = span.attribute("step.kind", "tool");
    assert_eq!(
        span.attributes.get("step.kind").map(String::as_str),
        Some("tool")
    );
}

/// The export records now carry the generic bounds the metric vocabulary
/// always had and this side never did: a blank key, an unbounded value, a
/// multi-line value, and an inverted time range are each refused.
#[test]
fn an_unbounded_or_malformed_span_attribute_is_refused() {
    let base = AgentOtelSpanExport::from_telemetry_context(
        "agent.workflow.step",
        AgentTimestampMillis::new(10),
        AgentTimestampMillis::new(20),
        &telemetry_context(),
    )
    .expect("the span builds");

    base.clone().validate().expect("the base record is valid");

    let blank_key = base.clone().attribute("   ", "value");
    assert!(blank_key.validate().is_err(), "a blank key is refused");

    let overlong = base.clone().attribute("step.kind", "x".repeat(4096));
    assert!(
        overlong.validate().is_err(),
        "an unbounded value is refused"
    );

    let multiline = base.clone().attribute("step.kind", "tool\nmore");
    assert!(
        multiline.validate().is_err(),
        "a multi-line value is refused"
    );

    let mut inverted = base;
    inverted.start_time = AgentTimestampMillis::new(30);
    assert!(
        inverted.validate().is_err(),
        "an end before its start is refused"
    );
}
