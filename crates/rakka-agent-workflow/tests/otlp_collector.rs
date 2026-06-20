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
