//! Agent workflow OpenTelemetry Collector topology contract tests.

use std::path::Path;
use std::process::Command;

const COLLECTOR_TOPOLOGY: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-otel-collector-topology.yaml");
const COLLECTOR_README: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-otel-collector-topology.md");

#[test]
fn collector_topology_documents_have_required_kubernetes_shape() {
    let docs = manifest_documents();

    assert_eq!(docs.len(), 11);
    for doc in docs {
        assert!(doc.contains("apiVersion:"), "missing apiVersion: {doc}");
        assert!(doc.contains("kind:"), "missing kind: {doc}");
        assert!(doc.contains("metadata:"), "missing metadata: {doc}");
        assert!(doc.contains("  name:"), "missing metadata.name: {doc}");
    }
}

#[test]
fn collector_topology_uses_rakka_system_namespace() {
    let namespace = document_named("Namespace", "rakka-system");
    assert!(namespace.contains("rakka.rs/topology: agent-workflow-otel"));

    for doc in manifest_documents().into_iter().filter(|doc| {
        !doc.contains("kind: Namespace")
            && !doc.contains("kind: ClusterRole")
            && !doc.contains("kind: ClusterRoleBinding")
    }) {
        assert!(
            doc.contains("  namespace: rakka-system"),
            "namespaced resource should default to rakka-system: {doc}"
        );
    }
}

#[test]
fn collector_topology_defines_agent_and_gateway_collectors() {
    let service_account = document_named("ServiceAccount", "rakka-otel-collector");
    assert!(service_account.contains("namespace: rakka-system"));

    let agent = document_named("DaemonSet", "rakka-otel-agent");
    for expected in [
        "serviceAccountName: rakka-otel-collector",
        "image: otel/opentelemetry-collector-contrib:0.107.0",
        "--config=/conf/collector.yaml",
        "name: K8S_NODE_NAME",
        "fieldPath: spec.nodeName",
        "name: K8S_NODE_IP",
        "fieldPath: status.hostIP",
        "name: otlp-grpc",
        "containerPort: 4317",
        "hostPort: 4317",
        "name: otlp-http",
        "containerPort: 4318",
        "hostPort: 4318",
        "readinessProbe:",
        "livenessProbe:",
        "name: collector-config",
        "name: rakka-otel-agent-config",
    ] {
        assert!(agent.contains(expected), "agent missing {expected}");
    }

    let gateway = document_named("Deployment", "rakka-otel-gateway");
    for expected in [
        "replicas: 2",
        "maxUnavailable: 0",
        "serviceAccountName: rakka-otel-collector",
        "image: otel/opentelemetry-collector-contrib:0.107.0",
        "name: RAKKA_OTEL_BACKEND_OTLP_ENDPOINT",
        "otel-backend.rakka-system.svc.cluster.local:4317",
        "name: otlp-grpc",
        "containerPort: 4317",
        "name: otlp-http",
        "containerPort: 4318",
        "readinessProbe:",
        "livenessProbe:",
        "name: rakka-otel-gateway-config",
    ] {
        assert!(gateway.contains(expected), "gateway missing {expected}");
    }

    let gateway_service = document_named("Service", "rakka-otel-collector");
    assert!(gateway_service.contains("type: ClusterIP"));
    assert!(gateway_service.contains("app.kubernetes.io/name: rakka-otel-gateway"));
    assert!(gateway_service.contains("name: otlp-grpc"));
    assert!(gateway_service.contains("port: 4317"));
    assert!(gateway_service.contains("name: otlp-http"));
    assert!(gateway_service.contains("port: 4318"));

    let pdb = document_named("PodDisruptionBudget", "rakka-otel-gateway");
    assert!(pdb.contains("minAvailable: 1"));
}

#[test]
fn collector_configs_define_three_signal_pipelines() {
    for config_name in ["rakka-otel-agent-config", "rakka-otel-gateway-config"] {
        let config = document_named("ConfigMap", config_name);
        for expected in [
            "receivers:",
            "otlp:",
            "endpoint: 0.0.0.0:4317",
            "endpoint: 0.0.0.0:4318",
            "processors:",
            "memory_limiter:",
            "batch:",
            "exporters:",
            "service:",
            "pipelines:",
            "traces:",
            "metrics:",
            "logs:",
        ] {
            assert!(
                config.contains(expected),
                "{config_name} missing {expected}"
            );
        }
    }

    let agent_config = document_named("ConfigMap", "rakka-otel-agent-config");
    assert!(agent_config.contains("kubeletstats:"));
    assert!(agent_config.contains("endpoint: https://${env:K8S_NODE_IP}:10250"));
    assert!(agent_config.contains("metric_groups: [node, pod, container]"));
    assert!(agent_config.contains("hostmetrics:"));
    assert!(agent_config.contains("otlp/gateway:"));
    assert!(agent_config.contains("rakka-otel-collector.rakka-system.svc.cluster.local:4317"));

    let gateway_config = document_named("ConfigMap", "rakka-otel-gateway-config");
    assert!(gateway_config.contains("debug:"));
    assert!(gateway_config.contains("otlp/primary:"));
    assert!(gateway_config.contains("${env:RAKKA_OTEL_BACKEND_OTLP_ENDPOINT}"));
}

#[test]
fn collector_configs_enrich_kubernetes_resource_attributes() {
    for config_name in ["rakka-otel-agent-config", "rakka-otel-gateway-config"] {
        let config = document_named("ConfigMap", config_name);
        for expected in [
            "k8sattributes:",
            "auth_type: serviceAccount",
            "k8s.namespace.name",
            "k8s.pod.name",
            "k8s.pod.uid",
            "k8s.node.name",
            "k8s.deployment.name",
            "container.name",
            "resource/rakka:",
            "key: service.namespace",
            "value: rakka-system",
            "key: deployment.environment.name",
            "value: local",
        ] {
            assert!(
                config.contains(expected),
                "{config_name} missing {expected}"
            );
        }
    }
}

#[test]
fn collector_gateway_documents_redaction_sampling_and_backend_export() {
    let gateway_config = document_named("ConfigMap", "rakka-otel-gateway-config");
    for expected in [
        "transform/redact:",
        "delete_key(attributes, \"prompt_text\")",
        "delete_key(attributes, \"completion_text\")",
        "delete_key(attributes, \"tool_arguments\")",
        "delete_key(attributes, \"tool_output\")",
        "delete_key(attributes, \"artifact_uri\")",
        "delete_key(attributes, \"authorization\")",
        "delete_key(attributes, \"workflow_id\")",
        "delete_key(attributes, \"run_id\")",
        "delete_key(attributes, \"correlation_id\")",
        "probabilistic_sampler:",
        "sampling_percentage: 100",
        "sending_queue:",
        "queue_size: 4096",
        "retry_on_failure:",
        "exporters: [debug, otlp/primary]",
    ] {
        assert!(
            gateway_config.contains(expected),
            "gateway missing {expected}"
        );
    }
}

#[test]
fn collector_readme_explains_runtime_contract_and_validation() {
    for expected in [
        "OpenTelemetry Collector agent and gateway patterns",
        "rakka-otel-collector.rakka-system.svc.cluster.local:4317",
        "DaemonSet/rakka-otel-agent",
        "Deployment/rakka-otel-gateway",
        "k8sattributes",
        "container.name",
        "rakka.node.id",
        "transform/redact",
        "probabilistic_sampler",
        "kubectl apply --dry-run=client",
        "cargo test -p rakka-k8s --test agent_workflow_otel_collector_topology",
        "NetworkPolicies in Slice 6.6",
        "otel.collector.gateway.backend.endpoint",
    ] {
        assert!(
            COLLECTOR_README.contains(expected),
            "README missing {expected}"
        );
    }
}

#[test]
fn optional_kubectl_validation_for_collector_topology_is_gated() {
    if std::env::var("RAKKA_AGENT_WORKFLOW_OTEL_VALIDATE_MANIFESTS")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!(
            "skipping collector topology validation; set RAKKA_AGENT_WORKFLOW_OTEL_VALIDATE_MANIFESTS=1"
        );
        return;
    }

    let manifest = repo_root()
        .join("docs")
        .join("plans")
        .join("agentic-workflow")
        .join("kubernetes-otel-collector-topology.yaml");
    let output = Command::new("kubectl")
        .args(["apply", "--dry-run=client", "-f"])
        .arg(manifest)
        .output()
        .expect("kubectl should be available when validation is enabled");
    assert!(
        output.status.success(),
        "kubectl dry-run failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn manifest_documents() -> Vec<&'static str> {
    COLLECTOR_TOPOLOGY
        .split("\n---")
        .map(str::trim)
        .filter(|doc| !doc.is_empty() && !doc.starts_with('#'))
        .collect()
}

fn document_named(kind: &str, name: &str) -> &'static str {
    let kind = format!("kind: {kind}");
    let name = format!("  name: {name}");
    manifest_documents()
        .into_iter()
        .find(|doc| {
            doc.lines().any(|line| line.trim() == kind) && doc.lines().any(|line| line == name)
        })
        .unwrap_or_else(|| panic!("missing document {kind} {name}"))
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rakka-k8s crate should live below workspace root")
}
