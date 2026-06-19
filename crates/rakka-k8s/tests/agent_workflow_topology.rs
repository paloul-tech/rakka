//! Agent workflow Kubernetes reference topology contract tests.

use std::path::Path;
use std::process::Command;

const TOPOLOGY: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-reference-topology.yaml");
const TOPOLOGY_README: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-reference-topology.md");

#[test]
fn topology_documents_have_required_kubernetes_shape() {
    let docs = manifest_documents();

    assert_eq!(docs.len(), 10);
    for doc in docs {
        assert!(doc.contains("apiVersion:"), "missing apiVersion: {doc}");
        assert!(doc.contains("kind:"), "missing kind: {doc}");
        assert!(doc.contains("metadata:"), "missing metadata: {doc}");
        assert!(doc.contains("  name:"), "missing metadata.name: {doc}");
    }
}

#[test]
fn topology_defaults_to_rakka_system_namespace() {
    let namespace = document_named("Namespace", "rakka-system");
    assert!(namespace.contains("rakka.rs/topology: agent-workflow"));

    for doc in manifest_documents()
        .into_iter()
        .filter(|doc| !doc.contains("kind: Namespace"))
    {
        assert!(
            doc.contains("  namespace: rakka-system"),
            "namespaced resource should default to rakka-system: {doc}"
        );
    }
}

#[test]
fn topology_wires_local_docker_postgres_through_external_name_service() {
    let postgres_service = document_named("Service", "rakka-postgres");
    assert!(postgres_service.contains("type: ExternalName"));
    assert!(postgres_service.contains("externalName: host.docker.internal"));
    assert!(postgres_service.contains("name: postgres"));
    assert!(postgres_service.contains("port: 5432"));

    let postgres_secret = document_named("Secret", "rakka-postgres-credentials");
    assert!(postgres_secret.contains("username: postgres"));
    assert!(postgres_secret.contains("password: postgres"));
    assert!(postgres_secret.contains(
        "postgres://postgres:postgres@rakka-postgres.rakka-system.svc.cluster.local:5432/postgres"
    ));

    let config = document_named("ConfigMap", "rakka-agent-workflow-config");
    assert!(config.contains("RAKKA_POSTGRES_HOST: rakka-postgres.rakka-system.svc.cluster.local"));
    assert!(config.contains("RAKKA_POSTGRES_SSL_MODE: disable"));
    assert!(config.contains("RAKKA_POSTGRES_DSN_SECRET: rakka-postgres-credentials"));
}

#[test]
fn topology_separates_public_api_from_internal_remoting() {
    let internal_service = document_named("Service", "rakka-agent-internal");
    assert!(internal_service.contains("clusterIP: None"));
    assert!(internal_service.contains("publishNotReadyAddresses: true"));
    assert!(internal_service.contains("app.kubernetes.io/component: internal-remoting"));
    assert!(internal_service.contains("name: remoting"));
    assert!(internal_service.contains("port: 2552"));

    let public_service = document_named("Service", "rakka-agent-public");
    assert!(public_service.contains("app.kubernetes.io/component: public-api"));
    assert!(public_service.contains("name: http"));
    assert!(public_service.contains("port: 80"));
    assert!(public_service.contains("name: grpc"));
    assert!(public_service.contains("port: 50051"));
    assert!(
        !public_service.contains("remoting"),
        "public service must not expose internal remoting"
    );
}

#[test]
fn topology_defines_agent_workflow_runtime_contract() {
    let deployment = document_named("Deployment", "rakka-agent-workflow");

    for expected in [
        "replicas: 3",
        "type: RollingUpdate",
        "maxUnavailable: 0",
        "serviceAccountName: rakka-agent-workflow",
        "terminationGracePeriodSeconds: 45",
        "runAsNonRoot: true",
        "image: ghcr.io/rakka-rs/rakka-agent-workflow:0.1.0",
        "name: remoting",
        "containerPort: 2552",
        "name: http",
        "containerPort: 8080",
        "name: grpc",
        "containerPort: 50051",
        "readinessProbe:",
        "path: /ready",
        "livenessProbe:",
        "path: /live",
        "startupProbe:",
        "preStop:",
        "path: /drain",
        "RAKKA_POSTGRES_DSN",
        "RAKKA_ARTIFACT_ACCESS_KEY_ID",
        "RAKKA_ARTIFACT_SECRET_ACCESS_KEY",
        "OTEL_RESOURCE_ATTRIBUTES",
        "k8s.namespace.name=$(RAKKA_NAMESPACE)",
        "k8s.pod.name=$(RAKKA_POD_NAME)",
        "rakka.node.id=$(RAKKA_NODE_ID)",
    ] {
        assert!(
            deployment.contains(expected),
            "deployment missing {expected}"
        );
    }
}

#[test]
fn topology_carries_compatibility_observability_and_artifact_config() {
    let config = document_named("ConfigMap", "rakka-agent-workflow-config");
    for expected in [
        "RAKKA_DEPLOYMENT_PROFILE: production-like",
        "RAKKA_AGENT_WORKFLOW_CURRENT_STATE_SCHEMA_VERSION: \"1\"",
        "RAKKA_AGENT_WORKFLOW_CURRENT_INDEX_SCHEMA_VERSION: \"1\"",
        "RAKKA_AGENT_WORKFLOW_COMPAT_POLICY: n-to-n-plus-one",
        "RAKKA_REQUIRED_SERVICES: telemetry-resource,otlp-exporter,postgres,durable-state,query-index,artifact-store,actor-system,remoting,sharding,workflow-registry,operational-snapshots",
        "RAKKA_ARTIFACT_STORE_KIND: s3-compatible",
        "RAKKA_ARTIFACT_ENDPOINT: http://rakka-object-store.rakka-system.svc.cluster.local:9000",
        "RAKKA_ARTIFACT_BUCKET: rakka-agent-artifacts",
        "OTEL_SERVICE_NAME: rakka-agent-workflow",
        "OTEL_EXPORTER_OTLP_ENDPOINT: http://rakka-otel-collector.rakka-system.svc.cluster.local:4317",
        "OTEL_EXPORTER_OTLP_PROTOCOL: grpc",
        "RAKKA_PROCESS_ALLOWLIST_REQUIRED: \"true\"",
        "RAKKA_PROCESS_INHERIT_ENVIRONMENT: \"false\"",
        "RAKKA_PROTOCOL_VERSION: \"1.0\"",
        "RAKKA_COMPAT_MIN: \"1.0\"",
        "RAKKA_COMPAT_MAX: \"1.1\"",
    ] {
        assert!(config.contains(expected), "config missing {expected}");
    }

    let deployment = document_named("Deployment", "rakka-agent-workflow");
    for expected in [
        "rakka.rs/agent-workflow-spec-version: \"1.0\"",
        "rakka.rs/protocol-version: \"1.0\"",
        "rakka.rs/compatible-min: \"1.0\"",
        "rakka.rs/compatible-max: \"1.1\"",
        "rakka.rs/compat-policy: n-to-n-plus-one",
        "rakka.rs/manifest-version: \"1.0\"",
        "rakka.rs/generated-api-version: \"1.0\"",
    ] {
        assert!(
            deployment.contains(expected),
            "deployment annotations missing {expected}"
        );
    }
}

#[test]
fn topology_readme_explains_local_runtime_and_helm_path() {
    for expected in [
        "Docker Desktop",
        "rakka-system",
        "host.docker.internal",
        "rakka-postgres.rakka-system.svc.cluster.local",
        "kubectl apply --dry-run=client",
        "pg-check",
        "kubernetes-startup-readiness.md",
        "Service Boundaries",
        "Object Storage",
        "Compatibility And Migration",
        "Helm Path",
        "namespaceOverride",
        "postgres.externalName",
        "workflow.indexSchemaVersion",
    ] {
        assert!(
            TOPOLOGY_README.contains(expected),
            "README missing {expected}"
        );
    }
}

#[test]
fn optional_kubectl_validation_for_agent_workflow_topology_is_gated() {
    if std::env::var("RAKKA_AGENT_WORKFLOW_K8S_VALIDATE_MANIFESTS")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!(
            "skipping agent workflow topology validation; set RAKKA_AGENT_WORKFLOW_K8S_VALIDATE_MANIFESTS=1"
        );
        return;
    }

    let manifest = repo_root()
        .join("docs")
        .join("plans")
        .join("agentic-workflow")
        .join("kubernetes-reference-topology.yaml");
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
    TOPOLOGY
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
        .find(|doc| doc.contains(&kind) && doc.contains(&name))
        .unwrap_or_else(|| panic!("missing document {kind} {name}"))
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rakka-k8s crate should live below workspace root")
}
