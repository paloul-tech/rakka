//! Kubernetes example manifest contract tests.

use std::path::Path;
use std::process::Command;

use rakka_k8s::{
    KubernetesDnsDiscoveryConfig, DEFAULT_DRAIN_PATH, DEFAULT_LIVENESS_PATH, DEFAULT_READINESS_PATH,
};

const MANIFEST: &str = include_str!("../../../examples/kubernetes/rakka-node.yaml");
const SCENARIO: &str = include_str!("../../../examples/kubernetes/local-cluster-scenario.sh");
const KUBERNETES_README: &str = include_str!("../../../examples/kubernetes/README.md");

#[test]
fn manifest_documents_have_required_kubernetes_shape() {
    let docs = manifest_documents();

    assert_eq!(docs.len(), 6);
    for doc in docs {
        assert!(doc.contains("apiVersion:"), "missing apiVersion: {doc}");
        assert!(doc.contains("kind:"), "missing kind: {doc}");
        assert!(doc.contains("metadata:"), "missing metadata: {doc}");
        assert!(doc.contains("  name:"), "missing metadata.name: {doc}");
    }
}

#[test]
fn manifest_references_readiness_liveness_and_drain_hooks() {
    assert!(MANIFEST.contains(DEFAULT_READINESS_PATH));
    assert!(MANIFEST.contains(DEFAULT_LIVENESS_PATH));
    assert!(MANIFEST.contains(DEFAULT_DRAIN_PATH));
    assert!(MANIFEST.contains("readinessProbe:"));
    assert!(MANIFEST.contains("livenessProbe:"));
    assert!(MANIFEST.contains("preStop:"));
    assert!(MANIFEST.contains("path: /drain"));
    assert!(MANIFEST.contains("reason kubernetes-prestop"));
}

#[test]
fn dns_discovery_config_matches_headless_service_shape() {
    let dns = KubernetesDnsDiscoveryConfig::new("rakka-system", "rakka-internal", 2552);

    assert_eq!(
        dns.pod_host("rakka-node-0"),
        "rakka-node-0.rakka-internal.rakka-system.svc.cluster.local"
    );
    assert!(MANIFEST.contains("name: rakka-internal"));
    assert!(MANIFEST.contains("clusterIP: None"));
    assert!(MANIFEST.contains("publishNotReadyAddresses: true"));
    assert!(MANIFEST.contains("serviceName: rakka-internal"));
    assert!(MANIFEST.contains("RAKKA_HEADLESS_SERVICE"));
    assert!(MANIFEST.contains("RAKKA_CLUSTER_DOMAIN"));
}

#[test]
fn manifest_documents_ports_environment_and_rolling_compatibility() {
    for expected in [
        "RAKKA_DISCOVERY_PROVIDER",
        "kubernetes-dns",
        "RAKKA_STATEFULSET_NAME",
        "RAKKA_EXPECTED_REPLICAS",
        "RAKKA_DEPLOYMENT_PROFILE",
        "production-like",
        "RAKKA_REMOTING_TRUST_BOUNDARY",
        "trusted-cluster",
        "RAKKA_REMOTING_ALLOWED_PEERS",
        "discovery",
        "RAKKA_POD_IP",
        "name: remoting",
        "containerPort: 2552",
        "RAKKA_REMOTING_BIND_ADDR",
        "0.0.0.0:2552",
        "RAKKA_REMOTING_ADVERTISE_PORT",
        "name: http",
        "containerPort: 8080",
        "RAKKA_HTTP_BIND_ADDR",
        "name: grpc",
        "containerPort: 50051",
        "RAKKA_GRPC_BIND_ADDR",
        "RAKKA_METRICS_PATH",
        "RAKKA_OTEL_METRICS_PATH",
        "RAKKA_SNAPSHOTS_PATH",
        "RAKKA_SCENARIO_ROUTE_PATH",
        "/scenario/sharding/route-remote",
        "RAKKA_ACTOR_ASK_TIMEOUT_MS",
        "RAKKA_REMOTE_CONNECT_TIMEOUT_MS",
        "RAKKA_REMOTE_IDLE_TIMEOUT_MS",
        "RAKKA_STREAM_DRAIN_TIMEOUT_MS",
        "RAKKA_PROCESS_STARTUP_TIMEOUT_MS",
        "RAKKA_PROCESS_SHUTDOWN_TIMEOUT_MS",
        "RAKKA_PROCESS_ALLOWLIST_REQUIRED",
        "RAKKA_PROCESS_INHERIT_ENVIRONMENT",
        "RAKKA_K8S_PRESTOP_TIMEOUT_MS",
        "RAKKA_PROTOCOL_VERSION",
        "RAKKA_COMPAT_MIN",
        "RAKKA_COMPAT_MAX",
        "RAKKA_COMPAT_POLICY",
        "RAKKA_MANIFEST_VERSION",
        "RAKKA_GENERATED_API_VERSION",
        "n-to-n-plus-one",
        "rakka.rs/manifest-version: \"1.0\"",
        "rakka.rs/generated-api-version: \"1.0\"",
        "minAvailable: 2",
    ] {
        assert!(MANIFEST.contains(expected), "missing {expected}");
    }
}

#[test]
fn local_cluster_scenario_is_documented_and_dry_run_safe() {
    for expected in [
        "RAKKA_K8S_SCENARIO_DRY_RUN",
        "kubectl apply",
        "rollout status",
        "wait --for=condition=Ready",
        "get pod -l app.kubernetes.io/name=rakka-node -o wide",
        "READY_PATH",
        "LIVE_PATH",
        "METRICS_PATH",
        "SNAPSHOTS_PATH",
        "ROUTE_PATH",
        "DRAIN_PATH",
        "expect GET",
        "jsonpath={.metadata.uid}",
        "delete",
        "pod/$POD1",
        "partition",
        "set image",
    ] {
        assert!(SCENARIO.contains(expected), "missing {expected}");
    }

    let script = repo_root().join("examples/kubernetes/local-cluster-scenario.sh");
    let output = Command::new("sh")
        .arg(script)
        .env("RAKKA_K8S_SCENARIO_DRY_RUN", "1")
        .env("RAKKA_K8S_NEXT_IMAGE", "example/rakka-node:next")
        .output()
        .expect("dry-run scenario should execute");
    assert!(
        output.status.success(),
        "dry-run scenario failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("rollout status statefulset/rakka-node"));
    assert!(stdout.contains("http://127.0.0.1:8080/ready"));
    assert!(stdout.contains("http://127.0.0.1:8080/live"));
    assert!(stdout.contains("/metrics | grep rakka_http_request_latency_ms"));
    assert!(stdout.contains("/snapshots | grep kubernetes_health"));
    assert!(stdout.contains("http://127.0.0.1:8080/drain"));
    assert!(stdout.contains("delete pod/rakka-node-1"));
    assert!(stdout.contains("expect_remote=1"));
    assert!(stdout.contains("rollingUpdate"));
    assert!(stdout.contains("example/rakka-node:next"));
}

#[test]
fn kubernetes_readme_explains_multi_node_scenario_contract() {
    for expected in [
        "Application Image Contract",
        "internal Rakka remoting",
        "public HTTP/gRPC",
        "/scenario/sharding/route-remote",
        "/metrics",
        "/snapshots",
        "partitioned rolling update",
        "RAKKA_K8S_NEXT_IMAGE",
        "readiness should fail after drain",
        "coordinated pre-stop path",
        "kubernetes-prestop",
    ] {
        assert!(
            KUBERNETES_README.contains(expected),
            "README missing {expected}"
        );
    }
}

#[test]
fn optional_kubectl_manifest_validation_is_gated() {
    if std::env::var("RAKKA_K8S_VALIDATE_MANIFESTS")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!("skipping kubectl manifest validation; set RAKKA_K8S_VALIDATE_MANIFESTS=1");
        return;
    }

    let manifest = repo_root().join("examples/kubernetes/rakka-node.yaml");
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

#[test]
fn optional_local_cluster_scenario_is_gated() {
    if std::env::var("RAKKA_K8S_RUN_LOCAL_CLUSTER").ok().as_deref() != Some("1") {
        eprintln!("skipping local cluster scenario; set RAKKA_K8S_RUN_LOCAL_CLUSTER=1");
        return;
    }

    let script = repo_root().join("examples/kubernetes/local-cluster-scenario.sh");
    let output = Command::new("sh")
        .arg(script)
        .output()
        .expect("local cluster scenario should execute when enabled");
    assert!(
        output.status.success(),
        "local cluster scenario failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn manifest_documents() -> Vec<&'static str> {
    MANIFEST
        .split("\n---")
        .map(str::trim)
        .filter(|doc| !doc.is_empty() && !doc.starts_with('#'))
        .collect()
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rakka-k8s crate should live below workspace root")
}
