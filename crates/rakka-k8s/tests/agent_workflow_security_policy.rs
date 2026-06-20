//! Agent workflow Kubernetes security policy contract tests.

use std::path::Path;
use std::process::Command;

const SECURITY_POLICY: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-security-policy.yaml");
const SECURITY_GUIDE: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-security-policy.md");
const APP_TOPOLOGY: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-reference-topology.yaml");
const COLLECTOR_TOPOLOGY: &str =
    include_str!("../../../docs/plans/agentic-workflow/kubernetes-otel-collector-topology.yaml");

#[test]
fn security_policy_documents_have_required_kubernetes_shape() {
    let docs = security_policy_documents();

    assert_eq!(docs.len(), 10);
    for doc in docs {
        assert!(doc.contains("apiVersion: networking.k8s.io/v1"));
        assert!(doc.lines().any(|line| line == "kind: NetworkPolicy"));
        assert!(doc.contains("metadata:"), "missing metadata: {doc}");
        assert!(doc.contains("  name:"), "missing metadata.name: {doc}");
        assert!(doc.contains("spec:"), "missing spec: {doc}");
    }
}

#[test]
fn security_policy_defaults_to_rakka_system_namespace() {
    for doc in security_policy_documents() {
        assert!(
            doc.contains("  namespace: rakka-system"),
            "NetworkPolicy should default to rakka-system: {doc}"
        );
        assert!(doc.contains("rakka.rs/policy: security-envelope"));
    }
}

#[test]
fn security_policy_starts_with_default_deny_and_dns() {
    let deny_ingress = security_policy_named("rakka-default-deny-ingress");
    assert!(deny_ingress.contains("podSelector: {}"));
    assert!(deny_ingress.contains("policyTypes:\n    - Ingress"));
    assert!(
        !deny_ingress.contains("ingress:"),
        "default deny ingress should not open lanes"
    );

    let deny_egress = security_policy_named("rakka-default-deny-egress");
    assert!(deny_egress.contains("podSelector: {}"));
    assert!(deny_egress.contains("policyTypes:\n    - Egress"));
    assert!(
        !deny_egress.contains("egress:"),
        "default deny egress should not open lanes"
    );

    let dns = security_policy_named("rakka-allow-dns-egress");
    assert!(dns.contains("kubernetes.io/metadata.name: kube-system"));
    assert!(dns.contains("k8s-app: kube-dns"));
    assert!(dns.contains("protocol: UDP"));
    assert!(dns.contains("protocol: TCP"));
    assert!(dns.contains("port: 53"));
}

#[test]
fn security_policy_limits_public_internal_database_and_collector_lanes() {
    let public_api = security_policy_named("rakka-public-api-ingress");
    assert!(public_api.contains("app.kubernetes.io/component: agent-runtime"));
    assert!(public_api.contains("rakka.rs/ingress: public"));
    assert!(public_api.contains("app.kubernetes.io/component: ingress-controller"));
    assert!(public_api.contains("port: 8080"));
    assert!(public_api.contains("port: 50051"));
    assert!(
        !public_api.contains("2552"),
        "public API policy must not expose internal remoting"
    );

    let remoting = security_policy_named("rakka-agent-remoting");
    assert!(remoting.contains("policyTypes:\n    - Ingress\n    - Egress"));
    assert!(remoting.contains("port: 2552"));
    assert!(remoting.contains("app.kubernetes.io/component: agent-runtime"));

    let postgres = security_policy_named("rakka-agent-postgres-egress");
    assert!(postgres.contains("policyTypes:\n    - Egress"));
    assert!(postgres.contains("cidr: 0.0.0.0/0"));
    assert!(postgres.contains("port: 5432"));

    let artifacts = security_policy_named("rakka-agent-artifact-egress");
    assert!(artifacts.contains("app.kubernetes.io/name: rakka-object-store"));
    assert!(artifacts.contains("port: 9000"));
    assert!(artifacts.contains("port: 443"));

    let app_otel = security_policy_named("rakka-agent-otel-egress");
    assert!(app_otel.contains("app.kubernetes.io/name: rakka-otel-gateway"));
    assert!(app_otel.contains("port: 4317"));
    assert!(app_otel.contains("port: 4318"));

    let gateway = security_policy_named("rakka-otel-gateway");
    assert!(gateway.contains("app.kubernetes.io/name: rakka-agent-workflow"));
    assert!(gateway.contains("app.kubernetes.io/name: rakka-otel-agent"));
    assert!(gateway.contains("port: 4317"));
    assert!(gateway.contains("port: 443"));

    let agent = security_policy_named("rakka-otel-agent");
    assert!(agent.contains("app.kubernetes.io/name: rakka-agent-workflow"));
    assert!(agent.contains("app.kubernetes.io/name: rakka-otel-gateway"));
    assert!(agent.contains("port: 10250"));
}

#[test]
fn security_guide_documents_auth_secret_tool_and_operational_boundaries() {
    for expected in [
        "Public workflow API",
        "Human checkpoints",
        "Internal remoting",
        "PostgreSQL",
        "Collector",
        "Tool/process execution",
        "Operational endpoints",
        "authenticated principal identity",
        "authorization scope",
        "idempotency key or message id",
        "native Kubernetes NetworkPolicy cannot inspect HTTP paths",
        "Docker Desktop may validate NetworkPolicy objects without enforcing them",
        "automountServiceAccountToken: false",
        "RAKKA_PROCESS_ALLOWLIST_REQUIRED=true",
        "RAKKA_PROCESS_INHERIT_ENVIRONMENT=false",
        "Model/tool/provider egress is opened explicitly per deployment",
        "Policy Checklist",
        "Kubernetes NetworkPolicy",
        "Kubernetes Pod Security Standards",
        "Kubernetes Secrets",
        "Kubernetes Service Accounts",
    ] {
        assert!(
            SECURITY_GUIDE.contains(expected),
            "security guide missing {expected}"
        );
    }
}

#[test]
fn reference_app_topology_uses_safe_service_account_and_container_defaults() {
    let service_account = app_document_named("ServiceAccount", "rakka-agent-workflow");
    assert!(service_account.contains("automountServiceAccountToken: false"));

    let config = app_document_named("ConfigMap", "rakka-agent-workflow-config");
    assert!(config.contains("RAKKA_PROCESS_ALLOWLIST_REQUIRED: \"true\""));
    assert!(config.contains("RAKKA_PROCESS_INHERIT_ENVIRONMENT: \"false\""));

    let deployment = app_document_named("Deployment", "rakka-agent-workflow");
    for expected in [
        "serviceAccountName: rakka-agent-workflow",
        "automountServiceAccountToken: false",
        "runAsNonRoot: true",
        "seccompProfile:",
        "type: RuntimeDefault",
        "securityContext:",
        "allowPrivilegeEscalation: false",
        "readOnlyRootFilesystem: true",
        "capabilities:",
        "drop:",
        "- ALL",
    ] {
        assert!(
            deployment.contains(expected),
            "app deployment missing {expected}"
        );
    }

    let public_service = app_document_named("Service", "rakka-agent-public");
    assert!(
        !public_service.contains("remoting"),
        "public service must not expose remoting"
    );
}

#[test]
fn collector_topology_uses_hardened_containers_with_intentional_rbac_exception() {
    let collector_service_account =
        collector_document_named("ServiceAccount", "rakka-otel-collector");
    assert!(
        !collector_service_account.contains("automountServiceAccountToken: false"),
        "Collector service account needs API access for Kubernetes enrichment"
    );

    let cluster_role = collector_document_named("ClusterRole", "rakka-otel-collector");
    for expected in [
        "resources: [\"pods\", \"namespaces\", \"nodes\"]",
        "resources: [\"nodes/stats\"]",
        "verbs: [\"get\"]",
        "resources: [\"deployments\", \"replicasets\"]",
    ] {
        assert!(cluster_role.contains(expected), "RBAC missing {expected}");
    }

    for (kind, name) in [
        ("DaemonSet", "rakka-otel-agent"),
        ("Deployment", "rakka-otel-gateway"),
    ] {
        let workload = collector_document_named(kind, name);
        for expected in [
            "runAsNonRoot: true",
            "seccompProfile:",
            "type: RuntimeDefault",
            "allowPrivilegeEscalation: false",
            "readOnlyRootFilesystem: true",
            "capabilities:",
            "drop:",
            "- ALL",
        ] {
            assert!(
                workload.contains(expected),
                "{kind}/{name} missing {expected}"
            );
        }
    }
}

#[test]
fn optional_kubectl_validation_for_security_policy_is_gated() {
    if std::env::var("RAKKA_AGENT_WORKFLOW_SECURITY_VALIDATE_MANIFESTS")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!(
            "skipping security policy validation; set RAKKA_AGENT_WORKFLOW_SECURITY_VALIDATE_MANIFESTS=1"
        );
        return;
    }

    let manifest = repo_root()
        .join("docs")
        .join("plans")
        .join("agentic-workflow")
        .join("kubernetes-security-policy.yaml");
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

fn security_policy_documents() -> Vec<&'static str> {
    manifest_documents(SECURITY_POLICY)
}

fn security_policy_named(name: &str) -> &'static str {
    document_named(SECURITY_POLICY, "NetworkPolicy", name)
}

fn app_document_named(kind: &str, name: &str) -> &'static str {
    document_named(APP_TOPOLOGY, kind, name)
}

fn collector_document_named(kind: &str, name: &str) -> &'static str {
    document_named(COLLECTOR_TOPOLOGY, kind, name)
}

fn manifest_documents(manifest: &'static str) -> Vec<&'static str> {
    manifest
        .split("\n---")
        .map(str::trim)
        .filter(|doc| !doc.is_empty() && !doc.starts_with('#'))
        .collect()
}

fn document_named(manifest: &'static str, kind: &str, name: &str) -> &'static str {
    let kind = format!("kind: {kind}");
    let name = format!("  name: {name}");
    manifest_documents(manifest)
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
