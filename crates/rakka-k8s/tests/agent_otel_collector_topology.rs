//! The agent-domain OpenTelemetry Collector topology.
//!
//! Specification: [17.14](../../../docs/plans/rakka-agent/spec.md) (allowlist
//! and redaction as defence in depth),
//! [17.16](../../../docs/plans/rakka-agent/spec.md) (tail sampling and
//! trace-ID-aware routing), [17.17](../../../docs/plans/rakka-agent/spec.md)
//! (the OTLP and Collector boundary).
//!
//! The workflow domain's topology is contract-tested by its sibling
//! `agent_workflow_otel_collector_topology.rs`, and this suite deliberately
//! mirrors its shape. What it does *not* mirror is the content: that topology
//! is a **denylist** of six content keys, none of which the GenAI vocabulary
//! uses, and its metric rules drop workflow identifiers. The agent domain's is
//! an allowlist keyed on `gen_ai.*` and `rakka.agent.*`.
//!
//! **This suite deliberately does not check the allowlists themselves.** It
//! cannot: `rakka-k8s` sits below `rakka-agent` in the crate DAG and cannot
//! see `AGENT_SPAN_ATTRIBUTE_KEYS`. Checking a list of strings against a copy
//! of that list in this file would be a configuration validated against
//! itself, which passes forever while dropping everything in production. The
//! bijection lives in `crates/rakka-agent/tests/collector_allowlist.rs`, which
//! reads the same YAML and the constants.

use std::path::Path;
use std::process::Command;

const COLLECTOR_TOPOLOGY: &str =
    include_str!("../../../docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.yaml");
const COLLECTOR_README: &str =
    include_str!("../../../docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.md");

/// The pinned distribution. An upgrade is the
/// [17.20](../../../docs/plans/rakka-agent/spec.md) review, and the manifest
/// and this assertion move together.
const COLLECTOR_IMAGE: &str = "otel/opentelemetry-collector-contrib:0.159.0";

#[test]
fn the_topology_has_the_required_kubernetes_shape() {
    let documents = manifest_documents();
    assert_eq!(
        documents.len(),
        12,
        "the topology is twelve documents: namespace, RBAC, two configs, three \
         services, a DaemonSet, a Deployment, and a PDB"
    );
    for document in &documents {
        for required in ["apiVersion:", "kind:", "metadata:", "  name:"] {
            assert!(
                document.contains(required),
                "a manifest document is missing `{required}`"
            );
        }
    }
}

#[test]
fn the_topology_lives_in_the_rakka_system_namespace() {
    let namespace = document_named("Namespace", "rakka-system");
    assert!(
        namespace.contains("rakka.rs/topology: rakka-agent-otel"),
        "the namespace labels which topology it carries"
    );
    for document in manifest_documents() {
        let cluster_scoped = [
            "kind: Namespace",
            "kind: ClusterRole",
            "kind: ClusterRoleBinding",
        ]
        .iter()
        .any(|kind| document.lines().any(|line| line.trim() == *kind));
        if cluster_scoped {
            continue;
        }
        assert!(
            document.contains("  namespace: rakka-system"),
            "a namespaced document does not declare its namespace"
        );
    }
}

#[test]
fn both_tiers_pin_the_reviewed_collector_distribution() {
    for (kind, name) in [
        ("DaemonSet", "rakka-agent-otel-agent"),
        ("Deployment", "rakka-agent-otel-gateway"),
    ] {
        let document = document_named(kind, name);
        assert!(
            document.contains(COLLECTOR_IMAGE),
            "{name} does not pin {COLLECTOR_IMAGE}; an unpinned or drifted \
             distribution is not a compatibility guarantee (17.17)"
        );
        assert!(
            document.contains("--config=/conf/collector.yaml"),
            "{name} does not load its ConfigMap"
        );
    }
}

/// Tail sampling needs pod addresses, and a `ClusterIP` service cannot give
/// them.
///
/// This is the assertion that would have caught the failure mode by itself.
/// [17.16](../../../docs/plans/rakka-agent/spec.md) requires every span of one
/// trace to reach the same decision instance; the agent tier's `loadbalancing`
/// exporter achieves that by resolving gateway **pods**, and its `k8s`
/// resolver reads a headless service. Point it at a `ClusterIP` and kube-proxy
/// spreads spans of one trace across gateway replicas, each of which then
/// tail-samples a partial trace — while every other string assertion in this
/// file still passes.
#[test]
fn the_gateway_service_the_router_resolves_is_headless() {
    let headless = document_named("Service", "rakka-agent-otel-gateway-headless");
    assert!(
        headless.contains("clusterIP: None"),
        "the service the loadbalancing resolver reads must be headless"
    );
    assert!(
        headless.contains("app.kubernetes.io/name: rakka-agent-otel-gateway"),
        "and must select the gateway pods"
    );
    let agent_config = document_named("ConfigMap", "rakka-agent-otel-agent-config");
    assert!(
        agent_config.contains("routing_key: traceID"),
        "the agent tier routes by trace id"
    );
    assert!(
        agent_config.contains("service: rakka-agent-otel-gateway-headless.rakka-system"),
        "and resolves the headless service, not the ClusterIP one"
    );
}

/// Metrics are routed by a different exporter, and the reason is not cosmetic.
///
/// The `loadbalancing` exporter refuses `routing_key: traceID` for metrics —
/// the pinned distribution rejects the configuration at startup. Wiring the
/// metrics pipeline to it would produce a Collector that never starts, which a
/// string assertion for `loadbalancing` would happily have called correct.
#[test]
fn the_agent_tier_routes_metrics_off_the_trace_router() {
    let agent_config = document_named("ConfigMap", "rakka-agent-otel-agent-config");
    let metrics_pipeline = pipeline_of(agent_config, "metrics");
    assert!(
        metrics_pipeline.contains("otlp/gateway"),
        "the metrics pipeline uses the plain gateway exporter"
    );
    assert!(
        !metrics_pipeline.contains("loadbalancing"),
        "and never the trace-id router, which cannot serve metrics"
    );
    for signal in ["traces", "logs"] {
        assert!(
            pipeline_of(agent_config, signal).contains("loadbalancing"),
            "the {signal} pipeline routes by trace id"
        );
    }
}

/// The gateway allowlists, tail-samples, and batches — in that order.
///
/// Order is load-bearing: allowlisting after sampling would retain traces
/// whose attributes were then stripped, and batching before sampling would
/// hand the sampler batches rather than traces.
#[test]
fn the_gateway_allowlists_then_tail_samples() {
    let gateway = document_named("ConfigMap", "rakka-agent-otel-gateway-config");
    let traces = pipeline_of(gateway, "traces");
    let allowlist = traces
        .find("transform/allowlist")
        .expect("the traces pipeline allowlists");
    let sampling = traces
        .find("tail_sampling")
        .expect("the traces pipeline tail-samples");
    let batch = traces.rfind("batch").expect("the traces pipeline batches");
    assert!(
        allowlist < sampling && sampling < batch,
        "allowlist, then sample, then batch"
    );
    assert!(
        !gateway.contains("probabilistic_sampler:"),
        "the head sampler is replaced by tail sampling, not run beside it"
    );
    for signal in ["metrics", "logs"] {
        assert!(
            pipeline_of(gateway, signal).contains("transform/allowlist"),
            "the {signal} pipeline allowlists too"
        );
    }
}

/// Every one of 17.16's eight retention classes has a policy.
#[test]
fn every_retention_class_has_a_policy() {
    let gateway = document_named("ConfigMap", "rakka-agent-otel-gateway-config");
    for policy in [
        "error-status",
        "stable-failure-code",
        "security-denial-or-revocation",
        "indeterminate-effect-or-reconciliation",
        "checkpoint-escalation-or-timeout",
        "recovery-failure-or-stale-owner",
        "configured-high-latency",
        "excessive-retry",
        "version-under-investigation",
        "routine-successful-turns",
    ] {
        assert!(
            gateway.contains(&format!("name: {policy}")),
            "17.16 retention class `{policy}` has no tail-sampling policy"
        );
    }
    // Every agent-domain span attribute is a string, because `AgentAttributes`
    // is a `BTreeMap<String, String>`. A `numeric_attribute` policy over one
    // of them matches nothing in production while passing its own test, which
    // is the exact failure this slice was warned about.
    assert!(
        !gateway.contains("type: numeric_attribute"),
        "no retention policy may select numerically on a string attribute"
    );
    for sizing in [
        "decision_wait:",
        "num_traces:",
        "expected_new_traces_per_sec:",
    ] {
        assert!(
            gateway.contains(sizing),
            "17.16 requires decision wait and trace buffers to be sized explicitly; \
             `{sizing}` is absent"
        );
    }
}

/// Both tiers report their own health.
///
/// [17.17](../../../docs/plans/rakka-agent/spec.md) asks the Collector for its
/// own internal telemetry covering refusal, queue, drop, processing, and
/// export failures. No manifest in this repository enabled `service.telemetry`
/// before this one, so the export path's health was invisible on the
/// Collector's side of the boundary as well as Rakka's.
#[test]
fn both_tiers_publish_their_own_telemetry() {
    for name in [
        "rakka-agent-otel-agent-config",
        "rakka-agent-otel-gateway-config",
    ] {
        let config = document_named("ConfigMap", name);
        assert!(
            config.contains("      telemetry:"),
            "{name} does not enable the Collector's own telemetry"
        );
        assert!(
            config.contains("port: 8888"),
            "{name} does not expose its telemetry endpoint"
        );
    }
    for (kind, name) in [
        ("DaemonSet", "rakka-agent-otel-agent"),
        ("Deployment", "rakka-agent-otel-gateway"),
    ] {
        assert!(
            document_named(kind, name).contains("containerPort: 8888"),
            "{name} does not expose the telemetry port it publishes on"
        );
    }
}

/// The backend exporter is queued and retried, and the backend is the
/// operator's.
#[test]
fn the_backend_exporter_is_bounded_and_operator_selected() {
    let gateway = document_named("ConfigMap", "rakka-agent-otel-gateway-config");
    for required in [
        "sending_queue:",
        "queue_size: 4096",
        "retry_on_failure:",
        "${env:RAKKA_AGENT_OTEL_BACKEND_OTLP_ENDPOINT}",
    ] {
        assert!(
            gateway.contains(required),
            "the backend exporter is missing `{required}`"
        );
    }
    let deployment = document_named("Deployment", "rakka-agent-otel-gateway");
    for env in [
        "RAKKA_AGENT_OTEL_BACKEND_OTLP_ENDPOINT",
        "RAKKA_AGENT_SETTINGS_REVISION_UNDER_INVESTIGATION",
    ] {
        assert!(
            deployment.contains(env),
            "the deployment does not declare `{env}`, so a config referencing it \
             would fail to start"
        );
    }
}

#[test]
fn the_readme_explains_the_contract_and_its_revalidation() {
    for required in [
        "kubectl apply --dry-run=client",
        "RAKKA_AGENT_OTEL_VALIDATE_MANIFESTS",
        "RAKKA_AGENT_OTEL_VALIDATE_COLLECTOR_CONFIG",
        COLLECTOR_IMAGE,
        "1.36.0",
        "tail_sampling",
        "loadbalancing",
        "revalidat",
    ] {
        assert!(
            COLLECTOR_README.contains(required),
            "the topology README does not mention `{required}`"
        );
    }
}

/// Optional: validate the manifests against a real `kubectl`.
#[test]
fn optional_kubectl_validation_for_the_agent_topology_is_gated() {
    if std::env::var("RAKKA_AGENT_OTEL_VALIDATE_MANIFESTS")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!(
            "skipping agent collector topology validation; set \
             RAKKA_AGENT_OTEL_VALIDATE_MANIFESTS=1 to run it"
        );
        return;
    }
    let manifest = repo_root()
        .join("docs")
        .join("plans")
        .join("rakka-agent")
        .join("kubernetes-agent-otel-collector-topology.yaml");
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

/// Optional: validate both Collector configurations against the pinned
/// distribution itself.
///
/// This is the arm that earns its keep. `kubectl` validates Kubernetes
/// objects and knows nothing about what is inside a ConfigMap, and a string
/// assertion knows only that a word appears. Running the real
/// `otel/opentelemetry-collector-contrib` binary's own `validate` against
/// these configurations found three defects that neither could:
/// `container.name` is no longer a `k8sattributes` metadata field at this
/// distribution, and the `loadbalancing` exporter refuses `routing_key:
/// traceID` for metrics — a configuration that would have failed to start in
/// production while passing every test above.
#[test]
fn optional_collector_config_validation_is_gated() {
    if std::env::var("RAKKA_AGENT_OTEL_VALIDATE_COLLECTOR_CONFIG")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!(
            "skipping Collector config validation; set \
             RAKKA_AGENT_OTEL_VALIDATE_COLLECTOR_CONFIG=1 (needs a container runtime) \
             to run it"
        );
        return;
    }
    let directory = std::env::temp_dir().join("rakka-agent-otel-collector-config");
    std::fs::create_dir_all(&directory).expect("a temporary directory for the configs");
    for name in [
        "rakka-agent-otel-agent-config",
        "rakka-agent-otel-gateway-config",
    ] {
        let config = configmap_payload(name);
        let path = directory.join(format!("{name}.yaml"));
        std::fs::write(&path, config).expect("the config writes");
        let output = Command::new("docker")
            .args(["run", "--rm", "-v"])
            .arg(format!("{}:/conf/collector.yaml:ro", path.display()))
            .args([
                "-e",
                "RAKKA_AGENT_OTEL_BACKEND_OTLP_ENDPOINT=otel-backend:4317",
                "-e",
                "RAKKA_AGENT_SETTINGS_REVISION_UNDER_INVESTIGATION=",
                COLLECTOR_IMAGE,
                "validate",
                "--config=/conf/collector.yaml",
            ])
            .output()
            .expect("a container runtime should be available when validation is enabled");
        assert!(
            output.status.success(),
            "{name} is not a valid {COLLECTOR_IMAGE} configuration:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// The `collector.yaml` payload of one ConfigMap, with its block-scalar
/// indentation removed.
fn configmap_payload(name: &str) -> String {
    let document = document_named("ConfigMap", name);
    let body = document
        .split_once("  collector.yaml: |\n")
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("{name} carries no collector.yaml"));
    body.lines()
        .map(|line| line.strip_prefix("    ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// One pipeline's block from a ConfigMap document.
fn pipeline_of(document: &'static str, signal: &str) -> String {
    // Anchored after `pipelines:` on purpose: `service.telemetry.metrics` sits
    // at the same indent and earlier in the document, so an unanchored search
    // for `metrics:` reads the telemetry block and asserts nothing about the
    // pipeline it claims to be checking.
    let pipelines = document
        .find("      pipelines:\n")
        .unwrap_or_else(|| panic!("no pipelines block"));
    let marker = format!("        {signal}:\n");
    let start = document[pipelines..]
        .find(&marker)
        .unwrap_or_else(|| panic!("no `{signal}` pipeline"))
        + pipelines
        + marker.len();
    let rest = &document[start..];
    // The next sibling pipeline is a line indented by *exactly* eight spaces.
    // Searching for `"\n        "` alone matches the pipeline's own ten-space
    // children and truncates the block to nothing — which is a test that reads
    // an empty string and asserts nothing about it.
    let end = rest
        .match_indices('\n')
        .find(|(offset, _)| {
            let line = &rest[offset + 1..];
            line.starts_with("        ") && !line.starts_with("         ")
        })
        .map_or(rest.len(), |(offset, _)| offset + 1);
    rest[..end].to_string()
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
