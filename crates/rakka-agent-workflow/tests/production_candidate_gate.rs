//! Production-candidate gate documentation coverage.

const GATE: &str =
    include_str!("../../../docs/plans/agentic-workflow/phase-7-5-production-candidate-gate.md");
const IMPLEMENTATION_PLAN: &str =
    include_str!("../../../docs/plans/agentic-workflow/agentic-workflow-implementation-plan.md");
const README: &str = include_str!("../../../README.md");

#[test]
fn production_candidate_gate_defines_release_position_and_phase_finalization() {
    for expected in [
        "Status: implemented.",
        "Release readiness is not permission to publish",
        "Production candidate:",
        "Preview candidate:",
        "Blocked candidate:",
        "## Gate Inputs",
        "## Release Checklist",
        "## Phase 7 Finalization",
    ] {
        assert!(GATE.contains(expected), "gate missing {expected}");
    }
}

#[test]
fn production_candidate_gate_names_required_validation_levels() {
    for expected in [
        "cargo fmt --check",
        "cargo test -p rakka-testkit --test repository_hygiene",
        "cargo test -p rakka-agent-workflow",
        "cargo test -p rakka-agent-workflow --all-features",
        "cargo clippy -p rakka-agent-workflow --all-targets -- -D warnings",
        "cargo run -p rakka-example-minimal-local-agent-workflow",
        "cargo test -p rakka-agent-workflow --test minimal_local_workflow",
        "cargo test -p rakka-agent-workflow --test failure_injection",
        "cargo test -p rakka-agent-workflow --test load_backpressure_cardinality",
        "cargo test -p rakka-agent-workflow --test api_compatibility",
        "cargo test -p rakka-agent-workflow --test operational_runbooks",
        "cargo test -p rakka-agent-workflow --test production_candidate_gate",
        "cargo test -p rakka-agent-workflow --features sharding --test sharded_run",
        "RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1",
        "RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres",
        "cargo test -p rakka-agent-workflow --features postgres --test postgres_query_index",
        "cargo test -p rakka-persistence-postgres -- --test-threads=1",
        "cargo test -p rakka-k8s --test agent_workflow_topology",
        "cargo test -p rakka-k8s --test agent_workflow_otel_collector_topology",
        "cargo test -p rakka-k8s --test agent_workflow_security_policy",
    ] {
        assert!(GATE.contains(expected), "gate missing {expected}");
    }
}

#[test]
fn production_candidate_gate_covers_kubernetes_otel_and_postgres_operations() {
    for expected in [
        "rakka-system",
        "RAKKA_AGENT_WORKFLOW_K8S_VALIDATE_MANIFESTS=1",
        "RAKKA_AGENT_WORKFLOW_OTEL_VALIDATE_MANIFESTS=1",
        "RAKKA_AGENT_WORKFLOW_SECURITY_VALIDATE_MANIFESTS=1",
        "OpenTelemetry Collector topology",
        "host.docker.internal",
        "0.0.0.0:5432->5432/tcp",
        "RAKKA_POSTGRES_DSN",
        "NetworkPolicy-capable CNI",
        "Helm-style values",
    ] {
        assert!(GATE.contains(expected), "gate missing {expected}");
    }
}

#[test]
fn production_candidate_gate_lists_known_limitations_and_non_goals() {
    for expected in [
        "not exactly once",
        "idempotency keys",
        "reconciliation",
        "managed database",
        "hosted dashboards",
        "provider-specific model/tool adapters",
        "fully rendered Helm charts",
        "sustained cluster soak tests",
        "Multi-region operation",
    ] {
        assert!(GATE.contains(expected), "gate missing {expected}");
    }
}

#[test]
fn readme_and_implementation_plan_expose_agent_workflow_gate() {
    assert!(
        README.contains("docs/plans/agentic-workflow/"),
        "README should point to agent workflow planning and support material"
    );
    assert!(
        README.contains("production-candidate support material"),
        "README should summarize production-candidate support material"
    );
    assert!(
        IMPLEMENTATION_PLAN
            .contains("### Slice 7.5: Production Candidate Gate\n\nStatus: implemented."),
        "implementation plan should mark Slice 7.5 implemented"
    );
    assert!(
        IMPLEMENTATION_PLAN.contains("Phase 7 status:"),
        "implementation plan should include final Phase 7 status"
    );
    assert!(
        IMPLEMENTATION_PLAN.contains("Complete. Production hardening now has"),
        "implementation plan should summarize Phase 7 completion"
    );
}
