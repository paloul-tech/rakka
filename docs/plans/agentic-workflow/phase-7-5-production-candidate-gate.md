# Phase 7.5 Production Candidate Gate

Status: implemented.

This document is the final Phase 7 gate for deciding whether the Rakka agent
workflow work is ready to be treated as a production candidate, or whether it
should remain a preview. It ties together the implemented runtime slices,
examples, failure tests, compatibility checks, OpenTelemetry support,
Kubernetes reference topology, operational runbooks, known limitations, and
release checklist.

Release readiness is not permission to publish crates, images, generated
bundles, Helm charts, or release artifacts. Publishing still requires explicit
approval for that exact action.

## Release Position

The current gate supports a production-candidate decision for the agent
workflow subsystem when all required checks pass and the optional
infrastructure checks for the target deployment are either passed or explicitly
deferred in release notes.

Use this classification:

- Production candidate: local deterministic tests, feature-gated workflow
  tests, PostgreSQL-backed query index checks, multi-process compatibility,
  Kubernetes manifest validation, and OpenTelemetry Collector topology checks
  are all run successfully for the target build.
- Preview candidate: local deterministic tests pass, but PostgreSQL,
  multi-process, Kubernetes, or Collector validation is deferred.
- Blocked candidate: any required local test fails, any schema compatibility
  gate fails, or the target deployment cannot satisfy the documented
  reliability, security, telemetry, or drain contracts.

The production candidate claim is scoped to durable long-running agent
workflow foundations. It does not claim exactly-once external side effects,
managed cloud infrastructure, hosted dashboards, provider-specific model/tool
adapters, or fully rendered Helm charts.

## Gate Inputs

Primary planning and support material:

- `docs/plans/agentic-workflow/agentic-workflow-spec.md`
- `docs/plans/agentic-workflow/agentic-workflow-implementation-plan.md`
- `docs/plans/agentic-workflow/phase-0-1-api-boundary.md`
- `docs/plans/agentic-workflow/phase-1-4-minimal-local-workflow.md`
- `docs/plans/agentic-workflow/phase-7-1-failure-injection-suite.md`
- `docs/plans/agentic-workflow/phase-7-2-load-backpressure-cardinality.md`
- `docs/plans/agentic-workflow/phase-7-3-api-review-compatibility.md`
- `docs/plans/agentic-workflow/phase-7-4-operational-runbooks-dashboards.md`

Kubernetes and OpenTelemetry support material:

- `docs/plans/agentic-workflow/kubernetes-reference-topology.md`
- `docs/plans/agentic-workflow/kubernetes-reference-topology.yaml`
- `docs/plans/agentic-workflow/kubernetes-startup-readiness.md`
- `docs/plans/agentic-workflow/kubernetes-drain-shutdown.md`
- `docs/plans/agentic-workflow/kubernetes-autoscaling-signals.md`
- `docs/plans/agentic-workflow/kubernetes-otel-collector-topology.md`
- `docs/plans/agentic-workflow/kubernetes-otel-collector-topology.yaml`
- `docs/plans/agentic-workflow/kubernetes-security-policy.md`
- `docs/plans/agentic-workflow/kubernetes-security-policy.yaml`
- `docs/plans/agentic-workflow/otel-collector-local.yaml`

Runnable examples and tests:

- `examples/minimal-local-agent-workflow`
- `crates/rakka-agent-workflow/tests`
- `crates/rakka-k8s/tests/agent_workflow_topology.rs`
- `crates/rakka-k8s/tests/agent_workflow_otel_collector_topology.rs`
- `crates/rakka-k8s/tests/agent_workflow_security_policy.rs`
- `crates/rakka-testkit/tests/compatibility_matrix.rs`

## Required Local Gate

Run these checks before any optional infrastructure gate:

```sh
cargo fmt --check
cargo test -p rakka-testkit --test repository_hygiene
cargo test -p rakka-agent-workflow
cargo test -p rakka-agent-workflow --all-features
cargo clippy -p rakka-agent-workflow --all-targets -- -D warnings
cargo clippy -p rakka-agent-workflow --all-targets --all-features -- -D warnings
```

The required local gate proves that the default and all-feature agent workflow
surfaces compile, that deterministic integration tests pass, that documentation
conventions are enforced, and that feature-gated APIs remain warning-free.

## Example Acceptance Matrix

| Level | Command | Acceptance |
| --- | --- | --- |
| Local standalone example | `cargo run -p rakka-example-minimal-local-agent-workflow` | Demonstrates definition registration, durable command acceptance, recovery from durable inbox state, one deterministic step, completion, and bounded command metrics. |
| Local integration path | `cargo test -p rakka-agent-workflow --test minimal_local_workflow` | Proves the minimal workflow path as a repeatable integration test. |
| Failure recovery | `cargo test -p rakka-agent-workflow --test failure_injection` | Proves durable recovery for crash, timer, human checkpoint, effect, lease, and timeout boundaries. |
| Dispatcher and timer pressure | `cargo test -p rakka-agent-workflow --test load_backpressure_cardinality` | Proves bounded local pressure behavior and bounded hot metric labels. |
| API compatibility | `cargo test -p rakka-agent-workflow --test api_compatibility` | Proves public API exports, additive wire contracts, N/N+1 state/index compatibility, and manifest compatibility metadata. |
| Operational runbooks | `cargo test -p rakka-agent-workflow --test operational_runbooks` | Proves the runbooks name real metrics, snapshots, query APIs, log fields, audit fields, Kubernetes commands, and PostgreSQL tables. |
| Production gate docs | `cargo test -p rakka-agent-workflow --test production_candidate_gate` | Proves this gate and the root README continue to point at the implemented support material. |
| Sharded runtime | `cargo test -p rakka-agent-workflow --features sharding --test sharded_run` | Proves stable run routing and recovery after passivation for the sharded runtime path. |
| Multi-process compatibility | `RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 cargo test -p rakka-testkit --test compatibility_matrix optional_multi_process_compatibility_example_is_gated -- --nocapture` | Proves the repository multi-process compatibility example can run on the local machine. |
| PostgreSQL query index | `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-agent-workflow --features postgres --test postgres_query_index -- --test-threads=1` | Proves PostgreSQL-backed run, timer, dispatch, checkpoint, audit, migration, and stale-write query-index behavior. |
| PostgreSQL persistence | `RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres -- --test-threads=1` | Proves the shared PostgreSQL persistence layer required by durable production deployments. |
| Kubernetes topology | `cargo test -p rakka-k8s --test agent_workflow_topology` | Proves the reference topology uses the `rakka-system` namespace, local Docker PostgreSQL service wiring, startup/readiness settings, compatibility metadata, and stable Helm-style values. |
| Kubernetes Collector topology | `cargo test -p rakka-k8s --test agent_workflow_otel_collector_topology` | Proves the OpenTelemetry agent/gateway Collector topology, three-signal pipelines, resource enrichment, redaction, sampling, and backend placeholders. |
| Kubernetes security policy | `cargo test -p rakka-k8s --test agent_workflow_security_policy` | Proves the security envelope, default-deny NetworkPolicy shape, service boundaries, hardened app pod defaults, Collector permissions, and policy documentation. |

## Optional Cluster Gate

These checks require the local Docker Desktop Kubernetes context, `kubectl`,
and the `rakka-system` namespace contract from Phase 6.

Validate the agent workflow manifests against the active cluster API:

```sh
RAKKA_AGENT_WORKFLOW_K8S_VALIDATE_MANIFESTS=1 \
  cargo test -p rakka-k8s --test agent_workflow_topology \
  optional_kubectl_validation_for_agent_workflow_topology_is_gated -- --nocapture
```

Validate the OpenTelemetry Collector topology:

```sh
RAKKA_AGENT_WORKFLOW_OTEL_VALIDATE_MANIFESTS=1 \
  cargo test -p rakka-k8s --test agent_workflow_otel_collector_topology \
  optional_kubectl_validation_for_collector_topology_is_gated -- --nocapture
```

Validate the security policy topology:

```sh
RAKKA_AGENT_WORKFLOW_SECURITY_VALIDATE_MANIFESTS=1 \
  cargo test -p rakka-k8s --test agent_workflow_security_policy \
  optional_kubectl_validation_for_security_policy_is_gated -- --nocapture
```

For local Docker Desktop, the PostgreSQL reference service expects the
developer's Docker-published PostgreSQL container to expose
`0.0.0.0:5432->5432/tcp`. Kubernetes workloads reach it through the
`rakka-postgres` `ExternalName` service that targets `host.docker.internal`.
Production clusters should replace that local route with a managed database,
private endpoint, or cloud-provider connection method while preserving the
`RAKKA_POSTGRES_DSN` secret contract.

## Release Checklist

Before marking an agent workflow build as a production candidate:

1. Confirm the root README points to `docs/plans/agentic-workflow/`.
2. Confirm every implemented slice in
   `docs/plans/agentic-workflow/agentic-workflow-implementation-plan.md`
   contains `Status: implemented.`.
3. Run the required local gate.
4. Run the local standalone example and record its output.
5. Run the sharded runtime test when the deployment uses sharding.
6. Run the PostgreSQL query-index and persistence checks for any
   PostgreSQL-backed deployment.
7. Run the multi-process compatibility check when local process launch and
   loopback networking are available.
8. Run the Kubernetes manifest tests and, for cluster-backed release notes, the
   optional `kubectl` dry-run gates.
9. Confirm the OpenTelemetry Collector topology matches the target telemetry
   backend, sampling policy, redaction policy, and resource attributes.
10. Confirm operational dashboards and alerts have equivalents for the target
    monitoring stack, even if they are not checked in as backend-specific JSON.
11. Confirm NetworkPolicy enforcement is supported by the target CNI before
    treating `kubernetes-security-policy.yaml` as enforced network isolation.
12. Confirm release notes classify any deferred optional gate as a preview
    limitation.
13. Confirm no crates, images, charts, or artifacts are published without
    explicit approval for that exact action.

## Known Limitations And Non-Goals

- External model calls, tool calls, webhooks, and provider callbacks are not exactly once.
  Durable outbox scheduling provides recoverable intent; target systems still
  need idempotency keys, reconciliation, and compensation policy.
- The Kubernetes topology is a reference contract, not a production Helm chart.
  Helm-style values are documented, but templates are future work.
- The local PostgreSQL route through `host.docker.internal` is for Docker
  Desktop development only. Production needs a managed database or secure
  private service endpoint, tested backup/restore, pool sizing, migrations,
  index maintenance, and vacuum policy.
- The OpenTelemetry Collector topology defines safe pipelines and local
  validation. Backend-specific dashboards, SLOs, retention policies, storage
  costs, and alert routing remain deployment-owned.
- NetworkPolicy manifests require a NetworkPolicy-capable CNI. If the cluster
  ignores NetworkPolicy, the policy file is documentation rather than
  enforcement.
- Artifact references and redaction policies are modeled, but production
  object-store lifecycle, encryption, malware scanning, and legal retention
  controls remain application and platform responsibilities.
- Model and tool adapter traits define the boundary, but provider-specific model/tool adapters
  for credentials, quotas, sandboxing, and prompt/content policy are outside this
  candidate gate.
- Load tests are deterministic local pressure tests. They do not replace
  sustained cluster soak tests, provider quota tests, PostgreSQL contention
  tests, Collector queue/memory-limiter tests, or chaos rehearsals.
- Multi-region operation, disaster recovery, and cross-cluster active-active
  workflow execution are not part of this gate.

## Phase 7 Finalization

Phase 7 is complete when this document, the runbook document, the API
compatibility document, the load/cardinality document, and the failure-injection
document are all present and covered by tests. The next implementation phase
should either promote the candidate into product documentation and release
notes, or create the deployment-specific Helm, dashboard, and production
adapter work needed for a real cluster rollout.
