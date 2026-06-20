# Rakka Agent Workflow Kubernetes Startup And Readiness

Status: Slice 6.2 reference artifact.

This document defines the startup order for a Rakka agent workflow pod in
Kubernetes. Readiness should fail closed until every required dependency and
compatibility check has completed.

## Readiness Principle

Kubernetes readiness means the pod may receive public workflow traffic. It
does not mean every background loop has run once, but it does mean the pod can
accept commands without relying on process-local memory for correctness.

Readiness must remain false while:

- the local Rakka node has not joined or become `Up`;
- deployment compatibility has not been accepted;
- any required startup service is missing;
- the pod has begun Kubernetes pre-stop drain.

Liveness should remain true during normal startup, shard rebalance, and drain.
It should fail only for stuck runtime conditions that require restart.

## Required Startup Services

The reference topology sets:

```text
RAKKA_REQUIRED_SERVICES=telemetry-resource,otlp-exporter,postgres,durable-state,query-index,artifact-store,actor-system,remoting,sharding,workflow-registry,operational-snapshots
```

These names map to `AgentWorkflowStartupStep` values in
`rakka-agent-workflow` when the `k8s` feature is enabled.

## Startup Order

1. Configure OpenTelemetry resource attributes.
   Include service name, service namespace, service version, deployment
   environment, Kubernetes namespace, pod name, pod UID, node name, deployment
   name, and Rakka node id.

2. Configure telemetry exporters or bridge.
   The pod should know whether OTLP export is required, optional, or local
   debug-only before accepting traffic.

3. Connect PostgreSQL.
   Validate credentials and network reachability. In local Docker Desktop
   testing, this is the `rakka-postgres` Service pointing to
   `host.docker.internal`.

4. Connect durable state stores.
   Durable inbox, outbox, run state, timers, dispatcher state, and related
   stores should be available before commands are accepted.

5. Initialize or verify the query index.
   Run index migrations or schema checks and apply the N/N+1 compatibility
   policy for existing index state.

6. Validate artifact-store configuration.
   Large prompts, completions, tool outputs, files, and audit artifacts should
   have an out-of-line storage target before workflow runs begin.

7. Initialize the actor system.
   Create the local actor system, runtime settings, metrics recorder, and
   coordinated shutdown registry.

8. Initialize internal remoting.
   Bind the remoting port and advertise the pod DNS identity derived from the
   headless service.

9. Initialize sharding.
   Register agent run entities and shard settings if the deployment uses
   sharded runtime.

10. Register workflow definitions.
    Register all workflow type/version definitions expected by the deployment.
    The deployment should reject disabled or incompatible definition versions
    before readiness succeeds.

11. Register operational snapshots.
    Register runtime, recovery, outbox, human checkpoint, and shard snapshots
    before traffic begins so operators can inspect the pod immediately.

12. Accept compatibility.
    Mark compatibility accepted only after protocol, durable state schema,
    index schema, and workflow definition compatibility checks pass.

13. Let readiness pass.
    The Kubernetes readiness probe may return success only after the node is
    `Up`, compatibility is accepted, and all required startup services have
    been registered.

## Application Wiring

Applications can drive readiness with:

```rust
use rakka_agent_workflow::{
    AgentWorkflowKubernetesStartup, AgentWorkflowStartupStep,
};
use rakka_k8s::KubernetesNodeHealth;

let health = KubernetesNodeHealth::new(local_node_id);
let mut startup = AgentWorkflowKubernetesStartup::new(health.clone());

startup.complete_step(AgentWorkflowStartupStep::TelemetryResource);
startup.complete_step(AgentWorkflowStartupStep::OtlpExporter);
startup.complete_step(AgentWorkflowStartupStep::Postgres);
startup.complete_step(AgentWorkflowStartupStep::DurableState);
startup.complete_step(AgentWorkflowStartupStep::QueryIndex);
startup.complete_step(AgentWorkflowStartupStep::ArtifactStore);
startup.complete_step(AgentWorkflowStartupStep::ActorSystem);
startup.complete_step(AgentWorkflowStartupStep::Remoting);
startup.complete_step(AgentWorkflowStartupStep::Sharding);
startup.complete_step(AgentWorkflowStartupStep::WorkflowRegistry);
startup.complete_step(AgentWorkflowStartupStep::OperationalSnapshots);
startup.accept_compatibility();
```

The lower-level `KubernetesNodeHealth` still owns cluster membership,
compatibility, drain, liveness, and probe snapshots. The agent workflow startup
helper only supplies the required service vocabulary and checklist.

## Failure Behavior

- If PostgreSQL is unreachable, do not register `postgres` or `durable-state`.
- If query migrations fail, do not register `query-index`.
- If OTLP export is required and misconfigured, do not register
  `otlp-exporter`.
- If artifact storage is required and unavailable, do not register
  `artifact-store`.
- If workflow definition compatibility fails, call
  `record_compatibility_failure` and keep readiness failed closed.
- If a pod begins drain, readiness should fail even if all startup services
  were previously registered.

## Local Validation

The manifest contract tests validate the startup service list. Local cluster
validation should additionally verify:

```sh
kubectl -n rakka-system get deploy/rakka-agent-workflow -o jsonpath='{.spec.template.spec.containers[0].readinessProbe.httpGet.path}'
kubectl -n rakka-system get deploy/rakka-agent-workflow -o jsonpath='{.spec.template.spec.containers[0].lifecycle.preStop.httpGet.path}'
```

Expected values are `/ready` and `/drain`.
