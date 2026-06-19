# Rakka Agent Workflow Kubernetes Drain And Shutdown

Status: Slice 6.3 reference artifact.

This document defines the near-production shutdown behavior for Rakka agent
workflow pods running in Kubernetes. Drain is intentionally conservative:
readiness fails first, public workflow ingress closes next, and accepted work
continues to rely on durable inbox, outbox, timer, and run-state recovery.

## Drain Principle

Kubernetes drain is not a correctness boundary. It is an availability and
handoff boundary. Correctness comes from accepting commands only after durable
persistence and from replaying accepted commands, scheduled effects, timers,
and run state after interruption.

The pod should treat drain as started when any of these happens:

- Kubernetes calls the configured pre-stop drain endpoint.
- A local operator calls the same drain endpoint.
- Coordinated shutdown begins with the Kubernetes pre-stop reason.
- The application explicitly marks `KubernetesNodeHealth` draining.

Once drain begins:

- readiness must fail with `node-draining`;
- new public workflow commands must be rejected before durable inbox
  acceptance;
- already accepted commands must remain recoverable;
- scheduled durable outbox effects must remain recoverable;
- telemetry buffers should be flushed before process exit;
- abrupt termination may leave work incomplete, but not lost.

## Coordinated Shutdown Order

Agent workflow deployments should register workflow-specific hooks into Rakka's
coordinated shutdown registry and run that registry from the Kubernetes
pre-stop drain endpoint.

The intended order is:

1. Stop ingress.
   Begin Kubernetes drain on the shared health model and reject new public
   workflow commands through `AgentWorkflowIngressGate`.

2. Drain adapters.
   Drain stream sources, stream sinks, model adapters, tool adapters, and
   process actors that have graceful stop support.

3. Hand off shards.
   Mark the local sharding runtime leaving and allow other nodes to claim or
   recover shard ownership.

4. Flush persistence.
   Flush durable state, pending database writes, audit sinks, and telemetry
   exporters. The agent workflow telemetry flush hook is registered here.

5. Stop actors and remoting.
   Stop local process actors, workflow actors, runtime actors, and remoting
   after public ingress is closed and durable buffers have had a chance to
   flush.

6. Exit.
   Let Kubernetes complete termination. If the pod is killed before all hooks
   finish, durable recovery remains the source of truth.

## Rakka API Surface

When the `rakka-agent-workflow` crate is built with the `k8s` feature, Slice
6.3 adds:

- `AgentWorkflowIngressGate`, a small public ingress guard backed by
  `KubernetesNodeHealth`.
- `AgentWorkflowDrainError`, which distinguishes drain rejection from durable
  inbox acceptance failures.
- `register_agent_workflow_ingress_stop_task`, which registers the standard
  stop-ingress shutdown task in the `stop-ingress` phase.
- `register_agent_workflow_telemetry_flush_task`, which registers a
  best-effort telemetry flush task in the `flush-persistence` phase.
- stable task and operation constants for drain reports, tests, and future
  OpenTelemetry attributes.

Applications should place `AgentWorkflowIngressGate` at public HTTP/gRPC
workflow command boundaries. Internal recovery, replay, and dispatcher paths
should not use this public ingress gate because they need to continue moving
already accepted durable work.

## Drain Reports

The lower-level `rakka-k8s` drain controller maps coordinated shutdown reports
into `KubernetesDrainReport`. Agent workflow hooks use stable task names so
operators can identify workflow-specific blockers:

- `agent-workflow-stop-ingress`;
- `agent-workflow-flush-telemetry`.

Future slices can add additional named workflow blockers for adapter drain,
shard handoff, durable outbox flush, human checkpoint persistence, and
artifact-store finalization without changing the report shape.

## Application Wiring

Minimal wiring looks like:

```rust
use rakka_agent_workflow::{
    register_agent_workflow_ingress_stop_task,
    register_agent_workflow_telemetry_flush_task,
    AgentWorkflowIngressGate,
};
use rakka_core::CoordinatedShutdown;
use rakka_k8s::{KubernetesDrainController, KubernetesNodeHealth};
use std::time::Duration;

let health = KubernetesNodeHealth::new(local_node_id);
let gate = AgentWorkflowIngressGate::new(health.clone());
let shutdown = CoordinatedShutdown::new();

register_agent_workflow_ingress_stop_task(&shutdown, gate.clone())?;
register_agent_workflow_telemetry_flush_task(&shutdown, || async {
    // Flush OTLP SDK, bridge receiver, audit sink, or local telemetry buffer.
    Ok(())
})?;

// Public command handlers call this before durable inbox acceptance.
gate.ensure_accepting()?;

// The /drain endpoint runs coordinated shutdown through the k8s drain adapter.
let drain = KubernetesDrainController::from_coordinated_shutdown(
    health.clone(),
    shutdown,
);
let report = drain.drain(Duration::from_secs(40)).await;
```

Production applications should add stream, process, sharding, persistence, and
adapter tasks around these hooks using the existing Rakka coordinated shutdown
and `rakka-k8s` drain helpers.

## Kubernetes Contract

The reference topology already configures:

- `terminationGracePeriodSeconds: 45`;
- readiness probe `GET /ready`;
- liveness probe `GET /live`;
- pre-stop hook `GET /drain`.

The drain endpoint should use a timeout slightly lower than the termination
grace period so the process can return a final report and exit cleanly. For the
current manifest, a 35-40 second drain deadline leaves room for HTTP response
and process shutdown overhead.

Liveness should remain true during drain. Readiness should fail as soon as
drain begins so Kubernetes stops routing new public traffic to the pod.

## Local Validation

The Slice 6.3 test coverage verifies:

- the workflow stop-ingress task is registered in the coordinated shutdown
  `stop-ingress` phase;
- the workflow telemetry flush task is registered in the `flush-persistence`
  phase;
- pre-drain public commands can cross the durable inbox boundary;
- drain flips readiness false with `node-draining`;
- post-drain public commands are rejected by the ingress gate;
- a command accepted before drain remains recoverable after interruption.

Run the focused tests with:

```sh
cargo test -p rakka-agent-workflow --features k8s --test kubernetes_drain
```

## Open Items

- Add explicit drain hooks for durable outbox effect flush once the runtime
  exposes a concrete queue-drain operation.
- Add named model/tool adapter drain hooks after adapter runtimes become
  process-backed instead of trait-only boundaries.
- Add drain report fields or task names for shard handoff pressure when the
  sharded runtime exposes pending handoff counts.
- Add Helm values for drain deadline, termination grace period, and failure
  policy defaults.
