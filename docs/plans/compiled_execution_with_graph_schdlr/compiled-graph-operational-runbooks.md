# Compiled Graph Operational Runbooks

Status: implemented.

This document is the graph-runtime companion to
`docs/plans/agentic-workflow/phase-7-4-operational-runbooks-dashboards.md`.
It focuses on compiled execution plans, durable graph run state, graph
scheduler decisions, effect-node dispatch, runtime events, and the PostgreSQL
graph query projections added for the compiled graph runtime.

The broader Kubernetes, OpenTelemetry, security, and production-candidate
contracts remain in:

- `docs/plans/agentic-workflow/phase-7-4-operational-runbooks-dashboards.md`
- `docs/plans/agentic-workflow/kubernetes-drain-shutdown.md`
- `docs/plans/agentic-workflow/kubernetes-autoscaling-signals.md`
- `docs/plans/agentic-workflow/phase-7-5-production-candidate-gate.md`
- `docs/plans/compiled_execution_with_graph_schdlr/runtime-event-projection-live-stream-guidance.md`

## Incident Ownership

Use this split before debugging the wrong layer:

| Symptom | Usually product backend owned | Usually Rakka runtime owned |
| --- | --- | --- |
| Workflow cannot be edited, compiled, deployed, enabled, or authorized. | Visual editor, product DSL, compiler, release workflow, auth policy, billing, trigger registration. | Not a Rakka runtime incident unless a compiled plan already reached durable command acceptance. |
| Trigger does not start a run. | API route, webhook route, cron registration, tenant policy, product deduplication key selection. | Durable inbox acceptance, `AgentCommand` validation, recovery after accepted command. |
| Third-party credential cannot be resolved. | Secret store, credential binding ownership, provider account policy, quota. | Credential resolver contract, no secret material persistence, effect failure classification. |
| Node is waiting, failed, skipped, cancelled, or stuck after durable run start. | Product-specific adapter behavior or business policy may explain the outcome. | Graph scheduler, durable graph run state, durable outbox/timer/checkpoint state, query index, runtime events. |
| UI timeline is stale but durable graph state moved. | Product projection/live stream adapter. | `AgentRuntimeEventSink` failure, event ordering validation, event projection rebuild. |

Rakka incidents are runtime incidents only after a compiled plan or graph run
has crossed a durable boundary. Product backend incidents remain outside the
Rakka kernel when they involve editor DSL, raw visual graph data, auth,
credentials, trigger registration, or user-facing APIs.

## Operator Entry Points

Start from the same operational surfaces as the agent workflow runbook:

```sh
kubectl -n rakka-system get pods,deploy,svc
kubectl -n rakka-system port-forward svc/rakka-agent-public 8080:80
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:8080/live
curl -fsS http://localhost:8080/metrics
curl -fsS http://localhost:8080/snapshots
```

Then use graph-specific query dimensions:

```rust
AgentWorkflowRunQuery::new()
    .graph_plan_fingerprint("sha256:...")
    .graph_node_status(AgentGraphNodeStatus::Waiting)
    .graph_node_kind(AgentCompiledNodeKind::ToolCall)
    .graph_wait_reason(AgentGraphWaitReason::Effect)
    .limit(100)

AgentWorkflowRunQuery::new()
    .graph_node_kind(AgentCompiledNodeKind::ToolCall)
    .graph_error_code("tool-timeout")
    .limit(100)

AgentDispatchQuery::new()
    .graph_plan_fingerprint("sha256:...")
    .graph_node_id("effect")
    .graph_node_kind(AgentCompiledNodeKind::ToolCall)
    .stuck_at_or_before(now)
    .limit(100)
```

PostgreSQL-backed deployments should verify these projection tables exist:

- `rakka_agent_workflow_run_index`
- `rakka_agent_workflow_graph_node_index`
- `rakka_agent_workflow_dispatch_index`
- `rakka_agent_workflow_timer_index`
- `rakka_agent_workflow_checkpoint_index`
- `rakka_agent_workflow_audit_index`
- `rakka_agent_workflow_runtime_event_projection`

Runtime event timelines should resume by `(run_id, event_sequence)`. Use
`AgentRuntimeEventProjection` for a compact run-level event view, and use the
durable `AgentGraphRunState` as the correctness source when the event stream is
late or incomplete.

## First Ten Minutes

1. Decide ownership.
   If no durable graph run exists, inspect the product backend compiler,
   trigger registration, credential binding, auth, and deployment path first.
2. Find the compiled plan version.
   Use `workflow_type`, `definition_version`, and `graph_plan_fingerprint`.
   Raw `run_id`, `node_id`, `effect_id`, and `checkpoint_id` belong in traces,
   logs, audit records, snapshots, and query-index rows, not hot metric labels.
3. Find affected graph nodes.
   Query by `graph_node_status`, `graph_node_kind`, `graph_wait_reason`, and
   `graph_error_code`.
4. Separate parked nodes from broken nodes.
   Waiting for an effect, timer, human checkpoint, child workflow, or signal is
   normal until the service-level objective is exceeded.
5. Follow durable causality.
   Pivot with `correlation_id`, `causation_id`, `effect_id`, `timer_id`,
   `checkpoint_id`, and runtime `event_sequence`.

## Recovery: In-Flight Effect Re-Linking

When a run actor starts or restarts it reloads durable run state and the durable
inbox/outbox, then reconciles the two: every recovered in-flight outbox effect is
re-linked to its graph node, restoring the node's `Waiting` status and
`scheduled_effect_ids`.

Why this exists: `schedule_node_effect` commits the effect to the durable outbox
*before* the graph transition that records it is persisted, so a node is only
marked `Waiting` once its effect is durably enqueued. If a crash lands in that
window the effect is enqueued but the node link is not yet persisted. The run
actor does not re-drive in-flight nodes after recovery, so without this step the
effect's eventual `EffectCompleted`/`EffectFailed` could not resolve its node
(`UnknownEffect`) and the result would be orphaned while the node stayed stuck.

Operator implications:

- No manual action is required. After a crash a node may briefly appear in
  `Running` with a corresponding due outbox effect; once its owning actor
  recovers, the node returns to `Waiting` and the effect completes normally.
- The pass is idempotent and safe across repeated restarts: a node already
  linked to its effect, or already resolved (`Completed`, `Failed`, `Skipped`,
  `Cancelled`, `Terminal`), is left untouched.
- Scope: this covers durable outbox effects (effect nodes, including
  `ChildWorkflowCommand`). Human approval requests share the outbox but link
  through `checkpoint_ids` and are owned by the human-checkpoint path; durable
  timers use a separate store. A node stuck after a crash that does *not* return
  to `Waiting` despite a due effect is a bug worth escalating, not expected
  behavior.

## Runbook: Stuck Graph Nodes

Symptoms:

- `AgentGraphRunProjection.waiting_node_count` remains high.
- `AgentGraphRunProjection.runnable_node_count` stays positive but no node
  starts.
- `rakka.agent_workflow.runtime.mailbox_depth` or
  `rakka.agent_workflow.recovery.events` rises.
- Runtime events show repeated `NodeRunnable` without corresponding
  `NodeStarted`, or `RunWaiting` without later `RunResumed`.

Queries:

```rust
AgentWorkflowRunQuery::new()
    .graph_node_status(AgentGraphNodeStatus::Runnable)
    .limit(100)

AgentWorkflowRunQuery::new()
    .graph_node_status(AgentGraphNodeStatus::Waiting)
    .graph_wait_reason(AgentGraphWaitReason::Effect)
    .limit(100)
```

Actions:

- Check actor mailbox depth and recovery errors before changing graph logic.
- If only one plan fingerprint is affected, inspect plan validation,
  branch/skip propagation, and bounded loop settings for that compiled plan.
- If many plans are affected, inspect runtime capacity, dispatcher backlog,
  PostgreSQL latency, and shard ownership.
- If the node is waiting for dependencies, verify upstream nodes are terminal
  as completed or skipped, not still running after a crash.
- If a node sits in `Running` with a due outbox effect right after a restart, it
  should self-heal back to `Waiting`; see "Recovery: In-Flight Effect
  Re-Linking" above before intervening.

## Runbook: Failed Graph Effects

Symptoms:

- Runs fail with graph nodes of kind `ToolCall`, `ModelCall`, `HttpCall`,
  `GrpcCall`, `ProcessCall`, `StreamPublish`, `ArtifactWrite`, or
  `ChildWorkflowCommand`.
- `rakka.agent_workflow.dispatcher.fleet` shows retry, timeout, exhausted, or
  fenced outcomes.
- `rakka.agent_workflow.dispatcher.backlog`,
  `rakka.agent_workflow.dispatcher.in_flight`, or
  `rakka.agent_workflow.outbox.due_effects` remains high.

Queries:

```rust
AgentWorkflowRunQuery::new()
    .graph_node_status(AgentGraphNodeStatus::Failed)
    .graph_error_code("provider-timeout")
    .limit(100)

AgentDispatchQuery::new()
    .graph_node_kind(AgentCompiledNodeKind::ToolCall)
    .status(AgentDispatchStatus::RetryScheduled)
    .limit(100)
```

Actions:

- Determine whether the failure is an adapter/provider incident, credential
  resolver failure, product policy decision, or Rakka outbox/dispatcher
  failure.
- Reconcile external side effects by idempotency key before retrying manually.
- Treat credential binding failures as product backend/secret-store incidents
  unless Rakka persisted secret material, which should never happen.
- Confirm `AgentCredentialBindingRef` values appear only as references and
  resolved credentials do not appear in durable graph state, runtime events,
  logs, metrics, snapshots, or query indexes.

## Runbook: Late Graph Timers

Symptoms:

- Nodes of kind `TimerWait` remain waiting past the workflow SLA.
- `rakka.agent_workflow.timers.late_by_ms` increases.
- Runs queryable by `due_timer_at_or_before(now)` do not resume.

Queries:

```rust
AgentWorkflowRunQuery::new()
    .graph_node_kind(AgentCompiledNodeKind::TimerWait)
    .due_timer_at_or_before(now)
    .limit(100)

AgentTimerQuery::new()
    .status(AgentTimerStatus::Pending)
    .due_at_or_before(now)
    .limit(100)
```

Actions:

- Inspect timer scanner capacity, PostgreSQL latency, actor mailbox depth, and
  drain state.
- Avoid increasing timer batch size without bounds. Prefer adding scanner or
  worker capacity and keeping batch limits explicit.
- If the timer fired but the graph did not resume, inspect durable inbox
  acceptance for the timer-fired command and runtime event sink errors.

## Runbook: Open Graph Human Checkpoints

Symptoms:

- Nodes of kind `HumanCheckpoint` remain waiting.
- `rakka.agent_workflow.human.waiting_runs` or
  `rakka.agent_workflow.human.wait.latency_ms` exceeds the review SLA.
- The approval UI shows a decision but the graph node does not resume.

Queries:

```rust
AgentWorkflowRunQuery::new()
    .graph_node_kind(AgentCompiledNodeKind::HumanCheckpoint)
    .waiting_reason(AgentRunQueryWaitingReason::Human)
    .checkpoint_created_at_or_before(sla_cutoff)
    .limit(100)
```

Actions:

- Check product approval UI health, notification delivery, roles, escalation
  targets, and authorization.
- If the decision was accepted durably, inspect `HumanDecisionAccepted`,
  `RunResumed`, and node completion runtime events.
- If the decision was rejected before durable inbox acceptance, treat it as a
  product API/auth/checkpoint routing incident.

## Runbook: Runtime Event Sink Failures

Symptoms:

- Durable graph state changes, but the UI timeline or live stream does not
  update.
- Runtime event sink writes fail after state persistence.
- `AgentRuntimeEventProjection.last_event_sequence` lags behind
  `AgentGraphRunProjection.last_event_sequence`.

Queries and checks:

```rust
AgentRuntimeEventProjection::from_events(&events)
PostgresAgentWorkflowQueryIndex::runtime_event_projection(run_id)
```

Actions:

- Do not roll back graph state because an event sink failed. Runtime events are
  post-persistence projections.
- Rebuild product projections from durable runtime events or durable graph run
  state where available.
- Verify the product live stream resumes from `(run_id, event_sequence)` and
  treats duplicate events as idempotent.
- Alert on sustained projection lag, not on a single retryable sink failure.

## Runbook: Graph Drain Blockers

Symptoms:

- `/ready` returns not-ready with `node-draining`.
- Deployment rollout stalls during pod termination.
- `rakka.shutdown.timeouts` increments.
- Graph runs remain waiting on effects, timers, human checkpoints, child
  workflows, or signals while a pod drains.

Commands:

```sh
kubectl -n rakka-system rollout status deploy/rakka-agent-workflow
kubectl -n rakka-system describe pod -l app.kubernetes.io/name=rakka-agent-workflow
kubectl -n rakka-system logs deploy/rakka-agent-workflow
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:8080/snapshots
```

Actions:

- Confirm public ingress stopped before new graph start commands can cross the
  durable inbox boundary.
- Inspect dispatcher in-flight work, stream pressure, process-running gauges,
  and sharded run ownership.
- Allow abrupt termination to rely on durable recovery. Drain reduces
  disruption; it is not the correctness boundary.
- If graph drain blockers repeat, reduce per-pod concurrency or increase
  termination grace only after recovery tests are passing.

## Dashboard Panels

Add graph-specific panels beside the generic workflow panels:

| Panel | Signals and dimensions |
| --- | --- |
| Graph run health | `rakka.agent_workflow.run.active` by `workflow_type`, `definition_version`, `status`, and bounded graph status grouping from query projections. |
| Graph node states | Query-index counts by `graph_plan_fingerprint`, `graph_node_status`, `graph_node_kind`, `graph_wait_reason`, and `graph_error_code`. |
| Graph effect pressure | `rakka.agent_workflow.outbox.due_effects`, `rakka.agent_workflow.dispatcher.backlog`, `rakka.agent_workflow.dispatcher.in_flight`, and `rakka.agent_workflow.dispatcher.latency_ms` by `target_class`. |
| Timer waits | `rakka.agent_workflow.timers`, `rakka.agent_workflow.timers.late_by_ms`, and due graph timer query count. |
| Human waits | `rakka.agent_workflow.human.waiting_runs`, `rakka.agent_workflow.human.wait.latency_ms`, and old graph human checkpoint query count. |
| Event projection lag | Difference between graph `last_event_sequence` and runtime event projection `last_event_sequence`. |
| Runtime capacity | `rakka.agent_workflow.runtime.mailbox_depth`, `rakka.agent_workflow.recovery.events`, `rakka.agent_workflow.recovery.latency_ms`, and `rakka.agent_workflow.postgres.latency_ms`. |
| Drain and rollout | `rakka.k8s.readiness`, `rakka.k8s.compatibility`, `rakka.shutdown.timeouts`, and sampled drain blockers from `/snapshots`. |

Hot metric labels must remain bounded. Do not use raw `run_id`, `node_id`,
`effect_id`, `timer_id`, `checkpoint_id`, prompt text, credential refs, or full
error messages as metric labels. Use those values in traces, structured logs,
durable audit, query-index rows, and snapshots.

## Alert Recommendations

| Alert | Suggested signal | First action |
| --- | --- | --- |
| Stuck graph runnable nodes | Runnable graph-node query count remains positive while `NodeStarted` events do not advance. | Inspect actor mailbox depth, sharding ownership, and scheduler recovery. |
| Graph effect failures | Failed node query by `graph_error_code` spikes or dispatcher retry/exhaustion rises. | Inspect provider health, credential resolver results, idempotency keys, and audit. |
| Late graph timers | TimerWait due query count or `rakka.agent_workflow.timers.late_by_ms` exceeds SLA. | Inspect timer scanner capacity and PostgreSQL latency. |
| Human checkpoint age | Old HumanCheckpoint query count or `rakka.agent_workflow.human.wait.latency_ms` exceeds SLA. | Inspect product approval UI, roles, escalation, and notification delivery. |
| Runtime event projection lag | Runtime event projection sequence lags durable graph state sequence. | Rebuild projection and inspect event sink failures. |
| Graph drain blockers | `rakka.shutdown.timeouts` increments with graph waits or in-flight dispatches. | Inspect drain report, reduce concurrency, and confirm recovery tests. |
| PostgreSQL graph index latency | `rakka.agent_workflow.postgres.latency_ms` rises for graph query/index operations. | Inspect indexes, locks, connection pool pressure, and query plans. |

## Autoscaling Guidance

Scale Rakka runtime capacity on recoverable work pressure:

- pending durable commands: `rakka.agent_workflow.inbox.pending_commands`;
- due durable effects: `rakka.agent_workflow.outbox.due_effects`;
- dispatcher backlog and in-flight work:
  `rakka.agent_workflow.dispatcher.backlog` and
  `rakka.agent_workflow.dispatcher.in_flight`;
- dispatch latency: `rakka.agent_workflow.dispatcher.latency_ms`;
- mailbox depth: `rakka.agent_workflow.runtime.mailbox_depth`;
- recovery latency: `rakka.agent_workflow.recovery.latency_ms`;
- PostgreSQL latency: `rakka.agent_workflow.postgres.latency_ms`;
- shard ownership distribution: `rakka.agent_workflow.shard.owned`.

Do not scale Rakka pods solely because humans are slow to approve checkpoints
or because an external provider is rate limiting every request. Those are
product/support or provider-capacity incidents unless Rakka queues are also
backing up.

## Compatibility And Release Gates

Run the graph runtime gates before calling this feature production-candidate
quality:

```sh
cargo test -p rakka-agent-workflow --test compiled_plan_contract
cargo test -p rakka-agent-workflow --test graph_state_contract
cargo test -p rakka-agent-workflow --test graph_scheduler
cargo test -p rakka-agent-workflow --test effect_bridge
cargo test -p rakka-agent-workflow --test runtime_events
cargo test -p rakka-agent-workflow --test failure_injection
cargo test -p rakka-agent-workflow --test load_backpressure_cardinality
cargo test -p rakka-agent-workflow --test api_compatibility
cargo test -p rakka-agent-workflow --test operational_runbooks
cargo test -p rakka-agent-workflow --features sharding --test sharded_run
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@127.0.0.1:5432/postgres \
  cargo test -p rakka-agent-workflow --features postgres --test postgres_query_index -- --test-threads=1
```

For target deployments that use Kubernetes, OpenTelemetry, process tools,
multi-process compatibility, or PostgreSQL persistence, also run the matching
optional gates documented in
`docs/plans/agentic-workflow/phase-7-5-production-candidate-gate.md`.
