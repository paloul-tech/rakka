# Phase 7.4 Operational Runbooks and Dashboards

Status: implemented.

This document gives operators a backend-neutral guide for diagnosing durable
agent workflow incidents. It assumes the Kubernetes reference topology in the
`rakka-system` namespace, OpenTelemetry export through the Collector topology,
Prometheus-compatible metrics for local inspection, JSON operational snapshots,
durable audit records, and the workflow query index.

## Operator Entry Points

Use these entry points before jumping into application-specific code:

```sh
kubectl -n rakka-system get pods,deploy,svc
kubectl -n rakka-system port-forward svc/rakka-agent-public 8080:80
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:8080/live
curl -fsS http://localhost:8080/metrics
curl -fsS http://localhost:8080/snapshots
```

Primary diagnostic surfaces:

- Metrics: OTLP metrics and Prometheus-compatible `/metrics`.
- Traces: OpenTelemetry trace ids, span ids, span links, causation ids, and
  correlation ids.
- Structured logs: OpenTelemetry-shaped log events with workflow/run/effect
  attributes.
- Durable audit: queryable audit records that survive telemetry backend loss.
- Snapshots: bounded JSON views under `/snapshots`.
- Query index: bounded run, timer, and dispatch lookups backed by in-memory or
  PostgreSQL implementations.
- Kubernetes probes: readiness, liveness, drain, compatibility, and startup
  service state.

## First Ten Minutes

1. Check Kubernetes readiness and drain state.
   Use `/ready`, pod events, and deployment rollout status. If readiness says
   `compatibility-not-accepted`, `node-draining`, or `missing-service:*`, treat
   it as a startup or deployment problem before debugging workflow logic.
2. Find the workflow group.
   Start from `workflow_type`, `definition_version`, tenant tier, target class,
   and status metrics. Do not put raw run ids into hot metric labels.
3. Find specific affected runs.
   Use the query index for waiting, failed, due-timer, and stuck-dispatcher
   lookups. Then use run ids in traces, logs, audit, and snapshots.
4. Follow correlation.
   Pivot on `correlation_id` and `causation_id` across structured logs, audit
   records, and trace span links.
5. Separate durable state from external uncertainty.
   Durable inbox acceptance and durable outbox scheduling are recoverable.
   External side effects are not exactly once and may need idempotency keys or
   reconciliation.

## Runbook: Waiting Runs

Symptoms:

- `rakka.agent_workflow.run.active` remains high.
- `rakka.agent_workflow.inbox.pending_commands` grows.
- `agent_workflow_runtime` shows sampled runs in `waiting-for-timer`,
  `waiting-for-human`, or `waiting-for-effect`.

Query index:

```rust
AgentWorkflowRunQuery::new().waiting().limit(100)
AgentWorkflowRunQuery::new()
    .waiting_reason(AgentRunQueryWaitingReason::Human)
    .limit(100)
```

Snapshot fields:

- `agent_workflow_runtime.status_counts`
- `agent_workflow_runtime.sampled_runs[].status`
- `agent_workflow_runtime.sampled_runs[].pending_command_count`
- `agent_workflow_runtime.sampled_runs[].due_effect_count`

Trace, log, and audit pivots:

- `trace_id`, `span_id`, `span_links`
- `workflow_id`, `run_id`, `step_id`
- `correlation_id`, `causation_id`
- audit kinds `checkpoint-created`, `human-decision-made`,
  `tool-requested`, `tool-response-received`

Actions:

- If the run waits for a timer, inspect overdue timers.
- If the run waits for a human, inspect checkpoint age and escalation.
- If the run waits for an effect, inspect dispatcher backlog and failed
  effects.
- If pending commands grow without transitions, inspect actor mailbox depth,
  recovery errors, and PostgreSQL latency.

## Runbook: Stuck Dispatchers

Symptoms:

- `rakka.agent_workflow.dispatcher.backlog` stays above zero.
- `rakka.agent_workflow.dispatcher.in_flight` stays high or flat.
- `rakka.agent_workflow.outbox.due_effects` grows.
- Dispatch spans show old leases or repeated provider timeouts.

Query index:

```rust
AgentWorkflowRunQuery::new().stuck_dispatcher_at_or_before(now).limit(100)
AgentDispatchQuery::new()
    .status(AgentDispatchStatus::Claimed)
    .stuck_at_or_before(now)
    .limit(100)
```

Snapshot fields:

- dispatcher fleet snapshot `due_dispatch_count`
- dispatcher fleet snapshot `in_flight_count`
- dispatcher fleet snapshot `expired_lease_count`
- dispatcher fleet snapshot `sampled_entries[].worker_id`
- `agent_workflow_outbox.due_effect_count`

Trace, log, and audit pivots:

- `effect_id`, `run_id`, `target_class`
- `idempotency_key` in audit or provider logs, not hot metrics
- `correlation_id` shared by the command, effect, and callback

Actions:

- Check provider quota, network policy, tool/process availability, and
  dispatcher target concurrency limits.
- If leases expire and recover, confirm another worker can reclaim. This is
  expected after crashes.
- If the same target saturates, lower per-target concurrency and scale worker
  replicas.
- If outbox entries are dispatching but no provider result is durable, assume
  the external side effect may have happened and reconcile by idempotency key.

## Runbook: Overdue Timers

Symptoms:

- `rakka.agent_workflow.timers.late_by_ms` exceeds the workflow SLA.
- Runs stay in `waiting-for-timer`.
- Timer scans report `backpressure_limited` in test or diagnostic output.

Query index:

```rust
AgentTimerQuery::new()
    .status(AgentTimerStatus::Pending)
    .due_at_or_before(now)
    .limit(100)
AgentWorkflowRunQuery::new().due_timer_at_or_before(now).limit(100)
```

Snapshot fields:

- `agent_workflow_runtime.sampled_runs[].status`
- `agent_workflow_runtime.sampled_runs[].updated_at_millis`
- `agent_workflow_recovery.recovery_error_count`

Actions:

- Compare timer backlog with PostgreSQL latency and actor mailbox depth.
- Increase timer scanner replicas or reduce batch interval before increasing
  batch size without bounds.
- Check for pods stuck in drain or failed readiness, which can pause scanner
  progress.

## Runbook: Failed Effects

Symptoms:

- `rakka.agent_workflow.outbox.effects` records failed or duplicate outcomes.
- `rakka.agent_workflow.dispatcher.fleet` records failed, timeout, retry, or
  fenced dispatch details.
- Runs are `waiting-for-effect`, `failed`, or stuck on a tool/model step.

Query index:

```rust
AgentWorkflowRunQuery::new()
    .status(AgentRunStatus::Failed)
    .failed_step_id("step-tool")
    .limit(100)
AgentDispatchQuery::new()
    .status(AgentDispatchStatus::RetryScheduled)
    .limit(100)
```

Trace, log, and audit pivots:

- `effect_id`, `command_id`, `run_id`, `step_id`
- `error_code` and bounded `detail`
- audit kinds `model-requested`, `model-response-received`,
  `tool-requested`, `tool-response-received`, `run-failed`

Actions:

- Determine whether the failure is retryable, exhausted, or fenced.
- Confirm idempotency keys were sent to downstream systems.
- If the provider result is ambiguous, run reconciliation before manually
  resuming or retrying the workflow.
- For process tools, inspect process exit metrics and restart-budget logs.

## Runbook: Duplicate Callbacks

Symptoms:

- `rakka.agent_workflow.inbox.commands` records `outcome=duplicate`.
- Audit shows repeated provider callbacks or human decisions with the same
  idempotency or deduplication key.
- The run does not transition twice, but external systems may have retried.

Expected behavior:

- Duplicate durable commands should deduplicate by message id or
  deduplication key.
- Duplicate timer and human decision delivery should not resume the run twice.
- External providers can retry callbacks; use idempotency keys and audit to
  reconcile.

Trace, log, and audit pivots:

- `command_id`, `effect_id`, `checkpoint_id`
- `correlation_id`, `causation_id`
- `audit_event_id`, `audit_kind`

Actions:

- Treat a low duplicate rate as normal distributed-system behavior.
- Alert only on sustained duplicate spikes, especially after provider or
  network incidents.
- Check whether callbacks use stable idempotency keys and map to the expected
  effect id.

## Runbook: Human Checkpoint Age

Symptoms:

- `rakka.agent_workflow.human.waiting_runs` remains high.
- `rakka.agent_workflow.human.wait.latency_ms` exceeds the review SLA.
- `agent_workflow_human_checkpoints` shows old open or escalated checkpoints.

Query index:

```rust
AgentWorkflowRunQuery::new()
    .waiting_reason(AgentRunQueryWaitingReason::Human)
    .checkpoint_created_at_or_before(sla_cutoff)
    .limit(100)
```

Snapshot fields:

- `agent_workflow_human_checkpoints.waiting_run_count`
- `agent_workflow_human_checkpoints.open_checkpoint_count`
- `agent_workflow_human_checkpoints.escalated_checkpoint_count`
- `agent_workflow_human_checkpoints.due_checkpoint_count`

Actions:

- Check approval UI/service health and notification targets.
- Verify required roles and escalation target are valid.
- If a reviewer action was accepted but the run did not resume, inspect the
  durable inbox command and human decision audit event.

## Runbook: Drain Blockers

Symptoms:

- `/ready` fails with `node-draining`.
- Deployment rollout stalls with old pods terminating.
- `rakka.shutdown.timeouts` increments.
- `rakka.k8s.readiness` reports `outcome=not-ready`.

Commands:

```sh
kubectl -n rakka-system rollout status deploy/rakka-agent-workflow
kubectl -n rakka-system describe pod -l app.kubernetes.io/name=rakka-agent-workflow
kubectl -n rakka-system logs deploy/rakka-agent-workflow
curl -fsS http://localhost:8080/ready
curl -fsS http://localhost:8080/snapshots
```

Actions:

- Confirm ingress has stopped before pod termination.
- Inspect dispatcher in-flight work and outbox backlog.
- Inspect stream pressure and process-running gauges.
- Confirm telemetry flush and snapshot routes complete within
  `RAKKA_K8S_PRESTOP_TIMEOUT_MS`.
- If drain repeatedly times out, lower per-pod concurrency or increase
  termination grace after proving durable recovery works.

## Dashboard Catalog

Workflow overview:

| Panel | Signals |
| --- | --- |
| Active runs | `rakka.agent_workflow.run.active` by `workflow_type`, `definition_version`, `status`, `tenant_tier` |
| Run and step transitions | `rakka.agent_workflow.run.transitions` and `rakka.agent_workflow.step.transitions` by `transition`, `status`, `workflow_type`, `step_kind` |
| Pending inbox | `rakka.agent_workflow.inbox.pending_commands` and `rakka.agent_workflow.inbox.commands` by `outcome`, `detail`, `command_type` |
| Recovery | `rakka.agent_workflow.recovery.events`, `rakka.agent_workflow.recovery.latency_ms`, and `agent_workflow_recovery` |

Dispatch and effects:

| Panel | Signals |
| --- | --- |
| Due outbox effects | `rakka.agent_workflow.outbox.due_effects` by `target_class` |
| Outbox scheduling | `rakka.agent_workflow.outbox.effects` by `outcome`, `effect_kind`, `detail` |
| Dispatcher backlog | `rakka.agent_workflow.dispatcher.backlog` and `rakka.agent_workflow.dispatcher.in_flight` |
| Dispatch latency | `rakka.agent_workflow.dispatcher.latency_ms` by `target_class`, `outcome` |
| Adapter health | `rakka.agent_workflow.model_adapter.calls`, `rakka.agent_workflow.model_adapter.latency_ms`, `rakka.agent_workflow.tool_adapter.calls`, `rakka.agent_workflow.tool_adapter.latency_ms` |

Timers and human checkpoints:

| Panel | Signals |
| --- | --- |
| Timer lateness | `rakka.agent_workflow.timers.late_by_ms` and due timer query count |
| Timer delivery | `rakka.agent_workflow.timers` by `timer_status`, `outcome`, `detail` |
| Human waiting runs | `rakka.agent_workflow.human.waiting_runs` by checkpoint status |
| Human wait latency | `rakka.agent_workflow.human.wait.latency_ms` by `checkpoint_status`, `tenant_tier` |

Runtime and Kubernetes:

| Panel | Signals |
| --- | --- |
| Mailbox and stream pressure | `rakka.agent_workflow.runtime.mailbox_depth`, `rakka.agent_workflow.stream.pressure` |
| Process tools | `rakka.agent_workflow.process.running`, `rakka.process.exits` |
| PostgreSQL | `rakka.agent_workflow.postgres.latency_ms`, `rakka.persistence.operation.latency_ms` |
| Shards | `rakka.agent_workflow.shard.owned`, `rakka.sharding.shards_owned`, `agent_workflow_shards` |
| Kubernetes health | `rakka.k8s.readiness`, `rakka.k8s.compatibility`, `rakka.shutdown.timeouts` |

## Field Catalog

Metric labels must remain bounded. Use these labels for dashboards and
autoscaling. Use high-cardinality ids only in traces, logs, audit, and
snapshots.

Metric label fields:

- `workflow_type`
- `definition_version`
- `status`
- `step_kind`
- `transition`
- `effect_kind`
- `target_class`
- `timer_status`
- `checkpoint_status`
- `adapter_kind`
- `artifact_kind`
- `retry_attempt_bucket`
- `outcome`
- `detail`
- `error_code`
- `tenant_tier`
- `redaction`
- `component`
- `queue`
- `direction`
- `database_operation`
- `entity_type`
- `signal`

Trace fields:

- `trace_id`
- `span_id`
- `traceparent`
- `tracestate`
- `span_links`
- `workflow_id`
- `run_id`
- `step_id`
- `effect_id`
- `checkpoint_id`
- `command_id`
- `correlation_id`
- `causation_id`

Structured log fields:

- `workflow_id`
- `workflow_type`
- `definition_version`
- `run_id`
- `tenant_id`
- `step_id`
- `effect_id`
- `checkpoint_id`
- `command_id`
- `audit_event_id`
- `causation_id`
- `correlation_id`
- `redaction`
- `audit_kind`

Durable audit fields:

- `audit_event_id`
- `audit_kind`
- `workflow_id`
- `run_id`
- `definition_version`
- `tenant_id`
- `step_id`
- `effect_id`
- `checkpoint_id`
- `command_id`
- `correlation_id`
- `causation_id`
- `actor_principal`
- `artifact_refs`
- `content_hashes`
- `redaction`
- `occurred_at`

Snapshot names:

- `agent_workflow_runtime`
- `agent_workflow_outbox`
- `agent_workflow_recovery`
- `agent_workflow_human_checkpoints`
- `agent_workflow_shards`

PostgreSQL query-index tables:

- `rakka_agent_workflow_run_index`
- `rakka_agent_workflow_timer_index`
- `rakka_agent_workflow_dispatch_index`
- `rakka_agent_workflow_checkpoint_index`
- `rakka_agent_workflow_audit_index`

## Alert Recommendations

These recommendations intentionally avoid backend-specific syntax. Translate
them into Prometheus, Grafana, Datadog, Honeycomb, New Relic, or another
backend using the same metric and field names.

| Alert | Signal | Suggested trigger | First action |
| --- | --- | --- | --- |
| Readiness failed | `rakka.k8s.readiness` | Not ready for 2 probe windows | Check compatibility, startup services, and drain state. |
| Compatibility rejected | `rakka.k8s.compatibility` | Rejected during rollout | Stop rollout and inspect state/index schema versions. |
| Dispatcher backlog stuck | `rakka.agent_workflow.dispatcher.backlog` | Sustained positive backlog with flat in-flight work | Query stuck dispatches and inspect provider/tool health. |
| Timer lateness | `rakka.agent_workflow.timers.late_by_ms` | p95 exceeds workflow SLA | Query due timers and scale scanner/worker capacity. |
| Human checkpoint overdue | `rakka.agent_workflow.human.wait.latency_ms` | p95 exceeds review SLA | Query old checkpoints and inspect approval UI/notifications. |
| Failed effect spike | `rakka.agent_workflow.dispatcher.fleet` | Error or timeout outcomes spike | Inspect effect ids in logs/audit and reconcile providers. |
| Duplicate callback spike | `rakka.agent_workflow.inbox.commands` | Duplicate outcome rate rises abruptly | Check provider retries, idempotency keys, and recent network incidents. |
| PostgreSQL latency | `rakka.agent_workflow.postgres.latency_ms` | p95 exceeds storage SLA | Inspect connection pool, locks, indexes, and database saturation. |
| Drain timeout | `rakka.shutdown.timeouts` | Any timeout during rollout | Inspect in-flight dispatch, streams, process tools, and telemetry flush. |
| Mailbox saturation | `rakka.agent_workflow.runtime.mailbox_depth` | Sustained near-capacity depth | Scale replicas or reduce per-run/dispatcher concurrency. |

## Escalation Checklist

Escalate to the workflow owner when:

- durable state is current, but the application step cannot decide whether to
  retry, compensate, or cancel;
- external side-effect outcome is ambiguous and requires business
  reconciliation;
- human approval policy has no valid reviewer or escalation target;
- schema compatibility rejects a release and no migration/backfill plan exists.

Escalate to the platform owner when:

- pods cannot become ready because required services are missing;
- NetworkPolicy, DNS, service account, or secret configuration blocks required
  dependencies;
- PostgreSQL latency or lock contention affects multiple workflow types;
- OTLP Collector memory-limiter or queue pressure drops telemetry signals.
