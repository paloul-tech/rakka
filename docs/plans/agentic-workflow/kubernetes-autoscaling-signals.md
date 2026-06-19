# Rakka Agent Workflow Kubernetes Autoscaling Signals

Status: Slice 6.4 reference artifact.

This document defines the first autoscaling metric contract for Rakka agent
workflow deployments on Kubernetes. It is intentionally autoscaler-neutral:
the same signal set can feed HorizontalPodAutoscaler custom metrics, KEDA
ScaledObjects, Prometheus Adapter rules, or a future Rakka operator.

## Scaling Principle

CPU and memory are lagging indicators for long-running agent workflows. Scale
decisions should primarily look at durable work pressure and dispatch latency:

- active non-terminal runs;
- recoverable durable inbox commands;
- due durable outbox effects;
- dispatcher backlog and in-flight work;
- dispatch latency;
- human checkpoint wait pressure;
- actor mailbox depth;
- stream pressure;
- child process capacity;
- PostgreSQL latency;
- shard ownership distribution.

All hot metric labels must stay bounded. Run ids, workflow ids, command ids,
effect ids, shard ids, pod names, pod UIDs, node ids, prompts, completions, and
full error messages belong in traces, logs, audit records, snapshots, or
OpenTelemetry resource attributes, not autoscaling labels.

## Exposition

The reference topology keeps metrics on the application HTTP endpoint:

```text
GET /metrics
```

The same process may also export OTLP metrics through the OpenTelemetry bridge
or SDK path. Autoscalers should use the backend already deployed in the
cluster, typically Prometheus plus Prometheus Adapter for HPA, or Prometheus
queries from KEDA.

The reference manifest enables autoscaling metric emission with:

```text
RAKKA_AUTOSCALING_METRICS_ENABLED=true
RAKKA_AUTOSCALING_METRICS_INTERVAL_MS=10000
```

Applications should record gauges periodically, not on every command. A
10-second interval is a reasonable starting point for local and near-production
testing.

## Canonical Signals

| Signal | Kind | Unit | Primary Use | Bounded Labels |
| --- | --- | --- | --- | --- |
| `rakka.agent_workflow.run.active` | gauge | `{run}` | Scale on active non-terminal runs. | `workflow_type`, `definition_version`, `status`, `tenant_tier` |
| `rakka.agent_workflow.inbox.pending_commands` | gauge | `{command}` | Scale on durable commands waiting for run actors. | `queue`, `workflow_type`, `definition_version`, `tenant_tier` |
| `rakka.agent_workflow.outbox.due_effects` | gauge | `{effect}` | Scale on due external work. | `queue`, `workflow_type`, `definition_version`, `target_class`, `tenant_tier` |
| `rakka.agent_workflow.dispatcher.backlog` | gauge | `{effect}` | Scale dispatcher fleets by target class. | `target_class`, `outcome`, `detail` |
| `rakka.agent_workflow.dispatcher.in_flight` | gauge | `{effect}` | Detect saturated dispatcher capacity. | `target_class`, `outcome`, `detail` |
| `rakka.agent_workflow.dispatcher.latency_ms` | histogram | `ms` | Scale when due effects wait too long. | `target_class`, `outcome`, `detail` |
| `rakka.agent_workflow.human.waiting_runs` | gauge | `{run}` | Distinguish human wait pressure from compute pressure. | `workflow_type`, `definition_version`, `checkpoint_status`, `tenant_tier` |
| `rakka.agent_workflow.human.wait.latency_ms` | histogram | `ms` | Alert or scale supporting services when approvals lag. | `workflow_type`, `definition_version`, `checkpoint_status`, `tenant_tier` |
| `rakka.agent_workflow.runtime.mailbox_depth` | gauge | `{message}` | Scale on actor mailbox saturation. | `component`, `workflow_type`, `definition_version` |
| `rakka.agent_workflow.stream.pressure` | gauge | `1` | Scale on bounded stream pressure. | `component`, `direction`, `target_class` |
| `rakka.agent_workflow.process.running` | gauge | `{process}` | Track process adapter capacity. | `component`, `status`, `target_class` |
| `rakka.agent_workflow.postgres.latency_ms` | histogram | `ms` | Avoid hiding database saturation behind pod scaling. | `database_operation`, `outcome`, `detail` |
| `rakka.agent_workflow.shard.owned` | gauge | `{shard}` | Detect shard imbalance across replicas. | `entity_type`, `status`, `component` |

The Rust catalog is exposed as `AGENT_WORKFLOW_AUTOSCALING_SIGNALS` in
`rakka-agent-workflow`. Applications and tests should use the exported metric
constants rather than duplicating strings.

## HPA-Style Usage

For Kubernetes HPA custom metrics, start with aggregate backlog signals:

```promql
sum(rakka_agent_workflow_inbox_pending_commands)
sum(rakka_agent_workflow_outbox_due_effects)
sum(rakka_agent_workflow_dispatcher_backlog)
```

Then add latency once the workload has enough traffic for stable averages:

```promql
sum(rate(rakka_agent_workflow_dispatcher_latency_ms_sum[5m]))
  /
sum(rate(rakka_agent_workflow_dispatcher_latency_ms_count[5m]))
```

Suggested initial behavior:

- scale out when pending inbox commands stay above a per-replica target for
  several minutes;
- scale out when due outbox effects or dispatcher backlog grow faster than
  dispatch completion;
- scale out when sustained dispatch latency exceeds the workflow SLO;
- do not scale out only because human waiting runs are high, unless the scaled
  component actually reduces human wait time;
- do not scale out indefinitely when PostgreSQL latency is the dominant signal.

## KEDA-Style Usage

KEDA can use the same Prometheus queries. A typical trigger should target one
signal per ScaledObject and let the deployment behavior combine them:

```yaml
triggers:
  - type: prometheus
    metadata:
      serverAddress: http://prometheus.monitoring.svc.cluster.local:9090
      metricName: rakka_agent_workflow_outbox_due_effects
      threshold: "100"
      query: sum(rakka_agent_workflow_outbox_due_effects)
```

Use separate triggers for inbox backlog and due outbox effects so operators can
see which queue caused scale-out.

## Recording Guidance

Applications should update autoscaling gauges from bounded operational
snapshots or query-index summaries. Good sources are:

- runtime snapshots for active runs, pending inbox commands, due effects, and
  mailbox depth;
- dispatcher fleet state for backlog, in-flight work, and dispatch latency;
- human checkpoint runtime/index state for waiting runs and wait latency;
- stream/process adapters for pressure and process capacity;
- PostgreSQL query/persistence wrappers for operation latency;
- sharding snapshots for local owned shard count.

Recommended labels:

- use `workflow_type`, `definition_version`, and `tenant_tier` when those
  values are intentionally bounded;
- use `target_class` for model, tool, process, stream, timer, and human
  dispatch lanes;
- use `component` for runtime subsystem names such as `run-actor`,
  `dispatcher`, `tool-output`, `sandbox`, or `agent-run-sharding`;
- use OpenTelemetry resource attributes for pod, namespace, deployment, node,
  and Rakka node identifiers.

## Local Validation

Run the metric tests:

```sh
cargo test -p rakka-agent-workflow --test workflow_metrics
```

After an application image records the signals, scrape `/metrics` and confirm
the Prometheus names:

```sh
kubectl -n rakka-system port-forward deploy/rakka-agent-workflow 8080:8080
curl -s http://localhost:8080/metrics | grep rakka_agent_workflow_outbox_due_effects
curl -s http://localhost:8080/metrics | grep rakka_agent_workflow_dispatcher_latency_ms
```

## Open Items

- Add example HPA and KEDA manifests once a metrics backend is selected for the
  local Kubernetes environment.
- Add runtime periodic emission once the production application wiring owns a
  concrete metrics loop.
- Add dashboard panels and alerts after the OpenTelemetry Collector topology is
  finalized in Slice 6.5.
