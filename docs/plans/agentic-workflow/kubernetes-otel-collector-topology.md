# Rakka Agent Workflow OpenTelemetry Collector Topology

Status: Slice 6.5 reference artifact.

This document defines the near-production OpenTelemetry Collector topology for
Rakka agent workflow deployments on Kubernetes. It keeps the first runnable
shape as raw Kubernetes YAML while using names that can move cleanly into Helm
values later.

The topology follows the OpenTelemetry Collector agent and gateway patterns:
node-local collectors gather Kubernetes and host context, then forward to a
central gateway service that owns batching, redaction, sampling, routing, and
backend credentials. The official Collector configuration model is built from
receivers, processors, exporters, extensions, and service pipelines. The
gateway pattern gives Rakka one cluster OTLP endpoint while still allowing the
gateway deployment to scale horizontally.

## Files

- `kubernetes-otel-collector-topology.yaml`: namespace, RBAC, Collector
  ConfigMaps, node-agent DaemonSet, gateway Deployment, Services, and gateway
  PodDisruptionBudget.
- `kubernetes-reference-topology.yaml`: application Deployment that exports
  OTLP to `rakka-otel-collector.rakka-system.svc.cluster.local:4317` and
  supplies resource attributes used by Collector enrichment.

## Runtime Shape

Rakka application pods export traces, metrics, and logs over OTLP/gRPC to:

```text
http://rakka-otel-collector.rakka-system.svc.cluster.local:4317
```

That service fronts `Deployment/rakka-otel-gateway`. The gateway receives
application and node-agent OTLP traffic, applies memory limits, enriches
Kubernetes metadata, redacts high-risk agent payload fields, samples traces,
batches all signals, writes to the local debug exporter, and forwards to the
configured backend OTLP endpoint.

`DaemonSet/rakka-otel-agent` runs one Collector per node. It accepts optional
node-local OTLP traffic on ports `4317` and `4318`, scrapes kubelet node, pod,
and container metrics, collects host metrics, enriches telemetry with
Kubernetes metadata, and forwards all signals to the gateway service.

## Resource Attribute Contract

Application pods must provide stable resource attributes before export. The
Collector topology assumes these attributes are present or can be enriched:

- `service.name=rakka-agent-workflow`.
- `service.namespace=rakka-system`.
- `service.version`.
- `deployment.environment.name`.
- `k8s.namespace.name`.
- `k8s.pod.name`.
- `k8s.pod.uid`.
- `k8s.node.name`.
- `k8s.deployment.name`.
- `container.name`.
- `rakka.node.id`.

The `k8sattributes` processor uses `k8s.pod.uid`, `k8s.pod.ip`, or the network
connection to associate telemetry with Kubernetes metadata. Rakka pods export
pod UID and node name directly so gateway enrichment still works when app
telemetry skips the node-local agent and goes straight to the gateway service.

## Agent Collector

The node-agent Collector provides:

- OTLP/gRPC and OTLP/HTTP intake for optional node-local application export.
- `kubeletstats` receiver for node, pod, and container metrics.
- `hostmetrics` receiver for node CPU, memory, filesystem, network, and load.
- `memory_limiter`, `k8sattributes`, `resource/rakka`, and `batch`
  processors.
- `otlp/gateway` exporter with retry and bounded sending queue.

The DaemonSet exposes host ports `4317` and `4318` so workloads may choose a
node-local endpoint later. The current Rakka application topology uses the
gateway service endpoint by default because it is simpler for local Docker
Desktop testing and future Helm templating.

## Gateway Collector

The gateway Collector provides:

- one ClusterIP OTLP endpoint for all Rakka application pods and node agents;
- `memory_limiter` and `batch` processors for backpressure and efficient
  export;
- Kubernetes and Rakka resource enrichment;
- `transform/redact` rules for prompt, completion, tool, artifact, and
  authorization-like fields;
- `probabilistic_sampler` for trace volume control;
- `debug` exporter for local validation;
- `otlp/primary` exporter for the production backend endpoint.

The placeholder backend endpoint is:

```text
otel-backend.rakka-system.svc.cluster.local:4317
```

Production deployments should replace that value with a vendor, managed
OpenTelemetry endpoint, or in-cluster tracing/logging/metrics backend. TLS,
mTLS, authentication extensions, and vendor headers belong in the future Helm
values and secret wiring rather than hard-coded local defaults.

## Scaling Signals

Collector health and capacity should be monitored separately from Rakka
workflow pressure. Important Collector signals include memory limiter refusal
metrics, exporter queue capacity and queue size, exporter enqueue failures,
gateway pod CPU/memory, and backend export latency.

Scale the gateway Deployment when collector queue pressure or memory limiter
refusals persist. Scale or tune the node-agent DaemonSet by reducing scrape
frequency, disabling expensive receivers, or moving high-cardinality log
collection into a dedicated pipeline.

## Local Validation

Validate both Kubernetes manifests:

```sh
kubectl apply --dry-run=client -f docs/plans/agentic-workflow/kubernetes-otel-collector-topology.yaml
kubectl apply --dry-run=client -f docs/plans/agentic-workflow/kubernetes-reference-topology.yaml
```

Run the contract tests:

```sh
cargo test -p rakka-k8s --test agent_workflow_otel_collector_topology
cargo test -p rakka-k8s --test agent_workflow_topology
cargo test -p rakka-agent-workflow --test otlp_collector
```

After applying the topology, check Collector rollout and service endpoints:

```sh
kubectl -n rakka-system rollout status daemonset/rakka-otel-agent
kubectl -n rakka-system rollout status deployment/rakka-otel-gateway
kubectl -n rakka-system get svc rakka-otel-collector rakka-otel-agent
```

## Helm Path

The current manifest names should become chart values:

- `otel.collector.image.repository`, `otel.collector.image.tag`.
- `otel.collector.namespaceOverride`.
- `otel.collector.agent.enabled`.
- `otel.collector.agent.hostPorts.enabled`.
- `otel.collector.agent.kubeletStats.enabled`.
- `otel.collector.gateway.enabled`.
- `otel.collector.gateway.replicaCount`.
- `otel.collector.gateway.backend.endpoint`.
- `otel.collector.gateway.sampling.percentage`.
- `otel.collector.gateway.redaction.enabled`.
- `otel.collector.resources.agent`.
- `otel.collector.resources.gateway`.
- `otel.collector.rbac.create`.

## References

- OpenTelemetry Collector configuration:
  <https://opentelemetry.io/docs/collector/configuration/>.
- OpenTelemetry Collector deployment patterns:
  <https://opentelemetry.io/docs/collector/deploy/>.
- OpenTelemetry Collector on Kubernetes:
  <https://opentelemetry.io/docs/platforms/kubernetes/collector/>.
- OpenTelemetry Collector scaling guidance:
  <https://opentelemetry.io/docs/collector/scaling/>.

## Open Items

- Move backend endpoint and TLS/auth settings into Secrets or Helm values.
- Add NetworkPolicies in Slice 6.6 so only Rakka pods and node agents can reach
  the gateway OTLP service.
- Add optional Prometheus ServiceMonitor or PodMonitor manifests once the local
  monitoring stack is selected.
- Add HPA/KEDA examples for the gateway after Collector self-metrics are
  scraped in the local cluster.
