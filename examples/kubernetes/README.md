# Rakka Kubernetes Example

This directory contains a reviewable Kubernetes example for running three Rakka nodes with stable pod DNS, internal Rakka remoting, readiness/liveness probes, observability routes, a scenario endpoint for remote shard routing, and graceful pre-stop drain.

## Files

- `rakka-node.yaml`: namespace, config, headless internal service, public service, PodDisruptionBudget, and StatefulSet.
- `local-cluster-scenario.sh`: optional kind/minikube scenario runner for applying the manifest, waiting for readiness, checking metrics/snapshots, verifying remote sharded routing, exercising drain, replacing one pod, and validating a partitioned rolling update.

## Application Image Contract

The manifest is intentionally an application-image contract rather than a hosted demo image. Replace `ghcr.io/rakka-rs/rakka-node:0.1.0` with an image that runs a Rakka node and exposes:

- `GET /ready`: returns success only after cluster join, protocol compatibility acceptance, and required service registration.
- `GET /live`: stays healthy during normal rebalance and drain, but fails for stuck runtime conditions.
- `GET /drain`: starts coordinated shutdown with reason `kubernetes-prestop`, marks readiness false, hands off shards, drains streams, stops process actors, and returns a drain report.
- `GET /metrics`: returns Prometheus text metrics from the Rakka metrics exporter.
- `GET /snapshots`: returns JSON operational snapshots, including Kubernetes health.
- `GET /scenario/sharding/route-remote?entity_id=cart-v1g&item=apple&expect_remote=1`: routes a request through Rakka sharding to an entity owned by another pod through internal Rakka remoting, then returns a body containing the owning pod or node name.

The scenario script checks that readiness should fail after drain, so the application must make `/ready` return a non-2xx response after `/drain` begins.

## Ports

- `2552` named `remoting`: internal Rakka remoting and sharding traffic.
- `8080` named `http`: public HTTP adapter, readiness, liveness, and drain endpoints.
- `50051` named `grpc`: public gRPC adapter.

## Kubernetes Services

`rakka-internal` is a headless service with `clusterIP: None`. It is for internal Rakka remoting and Kubernetes DNS discovery only. It gives pods stable DNS names such as:

```text
rakka-node-0.rakka-internal.rakka-system.svc.cluster.local
```

That shape matches `KubernetesDnsDiscoveryConfig::new("rakka-system", "rakka-internal", 2552)`.

`rakka-public` is a normal `ClusterIP` service for public HTTP/gRPC traffic inside the cluster. External ingress, service mesh policy, and authentication are deliberately outside this example.

## Discovery And Remoting

The ConfigMap sets:

```text
RAKKA_DEPLOYMENT_PROFILE=production-like
RAKKA_DISCOVERY_PROVIDER=kubernetes-dns
RAKKA_HEADLESS_SERVICE=rakka-internal
RAKKA_EXPECTED_REPLICAS=3
RAKKA_REMOTING_TRUST_BOUNDARY=trusted-cluster
RAKKA_REMOTING_ALLOWED_PEERS=discovery
RAKKA_REMOTING_BIND_ADDR=0.0.0.0:2552
RAKKA_REMOTING_ADVERTISE_PORT=2552
```

The application should combine `RAKKA_POD_NAME`, `RAKKA_POD_UID`, `RAKKA_NAMESPACE`, `RAKKA_HEADLESS_SERVICE`, `RAKKA_CLUSTER_DOMAIN`, and `RAKKA_REMOTING_ADVERTISE_PORT` to derive the local node id and advertised pod DNS address.

Internal Rakka remoting should only be reachable by trusted Rakka pods. Use NetworkPolicy, service mesh policy, or equivalent cluster controls so `rakka-internal` is not exposed as a public API.

## Security And Operational Defaults

The example surfaces the V1 hardening defaults as environment variables:

```text
RAKKA_PROCESS_ALLOWLIST_REQUIRED=true
RAKKA_PROCESS_INHERIT_ENVIRONMENT=false
RAKKA_ACTOR_ASK_TIMEOUT_MS=5000
RAKKA_REMOTE_CONNECT_TIMEOUT_MS=2000
RAKKA_REMOTE_IDLE_TIMEOUT_MS=30000
RAKKA_STREAM_DRAIN_TIMEOUT_MS=5000
RAKKA_PROCESS_STARTUP_TIMEOUT_MS=5000
RAKKA_PROCESS_SHUTDOWN_TIMEOUT_MS=5000
RAKKA_K8S_PRESTOP_TIMEOUT_MS=30000
```

The app image should parse these values into the matching Rakka config builders. The Kubernetes `terminationGracePeriodSeconds` is `45`, leaving room after the `30s` pre-stop drain budget for reports and final cleanup.

## Health And Drain

The example expects the application image to expose:

- `GET /ready`: returns success after cluster join, compatibility acceptance, and required service registration.
- `GET /live`: stays healthy during ordinary shard rebalance and graceful drain, but fails for stuck runtime conditions.
- `GET /drain`: runs the coordinated pre-stop path and returns a drain report mapped from the coordinated shutdown report.

The StatefulSet pre-stop hook calls `/drain`, and `terminationGracePeriodSeconds` gives the node time to mark itself draining, stop ingress, drain adapters, leave membership, hand off shards, stop process actors, flush persistence hooks, and stop actor-system resources through the same coordinated shutdown path used by application termination.

## Observability

The local scenario checks:

- `/metrics` contains a stable Prometheus metric, defaulting to `rakka_http_request_latency_ms`.
- `/snapshots` contains a Kubernetes health snapshot, defaulting to `kubernetes_health`.

Override `RAKKA_K8S_METRICS_EXPECT` or `RAKKA_K8S_SNAPSHOTS_EXPECT` when your image exposes a different first metric or snapshot name.

## Rolling Compatibility

The manifest records an N/N+1 compatibility window:

```text
RAKKA_PROTOCOL_VERSION=1.0
RAKKA_COMPAT_MIN=1.0
RAKKA_COMPAT_MAX=1.1
RAKKA_COMPAT_POLICY=n-to-n-plus-one
```

During rolling updates, the next image should remain compatible with the current minor version until all pods have rolled.

When `RAKKA_K8S_NEXT_IMAGE` is set, the local scenario performs a partitioned rolling update. It first patches the StatefulSet rolling-update partition so only the highest ordinal updates, verifies mixed N/N+1 routing and probes, then lowers the partition to `0` to complete the rollout.

## Local Cluster Scenario

The scenario script is intentionally gated because it talks to the active Kubernetes context.

Dry run:

```sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh
```

Run against the current context:

```sh
RAKKA_K8S_IMAGE=your-registry/rakka-node:dev examples/kubernetes/local-cluster-scenario.sh
```

The script applies a temporary manifest with the provided image, waits for three pods, checks probes, checks metrics and snapshots, verifies remote sharded routing through `/scenario/sharding/route-remote`, calls drain on `rakka-node-1`, verifies readiness fails after drain, deletes that pod to exercise graceful replacement, checks the replacement pod UID changed, and optionally performs a partitioned rolling update when `RAKKA_K8S_NEXT_IMAGE` is set.

For the scenario checks, the image should include either `wget` or `curl`. Kubernetes itself uses native HTTP probes and does not require either tool for readiness, liveness, or pre-stop drain.

The script is intentionally gated because it applies resources, calls drain, deletes one pod, and can mutate the StatefulSet image during a rolling update.
