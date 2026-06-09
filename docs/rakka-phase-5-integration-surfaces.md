# Rakka Phase 5 Integration Surfaces

Phase 5 adds public edge adapters around the actor, sharding, process, stream, Kubernetes, and metrics foundations. The goal is not to replace application frameworks; it is to make Rakka actors and entities straightforward to expose as HTTP/gRPC services and straightforward to operate in Kubernetes.

## Public Edges and Internal Remoting

HTTP and gRPC are public integration surfaces. They are intended for clients, ingress, service-to-service APIs, and application protocol boundaries. The adapters decode requests, call services, actors, or sharded entities, map typed failures to protocol-specific responses, and record request metrics.

Rakka remote envelopes remain an internal cluster transport boundary. They carry typed actor/entity messages between Rakka nodes after routing decisions, serialization policy checks, and compatibility checks. External callers should reach actors and entities through HTTP/gRPC adapters or application-defined clients, not by constructing remote envelopes directly.

Kubernetes resources expose three categories of ports:

- Public HTTP/gRPC service ports for application traffic.
- Internal remoting ports behind the headless service for node-to-node routing.
- Health/drain endpoints that Kubernetes calls during scheduling, restarts, and rolling updates.

## Streaming, Cancellation, and Drain

Rakka streams use bounded buffers. Producers wait when buffers are full, so back-pressure is explicit instead of becoming unbounded memory growth.

HTTP request-body streaming pumps request chunks into a bounded stream. If a consumer is slower than the client, the pump waits for capacity. If the client disconnects or the response stream fails, the stream is cancelled and downstream code observes a typed stream error.

gRPC client and bidirectional streaming use the same bounded stream model. Dropping a response stream cancels blocked inbound pumps, and gRPC deadlines map to deadline-exceeded statuses while cancelling the bounded source.

Kubernetes drain marks readiness false first, then runs registered steps such as stream drains, process actor stops, and shard-leave/handoff hooks. This lets load balancers stop sending new traffic while existing bounded streams and process-backed work are given a deadline to finish.

## Metrics and Snapshots

Phase 5 metrics use backend-neutral recorders. The in-memory recorder is intended for tests and examples; production exporters can adapt the same metric observations into Prometheus, OpenTelemetry, or another backend.

The end-to-end example records HTTP and gRPC request latency observations and uses testkit helpers to assert stable labels such as route, method, status, outcome, and error code.

## Review Commands

Run the end-to-end example:

```sh
cargo run -p rakka-example-edge-gateway
```

Run testkit coverage for HTTP, gRPC, streams, Kubernetes health/drain, and metrics helpers:

```sh
cargo test -p rakka-testkit
```

Run Kubernetes manifest contract checks:

```sh
cargo test -p rakka-k8s kubernetes_manifests
```

Preview the local Kubernetes scenario without applying resources:

```sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh
```

Run full workspace validation:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
```
