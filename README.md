# Rakka

Rakka is a Rust actor framework planned around typed actors, durable state, Rakka-owned cluster coordination, Kubernetes operation, Protobuf remoting, and supervised child-process actors.

The current repository state includes Phase 5 foundations: local typed actors, durable state APIs, in-memory and PostgreSQL durable state stores, cluster membership/discovery foundations, Protobuf remote envelopes, deterministic cluster sharding, supervised process actors, process-backed entities, durable workflow inbox/outbox reliability, bounded streams, HTTP/gRPC adapters, Kubernetes health/drain hooks, operational metrics, and reviewable Kubernetes manifests.

See `docs/rakka-phase-3-remote-sharding.md` for the current remote entity routing flow and the boundary between production foundations and deterministic test scaffolding.
See `docs/rakka-phase-4-process-workflow.md` for process actor ownership, security defaults, and durable workflow reliability boundaries.
See `docs/rakka-phase-5-integration-surfaces.md` for HTTP, gRPC, stream, Kubernetes, and metrics integration boundaries.

## Validation Commands

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
```

## Examples

Run examples from the workspace root.

### Minimal Actor System

```sh
cargo run -p rakka-example-minimal-system
```

Expected output:

```text
Rakka Phase 1 actor replied with pong on tokio.
```

### Durable Counter

This example uses the in-memory durable state store to persist a counter, stop the first actor, spawn a second actor with the same persistence id, and recover the value.

```sh
cargo run -p rakka-example-durable-counter
```

Expected output:

```text
Rakka durable counter recovered value 2.
```

### Durable Workflow

This example uses the in-memory durable state store and a deterministic workflow clock. It accepts an inbox command with a deduplication key, detects a duplicate inbox command, schedules one outbox effect, detects a duplicate outbox effect, recovers the workflow from durable state, retries one failed dispatch, and then records a successful dispatch.

```sh
cargo run -p rakka-example-durable-workflow
```

Expected output:

```text
Accepted inbox work at revision 1; duplicate inbox reused message checkout-command-1.
Recovered 1 inbox item(s) and 1 due outbox item(s).
Duplicate outbox reused message email-confirmation.
First dispatch: email-confirmation failed on attempt 1 with temporary smtp outage; retry at 1100
Second dispatch: email-confirmation succeeded
Workflow revision after recovery dispatch: 6.
```

### Multi-Node Sharding

This example defaults to two local `ActorSystem`s in one process using the deterministic in-memory remote transport. It does not require Kubernetes, multiple terminals, or real network services. The example builds a two-node membership view, assigns shards for the `Cart` entity type, sends a `CartCommand` from node A to an entity owned by node B, and verifies that delivery happens on node B.

```sh
cargo run -p rakka-example-multi-node-sharding
```

Expected output:

```text
Rakka multi-node sharding routed add-apple to cart-N on rakka-1#uid-b.
Shard ownership revision 1 allocated 8 shards across 2 up nodes.
node-a local entity count: 0
node-b local entity count: 1
```

The exact `cart-N` value can vary because the example searches for an entity id owned by node B under the current shard allocation.

Run the same shape over real Tokio TCP loopback remoting in one process:

```sh
cargo run -p rakka-example-multi-node-sharding -- --networked-loopback
```

Expected output:

```text
Rakka networked sharding routed add-apple to cart-N on rakka-1#uid-b over TCP loopback.
Registered TCP peers: node-a 1, node-b 1; membership events: 3.
node-a local entity count: 0
node-b local entity count: 1
```

Run the local multi-process TCP example, which launches two child Rakka node processes on loopback ports:

```sh
cargo run -p rakka-example-multi-node-sharding -- --networked-processes
```

Expected output:

```text
rakka-1 received add-apple for cart-N.
Rakka networked sharding launched two node processes on 127.0.0.1:PORT and 127.0.0.1:PORT.
node-a: rakka-0 sent add-apple to cart-N on rakka-1#uid-b.
```

### External Binary Wrapper

This example wraps a line-json child process as a Rakka-owned service. It is self-contained: the example executable starts itself with a hidden child flag, so no host-installed binary is required.

```sh
cargo run -p rakka-example-external-binary-wrapper
```

Expected output:

```text
Rakka wrapped legacy-calculator and received result 42.
Captured child stderr: ["legacy child handled increment"]
```

### Edge Gateway

This end-to-end Phase 5 example runs an in-process HTTP gateway, gRPC unary and bidirectional-streaming adapters, bounded stream ingestion, a process-backed legacy service, Kubernetes readiness/drain checks, and in-memory metrics. It does not bind public ports or require Kubernetes; it exercises the public adapter surfaces directly.

```sh
cargo run -p rakka-example-edge-gateway
```

Expected output:

```text
HTTP actor gateway returned counter value 7.
HTTP entity gateway accepted book for cart-1.
HTTP process-backed legacy service returned 42.
gRPC unary actor value 10 and entity SKU pencil.
gRPC bidirectional stream routed 2 cart updates.
Streaming ingestion transformed 2 items into entity commands.
Kubernetes readiness passed before drain; drain outcome Complete.
Metrics captured HTTP route /counter/add and gRPC method Add.
```

## Kubernetes Example

The reviewable Kubernetes example is in `examples/kubernetes`. It includes a three-replica `StatefulSet`, a headless internal service for stable pod DNS, a public HTTP/gRPC service, readiness/liveness probes, a pre-stop drain hook, and a PodDisruptionBudget.

Inspect the manifest:

```sh
less examples/kubernetes/rakka-node.yaml
```

Run the manifest contract tests:

```sh
cargo test -p rakka-k8s kubernetes_manifests
```

Preview the local-cluster scenario without touching a cluster:

```sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh
```

Validate the manifest with `kubectl` against your active context:

```sh
RAKKA_K8S_VALIDATE_MANIFESTS=1 cargo test -p rakka-k8s optional_kubectl_manifest_validation_is_gated -- --nocapture
```

Run the optional local-cluster scenario against your active kind/minikube context:

```sh
RAKKA_K8S_IMAGE=your-registry/rakka-node:dev RAKKA_K8S_RUN_LOCAL_CLUSTER=1 cargo test -p rakka-k8s optional_local_cluster_scenario_is_gated -- --nocapture
```

The local-cluster scenario is gated because it applies resources, calls `/drain`, deletes one pod, and optionally performs a rolling update when `RAKKA_K8S_NEXT_IMAGE` is set.

## Optional PostgreSQL Test

The PostgreSQL plugin has an optional round-trip test. It is skipped unless `RAKKA_POSTGRES_TEST_DSN` is set:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://user:password@localhost:5432/rakka cargo test -p rakka-persistence-postgres
```
