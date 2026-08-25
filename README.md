[![CI](https://github.com/paloul-tech/rakka/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/paloul-tech/rakka/actions/workflows/ci.yml)

![Rakka - Rust Actor Framework](media/rakka-banner.png)

Rakka is a Rust actor framework built around typed actors, durable state, Rakka-owned cluster coordination, Kubernetes operation, Protobuf remoting, and supervised child-process actors.

The current repository state is a v1 release-candidate foundation: local typed actors, durable state APIs, typed persistence storage foundations, Akka-named event-sourced and durable-state behavior facades, in-memory and PostgreSQL persistence stores, cluster membership/discovery foundations, TCP and deterministic remoting, sharding, supervised process actors, process-backed entities, durable workflow inbox/outbox reliability, bounded streams, HTTP/gRPC adapters, Kubernetes health/drain hooks, operational metrics, generated contract examples, and reviewable Kubernetes manifests.

## Documentation
- `docs/rakka-phase-3-remote-sharding.md` for the current remote entity routing flow and the boundary between production foundations and deterministic test scaffolding.
- `docs/rakka-phase-4-process-workflow.md` for process actor ownership, security defaults, and durable workflow reliability boundaries.
- `docs/rakka-phase-5-integration-surfaces.md` for HTTP, gRPC, stream, Kubernetes, and metrics integration boundaries.
- `docs/rakka-compatibility.md` for v1 rolling-update compatibility rules, allowed version skew, and compatibility test commands.
- `docs/rakka-v1-api-review.md` for the current public API review notes, crate map, feature boundaries, and error-code policy.
- `docs/rakka-v1-generated-contracts.md` for generated gRPC contracts, mirrored HTTP routes, and the adapter boundary.
- `docs/rakka-v1-observability-exporters.md` for Prometheus/OpenTelemetry exporter adapters, snapshot routes, and cardinality guidance.
- `docs/rakka-v1-security-operational-defaults.md` for trusted remoting boundaries, process execution defaults, timeout budgets, and Kubernetes security assumptions.
- `docs/rakka-v1-release-packaging.md` for CI, release-candidate validation, packaging, and image build notes.
- `docs/rakka-v1-reliability-boundaries.md` for v1 reliability guarantees, non-guarantees, and operator/application responsibilities.
- `docs/rakka-v1-rolling-update-upgrade.md` for the N/N+1 Kubernetes rolling-update sequence.
- `docs/rakka-v1-known-limitations-roadmap.md` for known limitations and post-v1 roadmap items.
- `docs/rakka-v1-release-candidate-review.md` for the final v1 review checklist and example coverage matrix.
- `docs/rakka-api-boundary-inventory.md` for the facade/foundation/adapter/test-support API boundary.
- `docs/rakka-akka-parity-migration-notes.md` for the first migration notes toward Akka-like Rakka APIs.
- `docs/rakka-akka-parity-phase-2-actor-facade.md` for the actor facade, context ergonomics, testkit probes, and async closure tradeoffs.
- `docs/rakka-akka-parity-phase-5-cluster-receptionist-routers.md` for the Akka parity cluster extension, receptionist, router, and testkit guide.
- `docs/rakka-akka-parity-phase-6-streams.md` for the Akka-shaped bounded stream facade, process IO migration, and stream testkit probes.
- `docs/plans/agentic-workflow/` for the agent workflow spec, implementation plan, Kubernetes manifests, OpenTelemetry Collector topology, runbooks, dashboard guidance, and production-candidate support material.

Historical implementation plans live in `docs/plans/`.

## Crate Map

| Crate | Role |
| --- | --- |
| `rakka` | Top-level facade crate and curated prelude for application code. |
| `rakka-core` | Typed actors, actor refs, supervision, paths, shared metrics, and framework errors. |
| `rakka-persistence`, `rakka-persistence-postgres` | Durable state APIs, typed event/snapshot stores, event-sourced and durable-state behavior facades, query helpers, in-memory stores, and PostgreSQL persistence plugins. |
| `rakka-cluster`, `rakka-remote`, `rakka-sharding`, `rakka-sharding-postgres`, `rakka-discovery-etcd` | Membership, remoting, protocol compatibility, sharded entity routing, the PostgreSQL shard coordinator, leadership lease, and remembered-entity store, and the etcd external-arbiter discovery provider. |
| `rakka-process`, `rakka-workflow`, `rakka-stream` | Child-process actors, durable inbox/outbox reliability, and bounded stream primitives. |
| `rakka-agent-workflow` | Durable agent-workflow execution kernel: product-neutral compiled execution IR, durable graph run state, deterministic graph scheduler, and the durable outbox effect bridge. |
| `rakka-agent`, `rakka-agent-postgres` | Durable agent domain: agent/task/run entities, choreography, the durable loop, model adapter trait (Rig behind the `rig` feature), effects and tool authority, budgets, checkpoints/HITL, session and private memory, retrieval, observability; plus the PostgreSQL memory stores and pgvector retrieval adapter. |
| `rakka-agent-knowledge-graph` | Database-agnostic communal knowledge graph: provenance-bearing claims with the `Proposed`/`Verified`/`Disputed`/`Retracted` trust lattice, append-only transitions, the checkpoint-grant promotion gate, the portable store SPI, the in-memory reference store, and the backend conformance harness. |
| `rakka-http`, `rakka-grpc`, `rakka-k8s` | Edge adapters and Kubernetes operation surfaces. |
| `rakka-a2a` | A2A protocol adapter for durable agent-workflow runs: durable request handler, task projection and streaming replay, push config persistence and dispatch boundary, sharded run owner host, and dynamic agent card. |
| `rakka-testkit` | Cross-crate integration helpers and compatibility fixtures. |

## Validation Commands

The two primary local validation entry points are:

```sh
scripts/validate.sh
scripts/package-check.sh
```

`scripts/validate.sh` runs the required format, clippy, workspace tests, minimal feature checks, docs, and safe Kubernetes dry-run checks. `scripts/package-check.sh` validates publishable crate package file lists in Cargo offline mode, fully packages crates without unpublished internal Rakka dependencies, and confirms examples remain excluded from publishing.

The underlying commands are:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
cargo test -p rakka-example-generated-contracts --test generated_contracts -- --nocapture
cargo test -p rakka-core --test observability_exporters
cargo test -p rakka-core --test security_operational_defaults
cargo test -p rakka-http --test observability_routes
cargo run -p rakka-example-local-receptionist-router
cargo run -p rakka-example-pool-router
cargo run -p rakka-example-clustered-receptionist
cargo run -p rakka-example-streams
cargo check -p rakka-stream --no-default-features
cargo check -p rakka-process --no-default-features
cargo doc --workspace --all-features --no-deps
```

Optional checks are gated because they need external services, child processes, or a mutable cluster.

The optional multi-process compatibility check launches two loopback node processes:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
```

The optional PostgreSQL persistence check expects a local PostgreSQL database:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres
```

The optional etcd discovery check expects a reachable etcd:

```sh
RAKKA_ETCD_TEST_ENDPOINTS=http://127.0.0.1:2379 cargo test -p rakka-discovery-etcd --test etcd_discovery -- --nocapture
```

The optional Kubernetes local-cluster scenario expects `kubectl`, a current context, and an application image that satisfies the manifest contract:

```sh
RAKKA_K8S_RUN_LOCAL_CLUSTER=1 RAKKA_K8S_IMAGE=<image> examples/kubernetes/local-cluster-scenario.sh
```

## Release Packaging

Rakka workspace crates share release metadata from the root manifest, and internal crate dependencies include explicit versions for packaging. Example packages are review/test assets and remain `publish = false`.

Strict publishing policy: do not publish any Rakka crate, container image, release artifact, or generated bundle to a public or private registry without explicit user approval for that exact action. `scripts/package-check.sh` and `cargo package` are validation-only commands, and the package-check script always runs `cargo package` with `--offline`.

Before cutting a v1 release candidate, run:

```sh
scripts/validate.sh
scripts/package-check.sh
```

Then review `CHANGELOG.md`, `docs/rakka-v1-release-packaging.md`, and `docs/rakka-v1-release-candidate-review.md`.

## Examples

Run examples from the workspace root.

### Minimal Actor System

```sh
cargo run -p rakka-example-minimal-system
```

Expected output:

```text
Rakka Phase 2 actor facade replied with pong on tokio.
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

### Event-Sourced Counter

This example uses `EventSourcedBehavior`, the in-memory event journal, snapshots, replies after persistence, and snapshot retention.

```sh
cargo run -p rakka-example-event-sourced-counter
```

Expected output:

```text
Rakka event-sourced counter values: 1, 2, recovered 2.
```

### Sharded Cart Persistence

This example derives event-sourced persistence ids from sharding entity type and entity ids using `PersistenceId::of`, writes cart state on a shard owned by node A, gracefully moves the shard to node B, and proves node B recovers the same cart state by replaying persistence.

```sh
cargo run -p rakka-example-sharded-cart-persistence
```

Expected output:

```text
Rakka sharded cart movement (in-memory) used entity type CartMovement and persistence id CartMovement|cart-0.
node A initially owned cart-0 on shard N and wrote cart total 2.
ownership moved from rakka-0#uid-a to rakka-1#uid-b at coordinator revision N.
node B recovered cart total 2 from persistence; persisted coordinator revision N was reloadable.
```

The same example can use PostgreSQL for both the shard coordinator snapshot and entity event/snapshot storage:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo run -p rakka-example-sharded-cart-persistence -- --postgres
```

### Local Receptionist And Group Router

This example registers two typed service actors with the local receptionist and routes work through a receptionist-backed group router.

```sh
cargo run -p rakka-example-local-receptionist-router
```

Expected output:

```text
Rakka local receptionist group router delivered [worker-a:1, worker-b:2] across 2 routees.
```

### Pool Router

This example spawns a local worker pool and routes six jobs in deterministic round-robin order.

```sh
cargo run -p rakka-example-pool-router
```

Expected output:

```text
Rakka pool router sent 6 jobs through 3 routees: [(0, 0), (1, 1), (2, 2), (3, 0), (4, 1), (5, 2)].
```

### Clustered Receptionist

This example creates two logical cluster nodes in one process, propagates a local receptionist listing from node A to node B, and routes from node B through the propagated listing.

```sh
cargo run -p rakka-example-clustered-receptionist
```

Expected output:

```text
Rakka clustered receptionist propagated true and routed rakka-0:7 through 1 remote routee.
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

### Stream Facade

This example exercises the Akka-shaped bounded stream facade: finite operators, an acked actor sink, process stdout as a facade source, and stream testkit probes. It is self-contained and starts itself with a hidden child flag for the process stdout example.

```sh
cargo run -p rakka-example-streams
```

Expected output:

```text
Finite stream operators produced [6, 8].
Acked actor sink delivered ["init", "apple", "banana", "complete"].
Process stdout facade source read "child-stream-output".
Stream testkit probe collected ["probe-one", "probe-two"].
```

### Edge Gateway

This end-to-end edge integration example runs an in-process HTTP gateway, gRPC unary and bidirectional-streaming adapters, bounded stream ingestion, a process-backed legacy service, Kubernetes readiness/drain checks, and in-memory metrics. It does not bind public ports or require Kubernetes; it exercises the public adapter surfaces directly.

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
Observability routes exposed /metrics, /otel/metrics, and /snapshots.
```

### Generated Contracts

This V1 hardening example starts from a `.proto` service contract, generates tonic client/server code at build time, implements the generated services with Rakka adapters, mirrors the same messages through HTTP JSON and binary routes, accepts a durable workflow command, and wraps a line-json child process.

```sh
cargo run -p rakka-example-generated-contracts
```

Expected output:

```text
Generated gRPC CounterService returned value 7.
Generated gRPC CartService accepted book and CatalogService returned ["book", "box"].
Generated gRPC streaming accepted 2 upload item(s) and 2 bidi ack(s).
Generated gRPC WorkflowService revision 1 and LegacyService result 42.
Mirrored HTTP JSON returned counter 12, cart pencil, workflow revision 2, legacy 100; binary counter 23.
```

## Kubernetes Example

The reviewable Kubernetes example is in `examples/kubernetes`. It includes a three-replica `StatefulSet`, a headless internal service for stable pod DNS and internal Rakka remoting, a public HTTP/gRPC service, observability routes, readiness/liveness probes, a pre-stop drain hook, and a PodDisruptionBudget.

Inspect the manifest:

```sh
less examples/kubernetes/rakka-node.yaml
```

Run the manifest contract tests:

```sh
cargo test -p rakka-k8s --test kubernetes_manifests
```

Preview the local-cluster scenario without touching a cluster:

```sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh
```

Preview the optional N/N+1 rolling-update path:

```sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 RAKKA_K8S_NEXT_IMAGE=your-registry/rakka-node:next examples/kubernetes/local-cluster-scenario.sh
```

Validate the manifest with `kubectl` against your active context:

```sh
RAKKA_K8S_VALIDATE_MANIFESTS=1 cargo test -p rakka-k8s optional_kubectl_manifest_validation_is_gated -- --nocapture
```

Run the optional local-cluster scenario against your active kind/minikube context:

```sh
RAKKA_K8S_IMAGE=your-registry/rakka-node:dev RAKKA_K8S_RUN_LOCAL_CLUSTER=1 cargo test -p rakka-k8s optional_local_cluster_scenario_is_gated -- --nocapture
```

The local-cluster scenario is gated because it applies resources, checks `/metrics`, `/snapshots`, and `/scenario/sharding/route-remote`, calls `/drain`, verifies readiness fails after drain, deletes one pod, validates replacement, and optionally performs a partitioned rolling update when `RAKKA_K8S_NEXT_IMAGE` is set.

## Optional PostgreSQL Test

The PostgreSQL plugin has an optional round-trip test. It is skipped unless `RAKKA_POSTGRES_TEST_DSN` is set:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://user:password@localhost:5432/rakka cargo test -p rakka-persistence-postgres
```
