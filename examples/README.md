# Rakka Examples

Every example is a workspace package you run from the repository root with
`cargo run -p <package>`. They are self-contained: unless an example says
otherwise, it needs no database, cluster, or network service, and anything it
spawns (child processes, loopback listeners) it starts and stops itself.

Examples that have their own README are linked below and document their own
runs. The rest are documented on this page, each with the output it prints.

## Pick an example

### Actors and routing

| Package | What it shows |
| --- | --- |
| [`rakka-example-minimal-system`](#minimal-actor-system) | An actor system, a spawned actor, and a request/reply |
| [`rakka-example-local-receptionist-router`](#local-receptionist-and-group-router) | Service discovery through the receptionist and a group router |
| [`rakka-example-pool-router`](#pool-router) | A worker pool behind a round-robin router |
| [`rakka-example-coordinated-shutdown`](#coordinated-shutdown) | Ordered shutdown of HTTP, stream, process, and actor work |

### Persistence and reliability

| Package | What it shows |
| --- | --- |
| [`rakka-example-durable-counter`](#durable-counter) | Durable state that survives an actor restart |
| [`rakka-example-event-sourced-counter`](#event-sourced-counter) | Event sourcing with snapshots and recovery |
| [`rakka-example-durable-workflow`](#durable-workflow) | Inbox dedup, outbox retry, and recovery |

### Cluster and sharding

| Package | What it shows |
| --- | --- |
| [`rakka-example-clustered-receptionist`](#clustered-receptionist) | A receptionist listing propagated between two nodes |
| [`rakka-example-multi-node-sharding`](#multi-node-sharding) | Sharded entities across two nodes, in-memory or over TCP |
| [`rakka-example-sharded-cart-persistence`](#sharded-cart-persistence) | A sharded entity that moves nodes and recovers its state |
| [`rakka-example-clustered-counter-http`](clustered-counter-http/) | A multi-process cluster behind a REST API |
| [`rakka-example-clustered-counter-grpc`](clustered-counter-grpc/) | A multi-process cluster behind a generated gRPC service |

### Streams, processes, and edge adapters

| Package | What it shows |
| --- | --- |
| [`rakka-example-streams`](#streams) | Bounded stream operators, actor sinks, and stream test probes |
| [`rakka-example-external-binary-wrapper`](#external-binary-wrapper) | A child process wrapped as a supervised Rakka service |
| [`rakka-example-edge-gateway`](#edge-gateway) | HTTP and gRPC adapters, stream ingestion, drain, and metrics |
| [`rakka-example-generated-contracts`](#generated-contracts) | A `.proto` contract served over gRPC and mirrored over HTTP |

### Durable agents

| Package | What it shows |
| --- | --- |
| [`rakka-example-minimal-local-agent-workflow`](minimal-local-agent-workflow/) | The smallest path through the agent-workflow kernel |
| [`rakka-example-clustered-agent-workflow-http-grpc`](clustered-agent-workflow-http-grpc/) | Compiled agent workflows sharded across a multi-process cluster |
| [`rakka-example-clustered-sharded-entity-a2a-agents`](clustered-sharded-entity-a2a-agents/) | Durable agent runs exposed over A2A |
| [`rakka-example-durable-agent-acceptance`](durable-agent-acceptance/) | One sharded agent from start to finish |
| [`rakka-example-continuous-goal-acceptance`](continuous-goal-acceptance/) | A long-lived goal that wakes, runs, and passivates |
| [`rakka-example-multi-agent-goal-acceptance`](multi-agent-goal-acceptance/) | A root goal that delegates to specialist agents over A2A |
| [`rakka-example-coordination-capability-acceptance`](coordination-capability-acceptance/) | Handoff, teams, moderated conversations, and human tasks |
| [`rakka-example-agent-otlp-export-acceptance`](agent-otlp-export-acceptance/) | A real OpenTelemetry SDK exporting an agent run over OTLP |
| [`rakka-example-multi-pod-agent-fault-soak`](multi-pod-agent-fault-soak/) | Agents killed at every durable write across two processes |

### Kubernetes

[`kubernetes/`](kubernetes/) holds a reviewable three-replica manifest and a
local-cluster scenario script. [Kubernetes checks](#kubernetes-checks) below
lists the commands that validate them.

## Actors and routing

### Minimal actor system

Spawns one actor and asks it for a reply.

```sh
cargo run -p rakka-example-minimal-system
```

```text
Rakka Phase 2 actor facade replied with pong on tokio.
```

### Local receptionist and group router

Registers two typed service actors with the local receptionist and routes work
through a receptionist-backed group router.

```sh
cargo run -p rakka-example-local-receptionist-router
```

```text
Rakka local receptionist group router delivered [worker-a:1, worker-b:2] across 2 routees.
```

### Pool router

Spawns a local worker pool and routes six jobs in deterministic round-robin
order.

```sh
cargo run -p rakka-example-pool-router
```

```text
Rakka pool router sent 6 jobs through 3 routees: [(0, 0), (1, 1), (2, 2), (3, 0), (4, 1), (5, 2)].
```

### Coordinated shutdown

Registers shutdown work in the phase that owns it: stopping HTTP ingress,
draining a bounded stream, stopping a supervised child process, and a custom
task that writes a final audit record while persistence flushes. Then it runs
coordinated shutdown and checks that every task completed and that the stream
still delivered the item it held. For the child process, the example starts
itself as the child.

```sh
cargo run -p rakka-example-coordinated-shutdown
```

```text
audit actor recorded startup
audit actor recorded final-audit
Coordinated shutdown completed 9 phases and drained HTTP, streams, process actors, and custom tasks.
```

## Persistence and reliability

### Durable counter

Persists a counter in the in-memory durable state store, stops the actor,
spawns a second actor with the same persistence id, and recovers the value.

```sh
cargo run -p rakka-example-durable-counter
```

```text
Rakka durable counter recovered value 2.
```

### Event-sourced counter

Uses `EventSourcedBehavior` with the in-memory event journal: replies after
persistence, snapshots, and snapshot retention.

```sh
cargo run -p rakka-example-event-sourced-counter
```

```text
Rakka event-sourced counter values: 1, 2, recovered 2.
```

### Durable workflow

Uses the in-memory durable state store and a deterministic clock. It accepts
an inbox command with a deduplication key, detects a duplicate command,
schedules one outbox effect, detects a duplicate effect, recovers the workflow
from durable state, retries one failed dispatch, and records a successful one.

```sh
cargo run -p rakka-example-durable-workflow
```

```text
Accepted inbox work at revision 1; duplicate inbox reused message checkout-command-1.
Recovered 1 inbox item(s) and 1 due outbox item(s).
Duplicate outbox reused message email-confirmation.
First dispatch: email-confirmation failed on attempt 1 with temporary smtp outage; retry at 1100
Second dispatch: email-confirmation succeeded
Workflow revision after recovery dispatch: 6.
```

## Cluster and sharding

### Clustered receptionist

Creates two logical cluster nodes in one process, propagates a receptionist
listing from node A to node B, and routes from node B through the propagated
listing.

```sh
cargo run -p rakka-example-clustered-receptionist
```

```text
Rakka clustered receptionist propagated true and routed rakka-0:7 through 1 remote routee.
```

### Multi-node sharding

By default, runs two `ActorSystem`s in one process over the deterministic
in-memory transport. It builds a two-node membership, assigns shards for the
`Cart` entity type, sends a `CartCommand` from node A to an entity owned by
node B, and checks that it was delivered on node B.

```sh
cargo run -p rakka-example-multi-node-sharding
```

```text
Rakka multi-node sharding routed add-apple to cart-N on rakka-1#uid-b.
Shard ownership revision 1 allocated 8 shards across 2 up nodes.
node-a local entity count: 0
node-b facade local entity count: 1
node-b remembered entity count: 1
```

The `cart-N` value varies: the example searches for an entity id that the
current allocation places on node B.

The same shape over real Tokio TCP loopback remoting, in one process:

```sh
cargo run -p rakka-example-multi-node-sharding -- --networked-loopback
```

```text
Rakka networked sharding routed add-apple to cart-N on rakka-1#uid-b over TCP loopback.
Registered TCP peers: node-a 1, node-b 1; membership events: 3.
node-a facade local entity count: 0
node-b facade local entity count: 1
```

And as two child node processes on loopback ports:

```sh
cargo run -p rakka-example-multi-node-sharding -- --networked-processes
```

```text
rakka-1 received add-apple for cart-N.
rakka-1 facade local entity count: 1.
Rakka networked sharding launched two node processes on 127.0.0.1:PORT and 127.0.0.1:PORT.
node-a: rakka-0 sent add-apple to cart-N on rakka-1#uid-b.
```

### Sharded cart persistence

Derives event-sourced persistence ids from the sharding entity type and entity
id with `PersistenceId::of`, writes cart state on a shard owned by node A,
moves the shard to node B gracefully, and shows node B recovering the same cart
by replaying persistence.

```sh
cargo run -p rakka-example-sharded-cart-persistence
```

```text
Rakka sharded cart movement (in-memory) used entity type CartMovement and persistence id CartMovement|cart-0.
node A initially owned cart-0 on shard N and wrote cart total 2.
ownership moved from rakka-0#uid-a to rakka-1#uid-b at coordinator revision N.
node B recovered cart total 2 from persistence; persisted coordinator revision N was reloadable.
```

With PostgreSQL holding both the shard coordinator snapshot and the entity
events and snapshots:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo run -p rakka-example-sharded-cart-persistence -- --postgres
```

## Streams, processes, and edge adapters

### Streams

Exercises the Akka-shaped bounded stream facade: finite operators, an acked
actor sink, process stdout as a stream source, and stream testkit probes. For
the process source, the example starts itself as the child process.

```sh
cargo run -p rakka-example-streams
```

```text
Finite stream operators produced [6, 8].
Acked actor sink delivered ["init", "apple", "banana", "complete"].
Process stdout facade source read "child-stream-output".
Stream testkit probe collected ["probe-one", "probe-two"].
```

### External binary wrapper

Wraps a line-JSON child process as a Rakka-owned service. The example starts
itself as the child, so no binary has to be installed on the host.

```sh
cargo run -p rakka-example-external-binary-wrapper
```

```text
Rakka wrapped legacy-calculator and received result 42.
Captured child stderr: ["legacy child handled increment"]
```

### Edge gateway

An in-process HTTP gateway, gRPC unary and bidirectional-streaming adapters,
bounded stream ingestion, a process-backed legacy service, Kubernetes
readiness and drain checks, and in-memory metrics. It calls the adapters
directly, so it binds no public ports and needs no Kubernetes.

```sh
cargo run -p rakka-example-edge-gateway
```

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

### Generated contracts

Starts from a `.proto` service contract, generates tonic client and server
code at build time, implements the generated services with Rakka adapters,
mirrors the same messages over HTTP JSON and binary routes, accepts a durable
workflow command, and wraps a line-JSON child process. Building it needs
`protoc`.

```sh
cargo run -p rakka-example-generated-contracts
```

```text
Generated gRPC CounterService returned value 7.
Generated gRPC CartService accepted book and CatalogService returned ["book", "box"].
Generated gRPC streaming accepted 2 upload item(s) and 2 bidi ack(s).
Generated gRPC WorkflowService revision 1 and LegacyService result 42.
Mirrored HTTP JSON returned counter 12, cart pencil, workflow revision 2, legacy 100; binary counter 23.
```

## Kubernetes checks

[`kubernetes/README.md`](kubernetes/README.md) explains the manifest and the
scenario script. These commands check them.

The manifest contract tests, which need no cluster:

```sh
cargo test -p rakka-k8s --test kubernetes_manifests
```

Preview the local-cluster scenario, and its optional N/N+1 rolling update,
without touching a cluster:

```sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 RAKKA_K8S_NEXT_IMAGE=your-registry/rakka-node:next \
  examples/kubernetes/local-cluster-scenario.sh
```

The remaining checks are gated because they need `kubectl` with a current
context, a container runtime, or a cluster they may change.

Validate the manifest with `kubectl` against your active context:

```sh
RAKKA_K8S_VALIDATE_MANIFESTS=1 \
  cargo test -p rakka-k8s optional_kubectl_manifest_validation_is_gated -- --nocapture
```

Validate the agent OpenTelemetry Collector topology: its Kubernetes objects
with `kubectl`, and both Collector configurations against the pinned Collector
distribution, which needs a container runtime:

```sh
RAKKA_AGENT_OTEL_VALIDATE_MANIFESTS=1 \
  cargo test -p rakka-k8s --test agent_otel_collector_topology -- --nocapture
RAKKA_AGENT_OTEL_VALIDATE_COLLECTOR_CONFIG=1 \
  cargo test -p rakka-k8s --test agent_otel_collector_topology -- --nocapture
```

Run the local-cluster scenario against a kind or minikube context. It applies
the manifest with your image, checks `/metrics`, `/snapshots`, and remote
sharded routing, drains a pod and checks that readiness then fails, replaces
that pod, and performs a partitioned rolling update when
`RAKKA_K8S_NEXT_IMAGE` is set:

```sh
RAKKA_K8S_IMAGE=your-registry/rakka-node:dev RAKKA_K8S_RUN_LOCAL_CLUSTER=1 \
  cargo test -p rakka-k8s optional_local_cluster_scenario_is_gated -- --nocapture
```
