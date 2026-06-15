# Rakka Actor Framework Specification

Status: Initial draft
Date: 2026-06-02

## 1. Purpose

Rakka is a Rust actor framework for building highly scalable, durable, Kubernetes-native services. It is modeled against the core architectural ideas in Akka: typed actor protocols, actor references and paths, location-transparent routing, supervision trees, cluster membership, sharding, durable state, back-pressure-aware streams, and HTTP/gRPC integration.

Rakka adapts those concepts to Rust's ownership, memory safety, async ecosystem, and operational expectations for Kubernetes. The framework should make local actor execution cheap, distributed actor placement explicit but ergonomic, and failure visible enough that applications can be designed honestly for a fallible network.

## 2. Goals

- Use Rust to provide memory safety, predictable resource usage, small binary footprints, and high throughput.
- Support millions of lightweight local actors per cluster when workloads and memory budgets allow.
- Run cleanly on Kubernetes, including dynamic pod discovery, graceful rolling updates, health checks, and horizontal scale-out.
- Route to actors by logical identity regardless of the Kubernetes node or pod currently hosting them.
- Support stateful actors with durable state recovery, fencing, and single-active ownership.
- Provide supervision, lifecycle monitoring, backoff, coordinated shutdown, and dead-letter handling.
- Encapsulate external binaries as first-class actors so legacy software can run inside the actor cluster behind typed service protocols.
- Expose actors through HTTP and gRPC integration layers without making HTTP or gRPC the application core.

## 3. Non-Goals

- Rakka is not a web framework. HTTP and gRPC are integration boundaries.
- Rakka does not hide all distributed-systems failure modes. Default message delivery is best-effort and applications must use replies, acknowledgements, idempotency, or durable workflows where stronger semantics are needed.
- Rakka does not provide a Kubernetes service mesh replacement. Actor remoting requires direct pod-to-pod addressing for cluster internals.
- Rakka does not initially provide transparent code mobility or arbitrary remote deployment of closures.
- Rakka does not initially guarantee exactly-once side effects. Durable state and deduplication APIs will support idempotent design.

## 4. Akka-Inspired Baseline

The Akka concepts used as the model are:

- Actor systems manage actor hierarchies, shared runtime services, scheduling, logging, and configuration.
- Actors encapsulate state and behavior, process one message at a time, and communicate only by message passing.
- Actor references hide the actor instance and allow messages to be sent without external access to internal state.
- Actor paths provide stable logical addressing, while actor references identify a specific actor incarnation.
- Location transparency depends on serializable messages, asynchronous interaction, and remote-safe design.
- Supervision separates unexpected failure handling from business validation and supports resume, restart, and stop strategies.
- Akka Typed models actor protocols with typed `ActorRef[T]` and typed behaviors.
- Routers distribute messages to routees; cluster-aware group routers use discovery and eventually consistent registrations.
- Cluster membership tracks node health and lifecycle; sharding routes entities by logical identifiers and places shards across the cluster.
- Durable State persists the latest state for an entity and processes the next command only after state persistence succeeds.
- Streams provide bounded, back-pressure-aware processing where actors alone would risk unbounded mailboxes or lost data.
- Akka HTTP and Akka gRPC are integration libraries layered around actors, streams, and service protocols.

## 5. Core Architectural Concepts

### 5.1 Actor System

`ActorSystem` is the root runtime for one logical Rakka application instance inside a process.

Responsibilities:

- Own Tokio-backed async execution, dispatchers, timers, logging, metrics, tracing, configuration, and serialization registries.
- Host root guardians: `/user` for application actors and `/system` for framework actors.
- Own the local actor registry and remote transport endpoints.
- Join, leave, and drain a cluster.
- Run coordinated shutdown on process termination or Kubernetes pre-stop hooks.

Boundary:

- One process should normally run one `ActorSystem`.
- Multiple systems in one process are allowed for tests and advanced embedding but must not share global mutable runtime state.
- Rakka v1 targets Tokio only. Abstracting over async runtimes is deferred until the actor, cluster, persistence, process, HTTP, and gRPC layers are stable.

### 5.2 Actor

An actor is the smallest unit of stateful computation.

An actor has:

- A typed message protocol.
- Internal state owned only by the actor task.
- A behavior function or trait implementation that handles messages sequentially.
- A mailbox.
- A lifecycle: starting, running, stopping, stopped.
- Optional children and a supervisor strategy.

Rust shape, subject to implementation validation:

```rust
trait Actor: Send + 'static {
    type Msg: Message;

    async fn handle(&mut self, ctx: &mut ActorContext<Self>, msg: Self::Msg) -> ActorResult;
}
```

### 5.3 Actor Reference

`ActorRef<M>` is a typed capability to send messages of type `M`.

Requirements:

- Local refs must be cloneable, `Send`, and cheap to pass between actors.
- Remote refs must contain enough routing metadata to locate the actor or entity.
- External code must not access actor state through an actor ref.
- Actor refs identify an incarnation. A restarted actor keeps the same ref; a terminated and recreated actor at the same path gets a new incarnation.

### 5.4 Actor Path and Actor Identity

Rakka actor paths follow this logical form:

```text
rakka://<cluster>/<system>/<scope>/<segments>
```

Examples:

```text
rakka://prod/orders/user/gateway
rakka://prod/orders/entity/cart/tenant-7/cart-123
```

Identity types:

- `ActorPath`: logical name that may or may not currently be inhabited.
- `ActorUid`: unique runtime identity for a concrete actor lifetime.
- `EntityId`: domain identity for sharded stateful actors.
- `PersistenceId`: stable durable-state identity, derived from entity type plus entity id.
- `NodeId`: stable process incarnation identity, including pod identity plus runtime UID.

### 5.5 Messages

Messages must be:

- `Send + 'static` for local delivery.
- Serializable for remote delivery through the Rakka serialization registry.
- Versioned for cluster compatibility.
- Bounded by configured size limits.

Serialization requirements:

- Protobuf is the default remote message format.
- Remote message schemas must be versioned.
- Remote protocols must maintain N/N+1 compatibility during Kubernetes rolling updates.
- Schema changes should be additive by default, preserving old fields and accepting unknown compatible fields.
- Breaking protocol changes require an explicit migration plan that prevents incompatible nodes from coexisting in the same cluster.
- The serialization registry is pluggable from v1, allowing alternative codecs for trusted internal traffic, specialized payloads, or future performance optimizations.
- Unknown or incompatible message versions fail closed with typed transport errors and observable telemetry.

Remote message envelopes include:

- Source ref or anonymous sender.
- Destination path, entity id, route key, or service key.
- Message type id and schema version.
- Codec id.
- Trace context and causation metadata.
- Optional request id for ask/reply.
- Optional delivery requirements.

Default delivery semantics:

- At-most-once delivery in `rakka-core`.
- Per direct sender-target ordering where transport and mailbox configuration support it.
- No global, causal, or exactly-once ordering guarantee.
- Dead letters for undeliverable local messages and observable dropped remote messages.

Stronger semantics are explicit overlays outside the core actor runtime:

- Ask with timeout.
- Business acknowledgements.
- Idempotency keys.
- Durable inbox/outbox.
- Per-entity sequence numbers.
- At-least-once retry with deduplication.
- Durable workflow recovery.

### 5.6 Mailboxes and Dispatchers

Mailboxes are bounded by default.

Mailbox types:

- FIFO bounded mailbox.
- Priority mailbox.
- Stash mailbox for actors that temporarily defer messages.
- Durable inbox for selected workflows.

Dispatcher types:

- Default async dispatcher for non-blocking actors.
- Blocking dispatcher for filesystem, CPU-heavy, or legacy blocking calls.
- Process dispatcher for actors that own external binaries.
- System dispatcher for framework internals.

Back-pressure policy:

- Local sends to bounded full mailboxes return `SendError::Full` or publish dropped telemetry, depending on API.
- Remote sends must apply transport-level flow control and reject oversized or overloaded requests.
- Streams should be used for high-volume flows where the producer must be slowed rather than allowed to fill mailboxes.

### 5.7 Supervision and Monitoring

Supervision is configured outside the business message handler.

Strategies:

- `Resume`: keep state and continue with the next message.
- `Restart`: drop in-memory state, create a new actor instance, optionally recover durable state.
- `Stop`: terminate permanently.
- `Escalate`: let the parent decide.

Features:

- Backoff and retry limits.
- Restart budgets per time window.
- Pre-stop and post-restart hooks.
- Parent-child supervision trees.
- DeathWatch-style monitoring for any actor ref.
- Failure telemetry with panic payloads, process exit codes, and durable-store errors.

Rules:

- Validation errors belong in actor protocols, not supervision.
- Panics and unexpected IO/process/storage failures are supervision events.
- The message being processed during an unexpected failure is not automatically retried unless the actor or durable workflow explicitly requests it.

### 5.8 Cluster Membership

Each Rakka process is a cluster node.

Membership requirements:

- Nodes join through Kubernetes discovery, static seed lists, or test-only local discovery.
- Membership state tracks joining, up, leaving, unreachable, down, and removed.
- Node identity must include an incarnation UID to distinguish restarted pods.
- Failure detection must be configurable and observable.
- Split-brain policy must be explicit before production use.

Recommended implementation:

- Kubernetes: discover pod contact points using Kubernetes API or DNS from a headless service.
- Internal remoting: direct pod IP or stable pod DNS, not a load-balanced service.
- External traffic: separate Kubernetes Service or Ingress for HTTP/gRPC.
- Cluster bootstrap: wait for a configured minimum contact count on first deployment, then join an existing cluster or deterministically form one.

### 5.9 Sharding and Location-Transparent Entity Routing

Cluster sharding is the main mechanism for "route to actor by identity regardless of node."

Concepts:

- `EntityType`: named actor type, such as `Cart`, `Order`, or `LegacyPdfRenderer`.
- `EntityId`: domain id within an entity type.
- `ShardId`: deterministic partition derived from entity type and entity id.
- `ShardRegion`: local node component that accepts entity messages.
- `ShardCoordinator`: Rakka-owned internal cluster component that assigns shards to nodes.
- `EntityRef<M>`: typed logical ref for sharded entities.

Routing flow:

1. Caller sends `M` to `EntityRef<EntityType, EntityId>`.
2. Local `ShardRegion` maps entity id to shard id.
3. Region resolves shard owner from local cache or coordinator.
4. Region forwards to the owning node.
5. Owning region starts entity on demand, recovers durable state if needed, and delivers the message.

Requirements:

- Entity location may change without caller code changing.
- Only one active entity instance may own a persistence id at a time.
- Shard coordination is owned by Rakka cluster internals; Kubernetes is used for node discovery, lifecycle hooks, and infrastructure health, not as the primary shard-placement authority.
- Rebalancing must move shards when nodes join, leave, or fail.
- Messages for unknown shard owners are buffered within configured limits.
- During failover, callers may observe timeout, retry, duplicate, or dropped outcomes according to configured delivery mode.

### 5.10 Receptionist and Service Routing

The receptionist is a dynamic registry for discoverable actors and services.

Use it for:

- Stateless service actors.
- Actor groups with many equivalent instances.
- Local and cluster-wide service discovery.
- Group routers.

Do not use it for:

- Stateful entity identity. Use sharding.
- Durable actor uniqueness. Use sharding plus persistence fencing.

Router types:

- Pool router: local children only.
- Group router: discovered routees across the cluster.
- Consistent hash router: key-based routing to a service group.
- Shard router: entity-id routing with stable ownership and rebalancing.
- Broadcast router: controlled fan-out to registered routees.

### 5.11 Durable State

Durable state stores the latest state for a persistent actor after each accepted command.

Core API:

```rust
trait DurableActor: Actor {
    type State: StateCodec;
    type Command: Message;

    fn persistence_id(&self, ctx: &ActorContext<Self>) -> PersistenceId;
    fn empty_state(&self) -> Self::State;
    async fn handle_command(
        &mut self,
        state: &Self::State,
        command: Self::Command,
    ) -> DurableEffect<Self::State>;
}
```

Effects:

- `Persist(new_state)`.
- `Delete`.
- `Reply(reply_to, value)`.
- `None`.
- `Unhandled`.
- `Stop`.
- `Stash`.
- `ThenRun` side effects after successful persistence.

Requirements:

- The current state is loaded before command processing.
- The next command is not processed until the selected state change is durably committed.
- State writes use optimistic revision checks or equivalent fencing.
- Reads can be served from memory for the active entity.
- Store plugins must support snapshots, revisions, compare-and-set, deletes, and query by persistence id.
- Default production plugin should target PostgreSQL-compatible stores first, with later plugins for FoundationDB, Scylla/Cassandra, S3/object snapshots, and embedded test stores.

Optional later mode:

- Event sourcing with event replay, snapshots, projections, and durable event streams.

### 5.12 Streams

Rakka Streams provide bounded, back-pressure-aware pipelines for data flows.

Use streams for:

- Large request/response bodies.
- Process stdin/stdout pipes.
- File or network IO.
- Data ingestion.
- Fan-in/fan-out where mailboxes would overflow.
- gRPC streaming.

Requirements:

- Back-pressure across async boundaries.
- Bounded buffers by default.
- Cancellation and graceful drain.
- Actor interop through source/sink adapters.
- Process IO adapters for external-binary actors.

### 5.13 HTTP and gRPC Integration

HTTP and gRPC are integration layers around actors and streams.

HTTP requirements:

- Provide a small server toolkit using Rust ecosystem primitives.
- Route requests to actor refs, entity refs, streams, or service handlers.
- Support JSON, binary payloads, WebSocket, SSE, and streaming bodies.
- Keep HTTP routes outside actor state and business logic.

gRPC requirements:

- Use protobuf service descriptors and generated Rust types.
- Support unary, server streaming, client streaming, and bidirectional streaming.
- Map gRPC methods to actor protocols and stream pipelines.
- Support long-lived internal service contracts between Rakka services and non-Rakka services.

### 5.14 External Binary Actors

External binary actors let Rakka own legacy third-party software as a supervised, routable service.

For v1, process actors run child processes inside the Rakka node container. Rakka does not create per-actor Kubernetes sidecar containers in v1.

An external binary actor owns:

- The child process lifecycle.
- The process environment, working directory, arguments, stdin/stdout/stderr, and IPC sockets.
- Health checks, readiness, startup timeout, shutdown timeout, and restart policy.
- Resource configuration, including CPU/memory hints, file descriptors, temp dirs, and optional cgroup/container integration.
- Logs, metrics, traces, exit status, and crash diagnostics.
- Durable state needed to resume or reconcile after process restart.

Interaction modes:

- `stdio`: actor translates messages to stdin and parses stdout.
- `line-json`: newline-delimited JSON request/reply.
- `grpc`: actor starts binary and talks to its local gRPC port.
- `tcp` or Unix domain socket.
- `file-watch`: actor manages input/output files in a sandbox directory.
- `one-shot`: actor starts the binary per command with bounded runtime.

Supervision:

- Process exit is an actor failure signal.
- Unhealthy process state can trigger restart or stop.
- Restart uses backoff and max restart budgets.
- In-flight requests are failed, retried, or recovered according to a configured process protocol.
- Shutdown sends graceful signal first, then kill after timeout.

Security:

- Default to least-privilege process execution.
- Explicit allowlist for executable paths.
- No inherited secrets unless declared.
- Per-process working directory and temp directory.
- Optional seccomp/AppArmor profile guidance for Kubernetes.

Cluster semantics:

- A process-backed entity is still addressed by `EntityRef`.
- Sharding ensures the owning actor and its child process run on the selected node.
- Durable state plus fencing prevents two live process actors from claiming the same logical service identity after failover.

Future extension:

- Per-actor sidecar containers may be supported later for workloads that need stronger process isolation, independent container images, or Kubernetes-native resource policy per actor.

## 6. System Boundaries

### 6.1 Framework Boundary

Rakka provides:

- Actor runtime.
- Cluster and remote transport.
- Sharding and routing.
- Persistence interfaces and default plugins.
- Process ownership runtime.
- HTTP/gRPC adapters.
- Stream primitives.
- Testkit and simulation tools.
- Kubernetes bootstrap, health, and graceful drain helpers.

Applications provide:

- Actor protocols.
- Actor behavior.
- Domain state schemas.
- Persistence configuration.
- Business acknowledgements and idempotency rules.
- External binary protocol adapters.
- Deployment manifests and security policy choices.

### 6.2 Runtime Boundary

The runtime owns scheduling, mailboxes, actor tasks, timers, and dispatchers. User actor code owns domain logic but must not block default dispatchers or mutate state shared with other actors.

### 6.3 Cluster Boundary

Rakka internal remoting runs between trusted cluster nodes. External clients use HTTP/gRPC ingress or explicitly configured client protocols. Internal remoting is not a public API.

### 6.4 Persistence Boundary

The framework controls persistence ordering and revision checks. The storage backend controls durability, replication, backup, and disaster recovery. Rakka must expose health and lag information but cannot make a weak store strong.

### 6.5 Kubernetes Boundary

Kubernetes schedules pods, restarts failed processes, provides service discovery, and enforces resource/security policy. Rakka manages actor placement inside the cluster and must coordinate with Kubernetes lifecycle events.

### 6.6 External Process Boundary

The process actor is the sole owner of the child process. Other actors and HTTP/gRPC handlers interact with the process only through the owning actor protocol.

## 7. Kubernetes Architecture

Recommended deployment:

- `StatefulSet` or carefully configured `Deployment` for Rakka nodes.
- Headless service for Rakka remoting and management contact points.
- Separate service or ingress for public HTTP/gRPC.
- Kubernetes API or DNS discovery for bootstrap.
- Pod readiness remains false until the node has joined the cluster and required shards/services are available.
- Pre-stop hook triggers cluster leave and shard handoff.
- PodDisruptionBudget protects quorum and shard availability.
- Horizontal autoscaling uses actor mailbox depth, shard pressure, CPU, memory, and request latency metrics.

Networking:

- Actor remoting uses pod-to-pod addressing.
- Do not load-balance actor remoting through a normal Kubernetes Service.
- Service meshes may be used for public HTTP/gRPC if internal remoting can bypass or be configured for peer identity.

Rolling update behavior:

1. Pod receives termination notice.
2. Rakka marks node draining.
3. New shard allocations avoid the draining node.
4. Existing shards passivate or hand off.
5. Process actors receive graceful shutdown.
6. Node leaves cluster.
7. ActorSystem terminates and pod exits.

Compatibility requirement:

- During Kubernetes rolling updates, Rakka applications must support N/N+1 message compatibility so old and new nodes can coexist safely while pods are replaced.
- Remote protocol changes should be additive within a rolling window.
- Incompatible protocol changes must use an explicit migration path, such as a compatibility bridge, staged deployment, cluster drain, or separate cluster.

## 8. Use Cases

### 8.1 Stateful Domain Entity

An order, cart, device, account, or workflow is represented as a durable sharded actor. Clients use `EntityRef<OrderCommand>` with an entity id. The entity may move across pods but recovers state by `PersistenceId`.

### 8.2 Legacy Binary as a Cluster Service

A third-party renderer, scientific model, codec, parser, or rules engine is wrapped by a process actor. The actor owns the binary, converts typed actor commands into the binary protocol, streams IO with back-pressure, supervises crashes, and exposes the result through gRPC or HTTP.

### 8.3 Elastic Worker Pool

Stateless actors register under a service key. Group routers distribute work across reachable nodes. Pool routers handle local parallelism. Consistent hashing keeps related work sticky where useful.

### 8.4 Streaming Ingestion

An HTTP or gRPC stream feeds a Rakka stream pipeline. The stream stages batch, transform, and send commands to sharded entities while preserving bounded buffers and back-pressure.

### 8.5 Durable Workflow

A workflow actor persists state after each accepted command, sends side effects through a durable outbox, and resumes from the latest state after crash or relocation.

### 8.6 Kubernetes Scale-Out

The cluster starts with three pods, scales to dozens, rebalances shards, and routes entity messages by identity throughout pod churn.

## 9. Acceptance Criteria

### 9.1 Rust Safety and Footprint

- Core runtime, actor APIs, sharding, and persistence APIs compile on stable Rust.
- Public framework APIs do not require users to write `unsafe`.
- Any internal `unsafe` is isolated, documented, reviewed, and covered by tests.
- A minimal actor service builds as a small static or mostly static container image suitable for Kubernetes.
- Local actors process messages without user-managed locks for actor-owned state.

### 9.2 Actor Runtime

- Users can define a typed actor protocol and spawn local actors.
- Actors process one message at a time.
- `tell`, `ask`, timers, child spawning, watching, stopping, and dead letters are implemented.
- Bounded mailboxes expose full-mailbox behavior deterministically.
- Supervision supports resume, restart, stop, escalate, backoff, and retry limits.

### 9.3 Cluster and Routing

- A Rakka cluster can form automatically in Kubernetes with at least three pods.
- A caller can send to `EntityRef<T>` from any pod and reach the owning actor regardless of pod placement.
- When a pod is killed, affected sharded entities are restarted on surviving pods and recover durable state.
- When pods are added, shards rebalance without caller address changes.
- Internal remoting uses direct pod addressing, not load-balanced service addresses.
- Cluster membership, unreachable detection, downing, and split-brain policy are observable.

### 9.4 Durability and Resilience

- A durable actor persists its state before processing the next command.
- A durable actor recovers the latest committed state after actor restart, pod restart, or shard relocation.
- Concurrent activation of the same persistence id is prevented through shard ownership and storage fencing.
- Store write conflicts, unavailable stores, and revision mismatches surface as supervised failures or typed command failures.
- Durable actor tests can run against an in-memory store and at least one production-grade store plugin.

### 9.5 External Binary Actors

- A process actor can launch a configured executable with explicit args, env, cwd, and IO mode.
- The actor can expose the process as a typed actor protocol and as HTTP/gRPC service endpoints.
- Process stdout/stderr are captured with correlation to actor/entity identity.
- Startup, readiness, health check, graceful shutdown, timeout, crash, and restart are tested.
- If the process crashes while handling a request, the caller receives a typed failure or timeout and the actor applies its supervision policy.
- A process-backed entity remains routable by logical id after pod failure and recovers state before restarting the process.

### 9.6 Streams, HTTP, and gRPC

- Streams provide bounded buffers and propagate back-pressure from consumers to producers.
- HTTP routes can call actors and stream request/response bodies.
- gRPC services can map unary and streaming RPCs to actor protocols and Rakka streams.
- External APIs remain separate from internal remoting protocols.

### 9.7 Kubernetes Operations

- Readiness only succeeds after cluster join and required service registration.
- Liveness detects stuck runtime conditions without killing pods during ordinary shard rebalancing.
- Pre-stop drain moves or passivates shards before pod termination when time allows.
- Rolling updates preserve durable state and do not require clients to know actor locations.
- Rolling updates require N/N+1 remote message compatibility while old and new pods coexist.
- Metrics include node membership, actor counts, mailbox depth, shard ownership, persistence latency, remoting failures, process exits, and request latency.

## 10. Suggested Crate Boundaries

- `rakka-core`: actor refs, actor system, behavior traits, mailboxes, supervision, timers.
- `rakka-remote`: transport, serialization registry, codecs, remote refs, message envelopes, TLS/mTLS.
- `rakka-cluster`: membership, failure detection, bootstrap, split-brain policy.
- `rakka-sharding`: entity refs, shard coordinator, shard regions, passivation, rebalancing.
- `rakka-persistence`: durable state APIs, store traits, in-memory test store.
- `rakka-persistence-postgres`: PostgreSQL-compatible durable state plugin.
- `rakka-workflow`: durable inbox/outbox, retries, deduplication, and workflow reliability patterns.
- `rakka-stream`: bounded stream primitives and actor interop.
- `rakka-http`: HTTP integration adapters.
- `rakka-grpc`: protobuf/gRPC integration adapters.
- `rakka-process`: external binary actor runtime.
- `rakka-k8s`: Kubernetes discovery, health checks, drain hooks, manifests/examples.
- `rakka-testkit`: actor tests, cluster simulation, process actor harnesses.

## 11. Initial Delivery Plan

Phase 1: Local actor kernel

- Typed actors, actor refs, mailboxes, supervision, timers, watching, testkit.

Phase 2: Durable state

- Durable actor API, in-memory store, PostgreSQL store, recovery tests, revision fencing.

Phase 3: Cluster routing

- Remote transport, Kubernetes/local discovery, membership, entity refs, sharding, failover.

Phase 4: External process actors

- Process lifecycle, protocol adapters, IO streams, supervision, process-backed entity examples.

Phase 5: Integration surfaces

- HTTP and gRPC adapters, streaming, metrics, tracing, Kubernetes manifests.

## 12. Resolved Decisions

- Shard coordination is owned by Rakka cluster internals. Kubernetes supports discovery, pod lifecycle, and health integration, while Rakka decides shard ownership, rebalancing, and entity placement. Durable persistence fencing still protects stateful entities from concurrent activation during failover or partition recovery.
- Rakka v1 is Tokio-only. The framework will use Tokio as the async runtime for actor execution, timers, networking, process IO, HTTP, gRPC, and test support. Runtime abstraction is not part of the v1 scope.
- Protobuf is the default remote message format, and Rakka includes a pluggable serialization registry from v1. All remote schemas must be versioned, and incompatible versions fail closed with typed transport errors.
- Core actor delivery is at-most-once. Durable inbox/outbox, retries, deduplication, and workflow reliability live in opt-in modules built on top of the actor, sharding, and persistence layers.
- Process actors run child processes inside Rakka node containers for v1. Per-actor sidecar containers are deferred as a future extension for workloads needing stronger isolation or independent container images.
- Rakka applications must maintain N/N+1 message compatibility during Kubernetes rolling updates. Remote protocol changes should be additive within a rolling window, and incompatible changes require an explicit migration path.

## 13. Open Questions

- None for this draft.

## 14. Research Sources

- [Akka General Concepts](https://doc.akka.io/libraries/akka-core/current/general/index.html)
- [Akka Actor Systems](https://doc.akka.io/libraries/akka-core/current/general/actor-systems.html)
- [Akka What is an Actor?](https://doc.akka.io/libraries/akka-core/current/general/actors.html)
- [Akka Supervision and Monitoring](https://doc.akka.io/libraries/akka-core/current/general/supervision.html)
- [Akka Actor References, Paths and Addresses](https://doc.akka.io/libraries/akka-core/current/general/addressing.html)
- [Akka Location Transparency](https://doc.akka.io/libraries/akka-core/current/general/remoting.html)
- [Akka Message Delivery Reliability](https://doc.akka.io/libraries/akka-core/current/general/message-delivery-reliability.html)
- [Akka Typed Actors](https://doc.akka.io/libraries/akka-core/current/typed/index.html)
- [Akka Typed Interaction Patterns](https://doc.akka.io/libraries/akka-core/current/typed/interaction-patterns.html)
- [Akka Typed Fault Tolerance](https://doc.akka.io/libraries/akka-core/current/typed/fault-tolerance.html)
- [Akka Typed Actor Discovery](https://doc.akka.io/libraries/akka-core/current/typed/actor-discovery.html)
- [Akka Typed Routers](https://doc.akka.io/libraries/akka-core/current/typed/routers.html)
- [Akka Cluster](https://doc.akka.io/libraries/akka-core/current/typed/index-cluster.html)
- [Akka Cluster Usage](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html)
- [Akka Cluster Membership](https://doc.akka.io/libraries/akka-core/current/typed/cluster-membership.html)
- [Akka Cluster Sharding](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html)
- [Akka Cluster Sharding Concepts](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding-concepts.html)
- [Akka Durable State](https://doc.akka.io/libraries/akka-core/current/typed/durable-state/persistence.html)
- [Akka Streams Motivation](https://doc.akka.io/libraries/akka-core/current/stream/stream-introduction.html#motivation)
- [Akka HTTP Introduction](https://doc.akka.io/libraries/akka-http/current/introduction.html)
- [Akka gRPC Overview](https://doc.akka.io/libraries/akka-grpc/current/overview.html)
- [Akka gRPC Why gRPC?](https://doc.akka.io/libraries/akka-grpc/current/whygrpc.html)
- [Akka Cluster Bootstrap](https://doc.akka.io/libraries/akka-management/current/bootstrap/index.html)
- [Akka Kubernetes Cluster Formation](https://doc.akka.io/libraries/akka-management/current/kubernetes-deployment/forming-a-cluster.html)
- [Akka Kubernetes via DNS](https://doc.akka.io/libraries/akka-management/current/bootstrap/kubernetes.html)
