# Rakka Phase 5 Continuation Plan

## Purpose

This file is the working slice outline for Phase 5: streams, HTTP/gRPC integration, and Kubernetes operations. The v1 implementation plan defines the destination; this file defines the order of Phase 5 slices and the acceptance criteria for each one.

Working note: as we move forward, follow this slice outline from this file. Before starting a Phase 5 slice, read this file, implement only the next agreed slice, then update this file if scope or status changes.

## Phase 5 Evaluation

Phase 5 turns the Phase 1-4 runtime into public integration surfaces:

- `rakka-stream`: bounded, cancellation-aware, back-pressure-aware stream primitives and adapters.
- `rakka-http`: HTTP routes that call actors, sharded entities, service handlers, and streams.
- `rakka-grpc`: tonic/protobuf adapters that map unary and streaming RPCs to actor protocols and Rakka streams.
- `rakka-k8s`: Kubernetes health, readiness, liveness, drain, metrics, manifests, and example deployments.

Ordering matters. Stream primitives should land before HTTP/gRPC streaming. Unary HTTP/gRPC adapters should land before full streaming adapters. Kubernetes readiness and drain should be built on top of existing cluster/sharding lifecycle hooks rather than inventing a separate placement authority.

Important boundary decisions:

- HTTP and gRPC are external integration layers. Internal Rakka remoting remains a separate trusted-cluster protocol.
- Rakka owns shard placement and handoff. Kubernetes provides discovery, pod lifecycle signals, scheduling, and health integration.
- Core actor delivery remains at-most-once. Durable workflow reliability remains opt-in through `rakka-workflow`.
- Process actors still run child processes inside Rakka node containers for v1. Per-actor Kubernetes sidecars remain future work.
- Phase 5 should avoid becoming a general web framework, service mesh, metrics backend, Helm chart platform, or cloud-specific autoscaling system.

Risk note: full multi-pod Kubernetes examples require production-grade remote transport beyond deterministic in-memory transport. If that transport is not available when Kubernetes slices begin, either add the smallest required transport hardening as a prerequisite slice or gate true kind/minikube tests behind an explicit environment variable.

## Current Baseline

The following foundations are already in place:

- `rakka-core` has typed actors, bounded mailboxes, `tell`, `ask`, timers, child spawning, stopping, dead letters, watching, supervision, and a minimal metrics recorder boundary.
- `rakka-persistence` and `rakka-persistence-postgres` provide durable state, revision fencing, in-memory storage, and PostgreSQL storage.
- `rakka-remote`, `rakka-cluster`, and `rakka-sharding` provide envelope serialization, membership, discovery foundations, sharding, handoff, passivation, compatibility policies, and deterministic in-memory remote routing.
- `rakka-process` provides supervised child process ownership, protocol modes, process-backed entities, and a process testkit.
- `rakka-workflow` provides durable inbox/outbox, deduplication, retry, recovery, and telemetry events.
- `rakka-stream` exists as a crate stub with subsystem metadata and a default buffer capacity constant.
- `rakka-http` exists as a crate stub with subsystem metadata and default readiness/liveness path constants.
- `rakka-grpc` exists as a crate stub with subsystem metadata and a tonic `Status` result alias.
- `rakka-k8s` has Kubernetes pod identity and DNS discovery foundations for headless service pod discovery.

## Phase 5 Done Definition

Phase 5 should be considered complete when Rakka can demonstrate external HTTP/gRPC ingress and streaming ingestion driving actors/entities/workflows, and can show Kubernetes deployment behavior with health, drain, and metrics boundaries.

Minimum completion criteria:

- Streams provide bounded buffers, back-pressure, cancellation, graceful drain, actor interop, entity interop, process IO interop, and tests for overflow and cancellation.
- HTTP adapters can route requests to actor refs, entity refs, service handlers, and stream bodies while keeping route code outside actor state.
- HTTP adapters support JSON, binary bodies, request/response streaming, SSE, and WebSocket foundations.
- gRPC adapters can map generated protobuf unary calls and streaming calls to actors, entities, and Rakka streams.
- External API error mapping is typed and separate from internal remoting errors.
- Kubernetes readiness only succeeds after cluster join and required service registration.
- Kubernetes liveness avoids killing pods during ordinary rebalance or graceful drain.
- Pre-stop drain marks the node draining, avoids new shard ownership, hands off or passivates shards, shuts down process actors, and leaves the cluster when time allows.
- Metrics include node membership, actor counts, mailbox depth, shard ownership, persistence latency, remoting failures, process exits, stream pressure, and request latency.
- Example deployments and docs show HTTP gateway, gRPC service, streaming ingestion, and Kubernetes operation boundaries.

## Phase-Level Out of Scope

- Turning `rakka-http` into a full web framework.
- AuthN/AuthZ, OAuth/OIDC, TLS certificate management, or API gateway policy.
- Service mesh replacement or public exposure of internal remoting.
- Exactly-once external side effects.
- Per-actor Kubernetes sidecar containers.
- Cloud-specific autoscaling controllers.
- Production Helm chart lifecycle management beyond reviewable example manifests.
- Non-Tokio runtimes.

## Slice 5A: Stream Core Model and Backpressure Primitives

Status: implemented.

Goal: define the public `rakka-stream` vocabulary that HTTP, gRPC, process IO, and actor adapters will share.

Scope:

- Add typed stream error/result types.
- Add bounded source/sink/channel primitives with explicit capacity.
- Add stream item, completion, cancellation, drain, and closed-state semantics.
- Add producer/consumer back-pressure behavior using Tokio channels or equivalent bounded primitives.
- Add graceful drain APIs that stop accepting new items while allowing buffered items to flush.
- Add basic stream telemetry labels for pressure, cancellation, completion, and dropped items.

Acceptance criteria:

- A producer blocks or receives a typed back-pressure/full result when a bounded stream is full.
- A consumer can cancel and upstream producers observe cancellation.
- Drain completes buffered items and then closes the stream.
- Closing a stream wakes pending senders/receivers with typed errors.
- Tests cover bounded capacity, cancellation propagation, drain, and close behavior.

Out of scope:

- HTTP body adapters.
- gRPC streaming adapters.
- Process stdin/stdout adapters.

Implementation notes:

- Added `StreamError`, `StreamResult`, `StreamSendError`, `StreamLifecycle`, `StreamStatus`, and stable telemetry labels in `rakka-stream`.
- Added `BoundedStream`, `StreamSink`, `StreamSource`, and `bounded_channel` with explicit capacity, blocking send, non-blocking try-send, drain, close, and cancellation.
- Implemented close/cancel wakeups for pending producers and consumers, with send failures returning ownership of rejected items.
- Covered bounded capacity, cancellation propagation, graceful drain, forced close, pending wakeups, and invalid capacity in stream core tests.
- Continue with Slice 5B for actor/entity/process IO adapters that build on these primitives.

## Slice 5B: Actor, Entity, and Process IO Stream Adapters

Status: implemented.

Goal: connect stream primitives to the runtime pieces that already exist.

Scope:

- Add actor source and actor sink adapters.
- Add `ActorRef<M>` sink behavior with typed send failures and cancellation.
- Add `EntityRef<M>` sink behavior through `ShardRegion`.
- Add process stdin/stdout/stderr stream adapters on top of `rakka-process` pipes and protocol boundaries.
- Add stream fan-in/fan-out helpers where bounded buffers prevent mailbox overflow.
- Preserve actor ordering guarantees per adapter instance.

Acceptance criteria:

- A stream source can feed an actor sink without exceeding configured buffer capacity.
- An actor or entity sink surfaces stopped actor, missing owner, full mailbox, and cancellation failures.
- Process stdout/stderr can be consumed as bounded streams without corrupting existing stdio/line-json protocols.
- Cancelling a process IO stream closes the relevant pipe or returns a typed unsupported-operation error when ownership belongs to a protocol actor.
- Tests cover actor sink back-pressure, entity sink routing, process EOF, process read error, and cancellation.

Out of scope:

- Public HTTP streaming.
- Public gRPC streaming.
- Exactly-once message delivery.

Implementation notes:

- Added actor adapters: `ActorSink`, `ActorSinkError`, and `spawn_actor_source`/`ActorSource` for actor-to-stream ingress.
- Added entity adapters: `EntitySink` and `EntitySinkError` over `ShardRegion`/`EntityRef` with message-preserving no-route and delivery failures.
- Added stream pipe helpers: `pipe_stream`, `fan_in_streams`, and `broadcast_stream`.
- Added process IO adapters: bounded stdout/stderr byte streams, stdin byte sink, managed-process helpers, output pump cancellation, read/write error mapping, and typed unsupported-owner errors for protocol-actor-owned pipes.
- Covered actor source, actor sink ordering, mailbox full/closed, entity no-route/delivery, process EOF, process read error, process cancellation, stdin drain, and protocol-owned pipe rejection in stream adapter tests.
- Continue with Slice 5C for HTTP server and unary adapter foundations.

## Slice 5C: HTTP Server and Unary Adapter Foundations

Status: implemented.

Goal: provide a small HTTP integration toolkit that routes external requests into Rakka protocols without making actors depend on HTTP.

Scope:

- Choose the v1 HTTP server primitive from the Rust ecosystem and document the choice.
- Add HTTP error/result types and status mapping.
- Add route adapters for service handlers.
- Add route adapters for `ActorRef<M>` unary ask/tell patterns.
- Add route adapters for `EntityRef<M>` unary ask/tell patterns through `ShardRegion`.
- Support JSON and binary request/response payloads.
- Add request timeout, payload limit, and graceful shutdown configuration.

Acceptance criteria:

- A test HTTP route calls an actor and returns a JSON response.
- A test HTTP route calls a sharded entity and returns a JSON response.
- Binary payloads round trip through a service handler route.
- Actor/entity timeout maps to an HTTP timeout status without panicking.
- Payload limit failures map to a typed HTTP error.
- Route adapters do not require actor state to know about HTTP request types.

Out of scope:

- WebSocket.
- SSE.
- Streaming request/response bodies.
- AuthN/AuthZ.

Implementation notes:

- Chose Axum 0.7 on Tokio/Hyper as the v1 HTTP server primitive because it composes with Tower tests, keeps request parsing outside actor state, and remains a thin adapter layer rather than a full framework commitment.
- Added `HttpRouteConfig`, `HttpServerConfig`, Axum serve helper, request timeout, payload limit, and graceful shutdown configuration.
- Added typed `HttpError`/`HttpResult` with status mapping and JSON error bodies.
- Added unary JSON route adapters for service handlers, actor ask/tell, and entity ask/tell through `ShardRegion`.
- Added unary binary service route support for byte payloads.
- Covered actor JSON ask, entity JSON ask, binary round trip, actor timeout to gateway timeout, payload limit to typed error, and actor/entity tell acceptance in HTTP tests.
- Continue with Slice 5D for HTTP streaming, SSE, and WebSocket foundations.

## Slice 5D: HTTP Streaming, SSE, and WebSocket Foundations

Status: planned.

Goal: build public HTTP streaming adapters on top of `rakka-stream`.

Scope:

- Add request body stream adapter.
- Add response body stream adapter.
- Add SSE adapter for server-pushed events from Rakka streams.
- Add WebSocket message stream foundations.
- Propagate client disconnect as stream cancellation.
- Propagate stream errors into HTTP responses or close frames according to adapter type.
- Add graceful server drain behavior for in-flight streaming responses.

Acceptance criteria:

- HTTP request bodies feed bounded streams with back-pressure.
- A Rakka stream can produce a streaming HTTP response.
- SSE emits ordered events and closes cleanly on stream completion.
- WebSocket inbound/outbound messages can bridge to a stream pipeline.
- Client disconnect cancels upstream work.
- Tests cover back-pressure, disconnect, stream error mapping, and graceful drain.

Out of scope:

- Full web framework routing DSL.
- Browser-oriented session/state features.

## Slice 5E: gRPC Unary Adapter Foundations

Status: planned.

Goal: map generated protobuf unary RPCs to actors, entities, and service handlers.

Scope:

- Add gRPC adapter traits around tonic generated service types.
- Add unary service handler adapter.
- Add unary `ActorRef<M>` ask/tell adapter with typed request/response conversion.
- Add unary `EntityRef<M>` adapter through `ShardRegion`.
- Map Rakka errors to `tonic::Status` consistently.
- Respect gRPC deadlines and cancellation.
- Preserve Protobuf schema/version compatibility guidance for rolling updates.

Acceptance criteria:

- A generated unary gRPC service calls an actor and returns a protobuf response.
- A generated unary gRPC service calls a sharded entity and returns a protobuf response.
- Actor/entity timeout maps to `Status::deadline_exceeded`.
- Decode/validation failures map to typed client errors.
- Cancellation stops waiting for actor/entity replies.
- Tests cover unary success, timeout, cancellation, and error mapping.

Out of scope:

- Server, client, or bidirectional streaming RPCs.
- Code generation tooling beyond what tonic/prost already provide.

## Slice 5F: gRPC Streaming Adapter Foundations

Status: planned.

Goal: connect tonic streaming RPCs to `rakka-stream` pipelines.

Scope:

- Add server-streaming adapter from Rakka streams.
- Add client-streaming adapter into Rakka streams.
- Add bidirectional-streaming adapter with independent inbound/outbound cancellation.
- Map stream completion, cancellation, and errors to gRPC status/trailers.
- Apply bounded buffers between tonic streams and Rakka streams.
- Add deadline-aware drain behavior.

Acceptance criteria:

- Server streaming sends ordered items from a bounded Rakka stream.
- Client streaming applies back-pressure into a bounded stream pipeline.
- Bidirectional streaming handles independent read/write completion.
- Client cancellation cancels upstream work.
- Stream errors map to `tonic::Status` without leaking internal remoting details.
- Tests cover server streaming, client streaming, bidirectional streaming, back-pressure, deadline, and cancellation.

Out of scope:

- Generated API design guidelines beyond adapter examples.
- Public exposure of internal remoting envelopes.

## Slice 5G: Kubernetes Health, Readiness, Liveness, and Drain Hooks

Status: planned.

Goal: make Rakka node lifecycle visible and controllable from Kubernetes.

Scope:

- Add node health model for readiness and liveness.
- Readiness should depend on cluster join, compatibility acceptance, and required service registration.
- Liveness should detect stuck runtime conditions without firing during ordinary shard rebalance or drain.
- Add pre-stop drain API that marks the node draining, prevents new shard ownership, starts handoff/passivation, drains streams, stops process actors, and leaves the cluster when possible.
- Add drain timeout and partial-drain result types.
- Add integration hooks that HTTP readiness/liveness routes can call.

Acceptance criteria:

- Readiness is false before cluster join and true after required services are registered.
- Readiness becomes false when drain starts.
- Liveness remains true during normal rebalance and graceful drain.
- Drain avoids new shard ownership and initiates handoff/passivation for currently owned shards.
- Process actors receive graceful shutdown during drain.
- Tests cover join readiness, missing service readiness, normal rebalance liveness, drain readiness, and timeout/partial-drain results.

Out of scope:

- Kubernetes API watch controller.
- Helm charts.
- Cloud load-balancer integration.

## Slice 5H: Metrics, Tracing, and Operational Snapshots

Status: planned.

Goal: expose operational state needed for Kubernetes decisions and production debugging.

Scope:

- Extend metrics recording beyond the Phase 0 placeholder.
- Add metric names and attributes for node membership, actor counts, mailbox depth, shard ownership, persistence latency, remoting failures, process exits, stream pressure, HTTP latency, and gRPC latency.
- Add operational snapshot structs that tests and health endpoints can query without scraping logs.
- Add tracing spans for HTTP/gRPC request boundaries and stream pipelines.
- Keep metrics backend-neutral.

Acceptance criteria:

- Tests can record and inspect metrics using an in-memory recorder.
- Actor mailbox depth, shard ownership count, and process exit count can be observed.
- HTTP and gRPC adapters record request latency and error labels.
- Stream adapters record buffer pressure and cancellation counts.
- Operational snapshots can be serialized for diagnostics.

Out of scope:

- Prometheus exporter implementation unless needed for examples.
- OpenTelemetry exporter implementation.
- Dashboards.

## Slice 5I: Kubernetes Manifests and Local Cluster Example

Status: planned.

Goal: provide reviewable Kubernetes deployment examples and optional local-cluster tests.

Scope:

- Add example manifests for Rakka node `StatefulSet` or carefully configured `Deployment`.
- Add headless service for internal remoting/contact points.
- Add separate service for public HTTP/gRPC.
- Add readiness/liveness probes and pre-stop hook wiring.
- Add PodDisruptionBudget example.
- Add configuration examples for N/N+1 rolling updates.
- Add optional kind/minikube scenario for three pods, rolling update, pod kill, readiness/liveness, shard handoff, and process actor drain.

Acceptance criteria:

- Manifests are syntactically valid YAML and reference the documented readiness/liveness paths.
- DNS discovery config matches the headless service shape.
- Pre-stop hook calls the drain endpoint or command documented by Slice 5G.
- Example manifests document required ports and environment variables.
- Optional local-cluster tests are gated behind an explicit environment variable and skip cleanly by default.
- The local-cluster scenario demonstrates cluster formation, shard routing, pod termination drain, and N/N+1 compatibility.

Out of scope:

- Production Helm chart.
- Cloud-specific ingress, load balancer, or autoscaler resources.
- Per-actor sidecar containers.

## Slice 5J: End-to-End Examples, Testkit, and Documentation

Status: planned.

Goal: make Phase 5 behavior reviewable by humans and reusable in tests.

Scope:

- Add an HTTP gateway example that routes to an actor and a sharded entity.
- Add a gRPC service example that routes unary and streaming calls to actors/entities.
- Add a streaming ingestion example that batches or transforms input before sending entity commands.
- Add an example that exposes a process-backed service through HTTP or gRPC.
- Add testkit helpers for HTTP requests, gRPC unary calls, streaming assertions, Kubernetes health/drain assertions, and in-memory metrics inspection.
- Update README and docs with run commands, expected output, and reliability boundaries.
- Update this continuation plan with final Phase 5 status.

Acceptance criteria:

- Examples run with `cargo run` unless explicitly marked as Kubernetes-gated.
- Tests can exercise HTTP, gRPC, streams, health, drain, and metrics without host-specific external services.
- Docs explain public integration surfaces versus internal remoting.
- Docs explain how stream back-pressure, HTTP disconnects, gRPC cancellation, and Kubernetes drain interact.
- Phase 5 completion status is clear.

Out of scope:

- Public API stability guarantee beyond v1 draft expectations.
- Full application template generator.

## Suggested Next Slice

Continue with Slice 5A: stream core model and backpressure primitives. Streams are the lowest shared layer for HTTP streaming, gRPC streaming, process IO adapters, and ingestion examples, so they should land before public HTTP/gRPC streaming surfaces.
