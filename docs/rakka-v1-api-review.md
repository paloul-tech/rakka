# Rakka V1 Public API Review

This note records the current public API boundary for the v1 hardening work. It is not a final semver promise; it is the review checklist for deciding what should become stable before a release candidate.

## Stability Tiers

| Tier | Meaning |
| --- | --- |
| V1 candidate | Intended to remain additive through the v1 release candidate unless review finds a concrete safety or usability issue. |
| Adapter candidate | Public integration surface whose wire/API contract must remain compatible during N/N+1 rolling updates. |
| Test/support | Public only to support integration tests, examples, or downstream validation. Production code should not rely on it without accepting churn. |
| Internal foundation | Public today because crates are small and reviewable, but still eligible for visibility tightening before v1. |

## Crate Map

| Crate | Tier | Public Boundary |
| --- | --- | --- |
| `rakka` | V1 candidate | Top-level facade crate, curated `rakka::prelude`, and feature-gated module re-exports for application code. |
| `rakka-core` | V1 candidate | Typed actor runtime, actor refs, supervision options, paths, dead letters, telemetry names, `RakkaError`, metrics recorder traits, and Tokio runtime marker. |
| `rakka-persistence` | V1 candidate | Durable state store traits, in-memory store, durable actor helper, durable effects, revisions, and `DurableError`. |
| `rakka-persistence-postgres` | Adapter candidate | PostgreSQL durable state store, migration SQL, backend constants, and binary-state codec. |
| `rakka-cluster` | V1 candidate | Node identity, discovery snapshots/providers, membership state machine, protocol compatibility, and `ClusterError`. |
| `rakka-remote` | V1 candidate | Remote envelopes, serialization registry, request/reply correlation, in-memory transport, TCP remoting foundation, and `RemoteError`. |
| `rakka-sharding` | V1 candidate | Entity identity, shard coordinator, region routing, remembered entities, local and remote entity routes, cluster node runtime, and `ShardingError`/`ClusterShardingError`. |
| `rakka-sharding-postgres` | Adapter candidate | PostgreSQL shard coordinator store, lease, remembered entity store, migration SQL, backend constants, namespace-isolated coordinator snapshots, and fencing tokens. |
| `rakka-process` | Adapter candidate | Managed process ownership, process actor runtime, stdio/file/socket/local-gRPC modes, process-backed entities, and `ProcessError`. |
| `rakka-workflow` | V1 candidate | Durable inbox/outbox model, workflow clocks, recovery, retry scheduling, and `WorkflowError`. |
| `rakka-discovery-etcd` | Adapter candidate | etcd external-arbiter discovery provider: leased node registration, prefix-ranged peer discovery, and the strongly consistent membership that deterministic shard allocation relies on. |
| `rakka-agent-workflow` | V1 candidate | Compiled execution IR, durable graph run state, deterministic graph scheduler, outbox effect bridge, dispatcher fleet and claim filters, timers, triggers, runtime events, OTLP bridge, and the agent-dispatch bounds. Outside the publishable crate set until it enters it (see `docs/rakka-v1-release-packaging.md`). |
| `rakka-agent` | V1 candidate | Agent, task, run, team, and conversation entities, exchange choreography, loop runtime, model adapter trait (`rig` feature), effects and tool authority, budgets, admission, guardrails, checkpoints, goals and wakes, delegation and coordination, memory traits, telemetry (`otel` feature), operational queries, `AgentSchemaPolicy`, and the deterministic testkit. Outside the publishable crate set until it enters it. |
| `rakka-agent-postgres` | Adapter candidate | PostgreSQL session, snapshot, and private-memory stores, the pgvector retriever, and crate-owned migrations for the agent memory contracts. Outside the publishable crate set until it enters it. |
| `rakka-agent-knowledge-graph` | V1 candidate | Communal claims with provenance and the trust lattice, append-only transitions, the promotion gate, the portable store SPI, the in-memory reference store, and the backend conformance harness. Outside the publishable crate set until it enters it. |
| `rakka-agent-knowledge-graph-postgres` | Adapter candidate | PostgreSQL binding of the knowledge-graph store SPI with compare-and-set transitions and migrations. Outside the publishable crate set until it enters it. |
| `rakka-stream` | V1 candidate | Bounded stream source/sink primitives, lifecycle snapshots, stream errors, telemetry labels, and optional adapters. |
| `rakka-http` | Adapter candidate | Axum-backed route adapters, HTTP server helpers, streaming adapters, public API compatibility constants, and `HttpError`. |
| `rakka-grpc` | Adapter candidate | Tonic-backed unary/streaming adapters, generated API compatibility constants, and `GrpcError`. |
| `rakka-k8s` | Adapter candidate | Pod identity, DNS discovery, readiness/liveness health, drain orchestration, manifest conventions, and Kubernetes metrics. |
| `rakka-a2a` | Adapter candidate | A2A command mapping, task projection store trait with memory/PostgreSQL implementations, durable task-event streaming replay and watcher, builder-based durable A2A `RequestHandler`, sharded run owner host and router, push config store and dispatch boundary, dynamic agent card, route composition, and `RakkaA2AHandlerError`. |
| `rakka-testkit` | Test/support | Integration helpers, surface assertions, metric/drain helpers, and compatibility fixtures. |

## Feature Boundaries

The default workspace build keeps examples and compatibility tests easy to run, but v1 users should be able to opt out of optional integration layers:

- `rakka-stream` now has default features `adapters` and `process-io`.
- `rakka-stream --no-default-features` compiles the stream core without `rakka-sharding` or `rakka-process`.
- `rakka-http`, `rakka-grpc`, `rakka-k8s`, and `rakka-testkit` depend on stream core with `default-features = false` unless they intentionally need adapters.
- `rakka-process` exposes `testkit` behind a default-enabled feature. Production users can disable defaults to hide those helpers.

Review command:

```sh
cargo check -p rakka-stream --no-default-features
cargo check -p rakka-process --no-default-features
cargo check -p rakka-a2a --no-default-features
```

`rakka-agent` ships `default = ["rig"]` and builds and tests with
`--no-default-features` (the validation script enforces it); `otel` gates the
GenAI convention mapping and adds no dependency. The `rakka` facade exposes them
as `agent`, `agent-rig`, and `agent-otel`, making Rig opt-in at the facade.

`rakka-a2a` ships `default = []`; every adapter surface is opt-in through the
`server`, `sharding`, `postgres`, `http`, `k8s`, `otel`, `testkit`, and
`agents` features, exposed through the gated `rakka` facade features `a2a`,
`a2a-server`, `a2a-sharding`, `a2a-postgres`, `a2a-http`, `a2a-k8s`,
`a2a-otel`, `a2a-testkit`, and `a2a-agents`. `otel` gates the OpenTelemetry GenAI convention mapping for the
A2A edge — the ingress `SERVER` span the agent domain's adapter defers to the
protocol adapter — and activates `rakka-agent/otel`. It deliberately does not
gate trace-context propagation: extraction on ingress and injection on egress
are unconditional on every path, because gating them would remove trace
continuity from a default build.

## Error Codes

Rakka error types should expose stable machine-readable codes through `code()` when they are part of a public runtime or adapter boundary. `into_rakka_error()` converts typed crate errors to `RakkaError { subsystem, code, message }`.

Operator/user-facing adapter errors:

- `HttpError::code()` plus HTTP status mapping.
- `GrpcError::code()` plus tonic status and metadata mapping.
- `ProcessError::code()` for child-process configuration, lifecycle, protocol, sandbox, and endpoint failures.
- `KubernetesNodeHealth` exposes compatibility/readiness failures through readiness reasons and metrics rather than a single error enum.

Internal/runtime errors:

- `ClusterError::code()` covers membership, discovery, and protocol admission.
- `RemoteError::code()` covers codec, schema, envelope, and serialization failures.
- `ShardingError::code()` covers identity, owner-cache, and routing failures.
- `DurableError::code()` and `WorkflowError::code()` cover durable reliability boundaries.
- `StreamError::code()` covers bounded stream lifecycle and back-pressure failures.
- `RakkaA2AHandlerError::code()` covers A2A adapter mapping, projection, inbox/run-engine, push, owner-routing, draining, authorization, and lifecycle failures; the underlying `A2AMappingError`, `TaskProjectionError`, and `A2APushConfigError` codes are stable A2A adapter compatibility surfaces.

## Current Boundary Decisions

- Keep re-exporting the most common types at each crate root for v1 ergonomics.
- Add `rakka` as the preferred application-facing facade crate while component crates remain directly usable.
- Keep module namespaces public where they clarify ownership, for example `rakka_remote::registry` and `rakka_sharding::runtime`.
- Keep examples as separate unpublished packages.
- Keep `rakka-testkit` as the home for cross-crate compatibility fixtures instead of leaking those helpers into production crates.
- Do not promise final semver stability until Slice V1G release packaging/review.

## Review Notes By Crate

- `rakka`: facade crate added during Akka-parity Phase 0; `rakka::prelude` is intentionally curated and should not expose coordinator, route, transport, or adapter internals.
- `rakka-core`: public actor and metrics primitives are cohesive; no integration dependencies.
- `rakka-persistence`: durable store traits and in-memory implementation are narrow; PostgreSQL remains a separate plugin crate.
- `rakka-cluster`: protocol compatibility and membership admission are public and documented by the compatibility matrix.
- `rakka-remote`: both in-memory and TCP transports are public; TCP remains a v1 foundation but not a full production security story.
- `rakka-sharding`: local/remote routes and node runtime are public; durable shard coordinator storage, remembered entities, and leadership are opt-in through store and lease traits.
- `rakka-sharding-postgres`: PostgreSQL shard coordinator snapshots, leadership leases, and remembered entity identity are adapter scope and should track the sharding store/lease contracts without expanding the facade prelude.
- `rakka-process`: `testkit` is feature-gated; process sandbox defaults and allowlists should stay conservative.
- `rakka-workflow`: durable inbox/outbox is public; workflow engine orchestration beyond these primitives remains out of scope.
- `rakka-stream`: stream core can compile without process/sharding adapters.
- `rakka-http` and `rakka-grpc`: public adapter constants document N/N+1 API compatibility expectations.
- `rakka-k8s`: health, readiness, and drain are public; real Kubernetes client/watch integration remains future work.
- `rakka-testkit`: public helpers are intentionally test/support tier.

## Remaining Questions

- Should `rakka-http` continue re-exporting `axum::Router`, or should callers import Axum directly?
- Should `rakka-process::testkit` move entirely into `rakka-testkit` before v1, or is a feature-gated module acceptable?
- Which crates should receive stricter `#[doc(hidden)]` on lower-level module namespaces before release candidate?
- Which adapter crates should gain optional feature flags around concrete runtimes if alternative runtimes/transports are introduced after v1?
