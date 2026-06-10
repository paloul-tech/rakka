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
| `rakka-core` | V1 candidate | Typed actor runtime, actor refs, supervision options, paths, dead letters, telemetry names, `RakkaError`, metrics recorder traits, and Tokio runtime marker. |
| `rakka-persistence` | V1 candidate | Durable state store traits, in-memory store, durable actor helper, durable effects, revisions, and `DurableError`. |
| `rakka-persistence-postgres` | Adapter candidate | PostgreSQL durable state store, migration SQL, backend constants, and binary-state codec. |
| `rakka-cluster` | V1 candidate | Node identity, discovery snapshots/providers, membership state machine, protocol compatibility, and `ClusterError`. |
| `rakka-remote` | V1 candidate | Remote envelopes, serialization registry, request/reply correlation, in-memory transport, TCP remoting foundation, and `RemoteError`. |
| `rakka-sharding` | V1 candidate | Entity identity, shard coordinator, region routing, local and remote entity routes, cluster node runtime, and `ShardingError`/`ClusterShardingError`. |
| `rakka-process` | Adapter candidate | Managed process ownership, process actor runtime, stdio/file/socket/local-gRPC modes, process-backed entities, and `ProcessError`. |
| `rakka-workflow` | V1 candidate | Durable inbox/outbox model, workflow clocks, recovery, retry scheduling, and `WorkflowError`. |
| `rakka-stream` | V1 candidate | Bounded stream source/sink primitives, lifecycle snapshots, stream errors, telemetry labels, and optional adapters. |
| `rakka-http` | Adapter candidate | Axum-backed route adapters, HTTP server helpers, streaming adapters, public API compatibility constants, and `HttpError`. |
| `rakka-grpc` | Adapter candidate | Tonic-backed unary/streaming adapters, generated API compatibility constants, and `GrpcError`. |
| `rakka-k8s` | Adapter candidate | Pod identity, DNS discovery, readiness/liveness health, drain orchestration, manifest conventions, and Kubernetes metrics. |
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
```

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

## Current Boundary Decisions

- Keep re-exporting the most common types at each crate root for v1 ergonomics.
- Keep module namespaces public where they clarify ownership, for example `rakka_remote::registry` and `rakka_sharding::runtime`.
- Keep examples as separate unpublished packages.
- Keep `rakka-testkit` as the home for cross-crate compatibility fixtures instead of leaking those helpers into production crates.
- Do not promise final semver stability until Slice V1G release packaging/review.

## Review Notes By Crate

- `rakka-core`: public actor and metrics primitives are cohesive; no integration dependencies.
- `rakka-persistence`: durable store traits and in-memory implementation are narrow; PostgreSQL remains a separate plugin crate.
- `rakka-cluster`: protocol compatibility and membership admission are public and documented by the compatibility matrix.
- `rakka-remote`: both in-memory and TCP transports are public; TCP remains a v1 foundation but not a full production security story.
- `rakka-sharding`: local/remote routes and node runtime are public; durable shard coordinator storage remains out of scope.
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
