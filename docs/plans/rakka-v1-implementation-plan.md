# Rakka V1 Implementation Plan

Status: Draft for review
Date: 2026-06-02

## Summary

Build Rakka as a Rust 2021 Cargo workspace implementing the architecture in [Rakka Actor Framework Specification](./rakka-actor-framework-spec.md). V1 targets Tokio, typed actors, bounded mailboxes, supervision, durable state, Rakka-owned cluster and shard coordination, Protobuf remote messaging with a pluggable serialization registry, Kubernetes operation, external child-process actors, and HTTP/gRPC adapters.

Implementation proceeds in five milestones. Each milestone should ship with examples, tests, and docs before the next begins.

## Public Interfaces and Crates

Create these workspace crates:

- `rakka-core`: `Actor`, `ActorRef<M>`, `ActorContext`, `ActorSystem`, bounded mailboxes, `tell`, `ask`, timers, watching, lifecycle, supervision.
- `rakka-persistence`: `DurableActor`, `PersistenceId`, `DurableStateStore`, revision fencing, in-memory store.
- `rakka-persistence-postgres`: PostgreSQL durable-state plugin.
- `rakka-remote`: remote envelopes, Protobuf default codec, codec registry, transport errors, TLS/mTLS-ready transport boundary.
- `rakka-cluster`: membership, failure detection, node lifecycle, Rakka-owned coordination.
- `rakka-sharding`: `EntityRef<M>`, entity type/id, shard id, shard region, shard coordinator, rebalancing, passivation.
- `rakka-workflow`: durable inbox/outbox, retry, deduplication, workflow reliability patterns.
- `rakka-stream`: bounded stream adapters for actors, process IO, HTTP, and gRPC.
- `rakka-process`: supervised child-process actors inside Rakka node containers.
- `rakka-http`: HTTP adapters for actor refs, entity refs, service handlers, and streams.
- `rakka-grpc`: gRPC adapters using generated Protobuf types and actor/stream bridges.
- `rakka-k8s`: Kubernetes discovery, readiness, liveness, pre-stop drain, metrics, manifests, and example deployments.
- `rakka-testkit`: actor tests, cluster simulation, process actor harnesses, probes, and assertions.

Use Tokio only for v1. Use Protobuf as the default remote format. Keep `rakka-core` delivery at-most-once; stronger reliability lives in `rakka-workflow`.

## Implementation Roadmap

### Phase 0: Workspace Foundation

- Scaffold the Cargo workspace, crate boundaries, shared error conventions, tracing, metrics traits, and examples.
- Set Rust 2021, stable Rust, Tokio, `tracing`, `serde`, `prost`, and `tonic` as baseline ecosystem choices.
- Add CI commands for `cargo fmt --check`, `cargo clippy`, `cargo test`, and crate docs.

### Phase 1: Local Actor Kernel

- Implement typed actors with sequential message handling, bounded mailboxes, `ActorRef<M>`, `tell`, `ask`, timers, child spawning, stopping, dead letters, and DeathWatch-style monitoring.
- Implement supervision strategies: resume, restart, stop, escalate, backoff, and retry budget.
- Add `rakka-testkit` utilities for spawning systems, probing messages, asserting dead letters, and testing restarts.

### Phase 2: Durable State

- Implement `DurableActor`, latest-state recovery, revision fencing, compare-and-set writes, deletes, and in-memory durable store.
- Add PostgreSQL plugin with schema migrations, revision checks, persistence latency metrics, and integration tests.
- Ensure durable actors do not process the next command until persistence succeeds.

### Phase 3: Remote, Cluster, and Sharding

- Implement remote envelopes with message type id, schema version, codec id, trace metadata, source, destination, and typed transport errors.
- Implement Protobuf default codec plus pluggable serialization registry.
- Implement cluster membership, Kubernetes/local discovery, failure detection, graceful leave, and Rakka-owned shard coordination.
- Implement `EntityRef<M>`, shard routing, shard ownership cache, handoff, passivation, failover, and rebalancing.
- Enforce N/N+1 remote message compatibility during rolling updates.

### Phase 4: Process Actors and Reliability Modules

- Implement `rakka-process` for supervised child processes with explicit args, env, cwd, stdin/stdout/stderr, health checks, startup/shutdown timeouts, restart policy, and crash telemetry.
- Support `stdio`, `line-json`, local `grpc`, TCP/Unix socket, file-watch, and one-shot modes.
- Implement `rakka-workflow` durable inbox/outbox, retries, deduplication keys, and workflow recovery on top of persistence and actors.
- Document per-actor Kubernetes sidecars as future work, not v1 behavior.

### Phase 5: Streams, HTTP/gRPC, and Kubernetes Ops

- Implement bounded stream adapters with cancellation, drain, actor source/sink interop, process IO interop, and gRPC streaming support.
- Implement HTTP adapters for routing requests to actor refs, entity refs, service handlers, and streams.
- Implement gRPC adapters using generated Protobuf types and actor/stream bridges.
- Implement Kubernetes helpers for discovery, readiness, liveness, pre-stop drain, metrics, manifests, and example deployments.

## Test Plan

- Unit tests: actor lifecycle, mailbox bounds, ask timeouts, timers, supervision, watching, dead letters, serialization registry, durable-state revision behavior.
- Integration tests: PostgreSQL persistence, actor restart recovery, remote send/ask, cluster formation, shard routing, failover, rebalancing, process actor crash/restart.
- Kubernetes tests: local kind/minikube scenario with three pods, rolling update, pod kill, readiness/liveness, shard handoff, N/N+1 message compatibility.
- Compatibility tests: old/new Protobuf schema coexistence, unknown compatible fields, incompatible schema fail-closed behavior.
- Example acceptance tests: stateful cart entity, durable workflow, external binary wrapper, HTTP gateway, gRPC service, streaming ingestion.

## Assumptions

- The first implementation target is the full v1 roadmap, not only Phase 1.
- Cargo workspace starts from the current mostly empty repository.
- Tokio is mandatory for v1.
- Protobuf is the default remote message format, with a pluggable registry from v1.
- Core actor delivery remains at-most-once.
- Durable reliability features are opt-in via `rakka-workflow`.
- Process actors run child processes inside Rakka node containers for v1.
- Rakka owns shard coordination internally; Kubernetes provides discovery, lifecycle, and health integration.
