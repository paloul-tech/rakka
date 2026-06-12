# Rakka API Boundary Inventory

Status: Phase 0 draft
Date: 2026-06-11

## Boundary Tiers

| Tier | Meaning |
| --- | --- |
| Facade | Preferred application-facing API. These names should appear in examples and guides first. |
| Foundation | Public building blocks used by facades, advanced applications, and tests. Additive changes are preferred, but these APIs may still be reorganized behind facades before stability. |
| Adapter | Integration API for a specific runtime or edge boundary such as HTTP, gRPC, Kubernetes, PostgreSQL, TCP, or child processes. |
| Test/support | Public helpers intended for tests, examples, compatibility checks, and repository validation. |

## Phase 0 Ownership Decision

Rakka now has a top-level `rakka` facade crate. It owns:

- `rakka::prelude`, the curated import surface for application code.
- Module re-exports such as `rakka::actor`, `rakka::persistence`, and
  `rakka::sharding` for callers that want a single dependency but still need
  crate-specific APIs.

Component crates continue to own implementation-specific concepts:

- `rakka-core` owns the current local actor foundation.
- `rakka-persistence` owns durable state today and will own typed persistence
  foundations until a higher-level facade stabilizes.
- `rakka-cluster`, `rakka-remote`, and `rakka-sharding` own distributed
  foundations.
- `rakka-stream`, `rakka-process`, `rakka-http`, `rakka-grpc`, and `rakka-k8s`
  own integration adapters.
- `rakka-testkit` owns test/support helpers.

## Crate Inventory

| Crate | Tier | Phase 0 role |
| --- | --- | --- |
| `rakka` | Facade | Preferred application dependency and curated prelude. |
| `rakka-core` | Foundation | Actor runtime, actor refs, actor context, supervision, metrics, errors, and paths. |
| `rakka-persistence` | Foundation | Durable state actor API and store traits; future home for event-sourced behavior foundations. |
| `rakka-persistence-postgres` | Adapter | PostgreSQL durable-state backend and future journal/snapshot backend. |
| `rakka-cluster` | Foundation | Membership, node identity, discovery snapshots, and compatibility primitives. |
| `rakka-remote` | Foundation | Remote envelopes, serialization registry, request correlation, and transport traits. |
| `rakka-sharding` | Foundation | Entity identity, shard ownership, regions, local/remote routes, and node runtime. |
| `rakka-sharding-postgres` | Adapter | PostgreSQL durable shard coordinator store, leadership lease, and migration helpers. |
| `rakka-workflow` | Foundation | Durable inbox/outbox reliability primitives. |
| `rakka-stream` | Foundation | Bounded stream primitives and stream lifecycle semantics. |
| `rakka-process` | Adapter | Supervised child-process ownership and process-backed actors/entities. |
| `rakka-http` | Adapter | Axum-backed HTTP adapters. |
| `rakka-grpc` | Adapter | Tonic-backed gRPC adapters. |
| `rakka-k8s` | Adapter | Kubernetes health, drain, discovery, and manifest helpers. |
| `rakka-testkit` | Test/support | Cross-crate probes, assertions, compatibility fixtures, and repository hygiene tests. |

## Prelude Inventory

`rakka::prelude` intentionally contains common application primitives only:

- Core actors: `Actor`, `ActorRef`, `ActorContext`, `ActorSystem`, `Message`,
  `ReplyTo`, `actor_future`, `ActorFuture`, `ActorAction`, `ActorResult`, and
  common actor errors.
- Core runtime support: `RakkaError`, `RakkaResult`, supervision, termination,
  timers, and metrics recorder traits.
- Persistence basics when the `persistence` feature is enabled:
  `DurableActor`, `DurableActorContext`, `DurableEffect`, durable actor spawn
  helpers, durable state traits, `PersistenceId`, and `Revision`.
- Sharding facade and common configuration when the `sharding` feature is
  enabled: `ClusterSharding`, `Entity`, `EntityTypeKey`, `ShardedEntityRef`,
  `EntityType`, `EntityId`, `EntityRef`, `ShardingConfig`, shard allocation
  strategies, and durable coordinator store/lease hooks.
- Stream basics when the `stream` feature is enabled:
  `BoundedStream`, `StreamSink`, `StreamSource`, `StreamError`, and
  `StreamResult`.

The prelude does not expose low-level coordinator, route, TCP transport, remote
envelope, Kubernetes drain report, or node runtime types. Those remain available
through crate-specific modules until higher-level facades replace direct wiring.

## Compatibility Rules During Facade Migration

- Existing crate-level APIs remain available unless a later reviewed phase marks
  a breaking simplification.
- New examples should prefer `rakka::prelude` and `rakka::*` modules.
- Foundation APIs can be used in advanced examples when no facade exists yet,
  but the example should make the boundary explicit.
- Facade additions should be additive and covered by documentation tests or
  repository hygiene tests.
- A type should enter `rakka::prelude` only when it is expected to remain a
  stable application concept across later phases.

## Naming Conventions

- Prefer Akka-recognizable names where they map cleanly to Rust:
  `ActorSystem`, `ActorRef`, `ActorContext`, `Cluster`, `ClusterSharding`,
  `Entity`, `EntityRef`, `EntityTypeKey`, `PersistenceId`, `Source`, `Flow`,
  `Sink`, and `TestProbe`.
- Prefer Rust result-returning names where behavior differs from Akka's
  fire-and-forget API. For example, keep explicit mailbox pressure visible
  through `Result` or future `try_tell`/`send` naming.
- Keep transport, codec, route, and coordinator names out of first-path
  examples unless the example is specifically about those foundations.
