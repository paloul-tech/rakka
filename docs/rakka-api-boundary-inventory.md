# Rakka API Boundary Inventory

Status: current through the agent domain's Phase 6 (slice 6.4)
Date: 2026-09-01 (first drafted 2026-06-11 as the Phase 0 boundary)

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
  foundations; `rakka-discovery-etcd` owns the etcd external-arbiter
  discovery adapter.
- `rakka-stream`, `rakka-process`, `rakka-http`, `rakka-grpc`, `rakka-k8s`, and
  `rakka-a2a` own integration adapters.
- `rakka-agent-workflow` owns the durable agent-workflow execution kernel;
  `rakka-agent` owns the durable agent domain built on it; `rakka-agent-postgres`
  owns that domain's PostgreSQL memory and retrieval adapters.
- `rakka-agent-knowledge-graph` owns the database-agnostic communal
  knowledge-graph domain, portable store SPI, and backend conformance harness;
  `rakka-agent-knowledge-graph-postgres` owns its PostgreSQL binding.
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
| `rakka-sharding` | Foundation | Entity identity, shard ownership, regions, local/remote routes, remembered entity settings/stores, and node runtime. |
| `rakka-sharding-postgres` | Adapter | PostgreSQL durable shard coordinator store, leadership lease, remembered entity store, and migration helpers. |
| `rakka-workflow` | Foundation | Durable inbox/outbox reliability primitives. |
| `rakka-discovery-etcd` | Adapter | etcd-backed external-arbiter discovery: leased self-registration, prefix-ranged peer discovery, and strongly consistent membership for clusters that scale at runtime. |
| `rakka-agent-workflow` | Foundation | Durable agent-workflow execution kernel: the product-neutral compiled execution IR, durable graph run state, the deterministic graph scheduler, the durable outbox effect bridge, the dispatcher fleet with claim filters, timers and triggers, runtime events, and the OTLP bridge. |
| `rakka-agent` | Foundation | Durable agent domain over that kernel: agent, task, run, team, and conversation entities and their exchange choreography, the durable loop runtime, the provider-neutral model adapter (Rig behind `rig`), effects and tool authority, escrow budgets, autonomy admission, guardrails, checkpoints, goals and the wake controller, delegation, handoff, and coordination, memory traits and retrieval, structured telemetry (GenAI mapping behind `otel`), bounded operational queries, and deterministic test support. |
| `rakka-agent-postgres` | Adapter | PostgreSQL bindings for the agent memory contracts: session memory, context snapshots, agent-private long-term records, the pgvector retriever, and idempotent migrations, run against the shared memory conformance suite behind `RAKKA_POSTGRES_TEST_DSN`. |
| `rakka-stream` | Foundation | Bounded stream primitives and stream lifecycle semantics. |
| `rakka-process` | Adapter | Supervised child-process ownership and process-backed actors/entities. |
| `rakka-http` | Adapter | Axum-backed HTTP adapters. |
| `rakka-grpc` | Adapter | Tonic-backed gRPC adapters. |
| `rakka-k8s` | Adapter | Kubernetes health, drain, discovery, and manifest helpers. |
| `rakka-a2a` | Adapter | A2A protocol adapter over HTTP, PostgreSQL, and sharding: durable request handler, task projection and streaming replay, push config persistence and dispatch boundary, sharded run owner host, owner router, and dynamic agent card. |
| `rakka-agent-knowledge-graph` | Foundation | Communal knowledge-graph claims with provenance and the `Proposed`/`Verified`/`Disputed`/`Retracted` trust lattice, append-only transitions, the checkpoint-grant promotion gate, the portable vendor-neutral store SPI with capability reporting, the in-memory reference store, and the backend conformance harness. |
| `rakka-agent-knowledge-graph-postgres` | Adapter | PostgreSQL relational binding of the communal knowledge-graph store SPI: claim, transition, and operation-ledger tables, compare-and-set trust transitions, migration helpers, and the DSN-gated conformance and durability suites. |
| `rakka-testkit` | Test/support | Cross-crate probes, assertions, compatibility fixtures, and repository hygiene tests. |

## Prelude Inventory

`rakka::prelude` intentionally contains common application primitives only:

- Core actors: `Actor`, `ActorRef`, `ActorContext`, `ActorSystem`, `Message`,
  `ReplyTo`, `actor_future`, `ActorFuture`, `ActorAction`, `ActorResult`, and
  common actor errors.
- Core runtime support: `RakkaError`, `RakkaResult`, supervision, termination,
  timers, and metrics recorder traits.
- Service discovery: `Receptionist`, `ServiceKey`, `Listing`, receptionist
  subscriptions, registration handles, local-only listing snapshots, propagated
  remote listing hooks, and typed receptionist errors.
- Local routing: `Routers`, `PoolRouter`, `GroupRouter`, pool and group router
  builders, round-robin, random, and consistent-hash routing strategies,
  receptionist-backed group routing snapshots, explicit no-routee behavior, and
  message-preserving router tell errors.
- Persistence basics when the `persistence` feature is enabled:
  `DurableActor`, `DurableActorContext`, `DurableEffect`, durable actor spawn
  helpers, durable state traits, `PersistenceId`, and `Revision`.
- Cluster facade and common configuration when the `cluster` feature is enabled:
  `Cluster`, `ClusterRuntime`, `ClusterSettings`, cluster manager and
  subscription types, failure/downing hooks, and clustered receptionist
  propagation facades.
- Sharding facade and common configuration when the `sharding` feature is
  enabled: `ClusterSharding`, `Entity`, `EntityTypeKey`, `ShardedEntityRef`,
  `EntityType`, `EntityId`, `EntityRef`, `ShardingConfig`, shard allocation
  strategies, remembered entity settings, and durable coordinator store/lease
  hooks.
- Stream basics when the `stream` feature is enabled:
  `BoundedStream`, `StreamSink`, `StreamSource`, `StreamError`, and
  `StreamResult`.
- A2A adapter (feature-gated, the only non-core re-exports): under `a2a`,
  `RakkaA2AHandlerError`; under `a2a-server`, `RakkaA2ABuildError`,
  `RakkaA2ARequestHandler`, `RakkaA2AService`, `RakkaA2AServiceBuilder`, and
  `RakkaA2ASettings`. Nothing from `rakka-agent`, `rakka-agent-workflow`, or
  the `agents` feature of `rakka-a2a` is re-exported here.

The prelude does not expose low-level coordinator, route, TCP transport, remote
envelope, Kubernetes drain report, or node runtime types. Those remain available
through crate-specific modules until higher-level facades replace direct wiring.

The agent surface is reached through feature-gated facade modules rather than
the prelude: `rakka::agent_workflow` (default feature `agent-workflow`),
`rakka::agent` (`agent`, with `agent-rig` and `agent-otel` passthroughs), and
`rakka::a2a` (`a2a`, with `a2a-server`, `a2a-sharding`, `a2a-postgres`,
`a2a-http`, `a2a-k8s`, `a2a-otel`, `a2a-testkit`, and `a2a-agents`). No
agent-domain type enters `rakka::prelude`; the A2A adapter's service types
listed under "A2A adapter" in the Prelude Inventory above are its only
feature-gated re-exports there. The surface is documented in
[`rakka-agents.md`](rakka-agents.md).

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
