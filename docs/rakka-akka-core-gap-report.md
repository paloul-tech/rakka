# Rakka vs Akka Core Gap Report

Status: First-pass architecture review
Date: 2026-06-11

## Scope

This review compares the current Rakka workspace against a local shallow clone of
`akka/akka-core`.

- Rakka commit reviewed: `aa997b9c1d372186e558296c43fdb17dfa69e21e`
- Akka Core commit reviewed: `aded7b67a9dafcb32b8a5dc95f6debce3a97c0e9`
- Temporary Akka clone: `/private/tmp/akka-core-rakka-comparison`

The review focused on API equivalence and usability, not line-by-line behavior
compatibility. Akka HTTP and Akka gRPC are not part of `akka-core`, so Rakka's
HTTP/gRPC adapters were treated as Rakka-specific integration surfaces rather
than parity targets.

## Executive Summary

Rakka has a solid v1 foundation: local typed actors, bounded mailboxes, basic
supervision, dead letters, durable state, explicit Protobuf remoting, membership,
sharding, process actors, workflow primitives, stream buffers, HTTP/gRPC
adapters, Kubernetes readiness/drain, metrics, and a compatibility test layer.

It is not yet Akka-like from an application developer's point of view. The main
difference is that Akka exposes high-level, cohesive entry points such as
`ActorSystem`, `Behavior`/`Behaviors`, `ActorContext`, `Receptionist`,
`Routers`, `Cluster`, `ClusterSharding`, `Entity`, `EntityRef`, persistence
behaviors, stream `Source`/`Flow`/`Sink`, and rich testkits. Rakka exposes many
of the lower-level building blocks directly, which makes examples work but asks
users to wire runtime, discovery, ownership, routes, remoting, serialization,
and persistence by hand.

The highest-value simplification is to add Akka-shaped facade APIs over the
current internals before expanding features. In particular, sharding and actor
context ergonomics should be simplified first.

## Capability Map

| Area | Akka Core reference | Rakka reference | Status | First-pass gap |
| --- | --- | --- | --- | --- |
| Typed actor behavior | `akka-actor-typed/.../Behavior.scala`, `scaladsl/Behaviors.scala` | `crates/rakka-core/src/actor.rs` | Partial | Rakka uses an async trait with boxed futures. It lacks a behavior DSL, behavior transitions, `same`, `unhandled`, setup/deferred behavior, interceptors, MDC/log wrappers, and monitoring decorators. |
| Actor context | `akka-actor-typed/.../scaladsl/ActorContext.scala` | `crates/rakka-core/src/actor.rs` | Partial | Rakka has self/system/path, child spawn, `watch_with`, `schedule_once`, and stop child. It lacks child lookup/listing, anonymous spawn, unwatch, receive timeout, message adapters, context ask, ask-with-status, pipe-to-self, lifecycle-managed timers, logger, and execution context helpers. |
| Actor identity and paths | `ActorRef.scala`, `ActorRefResolver.scala` | `crates/rakka-core/src/path.rs`, `actor.rs` | Needs redesign | Rakka's `ActorPath` includes the incarnation fragment. Akka separates logical actor path from ref incarnation/UID. This affects duplicate-name enforcement, lookup, serialization, DeathWatch semantics, remote resolution, and documentation clarity. |
| Actor system lifecycle | `ActorSystem.scala` | `crates/rakka-core/src/system.rs` | Partial | Rakka's system is a local spawn registry with dead letters, metrics, snapshots, and best-effort shutdown. It lacks guardian behavior, typed system-as-root-ref, async termination future, system actors, scheduler/dispatcher access, config/settings, event stream, receptionist, extension registry, and coordinated shutdown integration. |
| Supervision | `SupervisorStrategy.scala`, `Behaviors.supervise` | `crates/rakka-core/src/supervision.rs`, `actor.rs` | Partial | Rakka supports resume, restart, stop, escalate, and simple backoff. It does not support per-exception strategies, restart windows, random backoff jitter, reset-after, logging controls, stop/keep children, restart stash capacity, or true parent escalation. |
| Timers and scheduling | `TimerScheduler.scala`, `ActorContext.setReceiveTimeout` | `ActorContext::schedule_once` | Partial | Rakka timers are detached Tokio tasks and are not lifecycle-managed on restart/stop unless the user keeps and aborts the handle. |
| Receptionist and service discovery | `receptionist/Receptionist.scala`, `ServiceKey` | Remote destination has `Service`, no local API | Absent | Rakka has no local or clustered receptionist API for registering, finding, subscribing to, or routing over typed service keys. |
| Routers | `scaladsl/Routers.scala` | Sharding routes only | Absent | No pool/group routers, random/round-robin/consistent-hash routing, broadcast predicate, routee props, or receptionist-backed group routing. |
| Cluster membership API | `akka-cluster/Cluster.scala`, `akka-cluster-typed/Cluster.scala` | `crates/rakka-cluster/src/*`, `rakka-sharding/src/runtime.rs` | Partial | Rakka has deterministic membership and discovery polling. It lacks Akka's extension entry point, manager/subscription actors, event replay/snapshot subscription API, gossip/read view, configurable failure detector implementations, split-brain resolver/downing provider, leader/role leader surface, and JMX-like operational API. |
| Cluster sharding | `akka-cluster-sharding-typed/.../ClusterSharding.scala` | `crates/rakka-sharding/src/*`, `examples/multi-node-sharding` | Partial but too low-level | Rakka has deterministic shard ownership, `EntityRef`, `ShardRegion`, local/remote routes, and TCP node runtime. It lacks the Akka-like facade: `ClusterSharding`, `EntityTypeKey`, `Entity`, `EntityContext`, `init`, `entity_ref_for`, passivation protocol, buffered handoff during passivation/rebalance, proxy-only mode, pluggable allocation strategy API, durable coordinator backend, and query actor surface. |
| Persistence | `akka-persistence-typed/.../EventSourcedBehavior.scala`, `state/scaladsl/DurableStateBehavior.scala`, `Effect.scala` | `crates/rakka-persistence/src/*` | Durable-state subset | Rakka implements optimistic durable state and a durable actor adapter. It lacks event sourcing, journals, snapshots, retention, tags, persistence query, change events/projections, plugin ids, recovery customization, persist-failure backoff, command stashing/unstashing, reply-effect typing, and standardized `PersistenceId(entity_type, entity_id)` helpers. |
| Streams | `akka-stream/.../Source.scala`, `Flow.scala`, `Sink.scala`, `akka-stream-typed/.../ActorSink.scala` | `crates/rakka-stream/src/*` | Minimal subset | Rakka has bounded source/sink handles, lifecycle states, actor/entity adapters, pipe/fan helpers, and process IO adapters. It lacks graph materialization, `Source`/`Flow`/`Sink` operators, async boundaries, materialized values, Reactive Streams interop, stream testkit, and actor sinks/sources with explicit ack backpressure protocols. |
| Remoting and serialization | `akka-remote/...`, `ActorRefResolver.scala`, serialization modules | `crates/rakka-remote/src/*` | Partial | Rakka's explicit envelopes and registry are good, but general actor path/service delivery is not wired to the local runtime. There is no actor-ref resolver, no serializer discovery/config story, no quarantine/association lifecycle comparable to Artery, and no built-in TLS/mTLS security posture. |
| Testkit | `akka-actor-testkit-typed/...`, `akka-stream-testkit`, `akka-persistence-testkit` | `crates/rakka-testkit/src/*` | Partial | Rakka has probes and integration helpers. It lacks synchronous behavior testing, effect capture, manual time, no-message assertions, fishing, expect-terminated, log capturing, serialization testkit, stream probes, persistence testkit, and multi-node test harness parity. |
| Distributed tools | `akka-cluster-tools`, `akka-distributed-data`, cluster singleton/pub-sub | No equivalent | Absent | Rakka currently does not include distributed data, cluster singleton, distributed pub-sub, reliable delivery, replicated event sourcing, or sharded daemon processes. |

## Highest Priority Recommendations

### P0: Separate Logical Path From Actor Incarnation

Current Rakka paths are formatted like:

```text
rakka://local/<system>/user/<actor>#<incarnation>
```

In Akka, the actor path is the logical hierarchical name, while an actor ref
also carries a UID/incarnation for serialization and lifecycle identity. Rakka's
current `ActorPath` conflates these concerns.

Recommended direction:

- Introduce `ActorPath` as the logical address without incarnation.
- Introduce `ActorUid` or `ActorIncarnation` as a separate ref/cell identity.
- Enforce unique live child names under a parent.
- Add `ActorRefResolver`-like serialization for refs that includes both path and
  incarnation.
- Keep a distinct `EntityId`/`EntityRef` model for sharded entities, since Akka
  also keeps `EntityRef` separate from `ActorRef`.

Why this matters: it unlocks actor lookup, actor selection/resolution, correct
DeathWatch language, duplicate-name errors, and clearer remote serialization.

### P0: Add an Akka-Shaped Actor Facade

Rakka should keep its trait-based actor API, but add a more ergonomic layer for
simple actors.

Recommended direction:

- Add a `rakka` facade crate or `rakka::prelude` that re-exports the stable user
  API across core, persistence, cluster, sharding, stream, and testkit.
- Add `ActorSystem::builder(name)` with config, metrics, registry, cluster, and
  shutdown hooks.
- Add a guardian/root behavior option so `ActorSystem` can be booted with the
  application root actor.
- Add closure/function actor helpers, for example `actor_fn`, `setup`, or a
  small `Behavior<M>` wrapper for users who do not need a full struct plus trait
  implementation.
- Split fire-and-forget from back-pressure-aware sends: keep explicit bounded
  errors, but consider naming like `try_tell` and `send().await` so intent is
  obvious.
- Add `ActorProps`/`SpawnOptions` with mailbox, supervision, dispatcher/blocking,
  and instrumentation options, then use it consistently in actor, sharding, and
  persistence APIs.

### P0: Expand `ActorContext` Before Adding More Features

A large portion of Akka's usability comes from `ActorContext`, not the actor ref
itself.

Recommended additions:

- `children()` and `child(name)`.
- `spawn`, `spawn_anonymous`, and `stop(child)` naming aligned with Akka.
- `watch(target)`, `watch_with(target, msg)`, and `unwatch(target)`.
- A default `ActorTerminated` message path for actors that model termination in
  their protocol.
- `set_receive_timeout` and `cancel_receive_timeout`.
- Lifecycle-managed `with_timers` or keyed timers that are cancelled on stop and
  restart.
- `message_adapter`, `ask`, `ask_with_status`, and `pipe_to_self`.
- Actor-scoped logging/tracing context.

### P0: Create a High-Level Sharding API

The current multi-node sharding example is strong evidence that the internals
work, but the user must assemble too many pieces manually.

Current user-facing concepts include `LocalEntityRoute`, `RemoteEntityRoute`,
`ShardRegion`, `SerializationRegistry`, `ClusterNodeRuntime`, `DiscoverySnapshot`,
and explicit ownership refresh.

Recommended facade:

```rust
let system = ActorSystem::builder("orders")
    .with_metrics(metrics)
    .build()
    .await?;

let sharding = ClusterSharding::new(&system);
let carts = Entity::new(EntityTypeKey::<CartCommand>::new("Cart"), |ctx| {
    CartEntity::new(ctx.entity_id(), PersistenceId::of(ctx.entity_type(), ctx.entity_id()))
})
.with_stop_message(CartCommand::Stop)
.with_shards(128);

sharding.init(carts).await?;

let cart = sharding.entity_ref::<CartCommand>("Cart", "cart-123")?;
cart.tell(CartCommand::AddItem { sku }).await?;
```

Facade responsibilities:

- Own shard-region creation.
- Own local vs remote route selection.
- Register inbound entity handlers.
- Register serializers/codecs or fail early with a clear error.
- Apply discovery and ownership updates.
- Expose `EntityContext` with `entity_type`, `entity_id`, `shard_id`, and
  passivation handle.
- Provide passivation and handoff buffering semantics.

### P0: Build Full Typed Persistence

Decision: Rakka should target full typed persistence parity, including event
sourcing, snapshots, query APIs, and persistence testkit. Durable state remains
one persistence mode, not the entire persistence story.

To reach Akka-like persistence parity, Rakka needs at least:

- `PersistenceId::of(entity_type, entity_id)` with separator validation.
- Durable state behavior facade instead of only trait adapter spawn helpers.
- Event-sourced behavior with command handlers, event handlers, event journals,
  snapshots, recovery, retention, tags, and metadata.
- Effect builders with `reply`, `no_reply`, `then_reply`, `stash`,
  `unstash_all`, and `unhandled`.
- Persist-failure backoff semantics.
- Query APIs for events, current events, persistence ids, durable state changes,
  tags or slices, and projection-friendly read models.
- Persistence testkit coverage for durable state, event sourcing, snapshots,
  queries, serialization, and failure/recovery behavior.

## Medium Priority Recommendations

### P1: Add Receptionist and Routers

Akka applications often avoid direct actor wiring by registering services and
routing to pools/groups.

Recommended Rakka subset:

- `ServiceKey<M>`.
- `Receptionist::register`, `deregister`, `find`, and `subscribe`.
- Local-only implementation first, clustered later.
- `Routers::pool(size, factory)` with round-robin and random routing.
- `Routers::group(service_key)` over receptionist listings.
- Consistent hashing as a later addition.

### P1: Introduce a Cluster Extension API

Rakka has the membership mechanics but not the user-facing cluster extension.

Recommended surface:

- `Cluster::get(&system)`.
- `cluster.manager().join/leave/down`.
- `cluster.subscriptions().subscribe(...)`.
- `cluster.state()`.
- `cluster.self_member()`.
- Integration with Kubernetes discovery and node runtime.
- Clear split-brain/downing policy hooks, even if only one conservative policy
  exists initially.

### P1: Integrate Coordinated Shutdown

Rakka has shutdown-related pieces in actors, process, workflow, and Kubernetes
drain, but they are not exposed as one actor-system lifecycle.

Recommended surface:

- `ActorSystem::terminate().await`.
- `ActorSystem::when_terminated()`.
- `CoordinatedShutdown::add_task(phase, name, task)`.
- Built-in phases for stop ingress, drain streams, leave cluster, handoff
  shards, stop process actors, flush persistence, stop actors, stop remoting.

### P1: Strengthen Testkit Ergonomics

Recommended additions:

- `ActorTestKit` that owns a system and named test guardian.
- `TestProbe` methods: `expect_message`, `expect_message_type`,
  `expect_no_message`, `receive_messages`, `await_assert`,
  `expect_terminated`, and `stop`.
- Manual time for timers and receive timeouts.
- Behavior/effect testkit for synchronous tests once Rakka has a behavior DSL.
- Serialization/compatibility testkit for registry and remote envelopes.
- Stream probes for demand, completion, cancellation, and errors.

### P1: Add Stream Facade or Narrow the Promise

If Rakka wants Akka Streams equivalence, the current `BoundedStream` model is
not enough. If not, the docs should say "bounded stream primitives" rather than
"streams" in the Akka sense.

Recommended intermediate API:

- `Source<T>`, `Flow<I, O>`, and `Sink<T>` wrappers over the existing bounded
  primitives.
- Core operators first: `map`, `filter`, `map_async`, `take`, `merge`, `broadcast`,
  `fold`, `run_collect`, `run_foreach`.
- Actor/entity sinks with explicit ack/back-pressure protocol.

## Intentional Rakka Differences Worth Keeping

Not every Akka feature should be copied directly.

- Keep bounded-mailbox errors explicit. Rust users benefit from `Result` instead
  of silent fire-and-forget behavior.
- Keep Protobuf/schema compatibility explicit for Kubernetes rolling updates.
- Keep process actors and durable workflow as Rakka differentiators.
- Avoid transparent arbitrary remote actor deployment unless there is a strong
  Rust-native safety and serialization story.
- Keep default internal remoting scoped to trusted cluster traffic unless and
  until TLS/mTLS is implemented.

## Concrete First Implementation Slices

1. Core identity cleanup: split path/incarnation and add named child registry.
2. Context ergonomics: children/child lookup, watch/unwatch, receive timeout,
   lifecycle-managed timers, context ask, and pipe-to-self.
3. Sharding facade: `ClusterSharding`, `EntityTypeKey`, `Entity`, `EntityContext`,
   `entity_ref_for`, and simplified examples.
4. Top-level facade crate or prelude: make the common app imports obvious.
5. Testkit expansion: no-message, termination, await-assert, manual time.
6. Persistence ergonomic pass: `PersistenceId::of`, durable behavior facade, and
   reply-effect helpers.
7. Receptionist plus simple routers.

## Source Files Reviewed

Rakka:

- `crates/rakka-core/src/actor.rs`
- `crates/rakka-core/src/system.rs`
- `crates/rakka-core/src/path.rs`
- `crates/rakka-core/src/supervision.rs`
- `crates/rakka-cluster/src/membership.rs`
- `crates/rakka-cluster/src/discovery.rs`
- `crates/rakka-remote/src/envelope.rs`
- `crates/rakka-remote/src/registry.rs`
- `crates/rakka-sharding/src/identity.rs`
- `crates/rakka-sharding/src/routing.rs`
- `crates/rakka-sharding/src/local.rs`
- `crates/rakka-sharding/src/remote.rs`
- `crates/rakka-sharding/src/runtime.rs`
- `crates/rakka-sharding/src/node_runtime.rs`
- `crates/rakka-persistence/src/actor.rs`
- `crates/rakka-persistence/src/effect.rs`
- `crates/rakka-persistence/src/store.rs`
- `crates/rakka-stream/src/lib.rs`
- `crates/rakka-stream/src/adapters.rs`
- `crates/rakka-testkit/src/lib.rs`
- `examples/multi-node-sharding/src/main.rs`
- `examples/durable-counter/src/main.rs`
- `docs/rakka-v1-api-review.md`
- `docs/rakka-v1-known-limitations-roadmap.md`
- `docs/rakka-actor-framework-spec.md`

Akka Core:

- `akka-actor-typed/src/main/scala/akka/actor/typed/ActorRef.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/ActorSystem.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/Behavior.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/ActorRefResolver.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/SupervisorStrategy.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/scaladsl/ActorContext.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/scaladsl/Behaviors.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/scaladsl/Routers.scala`
- `akka-actor-typed/src/main/scala/akka/actor/typed/receptionist/Receptionist.scala`
- `akka-cluster-typed/src/main/scala/akka/cluster/typed/Cluster.scala`
- `akka-cluster/src/main/scala/akka/cluster/Cluster.scala`
- `akka-cluster/src/main/scala/akka/cluster/ClusterEvent.scala`
- `akka-cluster-sharding-typed/src/main/scala/akka/cluster/sharding/typed/scaladsl/ClusterSharding.scala`
- `akka-persistence-typed/src/main/scala/akka/persistence/typed/scaladsl/EventSourcedBehavior.scala`
- `akka-persistence-typed/src/main/scala/akka/persistence/typed/scaladsl/Effect.scala`
- `akka-persistence-typed/src/main/scala/akka/persistence/typed/state/scaladsl/DurableStateBehavior.scala`
- `akka-persistence-typed/src/main/scala/akka/persistence/typed/state/scaladsl/Effect.scala`
- `akka-persistence-typed/src/main/scala/akka/persistence/typed/PersistenceId.scala`
- `akka-persistence-query/src/main/scala/akka/persistence/query/scaladsl/EventsByPersistenceIdQuery.scala`
- `akka-stream/src/main/scala/akka/stream/scaladsl/Source.scala`
- `akka-stream/src/main/scala/akka/stream/scaladsl/Flow.scala`
- `akka-stream/src/main/scala/akka/stream/scaladsl/Sink.scala`
- `akka-stream-typed/src/main/scala/akka/stream/typed/scaladsl/ActorSink.scala`
- `akka-actor-testkit-typed/src/main/scala/akka/actor/testkit/typed/scaladsl/ActorTestKit.scala`
- `akka-actor-testkit-typed/src/main/scala/akka/actor/testkit/typed/scaladsl/TestProbe.scala`
- `akka-actor-testkit-typed/src/main/scala/akka/actor/testkit/typed/scaladsl/BehaviorTestKit.scala`

## Validation

No Rakka code was changed in this pass, so no workspace tests were run. This is
a documentation/report-only review.
