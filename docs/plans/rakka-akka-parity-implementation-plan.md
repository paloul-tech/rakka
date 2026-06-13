# Rakka Akka Parity Implementation Plan

Status: Draft for review
Date: 2026-06-11

## Summary

This plan turns the first-pass Akka Core gap report into implementation slices.
The goal is not to clone Akka's internals. The goal is to make Rakka feel
equivalent at the user-facing API level while preserving Rust-native error
handling, explicit bounded resources, Protobuf compatibility, Kubernetes
operation, process actors, and durable workflow strengths.

This is a post-v1 API simplification and capability expansion plan. Existing
low-level APIs should remain usable during the transition, but new examples and
documentation should move toward the high-level surfaces described here.

Reference report: `docs/rakka-akka-core-gap-report.md`.

## Target Outcome

Rakka should support a compact Akka-like application shape:

```rust
let system = ActorSystem::builder("orders")
    .with_metrics(metrics)
    .with_serialization(registry)
    .build()
    .await?;

let cluster = Cluster::get(&system);
let sharding = ClusterSharding::get(&system);

sharding
    .init(Entity::new(EntityTypeKey::<CartCommand>::new("Cart"), |ctx| {
        CartEntity::event_sourced(ctx.entity_id())
    }))
    .await?;

let cart = sharding.entity_ref::<CartCommand>("Cart", "cart-123")?;
cart.tell(CartCommand::AddItem { sku }).await?;
system.terminate().await?;
```

## Guiding Decisions

- Target full typed persistence parity: event sourcing, snapshots, queries, and
  persistence testkit are in scope.
- Keep durable state as a first-class persistence mode alongside event sourcing.
- Add facade APIs before replacing existing internals.
- Keep bounded-resource behavior explicit; Rakka can expose async send variants
  without hiding mailbox pressure.
- Prefer Rust traits and builders where Akka uses Scala extension APIs, but keep
  the conceptual names recognizable.
- Every new public facade should have an example and testkit support in the same
  slice or the immediately following slice.

## Phase 0: API Boundary and Migration Groundwork

Objectives:

- Decide which crates own the new facade types and whether a top-level `rakka`
  crate should be added.
- Define compatibility rules for current low-level APIs while new facades land.
- Establish naming conventions aligned with Akka where helpful.

Deliverables:

- `rakka::prelude` or equivalent documented import surface.
- API inventory marking each existing public type as facade, foundation,
  adapter, or test/support.
- Migration notes for examples that will move from low-level wiring to facades.
- Compile-fail or repository hygiene tests to keep unstable internals out of the
  prelude.

Validation:

- `cargo doc --workspace --all-features --no-deps`
- A small documentation test showing the intended import surface.

Implementation status on 2026-06-11:

- Added `crates/rakka` as the top-level facade crate.
- Added `rakka::prelude` with common actor, durable-state, sharding identity,
  stream, error, supervision, timer, and metrics imports.
- Added `docs/rakka-api-boundary-inventory.md`.
- Added `docs/rakka-akka-parity-migration-notes.md`.
- Added repository hygiene coverage that keeps coordinator, route, transport,
  and adapter internals out of the prelude.

## Phase 1: Actor Identity, Paths, and System Lifecycle

Objectives:

- Separate logical actor paths from actor incarnations.
- Add a stronger actor-system lifecycle compatible with later cluster,
  sharding, and persistence facades.

Deliverables:

- `ActorPath` as a logical hierarchical path without incarnation.
- `ActorUid` or `ActorIncarnation` held by `ActorRef`/cell identity.
- Unique live child-name enforcement under a parent.
- `ActorRefResolver`-style serialization and resolution for local/remote-safe
  refs.
- Actor-system builder with metrics, serialization registry, runtime settings,
  and shutdown configuration.
- `ActorSystem::terminate().await` and `ActorSystem::when_terminated()`.
- System actor namespace or equivalent foundation for framework services.

Validation:

- Tests for duplicate child names, reincarnation with the same path, ref
  serialization, ref resolution failure, DeathWatch after reincarnation, and
  graceful system termination.
- Existing examples updated only where path formatting changes require it.

Implementation status on 2026-06-11:

- Split logical actor paths from actor incarnations with `ActorPath` and
  `ActorUid`.
- Added UID-bearing `ActorRef`, `ActorRuntimeSnapshot`, and `ActorTerminated`
  surfaces.
- Added live path registration with duplicate root and child-name rejection.
- Added `SerializedActorRef` and `ActorRefResolver` for typed local ref
  serialization and stale-ref rejection.
- Added `ActorSystem::builder`, opaque serialization registry storage, runtime
  settings, shutdown configuration, `terminate().await`, `when_terminated()`,
  and `is_terminated()`.
- Added `/system` namespace spawning helpers for framework-owned actors.
- Added local actor runtime coverage for duplicate names, reincarnation,
  resolver success and failures, DeathWatch UID preservation after
  reincarnation, and graceful system termination.

## Phase 2: Actor Facade and Context Ergonomics

Objectives:

- Make simple actors easier to write.
- Move common interaction patterns into `ActorContext`.

Deliverables:

- `Behavior<M>` or `actor_fn` facade for closure/function actors.
- `setup`-style deferred initialization that receives `ActorContext`.
- `ActorProps` or `SpawnOptions` covering mailbox, supervision, dispatcher,
  instrumentation, and blocking hints.
- `ActorContext::spawn`, `spawn_anonymous`, `children`, `child`, `stop`.
- `watch`, `watch_with`, and `unwatch`.
- Receive timeout support.
- Lifecycle-managed keyed timers.
- `message_adapter`, `ask`, `ask_with_status`, and `pipe_to_self`.
- Actor-scoped tracing/logging context.

Validation:

- Local actor runtime tests covering each context API.
- Testkit probes for timers, receive timeouts, watch/unwatch, context ask, and
  pipe-to-self.
- Minimal-system example rewritten to the high-level style.

Implementation status on 2026-06-11:

- Added `actor_fn` for synchronous function-style actors.
- Added `Behavior`, `BehaviorActor`, `SetupActor`, and `setup` for context-aware
  initialization.
- Added `ActorSystem::spawn`, `spawn_factory`, `spawn_with_options`, and
  anonymous spawn aliases while keeping the existing `spawn_actor` API.
- Added `SpawnOptions` and `ActorProps` aliases over `ActorOptions`, plus
  dispatcher, instrumentation, and blocking hints.
- Added `ActorContext::spawn`, `spawn_anonymous`, `children`, `child`, `stop`,
  `stop_child_named`, `watch`, `watch_with`, and `unwatch`.
- Added keyed one-shot timers, receive timeout configuration, message adapters,
  context ask, ask-with-status, and pipe-to-self helpers.
- Added actor trace context helpers for actor-scoped logging and tracing fields.
- Rewrote `examples/minimal-system` to use `rakka::prelude`,
  `ActorSystem::spawn`, `actor_fn`, and `terminate().await`.
- Added local actor runtime coverage for function actors, setup actors, context
  spawn/child lookup, anonymous children, watch/unwatch, adapters, timers,
  receive timeout, context ask, and pipe-to-self.
- Added reusable `rakka-testkit` probes for timers, receive timeouts,
  watch/unwatch, context ask, pipe-to-self, and termination assertions.
- Added `docs/rakka-akka-parity-phase-2-actor-facade.md` with actor-shape guidance, context
  idioms, testkit probe examples, and async closure facade tradeoffs.

Remaining follow-up:

- Prototype an additive async closure helper only if its call site avoids
  boxed-future annotations and higher-ranked lifetime noise for users.

## Phase 3: Full Typed Persistence

Objectives:

- Make persistence a full Rakka pillar equivalent to Akka Typed Persistence.
- Support both event-sourced and durable-state behaviors behind a coherent
  effect/recovery/testkit model.

Deliverables:

- Identity and metadata:
  - `PersistenceId::of(entity_type, entity_id)` with separator validation.
  - `SequenceNr`, `SnapshotSelection`, `SnapshotMetadata`, `RecoveryOptions`,
    event metadata, tags, and optional slice identifiers.
- Storage traits:
  - `EventJournal<E>` for append, replay, delete, and metadata.
  - `SnapshotStore<S>` for save, load, list, and delete.
  - Existing `DurableStateStore<S>` kept and aligned with the new query model.
  - In-memory implementations for all stores.
  - PostgreSQL journal, snapshot, and durable-state implementations.
- Behavior APIs:
  - `EventSourcedBehavior<Command, Event, State>`.
  - `DurableStateBehavior<Command, State>`.
  - Command handler and event/state handler builders.
  - Immutable and mutable-state constructors.
  - Signal handlers for recovery, snapshot, persist failure, pre-restart, and
    post-stop events.
- Effects:
  - `Effect::persist`, `persist_all`, `none`, `unhandled`, `stop`, `stash`,
    `unstash_all`, `reply`, `no_reply`, `then_run`, `then_reply`, `then_stop`.
  - `ReplyEffect` or Rust-equivalent enforced-reply mode.
  - Async effect handling if it can be implemented without losing ordering.
- Recovery and failure:
  - Snapshot-first recovery with event replay after snapshot.
  - Configurable snapshot selection and retention.
  - Persist-failure backoff that does not resume uncertain state.
  - Bounded command stash during persistence and recovery.
- Queries:
  - `events_by_persistence_id`.
  - `current_events_by_persistence_id`.
  - `persistence_ids` and `current_persistence_ids`.
  - `events_by_tag` or `events_by_slice`.
  - Durable-state ids and current state changes.
  - Query stream integration with `rakka-stream`.
- Testkit:
  - `PersistenceTestKit` with in-memory journal/snapshot/durable stores.
  - Event-sourced behavior testkit for effects, replies, recovery, and signals.
  - Durable-state behavior testkit aligned with the same assertions.
  - Serialization compatibility helpers for events, snapshots, and state.

Validation:

- Unit tests for journal ordering, replay windows, snapshot selection, retention,
  delete semantics, tags/slices, revision and sequence-number fencing.
- Actor integration tests for recovery, restart, backoff, stashing, replies,
  and signal order.
- PostgreSQL tests for journal, snapshot, durable state, and query APIs.
- Compatibility tests for event and snapshot schema evolution.
- Examples:
  - Event-sourced counter.
  - Durable-state counter migrated to the new behavior API.
  - Sharded event-sourced cart using `PersistenceId::of`.

Implementation status on 2026-06-11:

- Landed the Phase 3 storage foundation:
  - `PersistenceId::of(entity_type, entity_id)` with separator validation.
  - `SequenceNr`, `SnapshotSelection`, `SnapshotMetadata`, `RecoveryOptions`,
    `RetentionCriteria`, `PersistFailureBackoff`, `EventMetadata`,
    `EventRecord`, `TaggedEvent`, and optional slice metadata.
  - `EventJournal<E>` and `SnapshotStore<S>` traits.
  - `DurableStateStore<S>::persistence_ids` query hook.
  - `InMemoryEventJournal`, `InMemorySnapshotStore`, and durable-state
    persistence id queries.
  - `EventSourcedActor`, `EventSourcedEffect`, spawn helpers, snapshot-first
    recovery, journal replay, retention, write backoff, signals, stash
    directives, and post-commit side effects for the local actor runtime.
  - Akka-named `EventSourcedBehavior` and `DurableStateBehavior` builders over
    the lower-level actor runtimes.
  - Query helpers that materialize current persistence queries as
    `rakka-stream` bounded sources.
  - PostgreSQL durable-state, event journal, and snapshot store implementations.
  - `PersistenceTestKit` bundling in-memory journal, snapshot, and durable-state
    stores for reusable tests.
  - Event-sourced and durable-state behavior testkits for command/effect
    assertions.
  - Event-sourced counter and sharded cart examples.
- Remaining hardening after Phase 3:
  - Live/tailing persistence queries.
  - PostgreSQL transaction batching for multi-event appends.
  - Serialization compatibility fixtures for schema evolution.
  - Full Akka-style enforced reply typing if a Rust API can stay ergonomic.

## Phase 4: High-Level Cluster Sharding

Objectives:

- Hide low-level route, coordinator, runtime, and serializer wiring behind a
  high-level sharding facade.
- Integrate sharded entities with typed persistence.

Deliverables:

- `ClusterSharding::get(&system)`.
- `EntityTypeKey<M>`, `Entity<M>`, `EntityContext<M>`, and `EntityRef<M>`.
- `ClusterSharding::init(entity)` and `entity_ref_for`.
- Automatic local route, remote route, endpoint handler, serializer, and region
  registration where possible.
- Passivation protocol with `Passivate` and configurable stop message.
- Handoff buffering during passivation and rebalance.
- Proxy-only region mode.
- Pluggable shard allocation strategy API.
- Query surface for shard state and entity type registration state.
- Durable coordinator backend design, with in-memory first and persistent
  implementation after the facade is stable.

Validation:

- Rewrite `examples/multi-node-sharding` to the facade.
- Tests for local sharding, TCP loopback sharding, passivation buffering,
  handoff buffering, rebalance, proxy-only routing, remote ask, and
  persistence recovery after movement.

Implementation status on 2026-06-11:

- Added `ClusterSharding::get(&system)` and explicit local-node construction
  over the existing deterministic sharding runtime.
- Added Akka-named `EntityTypeKey<M>`, `Entity<M, A, F>`, and
  `EntityContext<M>` facade types.
- Added `ClusterSharding::init(entity)`, `init_proxy`, `entity_ref_for`, and
  `region_for`.
- Added `ShardedEntityRef<M>` so facade callers can `tell` and `ask` without
  carrying a `ShardRegion` at every call site while preserving the existing
  serializable logical `EntityRef<M>`.
- Added explicit passivation through the facade with configurable stop-message
  factories and local entity-count state queries.
- Added `ClusterShardingState` and `EntityTypeRegistrationState` query
  snapshots for registration mode, shard count, local entity count, and
  passivation settings.
- Added dependency-free `EntityContext::persistence_id()` string generation
  matching the `PersistenceId::of(entity_type, entity_id)` convention. A direct
  sharding-to-persistence dependency would currently form a crate cycle through
  stream/process/sharding.
- Updated `examples/multi-node-sharding` so the receiving node hosts its entity
  through the facade while the sending node continues to demonstrate explicit
  remote-route wiring.
- Added facade coverage for local sharding, proxy-only registration,
  passivation, typed message mismatch reporting, state queries, and remote
  inbound compatibility with facade-created regions.
- Added Phase 4A remote facade integration with
  `ClusterSharding::for_node_runtime`, `init_remote`,
  `init_remote_with_ask`, and `ShardedEntityRef::remote_ask`.
- `init_remote` now creates the local route, wraps it in the runtime remote
  route, registers the networked shard region, and installs the inbound remote
  tell handler through `ClusterNodeRuntime`.
- `init_remote_with_ask` installs a combined tell/ask endpoint handler so one
  entity type can support both at-most-once remote tells and request/reply
  remote asks.
- Rewrote the TCP loopback and multi-process paths in
  `examples/multi-node-sharding` to use the facade for both sender and receiver
  regions.
- Added Phase 4A TCP coverage for facade remote tell, facade remote ask,
  missing serializer failure, and ownership refresh after graceful-leave
  handoff.
- Added Phase 4B handoff/passivation buffering with `ShardBufferConfig`,
  bounded per-shard queues, overflow errors, TTL-based expiry, and explicit
  low-level `ShardRegion::with_buffering` opt-in.
- Facade-created `Entity` registrations now enable default buffering and expose
  `with_buffering`, `with_handoff_buffer`, `without_buffering`, and
  `with_passivation_buffer_duration`.
- `ClusterSharding::passivate_entity` now opens a short buffering window before
  stopping the local entity and flushes buffered messages once the window
  closes.
- HTTP and gRPC adapters now map full shard buffers to stable
  `entity-shard-buffer-full` errors.
- Added Phase 4B tests for facade passivation buffering, handoff flush on shard
  acquire, and bounded-buffer overflow returning the original message.
- Added Phase 4C `ShardAllocationStrategy` hooks with read-only allocation and
  rebalance contexts, strategy-requested reassignments, and validation in
  `ShardCoordinator`.
- Preserved the original deterministic modulo ownership as
  `DeterministicModuloShardAllocationStrategy` and added
  `LeastShardAllocationStrategy` for bounded least-shard rebalancing.
- `ShardRegion`, `Entity`, remote facade registration, and proxy-only
  registration now carry allocation strategy metadata into
  `ClusterShardingRuntime`.
- Added Phase 4C tests for custom initial allocation, bounded least-shard
  rebalance, and facade-to-runtime strategy propagation.
- Began Phase 4D durable coordinator work with
  `ShardCoordinatorStore`, `PersistedShardCoordinatorState`, and
  `InMemoryShardCoordinatorStore`.
- `ShardCoordinator` can now recover from a persisted `ShardOwnershipSnapshot`
  while preserving revision and validating entity type, shard count, and shard
  ids.
- `ClusterShardingRuntime`, `ClusterNodeRuntimeBuilder`, and
  `ClusterSharding` facade constructors can opt in to durable coordinator
  storage without changing the default ephemeral behavior.
- Added durable coordinator tests for revision fencing, initial snapshot
  persistence, runtime recovery, persisted config mismatch, and facade wiring.
- Added `docs/rakka-akka-parity-phase-4-durable-coordinator-rationale.md`
  capturing the control-plane reasoning and Akka comparison.
- Implemented Phase 4D1 async coordinator store boundaries with
  `AsyncShardCoordinatorStore`, `CoordinatorStoreFuture`, async runtime
  registration/update APIs, async node-runtime APIs, async facade init/proxy
  APIs, and fail-closed sync behavior for async-only stores.
- Implemented Phase 4D2 PostgreSQL coordinator store with the
  `rakka-sharding-postgres` adapter crate, namespace-isolated coordinator
  snapshots, migration SQL, compare-and-set revision fencing, and gated
  PostgreSQL recovery tests.
- Implemented Phase 4D3 coordinator leadership leases with
  `ShardCoordinatorLease`, `LeaseToken`, in-memory and PostgreSQL lease
  backends, async runtime/node renewal and release APIs, facade constructors for
  async store-plus-lease wiring, and stale-token publication guards.
- Completed Phase 4D4 remembered entities evaluation with an accepted opt-in,
  bounded semantics decision and a dedicated Phase 4D5 implementation plan.
- Implemented Phase 4D5 remembered entities with facade opt-in, explicit async
  forget APIs, activation recording, bounded replay on ownership/acquire,
  in-memory and PostgreSQL remembered stores, tests, docs, and an example.
- Implemented Phase 4D6 recovery-after-movement examples by upgrading
  `examples/sharded-cart-persistence` to facade-created sharded cart entities
  with in-memory and PostgreSQL coordinator/event/snapshot paths, tests that
  assert state survives ownership movement, and docs that separate coordinator
  recovery from entity persistence recovery.

Phase 4 follow-ups:

Completed. See `docs/plans/rakka-akka-parity-phase-4d-follow-up-plan.md` for
the detailed slice plan and implementation notes.

- Persistent coordinator store implementations, starting with PostgreSQL.
- Coordinator leadership/lease semantics for multi-coordinator deployments.
- Remembered-entity semantics evaluated against Akka Cluster Sharding and
  implemented as opt-in bounded replay.
- Persistence recovery-after-movement examples on top of facade-created
  sharded entities.

## Phase 5: Cluster Extension, Receptionist, and Routers

Objectives:

- Give users a small cluster API and typed service discovery/routing tools.

Deliverables:

- `Cluster::get(&system)`.
- `cluster.manager().join`, `join_seed_nodes`, `leave`, `down`.
- `cluster.subscriptions().subscribe` with initial snapshot or initial events.
- `cluster.state()` and `cluster.self_member()`.
- Kubernetes discovery integration through the cluster extension.
- Failure-detector and downing policy configuration hooks.
- Local `Receptionist` with `ServiceKey<M>`, register, deregister, find, and
  subscribe.
- Clustered receptionist propagation after local semantics are stable.
- `Routers::pool` with round-robin and random routing.
- `Routers::group` over receptionist listings.
- Consistent-hash routing as a follow-up inside this phase if time permits.

Validation:

- Tests for subscription replay, join/leave/down commands, receptionist
  lifecycle cleanup, routee removal, pool fairness, group route refresh, and
  clustered service lookup.

Implementation status on 2026-06-12:

- Implemented Phase 5A cluster extension facade with `Cluster::get`,
  configured local-node construction, `ClusterManager` commands for
  `join_self`, `join`, `join_seed_nodes`, `leave`, and `down`,
  replayable cluster subscriptions, `ClusterState`, `SelfMember`,
  `ClusterUpdate`, and `ClusterEvent`.
- Implemented Phase 5B cluster runtime hooks with `ClusterSettings`,
  `ClusterRuntime`, explicit discovery polling, timeout failure detection,
  timeout and no-op downing strategies, direct discovery snapshot application,
  and `ClusterNodeRuntime` bridge APIs for mirroring high-level cluster state
  into the sharding/remoting runtime.
- Implemented Phase 5C local receptionist with `ServiceKey<M>`,
  `Receptionist::get`, typed local `register`, `deregister`, `find`, and
  `subscribe`, `Listing<M>`, drop-safe registration leases, actor termination
  cleanup, subscription updates, duplicate-registration idempotence, and
  service-key type mismatch protection.

## Phase 6: Streams Facade and Stream Testkit

Objectives:

- Decide whether Rakka claims Akka Streams parity or a smaller bounded-stream
  story. If parity remains the target, introduce a recognizable stream facade.

Deliverables:

- `Source<T>`, `Flow<I, O>`, and `Sink<T>` wrappers over the current bounded
  primitives.
- Core operators: `map`, `filter`, `map_async`, `take`, `merge`, `broadcast`,
  `fold`, `run_collect`, and `run_foreach`.
- Actor sink/source with explicit ack back-pressure protocol.
- Entity sink/source integration with sharded `EntityRef`.
- Stream testkit probes for demand, item assertions, completion, cancellation,
  and failure.

Validation:

- Operator tests for ordering, back-pressure, cancellation, completion, and
  error propagation.
- HTTP/gRPC/process adapters moved onto the facade where it reduces duplicate
  lifecycle semantics.

## Phase 7: Coordinated Shutdown and Operational Integration

Objectives:

- Make actor-system shutdown the single operational path across HTTP/gRPC,
  streams, cluster, sharding, persistence, process actors, and Kubernetes drain.

Deliverables:

- `CoordinatedShutdown` registry with named phases and tasks.
- Built-in phases:
  - stop ingress
  - drain HTTP/gRPC and streams
  - leave cluster
  - hand off shards
  - stop process actors
  - flush persistence
  - stop user actors
  - stop system actors
  - stop remoting
- Kubernetes pre-stop integration using the same shutdown path.
- Shutdown observability: phase durations, failures, and timeout metrics.

Validation:

- Tests for phase ordering, timeout behavior, repeated terminate calls,
  Kubernetes drain integration, shard handoff during shutdown, and process actor
  cleanup.

## Cross-Cutting Requirements

- Keep `unsafe_code` forbidden.
- Preserve existing validation commands unless a phase explicitly updates them.
- Add examples with each facade so usability is reviewed continuously.
- Add docs before marking a facade stable.
- Keep compatibility fixtures updated when persistence events, snapshots,
  remote envelopes, or generated contracts evolve.
- Prefer additive API changes until the user approves a breaking simplification.

## Suggested Review Order

1. Confirm the persistence target and phase ordering.
2. Decide whether to add a top-level `rakka` crate or keep crate-level preludes.
3. Review the actor path/incarnation design before implementation starts.
4. Review the persistence storage traits before PostgreSQL schema work starts.
5. Review the sharding facade API using the rewritten multi-node example before
   expanding cluster behavior.

## Out of Scope For This Plan

- Public package publishing.
- Akka classic actor compatibility.
- Transparent remote deployment of arbitrary closures.
- Full Akka Distributed Data, cluster singleton, distributed pub-sub, reliable
  delivery, and replicated event sourcing. These remain candidates for a later
  distributed-tools plan.
