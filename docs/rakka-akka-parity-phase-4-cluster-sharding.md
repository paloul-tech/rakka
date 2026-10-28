# Rakka Akka Parity Phase 4: High-Level Cluster Sharding

Phase 4 adds an Akka-style sharding facade on top of the existing
`ShardCoordinator`, `ShardRegion`, `LocalEntityRoute`, and remote route
foundation.

## Facade Flow

Application code should prefer the facade for local entity registration:

```rust
use rakka::prelude::*;

let system = ActorSystem::new("shopping");
let sharding = ClusterSharding::get(&system);
let key = EntityTypeKey::<CartCommand>::new("Cart")
    .with_number_of_shards(32)?;

let registration = sharding.init(Entity::of(key.clone(), |context| CartEntity {
    persistence_id: context.persistence_id(),
    entity_id: context.entity_id().as_str().to_string(),
}))?;

let cart = registration.entity_ref_for("cart-42");
cart.tell(CartCommand::Add("apple".to_string()))?;
let total = cart
    .ask(|reply_to| CartCommand::GetTotal { reply_to }, timeout)
    .await?;
```

The high-level `ShardedEntityRef<M>` carries its initialized `ShardRegion`, so
callers do not need to pass the region on every `tell` or `ask`. The existing
logical `EntityRef<M>` remains available for serialization and lower-level
routing.

## Entity Context

`EntityContext<M>` exposes:

- `entity_type_key()`
- `entity_type()`
- `entity_id()`
- `shard_id()`
- `local_node_id()`
- `actor_name()`
- `persistence_id()`

`persistence_id()` returns the same string format used by
`rakka_persistence::PersistenceId::of(entity_type, entity_id)`. It intentionally
does not return `PersistenceId` directly because a direct dependency from
`rakka-sharding` to `rakka-persistence` currently creates a crate cycle.

## Passivation And State

`Entity::with_stop_message_factory` configures a message that is delivered
before explicit facade passivation stops the local actor:

```rust
let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_idle_passivation(Duration::from_secs(60))
    .with_stop_message_factory(|| CartCommand::Stop);

sharding.init(entity)?;
sharding.passivate_entity(&key, "cart-42")?;
```

`ClusterSharding::state()` and `registration_state(&key)` expose registration
mode, shard count, local entity count, passivation settings, and owner revision
for diagnostics and tests. The local entity count covers active incarnations
only; an incarnation that is stopping is not counted.

A local entity occupies one slot in its route that is either active or
stopping. Delivery, the decision to stop an incarnation, and the stop itself run
under one lock, so a message is never queued behind a stop the route initiated,
and a delivery never fails `actor-path-in-use` because the previous incarnation
is still terminating. A message that arrives while an entity stops is held,
bounded by the entity's configured mailbox capacity (`ActorOptions`), and
delivered to the next incarnation in arrival order ahead of anything sent
later; past that bound the sender gets `EntityDeliveryFailure::MailboxFull`
with its message back. A held message is dropped, with a warning that names the
count, only when the actor system is terminating or the entity's shard has left
this node.

Idle passivation arms one timer task per active entity; each delivery moves the
deadline instead of starting another sleeper, and
`LocalEntityRoute::armed_idle_timers()` reports how many timer tasks are
sleeping. A stop the route cannot queue because the mailbox is full leaves the
entity active and registered: the idle timer tries again after one more
timeout, and an explicit passivation returns `false`. The stop message is told
again on the next attempt, so stop messages must be idempotent.

An entity that stops itself by returning `ActorAction::Stop` from `handle`
leaves its mailbox loop at once. Whatever was already queued behind the message
it was handling is published as `MailboxClosed` dead letters, and a message that
reaches the route afterwards is held for the next incarnation.
`rakka-process`'s `ProcessBackedEntityAction::Passivate` and the
`rakka-persistence` actor facades stop entities this way. Passivating through
the facade's `passivate_entity`, which queues the stop message and the stop in
one critical section, or through the idle timer carries no such residual.

A remote ask whose local delivery fails at the owner (a draining shard, a full
mailbox, a spawn failure) is answered at once with a typed rejection carrying
the delivery's stable code, surfaced to the asker as
`RemoteRequestError::Rejected { code, message }`. The asker no longer waits out
its timeout, and its connection to the owner stays open.

## Handoff And Passivation Buffering

Facade-created entity types enable bounded shard buffering by default. Messages
sent during graceful shard handoff, temporary owner-cache gaps, or explicit
facade passivation are accepted into the region buffer and flushed when the
shard/entity becomes available again. The region buffer is no longer needed to
survive an entity's own stop window, which the local route now covers by
holding those messages itself (see "Passivation And State"); the two compose.

```rust
let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_handoff_buffer(128)
    .with_passivation_buffer_duration(Duration::from_millis(50));

sharding.init(entity)?;
```

Use `Entity::with_buffering(ShardBufferConfig::default())` to set capacity,
overflow behavior, and TTL together. Use `Entity::without_buffering()` for
fail-fast behavior that mirrors the lower-level route errors.

Lower-level `ShardRegion::new` and `ShardRegion::from_snapshot` remain
unbuffered unless explicitly configured:

```rust
let region = ShardRegion::from_snapshot(entity_type, config, &snapshot, route)?
    .with_buffering(ShardBufferConfig::new(64, Duration::from_secs(5)));
```

When the bounded buffer is full, `tell` returns the original message with
`EntityDeliveryFailure::ShardBufferFull`; `ask` maps the same condition to
`EntityAskError::ShardBufferFull`. HTTP and gRPC adapters expose the stable
`entity-shard-buffer-full` code.

## Allocation Strategies

Shard ownership is pluggable through `ShardAllocationStrategy`. The default is
`DeterministicModuloShardAllocationStrategy`, which preserves the original
stable modulo ownership model.

```rust
let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_least_shard_allocation(1, 10);

sharding.init(entity)?;
```

Use `LeastShardAllocationStrategy` when a newly joined node should receive a
bounded number of shards per rebalance pass based on current owner counts:

```rust
let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_allocation_strategy(LeastShardAllocationStrategy::new(1, 4));
```

Custom strategies implement `ShardAllocationStrategy` and receive read-only
`ShardAllocationContext` / `ShardRebalanceContext` values. The coordinator
keeps responsibility for validating target owners and assigning move reasons,
so graceful-leave handoff and owner-unavailable failover continue to use the
same `ShardDecision` model.

Low-level users can attach a strategy directly to a region:

```rust
let region = ShardRegion::new(entity_type, config, route)
    .with_allocation_strategy(LeastShardAllocationStrategy::default());
```

## Remote Runtime Bridge

Networked sharding should use a facade companion for `ClusterNodeRuntime`:

```rust
let system = ActorSystem::new("shopping-node-a");
let mut runtime = ClusterNodeRuntime::builder(local_node)
    .with_registry(registry)
    .build()
    .await?;
let sharding = ClusterSharding::for_node_runtime(&system, &runtime)?;

let registration = sharding.init_remote(
    &mut runtime,
    Entity::of(key.clone(), |context| CartEntity::new(context)),
)?;

runtime.apply_discovery(discovery_snapshot)?;
let cart = sharding.entity_ref_for(&key, "cart-42")?;
cart.tell(CartCommand::Add("apple".to_string()))?;
```

`init_remote` creates the local entity route, wraps it in the runtime's remote
route, registers the shard region with the networked sharding runtime, and
installs the inbound remote tell handler for the entity type.

Use `init_remote_with_ask` when remote request/reply should share the same
entity type:

```rust
sharding.init_remote_with_ask(
    &mut runtime,
    Entity::of(key.clone(), |context| CartEntity::new(context)),
    |request: CartGet, reply_to| CartCommand::Get {
        id: request.id,
        reply_to,
    },
)?;

let reply: CartReply = cart
    .remote_ask(&runtime.ask_client(), CartGet { id }, timeout)
    .await?;
```

The combined remote handler accepts tell envelopes and ask envelopes for the
same entity type. Ask envelopes are detected by request metadata and converted
into the entity command with the supplied builder function.

## Durable Coordinator Store

Cluster sharding can opt in to durable shard coordinator snapshots. The store is
control-plane state only: it keeps entity type, shard count, owner assignments,
allocation strategy name, revision, and update time. Entity event/state
persistence remains owned by `rakka-persistence`.

```rust
use rakka::prelude::*;

let system = ActorSystem::new("shopping");
let store = InMemoryShardCoordinatorStore::new();
let sharding = ClusterSharding::get_with_coordinator_store(&system, store.clone());

let key = EntityTypeKey::<CartCommand>::new("Cart")
    .with_number_of_shards(32)?;
sharding.init(Entity::of(key.clone(), |context| CartEntity::new(context)))?;
```

Networked nodes can configure the same store through
`ClusterNodeRuntime::builder(local_node).with_shard_coordinator_store(store)`.
Low-level runtime users can use
`ClusterShardingRuntime::with_coordinator_store(membership, store)`.

Persistent coordinator backends should use the async store path so database I/O
does not block runtime threads:

```rust
use rakka_sharding_postgres::PostgresShardCoordinatorStore;
use tokio_postgres::NoTls;

let (client, connection) = tokio_postgres::connect(
    "postgres://postgres:postgres@localhost:5432/postgres",
    NoTls,
).await?;
tokio::spawn(async move {
    let _ = connection.await;
});

let store = PostgresShardCoordinatorStore::builder(client)
    .with_namespace("shopping-prod")
    .migrate()
    .await?;

let sharding = ClusterSharding::get_with_async_coordinator_store(&system, store);

sharding
    .init_async(Entity::of(key.clone(), |context| CartEntity::new(context)))
    .await?;
```

Multi-coordinator deployments can pair the async store with a leadership lease.
The store's revision compare-and-set protects each snapshot update from stale
writers; the lease decides which node may make coordinator decisions at all.
When a lease is rejected, lost, expired, or stolen by a higher fencing token,
the runtime fails before publishing new owner caches.

```rust
use rakka_sharding_postgres::{
    PostgresShardCoordinatorLease,
    PostgresShardCoordinatorStore,
};
use tokio_postgres::NoTls;

let (store_client, store_connection) = tokio_postgres::connect(dsn, NoTls).await?;
tokio::spawn(async move {
    let _ = store_connection.await;
});
let (lease_client, lease_connection) = tokio_postgres::connect(dsn, NoTls).await?;
tokio::spawn(async move {
    let _ = lease_connection.await;
});

let store = PostgresShardCoordinatorStore::builder(store_client)
    .with_namespace("shopping-prod")
    .migrate()
    .await?;
let lease = PostgresShardCoordinatorLease::builder(lease_client)
    .with_namespace("shopping-prod")
    .migrate()
    .await?;

let sharding = ClusterSharding::get_with_async_coordinator_store_and_lease(
    &system,
    store,
    lease,
);
```

Networked nodes can use
`ClusterNodeRuntime::builder(local_node).with_async_shard_coordinator_store(store)`
and `.with_shard_coordinator_lease(lease)`.
Runtime users can call `register_region_async`, `apply_discovery_async`,
`heartbeat_async`, `mark_leaving_async`, `mark_down_async`, `tick_async`,
`renew_coordinator_leases_async`, and `release_coordinator_leases_async`.
When a lease is configured, sync sharding APIs fail closed with
`ShardingError::AsyncCoordinatorLeaseRequiresAsyncApi`.

Coordinator writes use snapshot revision compare-and-set. A stale writer returns
`ShardingError::CoordinatorRevisionConflict`; incompatible persisted state
returns `ShardingError::PersistedCoordinatorSnapshotMismatch`. Calling a sync
sharding API with an async-only store returns
`ShardingError::AsyncCoordinatorStoreRequiresAsyncApi`.

See `docs/rakka-akka-parity-phase-4-durable-coordinator-rationale.md` for the
architectural reasoning and Akka comparison.

## One Activation Per Entity Is Not Guaranteed

No wiring gives one activation per entity during a join:

- Each node reconciles ownership from its own membership view, and a region
  takes a gained shard without a handshake. During a join, a rebalance, or a
  transient membership disagreement, two nodes can briefly each own one shard
  and activate the same entity.
- The coordinator lease does not close that window. It gates who may register a
  region and make coordinator decisions, so it fits a single-coordinator
  topology; a node that does not hold it cannot host the entity type at all
  (`ShardingError::CoordinatorLeaseRejected`). Nothing carries the holder's
  ownership to other nodes.
- A shared coordinator store without a lease is not a supported topology.
  Per-node coordinators race on the store, and a node whose persist loses the
  compare-and-set keeps its in-memory ownership unpublished until membership
  changes again.
- The contract is the one `docs/rakka-v1-reliability-boundaries.md` states:
  single-writer correctness rests on the revision compare-and-set and idempotent
  effects. Every durable write an entity makes must be conditioned on the
  revision it read. A write that takes a fresh revision unconditionally is
  outside the contract and can overwrite newer state during a join.
- Entity types that keep state in an actor must cope with two activations:
  recover from the store, and treat a refused compare-and-set as "reload and
  retry". The `rakka-agent-workflow` run actor does this: a lost compare-and-set
  marks the runner or the durable inbox stale (`is_stale()`), and the actor
  recovers every component before its next command.

## Remembered Entities

Entity types can opt into Akka-style remembered entities. Rakka records an
entity id after the local actor is successfully activated, replays remembered
ids in bounded batches when a shard is owned or acquired locally, and keeps the
remembered identity store separate from coordinator ownership snapshots and
entity persistence.

```rust
let remembered = InMemoryRememberedEntityStore::new();
let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_remembered_entities(
        RememberedEntities::enabled()
            .with_start_batch_size(64)
            .with_store(remembered.clone()),
    );

let registration = sharding.init(entity)?;
let cart = registration.entity_ref_for("cart-42");
cart.tell(CartCommand::Add("apple".to_string()))?;
```

Passivation remains a local resource-management mechanism. Idle passivation and
ordinary explicit passivation stop local actors but leave the remembered id in
place. To remove the identity from the remembered set, call the explicit async
forget API:

```rust
sharding.forget_entity(&key, "cart-42").await?;
```

Persistent remembered identity can use `PostgresRememberedEntityStore` from
`rakka-sharding-postgres`:

```rust
let remembered = PostgresRememberedEntityStore::builder(client)
    .with_namespace("shopping-prod")
    .migrate()
    .await?;

let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_remembered_entities(
        RememberedEntities::enabled().with_store(remembered),
    );

sharding.init_async(entity).await?;
```

Remembered entities are not persistence. They decide which entity ids should be
started again; event-sourced or durable-state recovery still belongs to
`rakka-persistence`. High-cardinality remembered sets can slow shard acquisition,
so production configurations should choose batch sizing and store namespaces
deliberately.

## Current Boundary

The facade now owns local, proxy-only, and networked entity registration.
Serialization codecs still live in the `SerializationRegistry` supplied to
`ClusterNodeRuntime`; missing codecs fail at send/decode time with typed remote
delivery errors.

Remaining Phase 4 work should focus on persistence recovery examples over
movement and any operational polish found while exercising remembered entities
with durable stores.
