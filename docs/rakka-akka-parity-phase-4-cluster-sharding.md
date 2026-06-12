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
for diagnostics and tests.

## Handoff And Passivation Buffering

Facade-created entity types enable bounded shard buffering by default. Messages
sent during graceful shard handoff, temporary owner-cache gaps, or explicit
facade passivation are accepted into the region buffer and flushed when the
shard/entity becomes available again.

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

Networked nodes can use
`ClusterNodeRuntime::builder(local_node).with_async_shard_coordinator_store(store)`.
Runtime users can call `register_region_async`, `apply_discovery_async`,
`heartbeat_async`, `mark_leaving_async`, `mark_down_async`, and `tick_async`.

Coordinator writes use snapshot revision compare-and-set. A stale writer returns
`ShardingError::CoordinatorRevisionConflict`; incompatible persisted state
returns `ShardingError::PersistedCoordinatorSnapshotMismatch`. Calling a sync
sharding API with an async-only store returns
`ShardingError::AsyncCoordinatorStoreRequiresAsyncApi`.

See `docs/rakka-akka-parity-phase-4-durable-coordinator-rationale.md` for the
architectural reasoning and Akka comparison.

## Current Boundary

The facade now owns local, proxy-only, and networked entity registration.
Serialization codecs still live in the `SerializationRegistry` supplied to
`ClusterNodeRuntime`; missing codecs fail at send/decode time with typed remote
delivery errors. Remaining Phase 4 work should focus on persistent coordinator
backends, coordinator leadership/lease semantics, remembered entities, and
persistence recovery examples over movement.
