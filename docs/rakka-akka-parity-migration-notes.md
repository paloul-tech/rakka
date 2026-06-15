# Rakka Akka Parity Migration Notes

Status: updated through Phase 6
Date: 2026-06-14

## Phase 0

Phase 0 introduces the top-level `rakka` crate and `rakka::prelude`. This is the
preferred import surface for new application examples while the lower-level
component crates remain available.

Before:

```rust
use rakka_core::{actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorSystem};
use rakka_persistence::{DurableActor, DurableEffect, PersistenceId};
use rakka_sharding::{EntityId, EntityRef, EntityType, ShardingConfig};
```

After:

```rust
use rakka::prelude::*;
```

Use module imports from `rakka` when code needs an adapter or foundation that is
not intentionally included in the prelude:

```rust
use rakka::remote::SerializationRegistry;
use rakka::sharding::ShardRegion;
```

## Example Migration Order

1. Minimal actor examples should move to `rakka::prelude` first because they
   only need core actor primitives.
2. Durable-state examples should move next, keeping the existing durable actor
   model until the typed persistence phase lands.
3. Multi-node sharding examples should continue using low-level sharding
   foundations until the high-level `ClusterSharding` facade exists.
4. HTTP, gRPC, Kubernetes, process, and workflow examples should import their
   adapter APIs through `rakka::<module>` when convenient, but they do not need
   behavioral changes in Phase 0.

## Compatibility Notes

- `rakka-core`, `rakka-persistence`, `rakka-sharding`, and other existing crates
  are not deprecated in Phase 0.
- `rakka::prelude` is intentionally smaller than the sum of all current public
  APIs.
- Foundation types that are expected to be hidden by future facades should not
  be added to the prelude merely for convenience.
- Later phases may add Akka-like names such as `ClusterSharding`,
  `EntityTypeKey`, `EventSourcedBehavior`, `Source`, `Flow`, and `Sink`; until
  then, the existing foundation APIs remain the implementation path.

## Phase 1

Phase 1 separates logical actor paths from concrete actor incarnations. Code
that previously treated `ActorPath` text as including a `#incarnation` suffix
should now use `ActorRef::uid()` or `ActorRuntimeSnapshot::uid()` for concrete
identity.

Before:

```rust
let path = actor.path().to_string();
```

After:

```rust
let path = actor.path().to_string();
let uid = actor.uid();
```

`ActorSystem::shutdown()` remains as a non-awaiting compatibility helper. New
code that wants graceful lifecycle completion should prefer:

```rust
system.terminate().await?;
system.when_terminated().await;
```

`ActorRefResolver` can serialize and resolve live local typed refs. Resolution
intentionally fails when the path is empty, belongs to another system, has been
reincarnated with a new uid, or is requested with the wrong message type.

## Phase 2

Phase 2 adds high-level actor and context ergonomics while keeping existing
`Actor` implementations valid.

Manual actor structs remain supported:

```rust
let actor = system.spawn_actor("echo", EchoActor)?;
```

New examples should prefer the facade names:

```rust
let actor = system.spawn("echo", actor_fn(|_ctx, msg| {
    match msg {
        EchoMessage::Ping { reply_to } => {
            let _ = reply_to.reply("pong");
            Ok(ActorAction::Continue)
        }
    }
}))?;
```

Use `setup` when initialization needs the actor context:

```rust
let actor = system.spawn("configured", setup(|ctx| {
    let path = ctx.path().to_string();
    Ok(MyBehavior { path })
}))?;
```

Inside actors, prefer `ctx.spawn`, `ctx.spawn_anonymous`, `ctx.children`,
`ctx.child`, `ctx.watch_with`, `ctx.start_timer_once`, `ctx.ask`, and
`ctx.pipe_to_self` over ad hoc task or timer wiring.

Tests that exercise these context APIs should prefer the reusable
`rakka-testkit` probes: `spawn_actor_context_probe`, `spawn_stop_probe`,
`spawn_echo_probe`, `expect_terminated`, `TestProbe::expect_message_eq`, and
`TestProbe::expect_no_message`.

`actor_fn` is intentionally synchronous today. Async actor work remains
supported through manual `Actor` implementations, `Behavior`, `setup`,
`ctx.ask`, and `ctx.pipe_to_self`. A future fully async closure helper should be
additive so existing `actor_fn` call sites remain stable.

## Phase 3

Phase 3 starts the full typed persistence track. Existing durable actors and
durable-state stores remain valid, and the storage vocabulary now has the
Akka-like pieces needed for event sourcing.

Prefer structured persistence ids for new entity-style code:

```rust
let persistence_id = PersistenceId::of("cart", cart_id)?;
```

Use `InMemoryEventJournal`, `InMemorySnapshotStore`, and
`InMemoryDurableStateStore` directly for low-level store tests, or use
`PersistenceTestKit` when a test needs all three stores:

```rust
let kit = PersistenceTestKit::<CartEvent, CartSnapshot, CartSnapshot>::new();
let journal = kit.journal();
let snapshots = kit.snapshots();
let durable_state = kit.durable_state();
```

New event-sourced application code should prefer `EventSourcedBehavior`:

```rust
let behavior = EventSourcedBehavior::builder(persistence_id, CartState::default())
    .on_command(|state, command| match command {
        CartCommand::Add { item, reply_to } => {
            EventSourcedEffect::persist(CartEvent::Added(item))
                .then_reply(reply_to, state.total + 1)
        }
    })
    .on_event(|state, event| state.apply(event))
    .build()?
    .with_retention_criteria(RetentionCriteria::snapshot_every(100).keep_snapshots(2));

let cart = behavior.spawn(&system, "cart", kit.journal(), kit.snapshots())?;
```

Durable state now has the same builder shape:

```rust
let behavior = DurableStateBehavior::builder(persistence_id, CartState::default())
    .on_command(|state, command| DurableEffect::persist(state.handle(command)))
    .build()?;
```

Use `EventSourcedBehaviorTestKit` and `DurableStateBehaviorTestKit` for
command/effect assertions. Use `current_events_by_persistence_id`,
`current_events_by_tag`, `current_persistence_ids`, and
`current_durable_state_ids` when a query should be consumed as a
`rakka-stream` source.

`EventSourcedActor` and `DurableActor` remain available for advanced cases that
need fully async command handlers. The behavior builders intentionally keep
their command and event handlers synchronous so ordinary call sites stay small
and avoid higher-ranked async closure complexity.

The sharded cart persistence example now demonstrates persistence recovery
after shard movement:

```bash
cargo run -p rakka-example-sharded-cart-persistence
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo run -p rakka-example-sharded-cart-persistence -- --postgres
```

See `docs/rakka-akka-parity-phase-4d6-recovery-after-movement.md` for the
boundary between shard coordinator recovery and entity persistence recovery.

## Phase 5

Phase 5 adds the Akka-style cluster extension, typed receptionist, and local
router facades. New service-discovery code should prefer `ServiceKey<M>`,
`Receptionist::get(&system)`, and `Routers::group(key)` over hand-maintained
actor-ref sets.

Before:

```rust
let workers = vec![worker_a, worker_b];
workers[index % workers.len()].tell(command)?;
```

After:

```rust
let key = ServiceKey::<WorkerCommand>::new("workers");
let receptionist = Receptionist::get(&system);
let _registration = receptionist.register(&key, worker)?;
let router = Routers::group(key).with_round_robin().spawn(&system, "workers")?;
router.tell(command)?;
```

Use `Routers::pool("worker", size, factory)` when the router should own local
routee actors. Use `with_consistent_hash(|message| key)` for stateless
key-sticky work. Use `ClusterSharding` instead when the key is durable entity
identity or when ownership movement, passivation, remembered entities, and
recovery are correctness requirements.

Cluster membership now has a compact facade:

```rust
let cluster = Cluster::get(&system);
cluster.manager().join_self()?;
let mut events = cluster
    .subscriptions()
    .subscribe(ClusterSubscriptionReplay::InitialState);
```

Tests should prefer the Phase 5 `rakka-testkit` helpers:
`expect_receptionist_listing_count`, `assert_receptionist_listing_contains`,
`assert_pool_routee_count`, `assert_group_routee_count`,
`expect_cluster_member_up`, and `assert_cluster_event_node`.

Clustered receptionist propagation now has two reviewable paths:
`ClusteredReceptionist::propagate_to` proves the deterministic in-process
listing model and group-router integration, while `rakka-remote` provides TCP
loopback propagation with transport-serializable remote service routees,
materialized local proxies, and normal group-router delivery.

## Phase 6

Phase 6 adds an Akka-shaped bounded stream facade over Rakka's existing
`StreamSink<T>` and `StreamSource<T>` runtime. New stream code should prefer
`Source<T>`, `Flow<I, O>`, and `Sink<T, M>` when the pipeline is easier to read
as a materialized stream.

Before:

```rust
let (sink, source) = bounded_channel(8)?;
sink.send("work".to_owned()).await?;
sink.drain()?;
let items = collect_stream_source(&source).await?;
```

After:

```rust
let items = Source::from_iter(["work".to_owned()])
    .map(|item| item.to_uppercase())
    .run_collect()
    .await?;
```

Low-level bounded handles remain valid and now have consuming facade
conversions:

```rust
let (sink, source) = bounded_channel(8)?;
sink.send("work".to_owned()).await?;
sink.drain()?;

let items = source.into_source().run_collect().await?;
let written = Source::from_iter(items).run_with(sink.into_sink()).await?;
```

Use explicit ack boundaries when an actor sink must provide back-pressure:

```rust
let delivered = Source::from_iter(commands)
    .run_with(Sink::actor_ref_with_ack(
        worker,
        AckProtocol::new("ack").with_timeout(Duration::from_secs(1)),
    ))
    .await?;
```

Process IO adapters have facade entry points for ordinary stream pipelines:

```rust
let (stdout, stdout_pump) =
    Source::process_stdout(&mut process, ProcessOutputConfig::default())?;
let chunks = stdout.run_collect().await?;
let bytes_read = stdout_pump.expect("stdout pump").await??;
```

Tests should prefer `rakka-testkit` stream probes over sleeps:

```rust
let (source, source_probe) = StreamTestKit::source_probe::<String>()?;
let run = tokio::spawn(async move { source.run_collect().await });
source_probe.send_next("one".to_owned()).await?;
source_probe.send_complete()?;
assert_eq!(run.await??, vec!["one".to_owned()]);
```

Run the self-contained stream facade example for finite operators, acked actor
sinks, process stdout, and probe usage:

```bash
cargo run -p rakka-example-streams
```
