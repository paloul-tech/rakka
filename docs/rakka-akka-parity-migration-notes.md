# Rakka Akka Parity Migration Notes

Status: Phase 1 draft
Date: 2026-06-11

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

This first Phase 3 slice is storage and testkit infrastructure. The
`EventSourcedActor` adapter can already persist tagged events, recover from the
latest matching snapshot, replay later journal entries, and run post-commit side
effects:

```rust
let actor = spawn_event_sourced_actor(
    &system,
    "cart",
    CartActor::new(persistence_id),
    kit.journal(),
    kit.snapshots(),
)?;
```

The Akka-named `EventSourcedBehavior` and `DurableStateBehavior` builders,
reply-effect/stash/signal polish, query streams, and PostgreSQL journal/snapshot
plugins remain planned follow-up work.
