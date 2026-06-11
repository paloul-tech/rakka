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
