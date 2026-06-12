# Rakka Phase 4D6: Recovery After Movement

Slice 4D6 proves the combined control-plane and entity-persistence story for
cluster sharding.

The runnable proof is `examples/sharded-cart-persistence`. It now creates two
logical cluster nodes with the high-level `ClusterSharding` facade, starts an
event-sourced cart entity on the node that owns the cart shard, gracefully moves
that shard to the second node, and verifies the second node recovers the cart by
the same `PersistenceId`.

## Run It

Fast in-memory coordinator and persistence stores:

```bash
cargo run -p rakka-example-sharded-cart-persistence
```

PostgreSQL coordinator, journal, and snapshot stores:

```bash
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo run -p rakka-example-sharded-cart-persistence -- --postgres
```

The PostgreSQL path uses a unique coordinator namespace and entity type per run
so repeated local executions do not collide with prior persisted events.

## Expected Sequence

The example prints the sequence Slice 4D6 needs to make visible:

```text
Rakka sharded cart movement (in-memory) used entity type CartMovement and persistence id CartMovement|cart-0.
node A initially owned cart-0 on shard N and wrote cart total 2.
ownership moved from rakka-0#uid-a to rakka-1#uid-b at coordinator revision N.
node B recovered cart total 2 from persistence; persisted coordinator revision N was reloadable.
```

The exact shard and revision numbers may change as sharding policies evolve.

## What Sharding Recovers

Sharding owns control-plane placement:

- entity type to shard mapping;
- shard owner cache refresh;
- graceful handoff from the leaving owner;
- durable coordinator ownership revision reload.

The durable coordinator store does not contain cart items. It only stores the
ownership snapshot and revision needed for a restarted coordinator to resume
from the last accepted shard-placement decision.

## What Persistence Recovers

Typed persistence owns entity data:

- `CartEntity` is a sharded actor facade;
- the facade starts a child `EventSourcedBehavior`;
- the child uses `PersistenceId::of(entity_type, entity_id)`;
- node B starts the same persistence id after movement and recovers the cart
  total from the event journal and snapshots.

This keeps coordinator durability separate from entity state durability, which
matches Akka's conceptual split between cluster sharding placement and typed
persistence recovery.

## Boundary

This example keeps cart asks local before and after movement because the command
protocol carries `ReplyTo`, which is a local one-shot reply capability. Use
`examples/multi-node-sharding` and `docs/rakka-phase-3-remote-sharding.md` for
the remote transport, serialization, and TCP loopback proof.

Together, the two examples cover the current parity story:

- remote sharding routes encoded entity messages across node runtimes;
- recovery-after-movement proves durable coordinator state and persistent entity
  state survive ownership changes.
