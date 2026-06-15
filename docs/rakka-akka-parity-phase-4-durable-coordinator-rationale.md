# Rakka Akka Parity Phase 4D: Durable Coordinator Rationale

This supplemental note captures the reasoning behind adding durable shard
coordinator state to Phase 4 of the Rakka/Akka parity plan.

## What The Coordinator Owns

The shard coordinator is control-plane state. It records which cluster node owns
each shard for an entity type, plus a monotonically increasing ownership
revision. It does not store entity business state, event-sourced state, durable
state, snapshots, or messages.

That distinction matters because durable coordinator storage should not replace
typed persistence. It preserves shard ownership decisions so the sharding layer
can recover cleanly after coordinator restart, process restart, or control-plane
handoff.

## Why Make It Durable

Durable coordinator state gives Rakka several practical advantages:

- Less avoidable shard churn after coordinator restart. A recovered coordinator
  can publish the same ownership map instead of recomputing all shard owners
  from scratch.
- Clear revision fencing. Compare-and-set writes make stale coordinators fail
  visibly instead of silently overwriting a newer ownership decision.
- Preservation of allocation decisions from custom strategies. If a strategy
  made a non-modulo placement decision, restart should not erase it.
- Cleaner failover behavior. Regions can converge on the last known ownership
  revision before applying membership changes.
- A foundation for remembered entities and persistence recovery examples after
  shard movement.

The important usability point is that applications should not need to learn a
new sharding model. Durable coordinator storage should be an optional backend
hook on the same `ClusterShardingRuntime`, `ClusterNodeRuntimeBuilder`, and
`ClusterSharding` facade APIs.

## Akka Comparison

Akka Cluster Sharding has a shard coordinator that owns shard allocation state.
Akka historically supported coordinator state through persistence and also uses
cluster-replicated/distributed state for coordinator data depending on the
configured mode and Akka version. The conceptual parity target for Rakka is:

- coordinator ownership is recoverable,
- coordinator writes are fenced by revision,
- regions observe consistent ownership revisions,
- entity persistence remains independent from coordinator persistence.

Rakka does not need to copy Akka's exact storage implementation to reach user
parity. A Rust-native `ShardCoordinatorStore` trait gives Rakka a stable API for
in-memory tests, single-process use, and later PostgreSQL or persistence-backed
stores.

## First-Pass Scope

Phase 4D starts with:

- `ShardCoordinatorStore`,
- `PersistedShardCoordinatorState`,
- `InMemoryShardCoordinatorStore`,
- recovery of `ShardCoordinator` from `ShardOwnershipSnapshot`,
- optional durable store wiring in the low-level runtime, node runtime builder,
  and high-level facade,
- tests for revision conflicts, persisted registration, recovery, and snapshot
  configuration mismatch.

This first pass intentionally keeps the store synchronous and sharding-owned.
That keeps `rakka-sharding` free of a dependency cycle with `rakka-persistence`
while leaving room for persistent backends to implement the same trait.

## Remaining Work

The next durability steps are:

- persistent coordinator store implementations, starting with PostgreSQL or a
  persistence-adapter bridge,
- operational examples that combine remote sharding, entity persistence, and
  recovery after shard movement,
- remembered-entity semantics if Rakka decides to match that Akka feature,
- coordinator leadership/lease semantics for multi-coordinator deployments.
