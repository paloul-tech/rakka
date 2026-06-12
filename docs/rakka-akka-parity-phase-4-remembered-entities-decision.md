# Rakka Phase 4 Remembered Entities Decision

Status: accepted for implementation
Date: 2026-06-12

## Context

Akka Cluster Sharding includes remembered entities so entities that were active
before movement, restart, or rebalance can be started again when their shard is
owned. Rakka currently supports demand-started sharded entities, idle
passivation, explicit passivation buffering, durable coordinator snapshots, and
typed persistence, but it does not yet retain the identity of activated
entities as sharding control-plane data.

Remembered entities are useful for workloads where entity liveness itself is
important, not only entity state. Examples include projections driven by entity
timers, long-lived sessions, subscriptions, process-backed workers, or entities
that must resume local side effects after shard movement.

Remembered entities are also easy to misuse. A large remembered set can create a
restart storm, turn passivation into an illusion, or make storage cardinality
unbounded. Rakka should therefore implement the feature as explicit,
bounded, and separate from entity persistence.

## Decision

Rakka will support remembered entities as an opt-in Cluster Sharding feature.

The accepted semantics are:

- Remember an entity only after successful local activation.
- Do not remember failed spawn attempts or messages that are rejected before
  local activation.
- Do not forget an entity during idle passivation or ordinary explicit
  passivation.
- Forget an entity only through an explicit facade API.
- Restart remembered entities lazily when a shard is acquired.
- Bound replay with configurable batch size and pacing.
- Store remembered identities per shard, separate from coordinator ownership
  snapshots.
- Keep entity state recovery in `rakka-persistence`; remembered entities only
  decide which entity ids should be activated again.

This keeps the default Rakka model passivation-friendly and demand-started, and
lets applications opt into Akka-style liveness recovery when the tradeoff is
worth it.

## API Direction

The preferred high-level API is:

```rust
let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_remembered_entities(
        RememberedEntities::enabled()
            .with_start_batch_size(64)
            .with_store(remembered_store),
    );
```

The facade should expose explicit forget operations:

```rust
sharding.forget_entity(&key, "cart-1").await?;
sharding.forget_entity_id(&key, &entity_id).await?;
```

Remembered entity stores should be async-first:

```rust
pub trait RememberedEntityStore: Debug + Send + Sync + 'static {
    fn remember<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, ()>;

    fn forget<'a>(
        &'a self,
        shard: &'a ShardKey,
        entity_id: &'a EntityId,
    ) -> RememberedStoreFuture<'a, ()>;

    fn remembered_for_shard<'a>(
        &'a self,
        shard: &'a ShardKey,
    ) -> RememberedStoreFuture<'a, Vec<EntityId>>;
}
```

The implementation should provide an in-memory store in `rakka-sharding` and a
PostgreSQL store in `rakka-sharding-postgres`.

## Activation Semantics

Remembering happens after the local route confirms that an entity actor was
created or already existed locally for the owned shard. This avoids recording
entities that never actually became active.

On shard acquisition, remembered ids are loaded for that shard and activated in
bounded batches. Activation should use the normal entity factory path so
event-sourced and durable-state entities recover through their persistence
behavior. Remembered replay must not invent persistence state or bypass
recovery signals.

On graceful handoff, the old owner drains and stops local entities first. The
new owner starts remembered entities only after the shard is acquired.

## Passivation And Forget

Idle passivation remains a local resource-management mechanism. It stops a
local actor but does not remove the entity id from the remembered set.

Explicit passivation also stops the local actor but does not forget by default.
Applications that want to remove identity from the remembered set must call the
explicit forget API. This avoids ambiguous behavior where a stop command could
mean either "sleep for now" or "delete this entity from remembered identity."

## Storage Model

Remembered identity should be stored by `(namespace, entity_type, shard_id,
entity_id)` in persistent adapters. This keeps lookups aligned with shard
acquisition and avoids bloating the durable coordinator ownership snapshot.

Remembered entity data is control-plane identity, not entity business state.
It should not live in the event journal, snapshot store, or durable-state table.

## Resource Controls

The feature must expose defaults and controls that make large remembered sets
safe:

- start batch size,
- delay or yield between batches,
- maximum ids loaded per shard or an explicit documented absence of such a
  limit,
- metrics for replayed, pending, forgotten, and failed remembered activations,
- clear warnings that high-cardinality remembered sets can slow recovery.

## Akka Parity Position

This closes a meaningful Akka Cluster Sharding parity gap, but Rakka should keep
the feature narrower than Akka at first:

- opt-in only,
- async store contract only,
- no default remembered entities,
- no automatic forget on passivation,
- no coupling to the coordinator snapshot.

## Alternatives Rejected

Deferring remembered entities would keep Rakka simpler but leave applications to
build activation indexes manually. That workaround is acceptable today but not
parity-equivalent.

Remembering on first routed message before activation was rejected because it
can persist identities that fail to spawn or are rejected by ownership checks.

Forgetting on passivation was rejected because passivation is already used as a
local memory-management tool.

Storing remembered ids inside coordinator ownership snapshots was rejected
because ownership changes and entity identity cardinality have very different
scaling behavior.

## Follow-on Work

The implementation plan is
`docs/plans/rakka-akka-parity-phase-4d5-remembered-entities-implementation-plan.md`.
