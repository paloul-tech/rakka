# Rakka Akka Parity Phase 4D5 Remembered Entities Implementation Plan

Status: ready for implementation
Date: 2026-06-12

## Purpose

Implement remembered entities after the Slice 4D4 decision accepted the feature
as an opt-in Cluster Sharding parity target.

Decision record:
`docs/rakka-akka-parity-phase-4-remembered-entities-decision.md`.

## Target Semantics

- Remember after successful local activation.
- Do not remember failed spawn attempts.
- Do not forget on idle passivation.
- Do not forget on ordinary explicit passivation.
- Forget only through explicit facade APIs.
- Replay remembered ids lazily and in bounded batches when a shard is acquired.
- Keep remembered identity storage separate from coordinator ownership
  snapshots and from entity persistence.

## Slice 1: Foundation Types

Add `remembered_entities.rs` to `rakka-sharding`.

Public types:

- `RememberedEntities`
- `RememberedEntityStore`
- `RememberedStoreFuture`
- `InMemoryRememberedEntityStore`
- `RememberedEntityReplay`
- `RememberedEntityReplaySettings`
- `RememberedEntityError` if the existing `ShardingError` variants would be
  too broad

Initial API:

```rust
pub struct RememberedEntities {
    enabled: bool,
    start_batch_size: usize,
    start_batch_delay: Duration,
    store: Arc<dyn RememberedEntityStore>,
}

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

Default settings:

- disabled by default,
- `start_batch_size = 64`,
- `start_batch_delay = Duration::ZERO`,
- in-memory store available only when explicitly configured.

Tests:

- default settings are disabled,
- batch size must be non-zero,
- in-memory store remembers, forgets, lists by shard, and isolates entity
  types/shards.

## Slice 2: Entity Configuration

Add remembered settings to `Entity`.

API:

```rust
Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_remembered_entities(RememberedEntities::enabled().with_store(store));
```

Registration state should expose:

- `remembered_entities_enabled()`,
- `remembered_start_batch_size()`,
- `remembered_start_batch_delay()`.

Repository hygiene should keep store internals out of `rakka::prelude`, but the
high-level `RememberedEntities` settings type may be prelude-worthy once the API
settles.

Tests:

- settings propagate from `Entity` to `EntityTypeRegistrationState`,
- proxy-only registration rejects remembered entities unless a concrete local
  activation path exists,
- async init is required when the remembered store is async-only.

## Slice 3: Remember On Activation

Instrument `LocalEntityRoute` and `ShardRegion` so successful local activation
can be observed.

Preferred shape:

- Keep route spawning synchronous where it is today.
- Add an activation callback or hook that returns an async future only after the
  actor was confirmed local.
- For a first pass, perform remembering from the facade/region wrapper around
  successful `tell`/`ask` local activation.

Important rule:

- Remember only after the route can prove the actor exists locally.

Tests:

- first routed message remembers the entity,
- repeated messages do not duplicate store records,
- failed spawn does not remember,
- remote-owned routes do not remember locally.

## Slice 4: Forget APIs And Passivation Semantics

Add facade methods:

```rust
pub async fn forget_entity<M>(
    &self,
    key: &EntityTypeKey<M>,
    entity_id: impl Into<String>,
) -> ClusterShardingResult<bool>;

pub async fn forget_entity_id<M>(
    &self,
    key: &EntityTypeKey<M>,
    entity_id: &EntityId,
) -> ClusterShardingResult<bool>;
```

Behavior:

- Forget removes the id from the remembered store.
- Forget should passivate the local entity if it is currently active on this
  node.
- Idle passivation does not call forget.
- Existing `passivate_entity` does not call forget.

Tests:

- explicit forget removes the remembered id,
- forget passivates an active local entity,
- idle passivation leaves the remembered id,
- explicit passivation leaves the remembered id.

## Slice 5: Replay On Shard Acquisition

When a shard is acquired locally:

1. Load remembered ids for the shard.
2. Start entities through the normal local route/factory path.
3. Process ids in batches.
4. Keep route ownership and handoff state authoritative.

Avoid starting remembered entities while the shard is still draining or
transferring. Replay begins only in the acquired state.

Tests:

- restart recovers remembered ids for owned shards,
- graceful handoff starts remembered entities on the new owner after acquire,
- old owner does not restart remembered entities while draining,
- replay respects batch size,
- replay failures are surfaced and counted without corrupting ownership.

## Slice 6: PostgreSQL Remembered Entity Store

Add `PostgresRememberedEntityStore` to `rakka-sharding-postgres`.

Migration:

```sql
CREATE TABLE IF NOT EXISTS rakka_shard_remembered_entities (
    namespace TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    shard_id INTEGER NOT NULL CHECK (shard_id >= 0),
    entity_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, entity_type, shard_id, entity_id)
);
```

Required behavior:

- idempotent `remember`,
- idempotent `forget`,
- stable sorted listing for deterministic replay,
- namespace isolation,
- migration protected by the existing advisory migration lock.

Gated tests:

```bash
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-sharding-postgres -- --nocapture
```

## Slice 7: Docs And Examples

Update:

- `docs/rakka-akka-parity-phase-4-cluster-sharding.md`,
- `docs/rakka-api-boundary-inventory.md`,
- `docs/rakka-v1-api-review.md`,
- `docs/rakka-v1-reliability-boundaries.md` if remembered entity guarantees
  need an operator-facing statement.

Docs must warn:

- remembered entities are not persistence,
- high-cardinality sets can slow shard acquisition,
- passivation does not forget,
- use persistence for entity state recovery.

Example candidates:

- add an in-memory remembered cart scenario to `examples/multi-node-sharding`,
- use the Postgres store in the later recovery-after-movement example.

## Validation

Required:

```bash
cargo fmt --all -- --check
cargo test -p rakka-sharding
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc -p rakka-sharding --no-deps
```

Gated:

```bash
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-sharding-postgres -- --nocapture
```

## Open Implementation Questions

- Should replay be synchronous with registration/acquisition or scheduled on a
  background task owned by the facade?
- Should replay expose per-shard progress snapshots?
- Should the first implementation include a hard maximum remembered ids per
  shard, or only batch controls and documentation?
- Should `RememberedEntities` enter `rakka::prelude` immediately, or wait until
  after the API is validated by examples?
