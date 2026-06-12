# Rakka Akka Parity Phase 4D Follow-up Plan

Status: Draft for review
Date: 2026-06-12

## Purpose

This plan breaks the remaining Phase 4D durable coordinator work into
reviewable implementation slices. It builds on the first-pass durable
coordinator foundation:

- `ShardCoordinatorStore`
- `PersistedShardCoordinatorState`
- `InMemoryShardCoordinatorStore`
- coordinator recovery from `ShardOwnershipSnapshot`
- optional store wiring in `ClusterShardingRuntime`, `ClusterNodeRuntimeBuilder`,
  and `ClusterSharding`

Reference rationale:
`docs/rakka-akka-parity-phase-4-durable-coordinator-rationale.md`.

## Target Outcome

Rakka should support production-shaped cluster sharding where coordinator state,
leadership, remembered entity identity, and entity persistence work together:

```rust
let coordinator_store = PostgresShardCoordinatorStore::builder(pool)
    .with_namespace("shopping-prod")
    .migrate()
    .await?;

let lease = PostgresShardCoordinatorLease::builder(pool)
    .with_namespace("shopping-prod")
    .with_ttl(Duration::from_secs(15))
    .build();

let mut runtime = ClusterNodeRuntime::builder(local_node)
    .with_registry(registry)
    .with_shard_coordinator_store(coordinator_store)
    .with_shard_coordinator_lease(lease)
    .build()
    .await?;

let sharding = ClusterSharding::for_node_runtime(&system, &runtime)?;
sharding
    .init_remote(
        &mut runtime,
        Entity::of(cart_key.clone(), |context| CartEntity::event_sourced(context))
            .with_remembered_entities(RememberedEntities::enabled()),
    )
    .await?;
```

The final API does not need to match this sketch exactly. The important outcome
is that applications can opt into durable coordinator behavior through the same
high-level sharding surfaces without learning coordinator internals.

## Guiding Decisions

- Durable coordinator state remains control-plane state; entity events,
  snapshots, and durable state stay in `rakka-persistence`.
- PostgreSQL support should live in an adapter crate, preferably
  `rakka-sharding-postgres`, rather than adding a sharding dependency to the
  core sharding crate.
- Persistent stores should not require blocking database calls on Tokio runtime
  threads. If a synchronous convenience API remains, persistent backends must
  have async-first registration paths.
- Lease/fencing is separate from snapshot CAS. CAS prevents stale revisions;
  leases decide who is allowed to make coordinator decisions.
- Remembered entities should be opt-in and bounded. The default remains
  passivation-friendly, demand-started entities.
- Examples should demonstrate observable recovery after movement, not just that
  the APIs compile.

## Current Architectural Gap

The first-pass `ShardCoordinatorStore` is synchronous because it was introduced
to validate the model with an in-memory backend. PostgreSQL and most production
stores are async. Before adding a persistent backend, decide how durable
coordinator operations enter the runtime:

- Option A: add async store traits and async sharding registration/update APIs,
  while preserving sync APIs for ephemeral and in-memory usage.
- Option B: keep the sync trait and implement persistent stores through a
  dedicated blocking worker thread.
- Option C: replace the sync trait with future-returning methods and make the
  high-level durable sharding facade async-first.

Recommendation: use Option A first. It is additive, avoids blocking runtime
threads, and lets the existing sync facade remain pleasant for local tests and
default ephemeral sharding.

## Slice 4D1: Async Store Boundary and API Shape

Status: implemented.

Goal: make persistent coordinator backends possible without compromising the
current ergonomic sync facade.

Scope:

- Add an async-capable coordinator store contract, using the same boxed future
  style as `rakka-persistence::StoreFuture` instead of introducing
  `async_trait`.
- Keep `InMemoryShardCoordinatorStore` usable from both sync and async paths.
- Add async runtime operations for every path that may hit persistent storage:
  region registration, discovery application, heartbeat, leave, down, and tick.
- Add facade/node-runtime async variants only where persistent storage is
  involved, for example `init_async`, `init_remote_async`, and
  `register_entity_region_async`.
- Make sync APIs fail with a clear typed error if called with an async-only
  persistent store, rather than blocking implicitly.
- Document which APIs are sync-local conveniences and which are production
  durable paths.

Proposed API surface:

```rust
pub type CoordinatorStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = ShardingResult<T>> + Send + 'a>>;

pub trait AsyncShardCoordinatorStore: Debug + Send + Sync + 'static {
    fn backend_name(&self) -> &'static str;
    fn load<'a>(
        &'a self,
        entity_type: &'a EntityType,
    ) -> CoordinatorStoreFuture<'a, Option<PersistedShardCoordinatorState>>;
    fn compare_and_set<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
        state: PersistedShardCoordinatorState,
    ) -> CoordinatorStoreFuture<'a, PersistedShardCoordinatorState>;
    fn delete<'a>(
        &'a self,
        entity_type: &'a EntityType,
        expected_revision: u64,
    ) -> CoordinatorStoreFuture<'a, ()>;
}
```

Acceptance criteria:

- Existing sharding tests continue to pass through the current sync APIs.
- New async tests cover registration, recovery, and rebalance persistence using
  the in-memory store through the async contract.
- No persistent store implementation is required in this slice.
- Clippy and docs explain when to use sync versus async sharding registration.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-sharding
cargo clippy -p rakka-sharding --all-targets -- -D warnings
cargo doc -p rakka-sharding --no-deps
```

Implementation status:

- Added `CoordinatorStoreFuture` and `AsyncShardCoordinatorStore` using the same
  boxed-future style as persistence stores.
- Existing synchronous `ShardCoordinatorStore` implementations automatically
  satisfy the async contract.
- `ClusterShardingRuntime` can now be constructed with sync or async-only
  coordinator stores.
- Added async runtime APIs for region registration, discovery, heartbeat,
  leaving, downing, and failure-detection ticks.
- Added async node-runtime registration/update APIs and async coordinator-store
  builder hooks.
- Added async facade constructors plus `init_async`, `init_remote_async`,
  `init_remote_with_ask_async`, and async proxy registration.
- Sync APIs now fail with
  `ShardingError::AsyncCoordinatorStoreRequiresAsyncApi` when used with an
  async-only coordinator store.
- Added tests for sync rejection, async registration persistence, async
  recovery, async rebalance persistence, and async facade initialization.

## Slice 4D2: PostgreSQL Coordinator Store

Goal: add a production-shaped persistent coordinator snapshot backend.

Status: implemented.

Scope:

- Add `rakka-sharding-postgres` as an adapter crate, or explicitly decide to
  host the adapter in `rakka-persistence-postgres` with feature-gated
  `rakka-sharding` support. The preferred default is a new adapter crate.
- Add `PostgresShardCoordinatorStore` implementing the async coordinator store
  contract.
- Add migration SQL for coordinator snapshots.
- Add namespace support so multiple clusters/environments can share one
  PostgreSQL database safely.
- Store `PersistedShardCoordinatorState` with schema version, entity type,
  shard count, revision, allocation strategy name, serialized assignments, and
  update timestamp.
- Use compare-and-set SQL for writes:
  - insert only when `expected_revision == 0`,
  - update only when the stored revision matches `expected_revision`,
  - return `CoordinatorRevisionConflict` with the actual stored revision on
    conflict.
- Add read-side validation for persisted entity type and shard count before
  runtime recovery.
- Add optional migration helper and docs consistent with existing
  `rakka-persistence-postgres` migration style.

Implementation status:

- Added `rakka-sharding-postgres` as a dedicated adapter crate.
- Added `PostgresShardCoordinatorStore` and builder APIs over
  `AsyncShardCoordinatorStore`.
- Added `MIGRATION_SQL` for the `rakka_shard_coordinator_state` table with
  namespace, entity type, revision, shard count, allocation strategy, JSONB
  snapshot state, schema version, and update timestamp columns.
- Added namespace isolation with a default namespace and explicit
  `.with_namespace(...)` builder support.
- Added compare-and-set insert/update/delete SQL that returns
  `CoordinatorRevisionConflict` on stale revisions.
- Added read-side validation that rejects rows whose persisted snapshot does
  not match the row revision, shard count, or allocation strategy metadata.
- Added gated PostgreSQL tests for migration and round-trip writes, namespace
  isolation, conflict detection, deletes, and runtime recovery without
  unnecessary rewrites.
- Added CI/package/review documentation references for the new adapter crate.

Suggested schema:

```sql
CREATE TABLE IF NOT EXISTS rakka_shard_coordinator_state (
    namespace TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    number_of_shards INTEGER NOT NULL CHECK (number_of_shards > 0),
    allocation_strategy TEXT NOT NULL,
    state_json JSONB NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, entity_type)
);
```

Acceptance criteria:

- Gated PostgreSQL tests cover migration, load-missing, initial insert, CAS
  update, stale revision conflict, delete, namespace isolation, and snapshot
  recovery through `ClusterShardingRuntime`.
- Store tests are gated by `RAKKA_POSTGRES_TEST_DSN`, matching the existing
  PostgreSQL integration pattern.
- Runtime tests prove a recovered coordinator does not rewrite the snapshot when
  membership has not changed.
- Documentation includes a minimal configuration example.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-sharding
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-sharding-postgres -- --nocapture
cargo check --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Slice 4D3: Coordinator Leadership and Lease Semantics

Goal: prevent multiple coordinators from making ownership decisions for the
same entity type at the same time.

Status: implemented.

Scope:

- Add a lease abstraction independent of the snapshot store:

```rust
pub trait ShardCoordinatorLease: Debug + Send + Sync + 'static {
    fn lease_name(&self) -> &'static str;
    fn acquire<'a>(
        &'a self,
        entity_type: &'a EntityType,
        holder: &'a NodeId,
    ) -> CoordinatorLeaseFuture<'a, LeaseToken>;
    fn renew<'a>(&'a self, token: &'a LeaseToken) -> CoordinatorLeaseFuture<'a, ()>;
    fn release<'a>(&'a self, token: LeaseToken) -> CoordinatorLeaseFuture<'a, ()>;
}
```

- Add `LeaseToken` carrying namespace, entity type, holder node, fencing token,
  and expiry metadata.
- Require a valid lease before a runtime can reconcile and persist coordinator
  decisions for an entity type.
- Renew leases periodically from `ClusterNodeRuntime`.
- Stop publishing ownership changes when a lease is lost, expired, or stolen.
- Include the lease fencing token in persistent coordinator writes where the
  backend supports it.
- Add `InMemoryShardCoordinatorLease` for deterministic tests.
- Add `PostgresShardCoordinatorLease` using a PostgreSQL table and
  `expires_at`-based acquisition.

Implementation status:

- Added `ShardCoordinatorLease`, `CoordinatorLeaseFuture`, and `LeaseToken` to
  the sharding foundation.
- Added typed lease errors for rejected acquisition, lost/stale tokens, backend
  failures, and sync API use with async leases.
- Added `InMemoryShardCoordinatorLease` for deterministic tests and
  single-process experiments.
- Added runtime lease state so configured runtimes acquire or renew leadership
  before coordinator creation, reconciliation, persistence, handoff, or
  publication.
- Added explicit async renewal and release APIs on `ClusterShardingRuntime` and
  `ClusterNodeRuntime`.
- Added `ClusterNodeRuntimeBuilder::with_shard_coordinator_lease` and shared
  reference variants.
- Added high-level `ClusterSharding` constructors for async coordinator stores
  paired with leadership leases.
- Added `PostgresShardCoordinatorLease` with the
  `rakka_shard_coordinator_lease` table, namespace isolation,
  `expires_at`-based acquisition, renewal, release, and monotonically
  increasing fencing tokens.
- Added tests for acquire, renew, release, active-holder rejection, expiry,
  stolen lease/stale token rejection, runtime holder enforcement, and stale
  runtime publication prevention.

Suggested PostgreSQL lease schema:

```sql
CREATE TABLE IF NOT EXISTS rakka_shard_coordinator_lease (
    namespace TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    holder_node TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, entity_type)
);
```

Failure model:

- If lease acquisition fails, the node may host shard regions but must not act
  as coordinator for that entity type.
- If lease renewal fails, the runtime marks coordinator authority suspended and
  returns a typed leadership error for operations requiring reconciliation.
- If another node acquires a higher fencing token, stale snapshot writes fail
  even if a revision number happens to match.

Acceptance criteria:

- Tests cover acquire, renew, release, expiry, stolen lease, and stale fencing
  token rejection.
- Runtime tests prove only the lease holder can persist and publish new
  ownership revisions.
- Lost lease does not corrupt region owner caches with a partial update.
- Documentation explains the difference between revision CAS and leases.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-sharding
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-sharding-postgres -- --nocapture
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Slice 4D4: Remembered Entities Evaluation

Goal: decide whether and how Rakka should support Akka-style remembered
entities.

Status: implemented.

Decision: accepted for implementation as an opt-in, bounded Cluster Sharding
feature. See
`docs/rakka-akka-parity-phase-4-remembered-entities-decision.md`.

Evaluation questions:

- Should remembered entities be a parity target for v1-style high-level
  sharding, or a documented future feature?
- Should remembering happen on first routed message, explicit activation, or
  both?
- Should explicit passivation forget the entity, or should a separate stop
  protocol be required to remove it from the remembered set?
- What bounded-resource controls are required for large remembered sets?
- Should remembered entities be stored per shard, per entity type, or as an
  event log of start/stop decisions?
- How should remembered entities interact with event-sourced/durable-state
  recovery and with shard handoff buffering?

Recommended initial decision:

- Add remembered entities as opt-in.
- Remember on successful local activation.
- Forget only on explicit facade command, not ordinary idle passivation.
- Restart remembered entities lazily in bounded batches when a shard is
  acquired.
- Keep remembered entity storage separate from coordinator ownership snapshots.

Potential API:

```rust
let entity = Entity::of(key.clone(), |context| CartEntity::new(context))
    .with_remembered_entities(
        RememberedEntities::enabled()
            .with_start_batch_size(64)
            .with_store(remembered_store),
    );
```

Potential storage trait:

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
    ) -> RememberedStoreFuture<'a, bool>;
    fn remembered_for_shard<'a>(
        &'a self,
        shard: &'a ShardKey,
    ) -> RememberedStoreFuture<'a, Vec<EntityId>>;
}
```

Acceptance criteria for the evaluation slice:

- A short decision record is added to `docs/` with the recommended semantics.
- If accepted, a follow-on implementation plan is added for remembered entity
  storage, region activation, passivation semantics, and tests.
- If deferred, docs state the gap against Akka and the recommended workaround:
  demand-started persistent entities plus application-level activation indexes.

Implementation status:

- Added the remembered entities decision record.
- Accepted remembered entities as opt-in and bounded.
- Chose remember-after-successful-local-activation semantics.
- Chose explicit forget APIs instead of forget-on-passivation.
- Chose per-shard remembered identity storage outside coordinator snapshots and
  outside entity persistence.
- Added
  `docs/plans/rakka-akka-parity-phase-4d5-remembered-entities-implementation-plan.md`
  as the follow-on implementation plan.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-sharding
```

## Slice 4D5: Remembered Entities Implementation

Goal: implement remembered entities if Slice 4D4 accepts the feature.

Status: implemented.

Scope:

- Add `RememberedEntities` settings to `Entity`.
- Add in-memory remembered entity store for tests.
- Add PostgreSQL remembered entity store if the PostgreSQL coordinator adapter
  crate exists.
- Record remembered entity ids only after successful local activation.
- Load remembered ids for a shard during shard acquisition and start entities in
  bounded batches.
- Add explicit forget APIs on the facade, for example
  `ClusterSharding::forget_entity(&key, entity_id)`.
- Make idle passivation stop local actors without removing remembered identity.
- Ensure graceful handoff drains old owners before the new owner starts
  remembered entities.

Suggested PostgreSQL schema:

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

Acceptance criteria:

- Tests cover remember on activation, forget, recovery after runtime restart,
  reacquire after handoff, idle passivation restart, and bounded activation.
- Remembered entity recovery does not bypass entity persistence recovery.
- Docs warn about high-cardinality remembered sets and describe batching.

Implementation status:

- Added remembered entity settings, async store trait, in-memory store, replay
  settings, and replay summaries to `rakka-sharding`.
- Added facade opt-in and explicit async forget APIs.
- Added activation recording after successful local activation/reuse.
- Added lazy replay on ownership refresh and shard acquisition.
- Added PostgreSQL remembered entity store and gated integration tests.
- Updated the cluster sharding doc and multi-node sharding example.

## Slice 4D6: Recovery-after-Movement Examples

Goal: prove the durable coordinator and entity persistence story with examples
that move ownership between nodes.

Scope:

- Add or rewrite an example that combines:
  - high-level `ClusterSharding` facade,
  - remote sharding,
  - durable coordinator store,
  - event-sourced or durable-state entity persistence,
  - graceful leave or failover movement.
- Keep one deterministic in-memory example for fast local runs.
- Add a gated PostgreSQL version that uses persistent entity state and
  persistent coordinator state.
- Show the expected sequence in logs:
  - node A owns shard,
  - entity writes state,
  - shard moves to node B,
  - node B recovers entity state by `PersistenceId`,
  - coordinator ownership revision remains recoverable.
- Add docs that explain what is recovered by sharding versus by persistence.

Candidate examples:

- `examples/sharded-cart-persistence`: upgrade from direct spawned actors to
  facade-created sharded entities.
- `examples/multi-node-sharding`: add a durable coordinator mode and a
  persisted cart command path.
- New `examples/sharded-cart-movement` if combining both concerns makes the
  existing examples too dense.

Acceptance criteria:

- The fast example runs without external services.
- The PostgreSQL example is gated behind `RAKKA_POSTGRES_TEST_DSN` or explicit
  CLI configuration.
- Tests or compatibility examples assert that state written before movement is
  visible after movement.
- Product docs link the example from the Phase 4 sharding page and the typed
  persistence docs.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-sharding
cargo test -p rakka-persistence
cargo check --workspace --all-features
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  cargo test -p rakka-sharding-postgres -- --nocapture
```

## Cross-cutting Test Matrix

Each implementation slice should update tests across the relevant surface:

| Concern | Required coverage |
| --- | --- |
| Store correctness | load missing, insert, update, stale CAS, delete, namespace isolation |
| Runtime recovery | first registration, restart recovery, no-op recovery, config mismatch |
| Rebalance persistence | leave, down, unreachable tick, join rebalance, custom allocation |
| Leadership | acquire, renew, expire, stolen lease, lost lease during publish |
| Region safety | publish only after durable write, no stale ownership after failed write |
| Remembered entities | activation, forget, passivation, handoff, restart, bounded replay |
| Persistence movement | state before move is visible after move |
| Docs/API | rustdoc for new public types, examples compile, plan/docs updated |

## Risks and Mitigations

- Async API sprawl: keep sync local APIs, but make durable production paths
  async-first and clearly named.
- Blocking database operations in actors: do not hide blocking DB work behind
  the current sync trait for production backends.
- Split-brain coordinators: CAS alone is insufficient; add leases before
  claiming production multi-coordinator safety.
- Remembered entity cardinality: make the feature opt-in, batched, and
  explicitly documented.
- Crate dependency cycles: keep `rakka-sharding` free of persistence/PostgreSQL
  dependencies; put database adapters in adapter crates.
- Schema evolution: include namespace and schema version from the first
  PostgreSQL migration.

## Recommended Implementation Order

1. Slice 4D1: async store boundary and API shape.
2. Slice 4D2: PostgreSQL coordinator store.
3. Slice 4D3: coordinator leadership and leases.
4. Slice 4D4: remembered entities evaluation.
5. Slice 4D5: remembered entities implementation, only if accepted.
6. Slice 4D6: recovery-after-movement examples and docs.

This order keeps durable storage and leadership correct before adding remembered
entities, and it leaves the examples until the behavior they demonstrate is
real enough to be useful.
