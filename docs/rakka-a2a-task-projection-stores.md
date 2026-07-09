# Rakka A2A Task Projection Stores

This document explains the two `A2ATaskProjectionStore` implementations in
`rakka-a2a` — `InMemoryA2ATaskProjectionStore` and
`PostgresA2ATaskProjectionStore` — what they do and do not affect, how their
behavior differs across a cluster, and how to choose between them.

## Summary

The task projection store is a **query/observability read model over durable
run state**, not a correctness component. Correctness always comes from durable
run plus inbox/outbox state (`rakka_durable_state`), never from the projection.
See `crates/rakka-a2a/src/projection.rs` (module docs) and
[Rakka V1 Reliability Boundaries](./rakka-v1-reliability-boundaries.md).

Because of this, the choice of projection store **does not change** delivery
semantics, task durability, owner failover, or recovery correctness. It changes
only the *read surface*: how complete cluster-wide queries are, whether the
read model survives a restart, whether external tools can read it, and how much
per-pod memory it costs.

Single-task operations behave the same under either store. `get_task`, `send`,
`cancel`, and opening a stream all route to the shard owner
(`A2ARunRouter` → `route_for_projection` in `handler.rs`), so
`GET /a2a/tasks/$TASK` on any node is correct regardless of which store backs
the read model. This is why the in-memory store is sufficient for single-node
and demo deployments.

## Shared contract

Both stores implement the same `A2ATaskProjectionStore` (and
`A2ATaskEventWatcher`) trait, with identical semantics for:

- Tenant scoping refusal (`TenantRequired`) when the store requires it.
- Snapshot-only bootstrap of unknown tasks; orphan (non-snapshot) events for
  unknown tasks are rejected and not recorded.
- Bounded per-task event retention (`A2ATaskEventRetention`), preserving the
  newest snapshot and reporting `ReplayWindowExpired` / `InvalidReplayCursor`
  instead of a silent replay gap.
- Deterministic list pagination and tenant/context/status/timestamp filtering.

The stores differ in three trait signals — `supports_shared_replay()`,
`requires_tenant_scope()`, and durability of the backing storage — and those
signals drive the behavior below.

## Where they differ

| Capability | `InMemoryA2ATaskProjectionStore` | `PostgresA2ATaskProjectionStore` |
| --- | --- | --- |
| `get_task` / `send` / `cancel` cross-node | Correct — routed to shard owner | Correct — routed to shard owner |
| `ListTasks` completeness | Per-pod; drifts after boot (see below) | Complete and consistent cluster-wide |
| Cross-node stream replay | Must hop to the shard owner (`supports_shared_replay() == false`) | Served from the shared durable log, no owner hop (`== true`) |
| Survives pod restart | No — held in heap; rebuilt on boot | Yes — durable rows persist |
| External SQL / BI / ops access | No — only via the A2A API | Yes — query `rakka_a2a_tasks` directly |
| Per-pod memory | Every projection + bounded event log in heap | Offloaded to PostgreSQL, paginated |
| Multi-tenant scoping | `local()` permits unscoped reads; `tenant_scoped()` requires a tenant | Always tenant-scoped; refuses `tenant = None` |
| Backend name (telemetry) | `memory` | `postgres` |

### The `ListTasks` drift (the sharpest difference)

`list_tasks` is **not routed** to an owner — a list query has no single owner
to ask, so the handler reads the local `task_store.list()` directly. With the
in-memory store this produces a per-pod, drifting view:

- On boot, `RakkaA2AService::recover_task_projections()` walks **every**
  `agent-run:*` id in the shared durable-state table and rebuilds a projection
  for each into local heap. Immediately after startup a pod's list is therefore
  fairly complete.
- But recovery runs **once, at boot**. After that there is no periodic
  re-sync. A task created on the pod that owns its shard is appended only to
  *that* pod's in-memory store; other pods do not observe it until they
  themselves restart. So each pod's `ListTasks` answer **diverges over time**
  and depends on which pod served the request.

With the PostgreSQL store, `list`, `projection`, and `replay_events` read the
shared tables, so every pod returns the same complete, current result — no
drift, and no restart needed to converge.

## When to prefer `PostgresA2ATaskProjectionStore`

Choose the PostgreSQL store when any of these hold:

1. **Cluster-wide `ListTasks` / task search must be complete and consistent**
   regardless of which pod answers. This is the single biggest reason.
2. **External systems need to read task state** — dashboards, admin UIs,
   analytics, reconciliation — without going through the A2A API or waking
   sharded entities. SQL, joins, indexes, and retention become available.
3. **The read model must survive restarts** as a durable, queryable view
   rather than a self-healing in-heap cache.
4. **Cross-node streaming at scale**, where non-owner stream reconnects should
   not repeatedly poll the shard owner for replay; the shared log serves it
   locally.
5. **High total task volume**, where materializing every run's projection into
   every pod's heap at boot is a memory and startup-cost problem.
6. **Real multi-tenancy**, where tenant-scoped reads should be enforced by the
   store itself rather than relying on the permissive `local()` mode.

## When `InMemoryA2ATaskProjectionStore` is sufficient

- Single-node deployments, or small demo/dev clusters, where correctness never
  depends on the read model.
- All durable, interesting behavior (durable acceptance, owner routing,
  recovery, failover, drain) is still exercised, because it flows through the
  durable run/inbox/outbox state, which lives in PostgreSQL independent of the
  projection store.
- Fewer moving parts: no additional DDL/migration, no projection write on the
  request hot path, and the lowest local-read latency.
- It makes the boundary explicit: the projection is a **rebuildable read model,
  not the source of truth.**

The clustered A2A example (`examples/clustered-sharded-entity-a2a-agents`) uses
`InMemoryA2ATaskProjectionStore::local()` for exactly these reasons, even when
`RAKKA_PERSISTENCE=postgres` persists the durable run/workflow/push-config
state. As a result that example has **no `rakka_a2a_tasks` table** — inspect its
durable model via `rakka_durable_state` instead.

## Cost of the PostgreSQL store

The PostgreSQL store is opt-in, not the default, because it has real costs:

- Every task state transition also writes a row and an event to PostgreSQL on
  the request hot path (write amplification).
- It adds connection and query latency to reads and writes.
- It becomes a shared dependency that must be provisioned, sized, and
  maintained; its retention and indexes are operational concerns.

For high write throughput these costs are significant, so enable it when the
aggregate query surface genuinely requires it — not by default.

## Schema and inspection

`PostgresA2ATaskProjectionStore::migrate()` creates two tables under an advisory
lock (idempotent, additive-only within a release):

- `rakka_a2a_tasks` — one projection row per `(tenant, task_id)`.
- `rakka_a2a_task_events` — the bounded public event log per task.

Inspect a deployment that uses the PostgreSQL store with, for example:

```sh
psql -U postgres -c '\dt' \
  -c 'select tenant, task_id, status, projection_revision from rakka_a2a_tasks;'
```

A deployment that uses the in-memory store has neither table; its durable model
is `rakka_durable_state` (keyed by `agent-run:`, `workflow:`, and
`a2a-push-config:` id prefixes).

## See also

- [Rakka V1 Reliability Boundaries](./rakka-v1-reliability-boundaries.md) —
  why the read model is never the correctness source.
- [Rakka API Boundary Inventory](./rakka-api-boundary-inventory.md) —
  `rakka-a2a` is an Adapter-tier crate.
- `examples/clustered-sharded-entity-a2a-agents/doc/kubernetes-testing.md` —
  observing the durable model in the example deployment.
