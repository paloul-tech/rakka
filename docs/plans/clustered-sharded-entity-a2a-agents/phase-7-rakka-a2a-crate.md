# Phase 7 Rakka A2A Crate Extraction

Status: planning draft
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
Source example: `examples/clustered-sharded-entity-a2a-agents/`

## Goal

Introduce a reusable `rakka-a2a` crate that turns the Phase 6
`clustered-sharded-entity-a2a-agents` incubator into a production-ready A2A
adapter for durable Rakka agent workflow runs.

The crate should own the generic A2A protocol integration, durable request
handler, task projection, streaming replay, push notification persistence and
dispatch scheduling, sharded run owner protocol, operational telemetry, and
PostgreSQL projection migrations. The example should become a thin product
composition that supplies a demo workflow, environment configuration,
discovery, manifests, and local run instructions.

## Evaluation Summary

The Phase 6 example has the right production shape:

- A2A REST and JSON-RPC ingress is load-balanced and can run on every node.
- Public A2A traffic is separated from private Rakka remoting.
- A2A `Task.id` maps to `AgentRunId` and the sharded entity id.
- Public commands are acknowledged only after durable inbox acceptance.
- Owner-only work crosses remoting through a versioned, remote-safe
  `A2ARunRequest`/`A2ARunResponse` protocol, not local actor messages.
- The owner entity is a small shell around a local `AgentRunActor`.
- Streams use bounded admission, replay cursors, heartbeat events, and normal
  disconnect/reconnect semantics.
- Push configs are durably stored with credential redaction, and push work is
  scheduled through the durable outbox rather than executed in HTTP handlers.
- Kubernetes topology, drain, readiness, self-fencing, and failure injection are
  documented.

The example also shows the boundaries that must change before reuse:

- The task projection and event log are process-local. Production reuse needs a
  shared projection store, with PostgreSQL as the first durable implementation.
- Cross-node stream replay currently depends on owner polling when the serving
  node is not the owner. Durable projection replay should be primary, with
  owner polling only as an optimization or compatibility fallback.
- Push delivery is scheduled but no production HTTP webhook dispatcher is
  included. The crate needs an explicit push dispatcher boundary and retry
  visibility before advertising `push_notifications=true`.
- The agent card is static and demo-specific. The crate needs a dynamic card
  producer driven by service metadata, workflow registrations, transport
  routes, and security configuration.
- The handler assumes one workflow. Production adapters need a workflow catalog
  and stable workflow selection policy.
- The durable stores and file persistence wrappers are example-owned. The crate
  should depend on Rakka component crates and expose store traits plus memory
  and PostgreSQL implementations.
- Debug helpers such as `HeaderObserver`, demo config, file discovery, local
  state directories, Docker image names, and Kubernetes YAML should remain
  outside the reusable crate.

## Production Boundary

`rakka-a2a` must preserve Rakka's existing reliability contract:

- Rakka actors, remoting, and sharding remain at-most-once delivery surfaces.
- Accepted A2A work is durable only after the workflow inbox write succeeds.
- External model calls, tool calls, peer A2A calls, webhooks, and push
  notifications are at-least-once unless the target system participates in
  idempotency.
- Rakka remoting remains trusted private cluster traffic, never public A2A
  transport.
- Runtime events and task projections are query/observability surfaces, not the
  source of correctness. Durable run state plus durable inbox/outbox state
  remain authoritative.
- Tenant and principal identity are part of the durable command boundary.
- The crate must never persist resolved credentials or secret material in
  plans, state, outbox effects, task events, logs, metrics, snapshots, or
  indexes. Store logical credential binding references or redacted
  secret-presence metadata only.

## Proposed Crate Shape

Add `crates/rakka-a2a` with `default = []`. Prefer component crate
dependencies over depending on the top-level `rakka` facade.

In the API boundary inventory, `rakka-a2a` is an Adapter-tier crate: a protocol
and edge adapter over HTTP, PostgreSQL, and sharding. Record it as Adapter when
updating `docs/rakka-api-boundary-inventory.md` in Slice 7.10.

Initial features:

- `server`: A2A SDK `RequestHandler`, REST, JSON-RPC, and agent-card router
  helpers.
- `sharding`: sharded owner entity, remote-safe protocol, router, and codec
  registration. Depends on `rakka-sharding`, `rakka-remote`, and
  `rakka-cluster` where needed.
- `postgres`: PostgreSQL task projection, push config store, migrations, and
  migration tests. Depends on `rakka-persistence-postgres` and
  `tokio-postgres`.
- `http`: shared route composition with Rakka HTTP observability route helpers
  where appropriate.
- `k8s`: drain/readiness helpers and snapshot registration guidance.
- `otel`: trace context propagation and OpenTelemetry label/attribute helpers.
- `testkit`: in-memory stores, fixtures, and compatibility probes.

Add a top-level facade feature only after the crate API settles:

- `crates/rakka/Cargo.toml`: `a2a = ["dep:rakka-a2a"]`.
- `crates/rakka/src/lib.rs`: gated `pub mod a2a` and curated
  `rakka::prelude` exports for the stable builder and request handler.

## Module Map

Candidate modules:

- `mapping`: A2A-to-Rakka identity normalization, metadata constants,
  `A2ACommandDraft`, payload policy, tenant resolution, trace context, and
  command validation.
- `task`: `A2ATaskProjection`, `A2ATaskEvent`, status mapping, artifact
  mapping, replay cursor parsing, compaction rules, and bounded task rendering.
- `projection`: async `A2ATaskProjectionStore` trait, in-memory store,
  PostgreSQL store, watcher interface, retention policy, and query pagination.
- `push`: push config store trait, redaction model, config validation, outbox
  scheduling, dispatcher adapter contract, retry/exhaustion metrics, and
  credential-binding policy hooks.
- `protocol`: versioned `A2ARunRequest`/`A2ARunResponse`, failure payloads,
  projection hints, timeout policy, stable type ids, and compatibility tests.
- `codec`: JSON payload codec registration for the crate-owned remote protocol.
- `host`: `A2ARunEntity`, `A2ARunHost`, owner request handling, local
  `AgentRunActor` mapping, passivation settings, and sharding initialization.
- `handler`: `RakkaA2ARequestHandler`, builder, drain gate, stream admission,
  local and sharded request paths, and A2A SDK `RequestHandler`
  implementation.
- `agent_card`: dynamic `AgentCardProducer`, workflow skill projection,
  transport URL selection, security schemes, and extended-card authorization
  hooks.
- `routes`: opt-in axum router composition for agent card, REST, JSON-RPC, and
  optional observability routes.
- `observability`: metrics names, bounded labels, operational snapshots,
  stream snapshots, task projection snapshots, push delivery snapshots, and
  trace context helpers.
- `testing`: fixtures, fake workflow catalog, in-memory service builder, A2A
  JSON request fixtures, and cluster compatibility helpers.

## Public API Direction

Expose a builder-based API so applications can choose their storage, security,
workflow catalog, and topology:

- `RakkaA2AServiceBuilder`
- `RakkaA2ARequestHandler`
- `RakkaA2AService`
- `RakkaA2ASettings`
- `A2AWorkflowCatalog`
- `A2ATenantResolver`
- `A2AAuthorizer`
- `A2ACredentialBindingResolver`
- `A2ATaskProjectionStore`
- `A2APushConfigStore`
- `A2APushDispatcher`
- `A2ARunRouter`
- `A2ARunHost`

The builder should make secure defaults easy:

- Require an explicit tenant resolver in tenant-scoped production mode.
- Reject raw push credentials by default; accept only a pre-resolved logical
  binding reference from the credential-binding hook. See Design Note DN-4.
- Require bounded payload policy and artifact strategy when large inputs are
  accepted.
- Require public base URLs for advertised agent-card interfaces.
- Keep drain state external enough to integrate with `rakka-k8s`.

Single-workflow convenience constructors are acceptable for local examples, but
the production path should use a workflow catalog keyed by stable workflow id,
workflow type, and definition version.

## PostgreSQL Projection Plan

Add crate-owned migrations for the A2A read model. The first production store
should include at least:

- `rakka_a2a_tasks`
  - primary key: `(tenant, task_id)`
  - context id, workflow id, workflow type, definition version
  - status, status timestamp, projection revision
  - bounded history JSON, artifacts JSON, public metadata JSON
  - created and updated timestamps
  - indexes for `(tenant, status, status_timestamp)`,
    `(tenant, context_id, updated_at)`, and `(tenant, workflow_id, updated_at)`
- `rakka_a2a_task_events`
  - primary key: `(tenant, task_id, sequence)`
  - event kind, occurred timestamp, projected state, redaction
  - payload JSON, public metadata JSON
  - indexes for replay and retention scans
- `rakka_a2a_push_configs`
  - primary key: `(tenant, task_id, config_id)`
  - URL and redacted public config JSON
  - credential binding ref or secret-presence metadata only
  - deleted flag, audit tail JSON, created and updated timestamps
  - index for active configs by `(tenant, task_id)`
- `rakka_a2a_projection_watermarks`
  - per-task scheduler watermarks for push/event processing where in-memory
    watermarks are not sufficient.

Migration requirements:

- Use explicit schema versions and additive changes.
- Support offline package checks.
- Use migration locks where available.
- Include downgrade-safe N/N+1 guidance for rolling updates.
- Retention may compact event tails but must preserve terminal task snapshots
  and replay cursor behavior.

## Design Notes

These notes resolve mechanisms and invariants that the slices below assume but
do not specify. Each is a pre-implementation decision point: record the concrete
choice here before starting the referenced slice.

### DN-1: PostgreSQL Migration And Schema Versioning Mechanism

Applies before Slice 7.3.

The repository has no shared migration framework. `rakka-persistence-postgres`
applies schema with idempotent `CREATE TABLE IF NOT EXISTS` string constants
self-applied at store construction; `rakka-sharding-postgres` adds migration
helpers and advisory-lock-style coordination for its durable coordinator. The
plan's requirements for "explicit schema versions", "migration locks", and
"N/N+1 downgrade-safe guidance" are therefore net-new unless an existing pattern
is reused.

Decision to record here before implementing 7.3:

- Reuse path (preferred): model the A2A schema on the
  `rakka-persistence-postgres` idempotent-DDL pattern plus the
  `rakka-sharding-postgres` migration-helper/advisory-lock pattern. Embed SQL as
  `const &str` so `cargo package --offline` and `scripts/package-check.sh` keep
  working with no external migration files.
- Versioned-mechanism path: if an ordered `schema_version` table with stepwise
  migrations is required, scope it as its own work item, not folded into "add a
  table".

Requirements the chosen mechanism must satisfy regardless of path:

- Idempotent apply that is safe to run from every node at startup.
- Concurrency-safe when many pods apply at once. Wrap apply in a Postgres
  advisory lock (`pg_advisory_lock`), matching the shard coordinator, and
  document the lock key.
- Offline-packageable: SQL ships embedded in the crate with no runtime file or
  network dependency.
- Additive-only columns/indexes within a release, with downgrade-safe N/N+1
  behavior: during a rolling update old pods must tolerate new columns and new
  pods must tolerate new-optional columns being absent.

### DN-2: Durable Stream Replay Watcher And Cursor Invariant

Applies before Slice 7.6.

Slice 7.6 makes durable event replay the primary streaming path with owner
polling as fallback, but does not specify how a non-owner serving node learns of
new durable events, nor how the client cursor stays consistent across the
owner-served and durable-replay seam. Both are resolved here.

Watcher abstraction:

- Define `A2ATaskEventWatcher` with in-memory and PostgreSQL implementations,
  keyed by `(tenant, task_id)`.
- In-memory: process-local broadcast/notify.
- PostgreSQL: choose and record the notification transport. Candidates:
  - `LISTEN/NOTIFY` on task-event insert. Lowest latency; needs a dedicated
    connection with reconnect handling. The payload must be bounded to
    `(tenant, task_id, high_watermark)` only, never event content or secrets.
  - Bounded interval polling of `rakka_a2a_task_events` /
    `rakka_a2a_projection_watermarks` above the last-served sequence. Simpler;
    higher latency; database load scales with active streams.
- The watcher only signals "there may be new events at or after sequence S". The
  serving node then reads durable events; it never trusts the notification
  payload as data.

Cursor invariant (correctness, holds in all paths):

- The client replay cursor is defined solely by durable projection event
  sequence for `(tenant, task_id)`. Nothing else advances it.
- A serving node must not emit an event or advance the client cursor past the
  highest durably persisted sequence. Owner-served live events (the polling
  fallback) may be surfaced only once durable, so a reconnect to a different node
  replaying from durable state sees neither a gap nor a duplicate at the seam.
- Compaction interacts with replay through the existing `resync` signal
  (`A2ARunResponseKind::StreamCursor`): a cursor older than the retained event
  tail returns `resync = true` and the client re-bootstraps from the current
  projection snapshot. Wire `resync` through the durable-replay path, not only
  the owner-polling path.
- Heartbeats and terminal close are cursor-neutral and never advance the cursor.

Acceptance additions for Slice 7.6:

- Reconnect through a different node after new events resumes with no gap and no
  duplicate at the durable-versus-owner-served boundary.
- A cursor older than the compaction window yields `resync`, not a silent gap.

### DN-3: Multi-Tenant Read Scoping And The Unscoped-Read Path

Applies before Slices 7.3 and 7.4.

The remote protocol currently allows `A2ARunRequest.tenant = None` as an
unscoped read that resolves the run's stored tenant, mirroring the local-mode
projection store. Against a shared multi-tenant durable store this is a
cross-tenant read hazard and must not survive extraction unchanged.

Decision:

- The unscoped (`tenant = None`) read path is a single-tenant / local-mode
  affordance only. When a tenant-scoped production configuration is active
  (explicit `A2ATenantResolver` plus a multi-tenant durable store), the builder
  must refuse to construct a handler that can issue unscoped reads. Every durable
  read and command then carries `Some` canonical tenant.
- Durable store queries are always tenant-scoped by primary key `(tenant, ...)`.
  In tenant-scoped mode there is no store method that returns a task without a
  tenant predicate.
- Tenant mismatch remains indistinguishable from missing task (unchanged
  security behavior).
- Add a gated PostgreSQL test asserting an unscoped read cannot be issued in
  tenant-scoped mode and that a tenant-scoped query never returns a
  foreign-tenant row.

### DN-4: Push Credential Binding Rejects, Never Holds

Applies before Slice 7.7.

The application backend owns secret storage; `rakka-a2a` must never hold secret
material, even transiently. Wording that implies the crate can "convert" a raw
credential is out of boundary.

Decision:

- The crate does not convert, transform, or store raw push credentials. It
  rejects raw secret material by default, or accepts a pre-resolved logical
  binding reference produced by the application's `A2ACredentialBindingResolver`.
- `A2APushConfigStore` persists URL plus redacted public config plus a
  credential-binding ref or secret-presence metadata only.
- No raw credential value enters state, outbox effects, task events, logs,
  metrics, snapshots, or indexes at any point, including transient in-handler
  values that could be logged on error.
- Advertise `push_notifications=true` only when a dispatcher is configured. Push
  delivery is at-least-once; the agent card and docs must not imply exactly-once
  delivery to the webhook target.

## Implementation Slices

Each slice must leave the workspace green: after every slice both
`cargo test -p rakka-a2a` and
`cargo test -p rakka-example-clustered-sharded-entity-a2a-agents` pass. The
example is migrated incrementally across Slices 7.3-7.9; where a slice leaves the
example partially migrated (for example, a crate store wired under an
example-local handler), keep it compiling and passing and name any temporary
bridge in the slice so Slice 7.9 removes it.

### Slice 7.1: Crate Skeleton And Dependency Boundary

Status: planned

Work:

- Add `crates/rakka-a2a` to the workspace.
- Define features, lint settings, crate docs, and a minimal `lib.rs`.
- Add A2A SDK dependencies using the example's current package names:
  `a2a-lf` as `a2a` and `a2a-server-lf` as `a2a-server` with
  `default-features = false`.
- Add a design note for SDK version policy and MSRV compatibility.
- Do not add top-level `rakka` facade exports yet.

Acceptance:

- `cargo test -p rakka-a2a` builds with default features.
- `cargo check -p rakka-a2a --no-default-features` passes and is added to the
  minimal-feature checks in `scripts/validate.sh`.
- The crate has no example-only configuration, discovery, Kubernetes, or demo
  workflow code.

### Slice 7.2: Mapping, Task Projection Types, And Remote Protocol

Status: planned

Work:

- Move and generalize `a2a_mapping.rs`, `task_projection.rs`, `protocol.rs`,
  `codec.rs`, and `stream_limits.rs`.
- Rename remote type ids from `rakka.examples.a2a.*` to stable
  `rakka.a2a.*` ids.
- Make metadata keys, replay cursors, status mapping, and error codes public
  only where they are compatibility commitments.
- Replace synchronous projection store methods with an async trait that can be
  implemented by memory and PostgreSQL stores.

Acceptance:

- Mapping and projection unit tests pass inside `rakka-a2a`.
- Remote payloads round trip through the Rakka serialization registry.
- Compatibility-sensitive strings are documented.

### Slice 7.3: Durable Projection Store And PostgreSQL Migration

Status: planned

Design inputs: DN-1 (migration mechanism), DN-3 (tenant read scoping).

Work:

- Implement `InMemoryA2ATaskProjectionStore` behind the async store trait.
- Implement `PostgresA2ATaskProjectionStore`.
- Add task and task-event migrations.
- Implement replay from shared durable events before owner polling.
- Add retention and compaction behavior for bounded event tails.
- Update the example to use the crate store instead of local projection code.

Acceptance:

- Any node can `get_task`, `list_tasks`, and replay stream events from shared
  PostgreSQL state after owner movement.
- Reconnect through a different node resumes from a valid replay cursor without
  duplicate events.
- Gated PostgreSQL tests validate migration, pagination, compaction, and
  tenant isolation.

### Slice 7.4: Durable Request Handler Builder

Status: planned

Design inputs: DN-3 (tenant read scoping).

Work:

- Move and generalize `RakkaA2ARequestHandler`.
- Replace direct field construction with `RakkaA2AServiceBuilder`.
- Add workflow catalog support.
- Add tenant resolver and authorizer hooks.
- Preserve local durable acceptance, duplicate handling, cancellation, reads,
  list, stream, and push config APIs.
- Keep drain gates as an injectable state so applications can connect them to
  Kubernetes readiness.

Acceptance:

- The handler implements the A2A SDK `RequestHandler`.
- Single-node tests cover send, duplicate retry, continuation, get, list,
  cancel, streaming, and push config CRUD.
- Tenant mismatch remains indistinguishable from missing task where that is the
  current security behavior.

### Slice 7.5: Sharded Owner Host

Status: planned

Work:

- Move and generalize `A2ARunEntity`, `A2ARunHost`, `A2ARunRouter`, and
  sharding initialization helpers.
- Keep the remote protocol serializable and free of actor refs, reply channels,
  stores, and `Arc` values.
- Add version mismatch and unsupported-operation handling.
- Preserve idle passivation and lazy recovery.
- Document which store handles must be shared across nodes.

Acceptance:

- Any public node can route accepted writes and owner snapshots to the shard
  owner.
- Owner restart, passivation, and shard movement recover from durable stores on
  next access.
- Existing example cluster tests move to crate or crate-backed integration
  tests.

### Slice 7.6: Streaming From Durable Events

Status: planned

Design inputs: DN-2 (replay watcher and cursor invariant).

Work:

- Make durable event replay the primary `send_streaming_message` and
  `subscribe_to_task` implementation.
- Keep bounded stream admission, per-node and per-task limits, heartbeats,
  lag handling, and terminal completion.
- Retain owner polling only as an optimization when the owner is local or when
  durable watcher support is unavailable.
- Add a store watcher abstraction for memory and PostgreSQL.

Acceptance:

- Streams survive serving-node restart through client reconnect.
- Streams do not cancel runs on disconnect.
- Slow clients are disconnected with retry guidance instead of causing
  unbounded buffering.

### Slice 7.7: Push Configs And Push Dispatch

Status: planned

Design inputs: DN-4 (push credential binding).

Work:

- Move push config validation, redaction, and outbox scheduling.
- Add PostgreSQL push config storage and scheduler watermark storage.
- Add `A2APushDispatcher` or an agent-workflow dispatcher adapter that sends
  A2A push webhooks from durable effects.
- Add credential-binding policy so raw credentials are rejected (never converted
  or held); only application-supplied logical binding refs are persisted. See
  Design Note DN-4.
- Advertise `push_notifications=true` only when delivery is configured.

Acceptance:

- Push configs survive process restart and can be listed efficiently by tenant
  and task.
- Push effects use stable idempotency keys derived from task event sequence and
  config id.
- Dispatcher retry/exhaustion is visible through metrics and snapshots.
- No resolved credential values are persisted or emitted.

### Slice 7.8: Agent Cards, Routes, And Observability

Status: planned

Work:

- Add dynamic `AgentCardProducer`.
- Add route composition helpers for agent card, REST, JSON-RPC, and optional
  observability routes.
- Add metrics for ingress, durable acceptance, duplicate/conflict/rejection,
  streams, projection, push, owner routing, and error codes.
- Add operational snapshots for task projections, streams, push delivery, and
  adapter health.
- Propagate W3C trace context from A2A transport metadata into durable command
  metadata and outbox effects.

Acceptance:

- Agent cards advertise load-balanced public URLs, implemented transports,
  streaming support, push support only when configured, and security schemes.
- Metrics labels are bounded and do not include task ids, actor paths, prompts,
  callback URLs, payloads, command args, temp paths, full errors, or secrets.
- Snapshots expose production review state without becoming correctness
  sources.

### Slice 7.9: Example Migration

Status: planned

Work:

- Refactor `examples/clustered-sharded-entity-a2a-agents` to consume
  `rakka-a2a`.
- Delete example-local copies of reusable handler, mapping, protocol,
  projection, stream, push, and sharded host code.
- Keep demo workflow, local env config, file discovery, etcd bootstrap,
  manifests, Dockerfile, and run instructions in the example.
- Update README and Phase 6 topology doc to point to `rakka-a2a` for reusable
  behavior.

Acceptance:

- Example behavior remains the same or improves where durable projection replay
  replaces owner polling.
- Existing Phase 6 validation tests still pass after imports move to the
  crate.
- The example remains unpublished.

### Slice 7.10: Facade, Docs, And Release Review

Status: planned

Work:

- Add the gated top-level `rakka` facade feature once APIs are stable enough.
- Document crate usage, topology, reliability boundary, migration policy,
  security model, and operational runbooks.
- Update `README.md`, relevant `docs/*.md`, `CHANGELOG.md`, and package
  inventory docs.
- Run packaging checks for the new crate.

Acceptance:

- Application examples can use `rakka::a2a` or `rakka::prelude` exports without
  depending on example code.
- Public APIs have docs and compile under workspace lints.
- Release review confirms no generated bundles, secrets, or accidental
  publishing artifacts were added.

## Test Strategy

Focused crate tests:

- A2A message/task metadata normalization.
- Workflow selection, tenant resolution, principal propagation, and trace
  context propagation.
- Payload policy, artifact references, and rejection of oversized inline
  payloads without an artifact strategy.
- Status mapping for every `AgentRunStatus`.
- Projection ordering, no-regression status rules, compaction, replay cursor
  validation, and pagination.
- Durable duplicate retry after inbox acceptance.
- Handler behavior for send, stream, get, list, cancel, subscribe, push config
  CRUD, and extended card unsupported/configured paths.
- Remote protocol codec round trips and version mismatch handling.
- Stream limit admission, lag, dropped receiver, heartbeat, and terminal close.
- Push config redaction, no-op resave, delete, outbox scheduling, idempotency
  key reuse, and dispatcher retry/exhaustion.

Cluster tests:

- Two in-process nodes with shared stores route owner writes and reads.
- Owner termination followed by retry returns the same task.
- Shard movement plus duplicate retry remains idempotent.
- Passivation plus lazy recovery serves `get_task`.
- Stream reconnect through a different node resumes from durable projection
  events.

Gated PostgreSQL tests:

- Migrations apply cleanly.
- Task and event projection survive process restart.
- Tenant-scoped list and get queries do not leak foreign-tenant tasks.
- Retention preserves terminal snapshots and reports expired replay windows.
- Push configs and scheduler watermarks survive restart.

Compatibility tests:

- Fixture JSON for A2A REST and JSON-RPC request/response shapes.
- Remote protocol fixture round trips for the current schema version.
- Metadata key and stable error-code assertions.
- Example-backed smoke tests using the crate.

## Validation

Narrow validation while implementing:

```sh
cargo test -p rakka-a2a
cargo test -p rakka-a2a --all-features
cargo check -p rakka-a2a --no-default-features
cargo test -p rakka-example-clustered-sharded-entity-a2a-agents
```

Add `cargo check -p rakka-a2a --no-default-features` to the minimal-feature
checks in `scripts/validate.sh`, alongside the existing `rakka-stream` and
`rakka-process` no-default-feature checks.

Gated validation when relevant:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-a2a --features postgres
```

Final validation before marking this plan implemented:

```sh
scripts/validate.sh
scripts/package-check.sh
```

## Documentation Updates

When the crate lands, update:

- `README.md`
- `CHANGELOG.md`
- `docs/rakka-api-boundary-inventory.md`
- `docs/rakka-v1-api-review.md`
- `docs/rakka-v1-reliability-boundaries.md`
- `docs/rakka-compatibility.md`
- `examples/clustered-sharded-entity-a2a-agents/README.md`
- `examples/clustered-sharded-entity-a2a-agents/doc/phase-6-production-topology.md`

The existing Phase 6 plan should remain implemented as the incubator milestone.
This Phase 7 plan should be marked implemented only after the example consumes
`rakka-a2a`, shared durable projection replay exists, push delivery has a
production dispatcher boundary, and the crate is documented and validated.

