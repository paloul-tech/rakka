# Rakka Akka Parity Phase 7 Detailed Plan

Status: implemented through Slice 7E
Date: 2026-06-15

## Purpose

This plan expands Phase 7 from
`docs/plans/rakka-akka-parity-implementation-plan.md` into implementation
slices for Akka-shaped coordinated shutdown and operational integration.

Phase 7 should make actor-system termination the single operational shutdown
path across:

- HTTP and gRPC ingress.
- Streams and stream adapters.
- Cluster membership.
- Cluster sharding and shard handoff.
- Remote transport and clustered receptionist proxies.
- Process actors.
- Persistence journals, snapshots, and query adapters.
- Kubernetes readiness, pre-stop drain, metrics, and snapshots.

## Evaluation

Rakka already has several shutdown-related foundations, but they are not yet
connected by one public lifecycle model:

- `ActorSystem::shutdown` stops actors immediately.
- `ActorSystem::terminate` stops actors and waits for active actors to finish.
- `rakka-k8s::KubernetesDrainController` runs drain steps and marks readiness
  false, but it is separate from actor-system termination.
- `rakka-http::serve_with_graceful_shutdown` accepts a caller-provided shutdown
  signal.
- Streams expose drain, close, cancellation, and facade materialization
  semantics.
- Cluster and sharding expose local leave and handoff primitives.
- Remote transport exposes peer draining.
- Process actors and managed processes have graceful stop policies.
- Persistence stores commit operations eagerly, but there is no shutdown hook
  vocabulary for flushing, checkpointing, or backend close readiness.

Recommendation: add coordinated shutdown as a core `rakka-core` lifecycle
extension and make Kubernetes drain, server shutdown, sharding leave, process
cleanup, and final actor-system termination register tasks into it. The
existing focused helpers should become adapters to coordinated shutdown rather
than competing shutdown systems.

## Target Outcome

Applications should be able to register operational hooks with recognizable
phase names and then run one shutdown path from application code, SIGTERM,
Kubernetes pre-stop, or `ActorSystem::terminate`:

```rust
let system = ActorSystem::new("orders");
let shutdown = CoordinatedShutdown::get(&system);

shutdown.add_task(
    ShutdownPhase::stop_ingress(),
    "stop-public-http",
    move |_context| async move {
        http_handle.shutdown().await?;
        Ok(())
    },
)?;

shutdown.add_task(
    ShutdownPhase::flush_persistence(),
    "flush-order-journal",
    move |_context| async move {
        journal.flush().await?;
        Ok(())
    },
)?;

let report = shutdown
    .run(CoordinatedShutdownReason::kubernetes_prestop())
    .await?;
```

`ActorSystem::terminate` should run the same registry once, reuse the same
idempotency guard, and return a report-aware error on failure or timeout.

## Non-goals

- Full Akka Management implementation.
- Replacing Kubernetes controllers, PodDisruptionBudgets, Services, or Ingress.
- Distributed consensus for shutdown ordering across nodes.
- Exactly-once stream draining or persistence flushing guarantees beyond each
  backend's committed operation semantics.
- Hidden background registration of every possible resource. Adapters should be
  explicit at first.
- Breaking removal of existing `ActorSystem::shutdown`, `ActorSystem::terminate`,
  or `KubernetesDrainController` APIs during this phase.

## Guiding Decisions

- Put the core registry in `rakka-core` so all higher-level crates can depend on
  it without cycles.
- Model phases as stable names with ordered dependencies, not just a fixed enum,
  so users can add phases before, after, or between built-ins.
- Execute phases sequentially by dependency order. Within a phase, start with
  sequential task execution for deterministic reports; add controlled
  per-phase parallelism only if the API remains clear.
- Make repeated shutdown calls idempotent. Concurrent callers should observe the
  same in-flight or completed shutdown outcome.
- Preserve timeout and failure detail in a serializable shutdown report.
- Make Kubernetes drain a caller of coordinated shutdown, while preserving the
  existing health model and drain report shape through adapters.
- Record shutdown metrics through the existing `MetricsRecorder`.
- Keep a testkit-first design: deterministic phase ordering and timeout tests
  should not depend on sleeps except where Tokio timeout behavior itself is the
  subject.

## Built-In Phase Topology

Phase 7 should expose built-in phase constants that match the main parity plan:

1. `stop-ingress`
2. `drain-http-grpc-and-streams`
3. `leave-cluster`
4. `handoff-shards`
5. `stop-process-actors`
6. `flush-persistence`
7. `stop-user-actors`
8. `stop-system-actors`
9. `stop-remoting`

The exact Rust names should be ergonomic, for example:

```rust
ShutdownPhase::stop_ingress()
ShutdownPhase::drain_adapters()
ShutdownPhase::leave_cluster()
ShutdownPhase::handoff_shards()
ShutdownPhase::stop_process_actors()
ShutdownPhase::flush_persistence()
ShutdownPhase::stop_user_actors()
ShutdownPhase::stop_system_actors()
ShutdownPhase::stop_remoting()
```

The core implementation should also support custom phases:

```rust
shutdown.add_phase_after("flush-search-index", ShutdownPhase::flush_persistence())?;
shutdown.add_phase_before("publish-final-metrics", ShutdownPhase::stop_remoting())?;
```

## Slice 7A: Core Shutdown Vocabulary And Phase Topology

Goal: add the public coordinated shutdown vocabulary and built-in phase graph
without changing existing termination behavior.

Status: implemented.

Scope:

- Add a `coordinated_shutdown` module in `rakka-core`.
- Add public types:
  - `CoordinatedShutdown`;
  - `CoordinatedShutdownSettings`;
  - `ShutdownPhase`;
  - `ShutdownTask`;
  - `ShutdownTaskContext`;
  - `CoordinatedShutdownReason`;
  - `CoordinatedShutdownReport`;
  - `ShutdownPhaseReport`;
  - `ShutdownTaskReport`;
  - `ShutdownOutcome`;
  - `ShutdownFailurePolicy`.
- Add built-in phase constants and helper constructors matching the topology
  above.
- Add phase registration APIs:
  - `add_phase_after`;
  - `add_phase_before`;
  - `add_phase_dependency`, if needed for advanced users;
  - deterministic validation for duplicate phases and dependency cycles.
- Add task registration APIs:
  - async task closures;
  - per-task timeout override;
  - failure policy override for fail-fast or continue;
  - optional task metadata for observability.
- Add rustdoc examples that show registering a custom phase and task.

Acceptance criteria:

- Built-in phases are returned in deterministic dependency order.
- Duplicate phase and task names fail with typed errors.
- Dependency cycles fail before shutdown begins.
- Task registration does not require running an actor system.
- Public docs state that this is the Rakka equivalent of Akka Coordinated
  Shutdown, adapted to Rust async tasks.

Implementation status:

- Added `rakka-core::coordinated_shutdown` with the public 7A vocabulary:
  `CoordinatedShutdown`, settings, phases, reasons, task descriptors, task
  options, failure policy, report types, task statuses, and task context.
- Added the built-in phase topology from the main parity plan with deterministic
  dependency ordering.
- Added custom phase insertion before or after built-in phases, explicit phase
  dependencies, duplicate validation, unknown-phase validation, and dependency
  cycle rollback.
- Added task registration APIs that accept async closures and store task
  descriptors without requiring an `ActorSystem`.
- Re-exported the stable vocabulary from `rakka_core`.
- Added public API tests covering built-in ordering, custom phase ordering,
  duplicate phases, unknown phases, cycle detection and rollback, task
  registration, duplicate tasks, and invalid names.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core coordinated_shutdown
cargo doc -p rakka-core --no-deps
```

## Slice 7B: Shutdown Runner, Idempotency, Reports, And Timeouts

Goal: make the registry executable with deterministic phase reports and
idempotent lifecycle semantics.

Status: implemented.

Scope:

- Implement `CoordinatedShutdown::run(reason)` and
  `CoordinatedShutdown::run_with_deadline(reason, deadline)`.
- Add shutdown state tracking:
  - not started;
  - running;
  - completed;
  - failed;
  - timed out.
- Make repeated calls return the same completed report or await the in-flight
  run.
- Enforce phase and task timeouts.
- Implement fail-fast versus continue-on-error behavior.
- Record phase start, phase end, task start, task end, elapsed duration, status,
  and failure message.
- Add serializable report snapshots for operational routes.
- Add errors that preserve the partial report when shutdown fails or times out.

Acceptance criteria:

- Tasks run in topological phase order.
- Repeated `run` calls do not execute tasks twice.
- Concurrent `run` calls share one execution.
- A timed-out task stops further phases under fail-fast policy.
- Continue-on-error policy completes later tasks and marks the report partial.
- Reports are deterministic enough for snapshot-style tests.

Implementation status:

- Added `CoordinatedShutdown::run` and `run_with_deadline` with one-shot
  execution semantics.
- Added watch-backed idempotency so repeated calls return the completed result
  and concurrent callers await the same in-flight shutdown.
- Added `CoordinatedShutdownError`, `CoordinatedShutdownResult`, and
  `CoordinatedShutdownSnapshot` so failed and timed-out shutdowns preserve
  partial reports.
- Added runner state tracking for not-started, running, and finished outcomes.
- Enforced default phase timeouts, default task timeouts, task-specific
  timeouts, and overall deadlines.
- Implemented fail-fast versus continue-on-error task policy behavior.
- Added deterministic phase and task reports with duration, status, and failure
  or timeout messages.
- Prevented registry mutation after shutdown has started.
- Added tests for phase-order execution, repeated-run idempotency, concurrent
  run sharing, fail-fast failure, continue-on-error partial reports, task
  timeout behavior, and overall deadline timeout behavior.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core coordinated_shutdown
cargo clippy -p rakka-core --all-targets -- -D warnings
```

## Slice 7C: ActorSystem Integration And Terminate Unification

Goal: make `ActorSystem` own a coordinated shutdown registry and use it as the
single termination path.

Status: implemented.

Scope:

- Store a coordinated shutdown registry inside `ActorSystemInner`.
- Add `CoordinatedShutdown::get(&ActorSystem)`.
- Add `ActorSystem::coordinated_shutdown()`.
- Extend `ActorSystemShutdownConfig` or add
  `CoordinatedShutdownSettings` to configure:
  - overall termination timeout;
  - default phase timeout;
  - default task timeout;
  - failure policy.
- Register built-in actor tasks:
  - stop user actors in `stop-user-actors`;
  - stop system actors in `stop-system-actors`;
  - mark system terminated after actor counts reach zero.
- Update `ActorSystem::terminate` to run coordinated shutdown with reason
  `actor-system-terminate`.
- Keep `ActorSystem::shutdown` as a compatibility method that still sends stop
  signals immediately, but document that `terminate` is the coordinated path.
- Make spawn rejection begin when coordinated shutdown enters `stop-user-actors`
  or earlier, depending on the chosen operational semantics.

Acceptance criteria:

- Existing actor-system lifecycle tests pass.
- `ActorSystem::terminate` runs user-registered shutdown tasks.
- Repeated `ActorSystem::terminate` calls are idempotent.
- `ActorSystem::when_terminated` completes after coordinated shutdown completes.
- Spawning during termination fails with the existing or improved
  `system-terminating` error.

Implementation status:

- Added an owned `CoordinatedShutdown` registry to `ActorSystemInner` and
  exposed it through `ActorSystem::coordinated_shutdown`.
- Added `CoordinatedShutdown::get(&ActorSystem)` as the Akka-shaped extension
  lookup helper.
- Extended `ActorSystemShutdownConfig` with coordinated shutdown settings while
  preserving the existing termination timeout constructor.
- Registered built-in actor-system shutdown tasks:
  - `stop-user-actors` in the `stop-user-actors` phase;
  - `stop-system-actors` in the `stop-system-actors` phase.
- Updated `ActorSystem::terminate` to run coordinated shutdown with reason
  `actor-system-terminate` and the configured termination deadline.
- Added `ActorSystem::terminate_with_report` for callers that need the
  coordinated shutdown report or report-bearing error.
- Kept `ActorSystem::shutdown` as the immediate compatibility stop signal.
- Preserved spawn rejection during termination through the existing
  `system-terminating` guard.
- Re-exported coordinated shutdown vocabulary through the top-level `rakka`
  prelude.
- Added actor-system lifecycle tests proving custom coordinated shutdown tasks
  run once through `terminate`, repeated terminate calls are idempotent, failure
  is surfaced, actors stop, `when_terminated` completes, and late spawns fail.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core local_actor_runtime
cargo test -p rakka-core coordinated_shutdown
cargo clippy -p rakka-core --all-targets -- -D warnings
```

## Slice 7D: HTTP, gRPC, And Stream Drain Adapters

Goal: connect public ingress and stream lifecycles to coordinated shutdown.

Status: implemented.

Scope:

- Add a small shutdown signal handle that can be cloned into HTTP and gRPC
  servers.
- Add HTTP adapter helpers:
  - register stop-ingress task;
  - convert coordinated shutdown notification into Axum graceful shutdown;
  - record server shutdown result.
- Add gRPC adapter helpers with the same shape as HTTP.
- Add stream adapter helpers:
  - register `StreamSink::drain`;
  - register `StreamSource::drain`;
  - register facade `Source` or `RunnableStream` cancellation when appropriate.
- Preserve the existing lower-level helpers and expose the new helpers as
  opt-in conveniences.
- Update stream docs with the coordinated shutdown drain path.

Acceptance criteria:

- HTTP server helper can be stopped by coordinated shutdown.
- gRPC helper follows the same shutdown signal model.
- Stream sink/source drains run in the adapter-drain phase.
- Closed or already-cancelled streams are reported as completed, not failed.
- Task reports include stable names for ingress and stream resources.

Implementation status:

- Added `rakka-http` shutdown handles, signals, snapshots, and server result
  recording.
- Added `register_http_shutdown_task` for `stop-ingress` and
  `serve_with_coordinated_shutdown` to bridge coordinated shutdown into Axum
  graceful shutdown.
- Added matching `rakka-grpc` shutdown handles, signals, snapshots, server
  result recording, and `register_grpc_shutdown_task`.
- Added `rakka-stream` drain registration helpers for `StreamSink` and
  `StreamSource` in the `drain-http-grpc-and-streams` phase.
- Treat closed and already-cancelled stream drains as completed shutdown work so
  repeated or late shutdown does not fail a report for terminal resources.
- Re-exported the new helpers from the integration crates and stream drain
  helpers through the top-level `rakka` prelude.
- Added focused tests for HTTP/gRPC signal triggering, result snapshots, stream
  drain task placement, and terminal stream drain completion.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-http
cargo test -p rakka-grpc
cargo test -p rakka-stream
cargo clippy -p rakka-http -p rakka-grpc -p rakka-stream --all-targets -- -D warnings
```

## Slice 7E: Cluster, Sharding, Receptionist, And Remoting Hooks

Goal: make distributed runtime components participate in the same shutdown
sequence.

Status: implemented.

Scope:

- Add cluster helper tasks:
  - mark local node leaving;
  - optionally down self only for configured test or force-shutdown modes;
  - publish cluster leave events into the report.
- Add sharding helper tasks:
  - invoke local leave on `ClusterShardingRuntime` or `ClusterNodeRuntime`;
  - summarize membership events, shard handoffs, and rebalances;
  - ensure new local activations are rejected after handoff begins.
- Add remote receptionist cleanup task:
  - stop materialized proxies during remoting shutdown;
  - expire or remove remote listings for the draining node.
- Add remote transport tasks:
  - drain known peers before stop-remoting;
  - stop or close remoting after actors and proxies are stopped.
- Keep hooks explicit to avoid hidden ownership of runtime handles.

Acceptance criteria:

- Cluster leave happens before shard handoff.
- Shard handoff reports stopped entity counts and ownership transitions.
- Remote sends reject with draining or closed transport errors after the remoting
  drain task begins.
- Clustered receptionist proxies are removed during shutdown.
- Existing Phase 4 and Phase 5 remote/sharding tests continue to pass.

Implementation status:

- Added explicit cluster coordinated-shutdown hooks for local graceful leave and
  force/test-only down-self behavior in the `leave-cluster` phase.
- Added a clustered receptionist prune hook in the `stop-remoting` phase so
  propagated listings from non-up members are removed during shutdown.
- Added shared synchronous and async sharding shutdown handles for mutable
  `ClusterShardingRuntime` and `ClusterNodeRuntime` ownership.
- Added sharding and networked-node leave task registration helpers in the
  `handoff-shards` phase, preserving the existing handoff update summaries for
  stopped entity counts and ownership transitions.
- Added TCP remoting drain and force-close task registration helpers in the
  `stop-remoting` phase.
- Added remote service proxy cleanup hooks for source-node removal and stale
  listing expiry during remoting shutdown.
- Re-exported the cluster and sharding hooks through the top-level `rakka`
  prelude; remote hooks are available through `rakka::remote`.
- Added tests covering local cluster leave/down tasks, clustered receptionist
  prune during shutdown, sharding handoff through coordinated shutdown, TCP
  drain rejecting future sends, and remote service proxy removal during
  shutdown.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-cluster
cargo test -p rakka-sharding
cargo test -p rakka-remote
cargo clippy -p rakka-cluster -p rakka-sharding -p rakka-remote --all-targets -- -D warnings
```

## Slice 7F: Process Actors And Persistence Flush Hooks

Goal: provide built-in registration helpers for resources that need explicit
cleanup before actor-system stop.

Status: completed.

Scope:

- Add process actor shutdown helpers:
  - register `ProcessActorCommand::Stop`;
  - use each process actor's configured timeout where available;
  - report already-stopped processes as completed.
- Add managed process helpers for direct `ManagedProcess` handles where the
  application owns the process outside an actor.
- Add persistence shutdown vocabulary:
  - `PersistenceShutdown` trait or lighter `flush` task helper;
  - optional adapters for event journals and snapshot stores;
  - report backend name and persistence id scope when available.
- Avoid promising a flush operation for stores that do not buffer writes.
  Those stores should report "no-op flush" or only register backend-close tasks.
- Add hooks for query streams that need cancellation before store cleanup.

Acceptance criteria:

- Process actor stop tasks complete, fail, or time out with typed report status.
- Direct managed process tasks kill after the configured graceful timeout.
- In-memory persistence can register a no-op flush task for test consistency.
- PostgreSQL persistence can register a backend readiness or close task without
  losing committed writes.
- Persistence query streams are cancelled before flush/backend-close tasks.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-process
cargo test -p rakka-persistence
cargo test -p rakka-persistence-postgres
cargo clippy -p rakka-process -p rakka-persistence -p rakka-persistence-postgres --all-targets -- -D warnings
```

Completion note:

- Added process coordinated-shutdown helpers for process actor stop commands,
  config-derived stop task timeouts, and directly owned `ManagedProcess`
  shutdown in the `stop-process-actors` phase.
- Process actor hooks treat already-stopped actors and `NotRunning` replies as
  successful idempotent shutdown while preserving real stop failures and task
  timeouts in coordinated shutdown reports.
- Added a persistence shutdown vocabulary with `PersistenceShutdown`,
  `PersistenceShutdownFuture`, a generic flush/check task helper, and query
  stream cancellation tasks that run in `drain-adapters` before
  `flush-persistence`.
- In-memory durable state, event journal, and snapshot store hooks report
  explicit `noop-flush` semantics for test consistency.
- PostgreSQL durable state, event journal, and snapshot store hooks report a
  `postgres-readiness-check` operation without changing the existing
  write-through commit semantics.
- Re-exported the new process and persistence helpers through the top-level
  `rakka` prelude.
- Added tests covering process actor stop registration, idempotent not-running
  stops, direct managed process shutdown after graceful timeout, in-memory
  no-op persistence shutdown, query cancellation before flush, and a DSN-gated
  PostgreSQL readiness shutdown task.

## Slice 7G: Kubernetes Pre-Stop Bridge And Operational Routes

Goal: make Kubernetes pre-stop drain run coordinated shutdown while preserving
the existing Kubernetes health and drain APIs.

Status: completed.

Scope:

- Add `KubernetesDrainController::from_coordinated_shutdown` or equivalent.
- Add a coordinated-shutdown-backed drain method that:
  - marks readiness false before `stop-ingress`;
  - runs shutdown with reason `kubernetes-prestop`;
  - maps `CoordinatedShutdownReport` into `KubernetesDrainReport`.
- Preserve existing custom `KubernetesDrainStep` registration during migration
  by wrapping each step as a coordinated shutdown task.
- Add a `/drain` route helper that runs the coordinated path.
- Keep `/ready` failed after drain begins.
- Update `examples/kubernetes` docs and manifest comments so pre-stop points to
  the coordinated shutdown path.
- Add an optional OS signal helper that runs coordinated shutdown for SIGTERM
  and Ctrl-C in application binaries.

Acceptance criteria:

- Existing k8s health and drain tests pass.
- Kubernetes drain and direct `ActorSystem::terminate` share the same report
  source.
- Readiness fails immediately after drain begins.
- Drain timeouts map to Kubernetes timeout reports with stable reason labels.
- The dry-run Kubernetes scenario still documents the same operator contract.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-k8s
cargo test -p rakka-http observability
cargo clippy -p rakka-k8s --all-targets -- -D warnings
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh
```

Completion note:

- Added `KubernetesDrainController::from_coordinated_shutdown` so Kubernetes
  pre-stop drain can run the same coordinated shutdown registry used by
  `ActorSystem::terminate`.
- Coordinated drain marks readiness false before running shutdown with reason
  `kubernetes-prestop`, and maps `CoordinatedShutdownReport` plus failed or
  timed-out runs back into `KubernetesDrainReport`.
- Existing `KubernetesDrainController::add_step` usage remains compatible; in
  coordinated mode each legacy drain step is wrapped as a coordinated shutdown
  task in `drain-adapters` while Kubernetes reports preserve the original step
  names.
- Added `KubernetesDrainReport::from_coordinated_shutdown_report` for callers
  that need direct report conversion.
- Added `kubernetes_drain_route` for a GET `/drain` style route that returns
  JSON drain reports and status codes for complete, partial, and timed-out
  drains.
- Added OS signal helpers for Ctrl-C/SIGTERM-driven coordinated shutdown,
  including a Kubernetes pre-stop reason helper.
- Updated the Kubernetes example README and manifest comments to describe the
  coordinated pre-stop path.
- Added tests covering coordinated drain/report mapping, shared
  `ActorSystem::terminate` report source behavior, timeout mapping with stable
  `kubernetes-prestop` reason text, `/drain` route JSON behavior, and updated
  example documentation assertions.

## Slice 7H: Shutdown Observability, Metrics, And Snapshots

Goal: make coordinated shutdown visible through existing metrics and
operational snapshot surfaces.

Status: completed in Slice 7H.

Scope:

- Add stable metric names in `rakka-core::metrics`, for example:
  - `rakka.shutdown.phase.duration_ms`;
  - `rakka.shutdown.task.duration_ms`;
  - `rakka.shutdown.task.failures`;
  - `rakka.shutdown.timeouts`;
  - `rakka.shutdown.running`.
- Record phase and task durations with attributes:
  - system;
  - phase;
  - task;
  - reason;
  - status.
- Add `CoordinatedShutdown::snapshot`.
- Add an operational snapshot provider helper for HTTP observability routes.
- Add report serialization tests so JSON output stays stable.
- Document how to expose shutdown reports through `/snapshots`.

Acceptance criteria:

- Metrics are recorded for completed, failed, and timed-out tasks.
- Shutdown snapshots include current phase while running and final report after
  completion.
- Prometheus and OpenTelemetry exporters can expose shutdown observations
  without custom code.
- Operational snapshot registry can include coordinated shutdown state by name.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-core operational_metrics
cargo test -p rakka-http observability_routes
cargo clippy -p rakka-core -p rakka-http --all-targets -- -D warnings
```

Completion notes:

- Added coordinated shutdown metric constants for running state, phase
  durations, task durations, task failures, and timeouts.
- Wired `CoordinatedShutdown` into the actor-system metrics recorder so
  `ActorSystem::terminate` and coordinated-shutdown-backed adapters emit the
  same observations.
- Extended `CoordinatedShutdownSnapshot` with current phase/task progress
  while running and retained final/partial reports after completion.
- Added HTTP observability helpers for registering coordinated shutdown state
  in `OperationalSnapshotRegistry` under the default
  `coordinated_shutdown` name or a caller-provided name.
- Covered stable JSON shape plus Prometheus/OpenTelemetry exporter visibility
  through focused core and HTTP tests.

## Slice 7I: Coordinated Shutdown Testkit

Goal: add reusable testkit utilities so applications and Rakka crates can test
shutdown behavior without sleeps.

Status: planned.

Scope:

- Add `CoordinatedShutdownTestKit` in `rakka-testkit`.
- Add helpers for:
  - recording task start and finish order;
  - creating controlled pending tasks;
  - releasing tasks manually;
  - injecting task failure;
  - asserting phase order;
  - asserting idempotency;
  - asserting report status and timeout labels.
- Add a test-only manual clock only if Tokio time pause is insufficient for the
  timeout cases.
- Add examples that show application authors how to test their registered
  shutdown tasks.

Acceptance criteria:

- Tests can assert phase ordering without arbitrary sleeps.
- Tests can block a phase until a permit is released.
- Timeout tests are deterministic with `tokio::time::pause` or an equivalent
  helper.
- The testkit supports both core-only tests and integration tests with an
  `ActorSystem`.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-testkit coordinated_shutdown
cargo test -p rakka-core coordinated_shutdown
cargo clippy -p rakka-testkit --all-targets -- -D warnings
```

## Slice 7J: Docs, Examples, And Migration Notes

Goal: make the coordinated shutdown path easy to discover and migrate to.

Status: planned.

Scope:

- Add `docs/rakka-akka-parity-phase-7-coordinated-shutdown.md`.
- Update `docs/rakka-akka-parity-migration-notes.md` with:
  - old direct `system.shutdown` and k8s drain examples;
  - new coordinated shutdown registration examples;
  - guidance for replacing ad hoc shutdown channels.
- Add a runnable example package or extend an existing example to show:
  - registering custom tasks;
  - graceful HTTP shutdown;
  - stream drain;
  - process actor stop;
  - final `ActorSystem::terminate`.
- Update `examples/kubernetes/README.md` with the coordinated pre-stop path.
- Update the main parity plan with Phase 7 completion notes after implementation.
- Add rustdoc examples for public helpers.

Acceptance criteria:

- Users can run the example locally without external services.
- Migration notes explain when to keep using low-level helpers.
- Kubernetes docs describe readiness, liveness, drain, and termination grace
  timing against coordinated shutdown phases.
- Main parity plan is updated only when implementation is complete.

Review commands:

```bash
cargo fmt --all -- --check
cargo run -p rakka-example-coordinated-shutdown
cargo test --workspace --all-features
cargo doc --workspace --all-features --no-deps
git diff --check
```

## Slice 7K: Full Operational Validation

Goal: verify the integrated shutdown path across crates and preserve the
Phase 7 parity boundary.

Status: planned.

Scope:

- Run full workspace validation.
- Add focused integration tests covering:
  - phase ordering;
  - repeated terminate calls;
  - task failure policy;
  - timeout behavior;
  - HTTP/gRPC graceful shutdown signals;
  - stream drain;
  - Kubernetes drain mapping;
  - shard handoff during shutdown;
  - process actor cleanup;
  - remote transport draining;
  - persistence flush/no-op flush reporting.
- Keep PostgreSQL tests gated by the existing DSN behavior.
- Keep Kubernetes local-cluster validation gated.
- Review public API names before marking Phase 7 complete.

Acceptance criteria:

- Full workspace checks pass.
- Phase 7 docs match implemented API names.
- No shutdown path requires hidden global state outside `ActorSystem`.
- Existing compatibility and repository hygiene tests continue to pass.

Review commands:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo doc --workspace --all-features --no-deps
git diff --check
```

## Suggested Implementation Order

1. Implement core vocabulary and reports in `rakka-core` with narrow tests.
2. Integrate `ActorSystem::terminate` after idempotency is proven.
3. Add low-risk adapters for streams, HTTP, and gRPC.
4. Add distributed-runtime hooks for cluster, sharding, receptionist, and
   remoting.
5. Add process and persistence hooks.
6. Convert Kubernetes drain into a coordinated shutdown adapter.
7. Add metrics, snapshots, and testkit utilities.
8. Finish docs, examples, and full validation.

## Open Design Questions For Review

- Should tasks within one phase run sequentially by default, or should Phase 7
  support bounded parallelism immediately?
- Should `ActorSystem::shutdown` remain a fire-and-forget stop signal forever,
  or should it become a deprecated alias for the coordinated path later?
- Should persistence stores get a formal `flush` trait now, or should Phase 7
  start with task helper closures and add the trait only after PostgreSQL needs
  it?
- Should OS signal handling live in `rakka-core`, `rakka-k8s`, or a small
  operational helper module re-exported by the top-level `rakka` crate?
- Should Kubernetes drain preserve its existing step report type as a separate
  public type long term, or should it eventually return coordinated shutdown
  reports directly?
