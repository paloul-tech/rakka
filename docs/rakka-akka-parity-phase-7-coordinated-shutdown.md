# Rakka Phase 7 Coordinated Shutdown

Phase 7 makes `ActorSystem::terminate` the operational shutdown path for Rakka
applications. The goal is the same shape Akka users expect: register named work
in ordered phases, start shutdown once, and let adapters for ingress, streams,
cluster, sharding, process actors, persistence, Kubernetes, metrics, and
snapshots all observe the same lifecycle.

## Phase Graph

Rakka ships the following built-in phases in dependency order:

1. `stop-ingress`
2. `drain-http-grpc-and-streams`
3. `leave-cluster`
4. `handoff-shards`
5. `stop-process-actors`
6. `flush-persistence`
7. `stop-user-actors`
8. `stop-system-actors`
9. `stop-remoting`

Custom phases can be inserted before or after built-in phases, but most
applications should start by registering tasks in the existing phases. Keeping
phase names stable helps reports, metrics, Kubernetes drain output, and tests
tell the same story.

## Registering Tasks

Use the actor-system-owned registry for application code:

```rust
use rakka::prelude::*;

# async fn example(system: ActorSystem) -> RakkaResult<()> {
let shutdown = CoordinatedShutdown::get(&system);

shutdown.add_task(ShutdownPhase::flush_persistence(), "flush-search-index", |_context| async {
    // Flush application-owned durable resources here.
    Ok(())
})?;

let report = system.terminate_with_report().await?;
assert_eq!(report.outcome(), ShutdownOutcome::Complete);
# Ok(()) }
```

Core-only tests and low-level integration tests can still create a standalone
registry with `CoordinatedShutdown::new()`.

## Adapter Tasks

Phase 7 adapters register into the same registry:

```rust
# use std::time::Duration;
# use rakka::prelude::*;
# use rakka::http::{register_http_shutdown_task, HttpShutdownHandle};
# use rakka::stream::{bounded_channel, register_stream_sink_drain};
# use rakka::process::{
#     register_configured_process_actor_stop_task, spawn_process_actor,
#     ExecutableAllowlist, ProcessActorConfig, ProcessSpec, ProcessStdio,
# };
# fn process_config() -> Result<ProcessActorConfig, std::io::Error> {
#     let executable = std::env::current_exe()?;
#     let allowlist = ExecutableAllowlist::from_exact_paths([executable.clone()]);
#     let spec = ProcessSpec::new(executable).stdin(ProcessStdio::Piped);
#     Ok(ProcessActorConfig::new(spec, allowlist))
# }
# async fn example(system: ActorSystem) -> Result<(), Box<dyn std::error::Error>> {
let shutdown = CoordinatedShutdown::get(&system);

let http = HttpShutdownHandle::new();
register_http_shutdown_task(&shutdown, "stop-public-http", http.clone())?;

let (sink, _source) = bounded_channel::<String>(16)?;
register_stream_sink_drain(&shutdown, "drain-orders-stream", sink)?;

let config = process_config()?;
let actor = spawn_process_actor(&system, "cooperative-child", config.clone())?;
register_configured_process_actor_stop_task(
    &shutdown,
    "stop-cooperative-child",
    actor,
    &config,
)?;
# Ok(()) }
```

The same pattern exists for gRPC shutdown, cluster leave/down hooks, sharding
handoff hooks, persistence flush/query-cancel hooks, and Kubernetes pre-stop
drain.

## Observability

When a coordinated shutdown registry is created by an `ActorSystem`, shutdown
metrics are recorded through the actor system's metrics recorder:

- `rakka.shutdown.running`
- `rakka.shutdown.phase.duration_ms`
- `rakka.shutdown.task.duration_ms`
- `rakka.shutdown.task.failures`
- `rakka.shutdown.timeouts`

Register the snapshot helper with HTTP observability routes to expose current
or final shutdown state through `/snapshots`:

```rust
# use rakka::prelude::*;
# use rakka::http::{register_coordinated_shutdown_snapshot, OperationalSnapshotRegistry};
# fn example(system: ActorSystem) {
let snapshots = OperationalSnapshotRegistry::new();
register_coordinated_shutdown_snapshot(&snapshots, system.coordinated_shutdown());
# }
```

While shutdown is running, `CoordinatedShutdownSnapshot` includes
`current_phase` and `current_task`. After completion or failure, it includes
the final or partial report.

## Testing

Use `rakka-testkit` for application shutdown tests that need deterministic
ordering without sleeps:

```rust
# use rakka::prelude::*;
# use rakka_testkit::{
#     assert_shutdown_outcome, assert_shutdown_task_status, CoordinatedShutdownTestKit,
# };
# async fn example() -> rakka_core::RakkaResult<()> {
let kit = CoordinatedShutdownTestKit::new();
let phase = ShutdownPhase::drain_adapters();
let controlled = kit.register_controlled_task(phase.clone(), "drain-stream")?;

let shutdown = kit.shutdown();
let run = tokio::spawn(async move {
    shutdown.run(CoordinatedShutdownReason::user_request()).await
});

controlled.wait_started().await?;
controlled.release();
let report = run.await.expect("shutdown task should join").unwrap();

assert_shutdown_outcome(&report, ShutdownOutcome::Complete);
assert_shutdown_task_status(&report, &phase, "drain-stream", ShutdownTaskStatus::Completed);
# Ok(()) }
```

The testkit also supports injected failures, idempotency assertions, phase-order
assertions, and timeout metric-label assertions.

## Kubernetes Path

`KubernetesDrainController::from_coordinated_shutdown` marks readiness false,
runs coordinated shutdown with reason `kubernetes-prestop`, and maps the
shutdown report into a Kubernetes drain report. The recommended timing shape is:

- readiness fails as soon as drain starts;
- liveness remains healthy unless the runtime is stuck;
- `/drain` waits for the coordinated shutdown budget;
- `terminationGracePeriodSeconds` is longer than the pre-stop budget so reports
  and final actor-system cleanup can finish.

## Example

Run the self-contained example:

```sh
cargo run -p rakka-example-coordinated-shutdown
```

It registers custom tasks, an HTTP graceful-shutdown signal, a stream drain, a
cooperative process actor stop, and then runs `ActorSystem::terminate` as the
single shutdown entry point.
