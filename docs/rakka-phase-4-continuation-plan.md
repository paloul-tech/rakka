# Rakka Phase 4 Continuation Plan

## Purpose

This file is the working slice outline for Phase 4: process actors and reliability modules. The v1 implementation plan defines the destination; this file defines the order of Phase 4 slices and the acceptance criteria for each one.

Working note: as we move forward, follow this slice outline from this file. Before starting a Phase 4 slice, read this file, implement only the next agreed slice, then update this file if scope or status changes.

## Phase 4 Evaluation

Phase 4 is two related but separable tracks:

- `rakka-process`: supervised child processes owned by actors inside Rakka node containers.
- `rakka-workflow`: opt-in durable reliability for inbox/outbox, retries, deduplication, and recovery.

The phase is well placed after Phase 3 because sharding now gives process-backed actors a routable logical identity, while durable state gives workflows and process-backed entities a recovery substrate. The main planning risk is scope bleed into Phase 5. Phase 4 should own process lifecycle, process protocols, durable delivery bookkeeping, and local examples. It should not build the public stream, HTTP, gRPC, or Kubernetes operations layers.

Important boundary decisions:

- Process actors run child processes inside Rakka node containers for v1.
- Per-actor Kubernetes sidecars remain future work and must be documented as out of scope.
- Process IO can use internal bounded pumps in Phase 4, but the public `rakka-stream` API remains Phase 5.
- Local `grpc` process mode means Rakka can start a child process, wait for its local endpoint, and supervise it. Public gRPC service adapters remain Phase 5.
- `rakka-core` delivery stays at-most-once. Durable retries, deduplication, and recovery live in `rakka-workflow`.

## Current Baseline

The following foundations are already in place:

- `rakka-core` has typed actors, bounded mailboxes, `tell`, `ask`, timers, child spawning, stopping, dead letters, watching, and supervision foundations.
- `rakka-persistence` has durable actor state, latest-state recovery, revision fencing, compare-and-set writes, deletes, and an in-memory store.
- `rakka-persistence-postgres` has a PostgreSQL durable-state plugin.
- `rakka-remote`, `rakka-cluster`, and `rakka-sharding` provide remote envelopes, membership, sharding, ownership refresh, handoff, passivation, and compatibility foundations.
- `rakka-process` exists as a crate stub with `subsystem()` and a helper for creating `tokio::process::Command`.
- `rakka-workflow` exists as a crate stub with subsystem metadata and inbox/outbox telemetry labels.

## Phase 4 Done Definition

Phase 4 should be considered complete when Rakka can demonstrate a process-backed service and a durable workflow running on top of actors, persistence, and sharding.

Minimum completion criteria:

- A process actor can launch a configured executable with explicit args, env, cwd, stdin/stdout/stderr policy, startup timeout, shutdown timeout, and restart policy.
- Process stdout and stderr are captured with actor/entity identity and surfaced through typed telemetry events.
- Startup readiness, periodic health checks, graceful shutdown, crash detection, restart backoff, and restart budget behavior are covered by tests.
- `stdio` and `line-json` request/reply modes are implemented with bounded pending requests, request timeouts, malformed-output failures, and crash cleanup.
- `one-shot`, `file-watch`, TCP/Unix socket, and local `grpc` process modes have v1 foundations and tests appropriate to their boundary.
- A process-backed sharded entity remains addressable by `EntityRef<M>` and can recover durable state before starting or restarting its child process.
- `rakka-workflow` supports durable inbox/outbox state, deduplication keys, retry policy, recovery after restart, and observable exhausted retries.
- Examples and docs show an external binary wrapper and a durable workflow, and clearly mark per-actor Kubernetes sidecars as future work.

## Phase-Level Out of Scope

- Public HTTP adapters, public gRPC adapters, and public stream APIs. Those belong to Phase 5.
- Kubernetes manifests, pre-stop hooks, readiness/liveness integration, and metrics exporters beyond the local process/workflow telemetry surfaces. Those belong to Phase 5.
- Per-actor Kubernetes sidecar containers.
- Exactly-once external side effects. Phase 4 can provide durable deduplication and retry foundations, but external systems still require idempotent protocols or application-level reconciliation.
- Non-Tokio runtimes.

## Slice 4A: Process Configuration and Lifecycle Primitives

Status: implemented.

Goal: define the typed configuration and low-level process ownership primitives that every process mode will share.

Scope:

- Add `ProcessSpec` with executable path, args, environment, cwd, stdin/stdout/stderr policy, startup timeout, shutdown timeout, and optional resource hints.
- Add executable allowlist and validation errors for unsafe or incomplete specs.
- Add `ManagedProcess` or equivalent lifecycle primitive that can spawn, observe, and terminate one child process.
- Capture exit status, spawn failures, timeout failures, and signal/kill outcomes as typed errors/events.
- Implement graceful shutdown: close stdin or send a configured signal first, then kill after timeout.
- Keep platform-specific behavior isolated behind typed policies and tests.

Acceptance criteria:

- Invalid specs fail before spawn with typed errors.
- A long-running test process starts and stops gracefully.
- A process that ignores graceful shutdown is killed after the configured timeout.
- Spawn failure and non-zero exit status are surfaced as typed lifecycle events.
- No secrets or undeclared environment variables are inherited by default.

Out of scope:

- Actor-facing process protocols.
- Restart policy.
- Request/reply correlation.

Review commands:

```bash
cargo fmt --all -- --check
cargo test -p rakka-process
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Slice 4B: Process Actor Runtime and Supervision

Status: implemented.

Goal: turn process lifecycle primitives into an actor-owned runtime with supervision behavior.

Scope:

- Add a process actor facade or runtime API that owns exactly one `ManagedProcess`.
- Add process actor commands for start, stop, status, restart, and health observation.
- Add restart policy with backoff, jitter option, max restart budget, and terminal failure state.
- Add startup readiness checks and periodic health checks.
- Convert unexpected process exit into a supervision event.
- Ensure in-flight actor requests receive typed failure or timeout when the child exits.

Acceptance criteria:

- A process actor starts its child during actor startup or on explicit command.
- Unexpected child exit triggers restart according to policy.
- Restart budget exhaustion stops or marks the process actor failed deterministically.
- Startup timeout and health check failure trigger the configured supervision behavior.
- In-flight requests are failed or timed out with typed errors when the process dies.

Out of scope:

- Concrete process request/reply protocols.
- Durable workflow retry integration.

## Slice 4C: Stdio and Line-JSON Process Protocols

Status: implemented.

Goal: support the two most important long-running child-process protocols for legacy wrappers.

Scope:

- Add `stdio` mode for raw stdin/stdout request/reply adapters.
- Add `line-json` mode using newline-delimited JSON frames.
- Support request ids, pending request tracking, timeouts, and bounded pending capacity.
- Capture stderr independently for logs and diagnostics.
- Define malformed stdout, unknown request id, duplicate reply, and closed-stdin behavior.
- Add fixture binaries or test harnesses for echo, delayed reply, malformed output, and crash cases.

Acceptance criteria:

- A line-json child can receive a typed request and return a typed reply.
- Pending requests are removed on reply, timeout, process exit, or actor stop.
- Malformed output fails the affected request or process according to policy.
- Stderr is observable without corrupting stdout protocol parsing.
- Capacity limits fail closed instead of growing unbounded pending state.

Out of scope:

- Public streams API.
- HTTP/gRPC exposure.

## Slice 4D: One-Shot, File-Watch, Socket, and Local gRPC Modes

Status: implemented.

Goal: cover the remaining v1 process interaction modes without crossing into Phase 5 integration adapters.

Scope:

- Add `one-shot` mode that starts a child process per command with bounded runtime and output capture.
- Add `file-watch` mode with a sandbox directory, input/output file policy, and completion detection.
- Add TCP and Unix-domain socket mode foundations for connecting to child-owned local endpoints.
- Add local `grpc` mode foundations for starting a process, waiting for a local gRPC endpoint, and supervising endpoint readiness.
- Ensure every mode has clear timeout, cleanup, and crash semantics.

Acceptance criteria:

- One-shot mode returns stdout/stderr/exit status or timeout as a typed result.
- File-watch mode uses an explicit working directory and cleans up according to policy.
- Socket mode can wait for readiness and fail cleanly when the child never opens the endpoint.
- Local gRPC mode can supervise process startup and endpoint readiness without exposing public gRPC services.
- Mode-specific tests do not require external network services.

Out of scope:

- Generated gRPC service adapters.
- Public server streaming or client streaming APIs.

## Slice 4E: Process-Backed Sharded Entities

Status: implemented.

Goal: make external binaries usable as routable services behind `EntityRef<M>`.

Scope:

- Provide a process-backed entity pattern using `rakka-sharding` and `rakka-process`.
- Define how entity identity maps to process working directories, log labels, telemetry, and durable state.
- Ensure durable state can be recovered before the child process starts.
- Define passivation and shard handoff behavior for process-backed entities.
- Ensure child processes are stopped when an entity passivates or a shard is handed off.
- Add fencing guidance for preventing two child processes from owning the same logical service identity.

Acceptance criteria:

- A process-backed entity can be addressed through `EntityRef<M>`.
- The entity starts the child process on first routed message.
- Entity passivation stops the child process and removes local actor state.
- Handoff stops the old owner's child process before the new owner activates the entity.
- Durable state is recovered before process startup in the process-backed durable example.

Out of scope:

- Multi-pod Kubernetes process failover tests.
- Per-actor sidecar containers.

## Slice 4F: Workflow Data Model and Durable Inbox

Status: implemented.

Goal: establish durable workflow state and idempotent command acceptance.

Scope:

- Add workflow identifiers, message identifiers, deduplication keys, inbox entries, outbox entries, attempt metadata, and status enums.
- Decide the v1 storage shape on top of `rakka-persistence`, including how workflow state is snapshotted and fenced.
- Add durable inbox APIs for accepting commands after persistence succeeds.
- Add deduplication behavior for repeated keys.
- Add deterministic clock/test hooks for retry scheduling.

Acceptance criteria:

- A workflow can accept a command into a durable inbox and recover it after actor restart.
- Duplicate deduplication keys do not create duplicate inbox work.
- Inbox state transitions are persisted before the next command is processed.
- Revision conflicts surface as typed workflow failures.
- In-memory persistence tests cover recovery and deduplication.

Out of scope:

- Outbox dispatch.
- Cross-service exactly-once guarantees.

## Slice 4G: Durable Outbox, Retries, and Recovery

Status: planned.

Goal: add reliable side-effect scheduling on top of workflow state.

Scope:

- Add durable outbox APIs for scheduling actor/entity sends or application-defined dispatches.
- Add retry policy with max attempts, backoff, jitter option, next-at scheduling, and exhausted state.
- Add recovery loop that resumes pending inbox/outbox work after actor restart.
- Add deduplication for outbound effects when a stable deduplication key is available.
- Surface retry, success, timeout, and exhausted-retry telemetry events.

Acceptance criteria:

- A workflow recovers pending outbox entries after restart and dispatches them.
- Failed dispatches retry according to policy.
- Exhausted retries become observable durable state rather than disappearing.
- Duplicate outbound deduplication keys do not dispatch duplicate effects.
- Recovery does not process the next workflow command until required persistence has succeeded.

Out of scope:

- Exactly-once delivery to arbitrary external systems.
- Distributed scheduler service.

## Slice 4H: Phase 4 Examples, Testkit, and Documentation

Status: planned.

Goal: make Phase 4 behavior reviewable by humans and reusable in tests.

Scope:

- Add a minimal external binary wrapper example.
- Add a durable workflow example that demonstrates inbox, outbox, retry, deduplication, and recovery.
- Add process actor testkit helpers for fixture binaries, stdout/stderr assertions, crash/restart assertions, and timeout assertions.
- Document process actor security defaults, environment handling, working directories, and cleanup.
- Document per-actor Kubernetes sidecars as future work, not v1 behavior.
- Update this continuation plan with final status.

Acceptance criteria:

- Examples run with `cargo run`.
- Tests can exercise process crash/restart without relying on host-specific external binaries.
- Docs explain process actor ownership, process-backed entities, and durable workflow reliability boundaries.
- Phase 4 completion status is clear.

## Suggested Next Slice

Continue with Slice 4G: durable outbox, retries, and recovery. Slice 4F established the workflow data model, deterministic workflow clocks, durable inbox snapshots on top of `rakka-persistence`, inbox deduplication keys, persisted inbox status transitions, recovery from in-memory durable state, and typed revision-conflict workflow failures.
