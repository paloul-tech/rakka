# Rakka Phase 4: Process Actors and Durable Workflows

Phase 4 adds two opt-in reliability tools:

- `rakka-process` lets an actor own and supervise a child process inside the Rakka node container.
- `rakka-workflow` adds durable inbox/outbox state, deduplication, retries, and recovery on top of `rakka-persistence`.

These modules keep `rakka-core` simple. Core actor delivery remains at-most-once; durable retries and idempotency live in workflow code that chooses to pay that cost.

## Process Actor Ownership

A process actor owns exactly one child process at a time. The actor is responsible for spawning the child, observing startup readiness, checking health, shutting it down, and applying restart policy when the child exits unexpectedly.

The v1 ownership boundary is deliberately local:

- Child processes run inside the same Kubernetes container as the Rakka node.
- The Rakka actor owns the child process handles, stdio pipes, lifecycle state, and supervision decisions.
- Process-backed sharded entities map an `EntityRef<M>` identity to a child process only on the current shard owner.
- Passivation and graceful shard handoff stop the old child before another owner activates the same logical entity.

Per-actor Kubernetes sidecars are future work. They are not part of v1 because they need scheduler integration, pod-level lifecycle coordination, Kubernetes readiness/liveness semantics, and stronger fencing than a local child process requires.

## Security Defaults

`ProcessSpec` is explicit by default:

- Executables must be absolute paths and accepted by an `ExecutableAllowlist`.
- Parent environment variables are not inherited unless `inherit_environment()` is explicitly set.
- Environment variables must be declared one by one, which reduces accidental secret leakage.
- Working directories must be absolute.
- Stdin, stdout, and stderr default to `Null`; protocols opt into `Piped`.
- Graceful shutdown defaults to closing stdin, then killing the child after the configured shutdown timeout.
- Resource hints are declarative in v1. Hard enforcement belongs to deployment policy and later platform integrations.

For process-backed entities, use stable working directories derived from entity type, shard id, and entity id. Do not share mutable working directories across two logical entities unless the child protocol has its own fencing.

## Process Modes

Phase 4 includes local process modes only:

- Managed process lifecycle for long-running children.
- Process actor start, stop, status, readiness, health checks, restart backoff, and restart budget.
- Raw stdio request/reply with line-framed bytes.
- Line-json request/reply with request ids and bounded pending requests.
- One-shot command execution with bounded runtime and output capture.
- File-watch mode with sandbox-relative inputs, outputs, cleanup, and timeout.
- TCP, Unix socket, and local gRPC readiness foundations for child-owned local endpoints.

Public HTTP, public gRPC, public streaming, Kubernetes manifests, metrics exporters, and pod lifecycle hooks remain Phase 5 work.

## Durable Workflow Boundaries

`rakka-workflow` stores a workflow snapshot under a `PersistenceId`. The snapshot contains inbox entries, outbox entries, deduplication indexes, retry attempt metadata, and workflow timestamps.

The durable inbox gives idempotent command acceptance:

- Commands are persisted before being considered accepted.
- Duplicate message ids or deduplication keys return the existing durable entry.
- Inbox status transitions are persisted before the next command is processed.
- Revision conflicts surface as typed workflow errors.

The durable outbox gives reliable side-effect scheduling:

- Outbox entries can target actor paths, sharded entities, or application-defined dispatchers.
- Dispatching is persisted before an external side effect starts.
- Success, retry, timeout, and exhausted-retry outcomes are persisted and returned as telemetry events.
- Failed dispatches retry according to bounded backoff and deterministic jitter policy.
- Duplicate outbound deduplication keys reuse existing outbox work.

This is not exactly-once delivery to arbitrary external systems. External effects still need idempotent APIs, stable deduplication keys, or reconciliation logic. Rakka provides durable intent and retry bookkeeping; the receiving system must still tolerate repeats.

## Testkit Helpers

`rakka_process::testkit` provides reusable helpers for process integration tests:

- `ProcessFixture` creates allowlisted fixture specs and stdio fixture specs.
- Temporary directory, TCP port, and Unix socket path helpers avoid host-specific fixtures.
- File log helpers wait for expected lifecycle lines.
- Process actor helpers start, stop, read status, wait for states, and assert restart-budget exhaustion.
- Stdio helpers send requests, read status/stderr, wait for pending counts, and assert stderr/output contents.
- One-shot helpers assert timeout outcomes.

The `rakka-process` tests use the in-tree `rakka-process-fixture` binary through Cargo's fixture executable path, so they do not require host-installed external services.

## Examples

Run the process wrapper example:

```bash
cargo run -p rakka-example-external-binary-wrapper
```

The example starts a child process in line-json mode, sends a typed request, captures stderr, and shuts the child down through actor ownership.

Run the durable workflow example:

```bash
cargo run -p rakka-example-durable-workflow
```

The example accepts inbox work with a deduplication key, schedules an outbox effect, recovers the workflow, retries one failed dispatch, and then records success.
