# Rakka V1 Reliability Boundaries

This document states what the v1 release candidate is intended to guarantee, what it deliberately does not guarantee, and where applications or operators must add policy.

## Summary

Rakka v1 separates core actor delivery from opt-in reliability modules:

- Core actors and remote entity routing are at-most-once by default.
- Durable state stores preserve accepted state revisions.
- Durable workflows provide inbox/outbox deduplication, retry scheduling, and recovery.
- Kubernetes readiness, drain, and compatibility checks make unsafe states visible and fail closed.

Rakka does not claim exactly-once delivery, exactly-once external side effects, public remoting security, or automatic schema migration safety.

## Core Actors

Core actor `tell` and `ask` use bounded mailboxes and Tokio tasks.

Guarantees:

- Typed actor refs only accept the message type they were created for.
- Bounded mailboxes reject sends when capacity is exhausted.
- Stopped actors reject new sends.
- Supervision and watch/dead-letter behavior make local failure visible.
- `ask` has an explicit timeout boundary.

Non-guarantees:

- `tell` is at-most-once. A successful enqueue does not guarantee the actor will complete application handling.
- `ask` timeout does not prove the actor did not process the request later.
- Core actors do not persist mailbox contents.
- Core actors do not deduplicate messages.

Use `rakka-workflow` when durable inbox/outbox behavior is required.

## Durable State

`rakka-persistence` defines durable state stores, and `rakka-persistence-postgres` provides a PostgreSQL implementation.

Guarantees:

- State writes are revision-fenced.
- Recovery reads the latest stored revision for a persistence id.
- PostgreSQL and in-memory stores share the same durable state contract.

Non-guarantees:

- Durable state does not make actor delivery exactly once.
- Durable state does not make external side effects transactional with state writes.
- Applications must choose persistence ids and state schemas carefully.

## Durable Workflows

`rakka-workflow` is the v1 reliability module for commands and effects that need durable bookkeeping.

Guarantees:

- Durable inbox entries can use deduplication keys.
- Durable outbox entries can use deduplication keys.
- Dispatch attempts, retry schedules, success, and failure state are recoverable.
- Recovery can identify due outbox work after a restart.

Non-guarantees:

- External systems may still receive duplicate effects after retry or crash boundaries.
- Exactly-once external side effects require idempotent downstream APIs or application reconciliation.
- Retry policy does not replace business-level compensation logic.

## Remoting and Sharding

`rakka-remote`, `rakka-cluster`, and `rakka-sharding` provide known-peer TCP remoting, compatibility admission, shard ownership, local and remote entity routing, graceful handoff, and passivation.

Guarantees:

- Remote envelopes are length-delimited Protobuf frames.
- TCP remoting fails closed for unknown peers, unexpected node ids, incompatible protocol versions, unsupported envelope versions, malformed frames, and unregistered handlers.
- Remote sends use bounded queues.
- Remote entity routing can deliver to local owners or forward to non-local owners.
- N/N+1 compatibility checks prevent incompatible nodes from joining ownership.
- Graceful handoff stops old local ownership before activating a new owner in the tested path.

Non-guarantees:

- Remote actor/entity delivery is still at-most-once by default.
- Rakka internal remoting is not a public client protocol.
- v1 does not provide built-in TLS/mTLS or certificate lifecycle management.
- v1 does not include a durable distributed consensus store for shard coordination.
- Network partitions, process crashes, and Kubernetes disruptions can require application-level retry or workflow recovery.

Use the compatibility policy and rolling-update upgrade note for safe mixed-version deployments.

## Process Actors

`rakka-process` lets a Rakka node own supervised child processes inside the same node container.

Guarantees:

- Child processes are started, supervised, stopped, and restarted according to explicit specs and budgets.
- Process specs are conservative by default: no inherited environment, null stdio, absolute program paths, and allowlist checks.
- Stdio, line-json, one-shot, file-watch, socket, and local-gRPC modes expose typed process interaction boundaries.
- Process-backed entities can participate in sharding and graceful handoff.

Non-guarantees:

- Rakka is not an OS sandbox.
- v1 process actors run child processes inside the Rakka node container, not per-actor sidecars.
- Applications and operators must protect filesystem permissions, secrets, resource limits, and executable provenance.
- Child process protocols must be designed for retries, cancellation, and partial output.

## Streams

`rakka-stream` provides bounded stream vocabulary and adapters.

Guarantees:

- Stream buffers and pressure decisions are explicit.
- Adapter errors distinguish capacity, closed, timeout, and process I/O failures.

Non-guarantees:

- Streams do not make downstream actor/entity handling durable.
- Streams do not replace workflow outbox reliability for external effects.

## HTTP and gRPC Adapters

`rakka-http` and `rakka-grpc` expose actor, entity, stream, workflow, and process-backed services through application-facing APIs.

Guarantees:

- Adapters return typed errors and metrics for supported boundaries.
- Generated contract examples show where application proto/HTTP contracts call Rakka adapters.

Non-guarantees:

- Rakka is not a full web framework.
- Authentication, authorization, request validation, rate limiting, TLS, ingress policy, and public API evolution are application/operator responsibilities.

## Kubernetes Operation

`rakka-k8s` and `examples/kubernetes` provide readiness, liveness, drain, metrics, snapshot, remoting, and rolling-update examples.

Guarantees:

- Readiness can fail during drain or compatibility rejection.
- Drain hooks can stop accepting work before pod termination.
- The example manifest separates internal remoting from public HTTP/gRPC services.
- The local-cluster scenario is gated and dry-run safe by default.

Non-guarantees:

- v1 does not ship a cloud-specific operator, Helm lifecycle, service mesh, or autoscaler.
- Kubernetes NetworkPolicy, pod security, service accounts, secret handling, and image provenance remain operator responsibilities.

## Observability

Rakka exposes backend-neutral metrics, Prometheus text exposition helpers, an OpenTelemetry-oriented bridge model, and operational snapshot routes.

Guarantees:

- Stable metric names and bounded-label guidance are documented.
- In-memory metrics are available for tests and examples.

Non-guarantees:

- v1 does not ship hosted dashboards, alert rules, or vendor-specific agents.
- Applications must keep entity ids, actor paths, route labels, and process names from exploding cardinality.
