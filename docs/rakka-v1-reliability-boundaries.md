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
- Remembered entities can record successfully activated ids and restart them
  after local ownership refresh or shard acquisition when the entity type opts
  in.

Non-guarantees:

- Remote actor/entity delivery is still at-most-once by default.
- Remembered entities are liveness metadata, not durable entity state or
  exactly-once delivery.
- Rakka internal remoting is not a public client protocol.
- v1 does not provide built-in TLS/mTLS or certificate lifecycle management.
- v1 does not include a durable distributed consensus store for shard coordination.
- Network partitions, process crashes, and Kubernetes disruptions can require application-level retry or workflow recovery.

Use the compatibility policy and rolling-update upgrade note for safe mixed-version deployments.

### External-arbiter membership contract

Symmetric clusters (every node hosts a slice of entities) determine shard
ownership from a **consistent external membership arbiter** rather than internal
gossip or consensus. A discovery/membership provider used this way must satisfy:

- Membership is whatever the external arbiter reports; a node is a member only
  while its registration there is live (for example an etcd lease that the node
  renews).
- The arbiter must be strongly consistent, so every node converges on the same
  up-set and therefore — with `DeterministicModuloShardAllocationStrategy`, which
  makes ownership a pure function of the up-set — the same shard ownership. No
  shared coordinator state is required.
- A node that cannot reach the arbiter loses its registration and is removed
  (fail-stop). If the arbiter is globally unavailable, no new ownership decisions
  are made and existing durable state is untouched.

Guarantees (under this contract):

- With a strongly-consistent arbiter, a network partition cannot produce two
  independent membership views, so an internal split-brain resolver is
  unnecessary and is intentionally omitted.

Non-guarantees:

- Arbiter liveness is not peer reachability. A node can hold a live registration
  yet be unreachable from peers; routed asks then fail with `entity-no-route`
  until the node is removed. Peer-reachability **self-fencing** (a node that
  cannot reach peers drops its own registration) closes this gap; reachability
  signals must never directly edit the ownership up-set, or independent nodes
  would disagree on owners.
- Without a consistent external arbiter (for example membership from internal
  gossip), partition safety is not guaranteed; Rakka does not ship an internal
  split-brain resolver.

See `rakka-cluster-coordination-strategy.md` for the rationale and direction.

### Single-writer for sharded durable entities

Core, remote, and sharded delivery are at-most-once, so single-writer is not a
property of topology. For a sharded entity backed by a durable state store,
single-writer is provided by **revision compare-and-set (CAS)** plus idempotent
inbox/outbox effects:

- Every durable write is conditioned on the expected revision; a stale or
  concurrent second writer's CAS fails (`CoordinatorRevisionConflict` for
  coordinator state, the store's revision-conflict error for entity state)
  instead of overwriting newer state.
- During a shard move or a transient membership disagreement two nodes may
  briefly drive the same entity; the durable CAS rejects the loser and idempotent
  effects dedupe, so the outcome is wasted or retried work, not corruption.
- This is the single-writer guarantee Rakka relies on instead of an Akka-style
  stop-the-world hand-off barrier. It holds only while every durable and outbox
  write stays CAS-guarded and idempotent; applications must preserve that.

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

## A2A Adapter

`rakka-a2a` exposes durable Rakka agent-workflow runs through the A2A protocol.

Guarantees:

- Public A2A commands are acknowledged only after the durable workflow inbox accepts them; a returned task means the command is durable, and duplicate retries deduplicate on the durable command id and dedup key.
- `A2A Task.id` maps to `AgentRunId` and the sharded entity id; owner-only work crosses remoting through the versioned, remote-safe `A2ARunRequest`/`A2ARunResponse` protocol, never local actor messages or store handles.
- Task projections and public task events are query/observability surfaces; durable run state plus durable inbox/outbox state remain authoritative. Stream replay is defined solely by durable projection event sequence, so reconnect through a different node resumes with no gap and no duplicate, and a cursor older than the retained window returns `resync` rather than a silent gap.
- Tenant and principal identity are part of the durable command boundary. In tenant-scoped mode every durable read and command carries a resolved tenant and unscoped reads are refused; a tenant mismatch is indistinguishable from a missing task.
- The crate never persists resolved credentials or secret material in plans, durable state, outbox effects, task events, logs, metrics, snapshots, or indexes; push credentials are rejected by default or replaced by an application-supplied logical binding reference.

Non-guarantees:

- A2A actor, remote, and sharded delivery inherit the at-most-once core contract; the durable guarantees above come from the inbox/outbox layer, not from delivery.
- Push notification (webhook) delivery is at-least-once: effects carry stable idempotency keys, and the webhook target must deduplicate for effective exactly-once processing.
- External model calls, tool calls, and peer A2A calls are at-least-once unless the target participates in idempotency.
- Rakka remoting stays trusted private cluster traffic and is never used as public A2A transport; authentication, authorization, TLS, and ingress policy remain application/operator responsibilities.

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
