# Phase 4 Clustered Sharded Entity A2A Agents

Status: implemented
Source spec: `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`

## Goal

Implement A2A streaming and push notification behavior over durable public task
events. Streams should be best-effort transport subscriptions, while task
execution remains durable and independent of any live client connection.

## Slices

### Slice 4.1: Task Event Projection Schema

Status: implemented

Work:

- Define the durable A2A task event table or store shape.
- Include tenant, task id, sequence number, event kind, event timestamp,
  projected task state, optional message, optional artifact, and redaction
  status.
- Define replay cursor encoding.
- Define retention and compaction policy.
- Define how projection records are rebuilt from Rakka runtime state when
  needed.

Acceptance:

- Events are ordered per tenant and task id.
- Replay cursors are opaque to clients and stable across process restart.
- Compaction never removes the latest task snapshot.

### Slice 4.2: Runtime Event To Task Event Bridge

Status: implemented

Work:

- Map Rakka runtime events to public A2A task events.
- Filter internal runtime events that should not be exposed.
- Coalesce high-frequency internal updates where public clients do not need
  every transition.
- Emit task snapshots before incremental updates.
- Keep event labels low-cardinality.

Acceptance:

- Public streams expose meaningful status, artifact, message, and terminal
  events.
- Internal-only errors and implementation details remain in logs/audit, not in
  the public task stream.

### Slice 4.3: `send_streaming_message`

Status: implemented

Work:

- Reuse Phase 2 `send_message` durable acceptance path.
- Return an SSE stream after durable acceptance.
- Emit the current task snapshot first.
- Replay task events from the durable event cursor.
- Continue with live updates through a projection watcher.
- Send heartbeat or status events when streams may be idle.
- End the stream on terminal task state or client disconnect.

Acceptance:

- Stream disconnect does not affect run execution.
- The initial event reflects durable accepted state.
- A quiet long-running task does not trip common idle timeouts when heartbeat is
  enabled.

### Slice 4.4: `subscribe_to_task`

Status: implemented

Work:

- Serve subscription streams from durable task events.
- Accept a replay cursor when available through metadata or query parameters in
  the selected binding.
- Fall back to current snapshot plus future events when no cursor is supplied.
- Authorize tenant/task access before opening the stream.
- Allow clients to reconnect through a different load-balanced node.

Acceptance:

- A client can reconnect to another node and recover the latest durable task
  state.
- Replay does not duplicate already acknowledged events when a valid cursor is
  supplied.
- A missing or compacted cursor returns a snapshot and resumes from the current
  projection.

### Slice 4.5: Stream Backpressure And Limits

Status: implemented

Work:

- Bound per-connection buffers.
- Bound per-node open streams.
- Bound per-task subscriber counts.
- Decide behavior for slow clients: drop connection with retry guidance rather
  than buffering unbounded events.
- Emit metrics for open streams, lagged streams, dropped streams, and replay
  latency.

Acceptance:

- Slow clients cannot grow unbounded memory.
- Over-limit errors are protocol-shaped and retryable where appropriate.
- Metrics do not use task ids as hot labels.

### Slice 4.6: Durable Push Config Store

Status: implemented

Implementation note: push configs are persisted in the example durable state
store. Tokens and auth credentials are redacted from persisted API records while
secret-presence metadata is retained for audit.

Work:

- Implement durable create, get, list, and delete for A2A
  `TaskPushNotificationConfig`.
- Key records by tenant, task id, and config id.
- Validate callback URL, auth shape, tenant, and target task access.
- Store only secret references or redacted auth metadata where possible.
- Project push config changes into audit logs.

Acceptance:

- Push config APIs work after process restart.
- Tenant isolation is enforced.
- Deleted configs are not used by future push sends.

### Slice 4.7: Push Delivery Through Durable Outbox

Status: implemented for durable scheduling; webhook worker deferred

Work:

- Schedule push sends as durable outbox effects when public task events are
  emitted.
- Use idempotency keys derived from task id, event sequence, and config id.
- Dispatch through the A2A SDK push sender or an adapter-owned HTTP client.
- Persist retry, success, failure, and exhaustion state.
- Avoid sending push notifications directly inside request handlers.

Acceptance:

- Push sends are retried durably.
- Duplicate dispatcher attempts do not create duplicate semantic notifications
  when the receiver honors idempotency.
- Push exhaustion is visible in task metadata, logs, or operational snapshots.

### Slice 4.8: Load Balancer Streaming Runbook

Status: implemented

Work:

- Document recommended ingress/load-balancer settings for SSE.
- Document that sticky sessions are optional and not required for correctness.
- Document reconnect expectations and cursor use.
- Document heartbeat behavior.

Acceptance:

- Operators have enough guidance to run streams behind a load balancer.

## Implementation Summary

- `examples/clustered-sharded-entity-a2a-agents/src/task_projection.rs` now
  carries public task event state, projected task state, redaction status,
  replay cursors, bounded per-task replay logs, snapshot-preserving compaction,
  and live watchers for stream subscribers. A replay cursor that the local log
  cannot prove contiguous (no log entry, or a sequence past the known
  revision) is rejected so callers re-bootstrap from the snapshot, and watcher
  senders are pruned once every subscriber disconnects.
- `examples/clustered-sharded-entity-a2a-agents/src/a2a_handler.rs` now serves
  `send_streaming_message`, `subscribe_to_task`, and A2A push config
  create/get/list/delete through the same durable acceptance and owner-routing
  boundaries used by Phase 3 commands. Streams subscribe to the local watcher
  before reading the snapshot (no lost-event window), and in clustered mode
  poll the shard owner's event log through `OpenStreamCursor` so streams on
  non-owner public nodes receive live updates, cursor replay without
  duplicates, and terminal completion.
- `examples/clustered-sharded-entity-a2a-agents/src/sharded_run_entity.rs`
  serves `OpenStreamCursor` (converge + replay after cursor, with snapshot
  resync when the cursor cannot be honored) and schedules push notification
  effects for task events emitted by read-path convergence, not just
  send/cancel commands.
- `examples/clustered-sharded-entity-a2a-agents/src/stream_limits.rs` adds
  bounded per-node/per-task stream admission and low-cardinality stream metrics.
- `examples/clustered-sharded-entity-a2a-agents/src/push_config.rs` adds the
  durable push config store plus durable notification effect scheduling for
  emitted public task events. Scheduling works from a per-task watermark over
  the retained event log: a schedule that fails after durable acceptance is
  healed by the next retry or read, and the derived idempotency keys
  deduplicate re-offered events. One config scan and one workflow inbox
  recovery serve each batch, with bounded re-drives on revision conflicts
  against the run actor's own inbox writes. Request handlers never send
  webhook HTTP calls directly.
- `examples/clustered-sharded-entity-a2a-agents/README.md` documents the Phase 4
  boundary and SSE/load-balancer expectations.

Follow-up before production push delivery: add an adapter-owned worker that
claims the scheduled notification effects, resolves credential bindings outside
the persisted config record, sends the A2A webhook, and records success,
retryable failure, or exhaustion through the workflow outbox APIs.

## Exit Criteria

- `send_streaming_message` and `subscribe_to_task` are served from durable
  task events.
- Stream reconnect works across public nodes.
- Push configs are durable.
- Push sends are durable outbox effects. Actual webhook HTTP dispatch is left to
  the follow-up adapter worker described above.

## References

- `docs/plans/clustered-sharded-entity-a2a-agents/spec.md`
- `crates/rakka-agent-workflow/src/runtime_events.rs`
- `crates/rakka-agent-workflow/src/outbox.rs`
- `crates/rakka-agent-workflow/src/dispatcher.rs`
- `crates/rakka-agent-workflow/src/postgres_query.rs`
- `crates/rakka-http/src/streaming.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/sse.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/push/store.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/push/sender.rs`
- `https://github.com/a2aproject/a2a-rs/blob/main/a2a-server/src/handler.rs`
