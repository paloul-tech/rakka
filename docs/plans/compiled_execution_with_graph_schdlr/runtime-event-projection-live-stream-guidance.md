# Runtime Event Projection And Live Stream Guidance

Status: implemented.

Runtime events are post-persistence projection records. Durable graph state
remains the source of correctness, while runtime events give product backends a
compact stream for run history, live execution views, logs, metrics, audit
correlation, and trace stitching.

## Boundary

Rakka owns the event contracts, validation, ordering rules, projection helpers,
and sink trait. The web product owns product API routes, authentication,
authorization, trigger registration, tenant policy, and the UI-specific view
model.

An HTTP, SSE, WebSocket, or GraphQL stream should therefore be an adapter over
`AgentRuntimeEventSink` output or a product-owned durable projection store. It
should not become Rakka's canonical product API.

## Projection Pipeline

The recommended flow is:

1. A graph state transition is persisted.
2. `AgentRuntimeEventDraft::after_persistence` finalizes the event from the
   persisted `AgentGraphRunState`.
3. The runtime records the event through `AgentRuntimeEventSink`.
4. The product backend updates its run-history projection and forwards live
   updates to connected clients.
5. Clients resume from the last observed per-run `event_sequence`.

`AgentRuntimeEventProjection` can rebuild a run-level projection from a full
ordered event stream. Product projections may store richer UI state, but should
keep `run_id`, `workflow_id`, `definition_version`, `plan_fingerprint`,
`last_event_sequence`, `last_scheduler_revision`, `last_event_kind`, and
terminal status as first-class fields.

## Live Stream Cursor

Use `(run_id, event_sequence)` as the resume cursor for run-scoped streams.
Cross-run global ordering is not required in v1. A product stream can expose:

- events after a cursor;
- latest projection snapshot plus events after that snapshot;
- completion marker when `RunCompleted`, `RunFailed`, or `RunCancelled` is
  observed.

If a sink write fails, the graph transition is still authoritative. Products
that require stronger event delivery should route runtime events into a durable
outbox or audit-backed projection path in a higher integration layer.

## Logs, Audit, And Trace Context

`AgentRuntimeEvent::correlation_fields` returns the stable field set shared by
runtime events, logs, audit records, and traces. `log_attributes` maps those
fields to the existing Rakka log/audit attribute names where available.

Use these fields for correlation:

- `workflow_id`;
- `run_id`;
- `definition_version`;
- `runtime_event_kind`;
- `node_id`, `effect_id`, `timer_id`, or `checkpoint_id` when scoped;
- `causation_id`;
- `correlation_id`;
- `telemetry_context`.

These ids are appropriate for logs, audit records, traces, and durable query
indexes. They are not appropriate as hot metric labels.

## Metric Cardinality

Runtime event `attributes` are validated as bounded hot-projection labels. They
may include values such as lifecycle status, node kind, effect kind, target
class, trigger kind, outcome, error code, tenant tier, and deployment channel.

Do not place raw ids, prompts, payloads, credential refs, unbounded error
strings, stack traces, or artifact ids in runtime event attributes. Put those
values in typed event fields, artifact refs, logs, audit records, or traces.

## Product UI Guidance

Run-history views should read from the product projection store or event sink
query, not from raw durable graph state for every transition. Durable graph
state is still useful for reconciliation, recovery, and detail views.

A practical UI model can include:

- run header: workflow id, version, plan fingerprint, terminal status, latest
  event sequence;
- timeline rows: event sequence, timestamp, event kind, scoped id, bounded
  attributes;
- node summary: latest observed status by node id, attempt counts, waiting
  state, selected branches, loop iteration counters;
- side-effect summary: effect id, target class, scheduled/completed/failed
  status, retry/outcome labels;
- correlation links: causation id, correlation id, trace context, audit record
  refs when available.

The projection should tolerate duplicate event delivery by treating identical
`(run_id, event_sequence)` records as idempotent and rejecting conflicting
records with the same cursor.
