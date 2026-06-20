# Phase 7.2 Load, Back-Pressure, and Cardinality

Status: implemented.

This document captures the deterministic load and pressure checks added for
Slice 7.2. These checks are invariant tests, not final throughput benchmarks:
they prove that high-volume local scenarios stay bounded by configured batch,
queue, sample, and label policies.

## Goals

- Prove dispatcher workers claim bounded batches and respect target
  concurrency limits.
- Prove timer scans expose backlog pressure instead of firing unbounded work in
  one pass.
- Prove operator query views stay capped under larger run counts.
- Prove hot workflow metrics do not leak run ids, effect ids, timer ids,
  command ids, worker ids, idempotency keys, causation ids, or correlation ids.
- Keep the checks fast enough for regular local and CI execution.

## Implemented Coverage

| Area | Expected Bound | Test |
| --- | --- | --- |
| Dispatcher backlog | One claim pass returns at most configured batch and target-concurrency capacity, with `backpressure_limited` true when due work remains. | `cargo test -p rakka-agent-workflow --test load_backpressure_cardinality dispatcher_load_claims_bounded_work_and_keeps_metric_series_bounded` |
| Dispatcher snapshot size | Fleet snapshots report full counts but include only the requested sampled entries. | `cargo test -p rakka-agent-workflow --test load_backpressure_cardinality dispatcher_load_claims_bounded_work_and_keeps_metric_series_bounded` |
| Dispatcher metric cardinality | Dispatcher fleet, in-flight, and backlog metrics collapse many runs into bounded series. | `cargo test -p rakka-agent-workflow --test load_backpressure_cardinality dispatcher_load_claims_bounded_work_and_keeps_metric_series_bounded` |
| Timer backlog | Timer scans fire only `max_batch_size` due timers and report remaining backlog through `backpressure_limited`. | `cargo test -p rakka-agent-workflow --test load_backpressure_cardinality timer_backlog_load_uses_batch_limit_and_bounded_metrics` |
| Timer metric cardinality | Timer and timer-lateness metrics use status/outcome labels, not raw timer or run ids. | `cargo test -p rakka-agent-workflow --test load_backpressure_cardinality timer_backlog_load_uses_batch_limit_and_bounded_metrics` |
| Query views | Run and timer query limits cap operator result sets under larger indexed run counts. | `cargo test -p rakka-agent-workflow --test load_backpressure_cardinality query_views_stay_bounded_under_large_run_counts` |

## Related Existing Coverage

These existing tests cover shared runtime pressure primitives that agent
workflows rely on:

```sh
cargo test -p rakka-core --test local_actor_runtime bounded_mailbox_reports_full
cargo test -p rakka-http --test http_streaming request_body_stream_applies_backpressure_until_consumer_reads
cargo test -p rakka-persistence --test typed_persistence persistence_query_helpers_return_bounded_streams
cargo test -p rakka-agent-workflow --test dispatcher_fleet target_concurrency_limits_bound_claims
cargo test -p rakka-agent-workflow --test timers scanner_bounds_due_work_and_reports_late_firing
```

## Slice Command

```sh
cargo test -p rakka-agent-workflow --test load_backpressure_cardinality
```

## Back-Pressure Tuning Notes

- `AgentDispatcherFleetSettings::max_batch_size` should be sized to keep one
  worker pass short enough for pod drain and lease-renewal expectations.
- Target-level dispatcher limits should be lower than provider quotas. Use
  per-model or per-tool limits when one downstream target is more fragile than
  the rest of the fleet.
- Timer scanner `max_batch_size` should be tuned from observed due-timer
  backlog, lateness, and persistence latency. A large backlog should scale
  scanners or replicas instead of removing the batch bound.
- Actor mailbox capacity should remain finite. Full mailboxes should become
  visible through errors, mailbox-depth metrics, or autoscaling signals.
- Streaming adapters should use bounded buffers and propagate back-pressure to
  producers; large model/tool outputs should move to artifact references.
- Query endpoints should always enforce limits. Cursor pagination is a future
  scale improvement for browsing large operator result sets without offset
  scans.

## Cardinality Policy

Hot metrics may use bounded labels such as workflow type, definition version,
status, operation, outcome, detail, effect kind, target class, timer status,
component, queue, direction, and tenant tier. They must not include raw
workflow/run/effect/timer/command ids, worker ids, idempotency keys, prompts,
completions, tool output, stack traces, or raw error messages.

High-cardinality ids remain appropriate for traces, span links, structured
logs, and durable audit records where individual event lookup is expected.

## Production Interpretation

Passing this slice means the local workflow surfaces preserve bounded behavior
under deterministic load. It does not replace:

- Kubernetes replica-level load tests;
- PostgreSQL connection-pool, lock, vacuum, and index tuning;
- OpenTelemetry Collector memory-limiter and queue pressure testing;
- downstream model/tool provider quota tests;
- sustained soak tests with realistic prompt, tool, artifact, and callback
  payloads.
