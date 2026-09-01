# Agent Domain Telemetry Validation Matrix

Status: implemented (slice 6.3b).

This document maps [specification 17](plans/rakka-agent/spec.md) to the code
that emits it, the deployment that carries it, and the tests that prove it. The
goal is not to claim the agent domain is well observed. It is to say precisely
which telemetry claims are *enforced*, which are *delegated* to the deployment,
and which remain *inferred* rather than demonstrated.

The companion documents are
[`rakka-agent-observability-catalogue.md`](rakka-agent-observability-catalogue.md),
which is the rendered metric and segment catalogue this page cites rather than
repeats;
[`rakka-agent-security-validation-matrix.md`](rakka-agent-security-validation-matrix.md);
and
[`rakka-agent-fault-injection-matrix.md`](rakka-agent-fault-injection-matrix.md),
whose closing section named telemetry validation as still owed.

## What the two slices found

Slice 6.3a's finding was an adapter that shipped fully unit-tested and entirely
unreachable. Slice 6.3b's is narrower and of the same family: **three defects
that only a real SDK, a real socket, and a real Collector could see.** Each is
recorded with the falsification that proves its test is load-bearing.

| Defect | What it was | What closed it | Falsified by |
| --- | --- | --- | --- |
| The delivery path recorded no metrics | `InProcessRunResultDelivery` threads a segment sink but never threaded a `MetricsRecorder`. Delivering a durable result is what folds the turn and settles the effect, so `rakka.agent.turn.duration`, `rakka.agent.model.tokens`, `rakka.agent.effect.outcomes` and `rakka.agent.effect.outstanding.duration` were recorded by **nobody** in an in-process deployment, while the sharded entity beside it reported a healthy metric surface. | `InProcessRunResultDelivery::with_metrics`, in the same shape as `with_segments` and for the same stated reason: every driver of a run must share one wiring. | Removing it drops the walk from 7 metrics, 3 histograms and 3 exemplars to 4, 1 and 1, failing `the_transcript_is_exactly_the_documented_one`; removing the same driver's *segment* sink drops it from 38 spans to 21 |
| `container.name` is not a Collector field | The shipped `k8sattributes` metadata list — copied from the workflow topology, which pins a 2024 distribution — names a field the current distribution rejects. The Collector refuses to start. | `k8s.container.name`, and a gated arm that runs the pinned distribution's own `validate`. | Restoring `container.name` fails `optional_collector_config_validation_is_gated` with the distribution's own error |
| `loadbalancing` cannot route metrics | Wiring the metrics pipeline to the `routing_key: traceID` exporter that traces need produces a Collector that fails at startup — and every string assertion for `loadbalancing` calls it correct. | A separate `otlp/gateway` exporter for the metrics pipeline; traces and logs keep the trace-id router. | Same gated arm; `the_agent_tier_routes_metrics_off_the_trace_router` catches the shape |
| A retention policy could select on nothing | A `tail_sampling` policy keyed on an attribute no mapping function writes retains nothing in production while passing every string assertion about the YAML. This is the failure the slice was explicitly warned about. | `crates/rakka-agent/tests/collector_allowlist.rs` asserts every policy key is a key `is_agent_span_attribute` accepts, **and** that the allowlist running before the sampler does not strip it. | Renaming one policy key to `rakka.agent.effect.state` fails both assertions; restoring `probabilistic_sampler` in the traces pipeline fails `the_gateway_allowlists_then_tail_samples` |
| The allowlist could drift from the code | `rakka-k8s` cannot see `AGENT_SPAN_ATTRIBUTE_KEYS`, so a topology test in that crate can only compare a list of strings to a copy of itself. | The bijection lives in `rakka-agent`, reads the same YAML, and fails in both directions. | Removing `rakka.agent.effect.status` from the span `keep_keys` fails `the_span_allowlist_is_the_span_vocabulary` |

## Specification 17, clause by clause

| Clause | Enforced where | Proof | Status |
| --- | --- | --- | --- |
| 17.1 telemetry is never a correctness input | Every sink is synchronous, infallible, and bounded; `AgentSegmentSink` states the rule | `trace_scenarios.rs` (scenario 26), `exporter_failure.rs::an_unreachable_collector_changes_no_durable_outcome` | Met |
| 17.2 instrumentation scope and resource | `agent_instrumentation_scope`, stamped on every batch by `bridge_export` | Export walk line 4 — every span on the wire carries schema URL `1.36.0` | Met |
| 17.3 session/task/goal correlation | `AgentSegmentIdentity`, `AgentGenAiIdentity` | `otel_span_mapping.rs` | Met for run identity; `of_task` still has no production caller, so `rakka.agent.task.id` and `.goal.id` are declared and unwritten — see "Owed" |
| 17.4 bounded trace segments | 24 `AgentSegmentOperation` classes closed at entity, dispatcher and A2A boundaries | `telemetry_segments.rs`; export walk line 2 (11 distinct span names on the wire) | **11 of 24 classes have a production call site** — see "Owed" |
| 17.5 durable trace context | `AgentTelemetryContext` persisted through every durable boundary | Export walk line 5 — all 38 spans of one run joined the ingress trace | Met |
| 17.6 required span model | `AgentGenAiOperation::span_name` / `span_kind` / status mapping | Export walk line 3 — all five span kinds reached the wire | Met |
| 17.12 metrics: catalogue and bounded labels | `AGENT_DOMAIN_METRIC_INSTRUMENTS` (30 instruments) | `metric_catalogue.rs` — bijection in both directions, plus the `signal` and `error.type` vocabularies | Met for the instruments that exist; the 17.12 clauses with no instrument are listed in the catalogue's own "Owed" section |
| 17.12 export queue, drops, failures | `rakka.agent.telemetry.export.{queue,drops,unmappable}` and the `flush.failures` signal vocabulary | `telemetry_segments.rs::a_bounded_sink_publishes_its_loss_once_per_drop`; `exporter_failure.rs` | Met |
| 17.12 exemplars | `sdk::ExemplarReservoir` at the application boundary | Export walk line 8 — 3 of 3 exported histograms carry an exemplar into the run's trace; dropping the attachment fails it | Met for the three segment-derived histograms; see "Delegated" for what an exemplar means here |
| 17.13 structured logs carry trace context | `AgentLogEvent`, `allowlist_agent_log`, the `tracing` bridge | `exporter_failure.rs::an_exported_log_record_carries_its_name_scope_and_severity` — trace and span id read off the **decoded** record, with its event name, pinned scope name and severity band; export walk line 10 sweeps a real log record | Met at the record; the bridge's own filter is a deployment decision — see "Delegated" |
| 17.14 minimise before export | Closed allowlist applied before a record is built; bridge attribute bounds | `otel_span_mapping.rs`; export walk line 10 sweeps the **decoded OTLP payload** | Met |
| 17.14 Collector allowlist as defence in depth | `transform/allowlist` `keep_keys` over 30 / 44 / 35 keys | `collector_allowlist.rs` — bijective against the constants | Met |
| 17.15 baggage is untrusted and never exported | `from_telemetry_context` copies no baggage | `otel_span_mapping.rs` (reverting the copy fails it) | Met |
| 17.16 retention classes | Ten `tail_sampling` policies | `agent_otel_collector_topology.rs`, `collector_allowlist.rs` | Met as configuration; **the decision window is inferred** — see "Inferred" |
| 17.16 trace-ID-aware routing | `loadbalancing` with `routing_key: traceID` over a headless service | `the_gateway_service_the_router_resolves_is_headless` | Met as configuration; **not exercised against a running two-replica gateway** — see "Inferred" |
| 17.16 sized together | Decision wait, `num_traces`, memory limiter, queues, retry, with the arithmetic written down | topology `.md`; `every_retention_class_has_a_policy` asserts the sizing keys exist | Met as configuration; the numbers are a starting point for a deployment to tune |
| 17.17 the application owns the SDK | `examples/agent-otlp-export-acceptance/src/sdk.rs` — SDK, subscriber, layer, exporter, credentials, shutdown/flush | Export walk line 1; no `crates/` directory imports `opentelemetry*` | Met |
| 17.17 the adapter preserves kind, status, events, links, scope | `AgentTelemetryExport::span_batch` | Export walk lines 3, 4 | Met |
| 17.17 unit, bucket, temporality semantics | `OpenTelemetryInstrumentView` → `Metric.unit`, `HistogramDataPoint.bounds`/`.bucket_counts` | Export walk lines 6, 7 — asserted on the decoded protobuf, including the `+Inf` bucket | Met |
| 17.17 Collector provides limiting, batching, retry, enrichment, allowlist, sampling, routing, health | The topology, both tiers | `agent_otel_collector_topology.rs` (12 tests) | Met as configuration; **TLS/mTLS/authentication and network isolation are delegated** |
| 17.17 pinned distribution, revalidated on upgrade | `otel/opentelemetry-collector-contrib:0.159.0`, with a stated procedure | `both_tiers_pin_the_reviewed_collector_distribution`; the gated `validate` arm | Met |
| 17.20 pinned convention revision, reviewed on upgrade | `AGENT_GENAI_CONVENTION_REVISION = "1.36.0"` | `the_documented_convention_revision_is_the_pinned_one` | Met |
| Scenario 24 sampling changes no durable record | Sampling is a Collector decision; no Rakka path reads a sampled flag for control | `trace_scenarios.rs` | Met |
| Scenario 25 no content or credentials in default telemetry | Two-layer redaction | Export walk line 10; `secret_exclusion.rs`; `agent_metrics.rs` | Met, now proven **on the wire** rather than at durable state |
| Scenario 26 unavailable export blocks nothing, loss is visible | Bounded sinks and counters | `exporter_failure.rs`, both always-run arms | Met, now proven against a **real exporter** rather than the decision sink |

## Pinned versions

| Component | Pin | Recorded in |
| --- | --- | --- |
| OpenTelemetry Rust SDK | `opentelemetry` / `opentelemetry_sdk` / `opentelemetry-otlp` / `opentelemetry-appender-tracing` / `opentelemetry-proto` `=0.29.0` | `[workspace.dependencies]`, with the rationale in a comment |
| Collector distribution (agent domain) | `otel/opentelemetry-collector-contrib:0.159.0` | `kubernetes-agent-otel-collector-topology.yaml`, both tiers |
| Collector distribution (workflow domain) | `otel/opentelemetry-collector-contrib:0.107.0` | `plans/agentic-workflow/kubernetes-otel-collector-topology.yaml` |
| GenAI semantic conventions | `1.36.0` | `AGENT_GENAI_CONVENTION_REVISION` |

**The SDK pin is chosen by two constraints that agree**, and it is worth
recording because the obvious reading — "take the newest" — is wrong here:

1. `opentelemetry-otlp` 0.29 is the **highest** release built on the
   workspace's declared `tonic 0.12` / `prost 0.13` generation.
2. `opentelemetry_sdk` **0.30 sealed `metrics::data`**: `ResourceMetrics`,
   `Histogram`, and `HistogramDataPoint` lost their public fields. 0.29 is the
   last release in which an application can *construct* them, and that is
   exactly what lets Rakka's already-aggregated `AgentOtlpBridgeExport` reach
   the exporter with the catalogue's declared units, bucket boundaries, and
   exemplars intact. On 0.30 or later the only path is re-recording every
   measurement through the `Meter` API and re-declaring the buckets in the
   application, which makes the catalogue advisory.

Upgrading past 0.29 is therefore a design change under
[17.17](plans/rakka-agent/spec.md#1717-otlp-and-collector-boundary), not a
version bump.

**The two Collector pins differ deliberately.** The workflow domain's topology
is a different domain's shipped artifact with its own gate and its own plan;
moving it is not this slice's change. The spread — a 2024 distribution against a
2025 convention revision — is recorded here rather than left implicit, and the
`container.name` defect above is what that spread looks like in practice.

## Delegated to the deployment, and named here so it is not assumed

- **Transport security.** The shipped exporters use `tls.insecure: true`
  against an in-cluster backend, and the agent tier's `hostPort` receivers are
  unauthenticated. TLS, mTLS, authentication, and network isolation are the
  operator's, exactly as
  [`rakka-v1-security-operational-defaults.md`](rakka-v1-security-operational-defaults.md)
  splits them for the substrate.
- **Exporter credentials.** They travel as OTLP headers, configured through
  `AgentOtlpExporterConfig::headers`. Rakka never persists, logs, or exports
  them — the walk sweeps for its own credential sentinel — but where they come
  from, how they rotate, and who may read them is the deployment's.
- **What an exemplar points at.** Rakka's `MetricsRecorder` has no trace
  identity to read: trace context here is an explicit value on a durable
  record, never an ambient one. The example's reservoir declares, in
  `EXEMPLAR_SOURCES`, which bounded segment class carries the identity for
  which histogram, and links to the most recent segment of that class. Two
  limits, stated rather than glossed: it is a **representative** link, not a
  per-measurement one; and the span id it carries is the segment's **parent** —
  the span the operation ran under — because a segment's own id is derived by
  the sink afterwards from an emission ordinal the reservoir never sees. A
  reader lands in the right trace at a real span. A deployment with a recorder
  that can read an ambient context may do better.
- **Which `tracing` targets reach the log pipeline.** The example's bridge is
  filtered to Rakka's own targets. That is not a preference: unfiltered, the
  appender turns the OTLP exporter's own transport events into log records, and
  exporting those produces more of them. It overflowed a runtime worker's stack
  and aborted the process before the filter was added. Any deployment wiring
  this bridge owns that filter.
- **The backend.** One or more exporters selected by the operator; the topology
  ships `debug` and one OTLP endpoint from an environment variable.

## Inferred, and why

- **Tail sampling is not exercised against a running gateway pair.** The
  routing, the policies and the sizing are contract-tested as configuration and
  the configuration is validated by the distribution's own `validate`. That the
  `k8s` resolver actually spreads a trace's spans to one replica needs a
  cluster, and no test here stands one up.
- **A trace can outlive its sampling decision.** A tail-sampling decision is
  made `decision_wait` after a trace's first span. An agent run parked on a
  human approval checkpoint resumes minutes or hours later, and the segments it
  closes then belong to the same trace and arrive after the decision. This is
  inherent to tail sampling, not a defect in the configuration, and it is why
  the retention classes select on attributes that appear early where possible.
  It is stated rather than measured.
- **The Collector's own telemetry is enabled, not asserted.** Both tiers
  publish on port 8888 and the manifests declare it; that the counters move
  under refusal, queue pressure, or export failure is not checked here.
- **The export walk is one node, one run.** Fleet-scoped telemetry — resident
  entity counts, activation rate, backlog and oldest age — has no instrument
  yet, for the reason the catalogue gives: Rakka keeps no index to enumerate
  them from, so they need the bounded, deployment-invoked sweep shape of
  `AgentMemoryRetentionSweep`.

## Owed, and why

- **13 of 24 segment classes have no production call site.** `wake-admit`,
  `autonomy-admit`, `budget-reserve`, `budget-settle`, `validate-task-result`,
  `handoff`, `team-operation`, `moderation-turn`, `workflow-invoke`,
  `goal-evaluate`, `delegate-to-peer`, `memory-operation`, and `retrieval` are
  mapped and unemitted. The wiring is the same sink-threading shape the run
  entity uses; the work is in the entities that own those transitions.
- **`AgentSegmentIdentity::of_task` has no caller**, so `rakka.agent.task.id`,
  `rakka.agent.goal.id`, and `rakka.agent.delegation.id` are allowlisted at
  both layers and written by nothing. This is the defect class slice 6.3a's
  follow-up pass closed for four other attributes, and it is still open for
  these three.
- **`gen_ai.agent.name` and `gen_ai.agent.version` are hard-coded `None`** in
  `identity_of`. The segment identity does not carry them.
- **The 17.12 clauses with no instrument** are enumerated in the observability
  catalogue's own "Owed" section and not repeated here.
- **NetworkPolicies for the agent-domain telemetry lanes**, matching the
  workflow domain's `kubernetes-security-policy.yaml`.

## Repeatable commands

```sh
# The export walk: a real run, a real SDK, a real OTLP socket.
cargo run -p rakka-example-agent-otlp-export-acceptance

# Everything that always runs.
cargo test -p rakka-example-agent-otlp-export-acceptance
cargo test -p rakka-agent --all-features --test collector_allowlist
cargo test -p rakka-agent --all-features --test metric_catalogue
cargo test -p rakka-agent --all-features --test otel_span_mapping
cargo test -p rakka-agent --all-features --test telemetry_segments
cargo test -p rakka-agent --all-features --test trace_scenarios
cargo test -p rakka-k8s --test agent_otel_collector_topology

# Gated: the Kubernetes objects, and the Collector configurations against the
# pinned distribution itself. The second is the one that knows what a
# Collector configuration means.
RAKKA_AGENT_OTEL_VALIDATE_MANIFESTS=1 \
  cargo test -p rakka-k8s --test agent_otel_collector_topology -- --nocapture
RAKKA_AGENT_OTEL_VALIDATE_COLLECTOR_CONFIG=1 \
  cargo test -p rakka-k8s --test agent_otel_collector_topology -- --nocapture

# Gated: export to a live Collector.
docker run --rm -p 4317:4317 \
  -v "$PWD/docs/plans/agentic-workflow/otel-collector-local.yaml:/conf/collector.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.159.0 --config=/conf/collector.yaml
RAKKA_AGENT_OTEL_COLLECTOR_ENDPOINT=http://127.0.0.1:4317 \
  cargo test -p rakka-example-agent-otlp-export-acceptance --test exporter_failure -- --nocapture
```

## Production interpretation

Passing these means the agent domain emits what the reviewed convention
revision requires, that the emission leaves the process over OTLP with its
kinds, statuses, scope, units, buckets, and exemplars intact, that no content
or credential reaches the wire, and that losing the export path changes no
durable outcome and is visible in bounded counters.

It does not remove the need for:

- TLS, authentication, and network isolation on every telemetry lane;
- a Collector deployment sized against the actual trace rate rather than the
  200/s this topology assumes, and tail-sampling policies tuned to it;
- a backend, its retention, and its access control — telemetry that carries the
  durable correlation identities 17.13 asks for is access-controlled data;
- alerting on the Collector's own refusal, queue, drop, and export-failure
  counters, which these manifests publish and nothing here consumes; and
- the 13 unemitted segment classes and three unwritten identity attributes
  above, before a dashboard built on them can be trusted to be complete.
