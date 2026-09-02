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
| 17.7 every loop decision is a durable event and a correlated span event | `AgentDecisionEvent` recorded by `AgentLoopState::record_decision`, flushed after commit through `AgentDecisionEventSink`; `decision_span_event` attaches each one to the `decide` span under the closed allowlist | `decision_events.rs` (exactly-once across owner loss; a drop is a declared gap), `otel_span_mapping.rs::decisions_and_usage_reach_the_span_through_their_mappers`; the no-reasoning rule swept on the decoded wire by export walk line 10 | Met for the mechanism. **Partial for the vocabulary and the field list**: 4 of 13 decision kinds and 2 of 4 sources have a writer, `safety_class` and `reason_code` have none, and sequence, causation, correlation, selected tools, budget outcome, gate result, and before/after labels stay on the durable event and never reach the span — see "Owed" |
| 17.8 model calls are GenAI `chat` spans | `AgentSegmentOperation::ModelInference` closed by the dispatch pipeline → `chat {profile}`, `CLIENT`, `gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`; failures carry `error.type` and `rakka.error.code`; tokens are never invented (a zero direction is omitted, not written) | `telemetry_segments.rs::the_real_dispatch_pipeline_closes_its_own_segments`, `otel_span_mapping.rs::the_genai_attributes_are_written_by_the_mapping`, `::a_usage_direction_with_no_evidence_is_omitted_rather_than_written_as_zero`, `agent_metrics.rs::a_direction_the_provider_did_not_report_records_no_sample` | **Partial.** `gen_ai.usage.*` is attached to the `effect-dispatch` `CONSUMER` span, not the `chat` span; the provider name is the adapter boundary and the request model is the bounded profile; no response model, finish reason, cached/reasoning tokens, streaming timing, snapshot digest, or model-call latency instrument — see "Owed" |
| 17.9 effects are traceable from decision to outcome | `EffectSchedule` (`PRODUCER`, after durable acceptance), `ToolAuthorize`, `EffectDispatch` (`CONSUMER`, after the durable `Started` write), `ExecuteTool` (`gen_ai.tool.name`/`.type`) with `rakka.agent.effect.kind`, `.attempt`, `.status` (incl. `indeterminate`) and a bounded error code; the indeterminate close is an `ERROR` span two retention policies select on; the superseded-generation span link ties a re-dispatch to the attempt it replaced | `telemetry_segments.rs::the_real_dispatch_pipeline_closes_its_own_segments`, `effect_dispatch.rs::the_ambiguous_recovery_settlements_close_the_segments_that_select_them`, `otel_span_mapping.rs::the_spans_of_a_reconciled_re_invocation_reach_the_exporter`, `collector_allowlist.rs` (policy values against `AgentRunEffectStatus::as_label`) | Met for kind, attempt, outcome, error code, retention, and "never the idempotency key". **Partial** for the rest: no generation, safety class, or idempotency class on an effect span, durations rather than timestamps, no queue-delay instrument, and the indeterminate span links neither the ambiguous attempt nor a reconciliation decision — see "Owed". A tool's own HTTP/RPC/database child spans are the application's — see "Delegated" |
| 17.10 memory tier and retrieval outcome are distinguishable | `rakka.agent.memory.retrievals` (`backend`, `outcome`) and `rakka.agent.memory.ingress.outcomes` recorded from `RetrievalReport` at the turn fold; content is structurally unreachable — the snapshot types no mapper reads, a closed metric label set, a closed span allowlist | `metric_catalogue.rs` (bijection and bounded labels), `secret_exclusion.rs::the_metric_series_of_a_credentialed_run_carry_no_identifier_or_secret`, `agent_metrics.rs::a_run_records_bounded_agent_metrics_and_no_identifiers` | **Partial.** The two memory instruments have no test asserting a run records them; the `memory-operation` and `retrieval` segment classes are mapped and closed by nobody, so `rakka.agent.memory.tier` is allowlisted and unwritten; record count, latency, embedding, digest, and watermark reach no signal — see "Owed" |
| 17.11 checkpoints are distinguished and no span is held across a wait | `rakka.agent.checkpoint.kind` over `AgentCheckpointKind::as_label` on the `checkpoint-open` segment, closed inside the commit that parks the run; `run-resume` closes only when the phase actually leaves a wait; `AgentSegmentTimer` has no open-span type | `telemetry_segments.rs::a_checkpoint_park_closes_its_segment_and_holds_none_open`, `::a_run_closes_a_segment_for_every_committed_transition`, `otel_span_mapping.rs::the_retention_classes_have_attributes_to_select_on`; the parked context is durable per `trace_scenarios.rs::a_parked_checkpoint_carries_the_segment_a_resume_doubly_links` | Met for kind and for the no-held-span rule. **Unmet: the resume span links neither the parked span nor the incoming request span** — `agent_durable_resume_telemetry_context` has no production caller and `close_segment` stamps the run's ordinary context; scenario 22 builds the links in its own body. Status, resolver class, policy class, and wait duration reach no signal — see "Owed" |
| 17.11 recovery spans carry cause and outcome | `run-recover` closed on both paths of `AgentRunFacade::recover`, `rakka.agent.recovery.events` and `.duration` by `outcome`, error type `rakka.agent.recovery`; no identity is a metric label | `run_sharding_wiring.rs::a_sharded_run_records_its_metrics_through_the_settings_recorder`, `acceptance.rs::a_segment_without_a_trace_is_counted_not_invented`, export walk line 8 (the recovery histogram's exemplar is the recovery span) | **Partial.** Outcome and duration only: the segment closes with no attribute, so cause, prior state, new owner, recovered pending counts, and stale-write conflicts are absent — see "Owed" |
| 17.12 metrics: catalogue and bounded labels | `AGENT_DOMAIN_METRIC_INSTRUMENTS` (30 instruments) | `metric_catalogue.rs` — bijection in both directions, plus the `signal` and `error.type` vocabularies | Met for the instruments that exist; the 17.12 clauses with no instrument are listed in the catalogue's own "Owed" section |
| 17.12 export queue, drops, failures | `rakka.agent.telemetry.export.{queue,drops,unmappable}` and the `flush.failures` signal vocabulary | `telemetry_segments.rs::a_bounded_sink_publishes_its_loss_once_per_drop`; `exporter_failure.rs` | Met |
| 17.12 exemplars | `sdk::ExemplarReservoir` at the application boundary | Export walk line 8 — 3 of 3 exported histograms carry an exemplar into the run's trace; dropping the attachment fails it | Met for the three segment-derived histograms; see "Delegated" for what an exemplar means here |
| 17.13 structured logs carry trace context | `AgentLogEvent`, `allowlist_agent_log`, the `tracing` bridge | `exporter_failure.rs::an_exported_log_record_carries_its_name_scope_and_severity` — trace and span id read off the **decoded** record, with its event name, pinned scope name and severity band; export walk line 10 sweeps a real log record | Met at the record; the bridge's own filter is a deployment decision — see "Delegated" |
| 17.13 runtime events follow the durable transition, once | Task, team, and conversation history are appended on the settle pass after the compare-and-set that decided them, on a sequence the transition consumed, and deduplicated at the store on that sequence; the run's decision ring deduplicates on a derived operation id; a sink fault is infrastructure, never a refusal; `AgentCoordinationReplay` supplies the scoped cursor, bounded retention, and explicit `WindowExpired`; the six struggle signals are pure derivations over a snapshot | `goal_lifecycle.rs::the_audit_trail_is_history_recorded_once_per_transition`, `conversation_recovery.rs::a_loss_between_the_commit_and_the_history_flush_re_flushes_the_same_slots`, `decision_events.rs::the_decision_sequence_survives_any_owner_loss_exactly_once`, `conversation_turns.rs::history_faults_classify_as_infrastructure_not_refusals`, `coordination_replay.rs` (scenario 45), `operational_query.rs::moderation_exhaustion_reports_a_conversation_nothing_can_advance` ("a struggle signal mutates nothing") | Met. The decision ring is the one lossy log by design (`decision_drops` is the declared gap); the three history logs never lose an entry |
| 17.13 audit events | **Audit is history**, by recorded judgment against the section's `AgentAuditSink` wording: the three replayable history logs plus the decision ring are the audit record, each entry carrying identities, revisions, a digest, and a bounded detail and never a payload or credential; `AgentAuditEvent` exists only in the workflow substrate and `rakka-agent` never references it | `secret_exclusion.rs::the_sweep_names_every_durable_record_kind_and_the_workflow_substrate` (every `AgentRecordKind` classified without a wildcard), `goal_view.rs::content_never_leaks_into_the_view`, `trace_scenarios.rs::default_telemetry_carries_no_content_or_credentials` | **Partial.** Goal, wake, epoch, task, delegation-as-fan-in, team, conversation, and run decisions have a sequenced record; agent lifecycle and settings, autonomy admission, budget, tool binding and descriptor revision, and shard ownership have durable state but no event, and the agent entity class refuses replay by design — see "Owed" |
| 17.14 minimise before export | Closed allowlist applied before a record is built; bridge attribute bounds | `otel_span_mapping.rs`; export walk line 10 sweeps the **decoded OTLP payload** | Met |
| 17.14 Collector allowlist as defence in depth | `transform/allowlist` `keep_keys` over 30 / 44 / 35 keys; the datapoint rule conditioned on the instrument name (`^rakka\.agent`, no trailing dot, so it reaches `rakka.agent_workflow.*` as well) and the span rule on the emitting scope (`instrumentation_scope.name == "rakka.agent"`), the log rule deliberately unconditioned because bridged `tracing` records do not carry the pinned scope | `collector_allowlist.rs` — bijective against the constants, plus the reach of each rule and the values every retention policy selects on | Met |
| 17.15 baggage is untrusted and never exported | `from_telemetry_context` copies no baggage | `otel_span_mapping.rs` (reverting the copy fails it) | Met |
| 17.16 retention classes | Ten `tail_sampling` policies | `agent_otel_collector_topology.rs`, `collector_allowlist.rs` | Met as configuration; **the decision window is inferred** — see "Inferred" |
| 17.16 trace-ID-aware routing | `loadbalancing` with `routing_key: traceID` over a headless service | `the_gateway_service_the_router_resolves_is_headless` | Met as configuration; **not exercised against a running two-replica gateway** — see "Inferred" |
| 17.16 sized together | Decision wait, `num_traces`, memory limiter, queues, retry, with the arithmetic written down | topology `.md`; `every_retention_class_has_a_policy` asserts the sizing keys exist | Met as configuration; the numbers are a starting point for a deployment to tune |
| 17.17 the application owns the SDK | `examples/agent-otlp-export-acceptance/src/sdk.rs` — SDK, subscriber, layer, exporter, credentials, shutdown/flush | Export walk line 1; no `crates/` directory imports `opentelemetry*` | Met |
| 17.17 the adapter preserves kind, status, events, links, scope | `AgentTelemetryExport::span_batch` | Export walk lines 3, 4 | Met |
| 17.17 unit, bucket, temporality semantics | `OpenTelemetryInstrumentView` → `Metric.unit`, `HistogramDataPoint.bounds`/`.bucket_counts` | Export walk lines 6, 7 — asserted on the decoded protobuf, including the `+Inf` bucket | Met |
| 17.17 Collector provides limiting, batching, retry, enrichment, allowlist, sampling, routing, health | The topology, both tiers | `agent_otel_collector_topology.rs` (12 tests) | Met as configuration; **TLS/mTLS/authentication and network isolation are delegated** |
| 17.17 pinned distribution, revalidated on upgrade | `otel/opentelemetry-collector-contrib:0.159.0`, with a stated procedure | `both_tiers_pin_the_reviewed_collector_distribution`; the gated `validate` arm | Met |
| 17.18 an authoritative point query that survives passivation and export loss | `agent_operational_snapshot` and `agent_task_operational_snapshot`: one durable read, no activation, schema checked, a non-optional `revision` and `observed_at`; `AgentCancellationProgress` carries all six states and is derived from what the record proves, never from acceptance of a cancel request; the view is documented as a read model that must never authorize or advance execution | `operational_query.rs::the_snapshot_answers_from_durable_state_with_telemetry_unavailable`, `::the_snapshot_reports_the_reference_facts_after_any_owner_loss`, `::the_task_snapshot_answers_the_continuous_checklist_while_passivated`, `::cancellation_progress_follows_the_durable_record` | Met for the durable half. **Partial** for the residency half: last activation, recovery, passivation, shard owner, and dispatcher state are absent from the agent-domain snapshot, and `next_wake` reads checkpoints only — see "Owed" |
| 17.18 session, task, and goal projections | `assemble_agent_session_view` (tenant + agent + run: snapshot, decisions with their lag, linked trace segments); `authorized_agent_goal_view` (bounded causal cut with per-node revisions, omissions as markers, deny ≡ absent); both reachable over A2A | `operational_query.rs::the_session_view_joins_decisions_and_reports_its_own_lag`, `goal_view.rs` (identical answers after restart without activation; non-owner reads exactly what a missing goal answers), `coordination_surface.rs::a_denied_goal_view_is_indistinguishable_from_an_absent_one` | Met for the session and goal views as far as they reach. **Partial**: the task projection is a point read and the assembling projection is not built; the session view omits correlated logs, retrieval metadata, residency transitions, and content artifacts; the goal view does not join teams, conversations, or pre-handoff run generations — see "Owed" |
| 17.19 operational views and alerts | The bounded instruments a dashboard can be built from are the catalogue's 30, plus the substrate's dispatcher, inbox, outbox, shard, and mailbox gauges that the catalogue deliberately does not mirror | `metric_catalogue.rs` holds the instrument set | **Not met as shipped artifacts, and delegated as thresholds.** No agent-domain dashboard or alert definition exists in any format; the one dashboard document in the repository is the workflow domain's and names no `rakka.agent.*` instrument. Nine of the section's bullet families have no instrument at all — see "Owed" and "Delegated" |
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

These four rows are the telemetry half of the agent domain's pinned-dependency
matrix. The whole matrix — these beside the A2A protocol, the A2A SDK, and Rig
— is the `Pinned dependencies` table of
[`rakka-compatibility.md`](rakka-compatibility.md), which a test holds to the
manifests and constants; this table is kept here for the rationale that
follows it.

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
- **A tool's own child spans.** `execute_tool` wraps the application's
  `AgentDispatchToolExecutor` and instruments nothing beneath it; the HTTP,
  RPC, database, process, or A2A client spans 17.9 expects under it are the
  application's, parented on the durable trace context Rakka hands the
  executor.
- **Alert thresholds and dashboards.** 17.19 leaves thresholds to deployment
  policy, and the substrate ships no hosted dashboards or alert rules for any
  domain. What Rakka owes is the bounded instrument to build one from, and
  the "Owed" section names the families that still lack one.

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

The rows for 17.7 through 17.11, 17.13's audit half, 17.18, and 17.19 were
added after the phase closed, on a reading of each section against the code
rather than against the slice plans. They owe the following, and two of them
are MUSTs the runtime does not meet today.

- **The resume span does not link the parked span** (17.11 MUST). The
  parked context is durable, `AgentTelemetryContext::span_links` is bounded
  and exported, and `agent_durable_resume_telemetry_context` builds the link —
  but nothing in `rakka-agent` calls it, and `close_segment` stamps every
  segment with the run's ordinary context. Scenario 22 constructs both links
  inside the test and asserts on its own construction, so it proves the
  helper, not the runtime. The same gap covers the incoming human or service
  request span.
- **The indeterminate transition links neither the ambiguous attempt nor a
  reconciliation decision** (17.9 MUST). The indeterminate close is a sibling
  of the attempt under the same parent, and there is no reconciliation segment
  class to link forward to; the only production span link runs the other way,
  from a re-dispatch to the generation it superseded.
- **Token usage rides the wrong span.** `gen_ai.usage.input_tokens` and
  `.output_tokens` are attached to the `effect-dispatch` `CONSUMER` segment at
  the attempt close, not to the `chat` `CLIENT` segment the convention places
  them on. A backend reading GenAI usage off model spans finds none.
- **The decision vocabulary is mostly unwritten.** `Continue`, `CallTools`,
  `SubmitResult`, and `Evaluate` are emitted; `Wait`, `Complete`, `Fail`,
  `RequestApproval`, `RequestAuthorization`, and `Reconcile` are declared with
  no writer; `delegate`, `handoff`, `team-operation`, and `moderated-turn` are
  not in the enum; `Human` and `AuthorizationService` sources have no writer;
  `AgentDecisionDraft::with_safety_class` and `::with_reason_code` are called
  only from a unit test, so `rakka.agent.decision.reason` and
  `rakka.agent.effect.safety` are allowlisted at both layers and unwritten on
  the decision path — the defect class named above, three more times.
- **17.8's provider fields have no slot.** Response model, finish reason,
  cached and reasoning tokens, streaming flag and first-chunk timing, the
  context-snapshot digest, and a model-call latency instrument do not exist;
  `rakka.agent.turn.duration` measures the durable round trip. The
  `rakka.agent_workflow.model.*` and `.tool.*` instruments are catalogued in
  the substrate and written by nothing.
- **Effect spans carry no generation, safety class, or idempotency class.**
  Generation is a dispatch-ticket attribute that never reaches a span; safety
  class reaches decision events and the `effect.outcomes` label but no effect
  or tool span; idempotency support is expressed nowhere (the "never the key"
  half is met by the closed allowlist). Queue delay has no instrument in
  either domain with a writer.
- **Span-link attributes are bounded, not allowlisted.** `segment_span`
  allowlists span and event attributes; link attributes are copied from the
  persisted context, bounded in count and length at persist and validated for
  shape at export, but not filtered by key. The one production writer sets
  `rakka.agent.link.kind`.
- **The memory instruments are recorded and unproven.**
  `rakka.agent.memory.retrievals` and `.ingress.outcomes` are written at the
  turn fold and asserted by no test outside the catalogue bijection;
  `RetrievalReport::selected` — the record count — is computed and discarded
  at the metric boundary; `rakka.agent.memory.tier` is written only by the
  unemitted `memory-operation` arm.
- **`run-recover` closes with no attribute.** Cause, prior state, new owner,
  recovered pending counts, and stale-write conflicts are absent; only
  `outcome` and the duration exist. Checkpoint status, resolver class, policy
  class, and an agent-domain wait duration are absent likewise — the workflow
  substrate's `human.wait.latency_ms` measures a checkpoint type this crate
  never constructs.
- **Five audit families have durable state and no event.** Agent lifecycle
  and settings (provenance on the revision, no sequenced log, and the agent
  class refuses replay by design), autonomy admission, budget, tool binding
  and descriptor revision, and shard ownership. `AgentAuditEventId` sits on
  three durable records as a reference nothing in the repository writes.
- **The residency half of 17.18 is absent** from the agent-domain snapshot —
  last activation, recovery, passivation, shard owner, dispatcher — and the
  run snapshot's `next_wake` reads checkpoints, with timer occurrences joined
  only through the task-side `next_pending_wake_for_task`. The assembling
  task projection is not built; the goal view does not join teams,
  conversations, or run generations before the latest handoff.
- **17.19 has no artifact.** No dashboard, no alert rule, in any format, and
  nine bullet families with no instrument to build one from: autonomy
  admission, budget, streaming delay, decision latency, A2A acceptance
  latency, dependency age, delegation depth and fan-out and cycles, team
  backlog and claim age, checkpoint count and age and timeout. The families
  that do have an instrument mostly have a transition counter and no gauge or
  duration, for the reason the catalogue gives.

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
