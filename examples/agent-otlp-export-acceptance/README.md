# Agent OTLP export acceptance

A real OpenTelemetry SDK at a real application binary, exporting one real
sharded agent run to a real OTLP receiver over a real gRPC socket.

[Specification 17.17](../../docs/plans/rakka-agent/spec.md) puts the SDK, the
`tracing` subscriber and layer, the OTLP exporter, exporter credentials, and
shutdown/flush at the **application binary**, and keeps the Rakka crates SDK-
and version-neutral. Slice 6.3a built everything up to that line — a bounded
segment vocabulary closed on the path a run takes, a GenAI convention mapping
behind the `otel` feature, a documented metric catalogue with units and bucket
boundaries — and stopped at `AgentOtlpBridgeExport`, a serializable record
nothing in the workspace ever sent. This example is the other side of the line.

## What it proves that no other test can

Everything else in the repository asserts on Rakka's own types. This asserts on
**decoded OTLP protobuf that crossed a socket**. A mapping that OTLP rejects, a
signal wired to the wrong service, a unit or bucket boundary dropped in
translation, an attribute that survives the allowlist and reaches the wire —
none of those are visible from inside the bridge, and all of them are visible
here.

The receiver in `src/collector.rs` is a real gRPC server speaking the generated
OTLP service definitions on an ephemeral port. It is not a Collector: it applies
no processor, and the agent Collector's own configuration is contract-tested
separately against
[`kubernetes-agent-otel-collector-topology.yaml`](../../docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.yaml).
But it is the OTLP boundary, and it needs no container, so this claim is never
gate-only.

## Run

```sh
cargo run -p rakka-example-agent-otlp-export-acceptance
```

## Expected stdout

```text
ok  1/12 one sharded run completed with the SDK, subscriber, exporter and flush owned by this binary: 38 spans mapped
ok  2/12 11 distinct convention span names left the binary, mapped from the ungated segment vocabulary
ok  3/12 all five span kinds exported, the A2A ingress SERVER span among them
ok  4/12 every exported span carries the pinned convention revision 1.36.0
ok  5/12 all 38 spans joined the ingress trace, each with its own span id
ok  6/12 7 metrics reached the OTLP receiver, every one carrying its catalogued unit
ok  7/12 3 histograms exported with the catalogue's boundaries and the +Inf bucket
ok  8/12 3 of 3 exported histograms carry an exemplar linking to the run's trace
ok  9/12 38 spans and every metric crossed a real OTLP gRPC socket
ok 10/12 no prompt, tool argument, result, or exporter credential appears in the decoded OTLP payload, the bridge record's Debug, or its serialization
ok 11/12 the run completed, the tool ran exactly once, and all 4 deciding transitions were durably recorded: telemetry changed no durable outcome
ok 12/12 export queue depth published, nothing dropped, and 1 pre-trace segment counted unmappable rather than exported under an invented trace
```

## What each line demonstrates

| Line | Claim |
| --- | --- |
| 1 | The binary owns the SDK, the `tracing` subscriber and layer, the exporter, the credentials, and shutdown/flush. Rakka owns none of them. |
| 2 | The ungated `AgentSegmentOperation` vocabulary maps to GenAI convention span names, from production call sites rather than a test. |
| 3 | All five span kinds reach the wire, including the A2A ingress `SERVER` span — the only one of its kind in the workspace. |
| 4 | The pinned convention revision travels with the data, in every span's instrumentation scope (17.2, 17.20). |
| 5 | The `traceparent` accepted at A2A ingress survived every durable boundary, and each span carries its own derived id. |
| 6 | Every exported metric carries the unit its catalogue entry declares (17.17). |
| 7 | Histograms export with the catalogue's bucket boundaries and the OTLP `+Inf` overflow bucket, not as bare count/sum. |
| 8 | Every exported histogram carries an exemplar into the trace that produced it — the 17.12 clause slice 6.3a recorded as owed to the application boundary. |
| 9 | The batch crossed a real OTLP gRPC socket and was accepted, signal by signal. |
| 10 | Scenario 25 at the last boundary it could leak from: no prompt, tool argument, result, or exporter credential appears in the decoded payload. |
| 11 | Telemetry is never a correctness input (17.1): the run completed and the external system was called exactly once. |
| 12 | Export queue depth is published, nothing was dropped, and the one segment that closed before the run had a trace was **counted** rather than exported under an invented trace. |

## Deliberate limits

- **The receiver is not a Collector.** Allowlisting, tail sampling, trace-ID
  aware routing, and the Collector's own health telemetry are properties of the
  deployed configuration, and they are proven against the manifests in
  `crates/rakka-k8s/tests/agent_otel_collector_topology.rs` and
  `crates/rakka-agent/tests/collector_allowlist.rs`. A live-Collector arm is
  available behind `RAKKA_AGENT_OTEL_COLLECTOR_ENDPOINT`; see
  `tests/exporter_failure.rs`.
- **`rakka.agent.model.tokens` is absent from the export**, and correctly so:
  the deterministic model adapter reports no token usage, and slice 6.3a's rule
  is that a zero is not evidence of a zero, so an unreported direction records
  no sample.
- **The exemplar correspondence is declared, not inferred.** Rakka's
  `MetricsRecorder` has no trace identity to read — trace context here is an
  explicit value on a durable record, never an ambient one — so
  `sdk::EXEMPLAR_SOURCES` names which segment class carries the identity for
  which histogram, and a test asserts every entry names a real instrument and a
  real segment class.
