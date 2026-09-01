//! The walk's transcript and the typed facts behind it.

/// One stable line per claim the walk proves.
#[derive(Debug, Clone)]
pub struct AcceptanceReport {
    /// The transcript, one line per claim.
    pub lines: Vec<String>,
    /// Spans the OTLP receiver was handed.
    pub spans_exported: usize,
    /// Metric groups the OTLP receiver was handed.
    pub metrics_exported: usize,
    /// Exported histograms carrying an exemplar into the run's trace.
    pub histograms_with_exemplars: usize,
    /// Distinct span kinds that reached the exporter.
    pub span_kinds: usize,
}

/// The exact transcript the walk prints, and the one `README.md` documents.
pub const EXPECTED_TRANSCRIPT: &[&str] = &[
    "ok  1/12 one sharded run completed with the SDK, subscriber, exporter and flush owned by this binary: 38 spans mapped",
    "ok  2/12 11 distinct convention span names left the binary, mapped from the ungated segment vocabulary",
    "ok  3/12 all five span kinds exported, the A2A ingress SERVER span among them",
    "ok  4/12 every exported span carries the pinned convention revision 1.36.0",
    "ok  5/12 all 38 spans joined the ingress trace, each with its own span id",
    "ok  6/12 7 metrics reached the OTLP receiver, every one carrying its catalogued unit",
    "ok  7/12 3 histograms exported with the catalogue's boundaries and the +Inf bucket",
    "ok  8/12 3 of 3 exported histograms carry an exemplar linking to the run's trace",
    "ok  9/12 38 spans and every metric crossed a real OTLP gRPC socket",
    "ok 10/12 no prompt, tool argument, result, or exporter credential appears in \
     the decoded OTLP payload, the bridge record's Debug, or its serialization",
    "ok 11/12 the run completed, the tool ran exactly once, and all 4 deciding transitions were \
     durably recorded: telemetry changed no durable outcome",
    "ok 12/12 export queue depth published, nothing dropped, and 1 pre-trace segment counted unmappable rather than exported under an invented trace",
];
