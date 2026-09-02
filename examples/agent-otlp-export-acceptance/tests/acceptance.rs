//! The walk's transcript is the documented one, and the mechanisms behind the
//! lines that report a number rather than assert one.

use std::sync::Arc;

use rakka_agent::otel::AgentGenAiSpanExporter;
use rakka_agent::{AgentSegmentOperation, AgentSegmentSink, AgentTelemetrySegment};
use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis};
use rakka_example_agent_otlp_export_acceptance::{
    run_acceptance, CONTENT_SENTINELS, EXPECTED_TRANSCRIPT,
};

#[test]
fn the_readme_transcript_matches_the_const() {
    let readme = include_str!("../README.md");
    let section = readme
        .split("## Expected stdout")
        .nth(1)
        .expect("the README documents the expected stdout");
    let block = section
        .split("```text\n")
        .nth(1)
        .and_then(|rest| rest.split("\n```").next())
        .expect("the expected stdout is a text fence");
    assert_eq!(
        block.lines().collect::<Vec<_>>(),
        EXPECTED_TRANSCRIPT,
        "the README transcript and the const disagree"
    );
}

#[tokio::test]
async fn the_transcript_is_exactly_the_documented_one() {
    let report = run_acceptance().await;
    assert_eq!(report.lines, EXPECTED_TRANSCRIPT);
    assert_eq!(report.span_kinds, 5, "all five span kinds reached the wire");
    assert!(
        report.spans_exported > 0,
        "the OTLP receiver was handed spans"
    );
    assert_eq!(
        report.histograms_with_exemplars, 3,
        "every exported histogram carries an exemplar"
    );
    assert_eq!(
        report.metrics_exported, 7,
        "the metric surface exported; the walk names every instrument it \
         expects, so a changed count is a wiring change to look at rather \
         than a floor to raise"
    );
}

/// Line 12 reports a number; this proves the mechanism behind it.
///
/// A segment whose durable context carries no `traceparent` has no trace to
/// belong to. Slice 6.3a refuses to invent one — a fabricated trace id is a
/// causal claim an operator would follow — and counts the segment as
/// unmappable instead. The walk's first `run-recover` segment is exactly that
/// case: it closes when the run entity activates, before any durable state
/// exists to carry a trace.
#[test]
fn a_segment_without_a_trace_is_counted_not_invented() {
    let exporter = AgentGenAiSpanExporter::new();
    let traceless = AgentTelemetrySegment::new(
        AgentSegmentOperation::RunRecover,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .ok();
    exporter.record(&traceless);
    assert_eq!(exporter.unmappable(), 1, "no trace, no exported span");
    assert_eq!(exporter.buffered(), 0, "and nothing invented in its place");

    let traced = AgentTelemetrySegment::new(
        AgentSegmentOperation::RunRecover,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(AgentTelemetryContext {
        trace_parent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        ..AgentTelemetryContext::default()
    })
    .ok();
    exporter.record(&traced);
    assert_eq!(exporter.unmappable(), 1, "the traced one maps");
    assert_eq!(exporter.buffered(), 1);
}

/// The content sentinels the walk plants are the ones its sweep looks for.
///
/// A sweep whose sentinels drifted away from what the adapter plants would
/// pass over a real leak, which is why both come from one array.
#[test]
fn the_sentinels_are_planted_where_the_sweep_looks() {
    let adapter = format!(
        "{:?}",
        rakka_example_agent_otlp_export_acceptance::flow::scripted_adapter()
    );
    for sentinel in CONTENT_SENTINELS {
        assert!(
            adapter.contains(sentinel),
            "the scripted model never plants {sentinel}, so sweeping for it proves nothing"
        );
    }
}

/// The exemplar correspondence is declared, and every entry names a real
/// metric and a real segment class.
#[test]
fn every_exemplar_source_names_a_real_metric_and_a_real_segment_class() {
    let classes: Vec<&'static str> = rakka_agent::AGENT_DOMAIN_METRIC_INSTRUMENTS
        .iter()
        .map(|instrument| instrument.name)
        .collect();
    for (metric, class) in rakka_example_agent_otlp_export_acceptance::sdk::EXEMPLAR_SOURCES {
        assert!(
            classes.contains(metric),
            "{metric} is an exemplar source but is not a catalogued instrument"
        );
        assert!(
            segment_labels().contains(class),
            "{class} is an exemplar source but is not a bounded segment class"
        );
    }
}

fn segment_labels() -> Vec<&'static str> {
    let _unused: Arc<()> = Arc::new(());
    vec![
        AgentSegmentOperation::RunRecover.as_label(),
        AgentSegmentOperation::InvokeAgent { agent_name: None }.as_label(),
        AgentSegmentOperation::EffectDispatch {
            effect_kind: "model",
        }
        .as_label(),
    ]
}
