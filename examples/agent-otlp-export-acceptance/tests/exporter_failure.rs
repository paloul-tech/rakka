//! Exporter failure, proven by behaviour rather than by grep.
//!
//! The gateway Collector configuration carries `sending_queue` and
//! `retry_on_failure`, and until this slice the only test of that was a string
//! assertion that the words were present in the YAML. Scenario 26 was in the
//! same position on the Rakka side: it was proven against the durable decision
//! sink's refusal, which is not the export path.
//!
//! So these arms break the export path for real — an endpoint nothing is
//! listening on, and a buffer smaller than the run that fills it — and assert
//! the three things [17.1](../../../docs/plans/rakka-agent/spec.md) and
//! [17.12](../../../docs/plans/rakka-agent/spec.md) require: the durable
//! outcome does not change, the loss is counted in bounded metrics, and the
//! drain still returns.
//!
//! Both always run. The live-Collector arm below is gated in the established
//! idiom, so the claim is never gate-only.

use std::sync::Arc;

use rakka_a2a::agents::A2AAgentTarget;
use rakka_agent::otel::AgentGenAiSpanExporter;
use rakka_agent::{
    AgentRunStatus, AgentSegmentOperation, AgentSegmentSink, AgentTelemetrySegment,
    METRIC_AGENT_TELEMETRY_EXPORT_DROPS, METRIC_AGENT_TELEMETRY_FLUSH_FAILURES,
};
use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis};
use rakka_core::InMemoryMetricsRecorder;
use rakka_example_agent_otlp_export_acceptance::collector::InProcessCollector;
use rakka_example_agent_otlp_export_acceptance::flow::{
    drive_run, run_status, scripted_adapter, task_definition, EXPORTER_CREDENTIAL,
};
use rakka_example_agent_otlp_export_acceptance::sdk::{
    export_resource, exporter_config, AgentTelemetryExport, AGENT_OTLP_EXPORT_SIGNALS,
};
use rakka_example_agent_otlp_export_acceptance::wiring::World;

/// An address on the loopback interface with nothing listening on it.
///
/// Port 1 is privileged and unbound in every environment this suite runs in,
/// so the connection is refused rather than hanging — the arm is about export
/// failure, not about timeouts.
const UNREACHABLE: &str = "http://127.0.0.1:1";

fn agent_target() -> A2AAgentTarget {
    A2AAgentTarget::new(
        rakka_agent::AgentId::new("support-agent").expect("the agent id is valid"),
        task_definition(),
    )
}

/// An unavailable exporter changes no durable outcome, is counted, and drains.
#[tokio::test]
async fn an_unreachable_collector_changes_no_durable_outcome() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let export = AgentTelemetryExport::install(
        &exporter_config(UNREACHABLE, EXPORTER_CREDENTIAL),
        &export_resource(),
    )
    .expect("an exporter builds against an endpoint nothing answers")
    .with_metrics(metrics.clone());

    let world = World::new(scripted_adapter(), agent_target());
    let run_scope = drive_run(&world).await;

    let outcome = export
        .flush(
            &world.spans,
            &world.metrics.snapshot(),
            Vec::new(),
            &world.exemplars,
        )
        .await;
    assert!(
        outcome.failed_signals > 0,
        "an endpoint nothing answers must fail, not silently succeed"
    );
    assert_eq!(
        outcome.spans, 0,
        "a failed export reports nothing as exported"
    );

    // The correctness claim: the run is exactly where it would have been.
    assert_eq!(
        run_status(&world, &run_scope).await,
        Some(AgentRunStatus::Completed),
        "telemetry loss must not block the run"
    );
    assert_eq!(
        world.tools.invocation_count("charge-card"),
        1,
        "and must not change what the external system saw"
    );

    // The visibility claim: the loss is a bounded counter, by signal.
    let snapshot = metrics.snapshot();
    let failures = snapshot.observations_named(METRIC_AGENT_TELEMETRY_FLUSH_FAILURES);
    assert!(
        !failures.is_empty(),
        "an export failure that counts nothing is a silent loss"
    );
    for observation in &failures {
        let signal = observation
            .attributes()
            .iter()
            .find(|attribute| attribute.key() == "signal")
            .map(|attribute| attribute.value().to_string())
            .expect("every flush failure names its signal");
        assert!(
            AGENT_OTLP_EXPORT_SIGNALS.contains(&signal.as_str()),
            "`{signal}` is written but not declared in AGENT_OTLP_EXPORT_SIGNALS"
        );
    }

    // The drain claim: shutdown still returns rather than hanging on a dead
    // endpoint. A drain that blocks here would block a coordinated shutdown.
    let _drained = export.shutdown();
}

/// A saturated buffer drops, counts, and keeps the run's outcome intact.
///
/// The ring drops the **oldest** and counts, which is the rule
/// `AgentSegmentSink` states: a sink that cannot keep up must never block or
/// fail the operation that produced the segment.
#[tokio::test]
async fn a_saturated_span_buffer_drops_and_counts() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let exporter = AgentGenAiSpanExporter::with_capacity(4).with_metrics(metrics.clone());
    let segment = |at: u64| {
        AgentTelemetrySegment::new(
            AgentSegmentOperation::Decide { phase: "propose" },
            AgentTimestampMillis::new(at),
            AgentTimestampMillis::new(at + 1),
        )
        .telemetry(AgentTelemetryContext {
            trace_parent: Some(format!(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b71692033{at:02}-01"
            )),
            ..AgentTelemetryContext::default()
        })
        .ok()
    };
    for at in 0..12 {
        exporter.record(&segment(at));
    }
    assert_eq!(exporter.buffered(), 4, "the ring never exceeds its bound");
    assert_eq!(exporter.dropped(), 8, "and counts every span it dropped");

    // The counters reach the metric surface on the flush, which is the
    // exporter's one natural periodic point.
    let bridged = exporter.bridge_export(
        exporter_config(UNREACHABLE, EXPORTER_CREDENTIAL),
        export_resource(),
        &InMemoryMetricsRecorder::new().snapshot(),
        Vec::new(),
    );
    assert!(bridged.is_ok(), "a bounded batch still builds");
    let drops: f64 = metrics
        .snapshot()
        .observations_named(METRIC_AGENT_TELEMETRY_EXPORT_DROPS)
        .iter()
        .map(|observation| observation.value())
        .sum();
    assert!(
        (drops - 8.0).abs() < f64::EPSILON,
        "the drop counter reports the eight lost spans, saw {drops}"
    );
}

/// A failed flush leaves the buffer intact for the next one.
///
/// Slice 6.3a's `bridge_export` stages under the lock and clears only once the
/// batch has been built, because passing `drain()` as an argument destroyed up
/// to a full buffer of already-mapped spans on any validation error — while
/// `buffered()` and `dropped()` both reported a clean pipeline. This is that
/// invariant, asserted from outside.
#[test]
fn a_failed_flush_strands_nothing() {
    let exporter = AgentGenAiSpanExporter::new();
    exporter.record(
        &AgentTelemetrySegment::new(
            AgentSegmentOperation::Decide { phase: "propose" },
            AgentTimestampMillis::new(1),
            AgentTimestampMillis::new(2),
        )
        .telemetry(AgentTelemetryContext {
            trace_parent: Some(
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
            ),
            ..AgentTelemetryContext::default()
        })
        .ok(),
    );
    assert_eq!(exporter.buffered(), 1);

    // A blank endpoint fails the bridge's own validation.
    let refused = exporter.bridge_export(
        exporter_config("", EXPORTER_CREDENTIAL),
        export_resource(),
        &InMemoryMetricsRecorder::new().snapshot(),
        Vec::new(),
    );
    assert!(refused.is_err(), "a blank endpoint is refused");
    assert_eq!(
        exporter.buffered(),
        1,
        "and the refused flush left the span where it was"
    );

    let accepted = exporter.bridge_export(
        exporter_config("http://collector:4317", EXPORTER_CREDENTIAL),
        export_resource(),
        &InMemoryMetricsRecorder::new().snapshot(),
        Vec::new(),
    );
    assert_eq!(
        accepted.expect("the good flush builds").spans.len(),
        1,
        "the next working flush ships what the failed one kept"
    );
    assert_eq!(exporter.buffered(), 0, "and only then is it cleared");
}

/// Every declared export signal is one this binary actually writes.
///
/// The bijection `rakka_agent::AGENT_TELEMETRY_SIGNALS` keeps for the crate's
/// own values, kept here for the binary's. Without it the three `otlp-*`
/// values would be documentation that nothing produces — the defect class
/// slice 6.3a's follow-up pass existed to close.
#[tokio::test]
async fn every_declared_export_signal_is_written() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let export = AgentTelemetryExport::install(
        &exporter_config(UNREACHABLE, EXPORTER_CREDENTIAL),
        &export_resource(),
    )
    .expect("the exporter builds")
    .with_metrics(metrics.clone());

    let world = World::new(scripted_adapter(), agent_target());
    let _run_scope = drive_run(&world).await;
    let _outcome = export
        .flush(
            &world.spans,
            &world.metrics.snapshot(),
            vec![log_event()],
            &world.exemplars,
        )
        .await;

    let written: std::collections::BTreeSet<String> = metrics
        .snapshot()
        .observations_named(METRIC_AGENT_TELEMETRY_FLUSH_FAILURES)
        .iter()
        .filter_map(|observation| {
            observation
                .attributes()
                .iter()
                .find(|attribute| attribute.key() == "signal")
                .map(|attribute| attribute.value().to_string())
        })
        .collect();
    for signal in AGENT_OTLP_EXPORT_SIGNALS {
        assert!(
            written.contains(*signal),
            "`{signal}` is declared but this binary never writes it; wrote {written:?}"
        );
    }
}

fn log_event() -> rakka_agent_workflow::AgentLogEvent {
    rakka_agent_workflow::AgentLogEvent::new(
        "rakka.agent.run.transition",
        rakka_agent_workflow::AgentLogSeverity::Info,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
}

/// The live-Collector arm, gated in the established idiom.
///
/// Point it at a real `otel/opentelemetry-collector-contrib` and it proves the
/// same batch a real distribution accepts. Unset, it announces the skip rather
/// than passing silently — and the two arms above have already proven the
/// failure behaviour without it.
#[tokio::test]
async fn optional_live_collector_export_is_gated() {
    let Ok(endpoint) = std::env::var("RAKKA_AGENT_OTEL_COLLECTOR_ENDPOINT") else {
        eprintln!(
            "skipping the live Collector export; set \
             RAKKA_AGENT_OTEL_COLLECTOR_ENDPOINT=http://127.0.0.1:4317 to run it"
        );
        return;
    };
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let export = AgentTelemetryExport::install(
        &exporter_config(&endpoint, EXPORTER_CREDENTIAL),
        &export_resource(),
    )
    .expect("the exporter builds against the live endpoint")
    .with_metrics(metrics.clone());

    let world = World::new(scripted_adapter(), agent_target());
    let _run_scope = drive_run(&world).await;
    let outcome = export
        .flush(
            &world.spans,
            &world.metrics.snapshot(),
            Vec::new(),
            &world.exemplars,
        )
        .await;
    assert_eq!(
        outcome.failed_signals, 0,
        "a live Collector accepts the batch this binary builds"
    );
    assert!(outcome.spans > 0, "and is handed the run's spans");
    export.shutdown().expect("the live exporters drain");
}

/// Keeps the in-process receiver reachable from this suite's imports.
#[tokio::test]
async fn the_in_process_receiver_accepts_what_the_walk_ships() {
    let collector = InProcessCollector::start().await;
    let export = AgentTelemetryExport::install(
        &exporter_config(collector.endpoint(), EXPORTER_CREDENTIAL),
        &export_resource(),
    )
    .expect("the exporter builds");
    let world = World::new(scripted_adapter(), agent_target());
    let _run_scope = drive_run(&world).await;
    let outcome = export
        .flush(
            &world.spans,
            &world.metrics.snapshot(),
            Vec::new(),
            &world.exemplars,
        )
        .await;
    assert_eq!(outcome.failed_signals, 0, "the receiver accepts the batch");
    assert!(
        !collector.received().traces().is_empty(),
        "and holds what it was handed"
    );
}
