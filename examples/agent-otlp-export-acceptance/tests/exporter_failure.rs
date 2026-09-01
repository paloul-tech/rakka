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

/// The durable `traceparent` the log arm correlates against.
const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// The trace id half of [`TRACEPARENT`], as OTLP's 16 raw bytes.
const INGRESS_TRACE_ID: [u8; 16] = [
    0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c,
];

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
    let _drained = export.shutdown().await;
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
    export.shutdown().await.expect("the live exporters drain");
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
    let traces = collector.received().traces();
    assert!(!traces.is_empty(), "and holds what it was handed");

    // The span exporter's resource, which is set nowhere near the other two.
    // `SpanExporter` takes it through `set_resource` rather than a builder or
    // a provider and starts at `Resource::default()`, so a missed call leaves
    // every span under `unknown_service` while metrics and logs carry the
    // deployment — and nothing else in this suite reads the field.
    let service: Vec<String> = traces
        .iter()
        .filter_map(|resource| resource.resource.as_ref())
        .flat_map(|resource| resource.attributes.iter())
        .filter(|attribute| attribute.key == "service.name")
        .filter_map(|attribute| attribute.value.as_ref())
        .filter_map(|value| match &value.value {
            Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(name)) => {
                Some(name.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        service,
        vec!["rakka-agent-otlp-export-acceptance".to_string()],
        "every exported ResourceSpans names the service that produced it"
    );
}

/// A bridge that cannot build counts all three signals it just lost.
///
/// This arm is not the unreachable-endpoint one. There the bridge builds and
/// `ship` counts each signal as its own export fails; here the batch is never
/// built, so **every** signal is lost in one step — and that arm raised
/// `failed_signals` to 3 while calling `count_failure` for none of them. A
/// deployment with a misconfigured endpoint takes it on every periodic flush,
/// losing everything with `rakka.agent.telemetry.flush.failures` reading zero.
#[tokio::test]
async fn a_refused_bridge_counts_every_signal_it_lost() {
    let collector = InProcessCollector::start().await;
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let export = AgentTelemetryExport::install(
        &exporter_config(collector.endpoint(), EXPORTER_CREDENTIAL),
        &export_resource(),
    )
    .expect("the exporter builds against a reachable endpoint")
    .with_metrics(metrics.clone());
    let world = World::new(scripted_adapter(), agent_target());
    let _run_scope = drive_run(&world).await;

    // A blank event name is one of the two documented ways `bridge` refuses,
    // and the one that leaves the endpoint healthy — so a failure here can
    // only be the bridge's, never the socket's.
    let outcome = export
        .flush(
            &world.spans,
            &world.metrics.snapshot(),
            vec![rakka_agent_workflow::AgentLogEvent::new(
                "",
                rakka_agent_workflow::AgentLogSeverity::Info,
                AgentTimestampMillis::new(1),
                AgentTimestampMillis::new(2),
            )],
            &world.exemplars,
        )
        .await;
    assert_eq!(
        outcome.failed_signals,
        AGENT_OTLP_EXPORT_SIGNALS.len(),
        "a refused bridge loses every signal"
    );

    let counted: std::collections::BTreeSet<String> = metrics
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
            counted.contains(*signal),
            "`{signal}` was lost and not counted; counted {counted:?}"
        );
    }
    assert!(
        collector.received().traces().is_empty(),
        "and nothing was shipped, so the count is the only record of the loss"
    );
}

/// A log record reaches the wire with each field in the slot OTLP reads it from.
///
/// Nothing in this suite decoded an exported log record before, and four
/// defects were living in that one gap: the event name written into `target`,
/// which OTLP reads as the **`ScopeLogs` grouping key** rather than as a name;
/// `event_name` and `severity_text` exported empty as a result; the pinned
/// instrumentation scope replaced by a bare `"rakka.agent"`; and every band
/// above WARN — `AgentLogSeverity::Fatal` among them — collapsed onto ERROR.
///
/// Each assertion below fails on exactly one of those, and the two records
/// carry deliberately different event names: under the old mapping they
/// arrived as two scope blocks named after the events, so the "one scope"
/// assertion is what pins `target` down.
#[tokio::test]
async fn an_exported_log_record_carries_its_name_scope_and_severity() {
    let collector = InProcessCollector::start().await;
    let export = AgentTelemetryExport::install(
        &exporter_config(collector.endpoint(), EXPORTER_CREDENTIAL),
        &export_resource(),
    )
    .expect("the exporter builds");
    let world = World::new(scripted_adapter(), agent_target());

    let outcome = export
        .flush(
            &world.spans,
            &world.metrics.snapshot(),
            vec![
                log_event(),
                rakka_agent_workflow::AgentLogEvent::new(
                    "rakka.agent.run.failed",
                    rakka_agent_workflow::AgentLogSeverity::Fatal,
                    AgentTimestampMillis::new(3),
                    AgentTimestampMillis::new(4),
                )
                .telemetry_context(&AgentTelemetryContext {
                    trace_parent: Some(TRACEPARENT.to_string()),
                    ..AgentTelemetryContext::default()
                })
                .expect("the durable trace context applies"),
            ],
            &world.exemplars,
        )
        .await;
    assert_eq!(outcome.failed_signals, 0, "the receiver accepts the logs");
    assert_eq!(outcome.logs, 2, "and both records were emitted");

    let received = collector.received().logs();
    let scopes: Vec<_> = received
        .iter()
        .flat_map(|resource| resource.scope_logs.iter())
        .collect();
    assert_eq!(
        scopes.len(),
        1,
        "two event names must not split one batch into two scope blocks, saw {scopes:?}"
    );
    let scope = scopes[0].scope.as_ref().expect("the block names its scope");
    let pinned = rakka_agent::otel::agent_instrumentation_scope();
    assert_eq!(
        scope.name, pinned.name,
        "the pinned scope name reached logs"
    );

    // What the pinned SDK can carry, and what it drops. `emit_logs` builds the
    // record on the full pinned scope, but 0.29's log transform reduces it:
    // `group_logs_by_resource_and_scope` always passes its grouping key as the
    // `Some(target)` arm of `InstrumentationScope::from`, and that arm hardcodes
    // `version: String::new()` and `attributes: vec![]`. Only the name survives
    // — and only because no `target` is set, so the key falls back to the
    // scope's own name. The `ScopeLogs.schema_url` beside it is the *resource's*
    // schema URL, which is a different claim and is deliberately unset.
    //
    // Asserted as empty rather than left unchecked so a future SDK bump that
    // fixes the transform fails here instead of quietly changing the wire.
    assert!(
        scope.version.is_empty(),
        "opentelemetry-proto 0.29 cannot carry a log scope version; if this now \
         holds `{}`, assert it equals the pinned version instead",
        scope.version
    );
    assert!(
        scopes[0].schema_url.is_empty(),
        "the log block's schema URL follows the resource, which declares none"
    );

    let records = &scopes[0].log_records;
    assert_eq!(records.len(), 2, "both records are in the one block");
    let names: std::collections::BTreeSet<&str> = records
        .iter()
        .map(|record| record.event_name.as_str())
        .collect();
    assert_eq!(
        names,
        ["rakka.agent.run.failed", "rakka.agent.run.transition"]
            .into_iter()
            .collect(),
        "each record carries its own event name in `event_name`"
    );

    let fatal = records
        .iter()
        .find(|record| record.event_name == "rakka.agent.run.failed")
        .expect("the fatal record arrived");
    assert_eq!(
        fatal.severity_number,
        i32::from(rakka_agent_workflow::AgentLogSeverity::Fatal.severity_number()),
        "a fatal exports as FATAL, not as the ERROR the catch-all made of it"
    );
    assert_eq!(fatal.severity_text, "FATAL", "and names the band it is in");
    assert_eq!(
        fatal.trace_id,
        INGRESS_TRACE_ID.to_vec(),
        "the durable trace context reached the record"
    );
    assert!(
        !fatal.span_id.is_empty(),
        "and so did the span it was propagated from"
    );
}
