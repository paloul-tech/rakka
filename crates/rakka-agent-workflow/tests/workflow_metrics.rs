//! Workflow metric instrument tests.

use std::collections::BTreeSet;

use rakka_agent_workflow::{
    agent_metric_instrument, is_bounded_agent_metric_attribute,
    is_forbidden_agent_metric_attribute, record_agent_counter, record_agent_gauge,
    record_agent_histogram, validate_agent_metric_attributes,
    AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES, AGENT_METRIC_ATTR_DETAIL, AGENT_METRIC_ATTR_OPERATION,
    AGENT_METRIC_ATTR_OUTCOME, AGENT_METRIC_ATTR_STATUS, AGENT_METRIC_ATTR_TRANSITION,
    AGENT_METRIC_ATTR_WORKFLOW_TYPE, AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES,
    AGENT_WORKFLOW_METRIC_INSTRUMENTS, FORBIDDEN_HOT_METRIC_FIELDS, METRIC_AGENT_INBOX_COMMANDS,
    METRIC_AGENT_RECOVERY_LATENCY_MS, METRIC_AGENT_RUN_TRANSITIONS, METRIC_AGENT_TIMERS_LATE_BY_MS,
};
use rakka_core::{
    export_open_telemetry_metrics, export_prometheus_text, prometheus_metric_name,
    InMemoryMetricsRecorder, MetricKind,
};

#[test]
fn metric_registry_uses_stable_names_kinds_units_and_bounded_attributes() {
    let mut names = BTreeSet::new();
    for instrument in AGENT_WORKFLOW_METRIC_INSTRUMENTS {
        assert!(
            instrument.name.starts_with("rakka.agent_workflow."),
            "unexpected metric namespace: {}",
            instrument.name
        );
        assert!(
            names.insert(instrument.name),
            "duplicate metric instrument: {}",
            instrument.name
        );
        assert!(!instrument.unit.trim().is_empty());
        assert!(!instrument.description.trim().is_empty());
        assert!(prometheus_metric_name(instrument.name).starts_with("rakka_agent_workflow_"));
    }

    let inbox = agent_metric_instrument(METRIC_AGENT_INBOX_COMMANDS)
        .expect("inbox metric should be registered");
    assert_eq!(inbox.kind, MetricKind::Counter);

    for key in AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES {
        assert!(
            is_bounded_agent_metric_attribute(key),
            "bounded key should be accepted: {key}"
        );
        assert!(
            !is_forbidden_agent_metric_attribute(key),
            "bounded key must not also be forbidden: {key}"
        );
    }

    for key in FORBIDDEN_HOT_METRIC_FIELDS {
        assert!(
            is_forbidden_agent_metric_attribute(key),
            "domain forbidden key should be rejected: {key}"
        );
    }
}

#[test]
fn metric_attribute_validation_rejects_high_cardinality_labels() {
    let error = validate_agent_metric_attributes(&[("run_id", "run-123")])
        .expect_err("raw run ids must not be hot metric labels");
    assert_eq!(error.code(), "unbounded-metric-attribute-key");

    let error = validate_agent_metric_attributes(&[("provider", "open-ended-provider")])
        .expect_err("unknown labels should not be recorded without a bounded policy");
    assert_eq!(error.code(), "unbounded-metric-attribute-key");

    let long_value = "x".repeat(AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES + 1);
    let error =
        validate_agent_metric_attributes(&[(AGENT_METRIC_ATTR_DETAIL, long_value.as_str())])
            .expect_err("full error strings should not become labels");
    assert_eq!(error.code(), "metric-attribute-value-too-large");

    let error = validate_agent_metric_attributes(&[(AGENT_METRIC_ATTR_DETAIL, "line-1\nline-2")])
        .expect_err("multi-line values should stay in logs or audit");
    assert_eq!(error.code(), "unbounded-metric-attribute-value");
}

#[test]
fn recording_helpers_emit_prometheus_and_opentelemetry_exportable_metrics() {
    let metrics = InMemoryMetricsRecorder::new();

    record_agent_counter(
        &metrics,
        METRIC_AGENT_RUN_TRANSITIONS,
        1,
        &[
            (AGENT_METRIC_ATTR_WORKFLOW_TYPE, "research"),
            (AGENT_METRIC_ATTR_TRANSITION, "begin-step"),
            (AGENT_METRIC_ATTR_STATUS, "running"),
            (AGENT_METRIC_ATTR_OUTCOME, "success"),
        ],
    )
    .expect("bounded counter labels should record");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_TIMERS_LATE_BY_MS,
        25.0,
        &[(AGENT_METRIC_ATTR_OUTCOME, "fired")],
    )
    .expect("bounded gauge labels should record");
    record_agent_histogram(
        &metrics,
        METRIC_AGENT_RECOVERY_LATENCY_MS,
        12.0,
        &[
            (AGENT_METRIC_ATTR_OPERATION, "recover"),
            (AGENT_METRIC_ATTR_OUTCOME, "success"),
        ],
    )
    .expect("bounded histogram labels should record");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.counter_total(METRIC_AGENT_RUN_TRANSITIONS), 1.0);
    assert_eq!(
        snapshot.last_gauge(METRIC_AGENT_TIMERS_LATE_BY_MS),
        Some(25.0)
    );

    let prometheus = export_prometheus_text(&snapshot);
    assert!(prometheus.contains("rakka_agent_workflow_run_transitions"));
    assert!(prometheus.contains("rakka_agent_workflow_recovery_latency_ms_count"));

    let otel =
        export_open_telemetry_metrics(&snapshot, &[("service.name", "rakka-agent-workflow-test")]);
    let exported_names = otel
        .metrics()
        .iter()
        .map(|metric| metric.name())
        .collect::<Vec<_>>();
    assert!(exported_names.contains(&METRIC_AGENT_RUN_TRANSITIONS));
    assert!(exported_names.contains(&METRIC_AGENT_TIMERS_LATE_BY_MS));
    assert!(exported_names.contains(&METRIC_AGENT_RECOVERY_LATENCY_MS));
}
