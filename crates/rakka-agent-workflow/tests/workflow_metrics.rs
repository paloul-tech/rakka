//! Workflow metric instrument tests.

use std::collections::BTreeSet;

use rakka_agent_workflow::{
    agent_autoscaling_signal, agent_metric_instrument, is_agent_autoscaling_metric,
    is_bounded_agent_metric_attribute, is_forbidden_agent_metric_attribute, record_agent_counter,
    record_agent_gauge, record_agent_histogram, validate_agent_metric_attributes,
    AgentAutoscalingSignalRole, AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES,
    AGENT_METRIC_ATTR_COMPONENT, AGENT_METRIC_ATTR_DATABASE_OPERATION, AGENT_METRIC_ATTR_DETAIL,
    AGENT_METRIC_ATTR_DIRECTION, AGENT_METRIC_ATTR_OPERATION, AGENT_METRIC_ATTR_OUTCOME,
    AGENT_METRIC_ATTR_QUEUE, AGENT_METRIC_ATTR_STATUS, AGENT_METRIC_ATTR_TARGET_CLASS,
    AGENT_METRIC_ATTR_TENANT_TIER, AGENT_METRIC_ATTR_TRANSITION, AGENT_METRIC_ATTR_WORKFLOW_TYPE,
    AGENT_WORKFLOW_AUTOSCALING_SIGNALS, AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES,
    AGENT_WORKFLOW_METRIC_INSTRUMENTS, FORBIDDEN_HOT_METRIC_FIELDS, METRIC_AGENT_ACTIVE_RUNS,
    METRIC_AGENT_DISPATCH_LATENCY_MS, METRIC_AGENT_DUE_OUTBOX_EFFECTS,
    METRIC_AGENT_HUMAN_WAITING_RUNS, METRIC_AGENT_INBOX_COMMANDS, METRIC_AGENT_MAILBOX_DEPTH,
    METRIC_AGENT_PENDING_INBOX_COMMANDS, METRIC_AGENT_POSTGRES_LATENCY_MS,
    METRIC_AGENT_PROCESS_RUNNING, METRIC_AGENT_RECOVERY_LATENCY_MS, METRIC_AGENT_RUN_TRANSITIONS,
    METRIC_AGENT_SHARD_OWNERSHIP_COUNT, METRIC_AGENT_STREAM_PRESSURE,
    METRIC_AGENT_TIMERS_LATE_BY_MS,
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

    let error = validate_agent_metric_attributes(&[("owner_node_id", "rakka-agent-0:pod-uid")])
        .expect_err("node ownership details should stay in resource attributes or snapshots");
    assert_eq!(error.code(), "unbounded-metric-attribute-key");
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

#[test]
fn autoscaling_signal_catalog_covers_scale_drivers_with_bounded_labels() {
    let mut names = BTreeSet::new();
    let mut roles = BTreeSet::new();

    for signal in AGENT_WORKFLOW_AUTOSCALING_SIGNALS {
        assert!(
            names.insert(signal.metric_name),
            "duplicate autoscaling signal: {}",
            signal.metric_name
        );
        roles.insert(signal.role.as_label());
        assert_eq!(
            agent_autoscaling_signal(signal.metric_name),
            Some(signal),
            "signal lookup should round trip"
        );
        assert!(
            is_agent_autoscaling_metric(signal.metric_name),
            "signal should be recognized as autoscaling metric"
        );

        let instrument = agent_metric_instrument(signal.metric_name).unwrap_or_else(|| {
            panic!(
                "autoscaling metric should be registered: {}",
                signal.metric_name
            )
        });
        assert_eq!(instrument.kind, signal.kind);
        assert_eq!(instrument.unit, signal.unit);
        assert!(!signal.recommended_aggregation.trim().is_empty());
        assert!(!signal.description.trim().is_empty());

        for attribute in signal.bounded_attributes {
            assert!(
                is_bounded_agent_metric_attribute(attribute),
                "autoscaling attribute should be bounded: {attribute}"
            );
            assert!(
                !is_forbidden_agent_metric_attribute(attribute),
                "autoscaling attribute must not be forbidden: {attribute}"
            );
        }
    }

    for role in [
        AgentAutoscalingSignalRole::Workload,
        AgentAutoscalingSignalRole::Backlog,
        AgentAutoscalingSignalRole::Latency,
        AgentAutoscalingSignalRole::Saturation,
        AgentAutoscalingSignalRole::Availability,
        AgentAutoscalingSignalRole::Distribution,
    ] {
        assert!(
            roles.contains(role.as_label()),
            "autoscaling catalog missing role {}",
            role.as_label()
        );
    }

    for required in [
        METRIC_AGENT_ACTIVE_RUNS,
        METRIC_AGENT_PENDING_INBOX_COMMANDS,
        METRIC_AGENT_DUE_OUTBOX_EFFECTS,
        METRIC_AGENT_DISPATCH_LATENCY_MS,
        METRIC_AGENT_HUMAN_WAITING_RUNS,
        METRIC_AGENT_MAILBOX_DEPTH,
        METRIC_AGENT_STREAM_PRESSURE,
        METRIC_AGENT_PROCESS_RUNNING,
        METRIC_AGENT_POSTGRES_LATENCY_MS,
        METRIC_AGENT_SHARD_OWNERSHIP_COUNT,
    ] {
        assert!(
            names.contains(required),
            "autoscaling signal catalog missing {required}"
        );
    }
}

#[test]
fn autoscaling_metrics_record_as_exportable_gauges_and_histograms() {
    let metrics = InMemoryMetricsRecorder::new();

    record_agent_gauge(
        &metrics,
        METRIC_AGENT_ACTIVE_RUNS,
        7.0,
        &[
            (AGENT_METRIC_ATTR_WORKFLOW_TYPE, "research"),
            (AGENT_METRIC_ATTR_STATUS, "active"),
            (AGENT_METRIC_ATTR_TENANT_TIER, "standard"),
        ],
    )
    .expect("active runs should record with bounded labels");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_PENDING_INBOX_COMMANDS,
        11.0,
        &[(AGENT_METRIC_ATTR_QUEUE, "durable-inbox")],
    )
    .expect("pending inbox commands should record");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_DUE_OUTBOX_EFFECTS,
        5.0,
        &[
            (AGENT_METRIC_ATTR_QUEUE, "durable-outbox"),
            (AGENT_METRIC_ATTR_TARGET_CLASS, "tool"),
        ],
    )
    .expect("due outbox effects should record");
    record_agent_histogram(
        &metrics,
        METRIC_AGENT_DISPATCH_LATENCY_MS,
        250.0,
        &[
            (AGENT_METRIC_ATTR_TARGET_CLASS, "tool"),
            (AGENT_METRIC_ATTR_OUTCOME, "success"),
        ],
    )
    .expect("dispatch latency should record");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_HUMAN_WAITING_RUNS,
        2.0,
        &[(AGENT_METRIC_ATTR_WORKFLOW_TYPE, "approval")],
    )
    .expect("human wait gauge should record");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_MAILBOX_DEPTH,
        32.0,
        &[(AGENT_METRIC_ATTR_COMPONENT, "run-actor")],
    )
    .expect("mailbox depth should record");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_STREAM_PRESSURE,
        0.75,
        &[
            (AGENT_METRIC_ATTR_COMPONENT, "tool-output"),
            (AGENT_METRIC_ATTR_DIRECTION, "outbound"),
        ],
    )
    .expect("stream pressure should record");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_PROCESS_RUNNING,
        1.0,
        &[
            (AGENT_METRIC_ATTR_COMPONENT, "sandbox"),
            (AGENT_METRIC_ATTR_STATUS, "running"),
        ],
    )
    .expect("process running should record");
    record_agent_histogram(
        &metrics,
        METRIC_AGENT_POSTGRES_LATENCY_MS,
        18.0,
        &[
            (AGENT_METRIC_ATTR_DATABASE_OPERATION, "query-index-upsert"),
            (AGENT_METRIC_ATTR_OUTCOME, "success"),
        ],
    )
    .expect("postgres latency should record");
    record_agent_gauge(
        &metrics,
        METRIC_AGENT_SHARD_OWNERSHIP_COUNT,
        16.0,
        &[
            (AGENT_METRIC_ATTR_COMPONENT, "agent-run-sharding"),
            ("entity_type", "AgentRun"),
        ],
    )
    .expect("shard ownership should record");

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.last_gauge(METRIC_AGENT_ACTIVE_RUNS), Some(7.0));
    assert_eq!(
        snapshot.last_gauge(METRIC_AGENT_PENDING_INBOX_COMMANDS),
        Some(11.0)
    );
    assert_eq!(
        snapshot.last_gauge(METRIC_AGENT_DUE_OUTBOX_EFFECTS),
        Some(5.0)
    );
    assert_eq!(snapshot.last_gauge(METRIC_AGENT_MAILBOX_DEPTH), Some(32.0));
    assert_eq!(
        snapshot.last_gauge(METRIC_AGENT_STREAM_PRESSURE),
        Some(0.75)
    );
    assert_eq!(snapshot.last_gauge(METRIC_AGENT_PROCESS_RUNNING), Some(1.0));
    assert_eq!(
        snapshot.last_gauge(METRIC_AGENT_SHARD_OWNERSHIP_COUNT),
        Some(16.0)
    );

    let prometheus = export_prometheus_text(&snapshot);
    assert!(prometheus.contains("rakka_agent_workflow_run_active"));
    assert!(prometheus.contains("rakka_agent_workflow_dispatcher_latency_ms_count"));
    assert!(prometheus.contains("rakka_agent_workflow_postgres_latency_ms_count"));

    let otel =
        export_open_telemetry_metrics(&snapshot, &[("service.name", "rakka-agent-workflow-test")]);
    let exported_names = otel
        .metrics()
        .iter()
        .map(|metric| metric.name())
        .collect::<Vec<_>>();
    assert!(exported_names.contains(&METRIC_AGENT_ACTIVE_RUNS));
    assert!(exported_names.contains(&METRIC_AGENT_DISPATCH_LATENCY_MS));
    assert!(exported_names.contains(&METRIC_AGENT_POSTGRES_LATENCY_MS));
}
