//! Production observability exporter tests.

use rakka_core::{
    export_open_telemetry_metrics, export_prometheus_text, prometheus_label_name,
    prometheus_metric_name, InMemoryMetricsRecorder, MetricAttribute, MetricsRecorder,
    OpenTelemetryInstrumentKind, METRIC_ACTOR_MAILBOX_DEPTH, METRIC_CLUSTER_MEMBERS,
    METRIC_GRPC_REQUEST_LATENCY_MS, METRIC_HTTP_REQUEST_LATENCY_MS, METRIC_PROCESS_EXITS,
    METRIC_REMOTE_FAILURES, METRIC_SHARD_OWNERSHIP_COUNT, METRIC_STREAM_PRESSURE,
};

#[test]
fn prometheus_export_maps_stable_metrics_and_kinds() {
    let recorder = InMemoryMetricsRecorder::new();
    recorder.record_histogram(
        METRIC_HTTP_REQUEST_LATENCY_MS,
        10.0,
        &[("route", "/counter/add"), ("method", "POST")],
    );
    recorder.record_histogram(
        METRIC_HTTP_REQUEST_LATENCY_MS,
        20.0,
        &[("method", "POST"), ("route", "/counter/add")],
    );
    recorder.record_histogram(
        METRIC_GRPC_REQUEST_LATENCY_MS,
        5.0,
        &[("service", "CounterService"), ("method", "Add")],
    );
    recorder.record_gauge(METRIC_STREAM_PRESSURE, 0.75, &[("stream", "ingress")]);
    recorder.increment_counter(METRIC_PROCESS_EXITS, 2, &[("process", "legacy")]);
    recorder.increment_counter(
        METRIC_REMOTE_FAILURES,
        1,
        &[("operation", "inbound"), ("error", "decode-error")],
    );
    recorder.record_gauge(METRIC_CLUSTER_MEMBERS, 3.0, &[("state", "up")]);
    recorder.record_gauge(
        METRIC_SHARD_OWNERSHIP_COUNT,
        8.0,
        &[("entity_type", "Cart"), ("owner", "rakka-0#uid-a")],
    );
    recorder.record_gauge(
        METRIC_ACTOR_MAILBOX_DEPTH,
        4.0,
        &[("system", "gateway"), ("actor", "/user/counter")],
    );

    let text = export_prometheus_text(&recorder.snapshot());

    assert_eq!(
        prometheus_metric_name(METRIC_HTTP_REQUEST_LATENCY_MS),
        "rakka_http_request_latency_ms"
    );
    assert_eq!(prometheus_label_name("route.kind"), "route_kind");
    assert!(text.contains(
        "# HELP rakka_http_request_latency_ms Rakka metric rakka.http.request.latency_ms."
    ));
    assert!(text.contains("# TYPE rakka_http_request_latency_ms summary"));
    assert!(text
        .contains("rakka_http_request_latency_ms_count{method=\"POST\",route=\"/counter/add\"} 2"));
    assert!(text
        .contains("rakka_http_request_latency_ms_sum{method=\"POST\",route=\"/counter/add\"} 30"));
    assert!(text.contains(
        "rakka_grpc_request_latency_ms_count{method=\"Add\",service=\"CounterService\"} 1"
    ));
    assert!(text.contains("rakka_stream_pressure{stream=\"ingress\"} 0.75"));
    assert!(text.contains("rakka_process_exits{process=\"legacy\"} 2"));
    assert!(text.contains("rakka_remote_failures{error=\"decode-error\",operation=\"inbound\"} 1"));
    assert!(text.contains("rakka_cluster_members{state=\"up\"} 3"));
    assert!(text
        .contains("rakka_sharding_shards_owned{entity_type=\"Cart\",owner=\"rakka-0#uid-a\"} 8"));
    assert!(
        text.contains("rakka_actor_mailbox_depth{actor=\"/user/counter\",system=\"gateway\"} 4")
    );
}

#[test]
fn open_telemetry_bridge_preserves_canonical_names_and_resource_attributes() {
    let recorder = InMemoryMetricsRecorder::new();
    recorder.increment_counter(METRIC_PROCESS_EXITS, 1, &[("process", "legacy")]);
    recorder.record_gauge(METRIC_STREAM_PRESSURE, 0.5, &[("stream", "ingress")]);
    recorder.record_histogram(METRIC_HTTP_REQUEST_LATENCY_MS, 7.0, &[("route", "/ready")]);

    let export = export_open_telemetry_metrics(
        &recorder.snapshot(),
        &[
            ("service.name", "rakka-node"),
            ("deployment.environment", "test"),
        ],
    );

    assert_eq!(
        export.resource_attributes(),
        &[
            MetricAttribute::new("deployment.environment", "test"),
            MetricAttribute::new("service.name", "rakka-node"),
        ]
    );
    let process = export
        .metrics()
        .iter()
        .find(|metric| metric.name() == METRIC_PROCESS_EXITS)
        .expect("process exits metric should be exported");
    assert_eq!(process.kind(), OpenTelemetryInstrumentKind::Counter);
    assert_eq!(process.data_points()[0].value(), Some(1.0));

    let stream = export
        .metrics()
        .iter()
        .find(|metric| metric.name() == METRIC_STREAM_PRESSURE)
        .expect("stream pressure metric should be exported");
    assert_eq!(stream.kind(), OpenTelemetryInstrumentKind::Gauge);
    assert_eq!(stream.data_points()[0].value(), Some(0.5));

    let http = export
        .metrics()
        .iter()
        .find(|metric| metric.name() == METRIC_HTTP_REQUEST_LATENCY_MS)
        .expect("HTTP latency metric should be exported");
    assert_eq!(http.kind(), OpenTelemetryInstrumentKind::Histogram);
    assert_eq!(http.data_points()[0].count(), Some(1));
    assert_eq!(http.data_points()[0].sum(), Some(7.0));
}
