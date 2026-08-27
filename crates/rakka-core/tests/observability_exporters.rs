//! Production observability exporter tests.

use rakka_core::{
    export_open_telemetry_metrics, export_open_telemetry_metrics_with_instruments,
    export_prometheus_text, prometheus_label_name, prometheus_metric_name, InMemoryMetricsRecorder,
    MetricAttribute, MetricsRecorder, OpenTelemetryDataPoint, OpenTelemetryInstrumentKind,
    OpenTelemetryInstrumentView, METRIC_ACTOR_MAILBOX_DEPTH, METRIC_CLUSTER_MEMBERS,
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

/// The catalogue-free export is unchanged by the extension: no unit, no
/// bucket boundaries, count and sum exactly as before. This is the row the
/// Prometheus exporter and every existing caller still take.
#[test]
fn an_export_without_instruments_carries_no_unit_and_no_buckets() {
    let recorder = InMemoryMetricsRecorder::new();
    recorder.record_histogram(METRIC_HTTP_REQUEST_LATENCY_MS, 7.0, &[("route", "/ready")]);
    recorder.increment_counter(METRIC_PROCESS_EXITS, 1, &[]);

    let export = export_open_telemetry_metrics(&recorder.snapshot(), &[]);
    for metric in export.metrics() {
        assert_eq!(metric.unit(), None, "{} gained a unit", metric.name());
        for point in metric.data_points() {
            assert!(point.bucket_boundaries().is_empty());
            assert!(point.bucket_counts().is_empty());
        }
    }
}

/// With an instrument catalogue the export carries the unit and buckets the
/// semantic conventions require, rather than dropping them silently while
/// claiming compliance.
#[test]
fn an_instrument_catalogue_supplies_the_unit_and_the_buckets() {
    let recorder = InMemoryMetricsRecorder::new();
    for value in [0.5_f64, 4.0, 25.0, 900.0] {
        recorder.record_histogram(
            METRIC_HTTP_REQUEST_LATENCY_MS,
            value,
            &[("route", "/ready")],
        );
    }
    recorder.increment_counter(METRIC_PROCESS_EXITS, 2, &[]);

    let boundaries = [1.0_f64, 5.0, 50.0];
    let export = export_open_telemetry_metrics_with_instruments(
        &recorder.snapshot(),
        &[],
        &[
            OpenTelemetryInstrumentView {
                name: METRIC_HTTP_REQUEST_LATENCY_MS,
                unit: "ms",
                bucket_boundaries: &boundaries,
            },
            OpenTelemetryInstrumentView {
                name: METRIC_PROCESS_EXITS,
                unit: "{exit}",
                bucket_boundaries: &[],
            },
        ],
    );

    let http = export
        .metrics()
        .iter()
        .find(|metric| metric.name() == METRIC_HTTP_REQUEST_LATENCY_MS)
        .expect("the latency metric is exported");
    assert_eq!(http.unit(), Some("ms"));
    let point = &http.data_points()[0];
    assert_eq!(point.count(), Some(4));
    assert_eq!(point.sum(), Some(929.5));
    assert_eq!(point.bucket_boundaries(), &boundaries);
    // One longer than the boundaries: the trailing entry is `+Inf`, and 900.0
    // is the only observation that exceeds every bound.
    assert_eq!(point.bucket_counts(), &[1, 1, 1, 1]);

    let exits = export
        .metrics()
        .iter()
        .find(|metric| metric.name() == METRIC_PROCESS_EXITS)
        .expect("the exit counter is exported");
    assert_eq!(exits.unit(), Some("{exit}"));
    assert!(exits.data_points()[0].bucket_counts().is_empty());
}

/// A partial catalogue degrades the series it does not name to the count/sum
/// form rather than dropping it.
#[test]
fn a_metric_outside_the_catalogue_still_exports_without_a_unit() {
    let recorder = InMemoryMetricsRecorder::new();
    recorder.record_histogram(METRIC_GRPC_REQUEST_LATENCY_MS, 3.0, &[]);

    let export = export_open_telemetry_metrics_with_instruments(
        &recorder.snapshot(),
        &[],
        &[OpenTelemetryInstrumentView {
            name: METRIC_HTTP_REQUEST_LATENCY_MS,
            unit: "ms",
            bucket_boundaries: &[1.0],
        }],
    );

    let grpc = export
        .metrics()
        .iter()
        .find(|metric| metric.name() == METRIC_GRPC_REQUEST_LATENCY_MS)
        .expect("an uncatalogued metric is still exported");
    assert_eq!(grpc.unit(), None);
    assert_eq!(grpc.data_points()[0].count(), Some(1));
    assert!(grpc.data_points()[0].bucket_counts().is_empty());
}

/// A bucket-count vector that does not match its boundaries is stored
/// bucketless: a telemetry record is never a correctness input, and a wrong
/// distribution is worse than an absent one.
#[test]
fn a_mismatched_bucket_pair_is_stored_without_buckets() {
    let point = OpenTelemetryDataPoint::histogram_with_buckets(
        Vec::new(),
        3,
        6.0,
        vec![1.0, 5.0],
        vec![1, 2],
    );
    assert!(point.bucket_boundaries().is_empty());
    assert!(point.bucket_counts().is_empty());
    assert_eq!(point.count(), Some(3));
    assert_eq!(point.sum(), Some(6.0));
}

/// The extension is additive: a bridge record serialized before the unit and
/// bucket fields existed decodes with a unit-less metric and bucketless data
/// points, so an older exporter's payload is still readable.
#[test]
fn a_pre_extension_metric_record_decodes_without_unit_or_buckets() {
    let recorder = InMemoryMetricsRecorder::new();
    recorder.record_histogram(METRIC_HTTP_REQUEST_LATENCY_MS, 7.0, &[]);
    let export = export_open_telemetry_metrics_with_instruments(
        &recorder.snapshot(),
        &[],
        &[OpenTelemetryInstrumentView {
            name: METRIC_HTTP_REQUEST_LATENCY_MS,
            unit: "ms",
            bucket_boundaries: &[1.0, 10.0],
        }],
    );

    let mut encoded: serde_json::Value =
        serde_json::to_value(&export).expect("the export serializes");
    for metric in encoded["metrics"]
        .as_array_mut()
        .expect("metrics is an array")
    {
        let metric = metric.as_object_mut().expect("a metric is an object");
        metric.remove("unit");
        for point in metric["data_points"]
            .as_array_mut()
            .expect("data_points is an array")
        {
            let point = point.as_object_mut().expect("a data point is an object");
            point.remove("bucket_boundaries");
            point.remove("bucket_counts");
        }
    }

    let decoded: rakka_core::OpenTelemetryMetricsExport =
        serde_json::from_value(encoded).expect("a pre-extension record decodes");
    let metric = &decoded.metrics()[0];
    assert_eq!(metric.unit(), None);
    assert!(metric.data_points()[0].bucket_boundaries().is_empty());
    assert!(metric.data_points()[0].bucket_counts().is_empty());
    assert_eq!(metric.data_points()[0].count(), Some(1));
}
