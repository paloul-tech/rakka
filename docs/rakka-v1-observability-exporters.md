# Rakka V1 Observability Exporters

Slice V1F adds production-oriented exporter adapters on top of Rakka's backend-neutral metrics model. Runtime crates still record observations through `rakka_core::MetricsRecorder`; applications decide which exporter routes to expose and which production collector receives them.

## Metrics Boundary

`rakka-core` remains the canonical metrics boundary:

- metric names are defined as stable constants such as `METRIC_HTTP_REQUEST_LATENCY_MS`, `METRIC_GRPC_REQUEST_LATENCY_MS`, `METRIC_STREAM_PRESSURE`, and `METRIC_REMOTE_FAILURES`;
- observations are recorded as counters, gauges, and histograms with string attributes;
- `InMemoryMetricsRecorder` remains the test/example recorder;
- production exporters consume `MetricsSnapshot` values instead of coupling runtime crates to a specific backend.

The current exporter helpers are:

- `rakka_core::export_prometheus_text` for Prometheus text exposition;
- `rakka_core::export_open_telemetry_metrics` for a serializable OpenTelemetry-oriented bridge model;
- `rakka_http::prometheus_metrics_route` for a GET `/metrics` style route;
- `rakka_http::open_telemetry_metrics_json_route` for a JSON bridge route;
- `rakka_http::OperationalSnapshotRegistry` and `rakka_http::operational_snapshots_route` for named operational JSON snapshots.

## Prometheus Export

Rakka's canonical metric names use dots, for example `rakka.http.request.latency_ms`. Prometheus metric identifiers cannot contain dots, so the exporter maps dots and other unsupported characters to underscores. The canonical metric remains visible in the `# HELP` line.

Example mapping:

```text
rakka.http.request.latency_ms -> rakka_http_request_latency_ms
rakka.remote.failures -> rakka_remote_failures
```

Counters are summed by metric name and label set. Gauges export the latest value by metric name and label set. Histograms currently export summary-compatible `_count` and `_sum` series because Rakka's backend-neutral recorder stores raw observations without fixed buckets.

## OpenTelemetry Bridge

`export_open_telemetry_metrics` returns a serializable bridge object with:

- resource attributes such as `service.name`, `service.namespace`, `deployment.environment`, and Kubernetes pod identity;
- canonical Rakka metric names;
- instrument kind: counter, gauge, or histogram;
- cumulative temporality;
- data points with attributes and either scalar values or histogram count/sum.

This deliberately avoids depending on one OpenTelemetry SDK version inside Rakka v1. Applications can map the bridge into their chosen OpenTelemetry SDK, collector, or OTLP exporter.

Tracing remains based on the `tracing` crate. HTTP and gRPC adapters create request-boundary spans, stream helpers create stream pipeline spans, and remoting emits connection/failure events under `rakka.*` targets. Applications that want OpenTelemetry traces should install a `tracing` subscriber with an OpenTelemetry layer at the binary boundary.

## Operational Snapshots

`OperationalSnapshotRegistry` is a small named-provider registry for JSON diagnostics. It is intentionally generic so applications can register the state they actually own:

```rust
let snapshots = rakka_http::OperationalSnapshotRegistry::new();

snapshots.register_snapshot("actor_system", {
    let system = system.clone();
    move || system.record_metrics()
});

snapshots.register_snapshot("kubernetes_health", {
    let health = health.clone();
    move || health.snapshot()
});
```

Common snapshot providers include actor system snapshots, cluster membership operational snapshots, shard ownership snapshots, process actor status, stream status, Kubernetes health, HTTP/gRPC adapter state, and application-specific service state.

## Cardinality Guidance

Keep labels bounded and operationally meaningful:

- Prefer route templates such as `/cart/:id` or `/counter/add`; do not use raw request paths with unbounded ids.
- Prefer actor system names and actor role names; avoid full per-entity actor paths when entity ids are high cardinality.
- Prefer entity type and owning node for sharding metrics; avoid entity id labels for hot paths.
- Prefer process role names such as `legacy-calculator`; avoid command arguments, PIDs, temp paths, or user-provided filenames.
- Prefer stable error codes from Rakka error types; avoid full error messages as labels.
- Use Kubernetes pod, namespace, and node identity as resource attributes rather than repeating them on every high-frequency metric label set.

## Example

The edge gateway example exposes Prometheus text metrics, OpenTelemetry bridge JSON, and named operational snapshots through its in-process router:

```sh
cargo run -p rakka-example-edge-gateway
```

Expected output includes:

```text
Observability routes exposed /metrics, /otel/metrics, and /snapshots.
```

## Validation

Run focused exporter and route tests:

```sh
cargo test -p rakka-core --test observability_exporters
cargo test -p rakka-http --test observability_routes
```

Run the example:

```sh
cargo run -p rakka-example-edge-gateway
```
