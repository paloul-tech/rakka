//! Metrics traits, stable metric names, test-friendly recorders, and exporter adapters.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

/// Key/value metric attributes.
pub type MetricAttributes<'a> = &'a [(&'a str, &'a str)];

/// Active actor count gauge.
pub const METRIC_ACTOR_COUNT: &str = "rakka.actor.count";

/// Actor mailbox depth gauge.
pub const METRIC_ACTOR_MAILBOX_DEPTH: &str = "rakka.actor.mailbox.depth";

/// Cluster members by membership state gauge.
pub const METRIC_CLUSTER_MEMBERS: &str = "rakka.cluster.members";

/// Shards owned by node gauge.
pub const METRIC_SHARD_OWNERSHIP_COUNT: &str = "rakka.sharding.shards_owned";

/// Durable state operation latency histogram in milliseconds.
pub const METRIC_PERSISTENCE_LATENCY_MS: &str = "rakka.persistence.operation.latency_ms";

/// Remote transport, envelope, and codec failure counter.
pub const METRIC_REMOTE_FAILURES: &str = "rakka.remote.failures";

/// Child process exit counter.
pub const METRIC_PROCESS_EXITS: &str = "rakka.process.exits";

/// Stream bounded-buffer pressure gauge.
pub const METRIC_STREAM_PRESSURE: &str = "rakka.stream.pressure";

/// Stream cancellation counter.
pub const METRIC_STREAM_CANCELLATIONS: &str = "rakka.stream.cancellations";

/// HTTP request latency histogram in milliseconds.
pub const METRIC_HTTP_REQUEST_LATENCY_MS: &str = "rakka.http.request.latency_ms";

/// gRPC request latency histogram in milliseconds.
pub const METRIC_GRPC_REQUEST_LATENCY_MS: &str = "rakka.grpc.request.latency_ms";

/// Kubernetes readiness state gauge.
pub const METRIC_K8S_READINESS: &str = "rakka.k8s.readiness";

/// Kubernetes cluster compatibility state gauge.
pub const METRIC_K8S_COMPATIBILITY: &str = "rakka.k8s.compatibility";

/// Coordinated shutdown phase duration histogram in milliseconds.
pub const METRIC_SHUTDOWN_PHASE_DURATION_MS: &str = "rakka.shutdown.phase.duration_ms";

/// Coordinated shutdown task duration histogram in milliseconds.
pub const METRIC_SHUTDOWN_TASK_DURATION_MS: &str = "rakka.shutdown.task.duration_ms";

/// Coordinated shutdown task failure counter.
pub const METRIC_SHUTDOWN_TASK_FAILURES: &str = "rakka.shutdown.task.failures";

/// Coordinated shutdown timeout counter for task and phase deadlines.
pub const METRIC_SHUTDOWN_TIMEOUTS: &str = "rakka.shutdown.timeouts";

/// Coordinated shutdown running-state gauge.
pub const METRIC_SHUTDOWN_RUNNING: &str = "rakka.shutdown.running";

/// Minimal metrics sink used across runtime crates.
pub trait MetricsRecorder: Send + Sync {
    /// Increments a monotonically increasing counter.
    fn increment_counter(&self, name: &str, value: u64, attributes: MetricAttributes<'_>);

    /// Records the latest value for a gauge.
    fn record_gauge(&self, name: &str, value: f64, attributes: MetricAttributes<'_>);

    /// Records a histogram observation.
    fn record_histogram(&self, name: &str, value: f64, attributes: MetricAttributes<'_>);
}

/// Kind of metric observation captured by an in-memory recorder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MetricKind {
    /// Monotonically increasing counter.
    Counter,
    /// Point-in-time gauge value.
    Gauge,
    /// Distribution sample.
    Histogram,
}

impl MetricKind {
    /// Stable metric-kind label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// Owned key/value metric attribute.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MetricAttribute {
    key: String,
    value: String,
}

impl MetricAttribute {
    /// Creates an owned metric attribute.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Attribute key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Attribute value.
    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// One recorded metric observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricObservation {
    kind: MetricKind,
    name: String,
    value: f64,
    attributes: Vec<MetricAttribute>,
}

impl MetricObservation {
    /// Creates a metric observation.
    #[must_use]
    pub fn new(
        kind: MetricKind,
        name: impl Into<String>,
        value: f64,
        attributes: MetricAttributes<'_>,
    ) -> Self {
        Self {
            kind,
            name: name.into(),
            value,
            attributes: attributes
                .iter()
                .map(|(key, value)| MetricAttribute::new(*key, *value))
                .collect(),
        }
    }

    /// Observation kind.
    #[must_use]
    pub const fn kind(&self) -> MetricKind {
        self.kind
    }

    /// Metric name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Numeric value.
    #[must_use]
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Owned attributes.
    #[must_use]
    pub fn attributes(&self) -> &[MetricAttribute] {
        &self.attributes
    }

    /// Returns one attribute value by key.
    #[must_use]
    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes
            .iter()
            .find(|attribute| attribute.key() == key)
            .map(MetricAttribute::value)
    }
}

/// Serializable point-in-time view of in-memory metric observations.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    observations: Vec<MetricObservation>,
}

impl MetricsSnapshot {
    /// Creates a metrics snapshot from observations.
    #[must_use]
    pub fn new(observations: Vec<MetricObservation>) -> Self {
        Self { observations }
    }

    /// Recorded observations in insertion order.
    #[must_use]
    pub fn observations(&self) -> &[MetricObservation] {
        &self.observations
    }

    /// Returns observations with the provided metric name.
    #[must_use]
    pub fn observations_named(&self, name: &str) -> Vec<&MetricObservation> {
        self.observations
            .iter()
            .filter(|observation| observation.name() == name)
            .collect()
    }

    /// Returns the last observation matching the name and kind.
    #[must_use]
    pub fn last_observation(&self, name: &str, kind: MetricKind) -> Option<&MetricObservation> {
        self.observations
            .iter()
            .rev()
            .find(|observation| observation.name() == name && observation.kind() == kind)
    }

    /// Returns the last gauge value for the provided metric name.
    #[must_use]
    pub fn last_gauge(&self, name: &str) -> Option<f64> {
        self.last_observation(name, MetricKind::Gauge)
            .map(MetricObservation::value)
    }

    /// Returns the sum of counter increments for the provided metric name.
    #[must_use]
    pub fn counter_total(&self, name: &str) -> f64 {
        self.observations
            .iter()
            .filter(|observation| {
                observation.name() == name && observation.kind() == MetricKind::Counter
            })
            .map(MetricObservation::value)
            .sum()
    }
}

/// Configuration for rendering Rakka metrics in the Prometheus text exposition format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrometheusTextConfig {
    include_help: bool,
}

impl Default for PrometheusTextConfig {
    fn default() -> Self {
        Self { include_help: true }
    }
}

impl PrometheusTextConfig {
    /// Creates a Prometheus text exporter configuration with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self { include_help: true }
    }

    /// Sets whether `# HELP` comments should be emitted.
    #[must_use]
    pub const fn include_help(mut self, include_help: bool) -> Self {
        self.include_help = include_help;
        self
    }

    /// Returns true when `# HELP` comments should be emitted.
    #[must_use]
    pub const fn help_enabled(&self) -> bool {
        self.include_help
    }
}

/// Serializes a metrics snapshot to Prometheus text exposition format.
///
/// Rakka metric constants use dot-separated names such as
/// `rakka.http.request.latency_ms`. Prometheus metric identifiers cannot
/// contain dots, so this exporter deterministically maps them to underscores
/// such as `rakka_http_request_latency_ms`.
#[must_use]
pub fn export_prometheus_text(snapshot: &MetricsSnapshot) -> String {
    export_prometheus_text_with_config(snapshot, &PrometheusTextConfig::default())
}

/// Serializes a metrics snapshot to Prometheus text exposition format with a custom config.
#[must_use]
pub fn export_prometheus_text_with_config(
    snapshot: &MetricsSnapshot,
    config: &PrometheusTextConfig,
) -> String {
    let mut output = String::new();
    for metric in aggregate_metrics(snapshot) {
        let prometheus_name = prometheus_metric_name(&metric.name);
        if config.help_enabled() {
            let help = prometheus_help_text(&metric.name);
            let _ = writeln!(output, "# HELP {prometheus_name} {help}");
        }
        let metric_type = match metric.kind {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "summary",
        };
        let _ = writeln!(output, "# TYPE {prometheus_name} {metric_type}");

        match metric.kind {
            MetricKind::Counter => {
                for (attributes, value) in metric.counters {
                    write_prometheus_sample(&mut output, &prometheus_name, &attributes, value);
                }
            }
            MetricKind::Gauge => {
                for (attributes, value) in metric.gauges {
                    write_prometheus_sample(&mut output, &prometheus_name, &attributes, value);
                }
            }
            MetricKind::Histogram => {
                for (attributes, summary) in metric.histograms {
                    let count_name = format!("{prometheus_name}_count");
                    let sum_name = format!("{prometheus_name}_sum");
                    write_prometheus_sample(
                        &mut output,
                        &count_name,
                        &attributes,
                        summary.count as f64,
                    );
                    write_prometheus_sample(&mut output, &sum_name, &attributes, summary.sum);
                }
            }
        }
    }
    output
}

/// Converts a Rakka metric name into a Prometheus-compatible metric identifier.
#[must_use]
pub fn prometheus_metric_name(name: &str) -> String {
    sanitize_prometheus_identifier(name, true)
}

/// Converts a Rakka metric attribute key into a Prometheus-compatible label identifier.
#[must_use]
pub fn prometheus_label_name(name: &str) -> String {
    sanitize_prometheus_identifier(name, false)
}

/// Serializable OpenTelemetry-oriented view of a Rakka metrics snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenTelemetryMetricsExport {
    resource_attributes: Vec<MetricAttribute>,
    metrics: Vec<OpenTelemetryMetric>,
}

impl OpenTelemetryMetricsExport {
    /// Creates an OpenTelemetry metrics export view.
    #[must_use]
    pub fn new(
        resource_attributes: Vec<MetricAttribute>,
        metrics: Vec<OpenTelemetryMetric>,
    ) -> Self {
        Self {
            resource_attributes,
            metrics,
        }
    }

    /// Resource attributes that should be attached to the emitted resource.
    #[must_use]
    pub fn resource_attributes(&self) -> &[MetricAttribute] {
        &self.resource_attributes
    }

    /// Metrics grouped by canonical Rakka metric name and kind.
    #[must_use]
    pub fn metrics(&self) -> &[OpenTelemetryMetric] {
        &self.metrics
    }
}

/// OpenTelemetry instrument kind used by the bridge model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpenTelemetryInstrumentKind {
    /// Monotonic cumulative counter.
    Counter,
    /// Observable point-in-time gauge.
    Gauge,
    /// Histogram distribution point.
    Histogram,
}

/// OpenTelemetry aggregation temporality used by the bridge model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OpenTelemetryTemporality {
    /// Cumulative values since recorder start or last recorder reset.
    Cumulative,
}

/// One OpenTelemetry-oriented metric group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenTelemetryMetric {
    name: String,
    kind: OpenTelemetryInstrumentKind,
    temporality: OpenTelemetryTemporality,
    data_points: Vec<OpenTelemetryDataPoint>,
    /// UCUM-compatible unit, when the caller supplied an instrument
    /// definition for the metric. A record exported before the field existed
    /// decodes without one, so the extension is additive.
    #[serde(default)]
    unit: Option<String>,
}

impl OpenTelemetryMetric {
    /// Creates an OpenTelemetry metric group.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        kind: OpenTelemetryInstrumentKind,
        temporality: OpenTelemetryTemporality,
        data_points: Vec<OpenTelemetryDataPoint>,
    ) -> Self {
        Self {
            name: name.into(),
            kind,
            temporality,
            data_points,
            unit: None,
        }
    }

    /// Sets the UCUM-compatible unit.
    #[must_use]
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// UCUM-compatible unit, when one was supplied.
    #[must_use]
    pub fn unit(&self) -> Option<&str> {
        self.unit.as_deref()
    }

    /// Canonical Rakka metric name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// OpenTelemetry instrument kind.
    #[must_use]
    pub const fn kind(&self) -> OpenTelemetryInstrumentKind {
        self.kind
    }

    /// Aggregation temporality.
    #[must_use]
    pub const fn temporality(&self) -> OpenTelemetryTemporality {
        self.temporality
    }

    /// Metric data points.
    #[must_use]
    pub fn data_points(&self) -> &[OpenTelemetryDataPoint] {
        &self.data_points
    }
}

/// One OpenTelemetry-oriented data point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenTelemetryDataPoint {
    attributes: Vec<MetricAttribute>,
    value: Option<f64>,
    count: Option<u64>,
    sum: Option<f64>,
    /// Explicit histogram bucket upper bounds, ascending. Empty when the
    /// caller supplied no boundaries for the instrument. A record exported
    /// before the field existed decodes empty.
    #[serde(default)]
    bucket_boundaries: Vec<f64>,
    /// Counts per bucket, one longer than [`Self::bucket_boundaries`]: the
    /// final entry is the `+Inf` overflow bucket, as OTLP requires.
    #[serde(default)]
    bucket_counts: Vec<u64>,
}

impl OpenTelemetryDataPoint {
    /// Creates a scalar counter or gauge data point.
    #[must_use]
    pub fn scalar(attributes: Vec<MetricAttribute>, value: f64) -> Self {
        Self {
            attributes,
            value: Some(value),
            count: None,
            sum: None,
            bucket_boundaries: Vec::new(),
            bucket_counts: Vec::new(),
        }
    }

    /// Creates a histogram data point with count and sum.
    #[must_use]
    pub fn histogram(attributes: Vec<MetricAttribute>, count: u64, sum: f64) -> Self {
        Self {
            attributes,
            value: None,
            count: Some(count),
            sum: Some(sum),
            bucket_boundaries: Vec::new(),
            bucket_counts: Vec::new(),
        }
    }

    /// Creates a histogram data point carrying explicit bucket boundaries.
    ///
    /// `bucket_counts` must be one longer than `bucket_boundaries`; the extra
    /// entry is the `+Inf` overflow bucket. A mismatched pair is stored as a
    /// bucketless point rather than as a malformed one, because a telemetry
    /// record is never a correctness input and a wrong distribution is worse
    /// than an absent one.
    #[must_use]
    pub fn histogram_with_buckets(
        attributes: Vec<MetricAttribute>,
        count: u64,
        sum: f64,
        bucket_boundaries: Vec<f64>,
        bucket_counts: Vec<u64>,
    ) -> Self {
        let consistent =
            !bucket_boundaries.is_empty() && bucket_counts.len() == bucket_boundaries.len() + 1;
        Self {
            attributes,
            value: None,
            count: Some(count),
            sum: Some(sum),
            bucket_boundaries: if consistent {
                bucket_boundaries
            } else {
                Vec::new()
            },
            bucket_counts: if consistent {
                bucket_counts
            } else {
                Vec::new()
            },
        }
    }

    /// Data point attributes.
    #[must_use]
    pub fn attributes(&self) -> &[MetricAttribute] {
        &self.attributes
    }

    /// Scalar value for counter or gauge points.
    #[must_use]
    pub const fn value(&self) -> Option<f64> {
        self.value
    }

    /// Histogram sample count.
    #[must_use]
    pub const fn count(&self) -> Option<u64> {
        self.count
    }

    /// Histogram sample sum.
    #[must_use]
    pub const fn sum(&self) -> Option<f64> {
        self.sum
    }

    /// Explicit histogram bucket upper bounds, ascending.
    #[must_use]
    pub fn bucket_boundaries(&self) -> &[f64] {
        &self.bucket_boundaries
    }

    /// Counts per bucket, one longer than [`Self::bucket_boundaries`].
    #[must_use]
    pub fn bucket_counts(&self) -> &[u64] {
        &self.bucket_counts
    }
}

/// What a caller's instrument catalogue tells the exporter about one metric.
///
/// The recorder stores raw observations and knows nothing about instruments,
/// so unit and bucket semantics have to arrive from the domain that declared
/// them. This borrowed view is the whole contract: `rakka-core` stays free of
/// every domain's catalogue type while still emitting the unit and bucket
/// fields the OpenTelemetry semantic conventions require rather than dropping
/// them silently.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OpenTelemetryInstrumentView<'a> {
    /// Canonical Rakka metric name this view describes.
    pub name: &'a str,
    /// UCUM-compatible unit label, or the empty string when the instrument
    /// declares none.
    pub unit: &'a str,
    /// Explicit histogram bucket upper bounds, ascending. Empty for counters,
    /// gauges, and histograms whose distribution the caller does not bucket.
    ///
    /// A set that is not strictly ascending, or that carries a NaN or an
    /// infinity, is *ignored* rather than trusted — see
    /// [`Self::has_usable_buckets`].
    pub bucket_boundaries: &'a [f64],
}

impl OpenTelemetryInstrumentView<'_> {
    /// Whether [`Self::bucket_boundaries`] can be bucketed against.
    ///
    /// The bucketing walk takes the first boundary an observation does not
    /// exceed, which is the right answer only for a strictly ascending set. A
    /// declaration is caller-supplied — this is a public entry point, and the
    /// only consistency check anywhere near it was that `bucket_counts` is one
    /// longer than `bucket_boundaries`, which an unsorted or duplicated set
    /// satisfies — so boundaries such as `[10.0, 5.0, 50.0]` placed every
    /// observation in the first bound it did not exceed and produced a
    /// non-monotonic cumulative histogram: a Collector rejects it, or renders
    /// nonsense quantiles, while the unit and the bucket vector make it look
    /// authoritative. A NaN boundary is never `>=` anything, so it silently
    /// swallowed the ordering too.
    ///
    /// An unusable declaration degrades to the count/sum form, exactly as a
    /// metric the catalogue does not name does, on the principle this module
    /// already states for a mismatched bucket pair: a wrong distribution is
    /// worse than an absent one.
    #[must_use]
    pub fn has_usable_buckets(&self) -> bool {
        !self.bucket_boundaries.is_empty()
            && self
                .bucket_boundaries
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            && self
                .bucket_boundaries
                .iter()
                .all(|boundary| boundary.is_finite())
    }
}

/// Converts a Rakka metrics snapshot into a serializable OpenTelemetry-oriented bridge model.
///
/// This helper intentionally does not depend on a concrete OpenTelemetry SDK.
/// Applications can map the returned resource attributes, instrument kinds,
/// temporality, and data points into the SDK/exporter stack they already use.
///
/// The exported metrics carry no unit and no bucket boundaries, because a
/// snapshot alone does not know them. A caller that has an instrument
/// catalogue should use [`export_open_telemetry_metrics_with_instruments`].
#[must_use]
pub fn export_open_telemetry_metrics(
    snapshot: &MetricsSnapshot,
    resource_attributes: MetricAttributes<'_>,
) -> OpenTelemetryMetricsExport {
    export_open_telemetry_metrics_with_instruments(snapshot, resource_attributes, &[])
}

/// Converts a snapshot into the bridge model, carrying instrument units and
/// bucketing histogram observations against declared boundaries.
///
/// An observation whose metric names no instrument in `instruments` is
/// exported exactly as [`export_open_telemetry_metrics`] exports it, so a
/// partial catalogue degrades to the count/sum form rather than dropping the
/// series.
#[must_use]
pub fn export_open_telemetry_metrics_with_instruments(
    snapshot: &MetricsSnapshot,
    resource_attributes: MetricAttributes<'_>,
    instruments: &[OpenTelemetryInstrumentView<'_>],
) -> OpenTelemetryMetricsExport {
    let resource_attributes = owned_attributes(resource_attributes);
    let metrics = aggregate_metrics_with_instruments(snapshot, instruments)
        .into_iter()
        .map(|metric| {
            let kind = match metric.kind {
                MetricKind::Counter => OpenTelemetryInstrumentKind::Counter,
                MetricKind::Gauge => OpenTelemetryInstrumentKind::Gauge,
                MetricKind::Histogram => OpenTelemetryInstrumentKind::Histogram,
            };
            let data_points = match metric.kind {
                MetricKind::Counter => metric
                    .counters
                    .into_iter()
                    .map(|(attributes, value)| OpenTelemetryDataPoint::scalar(attributes, value))
                    .collect(),
                MetricKind::Gauge => metric
                    .gauges
                    .into_iter()
                    .map(|(attributes, value)| OpenTelemetryDataPoint::scalar(attributes, value))
                    .collect(),
                MetricKind::Histogram => metric
                    .histograms
                    .into_iter()
                    .map(|(attributes, summary)| {
                        if summary.bucket_counts.is_empty() {
                            OpenTelemetryDataPoint::histogram(
                                attributes,
                                summary.count,
                                summary.sum,
                            )
                        } else {
                            OpenTelemetryDataPoint::histogram_with_buckets(
                                attributes,
                                summary.count,
                                summary.sum,
                                metric.bucket_boundaries.clone(),
                                summary.bucket_counts,
                            )
                        }
                    })
                    .collect(),
            };
            let exported = OpenTelemetryMetric::new(
                metric.name,
                kind,
                OpenTelemetryTemporality::Cumulative,
                data_points,
            );
            match metric.unit {
                Some(unit) => exported.with_unit(unit),
                None => exported,
            }
        })
        .collect();
    OpenTelemetryMetricsExport::new(resource_attributes, metrics)
}

/// Metrics recorder that keeps all observations in memory for tests.
#[derive(Debug, Default, Clone)]
pub struct InMemoryMetricsRecorder {
    observations: Arc<Mutex<Vec<MetricObservation>>>,
}

impl InMemoryMetricsRecorder {
    /// Creates an empty in-memory metrics recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of recorded observations.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot::new(
            self.observations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }

    /// Clears all recorded observations.
    pub fn clear(&self) {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn push(&self, observation: MetricObservation) {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(observation);
    }
}

impl MetricsRecorder for InMemoryMetricsRecorder {
    fn increment_counter(&self, name: &str, value: u64, attributes: MetricAttributes<'_>) {
        self.push(MetricObservation::new(
            MetricKind::Counter,
            name,
            value as f64,
            attributes,
        ));
    }

    fn record_gauge(&self, name: &str, value: f64, attributes: MetricAttributes<'_>) {
        self.push(MetricObservation::new(
            MetricKind::Gauge,
            name,
            value,
            attributes,
        ));
    }

    fn record_histogram(&self, name: &str, value: f64, attributes: MetricAttributes<'_>) {
        self.push(MetricObservation::new(
            MetricKind::Histogram,
            name,
            value,
            attributes,
        ));
    }
}

/// Metrics recorder that intentionally drops all observations.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn increment_counter(&self, _name: &str, _value: u64, _attributes: MetricAttributes<'_>) {}

    fn record_gauge(&self, _name: &str, _value: f64, _attributes: MetricAttributes<'_>) {}

    fn record_histogram(&self, _name: &str, _value: f64, _attributes: MetricAttributes<'_>) {}
}

#[derive(Debug)]
struct AggregatedMetric {
    name: String,
    kind: MetricKind,
    unit: Option<String>,
    bucket_boundaries: Vec<f64>,
    counters: BTreeMap<Vec<MetricAttribute>, f64>,
    gauges: BTreeMap<Vec<MetricAttribute>, f64>,
    histograms: BTreeMap<Vec<MetricAttribute>, HistogramSummary>,
}

#[derive(Debug, Default, Clone)]
struct HistogramSummary {
    count: u64,
    sum: f64,
    bucket_counts: Vec<u64>,
}

fn aggregate_metrics(snapshot: &MetricsSnapshot) -> Vec<AggregatedMetric> {
    aggregate_metrics_with_instruments(snapshot, &[])
}

/// The bucket an observation falls in: the first boundary it does not exceed,
/// or the trailing `+Inf` bucket when it exceeds them all.
fn histogram_bucket_index(boundaries: &[f64], value: f64) -> usize {
    boundaries
        .iter()
        .position(|boundary| value <= *boundary)
        .unwrap_or(boundaries.len())
}

fn aggregate_metrics_with_instruments(
    snapshot: &MetricsSnapshot,
    instruments: &[OpenTelemetryInstrumentView<'_>],
) -> Vec<AggregatedMetric> {
    let mut metrics = BTreeMap::<(String, MetricKind), AggregatedMetric>::new();
    for observation in snapshot.observations() {
        let key = (observation.name().to_owned(), observation.kind());
        let metric = metrics.entry(key).or_insert_with(|| {
            let instrument = instruments
                .iter()
                .find(|instrument| instrument.name == observation.name());
            AggregatedMetric {
                name: observation.name().to_owned(),
                kind: observation.kind(),
                unit: instrument
                    .map(|instrument| instrument.unit)
                    .filter(|unit| !unit.is_empty())
                    .map(ToOwned::to_owned),
                bucket_boundaries: instrument
                    .filter(|instrument| instrument.has_usable_buckets())
                    .map(|instrument| instrument.bucket_boundaries.to_vec())
                    .unwrap_or_default(),
                counters: BTreeMap::new(),
                gauges: BTreeMap::new(),
                histograms: BTreeMap::new(),
            }
        });
        let attributes = sorted_owned_attributes(observation.attributes());
        match observation.kind() {
            MetricKind::Counter => {
                *metric.counters.entry(attributes).or_default() += observation.value();
            }
            MetricKind::Gauge => {
                metric.gauges.insert(attributes, observation.value());
            }
            MetricKind::Histogram => {
                // A non-finite observation is dropped rather than summed: one
                // NaN makes the exported `sum` of the whole series NaN, and
                // `value <= boundary` is false for every boundary, so it also
                // lands in the `+Inf` bucket while claiming to be a sample.
                // Losing one bad observation is the recoverable direction.
                if !observation.value().is_finite() {
                    continue;
                }
                let boundaries = metric.bucket_boundaries.clone();
                let summary = metric.histograms.entry(attributes).or_default();
                summary.count = summary.count.saturating_add(1);
                summary.sum += observation.value();
                if !boundaries.is_empty() {
                    if summary.bucket_counts.len() != boundaries.len() + 1 {
                        summary.bucket_counts = vec![0; boundaries.len() + 1];
                    }
                    let index = histogram_bucket_index(&boundaries, observation.value());
                    summary.bucket_counts[index] = summary.bucket_counts[index].saturating_add(1);
                }
            }
        }
    }
    metrics.into_values().collect()
}

fn owned_attributes(attributes: MetricAttributes<'_>) -> Vec<MetricAttribute> {
    sorted_owned_attributes(
        &attributes
            .iter()
            .map(|(key, value)| MetricAttribute::new(*key, *value))
            .collect::<Vec<_>>(),
    )
}

fn sorted_owned_attributes(attributes: &[MetricAttribute]) -> Vec<MetricAttribute> {
    let mut attributes = attributes.to_vec();
    attributes.sort();
    attributes
}

fn sanitize_prometheus_identifier(identifier: &str, allow_colon: bool) -> String {
    let mut sanitized = String::with_capacity(identifier.len().max(1));
    for (index, character) in identifier.chars().enumerate() {
        let valid = character == '_'
            || character.is_ascii_alphabetic()
            || (allow_colon && character == ':')
            || (index > 0 && character.is_ascii_digit());
        if valid {
            sanitized.push(character);
        } else if index == 0 && character.is_ascii_digit() {
            sanitized.push('_');
            sanitized.push(character);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        "_".to_owned()
    } else {
        sanitized
    }
}

fn prometheus_help_text(canonical_name: &str) -> String {
    format!(
        "Rakka metric {}.",
        canonical_name.replace('\\', r"\\").replace('\n', r"\n")
    )
}

fn write_prometheus_sample(
    output: &mut String,
    name: &str,
    attributes: &[MetricAttribute],
    value: f64,
) {
    let _ = write!(output, "{name}");
    write_prometheus_labels(output, attributes);
    let _ = writeln!(output, " {}", prometheus_number(value));
}

fn write_prometheus_labels(output: &mut String, attributes: &[MetricAttribute]) {
    if attributes.is_empty() {
        return;
    }

    output.push('{');
    for (index, attribute) in attributes.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        let key = prometheus_label_name(attribute.key());
        let value = prometheus_label_value(attribute.value());
        let _ = write!(output, "{key}=\"{value}\"");
    }
    output.push('}');
}

fn prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}

fn prometheus_number(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "+Inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_owned()
    } else {
        value.to_string()
    }
}
