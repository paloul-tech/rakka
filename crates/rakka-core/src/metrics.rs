//! Metrics traits, stable metric names, and test-friendly recorders.

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
