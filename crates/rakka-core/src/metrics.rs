//! Metrics traits shared by Rakka runtime crates.

/// Key/value metric attributes.
pub type MetricAttributes<'a> = &'a [(&'a str, &'a str)];

/// Minimal metrics sink used by Phase 0 crate boundaries.
pub trait MetricsRecorder: Send + Sync {
    /// Increments a monotonically increasing counter.
    fn increment_counter(&self, name: &str, value: u64, attributes: MetricAttributes<'_>);

    /// Records the latest value for a gauge.
    fn record_gauge(&self, name: &str, value: f64, attributes: MetricAttributes<'_>);

    /// Records a histogram observation.
    fn record_histogram(&self, name: &str, value: f64, attributes: MetricAttributes<'_>);
}

/// Metrics recorder that intentionally drops all observations.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn increment_counter(&self, _name: &str, _value: u64, _attributes: MetricAttributes<'_>) {}

    fn record_gauge(&self, _name: &str, _value: f64, _attributes: MetricAttributes<'_>) {}

    fn record_histogram(&self, _name: &str, _value: f64, _attributes: MetricAttributes<'_>) {}
}
