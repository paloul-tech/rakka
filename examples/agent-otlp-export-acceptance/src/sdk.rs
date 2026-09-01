//! The OpenTelemetry SDK boundary this example owns.
//!
//! [Specification 17.17](../../../docs/plans/rakka-agent/spec.md) places the
//! SDK, the `tracing` subscriber and layer, the OTLP exporter, exporter
//! credentials, and shutdown/flush at the **application binary**, and keeps the
//! Rakka crates SDK- and version-neutral. This module is that binary's half of
//! the contract: everything below imports `opentelemetry*`, and nothing that
//! does lives in a `crates/` directory.
//!
//! Spans and metrics are handed to the OTLP exporters **directly**, as batches
//! this module builds from [`AgentOtlpBridgeExport`]. That is deliberate and it
//! is the only mapping that preserves what 17.17 requires. A span's trace and
//! span ids are already authoritative in the durable record, so minting a new
//! span through the `Tracer` API would replace them; and Rakka's metrics
//! arrive *already aggregated*, with the unit and bucket boundaries its
//! catalogue declares, so re-recording them through the `Meter` API would
//! re-aggregate them and re-declare the buckets here.
//!
//! Logs go the other way, through an [`SdkLoggerProvider`], because an
//! `SdkLogRecord` can only be minted by a logger — and because that same
//! provider is what the `tracing` layer feeds, so Rakka's own `tracing::warn!`
//! output and its durable [`AgentLogEvent`] records leave through one pipeline
//! with one flush.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity};
use opentelemetry::trace::{
    Event, Link, SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState,
};
use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::data::{
    Gauge, GaugeDataPoint, Histogram, HistogramDataPoint, Metric, ResourceMetrics, ScopeMetrics,
    Sum, SumDataPoint,
};
use opentelemetry_sdk::metrics::exporter::PushMetricExporter as _;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanExporter as _, SpanLinks};
use opentelemetry_sdk::Resource;

use rakka_agent::observability::{
    AgentSegmentSink, AgentTelemetrySegment, METRIC_AGENT_EFFECT_OUTSTANDING_DURATION,
    METRIC_AGENT_MODEL_TOKENS, METRIC_AGENT_RECOVERY_DURATION, METRIC_AGENT_TURN_DURATION,
};
use rakka_agent::otel::{agent_instrumentation_scope, AgentGenAiSpanExporter};
use rakka_agent_workflow::{
    AgentAttributes, AgentLogEvent, AgentOtelInstrumentationScope, AgentOtelResource,
    AgentOtelSpanExport, AgentOtelSpanKind, AgentOtelSpanStatus, AgentOtlpExporterConfig,
    AgentSpanLink,
};
use rakka_core::{
    MetricsRecorder, MetricsSnapshot, OpenTelemetryDataPoint, OpenTelemetryInstrumentKind,
    OpenTelemetryMetric,
};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Layer as _, SubscriberExt as _};
use tracing_subscriber::util::SubscriberInitExt as _;

/// Which bounded segment class carries the trace identity for which histogram.
///
/// An exemplar links a representative measurement to the trace that produced
/// it, and Rakka's `MetricsRecorder` has no trace identity to read — trace
/// context here is an explicit value on a durable record, never an ambient one
/// ([`docs/rakka-v1-observability-exporters.md`]). What *does* carry both is a
/// closed [`AgentTelemetrySegment`]: it ends in the same synchronous region
/// that records the measurement, and it carries the run's durable trace
/// context. See [`ExemplarIdentity`] for exactly which span the resulting
/// exemplar names.
///
/// So the correspondence is declared here rather than inferred, and
/// `tests/acceptance.rs` asserts every histogram in a real export carries an
/// exemplar. A histogram whose producing class stops closing a segment loses
/// its exemplar and fails that assertion, rather than silently exporting
/// without one.
pub const EXEMPLAR_SOURCES: &[(&str, &str)] = &[
    // The recovery duration is literally the segment's own width: `run.rs`
    // records `segment.start`/`segment.end` and closes that same segment on
    // the next statement.
    (METRIC_AGENT_RECOVERY_DURATION, "run-recover"),
    // Both are recorded inside `advance_loop`, in the block that closes the
    // resident invocation slice.
    (METRIC_AGENT_TURN_DURATION, "invoke-agent"),
    (METRIC_AGENT_MODEL_TOKENS, "invoke-agent"),
    // Settled by the run entity from the effect's durable timestamps; the
    // dispatcher attempt that produced that durable result is the span an
    // operator follows from the measurement.
    (METRIC_AGENT_EFFECT_OUTSTANDING_DURATION, "effect-dispatch"),
];

/// The trace identity a histogram's exemplar points at.
///
/// **The span id is the segment's parent, not the segment's own**, and the
/// distinction is worth stating rather than glossing. A durable
/// `AgentTelemetryContext`'s `traceparent` names the span the work was
/// propagated *from* — slice 6.3a's review found the adapter writing it back
/// as a record's own id and collapsing 25 spans onto one — and a segment's own
/// span id is derived by the sink afterwards, from the fully populated record
/// plus an emission ordinal this reservoir never sees.
///
/// So the exemplar lands a reader in the right trace, at the span the
/// operation ran under. That is a representative link, which is what an
/// exemplar is; it is not a per-measurement one, and
/// `docs/rakka-agent-telemetry-validation-matrix.md` records it as such rather
/// than implying more.
#[derive(Debug, Clone, Copy)]
pub struct ExemplarIdentity {
    /// Trace id, as the 16 raw bytes OTLP carries.
    pub trace_id: [u8; 16],
    /// The parent span id the segment ran under, as OTLP's 8 raw bytes.
    pub span_id: [u8; 8],
    /// When the producing segment ended.
    pub time: SystemTime,
}

/// A bounded, application-owned exemplar reservoir fed by closed segments.
///
/// One entry per segment class named in [`EXEMPLAR_SOURCES`] — the reservoir
/// cannot grow with traffic, which is the property that lets it sit on the
/// hot path of an [`AgentSegmentSink`].
#[derive(Debug, Default)]
pub struct ExemplarReservoir {
    latest: Mutex<BTreeMap<&'static str, ExemplarIdentity>>,
}

impl ExemplarReservoir {
    /// Creates an empty reservoir.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the identity an exemplar for `metric` should point at.
    #[must_use]
    pub fn identity_for(&self, metric: &str) -> Option<ExemplarIdentity> {
        let class = EXEMPLAR_SOURCES
            .iter()
            .find(|(name, _)| *name == metric)
            .map(|(_, class)| *class)?;
        let latest = self.latest.lock().ok()?;
        latest.get(class).copied()
    }

    /// Observes one closed segment, keeping it if its class is a source.
    fn observe(&self, segment: &AgentTelemetrySegment) {
        let class = segment.operation.as_label();
        if !EXEMPLAR_SOURCES.iter().any(|(_, source)| *source == class) {
            return;
        }
        let Some(trace_parent) = segment.telemetry.trace_parent.as_deref() else {
            return;
        };
        let Some(trace_id) = trace_id_bytes(trace_parent) else {
            return;
        };
        let span_id = span_id_bytes(trace_parent).unwrap_or([0; 8]);
        let Ok(mut latest) = self.latest.lock() else {
            return;
        };
        latest.insert(
            class,
            ExemplarIdentity {
                trace_id,
                span_id,
                time: system_time(segment.end.as_millis()),
            },
        );
    }
}

/// An [`AgentSegmentSink`] that feeds the reservoir and forwards everything.
///
/// Decorating rather than replacing is what keeps the export path unchanged:
/// the inner sink is still the one that maps and buffers, and this wrapper
/// only reads what passes through.
#[derive(Debug)]
pub struct ExemplarSegmentSink {
    inner: Arc<AgentGenAiSpanExporter>,
    reservoir: Arc<ExemplarReservoir>,
}

impl ExemplarSegmentSink {
    /// Wraps `inner`, feeding `reservoir` from every segment it forwards.
    #[must_use]
    pub fn new(inner: Arc<AgentGenAiSpanExporter>, reservoir: Arc<ExemplarReservoir>) -> Self {
        Self { inner, reservoir }
    }
}

impl AgentSegmentSink for ExemplarSegmentSink {
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn record(&self, segment: &AgentTelemetrySegment) {
        self.reservoir.observe(segment);
        self.inner.record(segment);
    }
}

/// What one flush shipped, and what it lost.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentExportOutcome {
    /// Span records accepted by the exporter.
    pub spans: usize,
    /// Metric groups accepted by the exporter.
    pub metrics: usize,
    /// Log records emitted through the logger provider.
    pub logs: usize,
    /// Signals whose export returned an error.
    pub failed_signals: usize,
}

/// The application's OTLP export boundary: SDK, exporters, and flush.
pub struct AgentTelemetryExport {
    spans: opentelemetry_otlp::SpanExporter,
    metrics: opentelemetry_otlp::MetricExporter,
    logs: SdkLoggerProvider,
    resource: Resource,
    scope: InstrumentationScope,
    /// The bridge configuration this exporter was built from, kept so the
    /// batch Rakka validates is the batch the SDK ships.
    config: AgentOtlpExporterConfig,
    bridge_resource: AgentOtelResource,
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
    started_at: SystemTime,
}

impl std::fmt::Debug for AgentTelemetryExport {
    /// Hand-written: a `MetricsRecorder` is a caller-supplied trait object
    /// with no `Debug` bound.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentTelemetryExport")
            .field("endpoint", &self.config.endpoint)
            .field("scope", &self.scope.name())
            .field("metrics", &self.metrics_recorder.is_some())
            .finish()
    }
}

impl AgentTelemetryExport {
    /// Builds the three OTLP exporters from Rakka's own bridge configuration.
    ///
    /// [`AgentOtlpExporterConfig`] is what the agent domain already uses to
    /// describe an exporter — endpoint, protocol, timeout, and headers — so
    /// the configuration the bridge validates is the configuration the real
    /// exporter receives. Its headers are where **exporter credentials** enter:
    /// they are read here and never persisted, logged, or exported.
    pub fn install(
        config: &AgentOtlpExporterConfig,
        resource: &AgentOtelResource,
    ) -> Result<Self, String> {
        let scope = instrumentation_scope(&agent_instrumentation_scope());
        let sdk_resource = sdk_resource(resource);

        let spans = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.traces_endpoint.as_ref().unwrap_or(&config.endpoint))
            .with_metadata(credential_metadata(&config.headers))
            .with_timeout(export_timeout(config))
            .build()
            .map_err(|error| format!("span exporter: {error}"))?;
        let metrics = opentelemetry_otlp::MetricExporter::builder()
            .with_tonic()
            .with_endpoint(config.metrics_endpoint.as_ref().unwrap_or(&config.endpoint))
            .with_metadata(credential_metadata(&config.headers))
            .with_timeout(export_timeout(config))
            .with_temporality(Temporality::Cumulative)
            .build()
            .map_err(|error| format!("metric exporter: {error}"))?;
        let log_exporter = opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_endpoint(config.logs_endpoint.as_ref().unwrap_or(&config.endpoint))
            .with_metadata(credential_metadata(&config.headers))
            .with_timeout(export_timeout(config))
            .build()
            .map_err(|error| format!("log exporter: {error}"))?;

        let logs = SdkLoggerProvider::builder()
            .with_resource(sdk_resource.clone())
            .with_batch_exporter(log_exporter)
            .build();

        Ok(Self {
            spans,
            metrics,
            logs,
            resource: sdk_resource,
            scope,
            config: config.clone(),
            bridge_resource: resource.clone(),
            metrics_recorder: None,
            started_at: SystemTime::now(),
        })
    }

    /// Counts export failures into `metrics` as
    /// `rakka.agent.telemetry.flush.failures`, one bounded `signal` per OTLP
    /// signal.
    ///
    /// The counter is Rakka's; the `signal` values are this binary's, because
    /// the exporter is. `AGENT_OTLP_EXPORT_SIGNALS` declares them and
    /// `tests/exporter_failure.rs` holds them to a bijection, the same shape
    /// `rakka_agent::AGENT_TELEMETRY_SIGNALS` keeps for the values the crate
    /// itself writes.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics_recorder = Some(metrics);
        self
    }

    /// Installs the `tracing` subscriber that bridges Rakka's own logs to OTLP.
    ///
    /// Idempotent, and the guard is deliberate: a process has one global
    /// subscriber, and a second installation must leave the first one working
    /// rather than abort. `rakka_agent::testkit::CapturingSubscriber` is
    /// unaffected and stays as it is — it exists so the *workspace* need not
    /// depend on `tracing-subscriber`, which is still true: this dependency
    /// belongs to the example.
    pub fn install_tracing_bridge(&self) {
        static INSTALLED: OnceLock<()> = OnceLock::new();
        INSTALLED.get_or_init(|| {
            let bridge = OpenTelemetryTracingBridge::new(&self.logs).with_filter(
                // **The feedback loop, and why the filter is not optional.**
                // The appender turns a `tracing` event into a log record; the
                // logger provider exports it over OTLP; the OTLP exporter is
                // tonic over hyper, which emits `tracing` events of its own —
                // which the appender turns into log records. Unfiltered, that
                // is unbounded mutual recursion, and it does not fail
                // gracefully: this example overflowed a runtime worker's stack
                // and aborted the process before the filter was added.
                //
                // Restricting the bridge to Rakka's own targets is both the
                // fix and the intent. The application boundary exports the
                // application's logs; the transport's internal chatter is the
                // SDK's own diagnostic channel and belongs nowhere near the
                // pipeline it is describing.
                Targets::new()
                    .with_target("rakka", LevelFilter::INFO)
                    .with_target("rakka_agent", LevelFilter::INFO)
                    .with_target("rakka_agent_workflow", LevelFilter::INFO)
                    .with_target("rakka_a2a", LevelFilter::INFO)
                    .with_default(LevelFilter::OFF),
            );
            let _ = tracing_subscriber::registry().with(bridge).try_init();
        });
    }

    /// Builds one bridge export and ships it, in one call.
    ///
    /// A caller that needs to *inspect* what it shipped must use
    /// [`Self::bridge`] and [`Self::ship`] instead and pass the same value to
    /// both: `bridge_export` empties the exporter's ring on success, by
    /// design, so building twice ships an empty second batch.
    pub async fn flush(
        &self,
        spans: &AgentGenAiSpanExporter,
        metrics: &MetricsSnapshot,
        logs: Vec<AgentLogEvent>,
        exemplars: &ExemplarReservoir,
    ) -> AgentExportOutcome {
        match self.bridge(spans, metrics, logs) {
            Ok(bridge) => self.ship(&bridge, exemplars).await,
            Err(_) => AgentExportOutcome {
                failed_signals: 3,
                ..AgentExportOutcome::default()
            },
        }
    }

    /// Ships one already-built bridge export to the Collector.
    ///
    /// Telemetry is never a correctness input
    /// ([17.1](../../../docs/plans/rakka-agent/spec.md)), so nothing here can
    /// fail a caller: a signal whose export errors is counted and the others
    /// still ship.
    pub async fn ship(
        &self,
        bridge: &rakka_agent_workflow::AgentOtlpBridgeExport,
        exemplars: &ExemplarReservoir,
    ) -> AgentExportOutcome {
        let mut outcome = AgentExportOutcome::default();

        let batch = self.span_batch(&bridge.spans);
        if !batch.is_empty() {
            outcome.spans = batch.len();
            if self.spans.export(batch).await.is_err() {
                outcome.failed_signals += 1;
                outcome.spans = 0;
                self.count_failure("otlp-traces");
            }
        }

        let mut resource_metrics = self.resource_metrics(&bridge.metrics, exemplars);
        outcome.metrics = resource_metrics
            .scope_metrics
            .iter()
            .map(|scope| scope.metrics.len())
            .sum();
        if outcome.metrics > 0 && self.metrics.export(&mut resource_metrics).await.is_err() {
            outcome.failed_signals += 1;
            outcome.metrics = 0;
            self.count_failure("otlp-metrics");
        }

        outcome.logs = self.emit_logs(&bridge.logs);
        if outcome.logs > 0 && self.logs.force_flush().is_err() {
            outcome.failed_signals += 1;
            outcome.logs = 0;
            self.count_failure("otlp-logs");
        }

        outcome
    }

    /// Force-flushes and shuts down every exporter.
    ///
    /// This is the shutdown/flush half of the 17.17 application boundary, and
    /// the drain a coordinated shutdown calls.
    pub fn shutdown(&self) -> Result<(), OTelSdkError> {
        let logs = self.logs.shutdown();
        let metrics = self.metrics.shutdown();
        logs.and(metrics)
    }

    /// Maps bridge span records into the SDK's own span batch.
    ///
    /// Public because it is the seam the acceptance walk asserts on: span
    /// kind, status, events, links, and the pinned instrumentation scope are
    /// each things 17.17 requires the adapter to preserve, and each is a field
    /// of the value this returns.
    #[must_use]
    pub fn span_batch(&self, spans: &[AgentOtelSpanExport]) -> Vec<SpanData> {
        spans.iter().map(|span| self.span_data(span)).collect()
    }

    /// Builds the bridge export without shipping it.
    ///
    /// # Errors
    ///
    /// Returns the bridge's own validation error when the batch cannot be
    /// built — a blank endpoint, or a caller-supplied log outside its bounds.
    pub fn bridge(
        &self,
        spans: &AgentGenAiSpanExporter,
        metrics: &MetricsSnapshot,
        logs: Vec<AgentLogEvent>,
    ) -> rakka_agent_workflow::AgentOtlpResult<rakka_agent_workflow::AgentOtlpBridgeExport> {
        spans.bridge_export(
            self.config.clone(),
            self.bridge_resource.clone(),
            metrics,
            logs,
        )
    }

    fn count_failure(&self, signal: &'static str) {
        let Some(metrics) = self.metrics_recorder.as_ref() else {
            return;
        };
        rakka_agent::record_agent_domain_counter(
            metrics.as_ref(),
            rakka_agent::METRIC_AGENT_TELEMETRY_FLUSH_FAILURES,
            1,
            &[("signal", signal)],
        )
        .ok();
    }

    fn span_data(&self, span: &AgentOtelSpanExport) -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                TraceId::from_hex(&span.trace_id).unwrap_or(TraceId::INVALID),
                SpanId::from_hex(&span.span_id).unwrap_or(SpanId::INVALID),
                trace_flags(&span.trace_flags),
                false,
                trace_state(span.trace_state.as_deref()),
            ),
            parent_span_id: span
                .parent_span_id
                .as_deref()
                .and_then(|id| SpanId::from_hex(id).ok())
                .unwrap_or(SpanId::INVALID),
            span_kind: span_kind(span.kind),
            name: Cow::Owned(span.name.clone()),
            start_time: system_time(span.start_time.as_millis()),
            end_time: system_time(span.end_time.as_millis()),
            attributes: key_values(&span.attributes),
            dropped_attributes_count: 0,
            events: span_events(span),
            links: span_links(&span.links),
            status: span_status(span.status),
            instrumentation_scope: self.scope.clone(),
        }
    }

    /// Maps Rakka's already-aggregated metrics into the SDK's own batch.
    ///
    /// Public for the same reason as [`Self::span_batch`]: the unit, the
    /// bucket boundaries, and the exemplar are what 17.17 and 17.12 require to
    /// survive, and they are fields of the value this returns.
    #[must_use]
    pub fn resource_metrics(
        &self,
        export: &rakka_core::OpenTelemetryMetricsExport,
        exemplars: &ExemplarReservoir,
    ) -> ResourceMetrics {
        let now = SystemTime::now();
        let metrics = export
            .metrics()
            .iter()
            .map(|metric| self.metric(metric, exemplars, now))
            .collect();
        ResourceMetrics {
            resource: self.resource.clone(),
            scope_metrics: vec![ScopeMetrics {
                scope: self.scope.clone(),
                metrics,
            }],
        }
    }

    fn metric(
        &self,
        metric: &OpenTelemetryMetric,
        exemplars: &ExemplarReservoir,
        now: SystemTime,
    ) -> Metric {
        let exemplar = exemplars.identity_for(metric.name());
        let data: Box<dyn opentelemetry_sdk::metrics::data::Aggregation> = match metric.kind() {
            OpenTelemetryInstrumentKind::Counter => Box::new(Sum {
                data_points: metric
                    .data_points()
                    .iter()
                    .map(|point| SumDataPoint {
                        attributes: metric_key_values(point),
                        value: point.value().unwrap_or_default(),
                        exemplars: Vec::new(),
                    })
                    .collect(),
                start_time: self.started_at,
                time: now,
                temporality: Temporality::Cumulative,
                is_monotonic: true,
            }),
            OpenTelemetryInstrumentKind::Gauge => Box::new(Gauge {
                data_points: metric
                    .data_points()
                    .iter()
                    .map(|point| GaugeDataPoint {
                        attributes: metric_key_values(point),
                        value: point.value().unwrap_or_default(),
                        exemplars: Vec::new(),
                    })
                    .collect(),
                start_time: Some(self.started_at),
                time: now,
            }),
            OpenTelemetryInstrumentKind::Histogram => Box::new(Histogram {
                data_points: metric
                    .data_points()
                    .iter()
                    .map(|point| histogram_point(point, exemplar))
                    .collect(),
                start_time: self.started_at,
                time: now,
                temporality: Temporality::Cumulative,
            }),
        };
        Metric {
            name: Cow::Owned(metric.name().to_string()),
            description: Cow::Borrowed(""),
            // The catalogue's declared unit, carried through rather than
            // re-declared here. 17.17 requires the adapter to preserve
            // applicable metric unit semantics.
            unit: Cow::Owned(metric.unit().unwrap_or_default().to_string()),
            data,
        }
    }

    fn emit_logs(&self, logs: &[AgentLogEvent]) -> usize {
        let logger = self.logs.logger("rakka.agent");
        let mut emitted = 0;
        for log in logs {
            let mut record = logger.create_log_record();
            record.set_target(log.event_name.clone());
            record.set_timestamp(system_time(log.timestamp.as_millis()));
            record.set_observed_timestamp(system_time(log.observed_timestamp.as_millis()));
            record.set_severity_number(severity(log.severity_number));
            if let (Some(trace), Some(span)) = (log.trace_id.as_deref(), log.span_id.as_deref()) {
                if let (Ok(trace), Ok(span)) = (TraceId::from_hex(trace), SpanId::from_hex(span)) {
                    record.set_trace_context(
                        trace,
                        span,
                        log.trace_flags.as_deref().map(trace_flags),
                    );
                }
            }
            if let Some(body) = &log.body {
                record.set_body(AnyValue::String(body.to_string().into()));
            }
            record.add_attributes(
                log.attributes
                    .iter()
                    .map(|(key, value)| (key.clone(), AnyValue::String(value.clone().into()))),
            );
            logger.emit(record);
            emitted += 1;
        }
        emitted
    }
}

fn histogram_point(
    point: &OpenTelemetryDataPoint,
    exemplar: Option<ExemplarIdentity>,
) -> HistogramDataPoint<f64> {
    let count = point.count().unwrap_or_default();
    HistogramDataPoint {
        attributes: metric_key_values(point),
        count,
        // The catalogue's declared boundaries, carried through. A metric the
        // catalogue does not name arrives bucketless and stays that way rather
        // than being re-bucketed against a guess.
        bounds: point.bucket_boundaries().to_vec(),
        bucket_counts: point.bucket_counts().to_vec(),
        min: None,
        max: None,
        sum: point.sum().unwrap_or_default(),
        exemplars: match exemplar {
            Some(identity) if count > 0 => vec![opentelemetry_sdk::metrics::data::Exemplar {
                filtered_attributes: Vec::new(),
                time: identity.time,
                value: point.sum().unwrap_or_default(),
                span_id: identity.span_id,
                trace_id: identity.trace_id,
            }],
            _ => Vec::new(),
        },
    }
}

fn metric_key_values(point: &OpenTelemetryDataPoint) -> Vec<KeyValue> {
    point
        .attributes()
        .iter()
        .map(|attribute| KeyValue::new(attribute.key().to_string(), attribute.value().to_string()))
        .collect()
}

fn key_values(attributes: &AgentAttributes) -> Vec<KeyValue> {
    attributes
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect()
}

fn span_events(span: &AgentOtelSpanExport) -> SpanEvents {
    let mut events = SpanEvents::default();
    events.events = span
        .events
        .iter()
        .map(|event| {
            Event::new(
                Cow::Owned(event.name.clone()),
                system_time(event.time.as_millis()),
                key_values(&event.attributes),
                0,
            )
        })
        .collect();
    events
}

fn span_links(links: &[AgentSpanLink]) -> SpanLinks {
    let mut span_links = SpanLinks::default();
    span_links.links = links
        .iter()
        .map(|link| {
            Link::new(
                SpanContext::new(
                    TraceId::from_hex(&link.trace_id).unwrap_or(TraceId::INVALID),
                    SpanId::from_hex(&link.span_id).unwrap_or(SpanId::INVALID),
                    TraceFlags::default(),
                    true,
                    trace_state(link.trace_state.as_deref()),
                ),
                key_values(&link.attributes),
                0,
            )
        })
        .collect();
    span_links
}

const fn span_kind(kind: AgentOtelSpanKind) -> SpanKind {
    match kind {
        AgentOtelSpanKind::Internal => SpanKind::Internal,
        AgentOtelSpanKind::Server => SpanKind::Server,
        AgentOtelSpanKind::Client => SpanKind::Client,
        AgentOtelSpanKind::Producer => SpanKind::Producer,
        AgentOtelSpanKind::Consumer => SpanKind::Consumer,
    }
}

fn span_status(status: AgentOtelSpanStatus) -> Status {
    match status {
        AgentOtelSpanStatus::Unset => Status::Unset,
        AgentOtelSpanStatus::Ok => Status::Ok,
        AgentOtelSpanStatus::Error => Status::error(""),
    }
}

fn trace_flags(flags: &str) -> TraceFlags {
    TraceFlags::new(u8::from_str_radix(flags.trim_start_matches("0x"), 16).unwrap_or_default())
}

fn trace_state(state: Option<&str>) -> TraceState {
    state
        .and_then(|value| value.parse::<TraceState>().ok())
        .unwrap_or_default()
}

fn system_time(millis: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
}

const fn severity(number: u8) -> Severity {
    match number {
        1..=4 => Severity::Trace,
        5..=8 => Severity::Debug,
        9..=12 => Severity::Info,
        13..=16 => Severity::Warn,
        _ => Severity::Error,
    }
}

fn instrumentation_scope(scope: &AgentOtelInstrumentationScope) -> InstrumentationScope {
    let mut builder =
        InstrumentationScope::builder(scope.name.clone()).with_version(scope.version.clone());
    if let Some(schema_url) = &scope.schema_url {
        builder = builder.with_schema_url(schema_url.clone());
    }
    builder.build()
}

fn sdk_resource(resource: &AgentOtelResource) -> Resource {
    Resource::builder_empty()
        .with_attributes(key_values(&resource.attributes))
        .build()
}

/// The W3C `traceparent` field positions this module reads.
fn traceparent_field(trace_parent: &str, index: usize) -> Option<&str> {
    trace_parent.split('-').nth(index)
}

fn trace_id_bytes(trace_parent: &str) -> Option<[u8; 16]> {
    let id = TraceId::from_hex(traceparent_field(trace_parent, 1)?).ok()?;
    Some(id.to_bytes())
}

fn span_id_bytes(trace_parent: &str) -> Option<[u8; 8]> {
    let id = SpanId::from_hex(traceparent_field(trace_parent, 2)?).ok()?;
    Some(id.to_bytes())
}

fn export_timeout(config: &AgentOtlpExporterConfig) -> Duration {
    Duration::from_millis(config.timeout_ms.unwrap_or(10_000))
}

fn credential_metadata(headers: &AgentAttributes) -> tonic::metadata::MetadataMap {
    let mut metadata = tonic::metadata::MetadataMap::new();
    for (key, value) in headers {
        if let (Ok(key), Ok(value)) = (
            key.parse::<tonic::metadata::MetadataKey<_>>(),
            value.parse::<tonic::metadata::MetadataValue<_>>(),
        ) {
            metadata.insert(key, value);
        }
    }
    metadata
}

/// The `signal` label values **this binary** writes onto
/// [`rakka_agent::METRIC_AGENT_TELEMETRY_FLUSH_FAILURES`].
///
/// One per OTLP signal, so a deployment can tell a failing trace endpoint from
/// a failing metric one — which is the whole operational point of the label.
/// Declared here rather than in `rakka-agent` because the OTLP exporter lives
/// here: a vocabulary a crate lists but never writes is a promise nothing
/// keeps, and slice 6.3a's own follow-up pass was needed because three
/// attributes had been declared and allowlisted while nothing wrote them.
pub const AGENT_OTLP_EXPORT_SIGNALS: &[&str] = &["otlp-logs", "otlp-metrics", "otlp-traces"];

/// The exporter configuration the walk and its tests share.
///
/// Credentials arrive as an OTLP header — the one place
/// [17.14](../../../docs/plans/rakka-agent/spec.md) allows authentication
/// material to travel — and `AgentOtlpExporterConfig` already models exactly
/// that, so the deployment's own configuration type is what configures the
/// real exporter rather than a second one invented here.
#[must_use]
pub fn exporter_config(endpoint: &str, credential: &str) -> AgentOtlpExporterConfig {
    let mut headers = AgentAttributes::new();
    headers.insert("authorization".to_string(), format!("Bearer {credential}"));
    AgentOtlpExporterConfig {
        headers,
        timeout_ms: Some(5_000),
        ..AgentOtlpExporterConfig::grpc(endpoint)
    }
}

/// The OTLP resource this binary declares for its exports.
#[must_use]
pub fn export_resource() -> AgentOtelResource {
    AgentOtelResource::new("rakka-agent-otlp-export-acceptance")
        .service_namespace("rakka-system")
        .deployment_environment("local")
}
