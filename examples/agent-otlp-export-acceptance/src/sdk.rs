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

        let mut spans = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(config.traces_endpoint.as_ref().unwrap_or(&config.endpoint))
            .with_metadata(credential_metadata(&config.headers))
            .with_timeout(export_timeout(config))
            .build()
            .map_err(|error| format!("span exporter: {error}"))?;
        // The span exporter is the one of the three that takes its resource
        // through a setter rather than through a builder or a provider, and it
        // is silent about not having one: `TonicTracesClient::new` starts the
        // field at `Resource::default()` and `export()` groups every batch
        // against whatever it holds, for the exporter's life. Miss this call
        // and metrics and logs carry the deployment's identity while every
        // span lands under `unknown_service` — unjoinable to the metric whose
        // exemplar points at it, which is the one link 17.12 exists for.
        spans.set_resource(&sdk_resource);
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
            Err(_) => {
                // Every signal is lost here, so every signal is counted here.
                // `ship` pairs each `failed_signals += 1` with a
                // `count_failure`; this arm used to raise the number and count
                // none of them — and it is the *worst* failure of the three,
                // not a lesser one. `bridge` fails on a blank endpoint or on a
                // caller-supplied log outside its bounds, so a deployment that
                // has misconfigured its endpoint takes this arm on every
                // periodic flush: spans, metrics and logs all dropped, for as
                // long as the misconfiguration lasts, while
                // `rakka.agent.telemetry.flush.failures` reads exactly zero.
                for signal in AGENT_OTLP_EXPORT_SIGNALS.iter().copied() {
                    self.count_failure(signal);
                }
                AgentExportOutcome {
                    failed_signals: AGENT_OTLP_EXPORT_SIGNALS.len(),
                    ..AgentExportOutcome::default()
                }
            }
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
        if outcome.logs > 0 && self.force_flush_logs().await.is_err() {
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
    ///
    /// Async because the log half is a blocking wait that must not run on the
    /// caller's runtime thread — `off_runtime` in this module says why.
    pub async fn shutdown(&self) -> Result<(), OTelSdkError> {
        let provider = self.logs.clone();
        let logs = off_runtime(move || provider.shutdown()).await;
        // The metric half needs no blocking thread: `TonicMetricsClient`'s
        // shutdown takes its lock and drops the transport client, with nothing
        // in flight to wait on.
        let metrics = self.metrics.shutdown();
        logs.and(metrics)
    }

    /// Force-flushes the log pipeline without parking the async runtime.
    async fn force_flush_logs(&self) -> Result<(), OTelSdkError> {
        let provider = self.logs.clone();
        off_runtime(move || provider.force_flush()).await
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
                    .map(|point| histogram_point(point, exemplar, (self.started_at, now)))
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

    /// Mints one `SdkLogRecord` per durable log event, on the pinned scope.
    ///
    /// Two of the field choices here are not interchangeable with the obvious
    /// alternative, and both were wrong before:
    ///
    /// * **The scope, not a name.** `SdkLoggerProvider::logger(name)` builds a
    ///   scope out of that name alone; 17.17 asks the adapter to preserve the
    ///   pinned instrumentation scope on each signal, not on two of three.
    ///   How much of it survives is the pinned transform's call rather than
    ///   this mapping's: `group_logs_by_resource_and_scope` rebuilds every
    ///   block through the `Some(target)` arm of
    ///   `InstrumentationScope::from`, which hardcodes an empty version and no
    ///   attributes, so only the name reaches a log record in 0.29.
    ///   `tests/exporter_failure.rs` pins that boundary from the wire, so a
    ///   later SDK that widens it is noticed rather than assumed.
    /// * **`event_name`, not `target`.** OTLP's transform reads a record's
    ///   `target` as the **`ScopeLogs` grouping key** and its `event_name` as
    ///   the event's name. Writing the event name into `target` therefore
    ///   split one batch into one scope block per distinct event, each block
    ///   named after an event and carrying the pinned scope nowhere, while the
    ///   field that is supposed to hold the name exported empty.
    fn emit_logs(&self, logs: &[AgentLogEvent]) -> usize {
        let logger = self.logs.logger_with_scope(self.scope.clone());
        let mut emitted = 0;
        for log in logs {
            let mut record = logger.create_log_record();
            if let Some(event_name) = interned_event_name(&log.event_name) {
                record.set_event_name(event_name);
            }
            record.set_timestamp(system_time(log.timestamp.as_millis()));
            record.set_observed_timestamp(system_time(log.observed_timestamp.as_millis()));
            if let Some(severity) = severity(log.severity_number) {
                record.set_severity_number(severity);
                // The band's own short name, so text and number on the wire
                // can never disagree. The durable record carries a
                // `severity_text` of its own, but it is a `String` and this
                // setter takes `&'static str`; deriving the text from the
                // number that is actually being exported is both the way
                // through that boundary and the stronger guarantee.
                record.set_severity_text(severity.name());
            }
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
    window: (SystemTime, SystemTime),
) -> HistogramDataPoint<f64> {
    let count = point.count().unwrap_or_default();
    let sum = point.sum().unwrap_or_default();
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
        sum,
        exemplars: match exemplar {
            Some(identity) if count > 0 => vec![opentelemetry_sdk::metrics::data::Exemplar {
                filtered_attributes: Vec::new(),
                time: exemplar_time(identity.time, window),
                // The mean, not the running total. `sum` is cumulative: with
                // two 60 ms turns recorded it claimed a 120 ms sample from a
                // distribution whose largest observation was 60, and after ten
                // it sits past the `+Inf` edge of the very histogram it says
                // it was drawn from. Rakka's metrics reach this boundary
                // already aggregated, so no individual measurement survives to
                // be quoted — and of what does survive, the mean is the one
                // value guaranteed to lie inside the distribution being
                // sampled. That is a representative value, which is what
                // `docs/rakka-agent-telemetry-validation-matrix.md` already
                // records this exemplar as.
                #[allow(clippy::cast_precision_loss)]
                value: sum / count as f64,
                span_id: identity.span_id,
                trace_id: identity.trace_id,
            }],
            _ => Vec::new(),
        },
    }
}

/// Places an exemplar inside the collection window of the point it decorates.
///
/// The identity's time is the producing segment's end, taken from the agent
/// domain's [`AgentTimestampMillis`] — a wall clock in a deployment, but a
/// counter in a deterministic harness, and this walk's starts at 1. That dated
/// every exported exemplar to 1970 while its data point's window opened at
/// `SystemTime::now()`: an exemplar 56 years before its own `start_time`.
///
/// An exemplar outside its point's window is not a slightly wrong exemplar.
/// Backends drop it, and the trace link the exemplar exists to carry goes with
/// it. So a time the window cannot contain collapses to the window's end —
/// the nearest instant that keeps the link — rather than being shipped as a
/// timestamp no collector will keep.
fn exemplar_time(at: SystemTime, (start, end): (SystemTime, SystemTime)) -> SystemTime {
    if at < start || at > end {
        end
    } else {
        at
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

/// The OTLP severity band a durable severity number names.
///
/// All twenty-four bands, one to one, and `None` for OTLP's UNSPECIFIED.
///
/// The range-and-catch-all this replaces was wrong at both ends. It collapsed
/// everything from 17 upwards onto `ERROR`, so `AgentLogSeverity::Fatal` —
/// severity number 21, and a variant `opentelemetry::logs::Severity` has —
/// left as `SEVERITY_NUMBER_ERROR` and no alert keyed on `>= FATAL` could ever
/// fire for a Rakka fatal. At the other end it mapped 0, which
/// `validate_agent_log_event` admits, onto `ERROR` too: an error that never
/// happened. A number outside the data model's range is not a band, and the
/// record leaves the field unset rather than claim one its producer did not.
const fn severity(number: u8) -> Option<Severity> {
    match number {
        1 => Some(Severity::Trace),
        2 => Some(Severity::Trace2),
        3 => Some(Severity::Trace3),
        4 => Some(Severity::Trace4),
        5 => Some(Severity::Debug),
        6 => Some(Severity::Debug2),
        7 => Some(Severity::Debug3),
        8 => Some(Severity::Debug4),
        9 => Some(Severity::Info),
        10 => Some(Severity::Info2),
        11 => Some(Severity::Info3),
        12 => Some(Severity::Info4),
        13 => Some(Severity::Warn),
        14 => Some(Severity::Warn2),
        15 => Some(Severity::Warn3),
        16 => Some(Severity::Warn4),
        17 => Some(Severity::Error),
        18 => Some(Severity::Error2),
        19 => Some(Severity::Error3),
        20 => Some(Severity::Error4),
        21 => Some(Severity::Fatal),
        22 => Some(Severity::Fatal2),
        23 => Some(Severity::Fatal3),
        24 => Some(Severity::Fatal4),
        _ => None,
    }
}

/// How many distinct log event names this binary will intern.
///
/// An event name is an event *class*, so the live set is small and closed in
/// practice; the cap is what makes that a property rather than an assumption.
const EVENT_NAME_CAPACITY: usize = 128;

/// Interns a durable event name so it can be set on an `SdkLogRecord`.
///
/// `LogRecord::set_event_name` takes `&'static str` and `SdkLogRecord` stores
/// it as one, while [`AgentLogEvent::event_name`] is a runtime `String`: the
/// name has to outlive the record to reach the wire at all. Interning is the
/// only way across that boundary that does not copy per record, and leaking is
/// what interning is — so it is bounded, and the bound is the point. Past
/// [`EVENT_NAME_CAPACITY`] distinct names the record ships without one rather
/// than growing the process's memory with its traffic, which is the same rule
/// every other bounded structure in this pipeline follows.
fn interned_event_name(name: &str) -> Option<&'static str> {
    static NAMES: OnceLock<Mutex<BTreeMap<String, &'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()?;
    if let Some(interned) = names.get(name) {
        return Some(interned);
    }
    if names.len() >= EVENT_NAME_CAPACITY {
        return None;
    }
    let interned: &'static str = Box::leak(name.to_owned().into_boxed_str());
    names.insert(name.to_owned(), interned);
    Some(interned)
}

/// Runs one blocking SDK call on a blocking thread instead of the caller's.
///
/// **Not a nicety — the alternative deadlocks until it times out.**
/// `SdkLoggerProvider`'s force-flush and shutdown are synchronous:
/// `BatchLogProcessor` hands the batch to its own worker thread and then waits
/// on a `recv_timeout`, and the export that worker runs is
/// `futures_executor::block_on(exporter.export(..))` over a tonic `Channel`
/// whose `Buffer` service task was `tokio::spawn`ed when the exporter was
/// built. Called straight from an `async fn`, either one parks the very
/// runtime thread that has to poll that task, so the export can never complete
/// and the wait always runs to its timeout — five seconds a call, against a
/// Collector that is answering normally, on any current-thread runtime.
/// Measured before this existed: one log record turned a 0.09 s test into a
/// 10.10 s one, and reported the healthy export as a failure.
async fn off_runtime<F>(work: F) -> Result<(), OTelSdkError>
where
    F: FnOnce() -> Result<(), OTelSdkError> + Send + 'static,
{
    match tokio::task::spawn_blocking(work).await {
        Ok(result) => result,
        Err(error) => Err(OTelSdkError::InternalFailure(format!(
            "the blocking export task did not complete: {error}"
        ))),
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
