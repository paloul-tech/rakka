//! OTLP and OpenTelemetry Collector bridge helpers for agent workflows.
//!
//! Rakka keeps OpenTelemetry SDK ownership at the application boundary. This
//! module provides resource helpers, OTLP exporter configuration metadata, and
//! a serializable bridge envelope that applications can map into their chosen
//! SDK or direct OTLP exporter.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use rakka_core::{
    export_open_telemetry_metrics, MetricAttribute, MetricsSnapshot, OpenTelemetryMetricsExport,
};
use serde::{Deserialize, Serialize};

use crate::{
    validate_agent_log_event, AgentAttributes, AgentAuditError, AgentLogEvent,
    AgentRedactionPolicy, AgentSpanLink, AgentTelemetryContext, AgentTimestampMillis,
    AgentTraceContext, AgentTraceError,
};

/// OTLP gRPC default endpoint.
pub const DEFAULT_AGENT_OTLP_GRPC_ENDPOINT: &str = "http://localhost:4317";

/// OTLP HTTP default endpoint.
pub const DEFAULT_AGENT_OTLP_HTTP_ENDPOINT: &str = "http://localhost:4318";

/// Environment variable for the base OTLP endpoint.
pub const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Environment variable for the trace-specific OTLP endpoint.
pub const OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";

/// Environment variable for the metric-specific OTLP endpoint.
pub const OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";

/// Environment variable for the log-specific OTLP endpoint.
pub const OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

/// Environment variable for the shared OTLP protocol.
pub const OTEL_EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";

/// Environment variable for shared OTLP headers.
pub const OTEL_EXPORTER_OTLP_HEADERS: &str = "OTEL_EXPORTER_OTLP_HEADERS";

/// Environment variable for the shared OTLP timeout in milliseconds.
pub const OTEL_EXPORTER_OTLP_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_TIMEOUT";

/// Resource attribute key for service name.
pub const OTEL_RESOURCE_SERVICE_NAME: &str = "service.name";

/// Resource attribute key for service namespace.
pub const OTEL_RESOURCE_SERVICE_NAMESPACE: &str = "service.namespace";

/// Resource attribute key for service version.
pub const OTEL_RESOURCE_SERVICE_VERSION: &str = "service.version";

/// Resource attribute key for service instance id.
pub const OTEL_RESOURCE_SERVICE_INSTANCE_ID: &str = "service.instance.id";

/// Resource attribute key for deployment environment name.
pub const OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME: &str = "deployment.environment.name";

/// Resource attribute key for Kubernetes namespace name.
pub const OTEL_RESOURCE_K8S_NAMESPACE_NAME: &str = "k8s.namespace.name";

/// Resource attribute key for Kubernetes pod name.
pub const OTEL_RESOURCE_K8S_POD_NAME: &str = "k8s.pod.name";

/// Resource attribute key for Kubernetes pod UID.
pub const OTEL_RESOURCE_K8S_POD_UID: &str = "k8s.pod.uid";

/// Resource attribute key for Kubernetes node name.
pub const OTEL_RESOURCE_K8S_NODE_NAME: &str = "k8s.node.name";

/// Resource attribute key for Kubernetes deployment name.
pub const OTEL_RESOURCE_K8S_DEPLOYMENT_NAME: &str = "k8s.deployment.name";

/// Resource attribute key for container name.
pub const OTEL_RESOURCE_CONTAINER_NAME: &str = "container.name";

/// Resource attribute key for the Rakka node id.
pub const OTEL_RESOURCE_RAKKA_NODE_ID: &str = "rakka.node.id";

/// Shared result type for agent workflow OTLP bridge helpers.
pub type AgentOtlpResult<T> = Result<T, AgentOtlpError>;

/// Boxed future returned by deterministic OTLP bridge receivers.
pub type AgentOtlpReceiverFuture<'a, T> =
    Pin<Box<dyn Future<Output = AgentOtlpResult<T>> + Send + 'a>>;

/// OTLP bridge errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentOtlpError {
    /// Resource attributes failed validation.
    InvalidResource {
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Exporter configuration failed validation.
    InvalidExporter {
        /// Invalid field.
        field: &'static str,
        /// Stable validation reason.
        reason: &'static str,
    },
    /// Trace context on a span failed validation.
    TraceContext {
        /// Trace-context failure.
        error: AgentTraceError,
    },
    /// Log event validation failed.
    LogEvent {
        /// Log event validation failure.
        error: AgentAuditError,
    },
    /// Receiver failed to accept an export.
    Receiver {
        /// Stable bounded failure message.
        message: String,
    },
}

impl AgentOtlpError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidResource { .. } => "invalid-otel-resource",
            Self::InvalidExporter { .. } => "invalid-otlp-exporter",
            Self::TraceContext { .. } => "otlp-trace-context",
            Self::LogEvent { .. } => "otlp-log-event",
            Self::Receiver { .. } => "otlp-receiver",
        }
    }
}

impl Display for AgentOtlpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidResource { field, reason } => {
                write!(f, "invalid OpenTelemetry resource field {field}: {reason}")
            }
            Self::InvalidExporter { field, reason } => {
                write!(f, "invalid OTLP exporter field {field}: {reason}")
            }
            Self::TraceContext { error } => Display::fmt(error, f),
            Self::LogEvent { error } => Display::fmt(error, f),
            Self::Receiver { message } => write!(f, "OTLP bridge receiver failed: {message}"),
        }
    }
}

impl Error for AgentOtlpError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::TraceContext { error } => Some(error),
            Self::LogEvent { error } => Some(error),
            Self::InvalidResource { .. } | Self::InvalidExporter { .. } | Self::Receiver { .. } => {
                None
            }
        }
    }
}

impl From<AgentTraceError> for AgentOtlpError {
    fn from(error: AgentTraceError) -> Self {
        Self::TraceContext { error }
    }
}

impl From<AgentAuditError> for AgentOtlpError {
    fn from(error: AgentAuditError) -> Self {
        Self::LogEvent { error }
    }
}

/// OTLP wire protocol selected by an application-owned exporter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentOtlpProtocol {
    /// OTLP over gRPC.
    Grpc,
    /// OTLP over HTTP/protobuf.
    HttpProtobuf,
}

impl AgentOtlpProtocol {
    /// Stable OpenTelemetry environment variable value for this protocol.
    #[must_use]
    pub const fn as_env_value(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::HttpProtobuf => "http/protobuf",
        }
    }

    /// Default endpoint for this protocol.
    #[must_use]
    pub const fn default_endpoint(self) -> &'static str {
        match self {
            Self::Grpc => DEFAULT_AGENT_OTLP_GRPC_ENDPOINT,
            Self::HttpProtobuf => DEFAULT_AGENT_OTLP_HTTP_ENDPOINT,
        }
    }
}

/// OTLP signal type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentOtlpSignal {
    /// Traces signal.
    Traces,
    /// Metrics signal.
    Metrics,
    /// Logs signal.
    Logs,
}

/// Application-owned OTLP exporter configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOtlpExporterConfig {
    /// Base endpoint used when a signal-specific endpoint is not supplied.
    pub endpoint: String,
    /// Trace-specific endpoint override.
    pub traces_endpoint: Option<String>,
    /// Metric-specific endpoint override.
    pub metrics_endpoint: Option<String>,
    /// Log-specific endpoint override.
    pub logs_endpoint: Option<String>,
    /// OTLP wire protocol.
    pub protocol: AgentOtlpProtocol,
    /// Export timeout budget in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Headers to attach to outbound OTLP requests.
    pub headers: AgentAttributes,
}

impl AgentOtlpExporterConfig {
    /// Creates a gRPC OTLP exporter configuration.
    #[must_use]
    pub fn grpc(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            traces_endpoint: None,
            metrics_endpoint: None,
            logs_endpoint: None,
            protocol: AgentOtlpProtocol::Grpc,
            timeout_ms: None,
            headers: AgentAttributes::new(),
        }
    }

    /// Creates an HTTP/protobuf OTLP exporter configuration.
    #[must_use]
    pub fn http_protobuf(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            traces_endpoint: None,
            metrics_endpoint: None,
            logs_endpoint: None,
            protocol: AgentOtlpProtocol::HttpProtobuf,
            timeout_ms: None,
            headers: AgentAttributes::new(),
        }
    }

    /// Builds exporter configuration from OpenTelemetry-style environment values.
    pub fn from_env_map(env: &AgentAttributes) -> AgentOtlpResult<Self> {
        let protocol = parse_protocol(env.get(OTEL_EXPORTER_OTLP_PROTOCOL).map(String::as_str))?;
        let mut config = match protocol {
            AgentOtlpProtocol::Grpc => Self::grpc(
                env.get(OTEL_EXPORTER_OTLP_ENDPOINT)
                    .map(String::as_str)
                    .unwrap_or_else(|| protocol.default_endpoint()),
            ),
            AgentOtlpProtocol::HttpProtobuf => Self::http_protobuf(
                env.get(OTEL_EXPORTER_OTLP_ENDPOINT)
                    .map(String::as_str)
                    .unwrap_or_else(|| protocol.default_endpoint()),
            ),
        };
        config.traces_endpoint = env.get(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT).cloned();
        config.metrics_endpoint = env.get(OTEL_EXPORTER_OTLP_METRICS_ENDPOINT).cloned();
        config.logs_endpoint = env.get(OTEL_EXPORTER_OTLP_LOGS_ENDPOINT).cloned();
        config.timeout_ms = env
            .get(OTEL_EXPORTER_OTLP_TIMEOUT)
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| AgentOtlpError::InvalidExporter {
                        field: OTEL_EXPORTER_OTLP_TIMEOUT,
                        reason: "must be an unsigned integer number of milliseconds",
                    })
            })
            .transpose()?;
        if let Some(headers) = env.get(OTEL_EXPORTER_OTLP_HEADERS) {
            config.headers = parse_otlp_headers(headers)?;
        }
        config.validate()?;
        Ok(config)
    }

    /// Sets a trace-specific endpoint override.
    #[must_use]
    pub fn traces_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.traces_endpoint = Some(endpoint.into());
        self
    }

    /// Sets a metric-specific endpoint override.
    #[must_use]
    pub fn metrics_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.metrics_endpoint = Some(endpoint.into());
        self
    }

    /// Sets a log-specific endpoint override.
    #[must_use]
    pub fn logs_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.logs_endpoint = Some(endpoint.into());
        self
    }

    /// Sets an export timeout in milliseconds.
    #[must_use]
    pub const fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    /// Adds an OTLP request header.
    #[must_use]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Returns the endpoint for one signal.
    #[must_use]
    pub fn endpoint_for_signal(&self, signal: AgentOtlpSignal) -> &str {
        match signal {
            AgentOtlpSignal::Traces => self.traces_endpoint.as_deref().unwrap_or(&self.endpoint),
            AgentOtlpSignal::Metrics => self.metrics_endpoint.as_deref().unwrap_or(&self.endpoint),
            AgentOtlpSignal::Logs => self.logs_endpoint.as_deref().unwrap_or(&self.endpoint),
        }
    }

    /// Validates the exporter configuration.
    pub fn validate(&self) -> AgentOtlpResult<()> {
        validate_endpoint("endpoint", &self.endpoint)?;
        if let Some(endpoint) = &self.traces_endpoint {
            validate_endpoint("traces_endpoint", endpoint)?;
        }
        if let Some(endpoint) = &self.metrics_endpoint {
            validate_endpoint("metrics_endpoint", endpoint)?;
        }
        if let Some(endpoint) = &self.logs_endpoint {
            validate_endpoint("logs_endpoint", endpoint)?;
        }
        for (key, value) in &self.headers {
            if is_blank(key) {
                return Err(AgentOtlpError::InvalidExporter {
                    field: "headers.key",
                    reason: "required",
                });
            }
            if value.contains('\n') || value.contains('\r') {
                return Err(AgentOtlpError::InvalidExporter {
                    field: "headers.value",
                    reason: "must be a single-line header value",
                });
            }
        }
        Ok(())
    }
}

impl Default for AgentOtlpExporterConfig {
    fn default() -> Self {
        Self::grpc(DEFAULT_AGENT_OTLP_GRPC_ENDPOINT)
    }
}

/// OpenTelemetry resource attributes for an agent workflow process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOtelResource {
    /// OpenTelemetry resource attributes.
    pub attributes: AgentAttributes,
}

impl AgentOtelResource {
    /// Creates a resource with a required service name.
    #[must_use]
    pub fn new(service_name: impl Into<String>) -> Self {
        let mut attributes = AgentAttributes::new();
        attributes.insert(OTEL_RESOURCE_SERVICE_NAME.to_string(), service_name.into());
        Self { attributes }
    }

    /// Sets service namespace.
    #[must_use]
    pub fn service_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.attributes.insert(
            OTEL_RESOURCE_SERVICE_NAMESPACE.to_string(),
            namespace.into(),
        );
        self
    }

    /// Sets service version.
    #[must_use]
    pub fn service_version(mut self, version: impl Into<String>) -> Self {
        self.attributes
            .insert(OTEL_RESOURCE_SERVICE_VERSION.to_string(), version.into());
        self
    }

    /// Sets service instance id.
    #[must_use]
    pub fn service_instance_id(mut self, instance_id: impl Into<String>) -> Self {
        self.attributes.insert(
            OTEL_RESOURCE_SERVICE_INSTANCE_ID.to_string(),
            instance_id.into(),
        );
        self
    }

    /// Sets deployment environment name.
    #[must_use]
    pub fn deployment_environment(mut self, environment: impl Into<String>) -> Self {
        self.attributes.insert(
            OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT_NAME.to_string(),
            environment.into(),
        );
        self
    }

    /// Sets Kubernetes namespace name.
    #[must_use]
    pub fn k8s_namespace_name(mut self, namespace: impl Into<String>) -> Self {
        self.attributes.insert(
            OTEL_RESOURCE_K8S_NAMESPACE_NAME.to_string(),
            namespace.into(),
        );
        self
    }

    /// Sets Kubernetes pod name.
    #[must_use]
    pub fn k8s_pod_name(mut self, pod_name: impl Into<String>) -> Self {
        self.attributes
            .insert(OTEL_RESOURCE_K8S_POD_NAME.to_string(), pod_name.into());
        self
    }

    /// Sets Kubernetes pod UID.
    #[must_use]
    pub fn k8s_pod_uid(mut self, pod_uid: impl Into<String>) -> Self {
        self.attributes
            .insert(OTEL_RESOURCE_K8S_POD_UID.to_string(), pod_uid.into());
        self
    }

    /// Sets Kubernetes node name.
    #[must_use]
    pub fn k8s_node_name(mut self, node_name: impl Into<String>) -> Self {
        self.attributes
            .insert(OTEL_RESOURCE_K8S_NODE_NAME.to_string(), node_name.into());
        self
    }

    /// Sets Kubernetes deployment name.
    #[must_use]
    pub fn k8s_deployment_name(mut self, deployment_name: impl Into<String>) -> Self {
        self.attributes.insert(
            OTEL_RESOURCE_K8S_DEPLOYMENT_NAME.to_string(),
            deployment_name.into(),
        );
        self
    }

    /// Sets container name.
    #[must_use]
    pub fn container_name(mut self, container_name: impl Into<String>) -> Self {
        self.attributes.insert(
            OTEL_RESOURCE_CONTAINER_NAME.to_string(),
            container_name.into(),
        );
        self
    }

    /// Sets Rakka node id.
    #[must_use]
    pub fn rakka_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.attributes
            .insert(OTEL_RESOURCE_RAKKA_NODE_ID.to_string(), node_id.into());
        self
    }

    /// Adds a custom resource attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Returns resource attributes as core metric attributes.
    #[must_use]
    pub fn to_metric_attributes(&self) -> Vec<MetricAttribute> {
        self.attributes
            .iter()
            .map(|(key, value)| MetricAttribute::new(key.clone(), value.clone()))
            .collect()
    }

    /// Validates resource attributes.
    pub fn validate(&self) -> AgentOtlpResult<()> {
        match self.attributes.get(OTEL_RESOURCE_SERVICE_NAME) {
            Some(value) if !is_blank(value) => {}
            _ => {
                return Err(AgentOtlpError::InvalidResource {
                    field: OTEL_RESOURCE_SERVICE_NAME,
                    reason: "required",
                });
            }
        }
        for (key, value) in &self.attributes {
            if is_blank(key) {
                return Err(AgentOtlpError::InvalidResource {
                    field: "attributes.key",
                    reason: "required",
                });
            }
            if value.contains('\n') || value.contains('\r') {
                return Err(AgentOtlpError::InvalidResource {
                    field: "attributes.value",
                    reason: "must be a single-line value",
                });
            }
        }
        Ok(())
    }
}

/// OpenTelemetry-oriented span bridge record for agent workflow traces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOtelSpanExport {
    /// Span name.
    pub name: String,
    /// Trace id.
    pub trace_id: String,
    /// Span id.
    pub span_id: String,
    /// Parent span id, when known.
    pub parent_span_id: Option<String>,
    /// W3C trace flags.
    pub trace_flags: String,
    /// W3C tracestate value.
    pub trace_state: Option<String>,
    /// Span start timestamp.
    pub start_time: AgentTimestampMillis,
    /// Span end timestamp.
    pub end_time: AgentTimestampMillis,
    /// Span links.
    pub links: Vec<AgentSpanLink>,
    /// Span attributes.
    pub attributes: AgentAttributes,
}

impl AgentOtelSpanExport {
    /// Builds a span bridge record from a durable telemetry context.
    pub fn from_telemetry_context(
        name: impl Into<String>,
        start_time: AgentTimestampMillis,
        end_time: AgentTimestampMillis,
        telemetry_context: &AgentTelemetryContext,
    ) -> AgentOtlpResult<Self> {
        let trace_parent =
            telemetry_context
                .trace_parent
                .as_deref()
                .ok_or(AgentOtlpError::TraceContext {
                    error: AgentTraceError::MissingTraceParent,
                })?;
        let trace_context = AgentTraceContext::from_trace_parent(
            trace_parent,
            telemetry_context.trace_state.as_deref(),
        )?;
        Ok(Self {
            name: name.into(),
            trace_id: trace_context.trace_id,
            span_id: trace_context.span_id,
            parent_span_id: None,
            trace_flags: trace_context.trace_flags,
            trace_state: trace_context.trace_state,
            start_time,
            end_time,
            links: telemetry_context.span_links.clone(),
            attributes: telemetry_context.baggage.clone(),
        })
    }

    /// Sets parent span id.
    #[must_use]
    pub fn parent_span_id(mut self, parent_span_id: impl Into<String>) -> Self {
        self.parent_span_id = Some(parent_span_id.into());
        self
    }

    /// Adds a span attribute.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }

    /// Validates the span export record.
    pub fn validate(&self) -> AgentOtlpResult<()> {
        if is_blank(&self.name) {
            return Err(AgentOtlpError::InvalidExporter {
                field: "span.name",
                reason: "required",
            });
        }
        AgentTraceContext::new(
            self.trace_id.clone(),
            self.span_id.clone(),
            self.trace_flags.clone(),
            self.trace_state.clone(),
        )?;
        Ok(())
    }
}

/// Serializable bridge export that can be mapped to OTLP by application code.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentOtlpBridgeExport {
    /// Exporter configuration selected by the application.
    pub exporter: AgentOtlpExporterConfig,
    /// Resource attributes shared by all signals in this export.
    pub resource: AgentOtelResource,
    /// OpenTelemetry-oriented metrics export.
    pub metrics: OpenTelemetryMetricsExport,
    /// OpenTelemetry-oriented span bridge records.
    pub spans: Vec<AgentOtelSpanExport>,
    /// OpenTelemetry-compatible structured log records.
    pub logs: Vec<AgentLogEvent>,
}

impl AgentOtlpBridgeExport {
    /// Builds an OTLP bridge export from a metrics snapshot, spans, and logs.
    pub fn from_signals(
        exporter: AgentOtlpExporterConfig,
        resource: AgentOtelResource,
        metrics_snapshot: &MetricsSnapshot,
        spans: Vec<AgentOtelSpanExport>,
        logs: Vec<AgentLogEvent>,
    ) -> AgentOtlpResult<Self> {
        exporter.validate()?;
        resource.validate()?;
        for span in &spans {
            span.validate()?;
        }
        for log in &logs {
            validate_agent_log_event(log, AgentRedactionPolicy::new())?;
        }

        let resource_pairs = resource
            .attributes
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect::<Vec<_>>();
        let metrics = export_open_telemetry_metrics(metrics_snapshot, &resource_pairs);
        Ok(Self {
            exporter,
            resource,
            metrics,
            spans,
            logs,
        })
    }
}

/// Deterministic receiver abstraction for OTLP bridge exports.
pub trait AgentOtlpBridgeReceiver {
    /// Exports one bridge payload.
    fn export_bridge<'a>(
        &'a mut self,
        export: AgentOtlpBridgeExport,
    ) -> AgentOtlpReceiverFuture<'a, ()>;
}

/// In-memory receiver for deterministic tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAgentOtlpReceiver {
    exports: Vec<AgentOtlpBridgeExport>,
}

impl InMemoryAgentOtlpReceiver {
    /// Creates an empty receiver.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            exports: Vec::new(),
        }
    }

    /// Returns captured exports.
    #[must_use]
    pub fn exports(&self) -> &[AgentOtlpBridgeExport] {
        &self.exports
    }
}

impl AgentOtlpBridgeReceiver for InMemoryAgentOtlpReceiver {
    fn export_bridge<'a>(
        &'a mut self,
        export: AgentOtlpBridgeExport,
    ) -> AgentOtlpReceiverFuture<'a, ()> {
        Box::pin(async move {
            export.exporter.validate()?;
            export.resource.validate()?;
            for span in &export.spans {
                span.validate()?;
            }
            for log in &export.logs {
                validate_agent_log_event(log, AgentRedactionPolicy::new())?;
            }
            self.exports.push(export);
            Ok(())
        })
    }
}

fn parse_protocol(value: Option<&str>) -> AgentOtlpResult<AgentOtlpProtocol> {
    match value.unwrap_or(AgentOtlpProtocol::Grpc.as_env_value()) {
        "grpc" => Ok(AgentOtlpProtocol::Grpc),
        "http/protobuf" => Ok(AgentOtlpProtocol::HttpProtobuf),
        _ => Err(AgentOtlpError::InvalidExporter {
            field: OTEL_EXPORTER_OTLP_PROTOCOL,
            reason: "must be grpc or http/protobuf",
        }),
    }
}

fn parse_otlp_headers(value: &str) -> AgentOtlpResult<AgentAttributes> {
    let mut headers = AgentAttributes::new();
    if value.trim().is_empty() {
        return Ok(headers);
    }
    for pair in value.split(',') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(AgentOtlpError::InvalidExporter {
                field: OTEL_EXPORTER_OTLP_HEADERS,
                reason: "headers must be comma-separated key=value pairs",
            });
        };
        let key = key.trim();
        let value = value.trim();
        if is_blank(key) {
            return Err(AgentOtlpError::InvalidExporter {
                field: "headers.key",
                reason: "required",
            });
        }
        headers.insert(key.to_string(), value.to_string());
    }
    Ok(headers)
}

fn validate_endpoint(field: &'static str, endpoint: &str) -> AgentOtlpResult<()> {
    if is_blank(endpoint) {
        return Err(AgentOtlpError::InvalidExporter {
            field,
            reason: "required",
        });
    }
    if endpoint.contains('\n') || endpoint.contains('\r') {
        return Err(AgentOtlpError::InvalidExporter {
            field,
            reason: "must be a single-line endpoint",
        });
    }
    Ok(())
}

fn is_blank(value: &str) -> bool {
    value.trim().is_empty()
}
