//! An in-process OTLP receiver, so the export claim is about the wire.
//!
//! [Specification 17.17](../../../docs/plans/rakka-agent/spec.md) asks a
//! production deployment to export over OTLP to an OpenTelemetry Collector,
//! and the temptation when proving that is to assert on the batch the mapping
//! built and stop there. That proves the mapping and nothing about the
//! export — a serialization that OTLP rejects, an endpoint the exporter never
//! reaches, a signal wired to the wrong service would all pass.
//!
//! So this is a real gRPC server speaking the real OTLP service definitions,
//! bound to an ephemeral port. The exporter under test connects to it over a
//! socket and the assertions read the **decoded protobuf the server received**.
//! It is not a Collector — it applies no processor, and the Collector's own
//! configuration is contract-tested separately against the manifests — but it
//! is the OTLP boundary, and it needs no container, so the claim is never
//! gate-only.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::logs::v1::ResourceLogs;
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::trace::v1::ResourceSpans;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

/// Everything an in-process receiver has been handed, as decoded OTLP.
#[derive(Debug, Default)]
pub struct ReceivedSignals {
    traces: Mutex<Vec<ResourceSpans>>,
    metrics: Mutex<Vec<ResourceMetrics>>,
    logs: Mutex<Vec<ResourceLogs>>,
}

impl ReceivedSignals {
    /// Every `ResourceSpans` message received, in arrival order.
    #[must_use]
    pub fn traces(&self) -> Vec<ResourceSpans> {
        self.traces
            .lock()
            .map(|held| held.clone())
            .unwrap_or_default()
    }

    /// Every `ResourceMetrics` message received, in arrival order.
    #[must_use]
    pub fn metrics(&self) -> Vec<ResourceMetrics> {
        self.metrics
            .lock()
            .map(|held| held.clone())
            .unwrap_or_default()
    }

    /// Every `ResourceLogs` message received, in arrival order.
    #[must_use]
    pub fn logs(&self) -> Vec<ResourceLogs> {
        self.logs
            .lock()
            .map(|held| held.clone())
            .unwrap_or_default()
    }
}

/// The receiver's three OTLP services over one shared record.
#[derive(Debug, Clone)]
struct Receiver {
    received: Arc<ReceivedSignals>,
}

#[tonic::async_trait]
impl TraceService for Receiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        if let Ok(mut traces) = self.received.traces.lock() {
            traces.extend(request.into_inner().resource_spans);
        }
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for Receiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        if let Ok(mut metrics) = self.received.metrics.lock() {
            metrics.extend(request.into_inner().resource_metrics);
        }
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl LogsService for Receiver {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        if let Ok(mut logs) = self.received.logs.lock() {
            logs.extend(request.into_inner().resource_logs);
        }
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

/// A running in-process OTLP receiver on an ephemeral port.
#[derive(Debug)]
pub struct InProcessCollector {
    endpoint: String,
    received: Arc<ReceivedSignals>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl InProcessCollector {
    /// Binds an ephemeral port and serves all three OTLP services.
    ///
    /// # Panics
    ///
    /// Panics if the loopback port cannot be bound, which would make every
    /// export assertion below vacuous.
    pub async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind::<SocketAddr>(
            "127.0.0.1:0".parse().expect("the loopback address parses"),
        )
        .await
        .expect("an ephemeral loopback port binds");
        let address = listener
            .local_addr()
            .expect("the bound listener reports its address");
        let received = Arc::new(ReceivedSignals::default());
        let receiver = Receiver {
            received: received.clone(),
        };
        let (shutdown, stopped) = oneshot::channel();
        tokio::spawn(async move {
            let _served = tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(receiver.clone()))
                .add_service(MetricsServiceServer::new(receiver.clone()))
                .add_service(LogsServiceServer::new(receiver))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _stopped = stopped.await;
                    },
                )
                .await;
        });
        Self {
            endpoint: format!("http://{address}"),
            received,
            shutdown: Some(shutdown),
        }
    }

    /// The OTLP gRPC endpoint an exporter should be pointed at.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// What the receiver has been handed so far.
    #[must_use]
    pub fn received(&self) -> Arc<ReceivedSignals> {
        self.received.clone()
    }

    /// Stops serving.
    pub fn stop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _signalled = shutdown.send(());
        }
    }
}

impl Drop for InProcessCollector {
    fn drop(&mut self) {
        self.stop();
    }
}
