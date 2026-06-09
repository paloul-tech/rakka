//! Streaming gRPC adapters backed by Rakka bounded streams.
#![allow(clippy::result_large_err)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use futures_util::stream::{self, Stream};
use futures_util::StreamExt;
use rakka_stream::{bounded_channel, StreamSink, StreamSource};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Streaming};

use crate::{request_timeout_from_metadata, GrpcError, GrpcResult, GrpcStreamConfig};

/// Boxed tonic-compatible response stream emitted by Rakka gRPC adapters.
pub type GrpcResponseStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send + 'static>>;

/// Join handle returned by gRPC inbound stream pump tasks.
pub type GrpcStreamPump = JoinHandle<GrpcResult<usize>>;

/// Trait implemented by server-streaming handlers accepted by Rakka gRPC adapters.
pub trait GrpcServerStreamingService<Req, Resp>: Send {
    /// Future returned by the handler.
    type Future: Future<Output = GrpcResult<StreamSource<Resp>>> + Send;

    /// Calls the server-streaming handler with a decoded protobuf request.
    fn call(self, request: Req) -> Self::Future;
}

impl<Req, Resp, F, Fut> GrpcServerStreamingService<Req, Resp> for F
where
    F: FnOnce(Req) -> Fut + Send,
    Fut: Future<Output = GrpcResult<StreamSource<Resp>>> + Send,
{
    type Future = Fut;

    fn call(self, request: Req) -> Self::Future {
        self(request)
    }
}

/// Creates a tonic server-streaming response from a Rakka stream source.
#[must_use]
pub fn server_streaming_response<T>(source: StreamSource<T>) -> Response<GrpcResponseStream<T>>
where
    T: Send + 'static,
{
    Response::new(response_stream_from_source(source, None))
}

/// Calls a server-streaming service handler from a tonic generated service method.
pub async fn server_streaming_service<Req, Resp, S>(
    request: Request<Req>,
    config: GrpcStreamConfig,
    service: S,
) -> GrpcResult<Response<GrpcResponseStream<Resp>>>
where
    Resp: Send + 'static,
    S: GrpcServerStreamingService<Req, Resp>,
{
    let timeout = effective_stream_timeout(&request, config);
    let payload = request.into_inner();
    let source = run_with_timeout(service.call(payload), timeout).await?;
    Ok(server_streaming_response(source))
}

/// Bounded client-streaming request body exposed as a Rakka stream source.
#[derive(Debug)]
pub struct GrpcClientStreaming<T> {
    source: Option<StreamSource<T>>,
    pump: Option<GrpcStreamPump>,
}

impl<T> GrpcClientStreaming<T> {
    /// Creates a client-streaming adapter from a source and inbound pump.
    #[must_use]
    pub fn new(source: StreamSource<T>, pump: GrpcStreamPump) -> Self {
        Self {
            source: Some(source),
            pump: Some(pump),
        }
    }

    /// Bounded source receiving inbound client stream messages.
    #[must_use]
    pub fn source(&self) -> &StreamSource<T> {
        self.source
            .as_ref()
            .expect("gRPC client stream source was already taken")
    }

    /// Returns true when the inbound pump has finished.
    #[must_use]
    pub fn pump_is_finished(&self) -> bool {
        self.pump
            .as_ref()
            .map(JoinHandle::is_finished)
            .unwrap_or(true)
    }

    /// Cancels the inbound stream and wakes a waiting pump.
    pub fn cancel(&self, reason: impl Into<String>) -> usize {
        self.source().cancel(reason)
    }

    /// Awaits the inbound pump result.
    pub async fn join(&mut self) -> GrpcResult<usize> {
        let Some(pump) = self.pump.take() else {
            return Ok(0);
        };

        pump.await
            .map_err(|error| GrpcError::stream_pump(error.to_string()).into_status())?
    }

    /// Takes the bounded source, leaving this adapter without a source handle.
    pub fn take_source(&mut self) -> Option<StreamSource<T>> {
        self.source.take()
    }

    /// Consumes this adapter into the bounded source and pump handle.
    #[must_use]
    pub fn into_parts(mut self) -> (Option<StreamSource<T>>, Option<GrpcStreamPump>) {
        (self.source.take(), self.pump.take())
    }
}

impl<T> Drop for GrpcClientStreaming<T> {
    fn drop(&mut self) {
        let should_cancel = self.pump.as_ref().is_some_and(|pump| !pump.is_finished());
        if should_cancel {
            if let Some(source) = &self.source {
                cancel_if_active(source, "gRPC client stream adapter dropped");
            }
        }
    }
}

/// Creates a bounded stream from a tonic client-streaming request.
pub fn client_stream_from_request<T>(
    request: Request<Streaming<T>>,
    config: GrpcStreamConfig,
) -> GrpcResult<GrpcClientStreaming<T>>
where
    T: Send + 'static,
{
    let timeout = effective_stream_timeout(&request, config);
    client_stream_from_stream_with_timeout(request.into_inner(), config, timeout)
}

/// Creates a bounded stream from any tonic-compatible inbound stream.
pub fn client_stream_from_stream<S, T>(
    stream: S,
    config: GrpcStreamConfig,
) -> GrpcResult<GrpcClientStreaming<T>>
where
    S: Stream<Item = GrpcResult<T>> + Send + 'static,
    T: Send + 'static,
{
    client_stream_from_stream_with_timeout(stream, config, config.request_timeout_value())
}

/// Calls a client-streaming handler from a tonic generated service method.
pub async fn client_streaming_service<Req, Resp, H, Fut>(
    request: Request<Streaming<Req>>,
    config: GrpcStreamConfig,
    handler: H,
) -> GrpcResult<Response<Resp>>
where
    Req: Send + 'static,
    H: FnOnce(GrpcClientStreaming<Req>) -> Fut + Send,
    Fut: Future<Output = GrpcResult<Resp>> + Send,
{
    let timeout = effective_stream_timeout(&request, config);
    let inbound = client_stream_from_stream_with_timeout(request.into_inner(), config, timeout)?;
    let response = run_with_timeout(handler(inbound), timeout).await?;
    Ok(Response::new(response))
}

/// Calls a client-streaming handler from any tonic-compatible inbound stream.
pub async fn client_streaming_service_from_stream<S, Req, Resp, H, Fut>(
    stream: S,
    config: GrpcStreamConfig,
    handler: H,
) -> GrpcResult<Response<Resp>>
where
    S: Stream<Item = GrpcResult<Req>> + Send + 'static,
    Req: Send + 'static,
    H: FnOnce(GrpcClientStreaming<Req>) -> Fut + Send,
    Fut: Future<Output = GrpcResult<Resp>> + Send,
{
    let inbound = client_stream_from_stream(stream, config)?;
    let response = run_with_timeout(handler(inbound), config.request_timeout_value()).await?;
    Ok(Response::new(response))
}

/// Bidirectional gRPC bridge backed by independent inbound and outbound Rakka streams.
pub struct GrpcBidiStreaming<Req, Resp> {
    inbound: Option<StreamSource<Req>>,
    outbound: Option<StreamSink<Resp>>,
    response: Option<GrpcResponseStream<Resp>>,
    inbound_pump: Option<GrpcStreamPump>,
}

impl<Req, Resp> std::fmt::Debug for GrpcBidiStreaming<Req, Resp> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcBidiStreaming")
            .field("inbound", &self.inbound)
            .field("outbound", &self.outbound)
            .field("has_response", &self.response.is_some())
            .field(
                "inbound_pump_finished",
                &self
                    .inbound_pump
                    .as_ref()
                    .map(JoinHandle::is_finished)
                    .unwrap_or(true),
            )
            .finish()
    }
}

impl<Req, Resp> GrpcBidiStreaming<Req, Resp> {
    /// Creates a bidirectional gRPC bridge.
    #[must_use]
    pub fn new(
        inbound: StreamSource<Req>,
        outbound: StreamSink<Resp>,
        response: GrpcResponseStream<Resp>,
        inbound_pump: GrpcStreamPump,
    ) -> Self {
        Self {
            inbound: Some(inbound),
            outbound: Some(outbound),
            response: Some(response),
            inbound_pump: Some(inbound_pump),
        }
    }

    /// Bounded source receiving inbound client messages.
    #[must_use]
    pub fn inbound(&self) -> &StreamSource<Req> {
        self.inbound
            .as_ref()
            .expect("gRPC bidirectional inbound source was already taken")
    }

    /// Bounded sink used by handlers to send outbound response messages.
    #[must_use]
    pub fn outbound(&self) -> &StreamSink<Resp> {
        self.outbound
            .as_ref()
            .expect("gRPC bidirectional outbound sink was already taken")
    }

    /// Returns true when the inbound pump has finished.
    #[must_use]
    pub fn inbound_pump_is_finished(&self) -> bool {
        self.inbound_pump
            .as_ref()
            .map(JoinHandle::is_finished)
            .unwrap_or(true)
    }

    /// Takes the response stream to return from a tonic generated service method.
    pub fn take_response(&mut self) -> Option<GrpcResponseStream<Resp>> {
        self.response.take()
    }

    /// Awaits the inbound pump result.
    pub async fn join_inbound(&mut self) -> GrpcResult<usize> {
        let Some(pump) = self.inbound_pump.take() else {
            return Ok(0);
        };

        pump.await
            .map_err(|error| GrpcError::stream_pump(error.to_string()).into_status())?
    }

    /// Consumes this bridge into its Rakka endpoints, response stream, and pump handle.
    #[must_use]
    pub fn into_parts(
        mut self,
    ) -> (
        StreamSource<Req>,
        StreamSink<Resp>,
        GrpcResponseStream<Resp>,
        Option<GrpcStreamPump>,
    ) {
        (
            self.inbound
                .take()
                .expect("gRPC bidirectional inbound source was already taken"),
            self.outbound
                .take()
                .expect("gRPC bidirectional outbound sink was already taken"),
            self.response
                .take()
                .expect("gRPC bidirectional response stream was already taken"),
            self.inbound_pump.take(),
        )
    }
}

impl<Req, Resp> Drop for GrpcBidiStreaming<Req, Resp> {
    fn drop(&mut self) {
        if let Some(inbound) = &self.inbound {
            cancel_if_active(inbound, "gRPC bidirectional bridge dropped");
        }
        if let Some(outbound) = &self.outbound {
            outbound.cancel("gRPC bidirectional bridge dropped");
        }
    }
}

/// Creates a bidirectional gRPC bridge from a tonic streaming request.
pub fn bidi_stream_pair_from_request<Req, Resp>(
    request: Request<Streaming<Req>>,
    config: GrpcStreamConfig,
) -> GrpcResult<GrpcBidiStreaming<Req, Resp>>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let timeout = effective_stream_timeout(&request, config);
    bidi_stream_pair_from_stream_with_timeout(request.into_inner(), config, timeout)
}

/// Creates a bidirectional gRPC bridge from any tonic-compatible inbound stream.
pub fn bidi_stream_pair_from_stream<S, Req, Resp>(
    stream: S,
    config: GrpcStreamConfig,
) -> GrpcResult<GrpcBidiStreaming<Req, Resp>>
where
    S: Stream<Item = GrpcResult<Req>> + Send + 'static,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    bidi_stream_pair_from_stream_with_timeout(stream, config, config.request_timeout_value())
}

/// Creates a bidirectional bridge from already-created Rakka streams.
pub fn bidi_stream_pair<Req, Resp>(
    inbound: StreamSource<Req>,
    outbound: StreamSink<Resp>,
    outbound_source: StreamSource<Resp>,
    inbound_pump: GrpcStreamPump,
) -> GrpcBidiStreaming<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let response = response_stream_from_source(outbound_source, None);
    GrpcBidiStreaming::new(inbound, outbound, response, inbound_pump)
}

/// Calls a bidirectional-streaming handler from a tonic generated service method.
pub fn bidi_streaming_service<Req, Resp, H, Fut>(
    request: Request<Streaming<Req>>,
    config: GrpcStreamConfig,
    handler: H,
) -> GrpcResult<Response<GrpcResponseStream<Resp>>>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    H: FnOnce(StreamSource<Req>, StreamSink<Resp>) -> Fut + Send + 'static,
    Fut: Future<Output = GrpcResult<()>> + Send + 'static,
{
    let bridge = bidi_stream_pair_from_request(request, config)?;
    Ok(Response::new(spawn_bidi_handler(bridge, config, handler)))
}

/// Calls a bidirectional-streaming handler from any tonic-compatible inbound stream.
pub fn bidi_streaming_service_from_stream<S, Req, Resp, H, Fut>(
    stream: S,
    config: GrpcStreamConfig,
    handler: H,
) -> GrpcResult<Response<GrpcResponseStream<Resp>>>
where
    S: Stream<Item = GrpcResult<Req>> + Send + 'static,
    Req: Send + 'static,
    Resp: Send + 'static,
    H: FnOnce(StreamSource<Req>, StreamSink<Resp>) -> Fut + Send + 'static,
    Fut: Future<Output = GrpcResult<()>> + Send + 'static,
{
    let bridge = bidi_stream_pair_from_stream(stream, config)?;
    Ok(Response::new(spawn_bidi_handler(bridge, config, handler)))
}

fn client_stream_from_stream_with_timeout<S, T>(
    stream: S,
    config: GrpcStreamConfig,
    timeout: Duration,
) -> GrpcResult<GrpcClientStreaming<T>>
where
    S: Stream<Item = GrpcResult<T>> + Send + 'static,
    T: Send + 'static,
{
    let (sink, source) =
        bounded_channel(config.buffer_capacity_value()).map_err(stream_status_from_error)?;
    let pump = spawn_inbound_stream_pump(stream, sink, timeout);
    Ok(GrpcClientStreaming::new(source, pump))
}

fn bidi_stream_pair_from_stream_with_timeout<S, Req, Resp>(
    stream: S,
    config: GrpcStreamConfig,
    timeout: Duration,
) -> GrpcResult<GrpcBidiStreaming<Req, Resp>>
where
    S: Stream<Item = GrpcResult<Req>> + Send + 'static,
    Req: Send + 'static,
    Resp: Send + 'static,
{
    let (inbound_sink, inbound_source) =
        bounded_channel(config.buffer_capacity_value()).map_err(stream_status_from_error)?;
    let (outbound_sink, outbound_source) =
        bounded_channel(config.buffer_capacity_value()).map_err(stream_status_from_error)?;
    let inbound_cancel = inbound_sink.clone();
    let pump = spawn_inbound_stream_pump(stream, inbound_sink, timeout);
    let response = response_stream_from_source(
        outbound_source,
        Some(Box::new(move || {
            inbound_cancel.cancel("gRPC response stream dropped");
        })),
    );
    Ok(GrpcBidiStreaming::new(
        inbound_source,
        outbound_sink,
        response,
        pump,
    ))
}

fn spawn_bidi_handler<Req, Resp, H, Fut>(
    bridge: GrpcBidiStreaming<Req, Resp>,
    config: GrpcStreamConfig,
    handler: H,
) -> GrpcResponseStream<Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    H: FnOnce(StreamSource<Req>, StreamSink<Resp>) -> Fut + Send + 'static,
    Fut: Future<Output = GrpcResult<()>> + Send + 'static,
{
    let (inbound, outbound, response, _pump) = bridge.into_parts();
    let outbound_for_handler = outbound.clone();
    let outbound_for_completion = outbound.clone();
    tokio::spawn(async move {
        let result = run_with_timeout(
            handler(inbound, outbound_for_handler),
            config.request_timeout_value(),
        )
        .await;

        match result {
            Ok(()) => {
                match tokio::time::timeout(config.drain_timeout_value(), async {
                    outbound_for_completion.drain()
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        outbound_for_completion.cancel(error.to_string());
                    }
                    Err(_elapsed) => {
                        outbound_for_completion
                            .cancel("gRPC bidirectional handler drain timed out");
                    }
                }
            }
            Err(status) => {
                outbound_for_completion.cancel(status.message().to_owned());
            }
        }
    });
    response
}

fn spawn_inbound_stream_pump<S, T>(
    stream: S,
    sink: StreamSink<T>,
    timeout: Duration,
) -> GrpcStreamPump
where
    S: Stream<Item = GrpcResult<T>> + Send + 'static,
    T: Send + 'static,
{
    tokio::spawn(async move {
        let pump = async {
            let mut stream = Box::pin(stream);
            let mut count = 0usize;

            while let Some(next) = stream.next().await {
                let item = next.inspect_err(|_status| {
                    sink.cancel("gRPC inbound stream failed");
                })?;

                sink.send(item).await.map_err(|error| {
                    let (error, _item) = error.into_parts();
                    stream_status_from_error(error)
                })?;
                count = count.saturating_add(1);
            }

            sink.drain().map_err(stream_status_from_error)?;
            Ok(count)
        };

        tokio::time::timeout(timeout, pump)
            .await
            .unwrap_or_else(|_elapsed| {
                sink.cancel("gRPC inbound stream timed out");
                Err(GrpcError::StreamTimeout { timeout }.into_status())
            })
    })
}

fn response_stream_from_source<T>(
    source: StreamSource<T>,
    drop_cancel: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
) -> GrpcResponseStream<T>
where
    T: Send + 'static,
{
    Box::pin(stream::unfold(
        ResponseState::Open(ResponseGuard::new(source, drop_cancel)),
        |state| async move {
            match state {
                ResponseState::Done => None,
                ResponseState::Open(mut guard) => {
                    match guard.source.next().await.map_err(stream_status_from_error) {
                        Ok(Some(item)) => Some((Ok(item), ResponseState::Open(guard))),
                        Ok(None) => {
                            guard.mark_completed();
                            None
                        }
                        Err(status) => {
                            guard.mark_completed();
                            Some((Err(status), ResponseState::Done))
                        }
                    }
                }
            }
        },
    ))
}

async fn run_with_timeout<T>(
    future: impl Future<Output = GrpcResult<T>> + Send,
    timeout: Duration,
) -> GrpcResult<T> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_elapsed| GrpcError::StreamTimeout { timeout }.into_status())?
}

fn effective_stream_timeout<T>(request: &Request<T>, config: GrpcStreamConfig) -> Duration {
    request_timeout_from_metadata(request.metadata())
        .map(|deadline| deadline.min(config.request_timeout_value()))
        .unwrap_or_else(|| config.request_timeout_value())
}

fn stream_status_from_error(error: rakka_stream::StreamError) -> tonic::Status {
    GrpcError::from_stream_error(error).into_status()
}

fn cancel_if_active<T>(source: &StreamSource<T>, reason: impl Into<String>) {
    if !source.status().lifecycle().is_terminal() {
        source.cancel(reason);
    }
}

enum ResponseState<T> {
    Open(ResponseGuard<T>),
    Done,
}

struct ResponseGuard<T> {
    source: StreamSource<T>,
    completed: bool,
    drop_cancel: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
}

impl<T> ResponseGuard<T> {
    fn new(
        source: StreamSource<T>,
        drop_cancel: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            source,
            completed: false,
            drop_cancel,
        }
    }

    fn mark_completed(&mut self) {
        self.completed = true;
    }
}

impl<T> Drop for ResponseGuard<T> {
    fn drop(&mut self) {
        if !self.completed {
            cancel_if_active(&self.source, "gRPC response stream dropped");
            if let Some(drop_cancel) = self.drop_cancel.take() {
                drop_cancel();
            }
        }
    }
}
