//! HTTP streaming, SSE, and WebSocket adapters backed by Rakka streams.

use std::borrow::Cow;
use std::convert::Infallible;

use axum::body::{Body, Bytes};
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::http::header::{HeaderValue, CONTENT_TYPE};
use axum::http::{Request, Response, StatusCode};
use axum::response::sse::{Event, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use futures_util::stream::{self, Stream};
use futures_util::{SinkExt, StreamExt};
use rakka_stream::{bounded_channel, StreamError, StreamSink, StreamSource};
use tokio::task::JoinHandle;

use crate::{HttpError, HttpResult, HttpRouteConfig};

pub use axum::extract::ws::{CloseFrame as WebSocketCloseFrame, Message as WebSocketMessage};

/// Join handle returned by HTTP request body pump tasks.
pub type HttpRequestBodyPump = JoinHandle<HttpResult<usize>>;

/// Bounded request body stream plus its background body pump.
#[derive(Debug)]
pub struct HttpRequestBodyStream {
    source: StreamSource<Bytes>,
    pump: Option<HttpRequestBodyPump>,
}

impl HttpRequestBodyStream {
    /// Bounded source receiving request body chunks.
    #[must_use]
    pub const fn source(&self) -> &StreamSource<Bytes> {
        &self.source
    }

    /// Returns true when the request body pump has finished.
    #[must_use]
    pub fn pump_is_finished(&self) -> bool {
        match &self.pump {
            Some(pump) => pump.is_finished(),
            None => true,
        }
    }

    /// Cancels the request body stream and wakes the body pump.
    pub fn cancel(&self, reason: impl Into<String>) -> usize {
        self.source.cancel(reason)
    }

    /// Awaits the request body pump result.
    pub async fn join(&mut self) -> HttpResult<usize> {
        let Some(pump) = self.pump.take() else {
            return Ok(0);
        };

        pump.await.map_err(|error| HttpError::Stream {
            message: error.to_string(),
        })?
    }

    /// Consumes this adapter into the bounded source and pump handle.
    #[must_use]
    pub fn into_parts(mut self) -> (StreamSource<Bytes>, Option<HttpRequestBodyPump>) {
        (self.source, self.pump.take())
    }
}

/// Creates a bounded stream from an HTTP request body.
pub fn request_body_stream_from_request(
    request: Request<Body>,
    config: HttpRouteConfig,
    capacity: usize,
) -> HttpResult<HttpRequestBodyStream> {
    request_body_stream_from_body(request.into_body(), config, capacity)
}

/// Creates a bounded stream from an HTTP body.
pub fn request_body_stream_from_body(
    body: Body,
    config: HttpRouteConfig,
    capacity: usize,
) -> HttpResult<HttpRequestBodyStream> {
    let (sink, source) = bounded_channel(capacity).map_err(HttpError::from_stream_error)?;
    let pump = spawn_request_body_pump(body, sink, config);
    Ok(HttpRequestBodyStream {
        source,
        pump: Some(pump),
    })
}

/// Creates a POST route that exposes the request body as a bounded stream.
pub fn request_body_stream_route<F, Fut>(
    path: &'static str,
    config: HttpRouteConfig,
    capacity: usize,
    handler: F,
) -> Router
where
    F: Fn(HttpRequestBodyStream) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = HttpResult<Response<Body>>> + Send + 'static,
{
    Router::new().route(
        path,
        post(move |request: Request<Body>| {
            let handler = handler.clone();
            async move {
                let stream = request_body_stream_from_request(request, config, capacity)?;
                handler(stream).await
            }
        }),
    )
}

/// Creates a streaming binary HTTP response from a Rakka byte stream.
#[must_use]
pub fn byte_stream_response(source: StreamSource<Bytes>) -> Response<Body> {
    byte_stream_response_with_content_type(source, None)
}

/// Creates a streaming binary HTTP response with an explicit content type.
#[must_use]
pub fn byte_stream_response_with_content_type(
    source: StreamSource<Bytes>,
    content_type: Option<HeaderValue>,
) -> Response<Body> {
    let mut response = Response::new(Body::from_stream(byte_stream(source)));
    *response.status_mut() = StatusCode::OK;
    if let Some(content_type) = content_type {
        response.headers_mut().insert(CONTENT_TYPE, content_type);
    }
    response
}

/// Creates a GET route that responds with a Rakka byte stream.
pub fn byte_stream_route<F>(
    path: &'static str,
    content_type: Option<HeaderValue>,
    source_factory: F,
) -> Router
where
    F: Fn() -> HttpResult<StreamSource<Bytes>> + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        get(move || {
            let source_factory = source_factory.clone();
            let content_type = content_type.clone();
            async move {
                let source = source_factory()?;
                Ok::<_, HttpError>(byte_stream_response_with_content_type(source, content_type))
            }
        }),
    )
}

/// Creates an SSE response from a Rakka string stream.
pub fn sse_response_from_stream(
    source: StreamSource<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>> + Send> {
    Sse::new(sse_event_stream(source))
}

/// Creates a GET route that responds with server-sent events from a Rakka stream.
pub fn sse_stream_route<F>(path: &'static str, source_factory: F) -> Router
where
    F: Fn() -> HttpResult<StreamSource<String>> + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        get(move || {
            let source_factory = source_factory.clone();
            async move {
                let source = source_factory()?;
                Ok::<_, HttpError>(sse_response_from_stream(source))
            }
        }),
    )
}

/// WebSocket stream bridge endpoints for one upgraded connection.
pub struct WebSocketBridge {
    inbound: StreamSink<WebSocketMessage>,
    outbound: StreamSource<WebSocketMessage>,
}

impl WebSocketBridge {
    /// Creates a WebSocket bridge from an inbound sink and outbound source.
    #[must_use]
    pub const fn new(
        inbound: StreamSink<WebSocketMessage>,
        outbound: StreamSource<WebSocketMessage>,
    ) -> Self {
        Self { inbound, outbound }
    }

    /// Bounded stream sink receiving client WebSocket messages.
    #[must_use]
    pub const fn inbound(&self) -> &StreamSink<WebSocketMessage> {
        &self.inbound
    }

    /// Bounded stream source sending server WebSocket messages.
    #[must_use]
    pub const fn outbound(&self) -> &StreamSource<WebSocketMessage> {
        &self.outbound
    }
}

impl std::fmt::Debug for WebSocketBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketBridge")
            .field("inbound", &self.inbound)
            .field("outbound", &self.outbound)
            .finish()
    }
}

/// Summary returned when a WebSocket bridge closes normally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebSocketBridgeSummary {
    inbound_messages: usize,
    outbound_messages: usize,
}

impl WebSocketBridgeSummary {
    /// Creates a WebSocket bridge summary.
    #[must_use]
    pub const fn new(inbound_messages: usize, outbound_messages: usize) -> Self {
        Self {
            inbound_messages,
            outbound_messages,
        }
    }

    /// Number of client messages forwarded into the inbound Rakka stream.
    #[must_use]
    pub const fn inbound_messages(&self) -> usize {
        self.inbound_messages
    }

    /// Number of Rakka outbound stream messages sent to the WebSocket client.
    #[must_use]
    pub const fn outbound_messages(&self) -> usize {
        self.outbound_messages
    }
}

/// Creates paired Rakka streams for one WebSocket bridge.
pub fn websocket_bridge_pair(
    capacity: usize,
) -> HttpResult<(
    WebSocketBridge,
    StreamSource<WebSocketMessage>,
    StreamSink<WebSocketMessage>,
)> {
    let (inbound_sink, inbound_source) =
        bounded_channel(capacity).map_err(HttpError::from_stream_error)?;
    let (outbound_sink, outbound_source) =
        bounded_channel(capacity).map_err(HttpError::from_stream_error)?;
    Ok((
        WebSocketBridge::new(inbound_sink, outbound_source),
        inbound_source,
        outbound_sink,
    ))
}

/// Creates a GET route that upgrades to a WebSocket backed by Rakka streams.
pub fn websocket_stream_route<F>(path: &'static str, bridge_factory: F) -> Router
where
    F: Fn() -> HttpResult<WebSocketBridge> + Clone + Send + Sync + 'static,
{
    Router::new().route(
        path,
        get(move |upgrade: WebSocketUpgrade| {
            let bridge_factory = bridge_factory.clone();
            async move {
                match bridge_factory() {
                    Ok(bridge) => upgrade
                        .on_upgrade(move |socket| async move {
                            let _result = run_websocket_bridge(socket, bridge).await;
                        })
                        .into_response(),
                    Err(error) => error.into_response(),
                }
            }
        }),
    )
}

/// Bridges an upgraded Axum WebSocket with Rakka inbound and outbound streams.
pub async fn run_websocket_bridge(
    socket: WebSocket,
    bridge: WebSocketBridge,
) -> HttpResult<WebSocketBridgeSummary> {
    let (sender, receiver) = socket.split();
    run_websocket_bridge_io(receiver, sender, bridge).await
}

/// Bridges any WebSocket-like stream and sink with Rakka inbound and outbound streams.
pub async fn run_websocket_bridge_io<R, S, E>(
    mut receiver: R,
    mut sender: S,
    bridge: WebSocketBridge,
) -> HttpResult<WebSocketBridgeSummary>
where
    R: Stream<Item = Result<WebSocketMessage, E>> + Unpin,
    S: futures_util::Sink<WebSocketMessage, Error = E> + Unpin,
    E: std::fmt::Display,
{
    let inbound = bridge.inbound;
    let outbound = bridge.outbound;
    let mut inbound_messages = 0usize;
    let mut outbound_messages = 0usize;

    loop {
        tokio::select! {
            received = receiver.next() => {
                match received {
                    Some(Ok(WebSocketMessage::Close(_))) | None => {
                        inbound.drain().map_err(HttpError::from_stream_error)?;
                        outbound.cancel("websocket client disconnected");
                        return Ok(WebSocketBridgeSummary::new(inbound_messages, outbound_messages));
                    }
                    Some(Ok(message)) => {
                        inbound.send(message).await.map_err(|error| {
                            let (error, _message) = error.into_parts();
                            HttpError::from_stream_error(error)
                        })?;
                        inbound_messages = inbound_messages.saturating_add(1);
                    }
                    Some(Err(error)) => {
                        inbound.cancel(format!("websocket receive failed: {error}"));
                        outbound.cancel(format!("websocket receive failed: {error}"));
                        return Err(HttpError::WebSocket {
                            message: error.to_string(),
                        });
                    }
                }
            }
            next = outbound.next() => {
                match next {
                    Ok(Some(message)) => {
                        sender.send(message).await.map_err(|error| HttpError::WebSocket {
                            message: error.to_string(),
                        })?;
                        outbound_messages = outbound_messages.saturating_add(1);
                    }
                    Ok(None) => {
                        let _sent = sender.send(WebSocketMessage::Close(None)).await;
                        inbound.drain().map_err(HttpError::from_stream_error)?;
                        return Ok(WebSocketBridgeSummary::new(inbound_messages, outbound_messages));
                    }
                    Err(error) => {
                        let http_error = HttpError::from_stream_error(error);
                        let _sent = sender.send(stream_error_close_frame(&http_error)).await;
                        inbound.cancel(http_error.to_string());
                        return Err(http_error);
                    }
                }
            }
        }
    }
}

fn spawn_request_body_pump(
    body: Body,
    sink: StreamSink<Bytes>,
    config: HttpRouteConfig,
) -> HttpRequestBodyPump {
    tokio::spawn(async move {
        let mut stream = body.into_data_stream();
        let limit = config.max_payload_bytes_value();
        let mut total_bytes = 0usize;

        while let Some(next) = stream.next().await {
            let chunk = next.map_err(|error| {
                sink.cancel(format!("request body read failed: {error}"));
                HttpError::BodyRead {
                    message: error.to_string(),
                }
            })?;

            total_bytes = total_bytes.saturating_add(chunk.len());
            if total_bytes > limit {
                sink.cancel(format!("request body exceeded {limit} byte limit"));
                return Err(HttpError::PayloadTooLarge { limit });
            }

            sink.send(chunk).await.map_err(|error| {
                let (error, _chunk) = error.into_parts();
                HttpError::from_stream_error(error)
            })?;
        }

        sink.drain().map_err(HttpError::from_stream_error)?;
        Ok(total_bytes)
    })
}

fn byte_stream(source: StreamSource<Bytes>) -> impl Stream<Item = HttpResult<Bytes>> + Send {
    stream::unfold(SourceState::Open(source), |state| async move {
        match state {
            SourceState::Done => None,
            SourceState::Open(source) => match source.next().await {
                Ok(Some(bytes)) => Some((Ok(bytes), SourceState::Open(source))),
                Ok(None) => None,
                Err(error) => Some((Err(HttpError::from_stream_error(error)), SourceState::Done)),
            },
        }
    })
}

fn sse_event_stream(
    source: StreamSource<String>,
) -> impl Stream<Item = Result<Event, Infallible>> + Send {
    stream::unfold(SourceState::Open(source), |state| async move {
        match state {
            SourceState::Done => None,
            SourceState::Open(source) => match source.next().await {
                Ok(Some(data)) => {
                    Some((Ok(Event::default().data(data)), SourceState::Open(source)))
                }
                Ok(None) => None,
                Err(error) => Some((
                    Ok(Event::default()
                        .event("error")
                        .data(sse_error_message(error))),
                    SourceState::Done,
                )),
            },
        }
    })
}

fn sse_error_message(error: StreamError) -> String {
    error.to_string().replace('\r', "").replace('\n', " ")
}

fn stream_error_close_frame(error: &HttpError) -> WebSocketMessage {
    WebSocketMessage::Close(Some(WebSocketCloseFrame {
        code: 1011,
        reason: Cow::Owned(error.to_string()),
    }))
}

enum SourceState<T> {
    Open(StreamSource<T>),
    Done,
}
