//! HTTP streaming, SSE, and WebSocket adapter behavior tests.

use std::fmt::{self, Display, Formatter};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::{to_bytes, Body, Bytes};
use axum::http::header::{HeaderValue, CONTENT_TYPE};
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use futures_util::stream;
use futures_util::Sink;
use rakka_http::{
    byte_stream_response, byte_stream_route, request_body_stream_from_body,
    run_websocket_bridge_io, sse_response_from_stream, sse_stream_route, websocket_bridge_pair,
    WebSocketMessage,
};
use rakka_stream::{bounded_channel, StreamLifecycle, StreamSink};
use tower::ServiceExt;

#[tokio::test]
async fn request_body_stream_applies_backpressure_until_consumer_reads() {
    let body = Body::from_stream(stream::iter([
        Ok::<_, std::io::Error>(Bytes::from_static(b"one")),
        Ok::<_, std::io::Error>(Bytes::from_static(b"two")),
    ]));
    let mut request_stream =
        request_body_stream_from_body(body, Default::default(), 1).expect("body stream");

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(request_stream.source().status().depth(), 1);
    assert!(
        !request_stream.pump_is_finished(),
        "body pump should wait for bounded capacity"
    );

    assert_eq!(
        request_stream
            .source()
            .next()
            .await
            .expect("first chunk")
            .expect("first item")
            .as_ref(),
        b"one"
    );
    assert_eq!(
        request_stream
            .source()
            .next()
            .await
            .expect("second chunk")
            .expect("second item")
            .as_ref(),
        b"two"
    );
    assert_eq!(
        request_stream
            .source()
            .next()
            .await
            .expect("body completion"),
        None
    );
    assert_eq!(request_stream.join().await.expect("pump bytes"), 6);
}

#[tokio::test]
async fn byte_stream_route_returns_streamed_response_and_graceful_drain_completes() {
    let router = byte_stream_route(
        "/bytes",
        Some(HeaderValue::from_static("application/octet-stream")),
        || {
            let (sink, source) = bounded_channel(2).expect("stream should be created");
            tokio::spawn(async move {
                sink.send(Bytes::from_static(b"hello "))
                    .await
                    .expect("first chunk");
                sink.send(Bytes::from_static(b"world"))
                    .await
                    .expect("second chunk");
                sink.drain().expect("response stream should drain");
            });
            Ok(source)
        },
    );

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/bytes")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).expect("content type"),
        "application/octet-stream"
    );
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    assert_eq!(body.as_ref(), b"hello world");
}

#[tokio::test]
async fn byte_stream_response_surfaces_stream_error_to_body_collector() {
    let (_sink, source) = bounded_channel::<Bytes>(1).expect("stream should be created");
    source.cancel("upstream failed");
    let response = byte_stream_response(source);

    let error = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect_err("body collector should observe stream error");
    assert!(
        error.to_string().contains("HTTP stream failed"),
        "unexpected body error: {error}"
    );
}

#[tokio::test]
async fn sse_route_emits_ordered_events_and_closes_cleanly() {
    let router = sse_stream_route("/events", || {
        let (sink, source) = bounded_channel(2).expect("stream should be created");
        tokio::spawn(async move {
            sink.send("one".to_owned()).await.expect("first event");
            sink.send("two".to_owned()).await.expect("second event");
            sink.drain().expect("sse stream should drain");
        });
        Ok(source)
    });

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/events")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("sse body");
    let body = String::from_utf8(body.to_vec()).expect("sse utf8");
    assert!(body.contains("data: one\n\n"), "{body}");
    assert!(body.contains("data: two\n\n"), "{body}");
    assert!(
        body.find("data: one").expect("first event")
            < body.find("data: two").expect("second event"),
        "{body}"
    );
}

#[tokio::test]
async fn sse_response_turns_stream_error_into_error_event() {
    let (_sink, source) = bounded_channel::<String>(1).expect("stream should be created");
    source.cancel("publisher failed");
    let response = sse_response_from_stream(source).into_response();

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("sse body");
    let body = String::from_utf8(body.to_vec()).expect("sse utf8");
    assert!(body.contains("event: error\n"), "{body}");
    assert!(
        body.contains("stream was cancelled: publisher failed"),
        "{body}"
    );
}

#[tokio::test]
async fn websocket_bridge_forwards_inbound_and_cancels_outbound_on_disconnect() {
    let (bridge, inbound, outbound) = websocket_bridge_pair(4).expect("bridge pair");
    let sent = Arc::new(Mutex::new(Vec::new()));
    let receiver = stream::iter([
        Ok::<_, TestWebSocketError>(WebSocketMessage::Text("hello".to_owned())),
        Ok::<_, TestWebSocketError>(WebSocketMessage::Close(None)),
    ]);

    let summary = run_websocket_bridge_io(receiver, RecordingSink::new(Arc::clone(&sent)), bridge)
        .await
        .expect("bridge should close normally");

    assert_eq!(summary.inbound_messages(), 1);
    assert_eq!(summary.outbound_messages(), 0);
    assert_eq!(
        inbound.next().await.expect("inbound stream item"),
        Some(WebSocketMessage::Text("hello".to_owned()))
    );
    assert_eq!(inbound.next().await.expect("inbound drain"), None);
    wait_for_cancelled(&outbound).await;
    let send_error = outbound
        .send(WebSocketMessage::Text("after-close".to_owned()))
        .await
        .expect_err("outbound source should be cancelled");
    assert!(matches!(
        send_error.error(),
        rakka_stream::StreamError::Cancelled { .. }
    ));
    assert!(
        sent.lock()
            .expect("sent messages should not poison")
            .is_empty(),
        "server should not send frames for an inbound-only close"
    );
}

#[tokio::test]
async fn websocket_bridge_sends_outbound_messages_and_close_on_drain() {
    let (bridge, _inbound, outbound) = websocket_bridge_pair(4).expect("bridge pair");
    let sent = Arc::new(Mutex::new(Vec::new()));
    outbound
        .send(WebSocketMessage::Text("world".to_owned()))
        .await
        .expect("outbound send");
    outbound.drain().expect("outbound should drain");
    let receiver = stream::pending::<Result<WebSocketMessage, TestWebSocketError>>();

    let summary = run_websocket_bridge_io(receiver, RecordingSink::new(Arc::clone(&sent)), bridge)
        .await
        .expect("bridge should close normally");

    assert_eq!(summary.inbound_messages(), 0);
    assert_eq!(summary.outbound_messages(), 1);
    let sent = sent.lock().expect("sent messages should not poison");
    assert_eq!(sent[0], WebSocketMessage::Text("world".to_owned()));
    assert!(matches!(sent[1], WebSocketMessage::Close(None)));
}

async fn wait_for_cancelled(sink: &StreamSink<WebSocketMessage>) {
    for _ in 0..50 {
        if sink.status().lifecycle() == StreamLifecycle::Cancelled {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for outbound cancellation");
}

#[derive(Debug, Clone)]
struct TestWebSocketError;

impl Display for TestWebSocketError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("test websocket error")
    }
}

struct RecordingSink {
    sent: Arc<Mutex<Vec<WebSocketMessage>>>,
}

impl RecordingSink {
    fn new(sent: Arc<Mutex<Vec<WebSocketMessage>>>) -> Self {
        Self { sent }
    }
}

impl Sink<WebSocketMessage> for RecordingSink {
    type Error = TestWebSocketError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: WebSocketMessage) -> Result<(), Self::Error> {
        self.sent
            .lock()
            .expect("sent messages should not poison")
            .push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
