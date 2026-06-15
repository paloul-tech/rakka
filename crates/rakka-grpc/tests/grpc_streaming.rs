//! Streaming gRPC adapter behavior tests.

use std::time::Duration;

use futures_util::stream;
use futures_util::StreamExt;
use rakka_grpc::{
    bidi_stream_pair_from_stream, client_stream_from_stream, server_streaming_response,
    stream_status, GrpcResponseStream, GrpcResult, GrpcStreamConfig,
    RAKKA_GRPC_ERROR_CODE_METADATA,
};
use rakka_stream::{bounded_channel, StreamError, StreamLifecycle};
use tonic::{Code, Status};

#[tokio::test]
async fn server_streaming_sends_ordered_items_from_bounded_stream() {
    let (sink, source) = bounded_channel(2).expect("stream should be created");
    send_or_panic(&sink, StreamReply { value: 1 }).await;
    send_or_panic(&sink, StreamReply { value: 2 }).await;
    sink.drain().expect("source should drain");

    let response = server_streaming_response(source);
    let replies = collect_response_values(response.into_inner())
        .await
        .expect("server stream should complete");

    assert_eq!(replies, vec![1, 2]);
}

#[tokio::test]
async fn server_streaming_drop_cancels_upstream_source() {
    let (sink, source) = bounded_channel::<StreamReply>(1).expect("stream should be created");
    let response = server_streaming_response(source);

    drop(response.into_inner());

    assert_eq!(sink.status().lifecycle(), StreamLifecycle::Cancelled);
    assert_eq!(
        sink.status().cancel_reason(),
        Some("gRPC response stream dropped")
    );
}

#[tokio::test]
async fn stream_error_maps_to_sanitized_grpc_status() {
    let (_sink, source) = bounded_channel::<StreamReply>(1).expect("stream should be created");
    source.cancel("internal remote envelope type leaked here");
    let mut stream = server_streaming_response(source).into_inner();

    let status = stream
        .next()
        .await
        .expect("stream should emit terminal status")
        .expect_err("cancelled stream should return status");

    assert_eq!(status.code(), Code::Cancelled);
    assert_status_error_code(&status, "stream-cancelled");
    assert!(!status.message().contains("remote envelope"));
}

#[test]
fn stream_operator_error_maps_to_grpc_internal_status() {
    let status = stream_status(StreamError::Operator {
        message: "map_async task failed".to_owned(),
    });

    assert_eq!(status.code(), Code::Internal);
    assert_status_error_code(&status, "stream-operator-error");
    assert!(status.message().contains("map_async task failed"));
}

#[tokio::test]
async fn client_streaming_applies_backpressure_into_bounded_stream() {
    let inbound = stream::iter([
        Ok(StreamRequest { value: 1 }),
        Ok(StreamRequest { value: 2 }),
    ]);
    let mut adapter = client_stream_from_stream(
        inbound,
        GrpcStreamConfig::default()
            .buffer_capacity(1)
            .request_timeout(Duration::from_secs(1)),
    )
    .expect("client stream should be accepted");

    wait_for_depth(adapter.source(), 1).await;
    assert!(
        !adapter.pump_is_finished(),
        "second item should wait for bounded capacity"
    );

    assert_eq!(
        adapter
            .source()
            .next()
            .await
            .expect("source should read")
            .expect("first item")
            .value,
        1
    );
    assert_eq!(
        adapter
            .source()
            .next()
            .await
            .expect("source should read")
            .expect("second item")
            .value,
        2
    );
    assert!(adapter
        .source()
        .next()
        .await
        .expect("source should complete")
        .is_none());
    assert_eq!(adapter.join().await.expect("pump should complete"), 2);
}

#[tokio::test]
async fn client_streaming_deadline_cancels_inbound_source() {
    let inbound = stream::pending::<GrpcResult<StreamRequest>>();
    let mut adapter = client_stream_from_stream(
        inbound,
        GrpcStreamConfig::default()
            .buffer_capacity(1)
            .request_timeout(Duration::from_millis(5)),
    )
    .expect("client stream should be accepted");

    let status = adapter
        .join()
        .await
        .expect_err("pump should observe deadline");

    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_status_error_code(&status, "stream-timeout");
    assert_eq!(
        adapter.source().status().lifecycle(),
        StreamLifecycle::Cancelled
    );
}

#[tokio::test]
async fn bidirectional_streaming_routes_inbound_to_independent_outbound_stream() {
    let inbound = stream::iter([
        Ok(StreamRequest { value: 1 }),
        Ok(StreamRequest { value: 2 }),
    ]);
    let bridge = bidi_stream_pair_from_stream::<_, StreamRequest, StreamReply>(
        inbound,
        GrpcStreamConfig::default()
            .buffer_capacity(1)
            .request_timeout(Duration::from_secs(1)),
    )
    .expect("bidirectional bridge should be created");
    let (inbound_source, outbound_sink, response_stream, inbound_pump) = bridge.into_parts();

    let worker = tokio::spawn(async move {
        while let Some(request) = inbound_source
            .next()
            .await
            .expect("inbound source should not fail")
        {
            send_or_panic(
                &outbound_sink,
                StreamReply {
                    value: request.value * 10,
                },
            )
            .await;
        }
        outbound_sink.drain().expect("outbound stream should drain");
    });

    let replies = collect_response_values(response_stream)
        .await
        .expect("response stream should complete");

    assert_eq!(replies, vec![10, 20]);
    worker.await.expect("worker should finish");
    assert_eq!(
        inbound_pump
            .expect("inbound pump should exist")
            .await
            .expect("pump task should finish")
            .expect("pump should complete"),
        2
    );
}

#[tokio::test]
async fn bidirectional_response_drop_cancels_blocked_inbound_pump() {
    let inbound = stream::iter([
        Ok(StreamRequest { value: 1 }),
        Ok(StreamRequest { value: 2 }),
    ]);
    let bridge = bidi_stream_pair_from_stream::<_, StreamRequest, StreamReply>(
        inbound,
        GrpcStreamConfig::default()
            .buffer_capacity(1)
            .request_timeout(Duration::from_secs(1)),
    )
    .expect("bidirectional bridge should be created");
    let (inbound_source, _outbound_sink, response_stream, inbound_pump) = bridge.into_parts();

    wait_for_depth(&inbound_source, 1).await;
    drop(response_stream);

    let status = inbound_pump
        .expect("inbound pump should exist")
        .await
        .expect("pump task should finish")
        .expect_err("blocked inbound pump should observe cancellation");

    assert_eq!(status.code(), Code::Cancelled);
    assert_status_error_code(&status, "stream-cancelled");
}

async fn collect_response_values(
    mut stream: GrpcResponseStream<StreamReply>,
) -> GrpcResult<Vec<i64>> {
    let mut values = Vec::new();
    while let Some(next) = stream.next().await {
        values.push(next?.value);
    }
    Ok(values)
}

async fn wait_for_depth<T>(source: &rakka_stream::StreamSource<T>, expected: usize) {
    for _attempt in 0..20 {
        if source.status().depth() == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(source.status().depth(), expected);
}

async fn send_or_panic<T>(sink: &rakka_stream::StreamSink<T>, item: T) {
    if let Err(error) = sink.send(item).await {
        panic!("stream send failed: {}", error.error());
    }
}

fn assert_status_error_code(status: &Status, expected: &str) {
    let code = status
        .metadata()
        .get(RAKKA_GRPC_ERROR_CODE_METADATA)
        .expect("status should include Rakka error code")
        .to_str()
        .expect("error code should be ASCII metadata");
    assert_eq!(code, expected);
}

#[derive(Clone, PartialEq, prost::Message)]
struct StreamRequest {
    #[prost(int64, tag = "1")]
    value: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct StreamReply {
    #[prost(int64, tag = "1")]
    value: i64,
}
