//! Bounded stream core behavior tests.

use std::time::Duration;

use rakka_core::{
    CoordinatedShutdown, CoordinatedShutdownReason, InMemoryMetricsRecorder, ShutdownOutcome,
    ShutdownPhase, ShutdownTaskStatus, METRIC_STREAM_CANCELLATIONS, METRIC_STREAM_PRESSURE,
};
use rakka_stream::{
    bounded_channel, register_stream_sink_drain, register_stream_source_drain, StreamError,
    StreamLifecycle,
};

#[tokio::test]
async fn producer_observes_backpressure_and_resumes_when_space_is_available() {
    let (sink, source) = bounded_channel(1).expect("stream should be created");

    sink.try_send("first".to_owned())
        .expect("first item should fit");

    let full = sink
        .try_send("second".to_owned())
        .expect_err("second item should not fit");
    assert_eq!(full.error(), &StreamError::Full { capacity: 1 });
    assert_eq!(full.item(), "second");

    let pending_send = tokio::spawn(async move { sink.send("second".to_owned()).await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !pending_send.is_finished(),
        "producer should be waiting for bounded capacity"
    );

    assert_eq!(
        source.next().await.expect("first item"),
        Some("first".into())
    );
    pending_send
        .await
        .expect("send task should finish")
        .expect("second send should resume");
    assert_eq!(
        source.next().await.expect("second item"),
        Some("second".into())
    );
}

#[tokio::test]
async fn consumer_cancel_wakes_pending_sender_and_receiver_observes_cancel() {
    let (sink, source) = bounded_channel(1).expect("stream should be created");

    sink.try_send("first".to_owned())
        .expect("first item should fit");

    let pending_send = tokio::spawn(async move { sink.send("second".to_owned()).await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !pending_send.is_finished(),
        "producer should be waiting before cancellation"
    );

    assert_eq!(source.cancel("client disconnected"), 1);

    let send_error = pending_send
        .await
        .expect("send task should finish")
        .expect_err("pending sender should observe cancellation");
    assert_eq!(
        send_error.error(),
        &StreamError::Cancelled {
            reason: Some("client disconnected".to_owned())
        }
    );
    assert_eq!(send_error.item(), "second");

    assert_eq!(
        source
            .next()
            .await
            .expect_err("consumer should observe cancel"),
        StreamError::Cancelled {
            reason: Some("client disconnected".to_owned())
        }
    );

    let status = source.status();
    assert_eq!(status.lifecycle(), StreamLifecycle::Cancelled);
    assert_eq!(status.dropped_items(), 1);
    assert_eq!(status.cancel_reason(), Some("client disconnected"));
}

#[tokio::test]
async fn drain_rejects_new_items_and_flushes_buffered_items() {
    let (sink, source) = bounded_channel(2).expect("stream should be created");

    sink.try_send("first".to_owned())
        .expect("first item should fit");
    sink.try_send("second".to_owned())
        .expect("second item should fit");

    sink.drain().expect("drain should start");

    let rejected = sink
        .try_send("third".to_owned())
        .expect_err("draining stream should reject new items");
    assert_eq!(rejected.error(), &StreamError::Draining);
    assert_eq!(rejected.item(), "third");

    assert_eq!(source.status().lifecycle(), StreamLifecycle::Draining);
    assert_eq!(
        source.next().await.expect("first item"),
        Some("first".into())
    );
    assert_eq!(
        source.next().await.expect("second item"),
        Some("second".into())
    );
    assert_eq!(source.next().await.expect("drain completion"), None);
    assert_eq!(source.status().lifecycle(), StreamLifecycle::Completed);

    let completed_send = sink
        .try_send("after-complete".to_owned())
        .expect_err("completed stream should reject sends");
    assert_eq!(completed_send.error(), &StreamError::Closed);
}

#[tokio::test]
async fn close_wakes_pending_receiver_with_typed_error() {
    let (sink, source) = bounded_channel::<String>(1).expect("stream should be created");

    let pending_receive = tokio::spawn(async move { source.next().await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !pending_receive.is_finished(),
        "receiver should wait while stream is open and empty"
    );

    assert_eq!(sink.close(), 0);

    assert_eq!(
        pending_receive
            .await
            .expect("receive task should finish")
            .expect_err("receiver should observe close"),
        StreamError::Closed
    );
}

#[tokio::test]
async fn close_wakes_pending_sender_and_drops_buffer() {
    let (sink, source) = bounded_channel(1).expect("stream should be created");

    sink.try_send("first".to_owned())
        .expect("first item should fit");

    let pending_send = tokio::spawn(async move { sink.send("second".to_owned()).await });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !pending_send.is_finished(),
        "producer should be waiting before close"
    );

    assert_eq!(source.close(), 1);

    let send_error = pending_send
        .await
        .expect("send task should finish")
        .expect_err("pending sender should observe close");
    assert_eq!(send_error.error(), &StreamError::Closed);
    assert_eq!(send_error.item(), "second");

    assert_eq!(
        source
            .next()
            .await
            .expect_err("consumer should observe close"),
        StreamError::Closed
    );
    assert_eq!(source.status().lifecycle(), StreamLifecycle::Closed);
    assert_eq!(source.status().dropped_items(), 1);
}

#[test]
fn zero_capacity_is_rejected() {
    let error = bounded_channel::<String>(0).expect_err("zero capacity should be invalid");
    assert_eq!(error, StreamError::InvalidCapacity { capacity: 0 });
}

#[test]
fn stream_status_records_pressure_and_cancellation_metrics() {
    let (sink, source) = bounded_channel(2).expect("stream should be created");
    sink.try_send("first".to_string())
        .expect("first item should enqueue");
    let recorder = InMemoryMetricsRecorder::new();

    let open_status = source.status();
    open_status.record_metrics(&recorder, "ingress");
    assert_eq!(open_status.pressure(), 0.5);
    assert!(serde_json::to_string(&open_status)
        .expect("stream status should serialize")
        .contains("\"depth\":1"));

    source.cancel("client disconnected");
    let cancelled = source.status();
    cancelled.record_metrics(&recorder, "ingress");

    let metrics = recorder.snapshot();
    assert_eq!(metrics.last_gauge(METRIC_STREAM_PRESSURE), Some(0.0));
    assert_eq!(metrics.counter_total(METRIC_STREAM_CANCELLATIONS), 1.0);
    assert_eq!(
        metrics
            .last_observation(METRIC_STREAM_CANCELLATIONS, rakka_core::MetricKind::Counter)
            .and_then(|observation| observation.attribute("reason")),
        Some("client disconnected")
    );
}

#[tokio::test]
async fn coordinated_shutdown_drains_registered_stream_sink() {
    let shutdown = CoordinatedShutdown::new();
    let (sink, source) = bounded_channel(2).expect("stream should be created");
    sink.try_send("first".to_owned())
        .expect("first item should enqueue");

    register_stream_sink_drain(&shutdown, "drain-orders-stream", sink.clone()).unwrap();

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();

    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    assert_eq!(source.status().lifecycle(), StreamLifecycle::Draining);
    assert_eq!(
        report
            .phases()
            .iter()
            .find(|phase| phase.phase() == &ShutdownPhase::drain_adapters())
            .and_then(|phase| phase.tasks().first())
            .map(|task| task.status()),
        Some(ShutdownTaskStatus::Completed)
    );
    let rejected = sink
        .try_send("second".to_owned())
        .expect_err("drained stream should reject new sends");
    assert_eq!(rejected.error(), &StreamError::Draining);
}

#[tokio::test]
async fn coordinated_shutdown_stream_drain_treats_terminal_streams_as_completed() {
    let shutdown = CoordinatedShutdown::new();
    let (closed_sink, closed_source) = bounded_channel::<String>(1).expect("closed stream");
    let (cancelled_sink, cancelled_source) =
        bounded_channel::<String>(1).expect("cancelled stream");

    assert_eq!(closed_source.close(), 0);
    assert_eq!(cancelled_sink.cancel("already gone"), 0);

    register_stream_sink_drain(&shutdown, "drain-closed-stream", closed_sink).unwrap();
    register_stream_source_drain(&shutdown, "drain-cancelled-stream", cancelled_source).unwrap();

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();

    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
}
