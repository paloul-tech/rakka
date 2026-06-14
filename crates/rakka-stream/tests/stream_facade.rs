//! Stream facade vocabulary tests.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use rakka_stream::{
    bounded_channel, Flow, Sink, Source, StreamError, StreamRunError, StreamRunSettings,
    DEFAULT_BUFFER_CAPACITY,
};

#[test]
fn stream_run_settings_defaults_are_bounded_and_named_later() {
    let settings = StreamRunSettings::default();

    assert_eq!(settings.default_buffer_capacity(), DEFAULT_BUFFER_CAPACITY);
    assert_eq!(settings.operator_buffer_capacity(), DEFAULT_BUFFER_CAPACITY);
    assert_eq!(settings.stream_name(), None);
    assert_eq!(settings.cancellation_reason(), "stream cancelled");

    let named = settings
        .with_stream_name("orders")
        .with_cancellation_reason("orders stream cancelled");
    assert_eq!(named.stream_name(), Some("orders"));
    assert_eq!(named.cancellation_reason(), "orders stream cancelled");
    assert_eq!(named.without_stream_name().stream_name(), None);
}

#[test]
fn stream_run_settings_reject_zero_capacities() {
    assert_eq!(
        StreamRunSettings::new(0, 1).unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
    assert_eq!(
        StreamRunSettings::new(1, 0).unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
    assert_eq!(
        StreamRunSettings::default()
            .with_default_buffer_capacity(0)
            .unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
    assert_eq!(
        StreamRunSettings::default()
            .with_operator_buffer_capacity(0)
            .unwrap_err(),
        StreamError::InvalidCapacity { capacity: 0 }
    );
}

#[test]
fn facade_vocabulary_constructs_without_materializing() {
    let settings = StreamRunSettings::new(8, 4)
        .expect("valid settings")
        .with_stream_name("facade-test");
    let source = Source::<u64>::empty().with_settings(settings.clone());
    let flow = Flow::<u64, u64>::identity().with_settings(settings.clone());
    let sink = Sink::<u64, ()>::ignore().with_settings(settings.clone());

    assert!(source.is_empty());
    assert!(flow.is_identity());
    assert!(sink.is_ignore());
    assert_eq!(source.settings().stream_name(), Some("facade-test"));
    assert_eq!(flow.settings().operator_buffer_capacity(), 4);
    assert_eq!(sink.settings().default_buffer_capacity(), 8);

    let runnable = source.to(sink);
    assert_eq!(
        runnable.source_settings().stream_name(),
        Some("facade-test")
    );
    assert_eq!(runnable.sink_settings().stream_name(), Some("facade-test"));
}

#[tokio::test]
async fn source_constructors_materialize_to_collect_sink() {
    assert_eq!(
        Source::<u64>::empty().run_collect().await.unwrap(),
        Vec::<u64>::new()
    );
    assert_eq!(Source::single(7).run_collect().await.unwrap(), vec![7]);
    assert_eq!(
        Source::from_iter([1, 2, 3]).run_collect().await.unwrap(),
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn sink_foreach_and_fold_materialize_results() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let observed_for_sink = Arc::clone(&observed);
    Source::from_iter([1, 2, 3])
        .run_foreach(move |item| {
            observed_for_sink
                .lock()
                .expect("observed mutex should not poison")
                .push(item);
        })
        .await
        .unwrap();
    assert_eq!(
        *observed.lock().expect("observed mutex should not poison"),
        vec![1, 2, 3]
    );

    let sum = Source::from_iter([1, 2, 3])
        .run_with(Sink::fold(0, |sum, item| sum + item))
        .await
        .unwrap();
    assert_eq!(sum, 6);
}

#[tokio::test]
async fn runnable_stream_runs_connected_source_and_sink() {
    let runnable = Source::from_iter(["a".to_owned(), "b".to_owned()]).to(Sink::collect());

    assert_eq!(
        runnable.run().await.unwrap(),
        vec!["a".to_owned(), "b".to_owned()]
    );
}

#[tokio::test]
async fn source_and_sink_wrap_low_level_bounded_primitives() {
    let (input_sink, input_source) = bounded_channel(2).unwrap();
    input_sink.try_send("one".to_owned()).unwrap();
    input_sink.try_send("two".to_owned()).unwrap();
    input_sink.drain().unwrap();

    assert_eq!(
        Source::from_stream_source(input_source)
            .run_collect()
            .await
            .unwrap(),
        vec!["one".to_owned(), "two".to_owned()]
    );

    let (output_sink, output_source) = bounded_channel(2).unwrap();
    let forwarded = Source::from_iter(["three".to_owned(), "four".to_owned()])
        .run_with(Sink::from_stream_sink(output_sink.clone()))
        .await
        .unwrap();
    output_sink.drain().unwrap();

    assert_eq!(forwarded, 2);
    assert_eq!(
        output_source.next().await.unwrap(),
        Some("three".to_owned())
    );
    assert_eq!(output_source.next().await.unwrap(), Some("four".to_owned()));
    assert_eq!(output_source.next().await.unwrap(), None);
}

#[tokio::test]
async fn facade_run_errors_preserve_source_and_sink_lifecycle() {
    let (sink, source) = bounded_channel::<u64>(1).unwrap();
    sink.cancel("source cancelled");
    let source_error = Source::from_stream_source(source)
        .run_collect()
        .await
        .unwrap_err();
    assert!(matches!(
        source_error,
        StreamRunError::Source {
            error: StreamError::Cancelled { .. }
        }
    ));

    let (closed_sink, _closed_source) = bounded_channel(1).unwrap();
    closed_sink.close();
    let sink_error = Source::single(9)
        .run_with(Sink::from_stream_sink(closed_sink))
        .await
        .unwrap_err();
    assert_eq!(sink_error.code(), "sink-error");
    assert!(matches!(
        sink_error.sink_error().map(|error| error.error()),
        Some(StreamError::Closed)
    ));
}

#[tokio::test]
async fn linear_operators_compose_and_preserve_order() {
    let result = Source::from_iter([1, 2, 3, 4])
        .map(|item| item * 2)
        .filter(|item| *item > 4)
        .take(2)
        .run_collect()
        .await
        .unwrap();

    assert_eq!(result, vec![6, 8]);
}

#[tokio::test]
async fn flow_identity_and_from_fn_apply_through_via() {
    let identity = Source::from_iter([1, 2, 3])
        .via(Flow::identity())
        .run_collect()
        .await
        .unwrap();
    assert_eq!(identity, vec![1, 2, 3]);

    let mapped = Source::from_iter([1, 2, 3])
        .via(Flow::from_fn(|item| format!("item-{item}")))
        .run_collect()
        .await
        .unwrap();
    assert_eq!(
        mapped,
        vec![
            "item-1".to_owned(),
            "item-2".to_owned(),
            "item-3".to_owned()
        ]
    );
}

#[tokio::test]
async fn take_zero_completes_without_waiting_and_cancels_upstream_queue() {
    let (sink, source) = bounded_channel(1).unwrap();
    sink.try_send("buffered".to_owned()).unwrap();
    let pending_send = tokio::spawn({
        let sink = sink.clone();
        async move { sink.send("blocked".to_owned()).await }
    });

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        !pending_send.is_finished(),
        "sender should wait while the source buffer is full"
    );

    let collected = Source::from_stream_source(source)
        .take(0)
        .run_collect()
        .await
        .unwrap();
    assert!(collected.is_empty());

    let send_error = pending_send
        .await
        .expect("pending send task should finish")
        .expect_err("take(0) should cancel the upstream source");
    assert!(matches!(
        send_error.error(),
        StreamError::Cancelled { reason: Some(reason) }
            if reason == "take(0) completed"
    ));
    assert_eq!(send_error.item(), "blocked");
    assert_eq!(sink.status().dropped_items(), 1);
}

#[tokio::test]
async fn stream_facade_async_map_async_one_behaves_like_sequential_map() {
    let values = Source::from_iter([1, 2, 3])
        .map_async(1, |item| async move { item * 2 })
        .unwrap()
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![2, 4, 6]);
}

#[tokio::test]
async fn stream_facade_async_map_async_preserves_order_when_futures_complete_out_of_order() {
    let values = Source::from_iter([1_u64, 2, 3])
        .map_async(3, |item| async move {
            tokio::time::sleep(Duration::from_millis((4 - item) * 10)).await;
            item
        })
        .unwrap()
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![1, 2, 3]);
}

#[tokio::test]
async fn stream_facade_async_map_async_rejects_zero_parallelism() {
    let error = Source::from_iter([1])
        .map_async(0, |item| async move { item })
        .unwrap_err();

    assert_eq!(error.code(), "operator-error");
    assert!(matches!(
        error,
        StreamError::Operator { message }
            if message == "map_async parallelism must be greater than zero"
    ));
}

#[tokio::test]
async fn stream_facade_async_map_async_limits_in_flight_work_to_parallelism() {
    let current = Arc::new(AtomicUsize::new(0));
    let max = Arc::new(AtomicUsize::new(0));

    let values = Source::from_iter([1, 2, 3, 4])
        .map_async(2, {
            let current = Arc::clone(&current);
            let max = Arc::clone(&max);
            move |item| {
                let current = Arc::clone(&current);
                let max = Arc::clone(&max);
                async move {
                    let in_flight = current.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(in_flight, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    item
                }
            }
        })
        .unwrap()
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![1, 2, 3, 4]);
    assert_eq!(max.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn stream_facade_async_flow_from_async_fn_applies_through_via() {
    let flow = Flow::from_async_fn(2, |item| async move { format!("item-{item}") }).unwrap();
    let values = Source::from_iter([1, 2, 3])
        .via(flow)
        .run_collect()
        .await
        .unwrap();

    assert_eq!(
        values,
        vec![
            "item-1".to_owned(),
            "item-2".to_owned(),
            "item-3".to_owned()
        ]
    );
}

#[tokio::test]
async fn stream_facade_async_map_async_task_failure_surfaces_operator_error() {
    let error = Source::from_iter([1_u64])
        .map_async(1, |item| async move {
            if item == 1 {
                panic!("boom");
            }
            item
        })
        .unwrap()
        .run_collect()
        .await
        .unwrap_err();

    assert_eq!(error.code(), "source-error");
    assert!(matches!(
        error.source_error(),
        Some(StreamError::Operator { message }) if message.contains("map_async task failed")
    ));
}

#[tokio::test]
async fn stream_facade_async_take_cancels_in_flight_map_async_tasks() {
    struct DropGuard(Arc<AtomicUsize>);

    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let never_release = Arc::new(tokio::sync::Notify::new());

    let values = Source::from_iter([1, 2])
        .map_async(2, {
            let dropped = Arc::clone(&dropped);
            let never_release = Arc::clone(&never_release);
            move |item| {
                let dropped = Arc::clone(&dropped);
                let never_release = Arc::clone(&never_release);
                async move {
                    if item == 2 {
                        let _guard = DropGuard(dropped);
                        never_release.notified().await;
                    }
                    item
                }
            }
        })
        .unwrap()
        .take(1)
        .run_collect()
        .await
        .unwrap();

    assert_eq!(values, vec![1]);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while dropped.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stream_facade_fanout_merge_forwards_all_items_from_both_sources() {
    let mut values = Source::from_iter([1, 2])
        .merge(Source::from_iter([3, 4]))
        .run_collect()
        .await
        .unwrap();

    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn stream_facade_fanout_merge_all_empty_completes() {
    let values = Source::<u64>::merge_all(Vec::new())
        .run_collect()
        .await
        .unwrap();

    assert!(values.is_empty());
}

#[tokio::test]
async fn stream_facade_fanout_merge_surfaces_source_failure() {
    let (sink, failed_source) = bounded_channel::<u64>(1).unwrap();
    sink.cancel("merge input cancelled");

    let error = Source::from_iter([1])
        .merge(Source::from_stream_source(failed_source))
        .run_collect()
        .await
        .unwrap_err();

    assert!(matches!(
        error.source_error(),
        Some(StreamError::Cancelled {
            reason: Some(reason)
        }) if reason == "merge input cancelled"
    ));
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_rejects_zero_branches() {
    let error = Source::from_iter([1])
        .broadcast(0)
        .expect_err("zero broadcast branches should be rejected");

    assert_eq!(error.code(), "operator-error");
    assert!(matches!(
        error,
        StreamError::Operator { message }
            if message == "broadcast branch count must be greater than zero"
    ));
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_forwards_each_item_to_each_branch() {
    let mut branches = Source::from_iter([1, 2, 3]).broadcast(2).unwrap();
    let right = branches.pop().expect("right branch");
    let left = branches.pop().expect("left branch");

    let (left, right) = tokio::join!(left.run_collect(), right.run_collect());

    assert_eq!(left.unwrap(), vec![1, 2, 3]);
    assert_eq!(right.unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_backpressures_on_full_live_branch() {
    let settings = StreamRunSettings::new(8, 1).expect("valid stream settings");
    let mut branches = Source::from_iter([1, 2])
        .with_settings(settings)
        .broadcast(2)
        .unwrap();
    let slow = branches.pop().expect("slow branch");
    let fast = branches.pop().expect("fast branch");

    let fast_run = tokio::spawn(async move { fast.run_collect().await.unwrap() });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !fast_run.is_finished(),
        "fast branch should wait for slow live branch capacity"
    );

    let slow_run = tokio::spawn(async move { slow.run_collect().await.unwrap() });
    let (fast_values, slow_values) = tokio::join!(fast_run, slow_run);

    assert_eq!(fast_values.expect("fast task should finish"), vec![1, 2]);
    assert_eq!(slow_values.expect("slow task should finish"), vec![1, 2]);
}

#[tokio::test]
async fn stream_facade_fanout_broadcast_cancelled_branch_drops_out() {
    let settings = StreamRunSettings::new(8, 1).expect("valid stream settings");
    let mut branches = Source::from_iter([1, 2, 3])
        .with_settings(settings)
        .broadcast(2)
        .unwrap();
    let mut cancelled = branches.pop().expect("cancelled branch");
    let live = branches.pop().expect("live branch");

    cancelled.cancel("branch no longer needed");

    assert_eq!(live.run_collect().await.unwrap(), vec![1, 2, 3]);
}
