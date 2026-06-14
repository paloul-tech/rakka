//! Stream facade vocabulary tests.

use std::sync::{Arc, Mutex};

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
