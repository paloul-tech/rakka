//! Stream facade vocabulary tests.

use rakka_stream::{Flow, Sink, Source, StreamError, StreamRunSettings, DEFAULT_BUFFER_CAPACITY};

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
