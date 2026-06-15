//! Coordinated shutdown helpers for bounded streams.

use rakka_core::{
    CoordinatedShutdown, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
};

use crate::{StreamError, StreamResult, StreamSink, StreamSource};

/// Registers a drain task for a bounded stream sink.
///
/// ```no_run
/// use rakka_core::CoordinatedShutdown;
/// use rakka_stream::{bounded_channel, register_stream_sink_drain};
///
/// # fn example() -> rakka_core::RakkaResult<()> {
/// let shutdown = CoordinatedShutdown::new();
/// let (sink, _source) =
///     bounded_channel::<String>(16).map_err(rakka_stream::StreamError::into_rakka_error)?;
/// register_stream_sink_drain(&shutdown, "drain-orders-stream", sink)?;
/// # Ok(()) }
/// ```
pub fn register_stream_sink_drain<T>(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    sink: StreamSink<T>,
) -> RakkaResult<ShutdownTask>
where
    T: Send + 'static,
{
    shutdown.add_task_with_options(
        ShutdownPhase::drain_adapters(),
        task_name,
        ShutdownTaskOptions::default(),
        move |_context| {
            let sink = sink.clone();
            async move { stream_drain_result(sink.drain()) }
        },
    )
}

/// Registers a drain task for a bounded stream source.
///
/// ```no_run
/// use rakka_core::CoordinatedShutdown;
/// use rakka_stream::{bounded_channel, register_stream_source_drain};
///
/// # fn example() -> rakka_core::RakkaResult<()> {
/// let shutdown = CoordinatedShutdown::new();
/// let (_sink, source) =
///     bounded_channel::<String>(16).map_err(rakka_stream::StreamError::into_rakka_error)?;
/// register_stream_source_drain(&shutdown, "drain-orders-source", source)?;
/// # Ok(()) }
/// ```
pub fn register_stream_source_drain<T>(
    shutdown: &CoordinatedShutdown,
    task_name: impl Into<String>,
    source: StreamSource<T>,
) -> RakkaResult<ShutdownTask>
where
    T: Send + 'static,
{
    shutdown.add_task_with_options(
        ShutdownPhase::drain_adapters(),
        task_name,
        ShutdownTaskOptions::default(),
        move |_context| {
            let source = source.clone();
            async move { stream_drain_result(source.drain()) }
        },
    )
}

fn stream_drain_result(result: StreamResult<()>) -> RakkaResult<()> {
    match result {
        Ok(()) | Err(StreamError::Closed) | Err(StreamError::Cancelled { .. }) => Ok(()),
        Err(error) => Err(error.into_rakka_error()),
    }
}
