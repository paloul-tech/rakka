//! Coordinated shutdown helpers for bounded streams.

use rakka_core::{
    CoordinatedShutdown, RakkaResult, ShutdownPhase, ShutdownTask, ShutdownTaskOptions,
};

use crate::{StreamError, StreamResult, StreamSink, StreamSource};

/// Registers a drain task for a bounded stream sink.
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
