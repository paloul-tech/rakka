#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Bounded stream primitives shared by Rakka integration adapters.
//!
//! Rakka streams start with a small core vocabulary: typed stream errors,
//! bounded source/sink handles, explicit back-pressure, graceful drain, forced
//! close, and cancellation. Phase 6 adds Akka-shaped facade names such as
//! `Source`, `Flow`, and `Sink` while keeping the bounded runtime explicit.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex, MutexGuard};

use rakka_core::{
    MetricsRecorder, RakkaError, Subsystem, METRIC_STREAM_CANCELLATIONS, METRIC_STREAM_PRESSURE,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[cfg(feature = "adapters")]
mod adapters;
mod facade;
#[cfg(feature = "process-io")]
mod process_io;

#[cfg(feature = "adapters")]
pub use adapters::{
    broadcast_stream, fan_in_streams, pipe_stream, spawn_actor_source, ActorSink, ActorSinkError,
    ActorSinkResult, ActorSource, ActorSourceSpawnError, BroadcastError, BroadcastResult,
    EntitySink, EntitySinkError, EntitySinkResult, StreamPipeError, StreamPipeResult,
    StreamPipeSummary,
};
pub use facade::{Flow, RunnableStream, Sink, Source, StreamRunSettings};
#[cfg(feature = "process-io")]
pub use process_io::{
    managed_process_stderr_stream, managed_process_stdin_sink, managed_process_stdout_stream,
    process_input_sink_from_writer, process_output_stream_from_reader,
    protocol_actor_process_stream_unsupported, ManagedProcessStdinSink, ProcessInputSink,
    ProcessIoOwner, ProcessIoStream, ProcessOutputConfig, ProcessOutputStream, ProcessStreamError,
    ProcessStreamPump, ProcessStreamResult, DEFAULT_PROCESS_IO_CHUNK_SIZE,
};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-stream";

/// Default bounded buffer capacity for examples and tests.
pub const DEFAULT_BUFFER_CAPACITY: usize = 1024;

/// Convenient result alias for stream operations.
pub type StreamResult<T> = Result<T, StreamError>;

/// Failure returned by bounded stream lifecycle and back-pressure operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    /// A bounded stream was created with an unusable capacity.
    InvalidCapacity {
        /// Capacity value that was rejected.
        capacity: usize,
    },
    /// The stream is open but its bounded buffer is full.
    Full {
        /// Configured bounded buffer capacity.
        capacity: usize,
    },
    /// The stream is draining and no longer accepts new items.
    Draining,
    /// The stream was closed before the operation could complete.
    Closed,
    /// The stream was cancelled before the operation could complete.
    Cancelled {
        /// Optional human-readable cancellation reason.
        reason: Option<String>,
    },
}

impl StreamError {
    /// Converts this error to a framework error.
    #[must_use]
    pub fn into_rakka_error(self) -> RakkaError {
        RakkaError::new(Subsystem::Stream, self.code(), self.to_string())
    }

    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidCapacity { .. } => "invalid-capacity",
            Self::Full { .. } => "full",
            Self::Draining => "draining",
            Self::Closed => "closed",
            Self::Cancelled { .. } => "cancelled",
        }
    }

    /// Returns true when the error represents a terminal stream state.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::InvalidCapacity { .. } | Self::Closed | Self::Cancelled { .. }
        )
    }
}

impl Display for StreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity { capacity } => {
                write!(f, "stream capacity must be greater than zero: {capacity}")
            }
            Self::Full { capacity } => write!(f, "stream buffer is full at capacity {capacity}"),
            Self::Draining => f.write_str("stream is draining"),
            Self::Closed => f.write_str("stream is closed"),
            Self::Cancelled { reason } => match reason {
                Some(reason) => write!(f, "stream was cancelled: {reason}"),
                None => f.write_str("stream was cancelled"),
            },
        }
    }
}

impl Error for StreamError {}

/// Send failure that returns ownership of the item that could not be accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSendError<T> {
    error: StreamError,
    item: T,
}

impl<T> StreamSendError<T> {
    /// Creates a send error from a stream error and rejected item.
    #[must_use]
    pub fn new(error: StreamError, item: T) -> Self {
        Self { error, item }
    }

    /// Stream error that prevented the send from completing.
    #[must_use]
    pub const fn error(&self) -> &StreamError {
        &self.error
    }

    /// Item that was rejected by the stream.
    #[must_use]
    pub const fn item(&self) -> &T {
        &self.item
    }

    /// Consumes this error and returns the rejected item.
    #[must_use]
    pub fn into_item(self) -> T {
        self.item
    }

    /// Consumes this error and returns both the stream error and rejected item.
    #[must_use]
    pub fn into_parts(self) -> (StreamError, T) {
        (self.error, self.item)
    }
}

impl<T> Display for StreamSendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.error, f)
    }
}

/// Observable stream lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamLifecycle {
    /// The stream accepts new items and consumers can receive them.
    Open,
    /// The stream rejects new items while buffered items flush.
    Draining,
    /// The stream drained all buffered items and completed normally.
    Completed,
    /// The stream was closed immediately and buffered items were dropped.
    Closed,
    /// The stream was cancelled immediately and buffered items were dropped.
    Cancelled,
}

impl StreamLifecycle {
    /// Returns true when this lifecycle no longer accepts items.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Closed | Self::Cancelled)
    }

    /// Stable lifecycle label used for metrics and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Draining => "draining",
            Self::Completed => "completed",
            Self::Closed => "closed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Snapshot of a bounded stream's lifecycle and buffer state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamStatus {
    capacity: usize,
    depth: usize,
    lifecycle: StreamLifecycle,
    dropped_items: usize,
    cancel_reason: Option<String>,
}

impl StreamStatus {
    /// Configured bounded buffer capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Number of buffered items currently available to consumers.
    #[must_use]
    pub const fn depth(&self) -> usize {
        self.depth
    }

    /// Current stream lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> StreamLifecycle {
        self.lifecycle
    }

    /// Number of buffered items dropped by close or cancellation.
    #[must_use]
    pub const fn dropped_items(&self) -> usize {
        self.dropped_items
    }

    /// Optional cancellation reason when the stream lifecycle is cancelled.
    #[must_use]
    pub fn cancel_reason(&self) -> Option<&str> {
        self.cancel_reason.as_deref()
    }

    /// Ratio of buffered items to configured capacity.
    #[must_use]
    pub fn pressure(&self) -> f64 {
        if self.capacity == 0 {
            0.0
        } else {
            self.depth as f64 / self.capacity as f64
        }
    }

    /// Records stream pressure and cancellation metrics for this status.
    pub fn record_metrics(&self, recorder: &dyn MetricsRecorder, stream_name: &str) {
        let span = tracing::debug_span!(
            target: "rakka.stream",
            "stream.pipeline",
            stream = stream_name,
            lifecycle = self.lifecycle.as_str(),
            capacity = self.capacity,
            depth = self.depth
        );
        let _entered = span.enter();
        let capacity = self.capacity.to_string();
        let depth = self.depth.to_string();
        recorder.record_gauge(
            METRIC_STREAM_PRESSURE,
            self.pressure(),
            &[
                ("stream", stream_name),
                ("lifecycle", self.lifecycle.as_str()),
                ("capacity", capacity.as_str()),
                ("depth", depth.as_str()),
            ],
        );

        if self.lifecycle == StreamLifecycle::Cancelled {
            recorder.increment_counter(
                METRIC_STREAM_CANCELLATIONS,
                1,
                &[
                    ("stream", stream_name),
                    ("reason", self.cancel_reason().unwrap_or("unspecified")),
                ],
            );
        }
    }
}

/// Stable telemetry labels emitted by stream adapters and instrumentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamTelemetryLabel {
    /// A producer observed bounded-buffer pressure.
    Pressure,
    /// An item entered the bounded buffer.
    ItemEnqueued,
    /// An item left the bounded buffer.
    ItemDequeued,
    /// The stream entered graceful drain.
    Draining,
    /// The stream completed normally after draining.
    Completed,
    /// The stream was closed immediately.
    Closed,
    /// The stream was cancelled.
    Cancelled,
    /// Buffered items were dropped.
    DroppedItems,
}

impl StreamTelemetryLabel {
    /// Stable label used for metrics and tracing attributes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pressure => "stream.pressure",
            Self::ItemEnqueued => "stream.item.enqueued",
            Self::ItemDequeued => "stream.item.dequeued",
            Self::Draining => "stream.draining",
            Self::Completed => "stream.completed",
            Self::Closed => "stream.closed",
            Self::Cancelled => "stream.cancelled",
            Self::DroppedItems => "stream.dropped_items",
        }
    }
}

impl Display for StreamTelemetryLabel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Bounded source/sink pair for stream items.
pub struct BoundedStream<T> {
    sink: StreamSink<T>,
    source: StreamSource<T>,
}

impl<T> fmt::Debug for BoundedStream<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedStream")
            .field("status", &self.sink.status())
            .finish()
    }
}

impl<T> BoundedStream<T> {
    /// Creates a bounded stream with the requested capacity.
    pub fn new(capacity: usize) -> StreamResult<Self> {
        let (sink, source) = bounded_channel(capacity)?;
        Ok(Self { sink, source })
    }

    /// Splits this bounded stream into producer and consumer handles.
    #[must_use]
    pub fn split(self) -> (StreamSink<T>, StreamSource<T>) {
        (self.sink, self.source)
    }
}

/// Creates a bounded stream sink/source pair.
pub fn bounded_channel<T>(capacity: usize) -> StreamResult<(StreamSink<T>, StreamSource<T>)> {
    if capacity == 0 {
        return Err(StreamError::InvalidCapacity { capacity });
    }

    let inner = Arc::new(Inner::new(capacity));
    Ok((
        StreamSink {
            inner: Arc::clone(&inner),
        },
        StreamSource { inner },
    ))
}

/// Producer handle for a bounded stream.
pub struct StreamSink<T> {
    inner: Arc<Inner<T>>,
}

impl<T> fmt::Debug for StreamSink<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamSink")
            .field("status", &self.status())
            .finish()
    }
}

impl<T> Clone for StreamSink<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> StreamSink<T> {
    /// Configured bounded buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.status().capacity()
    }

    /// Returns a point-in-time lifecycle and buffer snapshot.
    #[must_use]
    pub fn status(&self) -> StreamStatus {
        self.inner.status()
    }

    /// Attempts to enqueue an item without waiting for capacity.
    pub fn try_send(&self, item: T) -> Result<(), StreamSendError<T>> {
        let result = {
            let mut state = self.inner.lock();
            state.try_push(item)
        };

        if result.is_ok() {
            self.inner.readable.notify_one();
        }

        result
    }

    /// Enqueues an item, waiting until bounded capacity is available.
    pub async fn send(&self, mut item: T) -> Result<(), StreamSendError<T>> {
        loop {
            let notified = self.inner.writable.notified();

            let result = {
                let mut state = self.inner.lock();
                state.try_push(item)
            };

            match result {
                Ok(()) => {
                    self.inner.readable.notify_one();
                    return Ok(());
                }
                Err(error) if matches!(error.error(), StreamError::Full { .. }) => {
                    item = error.into_item();
                }
                Err(error) => return Err(error),
            }

            notified.await;
        }
    }

    /// Starts graceful drain and rejects future sends.
    pub fn drain(&self) -> StreamResult<()> {
        self.inner.drain()
    }

    /// Closes the stream immediately, dropping buffered items.
    ///
    /// Returns the number of buffered items dropped by the close.
    pub fn close(&self) -> usize {
        self.inner.close()
    }

    /// Cancels the stream immediately, dropping buffered items.
    ///
    /// Returns the number of buffered items dropped by cancellation.
    pub fn cancel(&self, reason: impl Into<String>) -> usize {
        self.inner.cancel(Some(reason.into()))
    }
}

/// Consumer handle for a bounded stream.
pub struct StreamSource<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for StreamSource<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> fmt::Debug for StreamSource<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamSource")
            .field("status", &self.status())
            .finish()
    }
}

impl<T> StreamSource<T> {
    /// Configured bounded buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.status().capacity()
    }

    /// Returns a point-in-time lifecycle and buffer snapshot.
    #[must_use]
    pub fn status(&self) -> StreamStatus {
        self.inner.status()
    }

    /// Receives the next item, normal completion, or a terminal lifecycle error.
    pub async fn next(&self) -> StreamResult<Option<T>> {
        loop {
            let notified = self.inner.readable.notified();
            let next = {
                let mut state = self.inner.lock();
                state.pop_next()
            };

            match next {
                NextItem::Item {
                    item,
                    completed_drain,
                } => {
                    self.inner.writable.notify_one();
                    if completed_drain {
                        self.inner.notify_all();
                    }
                    return Ok(Some(item));
                }
                NextItem::Completed => return Ok(None),
                NextItem::Closed => return Err(StreamError::Closed),
                NextItem::Cancelled { reason } => {
                    return Err(StreamError::Cancelled { reason });
                }
                NextItem::Pending => notified.await,
            }
        }
    }

    /// Starts graceful drain and rejects future sends.
    pub fn drain(&self) -> StreamResult<()> {
        self.inner.drain()
    }

    /// Closes the stream immediately, dropping buffered items.
    ///
    /// Returns the number of buffered items dropped by the close.
    pub fn close(&self) -> usize {
        self.inner.close()
    }

    /// Cancels the stream immediately, dropping buffered items.
    ///
    /// Returns the number of buffered items dropped by cancellation.
    pub fn cancel(&self, reason: impl Into<String>) -> usize {
        self.inner.cancel(Some(reason.into()))
    }
}

struct Inner<T> {
    state: Mutex<State<T>>,
    readable: Notify,
    writable: Notify,
}

impl<T> Inner<T> {
    fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(State::new(capacity)),
            readable: Notify::new(),
            writable: Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn status(&self) -> StreamStatus {
        self.lock().status()
    }

    fn drain(&self) -> StreamResult<()> {
        {
            let mut state = self.lock();
            state.start_drain()?;
        }

        self.notify_all();
        Ok(())
    }

    fn close(&self) -> usize {
        let dropped = {
            let mut state = self.lock();
            state.close()
        };

        self.notify_all();
        dropped
    }

    fn cancel(&self, reason: Option<String>) -> usize {
        let dropped = {
            let mut state = self.lock();
            state.cancel(reason)
        };

        self.notify_all();
        dropped
    }

    fn notify_all(&self) {
        self.readable.notify_waiters();
        self.writable.notify_waiters();
    }
}

struct State<T> {
    capacity: usize,
    lifecycle: Lifecycle,
    queue: VecDeque<T>,
    dropped_items: usize,
}

impl<T> State<T> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            lifecycle: Lifecycle::Open,
            queue: VecDeque::with_capacity(capacity),
            dropped_items: 0,
        }
    }

    fn try_push(&mut self, item: T) -> Result<(), StreamSendError<T>> {
        if let Some(error) = self.send_error() {
            return Err(StreamSendError::new(error, item));
        }

        if self.queue.len() >= self.capacity {
            return Err(StreamSendError::new(
                StreamError::Full {
                    capacity: self.capacity,
                },
                item,
            ));
        }

        self.queue.push_back(item);
        Ok(())
    }

    fn pop_next(&mut self) -> NextItem<T> {
        if let Some(item) = self.queue.pop_front() {
            if self.queue.is_empty() && matches!(self.lifecycle, Lifecycle::Draining) {
                self.lifecycle = Lifecycle::Completed;
                return NextItem::Item {
                    item,
                    completed_drain: true,
                };
            }

            return NextItem::Item {
                item,
                completed_drain: false,
            };
        }

        match &self.lifecycle {
            Lifecycle::Open => NextItem::Pending,
            Lifecycle::Draining => {
                self.lifecycle = Lifecycle::Completed;
                NextItem::Completed
            }
            Lifecycle::Completed => NextItem::Completed,
            Lifecycle::Closed => NextItem::Closed,
            Lifecycle::Cancelled { reason } => NextItem::Cancelled {
                reason: reason.clone(),
            },
        }
    }

    fn start_drain(&mut self) -> StreamResult<()> {
        match &self.lifecycle {
            Lifecycle::Open => {
                self.lifecycle = if self.queue.is_empty() {
                    Lifecycle::Completed
                } else {
                    Lifecycle::Draining
                };
                Ok(())
            }
            Lifecycle::Draining | Lifecycle::Completed => Ok(()),
            Lifecycle::Closed => Err(StreamError::Closed),
            Lifecycle::Cancelled { reason } => Err(StreamError::Cancelled {
                reason: reason.clone(),
            }),
        }
    }

    fn close(&mut self) -> usize {
        let dropped = self.drop_buffered_items();
        self.lifecycle = Lifecycle::Closed;
        dropped
    }

    fn cancel(&mut self, reason: Option<String>) -> usize {
        let dropped = self.drop_buffered_items();
        self.lifecycle = Lifecycle::Cancelled { reason };
        dropped
    }

    fn drop_buffered_items(&mut self) -> usize {
        let dropped = self.queue.len();
        self.queue.clear();
        self.dropped_items = self.dropped_items.saturating_add(dropped);
        dropped
    }

    fn status(&self) -> StreamStatus {
        StreamStatus {
            capacity: self.capacity,
            depth: self.queue.len(),
            lifecycle: self.lifecycle.status(),
            dropped_items: self.dropped_items,
            cancel_reason: self.lifecycle.cancel_reason().map(ToOwned::to_owned),
        }
    }

    fn send_error(&self) -> Option<StreamError> {
        match &self.lifecycle {
            Lifecycle::Open => None,
            Lifecycle::Draining => Some(StreamError::Draining),
            Lifecycle::Completed | Lifecycle::Closed => Some(StreamError::Closed),
            Lifecycle::Cancelled { reason } => Some(StreamError::Cancelled {
                reason: reason.clone(),
            }),
        }
    }
}

enum NextItem<T> {
    Item { item: T, completed_drain: bool },
    Completed,
    Closed,
    Cancelled { reason: Option<String> },
    Pending,
}

enum Lifecycle {
    Open,
    Draining,
    Completed,
    Closed,
    Cancelled { reason: Option<String> },
}

impl Lifecycle {
    fn status(&self) -> StreamLifecycle {
        match self {
            Self::Open => StreamLifecycle::Open,
            Self::Draining => StreamLifecycle::Draining,
            Self::Completed => StreamLifecycle::Completed,
            Self::Closed => StreamLifecycle::Closed,
            Self::Cancelled { .. } => StreamLifecycle::Cancelled,
        }
    }

    fn cancel_reason(&self) -> Option<&str> {
        match self {
            Self::Cancelled { reason } => reason.as_deref(),
            Self::Open | Self::Draining | Self::Completed | Self::Closed => None,
        }
    }
}

/// Subsystem associated with streams.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Stream
}
