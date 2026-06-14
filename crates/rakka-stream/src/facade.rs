//! Akka-shaped stream facade vocabulary over Rakka's bounded stream runtime.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::{
    bounded_channel, StreamError, StreamResult, StreamSendError, StreamSink, StreamSource,
    DEFAULT_BUFFER_CAPACITY,
};

/// Result returned by materialized stream facade runs.
pub type StreamRunResult<T, M> = Result<M, StreamRunError<T>>;

/// Failure returned while materializing a stream facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamRunError<T> {
    /// The source side terminated with a stream lifecycle error.
    Source {
        /// Source lifecycle failure.
        error: StreamError,
    },
    /// The sink side rejected an item.
    Sink {
        /// Sink send failure, including the rejected item.
        error: StreamSendError<T>,
    },
}

impl<T> StreamRunError<T> {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Source { .. } => "source-error",
            Self::Sink { .. } => "sink-error",
        }
    }

    /// Returns the source lifecycle error when this is a source failure.
    #[must_use]
    pub const fn source_error(&self) -> Option<&StreamError> {
        match self {
            Self::Source { error } => Some(error),
            Self::Sink { .. } => None,
        }
    }

    /// Returns the sink send error when this is a sink failure.
    #[must_use]
    pub const fn sink_error(&self) -> Option<&StreamSendError<T>> {
        match self {
            Self::Source { .. } => None,
            Self::Sink { error } => Some(error),
        }
    }
}

impl<T> Display for StreamRunError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source { error } => write!(f, "stream source failed: {error}"),
            Self::Sink { error } => write!(f, "stream sink failed: {error}"),
        }
    }
}

impl<T> Error for StreamRunError<T> where T: Debug {}

/// Materialization settings shared by stream facade sources, flows, and sinks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRunSettings {
    default_buffer_capacity: usize,
    operator_buffer_capacity: usize,
    stream_name: Option<String>,
    cancellation_reason: String,
}

impl StreamRunSettings {
    /// Creates run settings with explicit bounded capacities.
    pub fn new(
        default_buffer_capacity: usize,
        operator_buffer_capacity: usize,
    ) -> StreamResult<Self> {
        validate_capacity(default_buffer_capacity)?;
        validate_capacity(operator_buffer_capacity)?;
        Ok(Self {
            default_buffer_capacity,
            operator_buffer_capacity,
            stream_name: None,
            cancellation_reason: "stream cancelled".to_string(),
        })
    }

    /// Bounded capacity used by source and sink boundaries unless overridden.
    #[must_use]
    pub const fn default_buffer_capacity(&self) -> usize {
        self.default_buffer_capacity
    }

    /// Bounded capacity used by operator output buffers unless overridden.
    #[must_use]
    pub const fn operator_buffer_capacity(&self) -> usize {
        self.operator_buffer_capacity
    }

    /// Optional stream name used for diagnostics and metrics.
    #[must_use]
    pub fn stream_name(&self) -> Option<&str> {
        self.stream_name.as_deref()
    }

    /// Default cancellation reason used by facade materialization.
    #[must_use]
    pub fn cancellation_reason(&self) -> &str {
        &self.cancellation_reason
    }

    /// Sets the default bounded buffer capacity.
    pub fn with_default_buffer_capacity(mut self, capacity: usize) -> StreamResult<Self> {
        validate_capacity(capacity)?;
        self.default_buffer_capacity = capacity;
        Ok(self)
    }

    /// Sets the operator output bounded buffer capacity.
    pub fn with_operator_buffer_capacity(mut self, capacity: usize) -> StreamResult<Self> {
        validate_capacity(capacity)?;
        self.operator_buffer_capacity = capacity;
        Ok(self)
    }

    /// Sets a stream name used for diagnostics and metrics.
    #[must_use]
    pub fn with_stream_name(mut self, stream_name: impl Into<String>) -> Self {
        self.stream_name = Some(stream_name.into());
        self
    }

    /// Clears the stream name.
    #[must_use]
    pub fn without_stream_name(mut self) -> Self {
        self.stream_name = None;
        self
    }

    /// Sets the default cancellation reason used by facade materialization.
    #[must_use]
    pub fn with_cancellation_reason(mut self, reason: impl Into<String>) -> Self {
        self.cancellation_reason = reason.into();
        self
    }
}

impl Default for StreamRunSettings {
    fn default() -> Self {
        Self {
            default_buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            operator_buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            stream_name: None,
            cancellation_reason: "stream cancelled".to_string(),
        }
    }
}

/// Finite or live source of stream elements.
pub struct Source<T> {
    shape: SourceShape<T>,
    settings: StreamRunSettings,
}

impl<T> Source<T> {
    /// Creates an empty source facade.
    #[must_use]
    pub fn empty() -> Self {
        Self::empty_with_settings(StreamRunSettings::default())
    }

    /// Creates an empty source facade with explicit run settings.
    #[must_use]
    pub fn empty_with_settings(settings: StreamRunSettings) -> Self {
        Self {
            shape: SourceShape::Empty,
            settings,
        }
    }

    /// Creates a source with one element.
    #[must_use]
    pub fn single(item: T) -> Self {
        let mut items = VecDeque::with_capacity(1);
        items.push_back(item);
        Self {
            shape: SourceShape::Items(items),
            settings: StreamRunSettings::default(),
        }
    }

    /// Creates a facade source from a low-level bounded stream source.
    #[must_use]
    pub fn from_stream_source(source: StreamSource<T>) -> Self {
        Self {
            shape: SourceShape::StreamSource(source),
            settings: StreamRunSettings::default(),
        }
    }

    /// Creates a bounded queue source and returns its producer sink.
    pub fn queue(capacity: usize) -> StreamResult<(StreamSink<T>, Self)> {
        let (sink, source) = bounded_channel(capacity)?;
        Ok((sink, Self::from_stream_source(source)))
    }

    /// Alias for `queue`, emphasizing that the source boundary is bounded.
    pub fn bounded(capacity: usize) -> StreamResult<(StreamSink<T>, Self)> {
        Self::queue(capacity)
    }

    /// Returns true when this source is the empty source vocabulary shape.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        matches!(self.shape, SourceShape::Empty)
    }

    /// Run settings associated with this source.
    #[must_use]
    pub const fn settings(&self) -> &StreamRunSettings {
        &self.settings
    }

    /// Returns this source with updated run settings.
    #[must_use]
    pub fn with_settings(mut self, settings: StreamRunSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Connects this source to a sink, producing a runnable stream descriptor.
    #[must_use]
    pub fn to<M>(self, sink: Sink<T, M>) -> RunnableStream<T, M> {
        RunnableStream { source: self, sink }
    }

    /// Runs this source with a sink.
    pub async fn run_with<M>(mut self, mut sink: Sink<T, M>) -> StreamRunResult<T, M>
    where
        T: Send + 'static,
        M: Send + 'static,
    {
        while let Some(item) = self.next_item().await? {
            sink.consume(item).await?;
        }
        Ok(sink.finish())
    }

    /// Collects this source into a vector.
    pub async fn run_collect(self) -> StreamRunResult<T, Vec<T>>
    where
        T: Send + 'static,
    {
        self.run_with(Sink::collect()).await
    }

    /// Runs a callback for every source item.
    pub async fn run_foreach<F>(self, callback: F) -> StreamRunResult<T, ()>
    where
        T: Send + 'static,
        F: FnMut(T) + Send + 'static,
    {
        self.run_with(Sink::foreach(callback)).await
    }

    async fn next_item(&mut self) -> StreamRunResult<T, Option<T>> {
        match &mut self.shape {
            SourceShape::Empty => Ok(None),
            SourceShape::Items(items) => Ok(items.pop_front()),
            SourceShape::StreamSource(source) => source
                .next()
                .await
                .map_err(|error| StreamRunError::Source { error }),
        }
    }
}

impl<T> FromIterator<T> for Source<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        Self {
            shape: SourceShape::Items(iter.into_iter().collect()),
            settings: StreamRunSettings::default(),
        }
    }
}

impl<T> Debug for Source<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Source")
            .field("shape", &self.shape)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// Single-input single-output stream transformation.
#[derive(Clone)]
pub struct Flow<I, O> {
    shape: FlowShape,
    settings: StreamRunSettings,
    _input: PhantomData<fn() -> I>,
    _output: PhantomData<fn() -> O>,
}

impl<T> Flow<T, T> {
    /// Creates an identity flow facade.
    #[must_use]
    pub fn identity() -> Self {
        Self::identity_with_settings(StreamRunSettings::default())
    }

    /// Creates an identity flow facade with explicit run settings.
    #[must_use]
    pub const fn identity_with_settings(settings: StreamRunSettings) -> Self {
        Self {
            shape: FlowShape::Identity,
            settings,
            _input: PhantomData,
            _output: PhantomData,
        }
    }
}

impl<I, O> Flow<I, O> {
    /// Run settings associated with this flow.
    #[must_use]
    pub const fn settings(&self) -> &StreamRunSettings {
        &self.settings
    }

    /// Returns a copy of this flow with updated run settings.
    #[must_use]
    pub fn with_settings(mut self, settings: StreamRunSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Returns true when this flow is the identity vocabulary shape.
    #[must_use]
    pub const fn is_identity(&self) -> bool {
        matches!(self.shape, FlowShape::Identity)
    }
}

impl<I, O> Debug for Flow<I, O> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flow")
            .field("shape", &self.shape)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// Terminal stream consumer that materializes a value of type `M`.
pub struct Sink<T, M> {
    stage: Box<dyn SinkStage<T, M> + Send>,
    kind: SinkKind,
    settings: StreamRunSettings,
}

impl<T> Sink<T, ()>
where
    T: Send + 'static,
{
    /// Creates a sink facade that ignores all elements.
    #[must_use]
    pub fn ignore() -> Self {
        Self::ignore_with_settings(StreamRunSettings::default())
    }

    /// Creates an ignore sink facade with explicit run settings.
    #[must_use]
    pub fn ignore_with_settings(settings: StreamRunSettings) -> Self {
        Self {
            stage: Box::new(IgnoreStage),
            kind: SinkKind::Ignore,
            settings,
        }
    }

    /// Creates a sink facade that invokes a callback for every element.
    #[must_use]
    pub fn foreach<F>(callback: F) -> Self
    where
        F: FnMut(T) + Send + 'static,
    {
        Self {
            stage: Box::new(ForeachStage {
                callback,
                _item: PhantomData,
            }),
            kind: SinkKind::Foreach,
            settings: StreamRunSettings::default(),
        }
    }
}

impl<T> Sink<T, Vec<T>>
where
    T: Send + 'static,
{
    /// Creates a sink facade that collects all elements into a vector.
    #[must_use]
    pub fn collect() -> Self {
        Self {
            stage: Box::new(CollectStage { items: Vec::new() }),
            kind: SinkKind::Collect,
            settings: StreamRunSettings::default(),
        }
    }
}

impl<T> Sink<T, usize>
where
    T: Send + 'static,
{
    /// Creates a sink facade that forwards elements into a low-level stream sink.
    #[must_use]
    pub fn from_stream_sink(sink: StreamSink<T>) -> Self {
        Self {
            stage: Box::new(StreamSinkStage { sink, count: 0 }),
            kind: SinkKind::StreamSink,
            settings: StreamRunSettings::default(),
        }
    }
}

impl<T, M> Sink<T, M>
where
    T: Send + 'static,
    M: Send + 'static,
{
    /// Creates a sink facade that folds all elements into one materialized value.
    #[must_use]
    pub fn fold<F>(initial: M, folder: F) -> Self
    where
        F: FnMut(M, T) -> M + Send + 'static,
    {
        Self {
            stage: Box::new(FoldStage {
                state: Some(initial),
                folder,
                _item: PhantomData,
            }),
            kind: SinkKind::Fold,
            settings: StreamRunSettings::default(),
        }
    }
}

impl<T, M> Sink<T, M> {
    /// Run settings associated with this sink.
    #[must_use]
    pub const fn settings(&self) -> &StreamRunSettings {
        &self.settings
    }

    /// Returns this sink with updated run settings.
    #[must_use]
    pub fn with_settings(mut self, settings: StreamRunSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Returns true when this sink is the ignore vocabulary shape.
    #[must_use]
    pub const fn is_ignore(&self) -> bool {
        matches!(self.kind, SinkKind::Ignore)
    }
}

impl<T, M> Sink<T, M>
where
    T: Send + 'static,
    M: Send + 'static,
{
    async fn consume(&mut self, item: T) -> StreamRunResult<T, ()> {
        self.stage.consume(item).await
    }

    fn finish(self) -> M {
        self.stage.finish()
    }
}

impl<T, M> Debug for Sink<T, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sink")
            .field("kind", &self.kind)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// Descriptor for a source connected to a sink.
pub struct RunnableStream<T, M> {
    source: Source<T>,
    sink: Sink<T, M>,
}

impl<T, M> RunnableStream<T, M>
where
    T: Send + 'static,
    M: Send + 'static,
{
    /// Run settings inherited from the source side of this stream.
    #[must_use]
    pub const fn source_settings(&self) -> &StreamRunSettings {
        self.source.settings()
    }

    /// Run settings inherited from the sink side of this stream.
    #[must_use]
    pub const fn sink_settings(&self) -> &StreamRunSettings {
        self.sink.settings()
    }

    /// Runs the connected source and sink.
    pub async fn run(self) -> StreamRunResult<T, M> {
        self.source.run_with(self.sink).await
    }
}

impl<T, M> Debug for RunnableStream<T, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnableStream")
            .field("source_settings", self.source.settings())
            .field("sink_settings", self.sink.settings())
            .finish_non_exhaustive()
    }
}

enum SourceShape<T> {
    Empty,
    Items(VecDeque<T>),
    StreamSource(StreamSource<T>),
}

impl<T> Debug for SourceShape<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Empty"),
            Self::Items(items) => f.debug_struct("Items").field("len", &items.len()).finish(),
            Self::StreamSource(source) => f
                .debug_struct("StreamSource")
                .field("status", &source.status())
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlowShape {
    Identity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkKind {
    Ignore,
    Collect,
    Foreach,
    Fold,
    StreamSink,
}

trait SinkStage<T, M> {
    fn consume<'a>(
        &'a mut self,
        item: T,
    ) -> Pin<Box<dyn Future<Output = StreamRunResult<T, ()>> + Send + 'a>>;

    fn finish(self: Box<Self>) -> M;
}

struct IgnoreStage;

impl<T> SinkStage<T, ()> for IgnoreStage
where
    T: Send + 'static,
{
    fn consume<'a>(
        &'a mut self,
        _item: T,
    ) -> Pin<Box<dyn Future<Output = StreamRunResult<T, ()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn finish(self: Box<Self>) {}
}

struct CollectStage<T> {
    items: Vec<T>,
}

impl<T> SinkStage<T, Vec<T>> for CollectStage<T>
where
    T: Send + 'static,
{
    fn consume<'a>(
        &'a mut self,
        item: T,
    ) -> Pin<Box<dyn Future<Output = StreamRunResult<T, ()>> + Send + 'a>> {
        self.items.push(item);
        Box::pin(async { Ok(()) })
    }

    fn finish(self: Box<Self>) -> Vec<T> {
        self.items
    }
}

struct ForeachStage<T, F>
where
    F: FnMut(T),
{
    callback: F,
    _item: PhantomData<fn() -> T>,
}

impl<T, F> SinkStage<T, ()> for ForeachStage<T, F>
where
    T: Send + 'static,
    F: FnMut(T) + Send + 'static,
{
    fn consume<'a>(
        &'a mut self,
        item: T,
    ) -> Pin<Box<dyn Future<Output = StreamRunResult<T, ()>> + Send + 'a>> {
        (self.callback)(item);
        Box::pin(async { Ok(()) })
    }

    fn finish(self: Box<Self>) {}
}

struct FoldStage<T, M, F>
where
    F: FnMut(M, T) -> M,
{
    state: Option<M>,
    folder: F,
    _item: PhantomData<fn() -> T>,
}

impl<T, M, F> SinkStage<T, M> for FoldStage<T, M, F>
where
    T: Send + 'static,
    M: Send + 'static,
    F: FnMut(M, T) -> M + Send + 'static,
{
    fn consume<'a>(
        &'a mut self,
        item: T,
    ) -> Pin<Box<dyn Future<Output = StreamRunResult<T, ()>> + Send + 'a>> {
        let state = self
            .state
            .take()
            .expect("fold sink state should exist until finish");
        self.state = Some((self.folder)(state, item));
        Box::pin(async { Ok(()) })
    }

    fn finish(mut self: Box<Self>) -> M {
        self.state
            .take()
            .expect("fold sink state should exist at finish")
    }
}

struct StreamSinkStage<T> {
    sink: StreamSink<T>,
    count: usize,
}

impl<T> SinkStage<T, usize> for StreamSinkStage<T>
where
    T: Send + 'static,
{
    fn consume<'a>(
        &'a mut self,
        item: T,
    ) -> Pin<Box<dyn Future<Output = StreamRunResult<T, ()>> + Send + 'a>> {
        Box::pin(async move {
            self.sink
                .send(item)
                .await
                .map_err(|error| StreamRunError::Sink { error })?;
            self.count = self.count.saturating_add(1);
            Ok(())
        })
    }

    fn finish(self: Box<Self>) -> usize {
        self.count
    }
}

fn validate_capacity(capacity: usize) -> StreamResult<()> {
    if capacity == 0 {
        Err(StreamError::InvalidCapacity { capacity })
    } else {
        Ok(())
    }
}
