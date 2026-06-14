//! Akka-shaped stream facade vocabulary over Rakka's bounded stream runtime.

use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;

use crate::{StreamError, StreamResult, DEFAULT_BUFFER_CAPACITY};

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
#[derive(Clone)]
pub struct Source<T> {
    shape: SourceShape,
    settings: StreamRunSettings,
    _item: PhantomData<fn() -> T>,
}

impl<T> Source<T> {
    /// Creates an empty source facade.
    #[must_use]
    pub fn empty() -> Self {
        Self::empty_with_settings(StreamRunSettings::default())
    }

    /// Creates an empty source facade with explicit run settings.
    #[must_use]
    pub const fn empty_with_settings(settings: StreamRunSettings) -> Self {
        Self {
            shape: SourceShape::Empty,
            settings,
            _item: PhantomData,
        }
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

    /// Returns a copy of this source with updated run settings.
    #[must_use]
    pub fn with_settings(mut self, settings: StreamRunSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Connects this source to a sink, producing a runnable stream descriptor.
    #[must_use]
    pub fn to<M>(self, sink: Sink<T, M>) -> RunnableStream<M> {
        RunnableStream {
            source_settings: self.settings,
            sink_settings: sink.settings,
            _materialized: PhantomData,
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
#[derive(Clone)]
pub struct Sink<T, M> {
    shape: SinkShape,
    settings: StreamRunSettings,
    _item: PhantomData<fn() -> T>,
    _materialized: PhantomData<fn() -> M>,
}

impl<T> Sink<T, ()> {
    /// Creates a sink facade that ignores all elements.
    #[must_use]
    pub fn ignore() -> Self {
        Self::ignore_with_settings(StreamRunSettings::default())
    }

    /// Creates an ignore sink facade with explicit run settings.
    #[must_use]
    pub const fn ignore_with_settings(settings: StreamRunSettings) -> Self {
        Self {
            shape: SinkShape::Ignore,
            settings,
            _item: PhantomData,
            _materialized: PhantomData,
        }
    }
}

impl<T, M> Sink<T, M> {
    /// Run settings associated with this sink.
    #[must_use]
    pub const fn settings(&self) -> &StreamRunSettings {
        &self.settings
    }

    /// Returns a copy of this sink with updated run settings.
    #[must_use]
    pub fn with_settings(mut self, settings: StreamRunSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Returns true when this sink is the ignore vocabulary shape.
    #[must_use]
    pub const fn is_ignore(&self) -> bool {
        matches!(self.shape, SinkShape::Ignore)
    }
}

impl<T, M> Debug for Sink<T, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sink")
            .field("shape", &self.shape)
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}

/// Descriptor for a source connected to a sink.
#[derive(Clone)]
pub struct RunnableStream<M> {
    source_settings: StreamRunSettings,
    sink_settings: StreamRunSettings,
    _materialized: PhantomData<fn() -> M>,
}

impl<M> RunnableStream<M> {
    /// Run settings inherited from the source side of this stream.
    #[must_use]
    pub const fn source_settings(&self) -> &StreamRunSettings {
        &self.source_settings
    }

    /// Run settings inherited from the sink side of this stream.
    #[must_use]
    pub const fn sink_settings(&self) -> &StreamRunSettings {
        &self.sink_settings
    }
}

impl<M> Debug for RunnableStream<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunnableStream")
            .field("source_settings", &self.source_settings)
            .field("sink_settings", &self.sink_settings)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceShape {
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlowShape {
    Identity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkShape {
    Ignore,
}

fn validate_capacity(capacity: usize) -> StreamResult<()> {
    if capacity == 0 {
        Err(StreamError::InvalidCapacity { capacity })
    } else {
        Ok(())
    }
}
