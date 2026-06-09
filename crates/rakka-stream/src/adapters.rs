//! Actor, entity, and stream-to-stream adapter helpers.

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorRef, ActorSystem, Message, RakkaError,
    TellError,
};
use rakka_sharding::{
    EntityDeliveryFailure, EntityRef, EntityTellError, ShardRegion, ShardingError,
};

use crate::{bounded_channel, StreamError, StreamSendError, StreamSink, StreamSource};

/// Result alias for actor sink adapter operations.
pub type ActorSinkResult<T, M> = Result<T, ActorSinkError<M>>;

/// Result alias for entity sink adapter operations.
pub type EntitySinkResult<T, M> = Result<T, EntitySinkError<M>>;

/// Result alias for stream-to-stream pipe operations.
pub type StreamPipeResult<T> = Result<StreamPipeSummary, StreamPipeError<T>>;

/// Result alias for stream fan-out operations.
pub type BroadcastResult<T> = Result<StreamPipeSummary, BroadcastError<T>>;

/// Error returned while sending stream items into an actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorSinkError<M> {
    /// Source stream ended with an error before all items were delivered.
    Stream {
        /// Stream error observed while reading from the source.
        error: StreamError,
    },
    /// Actor mailbox was full.
    MailboxFull {
        /// Message that could not be delivered.
        message: M,
    },
    /// Actor mailbox was closed or the actor had already stopped.
    MailboxClosed {
        /// Message that could not be delivered.
        message: M,
    },
}

impl<M> ActorSinkError<M> {
    /// Creates an actor sink error from a stream read failure.
    #[must_use]
    pub const fn stream(error: StreamError) -> Self {
        Self::Stream { error }
    }

    /// Returns the rejected message when the failure happened after reading an item.
    #[must_use]
    pub const fn message(&self) -> Option<&M> {
        match self {
            Self::Stream { .. } => None,
            Self::MailboxFull { message } | Self::MailboxClosed { message } => Some(message),
        }
    }

    /// Consumes this error and returns the rejected message when one exists.
    #[must_use]
    pub fn into_message(self) -> Option<M> {
        match self {
            Self::Stream { .. } => None,
            Self::MailboxFull { message } | Self::MailboxClosed { message } => Some(message),
        }
    }

    fn from_tell(error: TellError<M>) -> Self {
        match error {
            TellError::Full(message) => Self::MailboxFull { message },
            TellError::Closed(message) => Self::MailboxClosed { message },
        }
    }
}

impl<M> Display for ActorSinkError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream { error } => Display::fmt(error, f),
            Self::MailboxFull { .. } => f.write_str("actor sink mailbox was full"),
            Self::MailboxClosed { .. } => f.write_str("actor sink mailbox was closed"),
        }
    }
}

impl<M> Error for ActorSinkError<M> where M: Debug {}

/// Sends stream items into one actor reference in source order.
pub struct ActorSink<M>
where
    M: Message,
{
    actor: ActorRef<M>,
}

impl<M> ActorSink<M>
where
    M: Message,
{
    /// Creates an actor sink adapter from an actor reference.
    #[must_use]
    pub const fn new(actor: ActorRef<M>) -> Self {
        Self { actor }
    }

    /// Actor reference used by this sink.
    #[must_use]
    pub const fn actor(&self) -> &ActorRef<M> {
        &self.actor
    }

    /// Attempts to send one item to the actor without waiting for mailbox space.
    pub fn try_send(&self, message: M) -> ActorSinkResult<(), M> {
        self.actor.tell(message).map_err(ActorSinkError::from_tell)
    }

    /// Alias for `try_send`, matching the adapter vocabulary used by entity sinks.
    pub fn send(&self, message: M) -> ActorSinkResult<(), M> {
        self.try_send(message)
    }

    /// Reads a source to completion and delivers each item to the actor in order.
    pub async fn drain_from(&self, source: &StreamSource<M>) -> ActorSinkResult<usize, M> {
        let mut delivered = 0usize;
        loop {
            match source.next().await {
                Ok(Some(message)) => {
                    self.try_send(message)?;
                    delivered = delivered.saturating_add(1);
                }
                Ok(None) => return Ok(delivered),
                Err(error) => return Err(ActorSinkError::stream(error)),
            }
        }
    }
}

impl<M> Clone for ActorSink<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
        }
    }
}

impl<M> Debug for ActorSink<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorSink")
            .field("actor", &self.actor)
            .finish()
    }
}

/// Error returned while creating an actor-backed stream source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorSourceSpawnError {
    /// Source stream could not be created.
    Stream {
        /// Stream construction failure.
        error: StreamError,
    },
    /// Actor source relay could not be spawned.
    Actor {
        /// Actor spawn failure.
        error: RakkaError,
    },
}

impl Display for ActorSourceSpawnError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream { error } => Display::fmt(error, f),
            Self::Actor { error } => Display::fmt(error, f),
        }
    }
}

impl Error for ActorSourceSpawnError {}

/// Actor reference plus stream source pair for actor-produced stream items.
pub struct ActorSource<M>
where
    M: Message,
{
    actor: ActorRef<M>,
    source: StreamSource<M>,
}

impl<M> ActorSource<M>
where
    M: Message,
{
    /// Actor reference that accepts source items.
    #[must_use]
    pub const fn actor(&self) -> &ActorRef<M> {
        &self.actor
    }

    /// Stream source receiving items sent to the actor reference.
    #[must_use]
    pub const fn source(&self) -> &StreamSource<M> {
        &self.source
    }

    /// Splits this adapter into its actor reference and stream source.
    #[must_use]
    pub fn split(self) -> (ActorRef<M>, StreamSource<M>) {
        (self.actor, self.source)
    }
}

impl<M> Debug for ActorSource<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorSource")
            .field("actor", &self.actor)
            .field("source", &self.source)
            .finish()
    }
}

/// Spawns an actor that forwards each received message into a bounded source.
pub fn spawn_actor_source<M>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    capacity: usize,
) -> Result<ActorSource<M>, ActorSourceSpawnError>
where
    M: Message,
{
    let (sink, source) =
        bounded_channel(capacity).map_err(|error| ActorSourceSpawnError::Stream { error })?;
    let actor = system
        .spawn_actor(name, StreamSourceActor::new(sink))
        .map_err(|error| ActorSourceSpawnError::Actor { error })?;
    Ok(ActorSource { actor, source })
}

struct StreamSourceActor<M>
where
    M: Message,
{
    sink: StreamSink<M>,
}

impl<M> StreamSourceActor<M>
where
    M: Message,
{
    fn new(sink: StreamSink<M>) -> Self {
        Self { sink }
    }
}

impl<M> Actor for StreamSourceActor<M>
where
    M: Message,
{
    type Msg = M;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        actor_future(async move {
            match self.sink.send(msg).await {
                Ok(()) => Ok(ActorAction::Continue),
                Err(_closed) => Ok(ActorAction::Stop),
            }
        })
    }
}

/// Error returned while sending stream items into a sharded entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntitySinkError<M> {
    /// Source stream ended with an error before all items were delivered.
    Stream {
        /// Stream error observed while reading from the source.
        error: StreamError,
    },
    /// Entity owner resolution failed before delivery.
    NoRoute {
        /// Message that could not be routed.
        message: M,
        /// Routing failure.
        error: ShardingError,
    },
    /// Entity route rejected delivery after owner resolution.
    Delivery {
        /// Message that could not be delivered.
        message: M,
        /// Delivery failure.
        failure: EntityDeliveryFailure,
    },
}

impl<M> EntitySinkError<M> {
    /// Creates an entity sink error from a stream read failure.
    #[must_use]
    pub const fn stream(error: StreamError) -> Self {
        Self::Stream { error }
    }

    /// Returns the rejected message when the failure happened after reading an item.
    #[must_use]
    pub const fn message(&self) -> Option<&M> {
        match self {
            Self::Stream { .. } => None,
            Self::NoRoute { message, .. } | Self::Delivery { message, .. } => Some(message),
        }
    }

    /// Consumes this error and returns the rejected message when one exists.
    #[must_use]
    pub fn into_message(self) -> Option<M> {
        match self {
            Self::Stream { .. } => None,
            Self::NoRoute { message, .. } | Self::Delivery { message, .. } => Some(message),
        }
    }

    fn from_tell(error: EntityTellError<M>) -> Self {
        match error {
            EntityTellError::NoRoute { message, error } => Self::NoRoute { message, error },
            EntityTellError::Delivery { message, failure } => Self::Delivery { message, failure },
        }
    }
}

impl<M> Display for EntitySinkError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream { error } => Display::fmt(error, f),
            Self::NoRoute { error, .. } => Display::fmt(error, f),
            Self::Delivery { failure, .. } => Display::fmt(failure, f),
        }
    }
}

impl<M> Error for EntitySinkError<M> where M: Debug {}

/// Sends stream items into one sharded entity in source order.
pub struct EntitySink<M>
where
    M: Message,
{
    region: ShardRegion<M>,
    entity: EntityRef<M>,
}

impl<M> EntitySink<M>
where
    M: Message,
{
    /// Creates an entity sink adapter from a shard region and entity reference.
    #[must_use]
    pub fn new(region: ShardRegion<M>, entity: EntityRef<M>) -> Self {
        Self { region, entity }
    }

    /// Shard region used by this sink.
    #[must_use]
    pub const fn region(&self) -> &ShardRegion<M> {
        &self.region
    }

    /// Entity reference targeted by this sink.
    #[must_use]
    pub const fn entity(&self) -> &EntityRef<M> {
        &self.entity
    }

    /// Attempts to send one item to the entity through the configured region.
    pub fn try_send(&self, message: M) -> EntitySinkResult<(), M> {
        self.region
            .tell(&self.entity, message)
            .map_err(EntitySinkError::from_tell)
    }

    /// Alias for `try_send`, matching the adapter vocabulary used by actor sinks.
    pub fn send(&self, message: M) -> EntitySinkResult<(), M> {
        self.try_send(message)
    }

    /// Reads a source to completion and delivers each item to the entity in order.
    pub async fn drain_from(&self, source: &StreamSource<M>) -> EntitySinkResult<usize, M> {
        let mut delivered = 0usize;
        loop {
            match source.next().await {
                Ok(Some(message)) => {
                    self.try_send(message)?;
                    delivered = delivered.saturating_add(1);
                }
                Ok(None) => return Ok(delivered),
                Err(error) => return Err(EntitySinkError::stream(error)),
            }
        }
    }
}

impl<M> Clone for EntitySink<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            region: self.region.clone(),
            entity: self.entity.clone(),
        }
    }
}

impl<M> Debug for EntitySink<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntitySink")
            .field("region", &self.region)
            .field("entity_type", &self.entity.entity_type())
            .field("entity_id", &self.entity.entity_id())
            .finish()
    }
}

/// Summary returned after a stream pipe completes normally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamPipeSummary {
    items: usize,
}

impl StreamPipeSummary {
    /// Creates a new pipe summary.
    #[must_use]
    pub const fn new(items: usize) -> Self {
        Self { items }
    }

    /// Number of items forwarded before normal completion.
    #[must_use]
    pub const fn items(&self) -> usize {
        self.items
    }
}

/// Error returned while piping items between bounded streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamPipeError<T> {
    /// Source stream ended with a typed error.
    Source {
        /// Source stream error.
        error: StreamError,
    },
    /// Destination stream rejected an item.
    Sink {
        /// Destination send failure, including the rejected item.
        error: StreamSendError<T>,
    },
}

impl<T> Display for StreamPipeError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source { error } => Display::fmt(error, f),
            Self::Sink { error } => Display::fmt(error, f),
        }
    }
}

impl<T> Error for StreamPipeError<T> where T: Debug {}

/// Pipes all items from one bounded source into one bounded sink.
pub async fn pipe_stream<T>(source: &StreamSource<T>, sink: &StreamSink<T>) -> StreamPipeResult<T> {
    let mut forwarded = 0usize;
    loop {
        match source.next().await {
            Ok(Some(item)) => {
                sink.send(item)
                    .await
                    .map_err(|error| StreamPipeError::Sink { error })?;
                forwarded = forwarded.saturating_add(1);
            }
            Ok(None) => return Ok(StreamPipeSummary::new(forwarded)),
            Err(error) => return Err(StreamPipeError::Source { error }),
        }
    }
}

/// Sequentially pipes multiple sources into one bounded sink.
pub async fn fan_in_streams<T>(
    sources: &[StreamSource<T>],
    sink: &StreamSink<T>,
) -> StreamPipeResult<T> {
    let mut forwarded = 0usize;
    for source in sources {
        let summary = pipe_stream(source, sink).await?;
        forwarded = forwarded.saturating_add(summary.items());
    }
    Ok(StreamPipeSummary::new(forwarded))
}

/// Error returned while broadcasting one source into multiple bounded sinks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastError<T> {
    /// Source stream ended with a typed error.
    Source {
        /// Source stream error.
        error: StreamError,
    },
    /// One destination stream rejected an item.
    Sink {
        /// Destination sink index.
        sink_index: usize,
        /// Destination send failure, including the rejected item.
        error: StreamSendError<T>,
    },
}

impl<T> Display for BroadcastError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source { error } => Display::fmt(error, f),
            Self::Sink { sink_index, error } => {
                write!(f, "broadcast sink {sink_index} rejected item: {error}")
            }
        }
    }
}

impl<T> Error for BroadcastError<T> where T: Debug {}

/// Broadcasts each source item into every destination sink in sink order.
pub async fn broadcast_stream<T>(
    source: &StreamSource<T>,
    sinks: &[StreamSink<T>],
) -> BroadcastResult<T>
where
    T: Clone,
{
    let mut forwarded = 0usize;
    loop {
        match source.next().await {
            Ok(Some(item)) => {
                for (sink_index, sink) in sinks.iter().enumerate() {
                    sink.send(item.clone())
                        .await
                        .map_err(|error| BroadcastError::Sink { sink_index, error })?;
                }
                forwarded = forwarded.saturating_add(1);
            }
            Ok(None) => return Ok(StreamPipeSummary::new(forwarded)),
            Err(error) => return Err(BroadcastError::Source { error }),
        }
    }
}
