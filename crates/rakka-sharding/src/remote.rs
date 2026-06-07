//! Remote-aware entity routing using `rakka-remote` envelopes.

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use rakka_cluster::NodeId;
use rakka_core::{Message, ReplyTo};
use rakka_remote::{
    RemoteDestination, RemoteEndpointError, RemoteEnvelope, RemoteEnvelopeHandler, RemoteError,
    RemoteRequestError, RemoteRequestRegistry, RemoteTransport, SerializationRegistry,
};

use crate::identity::{EntityId, EntityRef, EntityType};
use crate::routing::{
    EntityDeliveryFailure, EntityRoute, EntityTellError, RoutedEntityMessage, ShardRegion,
};

/// Failure returned by a remote entity outbound transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEntitySendFailure {
    /// Outbound transport rejected the envelope.
    Rejected(String),
}

impl Display for RemoteEntitySendFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(message) => {
                write!(f, "remote entity transport rejected envelope: {message}")
            }
        }
    }
}

impl Error for RemoteEntitySendFailure {}

/// Failure returned while accepting an inbound remote entity envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEntityInboundError<M> {
    /// Envelope was addressed to a destination kind other than a sharded entity.
    UnexpectedDestination {
        /// Destination carried by the remote envelope.
        destination: RemoteDestination,
    },
    /// Envelope was addressed to a different entity type than this inbound handler accepts.
    EntityTypeMismatch {
        /// Entity type expected by the local shard region.
        expected: EntityType,
        /// Entity type carried by the remote envelope.
        actual: EntityType,
    },
    /// Envelope payload could not be decoded into this entity protocol.
    Decode {
        /// Decode failure reported by the serialization registry.
        error: RemoteError,
    },
    /// Decoded message could not be delivered through the local shard region.
    Delivery {
        /// Delivery failure, including the decoded message when available.
        error: EntityTellError<M>,
    },
}

impl<M> Display for RemoteEntityInboundError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedDestination { destination } => {
                write!(
                    f,
                    "remote envelope is not addressed to an entity: {destination:?}"
                )
            }
            Self::EntityTypeMismatch { expected, actual } => {
                write!(
                    f,
                    "remote entity envelope for {actual} cannot be handled by region for {expected}"
                )
            }
            Self::Decode { error } => write!(f, "remote entity envelope decode failed: {error}"),
            Self::Delivery { error } => match error {
                EntityTellError::NoRoute { error, .. } => {
                    write!(f, "remote entity delivery could not resolve route: {error}")
                }
                EntityTellError::Delivery { failure, .. } => {
                    write!(f, "remote entity delivery failed: {failure}")
                }
            },
        }
    }
}

impl<M> Error for RemoteEntityInboundError<M> where M: Debug {}

/// Failure returned by a remote entity ask client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEntityAskError {
    /// Routing failed before remote delivery because the owner could not be resolved.
    NoRoute {
        /// Routing error.
        error: crate::ShardingError,
    },
    /// Request payload could not be encoded for remote entity delivery.
    Encode {
        /// Encode failure reported by the serialization registry.
        error: RemoteError,
    },
    /// Pending reply registration failed.
    Register {
        /// Request registry failure.
        error: RemoteRequestError,
    },
    /// Request envelope could not be sent to the owning node.
    Send {
        /// Failure reported by the remote transport.
        message: String,
    },
    /// Waiting for the remote reply failed.
    Reply {
        /// Reply correlation failure.
        error: RemoteRequestError,
    },
}

impl Display for RemoteEntityAskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRoute { error } => Display::fmt(error, f),
            Self::Encode { error } => write!(f, "remote entity ask encode failed: {error}"),
            Self::Register { error } => write!(f, "remote entity ask registration failed: {error}"),
            Self::Send { message } => write!(f, "remote entity ask send failed: {message}"),
            Self::Reply { error } => write!(f, "remote entity ask reply failed: {error}"),
        }
    }
}

impl Error for RemoteEntityAskError {}

/// Failure returned while accepting an inbound remote entity ask envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteEntityAskInboundError<M> {
    /// Envelope was addressed to a destination kind other than a sharded entity.
    UnexpectedDestination {
        /// Destination carried by the remote envelope.
        destination: RemoteDestination,
    },
    /// Envelope was addressed to a different entity type than this inbound handler accepts.
    EntityTypeMismatch {
        /// Entity type expected by the local shard region.
        expected: EntityType,
        /// Entity type carried by the remote envelope.
        actual: EntityType,
    },
    /// Request envelope did not contain request id metadata.
    MissingRequestId,
    /// Request envelope did not contain source node metadata.
    MissingSource,
    /// Source node metadata could not be parsed as a node id.
    InvalidSource {
        /// Source metadata carried by the request envelope.
        source: String,
    },
    /// Request payload could not be decoded into the request protocol.
    Decode {
        /// Decode failure reported by the serialization registry.
        error: RemoteError,
    },
    /// Decoded request could not be delivered through the local shard region.
    Delivery {
        /// Delivery failure, including the local actor message when available.
        error: EntityTellError<M>,
    },
}

impl<M> Display for RemoteEntityAskInboundError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedDestination { destination } => {
                write!(
                    f,
                    "remote ask envelope is not addressed to an entity: {destination:?}"
                )
            }
            Self::EntityTypeMismatch { expected, actual } => {
                write!(
                    f,
                    "remote ask envelope for {actual} cannot be handled by region for {expected}"
                )
            }
            Self::MissingRequestId => f.write_str("remote ask envelope is missing request_id"),
            Self::MissingSource => f.write_str("remote ask envelope is missing source node"),
            Self::InvalidSource { source } => {
                write!(f, "remote ask envelope source is not a node id: {source}")
            }
            Self::Decode { error } => write!(f, "remote ask envelope decode failed: {error}"),
            Self::Delivery { error } => match error {
                EntityTellError::NoRoute { error, .. } => {
                    write!(f, "remote ask delivery could not resolve route: {error}")
                }
                EntityTellError::Delivery { failure, .. } => {
                    write!(f, "remote ask delivery failed: {failure}")
                }
            },
        }
    }
}

impl<M> Error for RemoteEntityAskInboundError<M> where M: Debug {}

/// Outbound transport for already encoded remote entity envelopes.
pub trait RemoteEntityOutbound: Send + Sync + 'static {
    /// Sends one remote entity envelope to the node that owns the shard.
    fn send(&self, owner: &NodeId, envelope: RemoteEnvelope)
        -> Result<(), RemoteEntitySendFailure>;
}

impl<F> RemoteEntityOutbound for F
where
    F: Fn(&NodeId, RemoteEnvelope) -> Result<(), RemoteEntitySendFailure> + Send + Sync + 'static,
{
    fn send(
        &self,
        owner: &NodeId,
        envelope: RemoteEnvelope,
    ) -> Result<(), RemoteEntitySendFailure> {
        self(owner, envelope)
    }
}

/// Inbound handler that decodes remote entity envelopes and delivers them to a shard region.
pub struct RemoteEntityInbound<M>
where
    M: Message + Sync,
{
    region: ShardRegion<M>,
    registry: SerializationRegistry,
    _message: PhantomData<fn() -> M>,
}

impl<M> RemoteEntityInbound<M>
where
    M: Message + Sync,
{
    /// Creates an inbound remote entity envelope handler.
    #[must_use]
    pub fn new(region: ShardRegion<M>, registry: SerializationRegistry) -> Self {
        Self {
            region,
            registry,
            _message: PhantomData,
        }
    }

    /// Returns the shard region used for decoded entity delivery.
    #[must_use]
    pub const fn region(&self) -> &ShardRegion<M> {
        &self.region
    }

    /// Returns the serialization registry used for payload decoding.
    #[must_use]
    pub const fn registry(&self) -> &SerializationRegistry {
        &self.registry
    }

    /// Decodes and delivers one remote entity envelope.
    ///
    /// The envelope must target this handler's entity type. Ownership is resolved
    /// through the wrapped shard region before the message reaches the local route.
    pub fn handle(&self, envelope: RemoteEnvelope) -> Result<(), RemoteEntityInboundError<M>> {
        let (entity_type, entity_id) = match &envelope.destination {
            RemoteDestination::Entity {
                entity_type,
                entity_id,
            } => (
                EntityType::new(entity_type.clone()),
                EntityId::new(entity_id.clone()),
            ),
            destination => {
                return Err(RemoteEntityInboundError::UnexpectedDestination {
                    destination: destination.clone(),
                });
            }
        };

        if &entity_type != self.region.entity_type() {
            return Err(RemoteEntityInboundError::EntityTypeMismatch {
                expected: self.region.entity_type().clone(),
                actual: entity_type,
            });
        }

        let message = self
            .registry
            .decode_envelope(&envelope)
            .map_err(|error| RemoteEntityInboundError::Decode { error })?;
        let entity = EntityRef::new(entity_type, entity_id);
        self.region
            .tell(&entity, message)
            .map_err(|error| RemoteEntityInboundError::Delivery { error })
    }
}

impl<M> Clone for RemoteEntityInbound<M>
where
    M: Message + Sync,
{
    fn clone(&self) -> Self {
        Self {
            region: self.region.clone(),
            registry: self.registry.clone(),
            _message: PhantomData,
        }
    }
}

impl<M> Debug for RemoteEntityInbound<M>
where
    M: Message + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEntityInbound")
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

impl<M> RemoteEnvelopeHandler for RemoteEntityInbound<M>
where
    M: Message + Sync,
{
    fn handle(&self, envelope: RemoteEnvelope) -> Result<(), RemoteEndpointError> {
        let destination = envelope.destination.clone();
        RemoteEntityInbound::handle(self, envelope).map_err(|error| {
            RemoteEndpointError::HandlerRejected {
                destination,
                message: error.to_string(),
            }
        })
    }
}

/// Outbound adapter that sends remote entity envelopes through a remote transport.
pub struct RemoteTransportEntityOutbound<T>
where
    T: RemoteTransport,
{
    transport: T,
}

impl<T> RemoteTransportEntityOutbound<T>
where
    T: RemoteTransport,
{
    /// Creates a remote entity outbound adapter from a remote transport.
    #[must_use]
    pub const fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Returns the wrapped transport.
    #[must_use]
    pub const fn transport(&self) -> &T {
        &self.transport
    }
}

impl<T> Clone for RemoteTransportEntityOutbound<T>
where
    T: RemoteTransport + Clone,
{
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
        }
    }
}

impl<T> Debug for RemoteTransportEntityOutbound<T>
where
    T: RemoteTransport + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteTransportEntityOutbound")
            .field("transport", &self.transport)
            .finish()
    }
}

impl<T> RemoteEntityOutbound for RemoteTransportEntityOutbound<T>
where
    T: RemoteTransport,
{
    fn send(
        &self,
        owner: &NodeId,
        envelope: RemoteEnvelope,
    ) -> Result<(), RemoteEntitySendFailure> {
        self.transport
            .send(owner, envelope)
            .map_err(|error| RemoteEntitySendFailure::Rejected(error.to_string()))
    }
}

/// Remote ask client for sharded entities.
pub struct RemoteEntityAskClient<T>
where
    T: RemoteTransport,
{
    local_node_id: NodeId,
    requests: RemoteRequestRegistry,
    transport: T,
}

impl<T> RemoteEntityAskClient<T>
where
    T: RemoteTransport,
{
    /// Creates a remote entity ask client.
    #[must_use]
    pub const fn new(local_node_id: NodeId, requests: RemoteRequestRegistry, transport: T) -> Self {
        Self {
            local_node_id,
            requests,
            transport,
        }
    }

    /// Local node id used as the request source.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Pending reply registry used by this client.
    #[must_use]
    pub const fn requests(&self) -> &RemoteRequestRegistry {
        &self.requests
    }

    /// Wrapped remote transport.
    #[must_use]
    pub const fn transport(&self) -> &T {
        &self.transport
    }

    /// Sends a remote entity request and waits for its reply.
    pub async fn ask<Q, M, R>(
        &self,
        region: &ShardRegion<M>,
        entity: &EntityRef<M>,
        request: Q,
        timeout: Duration,
    ) -> Result<R, RemoteEntityAskError>
    where
        Q: Message + Sync,
        M: Message,
        R: Send + Sync + 'static,
    {
        let (owner, _shard_id) = region
            .resolve(entity)
            .map_err(|error| RemoteEntityAskError::NoRoute { error })?;
        let encoded_payload = self
            .requests
            .registry()
            .encode(&request)
            .map_err(|error| RemoteEntityAskError::Encode { error })?;
        let request_id = self
            .requests
            .next_request_id(self.local_node_id.to_string());
        let pending = self
            .requests
            .register::<R>(request_id.clone())
            .map_err(|error| RemoteEntityAskError::Register { error })?;
        let envelope = RemoteEnvelope::new(
            RemoteDestination::Entity {
                entity_type: entity.entity_type().as_str().to_string(),
                entity_id: entity.entity_id().as_str().to_string(),
            },
            encoded_payload,
        )
        .with_source(self.local_node_id.to_string())
        .with_request_id(request_id.clone());

        if let Err(error) = self.transport.send(&owner, envelope) {
            let _ = self.requests.remove(&request_id);
            return Err(RemoteEntityAskError::Send {
                message: error.to_string(),
            });
        }

        pending
            .wait(timeout)
            .await
            .map_err(|error| RemoteEntityAskError::Reply { error })
    }
}

impl<T> Clone for RemoteEntityAskClient<T>
where
    T: RemoteTransport + Clone,
{
    fn clone(&self) -> Self {
        Self {
            local_node_id: self.local_node_id.clone(),
            requests: self.requests.clone(),
            transport: self.transport.clone(),
        }
    }
}

impl<T> Debug for RemoteEntityAskClient<T>
where
    T: RemoteTransport + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEntityAskClient")
            .field("local_node_id", &self.local_node_id)
            .field("requests", &self.requests)
            .field("transport", &self.transport)
            .finish()
    }
}

/// Inbound handler that turns remote entity ask envelopes into local actor asks.
pub struct RemoteEntityAskInbound<Q, M, R, T, B>
where
    Q: Message + Sync,
    M: Message,
    R: Send + Sync + 'static,
    T: RemoteTransport,
    B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
{
    local_node_id: NodeId,
    region: ShardRegion<M>,
    registry: SerializationRegistry,
    transport: T,
    build: Arc<B>,
    _request: PhantomData<fn() -> Q>,
    _reply: PhantomData<fn() -> R>,
}

impl<Q, M, R, T, B> RemoteEntityAskInbound<Q, M, R, T, B>
where
    Q: Message + Sync,
    M: Message,
    R: Send + Sync + 'static,
    T: RemoteTransport + Clone,
    B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
{
    /// Creates an inbound remote ask handler for one sharded entity type.
    #[must_use]
    pub fn new(
        local_node_id: NodeId,
        region: ShardRegion<M>,
        registry: SerializationRegistry,
        transport: T,
        build: B,
    ) -> Self {
        Self {
            local_node_id,
            region,
            registry,
            transport,
            build: Arc::new(build),
            _request: PhantomData,
            _reply: PhantomData,
        }
    }

    /// Handles one remote entity ask envelope.
    pub fn handle(&self, envelope: RemoteEnvelope) -> Result<(), RemoteEntityAskInboundError<M>> {
        let request_id = envelope
            .request_id
            .clone()
            .ok_or(RemoteEntityAskInboundError::MissingRequestId)?;
        let source = envelope
            .source
            .clone()
            .ok_or(RemoteEntityAskInboundError::MissingSource)?;
        let requester = NodeId::from_str(&source)
            .map_err(|_error| RemoteEntityAskInboundError::InvalidSource { source })?;
        let (entity_type, entity_id) = match &envelope.destination {
            RemoteDestination::Entity {
                entity_type,
                entity_id,
            } => (
                EntityType::new(entity_type.clone()),
                EntityId::new(entity_id.clone()),
            ),
            destination => {
                return Err(RemoteEntityAskInboundError::UnexpectedDestination {
                    destination: destination.clone(),
                });
            }
        };

        if &entity_type != self.region.entity_type() {
            return Err(RemoteEntityAskInboundError::EntityTypeMismatch {
                expected: self.region.entity_type().clone(),
                actual: entity_type,
            });
        }

        let request = self
            .registry
            .decode_envelope::<Q>(&envelope)
            .map_err(|error| RemoteEntityAskInboundError::Decode { error })?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let message = (self.build)(request, ReplyTo::new(sender));
        let entity = EntityRef::new(entity_type, entity_id);
        self.region
            .tell(&entity, message)
            .map_err(|error| RemoteEntityAskInboundError::Delivery { error })?;

        let local_node_id = self.local_node_id.clone();
        let registry = self.registry.clone();
        let transport = self.transport.clone();
        tokio::spawn(async move {
            if let Ok(reply) = receiver.await {
                if let Ok(encoded_payload) = registry.encode(&reply) {
                    let envelope = RemoteEnvelope::new(
                        RemoteDestination::Reply {
                            request_id: request_id.clone(),
                        },
                        encoded_payload,
                    )
                    .with_source(local_node_id.to_string())
                    .with_request_id(request_id);
                    let _ = transport.send(&requester, envelope);
                }
            }
        });
        Ok(())
    }
}

impl<Q, M, R, T, B> Clone for RemoteEntityAskInbound<Q, M, R, T, B>
where
    Q: Message + Sync,
    M: Message,
    R: Send + Sync + 'static,
    T: RemoteTransport + Clone,
    B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            local_node_id: self.local_node_id.clone(),
            region: self.region.clone(),
            registry: self.registry.clone(),
            transport: self.transport.clone(),
            build: self.build.clone(),
            _request: PhantomData,
            _reply: PhantomData,
        }
    }
}

impl<Q, M, R, T, B> Debug for RemoteEntityAskInbound<Q, M, R, T, B>
where
    Q: Message + Sync,
    M: Message,
    R: Send + Sync + 'static,
    T: RemoteTransport + Debug,
    B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEntityAskInbound")
            .field("local_node_id", &self.local_node_id)
            .field("region", &self.region)
            .field("transport", &self.transport)
            .finish_non_exhaustive()
    }
}

impl<Q, M, R, T, B> RemoteEnvelopeHandler for RemoteEntityAskInbound<Q, M, R, T, B>
where
    Q: Message + Sync,
    M: Message,
    R: Send + Sync + 'static,
    T: RemoteTransport + Clone,
    B: Fn(Q, ReplyTo<R>) -> M + Send + Sync + 'static,
{
    fn handle(&self, envelope: RemoteEnvelope) -> Result<(), RemoteEndpointError> {
        let destination = envelope.destination.clone();
        RemoteEntityAskInbound::handle(self, envelope).map_err(|error| {
            RemoteEndpointError::HandlerRejected {
                destination,
                message: error.to_string(),
            }
        })
    }
}

/// Entity route that falls back to remote envelopes when local delivery reports `NotLocal`.
pub struct RemoteEntityRoute<M, L, O>
where
    M: Message + Sync,
    L: EntityRoute<M>,
    O: RemoteEntityOutbound,
{
    local_route: L,
    registry: SerializationRegistry,
    outbound: O,
    source: Option<String>,
    _message: PhantomData<fn() -> M>,
}

impl<M, L, O> RemoteEntityRoute<M, L, O>
where
    M: Message + Sync,
    L: EntityRoute<M>,
    O: RemoteEntityOutbound,
{
    /// Creates a remote-aware entity route.
    #[must_use]
    pub fn new(local_route: L, registry: SerializationRegistry, outbound: O) -> Self {
        Self {
            local_route,
            registry,
            outbound,
            source: None,
            _message: PhantomData,
        }
    }

    /// Sets the source metadata attached to remote envelopes.
    #[must_use]
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Returns the wrapped local route.
    #[must_use]
    pub fn local_route(&self) -> &L {
        &self.local_route
    }

    /// Returns the serialization registry used for remote payload encoding.
    #[must_use]
    pub const fn registry(&self) -> &SerializationRegistry {
        &self.registry
    }

    /// Returns the outbound transport.
    #[must_use]
    pub const fn outbound(&self) -> &O {
        &self.outbound
    }

    fn deliver_remote(
        &self,
        owner: NodeId,
        routed: RemoteRoutedEntityMessage<M>,
    ) -> Result<(), EntityTellError<M>> {
        let encoded_payload = match self.registry.encode(routed.message()) {
            Ok(encoded_payload) => encoded_payload,
            Err(error) => {
                return Err(EntityTellError::Delivery {
                    message: routed.into_message(),
                    failure: EntityDeliveryFailure::RemoteEncode(error.to_string()),
                });
            }
        };
        let mut envelope = RemoteEnvelope::new(
            RemoteDestination::Entity {
                entity_type: routed.entity_type.as_str().to_string(),
                entity_id: routed.entity_id.as_str().to_string(),
            },
            encoded_payload,
        );
        if let Some(source) = &self.source {
            envelope = envelope.with_source(source.clone());
        }

        self.outbound
            .send(&owner, envelope)
            .map_err(|error| EntityTellError::Delivery {
                message: routed.into_message(),
                failure: EntityDeliveryFailure::RemoteSend(error.to_string()),
            })
    }
}

impl<M, L, O> Clone for RemoteEntityRoute<M, L, O>
where
    M: Message + Sync,
    L: EntityRoute<M> + Clone,
    O: RemoteEntityOutbound + Clone,
{
    fn clone(&self) -> Self {
        Self {
            local_route: self.local_route.clone(),
            registry: self.registry.clone(),
            outbound: self.outbound.clone(),
            source: self.source.clone(),
            _message: PhantomData,
        }
    }
}

impl<M, L, O> Debug for RemoteEntityRoute<M, L, O>
where
    M: Message + Sync,
    L: EntityRoute<M> + Debug,
    O: RemoteEntityOutbound + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteEntityRoute")
            .field("local_route", &self.local_route)
            .field("outbound", &self.outbound)
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<M, L, O> EntityRoute<M> for RemoteEntityRoute<M, L, O>
where
    M: Message + Sync,
    L: EntityRoute<M>,
    O: RemoteEntityOutbound,
{
    fn deliver(&self, message: RoutedEntityMessage<M>) -> Result<(), EntityTellError<M>> {
        let remote = RemoteRoutedEntityMessage::from(&message);
        match self.local_route.deliver(message) {
            Ok(()) => Ok(()),
            Err(EntityTellError::Delivery {
                message,
                failure: EntityDeliveryFailure::NotLocal { owner },
            }) => self.deliver_remote(owner, remote.with_message(message)),
            Err(error) => Err(error),
        }
    }

    fn local_node_id(&self) -> Option<&NodeId> {
        self.local_route.local_node_id()
    }

    fn begin_shard_handoff(&self, shard_id: crate::ShardId) -> crate::ShardingResult<usize> {
        self.local_route.begin_shard_handoff(shard_id)
    }

    fn complete_shard_handoff(&self, shard_id: crate::ShardId) -> crate::ShardingResult<usize> {
        self.local_route.complete_shard_handoff(shard_id)
    }

    fn acquire_shard(&self, shard_id: crate::ShardId) -> crate::ShardingResult<usize> {
        self.local_route.acquire_shard(shard_id)
    }

    fn shard_handoff_state(&self, shard_id: crate::ShardId) -> Option<crate::ShardHandoffState> {
        self.local_route.shard_handoff_state(shard_id)
    }
}

struct RemoteRoutedEntityMessage<M> {
    entity_type: crate::EntityType,
    entity_id: crate::EntityId,
    message: Option<M>,
}

impl<M> RemoteRoutedEntityMessage<M> {
    fn with_message(mut self, message: M) -> Self {
        self.message = Some(message);
        self
    }

    fn message(&self) -> &M {
        self.message
            .as_ref()
            .expect("remote routed message must contain message")
    }

    fn into_message(mut self) -> M {
        self.message
            .take()
            .expect("remote routed message must contain message")
    }
}

impl<M> From<&RoutedEntityMessage<M>> for RemoteRoutedEntityMessage<M> {
    fn from(message: &RoutedEntityMessage<M>) -> Self {
        Self {
            entity_type: message.entity_type().clone(),
            entity_id: message.entity_id().clone(),
            message: None,
        }
    }
}
