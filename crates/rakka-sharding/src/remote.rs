//! Remote-aware entity routing using `rakka-remote` envelopes.

use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::marker::PhantomData;

use rakka_cluster::NodeId;
use rakka_core::Message;
use rakka_remote::{RemoteDestination, RemoteEnvelope, RemoteError, SerializationRegistry};

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
