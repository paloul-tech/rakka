//! Shard owner cache and typed entity routing surface.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::NodeId;
use rakka_core::{Message, ReplyTo};

use crate::coordinator::ShardOwnershipSnapshot;
use crate::error::{ShardingError, ShardingResult};
use crate::identity::{EntityId, EntityRef, EntityType, ShardId, ShardingConfig};

/// Delivery failure reported by an entity route handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityDeliveryFailure {
    /// Destination mailbox or route buffer was full.
    MailboxFull,
    /// Destination route was closed.
    MailboxClosed,
    /// Shard is owned by another node and requires remote transport.
    NotLocal {
        /// Node that currently owns the shard.
        owner: NodeId,
    },
    /// Entity actor could not be spawned.
    SpawnFailed(String),
    /// Message could not be encoded for remote entity delivery.
    RemoteEncode(String),
    /// Encoded remote entity envelope could not be sent.
    RemoteSend(String),
    /// Transport or route handler rejected the message.
    Rejected(String),
}

impl Display for EntityDeliveryFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MailboxFull => f.write_str("entity route mailbox was full"),
            Self::MailboxClosed => f.write_str("entity route mailbox was closed"),
            Self::NotLocal { owner } => write!(f, "entity shard is owned by remote node {owner}"),
            Self::SpawnFailed(message) => write!(f, "entity actor spawn failed: {message}"),
            Self::RemoteEncode(message) => write!(f, "remote entity encode failed: {message}"),
            Self::RemoteSend(message) => write!(f, "remote entity send failed: {message}"),
            Self::Rejected(message) => write!(f, "entity route rejected message: {message}"),
        }
    }
}

impl Error for EntityDeliveryFailure {}

/// Error returned by `EntityRef::tell` and `ShardRegion::tell`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityTellError<M> {
    /// Routing failed before delivery because the owner could not be resolved.
    NoRoute {
        /// Message that could not be routed.
        message: M,
        /// Routing error.
        error: ShardingError,
    },
    /// Route handler rejected delivery after owner resolution.
    Delivery {
        /// Message that could not be delivered.
        message: M,
        /// Delivery failure.
        failure: EntityDeliveryFailure,
    },
}

impl<M> EntityTellError<M> {
    /// Returns the message that could not be routed or delivered.
    #[must_use]
    pub fn into_message(self) -> M {
        match self {
            Self::NoRoute { message, .. } | Self::Delivery { message, .. } => message,
        }
    }

    fn into_ask_error(self) -> EntityAskError {
        match self {
            Self::NoRoute { error, .. } => EntityAskError::NoRoute(error),
            Self::Delivery { failure, .. } => match failure {
                EntityDeliveryFailure::MailboxFull => EntityAskError::MailboxFull,
                EntityDeliveryFailure::MailboxClosed => EntityAskError::MailboxClosed,
                EntityDeliveryFailure::NotLocal { owner } => EntityAskError::NotLocal { owner },
                EntityDeliveryFailure::SpawnFailed(message) => EntityAskError::SpawnFailed(message),
                EntityDeliveryFailure::RemoteEncode(message) => {
                    EntityAskError::RemoteEncode(message)
                }
                EntityDeliveryFailure::RemoteSend(message) => EntityAskError::RemoteSend(message),
                EntityDeliveryFailure::Rejected(message) => EntityAskError::Rejected(message),
            },
        }
    }
}

/// Error returned by `EntityRef::ask` and `ShardRegion::ask`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityAskError {
    /// Routing failed before delivery because the owner could not be resolved.
    NoRoute(ShardingError),
    /// Destination mailbox or route buffer was full.
    MailboxFull,
    /// Destination route was closed.
    MailboxClosed,
    /// Shard is owned by another node and requires remote transport.
    NotLocal {
        /// Node that currently owns the shard.
        owner: NodeId,
    },
    /// Entity actor could not be spawned.
    SpawnFailed(String),
    /// Message could not be encoded for remote entity delivery.
    RemoteEncode(String),
    /// Encoded remote entity envelope could not be sent.
    RemoteSend(String),
    /// Route handler rejected delivery.
    Rejected(String),
    /// Timed out waiting for a reply.
    Timeout,
    /// Reply channel was dropped before a reply was sent.
    ReplyDropped,
}

impl Display for EntityAskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRoute(error) => Display::fmt(error, f),
            Self::MailboxFull => f.write_str("entity route mailbox was full"),
            Self::MailboxClosed => f.write_str("entity route mailbox was closed"),
            Self::NotLocal { owner } => write!(f, "entity shard is owned by remote node {owner}"),
            Self::SpawnFailed(message) => write!(f, "entity actor spawn failed: {message}"),
            Self::RemoteEncode(message) => write!(f, "remote entity encode failed: {message}"),
            Self::RemoteSend(message) => write!(f, "remote entity send failed: {message}"),
            Self::Rejected(message) => write!(f, "entity route rejected message: {message}"),
            Self::Timeout => f.write_str("entity ask timed out"),
            Self::ReplyDropped => f.write_str("entity ask reply channel was dropped"),
        }
    }
}

impl Error for EntityAskError {}

/// Message resolved to a shard owner and ready for local or remote delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedEntityMessage<M> {
    entity_type: EntityType,
    entity_id: EntityId,
    shard_id: ShardId,
    owner: NodeId,
    message: M,
}

impl<M> RoutedEntityMessage<M> {
    /// Creates a routed entity message.
    #[must_use]
    pub fn new(
        entity_type: EntityType,
        entity_id: EntityId,
        shard_id: ShardId,
        owner: NodeId,
        message: M,
    ) -> Self {
        Self {
            entity_type,
            entity_id,
            shard_id,
            owner,
            message,
        }
    }

    /// Entity type.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Entity id.
    #[must_use]
    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    /// Shard id.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Owning node id resolved from the owner cache.
    #[must_use]
    pub fn owner(&self) -> &NodeId {
        &self.owner
    }

    /// Borrow the message payload.
    #[must_use]
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Returns the message payload.
    #[must_use]
    pub fn into_message(self) -> M {
        self.message
    }
}

/// Route handler used by a shard region after owner resolution.
pub trait EntityRoute<M>: Send + Sync + 'static
where
    M: Message,
{
    /// Delivers a message after the region resolves its shard owner.
    fn deliver(&self, message: RoutedEntityMessage<M>) -> Result<(), EntityTellError<M>>;
}

impl<M, F> EntityRoute<M> for F
where
    M: Message,
    F: Fn(RoutedEntityMessage<M>) -> Result<(), EntityTellError<M>> + Send + Sync + 'static,
{
    fn deliver(&self, message: RoutedEntityMessage<M>) -> Result<(), EntityTellError<M>> {
        self(message)
    }
}

/// Local cache of shard ownership for one entity type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardOwnerCache {
    entity_type: EntityType,
    config: ShardingConfig,
    revision: u64,
    owners: BTreeMap<ShardId, NodeId>,
}

impl ShardOwnerCache {
    /// Creates an empty owner cache for one entity type.
    #[must_use]
    pub fn empty(entity_type: EntityType, config: ShardingConfig) -> Self {
        Self {
            entity_type,
            config,
            revision: 0,
            owners: BTreeMap::new(),
        }
    }

    /// Creates an owner cache from an ownership snapshot.
    pub fn from_snapshot(
        entity_type: EntityType,
        config: ShardingConfig,
        snapshot: &ShardOwnershipSnapshot,
    ) -> ShardingResult<Self> {
        validate_snapshot(&entity_type, &config, snapshot)?;
        let mut cache = Self::empty(entity_type, config);
        cache.refresh(snapshot)?;
        Ok(cache)
    }

    /// Entity type cached by this owner cache.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Sharding configuration.
    #[must_use]
    pub const fn config(&self) -> &ShardingConfig {
        &self.config
    }

    /// Ownership revision from the last applied snapshot.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Refreshes the cache from a new ownership snapshot.
    pub fn refresh(&mut self, snapshot: &ShardOwnershipSnapshot) -> ShardingResult<()> {
        validate_snapshot(&self.entity_type, &self.config, snapshot)?;
        self.revision = snapshot.revision();
        self.owners = snapshot
            .assignments()
            .iter()
            .map(|assignment| (assignment.shard().shard_id(), assignment.owner().clone()))
            .collect();
        Ok(())
    }

    /// Returns the cached owner for a shard.
    pub fn owner_for_shard(&self, shard_id: ShardId) -> ShardingResult<&NodeId> {
        if !self.config.contains_shard(shard_id) {
            return Err(ShardingError::UnknownShard {
                shard_id,
                number_of_shards: self.config.number_of_shards(),
            });
        }

        self.owners
            .get(&shard_id)
            .ok_or_else(|| ShardingError::NoShardOwner {
                entity_type: self.entity_type.clone(),
                shard_id,
            })
    }

    /// Returns the cached owner for an entity id.
    pub fn owner_for_entity(&self, entity_id: &EntityId) -> ShardingResult<(&NodeId, ShardId)> {
        let shard_id = ShardId::for_entity(&self.entity_type, entity_id, &self.config);
        self.owner_for_shard(shard_id)
            .map(|owner| (owner, shard_id))
            .map_err(|error| match error {
                ShardingError::NoShardOwner { .. } => ShardingError::NoEntityOwner {
                    entity_type: self.entity_type.clone(),
                    entity_id: entity_id.clone(),
                    shard_id,
                },
                other => other,
            })
    }
}

/// Local shard region routing surface for one entity message protocol.
pub struct ShardRegion<M>
where
    M: Message,
{
    entity_type: EntityType,
    config: ShardingConfig,
    owner_cache: Arc<Mutex<ShardOwnerCache>>,
    route: Arc<dyn EntityRoute<M>>,
}

impl<M> ShardRegion<M>
where
    M: Message,
{
    /// Creates a shard region with an empty owner cache.
    #[must_use]
    pub fn new(
        entity_type: EntityType,
        config: ShardingConfig,
        route: impl EntityRoute<M>,
    ) -> Self {
        Self {
            owner_cache: Arc::new(Mutex::new(ShardOwnerCache::empty(
                entity_type.clone(),
                config.clone(),
            ))),
            entity_type,
            config,
            route: Arc::new(route),
        }
    }

    /// Creates a shard region from an ownership snapshot.
    pub fn from_snapshot(
        entity_type: EntityType,
        config: ShardingConfig,
        snapshot: &ShardOwnershipSnapshot,
        route: impl EntityRoute<M>,
    ) -> ShardingResult<Self> {
        Ok(Self {
            owner_cache: Arc::new(Mutex::new(ShardOwnerCache::from_snapshot(
                entity_type.clone(),
                config.clone(),
                snapshot,
            )?)),
            entity_type,
            config,
            route: Arc::new(route),
        })
    }

    /// Entity type routed by this region.
    #[must_use]
    pub fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    /// Sharding configuration.
    #[must_use]
    pub const fn config(&self) -> &ShardingConfig {
        &self.config
    }

    /// Current owner cache revision.
    #[must_use]
    pub fn owner_revision(&self) -> u64 {
        self.owner_cache
            .lock()
            .expect("shard owner cache mutex poisoned")
            .revision()
    }

    /// Returns a typed logical entity reference for this region's entity type.
    #[must_use]
    pub fn entity_ref(&self, entity_id: impl Into<String>) -> EntityRef<M> {
        EntityRef::new(self.entity_type().clone(), EntityId::new(entity_id))
    }

    /// Refreshes the shard owner cache.
    pub fn refresh_ownership(&self, snapshot: &ShardOwnershipSnapshot) -> ShardingResult<()> {
        self.owner_cache
            .lock()
            .expect("shard owner cache mutex poisoned")
            .refresh(snapshot)
    }

    /// Returns the owner and shard id for an entity reference.
    pub fn resolve(&self, entity: &EntityRef<M>) -> ShardingResult<(NodeId, ShardId)> {
        self.ensure_entity_type(entity)?;
        self.owner_cache
            .lock()
            .expect("shard owner cache mutex poisoned")
            .owner_for_entity(entity.entity_id())
            .map(|(owner, shard_id)| (owner.clone(), shard_id))
    }

    /// Sends a message without waiting for a reply.
    pub fn tell(&self, entity: &EntityRef<M>, message: M) -> Result<(), EntityTellError<M>> {
        let (owner, shard_id) = match self.resolve(entity) {
            Ok(resolved) => resolved,
            Err(error) => return Err(EntityTellError::NoRoute { message, error }),
        };
        let routed = RoutedEntityMessage::new(
            entity.entity_type().clone(),
            entity.entity_id().clone(),
            shard_id,
            owner,
            message,
        );
        self.route.deliver(routed)
    }

    /// Sends a request message and waits for its reply.
    pub async fn ask<R>(
        &self,
        entity: &EntityRef<M>,
        build: impl FnOnce(ReplyTo<R>) -> M,
        timeout: Duration,
    ) -> Result<R, EntityAskError>
    where
        R: Send + 'static,
    {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let message = build(ReplyTo::new(sender));
        self.tell(entity, message)
            .map_err(EntityTellError::into_ask_error)?;

        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_closed)) => Err(EntityAskError::ReplyDropped),
            Err(_elapsed) => Err(EntityAskError::Timeout),
        }
    }

    fn ensure_entity_type(&self, entity: &EntityRef<M>) -> ShardingResult<()> {
        if entity.entity_type() == self.entity_type() {
            Ok(())
        } else {
            Err(ShardingError::EntityTypeMismatch {
                expected: self.entity_type().clone(),
                actual: entity.entity_type().clone(),
            })
        }
    }
}

impl<M> Clone for ShardRegion<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            entity_type: self.entity_type.clone(),
            config: self.config.clone(),
            owner_cache: self.owner_cache.clone(),
            route: self.route.clone(),
        }
    }
}

impl<M> Debug for ShardRegion<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardRegion")
            .field("entity_type", self.entity_type())
            .field("number_of_shards", &self.config().number_of_shards())
            .field("owner_revision", &self.owner_revision())
            .finish_non_exhaustive()
    }
}

impl<M> EntityRef<M>
where
    M: Message,
{
    /// Sends a message through a shard region without waiting for a reply.
    pub fn tell(&self, region: &ShardRegion<M>, message: M) -> Result<(), EntityTellError<M>> {
        region.tell(self, message)
    }

    /// Sends a request through a shard region and waits for its reply.
    pub async fn ask<R>(
        &self,
        region: &ShardRegion<M>,
        build: impl FnOnce(ReplyTo<R>) -> M,
        timeout: Duration,
    ) -> Result<R, EntityAskError>
    where
        R: Send + 'static,
    {
        region.ask(self, build, timeout).await
    }
}

fn validate_snapshot(
    entity_type: &EntityType,
    config: &ShardingConfig,
    snapshot: &ShardOwnershipSnapshot,
) -> ShardingResult<()> {
    if snapshot.entity_type() == entity_type
        && snapshot.number_of_shards() == config.number_of_shards()
    {
        Ok(())
    } else {
        Err(ShardingError::OwnershipSnapshotMismatch {
            expected_entity_type: entity_type.clone(),
            actual_entity_type: snapshot.entity_type().clone(),
            expected_shards: config.number_of_shards(),
            actual_shards: snapshot.number_of_shards(),
        })
    }
}
