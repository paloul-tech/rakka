//! Shard owner cache and typed entity routing surface.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rakka_cluster::NodeId;
use rakka_core::{Message, ReplyTo};

use crate::coordinator::ShardOwnershipSnapshot;
use crate::error::{ShardingError, ShardingResult};
use crate::handoff::ShardHandoffState;
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
    /// Shard is temporarily unavailable during graceful handoff.
    ShardHandoff {
        /// Shard id being handed off.
        shard_id: ShardId,
        /// Current handoff state.
        state: ShardHandoffState,
    },
    /// Bounded shard buffer was full.
    ShardBufferFull {
        /// Shard id whose buffer was full.
        shard_id: ShardId,
        /// Configured capacity per shard.
        capacity: usize,
    },
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
            Self::ShardHandoff { shard_id, state } => {
                write!(f, "shard {shard_id} is {state} during graceful handoff")
            }
            Self::ShardBufferFull { shard_id, capacity } => {
                write!(f, "shard {shard_id} buffer is full at capacity {capacity}")
            }
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
                EntityDeliveryFailure::ShardHandoff { shard_id, state } => {
                    EntityAskError::ShardHandoff { shard_id, state }
                }
                EntityDeliveryFailure::ShardBufferFull { shard_id, capacity } => {
                    EntityAskError::ShardBufferFull { shard_id, capacity }
                }
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
    /// Shard is temporarily unavailable during graceful handoff.
    ShardHandoff {
        /// Shard id being handed off.
        shard_id: ShardId,
        /// Current handoff state.
        state: ShardHandoffState,
    },
    /// Bounded shard buffer was full.
    ShardBufferFull {
        /// Shard id whose buffer was full.
        shard_id: ShardId,
        /// Configured capacity per shard.
        capacity: usize,
    },
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
            Self::ShardHandoff { shard_id, state } => {
                write!(f, "shard {shard_id} is {state} during graceful handoff")
            }
            Self::ShardBufferFull { shard_id, capacity } => {
                write!(f, "shard {shard_id} buffer is full at capacity {capacity}")
            }
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

    /// Returns the local node id when this route represents local entity ownership.
    fn local_node_id(&self) -> Option<&NodeId> {
        None
    }

    /// Marks a shard as draining before ownership is published to a new owner.
    fn begin_shard_handoff(&self, _shard_id: ShardId) -> ShardingResult<usize> {
        Ok(0)
    }

    /// Stops local entities for a shard and marks it as transferring.
    fn complete_shard_handoff(&self, _shard_id: ShardId) -> ShardingResult<usize> {
        Ok(0)
    }

    /// Marks a shard as acquired by this route's local node.
    fn acquire_shard(&self, _shard_id: ShardId) -> ShardingResult<usize> {
        Ok(0)
    }

    /// Current known local handoff state for a shard, when tracked by this route.
    fn shard_handoff_state(&self, _shard_id: ShardId) -> Option<ShardHandoffState> {
        None
    }
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

/// Overflow behavior for a bounded shard message buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardBufferOverflow {
    /// Reject the new message and return it to the caller.
    FailFast,
    /// Drop the new message and keep already buffered messages.
    DropNewest,
    /// Drop the oldest buffered message and keep the new message.
    DropOldest,
}

/// Bounded buffering policy for messages sent during shard movement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardBufferConfig {
    capacity_per_shard: usize,
    overflow: ShardBufferOverflow,
    ttl: Duration,
}

impl ShardBufferConfig {
    /// Creates a buffering policy.
    #[must_use]
    pub fn new(capacity_per_shard: usize, ttl: Duration) -> Self {
        Self {
            capacity_per_shard: capacity_per_shard.max(1),
            overflow: ShardBufferOverflow::FailFast,
            ttl,
        }
    }

    /// Maximum queued messages per shard.
    #[must_use]
    pub const fn capacity_per_shard(&self) -> usize {
        self.capacity_per_shard
    }

    /// Overflow behavior.
    #[must_use]
    pub const fn overflow(&self) -> ShardBufferOverflow {
        self.overflow
    }

    /// Maximum time a buffered message remains eligible for delivery.
    #[must_use]
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Sets the maximum queued messages per shard.
    #[must_use]
    pub fn with_capacity_per_shard(mut self, capacity_per_shard: usize) -> Self {
        self.capacity_per_shard = capacity_per_shard.max(1);
        self
    }

    /// Sets overflow behavior.
    #[must_use]
    pub const fn with_overflow(mut self, overflow: ShardBufferOverflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// Sets maximum buffered-message lifetime.
    #[must_use]
    pub const fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }
}

impl Default for ShardBufferConfig {
    fn default() -> Self {
        Self::new(64, Duration::from_secs(5))
    }
}

struct ShardMessageBuffer<M> {
    config: ShardBufferConfig,
    queues: BTreeMap<ShardId, VecDeque<BufferedEntityMessage<M>>>,
    paused_entities: BTreeMap<EntityId, Instant>,
}

impl<M> ShardMessageBuffer<M> {
    fn new(config: ShardBufferConfig) -> Self {
        Self {
            config,
            queues: BTreeMap::new(),
            paused_entities: BTreeMap::new(),
        }
    }

    fn config(&self) -> &ShardBufferConfig {
        &self.config
    }

    fn enqueue(
        &mut self,
        entity_type: EntityType,
        entity_id: EntityId,
        shard_id: ShardId,
        message: M,
        enqueued_at: Instant,
    ) -> Result<(), EntityTellError<M>> {
        let queue = self.queues.entry(shard_id).or_default();
        if queue.len() < self.config.capacity_per_shard {
            queue.push_back(BufferedEntityMessage::new(
                entity_type,
                entity_id,
                shard_id,
                message,
                enqueued_at,
            ));
            return Ok(());
        }

        match self.config.overflow {
            ShardBufferOverflow::FailFast => Err(EntityTellError::Delivery {
                message,
                failure: EntityDeliveryFailure::ShardBufferFull {
                    shard_id,
                    capacity: self.config.capacity_per_shard,
                },
            }),
            ShardBufferOverflow::DropNewest => Ok(()),
            ShardBufferOverflow::DropOldest => {
                let _dropped = queue.pop_front();
                queue.push_back(BufferedEntityMessage::new(
                    entity_type,
                    entity_id,
                    shard_id,
                    message,
                    enqueued_at,
                ));
                Ok(())
            }
        }
    }

    fn requeue(&mut self, message: BufferedEntityMessage<M>) {
        let queue = self.queues.entry(message.shard_id).or_default();
        if queue.len() < self.config.capacity_per_shard {
            queue.push_back(message);
        }
    }

    fn pause_entity(&mut self, entity_id: EntityId, until: Instant) {
        self.paused_entities.insert(entity_id, until);
    }

    fn is_entity_paused(&mut self, entity_id: &EntityId, now: Instant) -> bool {
        self.paused_entities
            .retain(|_entity_id, paused_until| *paused_until > now);
        self.paused_entities.contains_key(entity_id)
    }

    fn resume_entity(&mut self, entity_id: &EntityId) {
        self.paused_entities.remove(entity_id);
    }

    fn drain_ready(&mut self, now: Instant) -> Vec<BufferedEntityMessage<M>> {
        self.paused_entities
            .retain(|_entity_id, paused_until| *paused_until > now);
        let mut ready = Vec::new();
        for queue in self.queues.values_mut() {
            let len = queue.len();
            for _ in 0..len {
                let Some(message) = queue.pop_front() else {
                    break;
                };
                if now.duration_since(message.enqueued_at) > self.config.ttl {
                    continue;
                }
                if self.paused_entities.contains_key(&message.entity_id) {
                    queue.push_back(message);
                } else {
                    ready.push(message);
                }
            }
        }
        self.queues.retain(|_shard_id, queue| !queue.is_empty());
        ready
    }

    fn message_count(&self) -> usize {
        self.queues.values().map(VecDeque::len).sum()
    }

    fn message_count_for_shard(&self, shard_id: ShardId) -> usize {
        self.queues.get(&shard_id).map_or(0, VecDeque::len)
    }
}

struct BufferedEntityMessage<M> {
    entity_type: EntityType,
    entity_id: EntityId,
    shard_id: ShardId,
    message: M,
    enqueued_at: Instant,
}

impl<M> BufferedEntityMessage<M> {
    fn new(
        entity_type: EntityType,
        entity_id: EntityId,
        shard_id: ShardId,
        message: M,
        enqueued_at: Instant,
    ) -> Self {
        Self {
            entity_type,
            entity_id,
            shard_id,
            message,
            enqueued_at,
        }
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
    buffer: Arc<Mutex<Option<ShardMessageBuffer<M>>>>,
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
            buffer: Arc::new(Mutex::new(None)),
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
            buffer: Arc::new(Mutex::new(None)),
        })
    }

    /// Enables bounded buffering for transient shard movement states.
    #[must_use]
    pub fn with_buffering(self, config: ShardBufferConfig) -> Self {
        *self
            .buffer
            .lock()
            .expect("shard message buffer mutex poisoned") = Some(ShardMessageBuffer::new(config));
        self
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
            .refresh(snapshot)?;
        self.flush_buffered();
        Ok(())
    }

    /// Returns configured buffering policy, when buffering is enabled.
    #[must_use]
    pub fn buffer_config(&self) -> Option<ShardBufferConfig> {
        self.buffer
            .lock()
            .expect("shard message buffer mutex poisoned")
            .as_ref()
            .map(|buffer| buffer.config().clone())
    }

    /// Number of messages currently buffered across all shards.
    #[must_use]
    pub fn buffered_message_count(&self) -> usize {
        self.buffer
            .lock()
            .expect("shard message buffer mutex poisoned")
            .as_ref()
            .map_or(0, ShardMessageBuffer::message_count)
    }

    /// Number of messages currently buffered for one shard.
    #[must_use]
    pub fn buffered_message_count_for_shard(&self, shard_id: ShardId) -> usize {
        self.buffer
            .lock()
            .expect("shard message buffer mutex poisoned")
            .as_ref()
            .map_or(0, |buffer| buffer.message_count_for_shard(shard_id))
    }

    /// Marks an entity as temporarily passivating so incoming messages are buffered.
    pub fn begin_entity_passivation(&self, entity_id: EntityId, duration: Duration) {
        let Some(paused_until) = Instant::now().checked_add(duration) else {
            return;
        };
        if let Some(buffer) = self
            .buffer
            .lock()
            .expect("shard message buffer mutex poisoned")
            .as_mut()
        {
            buffer.pause_entity(entity_id, paused_until);
        }
    }

    /// Clears an entity passivation pause and attempts to flush buffered messages.
    pub fn end_entity_passivation(&self, entity_id: &EntityId) {
        if let Some(buffer) = self
            .buffer
            .lock()
            .expect("shard message buffer mutex poisoned")
            .as_mut()
        {
            buffer.resume_entity(entity_id);
        }
        self.flush_buffered();
    }

    /// Attempts to deliver buffered messages whose shard/entity is available.
    pub fn flush_buffered(&self) {
        let ready = {
            let mut buffer = self
                .buffer
                .lock()
                .expect("shard message buffer mutex poisoned");
            let Some(buffer) = buffer.as_mut() else {
                return;
            };
            buffer.drain_ready(Instant::now())
        };

        for message in ready {
            self.flush_one_buffered(message);
        }
    }

    /// Local node id for routes that host local entities.
    #[must_use]
    pub fn local_node_id(&self) -> Option<&NodeId> {
        self.route.local_node_id()
    }

    /// Marks a shard as draining before ownership is published to a new owner.
    pub fn begin_shard_handoff(&self, shard_id: ShardId) -> ShardingResult<usize> {
        self.route.begin_shard_handoff(shard_id)
    }

    /// Stops local entities for a shard and marks it as transferring.
    pub fn complete_shard_handoff(&self, shard_id: ShardId) -> ShardingResult<usize> {
        self.route.complete_shard_handoff(shard_id)
    }

    /// Marks a shard as acquired by this region's local route.
    pub fn acquire_shard(&self, shard_id: ShardId) -> ShardingResult<usize> {
        let stopped = self.route.acquire_shard(shard_id)?;
        self.flush_buffered();
        Ok(stopped)
    }

    /// Current known local handoff state for a shard, when tracked by this route.
    #[must_use]
    pub fn shard_handoff_state(&self, shard_id: ShardId) -> Option<ShardHandoffState> {
        self.route.shard_handoff_state(shard_id)
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
        if let Err(error) = self.ensure_entity_type(entity) {
            return Err(EntityTellError::NoRoute { message, error });
        }
        let shard_id = entity.shard_id(&self.config);
        if self.is_entity_buffered(entity.entity_id()) {
            return self.enqueue_buffered(
                entity.entity_type().clone(),
                entity.entity_id().clone(),
                shard_id,
                message,
            );
        }
        let owner = match self
            .owner_cache
            .lock()
            .expect("shard owner cache mutex poisoned")
            .owner_for_entity(entity.entity_id())
        {
            Ok((owner, _shard_id)) => owner.clone(),
            Err(error) if self.can_buffer_no_route(&error) => {
                return self.enqueue_buffered(
                    entity.entity_type().clone(),
                    entity.entity_id().clone(),
                    shard_id,
                    message,
                );
            }
            Err(error) => return Err(EntityTellError::NoRoute { message, error }),
        };
        let routed = RoutedEntityMessage::new(
            entity.entity_type().clone(),
            entity.entity_id().clone(),
            shard_id,
            owner,
            message,
        );
        self.deliver_or_buffer(routed)
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

    fn deliver_or_buffer(&self, routed: RoutedEntityMessage<M>) -> Result<(), EntityTellError<M>> {
        let entity_type = routed.entity_type().clone();
        let entity_id = routed.entity_id().clone();
        match self.route.deliver(routed) {
            Ok(()) => Ok(()),
            Err(EntityTellError::Delivery {
                message,
                failure:
                    failure @ EntityDeliveryFailure::ShardHandoff {
                        shard_id: failure_shard_id,
                        state: _,
                    },
            }) => {
                if self.buffer_config().is_none() {
                    return Err(EntityTellError::Delivery { message, failure });
                }
                self.enqueue_buffered(entity_type, entity_id, failure_shard_id, message)
            }
            Err(error) => Err(error),
        }
    }

    fn enqueue_buffered(
        &self,
        entity_type: EntityType,
        entity_id: EntityId,
        shard_id: ShardId,
        message: M,
    ) -> Result<(), EntityTellError<M>> {
        let mut buffer = self
            .buffer
            .lock()
            .expect("shard message buffer mutex poisoned");
        if let Some(buffer) = buffer.as_mut() {
            buffer.enqueue(entity_type, entity_id, shard_id, message, Instant::now())
        } else {
            Err(EntityTellError::Delivery {
                message,
                failure: EntityDeliveryFailure::ShardHandoff {
                    shard_id,
                    state: self
                        .shard_handoff_state(shard_id)
                        .unwrap_or(ShardHandoffState::Transferring),
                },
            })
        }
    }

    fn enqueue_existing_buffered(&self, message: BufferedEntityMessage<M>) {
        if let Some(buffer) = self
            .buffer
            .lock()
            .expect("shard message buffer mutex poisoned")
            .as_mut()
        {
            buffer.requeue(message);
        }
    }

    fn flush_one_buffered(&self, buffered: BufferedEntityMessage<M>) {
        let entity = EntityRef::new(buffered.entity_type.clone(), buffered.entity_id.clone());
        let Ok((owner, shard_id)) = self.resolve(&entity) else {
            self.enqueue_existing_buffered(buffered);
            return;
        };
        match self.route.deliver(RoutedEntityMessage::new(
            buffered.entity_type.clone(),
            buffered.entity_id.clone(),
            shard_id,
            owner,
            buffered.message,
        )) {
            Ok(()) => {}
            Err(EntityTellError::Delivery {
                message,
                failure: EntityDeliveryFailure::ShardHandoff { shard_id, .. },
            }) => self.enqueue_existing_buffered(BufferedEntityMessage::new(
                buffered.entity_type,
                buffered.entity_id,
                shard_id,
                message,
                buffered.enqueued_at,
            )),
            Err(EntityTellError::NoRoute { message, .. }) => {
                self.enqueue_existing_buffered(BufferedEntityMessage::new(
                    buffered.entity_type,
                    buffered.entity_id,
                    buffered.shard_id,
                    message,
                    buffered.enqueued_at,
                ))
            }
            Err(EntityTellError::Delivery { .. }) => {}
        }
    }

    fn is_entity_buffered(&self, entity_id: &EntityId) -> bool {
        self.buffer
            .lock()
            .expect("shard message buffer mutex poisoned")
            .as_mut()
            .is_some_and(|buffer| buffer.is_entity_paused(entity_id, Instant::now()))
    }

    fn can_buffer_no_route(&self, error: &ShardingError) -> bool {
        matches!(
            error,
            ShardingError::NoShardOwner { .. } | ShardingError::NoEntityOwner { .. }
        ) && self.buffer_config().is_some()
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
            buffer: self.buffer.clone(),
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
