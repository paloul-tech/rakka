//! Local actor delivery route for sharded entities owned by this node.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use rakka_cluster::NodeId;
use rakka_core::{Actor, ActorOptions, ActorRef, ActorSystem, Message, TellError};

use crate::handoff::ShardHandoffState;
use crate::identity::{EntityId, EntityType, ShardId};
use crate::routing::{EntityDeliveryFailure, EntityRoute, EntityTellError, RoutedEntityMessage};

/// Context supplied when a local sharded entity actor is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalEntityContext {
    local_node_id: NodeId,
    entity_type: EntityType,
    entity_id: EntityId,
    shard_id: ShardId,
    actor_name: String,
}

impl LocalEntityContext {
    /// Creates local entity actor context.
    #[must_use]
    pub fn new(
        local_node_id: NodeId,
        entity_type: EntityType,
        entity_id: EntityId,
        shard_id: ShardId,
    ) -> Self {
        let actor_name = local_entity_actor_name(&entity_type, &entity_id, shard_id);
        Self {
            local_node_id,
            entity_type,
            entity_id,
            shard_id,
            actor_name,
        }
    }

    /// Local cluster node id.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
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

    /// Stable actor name used when spawning this local entity actor.
    #[must_use]
    pub fn actor_name(&self) -> &str {
        &self.actor_name
    }
}

/// Local route that starts sharded entity actors on demand.
pub struct LocalEntityRoute<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    local_node_id: NodeId,
    system: ActorSystem,
    actor_options: ActorOptions,
    factory: Arc<F>,
    actors: Arc<Mutex<BTreeMap<EntityId, LocalEntityHandle<M>>>>,
    shard_states: Arc<Mutex<BTreeMap<ShardId, ShardHandoffState>>>,
    _actor: PhantomData<fn() -> A>,
}

struct LocalEntityHandle<M>
where
    M: Message,
{
    shard_id: ShardId,
    actor_ref: ActorRef<M>,
}

impl<M> Clone for LocalEntityHandle<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            shard_id: self.shard_id,
            actor_ref: self.actor_ref.clone(),
        }
    }
}

impl<M, A, F> LocalEntityRoute<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    /// Creates a local entity route with default actor options.
    #[must_use]
    pub fn new(local_node_id: NodeId, system: ActorSystem, factory: F) -> Self {
        Self {
            local_node_id,
            system,
            actor_options: ActorOptions::default(),
            factory: Arc::new(factory),
            actors: Arc::new(Mutex::new(BTreeMap::new())),
            shard_states: Arc::new(Mutex::new(BTreeMap::new())),
            _actor: PhantomData,
        }
    }

    /// Sets actor options used for newly spawned entity actors.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Local cluster node id accepted by this route.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Number of currently cached local entity actors.
    #[must_use]
    pub fn entity_count(&self) -> usize {
        let mut actors = self
            .actors
            .lock()
            .expect("local entity registry mutex poisoned");
        actors.retain(|_entity_id, handle| !handle.actor_ref.is_terminated());
        actors.len()
    }

    /// Returns a cached local entity actor ref.
    #[must_use]
    pub fn entity_actor(&self, entity_id: &EntityId) -> Option<ActorRef<M>> {
        let mut actors = self
            .actors
            .lock()
            .expect("local entity registry mutex poisoned");
        if actors
            .get(entity_id)
            .is_some_and(|handle| handle.actor_ref.is_terminated())
        {
            actors.remove(entity_id);
            None
        } else {
            actors.get(entity_id).map(|handle| handle.actor_ref.clone())
        }
    }

    /// Current handoff state for a shard.
    #[must_use]
    pub fn shard_handoff_state(&self, shard_id: ShardId) -> ShardHandoffState {
        self.shard_states
            .lock()
            .expect("local shard state registry mutex poisoned")
            .get(&shard_id)
            .copied()
            .unwrap_or(ShardHandoffState::Owning)
    }

    /// Marks a shard as draining and rejects new local deliveries.
    pub fn mark_shard_draining(&self, shard_id: ShardId) -> usize {
        self.set_shard_state(shard_id, ShardHandoffState::Draining);
        0
    }

    /// Stops local entities for a shard and marks it as transferring.
    pub fn mark_shard_transferring(&self, shard_id: ShardId) -> usize {
        self.set_shard_state(shard_id, ShardHandoffState::Transferring);
        self.stop_entities_for_shard(shard_id)
    }

    /// Marks a shard as acquired by this local route.
    pub fn mark_shard_acquired(&self, shard_id: ShardId) -> usize {
        self.set_shard_state(shard_id, ShardHandoffState::Acquired);
        0
    }

    /// Marks a shard as normally owned by this local route.
    pub fn mark_shard_owning(&self, shard_id: ShardId) -> usize {
        self.set_shard_state(shard_id, ShardHandoffState::Owning);
        0
    }

    fn actor_for(
        &self,
        entity_type: &EntityType,
        entity_id: &EntityId,
        shard_id: ShardId,
    ) -> Result<ActorRef<M>, EntityDeliveryFailure> {
        self.ensure_shard_accepts_delivery(shard_id)?;

        let mut actors = self
            .actors
            .lock()
            .expect("local entity registry mutex poisoned");

        if let Some(handle) = actors
            .get(entity_id)
            .filter(|handle| !handle.actor_ref.is_terminated())
        {
            return Ok(handle.actor_ref.clone());
        }

        let context = LocalEntityContext::new(
            self.local_node_id.clone(),
            entity_type.clone(),
            entity_id.clone(),
            shard_id,
        );
        let actor_name = context.actor_name().to_string();
        let context_for_factory = context.clone();
        let factory = self.factory.clone();
        let actor_ref = self
            .system
            .spawn_actor_with_options(
                actor_name,
                move || factory(context_for_factory.clone()),
                self.actor_options.clone(),
            )
            .map_err(|error| EntityDeliveryFailure::SpawnFailed(error.to_string()))?;

        actors.insert(
            entity_id.clone(),
            LocalEntityHandle {
                shard_id,
                actor_ref: actor_ref.clone(),
            },
        );
        Ok(actor_ref)
    }

    fn set_shard_state(&self, shard_id: ShardId, state: ShardHandoffState) {
        self.shard_states
            .lock()
            .expect("local shard state registry mutex poisoned")
            .insert(shard_id, state);
    }

    fn ensure_shard_accepts_delivery(
        &self,
        shard_id: ShardId,
    ) -> Result<(), EntityDeliveryFailure> {
        match self.shard_handoff_state(shard_id) {
            ShardHandoffState::Owning | ShardHandoffState::Acquired => Ok(()),
            state @ (ShardHandoffState::Draining | ShardHandoffState::Transferring) => {
                Err(EntityDeliveryFailure::ShardHandoff { shard_id, state })
            }
        }
    }

    fn stop_entities_for_shard(&self, shard_id: ShardId) -> usize {
        let mut actors = self
            .actors
            .lock()
            .expect("local entity registry mutex poisoned");
        let entity_ids = actors
            .iter()
            .filter(|(_entity_id, handle)| handle.shard_id == shard_id)
            .map(|(entity_id, _handle)| entity_id.clone())
            .collect::<Vec<_>>();
        let mut stopped = 0;

        for entity_id in entity_ids {
            if let Some(handle) = actors.remove(&entity_id) {
                let _ = handle.actor_ref.stop();
                stopped += 1;
            }
        }

        stopped
    }
}

impl<M, A, F> Clone for LocalEntityRoute<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            local_node_id: self.local_node_id.clone(),
            system: self.system.clone(),
            actor_options: self.actor_options.clone(),
            factory: self.factory.clone(),
            actors: self.actors.clone(),
            shard_states: self.shard_states.clone(),
            _actor: PhantomData,
        }
    }
}

impl<M, A, F> Debug for LocalEntityRoute<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalEntityRoute")
            .field("local_node_id", &self.local_node_id)
            .field("entity_count", &self.entity_count())
            .finish_non_exhaustive()
    }
}

impl<M, A, F> EntityRoute<M> for LocalEntityRoute<M, A, F>
where
    M: Message,
    A: Actor<Msg = M>,
    F: Fn(LocalEntityContext) -> A + Send + Sync + 'static,
{
    fn deliver(&self, message: RoutedEntityMessage<M>) -> Result<(), EntityTellError<M>> {
        if message.owner() != &self.local_node_id {
            return Err(EntityTellError::Delivery {
                failure: EntityDeliveryFailure::NotLocal {
                    owner: message.owner().clone(),
                },
                message: message.into_message(),
            });
        }

        let actor_ref = match self.actor_for(
            message.entity_type(),
            message.entity_id(),
            message.shard_id(),
        ) {
            Ok(actor_ref) => actor_ref,
            Err(failure) => {
                return Err(EntityTellError::Delivery {
                    message: message.into_message(),
                    failure,
                });
            }
        };

        actor_ref
            .tell(message.into_message())
            .map_err(|error| match error {
                TellError::Full(message) => EntityTellError::Delivery {
                    message,
                    failure: EntityDeliveryFailure::MailboxFull,
                },
                TellError::Closed(message) => EntityTellError::Delivery {
                    message,
                    failure: EntityDeliveryFailure::MailboxClosed,
                },
            })
    }

    fn local_node_id(&self) -> Option<&NodeId> {
        Some(&self.local_node_id)
    }

    fn begin_shard_handoff(&self, shard_id: ShardId) -> crate::ShardingResult<usize> {
        Ok(self.mark_shard_draining(shard_id))
    }

    fn complete_shard_handoff(&self, shard_id: ShardId) -> crate::ShardingResult<usize> {
        Ok(self.mark_shard_transferring(shard_id))
    }

    fn acquire_shard(&self, shard_id: ShardId) -> crate::ShardingResult<usize> {
        Ok(self.mark_shard_acquired(shard_id))
    }

    fn shard_handoff_state(&self, shard_id: ShardId) -> Option<ShardHandoffState> {
        Some(LocalEntityRoute::shard_handoff_state(self, shard_id))
    }
}

fn local_entity_actor_name(
    entity_type: &EntityType,
    entity_id: &EntityId,
    shard_id: ShardId,
) -> String {
    format!(
        "shard-{}-{}-{}",
        sanitize(entity_type.as_str()),
        shard_id.as_u32(),
        sanitize(entity_id.as_str())
    )
}

fn sanitize(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        "entity".to_string()
    } else {
        sanitized
    }
}
