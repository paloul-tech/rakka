//! Local actor delivery route for sharded entities owned by this node.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rakka_cluster::NodeId;
use rakka_core::{
    Actor, ActorOptions, ActorRef, ActorSystem, ActorUid, Message, StopError, TellError,
};

use crate::handoff::ShardHandoffState;
use crate::identity::{EntityId, EntityType, ShardId};
use crate::routing::{EntityDeliveryFailure, EntityRoute, EntityTellError, RoutedEntityMessage};

type ActivationObserver = Arc<dyn Fn(LocalEntityContext) + Send + Sync>;

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
///
/// Each entity occupies one slot that is either active or stopping. Delivery, the
/// decision to stop an incarnation, and the stop itself all run under the slot
/// registry lock, so a message can never be queued behind a stop the route
/// initiated: a message that arrives while an incarnation stops is held, bounded by
/// the configured mailbox capacity, and delivered in order to the next incarnation.
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
    actors: Arc<Mutex<SlotRegistry<M>>>,
    shard_states: Arc<Mutex<BTreeMap<ShardId, ShardHandoffState>>>,
    armed_idle_timers: Arc<AtomicUsize>,
    idle_passivation_timeout: Option<Duration>,
    activation_observer: Option<ActivationObserver>,
    _actor: PhantomData<fn() -> A>,
}

type SlotRegistry<M> = BTreeMap<EntityId, LocalEntitySlot<M>>;

enum LocalEntitySlot<M>
where
    M: Message,
{
    Active {
        context: LocalEntityContext,
        actor_ref: ActorRef<M>,
        idle_deadline: Option<Instant>,
        timer_armed: bool,
    },
    Stopping {
        context: LocalEntityContext,
        actor_ref: ActorRef<M>,
        held: VecDeque<M>,
        reactivate: bool,
    },
}

impl<M> LocalEntitySlot<M>
where
    M: Message,
{
    fn context(&self) -> &LocalEntityContext {
        match self {
            Self::Active { context, .. } | Self::Stopping { context, .. } => context,
        }
    }

    fn is_terminated_active(&self) -> bool {
        matches!(self, Self::Active { actor_ref, .. } if actor_ref.is_terminated())
    }
}

/// Outcome of asking the route to stop one incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassivationOutcome {
    /// The incarnation was told to stop and its slot now holds messages for the next one.
    Stopping,
    /// The incarnation was already stopping.
    AlreadyStopping,
    /// No live incarnation was registered.
    Absent,
    /// The stop could not be queued (mailbox full); the incarnation stays active.
    Abandoned,
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
            armed_idle_timers: Arc::new(AtomicUsize::new(0)),
            idle_passivation_timeout: None,
            activation_observer: None,
            _actor: PhantomData,
        }
    }

    /// Sets actor options used for newly spawned entity actors.
    #[must_use]
    pub fn with_actor_options(mut self, actor_options: ActorOptions) -> Self {
        self.actor_options = actor_options;
        self
    }

    /// Enables idle passivation for local entities after the given timeout.
    #[must_use]
    pub fn with_idle_passivation(mut self, timeout: Duration) -> Self {
        self.idle_passivation_timeout = Some(timeout);
        self
    }

    /// Observes successful local entity activation or reuse.
    #[must_use]
    pub fn with_activation_observer(
        mut self,
        observer: impl Fn(LocalEntityContext) + Send + Sync + 'static,
    ) -> Self {
        self.activation_observer = Some(Arc::new(observer));
        self
    }

    /// Local cluster node id accepted by this route.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Configured idle passivation timeout, when enabled.
    #[must_use]
    pub const fn idle_passivation_timeout(&self) -> Option<Duration> {
        self.idle_passivation_timeout
    }

    /// Diagnostic: number of idle timer tasks currently sleeping for this route's
    /// entities. With idle passivation enabled this is at most one per active entity.
    #[must_use]
    pub fn armed_idle_timers(&self) -> usize {
        self.armed_idle_timers.load(Ordering::Acquire)
    }

    /// Number of active local entity incarnations.
    ///
    /// An incarnation that is stopping is not counted: it takes no further
    /// messages itself, and its slot only holds messages for the next incarnation.
    #[must_use]
    pub fn entity_count(&self) -> usize {
        let _removed = self.reap_terminated_entities();
        self.lock_actors()
            .values()
            .filter(|slot| matches!(slot, LocalEntitySlot::Active { .. }))
            .count()
    }

    /// Returns the actor ref of an active local entity.
    #[must_use]
    pub fn entity_actor(&self, entity_id: &EntityId) -> Option<ActorRef<M>> {
        let mut actors = self.lock_actors();
        remove_if_terminated_active(&mut actors, entity_id);
        match actors.get(entity_id) {
            Some(LocalEntitySlot::Active { actor_ref, .. }) => Some(actor_ref.clone()),
            _ => None,
        }
    }

    /// Removes active entity slots whose actor has terminated on its own.
    ///
    /// A stopping slot belongs to its stop watcher and is left alone.
    #[must_use]
    pub fn reap_terminated_entities(&self) -> usize {
        let mut actors = self.lock_actors();
        let before = actors.len();
        actors.retain(|_entity_id, slot| !slot.is_terminated_active());
        before - actors.len()
    }

    /// Explicitly passivates one local entity actor.
    ///
    /// Returns true when the entity is now stopping or was already stopping. See
    /// [`LocalEntityRoute::passivate_entity_with`] for the stop's bookkeeping.
    #[must_use]
    pub fn passivate_entity(&self, entity_id: &EntityId) -> bool {
        self.passivate_entity_with(entity_id, None)
    }

    /// Explicitly passivates one local entity actor, queueing `stop_message` ahead of
    /// the stop in the same critical section.
    ///
    /// Returns true when the entity is now stopping or was already stopping. Returns
    /// false when no live incarnation exists or when the stop could not be queued
    /// because the mailbox is full; in that case the entity stays active and a later
    /// passivation delivers the stop message again, so stop messages must be
    /// idempotent.
    ///
    /// A task watches the stopping incarnation and spawns the next one when
    /// messages were held for it. When this is called from a thread with no Tokio
    /// runtime, the stop is still queued but no watcher follows it: the next
    /// delivery to the entity, once the old incarnation has terminated, spawns the
    /// next incarnation and hands it whatever was held.
    #[must_use]
    pub fn passivate_entity_with(&self, entity_id: &EntityId, stop_message: Option<M>) -> bool {
        let mut actors = self.lock_actors();
        matches!(
            self.passivate_slot(&mut actors, entity_id, stop_message),
            PassivationOutcome::Stopping | PassivationOutcome::AlreadyStopping
        )
    }

    /// Explicitly passivates every active local entity actor in a shard.
    ///
    /// Returns the number of entities that entered the stopping state. Messages held
    /// for a stopping entity of that shard are dropped with a warning: the shard has
    /// left this node, so its next incarnation must not start here. An entity whose
    /// stop could not be queued because its mailbox was full is not counted; the
    /// route keeps retrying that stop in the background (with a Tokio runtime
    /// current) until it is queued, so the entity does not outlive the shard.
    #[must_use]
    pub fn passivate_shard(&self, shard_id: ShardId) -> usize {
        let mut actors = self.lock_actors();
        let entity_ids = actors
            .iter()
            .filter(|(_entity_id, slot)| slot.context().shard_id() == shard_id)
            .map(|(entity_id, _slot)| entity_id.clone())
            .collect::<Vec<_>>();
        let mut passivated = 0;

        for entity_id in entity_ids {
            match actors.get_mut(&entity_id) {
                Some(LocalEntitySlot::Stopping {
                    context,
                    held,
                    reactivate,
                    ..
                }) => {
                    if !held.is_empty() {
                        warn_dropped(context, held.len(), "shard left this node");
                    }
                    held.clear();
                    *reactivate = false;
                }
                _ => match self.passivate_slot(&mut actors, &entity_id, None) {
                    PassivationOutcome::Stopping => passivated += 1,
                    PassivationOutcome::Abandoned => {
                        if let Some(LocalEntitySlot::Active { actor_ref, .. }) =
                            actors.get(&entity_id)
                        {
                            self.spawn_owed_stop(entity_id.clone(), actor_ref.uid());
                        }
                    }
                    PassivationOutcome::AlreadyStopping | PassivationOutcome::Absent => {}
                },
            }
        }

        passivated
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
        self.passivate_shard(shard_id)
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

    fn lock_actors(&self) -> std::sync::MutexGuard<'_, SlotRegistry<M>> {
        self.actors
            .lock()
            .expect("local entity registry mutex poisoned")
    }

    /// The current Tokio runtime, when this call runs inside one. Route tasks (the
    /// stop watcher, the idle timer, an owed stop) are spawned only then; on a plain
    /// thread the route degrades to resolving a stop on the next delivery instead
    /// of panicking.
    fn runtime_handle() -> Option<tokio::runtime::Handle> {
        tokio::runtime::Handle::try_current().ok()
    }

    /// Retries a stop that `passivate_shard` could not queue because the entity's
    /// mailbox was full, until it is queued or the incarnation is gone.
    fn spawn_owed_stop(&self, entity_id: EntityId, uid: ActorUid) {
        let Some(handle) = Self::runtime_handle() else {
            return;
        };
        let route = self.clone();
        handle.spawn(async move {
            let mut delay = Duration::from_millis(10);
            loop {
                tokio::time::sleep(delay).await;
                let outcome = {
                    let mut actors = route.lock_actors();
                    let mine = matches!(
                        actors.get(&entity_id),
                        Some(LocalEntitySlot::Active { actor_ref, .. }) if actor_ref.uid() == uid
                    );
                    if !mine {
                        return;
                    }
                    route.passivate_slot(&mut actors, &entity_id, None)
                };
                if outcome != PassivationOutcome::Abandoned {
                    return;
                }
                delay = (delay * 2).min(Duration::from_secs(1));
            }
        });
    }

    /// Delivers one routed message under the registry lock. Returns the context to
    /// report to the activation observer once the lock is released.
    fn deliver_locked(
        &self,
        actors: &mut SlotRegistry<M>,
        message: RoutedEntityMessage<M>,
    ) -> Result<Option<LocalEntityContext>, EntityTellError<M>> {
        let entity_id = message.entity_id().clone();
        let context = LocalEntityContext::new(
            self.local_node_id.clone(),
            message.entity_type().clone(),
            entity_id.clone(),
            message.shard_id(),
        );

        match actors.get_mut(&entity_id) {
            Some(LocalEntitySlot::Active { actor_ref, .. }) if !actor_ref.is_terminated() => {
                let actor_ref = actor_ref.clone();
                if actor_ref.is_stopping() {
                    self.hold_behind_self_stop(actors, &entity_id, message.into_message());
                    return Ok(None);
                }
                match actor_ref.tell(message.into_message()) {
                    Ok(()) => {
                        self.note_delivery(actors, &entity_id);
                        Ok(Some(context))
                    }
                    Err(TellError::Full(message)) => Err(EntityTellError::Delivery {
                        message,
                        failure: EntityDeliveryFailure::MailboxFull,
                    }),
                    Err(TellError::Closed(message)) if actor_ref.is_terminated() => {
                        actors.remove(&entity_id);
                        self.activate_and_deliver(actors, context, VecDeque::new(), message)
                    }
                    Err(TellError::Closed(message)) => {
                        self.hold_behind_self_stop(actors, &entity_id, message);
                        Ok(None)
                    }
                }
            }
            Some(LocalEntitySlot::Stopping {
                actor_ref, held, ..
            }) if !actor_ref.is_terminated() => {
                if held.len() < self.actor_options.mailbox_capacity {
                    held.push_back(message.into_message());
                    Ok(None)
                } else {
                    Err(EntityTellError::Delivery {
                        message: message.into_message(),
                        failure: EntityDeliveryFailure::MailboxFull,
                    })
                }
            }
            Some(LocalEntitySlot::Stopping { .. }) => {
                // The watcher has not run yet; resolve the stop inline.
                let Some(LocalEntitySlot::Stopping { held, .. }) = actors.remove(&entity_id) else {
                    unreachable!("slot checked above")
                };
                self.activate_and_deliver(actors, context, held, message.into_message())
            }
            _ => {
                actors.remove(&entity_id);
                self.activate_and_deliver(actors, context, VecDeque::new(), message.into_message())
            }
        }
    }

    /// Converts an active slot whose actor is stopping on its own into a stopping slot
    /// holding `message`, and starts the stop watcher.
    fn hold_behind_self_stop(
        &self,
        actors: &mut SlotRegistry<M>,
        entity_id: &EntityId,
        message: M,
    ) {
        let Some(LocalEntitySlot::Active {
            context, actor_ref, ..
        }) = actors.remove(entity_id)
        else {
            unreachable!("caller holds an active slot")
        };
        let mut held = VecDeque::new();
        held.push_back(message);
        actors.insert(
            entity_id.clone(),
            LocalEntitySlot::Stopping {
                context,
                actor_ref: actor_ref.clone(),
                held,
                reactivate: false,
            },
        );
        self.spawn_stop_watcher(entity_id.clone(), actor_ref);
    }

    /// Spawns the next incarnation, drains `held` into it in order, then delivers
    /// `message`. The slot is registered active on success.
    fn activate_and_deliver(
        &self,
        actors: &mut SlotRegistry<M>,
        context: LocalEntityContext,
        held: VecDeque<M>,
        message: M,
    ) -> Result<Option<LocalEntityContext>, EntityTellError<M>> {
        let actor_ref = match self.activate_slot(actors, context.clone(), held) {
            Ok(actor_ref) => actor_ref,
            Err(failure) => {
                return Err(EntityTellError::Delivery { message, failure });
            }
        };
        match actor_ref.tell(message) {
            Ok(()) => {
                self.note_delivery(actors, context.entity_id());
                Ok(Some(context))
            }
            Err(TellError::Full(message)) => Err(EntityTellError::Delivery {
                message,
                failure: EntityDeliveryFailure::MailboxFull,
            }),
            Err(TellError::Closed(message)) => Err(EntityTellError::Delivery {
                message,
                failure: EntityDeliveryFailure::MailboxClosed,
            }),
        }
    }

    /// Spawns the next incarnation for `context`, drains `held` into it in order, and
    /// registers the slot as active. On a spawn failure the held messages are dropped
    /// with a warning and no slot is registered.
    fn activate_slot(
        &self,
        actors: &mut SlotRegistry<M>,
        context: LocalEntityContext,
        held: VecDeque<M>,
    ) -> Result<ActorRef<M>, EntityDeliveryFailure> {
        let actor_ref = match self.spawn_incarnation(&context) {
            Ok(actor_ref) => actor_ref,
            Err(failure) => {
                if !held.is_empty() {
                    warn_dropped(
                        &context,
                        held.len(),
                        "next incarnation could not be spawned",
                    );
                }
                return Err(failure);
            }
        };
        let total = held.len();
        for (index, message) in held.into_iter().enumerate() {
            if actor_ref.tell(message).is_err() {
                warn_dropped(
                    &context,
                    total - index,
                    "next incarnation stopped before taking held messages",
                );
                break;
            }
        }
        let entity_id = context.entity_id().clone();
        actors.insert(
            entity_id.clone(),
            LocalEntitySlot::Active {
                context,
                actor_ref: actor_ref.clone(),
                idle_deadline: None,
                timer_armed: false,
            },
        );
        if total > 0 {
            self.note_delivery(actors, &entity_id);
        }
        Ok(actor_ref)
    }

    fn spawn_incarnation(
        &self,
        context: &LocalEntityContext,
    ) -> Result<ActorRef<M>, EntityDeliveryFailure> {
        let context_for_factory = context.clone();
        let factory = self.factory.clone();
        self.system
            .spawn_actor_with_options(
                context.actor_name(),
                move || factory(context_for_factory.clone()),
                self.actor_options.clone(),
            )
            .map_err(|error| EntityDeliveryFailure::SpawnFailed(error.to_string()))
    }

    /// Stops the incarnation in `entity_id`'s slot: queues `stop_message` (if any) and
    /// then the stop, in one critical section, and starts the stop watcher.
    fn passivate_slot(
        &self,
        actors: &mut SlotRegistry<M>,
        entity_id: &EntityId,
        stop_message: Option<M>,
    ) -> PassivationOutcome {
        let actor_ref = match actors.get(entity_id) {
            None => return PassivationOutcome::Absent,
            Some(LocalEntitySlot::Stopping { .. }) => return PassivationOutcome::AlreadyStopping,
            Some(LocalEntitySlot::Active { actor_ref, .. }) => {
                if actor_ref.is_terminated() {
                    actors.remove(entity_id);
                    return PassivationOutcome::Absent;
                }
                actor_ref.clone()
            }
        };

        if let Some(stop_message) = stop_message {
            match actor_ref.tell(stop_message) {
                Ok(()) | Err(TellError::Closed(_)) => {}
                Err(TellError::Full(_)) => return PassivationOutcome::Abandoned,
            }
        }
        match actor_ref.stop() {
            Ok(()) | Err(StopError::MailboxClosed) => {}
            Err(StopError::MailboxFull) => return PassivationOutcome::Abandoned,
        }

        let Some(LocalEntitySlot::Active { context, .. }) = actors.remove(entity_id) else {
            unreachable!("slot checked above")
        };
        actors.insert(
            entity_id.clone(),
            LocalEntitySlot::Stopping {
                context,
                actor_ref: actor_ref.clone(),
                held: VecDeque::new(),
                reactivate: false,
            },
        );
        self.spawn_stop_watcher(entity_id.clone(), actor_ref);
        PassivationOutcome::Stopping
    }

    fn spawn_stop_watcher(&self, entity_id: EntityId, actor_ref: ActorRef<M>) {
        let Some(handle) = Self::runtime_handle() else {
            return;
        };
        let route = self.clone();
        handle.spawn(async move {
            let uid = actor_ref.uid();
            let _terminated = actor_ref.when_terminated().await;
            route.on_incarnation_stopped(&entity_id, uid);
        });
    }

    /// Runs once the stopping incarnation `uid` has terminated: spawns the next
    /// incarnation when messages were held or a replay asked for it, otherwise
    /// removes the slot.
    fn on_incarnation_stopped(&self, entity_id: &EntityId, uid: ActorUid) {
        let activated = {
            let mut actors = self.lock_actors();
            let ours = matches!(
                actors.get(entity_id),
                Some(LocalEntitySlot::Stopping { actor_ref, .. }) if actor_ref.uid() == uid
            );
            if !ours {
                return;
            }
            let Some(LocalEntitySlot::Stopping {
                context,
                held,
                reactivate,
                ..
            }) = actors.remove(entity_id)
            else {
                unreachable!("slot checked above")
            };
            if held.is_empty() && !reactivate {
                None
            } else {
                self.activate_slot(&mut actors, context.clone(), held)
                    .ok()
                    .map(|_actor_ref| context)
            }
        };
        if let Some(context) = activated {
            self.observe_activation(context);
        }
    }

    fn activate_existing_or_spawn(
        &self,
        entity_type: &EntityType,
        entity_id: &EntityId,
        shard_id: ShardId,
    ) -> crate::ShardingResult<bool> {
        let replay_error =
            |failure: EntityDeliveryFailure| crate::ShardingError::RememberedEntityReplay {
                entity_type: entity_type.clone(),
                entity_id: entity_id.clone(),
                shard_id,
                message: failure.to_string(),
            };
        let context = LocalEntityContext::new(
            self.local_node_id.clone(),
            entity_type.clone(),
            entity_id.clone(),
            shard_id,
        );
        let observed = {
            let mut actors = self.lock_actors();
            self.ensure_shard_accepts_delivery(shard_id)
                .map_err(replay_error)?;
            match actors.get_mut(entity_id) {
                Some(LocalEntitySlot::Active { actor_ref, .. }) if !actor_ref.is_terminated() => {
                    Some(context)
                }
                Some(LocalEntitySlot::Stopping {
                    actor_ref,
                    reactivate,
                    ..
                }) if !actor_ref.is_terminated() => {
                    *reactivate = true;
                    None
                }
                Some(LocalEntitySlot::Stopping { .. }) => {
                    let Some(LocalEntitySlot::Stopping { held, .. }) = actors.remove(entity_id)
                    else {
                        unreachable!("slot checked above")
                    };
                    self.activate_slot(&mut actors, context.clone(), held)
                        .map_err(replay_error)?;
                    Some(context)
                }
                _ => {
                    actors.remove(entity_id);
                    self.activate_slot(&mut actors, context.clone(), VecDeque::new())
                        .map_err(replay_error)?;
                    Some(context)
                }
            }
        };
        if let Some(context) = observed {
            self.observe_activation(context);
        }
        Ok(true)
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

    /// Records a delivery to an active slot: moves its idle deadline and, when no
    /// timer task is running for this incarnation, starts one.
    fn note_delivery(&self, actors: &mut SlotRegistry<M>, entity_id: &EntityId) {
        let Some(timeout) = self.idle_passivation_timeout else {
            return;
        };
        let Some(LocalEntitySlot::Active {
            actor_ref,
            idle_deadline,
            timer_armed,
            ..
        }) = actors.get_mut(entity_id)
        else {
            return;
        };
        let deadline = Instant::now() + timeout;
        *idle_deadline = Some(deadline);
        if !*timer_armed {
            let Some(handle) = Self::runtime_handle() else {
                return;
            };
            *timer_armed = true;
            self.armed_idle_timers.fetch_add(1, Ordering::AcqRel);
            let route = self.clone();
            let entity_id = entity_id.clone();
            let uid = actor_ref.uid();
            handle.spawn(async move {
                route.run_idle_timer(entity_id, uid, deadline).await;
            });
        }
    }

    /// One timer task per active incarnation: sleeps to the deadline, re-reads it under
    /// the lock, and passivates the incarnation once the deadline has passed. Exits when
    /// the incarnation is gone, stopping, or replaced by another uid.
    async fn run_idle_timer(self, entity_id: EntityId, uid: ActorUid, mut deadline: Instant) {
        let Some(timeout) = self.idle_passivation_timeout else {
            self.armed_idle_timers.fetch_sub(1, Ordering::AcqRel);
            return;
        };
        loop {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            let next = {
                let mut actors = self.lock_actors();
                let now = Instant::now();
                let mine = match actors.get(&entity_id) {
                    Some(LocalEntitySlot::Active {
                        actor_ref,
                        idle_deadline,
                        ..
                    }) if actor_ref.uid() == uid => Some(*idle_deadline),
                    _ => None,
                };
                match mine {
                    None => None,
                    Some(None) => {
                        if let Some(LocalEntitySlot::Active { timer_armed, .. }) =
                            actors.get_mut(&entity_id)
                        {
                            *timer_armed = false;
                        }
                        None
                    }
                    Some(Some(due)) if due > now => Some(due),
                    Some(Some(_due)) => {
                        if self.passivate_slot(&mut actors, &entity_id, None)
                            == PassivationOutcome::Abandoned
                        {
                            let pushed = now + timeout;
                            if let Some(LocalEntitySlot::Active { idle_deadline, .. }) =
                                actors.get_mut(&entity_id)
                            {
                                *idle_deadline = Some(pushed);
                            }
                            Some(pushed)
                        } else {
                            None
                        }
                    }
                }
                .or_else(|| {
                    // Exit is decided under the lock so a reader that sees the slot
                    // stopping or gone also sees the timer as disarmed.
                    self.armed_idle_timers.fetch_sub(1, Ordering::AcqRel);
                    None
                })
            };
            match next {
                Some(due) => deadline = due,
                None => break,
            }
        }
    }

    fn observe_activation(&self, context: LocalEntityContext) {
        if let Some(observer) = &self.activation_observer {
            observer(context);
        }
    }
}

fn remove_if_terminated_active<M>(actors: &mut SlotRegistry<M>, entity_id: &EntityId)
where
    M: Message,
{
    if actors
        .get(entity_id)
        .is_some_and(LocalEntitySlot::is_terminated_active)
    {
        actors.remove(entity_id);
    }
}

fn warn_dropped(context: &LocalEntityContext, dropped: usize, reason: &str) {
    tracing::warn!(
        entity_type = %context.entity_type(),
        entity_id = %context.entity_id(),
        shard_id = context.shard_id().as_u32(),
        dropped,
        reason,
        "held entity messages dropped"
    );
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
            armed_idle_timers: self.armed_idle_timers.clone(),
            idle_passivation_timeout: self.idle_passivation_timeout,
            activation_observer: self.activation_observer.clone(),
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

        let observed = {
            let mut actors = self.lock_actors();
            // Checked under the registry lock so a delivery cannot slip past a shard
            // transfer that already stopped this shard's entities.
            if let Err(failure) = self.ensure_shard_accepts_delivery(message.shard_id()) {
                return Err(EntityTellError::Delivery {
                    message: message.into_message(),
                    failure,
                });
            }
            self.deliver_locked(&mut actors, message)?
        };
        if let Some(context) = observed {
            self.observe_activation(context);
        }
        Ok(())
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

    fn activate_entity(
        &self,
        entity_type: &EntityType,
        entity_id: &EntityId,
        shard_id: ShardId,
    ) -> crate::ShardingResult<bool> {
        self.activate_existing_or_spawn(entity_type, entity_id, shard_id)
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
