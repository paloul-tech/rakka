//! Event-sourced actor adapter for the local actor runtime.

use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorRef,
    ActorSystem, Message, RakkaResult, ReplyTo, StopError, TimerHandle,
};

use crate::effect::DurableSideEffect;
use crate::error::DurableResult;
use crate::store::{
    DurableState, EventJournal, PersistenceEvent, PersistenceId, RecoveryOptions, SequenceNr,
    SnapshotRecord, SnapshotStore, TaggedEvent,
};

/// Boxed future returned by event-sourced actor command handlers.
pub type EventSourcedActorFuture<'a, E> =
    Pin<Box<dyn Future<Output = DurableResult<EventSourcedEffect<E>>> + Send + 'a>>;

/// Wraps an async block as an event-sourced actor future.
pub fn event_sourced_actor_future<'a, E>(
    future: impl Future<Output = DurableResult<EventSourcedEffect<E>>> + Send + 'a,
) -> EventSourcedActorFuture<'a, E>
where
    E: PersistenceEvent,
{
    Box::pin(future)
}

/// Event-sourced actor behavior.
pub trait EventSourcedActor: Send + 'static {
    /// Typed command protocol accepted by this actor.
    type Command: Message;
    /// Persisted event type.
    type Event: PersistenceEvent;
    /// Recovered state type.
    type State: DurableState;

    /// Stable durable identity for this actor instance.
    fn persistence_id(&self) -> PersistenceId;

    /// Empty state used when no events or snapshots exist.
    fn empty_state(&self) -> Self::State;

    /// Handles one command against the current recovered state.
    fn handle_command<'a>(
        &'a mut self,
        ctx: &'a mut EventSourcedActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> EventSourcedActorFuture<'a, Self::Event>;

    /// Applies one persisted event to a state value.
    fn apply_event(&mut self, state: &Self::State, event: &Self::Event) -> Self::State;
}

/// Event-sourced actor context.
pub struct EventSourcedActorContext<'a, M>
where
    M: Message,
{
    actor_context: &'a mut ActorContext<M>,
    persistence_id: PersistenceId,
    sequence_nr: SequenceNr,
}

impl<'a, M> EventSourcedActorContext<'a, M>
where
    M: Message,
{
    fn new(
        actor_context: &'a mut ActorContext<M>,
        persistence_id: PersistenceId,
        sequence_nr: SequenceNr,
    ) -> Self {
        Self {
            actor_context,
            persistence_id,
            sequence_nr,
        }
    }

    /// Returns the durable persistence id.
    #[must_use]
    pub fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Returns the highest recovered event sequence number.
    #[must_use]
    pub const fn sequence_nr(&self) -> SequenceNr {
        self.sequence_nr
    }

    /// Returns this actor's local actor ref.
    #[must_use]
    pub fn myself(&self) -> &ActorRef<M> {
        self.actor_context.myself()
    }

    /// Schedules a message to this actor once.
    pub fn schedule_once(&self, delay: std::time::Duration, msg: M) -> TimerHandle<M> {
        self.actor_context.schedule_once(delay, msg)
    }

    /// Stops a child actor.
    pub fn stop_child<T>(&self, child: &ActorRef<T>) -> Result<(), StopError>
    where
        T: Message,
    {
        self.actor_context.stop_child(child)
    }

    /// Returns the underlying local actor context for advanced local-runtime operations.
    pub fn actor_context(&mut self) -> &mut ActorContext<M> {
        self.actor_context
    }

    /// Creates a side effect that replies after the event-sourced effect commits.
    pub fn reply_after_persist<R>(
        &self,
        reply_to: ReplyTo<R>,
        reply: R,
    ) -> impl FnOnce() + Send + 'static
    where
        R: Send + 'static,
    {
        move || {
            let _ = reply_to.reply(reply);
        }
    }
}

/// Command effect returned by an event-sourced actor.
pub struct EventSourcedEffect<E>
where
    E: PersistenceEvent,
{
    events: Vec<TaggedEvent<E>>,
    snapshot: bool,
    stop: bool,
    side_effects: Vec<DurableSideEffect>,
}

impl<E> EventSourcedEffect<E>
where
    E: PersistenceEvent,
{
    /// Persists no events.
    #[must_use]
    pub fn none() -> Self {
        Self {
            events: Vec::new(),
            snapshot: false,
            stop: false,
            side_effects: Vec::new(),
        }
    }

    /// Persists one untagged event.
    #[must_use]
    pub fn persist(event: E) -> Self {
        Self::persist_tagged(TaggedEvent::new(event))
    }

    /// Persists one tagged event.
    #[must_use]
    pub fn persist_tagged(event: TaggedEvent<E>) -> Self {
        Self {
            events: vec![event],
            snapshot: false,
            stop: false,
            side_effects: Vec::new(),
        }
    }

    /// Persists all events in order.
    #[must_use]
    pub fn persist_all<I, T>(events: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<TaggedEvent<E>>,
    {
        Self {
            events: events.into_iter().map(Into::into).collect(),
            snapshot: false,
            stop: false,
            side_effects: Vec::new(),
        }
    }

    /// Stops the actor without persisting an event.
    #[must_use]
    pub fn stop() -> Self {
        Self {
            events: Vec::new(),
            snapshot: false,
            stop: true,
            side_effects: Vec::new(),
        }
    }

    /// Saves a snapshot after persisting and applying this effect.
    #[must_use]
    pub fn then_snapshot(mut self) -> Self {
        self.snapshot = true;
        self
    }

    /// Stops the actor after this effect is applied.
    #[must_use]
    pub fn and_stop(mut self) -> Self {
        self.stop = true;
        self
    }

    /// Runs a side effect after selected events and snapshot commit.
    #[must_use]
    pub fn then_run(mut self, side_effect: impl FnOnce() + Send + 'static) -> Self {
        self.side_effects.push(Box::new(side_effect));
        self
    }

    /// Returns the selected events.
    #[must_use]
    pub fn events(&self) -> &[TaggedEvent<E>] {
        &self.events
    }

    /// Returns true if this effect should save a snapshot.
    #[must_use]
    pub const fn should_snapshot(&self) -> bool {
        self.snapshot
    }

    /// Returns true if the actor should stop after this effect.
    #[must_use]
    pub const fn should_stop(&self) -> bool {
        self.stop
    }

    fn into_parts(self) -> (Vec<TaggedEvent<E>>, bool, bool, Vec<DurableSideEffect>) {
        (self.events, self.snapshot, self.stop, self.side_effects)
    }
}

impl<E> Debug for EventSourcedEffect<E>
where
    E: PersistenceEvent + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSourcedEffect")
            .field("events", &self.events)
            .field("snapshot", &self.snapshot)
            .field("stop", &self.stop)
            .field("side_effect_count", &self.side_effects.len())
            .finish()
    }
}

/// Spawns an event-sourced actor with default local actor and recovery options.
pub fn spawn_event_sourced_actor<A, Journal, Snapshots>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    actor: A,
    journal: Journal,
    snapshots: Snapshots,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: EventSourcedActor,
    Journal: EventJournal<A::Event>,
    Snapshots: SnapshotStore<A::State>,
{
    spawn_event_sourced_actor_with_options(
        system,
        name,
        actor,
        journal,
        snapshots,
        ActorOptions::default(),
        RecoveryOptions::default(),
    )
}

/// Spawns an event-sourced actor with explicit local actor and recovery options.
pub fn spawn_event_sourced_actor_with_options<A, Journal, Snapshots>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    actor: A,
    journal: Journal,
    snapshots: Snapshots,
    actor_options: ActorOptions,
    recovery_options: RecoveryOptions,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: EventSourcedActor,
    Journal: EventJournal<A::Event>,
    Snapshots: SnapshotStore<A::State>,
{
    let actor = Mutex::new(Some(actor));
    let journal = Mutex::new(Some(journal));
    let snapshots = Mutex::new(Some(snapshots));
    system.spawn_actor_with_options(
        name,
        move || {
            let actor = actor
                .lock()
                .expect("event-sourced actor factory mutex poisoned")
                .take()
                .expect("single-use event-sourced actor factory cannot restart");
            let journal = journal
                .lock()
                .expect("event journal factory mutex poisoned")
                .take()
                .expect("single-use event journal factory cannot restart");
            let snapshots = snapshots
                .lock()
                .expect("snapshot store factory mutex poisoned")
                .take()
                .expect("single-use snapshot store factory cannot restart");
            EventSourcedActorRuntime::new(actor, journal, snapshots, recovery_options)
        },
        actor_options,
    )
}

/// Spawns a restartable event-sourced actor factory with default options.
pub fn spawn_event_sourced_actor_factory<A, Journal, Snapshots, Factory>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    factory: Factory,
    journal: Journal,
    snapshots: Snapshots,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: EventSourcedActor,
    Journal: EventJournal<A::Event>,
    Snapshots: SnapshotStore<A::State>,
    Factory: Fn() -> A + Send + Sync + 'static,
{
    spawn_event_sourced_actor_factory_with_options(
        system,
        name,
        factory,
        journal,
        snapshots,
        ActorOptions::default(),
        RecoveryOptions::default(),
    )
}

/// Spawns a restartable event-sourced actor factory with explicit options.
pub fn spawn_event_sourced_actor_factory_with_options<A, Journal, Snapshots, Factory>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    factory: Factory,
    journal: Journal,
    snapshots: Snapshots,
    actor_options: ActorOptions,
    recovery_options: RecoveryOptions,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: EventSourcedActor,
    Journal: EventJournal<A::Event>,
    Snapshots: SnapshotStore<A::State>,
    Factory: Fn() -> A + Send + Sync + 'static,
{
    system.spawn_actor_with_options(
        name,
        move || {
            EventSourcedActorRuntime::new(
                factory(),
                journal.clone(),
                snapshots.clone(),
                recovery_options,
            )
        },
        actor_options,
    )
}

struct EventSourcedActorRuntime<A, Journal, Snapshots>
where
    A: EventSourcedActor,
    Journal: EventJournal<A::Event>,
    Snapshots: SnapshotStore<A::State>,
{
    actor: A,
    journal: Journal,
    snapshots: Snapshots,
    persistence_id: PersistenceId,
    recovery_options: RecoveryOptions,
    recovered: Option<RecoveredState<A::State>>,
}

impl<A, Journal, Snapshots> EventSourcedActorRuntime<A, Journal, Snapshots>
where
    A: EventSourcedActor,
    Journal: EventJournal<A::Event>,
    Snapshots: SnapshotStore<A::State>,
{
    fn new(
        actor: A,
        journal: Journal,
        snapshots: Snapshots,
        recovery_options: RecoveryOptions,
    ) -> Self {
        let persistence_id = actor.persistence_id();
        Self {
            actor,
            journal,
            snapshots,
            persistence_id,
            recovery_options,
            recovered: None,
        }
    }

    async fn recover(&mut self) -> RakkaResult<()> {
        let snapshot = self
            .snapshots
            .load(
                &self.persistence_id,
                self.recovery_options.snapshot_selection,
            )
            .await
            .map_err(|error| error.into_rakka_error())?;
        let (mut state, mut sequence_nr) = match snapshot {
            Some(snapshot) => snapshot.into_parts(),
            None => (self.actor.empty_state(), SequenceNr::INITIAL),
        };

        let replay_from = replay_from_after_snapshot(sequence_nr, self.recovery_options);
        if replay_from <= self.recovery_options.replay_to {
            let replayed = self
                .journal
                .replay(
                    &self.persistence_id,
                    replay_from,
                    self.recovery_options.replay_to,
                )
                .await
                .map_err(|error| error.into_rakka_error())?;
            for record in replayed {
                sequence_nr = record.metadata.sequence_nr;
                state = self.actor.apply_event(&state, &record.event);
            }
        }

        self.recovered = Some(RecoveredState { state, sequence_nr });
        Ok(())
    }

    async fn apply_effect(
        &mut self,
        effect: EventSourcedEffect<A::Event>,
    ) -> RakkaResult<ActorAction> {
        let (events, snapshot, stop, side_effects) = effect.into_parts();
        let recovered = self.recovered.as_ref().ok_or_else(|| {
            crate::DurableError::NotRecovered {
                persistence_id: self.persistence_id.clone(),
            }
            .into_rakka_error()
        })?;
        let mut state = recovered.state.clone();
        let mut sequence_nr = recovered.sequence_nr;

        if !events.is_empty() {
            let persisted = self
                .journal
                .append(&self.persistence_id, sequence_nr, events)
                .await
                .map_err(|error| error.into_rakka_error())?;
            for record in persisted {
                sequence_nr = record.metadata.sequence_nr;
                state = self.actor.apply_event(&state, &record.event);
            }
        }

        if snapshot {
            self.snapshots
                .save(&self.persistence_id, sequence_nr, state.clone())
                .await
                .map_err(|error| error.into_rakka_error())?;
        }

        self.recovered = Some(RecoveredState { state, sequence_nr });

        for side_effect in side_effects {
            side_effect();
        }

        if stop {
            Ok(ActorAction::Stop)
        } else {
            Ok(ActorAction::Continue)
        }
    }
}

impl<A, Journal, Snapshots> Actor for EventSourcedActorRuntime<A, Journal, Snapshots>
where
    A: EventSourcedActor,
    Journal: EventJournal<A::Event>,
    Snapshots: SnapshotStore<A::State>,
{
    type Msg = A::Command;

    fn started<'a>(&'a mut self, _ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        actor_future(async move {
            self.recover().await?;
            Ok(ActorAction::Continue)
        })
    }

    fn restarted<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _failure: &'a rakka_core::ActorFailure,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.recover().await?;
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            let recovered = self.recovered.as_ref().ok_or_else(|| {
                crate::DurableError::NotRecovered {
                    persistence_id: self.persistence_id.clone(),
                }
                .into_rakka_error()
            })?;
            let state = recovered.state.clone();
            let sequence_nr = recovered.sequence_nr;
            let persistence_id = self.persistence_id.clone();
            let mut event_ctx = EventSourcedActorContext::new(ctx, persistence_id, sequence_nr);
            let effect = self
                .actor
                .handle_command(&mut event_ctx, &state, msg)
                .await
                .map_err(|error| error.into_rakka_error())?;

            self.apply_effect(effect).await
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveredState<S>
where
    S: DurableState,
{
    state: S,
    sequence_nr: SequenceNr,
}

trait SnapshotRecordExt<S>
where
    S: DurableState,
{
    fn into_parts(self) -> (S, SequenceNr);
}

impl<S> SnapshotRecordExt<S> for SnapshotRecord<S>
where
    S: DurableState,
{
    fn into_parts(self) -> (S, SequenceNr) {
        (self.snapshot, self.metadata.sequence_nr)
    }
}

fn replay_from_after_snapshot(
    snapshot_sequence_nr: SequenceNr,
    options: RecoveryOptions,
) -> SequenceNr {
    let after_snapshot = snapshot_sequence_nr.next();
    if after_snapshot > options.replay_from {
        after_snapshot
    } else {
        options.replay_from
    }
}
