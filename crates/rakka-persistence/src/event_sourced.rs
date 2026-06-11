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
    DurableState, EventJournal, EventRecord, PersistFailureBackoff, PersistenceEvent,
    PersistenceId, RecoveryOptions, RetentionCriteria, SequenceNr, SnapshotRecord,
    SnapshotSelection, SnapshotStore, TaggedEvent,
};

/// Runtime signal emitted by an event-sourced actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistenceSignal {
    /// Recovery is about to load snapshots and replay events.
    RecoveryStarted {
        /// Durable identity being recovered.
        persistence_id: PersistenceId,
    },
    /// Recovery completed.
    RecoveryCompleted {
        /// Durable identity that recovered.
        persistence_id: PersistenceId,
        /// Highest recovered sequence number.
        sequence_nr: SequenceNr,
    },
    /// Events were persisted.
    EventsPersisted {
        /// Durable identity that persisted events.
        persistence_id: PersistenceId,
        /// First persisted sequence number.
        from: SequenceNr,
        /// Last persisted sequence number.
        to: SequenceNr,
    },
    /// Event persistence failed.
    PersistFailed {
        /// Durable identity being written.
        persistence_id: PersistenceId,
        /// Attempt number, starting at zero.
        attempt: u32,
        /// Stable error code.
        error_code: &'static str,
        /// Human-readable error detail.
        message: String,
    },
    /// A snapshot was saved.
    SnapshotSaved {
        /// Durable identity that saved a snapshot.
        persistence_id: PersistenceId,
        /// Snapshot sequence number.
        sequence_nr: SequenceNr,
    },
    /// A snapshot save failed.
    SnapshotFailed {
        /// Durable identity being snapshotted.
        persistence_id: PersistenceId,
        /// Attempt number, starting at zero.
        attempt: u32,
        /// Stable error code.
        error_code: &'static str,
        /// Human-readable error detail.
        message: String,
    },
    /// The actor is about to recover after a restart.
    PreRestart {
        /// Durable identity being restarted.
        persistence_id: PersistenceId,
    },
    /// The actor stopped.
    PostStop {
        /// Durable identity that stopped.
        persistence_id: PersistenceId,
    },
}

/// Explicit stash directive carried by an event-sourced effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StashDirective {
    /// Do not modify the stash.
    None,
    /// Mark the current command for stashing.
    Stash,
    /// Request unstashing of previously stashed commands.
    UnstashAll,
}

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

    /// Returns the snapshot retention policy.
    fn retention_criteria(&self) -> RetentionCriteria {
        RetentionCriteria::disabled()
    }

    /// Returns the retry policy used for failed persistence writes.
    fn persist_failure_backoff(&self) -> PersistFailureBackoff {
        PersistFailureBackoff::disabled()
    }

    /// Handles event-sourced runtime signals.
    fn on_signal(&mut self, _signal: PersistenceSignal) -> DurableResult<()> {
        Ok(())
    }
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
    stash: StashDirective,
    unhandled: bool,
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
            stash: StashDirective::None,
            unhandled: false,
            side_effects: Vec::new(),
        }
    }

    /// Marks a command as unhandled without persisting an event.
    #[must_use]
    pub fn unhandled() -> Self {
        Self {
            unhandled: true,
            ..Self::none()
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
            stash: StashDirective::None,
            unhandled: false,
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
            stash: StashDirective::None,
            unhandled: false,
            side_effects: Vec::new(),
        }
    }

    /// Marks the current command for stashing.
    #[must_use]
    pub fn stash() -> Self {
        Self {
            stash: StashDirective::Stash,
            ..Self::none()
        }
    }

    /// Requests unstashing of previously stashed commands.
    #[must_use]
    pub fn unstash_all() -> Self {
        Self {
            stash: StashDirective::UnstashAll,
            ..Self::none()
        }
    }

    /// Stops the actor without persisting an event.
    #[must_use]
    pub fn stop() -> Self {
        Self {
            events: Vec::new(),
            snapshot: false,
            stop: true,
            stash: StashDirective::None,
            unhandled: false,
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

    /// Replies after selected events and snapshot commit.
    #[must_use]
    pub fn then_reply<R>(self, reply_to: ReplyTo<R>, reply: R) -> Self
    where
        R: Send + 'static,
    {
        self.then_run(move || {
            let _ = reply_to.reply(reply);
        })
    }

    /// Replies without persisting an event.
    #[must_use]
    pub fn reply<R>(reply_to: ReplyTo<R>, reply: R) -> Self
    where
        R: Send + 'static,
    {
        Self::none().then_reply(reply_to, reply)
    }

    /// Persists no events and sends no reply.
    #[must_use]
    pub fn no_reply() -> Self {
        Self::none()
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

    /// Returns the stash directive selected by this effect.
    #[must_use]
    pub const fn stash_directive(&self) -> StashDirective {
        self.stash
    }

    /// Returns true when this effect marks a command as unhandled.
    #[must_use]
    pub const fn is_unhandled(&self) -> bool {
        self.unhandled
    }

    fn into_parts(self) -> EventSourcedEffectParts<E> {
        (
            self.events,
            self.snapshot,
            self.stop,
            self.stash,
            self.unhandled,
            self.side_effects,
        )
    }

    /// Splits this effect into parts for behavior testkits.
    #[must_use]
    pub fn into_test_parts(self) -> EventSourcedEffectParts<E> {
        self.into_parts()
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
            .field("stash", &self.stash)
            .field("unhandled", &self.unhandled)
            .field("side_effect_count", &self.side_effects.len())
            .finish()
    }
}

/// Parts returned by [`EventSourcedEffect::into_test_parts`].
pub type EventSourcedEffectParts<E> = (
    Vec<TaggedEvent<E>>,
    bool,
    bool,
    StashDirective,
    bool,
    Vec<DurableSideEffect>,
);

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
        self.actor
            .on_signal(PersistenceSignal::RecoveryStarted {
                persistence_id: self.persistence_id.clone(),
            })
            .map_err(|error| error.into_rakka_error())?;
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
        self.actor
            .on_signal(PersistenceSignal::RecoveryCompleted {
                persistence_id: self.persistence_id.clone(),
                sequence_nr,
            })
            .map_err(|error| error.into_rakka_error())?;
        Ok(())
    }

    async fn apply_effect(
        &mut self,
        effect: EventSourcedEffect<A::Event>,
    ) -> RakkaResult<ActorAction> {
        let (events, snapshot, stop, _stash, _unhandled, side_effects) = effect.into_parts();
        let recovered = self.recovered.as_ref().ok_or_else(|| {
            crate::DurableError::NotRecovered {
                persistence_id: self.persistence_id.clone(),
            }
            .into_rakka_error()
        })?;
        let mut state = recovered.state.clone();
        let mut sequence_nr = recovered.sequence_nr;
        let mut persisted_events = false;

        if !events.is_empty() {
            let persisted = self.append_with_backoff(sequence_nr, events).await?;
            let first_sequence_nr = persisted
                .first()
                .map_or(sequence_nr.next(), |record| record.metadata.sequence_nr);
            for record in persisted {
                sequence_nr = record.metadata.sequence_nr;
                state = self.actor.apply_event(&state, &record.event);
            }
            persisted_events = true;
            self.actor
                .on_signal(PersistenceSignal::EventsPersisted {
                    persistence_id: self.persistence_id.clone(),
                    from: first_sequence_nr,
                    to: sequence_nr,
                })
                .map_err(|error| error.into_rakka_error())?;
        }

        let retention = self.actor.retention_criteria();
        if snapshot || (persisted_events && retention.should_snapshot(sequence_nr)) {
            self.save_snapshot_with_backoff(sequence_nr, state.clone())
                .await?;
            self.apply_retention(sequence_nr).await?;
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

    async fn append_with_backoff(
        &mut self,
        expected_sequence_nr: SequenceNr,
        events: Vec<TaggedEvent<A::Event>>,
    ) -> RakkaResult<Vec<EventRecord<A::Event>>> {
        let backoff = self.actor.persist_failure_backoff();
        let mut attempt = 0;

        loop {
            match self
                .journal
                .append(&self.persistence_id, expected_sequence_nr, events.clone())
                .await
            {
                Ok(records) => return Ok(records),
                Err(error) => {
                    self.actor
                        .on_signal(PersistenceSignal::PersistFailed {
                            persistence_id: self.persistence_id.clone(),
                            attempt,
                            error_code: error.code(),
                            message: error.to_string(),
                        })
                        .map_err(|error| error.into_rakka_error())?;
                    if attempt >= backoff.max_retries() {
                        return Err(error.into_rakka_error());
                    }
                    attempt += 1;
                    tokio::time::sleep(backoff.retry_delay()).await;
                }
            }
        }
    }

    async fn save_snapshot_with_backoff(
        &mut self,
        sequence_nr: SequenceNr,
        state: A::State,
    ) -> RakkaResult<()> {
        let backoff = self.actor.persist_failure_backoff();
        let mut attempt = 0;

        loop {
            match self
                .snapshots
                .save(&self.persistence_id, sequence_nr, state.clone())
                .await
            {
                Ok(_record) => {
                    self.actor
                        .on_signal(PersistenceSignal::SnapshotSaved {
                            persistence_id: self.persistence_id.clone(),
                            sequence_nr,
                        })
                        .map_err(|error| error.into_rakka_error())?;
                    return Ok(());
                }
                Err(error) => {
                    self.actor
                        .on_signal(PersistenceSignal::SnapshotFailed {
                            persistence_id: self.persistence_id.clone(),
                            attempt,
                            error_code: error.code(),
                            message: error.to_string(),
                        })
                        .map_err(|error| error.into_rakka_error())?;
                    if attempt >= backoff.max_retries() {
                        return Err(error.into_rakka_error());
                    }
                    attempt += 1;
                    tokio::time::sleep(backoff.retry_delay()).await;
                }
            }
        }
    }

    async fn apply_retention(
        &mut self,
        latest_snapshot_sequence_nr: SequenceNr,
    ) -> RakkaResult<()> {
        let retention = self.actor.retention_criteria();
        let keep = retention.keep_snapshots_count();

        if keep != usize::MAX {
            let snapshots = self
                .snapshots
                .list(&self.persistence_id, SnapshotSelection::latest())
                .await
                .map_err(|error| error.into_rakka_error())?;
            for metadata in snapshots.iter().skip(keep) {
                self.snapshots
                    .delete(
                        &self.persistence_id,
                        SnapshotSelection::between(metadata.sequence_nr, metadata.sequence_nr),
                    )
                    .await
                    .map_err(|error| error.into_rakka_error())?;
            }
        }

        if retention.should_delete_events_on_snapshot() {
            let delete_to = if keep == 0 {
                latest_snapshot_sequence_nr
            } else {
                let retained = self
                    .snapshots
                    .list(&self.persistence_id, SnapshotSelection::latest())
                    .await
                    .map_err(|error| error.into_rakka_error())?;
                retained
                    .last()
                    .map_or(latest_snapshot_sequence_nr, |metadata| metadata.sequence_nr)
            };
            self.journal
                .delete_to(&self.persistence_id, delete_to)
                .await
                .map_err(|error| error.into_rakka_error())?;
        }

        Ok(())
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
            self.actor
                .on_signal(PersistenceSignal::PreRestart {
                    persistence_id: self.persistence_id.clone(),
                })
                .map_err(|error| error.into_rakka_error())?;
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

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a rakka_core::TerminationReason,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.actor
                .on_signal(PersistenceSignal::PostStop {
                    persistence_id: self.persistence_id.clone(),
                })
                .map_err(|error| error.into_rakka_error())?;
            Ok(ActorAction::Continue)
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
