//! Akka-style event-sourced behavior facade.

use std::marker::PhantomData;
use std::sync::Arc;

use rakka_core::{ActorOptions, ActorRef, ActorSystem, Message, RakkaResult};

use crate::error::{DurableError, DurableResult};
use crate::event_sourced::{
    event_sourced_actor_future, spawn_event_sourced_actor_with_options, EventSourcedActor,
    EventSourcedActorContext, EventSourcedActorFuture, EventSourcedEffect, PersistenceSignal,
};
use crate::store::{
    DurableState, EventJournal, PersistFailureBackoff, PersistenceEvent, PersistenceId,
    RecoveryOptions, RetentionCriteria, SnapshotStore,
};

type CommandHandler<C, E, S> =
    Arc<dyn Fn(&S, C) -> DurableResult<EventSourcedEffect<E>> + Send + Sync>;
type EventHandler<E, S> = Arc<dyn Fn(&S, &E) -> S + Send + Sync>;
type SignalHandler = Arc<dyn Fn(PersistenceSignal) -> DurableResult<()> + Send + Sync>;

/// Akka-named event-sourced behavior facade.
pub struct EventSourcedBehavior<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    persistence_id: PersistenceId,
    empty_state: S,
    command_handler: CommandHandler<C, E, S>,
    event_handler: EventHandler<E, S>,
    signal_handler: Option<SignalHandler>,
    recovery_options: RecoveryOptions,
    retention_criteria: RetentionCriteria,
    persist_failure_backoff: PersistFailureBackoff,
}

impl<C, E, S> Clone for EventSourcedBehavior<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            persistence_id: self.persistence_id.clone(),
            empty_state: self.empty_state.clone(),
            command_handler: self.command_handler.clone(),
            event_handler: self.event_handler.clone(),
            signal_handler: self.signal_handler.clone(),
            recovery_options: self.recovery_options,
            retention_criteria: self.retention_criteria,
            persist_failure_backoff: self.persist_failure_backoff,
        }
    }
}

impl<C, E, S> EventSourcedBehavior<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    /// Creates a behavior builder.
    #[must_use]
    pub fn builder(
        persistence_id: PersistenceId,
        empty_state: S,
    ) -> EventSourcedBehaviorBuilder<C, E, S> {
        EventSourcedBehaviorBuilder::new(persistence_id, empty_state)
    }

    /// Creates an event-sourced behavior from command and event handlers.
    #[must_use]
    pub fn new<CommandFn, EventFn>(
        persistence_id: PersistenceId,
        empty_state: S,
        command_handler: CommandFn,
        event_handler: EventFn,
    ) -> Self
    where
        CommandFn: Fn(&S, C) -> EventSourcedEffect<E> + Send + Sync + 'static,
        EventFn: Fn(&S, &E) -> S + Send + Sync + 'static,
    {
        Self::try_new(
            persistence_id,
            empty_state,
            move |state, command| Ok(command_handler(state, command)),
            event_handler,
        )
    }

    /// Creates an event-sourced behavior from a fallible command handler.
    #[must_use]
    pub fn try_new<CommandFn, EventFn>(
        persistence_id: PersistenceId,
        empty_state: S,
        command_handler: CommandFn,
        event_handler: EventFn,
    ) -> Self
    where
        CommandFn: Fn(&S, C) -> DurableResult<EventSourcedEffect<E>> + Send + Sync + 'static,
        EventFn: Fn(&S, &E) -> S + Send + Sync + 'static,
    {
        Self {
            persistence_id,
            empty_state,
            command_handler: Arc::new(command_handler),
            event_handler: Arc::new(event_handler),
            signal_handler: None,
            recovery_options: RecoveryOptions::default(),
            retention_criteria: RetentionCriteria::disabled(),
            persist_failure_backoff: PersistFailureBackoff::disabled(),
        }
    }

    /// Returns a copy with recovery options.
    #[must_use]
    pub fn with_recovery_options(mut self, recovery_options: RecoveryOptions) -> Self {
        self.recovery_options = recovery_options;
        self
    }

    /// Returns a copy with retention criteria.
    #[must_use]
    pub fn with_retention_criteria(mut self, retention_criteria: RetentionCriteria) -> Self {
        self.retention_criteria = retention_criteria;
        self
    }

    /// Returns a copy with persistence write backoff.
    #[must_use]
    pub fn with_persist_failure_backoff(
        mut self,
        persist_failure_backoff: PersistFailureBackoff,
    ) -> Self {
        self.persist_failure_backoff = persist_failure_backoff;
        self
    }

    /// Returns a copy with a signal handler.
    #[must_use]
    pub fn on_signal(
        mut self,
        handler: impl Fn(PersistenceSignal) -> DurableResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.signal_handler = Some(Arc::new(handler));
        self
    }

    /// Returns the persistence id.
    #[must_use]
    pub fn persistence_id_ref(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Returns a clone of the empty state used for recovery.
    #[must_use]
    pub fn initial_state(&self) -> S {
        self.empty_state.clone()
    }

    /// Evaluates one command for behavior testkits.
    pub fn evaluate_command(&self, state: &S, command: C) -> DurableResult<EventSourcedEffect<E>> {
        (self.command_handler)(state, command)
    }

    /// Applies one event for behavior testkits.
    #[must_use]
    pub fn evaluate_event(&self, state: &S, event: &E) -> S {
        (self.event_handler)(state, event)
    }

    /// Spawns this behavior with default local actor options.
    pub fn spawn<Journal, Snapshots>(
        self,
        system: &ActorSystem,
        name: impl AsRef<str>,
        journal: Journal,
        snapshots: Snapshots,
    ) -> RakkaResult<ActorRef<C>>
    where
        Journal: EventJournal<E>,
        Snapshots: SnapshotStore<S>,
    {
        self.spawn_with_options(system, name, journal, snapshots, ActorOptions::default())
    }

    /// Spawns this behavior with explicit local actor options.
    pub fn spawn_with_options<Journal, Snapshots>(
        self,
        system: &ActorSystem,
        name: impl AsRef<str>,
        journal: Journal,
        snapshots: Snapshots,
        actor_options: ActorOptions,
    ) -> RakkaResult<ActorRef<C>>
    where
        Journal: EventJournal<E>,
        Snapshots: SnapshotStore<S>,
    {
        let recovery_options = self.recovery_options;
        spawn_event_sourced_actor_with_options(
            system,
            name,
            self,
            journal,
            snapshots,
            actor_options,
            recovery_options,
        )
    }
}

impl<C, E, S> EventSourcedActor for EventSourcedBehavior<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    type Command = C;
    type Event = E;
    type State = S;

    fn persistence_id(&self) -> PersistenceId {
        self.persistence_id.clone()
    }

    fn empty_state(&self) -> Self::State {
        self.empty_state.clone()
    }

    fn handle_command<'a>(
        &'a mut self,
        _ctx: &'a mut EventSourcedActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> EventSourcedActorFuture<'a, Self::Event> {
        let result = (self.command_handler)(state, command);
        event_sourced_actor_future(async move { result })
    }

    fn apply_event(&mut self, state: &Self::State, event: &Self::Event) -> Self::State {
        (self.event_handler)(state, event)
    }

    fn retention_criteria(&self) -> RetentionCriteria {
        self.retention_criteria
    }

    fn persist_failure_backoff(&self) -> PersistFailureBackoff {
        self.persist_failure_backoff
    }

    fn on_signal(&mut self, signal: PersistenceSignal) -> DurableResult<()> {
        if let Some(handler) = &self.signal_handler {
            handler(signal)?;
        }
        Ok(())
    }
}

/// Builder for [`EventSourcedBehavior`].
pub struct EventSourcedBehaviorBuilder<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    persistence_id: PersistenceId,
    empty_state: S,
    command_handler: Option<CommandHandler<C, E, S>>,
    event_handler: Option<EventHandler<E, S>>,
    recovery_options: RecoveryOptions,
    retention_criteria: RetentionCriteria,
    persist_failure_backoff: PersistFailureBackoff,
    signal_handler: Option<SignalHandler>,
    _command: PhantomData<fn(C)>,
    _event: PhantomData<fn(E)>,
}

impl<C, E, S> EventSourcedBehaviorBuilder<C, E, S>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
{
    /// Creates a builder.
    #[must_use]
    pub fn new(persistence_id: PersistenceId, empty_state: S) -> Self {
        Self {
            persistence_id,
            empty_state,
            command_handler: None,
            event_handler: None,
            recovery_options: RecoveryOptions::default(),
            retention_criteria: RetentionCriteria::disabled(),
            persist_failure_backoff: PersistFailureBackoff::disabled(),
            signal_handler: None,
            _command: PhantomData,
            _event: PhantomData,
        }
    }

    /// Sets the command handler.
    #[must_use]
    pub fn on_command(
        mut self,
        handler: impl Fn(&S, C) -> EventSourcedEffect<E> + Send + Sync + 'static,
    ) -> Self {
        self.command_handler = Some(Arc::new(move |state, command| Ok(handler(state, command))));
        self
    }

    /// Sets a fallible command handler.
    #[must_use]
    pub fn try_on_command(
        mut self,
        handler: impl Fn(&S, C) -> DurableResult<EventSourcedEffect<E>> + Send + Sync + 'static,
    ) -> Self {
        self.command_handler = Some(Arc::new(handler));
        self
    }

    /// Sets the event handler.
    #[must_use]
    pub fn on_event(mut self, handler: impl Fn(&S, &E) -> S + Send + Sync + 'static) -> Self {
        self.event_handler = Some(Arc::new(handler));
        self
    }

    /// Sets recovery options.
    #[must_use]
    pub fn with_recovery_options(mut self, recovery_options: RecoveryOptions) -> Self {
        self.recovery_options = recovery_options;
        self
    }

    /// Sets retention criteria.
    #[must_use]
    pub fn with_retention_criteria(mut self, retention_criteria: RetentionCriteria) -> Self {
        self.retention_criteria = retention_criteria;
        self
    }

    /// Sets persistence write backoff.
    #[must_use]
    pub fn with_persist_failure_backoff(
        mut self,
        persist_failure_backoff: PersistFailureBackoff,
    ) -> Self {
        self.persist_failure_backoff = persist_failure_backoff;
        self
    }

    /// Sets a signal handler.
    #[must_use]
    pub fn on_signal(
        mut self,
        handler: impl Fn(PersistenceSignal) -> DurableResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.signal_handler = Some(Arc::new(handler));
        self
    }

    /// Builds the behavior.
    pub fn build(self) -> DurableResult<EventSourcedBehavior<C, E, S>> {
        Ok(EventSourcedBehavior {
            persistence_id: self.persistence_id,
            empty_state: self.empty_state,
            command_handler: self.command_handler.ok_or_else(|| {
                DurableError::store("behavior", "event-sourced command handler is missing")
            })?,
            event_handler: self.event_handler.ok_or_else(|| {
                DurableError::store("behavior", "event-sourced event handler is missing")
            })?,
            signal_handler: self.signal_handler,
            recovery_options: self.recovery_options,
            retention_criteria: self.retention_criteria,
            persist_failure_backoff: self.persist_failure_backoff,
        })
    }
}

/// Spawns an event-sourced behavior with default local actor options.
pub fn spawn_event_sourced_behavior<C, E, S, Journal, Snapshots>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    behavior: EventSourcedBehavior<C, E, S>,
    journal: Journal,
    snapshots: Snapshots,
) -> RakkaResult<ActorRef<C>>
where
    C: Message,
    E: PersistenceEvent,
    S: DurableState,
    Journal: EventJournal<E>,
    Snapshots: SnapshotStore<S>,
{
    behavior.spawn(system, name, journal, snapshots)
}
