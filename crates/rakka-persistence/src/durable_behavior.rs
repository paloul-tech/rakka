//! Akka-style durable-state behavior facade.

use std::marker::PhantomData;
use std::sync::Arc;

use rakka_core::{ActorOptions, ActorRef, ActorSystem, Message, RakkaResult};

use crate::actor::{
    durable_actor_future, spawn_durable_actor_with_options, DurableActor, DurableActorContext,
    DurableActorFuture, DurableStateSignal,
};
use crate::effect::DurableEffect;
use crate::error::{DurableError, DurableResult};
use crate::store::{DurableState, DurableStateStore, PersistFailureBackoff, PersistenceId};

type DurableCommandHandler<C, S> =
    Arc<dyn Fn(&S, C) -> DurableResult<DurableEffect<S>> + Send + Sync>;
type DurableSignalHandler = Arc<dyn Fn(DurableStateSignal) -> DurableResult<()> + Send + Sync>;

/// Akka-named durable-state behavior facade.
pub struct DurableStateBehavior<C, S>
where
    C: Message,
    S: DurableState,
{
    persistence_id: PersistenceId,
    empty_state: S,
    command_handler: DurableCommandHandler<C, S>,
    signal_handler: Option<DurableSignalHandler>,
    persist_failure_backoff: PersistFailureBackoff,
}

impl<C, S> Clone for DurableStateBehavior<C, S>
where
    C: Message,
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            persistence_id: self.persistence_id.clone(),
            empty_state: self.empty_state.clone(),
            command_handler: self.command_handler.clone(),
            signal_handler: self.signal_handler.clone(),
            persist_failure_backoff: self.persist_failure_backoff,
        }
    }
}

impl<C, S> DurableStateBehavior<C, S>
where
    C: Message,
    S: DurableState,
{
    /// Creates a behavior builder.
    #[must_use]
    pub fn builder(
        persistence_id: PersistenceId,
        empty_state: S,
    ) -> DurableStateBehaviorBuilder<C, S> {
        DurableStateBehaviorBuilder::new(persistence_id, empty_state)
    }

    /// Creates a durable-state behavior from a command handler.
    #[must_use]
    pub fn new<CommandFn>(
        persistence_id: PersistenceId,
        empty_state: S,
        command_handler: CommandFn,
    ) -> Self
    where
        CommandFn: Fn(&S, C) -> DurableEffect<S> + Send + Sync + 'static,
    {
        Self::try_new(persistence_id, empty_state, move |state, command| {
            Ok(command_handler(state, command))
        })
    }

    /// Creates a durable-state behavior from a fallible command handler.
    #[must_use]
    pub fn try_new<CommandFn>(
        persistence_id: PersistenceId,
        empty_state: S,
        command_handler: CommandFn,
    ) -> Self
    where
        CommandFn: Fn(&S, C) -> DurableResult<DurableEffect<S>> + Send + Sync + 'static,
    {
        Self {
            persistence_id,
            empty_state,
            command_handler: Arc::new(command_handler),
            signal_handler: None,
            persist_failure_backoff: PersistFailureBackoff::disabled(),
        }
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
        handler: impl Fn(DurableStateSignal) -> DurableResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.signal_handler = Some(Arc::new(handler));
        self
    }

    /// Evaluates one command for behavior testkits.
    pub fn evaluate_command(&self, state: &S, command: C) -> DurableResult<DurableEffect<S>> {
        (self.command_handler)(state, command)
    }

    /// Returns a clone of the empty state used for recovery.
    #[must_use]
    pub fn initial_state(&self) -> S {
        self.empty_state.clone()
    }

    /// Spawns this behavior with default local actor options.
    pub fn spawn<Store>(
        self,
        system: &ActorSystem,
        name: impl AsRef<str>,
        store: Store,
    ) -> RakkaResult<ActorRef<C>>
    where
        Store: DurableStateStore<S>,
    {
        self.spawn_with_options(system, name, store, ActorOptions::default())
    }

    /// Spawns this behavior with explicit local actor options.
    pub fn spawn_with_options<Store>(
        self,
        system: &ActorSystem,
        name: impl AsRef<str>,
        store: Store,
        actor_options: ActorOptions,
    ) -> RakkaResult<ActorRef<C>>
    where
        Store: DurableStateStore<S>,
    {
        spawn_durable_actor_with_options(system, name, self, store, actor_options)
    }
}

impl<C, S> DurableActor for DurableStateBehavior<C, S>
where
    C: Message,
    S: DurableState,
{
    type Command = C;
    type State = S;

    fn persistence_id(&self) -> PersistenceId {
        self.persistence_id.clone()
    }

    fn empty_state(&self) -> Self::State {
        self.empty_state.clone()
    }

    fn handle_command<'a>(
        &'a mut self,
        _ctx: &'a mut DurableActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> DurableActorFuture<'a, Self::State> {
        let result = (self.command_handler)(state, command);
        durable_actor_future(async move { result })
    }

    fn persist_failure_backoff(&self) -> PersistFailureBackoff {
        self.persist_failure_backoff
    }

    fn on_signal(&mut self, signal: DurableStateSignal) -> DurableResult<()> {
        if let Some(handler) = &self.signal_handler {
            handler(signal)?;
        }
        Ok(())
    }
}

/// Builder for [`DurableStateBehavior`].
pub struct DurableStateBehaviorBuilder<C, S>
where
    C: Message,
    S: DurableState,
{
    persistence_id: PersistenceId,
    empty_state: S,
    command_handler: Option<DurableCommandHandler<C, S>>,
    signal_handler: Option<DurableSignalHandler>,
    persist_failure_backoff: PersistFailureBackoff,
    _command: PhantomData<fn(C)>,
}

impl<C, S> DurableStateBehaviorBuilder<C, S>
where
    C: Message,
    S: DurableState,
{
    /// Creates a builder.
    #[must_use]
    pub fn new(persistence_id: PersistenceId, empty_state: S) -> Self {
        Self {
            persistence_id,
            empty_state,
            command_handler: None,
            signal_handler: None,
            persist_failure_backoff: PersistFailureBackoff::disabled(),
            _command: PhantomData,
        }
    }

    /// Sets the command handler.
    #[must_use]
    pub fn on_command(
        mut self,
        handler: impl Fn(&S, C) -> DurableEffect<S> + Send + Sync + 'static,
    ) -> Self {
        self.command_handler = Some(Arc::new(move |state, command| Ok(handler(state, command))));
        self
    }

    /// Sets a fallible command handler.
    #[must_use]
    pub fn try_on_command(
        mut self,
        handler: impl Fn(&S, C) -> DurableResult<DurableEffect<S>> + Send + Sync + 'static,
    ) -> Self {
        self.command_handler = Some(Arc::new(handler));
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
        handler: impl Fn(DurableStateSignal) -> DurableResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.signal_handler = Some(Arc::new(handler));
        self
    }

    /// Builds the behavior.
    pub fn build(self) -> DurableResult<DurableStateBehavior<C, S>> {
        Ok(DurableStateBehavior {
            persistence_id: self.persistence_id,
            empty_state: self.empty_state,
            command_handler: self.command_handler.ok_or_else(|| {
                DurableError::store("behavior", "durable-state command handler is missing")
            })?,
            signal_handler: self.signal_handler,
            persist_failure_backoff: self.persist_failure_backoff,
        })
    }
}

/// Spawns a durable-state behavior with default local actor options.
pub fn spawn_durable_state_behavior<C, S, Store>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    behavior: DurableStateBehavior<C, S>,
    store: Store,
) -> RakkaResult<ActorRef<C>>
where
    C: Message,
    S: DurableState,
    Store: DurableStateStore<S>,
{
    behavior.spawn(system, name, store)
}
