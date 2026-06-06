//! Durable actor adapter for the local actor runtime.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorOptions, ActorRef,
    ActorSystem, Message, RakkaResult, ReplyTo, StopError, TimerHandle,
};

use crate::effect::{DurableEffect, DurableStateChange};
use crate::error::DurableResult;
use crate::store::{DurableState, DurableStateStore, PersistenceId, Revision, StateRecord};

/// Boxed future returned by durable actor command handlers.
pub type DurableActorFuture<'a, S> =
    Pin<Box<dyn Future<Output = DurableResult<DurableEffect<S>>> + Send + 'a>>;

/// Wraps an async block as a durable actor future.
pub fn durable_actor_future<'a, S>(
    future: impl Future<Output = DurableResult<DurableEffect<S>>> + Send + 'a,
) -> DurableActorFuture<'a, S>
where
    S: DurableState,
{
    Box::pin(future)
}

/// Durable actor behavior.
pub trait DurableActor: Send + 'static {
    /// Typed command protocol accepted by this actor.
    type Command: Message;
    /// Durable state type.
    type State: DurableState;

    /// Stable durable identity for this actor instance.
    fn persistence_id(&self) -> PersistenceId;

    /// Empty state used when no durable record exists.
    fn empty_state(&self) -> Self::State;

    /// Handles one command against the current durable state.
    fn handle_command<'a>(
        &'a mut self,
        ctx: &'a mut DurableActorContext<'a, Self::Command>,
        state: &'a Self::State,
        command: Self::Command,
    ) -> DurableActorFuture<'a, Self::State>;
}

/// Durable actor context.
pub struct DurableActorContext<'a, M>
where
    M: Message,
{
    actor_context: &'a mut ActorContext<M>,
    persistence_id: PersistenceId,
    revision: Revision,
}

impl<'a, M> DurableActorContext<'a, M>
where
    M: Message,
{
    fn new(
        actor_context: &'a mut ActorContext<M>,
        persistence_id: PersistenceId,
        revision: Revision,
    ) -> Self {
        Self {
            actor_context,
            persistence_id,
            revision,
        }
    }

    /// Returns the durable persistence id.
    #[must_use]
    pub fn persistence_id(&self) -> &PersistenceId {
        &self.persistence_id
    }

    /// Returns the current durable revision.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
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

    /// Creates a side effect that replies after the durable effect commits.
    pub fn reply_after_commit<R>(
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

/// Spawns a durable actor with default local actor options.
pub fn spawn_durable_actor<A, Store>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    actor: A,
    store: Store,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: DurableActor,
    Store: DurableStateStore<A::State>,
{
    spawn_durable_actor_with_options(system, name, actor, store, ActorOptions::default())
}

/// Spawns a durable actor with explicit local actor options.
pub fn spawn_durable_actor_with_options<A, Store>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    actor: A,
    store: Store,
    options: ActorOptions,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: DurableActor,
    Store: DurableStateStore<A::State>,
{
    let actor = Mutex::new(Some(actor));
    let store = Mutex::new(Some(store));
    system.spawn_actor_with_options(
        name,
        move || {
            let actor = actor
                .lock()
                .expect("durable actor factory mutex poisoned")
                .take()
                .expect("single-use durable actor factory cannot restart");
            let store = store
                .lock()
                .expect("durable store factory mutex poisoned")
                .take()
                .expect("single-use durable store factory cannot restart");
            DurableActorRuntime::new(actor, store)
        },
        options,
    )
}

/// Spawns a restartable durable actor factory with default local actor options.
pub fn spawn_durable_actor_factory<A, Store, Factory>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    factory: Factory,
    store: Store,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: DurableActor,
    Store: DurableStateStore<A::State>,
    Factory: Fn() -> A + Send + Sync + 'static,
{
    spawn_durable_actor_factory_with_options(system, name, factory, store, ActorOptions::default())
}

/// Spawns a restartable durable actor factory with explicit local actor options.
pub fn spawn_durable_actor_factory_with_options<A, Store, Factory>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    factory: Factory,
    store: Store,
    options: ActorOptions,
) -> RakkaResult<ActorRef<A::Command>>
where
    A: DurableActor,
    Store: DurableStateStore<A::State>,
    Factory: Fn() -> A + Send + Sync + 'static,
{
    system.spawn_actor_with_options(
        name,
        move || DurableActorRuntime::new(factory(), store.clone()),
        options,
    )
}

struct DurableActorRuntime<A, Store>
where
    A: DurableActor,
    Store: DurableStateStore<A::State>,
{
    actor: A,
    store: Store,
    persistence_id: PersistenceId,
    record: Option<StateRecord<A::State>>,
}

impl<A, Store> DurableActorRuntime<A, Store>
where
    A: DurableActor,
    Store: DurableStateStore<A::State>,
{
    fn new(actor: A, store: Store) -> Self {
        let persistence_id = actor.persistence_id();
        Self {
            actor,
            store,
            persistence_id,
            record: None,
        }
    }

    async fn recover(&mut self) -> RakkaResult<()> {
        let loaded = self
            .store
            .load(&self.persistence_id)
            .await
            .map_err(|error| error.into_rakka_error())?;
        self.record =
            Some(loaded.unwrap_or_else(|| StateRecord::missing(self.actor.empty_state())));
        Ok(())
    }

    async fn apply_effect(&mut self, effect: DurableEffect<A::State>) -> RakkaResult<ActorAction> {
        let (state_change, stop, side_effects) = effect.into_parts();
        let current_revision = self
            .record
            .as_ref()
            .ok_or_else(|| {
                crate::DurableError::NotRecovered {
                    persistence_id: self.persistence_id.clone(),
                }
                .into_rakka_error()
            })?
            .revision;

        match state_change {
            DurableStateChange::None => {}
            DurableStateChange::Persist(state) => {
                let record = self
                    .store
                    .compare_and_set(&self.persistence_id, current_revision, state)
                    .await
                    .map_err(|error| error.into_rakka_error())?;
                self.record = Some(record);
            }
            DurableStateChange::Delete => {
                self.store
                    .delete(&self.persistence_id, current_revision)
                    .await
                    .map_err(|error| error.into_rakka_error())?;
                self.record = Some(StateRecord::missing(self.actor.empty_state()));
            }
        }

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

impl<A, Store> Actor for DurableActorRuntime<A, Store>
where
    A: DurableActor,
    Store: DurableStateStore<A::State>,
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
            let record = self.record.as_ref().ok_or_else(|| {
                crate::DurableError::NotRecovered {
                    persistence_id: self.persistence_id.clone(),
                }
                .into_rakka_error()
            })?;
            let state = record.state.clone();
            let revision = record.revision;
            let persistence_id = self.persistence_id.clone();
            let mut durable_ctx = DurableActorContext::new(ctx, persistence_id, revision);
            let effect = self
                .actor
                .handle_command(&mut durable_ctx, &state, msg)
                .await
                .map_err(|error| error.into_rakka_error())?;

            self.apply_effect(effect).await
        })
    }
}
