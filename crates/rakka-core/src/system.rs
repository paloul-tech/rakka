//! Local actor system.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

use crate::actor::{spawn_actor_task, Actor, ActorRef, ActorStopHandle};
use crate::dead_letter::DeadLetter;
use crate::path::ActorPath;
use crate::supervision::ActorOptions;
use crate::{RakkaError, RakkaResult};

/// Root runtime for local Rakka actors.
#[derive(Clone)]
pub struct ActorSystem {
    inner: Arc<ActorSystemInner>,
}

pub(crate) struct ActorSystemInner {
    name: String,
    next_actor_id: AtomicU64,
    dead_letters: broadcast::Sender<DeadLetter>,
    actors: Mutex<Vec<ActorStopHandle>>,
}

impl ActorSystem {
    /// Creates a new local actor system.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        let (dead_letters, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(ActorSystemInner {
                name: name.into(),
                next_actor_id: AtomicU64::new(1),
                dead_letters,
                actors: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Returns the actor system name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Subscribes to local dead-letter events.
    #[must_use]
    pub fn subscribe_dead_letters(&self) -> broadcast::Receiver<DeadLetter> {
        self.inner.dead_letters.subscribe()
    }

    /// Spawns a local actor with default options.
    pub fn spawn_actor<A>(&self, name: impl AsRef<str>, actor: A) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        let actor = Mutex::new(Some(actor));
        self.spawn_actor_with_options(
            name,
            move || {
                actor
                    .lock()
                    .expect("actor factory mutex poisoned")
                    .take()
                    .expect("single-use actor factory cannot restart")
            },
            ActorOptions::default(),
        )
    }

    /// Spawns a local actor using a restartable factory and default options.
    pub fn spawn_actor_factory<A, F>(
        &self,
        name: impl AsRef<str>,
        factory: F,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_actor_with_options(name, factory, ActorOptions::default())
    }

    /// Spawns a local actor using a restartable factory and explicit options.
    pub fn spawn_actor_with_options<A, F>(
        &self,
        name: impl AsRef<str>,
        factory: F,
        options: ActorOptions,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        if options.mailbox_capacity == 0 {
            return Err(RakkaError::core(
                "invalid-mailbox-capacity",
                "actor mailbox capacity must be greater than zero",
            ));
        }

        let path = self.next_user_path(name.as_ref());
        let actor_ref = spawn_actor_task(self.clone(), path, factory, options);
        self.register_actor(actor_ref.stop_handle());
        Ok(actor_ref)
    }

    /// Sends stop signals to all actors known by this system.
    pub fn shutdown(&self) {
        let actors = self
            .inner
            .actors
            .lock()
            .expect("actor registry mutex poisoned")
            .clone();

        for actor in actors {
            actor.stop();
        }
    }

    pub(crate) fn child_path(&self, parent: &ActorPath, child_name: &str) -> ActorPath {
        let incarnation = self.inner.next_actor_id.fetch_add(1, Ordering::Relaxed);
        parent.child(child_name, incarnation)
    }

    pub(crate) fn dead_letters(&self) -> broadcast::Sender<DeadLetter> {
        self.inner.dead_letters.clone()
    }

    pub(crate) fn register_actor(&self, actor: ActorStopHandle) {
        self.inner
            .actors
            .lock()
            .expect("actor registry mutex poisoned")
            .push(actor);
    }

    fn next_user_path(&self, actor_name: &str) -> ActorPath {
        let incarnation = self.inner.next_actor_id.fetch_add(1, Ordering::Relaxed);
        ActorPath::user(self.name(), actor_name, incarnation)
    }
}
