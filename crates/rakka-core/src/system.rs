//! Local actor system.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Notify};

use crate::actor::{
    spawn_actor_task, Actor, ActorCell, ActorRef, ActorRuntimeSnapshot, ActorStopHandle,
    SerializedActorRef,
};
use crate::dead_letter::DeadLetter;
use crate::metrics::{
    MetricsRecorder, NoopMetricsRecorder, METRIC_ACTOR_COUNT, METRIC_ACTOR_MAILBOX_DEPTH,
};
use crate::path::{validate_actor_path_segment, ActorPath, ActorUid};
use crate::receptionist::ReceptionistRegistry;
use crate::supervision::ActorOptions;
use crate::Message;
use crate::{RakkaError, RakkaResult};

/// Default actor-system termination timeout.
pub const DEFAULT_SYSTEM_TERMINATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Root runtime for local Rakka actors.
#[derive(Clone)]
pub struct ActorSystem {
    inner: Arc<ActorSystemInner>,
}

pub(crate) struct ActorSystemInner {
    name: String,
    next_actor_id: AtomicU64,
    dead_letters: broadcast::Sender<DeadLetter>,
    metrics: Arc<dyn MetricsRecorder>,
    serialization_registry: Option<ActorSystemSerializationRegistry>,
    runtime_settings: ActorSystemRuntimeSettings,
    shutdown_config: ActorSystemShutdownConfig,
    receptionist: Arc<ReceptionistRegistry>,
    actors: Mutex<Vec<ActorStopHandle>>,
    live_actors: Mutex<HashMap<ActorPath, Arc<ActorCell>>>,
    terminating: std::sync::atomic::AtomicBool,
    terminated: std::sync::atomic::AtomicBool,
    termination_notify: Notify,
}

/// Builder for [`ActorSystem`].
pub struct ActorSystemBuilder {
    name: String,
    metrics: Arc<dyn MetricsRecorder>,
    serialization_registry: Option<ActorSystemSerializationRegistry>,
    runtime_settings: ActorSystemRuntimeSettings,
    shutdown_config: ActorSystemShutdownConfig,
}

impl ActorSystemBuilder {
    /// Creates a builder with default runtime settings.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            metrics: Arc::new(NoopMetricsRecorder),
            serialization_registry: None,
            runtime_settings: ActorSystemRuntimeSettings::default(),
            shutdown_config: ActorSystemShutdownConfig::default(),
        }
    }

    /// Configures the actor-system metrics recorder.
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Configures an application serialization registry handle.
    ///
    /// The core crate stores this handle opaquely so higher-level crates can
    /// pass their own registry type without creating a dependency cycle.
    #[must_use]
    pub fn with_serialization_registry<T>(mut self, registry: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        self.serialization_registry = Some(ActorSystemSerializationRegistry::new(registry));
        self
    }

    /// Configures local runtime settings.
    #[must_use]
    pub fn with_runtime_settings(mut self, runtime_settings: ActorSystemRuntimeSettings) -> Self {
        self.runtime_settings = runtime_settings;
        self
    }

    /// Configures graceful shutdown behavior.
    #[must_use]
    pub fn with_shutdown_config(mut self, shutdown_config: ActorSystemShutdownConfig) -> Self {
        self.shutdown_config = shutdown_config;
        self
    }

    /// Builds the actor system.
    pub async fn build(self) -> RakkaResult<ActorSystem> {
        ActorSystem::from_builder(self)
    }
}

impl Debug for ActorSystemBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorSystemBuilder")
            .field("name", &self.name)
            .field("runtime_settings", &self.runtime_settings)
            .field("shutdown_config", &self.shutdown_config)
            .field(
                "has_serialization_registry",
                &self.serialization_registry.is_some(),
            )
            .finish()
    }
}

/// Opaque serialization registry handle stored by an actor system.
#[derive(Clone)]
pub struct ActorSystemSerializationRegistry {
    inner: Arc<dyn Any + Send + Sync>,
}

impl ActorSystemSerializationRegistry {
    /// Creates an opaque registry handle.
    #[must_use]
    pub fn new<T>(registry: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(registry),
        }
    }

    /// Returns true if the stored registry has type `T`.
    #[must_use]
    pub fn is<T>(&self) -> bool
    where
        T: Send + Sync + 'static,
    {
        self.inner.is::<T>()
    }

    /// Attempts to clone the stored registry handle as type `T`.
    #[must_use]
    pub fn downcast<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner.clone().downcast::<T>().ok()
    }
}

impl Debug for ActorSystemSerializationRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorSystemSerializationRegistry")
            .finish_non_exhaustive()
    }
}

/// Local actor-system runtime settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorSystemRuntimeSettings {
    default_mailbox_capacity: usize,
}

impl ActorSystemRuntimeSettings {
    /// Creates runtime settings with the supplied mailbox capacity.
    #[must_use]
    pub const fn new(default_mailbox_capacity: usize) -> Self {
        Self {
            default_mailbox_capacity,
        }
    }

    /// Returns the default mailbox capacity used by future facade spawn APIs.
    #[must_use]
    pub const fn default_mailbox_capacity(&self) -> usize {
        self.default_mailbox_capacity
    }
}

impl Default for ActorSystemRuntimeSettings {
    fn default() -> Self {
        Self {
            default_mailbox_capacity: crate::actor::DEFAULT_MAILBOX_CAPACITY,
        }
    }
}

/// Graceful shutdown settings for an actor system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorSystemShutdownConfig {
    termination_timeout: Duration,
}

impl ActorSystemShutdownConfig {
    /// Creates shutdown settings with the supplied termination timeout.
    #[must_use]
    pub const fn new(termination_timeout: Duration) -> Self {
        Self {
            termination_timeout,
        }
    }

    /// Returns how long `terminate` waits for actors to stop.
    #[must_use]
    pub const fn termination_timeout(&self) -> Duration {
        self.termination_timeout
    }
}

impl Default for ActorSystemShutdownConfig {
    fn default() -> Self {
        Self {
            termination_timeout: DEFAULT_SYSTEM_TERMINATION_TIMEOUT,
        }
    }
}

/// Resolver for serializing and resolving local typed actor references.
#[derive(Clone)]
pub struct ActorRefResolver {
    system: ActorSystem,
}

impl ActorRefResolver {
    /// Creates a resolver for an actor system.
    #[must_use]
    pub fn new(system: ActorSystem) -> Self {
        Self { system }
    }

    /// Serializes an actor reference.
    #[must_use]
    pub fn to_serialized_ref<M>(&self, actor_ref: &ActorRef<M>) -> SerializedActorRef
    where
        M: Message,
    {
        actor_ref.to_serialized_ref()
    }

    /// Resolves a serialized actor reference in this local actor system.
    pub fn resolve<M>(&self, serialized: &SerializedActorRef) -> RakkaResult<ActorRef<M>>
    where
        M: Message,
    {
        self.system.resolve_actor_ref(serialized)
    }
}

/// Serializable actor-system snapshot used by operational diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorSystemSnapshot {
    name: String,
    active_actors: usize,
    total_actors: usize,
    actors: Vec<ActorRuntimeSnapshot>,
}

impl ActorSystemSnapshot {
    /// Creates an actor-system snapshot.
    #[must_use]
    pub fn new(name: impl Into<String>, actors: Vec<ActorRuntimeSnapshot>) -> Self {
        let active_actors = actors.iter().filter(|actor| !actor.terminated()).count();
        let total_actors = actors.len();
        Self {
            name: name.into(),
            active_actors,
            total_actors,
            actors,
        }
    }

    /// Actor system name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Number of actors that have not terminated.
    #[must_use]
    pub const fn active_actors(&self) -> usize {
        self.active_actors
    }

    /// Number of actors ever registered with this system.
    #[must_use]
    pub const fn total_actors(&self) -> usize {
        self.total_actors
    }

    /// Actor runtime snapshots.
    #[must_use]
    pub fn actors(&self) -> &[ActorRuntimeSnapshot] {
        &self.actors
    }
}

impl ActorSystem {
    /// Creates an actor-system builder.
    #[must_use]
    pub fn builder(name: impl Into<String>) -> ActorSystemBuilder {
        ActorSystemBuilder::new(name)
    }

    /// Creates a new local actor system.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_metrics(name, Arc::new(NoopMetricsRecorder))
    }

    /// Creates a new local actor system with a metrics recorder.
    #[must_use]
    pub fn with_metrics(name: impl Into<String>, metrics: Arc<dyn MetricsRecorder>) -> Self {
        Self::builder(name)
            .with_metrics(metrics)
            .build_sync()
            .expect("default actor-system builder should be valid")
    }

    fn from_builder(builder: ActorSystemBuilder) -> RakkaResult<Self> {
        if builder.name.is_empty() {
            return Err(RakkaError::core(
                "invalid-system-name",
                "actor system name must not be empty",
            ));
        }

        validate_actor_path_segment(&builder.name)?;
        let (dead_letters, _) = broadcast::channel(1024);
        Ok(Self {
            inner: Arc::new(ActorSystemInner {
                name: builder.name,
                next_actor_id: AtomicU64::new(1),
                dead_letters,
                metrics: builder.metrics,
                serialization_registry: builder.serialization_registry,
                runtime_settings: builder.runtime_settings,
                shutdown_config: builder.shutdown_config,
                receptionist: Arc::new(ReceptionistRegistry::new()),
                actors: Mutex::new(Vec::new()),
                live_actors: Mutex::new(HashMap::new()),
                terminating: std::sync::atomic::AtomicBool::new(false),
                terminated: std::sync::atomic::AtomicBool::new(false),
                termination_notify: Notify::new(),
            }),
        })
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

    /// Shared metrics recorder configured for this actor system.
    #[must_use]
    pub fn metrics(&self) -> Arc<dyn MetricsRecorder> {
        self.inner.metrics.clone()
    }

    /// Configured serialization registry handle, if one was supplied.
    #[must_use]
    pub fn serialization_registry(&self) -> Option<ActorSystemSerializationRegistry> {
        self.inner.serialization_registry.clone()
    }

    /// Configured runtime settings.
    #[must_use]
    pub fn runtime_settings(&self) -> &ActorSystemRuntimeSettings {
        &self.inner.runtime_settings
    }

    /// Configured shutdown settings.
    #[must_use]
    pub fn shutdown_config(&self) -> &ActorSystemShutdownConfig {
        &self.inner.shutdown_config
    }

    /// Returns an actor reference resolver for this system.
    #[must_use]
    pub fn actor_ref_resolver(&self) -> ActorRefResolver {
        ActorRefResolver::new(self.clone())
    }

    pub(crate) fn receptionist_registry(&self) -> Arc<ReceptionistRegistry> {
        self.inner.receptionist.clone()
    }

    /// Returns a serializable actor-system snapshot.
    #[must_use]
    pub fn snapshot(&self) -> ActorSystemSnapshot {
        let actors = self
            .inner
            .actors
            .lock()
            .expect("actor registry mutex poisoned")
            .iter()
            .map(ActorStopHandle::snapshot)
            .collect();
        ActorSystemSnapshot::new(self.name(), actors)
    }

    /// Records actor-count and mailbox-depth metrics and returns the same snapshot.
    pub fn record_metrics(&self) -> ActorSystemSnapshot {
        let snapshot = self.snapshot();
        let total = snapshot.total_actors().to_string();
        let active = snapshot.active_actors().to_string();
        self.inner.metrics.record_gauge(
            METRIC_ACTOR_COUNT,
            snapshot.active_actors() as f64,
            &[
                ("system", snapshot.name()),
                ("state", "active"),
                ("total", total.as_str()),
                ("active", active.as_str()),
            ],
        );

        for actor in snapshot.actors() {
            let path = actor.path().to_string();
            let capacity = actor.mailbox_capacity().to_string();
            self.inner.metrics.record_gauge(
                METRIC_ACTOR_MAILBOX_DEPTH,
                actor.mailbox_depth() as f64,
                &[
                    ("system", snapshot.name()),
                    ("actor", path.as_str()),
                    ("capacity", capacity.as_str()),
                ],
            );
        }

        snapshot
    }

    /// Spawns a local actor with default options.
    pub fn spawn<A>(&self, name: impl AsRef<str>, actor: A) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        self.spawn_actor(name, actor)
    }

    /// Spawns a local actor using a restartable factory and default options.
    pub fn spawn_factory<A, F>(
        &self,
        name: impl AsRef<str>,
        factory: F,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_actor_factory(name, factory)
    }

    /// Spawns a local actor using a restartable factory and explicit options.
    pub fn spawn_with_options<A, F>(
        &self,
        name: impl AsRef<str>,
        factory: F,
        options: ActorOptions,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_actor_with_options(name, factory, options)
    }

    /// Spawns an anonymous user actor with default options.
    pub fn spawn_anonymous<A>(&self, actor: A) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        let actor = Mutex::new(Some(actor));
        self.spawn_anonymous_with_options(
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

    /// Spawns an anonymous user actor using a restartable factory and default options.
    pub fn spawn_anonymous_factory<A, F>(&self, factory: F) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_anonymous_with_options(factory, ActorOptions::default())
    }

    /// Spawns an anonymous user actor using a restartable factory and explicit options.
    pub fn spawn_anonymous_with_options<A, F>(
        &self,
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

        let (path, uid) = self.next_anonymous_user_identity();
        spawn_actor_task(self.clone(), path, uid, factory, options)
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

        let (path, uid) = self.next_user_identity(name.as_ref())?;
        spawn_actor_task(self.clone(), path, uid, factory, options)
    }

    /// Spawns a system actor in the reserved `/system` namespace.
    pub fn spawn_system_actor<A>(
        &self,
        name: impl AsRef<str>,
        actor: A,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
    {
        let actor = Mutex::new(Some(actor));
        self.spawn_system_actor_with_options(
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

    /// Spawns a restartable system actor with default options.
    pub fn spawn_system_actor_factory<A, F>(
        &self,
        name: impl AsRef<str>,
        factory: F,
    ) -> RakkaResult<ActorRef<A::Msg>>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        self.spawn_system_actor_with_options(name, factory, ActorOptions::default())
    }

    /// Spawns a restartable system actor with explicit options.
    pub fn spawn_system_actor_with_options<A, F>(
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

        let (path, uid) = self.next_system_identity(name.as_ref())?;
        spawn_actor_task(self.clone(), path, uid, factory, options)
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

    /// Stops all actors and waits for the system to terminate.
    pub async fn terminate(&self) -> RakkaResult<()> {
        self.inner
            .terminating
            .store(true, std::sync::atomic::Ordering::Release);
        self.shutdown();

        let timeout = self.inner.shutdown_config.termination_timeout();
        let outcome = tokio::time::timeout(timeout, async {
            loop {
                let notified = self.inner.termination_notify.notified();
                if self.active_actor_count() == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await;

        match outcome {
            Ok(()) => {
                self.inner
                    .terminated
                    .store(true, std::sync::atomic::Ordering::Release);
                self.inner.termination_notify.notify_waiters();
                Ok(())
            }
            Err(_elapsed) => Err(RakkaError::core(
                "system-termination-timeout",
                format!(
                    "actor system '{}' did not terminate within {:?}",
                    self.name(),
                    timeout
                ),
            )),
        }
    }

    /// Waits until `terminate` has completed for this actor system.
    pub async fn when_terminated(&self) {
        loop {
            let notified = self.inner.termination_notify.notified();
            if self
                .inner
                .terminated
                .load(std::sync::atomic::Ordering::Acquire)
            {
                break;
            }
            notified.await;
        }
    }

    /// Returns true once system termination has completed.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.inner
            .terminated
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn child_identity(
        &self,
        parent: &ActorPath,
        child_name: &str,
    ) -> RakkaResult<(ActorPath, ActorUid)> {
        validate_actor_path_segment(child_name)?;
        let uid = self.next_actor_uid();
        Ok((parent.child(child_name), uid))
    }

    pub(crate) fn anonymous_child_identity(
        &self,
        parent: &ActorPath,
    ) -> (String, ActorPath, ActorUid) {
        let uid = self.next_actor_uid();
        let name = format!("$anon-{}", uid.value());
        let path = parent.child(&name);
        (name, path, uid)
    }

    pub(crate) fn dead_letters(&self) -> broadcast::Sender<DeadLetter> {
        self.inner.dead_letters.clone()
    }

    pub(crate) fn register_actor_cell(&self, cell: Arc<ActorCell>) -> RakkaResult<()> {
        if self
            .inner
            .terminating
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(RakkaError::core(
                "system-terminating",
                "cannot spawn actor while actor system is terminating",
            ));
        }

        let mut live = self
            .inner
            .live_actors
            .lock()
            .expect("live actor registry mutex poisoned");
        if live
            .get(cell.path())
            .is_some_and(|registered| !registered.is_terminated())
        {
            return Err(RakkaError::core(
                "actor-path-in-use",
                format!("actor path '{}' is already live", cell.path()),
            ));
        }
        live.insert(cell.path().clone(), cell);
        Ok(())
    }

    pub(crate) fn unregister_actor_cell(&self, cell: &Arc<ActorCell>) {
        let mut live = self
            .inner
            .live_actors
            .lock()
            .expect("live actor registry mutex poisoned");
        if live
            .get(cell.path())
            .is_some_and(|registered| registered.uid() == cell.uid())
        {
            live.remove(cell.path());
        }
        drop(live);
        self.inner.termination_notify.notify_waiters();
    }

    pub(crate) fn register_actor(&self, actor: ActorStopHandle) {
        self.inner
            .actors
            .lock()
            .expect("actor registry mutex poisoned")
            .push(actor);
    }

    fn active_actor_count(&self) -> usize {
        self.inner
            .live_actors
            .lock()
            .expect("live actor registry mutex poisoned")
            .len()
    }

    fn resolve_actor_ref<M>(&self, serialized: &SerializedActorRef) -> RakkaResult<ActorRef<M>>
    where
        M: Message,
    {
        if serialized.system_name() != self.name() {
            return Err(RakkaError::core(
                "actor-ref-system-mismatch",
                format!(
                    "actor ref belongs to system '{}' but resolver is for '{}'",
                    serialized.system_name(),
                    self.name()
                ),
            ));
        }

        let live = self
            .inner
            .live_actors
            .lock()
            .expect("live actor registry mutex poisoned");
        let cell = live.get(serialized.path()).ok_or_else(|| {
            RakkaError::core(
                "actor-ref-not-found",
                format!("actor ref '{}' is not live", serialized.path()),
            )
        })?;

        if cell.uid() != serialized.uid() {
            return Err(RakkaError::core(
                "actor-ref-incarnation-mismatch",
                format!(
                    "actor ref '{}' has uid {} but live uid is {}",
                    serialized.path(),
                    serialized.uid(),
                    cell.uid()
                ),
            ));
        }

        if cell.message_type() != serialized.message_type() {
            return Err(RakkaError::core(
                "actor-ref-message-type-mismatch",
                format!(
                    "actor ref '{}' has message type '{}' but live type is '{}'",
                    serialized.path(),
                    serialized.message_type(),
                    cell.message_type()
                ),
            ));
        }

        ActorCell::typed_ref(cell).ok_or_else(|| {
            RakkaError::core(
                "actor-ref-message-type-mismatch",
                format!(
                    "actor ref '{}' could not be resolved as '{}'",
                    serialized.path(),
                    std::any::type_name::<M>()
                ),
            )
        })
    }

    fn next_user_identity(&self, actor_name: &str) -> RakkaResult<(ActorPath, ActorUid)> {
        validate_actor_path_segment(actor_name)?;
        Ok((
            ActorPath::user(self.name(), actor_name),
            self.next_actor_uid(),
        ))
    }

    fn next_anonymous_user_identity(&self) -> (ActorPath, ActorUid) {
        let uid = self.next_actor_uid();
        let name = format!("$anon-{}", uid.value());
        (ActorPath::user(self.name(), &name), uid)
    }

    fn next_system_identity(&self, actor_name: &str) -> RakkaResult<(ActorPath, ActorUid)> {
        validate_actor_path_segment(actor_name)?;
        Ok((
            ActorPath::system(self.name(), actor_name),
            self.next_actor_uid(),
        ))
    }

    fn next_actor_uid(&self) -> ActorUid {
        ActorUid::new(self.inner.next_actor_id.fetch_add(1, Ordering::Relaxed))
    }
}

trait BuildSync {
    fn build_sync(self) -> RakkaResult<ActorSystem>;
}

impl BuildSync for ActorSystemBuilder {
    fn build_sync(self) -> RakkaResult<ActorSystem> {
        ActorSystem::from_builder(self)
    }
}
