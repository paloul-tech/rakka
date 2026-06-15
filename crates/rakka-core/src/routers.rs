//! Local typed routers.

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;

use crate::{
    validate_actor_path_segment, Actor, ActorOptions, ActorRef, ActorSystem, Message, RakkaError,
    RakkaResult, Receptionist, ReceptionistError, ReceptionistResult, ReceptionistSubscription,
    ServiceKey, TellError,
};

const DEFAULT_CONSISTENT_HASH_VIRTUAL_NODES: usize = 32;

type HashMapper<M> = Arc<dyn Fn(&M) -> u64 + Send + Sync + 'static>;

struct ConsistentHashConfig<M>
where
    M: Message,
{
    mapper: HashMapper<M>,
    virtual_nodes: usize,
}

impl<M> ConsistentHashConfig<M>
where
    M: Message,
{
    fn new<K, H>(key_mapper: K) -> Self
    where
        K: Fn(&M) -> H + Send + Sync + 'static,
        H: Hash + 'static,
    {
        Self {
            mapper: Arc::new(move |message| hash_value(&key_mapper(message))),
            virtual_nodes: DEFAULT_CONSISTENT_HASH_VIRTUAL_NODES,
        }
    }
}

impl<M> Clone for ConsistentHashConfig<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            mapper: self.mapper.clone(),
            virtual_nodes: self.virtual_nodes,
        }
    }
}

/// Facade namespace for local and clustered router builders.
#[derive(Debug, Clone, Copy, Default)]
pub struct Routers;

impl Routers {
    /// Creates a local pool router builder.
    #[must_use]
    pub fn pool<A, F>(
        name: impl Into<String>,
        pool_size: usize,
        factory: F,
    ) -> PoolRouterBuilder<A, F>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        PoolRouterBuilder {
            name: name.into(),
            pool_size,
            factory,
            strategy: PoolRoutingStrategy::RoundRobin,
            consistent_hash: None,
            consistent_hash_virtual_nodes: DEFAULT_CONSISTENT_HASH_VIRTUAL_NODES,
            options: ActorOptions::default(),
        }
    }

    /// Creates a local receptionist-backed group router builder.
    #[must_use]
    pub fn group<M>(service_key: ServiceKey<M>) -> GroupRouterBuilder<M>
    where
        M: Message,
    {
        GroupRouterBuilder {
            service_key,
            strategy: GroupRoutingStrategy::RoundRobin,
            consistent_hash: None,
            consistent_hash_virtual_nodes: DEFAULT_CONSISTENT_HASH_VIRTUAL_NODES,
            no_routee_behavior: GroupNoRouteeBehavior::FailFast,
        }
    }
}

/// Routing strategy used by a local pool router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolRoutingStrategy {
    /// Route to live routees in deterministic round-robin order.
    RoundRobin,
    /// Route to a pseudo-random live routee.
    Random,
    /// Route messages with equal hash keys to the same live routee when the
    /// routee set is unchanged.
    ConsistentHash,
}

/// Builder for a local pool router.
pub struct PoolRouterBuilder<A, F>
where
    A: Actor,
    F: Fn() -> A + Send + Sync + 'static,
{
    name: String,
    pool_size: usize,
    factory: F,
    strategy: PoolRoutingStrategy,
    consistent_hash: Option<ConsistentHashConfig<A::Msg>>,
    consistent_hash_virtual_nodes: usize,
    options: ActorOptions,
}

impl<A, F> PoolRouterBuilder<A, F>
where
    A: Actor,
    F: Fn() -> A + Send + Sync + 'static,
{
    /// Uses deterministic round-robin routing.
    #[must_use]
    pub const fn with_round_robin(mut self) -> Self {
        self.strategy = PoolRoutingStrategy::RoundRobin;
        self
    }

    /// Uses pseudo-random routing over live routees.
    #[must_use]
    pub const fn with_random(mut self) -> Self {
        self.strategy = PoolRoutingStrategy::Random;
        self
    }

    /// Uses consistent-hash routing over live routees.
    ///
    /// Messages that map to the same key are routed to the same live routee
    /// while the routee set is unchanged. Routee changes remap only the keys
    /// whose ring segment moved.
    #[must_use]
    pub fn with_consistent_hash<K, H>(mut self, key_mapper: K) -> Self
    where
        K: Fn(&A::Msg) -> H + Send + Sync + 'static,
        H: Hash + 'static,
    {
        self.strategy = PoolRoutingStrategy::ConsistentHash;
        self.consistent_hash = Some(ConsistentHashConfig::new(key_mapper));
        if let Some(config) = &mut self.consistent_hash {
            config.virtual_nodes = self.consistent_hash_virtual_nodes;
        }
        self
    }

    /// Sets the number of virtual nodes used by consistent-hash routing.
    ///
    /// This only affects [`PoolRoutingStrategy::ConsistentHash`].
    #[must_use]
    pub fn with_consistent_hash_virtual_nodes(mut self, virtual_nodes: usize) -> Self {
        self.consistent_hash_virtual_nodes = virtual_nodes;
        if let Some(config) = &mut self.consistent_hash {
            config.virtual_nodes = virtual_nodes;
        }
        self
    }

    /// Sets the routing strategy.
    #[must_use]
    pub const fn with_strategy(mut self, strategy: PoolRoutingStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Sets spawn options applied to every routee actor.
    #[must_use]
    pub fn with_options(mut self, options: ActorOptions) -> Self {
        self.options = options;
        self
    }

    /// Alias for [`PoolRouterBuilder::with_options`].
    #[must_use]
    pub fn with_spawn_options(self, options: ActorOptions) -> Self {
        self.with_options(options)
    }

    /// Spawns routee actors and returns a local pool router facade.
    pub fn spawn(self, system: &ActorSystem) -> RakkaResult<PoolRouter<A::Msg>> {
        if self.pool_size == 0 {
            return Err(RakkaError::core(
                "invalid-pool-size",
                "pool router size must be greater than zero",
            ));
        }
        validate_pool_consistent_hash_config(self.strategy, self.consistent_hash.as_ref())?;
        validate_actor_path_segment(&self.name)?;

        let factory = Arc::new(self.factory);
        let mut routees = Vec::with_capacity(self.pool_size);
        for index in 0..self.pool_size {
            let routee_name = format!("{}-{index}", self.name);
            let factory = factory.clone();
            match system.spawn_actor_with_options(
                routee_name,
                move || factory.as_ref()(),
                self.options.clone(),
            ) {
                Ok(routee) => routees.push(routee),
                Err(error) => {
                    for routee in &routees {
                        let _ = routee.stop();
                    }
                    return Err(error);
                }
            }
        }

        Ok(PoolRouter {
            name: Arc::from(self.name),
            strategy: self.strategy,
            consistent_hash: self.consistent_hash,
            state: Arc::new(Mutex::new(PoolRouterState {
                routees,
                next_round_robin: 0,
                random_state: random_seed(),
            })),
        })
    }
}

impl<A, F> Debug for PoolRouterBuilder<A, F>
where
    A: Actor,
    F: Fn() -> A + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolRouterBuilder")
            .field("name", &self.name)
            .field("pool_size", &self.pool_size)
            .field("strategy", &self.strategy)
            .field(
                "consistent_hash_virtual_nodes",
                &self
                    .consistent_hash
                    .as_ref()
                    .map(|config| config.virtual_nodes),
            )
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

/// Local pool router facade.
pub struct PoolRouter<M>
where
    M: Message,
{
    name: Arc<str>,
    strategy: PoolRoutingStrategy,
    consistent_hash: Option<ConsistentHashConfig<M>>,
    state: Arc<Mutex<PoolRouterState<M>>>,
}

impl<M> PoolRouter<M>
where
    M: Message,
{
    /// Router name prefix used for routee actor names.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Routing strategy.
    #[must_use]
    pub const fn strategy(&self) -> PoolRoutingStrategy {
        self.strategy
    }

    /// Returns live routee actor refs known to this router.
    #[must_use]
    pub fn routees(&self) -> Vec<ActorRef<M>> {
        let mut state = self.state.lock().expect("pool router mutex poisoned");
        state.cleanup_terminated();
        state.routees.clone()
    }

    /// Returns the number of live routees known to this router.
    #[must_use]
    pub fn routee_count(&self) -> usize {
        let mut state = self.state.lock().expect("pool router mutex poisoned");
        state.cleanup_terminated();
        state.routees.len()
    }

    /// Returns true when the router has no live routees.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routee_count() == 0
    }

    /// Sends a message to one live routee.
    pub fn tell(&self, message: M) -> Result<(), PoolRouterTellError<M>> {
        let mut state = self.state.lock().expect("pool router mutex poisoned");
        state.cleanup_terminated();
        let Some(index) = self.select_routee_index(&mut state, &message) else {
            return Err(PoolRouterTellError::NoRoutees { message });
        };
        let routee = state.routees[index].clone();

        match routee.tell(message) {
            Ok(()) => Ok(()),
            Err(TellError::Full(message)) => Err(PoolRouterTellError::Full { message }),
            Err(TellError::Closed(message)) => {
                state.remove_routee_at(index, &routee);
                Err(PoolRouterTellError::Closed { message })
            }
        }
    }

    /// Requests all live routees to stop.
    pub fn stop_routees(&self) {
        for routee in self.routees() {
            let _ = routee.stop();
        }
    }

    fn select_routee_index(&self, state: &mut PoolRouterState<M>, message: &M) -> Option<usize> {
        match self.strategy {
            PoolRoutingStrategy::RoundRobin | PoolRoutingStrategy::Random => {
                state.select(self.strategy)
            }
            PoolRoutingStrategy::ConsistentHash => {
                let config = self
                    .consistent_hash
                    .as_ref()
                    .expect("consistent hash strategy requires a mapper");
                state.select_consistent_hash((config.mapper)(message), config.virtual_nodes)
            }
        }
    }
}

impl<M> Clone for PoolRouter<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            strategy: self.strategy,
            consistent_hash: self.consistent_hash.clone(),
            state: self.state.clone(),
        }
    }
}

impl<M> Debug for PoolRouter<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolRouter")
            .field("name", &self.name)
            .field("strategy", &self.strategy)
            .field(
                "consistent_hash_virtual_nodes",
                &self
                    .consistent_hash
                    .as_ref()
                    .map(|config| config.virtual_nodes),
            )
            .field("routee_count", &self.routee_count())
            .finish()
    }
}

/// Failure returned by [`PoolRouter::tell`].
pub enum PoolRouterTellError<M> {
    /// No live routees were available.
    NoRoutees {
        /// Message that could not be routed.
        message: M,
    },
    /// Selected routee mailbox was full.
    Full {
        /// Message that could not be enqueued.
        message: M,
    },
    /// Selected routee was closed.
    Closed {
        /// Message that could not be enqueued.
        message: M,
    },
}

impl<M> PoolRouterTellError<M> {
    /// Returns the message that could not be routed.
    #[must_use]
    pub fn into_message(self) -> M {
        match self {
            Self::NoRoutees { message } | Self::Full { message } | Self::Closed { message } => {
                message
            }
        }
    }
}

impl<M> Debug for PoolRouterTellError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRoutees { .. } => f.debug_struct("NoRoutees").finish_non_exhaustive(),
            Self::Full { .. } => f.debug_struct("Full").finish_non_exhaustive(),
            Self::Closed { .. } => f.debug_struct("Closed").finish_non_exhaustive(),
        }
    }
}

impl<M> Display for PoolRouterTellError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRoutees { .. } => f.write_str("pool router has no live routees"),
            Self::Full { .. } => f.write_str("selected pool routee mailbox was full"),
            Self::Closed { .. } => f.write_str("selected pool routee was closed"),
        }
    }
}

impl<M> Error for PoolRouterTellError<M> {}

/// Routing strategy used by a local group router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupRoutingStrategy {
    /// Route to live receptionist routees in deterministic round-robin order.
    RoundRobin,
    /// Route to a pseudo-random live receptionist routee.
    Random,
    /// Route messages with equal hash keys to the same live receptionist routee
    /// when the routee set is unchanged.
    ConsistentHash,
}

/// Behavior used when a group router has no live routees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupNoRouteeBehavior {
    /// Fail fast and return the message to the caller.
    FailFast,
    /// Drop the message and report success.
    Drop,
}

/// Builder for a local receptionist-backed group router.
pub struct GroupRouterBuilder<M>
where
    M: Message,
{
    service_key: ServiceKey<M>,
    strategy: GroupRoutingStrategy,
    consistent_hash: Option<ConsistentHashConfig<M>>,
    consistent_hash_virtual_nodes: usize,
    no_routee_behavior: GroupNoRouteeBehavior,
}

impl<M> GroupRouterBuilder<M>
where
    M: Message,
{
    /// Uses deterministic round-robin routing.
    #[must_use]
    pub const fn with_round_robin(mut self) -> Self {
        self.strategy = GroupRoutingStrategy::RoundRobin;
        self
    }

    /// Uses pseudo-random routing over live routees.
    #[must_use]
    pub const fn with_random(mut self) -> Self {
        self.strategy = GroupRoutingStrategy::Random;
        self
    }

    /// Uses consistent-hash routing over live receptionist routees.
    ///
    /// Messages that map to the same key are routed to the same live routee
    /// while the receptionist listing is unchanged.
    #[must_use]
    pub fn with_consistent_hash<K, H>(mut self, key_mapper: K) -> Self
    where
        K: Fn(&M) -> H + Send + Sync + 'static,
        H: Hash + 'static,
    {
        self.strategy = GroupRoutingStrategy::ConsistentHash;
        self.consistent_hash = Some(ConsistentHashConfig::new(key_mapper));
        if let Some(config) = &mut self.consistent_hash {
            config.virtual_nodes = self.consistent_hash_virtual_nodes;
        }
        self
    }

    /// Sets the number of virtual nodes used by consistent-hash routing.
    ///
    /// This only affects [`GroupRoutingStrategy::ConsistentHash`].
    #[must_use]
    pub fn with_consistent_hash_virtual_nodes(mut self, virtual_nodes: usize) -> Self {
        self.consistent_hash_virtual_nodes = virtual_nodes;
        if let Some(config) = &mut self.consistent_hash {
            config.virtual_nodes = virtual_nodes;
        }
        self
    }

    /// Sets the routing strategy.
    #[must_use]
    pub const fn with_strategy(mut self, strategy: GroupRoutingStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Fails fast and returns messages when no routee is available.
    #[must_use]
    pub const fn with_fail_fast_no_routees(mut self) -> Self {
        self.no_routee_behavior = GroupNoRouteeBehavior::FailFast;
        self
    }

    /// Drops messages when no routee is available.
    #[must_use]
    pub const fn with_drop_when_no_routees(mut self) -> Self {
        self.no_routee_behavior = GroupNoRouteeBehavior::Drop;
        self
    }

    /// Sets no-routee behavior.
    #[must_use]
    pub const fn with_no_routee_behavior(mut self, behavior: GroupNoRouteeBehavior) -> Self {
        self.no_routee_behavior = behavior;
        self
    }

    /// Creates the local group router and starts a receptionist subscription
    /// task that keeps routees fresh.
    pub fn spawn(
        self,
        system: &ActorSystem,
        name: impl Into<String>,
    ) -> RakkaResult<GroupRouter<M>> {
        let name = name.into();
        validate_actor_path_segment(&name)?;
        validate_group_consistent_hash_config(self.strategy, self.consistent_hash.as_ref())?;
        let receptionist = Receptionist::get(system);
        let subscription = receptionist.subscribe(&self.service_key)?;
        let initial = receptionist.find(&self.service_key)?;
        let inner = Arc::new(GroupRouterInner {
            name: Arc::from(name),
            service_key: self.service_key.clone(),
            receptionist,
            strategy: self.strategy,
            consistent_hash: self.consistent_hash,
            no_routee_behavior: self.no_routee_behavior,
            state: Mutex::new(GroupRouterRoutees {
                routees: initial.service_instances().to_vec(),
                next_round_robin: 0,
                random_state: random_seed(),
            }),
            update_task: Mutex::new(None),
        });

        let task = spawn_group_router_subscription(Arc::downgrade(&inner), subscription);
        *inner
            .update_task
            .lock()
            .expect("group router task mutex poisoned") = Some(task);

        Ok(GroupRouter { inner })
    }
}

impl<M> Debug for GroupRouterBuilder<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupRouterBuilder")
            .field("service_key", &self.service_key)
            .field("strategy", &self.strategy)
            .field(
                "consistent_hash_virtual_nodes",
                &self
                    .consistent_hash
                    .as_ref()
                    .map(|config| config.virtual_nodes),
            )
            .field("no_routee_behavior", &self.no_routee_behavior)
            .finish()
    }
}

/// Local receptionist-backed group router facade.
pub struct GroupRouter<M>
where
    M: Message,
{
    inner: Arc<GroupRouterInner<M>>,
}

impl<M> GroupRouter<M>
where
    M: Message,
{
    /// Router name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Service key used for receptionist lookup.
    #[must_use]
    pub fn service_key(&self) -> &ServiceKey<M> {
        &self.inner.service_key
    }

    /// Routing strategy.
    #[must_use]
    pub fn strategy(&self) -> GroupRoutingStrategy {
        self.inner.strategy
    }

    /// No-routee behavior.
    #[must_use]
    pub fn no_routee_behavior(&self) -> GroupNoRouteeBehavior {
        self.inner.no_routee_behavior
    }

    /// Refreshes routees from the local receptionist and returns a snapshot.
    pub fn refresh(&self) -> ReceptionistResult<GroupRouterSnapshot> {
        self.inner.refresh()?;
        Ok(self.snapshot())
    }

    /// Returns live routee actor refs known to this router.
    #[must_use]
    pub fn routees(&self) -> Vec<ActorRef<M>> {
        let _ = self.inner.refresh();
        let mut state = self
            .inner
            .state
            .lock()
            .expect("group router mutex poisoned");
        state.cleanup_terminated();
        state.routees.clone()
    }

    /// Returns the number of live routees known to this router.
    #[must_use]
    pub fn routee_count(&self) -> usize {
        self.routees().len()
    }

    /// Returns true when this router currently has no live routees.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routee_count() == 0
    }

    /// Returns an observable router state snapshot.
    #[must_use]
    pub fn snapshot(&self) -> GroupRouterSnapshot {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("group router mutex poisoned");
        state.cleanup_terminated();
        GroupRouterSnapshot::new(
            self.name(),
            self.inner.service_key.id(),
            self.strategy(),
            self.no_routee_behavior(),
            state.routees.len(),
        )
    }

    /// Sends a message to one live receptionist routee.
    pub fn tell(&self, message: M) -> Result<(), GroupRouterTellError<M>> {
        if let Err(error) = self.inner.refresh() {
            return Err(GroupRouterTellError::Receptionist { message, error });
        }

        let mut state = self
            .inner
            .state
            .lock()
            .expect("group router mutex poisoned");
        state.cleanup_terminated();
        let Some(index) = self.select_routee_index(&mut state, &message) else {
            return match self.inner.no_routee_behavior {
                GroupNoRouteeBehavior::FailFast => Err(GroupRouterTellError::NoRoutees { message }),
                GroupNoRouteeBehavior::Drop => Ok(()),
            };
        };
        let routee = state.routees[index].clone();

        match routee.tell(message) {
            Ok(()) => Ok(()),
            Err(TellError::Full(message)) => Err(GroupRouterTellError::Full { message }),
            Err(TellError::Closed(message)) => {
                state.remove_routee_at(index, &routee);
                Err(GroupRouterTellError::Closed { message })
            }
        }
    }

    fn select_routee_index(&self, state: &mut GroupRouterRoutees<M>, message: &M) -> Option<usize> {
        match self.inner.strategy {
            GroupRoutingStrategy::RoundRobin | GroupRoutingStrategy::Random => {
                state.select(self.inner.strategy)
            }
            GroupRoutingStrategy::ConsistentHash => {
                let config = self
                    .inner
                    .consistent_hash
                    .as_ref()
                    .expect("consistent hash strategy requires a mapper");
                state.select_consistent_hash((config.mapper)(message), config.virtual_nodes)
            }
        }
    }
}

impl<M> Clone for GroupRouter<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M> Debug for GroupRouter<M>
where
    M: Message,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupRouter")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// Observable group router state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRouterSnapshot {
    name: String,
    service_id: String,
    strategy: GroupRoutingStrategy,
    no_routee_behavior: GroupNoRouteeBehavior,
    routee_count: usize,
}

impl GroupRouterSnapshot {
    fn new(
        name: impl Into<String>,
        service_id: impl Into<String>,
        strategy: GroupRoutingStrategy,
        no_routee_behavior: GroupNoRouteeBehavior,
        routee_count: usize,
    ) -> Self {
        Self {
            name: name.into(),
            service_id: service_id.into(),
            strategy,
            no_routee_behavior,
            routee_count,
        }
    }

    /// Router name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Receptionist service id.
    #[must_use]
    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    /// Routing strategy.
    #[must_use]
    pub const fn strategy(&self) -> GroupRoutingStrategy {
        self.strategy
    }

    /// No-routee behavior.
    #[must_use]
    pub const fn no_routee_behavior(&self) -> GroupNoRouteeBehavior {
        self.no_routee_behavior
    }

    /// Number of live routees known to the router.
    #[must_use]
    pub const fn routee_count(&self) -> usize {
        self.routee_count
    }
}

/// Failure returned by [`GroupRouter::tell`].
pub enum GroupRouterTellError<M> {
    /// No live routees were available.
    NoRoutees {
        /// Message that could not be routed.
        message: M,
    },
    /// Selected routee mailbox was full.
    Full {
        /// Message that could not be enqueued.
        message: M,
    },
    /// Selected routee was closed.
    Closed {
        /// Message that could not be enqueued.
        message: M,
    },
    /// Receptionist refresh failed before routing.
    Receptionist {
        /// Message that could not be routed.
        message: M,
        /// Receptionist failure.
        error: ReceptionistError,
    },
}

impl<M> GroupRouterTellError<M> {
    /// Returns the message that could not be routed.
    #[must_use]
    pub fn into_message(self) -> M {
        match self {
            Self::NoRoutees { message }
            | Self::Full { message }
            | Self::Closed { message }
            | Self::Receptionist { message, .. } => message,
        }
    }
}

impl<M> Debug for GroupRouterTellError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRoutees { .. } => f.debug_struct("NoRoutees").finish_non_exhaustive(),
            Self::Full { .. } => f.debug_struct("Full").finish_non_exhaustive(),
            Self::Closed { .. } => f.debug_struct("Closed").finish_non_exhaustive(),
            Self::Receptionist { error, .. } => f
                .debug_struct("Receptionist")
                .field("error", error)
                .finish_non_exhaustive(),
        }
    }
}

impl<M> Display for GroupRouterTellError<M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRoutees { .. } => f.write_str("group router has no live routees"),
            Self::Full { .. } => f.write_str("selected group routee mailbox was full"),
            Self::Closed { .. } => f.write_str("selected group routee was closed"),
            Self::Receptionist { error, .. } => Display::fmt(error, f),
        }
    }
}

impl<M> Error for GroupRouterTellError<M> {}

struct GroupRouterInner<M>
where
    M: Message,
{
    name: Arc<str>,
    service_key: ServiceKey<M>,
    receptionist: Receptionist,
    strategy: GroupRoutingStrategy,
    consistent_hash: Option<ConsistentHashConfig<M>>,
    no_routee_behavior: GroupNoRouteeBehavior,
    state: Mutex<GroupRouterRoutees<M>>,
    update_task: Mutex<Option<JoinHandle<()>>>,
}

impl<M> GroupRouterInner<M>
where
    M: Message,
{
    fn refresh(&self) -> ReceptionistResult<()> {
        let listing = self.receptionist.find(&self.service_key)?;
        let mut state = self.state.lock().expect("group router mutex poisoned");
        state.replace_routees(listing.service_instances().to_vec());
        Ok(())
    }
}

impl<M> Drop for GroupRouterInner<M>
where
    M: Message,
{
    fn drop(&mut self) {
        if let Some(task) = self
            .update_task
            .lock()
            .expect("group router task mutex poisoned")
            .take()
        {
            task.abort();
        }
    }
}

struct GroupRouterRoutees<M>
where
    M: Message,
{
    routees: Vec<ActorRef<M>>,
    next_round_robin: usize,
    random_state: u64,
}

impl<M> GroupRouterRoutees<M>
where
    M: Message,
{
    fn replace_routees(&mut self, routees: Vec<ActorRef<M>>) {
        self.routees = routees
            .into_iter()
            .filter(|routee| !routee.is_terminated())
            .collect();
        self.normalize_cursor();
    }

    fn cleanup_terminated(&mut self) {
        self.routees.retain(|routee| !routee.is_terminated());
        self.normalize_cursor();
    }

    fn select(&mut self, strategy: GroupRoutingStrategy) -> Option<usize> {
        if self.routees.is_empty() {
            return None;
        }

        Some(match strategy {
            GroupRoutingStrategy::RoundRobin => {
                let index = self.next_round_robin % self.routees.len();
                self.next_round_robin = self.next_round_robin.wrapping_add(1);
                index
            }
            GroupRoutingStrategy::Random => {
                self.random_state = next_random(self.random_state);
                usize::try_from(self.random_state).unwrap_or(usize::MAX) % self.routees.len()
            }
            GroupRoutingStrategy::ConsistentHash => {
                unreachable!("consistent hash selection uses select_consistent_hash")
            }
        })
    }

    fn select_consistent_hash(&self, key_hash: u64, virtual_nodes: usize) -> Option<usize> {
        select_consistent_hash_routee(&self.routees, key_hash, virtual_nodes)
    }

    fn remove_routee_at(&mut self, index: usize, routee: &ActorRef<M>) {
        if self
            .routees
            .get(index)
            .is_some_and(|candidate| same_actor(candidate, routee))
        {
            self.routees.remove(index);
        } else {
            self.routees
                .retain(|candidate| !same_actor(candidate, routee));
        }
        self.normalize_cursor();
    }

    fn normalize_cursor(&mut self) {
        if !self.routees.is_empty() {
            self.next_round_robin %= self.routees.len();
        } else {
            self.next_round_robin = 0;
        }
    }
}

fn spawn_group_router_subscription<M>(
    inner: Weak<GroupRouterInner<M>>,
    mut subscription: ReceptionistSubscription<M>,
) -> JoinHandle<()>
where
    M: Message,
{
    tokio::spawn(async move {
        loop {
            match subscription.recv().await {
                Ok(listing) => {
                    let Some(inner) = inner.upgrade() else {
                        break;
                    };
                    let mut state = inner.state.lock().expect("group router mutex poisoned");
                    state.replace_routees(listing.service_instances().to_vec());
                }
                Err(ReceptionistError::SubscriptionLagged { .. }) => {
                    let Some(inner) = inner.upgrade() else {
                        break;
                    };
                    let _ = inner.refresh();
                }
                Err(ReceptionistError::SubscriptionClosed { .. })
                | Err(ReceptionistError::ServiceKeyTypeMismatch { .. }) => break,
            }
        }
    })
}

struct PoolRouterState<M>
where
    M: Message,
{
    routees: Vec<ActorRef<M>>,
    next_round_robin: usize,
    random_state: u64,
}

impl<M> PoolRouterState<M>
where
    M: Message,
{
    fn cleanup_terminated(&mut self) {
        self.routees.retain(|routee| !routee.is_terminated());
        if !self.routees.is_empty() {
            self.next_round_robin %= self.routees.len();
        } else {
            self.next_round_robin = 0;
        }
    }

    fn select(&mut self, strategy: PoolRoutingStrategy) -> Option<usize> {
        if self.routees.is_empty() {
            return None;
        }

        Some(match strategy {
            PoolRoutingStrategy::RoundRobin => {
                let index = self.next_round_robin % self.routees.len();
                self.next_round_robin = self.next_round_robin.wrapping_add(1);
                index
            }
            PoolRoutingStrategy::Random => {
                self.random_state = next_random(self.random_state);
                usize::try_from(self.random_state).unwrap_or(usize::MAX) % self.routees.len()
            }
            PoolRoutingStrategy::ConsistentHash => {
                unreachable!("consistent hash selection uses select_consistent_hash")
            }
        })
    }

    fn select_consistent_hash(&self, key_hash: u64, virtual_nodes: usize) -> Option<usize> {
        select_consistent_hash_routee(&self.routees, key_hash, virtual_nodes)
    }

    fn remove_routee_at(&mut self, index: usize, routee: &ActorRef<M>) {
        if self
            .routees
            .get(index)
            .is_some_and(|candidate| same_actor(candidate, routee))
        {
            self.routees.remove(index);
        } else {
            self.routees
                .retain(|candidate| !same_actor(candidate, routee));
        }

        if !self.routees.is_empty() {
            self.next_round_robin %= self.routees.len();
        } else {
            self.next_round_robin = 0;
        }
    }
}

fn validate_pool_consistent_hash_config<M>(
    strategy: PoolRoutingStrategy,
    config: Option<&ConsistentHashConfig<M>>,
) -> RakkaResult<()>
where
    M: Message,
{
    validate_consistent_hash_config(
        matches!(strategy, PoolRoutingStrategy::ConsistentHash),
        config,
    )
}

fn validate_group_consistent_hash_config<M>(
    strategy: GroupRoutingStrategy,
    config: Option<&ConsistentHashConfig<M>>,
) -> RakkaResult<()>
where
    M: Message,
{
    validate_consistent_hash_config(
        matches!(strategy, GroupRoutingStrategy::ConsistentHash),
        config,
    )
}

fn validate_consistent_hash_config<M>(
    consistent_hash: bool,
    config: Option<&ConsistentHashConfig<M>>,
) -> RakkaResult<()>
where
    M: Message,
{
    if !consistent_hash {
        return Ok(());
    }

    let Some(config) = config else {
        return Err(RakkaError::core(
            "missing-consistent-hash-mapper",
            "consistent hash routing requires a key mapper",
        ));
    };

    if config.virtual_nodes == 0 {
        return Err(RakkaError::core(
            "invalid-consistent-hash-virtual-nodes",
            "consistent hash virtual nodes must be greater than zero",
        ));
    }

    Ok(())
}

fn select_consistent_hash_routee<M>(
    routees: &[ActorRef<M>],
    key_hash: u64,
    virtual_nodes: usize,
) -> Option<usize>
where
    M: Message,
{
    if routees.is_empty() {
        return None;
    }

    let mut selected: Option<(u64, usize)> = None;
    let mut first: Option<(u64, usize)> = None;
    for (index, routee) in routees.iter().enumerate() {
        for virtual_node in 0..virtual_nodes {
            let point = routee_hash(routee, virtual_node);
            if first.map_or(true, |(first_point, _)| point < first_point) {
                first = Some((point, index));
            }
            if point >= key_hash
                && selected.map_or(true, |(selected_point, _)| point < selected_point)
            {
                selected = Some((point, index));
            }
        }
    }

    selected.or(first).map(|(_point, index)| index)
}

fn routee_hash<M>(routee: &ActorRef<M>, virtual_node: usize) -> u64
where
    M: Message,
{
    hash_value(&(routee.path().as_str(), routee.uid().value(), virtual_node))
}

fn hash_value<T>(value: &T) -> u64
where
    T: Hash + ?Sized,
{
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn same_actor<M>(left: &ActorRef<M>, right: &ActorRef<M>) -> bool
where
    M: Message,
{
    left.path() == right.path() && left.uid() == right.uid()
}

fn random_seed() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        });
    let mut hasher = DefaultHasher::new();
    now.hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    nonzero_random(hasher.finish())
}

fn next_random(state: u64) -> u64 {
    let mut value = nonzero_random(state);
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    nonzero_random(value)
}

const fn nonzero_random(value: u64) -> u64 {
    if value == 0 {
        0x9e37_79b9_7f4a_7c15
    } else {
        value
    }
}
