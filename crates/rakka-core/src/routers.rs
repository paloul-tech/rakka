//! Local typed routers.

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    validate_actor_path_segment, Actor, ActorOptions, ActorRef, ActorSystem, Message, RakkaError,
    RakkaResult, TellError,
};

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
            options: ActorOptions::default(),
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
        let Some(index) = state.select(self.strategy) else {
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
}

impl<M> Clone for PoolRouter<M>
where
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            strategy: self.strategy,
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
        })
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
