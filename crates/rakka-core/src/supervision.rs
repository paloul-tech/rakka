//! Supervision configuration for local actors.

use std::time::Duration;

/// Local actor supervision strategy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisionStrategy {
    /// Keep the current actor state and continue with the next message.
    Resume,
    /// Replace the actor instance using its factory and continue.
    Restart,
    /// Stop the actor after a failure.
    Stop,
    /// Escalate to the parent. In Phase 1 root actors treat escalation as stop.
    Escalate,
    /// Restart with exponential backoff and a finite restart budget.
    RestartWithBackoff {
        /// Initial delay before the first restart.
        min_backoff: Duration,
        /// Maximum restart delay.
        max_backoff: Duration,
        /// Maximum number of restarts before stopping.
        max_restarts: usize,
    },
}

/// Options used when spawning a local actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorOptions {
    /// Capacity of the actor's bounded mailbox.
    pub mailbox_capacity: usize,
    /// Supervision strategy used when the actor handler fails or panics.
    pub supervision: SupervisionStrategy,
    /// Dispatcher hint for future runtime integrations.
    pub dispatcher: DispatcherHint,
    /// Whether actor-level instrumentation should be enabled.
    pub instrumentation: bool,
    /// Whether this actor is expected to run blocking work.
    pub blocking: bool,
}

impl ActorOptions {
    /// Creates options with a custom mailbox capacity.
    #[must_use]
    pub fn with_mailbox_capacity(mut self, mailbox_capacity: usize) -> Self {
        self.mailbox_capacity = mailbox_capacity;
        self
    }

    /// Creates options with a custom supervision strategy.
    #[must_use]
    pub fn with_supervision(mut self, supervision: SupervisionStrategy) -> Self {
        self.supervision = supervision;
        self
    }

    /// Creates options with a dispatcher hint.
    #[must_use]
    pub const fn with_dispatcher(mut self, dispatcher: DispatcherHint) -> Self {
        self.dispatcher = dispatcher;
        self
    }

    /// Creates options with instrumentation enabled or disabled.
    #[must_use]
    pub const fn with_instrumentation(mut self, instrumentation: bool) -> Self {
        self.instrumentation = instrumentation;
        self
    }

    /// Creates options with a blocking-work hint.
    #[must_use]
    pub const fn with_blocking_hint(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }
}

impl Default for ActorOptions {
    fn default() -> Self {
        Self {
            mailbox_capacity: crate::actor::DEFAULT_MAILBOX_CAPACITY,
            supervision: SupervisionStrategy::Stop,
            dispatcher: DispatcherHint::Default,
            instrumentation: true,
            blocking: false,
        }
    }
}

/// Dispatcher hint recorded on spawn options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatcherHint {
    /// Use the actor system's default asynchronous dispatcher.
    Default,
    /// The actor may perform blocking work and should be isolated by future dispatchers.
    Blocking,
}

/// Akka-like spawn options alias.
pub type SpawnOptions = ActorOptions;

/// Akka-like actor props alias.
pub type ActorProps = ActorOptions;
