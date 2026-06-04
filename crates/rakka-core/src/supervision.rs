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
}

impl Default for ActorOptions {
    fn default() -> Self {
        Self {
            mailbox_capacity: crate::actor::DEFAULT_MAILBOX_CAPACITY,
            supervision: SupervisionStrategy::Stop,
        }
    }
}
