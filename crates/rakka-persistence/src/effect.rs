//! Durable actor effects.

use std::fmt::{self, Debug, Formatter};

use rakka_core::ReplyTo;

use crate::store::DurableState;

/// Side effect executed after a durable state change commits.
pub type DurableSideEffect = Box<dyn FnOnce() + Send + 'static>;

/// Parts returned by [`DurableEffect::into_test_parts`].
pub type DurableEffectParts<S> = (DurableStateChange<S>, bool, Vec<DurableSideEffect>);

/// Durable state change selected by a command handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableStateChange<S>
where
    S: DurableState,
{
    /// Leave durable state unchanged.
    None,
    /// Mark a command as intentionally unhandled.
    Unhandled,
    /// Persist a replacement state value.
    Persist(S),
    /// Delete durable state.
    Delete,
}

/// Command effect returned by a durable actor.
pub struct DurableEffect<S>
where
    S: DurableState,
{
    state_change: DurableStateChange<S>,
    stop: bool,
    side_effects: Vec<DurableSideEffect>,
}

impl<S> DurableEffect<S>
where
    S: DurableState,
{
    /// Leaves durable state unchanged.
    #[must_use]
    pub fn none() -> Self {
        Self {
            state_change: DurableStateChange::None,
            stop: false,
            side_effects: Vec::new(),
        }
    }

    /// Marks a command as unhandled without changing durable state.
    #[must_use]
    pub fn unhandled() -> Self {
        Self {
            state_change: DurableStateChange::Unhandled,
            stop: false,
            side_effects: Vec::new(),
        }
    }

    /// Persists a replacement state.
    #[must_use]
    pub fn persist(state: S) -> Self {
        Self {
            state_change: DurableStateChange::Persist(state),
            stop: false,
            side_effects: Vec::new(),
        }
    }

    /// Deletes durable state.
    #[must_use]
    pub fn delete() -> Self {
        Self {
            state_change: DurableStateChange::Delete,
            stop: false,
            side_effects: Vec::new(),
        }
    }

    /// Stops the actor without changing durable state.
    #[must_use]
    pub fn stop() -> Self {
        Self {
            state_change: DurableStateChange::None,
            stop: true,
            side_effects: Vec::new(),
        }
    }

    /// Stops the actor after this effect is applied.
    #[must_use]
    pub fn and_stop(mut self) -> Self {
        self.stop = true;
        self
    }

    /// Runs a side effect after any selected state change commits.
    #[must_use]
    pub fn then_run(mut self, side_effect: impl FnOnce() + Send + 'static) -> Self {
        self.side_effects.push(Box::new(side_effect));
        self
    }

    /// Replies after any selected durable state change commits.
    #[must_use]
    pub fn then_reply<R>(self, reply_to: ReplyTo<R>, reply: R) -> Self
    where
        R: Send + 'static,
    {
        self.then_run(move || {
            let _ = reply_to.reply(reply);
        })
    }

    /// Replies without changing durable state.
    #[must_use]
    pub fn reply<R>(reply_to: ReplyTo<R>, reply: R) -> Self
    where
        R: Send + 'static,
    {
        Self::none().then_reply(reply_to, reply)
    }

    /// Leaves durable state unchanged and sends no reply.
    #[must_use]
    pub fn no_reply() -> Self {
        Self::none()
    }

    /// Returns the selected state change.
    #[must_use]
    pub const fn state_change(&self) -> &DurableStateChange<S> {
        &self.state_change
    }

    /// Returns true if the actor should stop after this effect.
    #[must_use]
    pub const fn should_stop(&self) -> bool {
        self.stop
    }

    pub(crate) fn into_parts(self) -> DurableEffectParts<S> {
        (self.state_change, self.stop, self.side_effects)
    }

    /// Splits this effect into parts for behavior testkits.
    #[must_use]
    pub fn into_test_parts(self) -> DurableEffectParts<S> {
        self.into_parts()
    }
}

impl<S> Debug for DurableEffect<S>
where
    S: DurableState + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableEffect")
            .field("state_change", &self.state_change)
            .field("stop", &self.stop)
            .field("side_effect_count", &self.side_effects.len())
            .finish()
    }
}
