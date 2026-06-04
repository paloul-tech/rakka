#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Testkit utilities for local actor tests.

use std::time::Duration;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorRef, ActorSystem, Message, RakkaError,
    RakkaResult, Subsystem,
};
use tokio::sync::mpsc;

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-testkit";

/// Subsystem associated with testkit helpers.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::Testkit
}

/// Runs a future on Tokio for testkit callers.
pub async fn run_async<T>(future: impl std::future::Future<Output = T>) -> T {
    future.await
}

/// Probe actor that records every message it receives.
pub struct TestProbe<M>
where
    M: Message,
{
    actor_ref: ActorRef<M>,
    receiver: mpsc::Receiver<M>,
}

impl<M> TestProbe<M>
where
    M: Message,
{
    /// Spawns a probe actor in the provided system.
    pub fn spawn(system: &ActorSystem, name: impl AsRef<str>) -> RakkaResult<Self> {
        let (sender, receiver) = mpsc::channel(1024);
        let actor_ref = system.spawn_actor(name, ProbeActor { sender })?;
        Ok(Self {
            actor_ref,
            receiver,
        })
    }

    /// Returns the probe actor reference.
    #[must_use]
    pub fn actor_ref(&self) -> ActorRef<M> {
        self.actor_ref.clone()
    }

    /// Waits for the next probe message.
    pub async fn expect_message(&mut self, timeout: Duration) -> RakkaResult<M> {
        match tokio::time::timeout(timeout, self.receiver.recv()).await {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(RakkaError::new(
                Subsystem::Testkit,
                "probe-closed",
                "test probe channel closed",
            )),
            Err(_elapsed) => Err(RakkaError::new(
                Subsystem::Testkit,
                "probe-timeout",
                "timed out waiting for test probe message",
            )),
        }
    }
}

struct ProbeActor<M>
where
    M: Message,
{
    sender: mpsc::Sender<M>,
}

impl<M> Actor for ProbeActor<M>
where
    M: Message,
{
    type Msg = M;

    fn handle<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> rakka_core::ActorFuture<'a> {
        let sender = self.sender.clone();
        actor_future(async move {
            sender.send(msg).await.map_err(|_closed| {
                RakkaError::new(
                    Subsystem::Testkit,
                    "probe-receiver-closed",
                    "test probe receiver closed",
                )
            })?;
            Ok(ActorAction::Continue)
        })
    }
}
